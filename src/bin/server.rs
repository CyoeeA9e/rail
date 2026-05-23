use anyhow::{Context, Result};
use snow::Builder;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

use rail::{read_msg, transport_recv, transport_send, write_msg, PATTERN};

const PORT: u16 = 7411;

#[derive(Debug, Clone)]
struct Message {
    id: u64,
    from: String,
    to: String,
    body: String,
}

struct MailStore {
    messages: Vec<Message>,
    next_id: u64,
}

impl MailStore {
    fn new() -> Self {
        MailStore {
            messages: Vec::new(),
            next_id: 1,
        }
    }

    fn store(&mut self, from: String, to: String, body: String) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        self.messages.push(Message { id, from, to, body });
        id
    }

    fn list(&self) -> &[Message] {
        &self.messages
    }

    fn get(&self, id: u64) -> Option<&Message> {
        self.messages.iter().find(|m| m.id == id)
    }
}

#[derive(Debug)]
enum SessionState {
    Ready,
    SendFrom,
    SendTo,
    SendData,
}

struct Session {
    identity: String,
    from: String,
    to: String,
    body: String,
    state: SessionState,
    store: Arc<Mutex<MailStore>>,
}

impl Session {
    fn new(store: Arc<Mutex<MailStore>>) -> Self {
        Session {
            identity: String::new(),
            from: String::new(),
            to: String::new(),
            body: String::new(),
            state: SessionState::Ready,
            store,
        }
    }

    async fn process(&mut self, cmd: &str) -> Result<Option<Vec<String>>> {
        match self.state {
            SessionState::Ready => self.process_ready(cmd).await,
            SessionState::SendFrom => Ok(Some(self.process_send_from(cmd))),
            SessionState::SendTo => Ok(Some(self.process_send_to(cmd))),
            SessionState::SendData => self.process_send_data(cmd).await,
        }
    }

    async fn process_ready(&mut self, cmd: &str) -> Result<Option<Vec<String>>> {
        if let Some(name) = cmd.strip_prefix("HELO ") {
            self.identity = name.to_string();
            return Ok(Some(vec![format!("OK HELO {}", name)]));
        }
        if cmd == "SEND" {
            self.state = SessionState::SendFrom;
            self.from.clear();
            self.to.clear();
            self.body.clear();
            return Ok(Some(vec!["OK SEND".to_string()]));
        }
        if cmd == "LIST" {
            let store = self.store.lock().await;
            let msgs = store.list();
            let mut lines = vec![format!("OK LIST {}", msgs.len())];
            lines.extend(msgs.iter().map(|msg| {
                let preview = msg
                    .body
                    .lines()
                    .next()
                    .unwrap_or("")
                    .chars()
                    .take(40)
                    .collect::<String>();
                format!("{}|{}|{}|{}", msg.id, msg.from, msg.to, preview)
            }));
            return Ok(Some(lines));
        }
        if let Some(id_str) = cmd.strip_prefix("FETCH ") {
            let id: u64 = id_str.trim().parse().context("invalid message id")?;
            let store = self.store.lock().await;
            return if let Some(msg) = store.get(id) {
                Ok(Some(vec![
                    format!("OK FETCH {}|{}", msg.from, msg.to),
                    msg.body.clone(),
                ]))
            } else {
                Ok(Some(vec![format!("ERR NOT_FOUND {}", id)]))
            };
        }
        if cmd == "QUIT" {
            return Ok(None);
        }
        if cmd.starts_with("HELO") {
            return Ok(Some(vec!["ERR HELO requires a name".to_string()]));
        }
        Ok(Some(vec![format!("ERR UNKNOWN unknown command: {}", cmd)]))
    }

    fn process_send_from(&mut self, cmd: &str) -> Vec<String> {
        if let Some(addr) = cmd.strip_prefix("FROM:") {
            self.from = addr.to_string();
            self.state = SessionState::SendTo;
            vec!["OK FROM".to_string()]
        } else {
            vec!["ERR EXPECTED FROM:<addr>".to_string()]
        }
    }

    fn process_send_to(&mut self, cmd: &str) -> Vec<String> {
        if let Some(addr) = cmd.strip_prefix("TO:") {
            self.to = addr.to_string();
            self.state = SessionState::SendData;
            vec!["OK TO".to_string()]
        } else {
            vec!["ERR EXPECTED TO:<addr>".to_string()]
        }
    }

    async fn process_send_data(&mut self, cmd: &str) -> Result<Option<Vec<String>>> {
        self.body = cmd.to_string();
        self.state = SessionState::Ready;
        let id =
            self.store
                .lock()
                .await
                .store(self.from.clone(), self.to.clone(), self.body.clone());
        Ok(Some(vec![format!("OK STORED {}", id)]))
    }
}

async fn server_handshake(
    stream: &mut TcpStream,
    private_key: &[u8],
) -> Result<snow::TransportState> {
    let params: snow::params::NoiseParams = PATTERN.parse().context("invalid noise pattern")?;
    let mut handshake = Builder::new(params)
        .local_private_key(private_key)
        .build_responder()
        .context("failed to build responder")?;

    let msg1 = read_msg(stream).await?;
    let mut buf = vec![0u8; 65535];
    let _ = handshake
        .read_message(&msg1, &mut buf)
        .context("failed to read handshake msg 1")?;

    let mlen = handshake
        .write_message(&[], &mut buf)
        .context("failed to write handshake msg 2")?;
    write_msg(stream, &buf[..mlen]).await?;

    handshake
        .into_transport_mode()
        .context("failed to enter transport mode")
}

async fn handle_client(
    mut stream: TcpStream,
    private_key: &[u8],
    store: Arc<Mutex<MailStore>>,
) -> Result<()> {
    let mut transport = server_handshake(&mut stream, private_key).await?;
    info!("Noise NK handshake complete");

    let mut session = Session::new(store);

    loop {
        let cmd_bytes = transport_recv(&mut stream, &mut transport).await?;
        let cmd = String::from_utf8_lossy(&cmd_bytes);
        debug!("<< {}", cmd.trim());

        match session.process(&cmd).await {
            Ok(Some(responses)) => {
                for resp in &responses {
                    debug!(">> {}", resp);
                    transport_send(&mut stream, &mut transport, resp.as_bytes()).await?;
                }
            }
            Ok(None) => {
                transport_send(&mut stream, &mut transport, b"OK BYE").await?;
                break;
            }
            Err(e) => {
                warn!("command error: {:?}", e);
                transport_send(&mut stream, &mut transport, format!("ERR {}", e).as_bytes())
                    .await?;
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("rail=info")
        .init();

    let params: snow::params::NoiseParams = PATTERN.parse().context("invalid noise pattern")?;
    let builder = Builder::new(params);
    let keypair = builder
        .generate_keypair()
        .context("failed to generate keypair")?;
    let pk_hex = hex::encode(&keypair.public);

    info!("Rail Server v0.1.0");
    info!("Port: {}", PORT);
    info!("Noise: {}", PATTERN);
    info!("Server Public Key: {}", pk_hex);
    eprintln!("SERVER_KEY={}", pk_hex);

    let store = Arc::new(Mutex::new(MailStore::new()));
    let listener = TcpListener::bind(format!("0.0.0.0:{}", PORT))
        .await
        .context("failed to bind")?;

    info!("Listening on port {}", PORT);

    loop {
        let (stream, addr) = listener.accept().await?;
        info!("New connection from {}", addr);

        let store = store.clone();
        let pk = keypair.private.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_client(stream, &pk, store).await {
                error!("{}: {:?}", addr, e);
            }
            info!("{} disconnected", addr);
        });
    }
}
