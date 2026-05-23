use std::env;
use std::io::{self, Write};

use anyhow::{Context, Result};
use snow::Builder;
use tokio::net::TcpStream;

use rail::{read_msg, transport_recv, transport_send, write_msg, PATTERN};

async fn client_handshake(
    stream: &mut TcpStream,
    server_pub: &[u8],
) -> Result<snow::TransportState> {
    let params: snow::params::NoiseParams = PATTERN.parse().context("invalid noise pattern")?;
    let mut handshake = Builder::new(params)
        .remote_public_key(server_pub)
        .build_initiator()
        .context("failed to build initiator")?;

    let mut buf = vec![0u8; 65535];
    let mlen = handshake
        .write_message(&[], &mut buf)
        .context("failed to write handshake msg 1")?;
    write_msg(stream, &buf[..mlen]).await?;

    let msg2 = read_msg(stream).await?;
    let _ = handshake
        .read_message(&msg2, &mut buf)
        .context("failed to read handshake msg 2")?;

    handshake
        .into_transport_mode()
        .context("failed to enter transport mode")
}

async fn send_cmd(
    stream: &mut TcpStream,
    transport: &mut snow::TransportState,
    cmd: &str,
) -> Result<()> {
    transport_send(stream, transport, cmd.as_bytes()).await?;

    if cmd.starts_with("SEND") {
        let resp = transport_recv(stream, transport).await?;
        println!("  S: {}", String::from_utf8_lossy(&resp));
        if resp.starts_with(b"OK") {
            let from = read_line("  FROM: ")?;
            transport_send(stream, transport, format!("FROM:{}", from).as_bytes()).await?;
            let resp = transport_recv(stream, transport).await?;
            println!("  S: {}", String::from_utf8_lossy(&resp));

            let to = read_line("  TO: ")?;
            transport_send(stream, transport, format!("TO:{}", to).as_bytes()).await?;
            let resp = transport_recv(stream, transport).await?;
            println!("  S: {}", String::from_utf8_lossy(&resp));

            let body = read_line("  body: ")?;
            transport_send(stream, transport, body.as_bytes()).await?;
            let resp = transport_recv(stream, transport).await?;
            println!("  S: {}", String::from_utf8_lossy(&resp));
        }
    } else if cmd == "LIST" {
        let resp = transport_recv(stream, transport).await?;
        let text = String::from_utf8_lossy(&resp);
        println!("  S: {}", text);
        if let Some(count_str) = text.strip_prefix("OK LIST ") {
            if let Ok(count) = count_str.trim().parse::<usize>() {
                for _ in 0..count {
                    let line = transport_recv(stream, transport).await?;
                    println!("  S: {}", String::from_utf8_lossy(&line));
                }
            }
        }
    } else if cmd.starts_with("FETCH ") {
        let resp = transport_recv(stream, transport).await?;
        println!("  S: {}", String::from_utf8_lossy(&resp));
        if resp.starts_with(b"OK") {
            let body = transport_recv(stream, transport).await?;
            println!("  S: body = {}", String::from_utf8_lossy(&body));
        }
    } else {
        let resp = transport_recv(stream, transport).await?;
        println!("  S: {}", String::from_utf8_lossy(&resp));
    }

    Ok(())
}

fn read_line(prompt: &str) -> io::Result<String> {
    print!("{}", prompt);
    io::stdout().flush()?;
    let mut line = String::new();
    io::stdin().read_line(&mut line)?;
    Ok(line.trim().to_string())
}

fn print_usage() {
    eprintln!("Usage: railc <host> <port> <server_pubkey_hex>");
    eprintln!("  server_pubkey_hex: the hex-encoded server static public key");
    eprintln!();
    eprintln!("Commands: HELO <name>, SEND, LIST, FETCH <id>, QUIT");
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = env::args().collect();
    if args.len() < 4 {
        print_usage();
        std::process::exit(1);
    }

    let host = &args[1];
    let port: u16 = args[2].parse().context("invalid port")?;
    let server_pub = hex::decode(&args[3]).context("invalid server public key hex")?;

    println!("Connecting to {}:{} ...", host, port);
    let mut stream = TcpStream::connect(format!("{}:{}", host, port))
        .await
        .context("connection failed")?;
    println!("Connected, performing Noise NK handshake...");

    let mut transport = client_handshake(&mut stream, &server_pub).await?;
    println!("Handshake complete, secure channel established.");
    println!("Type commands (HELO, SEND, LIST, FETCH <id>, QUIT)");

    loop {
        let cmd = read_line("> ")?;
        if cmd.is_empty() {
            continue;
        }

        if cmd == "QUIT" {
            send_cmd(&mut stream, &mut transport, &cmd).await?;
            break;
        }

        if let Err(e) = send_cmd(&mut stream, &mut transport, &cmd).await {
            eprintln!("Error: {:?}", e);
            break;
        }
    }

    Ok(())
}
