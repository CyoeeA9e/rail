use anyhow::{Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub const PATTERN: &str = "Noise_NK_25519_ChaChaPoly_BLAKE2s";

pub async fn read_exact(stream: &mut TcpStream, len: usize) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    Ok(buf)
}

pub async fn read_msg(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let len_buf = read_exact(stream, 4).await?;
    let len = u32::from_be_bytes([len_buf[0], len_buf[1], len_buf[2], len_buf[3]]) as usize;
    read_exact(stream, len).await
}

pub async fn write_msg(stream: &mut TcpStream, data: &[u8]) -> Result<()> {
    let len = data.len() as u32;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(data).await?;
    Ok(())
}

pub async fn transport_send(
    stream: &mut TcpStream,
    transport: &mut snow::TransportState,
    plaintext: &[u8],
) -> Result<()> {
    let mut buf = vec![0u8; plaintext.len() + 64];
    let len = transport
        .write_message(plaintext, &mut buf)
        .context("transport encrypt failed")?;
    write_msg(stream, &buf[..len]).await
}

pub async fn transport_recv(
    stream: &mut TcpStream,
    transport: &mut snow::TransportState,
) -> Result<Vec<u8>> {
    let encrypted = read_msg(stream).await?;
    let mut buf = vec![0u8; encrypted.len()];
    let len = transport
        .read_message(&encrypted, &mut buf)
        .context("transport decrypt failed")?;
    buf.truncate(len);
    Ok(buf)
}
