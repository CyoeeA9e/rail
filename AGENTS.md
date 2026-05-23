## Project

Rail — communication server with custom protocol. Single port 7411, Noise NK encryption, SMTP/IMAP-inspired commands.

```
src/lib.rs          # shared noise framing helpers
src/bin/server.rs   # server binary   → cargo run --bin server
src/bin/client.rs   # client binary   → cargo run --bin client <host> <port> <key>
```

## How to run

```bash
# Terminal 1 — start server (key printed to stderr)
cargo run --bin server

# Terminal 2 — connect client
cargo run --bin client 127.0.0.1 7411 <SERVER_KEY>
```

## Noise NK handshake

- Pattern: `Noise_NK_25519_ChaChaPoly_BLAKE2s` (X25519 + ChaChaPoly + BLAKE2s)
- **Server** (responder): `Builder::new(params).local_private_key(&key).build_responder()`
- **Client** (initiator): `Builder::new(params).remote_public_key(&key).build_initiator()`
- Framing: 4-byte big-endian length prefix on every message (handshake + transport)
- `into_transport_mode()` returns a single `TransportState` (both read + write)

## Rail protocol

Each Noise transport message = one command/response string (no line terminators).

```
C: HELO <name>              → S: OK HELO <name>
C: SEND                     → S: OK SEND
C: FROM:<addr>              → S: OK FROM
C: TO:<addr>                → S: OK TO
C: <body>                   → S: OK STORED <id>
C: LIST                     → S: OK LIST <count> + <n> summary lines
C: FETCH <id>               → S: OK FETCH <from>|<to> + body
C: QUIT                     → S: OK BYE
```

## Architecture

- **`lib.rs`**: `read_msg`, `write_msg`, `transport_send`, `transport_recv`, `PATTERN`
- **`server.rs`**: `MailStore` (in-memory), `Session` (state machine), per-connection `tokio::spawn`
- **`client.rs`**: interactive loop with `read_line()` (returns `io::Result<String>`)

### Session state machine

`Session::process()` dispatches to per-state methods (`process_ready`, `process_send_from`, etc.), each using early returns. States: `Ready` ↔ `SendFrom` → `SendTo` → `SendData` → `Ready`.

## Rust Code Style

### Control Flow

- Prefer early-return over nested conditionals.
- Avoid deep nesting and excessive indentation.
- First reduce nesting with early-return.
- If indentation is still deep, extract the nested logic into a separate function.
- Prefer `match` only when handling meaningful variants.

### Data Transformations

- Prefer functional style when it improves readability.
- Prefer iterator chains over imperative temporary variables.
- Prefer `map`, `filter`, `filter_map`, `find`, `flat_map`, `collect`.
- Avoid manual loops for simple transformations.
- Avoid mutable temporary state when unnecessary.
- Avoid excessively long chains; split steps if readability suffers.

### Ownership

- Prefer borrowing over cloning.
- Avoid unnecessary `.clone()`.
- Prefer `&str` over `String`.
- Prefer `&[T]` over `&Vec<T>`.

### Error Handling

- Prefer `Result<T, E>`.
- Use `?` for propagation.
- Avoid `unwrap()` in production code.
- Do not silently ignore errors.

### General

- Prefer readability over cleverness.
- Keep patches small and localized.
- Preserve existing behavior unless explicitly requested.
- Avoid unrelated refactors.
- Match existing repository conventions.

### Validation

Run when possible:

```bash
cargo fmt
cargo clippy --all-targets --all-features
cargo test
```
