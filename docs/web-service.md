# Local web service

Status: experimental Phase 4 test interface.

## Start

```bash
cargo run --locked --bin mp -- --data-dir /tmp/mp-web-test web
```

The default URL is `http://127.0.0.1:1976`. The HTML, CSS, and JavaScript are
embedded in the Rust binary; no Node.js runtime or separate asset server is
required.

The browser can create, join, and open local channels, copy an invite, load
locally persisted text, display presence/typing events, and send live text.
Opening another channel stops the current channel node before starting the new
one.

## HTTP and WebSocket surface

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/api/status` | Local channels and active session |
| `POST` | `/api/channels` | Create and open a channel |
| `POST` | `/api/channels/join` | Persist an invite and open it |
| `POST` | `/api/channels/{id}/open` | Open a local channel |
| `GET` | `/api/channels/{id}/invite` | Explicitly reveal its capability |
| `GET` | `/api/channels/{id}/messages` | Read validated local text history |
| `POST` | `/api/messages` | Sign, persist, and broadcast live text |
| `POST` | `/api/typing` | Broadcast transient typing state |
| WebSocket | `/api/events` | Session snapshots and channel events |

All mutation requests use JSON. Request bodies are limited to 16 KiB, unknown
JSON fields are rejected, CORS is not enabled, and responses carry restrictive
browser security headers.

## Exposure boundary

The service has no user authentication. It listens on loopback by default
because the invite endpoint exposes channel capabilities. For access from
another computer, keep the service on loopback and forward it through SSH:

```bash
ssh -L 1976:127.0.0.1:1976 user@host-running-mp
```

Then open `http://127.0.0.1:1976` on the client computer. Passing a non-loopback
`--listen` address is an explicit diagnostic override and prints a warning.

## Phase boundary

The browser shows the local validated message store but does not synchronize
missing history from peers. Sending still requires an online peer. History head
exchange, offline recovery, attachments, and multiple simultaneously active
channels remain Phase 5 work.
