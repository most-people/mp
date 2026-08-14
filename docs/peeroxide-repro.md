# Peeroxide 1.7.3 minimal reproduction

This program tests only Peeroxide discovery, connection establishment, and a
four-byte `ping` / `pong` exchange. It does not depend on `mp-core`, CID
handling, storage, or the `mp-file/1` protocol.

## Build

```bash
cargo build --release --locked -p peeroxide-repro
```

Generate one unique 32-byte topic for a test run:

```bash
openssl rand -hex 32
```

Use the same value as `TOPIC` on both devices. Start the public or otherwise
reachable device first:

```bash
RUST_LOG=info,peeroxide=debug,peeroxide_dht=debug,libudx=debug \
  ./peeroxide-repro --topic TOPIC --timeout 60 \
  server --rounds 30 --overall-timeout 2400
```

After the server prints `READY`, start the other device:

```bash
RUST_LOG=info,peeroxide=debug,peeroxide_dht=debug,libudx=debug \
  ./peeroxide-repro --topic TOPIC --timeout 60 \
  client --rounds 30 --delay-ms 500
```

The client creates a new random Peeroxide identity and a new swarm for every
round. This tests 30 independent discovery, connection, and first-frame paths,
instead of sending 30 messages through one already-established connection.

## Result interpretation

Both processes must exit successfully with:

```text
SUMMARY role=server expected=30 accepted=30 passed=30 failed=0 deadline_elapsed=false
SUMMARY role=client expected=30 passed=30 failed=0
```

The first failing stage narrows the fault:

| Stage | Boundary |
| --- | --- |
| `bootstrap` | Public HyperDHT reachability |
| `join` / `flush` | Topic announce or lookup |
| `connect` | Discovery, handshake, or NAT traversal |
| `write` / `read` | SecretStream or UDX application-byte transport |
| `validate` | Corrupt or unexpected application bytes |
| `shutdown` | Peeroxide lifecycle cleanup |

Preserve stdout and stderr from both endpoints. An upstream report should
include the exact source revision, Rust target, operating system, NAT layout,
topic, both logs, and whether musl and glibc builds behave differently.

## Recorded result

The 2026-08-15 run used the same static x86-64 Linux binary on Ubuntu 24.04 at
`x.most.red` and Debian 12 behind NAT at `192.168.31.52`:

```text
binary sha256 45c54e91a09d0c3d7107777ec6923595fb9d8399d7aae4f50bd3c407d576b829
topic         fe4d765a9c21d20ac06bd1353dd74a47bf737a3609c18f3bc27383004c3dd9a7
server        expected=30 accepted=30 passed=30 failed=0
client        expected=30 passed=30 failed=0
```

Every client round used a fresh swarm identity. This result rules out a basic
Peeroxide 1.7.3 discovery, hole-punch, first-write, or first-read failure on
that device pair. No upstream defect report is warranted from the earlier
`mp` timeout symptom.
