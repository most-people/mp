# mp

`mp` is an experimental, Rust-native P2P file distribution and channel
messaging project built on Peeroxide.

The file-propagation milestone remains intentionally narrow:

```text
CID
  -> mp:// share link
  -> publisher announces the CID topic
  -> downloader verifies the complete file
  -> downloader becomes a seed
  -> propagation continues after the publisher exits
```

Phase 4 also implements live capability channels with signed per-writer hash
chains, duplicate suppression, and ephemeral presence/typing. History backfill
and attachments remain out of scope until Phase 5.

Project documents:

- [Roadmap](docs/roadmap.md)
- [Protocol v1](docs/protocol-v1.md)
- [First-round acceptance](docs/acceptance.md)
- [First-round implementation report](docs/first-round-report.md)
- [Peeroxide isolation test](docs/peeroxide-repro.md)
- [Phase 4 channel acceptance](docs/channel-acceptance.md)

## Development

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked

cargo run --bin mp -- doctor
cargo run --bin mp -- publish ./example.bin
cargo run --bin mp -- get 'mp://<cid>?filename=example.bin' --discovery-timeout 90
cargo run --bin mp -- node
cargo run --bin mp -- holdings
cargo run --bin mp -- channel create "team"
cargo run --bin mp -- channel join 'mp-channel://...'
cargo run --bin mp -- channel open '<channel-id>'
cargo run --bin mp -- channel invite '<channel-id>'
cargo run --bin mp -- channel list
```

Use `--data-dir <path>` to isolate an identity and its verified objects. A
successful `publish` or `get` remains online and seeds until interrupted.

`channel create` persists and prints a capability invite; `channel invite`
explicitly reveals it again. `channel join` and `channel open` enter a live
session without writing the capability to routine logs: plain input sends text,
`/typing on` and `/typing off` emit transient state, and `/quit` exits. Phase 4
is deliberately live-only; history synchronization and attachments are the
next phase.

Peeroxide blind-relay mode is available for diagnostics:

```bash
mp --force-relay '<32-byte-hex-public-key>@<ip>:<port>' node
```

Peeroxide 1.7.3 passed 30 independent cross-device `ping` / `pong` connections.
The 100 MiB sample passed in both directions with matching CID and SHA-256,
including publisher-exit propagation through a restarted downloader seed. The
first-round MVP is accepted on the tested devices; see the report for exact
evidence and remaining pre-release work.

The repository is experimental. Protocol and storage compatibility with
MostBox, Hyperdrive, BitTorrent, and IPFS UnixFS is not a goal for v1.
