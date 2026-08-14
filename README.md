# mp

`mp` is an experimental, Rust-native P2P file distribution and channel
messaging project built on Peeroxide.

The first implementation milestone is intentionally narrow:

```text
CID
  -> mp:// share link
  -> publisher announces the CID topic
  -> downloader verifies the complete file
  -> downloader becomes a seed
  -> propagation continues after the publisher exits
```

Project documents:

- [Roadmap](docs/roadmap.md)
- [Protocol v1](docs/protocol-v1.md)
- [First-round acceptance](docs/acceptance.md)
- [First-round implementation report](docs/first-round-report.md)
- [Peeroxide isolation test](docs/peeroxide-repro.md)

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
```

Use `--data-dir <path>` to isolate an identity and its verified objects. A
successful `publish` or `get` remains online and seeds until interrupted.

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
