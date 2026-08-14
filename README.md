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

## Development

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked

cargo run --bin mp -- doctor
cargo run --bin mp -- publish ./example.bin
cargo run --bin mp -- get 'mp://<cid>?filename=example.bin'
cargo run --bin mp -- node
cargo run --bin mp -- holdings
```

Use `--data-dir <path>` to isolate an identity and its verified objects. A
successful `publish` or `get` remains online and seeds until interrupted.

Peeroxide blind-relay mode is available for diagnostics:

```bash
mp --force-relay '<32-byte-hex-public-key>@<ip>:<port>' node
```

The first-round implementation is present, but cross-device transfer is not
yet accepted because Peeroxide 1.7.3 establishes discovery/handshake state and
then fails to carry application data on the tested paths. See the report for
the exact evidence; do not treat this repository as a working release yet.

The repository is experimental. Protocol and storage compatibility with
MostBox, Hyperdrive, BitTorrent, and IPFS UnixFS is not a goal for v1.
