# First-round acceptance

This document defines completion for phases 0 through 3.

## Automated checks

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Required coverage:

- CID golden values and streaming calculation.
- Link canonicalization and rejection cases.
- Control/data frame encoding, limits, and malformed input.
- Identity persistence.
- Atomic holding persistence and corrupt metadata handling.
- Import, duplicate import, missing object, and corrupt object behavior.
- Successful transfer, size mismatch, and CID mismatch behavior using an
  in-process transport harness.

## Two-device network acceptance

Devices:

- Public host: `root@x.most.red`
- NATed host: `root@192.168.31.52`

Run both directions with a generated 100 MiB file:

1. Start a publisher and wait until its CID topic is announced.
2. Start a downloader with only the printed `mp://` link.
3. Require direct or explicitly reported relayed connectivity.
4. Require identical file length and CID.
5. Confirm the downloader reports the holding and remains a seed.

## Three-node propagation acceptance

Use the local development machine as the third node when it can reach the
public HyperDHT network.

1. A publishes a file.
2. B downloads and verifies it.
3. Stop A and confirm its process is gone.
4. C downloads the same link while only B is seeding.
5. Compare C's CID and SHA-256 digest with A's source.
6. Restart B with `mp node` and repeat discovery from C.

## Failure acceptance

- Kill a sender midway through a transfer: no final object or holding may be
  created on the receiver.
- Modify a temporary download before verification: the CID check must fail.
- Delete or corrupt an object and restart: its topic must not be announced.
- Send an oversized control/data frame: close only that connection.
- Send an unknown protocol version: return `bad_request` and keep serving
  other peers.

## First-round exit criteria

- All automated checks pass.
- Both two-device directions pass.
- Publisher-exit propagation and seeder restart pass.
- No system-wide service or network configuration is required.
- Known limitations and exact test commands are recorded in the final report.
