# First-round implementation report

Date: 2026-08-14

## Outcome

Phases 0 through 3 are implemented at source level:

- Raw SHA-256 CIDv1 identity and exact digest topics.
- Strict `mp://` links and versioned `mp-file/1` frames.
- Persistent Ed25519 identity, content-addressed objects, and atomic holdings.
- Streaming request, offer, data, complete, size, and CID validation.
- Peeroxide node lifecycle, startup revalidation, topic rejoin, and connection
  routing.
- `publish`, `get`, `node`, `holdings`, and `doctor` CLI commands.
- Optional explicit Peeroxide blind-relay configuration.

The first-round network exit criteria are **not met**. Public HyperDHT
bootstrap and topic discovery work, but Peeroxide 1.7.3 did not deliver
application data after connection establishment on the tested devices.

## Verified checks

The repository checks passed:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

Covered behavior includes CID and frame golden values, strict parsing,
identity restart, duplicate import, missing/corrupt objects, corrupt metadata,
successful in-process multi-frame transfer, and size/CID failure cleanup.

`mp doctor` reached the public HyperDHT on all three devices:

- macOS x86_64 development machine.
- Ubuntu 24.04 x86_64 at `x.most.red`.
- Debian 12 x86_64 at `192.168.31.52` behind NAT.

The initial zero-holding doctor run exposed and fixed a lifecycle bug: calling
`SwarmHandle::flush()` before any topic join waits indefinitely in Peeroxide
1.7.3. `mp` now flushes startup joins only when at least one valid holding was
announced.

Release measurements:

| Build | Size | SHA-256 |
| --- | ---: | --- |
| x86_64 Linux musl static PIE | 2,771,664 bytes | `39eb4f404ecb77ef79260e1f49996a8aa51aaf8d697156fe6d0709629c8f7cf5` |
| x86_64 Linux glibc 2.36 PIE | 2,659,064 bytes | `5c0a2e5b76e39a41e567e10df23e0cfc7ea006c01f07bbf577502a1f1aa92f22` |

Both are comfortably below the 10 MB core target. Size alone does not satisfy
the performance gate.

## 100 MiB reproduction

Publisher A generated this source on `x.most.red`:

```text
size    104857600
sha256  e1ae7836e12cc2b2fd7944408e8ad0601e0a7b1080dce6200ea37f1ac78466f4
cid     bafkreihbvz4dnyjmykzp26keichivudadyfhweea3ttcadvdp4nmpbdg6q
peer    e562fcfbbd8d0e1094a753b21d2b9b8bb3ee7f5eb2e15a92cd9a50328f993ea5
```

An independent Peeroxide CLI lookup found exactly that peer on the raw
SHA-256 digest topic. NATed downloader B also logged the peer discovery and a
successful punch:

```text
discovered peer pk=e562fcfb
NAT settled + verified remote
holepuncher(initiator): probe received
punch successful
peer connected pk=e562fcfb
```

No `mp-file/1` request arrived intact. A eventually reported:

```text
file request failed ... RTO timeout exceeded
```

The same failure boundary was reproduced with:

- musl static builds on A and B;
- glibc 2.36 builds on A and B (`handshake failed: empty reply` on one run);
- a second identity on the public host;
- macOS publisher to the same-LAN Debian device;
- a dedicated Peeroxide blind relay, which received no sessions before the
  client deadline.

This isolates the current blocker below `mp-file/1`, in Peeroxide's
handshake/UDX connection path. The in-process message transport proves the
application state machine, but it is not a substitute for device acceptance.

## Remaining acceptance work

1. Reduce the failure to Peeroxide's own minimal write/read example and report
   it upstream with the two-device logs.
2. Pin a Peeroxide commit or release that carries bytes reliably on these
   devices.
3. Repeat 100 MiB in both directions.
4. Complete A publishes, B downloads, A exits, C downloads from B.
5. Restart B with `mp node` and repeat C discovery and CID verification.
6. Run interrupted-transfer and corrupt-object restart checks over the real
   network path.

Until those checks pass, phases 2 and 3 remain implemented but not accepted.
