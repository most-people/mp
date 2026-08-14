# First-round implementation report

Updated: 2026-08-15

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

The first-round MVP exit criteria are met on the tested devices. The 100 MiB
sample passed in both directions, a restarted downloader propagated it after
the original publisher exited, and interrupted/corrupt objects never became
seeds. The earlier Peeroxide blocker diagnosis was incorrect: a 90-second
application deadline cancelled a healthy transfer that required almost six
minutes on the tested path.

## Automated checks

The repository checks pass:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

The 25 tests cover CID and frame golden values, strict parsing, identity
restart, duplicate import, missing/corrupt objects, corrupt metadata,
successful in-process multi-frame transfer, size/CID failure cleanup, the
Peeroxide reproduction CLI, and stalled-peer timeout behavior.

`mp doctor` reached the public HyperDHT on all three devices:

- macOS x86_64 development machine.
- Ubuntu 24.04 x86_64 at `x.most.red`.
- Debian 12 x86_64 at `192.168.31.52` behind NAT.

The initial zero-holding doctor run exposed and fixed a lifecycle bug: calling
`SwarmHandle::flush()` before any topic join waits indefinitely in Peeroxide
1.7.3. `mp` now flushes startup joins only when at least one valid holding was
announced.

The accepted x86-64 Linux musl build is a stripped, static PIE:

```text
size    2603752
sha256  156ed3c144cad6cfb72ef2ef3b3c2e8d67d73a7bcde065ffc40cf38db0c60914
```

It remains comfortably below the 10 MB core target. This exact artifact also
passed a final public-to-NAT 1 KiB discovery, transfer, CID verification, and
automatic-seeding smoke test with the default discovery deadline.

## Peeroxide isolation

The independent `peeroxide-repro` crate uses only Peeroxide discovery,
connection establishment, and a four-byte `ping` / `pong` exchange. The server
ran on `x.most.red`; the client ran behind NAT on `192.168.31.52` and created a
fresh swarm identity for every round.

```text
server expected=30 accepted=30 passed=30 failed=0 deadline_elapsed=false
client expected=30 passed=30 failed=0
```

The static binary was 2,118,432 bytes with SHA-256
`45c54e91a09d0c3d7107777ec6923595fb9d8399d7aae4f50bd3c407d576b829`.
This rules out a general Peeroxide 1.7.3 first-frame failure on the tested
network path.

## File-transfer diagnosis

The same two devices then passed complete `mp-file/1` transfers for 1 KiB and
64 KiB files. Debug logs confirmed the exact message sequence, including a
65,537-byte tagged data message for the 64 KiB file.

The original 100 MiB sample was:

```text
size    104857600
sha256  e1ae7836e12cc2b2fd7944408e8ad0601e0a7b1080dce6200ea37f1ac78466f4
cid     bafkreihbvz4dnyjmykzp26keichivudadyfhweea3ttcadvdp4nmpbdg6q
peer    e562fcfbbd8d0e1094a753b21d2b9b8bb3ee7f5eb2e15a92cd9a50328f993ea5
```

An initial diagnostic rerun with a 600-second deadline transferred all 1,600
data frames from the public publisher to the NATed downloader in approximately
5 minutes 56 seconds. The downloaded CID and SHA-256 matched the source
exactly.

The earlier run used one 90-second absolute deadline for discovery plus the
entire transfer. `timeout_at` cancelled `receive_file` while the publisher was
still sending valid data. Once the receiving connection disappeared, the
publisher eventually logged `RTO timeout exceeded`. The generic log label
`file request failed` covered the whole server transfer and did not prove that
the request was missing; the successful rerun shows the request was a valid
110-byte frame.

The fix separates the timeout semantics:

- `--discovery-timeout` limits finding and connecting to a seed.
- Total transfer duration is not capped after connection establishment.
- A connection is abandoned only after 120 seconds without a successful peer
  read or write.

The fixed release build then repeated public-to-NAT with the default 90-second
discovery deadline. Transfer continued beyond 90 seconds and completed in
approximately 5 minutes 43 seconds with the same CID and SHA-256.

## Propagation and restart

The original public publisher A was stopped and its process confirmed absent.
The NATed downloader B was restarted with `mp node`; it revalidated the object,
restored the same persistent identity, joined one CID topic, and reported
`READY 1 topics`.

A fresh identity and empty store C on the public host then downloaded the full
100 MiB object only from B. B logged an accepted request and a complete
104,857,600-byte response. C reported B's public key as `SOURCE_PEER`, produced
the expected CID and SHA-256, and automatically announced the topic as a new
seed. The NAT-to-public transfer took approximately 5 minutes 45 seconds.

## Failure paths

- A cloned holding was modified by one byte. `holdings` reported
  `valid=0 invalid=1`, and node restart reported `READY 0 topics`.
- A sender was stopped after the receiver wrote 16,252,928 temporary bytes.
  The receiver failed after the 120-second idle timeout with no final object,
  no temporary object, and `valid=0 invalid=0`.
- Oversized frames and unknown protocol versions remain covered by the
  protocol tests and fail closed before content is accepted.

## Acceptance status

- Automated formatting, lint, and tests pass.
- Public-to-NAT and NAT-to-public 100 MiB transfers pass.
- Publisher-exit propagation and downloader restart pass.
- CID and SHA-256 verification pass at every completed hop.
- Interrupted and corrupt content never becomes a holding or announced topic.
- No system-wide service, open inbound port, or router configuration was
  required.

Phases 0 through 3 are accepted for this experimental first-round scope. The
next implementation phase is live channel messaging; broader performance,
soak, abuse, and mobile lifecycle testing remain pre-release work.
