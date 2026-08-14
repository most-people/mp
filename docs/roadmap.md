# mp implementation roadmap

## Goal

Build a Rust-native P2P core that uses one encrypted networking stack for
content-addressed file propagation and channel messaging. The core must be
suitable for a small CLI daemon first and native Android/iOS applications
later.

## Product model

- A file is identified only by its CID.
- A seed stores a complete verified copy of a file.
- A successful publisher or downloader keeps seeding by default.
- Availability comes from currently online seeds; permanent storage is not
  promised.
- Channel attachments reference a CID and reuse the file protocol.
- A magnet link, BitTorrent compatibility, cloud storage, payments, and
  blockchain features are outside the protocol.

## Phase 0: freeze protocol boundaries

Deliverables:

- Canonical file CID, share-link, topic, frame, identity, and error formats.
- A versioned `mp-file/1` protocol.
- A reserved `mp-channel/1` protocol boundary for the next implementation
  round.
- Golden byte and parsing tests.
- An exact Peeroxide dependency version.

Exit criteria:

- The same bytes always produce the same CID.
- Link and frame encoding round trips without ambiguity.
- Limits for control frames, data frames, and file size are explicit.

## Phase 1: Rust workspace and node foundation

Keep the product implementation in two crates:

```text
crates/mp-core  protocol, storage, node lifecycle, Peeroxide integration
crates/mp-cli   daemon and acceptance-test interface
```

The workspace also contains `peeroxide-repro`, an isolated diagnostic binary
with no dependency on `mp-core`.

Exit criteria:

- Formatting, clippy, and workspace tests pass.
- A node owns a persistent identity and reports its public key.
- The CLI can start a node and reach the public HyperDHT bootstrap network.

## Phase 2: single-source file loop

Deliverables:

- Import a single file into the object store.
- Generate a CID and `mp://` link.
- Announce the CID topic through Peeroxide.
- Request and stream a file from one seed.
- Write downloads to a temporary file, recompute the CID, and atomically
  promote only verified content.
- Emit concise progress and peer logs.

Exit criteria:

- Both directions pass between a public host and a NATed host.
- File length and CID match exactly.
- Interrupted or corrupt transfers never become holdings.

## Phase 3: persistent seeding and propagation

Deliverables:

- Persist verified holdings independently from user-visible paths.
- Revalidate holdings and rejoin all valid CID topics after restart.
- Make a successful downloader a seed without another import step.
- List CID, size, local object path, and current topic state.

Highest-priority acceptance path:

```text
A publishes
  -> B downloads and verifies
  -> A exits
  -> C downloads only from B
  -> B restarts and can seed again
```

## Phase 4: live channel messaging

Deliverables:

- Long-term Ed25519 identity.
- Capability-style channel invites and private discovery topics.
- One signed append-only hash chain per writer.
- Live text messages, deduplication, and signature verification.
- Ephemeral presence and typing events that are not stored in history.

## Phase 5: history and attachments

Deliverables:

- Exchange writer heads and request missing sequence ranges.
- Restore history after reconnect without replaying known messages.
- Let a new member sync history from any online member.
- Store only CID, display name, and size in attachment messages.

## Phase 6: BitTorrent-inspired transfer improvements

Add these only after measurements show a need:

- Fixed-size transfer chunks and a persisted receive bitmap.
- Resume after restart.
- Multi-peer range downloading.
- Piece availability, rarest-first scheduling, and endgame cancellation.

Chunks are transfer units, not a long-term sharded storage model. Every seed
still owns the complete verified object.

## Phase 7: performance gate

Compare against the Node.js implementation on the same devices and network
paths:

- 100 MiB and 1 GiB transfers.
- Public-to-NAT, NAT-to-public, and NAT-to-NAT paths.
- Cold discovery, hole-punch, and first-byte latency.
- Idle/active RSS, CPU, throughput, and failure rate.
- One, ten, and fifty active connections.

Continue toward an application release only when:

- Idle RSS is at most 30 MiB.
- The compressed Rust core is at most 10 MB.
- File throughput is at least 90% of the Node.js baseline.
- At least 95% of 30 repeated cross-NAT attempts succeed.
- File propagation and channel-history acceptance paths are lossless.

## Phase 8: Android

- Kotlin/Compose thin UI with UniFFI or JNI bindings to `mp-core`.
- Foreground service for active downloads and seeding.
- File picker, links, holdings, channels, and logs.
- Restore holdings and channel heads after process restart.
- Publish an ARM64 app bundle first and measure delivered/installed size.

## Phase 9: iOS and release hardening

- SwiftUI shell with a Rust XCFramework.
- Foreground publishing, downloading, chatting, and seeding.
- Save state before suspension and rejoin/resume on activation.
- Do not promise indefinite iOS background seeding.
- Add protocol fuzzing, crash recovery, quotas, abuse resistance, and static
  Linux builds.

## Current round

The first implementation round is phases 0 through 3. Channel code, transfer
resumption, multi-source scheduling, and mobile UI are not part of this round.
