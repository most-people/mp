# Phase 4 live-channel acceptance

Status: accepted on 2026-08-15 between `x.most.red` and
`192.168.31.52`.

## Scope

This phase accepts capability discovery, authenticated live text, one signed
hash chain per writer, duplicate suppression, and ephemeral presence/typing.
It does not accept history synchronization, offline sending, or attachments.
The current CLI runs one live channel per process; unifying file and multiple
channel topics in one daemon is required before the application phase.

## Automated gate

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

The protocol tests must include stable invite id/topic bytes, stable Ed25519
signature and message-id bytes, tamper rejection, duplicate suppression,
independent writer chains, restart validation, and corrupt-state rejection.

## Two-device gate

Build the same release binary on device A and B. Use fresh data directories so
the test identities and channel state are isolated.

On A:

```bash
mp --data-dir /tmp/mp-channel-a channel create "phase-4"
mp --data-dir /tmp/mp-channel-a channel open '<channel-id>'
```

Copy the complete printed invite to B:

```bash
mp --data-dir /tmp/mp-channel-b channel join 'mp-channel://...'
```

Acceptance sequence:

1. Both sessions emit an online `presence` event containing the authenticated
   remote public key.
2. A sends `from A`; both sessions emit the same text message id, writer, and
   sequence one.
3. B sends `from B`; both sessions accept an independent writer sequence one.
4. A sends `/typing on` and `/typing off`; only B emits the two typing events.
5. `channel list` on both devices reports two persistent messages. Typing and
   presence do not change the count.
6. Restart both sessions and confirm `channel list` retains the count and no
   historical text is emitted automatically.
7. Alter one hex character in the invite key and confirm `channel join` rejects
   the channel-id mismatch before network startup.

The run fails if an invalid signature or broken writer head is persisted, an
exact duplicate increments the message count, or either process exits because
of an untrusted connection-local frame.

## Accepted run

The final tested Linux x86_64 release was 2,810,168 bytes with SHA-256
`39e0998b64df8c45f5d042907158da1229e3ce13d024f55236b0bd044737ba51`.
Both uploaded copies matched before execution.

Observed channel state:

- Channel id:
  `0f6d231df59a450ea87eba26d7ecf5890ec895824fb6eec77345d453a47328f1`
- Public-server writer:
  `6d1cbfa884b82c6875a48ed40c41c4e5237858fae5b38ae679bec5b71b9beffb`
- NAT-device writer:
  `c51d843dff9819ea63abf945e797bf9ac0d27c98258c70f59c1c0d85bddf8273`
- Both directions produced identical message ids at sender and receiver.
- Presence arrived on both peers; typing on/off arrived only at the remote
  peer.
- Restart emitted presence but no historical text. Both writers then extended
  their persisted heads from sequence one through sequence three.
- Both devices finished with six text entries and a `0600` `channels.json`.
- Routine `channel open` output omitted the capability; only `channel create`
  and the explicit `channel invite` command reveal it.
- A one-character capability mutation was rejected with a channel-id mismatch
  before Peeroxide startup.

The invite capability is intentionally omitted from this report because it is
the channel access secret.
