# mp protocol v1

Status: experimental and allowed to break before the first tagged release.

## Dependency boundary

`mp` uses `peeroxide` 1.7.3 for HyperDHT discovery, Noise authentication,
hole-punching, UDX transport, and SecretStream encryption. `mp` owns all file,
storage, and future channel semantics above the encrypted stream.

An operator may explicitly force server connections through a Peeroxide blind
relay when the direct UDX path is unusable. The CLI must report this mode; the
relay cannot read SecretStream plaintext and does not change `mp-file/1`.

## Content identity

A v1 file CID is:

```text
CIDv1(codec=raw, multihash=sha2-256(file bytes))
```

Consequences:

- Only regular single files are supported.
- File names and paths do not affect identity.
- Directories and IPFS UnixFS compatibility are intentionally absent.
- The 32-byte SHA-256 multihash digest is the Peeroxide file topic. It is not
  hashed again.

## Share link

Canonical form:

```text
mp://<cid>?filename=<percent-encoded-display-name>
```

Rules:

- `<cid>` is a lowercase base32 CIDv1 string.
- `filename` is optional and advisory.
- A receiver never uses `filename` to decide whether content exists or is
  valid.
- Unknown query parameters are rejected during the experimental v1 phase.

## Persistent identity

Each data directory contains one random 32-byte seed. Peeroxide derives its
Ed25519/Noise identity from that seed. The seed is created once, written with
owner-only permissions where supported, and reused after restart.

The identity authenticates a connection. It does not grant access to a file;
knowing a CID and reaching an online seed is sufficient to request that file.

## Object store

Verified objects are stored under:

```text
objects/<cid>
```

`holdings.json` stores display metadata only. A holding is usable only when the
object can be read and recomputes to the recorded CID. Invalid records are not
announced.

Imports and downloads use a temporary file in the object directory. The final
object path is installed with an atomic rename only after verification.

## `mp-file/1` connection flow

Downloader:

1. Parse the link and extract the 32-byte CID digest topic.
2. Join the topic in client-only mode.
3. Connect to a discovered seed.
4. Send a `request` control frame.

Seeder:

1. Read and validate the request.
2. Confirm a readable, verified local object exists.
3. Send an `offer` control frame.
4. Send ordered data frames.
5. Send a `complete` control frame and close its write side.

Downloader:

1. Enforce the advertised and configured size limits.
2. Write data frames to a temporary file while hashing.
3. Require the exact advertised byte count and a `complete` frame.
4. Require the recomputed CID to equal the requested CID.
5. Promote the object, record the holding, leave client mode, and join the same
   topic in server-only mode.

## Frame envelope

Peeroxide SecretStream already preserves message boundaries. Every plaintext
message begins with one byte:

| Byte | Meaning | Limit |
| ---- | ------- | ----- |
| `0x01` | UTF-8 JSON control message | 64 KiB including tag |
| `0x02` | file data | 64 KiB payload |

Control messages use a tagged JSON object. Required shapes:

```json
{"type":"request","protocol":"mp-file/1","cid":"<cid>"}
{"type":"offer","protocol":"mp-file/1","cid":"<cid>","filename":"name","size":123}
{"type":"complete","protocol":"mp-file/1","cid":"<cid>","size":123}
{"type":"error","protocol":"mp-file/1","code":"not_found","message":"..."}
```

The maximum accepted file size in the first round is 10 GiB.

## Errors

Initial stable codes:

- `bad_request`: malformed frame, CID, or protocol version.
- `not_found`: the peer does not have a verified object.
- `too_large`: the advertised object exceeds the receiver limit.
- `size_mismatch`: received bytes do not match the offer/complete frame.
- `cid_mismatch`: final content identity differs from the request.
- `io`: local storage failed.

Errors from untrusted peers are connection-local and must not stop the node.

## Reserved channel boundary

The next implementation round will use `mp-channel/1` on the same Peeroxide
identity and transport. Channel discovery topics are distinct from file CID
topics. Each writer will own a signed append-only hash chain; live events and
history synchronization will be separate message kinds. File attachments will
contain a CID reference and use `mp-file/1` for bytes.
