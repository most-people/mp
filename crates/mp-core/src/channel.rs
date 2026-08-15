use std::fmt;
use std::str::FromStr;

use peeroxide::KeyPair;
use peeroxide_dht::crypto::{sign_detached, verify_detached};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use url::Url;

use crate::{MpError, Result};

/// Version marker for live channel frames and signed text entries.
pub const CHANNEL_PROTOCOL: &str = "mp-channel/1";
/// Maximum encoded live channel frame size.
pub const MAX_CHANNEL_FRAME_SIZE: usize = 16 * 1024;
/// Maximum UTF-8 channel display-name length.
pub const MAX_CHANNEL_NAME_SIZE: usize = 128;
/// Maximum UTF-8 text-message length.
pub const MAX_CHANNEL_TEXT_SIZE: usize = 4 * 1024;

const CHANNEL_ID_DOMAIN: &[u8] = b"mp-channel/1 id\0";
const CHANNEL_TOPIC_DOMAIN: &[u8] = b"mp-channel/1 topic\0";
const MESSAGE_SIGNATURE_DOMAIN: &[u8] = b"mp-channel/1 message signature\0";
const MESSAGE_ID_DOMAIN: &[u8] = b"mp-channel/1 message id\0";

/// Capability-bearing invitation for one private discovery topic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChannelInvite {
    id: [u8; 32],
    capability: [u8; 32],
    name: String,
}

impl ChannelInvite {
    /// Generate a new random channel capability.
    pub fn generate(name: impl Into<String>) -> Result<Self> {
        let mut capability = [0u8; 32];
        rand::rng().fill_bytes(&mut capability);
        Self::from_capability(name, capability)
    }

    /// Construct an invite from fixed capability bytes.
    pub fn from_capability(name: impl Into<String>, capability: [u8; 32]) -> Result<Self> {
        let name = normalize_channel_name(name.into())?;
        let id = channel_id(&capability);
        Ok(Self {
            id,
            capability,
            name,
        })
    }

    /// Stable hexadecimal channel identifier.
    pub fn id(&self) -> String {
        hex::encode(self.id)
    }

    /// Advisory channel display name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Secret capability bytes carried by the invitation.
    pub fn capability(&self) -> &[u8; 32] {
        &self.capability
    }

    /// Private Peeroxide discovery topic derived from the capability.
    pub fn topic(&self) -> [u8; 32] {
        hash_parts(CHANNEL_TOPIC_DOMAIN, &self.capability)
    }
}

impl fmt::Display for ChannelInvite {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut url = Url::parse(&format!("mp-channel://{}", self.id())).map_err(|_| fmt::Error)?;
        url.query_pairs_mut()
            .append_pair("key", &hex::encode(self.capability))
            .append_pair("name", &self.name);
        formatter.write_str(url.as_str())
    }
}

impl FromStr for ChannelInvite {
    type Err = MpError;

    fn from_str(value: &str) -> Result<Self> {
        let url = Url::parse(value)
            .map_err(|error| MpError::InvalidChannelInvite(format!("URL parse failed: {error}")))?;
        if url.scheme() != "mp-channel" {
            return Err(invalid_invite("scheme must be mp-channel"));
        }
        if !url.username().is_empty() || url.password().is_some() || url.port().is_some() {
            return Err(invalid_invite("credentials and ports are not allowed"));
        }
        if !url.path().is_empty() && url.path() != "/" {
            return Err(invalid_invite("path must be empty"));
        }
        if url.fragment().is_some() {
            return Err(invalid_invite("fragment is not allowed"));
        }

        let raw_id = url
            .host_str()
            .ok_or_else(|| invalid_invite("channel id is missing"))?;
        let parsed_id = decode_invite_hex::<32>(raw_id, "channel id")?;
        let mut capability = None;
        let mut name = None;
        for (key, value) in url.query_pairs() {
            match key.as_ref() {
                "key" if capability.is_none() => {
                    capability = Some(decode_invite_hex::<32>(&value, "capability key")?);
                }
                "name" if name.is_none() => name = Some(value.into_owned()),
                "key" | "name" => {
                    return Err(invalid_invite(format!("{key} may appear only once")));
                }
                _ => return Err(invalid_invite(format!("unknown query parameter: {key}"))),
            }
        }

        let capability = capability.ok_or_else(|| invalid_invite("capability key is missing"))?;
        let name = name.ok_or_else(|| invalid_invite("channel name is missing"))?;
        let invite = Self::from_capability(name, capability)?;
        if invite.id != parsed_id {
            return Err(invalid_invite(
                "channel id does not match the capability key",
            ));
        }
        Ok(invite)
    }
}

/// A signed, persistent text entry in one writer's append-only chain.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SignedChannelMessage {
    /// Exact protocol version.
    pub protocol: String,
    /// Hexadecimal channel id.
    pub channel_id: String,
    /// Hexadecimal Ed25519 writer public key.
    pub writer: String,
    /// One-based sequence number within this writer's chain.
    pub sequence: u64,
    /// Previous signed entry id, or `None` for sequence one.
    pub previous: Option<String>,
    /// Sender wall-clock time in Unix milliseconds.
    pub timestamp_ms: u64,
    /// UTF-8 text body.
    pub text: String,
    /// Detached Ed25519 signature in hexadecimal form.
    pub signature: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct UnsignedChannelMessage<'a> {
    protocol: &'a str,
    channel_id: &'a str,
    writer: &'a str,
    sequence: u64,
    previous: &'a Option<String>,
    timestamp_ms: u64,
    text: &'a str,
}

impl SignedChannelMessage {
    /// Create and sign an entry using an explicit writer-chain position.
    pub fn sign(
        channel_id: &str,
        sequence: u64,
        previous: Option<String>,
        timestamp_ms: u64,
        text: impl Into<String>,
        key_pair: &KeyPair,
    ) -> Result<Self> {
        validate_channel_id(channel_id)?;
        validate_chain_position(sequence, previous.as_deref())?;
        let text = text.into();
        validate_text(&text)?;

        let mut message = Self {
            protocol: CHANNEL_PROTOCOL.to_string(),
            channel_id: channel_id.to_string(),
            writer: hex::encode(key_pair.public_key),
            sequence,
            previous,
            timestamp_ms,
            text,
            signature: String::new(),
        };
        let signable = message.signable_bytes()?;
        message.signature = hex::encode(sign_detached(&signable, &key_pair.secret_key));
        Ok(message)
    }

    /// Verify structure and signature, returning the canonical entry id.
    pub fn verify(&self, expected_channel_id: &str) -> Result<String> {
        if self.protocol != CHANNEL_PROTOCOL {
            return Err(channel_error(format!(
                "unsupported protocol: {}",
                self.protocol
            )));
        }
        validate_channel_id(expected_channel_id)?;
        validate_channel_id(&self.channel_id)?;
        if self.channel_id != expected_channel_id {
            return Err(channel_error("message belongs to another channel"));
        }
        validate_chain_position(self.sequence, self.previous.as_deref())?;
        validate_text(&self.text)?;

        let writer = decode_channel_hex::<32>(&self.writer, "writer public key")?;
        let signature = decode_channel_hex::<64>(&self.signature, "message signature")?;
        if !verify_detached(&signature, &self.signable_bytes()?, &writer) {
            return Err(channel_error("message signature verification failed"));
        }
        self.id()
    }

    /// Compute the SHA-256 id of the complete signed entry.
    pub fn id(&self) -> Result<String> {
        let encoded = serde_json::to_vec(self)?;
        Ok(hex::encode(hash_parts(MESSAGE_ID_DOMAIN, &encoded)))
    }

    fn signable_bytes(&self) -> Result<Vec<u8>> {
        let unsigned = UnsignedChannelMessage {
            protocol: &self.protocol,
            channel_id: &self.channel_id,
            writer: &self.writer,
            sequence: self.sequence,
            previous: &self.previous,
            timestamp_ms: self.timestamp_ms,
            text: &self.text,
        };
        let encoded = serde_json::to_vec(&unsigned)?;
        let mut signable = Vec::with_capacity(MESSAGE_SIGNATURE_DOMAIN.len() + encoded.len());
        signable.extend_from_slice(MESSAGE_SIGNATURE_DOMAIN);
        signable.extend_from_slice(&encoded);
        Ok(signable)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ChannelFrame {
    Text {
        message: SignedChannelMessage,
    },
    Typing {
        protocol: String,
        channel_id: String,
        active: bool,
    },
}

/// Ephemeral and persistent events emitted by a running channel node.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ChannelEvent {
    /// An authenticated Peeroxide connection came online or went offline.
    Presence { peer: String, online: bool },
    /// An authenticated peer changed its transient typing state.
    Typing { peer: String, active: bool },
    /// A verified text entry was accepted into persistent history.
    Text {
        message_id: String,
        writer: String,
        sequence: u64,
        timestamp_ms: u64,
        text: String,
    },
}

pub(crate) fn encode_channel_frame(frame: &ChannelFrame) -> Result<Vec<u8>> {
    let encoded = serde_json::to_vec(frame)?;
    if encoded.len() > MAX_CHANNEL_FRAME_SIZE {
        return Err(channel_error(format!(
            "frame is {} bytes; maximum is {MAX_CHANNEL_FRAME_SIZE}",
            encoded.len()
        )));
    }
    Ok(encoded)
}

pub(crate) fn decode_channel_frame(encoded: &[u8]) -> Result<ChannelFrame> {
    if encoded.len() > MAX_CHANNEL_FRAME_SIZE {
        return Err(channel_error(format!(
            "frame is {} bytes; maximum is {MAX_CHANNEL_FRAME_SIZE}",
            encoded.len()
        )));
    }
    Ok(serde_json::from_slice(encoded)?)
}

pub(crate) fn validate_typing_frame(protocol: &str, channel_id: &str) -> Result<()> {
    if protocol != CHANNEL_PROTOCOL {
        return Err(channel_error(format!("unsupported protocol: {protocol}")));
    }
    validate_channel_id(channel_id)
}

fn channel_id(capability: &[u8; 32]) -> [u8; 32] {
    hash_parts(CHANNEL_ID_DOMAIN, capability)
}

fn hash_parts(domain: &[u8], value: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(value);
    hasher.finalize().into()
}

fn normalize_channel_name(name: String) -> Result<String> {
    let name = name.trim();
    if name.is_empty() {
        return Err(invalid_invite("channel name must not be empty"));
    }
    if name.len() > MAX_CHANNEL_NAME_SIZE {
        return Err(invalid_invite(format!(
            "channel name exceeds {MAX_CHANNEL_NAME_SIZE} bytes"
        )));
    }
    if name.chars().any(char::is_control) {
        return Err(invalid_invite(
            "channel name must not contain control characters",
        ));
    }
    Ok(name.to_string())
}

fn validate_channel_id(value: &str) -> Result<()> {
    decode_channel_hex::<32>(value, "channel id").map(|_| ())
}

fn validate_chain_position(sequence: u64, previous: Option<&str>) -> Result<()> {
    if sequence == 0 {
        return Err(channel_error("sequence must start at one"));
    }
    match (sequence, previous) {
        (1, None) => Ok(()),
        (1, Some(_)) => Err(channel_error("sequence one must not have a previous id")),
        (_, None) => Err(channel_error("sequence after one requires a previous id")),
        (_, Some(previous)) => {
            decode_channel_hex::<32>(previous, "previous message id").map(|_| ())
        }
    }
}

fn validate_text(text: &str) -> Result<()> {
    if text.trim().is_empty() {
        return Err(channel_error("text must not be empty"));
    }
    if text.len() > MAX_CHANNEL_TEXT_SIZE {
        return Err(channel_error(format!(
            "text exceeds {MAX_CHANNEL_TEXT_SIZE} bytes"
        )));
    }
    Ok(())
}

fn decode_invite_hex<const N: usize>(value: &str, label: &str) -> Result<[u8; N]> {
    decode_hex(value, label).map_err(invalid_invite)
}

fn decode_channel_hex<const N: usize>(value: &str, label: &str) -> Result<[u8; N]> {
    decode_hex(value, label).map_err(channel_error)
}

fn decode_hex<const N: usize>(value: &str, label: &str) -> std::result::Result<[u8; N], String> {
    let decoded = hex::decode(value).map_err(|_| format!("{label} must be hexadecimal"))?;
    let decoded: [u8; N] = decoded
        .try_into()
        .map_err(|bytes: Vec<u8>| format!("{label} must be {N} bytes, got {}", bytes.len()))?;
    if hex::encode(decoded) != value {
        return Err(format!("{label} must use canonical lowercase hexadecimal"));
    }
    Ok(decoded)
}

fn invalid_invite(message: impl Into<String>) -> MpError {
    MpError::InvalidChannelInvite(message.into())
}

fn channel_error(message: impl Into<String>) -> MpError {
    MpError::Channel(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    const CHANNEL_ID: &str = "3b06a8ab3b9f07253d1a77371bb2cfba93b0ff45c96f9721b851cbf2f47b3be5";

    #[test]
    fn invite_has_stable_id_topic_and_encoding() {
        let capability = std::array::from_fn(|index| index as u8);
        let invite = ChannelInvite::from_capability("测试 room", capability).unwrap();
        assert_eq!(invite.id(), CHANNEL_ID);
        assert_eq!(
            hex::encode(invite.topic()),
            "6c174e8a64c813d6f52c1b51203df6e7fd57d8915f788d7a903e671af73bb01f"
        );
        assert_eq!(
            invite.to_string(),
            format!(
                "mp-channel://{CHANNEL_ID}?key=000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f&name=%E6%B5%8B%E8%AF%95+room"
            )
        );
        assert_eq!(invite.to_string().parse::<ChannelInvite>().unwrap(), invite);
    }

    #[test]
    fn invite_rejects_capability_mismatch_and_unknown_query() {
        let capability = "00".repeat(32);
        let bad_id = "11".repeat(32);
        assert!(
            format!("mp-channel://{bad_id}?key={capability}&name=test")
                .parse::<ChannelInvite>()
                .is_err()
        );
        let invite = ChannelInvite::from_capability("test", [0u8; 32]).unwrap();
        assert!(
            format!("{}&tracker=x", invite)
                .parse::<ChannelInvite>()
                .is_err()
        );
    }

    #[test]
    fn signed_message_has_stable_signature_and_id() {
        let key_pair = KeyPair::from_seed([7u8; 32]);
        let message = SignedChannelMessage::sign(
            CHANNEL_ID,
            1,
            None,
            1_700_000_000_123,
            "hello mp",
            &key_pair,
        )
        .unwrap();
        assert_eq!(
            message.signature,
            "5eaf8b96d2dabaa5741112156c31e3ead9a6e6389ff196fd18ef4ee674c2cbeeef023b40a38593794850bafaab4eae2f4069187837300b542475aa7fde138f0b"
        );
        assert_eq!(
            message.verify(CHANNEL_ID).unwrap(),
            "eca43a1675cac787848664b2907ce058092333c2c4d3e45a82bdc90e0db0b79c"
        );
    }

    #[test]
    fn signature_rejects_tampering() {
        let key_pair = KeyPair::from_seed([9u8; 32]);
        let mut message =
            SignedChannelMessage::sign(CHANNEL_ID, 1, None, 42, "before", &key_pair).unwrap();
        message.text = "after".to_string();
        assert!(message.verify(CHANNEL_ID).is_err());
    }

    #[test]
    fn typing_frames_round_trip_without_persistent_fields() {
        let frame = ChannelFrame::Typing {
            protocol: CHANNEL_PROTOCOL.to_string(),
            channel_id: CHANNEL_ID.to_string(),
            active: true,
        };
        let encoded = encode_channel_frame(&frame).unwrap();
        let decoded = decode_channel_frame(&encoded).unwrap();
        assert!(matches!(decoded, ChannelFrame::Typing { active: true, .. }));
        assert!(!String::from_utf8(encoded).unwrap().contains("timestamp"));
    }
}
