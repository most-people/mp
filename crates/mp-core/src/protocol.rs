use serde::{Deserialize, Serialize};

use crate::{MpError, Result};

/// Stable protocol identifier carried by every control message.
pub const FILE_PROTOCOL: &str = "mp-file/1";
/// Tag for a JSON control frame.
pub const CONTROL_FRAME_TAG: u8 = 0x01;
/// Tag for a raw file-data frame.
pub const DATA_FRAME_TAG: u8 = 0x02;
/// Maximum control-frame size including its tag.
pub const MAX_CONTROL_FRAME_SIZE: usize = 64 * 1024;
/// Maximum raw payload in one data frame.
pub const MAX_DATA_FRAME_SIZE: usize = 64 * 1024;

/// Versioned control messages for `mp-file/1`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Control {
    /// Request one CID from a seed.
    Request { protocol: String, cid: String },
    /// Describe the object that will follow.
    Offer {
        protocol: String,
        cid: String,
        filename: String,
        size: u64,
    },
    /// Mark the exact end of a successful transfer.
    Complete {
        protocol: String,
        cid: String,
        size: u64,
    },
    /// Reject a request without stopping the node.
    Error {
        protocol: String,
        code: String,
        message: String,
    },
}

impl Control {
    /// Construct a file request.
    pub fn request(cid: impl Into<String>) -> Self {
        Self::Request {
            protocol: FILE_PROTOCOL.to_string(),
            cid: cid.into(),
        }
    }

    /// Construct an object offer.
    pub fn offer(cid: impl Into<String>, filename: impl Into<String>, size: u64) -> Self {
        Self::Offer {
            protocol: FILE_PROTOCOL.to_string(),
            cid: cid.into(),
            filename: filename.into(),
            size,
        }
    }

    /// Construct a transfer-complete message.
    pub fn complete(cid: impl Into<String>, size: u64) -> Self {
        Self::Complete {
            protocol: FILE_PROTOCOL.to_string(),
            cid: cid.into(),
            size,
        }
    }

    /// Construct a protocol error response.
    pub fn error(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Error {
            protocol: FILE_PROTOCOL.to_string(),
            code: code.into(),
            message: message.into(),
        }
    }

    /// Return the carried protocol identifier.
    pub fn protocol(&self) -> &str {
        match self {
            Self::Request { protocol, .. }
            | Self::Offer { protocol, .. }
            | Self::Complete { protocol, .. }
            | Self::Error { protocol, .. } => protocol,
        }
    }
}

/// Decoded plaintext frame.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Frame {
    /// A versioned control message.
    Control(Control),
    /// Ordered file bytes.
    Data(Vec<u8>),
}

/// Encode a control message into a tagged SecretStream plaintext frame.
pub fn encode_control(control: &Control) -> Result<Vec<u8>> {
    if control.protocol() != FILE_PROTOCOL {
        return Err(MpError::Protocol(format!(
            "unsupported protocol: {}",
            control.protocol()
        )));
    }
    let json = serde_json::to_vec(control)?;
    if json.len() + 1 > MAX_CONTROL_FRAME_SIZE {
        return Err(MpError::Protocol("control frame is too large".to_string()));
    }
    let mut frame = Vec::with_capacity(json.len() + 1);
    frame.push(CONTROL_FRAME_TAG);
    frame.extend_from_slice(&json);
    Ok(frame)
}

/// Encode ordered file bytes into a tagged frame.
pub fn encode_data(data: &[u8]) -> Result<Vec<u8>> {
    if data.is_empty() {
        return Err(MpError::Protocol(
            "data frame must not be empty".to_string(),
        ));
    }
    if data.len() > MAX_DATA_FRAME_SIZE {
        return Err(MpError::Protocol("data frame is too large".to_string()));
    }
    let mut frame = Vec::with_capacity(data.len() + 1);
    frame.push(DATA_FRAME_TAG);
    frame.extend_from_slice(data);
    Ok(frame)
}

/// Decode and validate one tagged plaintext frame.
pub fn decode_frame(frame: &[u8]) -> Result<Frame> {
    let (&tag, payload) = frame
        .split_first()
        .ok_or_else(|| MpError::Protocol("empty frame".to_string()))?;
    match tag {
        CONTROL_FRAME_TAG => {
            if frame.len() > MAX_CONTROL_FRAME_SIZE {
                return Err(MpError::Protocol("control frame is too large".to_string()));
            }
            let control: Control = serde_json::from_slice(payload)?;
            if control.protocol() != FILE_PROTOCOL {
                return Err(MpError::Protocol(format!(
                    "unsupported protocol: {}",
                    control.protocol()
                )));
            }
            Ok(Frame::Control(control))
        }
        DATA_FRAME_TAG => {
            if payload.is_empty() {
                return Err(MpError::Protocol(
                    "data frame must not be empty".to_string(),
                ));
            }
            if payload.len() > MAX_DATA_FRAME_SIZE {
                return Err(MpError::Protocol("data frame is too large".to_string()));
            }
            Ok(Frame::Data(payload.to_vec()))
        }
        _ => Err(MpError::Protocol(format!("unknown frame tag: {tag:#x}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_has_stable_golden_encoding() {
        let encoded = encode_control(&Control::request("bafk-test")).unwrap();
        assert_eq!(
            &encoded[1..],
            br#"{"type":"request","protocol":"mp-file/1","cid":"bafk-test"}"#
        );
        assert_eq!(
            decode_frame(&encoded).unwrap(),
            Frame::Control(Control::request("bafk-test"))
        );
    }

    #[test]
    fn rejects_unknown_fields() {
        let frame =
            b"\x01{\"type\":\"request\",\"protocol\":\"mp-file/1\",\"cid\":\"x\",\"extra\":1}";
        assert!(decode_frame(frame).is_err());
    }

    #[test]
    fn rejects_unknown_protocol_version() {
        let frame = b"\x01{\"type\":\"request\",\"protocol\":\"mp-file/2\",\"cid\":\"x\"}";
        assert!(decode_frame(frame).is_err());
    }

    #[test]
    fn data_round_trip_preserves_bytes() {
        let encoded = encode_data(&[0, 1, 2, 255]).unwrap();
        assert_eq!(
            decode_frame(&encoded).unwrap(),
            Frame::Data(vec![0, 1, 2, 255])
        );
    }
}
