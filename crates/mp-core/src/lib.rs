//! Core protocol, storage, and networking primitives for mp.

mod channel;
mod channel_node;
mod channel_store;
mod content_id;
mod error;
mod identity;
mod link;
mod node;
mod protocol;
mod storage;
mod transfer;

pub use channel::{
    CHANNEL_PROTOCOL, ChannelEvent, ChannelInvite, MAX_CHANNEL_FRAME_SIZE, MAX_CHANNEL_NAME_SIZE,
    MAX_CHANNEL_TEXT_SIZE, SignedChannelMessage,
};
pub use channel_node::{ChannelNode, ChannelStatus};
pub use channel_store::{ChannelStore, ChannelSummary};
pub use content_id::{
    MAX_FILE_SIZE, RAW_CODEC, SHA2_256_CODE, calculate_bytes_cid, calculate_file_cid,
    cid_from_sha256, parse_file_cid, topic_from_cid,
};
pub use error::{MpError, Result};
pub use identity::NodeIdentity;
pub use link::ShareLink;
pub use node::{DownloadResult, Node, NodeOptions, NodeStatus, RelayConfig};
pub use protocol::{
    CONTROL_FRAME_TAG, Control, DATA_FRAME_TAG, FILE_PROTOCOL, Frame, MAX_CONTROL_FRAME_SIZE,
    MAX_DATA_FRAME_SIZE, decode_frame, encode_control, encode_data,
};
pub use storage::{Holding, HoldingValidation, ImportResult, ObjectStore, ValidationReport};
pub use transfer::{MessageConnection, receive_file, serve_file};
