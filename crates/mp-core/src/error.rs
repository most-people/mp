use std::path::PathBuf;

use thiserror::Error;

/// Result type used by mp-core.
pub type Result<T> = std::result::Result<T, MpError>;

/// Errors produced by the mp core.
#[derive(Debug, Error)]
pub enum MpError {
    /// A local I/O operation failed.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON metadata or a control frame could not be decoded.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// A CID is malformed or does not use the v1 raw SHA-256 profile.
    #[error("invalid file CID: {0}")]
    InvalidCid(String),

    /// A share link is malformed or violates the v1 canonical form.
    #[error("invalid share link: {0}")]
    InvalidLink(String),

    /// A channel capability invite is malformed or inconsistent.
    #[error("invalid channel invite: {0}")]
    InvalidChannelInvite(String),

    /// A channel message, writer chain, or live event is invalid.
    #[error("channel error: {0}")]
    Channel(String),

    /// A wire frame violates the mp-file/1 protocol.
    #[error("protocol error: {0}")]
    Protocol(String),

    /// A file exceeds the configured first-round limit.
    #[error("file is too large: {size} bytes exceeds {max} bytes")]
    FileTooLarge { size: u64, max: u64 },

    /// A requested object is not held locally.
    #[error("object is not held locally: {0}")]
    NotFound(String),

    /// A file failed its expected size or CID verification.
    #[error("integrity check failed: expected {expected}, got {actual}")]
    Integrity { expected: String, actual: String },

    /// Persistent state is internally inconsistent.
    #[error("invalid state at {}: {message}", path.display())]
    InvalidState { path: PathBuf, message: String },

    /// Peer discovery or an encrypted connection failed.
    #[error("network error: {0}")]
    Network(String),

    /// An operation exceeded its deadline.
    #[error("operation timed out: {0}")]
    Timeout(String),
}
