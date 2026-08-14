use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

use cid::Cid;
use cid::multihash::Multihash;
use sha2::{Digest, Sha256};

use crate::{MpError, Result};

/// Multicodec code for raw binary data.
pub const RAW_CODEC: u64 = 0x55;
/// Multihash code for SHA2-256.
pub const SHA2_256_CODE: u64 = 0x12;
/// First-round maximum accepted file size: 10 GiB.
pub const MAX_FILE_SIZE: u64 = 10 * 1024 * 1024 * 1024;

/// Build the canonical v1 file CID from a SHA-256 digest.
pub fn cid_from_sha256(digest: [u8; 32]) -> Result<Cid> {
    let multihash = Multihash::<64>::wrap(SHA2_256_CODE, &digest)
        .map_err(|error| MpError::InvalidCid(error.to_string()))?;
    Ok(Cid::new_v1(RAW_CODEC, multihash))
}

/// Calculate a canonical v1 file CID from bytes.
pub fn calculate_bytes_cid(bytes: &[u8]) -> Result<Cid> {
    let digest: [u8; 32] = Sha256::digest(bytes).into();
    cid_from_sha256(digest)
}

/// Calculate a canonical v1 file CID and byte count from a local file.
pub fn calculate_file_cid(path: impl AsRef<Path>) -> Result<(Cid, u64)> {
    let path = path.as_ref();
    let metadata = std::fs::metadata(path)?;
    if !metadata.is_file() {
        return Err(MpError::InvalidState {
            path: path.to_path_buf(),
            message: "expected a regular file".to_string(),
        });
    }
    if metadata.len() > MAX_FILE_SIZE {
        return Err(MpError::FileTooLarge {
            size: metadata.len(),
            max: MAX_FILE_SIZE,
        });
    }

    let mut reader = BufReader::new(File::open(path)?);
    let mut hasher = Sha256::new();
    let mut total = 0u64;
    let mut buffer = [0u8; 64 * 1024];

    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        total += read as u64;
        hasher.update(&buffer[..read]);
    }

    let digest: [u8; 32] = hasher.finalize().into();
    Ok((cid_from_sha256(digest)?, total))
}

/// Parse and validate the canonical v1 raw SHA-256 CID profile.
pub fn parse_file_cid(value: &str) -> Result<Cid> {
    let cid: Cid = value
        .parse()
        .map_err(|error: cid::Error| MpError::InvalidCid(error.to_string()))?;

    if cid.version() != cid::Version::V1 {
        return Err(MpError::InvalidCid("CID version must be 1".to_string()));
    }
    if cid.codec() != RAW_CODEC {
        return Err(MpError::InvalidCid(format!(
            "codec must be raw ({RAW_CODEC:#x})"
        )));
    }
    if cid.hash().code() != SHA2_256_CODE || cid.hash().digest().len() != 32 {
        return Err(MpError::InvalidCid(
            "multihash must be SHA2-256".to_string(),
        ));
    }

    Ok(cid)
}

/// Return the exact 32-byte Peeroxide topic for a canonical file CID.
pub fn topic_from_cid(cid: &Cid) -> Result<[u8; 32]> {
    let canonical = parse_file_cid(&cid.to_string())?;
    let mut topic = [0u8; 32];
    topic.copy_from_slice(canonical.hash().digest());
    Ok(topic)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_file_matches_raw_cid_golden_value() {
        let cid = calculate_bytes_cid(b"").unwrap();
        assert_eq!(
            cid.to_string(),
            "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku"
        );
    }

    #[test]
    fn topic_is_the_unmodified_multihash_digest() {
        let cid = calculate_bytes_cid(b"mp topic").unwrap();
        assert_eq!(
            topic_from_cid(&cid).unwrap().as_slice(),
            cid.hash().digest()
        );
    }

    #[test]
    fn rejects_non_raw_cid() {
        let digest: [u8; 32] = Sha256::digest(b"dag").into();
        let multihash = Multihash::<64>::wrap(SHA2_256_CODE, &digest).unwrap();
        let cid = Cid::new_v1(0x70, multihash);
        assert!(parse_file_cid(&cid.to_string()).is_err());
    }
}
