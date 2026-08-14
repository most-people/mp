use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use peeroxide::KeyPair;
use rand::RngCore;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

use crate::{MpError, Result};

const IDENTITY_FILE: &str = "identity.seed";

/// Persistent node identity seed and derived Peeroxide key pair.
#[derive(Clone)]
pub struct NodeIdentity {
    seed: [u8; 32],
    path: PathBuf,
}

impl NodeIdentity {
    /// Load an existing identity or create it atomically.
    pub fn load_or_create(data_dir: impl AsRef<Path>) -> Result<Self> {
        let data_dir = data_dir.as_ref();
        fs::create_dir_all(data_dir)?;
        let path = data_dir.join(IDENTITY_FILE);
        if path.exists() {
            return Self::load(path);
        }

        let mut seed = [0u8; 32];
        rand::rng().fill_bytes(&mut seed);
        let temp = data_dir.join(format!(
            ".{IDENTITY_FILE}.{}.{:016x}.tmp",
            std::process::id(),
            rand::random::<u64>()
        ));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);

        let mut file = options.open(&temp)?;
        file.write_all(&seed)?;
        file.sync_all()?;
        fs::rename(&temp, &path)?;

        Ok(Self { seed, path })
    }

    fn load(path: PathBuf) -> Result<Self> {
        let mut bytes = Vec::new();
        File::open(&path)?.read_to_end(&mut bytes)?;
        let seed: [u8; 32] = bytes
            .try_into()
            .map_err(|bytes: Vec<u8>| MpError::InvalidState {
                path: path.clone(),
                message: format!("identity must be exactly 32 bytes, got {}", bytes.len()),
            })?;
        Ok(Self { seed, path })
    }

    /// Return the derived Peeroxide Ed25519 key pair.
    pub fn key_pair(&self) -> KeyPair {
        KeyPair::from_seed(self.seed)
    }

    /// Return the identity file path.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_survives_restart() {
        let temp = tempfile::tempdir().unwrap();
        let first = NodeIdentity::load_or_create(temp.path()).unwrap();
        let second = NodeIdentity::load_or_create(temp.path()).unwrap();
        assert_eq!(first.key_pair().public_key, second.key_pair().public_key);
        assert_eq!(fs::read(first.path()).unwrap().len(), 32);
    }

    #[test]
    fn rejects_truncated_identity() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join(IDENTITY_FILE), [1u8; 4]).unwrap();
        assert!(NodeIdentity::load_or_create(temp.path()).is_err());
    }
}
