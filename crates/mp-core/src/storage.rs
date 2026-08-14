use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use cid::Cid;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    MAX_FILE_SIZE, MpError, Result, ShareLink, calculate_file_cid, cid_from_sha256, parse_file_cid,
};

const HOLDINGS_FILE: &str = "holdings.json";
const OBJECTS_DIR: &str = "objects";
const COPY_BUFFER_SIZE: usize = 64 * 1024;

/// One verified complete object held by this node.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Holding {
    /// Canonical CID string.
    pub cid: String,
    /// Advisory display filename.
    pub filename: String,
    /// Complete object length.
    pub size: u64,
}

/// Result of importing a local file.
#[derive(Clone, Debug)]
pub struct ImportResult {
    /// Persisted holding.
    pub holding: Holding,
    /// Canonical share link.
    pub link: ShareLink,
    /// Local content-addressed object path.
    pub object_path: PathBuf,
}

/// Validation outcome for one persisted holding.
#[derive(Clone, Debug)]
pub struct HoldingValidation {
    /// Persisted record.
    pub holding: Holding,
    /// Failure reason, or `None` when usable.
    pub error: Option<String>,
}

/// Complete validation report used when starting a node.
#[derive(Clone, Debug, Default)]
pub struct ValidationReport {
    /// Holdings whose object bytes match their CID.
    pub valid: Vec<Holding>,
    /// Records that must not be announced.
    pub invalid: Vec<HoldingValidation>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct HoldingsState {
    version: u32,
    holdings: Vec<Holding>,
}

impl Default for HoldingsState {
    fn default() -> Self {
        Self {
            version: 1,
            holdings: Vec::new(),
        }
    }
}

struct StoreInner {
    data_dir: PathBuf,
    objects_dir: PathBuf,
    holdings_path: PathBuf,
    state: Mutex<HoldingsState>,
}

/// Content-addressed object store plus atomic holding metadata.
#[derive(Clone)]
pub struct ObjectStore {
    inner: Arc<StoreInner>,
}

impl ObjectStore {
    /// Open or create a data directory without modifying corrupt metadata.
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        let objects_dir = data_dir.join(OBJECTS_DIR);
        let holdings_path = data_dir.join(HOLDINGS_FILE);
        fs::create_dir_all(&objects_dir)?;

        let state = if holdings_path.exists() {
            let bytes = fs::read(&holdings_path)?;
            let state: HoldingsState =
                serde_json::from_slice(&bytes).map_err(|error| MpError::InvalidState {
                    path: holdings_path.clone(),
                    message: error.to_string(),
                })?;
            if state.version != 1 {
                return Err(MpError::InvalidState {
                    path: holdings_path.clone(),
                    message: format!("unsupported holdings version: {}", state.version),
                });
            }
            state
        } else {
            HoldingsState::default()
        };

        Ok(Self {
            inner: Arc::new(StoreInner {
                data_dir,
                objects_dir,
                holdings_path,
                state: Mutex::new(state),
            }),
        })
    }

    /// Return the root data directory.
    pub fn data_dir(&self) -> &Path {
        &self.inner.data_dir
    }

    /// Return the canonical path for a CID.
    pub fn object_path(&self, cid: &Cid) -> PathBuf {
        self.inner.objects_dir.join(cid.to_string())
    }

    /// Allocate a unique temporary object path in the atomic-rename directory.
    pub fn temporary_object_path(&self) -> PathBuf {
        self.inner.objects_dir.join(format!(
            ".mp-object-{}-{:016x}.tmp",
            std::process::id(),
            rand::random::<u64>()
        ))
    }

    /// Import, verify, and persist a complete local file.
    pub fn import_file(&self, source: impl AsRef<Path>) -> Result<ImportResult> {
        let source = source.as_ref();
        let metadata = fs::metadata(source)?;
        if !metadata.is_file() {
            return Err(MpError::InvalidState {
                path: source.to_path_buf(),
                message: "expected a regular file".to_string(),
            });
        }
        if metadata.len() > MAX_FILE_SIZE {
            return Err(MpError::FileTooLarge {
                size: metadata.len(),
                max: MAX_FILE_SIZE,
            });
        }

        let filename = sanitize_filename(
            source
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or("file"),
        );
        let temp_path = self.temporary_object_path();
        let mut temp_guard = TempGuard::new(temp_path.clone());
        let mut input = BufReader::new(File::open(source)?);
        let output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)?;
        let mut output = BufWriter::new(output);
        let mut hasher = Sha256::new();
        let mut total = 0u64;
        let mut buffer = vec![0u8; COPY_BUFFER_SIZE];

        loop {
            let read = input.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            total += read as u64;
            hasher.update(&buffer[..read]);
            output.write_all(&buffer[..read])?;
        }
        output.flush()?;
        output.get_ref().sync_all()?;

        let digest: [u8; 32] = hasher.finalize().into();
        let cid = cid_from_sha256(digest)?;
        let holding = self.install_verified_temp(&temp_path, &cid, &filename, total)?;
        temp_guard.disarm();
        let link = ShareLink::new(cid, Some(filename))?;
        let object_path = self.object_path(link.cid());
        Ok(ImportResult {
            holding,
            link,
            object_path,
        })
    }

    /// Verify a completed temporary file, install it, and persist its holding.
    pub fn commit_download(
        &self,
        temp_path: impl AsRef<Path>,
        expected_cid: &Cid,
        filename: &str,
        expected_size: u64,
    ) -> Result<Holding> {
        let temp_path = temp_path.as_ref();
        let (actual_cid, actual_size) = calculate_file_cid(temp_path)?;
        if actual_size != expected_size {
            return Err(MpError::Integrity {
                expected: format!("{expected_size} bytes"),
                actual: format!("{actual_size} bytes"),
            });
        }
        if &actual_cid != expected_cid {
            return Err(MpError::Integrity {
                expected: expected_cid.to_string(),
                actual: actual_cid.to_string(),
            });
        }
        self.install_verified_temp(
            temp_path,
            expected_cid,
            &sanitize_filename(filename),
            expected_size,
        )
    }

    /// Return persisted records without claiming their object bytes are valid.
    pub fn holdings(&self) -> Result<Vec<Holding>> {
        Ok(self.lock_state()?.holdings.clone())
    }

    /// Recompute every holding and separate announceable objects from failures.
    pub fn validate_holdings(&self) -> Result<ValidationReport> {
        let holdings = self.holdings()?;
        let mut report = ValidationReport::default();
        for holding in holdings {
            let validation = self.validate_holding(&holding);
            match validation.error {
                None => report.valid.push(holding),
                Some(_) => report.invalid.push(validation),
            }
        }
        Ok(report)
    }

    /// Return a holding only when its complete local object still matches.
    pub fn find_verified(&self, cid: &Cid) -> Result<Holding> {
        let record = self
            .holdings()?
            .into_iter()
            .find(|holding| holding.cid == cid.to_string())
            .ok_or_else(|| MpError::NotFound(cid.to_string()))?;
        let validation = self.validate_holding(&record);
        if let Some(error) = validation.error {
            return Err(MpError::InvalidState {
                path: self.object_path(cid),
                message: error,
            });
        }
        Ok(record)
    }

    fn validate_holding(&self, holding: &Holding) -> HoldingValidation {
        let result = (|| -> Result<()> {
            let cid = parse_file_cid(&holding.cid)?;
            let path = self.object_path(&cid);
            let (actual_cid, size) = calculate_file_cid(&path)?;
            if size != holding.size {
                return Err(MpError::Integrity {
                    expected: format!("{} bytes", holding.size),
                    actual: format!("{size} bytes"),
                });
            }
            if actual_cid != cid {
                return Err(MpError::Integrity {
                    expected: cid.to_string(),
                    actual: actual_cid.to_string(),
                });
            }
            Ok(())
        })();
        HoldingValidation {
            holding: holding.clone(),
            error: result.err().map(|error| error.to_string()),
        }
    }

    fn install_verified_temp(
        &self,
        temp_path: &Path,
        cid: &Cid,
        filename: &str,
        size: u64,
    ) -> Result<Holding> {
        let object_path = self.object_path(cid);
        if object_path.exists() {
            let (existing_cid, existing_size) = calculate_file_cid(&object_path)?;
            if existing_cid != *cid || existing_size != size {
                return Err(MpError::InvalidState {
                    path: object_path,
                    message: "existing object does not match its CID".to_string(),
                });
            }
            if temp_path != object_path {
                let _ = fs::remove_file(temp_path);
            }
        } else {
            fs::rename(temp_path, &object_path)?;
        }

        let holding = Holding {
            cid: cid.to_string(),
            filename: sanitize_filename(filename),
            size,
        };
        let mut state = self.lock_state()?;
        if let Some(existing) = state
            .holdings
            .iter_mut()
            .find(|existing| existing.cid == holding.cid)
        {
            *existing = holding.clone();
        } else {
            state.holdings.push(holding.clone());
        }
        state
            .holdings
            .sort_by(|left, right| left.cid.cmp(&right.cid));
        self.persist_state(&state)?;
        Ok(holding)
    }

    fn persist_state(&self, state: &HoldingsState) -> Result<()> {
        let temp_path = self.inner.data_dir.join(format!(
            ".{HOLDINGS_FILE}.{}.{:016x}.tmp",
            std::process::id(),
            rand::random::<u64>()
        ));
        let mut guard = TempGuard::new(temp_path.clone());
        let bytes = serde_json::to_vec_pretty(state)?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)?;
        file.write_all(&bytes)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        fs::rename(&temp_path, &self.inner.holdings_path)?;
        guard.disarm();
        Ok(())
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, HoldingsState>> {
        self.inner.state.lock().map_err(|_| MpError::InvalidState {
            path: self.inner.holdings_path.clone(),
            message: "holdings lock is poisoned".to_string(),
        })
    }
}

struct TempGuard {
    path: PathBuf,
    armed: bool,
}

impl TempGuard {
    fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for TempGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = fs::remove_file(&self.path);
        }
    }
}

pub(crate) fn sanitize_filename(filename: &str) -> String {
    let leaf = Path::new(filename)
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("download");
    let mut sanitized: String = leaf
        .chars()
        .filter(|character| !character.is_control())
        .map(|character| match character {
            '/' | '\\' | ':' => '_',
            other => other,
        })
        .take(200)
        .collect();
    sanitized = sanitized.trim().to_string();
    if sanitized.is_empty() || sanitized == "." || sanitized == ".." {
        "download".to_string()
    } else {
        sanitized
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn import_persists_a_verified_holding() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("hello.txt");
        fs::write(&source, b"hello mp").unwrap();
        let store = ObjectStore::open(temp.path().join("node")).unwrap();

        let imported = store.import_file(&source).unwrap();
        assert_eq!(imported.holding.size, 8);
        assert_eq!(fs::read(&imported.object_path).unwrap(), b"hello mp");

        let reopened = ObjectStore::open(temp.path().join("node")).unwrap();
        let report = reopened.validate_holdings().unwrap();
        assert_eq!(report.valid, vec![imported.holding]);
        assert!(report.invalid.is_empty());
    }

    #[test]
    fn corrupt_object_is_not_validated() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("hello.txt");
        fs::write(&source, b"hello mp").unwrap();
        let store = ObjectStore::open(temp.path().join("node")).unwrap();
        let imported = store.import_file(&source).unwrap();
        fs::write(&imported.object_path, b"corrupt").unwrap();

        let report = store.validate_holdings().unwrap();
        assert!(report.valid.is_empty());
        assert_eq!(report.invalid.len(), 1);
    }

    #[test]
    fn duplicate_import_keeps_one_holding() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("same.bin");
        fs::write(&source, b"same bytes").unwrap();
        let store = ObjectStore::open(temp.path().join("node")).unwrap();

        let first = store.import_file(&source).unwrap();
        let second = store.import_file(&source).unwrap();

        assert_eq!(first.holding.cid, second.holding.cid);
        assert_eq!(store.holdings().unwrap(), vec![second.holding]);
    }

    #[test]
    fn missing_object_is_not_validated() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("missing.bin");
        fs::write(&source, b"remove me").unwrap();
        let store = ObjectStore::open(temp.path().join("node")).unwrap();
        let imported = store.import_file(&source).unwrap();
        fs::remove_file(imported.object_path).unwrap();

        let report = store.validate_holdings().unwrap();
        assert!(report.valid.is_empty());
        assert_eq!(report.invalid.len(), 1);
    }

    #[test]
    fn corrupt_metadata_fails_closed() {
        let temp = tempfile::tempdir().unwrap();
        let node = temp.path().join("node");
        fs::create_dir_all(&node).unwrap();
        fs::write(node.join(HOLDINGS_FILE), b"not json").unwrap();
        assert!(ObjectStore::open(node).is_err());
    }
}
