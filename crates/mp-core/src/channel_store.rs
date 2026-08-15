use std::collections::{HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use peeroxide::KeyPair;
use serde::{Deserialize, Serialize};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

use crate::{ChannelInvite, MpError, Result, SignedChannelMessage};

const CHANNELS_FILE: &str = "channels.json";

/// Non-secret summary of a locally joined channel.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChannelSummary {
    /// Stable hexadecimal channel id.
    pub id: String,
    /// Advisory display name.
    pub name: String,
    /// Number of accepted persistent text entries.
    pub message_count: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StoredChannel {
    id: String,
    name: String,
    capability: String,
    messages: Vec<SignedChannelMessage>,
}

impl StoredChannel {
    fn invite(&self) -> Result<ChannelInvite> {
        let bytes = hex::decode(&self.capability)
            .map_err(|_| channel_state_error("capability must be hexadecimal"))?;
        let capability: [u8; 32] = bytes.try_into().map_err(|bytes: Vec<u8>| {
            channel_state_error(format!("capability must be 32 bytes, got {}", bytes.len()))
        })?;
        if hex::encode(capability) != self.capability {
            return Err(channel_state_error(
                "capability must use canonical lowercase hexadecimal",
            ));
        }
        let invite = ChannelInvite::from_capability(self.name.clone(), capability)?;
        if invite.id() != self.id {
            return Err(channel_state_error(
                "channel id does not match persisted capability",
            ));
        }
        Ok(invite)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ChannelsState {
    version: u32,
    channels: Vec<StoredChannel>,
}

impl Default for ChannelsState {
    fn default() -> Self {
        Self {
            version: 1,
            channels: Vec::new(),
        }
    }
}

struct ChannelStoreInner {
    data_dir: PathBuf,
    state_path: PathBuf,
    state: Mutex<ChannelsState>,
}

/// Atomic local channel membership and signed-message state.
#[derive(Clone)]
pub struct ChannelStore {
    inner: Arc<ChannelStoreInner>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AppendOutcome {
    Added { message_id: String },
    Duplicate,
}

impl ChannelStore {
    /// Open or create channel state, validating every capability and writer chain.
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        let state_path = data_dir.join(CHANNELS_FILE);
        fs::create_dir_all(&data_dir)?;
        let state = if state_path.exists() {
            let bytes = fs::read(&state_path)?;
            serde_json::from_slice(&bytes).map_err(|error| MpError::InvalidState {
                path: state_path.clone(),
                message: error.to_string(),
            })?
        } else {
            ChannelsState::default()
        };
        validate_state(&state).map_err(|error| MpError::InvalidState {
            path: state_path.clone(),
            message: error.to_string(),
        })?;

        Ok(Self {
            inner: Arc::new(ChannelStoreInner {
                data_dir,
                state_path,
                state: Mutex::new(state),
            }),
        })
    }

    /// Return the root data directory shared with the persistent node identity.
    pub fn data_dir(&self) -> &Path {
        &self.inner.data_dir
    }

    /// Create, join, and persist a new random channel.
    pub fn create(&self, name: impl Into<String>) -> Result<ChannelInvite> {
        let invite = ChannelInvite::generate(name)?;
        self.add(&invite)?;
        Ok(invite)
    }

    /// Persist membership from a capability invite, returning the local record.
    pub fn add(&self, invite: &ChannelInvite) -> Result<ChannelInvite> {
        let id = invite.id();
        let mut state = self.lock_state()?;
        if let Some(existing) = state.channels.iter().find(|channel| channel.id == id) {
            let existing = existing.invite()?;
            if existing.capability() != invite.capability() {
                return Err(MpError::Channel(
                    "channel id collision with a different capability".to_string(),
                ));
            }
            return Ok(existing);
        }

        state.channels.push(StoredChannel {
            id,
            name: invite.name().to_string(),
            capability: hex::encode(invite.capability()),
            messages: Vec::new(),
        });
        state.channels.sort_by(|left, right| left.id.cmp(&right.id));
        self.persist_state(&state)?;
        Ok(invite.clone())
    }

    /// Resolve one locally joined channel by id.
    pub fn invite(&self, channel_id: &str) -> Result<ChannelInvite> {
        self.lock_state()?
            .channels
            .iter()
            .find(|channel| channel.id == channel_id)
            .ok_or_else(|| MpError::NotFound(format!("channel {channel_id}")))?
            .invite()
    }

    /// List non-secret local channel summaries.
    pub fn channels(&self) -> Result<Vec<ChannelSummary>> {
        Ok(self
            .lock_state()?
            .channels
            .iter()
            .map(|channel| ChannelSummary {
                id: channel.id.clone(),
                name: channel.name.clone(),
                message_count: channel.messages.len(),
            })
            .collect())
    }

    /// Return accepted persistent text entries for one channel.
    pub fn messages(&self, channel_id: &str) -> Result<Vec<SignedChannelMessage>> {
        Ok(self
            .lock_state()?
            .channels
            .iter()
            .find(|channel| channel.id == channel_id)
            .ok_or_else(|| MpError::NotFound(format!("channel {channel_id}")))?
            .messages
            .clone())
    }

    pub(crate) fn create_message(
        &self,
        channel_id: &str,
        timestamp_ms: u64,
        text: String,
        key_pair: &KeyPair,
    ) -> Result<(SignedChannelMessage, String)> {
        let mut state = self.lock_state()?;
        let channel = state
            .channels
            .iter_mut()
            .find(|channel| channel.id == channel_id)
            .ok_or_else(|| MpError::NotFound(format!("channel {channel_id}")))?;
        let writer = hex::encode(key_pair.public_key);
        let previous = channel
            .messages
            .iter()
            .rev()
            .find(|message| message.writer == writer);
        let (sequence, previous) = match previous {
            Some(previous) => (previous.sequence + 1, Some(previous.id()?)),
            None => (1, None),
        };
        let message = SignedChannelMessage::sign(
            channel_id,
            sequence,
            previous,
            timestamp_ms,
            text,
            key_pair,
        )?;
        let message_id = message.verify(channel_id)?;
        channel.messages.push(message.clone());
        self.persist_state(&state)?;
        Ok((message, message_id))
    }

    pub(crate) fn append_message(&self, message: SignedChannelMessage) -> Result<AppendOutcome> {
        let mut state = self.lock_state()?;
        let channel = state
            .channels
            .iter_mut()
            .find(|channel| channel.id == message.channel_id)
            .ok_or_else(|| MpError::NotFound(format!("channel {}", message.channel_id)))?;
        let message_id = message.verify(&channel.id)?;

        for existing in &channel.messages {
            if existing.id()? == message_id {
                return Ok(AppendOutcome::Duplicate);
            }
        }

        let previous = channel
            .messages
            .iter()
            .rev()
            .find(|existing| existing.writer == message.writer);
        match previous {
            None if message.sequence == 1 && message.previous.is_none() => {}
            Some(previous)
                if message.sequence == previous.sequence + 1
                    && message.previous.as_deref() == Some(previous.id()?.as_str()) => {}
            _ => {
                return Err(MpError::Channel(format!(
                    "writer {} sequence {} does not extend its accepted head",
                    message.writer, message.sequence
                )));
            }
        }

        channel.messages.push(message);
        self.persist_state(&state)?;
        Ok(AppendOutcome::Added { message_id })
    }

    fn persist_state(&self, state: &ChannelsState) -> Result<()> {
        let temp_path = self.inner.data_dir.join(format!(
            ".{CHANNELS_FILE}.{}.{:016x}.tmp",
            std::process::id(),
            rand::random::<u64>()
        ));
        let mut guard = TempGuard::new(temp_path.clone());
        let bytes = serde_json::to_vec_pretty(state)?;
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file = options.open(&temp_path)?;
        file.write_all(&bytes)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        fs::rename(&temp_path, &self.inner.state_path)?;
        guard.disarm();
        Ok(())
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, ChannelsState>> {
        self.inner.state.lock().map_err(|_| MpError::InvalidState {
            path: self.inner.state_path.clone(),
            message: "channel-state lock is poisoned".to_string(),
        })
    }
}

fn validate_state(state: &ChannelsState) -> Result<()> {
    if state.version != 1 {
        return Err(channel_state_error(format!(
            "unsupported channels version: {}",
            state.version
        )));
    }
    let mut channel_ids = HashSet::new();
    for channel in &state.channels {
        if !channel_ids.insert(channel.id.clone()) {
            return Err(channel_state_error(format!(
                "duplicate channel id: {}",
                channel.id
            )));
        }
        channel.invite()?;
        validate_channel_messages(channel)?;
    }
    Ok(())
}

fn validate_channel_messages(channel: &StoredChannel) -> Result<()> {
    let mut heads: HashMap<String, (u64, String)> = HashMap::new();
    let mut message_ids = HashSet::new();
    for message in &channel.messages {
        let message_id = message.verify(&channel.id)?;
        if !message_ids.insert(message_id.clone()) {
            return Err(channel_state_error(format!(
                "duplicate message id: {message_id}"
            )));
        }
        match heads.get(&message.writer) {
            None if message.sequence == 1 && message.previous.is_none() => {}
            Some((sequence, previous))
                if message.sequence == sequence + 1
                    && message.previous.as_deref() == Some(previous.as_str()) => {}
            _ => {
                return Err(channel_state_error(format!(
                    "writer {} sequence {} does not extend its persisted head",
                    message.writer, message.sequence
                )));
            }
        }
        heads.insert(message.writer.clone(), (message.sequence, message_id));
    }
    Ok(())
}

fn channel_state_error(message: impl Into<String>) -> MpError {
    MpError::Channel(message.into())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persists_and_revalidates_independent_writer_chains() {
        let temp = tempfile::tempdir().unwrap();
        let store = ChannelStore::open(temp.path()).unwrap();
        let invite = ChannelInvite::from_capability("room", [3u8; 32]).unwrap();
        store.add(&invite).unwrap();
        let first_writer = KeyPair::from_seed([4u8; 32]);
        let second_writer = KeyPair::from_seed([5u8; 32]);

        let (first, first_id) = store
            .create_message(&invite.id(), 10, "one".to_string(), &first_writer)
            .unwrap();
        assert_eq!(first.sequence, 1);
        let second =
            SignedChannelMessage::sign(&invite.id(), 1, None, 11, "other writer", &second_writer)
                .unwrap();
        assert!(matches!(
            store.append_message(second.clone()).unwrap(),
            AppendOutcome::Added { .. }
        ));
        assert_eq!(
            store.append_message(second).unwrap(),
            AppendOutcome::Duplicate
        );
        let (third, _) = store
            .create_message(&invite.id(), 12, "two".to_string(), &first_writer)
            .unwrap();
        assert_eq!(third.sequence, 2);
        assert_eq!(third.previous.as_deref(), Some(first_id.as_str()));

        let reopened = ChannelStore::open(temp.path()).unwrap();
        assert_eq!(reopened.channels().unwrap()[0].message_count, 3);
        assert_eq!(reopened.messages(&invite.id()).unwrap().len(), 3);
    }

    #[test]
    fn rejects_a_valid_signature_on_a_broken_writer_chain() {
        let temp = tempfile::tempdir().unwrap();
        let store = ChannelStore::open(temp.path()).unwrap();
        let invite = ChannelInvite::from_capability("room", [6u8; 32]).unwrap();
        store.add(&invite).unwrap();
        let writer = KeyPair::from_seed([7u8; 32]);
        let broken = SignedChannelMessage::sign(
            &invite.id(),
            2,
            Some("00".repeat(32)),
            10,
            "broken",
            &writer,
        )
        .unwrap();
        assert!(store.append_message(broken).is_err());
        assert!(store.messages(&invite.id()).unwrap().is_empty());
    }

    #[test]
    fn corrupt_persisted_chain_fails_closed() {
        let temp = tempfile::tempdir().unwrap();
        let store = ChannelStore::open(temp.path()).unwrap();
        let invite = store.create("room").unwrap();
        let writer = KeyPair::from_seed([8u8; 32]);
        store
            .create_message(&invite.id(), 10, "valid".to_string(), &writer)
            .unwrap();

        let state_path = temp.path().join(CHANNELS_FILE);
        let mut state: serde_json::Value =
            serde_json::from_slice(&fs::read(&state_path).unwrap()).unwrap();
        state["channels"][0]["messages"][0]["text"] = serde_json::json!("tampered");
        fs::write(&state_path, serde_json::to_vec(&state).unwrap()).unwrap();
        assert!(ChannelStore::open(temp.path()).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn capability_state_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let store = ChannelStore::open(temp.path()).unwrap();
        store.create("private").unwrap();
        let mode = fs::metadata(temp.path().join(CHANNELS_FILE))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
    }
}
