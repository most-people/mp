use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use peeroxide::{JoinOpts, KeyPair, SwarmConfig, SwarmConnection, SwarmHandle, spawn};
use tokio::sync::{broadcast, mpsc};
use tokio::task::{JoinHandle, JoinSet};

use crate::channel::{
    ChannelFrame, decode_channel_frame, encode_channel_frame, validate_typing_frame,
};
use crate::channel_store::AppendOutcome;
use crate::{
    CHANNEL_PROTOCOL, ChannelEvent, ChannelInvite, ChannelStore, MpError, NodeIdentity,
    NodeOptions, Result,
};

const OUTBOUND_CAPACITY: usize = 1024;
const EVENT_CAPACITY: usize = 1024;

/// Startup state for one live channel process.
#[derive(Clone, Debug)]
pub struct ChannelStatus {
    /// Stable local Peeroxide public key.
    pub public_key: String,
    /// Joined channel id.
    pub channel_id: String,
    /// Advisory channel name.
    pub name: String,
    /// Explicit relay endpoint, when configured.
    pub relay: Option<String>,
}

/// Running Peeroxide node for one live `mp-channel/1` topic.
pub struct ChannelNode {
    store: ChannelStore,
    invite: ChannelInvite,
    key_pair: KeyPair,
    handle: SwarmHandle,
    swarm_task: JoinHandle<()>,
    connection_task: JoinHandle<()>,
    outbound: broadcast::Sender<Vec<u8>>,
    event_tx: mpsc::Sender<ChannelEvent>,
    events: mpsc::Receiver<ChannelEvent>,
    peer_count: Arc<AtomicUsize>,
    status: ChannelStatus,
}

impl ChannelNode {
    /// Start a live channel with default network options.
    pub async fn start(store: ChannelStore, invite: ChannelInvite) -> Result<Self> {
        Self::start_with_options(store, invite, NodeOptions::default()).await
    }

    /// Start a live channel with explicit experimental network controls.
    pub async fn start_with_options(
        store: ChannelStore,
        invite: ChannelInvite,
        options: NodeOptions,
    ) -> Result<Self> {
        let invite = store.add(&invite)?;
        let identity = NodeIdentity::load_or_create(store.data_dir())?;
        let key_pair = identity.key_pair();
        let public_key = hex::encode(key_pair.public_key);

        let mut config = SwarmConfig::with_public_bootstrap();
        config.key_pair = Some(key_pair.clone());
        if let Some(relay) = &options.force_relay {
            config.relay_through = Some(relay.public_key);
            config.relay_address = Some(relay.address);
        }
        let (swarm_task, handle, connection_rx) = spawn(config)
            .await
            .map_err(|error| MpError::Network(error.to_string()))?;
        handle
            .join(invite.topic(), JoinOpts::default())
            .await
            .map_err(|error| MpError::Network(error.to_string()))?;
        handle
            .flush()
            .await
            .map_err(|error| MpError::Network(error.to_string()))?;

        let (outbound, _) = broadcast::channel(OUTBOUND_CAPACITY);
        let (event_tx, events) = mpsc::channel(EVENT_CAPACITY);
        let peer_count = Arc::new(AtomicUsize::new(0));
        let connection_task = tokio::spawn(connection_loop(
            store.clone(),
            invite.id(),
            connection_rx,
            outbound.clone(),
            event_tx.clone(),
            Arc::clone(&peer_count),
        ));

        Ok(Self {
            store,
            status: ChannelStatus {
                public_key,
                channel_id: invite.id(),
                name: invite.name().to_string(),
                relay: options
                    .force_relay
                    .map(|relay| format!("{}@{}", hex::encode(relay.public_key), relay.address)),
            },
            invite,
            key_pair,
            handle,
            swarm_task,
            connection_task,
            outbound,
            event_tx,
            events,
            peer_count,
        })
    }

    /// Return immutable startup state.
    pub fn status(&self) -> &ChannelStatus {
        &self.status
    }

    /// Return the number of active authenticated transport connections.
    pub fn peer_count(&self) -> usize {
        self.peer_count.load(Ordering::Acquire)
    }

    /// Wait for the next accepted presence, typing, or text event.
    pub async fn next_event(&mut self) -> Option<ChannelEvent> {
        self.events.recv().await
    }

    /// Sign, persist, and broadcast one live text entry.
    pub async fn send_text(&self, text: impl Into<String>) -> Result<String> {
        if self.peer_count() == 0 {
            return Err(MpError::Channel(
                "cannot append live text without an online peer; history sync is Phase 5"
                    .to_string(),
            ));
        }
        let store = self.store.clone();
        let channel_id = self.invite.id();
        let key_pair = self.key_pair.clone();
        let text = text.into();
        let timestamp_ms = unix_timestamp_ms()?;
        let (message, message_id) = tokio::task::spawn_blocking(move || {
            store.create_message(&channel_id, timestamp_ms, text, &key_pair)
        })
        .await
        .map_err(|error| MpError::Network(format!("channel-store task failed: {error}")))??;
        let encoded = encode_channel_frame(&ChannelFrame::Text {
            message: message.clone(),
        })?;
        self.outbound.send(encoded).map_err(|_| {
            MpError::Network("all channel connections closed before send".to_string())
        })?;
        let _ = self
            .event_tx
            .send(text_event(message, message_id.clone()))
            .await;
        Ok(message_id)
    }

    /// Broadcast transient typing state without storing it.
    pub fn send_typing(&self, active: bool) -> Result<()> {
        if self.peer_count() == 0 {
            return Err(MpError::Channel(
                "cannot send typing state without an online peer".to_string(),
            ));
        }
        let encoded = encode_channel_frame(&ChannelFrame::Typing {
            protocol: CHANNEL_PROTOCOL.to_string(),
            channel_id: self.invite.id(),
            active,
        })?;
        self.outbound.send(encoded).map_err(|_| {
            MpError::Network("all channel connections closed before send".to_string())
        })?;
        Ok(())
    }

    /// Leave discovery, close connections, and wait for the swarm task.
    pub async fn shutdown(self) -> Result<()> {
        self.handle
            .destroy()
            .await
            .map_err(|error| MpError::Network(error.to_string()))?;
        self.connection_task.abort();
        let _ = self.connection_task.await;
        self.swarm_task
            .await
            .map_err(|error| MpError::Network(format!("swarm task failed: {error}")))?;
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
async fn connection_loop(
    store: ChannelStore,
    channel_id: String,
    mut connections: mpsc::Receiver<SwarmConnection>,
    outbound: broadcast::Sender<Vec<u8>>,
    events: mpsc::Sender<ChannelEvent>,
    peer_count: Arc<AtomicUsize>,
) {
    let mut tasks = JoinSet::new();
    loop {
        tokio::select! {
            connection = connections.recv() => {
                let Some(connection) = connection else { break };
                // Peeroxide 1.7.3 leaves inbound `topics` empty; this node owns only one topic.
                tasks.spawn(run_connection(
                    store.clone(),
                    channel_id.clone(),
                    connection,
                    outbound.subscribe(),
                    events.clone(),
                    Arc::clone(&peer_count),
                ));
            }
            completed = tasks.join_next(), if !tasks.is_empty() => {
                if let Some(Err(error)) = completed {
                    tracing::debug!(%error, "channel connection task stopped");
                }
            }
        }
    }
    tasks.abort_all();
}

async fn run_connection(
    store: ChannelStore,
    channel_id: String,
    mut connection: SwarmConnection,
    mut outbound: broadcast::Receiver<Vec<u8>>,
    events: mpsc::Sender<ChannelEvent>,
    peer_count: Arc<AtomicUsize>,
) {
    let remote_public_key = *connection.remote_public_key();
    let peer = hex::encode(remote_public_key);
    peer_count.fetch_add(1, Ordering::AcqRel);
    let _ = events
        .send(ChannelEvent::Presence {
            peer: peer.clone(),
            online: true,
        })
        .await;

    loop {
        tokio::select! {
            incoming = connection.peer.stream.read() => {
                let encoded = match incoming {
                    Ok(Some(encoded)) => encoded,
                    Ok(None) => break,
                    Err(error) => {
                        tracing::debug!(%peer, %error, "channel stream read failed");
                        break;
                    }
                };
                if let Err(error) = accept_incoming(
                    store.clone(),
                    &channel_id,
                    &remote_public_key,
                    &peer,
                    &events,
                    &encoded,
                ).await {
                    tracing::warn!(%peer, %error, "rejected channel frame");
                    break;
                }
            }
            outgoing = outbound.recv() => {
                let encoded = match outgoing {
                    Ok(encoded) => encoded,
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(%peer, skipped, "channel connection lagged outbound frames");
                        break;
                    }
                };
                if let Err(error) = connection.peer.stream.write(&encoded).await {
                    tracing::debug!(%peer, %error, "channel stream write failed");
                    break;
                }
            }
        }
    }

    peer_count.fetch_sub(1, Ordering::AcqRel);
    let _ = events
        .send(ChannelEvent::Presence {
            peer,
            online: false,
        })
        .await;
}

async fn accept_incoming(
    store: ChannelStore,
    channel_id: &str,
    remote_public_key: &[u8; 32],
    peer: &str,
    events: &mpsc::Sender<ChannelEvent>,
    encoded: &[u8],
) -> Result<()> {
    match decode_channel_frame(encoded)? {
        ChannelFrame::Text { message } => {
            if message.writer != hex::encode(remote_public_key) {
                return Err(MpError::Channel(
                    "message writer does not match authenticated transport peer".to_string(),
                ));
            }
            let store_for_append = store.clone();
            let event_message = message.clone();
            let outcome =
                tokio::task::spawn_blocking(move || store_for_append.append_message(message))
                    .await
                    .map_err(|error| {
                        MpError::Network(format!("channel-store task failed: {error}"))
                    })??;
            if let AppendOutcome::Added { message_id } = outcome {
                let _ = events.send(text_event(event_message, message_id)).await;
            }
        }
        ChannelFrame::Typing {
            protocol,
            channel_id: frame_channel_id,
            active,
        } => {
            validate_typing_frame(&protocol, &frame_channel_id)?;
            if frame_channel_id != channel_id {
                return Err(MpError::Channel(
                    "typing event belongs to another channel".to_string(),
                ));
            }
            let _ = events
                .send(ChannelEvent::Typing {
                    peer: peer.to_string(),
                    active,
                })
                .await;
        }
    }
    Ok(())
}

fn text_event(message: crate::SignedChannelMessage, message_id: String) -> ChannelEvent {
    ChannelEvent::Text {
        message_id,
        writer: message.writer,
        sequence: message.sequence,
        timestamp_ms: message.timestamp_ms,
        text: message.text,
    }
}

fn unix_timestamp_ms() -> Result<u64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| MpError::Channel(format!("system clock precedes Unix epoch: {error}")))?
        .as_millis();
    u64::try_from(millis)
        .map_err(|_| MpError::Channel("Unix timestamp does not fit in u64".to_string()))
}
