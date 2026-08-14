use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cid::Cid;
use peeroxide::{JoinOpts, SwarmConfig, SwarmConnection, SwarmHandle, spawn};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout_at};

use crate::{
    HoldingValidation, MpError, NodeIdentity, ObjectStore, Result, ShareLink, parse_file_cid,
    receive_file, serve_file, topic_from_cid,
};

type PendingDownloads = Arc<Mutex<HashMap<[u8; 32], mpsc::Sender<SwarmConnection>>>>;

/// Explicit blind-relay endpoint used to bypass an unusable direct path.
#[derive(Clone, Debug)]
pub struct RelayConfig {
    /// Relay Peeroxide public key.
    pub public_key: [u8; 32],
    /// Directly reachable relay UDP address.
    pub address: SocketAddr,
}

/// Optional networking controls for a node.
#[derive(Clone, Debug, Default)]
pub struct NodeOptions {
    /// Force server-side connections through this blind relay.
    pub force_relay: Option<RelayConfig>,
}

/// Startup information for an active node.
#[derive(Clone, Debug)]
pub struct NodeStatus {
    /// Stable Peeroxide public key in hexadecimal form.
    pub public_key: String,
    /// CIDs currently joined in server-only mode.
    pub announced_cids: Vec<String>,
    /// Persisted holdings rejected during startup validation.
    pub invalid_holdings: Vec<HoldingValidation>,
    /// Explicit relay endpoint, when configured.
    pub relay: Option<String>,
}

/// Result of a successful network download.
#[derive(Clone, Debug)]
pub struct DownloadResult {
    /// Verified holding added to local state.
    pub holding: crate::Holding,
    /// Canonical local object path.
    pub object_path: std::path::PathBuf,
    /// Public key of the peer that completed the transfer.
    pub remote_public_key: String,
}

/// Running Peeroxide node that serves verified holdings and can download new ones.
pub struct Node {
    store: ObjectStore,
    handle: SwarmHandle,
    swarm_task: JoinHandle<()>,
    connection_task: JoinHandle<()>,
    pending: PendingDownloads,
    announced: Arc<Mutex<HashSet<String>>>,
    status: NodeStatus,
}

impl Node {
    /// Start a node, validate persisted objects, and announce every valid CID.
    pub async fn start(store: ObjectStore) -> Result<Self> {
        Self::start_with_options(store, NodeOptions::default()).await
    }

    /// Start a node with explicit experimental network controls.
    pub async fn start_with_options(store: ObjectStore, options: NodeOptions) -> Result<Self> {
        let identity = NodeIdentity::load_or_create(store.data_dir())?;
        let public_key = hex::encode(identity.key_pair().public_key);
        let store_for_validation = store.clone();
        let validation =
            tokio::task::spawn_blocking(move || store_for_validation.validate_holdings())
                .await
                .map_err(|error| MpError::Network(format!("validation task failed: {error}")))??;

        let mut config = SwarmConfig::with_public_bootstrap();
        config.key_pair = Some(identity.key_pair());
        if let Some(relay) = &options.force_relay {
            config.relay_through = Some(relay.public_key);
            config.relay_address = Some(relay.address);
        }
        let (swarm_task, handle, connection_rx) = spawn(config)
            .await
            .map_err(|error| MpError::Network(error.to_string()))?;

        let mut announced_cids = Vec::new();
        for holding in &validation.valid {
            let cid = parse_file_cid(&holding.cid)?;
            join_server(&handle, &cid).await?;
            announced_cids.push(holding.cid.clone());
        }
        if !announced_cids.is_empty() {
            handle
                .flush()
                .await
                .map_err(|error| MpError::Network(error.to_string()))?;
        }

        announced_cids.sort();
        let announced = Arc::new(Mutex::new(announced_cids.iter().cloned().collect()));
        let pending = Arc::new(Mutex::new(HashMap::new()));
        let connection_task = tokio::spawn(connection_loop(
            store.clone(),
            connection_rx,
            Arc::clone(&pending),
        ));

        Ok(Self {
            store,
            handle,
            swarm_task,
            connection_task,
            pending,
            announced,
            status: NodeStatus {
                public_key,
                announced_cids,
                invalid_holdings: validation.invalid,
                relay: options
                    .force_relay
                    .map(|relay| format!("{}@{}", hex::encode(relay.public_key), relay.address)),
            },
        })
    }

    /// Return startup state. Newly downloaded CIDs are available via `announced_cids`.
    pub fn status(&self) -> &NodeStatus {
        &self.status
    }

    /// Return all CIDs currently announced by this process.
    pub fn announced_cids(&self) -> Result<Vec<String>> {
        let mut cids: Vec<_> = self
            .announced
            .lock()
            .map_err(|_| MpError::Network("announced-topic lock is poisoned".to_string()))?
            .iter()
            .cloned()
            .collect();
        cids.sort();
        Ok(cids)
    }

    /// Download a share link before its deadline, verify it, and become a seed.
    pub async fn download(&self, link: &ShareLink, timeout: Duration) -> Result<DownloadResult> {
        let store_for_lookup = self.store.clone();
        let cid_for_lookup = *link.cid();
        let local_holding =
            tokio::task::spawn_blocking(move || store_for_lookup.find_verified(&cid_for_lookup))
                .await
                .map_err(|error| {
                    MpError::Network(format!("holding lookup task failed: {error}"))
                })?;
        if let Ok(holding) = local_holding {
            return Ok(DownloadResult {
                object_path: self.store.object_path(link.cid()),
                holding,
                remote_public_key: "local".to_string(),
            });
        }

        let topic = topic_from_cid(link.cid())?;
        let (connection_tx, mut connection_rx) = mpsc::channel(16);
        {
            let mut pending = self
                .pending
                .lock()
                .map_err(|_| MpError::Network("pending-download lock is poisoned".to_string()))?;
            if pending.contains_key(&topic) {
                return Err(MpError::Protocol(format!(
                    "download already active for {}",
                    link.cid()
                )));
            }
            pending.insert(topic, connection_tx);
        }

        let result = self
            .download_inner(link.cid(), topic, timeout, &mut connection_rx)
            .await;
        self.pending
            .lock()
            .map_err(|_| MpError::Network("pending-download lock is poisoned".to_string()))?
            .remove(&topic);
        if result.is_err() {
            let _ = self.handle.leave(topic).await;
        }
        result
    }

    async fn download_inner(
        &self,
        cid: &Cid,
        topic: [u8; 32],
        timeout: Duration,
        connection_rx: &mut mpsc::Receiver<SwarmConnection>,
    ) -> Result<DownloadResult> {
        join_client(&self.handle, topic).await?;
        self.handle
            .flush()
            .await
            .map_err(|error| MpError::Network(error.to_string()))?;

        let deadline = Instant::now() + timeout;
        let mut last_error = None;
        loop {
            let connection = timeout_at(deadline, connection_rx.recv())
                .await
                .map_err(|_| download_timeout(cid, last_error.as_ref()))?
                .ok_or_else(|| MpError::Network("connection router stopped".to_string()))?;
            let remote_public_key = hex::encode(connection.remote_public_key());
            match timeout_at(deadline, receive_file(self.store.clone(), cid, connection)).await {
                Ok(Ok(received)) => {
                    self.handle
                        .leave(topic)
                        .await
                        .map_err(|error| MpError::Network(error.to_string()))?;
                    join_server(&self.handle, cid).await?;
                    self.handle
                        .flush()
                        .await
                        .map_err(|error| MpError::Network(error.to_string()))?;
                    self.announced
                        .lock()
                        .map_err(|_| {
                            MpError::Network("announced-topic lock is poisoned".to_string())
                        })?
                        .insert(cid.to_string());
                    return Ok(DownloadResult {
                        holding: received.holding,
                        object_path: received.object_path,
                        remote_public_key,
                    });
                }
                Ok(Err(error)) => {
                    tracing::warn!(peer = %remote_public_key, %error, "file transfer failed");
                    last_error = Some(error.to_string());
                }
                Err(_) => return Err(download_timeout(cid, last_error.as_ref())),
            }
        }
    }

    /// Stop discovery and all active connections, then wait for the swarm task.
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

async fn connection_loop(
    store: ObjectStore,
    mut connections: mpsc::Receiver<SwarmConnection>,
    pending: PendingDownloads,
) {
    while let Some(connection) = connections.recv().await {
        if connection.is_initiator {
            let sender = connection.topics.iter().find_map(|topic| {
                pending
                    .lock()
                    .ok()
                    .and_then(|pending| pending.get(topic).cloned())
            });
            if let Some(sender) = sender {
                if sender.send(connection).await.is_err() {
                    tracing::debug!("download receiver closed before connection delivery");
                }
            } else {
                tracing::debug!("dropping outgoing connection with no pending download");
            }
            continue;
        }

        let store = store.clone();
        let peer = hex::encode(connection.remote_public_key());
        tokio::spawn(async move {
            if let Err(error) = serve_file(store, connection).await {
                tracing::warn!(%peer, %error, "file request failed");
            }
        });
    }
}

async fn join_server(handle: &SwarmHandle, cid: &Cid) -> Result<()> {
    let topic = topic_from_cid(cid)?;
    let mut options = JoinOpts::default();
    options.server = true;
    options.client = false;
    handle
        .join(topic, options)
        .await
        .map_err(|error| MpError::Network(error.to_string()))
}

async fn join_client(handle: &SwarmHandle, topic: [u8; 32]) -> Result<()> {
    let mut options = JoinOpts::default();
    options.server = false;
    options.client = true;
    handle
        .join(topic, options)
        .await
        .map_err(|error| MpError::Network(error.to_string()))
}

fn download_timeout(cid: &Cid, last_error: Option<&String>) -> MpError {
    let detail = match last_error {
        Some(error) => format!("waiting for {cid}; last peer error: {error}"),
        None => format!("waiting for a seed for {cid}"),
    };
    MpError::Timeout(detail)
}
