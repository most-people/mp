use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use axum::body::Body;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{DefaultBodyLimit, Path as AxumPath, State};
use axum::http::{HeaderValue, Request, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{any, get, post};
use axum::{Json, Router};
use mp_core::{
    ChannelEvent, ChannelInvite, ChannelNode, ChannelStore, NodeOptions, SignedChannelMessage,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex as AsyncMutex, broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;

const COMMAND_CAPACITY: usize = 64;
const WEB_EVENT_CAPACITY: usize = 1024;
const API_BODY_LIMIT: usize = 16 * 1024;
const COMMAND_TIMEOUT: Duration = Duration::from_secs(30);

const INDEX_HTML: &str = include_str!("../web/index.html");
const APP_CSS: &str = include_str!("../web/app.css");
const APP_JS: &str = include_str!("../web/app.js");

#[derive(Clone)]
struct AppState {
    store: ChannelStore,
    node_options: NodeOptions,
    session: Arc<AsyncMutex<Option<SessionHandle>>>,
    events: broadcast::Sender<SocketPayload>,
}

struct SessionHandle {
    channel_id: String,
    name: String,
    public_key: String,
    peers: Arc<Mutex<HashSet<String>>>,
    commands: mpsc::Sender<SessionCommand>,
    task: JoinHandle<()>,
}

enum SessionCommand {
    SendText {
        text: String,
        reply: oneshot::Sender<std::result::Result<String, String>>,
    },
    SetTyping {
        active: bool,
        reply: oneshot::Sender<std::result::Result<(), String>>,
    },
    Shutdown {
        reply: oneshot::Sender<std::result::Result<(), String>>,
    },
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SessionSummary {
    channel_id: String,
    name: String,
    public_key: String,
    peer_count: usize,
    peers: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
enum SocketPayload {
    Snapshot {
        active: Option<SessionSummary>,
    },
    ChannelEvent {
        channel_id: String,
        event: ChannelEvent,
    },
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ChannelView {
    id: String,
    name: String,
    message_count: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct StatusResponse {
    channels: Vec<ChannelView>,
    active: Option<SessionSummary>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CreateChannelRequest {
    name: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct JoinChannelRequest {
    invite: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SendTextRequest {
    text: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TypingRequest {
    active: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CreatedChannelResponse {
    invite: String,
    active: SessionSummary,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ActiveChannelResponse {
    active: SessionSummary,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct InviteResponse {
    invite: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SentMessageResponse {
    message_id: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct HistoryMessage {
    message_id: String,
    writer: String,
    sequence: u64,
    timestamp_ms: u64,
    text: String,
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

impl SessionHandle {
    fn summary(&self) -> SessionSummary {
        let mut peers: Vec<_> = self
            .peers
            .lock()
            .map(|peers| peers.iter().cloned().collect())
            .unwrap_or_default();
        peers.sort();
        SessionSummary {
            channel_id: self.channel_id.clone(),
            name: self.name.clone(),
            public_key: self.public_key.clone(),
            peer_count: peers.len(),
            peers,
        }
    }

    async fn shutdown(self) -> std::result::Result<(), String> {
        let (reply, response) = oneshot::channel();
        if self
            .commands
            .send(SessionCommand::Shutdown { reply })
            .await
            .is_err()
        {
            self.task.abort();
            let _ = self.task.await;
            return Err("channel session stopped before shutdown".to_string());
        }
        let shutdown = tokio::time::timeout(COMMAND_TIMEOUT, response)
            .await
            .map_err(|_| "channel shutdown timed out".to_string())?
            .map_err(|_| "channel shutdown response was dropped".to_string())?;
        self.task
            .await
            .map_err(|error| format!("channel session task failed: {error}"))?;
        shutdown
    }
}

impl AppState {
    async fn status(&self) -> std::result::Result<StatusResponse, ApiError> {
        let channels = self
            .store
            .channels()
            .map_err(ApiError::internal)?
            .into_iter()
            .map(|channel| ChannelView {
                id: channel.id,
                name: channel.name,
                message_count: channel.message_count,
            })
            .collect();
        let active = self
            .session
            .lock()
            .await
            .as_ref()
            .map(SessionHandle::summary);
        Ok(StatusResponse { channels, active })
    }

    async fn activate(
        &self,
        invite: ChannelInvite,
    ) -> std::result::Result<SessionSummary, ApiError> {
        let channel_id = invite.id();
        let mut session = self.session.lock().await;
        if let Some(current) = session.as_ref()
            && current.channel_id == channel_id
        {
            return Ok(current.summary());
        }
        if let Some(current) = session.take() {
            current.shutdown().await.map_err(ApiError::internal)?;
        }

        let node = ChannelNode::start_with_options(
            self.store.clone(),
            invite.clone(),
            self.node_options.clone(),
        )
        .await
        .map_err(ApiError::network)?;
        let public_key = node.status().public_key.clone();
        let peers = Arc::new(Mutex::new(HashSet::new()));
        let (commands, command_rx) = mpsc::channel(COMMAND_CAPACITY);
        let task = tokio::spawn(session_loop(
            node,
            channel_id.clone(),
            Arc::clone(&peers),
            command_rx,
            self.events.clone(),
        ));
        let handle = SessionHandle {
            channel_id,
            name: invite.name().to_string(),
            public_key,
            peers,
            commands,
            task,
        };
        let summary = handle.summary();
        *session = Some(handle);
        let _ = self.events.send(SocketPayload::Snapshot {
            active: Some(summary.clone()),
        });
        Ok(summary)
    }

    async fn send_text(&self, text: String) -> std::result::Result<String, ApiError> {
        let commands = self.active_commands().await?;
        let (reply, response) = oneshot::channel();
        commands
            .send(SessionCommand::SendText { text, reply })
            .await
            .map_err(|_| ApiError::conflict("active channel session has stopped"))?;
        tokio::time::timeout(COMMAND_TIMEOUT, response)
            .await
            .map_err(|_| ApiError::conflict("message send timed out"))?
            .map_err(|_| ApiError::conflict("message response was dropped"))?
            .map_err(ApiError::conflict)
    }

    async fn set_typing(&self, active: bool) -> std::result::Result<(), ApiError> {
        let commands = self.active_commands().await?;
        let (reply, response) = oneshot::channel();
        commands
            .send(SessionCommand::SetTyping { active, reply })
            .await
            .map_err(|_| ApiError::conflict("active channel session has stopped"))?;
        tokio::time::timeout(COMMAND_TIMEOUT, response)
            .await
            .map_err(|_| ApiError::conflict("typing update timed out"))?
            .map_err(|_| ApiError::conflict("typing response was dropped"))?
            .map_err(ApiError::conflict)
    }

    async fn active_commands(&self) -> std::result::Result<mpsc::Sender<SessionCommand>, ApiError> {
        self.session
            .lock()
            .await
            .as_ref()
            .map(|session| session.commands.clone())
            .ok_or_else(|| ApiError::conflict("no channel is open"))
    }

    async fn stop(&self) -> std::result::Result<(), ApiError> {
        let session = self.session.lock().await.take();
        if let Some(session) = session {
            session.shutdown().await.map_err(ApiError::internal)?;
        }
        Ok(())
    }
}

impl ApiError {
    fn bad_request(message: impl ToString) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.to_string(),
        }
    }

    fn not_found(message: impl ToString) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.to_string(),
        }
    }

    fn conflict(message: impl ToString) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            message: message.to_string(),
        }
    }

    fn network(message: impl ToString) -> Self {
        Self {
            status: StatusCode::BAD_GATEWAY,
            message: message.to_string(),
        }
    }

    fn internal(message: impl ToString) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.to_string(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorResponse {
                error: self.message,
            }),
        )
            .into_response()
    }
}

/// Start the local browser interface and API server.
pub async fn serve(data_dir: &Path, node_options: NodeOptions, listen: SocketAddr) -> Result<()> {
    let store = ChannelStore::open(data_dir)
        .with_context(|| format!("failed to open data directory {}", data_dir.display()))?;
    let (events, _) = broadcast::channel(WEB_EVENT_CAPACITY);
    let state = AppState {
        store,
        node_options,
        session: Arc::new(AsyncMutex::new(None)),
        events,
    };
    let app = Router::new()
        .route("/", get(index))
        .route("/app.css", get(stylesheet))
        .route("/app.js", get(javascript))
        .route("/api/status", get(status))
        .route("/api/channels", post(create_channel))
        .route("/api/channels/join", post(join_channel))
        .route("/api/channels/{channel_id}/open", post(open_channel))
        .route("/api/channels/{channel_id}/invite", get(channel_invite))
        .route("/api/channels/{channel_id}/messages", get(channel_messages))
        .route("/api/messages", post(send_text))
        .route("/api/typing", post(set_typing))
        .route("/api/events", any(events_socket))
        .layer(DefaultBodyLimit::max(API_BODY_LIMIT))
        .layer(middleware::from_fn(security_headers))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .with_context(|| format!("failed to bind web service at {listen}"))?;
    let local_address = listener.local_addr()?;
    if !local_address.ip().is_loopback() {
        eprintln!("WARNING web service has no authentication and is listening on {local_address}");
    }
    println!("WEB http://{local_address}");
    println!("DATA {}", data_dir.display());
    println!("READY browser service");

    let shutdown_state = state.clone();
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                let _ = shutdown_state.stop().await;
            }
        })
        .await
        .context("web service failed")
}

async fn session_loop(
    mut node: ChannelNode,
    channel_id: String,
    peers: Arc<Mutex<HashSet<String>>>,
    mut commands: mpsc::Receiver<SessionCommand>,
    events: broadcast::Sender<SocketPayload>,
) {
    let mut shutdown_reply = None;
    loop {
        tokio::select! {
            command = commands.recv() => {
                match command {
                    Some(SessionCommand::SendText { text, reply }) => {
                        let result = node.send_text(text).await.map_err(|error| error.to_string());
                        let _ = reply.send(result);
                    }
                    Some(SessionCommand::SetTyping { active, reply }) => {
                        let result = node.send_typing(active).map_err(|error| error.to_string());
                        let _ = reply.send(result);
                    }
                    Some(SessionCommand::Shutdown { reply }) => {
                        shutdown_reply = Some(reply);
                        break;
                    }
                    None => break,
                }
            }
            event = node.next_event() => {
                let Some(event) = event else { break };
                if let ChannelEvent::Presence { peer, online } = &event
                    && let Ok(mut active_peers) = peers.lock()
                {
                    if *online {
                        active_peers.insert(peer.clone());
                    } else {
                        active_peers.remove(peer);
                    }
                }
                let _ = events.send(SocketPayload::ChannelEvent {
                    channel_id: channel_id.clone(),
                    event,
                });
            }
        }
    }

    let result = node.shutdown().await.map_err(|error| error.to_string());
    if let Some(reply) = shutdown_reply {
        let _ = reply.send(result);
    }
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn stylesheet() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "text/css; charset=utf-8")], APP_CSS)
}

async fn javascript() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
        APP_JS,
    )
}

async fn status(
    State(state): State<AppState>,
) -> std::result::Result<Json<StatusResponse>, ApiError> {
    Ok(Json(state.status().await?))
}

async fn create_channel(
    State(state): State<AppState>,
    Json(request): Json<CreateChannelRequest>,
) -> std::result::Result<Json<CreatedChannelResponse>, ApiError> {
    let store = state.store.clone();
    let invite = tokio::task::spawn_blocking(move || store.create(request.name))
        .await
        .map_err(ApiError::internal)?
        .map_err(ApiError::bad_request)?;
    let active = state.activate(invite.clone()).await?;
    Ok(Json(CreatedChannelResponse {
        invite: invite.to_string(),
        active,
    }))
}

async fn join_channel(
    State(state): State<AppState>,
    Json(request): Json<JoinChannelRequest>,
) -> std::result::Result<Json<ActiveChannelResponse>, ApiError> {
    let invite: ChannelInvite = request.invite.parse().map_err(ApiError::bad_request)?;
    let store = state.store.clone();
    let invite = tokio::task::spawn_blocking(move || store.add(&invite))
        .await
        .map_err(ApiError::internal)?
        .map_err(ApiError::bad_request)?;
    let active = state.activate(invite).await?;
    Ok(Json(ActiveChannelResponse { active }))
}

async fn open_channel(
    State(state): State<AppState>,
    AxumPath(channel_id): AxumPath<String>,
) -> std::result::Result<Json<ActiveChannelResponse>, ApiError> {
    let invite = state
        .store
        .invite(&channel_id)
        .map_err(|_| ApiError::not_found(format!("channel {channel_id} is not local")))?;
    let active = state.activate(invite).await?;
    Ok(Json(ActiveChannelResponse { active }))
}

async fn channel_invite(
    State(state): State<AppState>,
    AxumPath(channel_id): AxumPath<String>,
) -> std::result::Result<Json<InviteResponse>, ApiError> {
    let invite = state
        .store
        .invite(&channel_id)
        .map_err(|_| ApiError::not_found(format!("channel {channel_id} is not local")))?;
    Ok(Json(InviteResponse {
        invite: invite.to_string(),
    }))
}

async fn channel_messages(
    State(state): State<AppState>,
    AxumPath(channel_id): AxumPath<String>,
) -> std::result::Result<Json<Vec<HistoryMessage>>, ApiError> {
    let messages = state
        .store
        .messages(&channel_id)
        .map_err(|_| ApiError::not_found(format!("channel {channel_id} is not local")))?;
    let messages = messages
        .into_iter()
        .map(HistoryMessage::try_from)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(Json(messages))
}

async fn send_text(
    State(state): State<AppState>,
    Json(request): Json<SendTextRequest>,
) -> std::result::Result<Json<SentMessageResponse>, ApiError> {
    let message_id = state.send_text(request.text).await?;
    Ok(Json(SentMessageResponse { message_id }))
}

async fn set_typing(
    State(state): State<AppState>,
    Json(request): Json<TypingRequest>,
) -> std::result::Result<StatusCode, ApiError> {
    state.set_typing(request.active).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn events_socket(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    let active = state.status().await.ok().and_then(|status| status.active);
    let receiver = state.events.subscribe();
    ws.max_message_size(API_BODY_LIMIT)
        .on_upgrade(move |socket| websocket(socket, receiver, active))
}

async fn websocket(
    mut socket: WebSocket,
    mut events: broadcast::Receiver<SocketPayload>,
    active: Option<SessionSummary>,
) {
    if send_socket_payload(&mut socket, &SocketPayload::Snapshot { active })
        .await
        .is_err()
    {
        return;
    }
    loop {
        tokio::select! {
            event = events.recv() => {
                let event = match event {
                    Ok(event) => event,
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(_)) => break,
                };
                if send_socket_payload(&mut socket, &event).await.is_err() {
                    break;
                }
            }
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                    _ => {}
                }
            }
        }
    }
}

async fn send_socket_payload(
    socket: &mut WebSocket,
    payload: &SocketPayload,
) -> std::result::Result<(), ()> {
    let encoded = serde_json::to_string(payload).map_err(|_| ())?;
    socket
        .send(Message::Text(encoded.into()))
        .await
        .map_err(|_| ())
}

async fn security_headers(request: Request<Body>, next: Next) -> Response {
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(
            "default-src 'self'; connect-src 'self' ws: wss:; img-src 'self' data:; object-src 'none'; base-uri 'none'; frame-ancestors 'none'",
        ),
    );
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    headers.insert(
        "permissions-policy",
        HeaderValue::from_static("camera=(), microphone=(), geolocation=()"),
    );
    response
}

impl TryFrom<SignedChannelMessage> for HistoryMessage {
    type Error = ApiError;

    fn try_from(message: SignedChannelMessage) -> std::result::Result<Self, Self::Error> {
        let message_id = message.id().map_err(ApiError::internal)?;
        Ok(Self {
            message_id,
            writer: message.writer,
            sequence: message.sequence,
            timestamp_ms: message.timestamp_ms,
            text: message.text,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mp_core::ChannelInvite;
    use peeroxide::KeyPair;

    #[test]
    fn history_view_keeps_the_signed_entry_identity() {
        let invite = ChannelInvite::from_capability("web", [3u8; 32]).unwrap();
        let message = SignedChannelMessage::sign(
            &invite.id(),
            1,
            None,
            42,
            "hello web",
            &KeyPair::from_seed([4u8; 32]),
        )
        .unwrap();
        let expected_id = message.id().unwrap();
        let view = HistoryMessage::try_from(message).unwrap();
        assert_eq!(view.message_id, expected_id);
        assert_eq!(view.sequence, 1);
        assert_eq!(view.text, "hello web");
    }
}
