//! Generic JSON-RPC 2.0 client over a local WebSocket transport.
//!
//! ## Wire framing
//! Confirmed from codex-rs/app-server-client/src/remote.rs:738-783
//! (`connect_unix_socket_endpoint`) and lib.rs:1474 test:
//!   - Connect a raw `UnixStream`.
//!   - Perform a WebSocket HTTP-upgrade handshake over that stream using the
//!     synthetic URI `"ws://localhost/rpc"` (bytes stay on the local machine).
//!   - Each JSON-RPC message is transmitted as **one WebSocket Text frame** —
//!     NOT newline-delimited JSON (NDJSON).
//!
//! This module is deliberately codex-AGNOSTIC: no method names appear here.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use futures::{Sink, SinkExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
#[cfg(unix)]
use tokio::net::UnixStream;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_tungstenite::client_async_with_config;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

// The dummy HTTP-upgrade URI used for WebSocket-over-unix-socket (same constant
// as codex-rs/app-server-client/src/remote.rs UDS_WEBSOCKET_HANDSHAKE_URL).
const UDS_WEBSOCKET_HANDSHAKE_URL: &str = "ws://localhost/rpc";

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Mirror of codex-rs `REMOTE_APP_SERVER_MAX_WEBSOCKET_MESSAGE_SIZE` (128 MiB).
const MAX_WS_MESSAGE_SIZE: usize = 128 << 20;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// An incoming server→client notification (or server→client request when
/// `id` is `Some`).
#[derive(Debug, Clone)]
pub struct Notification {
    pub method: String,
    pub params: Value,
    /// `Some(_)` means the server is making a request and expects a `respond`
    /// call with this id.
    pub id: Option<Value>,
}

/// Errors returned by [`JsonRpc`] operations.
#[derive(Debug)]
pub enum CodexRpcError {
    /// Failed to connect or upgrade to WebSocket.
    Connect(String),
    /// The connection was closed before the operation could complete.
    Closed,
    /// The server returned a JSON-RPC error object.
    Rpc {
        code: i64,
        message: String,
        data: Option<Value>,
    },
    /// A request timed out (30 s default).
    Timeout,
    /// A frame could not be decoded as JSON-RPC.
    Decode(String),
}

impl std::fmt::Display for CodexRpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connect(msg) => write!(f, "connect error: {msg}"),
            Self::Closed => write!(f, "connection closed"),
            Self::Rpc { code, message, .. } => write!(f, "RPC error {code}: {message}"),
            Self::Timeout => write!(f, "request timed out"),
            Self::Decode(msg) => write!(f, "decode error: {msg}"),
        }
    }
}

impl std::error::Error for CodexRpcError {}

// ---------------------------------------------------------------------------
// Internal wire shapes (minimal — we only need what's necessary for routing)
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct WireMessage {
    #[serde(default)]
    id: Option<Value>,
    method: Option<String>,
    result: Option<Value>,
    error: Option<WireError>,
    #[serde(default)]
    params: Value,
}

#[derive(Deserialize)]
struct WireError {
    code: i64,
    message: String,
    #[serde(default)]
    data: Option<Value>,
}

#[derive(Serialize)]
struct WireRequest<'a> {
    jsonrpc: &'a str,
    id: u64,
    method: &'a str,
    params: &'a Value,
}

#[derive(Serialize)]
struct WireNotify<'a> {
    jsonrpc: &'a str,
    method: &'a str,
    params: &'a Value,
}

#[derive(Serialize)]
struct WireResponse<'a> {
    jsonrpc: &'a str,
    id: &'a Value,
    result: &'a Value,
}

// ---------------------------------------------------------------------------
// JsonRpc — the public client handle
// ---------------------------------------------------------------------------

type PendingMap =
    Arc<std::sync::Mutex<HashMap<u64, oneshot::Sender<Result<Value, CodexRpcError>>>>>;
pub(super) type NativeIngress = Arc<dyn Fn(&Notification) + Send + Sync>;

type CloseObserver = Arc<dyn Fn() + Send + Sync>;

#[derive(Default)]
struct CloseObservation {
    closed: bool,
    observer: Option<CloseObserver>,
}

fn observe_closed(state: &std::sync::Mutex<CloseObservation>) {
    let observer = {
        let mut state = state.lock().unwrap();
        if state.closed {
            return;
        }
        state.closed = true;
        state.observer.clone()
    };
    if let Some(observer) = observer {
        observer();
    }
}

struct PendingRequest {
    pending: PendingMap,
    id: u64,
}

impl Drop for PendingRequest {
    fn drop(&mut self) {
        self.pending.lock().unwrap().remove(&self.id);
    }
}

trait WebSocketIo: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T> WebSocketIo for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

type BoxedWebSocketIo = Box<dyn WebSocketIo>;
type LocalWebSocket = WebSocketStream<BoxedWebSocketIo>;

/// A codex-agnostic JSON-RPC 2.0 client over a local WebSocket transport.
///
/// Internally the connection is split: the writer is behind a `Mutex` so all
/// public methods can write concurrently; a background reader task routes
/// `{id, result|error}` responses to per-id oneshot channels and forwards
/// everything else (server notifications / server→client requests) to the
/// `notifications` unbounded-receiver.
pub struct JsonRpc {
    writer: Arc<Mutex<futures::stream::SplitSink<LocalWebSocket, Message>>>,
    next_id: Arc<AtomicU64>,
    pending: PendingMap,
    /// Held so the channel stays open even when no receiver has been taken yet.
    _notif_tx: mpsc::UnboundedSender<Notification>,
    notif_rx: std::sync::Mutex<Option<mpsc::UnboundedReceiver<Notification>>>,
    ingress: Arc<std::sync::Mutex<Option<NativeIngress>>>,
    reader: tokio::task::AbortHandle,
    close_observation: Arc<std::sync::Mutex<CloseObservation>>,
}

impl Drop for JsonRpc {
    fn drop(&mut self) {
        observe_closed(&self.close_observation);
        self.reader.abort();
    }
}

impl JsonRpc {
    /// Connect to a local Codex app-server endpoint descriptor.
    ///
    /// Unix paths use WebSocket-over-UDS. `ws://` descriptors are accepted only
    /// for numeric loopback addresses, which is the transport Codex exposes on
    /// Windows where Unix-domain sockets are unavailable.
    pub async fn connect(endpoint: &Path) -> Result<Self, CodexRpcError> {
        Self::connect_with_ingress(endpoint, None).await
    }

    pub(super) async fn connect_with_ingress(
        endpoint: &Path,
        ingress: Option<NativeIngress>,
    ) -> Result<Self, CodexRpcError> {
        let descriptor = endpoint.to_str().ok_or_else(|| {
            CodexRpcError::Connect(format!("app-server endpoint is not UTF-8: {endpoint:?}"))
        })?;
        if descriptor.starts_with("ws://") {
            return Self::connect_websocket(descriptor, ingress).await;
        }

        #[cfg(unix)]
        {
            let stream = UnixStream::connect(endpoint)
                .await
                .map_err(|e| CodexRpcError::Connect(e.to_string()))?;
            Self::connect_stream(UDS_WEBSOCKET_HANDSHAKE_URL, Box::new(stream), ingress).await
        }
        #[cfg(not(unix))]
        {
            Err(CodexRpcError::Connect(format!(
                "unsupported app-server endpoint on this platform: {descriptor}"
            )))
        }
    }

    /// Connect to a codex app-server socket at `path`.
    ///
    /// Performs the WebSocket upgrade handshake (HTTP Upgrade over the raw unix
    /// stream, using the synthetic URI `"ws://localhost/rpc"`).
    #[cfg(unix)]
    pub async fn connect_unix(path: &Path) -> Result<Self, CodexRpcError> {
        let stream = UnixStream::connect(path)
            .await
            .map_err(|e| CodexRpcError::Connect(e.to_string()))?;

        Self::connect_stream(UDS_WEBSOCKET_HANDSHAKE_URL, Box::new(stream), None).await
    }

    async fn connect_websocket(
        endpoint: &str,
        ingress: Option<NativeIngress>,
    ) -> Result<Self, CodexRpcError> {
        let address = endpoint
            .strip_prefix("ws://")
            .and_then(|value| value.parse::<SocketAddr>().ok())
            .filter(|address| address.ip().is_loopback())
            .ok_or_else(|| {
                CodexRpcError::Connect(format!(
                    "Codex app-server WebSocket endpoint must be a numeric loopback address: {endpoint}"
                ))
            })?;
        let stream = TcpStream::connect(address)
            .await
            .map_err(|e| CodexRpcError::Connect(e.to_string()))?;

        Self::connect_stream(endpoint, Box::new(stream), ingress).await
    }

    async fn connect_stream(
        handshake_url: &str,
        stream: BoxedWebSocketIo,
        ingress: Option<NativeIngress>,
    ) -> Result<Self, CodexRpcError> {
        let ws_config = WebSocketConfig {
            max_frame_size: Some(MAX_WS_MESSAGE_SIZE),
            max_message_size: Some(MAX_WS_MESSAGE_SIZE),
            ..WebSocketConfig::default()
        };

        let (ws, _response) = client_async_with_config(handshake_url, stream, Some(ws_config))
            .await
            .map_err(|e| CodexRpcError::Connect(e.to_string()))?;

        let (sink, stream) = ws.split();
        let writer = Arc::new(Mutex::new(sink));
        let pending: PendingMap = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let ingress = Arc::new(std::sync::Mutex::new(ingress));
        let close_observation = Arc::new(std::sync::Mutex::new(CloseObservation::default()));
        let (notif_tx, notif_rx) = mpsc::unbounded_channel();

        // Spawn the reader task.
        let reader = {
            let pending = Arc::clone(&pending);
            let notif_tx = notif_tx.clone();
            let ingress = ingress.clone();
            let closed = close_observation.clone();
            tokio::spawn(async move {
                reader_task(stream, pending, notif_tx, ingress).await;
                observe_closed(&closed);
            })
            .abort_handle()
        };

        Ok(Self {
            writer,
            next_id: Arc::new(AtomicU64::new(1)),
            pending,
            _notif_tx: notif_tx,
            notif_rx: std::sync::Mutex::new(Some(notif_rx)),
            ingress,
            reader,
            close_observation,
        })
    }

    /// Send a request and await the correlated response (30 s timeout).
    pub async fn request(&self, method: &str, params: Value) -> Result<Value, CodexRpcError> {
        self.request_with_admission(method, params, |submit| submit())
            .await
    }

    pub(super) fn install_ingress(&self, ingress: NativeIngress) {
        let mut slot = self.ingress.lock().unwrap();
        if slot.is_none() {
            *slot = Some(ingress);
        }
    }

    /// Install the binding-local lifecycle observer, including an EOF that raced registration.
    pub(super) fn install_close_observer(&self, observer: CloseObserver) {
        let already_closed = {
            let mut state = self.close_observation.lock().unwrap();
            if state.observer.is_some() {
                return;
            }
            state.observer = Some(observer.clone());
            state.closed
        };
        if already_closed {
            observer();
        }
    }

    pub(super) async fn request_with_admission(
        &self,
        method: &str,
        params: Value,
        admit: impl FnOnce(&mut dyn FnMut() -> Result<(), CodexRpcError>) -> Result<(), CodexRpcError>,
    ) -> Result<Value, CodexRpcError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(id, tx);
        let _pending = PendingRequest {
            pending: self.pending.clone(),
            id,
        };

        let msg = serde_json::to_string(&WireRequest {
            jsonrpc: "2.0",
            id,
            method,
            params: &params,
        })
        .map_err(|e| CodexRpcError::Decode(e.to_string()))?;

        {
            let mut writer = self.writer.lock().await;
            futures::future::poll_fn(|cx| std::pin::Pin::new(&mut *writer).poll_ready(cx))
                .await
                .map_err(|_| CodexRpcError::Closed)?;
            let mut frame = Some(Message::Text(msg.into()));
            admit(&mut || {
                std::pin::Pin::new(&mut *writer)
                    .start_send(frame.take().expect("one local admission"))
                    .map_err(|_| CodexRpcError::Closed)
            })?;
            // Local admission cannot be retracted by cancellation. Keep writer ownership through
            // flush, but the binding exclusion ends before this awaited I/O or any receipt wait.
            writer.flush().await.map_err(|_| CodexRpcError::Closed)?;
        }

        tokio::time::timeout(REQUEST_TIMEOUT, rx)
            .await
            .map_err(|_| CodexRpcError::Timeout)?
            .map_err(|_| CodexRpcError::Closed)?
    }

    /// Send a notification (no id, no response expected).
    pub async fn notify(&self, method: &str, params: Value) -> Result<(), CodexRpcError> {
        let msg = serde_json::to_string(&WireNotify {
            jsonrpc: "2.0",
            method,
            params: &params,
        })
        .map_err(|e| CodexRpcError::Decode(e.to_string()))?;

        self.writer
            .lock()
            .await
            .send(Message::Text(msg.into()))
            .await
            .map_err(|_| CodexRpcError::Closed)
    }

    /// Send a response to a server→client request (identified by `id`).
    pub async fn respond(&self, id: Value, result: Value) -> Result<(), CodexRpcError> {
        let msg = serde_json::to_string(&WireResponse {
            jsonrpc: "2.0",
            id: &id,
            result: &result,
        })
        .map_err(|e| CodexRpcError::Decode(e.to_string()))?;

        self.writer
            .lock()
            .await
            .send(Message::Text(msg.into()))
            .await
            .map_err(|_| CodexRpcError::Closed)
    }

    /// Consume the `UnboundedReceiver` for incoming server notifications and
    /// server→client requests.
    ///
    /// May only be called once; panics on a second call.
    pub fn notifications(&self) -> mpsc::UnboundedReceiver<Notification> {
        self.notif_rx
            .lock()
            .unwrap()
            .take()
            .expect("notifications() called more than once")
    }
}

// ---------------------------------------------------------------------------
// Reader task
// ---------------------------------------------------------------------------

async fn reader_task(
    mut stream: futures::stream::SplitStream<LocalWebSocket>,
    pending: PendingMap,
    notif_tx: mpsc::UnboundedSender<Notification>,
    ingress: Arc<std::sync::Mutex<Option<NativeIngress>>>,
) {
    while let Some(msg_result) = stream.next().await {
        let text = match msg_result {
            Ok(Message::Text(t)) => t,
            Ok(Message::Close(_)) | Err(_) => break,
            Ok(_) => continue, // Ping/Pong/Binary/Frame — ignore
        };

        let wire: WireMessage = match serde_json::from_str(&text) {
            Ok(v) => v,
            Err(_) => continue, // malformed frame — skip
        };

        match (wire.id.as_ref(), wire.method.as_deref()) {
            // Response or error to a pending client request (has numeric id, no method).
            (Some(id_val), None) => {
                // Try to extract a u64 id for pending lookup.
                if let Some(id_u64) = id_val.as_u64() {
                    let mut map = pending.lock().unwrap();
                    if let Some(tx) = map.remove(&id_u64) {
                        let result = if let Some(err) = wire.error {
                            Err(CodexRpcError::Rpc {
                                code: err.code,
                                message: err.message,
                                data: err.data,
                            })
                        } else {
                            Ok(wire.result.unwrap_or(Value::Null))
                        };
                        let _ = tx.send(result);
                    }
                }
            }
            // Notification (no id) or server→client request (has id + method).
            (id_opt, Some(method)) => {
                let notif = Notification {
                    method: method.to_owned(),
                    params: wire.params,
                    id: id_opt.cloned(),
                };
                let ingest = ingress.lock().unwrap().clone();
                if let Some(ingest) = ingest {
                    ingest(&notif);
                }
                // If the receiver is gone, stop the task.
                if notif_tx.send(notif).is_err() {
                    break;
                }
            }
            // No id and no method — unrecognised, skip.
            (None, None) => {}
        }
    }

    // Connection closed: fail all pending requests.
    let mut map = pending.lock().unwrap();
    for (_, tx) in map.drain() {
        let _ = tx.send(Err(CodexRpcError::Closed));
    }
}

#[cfg(test)]
#[path = "../../tests/unit/app_server_jsonrpc.rs"]
mod cancellation_tests;
