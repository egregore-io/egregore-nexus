//! Daemon-owned stream push socket for the packaged gateway.
//!
//! The socket is local-only and carries live `agent.update` frames after they have been appended
//! to `mem.stream_events`, plus ephemeral developer-event frames that are already authorized by
//! session subscription. The gateway still uses the stream store/materialized projection as the
//! authoritative reconnect path for conversation state; developer events are bounded to the
//! daemon boot epoch and never become durable rows.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use nexus_common::{now, NexusError};
use nexus_common::{GatewayProjectionBacklogConfig, GatewayProjectionDeliveryMode};

type Result<T> = std::result::Result<T, NexusError>;
use nexus_contracts::{
    AgentUpdateKind, DeveloperEventEnvelope, GatewayHookCapabilities, GatewayHookEvaluation,
    GatewayProjectionAck, GatewayProjectionEvent, HookEvaluationFailure, HookEvaluationResponse,
    SessionId,
};
use nexus_transcript::ToolCallObservation;

use crate::daemon::gateway_projection_backlog::{
    AppendProjection, GatewayProjectionBacklog, GatewayProjectionBacklogError,
    GatewayProjectionBacklogStats, GatewayProjectionGap, GatewayProjectionModeTransition,
};
use crate::daemon::services::fleet_events::{
    FleetEventService, FleetStatusObservation, FLEET_SESSION_KEY,
};
use crate::daemon::services::tool_call_events::ToolCallEventService;
use nexus_store::{repos::StreamEvents, Store};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
#[cfg(unix)]
use tokio::net::UnixListener;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use uuid::Uuid;

const PROTOCOL_VERSION: u32 = 1;
const MAX_FRAME_LEN: usize = 16 * 1024 * 1024;
const ENDPOINT_MANIFEST: &str = "gateway-stream-endpoint.json";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GatewayStreamLane {
    Agent,
    Raw,
    DeveloperEvent,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GatewayStreamSubscription {
    pub lane: GatewayStreamLane,
    pub session_id: String,
    pub after_id: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "camelCase")]
pub enum GatewayStreamClientFrame {
    #[serde(rename = "hello", rename_all = "camelCase")]
    Hello {
        version: u32,
        token: String,
        subscriptions: Vec<GatewayStreamSubscription>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hooks: Option<GatewayHookCapabilities>,
    },
    #[serde(rename = "subscribe", rename_all = "camelCase")]
    Subscribe {
        lane: GatewayStreamLane,
        session_id: String,
        after_id: i64,
    },
    #[serde(rename = "unsubscribe", rename_all = "camelCase")]
    Unsubscribe {
        lane: GatewayStreamLane,
        session_id: String,
    },
    #[serde(rename = "ping", rename_all = "camelCase")]
    Ping { id: Option<String> },
    /// Durable Gateway watermark after a projection transaction commits.
    #[serde(rename = "projection.ack", rename_all = "camelCase")]
    ProjectionAck { ack: GatewayProjectionAck },
    #[serde(rename = "hook.result", rename_all = "camelCase")]
    HookResult {
        correlation_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        result: Option<HookEvaluationResponse>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<HookEvaluationFailure>,
    },
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "camelCase")]
pub enum GatewayStreamFrame {
    #[serde(rename = "ready", rename_all = "camelCase")]
    Ready {
        version: u32,
        daemon_boot_id: String,
        resume: String,
    },
    #[serde(rename = "agent.update", rename_all = "camelCase")]
    AgentUpdate {
        session_id: String,
        stream_event_id: i64,
        kind: AgentUpdateKind,
        data: Value,
    },
    #[serde(rename = "raw", rename_all = "camelCase")]
    Raw {
        session_id: String,
        stream_raw_id: i64,
        chunk_base64: String,
        encoding: String,
    },
    #[serde(rename = "developer.event", rename_all = "camelCase")]
    DeveloperEvent {
        session_id: String,
        event: DeveloperEventEnvelope,
    },
    /// Canonical product fact for Gateway persistence. Agent-session and raw frames never use it.
    #[serde(rename = "projection", rename_all = "camelCase")]
    Projection { event: GatewayProjectionEvent },
    /// Explicit loss boundary when the volatile projection backlog overflows.
    #[serde(rename = "projection.gap", rename_all = "camelCase")]
    ProjectionGap { gap: GatewayProjectionGap },
    #[serde(rename = "hook.evaluate", rename_all = "camelCase")]
    HookEvaluate { evaluation: GatewayHookEvaluation },
    #[serde(rename = "gap", rename_all = "camelCase")]
    Gap {
        lane: GatewayStreamLane,
        session_id: String,
        after_id: i64,
        retry: String,
    },
    #[serde(rename = "pong", rename_all = "camelCase")]
    Pong { id: Option<String> },
    #[serde(rename = "error", rename_all = "camelCase")]
    Error { code: String, message: String },
}

#[derive(Clone)]
pub struct GatewayStreamPublisher {
    tx: broadcast::Sender<GatewayStreamFrame>,
    tool_call_events: ToolCallEventService,
    fleet_events: FleetEventService,
    projection_backlog: GatewayProjectionBacklog,
    hook_bridge: crate::daemon::gateway_hook_bridge::GatewayHookBridge,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GatewayStreamEndpoint {
    pub version: u32,
    pub path: PathBuf,
    pub token: String,
    pub daemon_boot_id: String,
    pub created_at: i64,
}

impl GatewayStreamPublisher {
    pub fn new(capacity: usize) -> Self {
        Self::new_with_caps_and_projection(
            capacity,
            crate::daemon::services::tool_call_events::DEFAULT_TOOL_CALL_EVENT_RING_CAP,
            crate::daemon::services::tool_call_events::DEFAULT_TOOL_CALL_START_CACHE_CAP,
            GatewayProjectionBacklog::new(GatewayProjectionBacklogConfig::default()),
        )
    }

    /// Production constructor pinned to the daemon boot epoch and resolved delivery policy.
    pub fn new_with_projection_config(
        capacity: usize,
        config: GatewayProjectionBacklogConfig,
        daemon_epoch: impl Into<String>,
    ) -> Self {
        Self::new_with_caps_and_projection(
            capacity,
            crate::daemon::services::tool_call_events::DEFAULT_TOOL_CALL_EVENT_RING_CAP,
            crate::daemon::services::tool_call_events::DEFAULT_TOOL_CALL_START_CACHE_CAP,
            GatewayProjectionBacklog::with_epoch(config, daemon_epoch),
        )
    }

    /// Build a publisher with explicit ephemeral tool-call event caps.
    ///
    /// Production uses [`GatewayStreamPublisher::new`]; tests and soak gates can lower these caps
    /// to prove dropped-cursor behavior without generating hundreds of events.
    pub fn new_with_tool_call_event_caps(
        capacity: usize,
        tool_call_ring_cap: usize,
        tool_call_start_cache_cap: usize,
    ) -> Self {
        Self::new_with_caps_and_projection(
            capacity,
            tool_call_ring_cap,
            tool_call_start_cache_cap,
            GatewayProjectionBacklog::new(GatewayProjectionBacklogConfig::default()),
        )
    }

    fn new_with_caps_and_projection(
        capacity: usize,
        tool_call_ring_cap: usize,
        tool_call_start_cache_cap: usize,
        projection_backlog: GatewayProjectionBacklog,
    ) -> Self {
        let (tx, _rx) = broadcast::channel(capacity);
        Self {
            tx,
            tool_call_events: ToolCallEventService::new(
                tool_call_ring_cap,
                tool_call_start_cache_cap,
            ),
            fleet_events: FleetEventService::default(),
            projection_backlog,
            hook_bridge: crate::daemon::gateway_hook_bridge::GatewayHookBridge::default(),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<GatewayStreamFrame> {
        self.tx.subscribe()
    }

    pub fn hook_bridge(&self) -> crate::daemon::gateway_hook_bridge::GatewayHookBridge {
        self.hook_bridge.clone()
    }

    /// Publish a canonical fact without ever blocking the transport path on Gateway availability.
    pub fn publish_projection(
        &self,
        event_id: impl Into<String>,
        kind: nexus_contracts::GatewayProjectionKind,
        occurred_at: i64,
        payload: Value,
    ) -> AppendProjection {
        let outcome = self
            .projection_backlog
            .append(event_id, kind, occurred_at, payload);
        match &outcome {
            AppendProjection::Buffered(event) => {
                // The frame is a wake signal. Buffered connections read ordered batches from the
                // backlog so a broadcast lag can never create a silent projection hole.
                let _ = self.tx.send(GatewayStreamFrame::Projection {
                    event: event.clone(),
                });
            }
            AppendProjection::LiveOnly(event) => {
                if self
                    .tx
                    .send(GatewayStreamFrame::Projection {
                        event: event.clone(),
                    })
                    .is_err()
                {
                    self.projection_backlog.note_live_drop();
                }
            }
            AppendProjection::Duplicate => {}
        }
        outcome
    }

    pub fn projection_stats(&self) -> GatewayProjectionBacklogStats {
        self.projection_backlog.stats()
    }

    pub fn set_projection_delivery_mode(
        &self,
        mode: GatewayProjectionDeliveryMode,
    ) -> GatewayProjectionModeTransition {
        self.projection_backlog.set_delivery_mode(mode)
    }

    fn projection_replay_batch(
        &self,
    ) -> crate::daemon::gateway_projection_backlog::GatewayProjectionReplay {
        self.projection_backlog.replay_batch()
    }

    fn ack_projection(
        &self,
        ack: &GatewayProjectionAck,
    ) -> std::result::Result<usize, GatewayProjectionBacklogError> {
        self.projection_backlog.ack(ack)
    }

    pub fn publish_agent_update(
        &self,
        session_id: &SessionId,
        stream_event_id: i64,
        kind: AgentUpdateKind,
        mut data: Value,
    ) {
        stamp_stream_event_id(&mut data, stream_event_id);
        let _ = self.tx.send(GatewayStreamFrame::AgentUpdate {
            session_id: session_id.0.clone(),
            stream_event_id,
            kind,
            data,
        });
    }

    /// Publish an ephemeral native tool-call observation to the gateway stream.
    ///
    /// This records only in the in-memory [`ToolCallEventService`] ring and broadcasts a
    /// `developer.event` frame to subscribed gateway connections. It deliberately avoids store
    /// writes, command intents, realtime bells, and turn injection.
    pub fn publish_tool_call_observation(
        &self,
        session_id: &SessionId,
        agent_name: &str,
        observation: ToolCallObservation,
    ) -> Option<DeveloperEventEnvelope> {
        let event = self
            .tool_call_events
            .publish_tool_call(session_id, agent_name, observation)?;
        let _ = self.tx.send(GatewayStreamFrame::DeveloperEvent {
            session_id: session_id.0.clone(),
            event: event.clone(),
        });
        Some(event)
    }

    fn tool_call_events(&self) -> ToolCallEventService {
        self.tool_call_events.clone()
    }

    /// Publish one fleet-wide agent status change as an ephemeral developer event on the
    /// `sys.fleet.status` topic (pseudo session [`FLEET_SESSION_KEY`]).
    ///
    /// Like tool-call events this records only in the in-memory ring and broadcasts a
    /// `developer.event` frame to subscribed gateway connections — no store writes, no bells,
    /// no turn injection. The durable `sys.agent.lifecycle` topic remains the record of truth.
    pub fn publish_fleet_status(
        &self,
        observation: FleetStatusObservation,
    ) -> DeveloperEventEnvelope {
        let event = self.fleet_events.publish(observation);
        let _ = self.tx.send(GatewayStreamFrame::DeveloperEvent {
            session_id: FLEET_SESSION_KEY.to_string(),
            event: event.clone(),
        });
        event
    }

    fn fleet_events(&self) -> FleetEventService {
        self.fleet_events.clone()
    }
}

pub struct GatewayStreamSocketHandle {
    endpoint: GatewayStreamEndpoint,
    task: JoinHandle<()>,
}

impl GatewayStreamSocketHandle {
    pub fn endpoint(&self) -> &GatewayStreamEndpoint {
        &self.endpoint
    }
}

impl Drop for GatewayStreamSocketHandle {
    fn drop(&mut self) {
        self.task.abort();
        cleanup_socket_path(&self.endpoint.path);
        remove_gateway_stream_endpoint_manifest();
    }
}

pub fn gateway_stream_endpoint_manifest_path() -> PathBuf {
    crate::daemon::lifecycle::nexus_home().join(ENDPOINT_MANIFEST)
}

pub fn read_gateway_stream_endpoint_manifest() -> Option<GatewayStreamEndpoint> {
    let raw = std::fs::read_to_string(gateway_stream_endpoint_manifest_path()).ok()?;
    serde_json::from_str(&raw).ok()
}

pub fn spawn_gateway_stream_socket(
    store: Arc<Store>,
    publisher: GatewayStreamPublisher,
    daemon_boot_id: String,
) -> std::io::Result<GatewayStreamSocketHandle> {
    let endpoint = GatewayStreamEndpoint {
        version: PROTOCOL_VERSION,
        path: gateway_stream_socket_path(&daemon_boot_id),
        token: Uuid::new_v4().to_string(),
        daemon_boot_id,
        created_at: now(),
    };
    cleanup_socket_path(&endpoint.path);
    let task = spawn_socket_listener(endpoint.clone(), store, publisher)?;
    write_gateway_stream_endpoint_manifest(&endpoint)?;
    Ok(GatewayStreamSocketHandle { endpoint, task })
}

pub async fn write_gateway_client_frame<W>(
    writer: &mut W,
    frame: &GatewayStreamClientFrame,
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_json_frame(writer, frame).await
}

pub async fn write_gateway_stream_frame<W>(
    writer: &mut W,
    frame: &GatewayStreamFrame,
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_json_frame(writer, frame).await
}

pub async fn read_gateway_client_frame<R>(
    reader: &mut R,
) -> std::io::Result<GatewayStreamClientFrame>
where
    R: AsyncRead + Unpin,
{
    read_json_frame(reader).await
}

pub async fn serve_gateway_stream_connection<S>(
    stream: S,
    store: Arc<Store>,
    publisher: GatewayStreamPublisher,
    token: String,
    daemon_boot_id: String,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (mut reader, mut writer) = tokio::io::split(stream);
    let (hello, hook_capabilities) = match read_gateway_client_frame(&mut reader).await {
        Ok(GatewayStreamClientFrame::Hello {
            version: _,
            token: received,
            subscriptions,
            hooks,
        }) => {
            if received != token {
                let _ = write_gateway_stream_frame(
                    &mut writer,
                    &GatewayStreamFrame::Error {
                        code: "unauthorized".to_string(),
                        message: "bad token".to_string(),
                    },
                )
                .await;
                return Ok(());
            }
            (subscriptions, hooks)
        }
        Ok(_) => {
            let _ = write_gateway_stream_frame(
                &mut writer,
                &GatewayStreamFrame::Error {
                    code: "protocol".to_string(),
                    message: "first frame must be hello".to_string(),
                },
            )
            .await;
            return Ok(());
        }
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
        Err(error) => {
            return Err(NexusError::Internal(format!(
                "gateway stream read hello: {error}"
            )))
        }
    };

    let (hook_provider, mut hook_requests) = hook_capabilities
        .map(|capabilities| publisher.hook_bridge.connect(capabilities))
        .map_or((None, None), |(provider, requests)| {
            (Some(provider), Some(requests))
        });
    let _hook_provider = hook_provider;

    write_gateway_stream_frame(
        &mut writer,
        &GatewayStreamFrame::Ready {
            version: PROTOCOL_VERSION,
            daemon_boot_id,
            resume: "store".to_string(),
        },
    )
    .await
    .map_err(|e| NexusError::Internal(format!("gateway stream write ready: {e}")))?;

    let mut subscriptions = SubscriptionSet::default();
    // Subscribe before reading replay so an event appended in the replay→tail handoff cannot lose
    // its wake signal. Buffered truth remains in the backlog either way.
    let mut live = publisher.subscribe();
    // Canonical projection is an implicit single-consumer lane. Replay precedes every ephemeral
    // subscription so durable facts cannot be overtaken by presentation frames after reconnect.
    let mut projection_batch_in_flight = send_projection_batch(&mut writer, &publisher).await?;
    for sub in hello {
        subscriptions.insert(sub.clone());
        let after_id = send_catchup(&mut writer, &store, &publisher, &sub).await?;
        subscriptions.set_after(sub.lane, &sub.session_id, after_id);
    }
    loop {
        tokio::select! {
            client = read_gateway_client_frame(&mut reader) => {
                match client {
                    Ok(GatewayStreamClientFrame::Subscribe { lane, session_id, after_id }) => {
                        let sub = GatewayStreamSubscription { lane, session_id, after_id };
                        subscriptions.insert(sub.clone());
                        let after_id = send_catchup(&mut writer, &store, &publisher, &sub).await?;
                        subscriptions.set_after(sub.lane, &sub.session_id, after_id);
                    }
                    Ok(GatewayStreamClientFrame::Unsubscribe { lane, session_id }) => {
                        subscriptions.remove(lane, &session_id);
                    }
                    Ok(GatewayStreamClientFrame::Ping { id }) => {
                        write_gateway_stream_frame(&mut writer, &GatewayStreamFrame::Pong { id })
                            .await
                            .map_err(|e| NexusError::Internal(format!("gateway stream write pong: {e}")))?;
                    }
                    Ok(GatewayStreamClientFrame::ProjectionAck { ack }) => {
                        match publisher.ack_projection(&ack) {
                            Ok(_) => {
                                // One replay window may contain many events. An ACK inside that
                                // window only trims its settled prefix; replaying immediately would
                                // resend the unacknowledged tail after every event and grow the
                                // Gateway queue quadratically. Release the next window only when
                                // the current window's terminal sequence is acknowledged.
                                if projection_batch_in_flight
                                    .is_some_and(|through_seq| ack.through_seq >= through_seq)
                                {
                                    projection_batch_in_flight =
                                        send_projection_batch(&mut writer, &publisher).await?;
                                }
                            }
                            Err(error) => {
                                write_gateway_stream_frame(
                                    &mut writer,
                                    &GatewayStreamFrame::Error {
                                        code: "projection_ack".to_string(),
                                        message: error.to_string(),
                                    },
                                )
                                .await
                                .map_err(|e| NexusError::Internal(format!("gateway stream write projection ACK error: {e}")))?;
                            }
                        }
                    }
                    Ok(GatewayStreamClientFrame::HookResult { correlation_id, result, error }) => {
                        let accepted = match (&_hook_provider, result, error) {
                            (Some(provider), Some(result), None) => {
                                provider.complete(correlation_id, Ok(result))
                            }
                            (Some(provider), None, Some(error)) => {
                                provider.complete(correlation_id, Err(error))
                            }
                            _ => false,
                        };
                        if !accepted {
                            write_gateway_stream_frame(
                                &mut writer,
                                &GatewayStreamFrame::Error {
                                    code: "hook_result".to_string(),
                                    message: "unknown or invalid hook correlation".to_string(),
                                },
                            )
                            .await
                            .map_err(|e| NexusError::Internal(format!("gateway stream write hook result error: {e}")))?;
                        }
                    }
                    Ok(GatewayStreamClientFrame::Hello { .. }) => {
                        write_gateway_stream_frame(
                            &mut writer,
                            &GatewayStreamFrame::Error {
                                code: "protocol".to_string(),
                                message: "hello already received".to_string(),
                            },
                        )
                        .await
                        .map_err(|e| NexusError::Internal(format!("gateway stream write error: {e}")))?;
                    }
                    Ok(GatewayStreamClientFrame::Unknown) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
                    Err(error) => return Err(NexusError::Internal(format!("gateway stream read frame: {error}"))),
                }
            }
            hook_request = async {
                hook_requests
                    .as_mut()
                    .expect("hook request branch requires a provider")
                    .recv()
                    .await
            }, if hook_requests.is_some() => {
                match hook_request {
                    Some(evaluation) => {
                        write_gateway_stream_frame(
                            &mut writer,
                            &GatewayStreamFrame::HookEvaluate { evaluation },
                        )
                        .await
                        .map_err(|e| NexusError::Internal(format!("gateway stream write hook evaluation: {e}")))?;
                    }
                    None => hook_requests = None,
                }
            }
            live_frame = live.recv() => {
                match live_frame {
                    Ok(frame) => {
                        if matches!(frame, GatewayStreamFrame::Projection { .. }) {
                            if publisher.projection_stats().delivery_mode
                                == GatewayProjectionDeliveryMode::BestEffort
                            {
                                write_gateway_stream_frame(&mut writer, &frame)
                                    .await
                                    .map_err(|e| NexusError::Internal(format!("gateway stream write best-effort projection: {e}")))?;
                            } else if projection_batch_in_flight.is_none() {
                                projection_batch_in_flight =
                                    send_projection_batch(&mut writer, &publisher).await?;
                            }
                        } else if subscriptions.matches(&frame) {
                            write_gateway_stream_frame(&mut writer, &frame)
                                .await
                                .map_err(|e| NexusError::Internal(format!("gateway stream write live frame: {e}")))?;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        for gap in subscriptions.gap_frames() {
                            write_gateway_stream_frame(&mut writer, &gap)
                                .await
                                .map_err(|e| NexusError::Internal(format!("gateway stream write gap: {e}")))?;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                }
            }
        }
    }
}

async fn send_projection_batch<W>(
    writer: &mut W,
    publisher: &GatewayStreamPublisher,
) -> Result<Option<i64>>
where
    W: AsyncWrite + Unpin,
{
    let replay = publisher.projection_replay_batch();
    let mut sent_through = None;
    if let Some(gap) = replay.gap {
        sent_through = Some(gap.through_seq);
        write_gateway_stream_frame(writer, &GatewayStreamFrame::ProjectionGap { gap })
            .await
            .map_err(|e| {
                NexusError::Internal(format!("gateway stream write projection gap: {e}"))
            })?;
    }
    for event in replay.events {
        sent_through = Some(event.seq);
        write_gateway_stream_frame(writer, &GatewayStreamFrame::Projection { event })
            .await
            .map_err(|e| NexusError::Internal(format!("gateway stream write projection: {e}")))?;
    }
    Ok(sent_through)
}

#[cfg(unix)]
fn spawn_socket_listener(
    endpoint: GatewayStreamEndpoint,
    store: Arc<Store>,
    publisher: GatewayStreamPublisher,
) -> std::io::Result<JoinHandle<()>> {
    let listener = UnixListener::bind(&endpoint.path)?;
    Ok(tokio::spawn(async move {
        loop {
            let Ok((stream, _addr)) = listener.accept().await else {
                break;
            };
            let store = store.clone();
            let publisher = publisher.clone();
            let token = endpoint.token.clone();
            let daemon_boot_id = endpoint.daemon_boot_id.clone();
            tokio::spawn(async move {
                let _ = serve_gateway_stream_connection(
                    stream,
                    store,
                    publisher,
                    token,
                    daemon_boot_id,
                )
                .await;
            });
        }
    }))
}

#[cfg(windows)]
fn spawn_socket_listener(
    endpoint: GatewayStreamEndpoint,
    store: Arc<Store>,
    publisher: GatewayStreamPublisher,
) -> std::io::Result<JoinHandle<()>> {
    use tokio::net::windows::named_pipe::{PipeMode, ServerOptions};

    let name = endpoint.path.to_string_lossy().into_owned();
    let first = ServerOptions::new()
        .pipe_mode(PipeMode::Byte)
        .first_pipe_instance(true)
        .create(&name)?;
    Ok(tokio::spawn(async move {
        let mut server = first;
        loop {
            if server.connect().await.is_err() {
                return;
            }
            let connected = server;
            let next = match ServerOptions::new().pipe_mode(PipeMode::Byte).create(&name) {
                Ok(next) => next,
                Err(_) => return,
            };
            let store = store.clone();
            let publisher = publisher.clone();
            let token = endpoint.token.clone();
            let daemon_boot_id = endpoint.daemon_boot_id.clone();
            tokio::spawn(async move {
                let _ = serve_gateway_stream_connection(
                    connected,
                    store,
                    publisher,
                    token,
                    daemon_boot_id,
                )
                .await;
            });
            server = next;
        }
    }))
}

fn write_gateway_stream_endpoint_manifest(endpoint: &GatewayStreamEndpoint) -> std::io::Result<()> {
    let path = gateway_stream_endpoint_manifest_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension(format!("json.tmp-{}", std::process::id()));
    let body = serde_json::to_string_pretty(endpoint)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    std::fs::write(&tmp, format!("{body}\n"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let _ = std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600));
    }
    // Unix rename replaces atomically. Windows does not replace an existing destination, so only
    // that platform needs the bounded remove-before-rename window.
    #[cfg(windows)]
    let _ = std::fs::remove_file(&path);
    std::fs::rename(tmp, path)
}

fn remove_gateway_stream_endpoint_manifest() {
    let _ = std::fs::remove_file(gateway_stream_endpoint_manifest_path());
}

fn gateway_stream_socket_path(daemon_boot_id: &str) -> PathBuf {
    #[cfg(windows)]
    {
        return PathBuf::from(format!(
            r"\\.\pipe\nexus-gateway-stream-{}",
            safe_socket_component(daemon_boot_id)
        ));
    }
    #[cfg(not(windows))]
    runtime_dir().join(format!(
        "nexus-gateway-stream-{}.sock",
        safe_socket_component(daemon_boot_id)
    ))
}

fn runtime_dir() -> PathBuf {
    std::env::var_os("XDG_RUNTIME_DIR")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
}

#[cfg(unix)]
fn cleanup_socket_path(path: &PathBuf) {
    let _ = std::fs::remove_file(path);
}

#[cfg(not(unix))]
fn cleanup_socket_path(_path: &PathBuf) {}

fn safe_socket_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| match ch {
            '/' | ':' | '.' | '\\' => '-',
            other => other,
        })
        .collect()
}

async fn write_json_frame<W, T>(writer: &mut W, value: &T) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let payload = serde_json::to_vec(value).map_err(invalid_data)?;
    if payload.len() > MAX_FRAME_LEN {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("gateway stream frame too large: {}", payload.len()),
        ));
    }
    writer.write_u32(payload.len() as u32).await?;
    writer.write_all(&payload).await?;
    writer.flush().await
}

async fn read_json_frame<R, T>(reader: &mut R) -> std::io::Result<T>
where
    R: AsyncRead + Unpin,
    T: for<'de> Deserialize<'de>,
{
    let len = reader.read_u32().await? as usize;
    if len > MAX_FRAME_LEN {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("gateway stream frame too large: {len}"),
        ));
    }
    let mut payload = vec![0_u8; len];
    reader.read_exact(&mut payload).await?;
    serde_json::from_slice(&payload).map_err(invalid_data)
}

fn invalid_data(error: serde_json::Error) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, error)
}

async fn send_catchup<W>(
    writer: &mut W,
    store: &Store,
    publisher: &GatewayStreamPublisher,
    sub: &GatewayStreamSubscription,
) -> Result<i64>
where
    W: AsyncWrite + Unpin,
{
    let mut last_id = sub.after_id;
    match sub.lane {
        GatewayStreamLane::Agent => {
            let rows = StreamEvents::new(store)
                .since(&SessionId(sub.session_id.clone()), sub.after_id)
                .await?;
            for row in rows {
                let kind = parse_agent_update_kind(&row.kind)?;
                let mut data =
                    serde_json::from_str::<Value>(&row.data).unwrap_or_else(|_| json!({}));
                stamp_stream_event_id(&mut data, row.id);
                last_id = row.id;
                write_gateway_stream_frame(
                    writer,
                    &GatewayStreamFrame::AgentUpdate {
                        session_id: row.session_id,
                        stream_event_id: row.id,
                        kind,
                        data,
                    },
                )
                .await
                .map_err(|e| NexusError::Internal(format!("gateway stream write catch-up: {e}")))?;
            }
        }
        GatewayStreamLane::Raw => {
            write_gateway_stream_frame(
                writer,
                &GatewayStreamFrame::Gap {
                    lane: GatewayStreamLane::Raw,
                    session_id: sub.session_id.clone(),
                    after_id: sub.after_id,
                    retry: "store".to_string(),
                },
            )
            .await
            .map_err(|e| NexusError::Internal(format!("gateway stream write raw gap: {e}")))?;
        }
        GatewayStreamLane::DeveloperEvent if sub.session_id == FLEET_SESSION_KEY => {
            let replay = publisher.fleet_events().replay(sub.after_id);
            if replay.gap.is_some() {
                write_gateway_stream_frame(
                    writer,
                    &GatewayStreamFrame::Gap {
                        lane: GatewayStreamLane::DeveloperEvent,
                        session_id: sub.session_id.clone(),
                        after_id: sub.after_id,
                        retry: "ephemeral".to_string(),
                    },
                )
                .await
                .map_err(|e| {
                    NexusError::Internal(format!("gateway stream write fleet gap: {e}"))
                })?;
            }
            for event in replay.events {
                write_gateway_stream_frame(
                    writer,
                    &GatewayStreamFrame::DeveloperEvent {
                        session_id: sub.session_id.clone(),
                        event,
                    },
                )
                .await
                .map_err(|e| {
                    NexusError::Internal(format!("gateway stream write fleet catch-up: {e}"))
                })?;
            }
            // The bounded, boot-scoped replay is not a complete-state contract. End every fresh
            // subscribe/reconnect catch-up with an ordered resync fact and reset the connection's
            // cursor to its seq even when the caller supplied an ahead-of-boot cursor.
            let resync = fleet_resync_event(replay.current_seq);
            last_id = resync.seq;
            write_gateway_stream_frame(
                writer,
                &GatewayStreamFrame::DeveloperEvent {
                    session_id: sub.session_id.clone(),
                    event: resync,
                },
            )
            .await
            .map_err(|e| NexusError::Internal(format!("gateway stream write fleet resync: {e}")))?;
        }
        GatewayStreamLane::DeveloperEvent => {
            let replay = publisher
                .tool_call_events()
                .subscribe_tool_calls(&SessionId(sub.session_id.clone()), sub.after_id)
                .replay;
            if replay.gap.is_some() {
                write_gateway_stream_frame(
                    writer,
                    &GatewayStreamFrame::Gap {
                        lane: GatewayStreamLane::DeveloperEvent,
                        session_id: sub.session_id.clone(),
                        after_id: sub.after_id,
                        retry: "ephemeral".to_string(),
                    },
                )
                .await
                .map_err(|e| {
                    NexusError::Internal(format!("gateway stream write developer-event gap: {e}"))
                })?;
            }
            for event in replay.events {
                last_id = event.seq;
                write_gateway_stream_frame(
                    writer,
                    &GatewayStreamFrame::DeveloperEvent {
                        session_id: sub.session_id.clone(),
                        event,
                    },
                )
                .await
                .map_err(|e| {
                    NexusError::Internal(format!(
                        "gateway stream write developer-event catch-up: {e}"
                    ))
                })?;
            }
        }
    }
    Ok(last_id)
}

/// Build a connection-local fleet reconciliation boundary at the replay's atomic current seq.
/// It is deliberately not inserted into the global ring or broadcast: one reconnect must never
/// force already-current subscribers to refresh, while the next global fact remains `seq + 1`.
fn fleet_resync_event(seq: i64) -> DeveloperEventEnvelope {
    DeveloperEventEnvelope {
        kind: nexus_contracts::DeveloperEventKind::AgentLifecycle,
        topic: crate::daemon::services::fleet_events::FLEET_STATUS_TOPIC.to_string(),
        seq,
        ts: now(),
        thread: None,
        dm: None,
        from: None,
        message_id: None,
        agent: None,
        session_id: Some(SessionId(FLEET_SESSION_KEY.to_string())),
        lifecycle: Some("resync".to_string()),
        current_work: None,
        data: Some(json!({
            "reason": "subscribe",
            "source": "members",
        })),
        tool: None,
        phase: None,
        ok: None,
    }
}

fn stamp_stream_event_id(data: &mut Value, stream_event_id: i64) {
    if let Value::Object(object) = data {
        object.insert("streamEventId".to_string(), json!(stream_event_id));
    }
}

fn parse_agent_update_kind(kind: &str) -> Result<AgentUpdateKind> {
    serde_json::from_value(json!(kind))
        .map_err(|e| NexusError::Internal(format!("unknown agent.update kind {kind:?}: {e}")))
}

#[derive(Default)]
struct SubscriptionSet {
    by_key: HashMap<(GatewayStreamLane, String), i64>,
}

impl SubscriptionSet {
    fn insert(&mut self, sub: GatewayStreamSubscription) {
        self.by_key.insert((sub.lane, sub.session_id), sub.after_id);
    }

    fn remove(&mut self, lane: GatewayStreamLane, session_id: &str) {
        self.by_key.remove(&(lane, session_id.to_string()));
    }

    fn set_after(&mut self, lane: GatewayStreamLane, session_id: &str, after_id: i64) {
        if let Some(current) = self.by_key.get_mut(&(lane, session_id.to_string())) {
            *current = after_id;
        }
    }

    fn matches(&mut self, frame: &GatewayStreamFrame) -> bool {
        match frame {
            GatewayStreamFrame::AgentUpdate {
                session_id,
                stream_event_id,
                ..
            } => self.advance_if_subscribed(GatewayStreamLane::Agent, session_id, *stream_event_id),
            GatewayStreamFrame::Raw {
                session_id,
                stream_raw_id,
                ..
            } => self.advance_if_subscribed(GatewayStreamLane::Raw, session_id, *stream_raw_id),
            GatewayStreamFrame::DeveloperEvent { session_id, event } => {
                self.advance_if_subscribed(GatewayStreamLane::DeveloperEvent, session_id, event.seq)
            }
            _ => false,
        }
    }

    fn advance_if_subscribed(
        &mut self,
        lane: GatewayStreamLane,
        session_id: &str,
        row_id: i64,
    ) -> bool {
        let Some(after_id) = self.by_key.get_mut(&(lane, session_id.to_string())) else {
            return false;
        };
        if row_id <= *after_id {
            return false;
        }
        *after_id = row_id;
        true
    }

    fn gap_frames(&self) -> Vec<GatewayStreamFrame> {
        self.by_key
            .iter()
            .map(|((lane, session_id), after_id)| GatewayStreamFrame::Gap {
                lane: *lane,
                session_id: session_id.clone(),
                after_id: *after_id,
                retry: if *lane == GatewayStreamLane::DeveloperEvent {
                    "ephemeral".to_string()
                } else {
                    "store".to_string()
                },
            })
            .collect()
    }
}
