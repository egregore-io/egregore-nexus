//! Daemon-owned OpenCode native session forwarder.
//!
//! Headed OpenCode records structured activity in its `opencode.db` `event` table. This module tails
//! that native store by `(aggregate_id, seq)` and emits Nexus `agent.update` rows through the shared
//! event sink. It is the OpenCode equivalent of the Claude native hook forwarder: the daemon owns
//! lifecycle and cursor persistence, while `nexus-agent` owns the pure OpenCode event translation.

use std::path::Path;
use std::sync::Arc;

use libsql::params;
use nexus_agent::adapter::opencode::native::{
    OpenCodeEventRow, OpenCodeForwardState, OpenCodeNativeAdapter,
};
use nexus_common::NexusError;
use nexus_contracts::ids::SessionId;
use nexus_contracts::ports::EventSink;
use nexus_contracts::WsEvent;
use nexus_dispatch::Bell;
use nexus_store::repos::AgentRuntimes;
use nexus_store::Store;
use nexus_transcript::ToolCallObservation;

use super::gateway_stream_socket::GatewayStreamPublisher;
use super::native_forwarder::{spawn_native_forwarder, NativeForwarderLoopConfig};

/// Default polling cadence for launch-local OpenCode `opencode.db` rows.
pub const DEFAULT_OPENCODE_NATIVE_FORWARDER_POLL_MS: u64 = 250;

pub use nexus_harness_opencode::storage::{
    OpenCodeRuntimeLaunch, OpenCodeRuntimeState, OpenCodeRuntimeStateRepo,
};

/// Counts returned from one forwarder pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OpenCodeForwarderStats {
    pub user_input_events: usize,
    pub text_events: usize,
    pub thinking_events: usize,
    pub tool_call_events: usize,
    pub turn_end_events: usize,
}

impl OpenCodeForwarderStats {
    /// Total emitted `agent.update` events for this pass.
    pub fn total_events(self) -> usize {
        self.user_input_events
            + self.text_events
            + self.thinking_events
            + self.tool_call_events
            + self.turn_end_events
    }
}

/// Observation-only sink for OpenCode native tool-call phases.
///
/// Implementations publish metadata side-channel events and must not insert durable rows, wake
/// realtime delivery, or inject turns.
pub trait OpenCodeToolObservationSink: Send + Sync {
    fn publish_tool_call(&self, session: &SessionId, observation: ToolCallObservation);
}

/// Gateway-backed, observation-only sink for OpenCode native tool-call phases.
pub(crate) struct GatewayOpenCodeToolObservationSink {
    publisher: GatewayStreamPublisher,
    agent_name: String,
}

impl GatewayOpenCodeToolObservationSink {
    pub(crate) fn new(publisher: GatewayStreamPublisher, agent_name: String) -> Self {
        Self {
            publisher,
            agent_name,
        }
    }
}

impl OpenCodeToolObservationSink for GatewayOpenCodeToolObservationSink {
    fn publish_tool_call(&self, session: &SessionId, observation: ToolCallObservation) {
        self.publisher
            .publish_tool_call_observation(session, &self.agent_name, observation);
    }
}

/// Forward newly appended OpenCode event rows into Nexus events once.
pub async fn forward_once(
    store: Arc<Store>,
    session: SessionId,
    events: Arc<dyn EventSink>,
) -> Result<OpenCodeForwarderStats, NexusError> {
    forward_once_with_tool_observations(store, session, events, None).await
}

/// Forward newly appended OpenCode event rows into Nexus events once, publishing optional
/// observation-only tool-call side-channel events alongside visible `agent.update` rows.
pub async fn forward_once_with_tool_observations(
    store: Arc<Store>,
    session: SessionId,
    events: Arc<dyn EventSink>,
    tool_observations: Option<Arc<dyn OpenCodeToolObservationSink>>,
) -> Result<OpenCodeForwarderStats, NexusError> {
    let repo = OpenCodeRuntimeStateRepo::new(&store);
    // A plugin-backed runtime owns its visible stream through the authenticated
    // plugin bridge. The DB observer still provides tool metadata and advances its
    // cursor, but must not republish the same text/input/tools or ring completion.
    // Legacy native-PTY forwarders retain their existing display authority.
    let plugin_display = AgentRuntimes::new(&store)
        .find_by_runtime_id(&session.0)
        .await?
        .is_some_and(|runtime| runtime.transport.as_deref() == Some("opencode-plugin"));
    let state = repo
        .find_by_runtime_id(&session)
        .await?
        .ok_or_else(|| NexusError::NotFound(format!("opencode_runtime_state:{}", session.0)))?;
    let opencode_session_id = match state.opencode_session_id.clone() {
        Some(id) => id,
        None => {
            let Some(session_id) =
                discover_latest_session_id(&state.opencode_db_path, state.launch_cwd.as_deref())
                    .await?
            else {
                return Ok(OpenCodeForwarderStats::default());
            };
            repo.set_opencode_session_id(&session, &session_id).await?;
            session_id
        }
    };
    let rows = read_event_rows(
        &state.opencode_db_path,
        &opencode_session_id,
        state.event_seq,
    )
    .await?;

    let mut translate_state = OpenCodeForwardState::from_persisted(
        Some(opencode_session_id),
        state.event_seq,
        Some(&state.parser_state_json),
    );
    let mut stats = OpenCodeForwarderStats::default();
    let adapter = OpenCodeNativeAdapter;
    for row in &rows {
        if let Some(sink) = tool_observations.as_deref() {
            for observation in adapter.tool_call_observations(row) {
                sink.publish_tool_call(&session, observation);
            }
        }
        for event in adapter.decode(row, &mut translate_state) {
            if plugin_display {
                continue;
            }
            record_stats(&mut stats, event.kind);
            events
                .emit(WsEvent::AgentUpdate {
                    session_id: session.clone(),
                    kind: event.kind,
                    data: event.data,
                })
                .await;
        }
    }

    if let Some(opencode_session_id) = translate_state.opencode_session_id.as_deref() {
        repo.set_opencode_session_id(&session, opencode_session_id)
            .await?;
    }
    repo.set_cursor(
        &session,
        translate_state.last_seq,
        &translate_state.to_persisted_json(),
    )
    .await?;
    Ok(stats)
}

/// Spawn a poll loop that forwards OpenCode native DB rows into `events`.
pub fn spawn_opencode_native_forwarder(
    store: Arc<Store>,
    session: SessionId,
    events: Arc<dyn EventSink>,
    bell: Bell,
    poll_ms: u64,
) -> tokio::task::JoinHandle<()> {
    spawn_opencode_native_forwarder_with_tool_events(store, session, events, bell, poll_ms, None)
}

/// Spawn an OpenCode forwarder with an optional ephemeral gateway developer-event publisher.
pub fn spawn_opencode_native_forwarder_with_tool_events(
    store: Arc<Store>,
    session: SessionId,
    events: Arc<dyn EventSink>,
    bell: Bell,
    poll_ms: u64,
    tool_events: Option<Arc<dyn OpenCodeToolObservationSink>>,
) -> tokio::task::JoinHandle<()> {
    let pass_session = session.clone();
    spawn_native_forwarder(
        NativeForwarderLoopConfig { session, poll_ms },
        move || {
            forward_once_with_tool_observations(
                store.clone(),
                pass_session.clone(),
                events.clone(),
                tool_events.clone(),
            )
        },
        move |session, stats| {
            if stats.turn_end_events > 0 {
                bell.ring(session);
            }
            if stats.total_events() > 0 {
                tracing::debug!(
                    target: "nexus::opencode_native_forwarder",
                    session = %session,
                    user_input_events = stats.user_input_events,
                    text_events = stats.text_events,
                    thinking_events = stats.thinking_events,
                    tool_call_events = stats.tool_call_events,
                    turn_end_events = stats.turn_end_events,
                    "forwarded OpenCode native event rows"
                );
            }
        },
        move |session, error| {
            tracing::warn!(
                target: "nexus::opencode_native_forwarder",
                session = %session,
                error = %error,
                "OpenCode native forward pass failed"
            );
        },
    )
}

async fn read_event_rows(
    db_path: &Path,
    opencode_session_id: &str,
    after_seq: i64,
) -> Result<Vec<OpenCodeEventRow>, NexusError> {
    if !db_path.exists() {
        return Ok(Vec::new());
    }
    let db = Store::open(&path_to_text(db_path)).await?;
    let rows = db
        .conn
        .query(
            "SELECT seq, type, data FROM event
             WHERE aggregate_id = ?1 AND seq > ?2
             ORDER BY seq ASC",
            params![opencode_session_id, after_seq.max(0)],
        )
        .await;
    let mut rows = match rows {
        Ok(rows) => rows,
        Err(error) if is_missing_table(&error) => return Ok(Vec::new()),
        Err(error) => return Err(store_err(error)),
    };
    let mut out = Vec::new();
    while let Some(row) = rows.next().await.map_err(store_err)? {
        let seq: i64 = row.get(0).map_err(store_err)?;
        let event_type: String = row.get(1).map_err(store_err)?;
        let data_raw: String = row.get(2).map_err(store_err)?;
        let data = serde_json::from_str(&data_raw)
            .map_err(|e| NexusError::Store(format!("invalid OpenCode event data: {e}")))?;
        out.push(OpenCodeEventRow {
            seq,
            event_type,
            data,
        });
    }
    Ok(out)
}

async fn discover_latest_session_id(
    db_path: &Path,
    launch_cwd: Option<&Path>,
) -> Result<Option<String>, NexusError> {
    let Some(launch_cwd) = launch_cwd else {
        return Ok(None);
    };
    if !db_path.exists() {
        return Ok(None);
    }
    let db = Store::open(&path_to_text(db_path)).await?;
    let rows = db
        .conn
        .query(
            "SELECT id FROM session
             WHERE directory = ?1
             ORDER BY time_updated DESC, time_created DESC
             LIMIT 1",
            params![path_to_text(launch_cwd)],
        )
        .await;
    let mut rows = match rows {
        Ok(rows) => rows,
        Err(error) if is_missing_table(&error) => return Ok(None),
        Err(error) => return Err(store_err(error)),
    };
    let Some(row) = rows.next().await.map_err(store_err)? else {
        return Ok(None);
    };
    row.get::<String>(0).map(Some).map_err(store_err)
}

fn record_stats(stats: &mut OpenCodeForwarderStats, kind: nexus_contracts::AgentUpdateKind) {
    match kind {
        nexus_contracts::AgentUpdateKind::UserInput => stats.user_input_events += 1,
        nexus_contracts::AgentUpdateKind::Text => stats.text_events += 1,
        nexus_contracts::AgentUpdateKind::Thinking => stats.thinking_events += 1,
        nexus_contracts::AgentUpdateKind::ToolCall => stats.tool_call_events += 1,
        nexus_contracts::AgentUpdateKind::TurnEnd => stats.turn_end_events += 1,
        nexus_contracts::AgentUpdateKind::Plan | nexus_contracts::AgentUpdateKind::Commands => {}
    }
}

fn path_to_text(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn store_err(e: libsql::Error) -> NexusError {
    NexusError::Store(e.to_string())
}

fn is_missing_table(error: &libsql::Error) -> bool {
    error.to_string().contains("no such table")
}
