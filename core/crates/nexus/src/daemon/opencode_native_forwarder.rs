//! Daemon-owned OpenCode native session forwarder.
//!
//! Headed OpenCode records structured activity in its `opencode.db` `event` table. This module tails
//! that native store by `(aggregate_id, seq)` and emits Nexus `agent.update` rows through the shared
//! event sink. It is the OpenCode equivalent of the Claude native hook forwarder: the daemon owns
//! lifecycle and cursor persistence, while `nexus-agent` owns the pure OpenCode event translation.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use libsql::params;
use nexus_agent::adapter::opencode::native::{
    OpenCodeEventRow, OpenCodeForwardState, OpenCodeNativeAdapter,
};
use nexus_common::{now, NexusError};
use nexus_contracts::ids::SessionId;
use nexus_contracts::ports::EventSink;
use nexus_contracts::WsEvent;
use nexus_dispatch::Bell;
use nexus_store::repos::{
    IdentitySessions, NativeThreadBindings, NewNativeThreadBinding, Sessions,
};
use nexus_store::Store;
use nexus_transcript::ToolCallObservation;

use super::gateway_stream_socket::GatewayStreamPublisher;
use super::native_forwarder::{spawn_native_forwarder, NativeForwarderLoopConfig};

/// Default polling cadence for launch-local OpenCode `opencode.db` rows.
pub const DEFAULT_OPENCODE_NATIVE_FORWARDER_POLL_MS: u64 = 250;

/// Runtime metadata written when a headed OpenCode runtime is launched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenCodeRuntimeLaunch {
    pub runtime_id: SessionId,
    pub opencode_db_path: PathBuf,
    pub opencode_session_id: Option<String>,
    pub launch_cwd: PathBuf,
    pub plugin_bridge_pid: Option<u32>,
    pub viewer_backend: String,
}

/// One row from `opencode_runtime_state`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenCodeRuntimeState {
    pub runtime_id: SessionId,
    pub opencode_db_path: PathBuf,
    pub opencode_session_id: Option<String>,
    pub launch_cwd: Option<PathBuf>,
    pub plugin_bridge_pid: Option<i64>,
    pub viewer_backend: String,
    pub event_seq: i64,
    pub parser_state_json: String,
    pub created_at: i64,
    pub updated_at: i64,
}

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

/// Repo for the OpenCode sidecar state table.
pub struct OpenCodeRuntimeStateRepo<'a> {
    store: &'a Store,
}

impl<'a> OpenCodeRuntimeStateRepo<'a> {
    /// Create a repo over the shared Nexus store.
    pub fn new(store: &'a Store) -> Self {
        Self { store }
    }

    /// Create the OpenCode-owned sidecar table if this store predates native OpenCode runtime state.
    ///
    /// The additive `parser_state_json` repair only ignores the idempotent duplicate-column case.
    /// Any other `ALTER TABLE` failure means the sidecar schema is unhealthy and must fail loudly
    /// before a forwarder starts dropping parser state.
    pub async fn ensure_schema(&self) -> Result<(), NexusError> {
        self.store
            .conn
            .execute(
                "CREATE TABLE IF NOT EXISTS opencode_runtime_state (
                    runtime_id TEXT PRIMARY KEY,
                    opencode_db_path TEXT NOT NULL,
                    opencode_session_id TEXT,
                    launch_cwd TEXT,
                    plugin_bridge_pid INTEGER,
                    viewer_backend TEXT NOT NULL DEFAULT 'tmux',
                    event_seq INTEGER NOT NULL DEFAULT 0,
                    parser_state_json TEXT NOT NULL DEFAULT '{}',
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                )",
                (),
            )
            .await
            .map_err(store_err)?;
        if let Err(error) = self
            .store
            .conn
            .execute(
                "ALTER TABLE opencode_runtime_state
                 ADD COLUMN parser_state_json TEXT NOT NULL DEFAULT '{}'",
                (),
            )
            .await
        {
            if !is_duplicate_column(&error) {
                return Err(store_err(error));
            }
        }
        if let Err(error) = self
            .store
            .conn
            .execute(
                "ALTER TABLE opencode_runtime_state
                 ADD COLUMN plugin_bridge_pid INTEGER",
                (),
            )
            .await
        {
            if !is_duplicate_column(&error) {
                return Err(store_err(error));
            }
        }
        if let Err(error) = self
            .store
            .conn
            .execute(
                "ALTER TABLE opencode_runtime_state
                 ADD COLUMN viewer_backend TEXT NOT NULL DEFAULT 'tmux'",
                (),
            )
            .await
        {
            if !is_duplicate_column(&error) {
                return Err(store_err(error));
            }
        }
        Ok(())
    }

    /// Insert or refresh launch metadata. Existing native session id and cursor survive re-launches.
    pub async fn upsert_launch(&self, launch: OpenCodeRuntimeLaunch) -> Result<(), NexusError> {
        self.ensure_schema().await?;
        if let Some(session_id) = launch.opencode_session_id.as_deref() {
            self.claim_native_thread_binding(&launch.runtime_id, session_id)
                .await?;
        }
        let ts = now();
        self.store
            .conn
            .execute(
                "INSERT INTO opencode_runtime_state (
                    runtime_id, opencode_db_path, opencode_session_id, launch_cwd,
                    plugin_bridge_pid, viewer_backend, event_seq, created_at, updated_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0, ?7, ?7)
                ON CONFLICT(runtime_id) DO UPDATE SET
                    opencode_db_path = excluded.opencode_db_path,
                    opencode_session_id = COALESCE(opencode_runtime_state.opencode_session_id, excluded.opencode_session_id),
                    launch_cwd = excluded.launch_cwd,
                    plugin_bridge_pid = excluded.plugin_bridge_pid,
                    viewer_backend = excluded.viewer_backend,
                    updated_at = excluded.updated_at",
                params![
                    launch.runtime_id.0,
                    path_to_text(&launch.opencode_db_path),
                    launch.opencode_session_id,
                    path_to_text(&launch.launch_cwd),
                    launch.plugin_bridge_pid.map(i64::from),
                    launch.viewer_backend,
                    ts,
                ],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Load OpenCode sidecar state for one Nexus runtime id.
    pub async fn find_by_runtime_id(
        &self,
        runtime_id: &SessionId,
    ) -> Result<Option<OpenCodeRuntimeState>, NexusError> {
        self.ensure_schema().await?;
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT runtime_id, opencode_db_path, opencode_session_id, launch_cwd,
                    plugin_bridge_pid, viewer_backend, event_seq, parser_state_json, created_at, updated_at
                 FROM opencode_runtime_state
                 WHERE runtime_id = ?1",
                params![runtime_id.0.clone()],
            )
            .await
            .map_err(store_err)?;
        let Some(row) = rows.next().await.map_err(store_err)? else {
            return Ok(None);
        };
        Ok(Some(row_to_state(&row)?))
    }

    /// Load OpenCode sidecar state by OpenCode's native `ses_*` session id.
    pub async fn find_by_opencode_session_id(
        &self,
        opencode_session_id: &str,
    ) -> Result<Option<OpenCodeRuntimeState>, NexusError> {
        self.ensure_schema().await?;
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT runtime_id, opencode_db_path, opencode_session_id, launch_cwd,
                    plugin_bridge_pid, viewer_backend, event_seq, parser_state_json, created_at, updated_at
                 FROM opencode_runtime_state
                 WHERE opencode_session_id = ?1",
                params![opencode_session_id],
            )
            .await
            .map_err(store_err)?;
        let Some(row) = rows.next().await.map_err(store_err)? else {
            return Ok(None);
        };
        Ok(Some(row_to_state(&row)?))
    }

    /// Persist the OpenCode plugin bridge process id reported by the launch shim.
    pub async fn set_plugin_bridge_pid(
        &self,
        runtime_id: &SessionId,
        plugin_bridge_pid: Option<u32>,
    ) -> Result<(), NexusError> {
        self.ensure_schema().await?;
        self.store
            .conn
            .execute(
                "UPDATE opencode_runtime_state
                 SET plugin_bridge_pid = ?2, updated_at = ?3
                 WHERE runtime_id = ?1",
                params![
                    runtime_id.0.clone(),
                    plugin_bridge_pid.map(i64::from),
                    now(),
                ],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Persist native OpenCode session discovery.
    pub async fn set_opencode_session_id(
        &self,
        runtime_id: &SessionId,
        opencode_session_id: &str,
    ) -> Result<(), NexusError> {
        self.ensure_schema().await?;
        self.claim_native_thread_binding(runtime_id, opencode_session_id)
            .await?;
        self.store
            .conn
            .execute(
                "UPDATE opencode_runtime_state
                 SET opencode_session_id = ?2, updated_at = ?3
                 WHERE runtime_id = ?1",
                params![runtime_id.0.clone(), opencode_session_id, now()],
            )
            .await
            .map_err(store_err)?;
        if self.store.has_split_authority() {
            IdentitySessions::new(self.store)
                .set_native_resume_key(&runtime_id.0, opencode_session_id)
                .await?;
        }
        Ok(())
    }

    async fn claim_native_thread_binding(
        &self,
        runtime_id: &SessionId,
        opencode_session_id: &str,
    ) -> Result<(), NexusError> {
        if opencode_session_id.is_empty() {
            return Ok(());
        }
        let Some(row) = Sessions::new(self.store)
            .find_by_session_id(runtime_id)
            .await?
        else {
            return Ok(());
        };
        let Some(agent_id) = row.agent_id else {
            return Ok(());
        };
        NativeThreadBindings::new(self.store)
            .claim(NewNativeThreadBinding {
                harness: "opencode".into(),
                native_thread_id: opencode_session_id.to_string(),
                agent_id,
                project: row.project,
                runtime_id: Some(runtime_id.0.clone()),
            })
            .await?;
        Ok(())
    }

    /// Delete the OpenCode sidecar state for a runtime id.
    pub async fn delete(&self, runtime_id: &SessionId) -> Result<(), NexusError> {
        self.ensure_schema().await?;
        self.store
            .conn
            .execute(
                "DELETE FROM opencode_runtime_state WHERE runtime_id = ?1",
                params![runtime_id.0.clone()],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Persist the last forwarded OpenCode event sequence and incremental parser state.
    pub async fn set_cursor(
        &self,
        runtime_id: &SessionId,
        event_seq: i64,
        parser_state_json: &str,
    ) -> Result<(), NexusError> {
        self.ensure_schema().await?;
        self.store
            .conn
            .execute(
                "UPDATE opencode_runtime_state
                 SET event_seq = CASE
                         WHEN ?2 > event_seq THEN ?2
                         ELSE event_seq
                     END,
                     parser_state_json = ?3,
                     updated_at = ?4
                 WHERE runtime_id = ?1",
                params![
                    runtime_id.0.clone(),
                    event_seq.max(0),
                    parser_state_json,
                    now()
                ],
            )
            .await
            .map_err(store_err)?;
        Ok(())
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

fn row_to_state(row: &libsql::Row) -> Result<OpenCodeRuntimeState, NexusError> {
    Ok(OpenCodeRuntimeState {
        runtime_id: SessionId(row.get::<String>(0).map_err(store_err)?),
        opencode_db_path: PathBuf::from(row.get::<String>(1).map_err(store_err)?),
        opencode_session_id: row.get::<Option<String>>(2).map_err(store_err)?,
        launch_cwd: row
            .get::<Option<String>>(3)
            .map_err(store_err)?
            .map(PathBuf::from),
        plugin_bridge_pid: row.get::<Option<i64>>(4).map_err(store_err)?,
        viewer_backend: row
            .get::<Option<String>>(5)
            .map_err(store_err)?
            .unwrap_or_else(|| "tmux".to_string()),
        event_seq: row.get::<Option<i64>>(6).map_err(store_err)?.unwrap_or(0),
        parser_state_json: row
            .get::<Option<String>>(7)
            .map_err(store_err)?
            .unwrap_or_else(|| "{}".to_string()),
        created_at: row.get::<Option<i64>>(8).map_err(store_err)?.unwrap_or(0),
        updated_at: row.get::<Option<i64>>(9).map_err(store_err)?.unwrap_or(0),
    })
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

fn is_duplicate_column(error: &libsql::Error) -> bool {
    error
        .to_string()
        .to_ascii_lowercase()
        .contains("duplicate column")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn opencode_schema_repair_ignores_duplicate_parser_state_column() {
        let store = Store::open(":memory:").await.unwrap();
        let repo = OpenCodeRuntimeStateRepo::new(&store);

        repo.ensure_schema().await.unwrap();
        repo.ensure_schema().await.unwrap();
    }

    #[tokio::test]
    async fn opencode_schema_repair_fails_loud_on_non_duplicate_alter_error() {
        let store = Store::open(":memory:").await.unwrap();
        store
            .conn
            .execute(
                "CREATE TABLE opencode_runtime_state (
                    runtime_id TEXT PRIMARY KEY,
                    parser_state_json TEXT NOT NULL DEFAULT '{}'
                )",
                (),
            )
            .await
            .unwrap();
        store
            .conn
            .execute("CREATE VIEW blocker AS SELECT 1", ())
            .await
            .unwrap();
        store
            .conn
            .execute("DROP TABLE opencode_runtime_state", ())
            .await
            .unwrap();
        store
            .conn
            .execute(
                "CREATE VIEW opencode_runtime_state AS SELECT 1 AS runtime_id",
                (),
            )
            .await
            .unwrap();

        let err = OpenCodeRuntimeStateRepo::new(&store)
            .ensure_schema()
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("view") || err.to_string().contains("not a table"),
            "unexpected error: {err}"
        );
    }
}
