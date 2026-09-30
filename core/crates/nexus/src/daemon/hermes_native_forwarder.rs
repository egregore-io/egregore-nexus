//! Daemon-owned Hermes native session forwarder.
//!
//! Headed Hermes records structured activity in `~/.hermes/state.db` `sessions` and `messages`
//! tables. This module tails that native store by `(session_id, messages.id)` and emits Nexus
//! `agent.update` rows through the shared event sink. It is the Hermes equivalent of the Claude and
//! OpenCode native forwarders: the daemon owns lifecycle and cursor persistence, while
//! `nexus-agent` owns pure row translation.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use libsql::params;
use nexus_agent::adapter::hermes::native::{
    HermesForwardState, HermesMessageRow, HermesNativeAdapter,
};
use nexus_common::{now, NexusError};
use nexus_contracts::ids::SessionId;
use nexus_contracts::ports::EventSink;
use nexus_contracts::WsEvent;
use nexus_dispatch::Bell;
use nexus_harness_hermes::{is_missing_column, run_child_pass, HARNESS_ID};
pub use nexus_harness_hermes::{HermesChildCursor, HermesChildStreamsRepo};
use nexus_store::repos::{
    IdentitySessions, NativeThreadBindings, NewNativeThreadBinding, Sessions,
};
use nexus_store::Store;
use nexus_transcript::ToolCallObservation;

use super::gateway_stream_socket::GatewayStreamPublisher;
use super::native_forwarder::{spawn_native_forwarder, NativeForwarderLoopConfig};

/// Default polling cadence for launch-local Hermes `state.db` rows.
pub const DEFAULT_HERMES_NATIVE_FORWARDER_POLL_MS: u64 = 250;

/// Runtime metadata written when a headed Hermes runtime is launched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HermesRuntimeLaunch {
    pub runtime_id: SessionId,
    pub hermes_db_path: PathBuf,
    pub hermes_session_id: Option<String>,
    pub launch_cwd: PathBuf,
    pub acp_child_pid: Option<u32>,
    /// Headed viewer mode selected at launch (`pty` or `tmux`).
    pub viewer_backend: String,
}

/// One row from `hermes_runtime_state`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HermesRuntimeState {
    pub runtime_id: SessionId,
    pub hermes_db_path: PathBuf,
    pub hermes_session_id: Option<String>,
    pub launch_cwd: Option<PathBuf>,
    pub acp_child_pid: Option<i64>,
    pub viewer_backend: String,
    pub message_id: i64,
    pub created_at: i64,
    pub updated_at: i64,
}

/// Counts returned from one forwarder pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HermesForwarderStats {
    pub user_input_events: usize,
    pub text_events: usize,
    pub thinking_events: usize,
    pub tool_call_events: usize,
    pub turn_end_events: usize,
    /// `child_agent.update` events emitted for descendant sessions. Never parent activity.
    pub child_events: usize,
    /// Child passes or child sessions that failed; the parent pass is unaffected.
    pub child_errors: usize,
    /// Discovery cuts that may hide unregistered descendants right now: a parent's child page
    /// that was full (resumed by keyset next pass), the per-pass registration cap, and known
    /// sessions at the depth limit whose children are never explored. Non-zero means this pass
    /// was not a complete enumeration.
    pub child_discovery_truncated: usize,
    /// Known sessions at some depth that were not explored this pass because more of them exist
    /// than the per-level frontier bound; they rotate in on later passes. Non-zero means
    /// exploration below them is spread over passes, not skipped.
    pub child_discovery_deferred: usize,
}

impl HermesForwarderStats {
    /// Total emitted `agent.update` events for this pass.
    pub fn total_events(self) -> usize {
        self.user_input_events
            + self.text_events
            + self.thinking_events
            + self.tool_call_events
            + self.turn_end_events
    }
}

/// Observation-only sink for Hermes native tool-call phases.
///
/// Implementations publish metadata side-channel events and must not insert durable rows, wake
/// realtime delivery, or inject turns.
pub trait HermesToolObservationSink: Send + Sync {
    fn publish_tool_call(&self, session: &SessionId, observation: ToolCallObservation);
}

/// Gateway-backed, observation-only sink for Hermes native tool-call phases.
pub(crate) struct GatewayHermesToolObservationSink {
    publisher: GatewayStreamPublisher,
    agent_name: String,
}

impl GatewayHermesToolObservationSink {
    pub(crate) fn new(publisher: GatewayStreamPublisher, agent_name: String) -> Self {
        Self {
            publisher,
            agent_name,
        }
    }
}

impl HermesToolObservationSink for GatewayHermesToolObservationSink {
    fn publish_tool_call(&self, session: &SessionId, observation: ToolCallObservation) {
        self.publisher
            .publish_tool_call_observation(session, &self.agent_name, observation);
    }
}

/// Repo for the Hermes sidecar state table.
pub struct HermesRuntimeStateRepo<'a> {
    store: &'a Store,
}

impl<'a> HermesRuntimeStateRepo<'a> {
    /// Create a repo over the shared Nexus store.
    pub fn new(store: &'a Store) -> Self {
        Self { store }
    }

    /// Create the Hermes-owned sidecar table if this store predates native Hermes runtime state.
    pub async fn ensure_schema(&self) -> Result<(), NexusError> {
        self.store
            .conn
            .execute(
                "CREATE TABLE IF NOT EXISTS hermes_runtime_state (
                    runtime_id TEXT PRIMARY KEY,
                    hermes_db_path TEXT NOT NULL,
                    hermes_session_id TEXT,
                    launch_cwd TEXT,
                    acp_child_pid INTEGER,
                    viewer_backend TEXT NOT NULL DEFAULT 'pty',
                    message_id INTEGER NOT NULL DEFAULT 0,
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
                "ALTER TABLE hermes_runtime_state
                 ADD COLUMN acp_child_pid INTEGER",
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
                "ALTER TABLE hermes_runtime_state
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
    pub async fn upsert_launch(&self, launch: HermesRuntimeLaunch) -> Result<(), NexusError> {
        self.ensure_schema().await?;
        if let Some(session_id) = launch.hermes_session_id.as_deref() {
            self.claim_native_thread_binding(&launch.runtime_id, session_id)
                .await?;
        }
        let ts = now();
        self.store
            .conn
            .execute(
                "INSERT INTO hermes_runtime_state (
                    runtime_id, hermes_db_path, hermes_session_id, launch_cwd,
                    acp_child_pid, viewer_backend, message_id, created_at, updated_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0, ?7, ?7)
                ON CONFLICT(runtime_id) DO UPDATE SET
                    hermes_db_path = excluded.hermes_db_path,
                    hermes_session_id = COALESCE(hermes_runtime_state.hermes_session_id, excluded.hermes_session_id),
                    launch_cwd = excluded.launch_cwd,
                    acp_child_pid = excluded.acp_child_pid,
                    viewer_backend = excluded.viewer_backend,
                    updated_at = excluded.updated_at",
                params![
                    launch.runtime_id.0,
                    path_to_text(&launch.hermes_db_path),
                    launch.hermes_session_id,
                    path_to_text(&launch.launch_cwd),
                    launch.acp_child_pid.map(i64::from),
                    launch.viewer_backend,
                    ts,
                ],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Load Hermes sidecar state for one Nexus runtime id.
    pub async fn find_by_runtime_id(
        &self,
        runtime_id: &SessionId,
    ) -> Result<Option<HermesRuntimeState>, NexusError> {
        self.ensure_schema().await?;
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT runtime_id, hermes_db_path, hermes_session_id, launch_cwd,
                    acp_child_pid, viewer_backend, message_id, created_at, updated_at
                 FROM hermes_runtime_state
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

    /// Persist the Hermes ACP child process id owned by the Hermes sidecar.
    pub async fn set_acp_child_pid(
        &self,
        runtime_id: &SessionId,
        acp_child_pid: Option<u32>,
    ) -> Result<(), NexusError> {
        self.ensure_schema().await?;
        self.store
            .conn
            .execute(
                "UPDATE hermes_runtime_state
                 SET acp_child_pid = ?2, updated_at = ?3
                 WHERE runtime_id = ?1",
                params![runtime_id.0.clone(), acp_child_pid.map(i64::from), now()],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Persist native Hermes session discovery.
    pub async fn set_hermes_session_id(
        &self,
        runtime_id: &SessionId,
        hermes_session_id: &str,
    ) -> Result<(), NexusError> {
        self.ensure_schema().await?;
        self.claim_native_thread_binding(runtime_id, hermes_session_id)
            .await?;
        self.store
            .conn
            .execute(
                "UPDATE hermes_runtime_state
                 SET hermes_session_id = ?2, updated_at = ?3
                 WHERE runtime_id = ?1",
                params![runtime_id.0.clone(), hermes_session_id, now()],
            )
            .await
            .map_err(store_err)?;
        if self.store.has_split_authority() {
            IdentitySessions::new(self.store)
                .set_native_resume_key(&runtime_id.0, hermes_session_id)
                .await?;
        }
        Ok(())
    }

    async fn claim_native_thread_binding(
        &self,
        runtime_id: &SessionId,
        hermes_session_id: &str,
    ) -> Result<(), NexusError> {
        if hermes_session_id.is_empty() {
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
                provider: HARNESS_ID.into(),
                kind: "harness".into(),
                native_thread_id: hermes_session_id.to_string(),
                agent_id,
                project: row.project,
                runtime_id: Some(runtime_id.0.clone()),
            })
            .await?;
        Ok(())
    }

    /// Persist the last forwarded Hermes message row id.
    pub async fn set_cursor(
        &self,
        runtime_id: &SessionId,
        message_id: i64,
    ) -> Result<(), NexusError> {
        self.ensure_schema().await?;
        self.store
            .conn
            .execute(
                "UPDATE hermes_runtime_state
                 SET message_id = CASE
                         WHEN ?2 > message_id THEN ?2
                         ELSE message_id
                     END,
                     updated_at = ?3
                 WHERE runtime_id = ?1",
                params![runtime_id.0.clone(), message_id.max(0), now()],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Delete the Hermes sidecar state for a runtime id.
    pub async fn delete(&self, runtime_id: &SessionId) -> Result<(), NexusError> {
        self.ensure_schema().await?;
        self.store
            .conn
            .execute(
                "DELETE FROM hermes_runtime_state WHERE runtime_id = ?1",
                params![runtime_id.0.clone()],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }
}

/// Forward newly appended Hermes message rows into Nexus events once.
pub async fn forward_once(
    store: Arc<Store>,
    session: SessionId,
    events: Arc<dyn EventSink>,
) -> Result<HermesForwarderStats, NexusError> {
    forward_once_with_tool_observations(store, session, events, None).await
}

/// Forward newly appended Hermes message rows into Nexus events once, publishing optional
/// observation-only tool-call side-channel events alongside visible `agent.update` rows.
pub async fn forward_once_with_tool_observations(
    store: Arc<Store>,
    session: SessionId,
    events: Arc<dyn EventSink>,
    tool_observations: Option<Arc<dyn HermesToolObservationSink>>,
) -> Result<HermesForwarderStats, NexusError> {
    let repo = HermesRuntimeStateRepo::new(&store);
    let state = repo
        .find_by_runtime_id(&session)
        .await?
        .ok_or_else(|| NexusError::NotFound(format!("hermes_runtime_state:{}", session.0)))?;
    let hermes_session_id = match state.hermes_session_id.clone() {
        Some(id) => id,
        None => {
            let Some(session_id) =
                discover_latest_session_id(&state.hermes_db_path, state.launch_cwd.as_deref())
                    .await?
            else {
                return Ok(HermesForwarderStats::default());
            };
            repo.set_hermes_session_id(&session, &session_id).await?;
            session_id
        }
    };
    let rows =
        read_message_rows(&state.hermes_db_path, &hermes_session_id, state.message_id).await?;

    let mut translate_state =
        HermesForwardState::from_cursor(Some(hermes_session_id.clone()), state.message_id);
    let mut stats = HermesForwarderStats::default();
    let adapter = HermesNativeAdapter;
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

    if let Some(hermes_session_id) = translate_state.hermes_session_id.as_deref() {
        repo.set_hermes_session_id(&session, hermes_session_id)
            .await?;
    }
    repo.set_cursor(&session, translate_state.last_message_id)
        .await?;

    // The child pass runs after the parent's cursor is committed, inside the harness crate,
    // and never fails the parent's pass: the daemon only merges its generic statistics.
    let child = run_child_pass(
        &store,
        &session,
        events.as_ref(),
        &state.hermes_db_path,
        &hermes_session_id,
    )
    .await;
    stats.child_events += child.child_events;
    stats.child_errors += child.child_errors;
    stats.child_discovery_truncated += child.child_discovery_truncated;
    stats.child_discovery_deferred += child.child_discovery_deferred;
    Ok(stats)
}

pub fn spawn_hermes_native_forwarder(
    store: Arc<Store>,
    session: SessionId,
    events: Arc<dyn EventSink>,
    bell: Bell,
    poll_ms: u64,
) -> tokio::task::JoinHandle<()> {
    spawn_hermes_native_forwarder_with_tool_events(store, session, events, bell, poll_ms, None)
}

/// Spawn a Hermes forwarder with an optional ephemeral gateway developer-event publisher.
pub fn spawn_hermes_native_forwarder_with_tool_events(
    store: Arc<Store>,
    session: SessionId,
    events: Arc<dyn EventSink>,
    bell: Bell,
    poll_ms: u64,
    tool_events: Option<Arc<dyn HermesToolObservationSink>>,
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
                    target: "nexus::hermes_native_forwarder",
                    session = %session,
                    user_input_events = stats.user_input_events,
                    text_events = stats.text_events,
                    thinking_events = stats.thinking_events,
                    tool_call_events = stats.tool_call_events,
                    turn_end_events = stats.turn_end_events,
                    "forwarded Hermes native message rows"
                );
            }
        },
        move |session, error| {
            tracing::warn!(
                target: "nexus::hermes_native_forwarder",
                session = %session,
                error = %error,
                "Hermes native forward pass failed"
            );
        },
    )
}

async fn read_message_rows(
    db_path: &Path,
    hermes_session_id: &str,
    after_message_id: i64,
) -> Result<Vec<HermesMessageRow>, NexusError> {
    if !db_path.exists() {
        return Ok(Vec::new());
    }
    let db = Store::open(&path_to_text(db_path)).await?;
    let rows = db
        .conn
        .query(
            "SELECT id, session_id, role, content, tool_call_id, tool_calls,
                    tool_name, timestamp, finish_reason, active
             FROM messages
             WHERE session_id = ?1 AND id > ?2 AND active = 1
             ORDER BY id ASC",
            params![hermes_session_id, after_message_id.max(0)],
        )
        .await;
    let mut rows = match rows {
        Ok(rows) => rows,
        Err(error) if is_missing_table(&error) => return Ok(Vec::new()),
        Err(error) => return Err(store_err(error)),
    };
    let mut out = Vec::new();
    while let Some(row) = rows.next().await.map_err(store_err)? {
        out.push(message_row(&row)?);
    }
    Ok(out)
}

fn message_row(row: &libsql::Row) -> Result<HermesMessageRow, NexusError> {
    let tool_calls_raw: Option<String> = row.get(5).map_err(store_err)?;
    let tool_calls = tool_calls_raw
        .as_deref()
        .filter(|raw| !raw.trim().is_empty())
        .map(|raw| {
            serde_json::from_str(raw)
                .map_err(|e| NexusError::Store(format!("invalid Hermes tool_calls JSON: {e}")))
        })
        .transpose()?;
    Ok(HermesMessageRow {
        id: row.get(0).map_err(store_err)?,
        session_id: row.get(1).map_err(store_err)?,
        role: row.get(2).map_err(store_err)?,
        content: row.get(3).map_err(store_err)?,
        tool_call_id: row.get(4).map_err(store_err)?,
        tool_calls,
        tool_name: row.get(6).map_err(store_err)?,
        timestamp: row.get(7).map_err(store_err)?,
        finish_reason: row.get(8).map_err(store_err)?,
        active: row.get::<Option<i64>>(9).map_err(store_err)?.unwrap_or(1) != 0,
    })
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
    let cwd = path_to_text(launch_cwd);
    let db = Store::open(&path_to_text(db_path)).await?;
    // Only a root session may be bound as the runtime's root: a child session
    // (parent_session_id set) sharing the cwd is never the owner. Stores that predate the
    // column fall back to the unfiltered query.
    let rows = db
        .conn
        .query(
            "SELECT id FROM sessions
             WHERE (cwd = ?1
                OR json_extract(COALESCE(model_config, '{}'), '$.cwd') = ?1)
               AND parent_session_id IS NULL
             ORDER BY COALESCE(ended_at, started_at) DESC, started_at DESC
             LIMIT 1",
            params![cwd.clone()],
        )
        .await;
    let rows = match rows {
        Err(error) if is_missing_column(&error) => {
            db.conn
                .query(
                    "SELECT id FROM sessions
                     WHERE cwd = ?1
                        OR json_extract(COALESCE(model_config, '{}'), '$.cwd') = ?1
                     ORDER BY COALESCE(ended_at, started_at) DESC, started_at DESC
                     LIMIT 1",
                    params![cwd],
                )
                .await
        }
        other => other,
    };
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

fn record_stats(stats: &mut HermesForwarderStats, kind: nexus_contracts::AgentUpdateKind) {
    match kind {
        nexus_contracts::AgentUpdateKind::UserInput => stats.user_input_events += 1,
        nexus_contracts::AgentUpdateKind::Text => stats.text_events += 1,
        nexus_contracts::AgentUpdateKind::Thinking => stats.thinking_events += 1,
        nexus_contracts::AgentUpdateKind::ToolCall => stats.tool_call_events += 1,
        nexus_contracts::AgentUpdateKind::TurnEnd => stats.turn_end_events += 1,
        nexus_contracts::AgentUpdateKind::Plan | nexus_contracts::AgentUpdateKind::Commands => {}
    }
}

fn row_to_state(row: &libsql::Row) -> Result<HermesRuntimeState, NexusError> {
    Ok(HermesRuntimeState {
        runtime_id: SessionId(row.get::<String>(0).map_err(store_err)?),
        hermes_db_path: PathBuf::from(row.get::<String>(1).map_err(store_err)?),
        hermes_session_id: row.get::<Option<String>>(2).map_err(store_err)?,
        launch_cwd: row
            .get::<Option<String>>(3)
            .map_err(store_err)?
            .map(PathBuf::from),
        acp_child_pid: row.get::<Option<i64>>(4).map_err(store_err)?,
        viewer_backend: row
            .get::<Option<String>>(5)
            .map_err(store_err)?
            .unwrap_or_else(|| "tmux".to_string()),
        message_id: row.get::<Option<i64>>(6).map_err(store_err)?.unwrap_or(0),
        created_at: row.get::<Option<i64>>(7).map_err(store_err)?.unwrap_or(0),
        updated_at: row.get::<Option<i64>>(8).map_err(store_err)?.unwrap_or(0),
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
