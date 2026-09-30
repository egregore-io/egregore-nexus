//! Claude-owned runtime state persisted in the shared Nexus store.
//!
//! Generic identity and active runtime rows live in `nexus-store`'s shared tables. This module
//! owns the Claude-specific sidecar state needed to revive a headed native bridge without
//! inferring bridge, transcript, tmux, or cursor paths from process-local memory.
//! Cross-pass streamed-vs-final producer suppression is intentionally shared in
//! `nexus-store.producer_identities`, not stored in Claude-specific sidecar columns.

use std::path::{Path, PathBuf};

use libsql::params;
use nexus_common::{now, NexusError};
use nexus_contracts::SessionId;
use nexus_store::repos::IdentitySessions;
use nexus_store::Store;

/// Runtime metadata written when a headed Claude native bridge is launched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeRuntimeLaunch {
    pub runtime_id: SessionId,
    pub bridge_dir: PathBuf,
    pub claude_session_id: Option<String>,
    pub launch_cwd: PathBuf,
    pub transcript_path: Option<PathBuf>,
    pub bridge_pid: Option<u32>,
    pub hook_pids_json: Option<String>,
}

/// One row from `claude_runtime_state`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeRuntimeState {
    pub runtime_id: SessionId,
    pub bridge_dir: Option<PathBuf>,
    pub claude_session_id: Option<String>,
    pub transcript_path: Option<PathBuf>,
    pub launch_cwd: Option<PathBuf>,
    pub tmux_socket: Option<String>,
    pub tmux_session: Option<String>,
    pub bridge_pid: Option<i64>,
    pub hook_pids_json: Option<String>,
    pub hook_cursor: i64,
    pub transcript_cursor: i64,
    pub message_delta_cursor: i64,
    pub created_at: i64,
    pub updated_at: i64,
}

/// Revive inputs derived from Claude-specific runtime state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeRuntimeReviveSeed {
    pub state_dir: PathBuf,
    pub bridge_dir: PathBuf,
    pub launch_cwd: PathBuf,
    pub transcript_path: PathBuf,
    pub claude_session_id: Option<String>,
}

/// Cursor column selector for generic Claude cursor updates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaudeRuntimeCursor {
    Hook,
    Transcript,
    MessageDelta,
}

/// Repo for the Claude sidecar state table.
pub struct ClaudeRuntimeStateRepo<'a> {
    store: &'a Store,
}

impl<'a> ClaudeRuntimeStateRepo<'a> {
    /// Create a repo over the shared Nexus store.
    pub fn new(store: &'a Store) -> Self {
        Self { store }
    }

    /// Create the Claude-owned sidecar table if this store predates native Claude runtime state.
    pub async fn ensure_schema(&self) -> Result<(), NexusError> {
        self.store
            .conn
            .execute(
                "CREATE TABLE IF NOT EXISTS claude_runtime_state (
                    runtime_id TEXT PRIMARY KEY,
                    bridge_dir TEXT,
                    claude_session_id TEXT,
                    transcript_path TEXT,
                    launch_cwd TEXT,
                    tmux_socket TEXT,
                    tmux_session TEXT,
                    bridge_pid INTEGER,
                    hook_pids_json TEXT,
                    hook_cursor INTEGER NOT NULL DEFAULT 0,
                    transcript_cursor INTEGER NOT NULL DEFAULT 0,
                    message_delta_cursor INTEGER NOT NULL DEFAULT 0,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                )",
                (),
            )
            .await
            .map_err(store_err)?;
        self.ensure_column("claude_runtime_state", "bridge_pid", "INTEGER")
            .await?;
        self.ensure_column("claude_runtime_state", "hook_pids_json", "TEXT")
            .await?;
        Ok(())
    }

    /// Insert or refresh launch metadata. Existing tmux and cursor fields survive so a re-launch
    /// does not erase revive progress. When a launch already has an exact native `--resume` id,
    /// persist it immediately as that Nexus runtime's opaque provider correlation hint.
    pub async fn upsert_launch(&self, launch: ClaudeRuntimeLaunch) -> Result<(), NexusError> {
        self.ensure_schema().await?;
        let ts = now();
        self.store
            .conn
            .execute(
                "INSERT INTO claude_runtime_state (
                    runtime_id, bridge_dir, claude_session_id, transcript_path, launch_cwd,
                    tmux_socket, tmux_session, bridge_pid, hook_pids_json, hook_cursor,
                    transcript_cursor, message_delta_cursor, created_at, updated_at
                ) VALUES (?1, ?2, NULL, ?3, ?4, NULL, NULL, ?5, ?6, 0, 0, 0, ?7, ?7)
                ON CONFLICT(runtime_id) DO UPDATE SET
                    bridge_dir = excluded.bridge_dir,
                    transcript_path = COALESCE(excluded.transcript_path, transcript_path),
                    launch_cwd = excluded.launch_cwd,
                    bridge_pid = excluded.bridge_pid,
                    hook_pids_json = excluded.hook_pids_json,
                    updated_at = excluded.updated_at",
                params![
                    launch.runtime_id.0.clone(),
                    path_to_text(&launch.bridge_dir),
                    launch.transcript_path.as_deref().map(path_to_text),
                    path_to_text(&launch.launch_cwd),
                    launch.bridge_pid.map(i64::from),
                    launch.hook_pids_json,
                    ts,
                ],
            )
            .await
            .map_err(store_err)?;
        if let Some(claude_session_id) = launch.claude_session_id.as_deref() {
            self.set_claude_session(
                &launch.runtime_id,
                claude_session_id,
                launch.transcript_path.clone(),
            )
            .await?;
        }
        Ok(())
    }

    /// Persist Claude-native process details owned by the Claude sidecar.
    pub async fn set_process_detail(
        &self,
        runtime_id: &SessionId,
        bridge_pid: Option<u32>,
        hook_pids_json: Option<&str>,
    ) -> Result<(), NexusError> {
        self.ensure_schema().await?;
        self.store
            .conn
            .execute(
                "UPDATE claude_runtime_state
                 SET bridge_pid = COALESCE(?2, bridge_pid),
                     hook_pids_json = COALESCE(?3, hook_pids_json),
                     updated_at = ?4
                 WHERE runtime_id = ?1",
                params![
                    runtime_id.0.clone(),
                    bridge_pid.map(i64::from),
                    hook_pids_json,
                    now(),
                ],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Persist the Claude session id discovered from hook or transcript records.
    pub async fn set_claude_session_id(
        &self,
        runtime_id: &SessionId,
        claude_session_id: &str,
    ) -> Result<(), NexusError> {
        self.set_claude_session(runtime_id, claude_session_id, None)
            .await
    }

    /// Persist the Claude session id and, when known, its transcript path.
    ///
    /// The value is an opaque, best-effort resume hint owned by Claude Code. Nexus deliberately
    /// does not validate it, claim it as identity, or require it to be unique across Nexus
    /// runtimes. Claude may resume a different native conversation than an operator expects; that
    /// provider behavior is documented as a caveat rather than hidden behind false ownership.
    pub async fn set_claude_session(
        &self,
        runtime_id: &SessionId,
        claude_session_id: &str,
        transcript_path: Option<PathBuf>,
    ) -> Result<(), NexusError> {
        self.ensure_schema().await?;
        self.store
            .conn
            .execute(
                "UPDATE claude_runtime_state
                 SET claude_session_id = ?2,
                     transcript_path = COALESCE(?3, transcript_path),
                     updated_at = ?4
                 WHERE runtime_id = ?1",
                params![
                    runtime_id.0.clone(),
                    claude_session_id,
                    transcript_path.as_deref().map(path_to_text),
                    now(),
                ],
            )
            .await
            .map_err(store_err)?;
        if self.store.has_split_authority() {
            IdentitySessions::new(self.store)
                .set_native_resume_key(&runtime_id.0, claude_session_id)
                .await?;
        }
        Ok(())
    }

    /// Alias for callers that treat the Claude id as the native bridge session id.
    pub async fn set_session(
        &self,
        runtime_id: &SessionId,
        claude_session_id: &str,
        transcript_path: Option<PathBuf>,
    ) -> Result<(), NexusError> {
        self.set_claude_session(runtime_id, claude_session_id, transcript_path)
            .await
    }

    /// Persist or refresh the transcript path independently from Claude session discovery.
    pub async fn set_transcript_path(
        &self,
        runtime_id: &SessionId,
        transcript_path: PathBuf,
    ) -> Result<(), NexusError> {
        self.ensure_schema().await?;
        self.store
            .conn
            .execute(
                "UPDATE claude_runtime_state
                 SET transcript_path = ?2, updated_at = ?3
                 WHERE runtime_id = ?1",
                params![runtime_id.0.clone(), path_to_text(&transcript_path), now()],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Persist the transcript path and reset its cursor to the supplied byte offset.
    ///
    /// Transcript cursors are monotonic for normal tailing, but a native Claude session can later
    /// reveal the real `~/.claude/projects/.../<session>.jsonl` path through hook payloads. When
    /// switching away from the launch-local placeholder, the cursor belongs to a different file and
    /// must be replaced rather than compared with the old path's cursor.
    pub async fn set_transcript_path_and_cursor(
        &self,
        runtime_id: &SessionId,
        transcript_path: PathBuf,
        transcript_cursor: i64,
    ) -> Result<(), NexusError> {
        self.ensure_schema().await?;
        self.store
            .conn
            .execute(
                "UPDATE claude_runtime_state
                 SET transcript_path = ?2,
                     transcript_cursor = ?3,
                     updated_at = ?4
                 WHERE runtime_id = ?1",
                params![
                    runtime_id.0.clone(),
                    path_to_text(&transcript_path),
                    transcript_cursor.max(0),
                    now(),
                ],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Persist the tmux attachment target for the headed Claude TUI side.
    pub async fn set_tmux(
        &self,
        runtime_id: &SessionId,
        tmux_socket: Option<&str>,
        tmux_session: &str,
    ) -> Result<(), NexusError> {
        self.ensure_schema().await?;
        self.store
            .conn
            .execute(
                "UPDATE claude_runtime_state
                 SET tmux_socket = ?2, tmux_session = ?3, updated_at = ?4
                 WHERE runtime_id = ?1",
                params![runtime_id.0.clone(), tmux_socket, tmux_session, now()],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Persist one cursor, never moving it below the value already stored for the runtime.
    pub async fn set_cursor(
        &self,
        runtime_id: &SessionId,
        cursor: ClaudeRuntimeCursor,
        value: i64,
    ) -> Result<(), NexusError> {
        match cursor {
            ClaudeRuntimeCursor::Hook => self.set_hook_cursor(runtime_id, value).await,
            ClaudeRuntimeCursor::Transcript => self.set_transcript_cursor(runtime_id, value).await,
            ClaudeRuntimeCursor::MessageDelta => {
                self.set_message_delta_cursor(runtime_id, value).await
            }
        }
    }

    /// Persist the hook JSONL cursor for the native bridge.
    pub async fn set_hook_cursor(
        &self,
        runtime_id: &SessionId,
        hook_cursor: i64,
    ) -> Result<(), NexusError> {
        self.set_cursors(runtime_id, Some(hook_cursor), None, None)
            .await
    }

    /// Persist the transcript JSONL cursor for the native bridge.
    pub async fn set_transcript_cursor(
        &self,
        runtime_id: &SessionId,
        transcript_cursor: i64,
    ) -> Result<(), NexusError> {
        self.set_cursors(runtime_id, None, Some(transcript_cursor), None)
            .await
    }

    /// Persist the message-display delta cursor for the native bridge.
    pub async fn set_message_delta_cursor(
        &self,
        runtime_id: &SessionId,
        message_delta_cursor: i64,
    ) -> Result<(), NexusError> {
        self.set_cursors(runtime_id, None, None, Some(message_delta_cursor))
            .await
    }

    /// Persist any known cursors. Cursor values are monotonic: lower values are ignored.
    pub async fn set_cursors(
        &self,
        runtime_id: &SessionId,
        hook_cursor: Option<i64>,
        transcript_cursor: Option<i64>,
        message_delta_cursor: Option<i64>,
    ) -> Result<(), NexusError> {
        self.ensure_schema().await?;
        self.store
            .conn
            .execute(
                "UPDATE claude_runtime_state
                 SET hook_cursor = CASE
                         WHEN ?2 IS NULL THEN hook_cursor
                         WHEN ?2 > hook_cursor THEN ?2
                         ELSE hook_cursor
                     END,
                     transcript_cursor = CASE
                         WHEN ?3 IS NULL THEN transcript_cursor
                         WHEN ?3 > transcript_cursor THEN ?3
                         ELSE transcript_cursor
                     END,
                     message_delta_cursor = CASE
                         WHEN ?4 IS NULL THEN message_delta_cursor
                         WHEN ?4 > message_delta_cursor THEN ?4
                         ELSE message_delta_cursor
                     END,
                     updated_at = ?5
                 WHERE runtime_id = ?1",
                params![
                    runtime_id.0.clone(),
                    hook_cursor,
                    transcript_cursor,
                    message_delta_cursor,
                    now(),
                ],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Load the Claude sidecar state for one Nexus runtime id.
    pub async fn find_by_runtime_id(
        &self,
        runtime_id: &SessionId,
    ) -> Result<Option<ClaudeRuntimeState>, NexusError> {
        self.ensure_schema().await?;
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT runtime_id, bridge_dir, claude_session_id, transcript_path, launch_cwd,
                    tmux_socket, tmux_session, bridge_pid, hook_pids_json, hook_cursor,
                    transcript_cursor, message_delta_cursor, created_at, updated_at
                 FROM claude_runtime_state
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

    /// List headed Claude runtime rows that still lack a native Claude session id.
    ///
    /// Restart recovery and `nexus attach` revival require an exact Claude `--resume <id>` hint.
    /// This read surface lets repair tooling find old sidecar rows that predate native-id
    /// persistence, then fill them through [`Self::set_claude_session`]. The hint remains opaque:
    /// Nexus does not validate Claude Code ownership or uniqueness.
    pub async fn list_missing_claude_session_id(
        &self,
    ) -> Result<Vec<ClaudeRuntimeState>, NexusError> {
        if !self.table_exists().await? {
            return Ok(Vec::new());
        }
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT runtime_id, bridge_dir, claude_session_id, transcript_path, launch_cwd,
                    tmux_socket, tmux_session, bridge_pid, hook_pids_json, hook_cursor,
                    transcript_cursor, message_delta_cursor, created_at, updated_at
                 FROM claude_runtime_state
                 WHERE claude_session_id IS NULL OR claude_session_id = ''
                 ORDER BY created_at, runtime_id",
                (),
            )
            .await
            .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(row_to_state(&row)?);
        }
        Ok(out)
    }

    async fn table_exists(&self) -> Result<bool, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT 1
                 FROM sqlite_master
                 WHERE type = 'table' AND name = 'claude_runtime_state'
                 LIMIT 1",
                (),
            )
            .await
            .map_err(store_err)?;
        Ok(rows.next().await.map_err(store_err)?.is_some())
    }

    /// Load Claude sidecar state by Claude Code's native session id.
    ///
    /// This diagnostic lookup predates the Nexus/Claude identity boundary. It may report ambiguity,
    /// but live MCP authentication and runtime identity never use the result: Claude's native id is
    /// only an opaque, best-effort resume hint.
    pub async fn find_by_claude_session_id(
        &self,
        claude_session_id: &str,
    ) -> Result<Option<ClaudeRuntimeState>, NexusError> {
        let rows = self
            .find_all_by_claude_session_id(claude_session_id)
            .await?;
        match rows.len() {
            0 => Ok(None),
            1 => Ok(rows.into_iter().next()),
            _ => Err(duplicate_resume_hint(claude_session_id, &rows)),
        }
    }

    /// Load all Claude sidecar rows that claim a Claude Code native session id.
    pub async fn find_all_by_claude_session_id(
        &self,
        claude_session_id: &str,
    ) -> Result<Vec<ClaudeRuntimeState>, NexusError> {
        self.ensure_schema().await?;
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT runtime_id, bridge_dir, claude_session_id, transcript_path, launch_cwd,
                    tmux_socket, tmux_session, bridge_pid, hook_pids_json, hook_cursor,
                    transcript_cursor, message_delta_cursor, created_at, updated_at
                 FROM claude_runtime_state
                 WHERE claude_session_id = ?1
                 ORDER BY created_at",
                params![claude_session_id],
            )
            .await
            .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(row_to_state(&row)?);
        }
        Ok(out)
    }

    /// Delete the Claude sidecar state for a runtime id.
    pub async fn delete(&self, runtime_id: &SessionId) -> Result<(), NexusError> {
        self.ensure_schema().await?;
        self.store
            .conn
            .execute(
                "DELETE FROM claude_runtime_state WHERE runtime_id = ?1",
                params![runtime_id.0.clone()],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }
}

/// Derive the paths and Claude session id needed to revive a headed Claude runtime.
pub fn claude_runtime_revive_seed(
    home: &Path,
    runtime_id: &SessionId,
    state: Option<&ClaudeRuntimeState>,
) -> ClaudeRuntimeReviveSeed {
    let default_state_dir = home.join(".nexus");
    let default_session_dir = default_state_dir
        .join("claude-sessions")
        .join(runtime_id.0.as_str());
    let stored_bridge_dir = state.and_then(|state| state.bridge_dir.clone());
    let bridge_dir = stored_bridge_dir
        .clone()
        .unwrap_or_else(|| default_session_dir.join("bridge"));
    let session_dir = bridge_dir
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| default_session_dir.clone());
    let state_dir = session_dir
        .parent()
        .and_then(|claude_sessions| claude_sessions.parent())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| default_state_dir.clone());
    let launch_cwd = state
        .and_then(|state| state.launch_cwd.clone())
        .unwrap_or_else(|| session_dir.join("cwd"));
    let transcript_path = state
        .and_then(|state| state.transcript_path.clone())
        .unwrap_or_else(|| bridge_dir.join("transcript.jsonl"));
    let claude_session_id = state.and_then(|state| state.claude_session_id.clone());

    ClaudeRuntimeReviveSeed {
        state_dir,
        bridge_dir,
        launch_cwd,
        transcript_path,
        claude_session_id,
    }
}

fn row_to_state(row: &libsql::Row) -> Result<ClaudeRuntimeState, NexusError> {
    Ok(ClaudeRuntimeState {
        runtime_id: SessionId(get_text(row, 0)?),
        bridge_dir: get_opt_text(row, 1)?.map(PathBuf::from),
        claude_session_id: get_opt_text(row, 2)?,
        transcript_path: get_opt_text(row, 3)?.map(PathBuf::from),
        launch_cwd: get_opt_text(row, 4)?.map(PathBuf::from),
        tmux_socket: get_opt_text(row, 5)?,
        tmux_session: get_opt_text(row, 6)?,
        bridge_pid: get_opt_int(row, 7)?,
        hook_pids_json: get_opt_text(row, 8)?,
        hook_cursor: get_opt_int(row, 9)?.unwrap_or(0),
        transcript_cursor: get_opt_int(row, 10)?.unwrap_or(0),
        message_delta_cursor: get_opt_int(row, 11)?.unwrap_or(0),
        created_at: get_opt_int(row, 12)?.unwrap_or(0),
        updated_at: get_opt_int(row, 13)?.unwrap_or(0),
    })
}

fn path_to_text(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn duplicate_resume_hint(claude_session_id: &str, rows: &[ClaudeRuntimeState]) -> NexusError {
    let runtime_ids = rows
        .iter()
        .map(|state| state.runtime_id.0.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    NexusError::Ambiguous(format!(
        "Claude resume hint {claude_session_id} appears in multiple runtime rows: {runtime_ids}"
    ))
}

fn get_text(row: &libsql::Row, idx: i32) -> Result<String, NexusError> {
    row.get::<String>(idx).map_err(store_err)
}

fn get_opt_text(row: &libsql::Row, idx: i32) -> Result<Option<String>, NexusError> {
    row.get::<Option<String>>(idx).map_err(store_err)
}

fn get_opt_int(row: &libsql::Row, idx: i32) -> Result<Option<i64>, NexusError> {
    row.get::<Option<i64>>(idx).map_err(store_err)
}

fn store_err(e: libsql::Error) -> NexusError {
    NexusError::Store(e.to_string())
}

impl<'a> ClaudeRuntimeStateRepo<'a> {
    async fn ensure_column(
        &self,
        table: &str,
        column: &str,
        definition: &str,
    ) -> Result<(), NexusError> {
        if self.column_exists(table, column).await? {
            return Ok(());
        }
        self.store
            .conn
            .execute(
                &format!("ALTER TABLE {table} ADD COLUMN {column} {definition}"),
                (),
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    async fn column_exists(&self, table: &str, column: &str) -> Result<bool, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(&format!("PRAGMA table_info({table})"), ())
            .await
            .map_err(store_err)?;
        while let Some(row) = rows.next().await.map_err(store_err)? {
            let name: String = row.get(1).map_err(store_err)?;
            if name == column {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

/// One tailed Claude subagent transcript (`<project>/<native session>/subagents/agent-<id>.jsonl`)
/// and where this runtime's child pass stopped in it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeChildCursor {
    pub runtime_id: SessionId,
    pub native_session_id: String,
    pub agent_id: String,
    pub path: PathBuf,
    /// Daemon boot epoch the cursor was last advanced under.
    pub epoch: String,
    /// Source generation: the file's first record uuid; empty until the first read.
    pub generation: String,
    /// Byte offset after the last record forwarded.
    pub cursor: i64,
    /// Set once the pass declared bounded unknown coverage for this file (restart, source
    /// replacement, truncation, unverifiable generation) and stopped advancing it; the
    /// declaration is repeated under every later daemon epoch while halted.
    pub halted: bool,
    /// Why the cursor halted (`daemon_restart_policy_undecided`, `source_replaced`,
    /// `source_truncated`, `generation_unverifiable`), empty while tailing.
    pub halt_reason: String,
    /// Resolution of the last record forwarded from this file (`root_verified` or
    /// `unresolved`); coverage declarations carry it instead of inferring one from the name.
    pub resolution: String,
    /// When the file was last served by a pass; passes serve the least recently served first.
    pub served_at: i64,
    /// Start offset and uuid of the last record forwarded: a pass re-reads that one record and
    /// checks it still ends at `cursor` before trusting the cursor (truncate-and-regrow shows
    /// up here even when the file is longer than the cursor again).
    pub anchor_offset: i64,
    pub anchor_uuid: String,
    pub updated_at: i64,
}

/// Repo for the Claude child stream cursors. Durable, next to `claude_runtime_state`.
pub struct ClaudeChildStreamsRepo<'a> {
    store: &'a Store,
}

impl<'a> ClaudeChildStreamsRepo<'a> {
    pub fn new(store: &'a Store) -> Self {
        Self { store }
    }

    /// Create the cursor table if this store predates Claude child streams.
    pub async fn ensure_schema(&self) -> Result<(), NexusError> {
        self.store
            .conn
            .execute(
                "CREATE TABLE IF NOT EXISTS claude_child_streams (
                    runtime_id TEXT NOT NULL,
                    native_session_id TEXT NOT NULL,
                    agent_id TEXT NOT NULL,
                    path TEXT NOT NULL,
                    epoch TEXT NOT NULL,
                    generation TEXT NOT NULL DEFAULT '',
                    cursor INTEGER NOT NULL DEFAULT 0,
                    halted INTEGER NOT NULL DEFAULT 0,
                    halt_reason TEXT NOT NULL DEFAULT '',
                    resolution TEXT NOT NULL DEFAULT 'unresolved',
                    served_at INTEGER NOT NULL DEFAULT 0,
                    anchor_offset INTEGER NOT NULL DEFAULT 0,
                    anchor_uuid TEXT NOT NULL DEFAULT '',
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY (runtime_id, native_session_id, agent_id)
                )",
                (),
            )
            .await
            .map_err(store_err)?;
        self.store
            .conn
            .execute(
                "CREATE TABLE IF NOT EXISTS claude_child_discovery (
                    runtime_id TEXT NOT NULL,
                    native_session_id TEXT NOT NULL,
                    next_skip INTEGER NOT NULL DEFAULT 0,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY (runtime_id, native_session_id)
                )",
                (),
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    pub async fn find(
        &self,
        runtime_id: &SessionId,
        native_session_id: &str,
        agent_id: &str,
    ) -> Result<Option<ClaudeChildCursor>, NexusError> {
        self.ensure_schema().await?;
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT runtime_id, native_session_id, agent_id, path, epoch, generation, cursor, halted, halt_reason, resolution, served_at, anchor_offset, anchor_uuid, updated_at
                 FROM claude_child_streams
                 WHERE runtime_id = ?1 AND native_session_id = ?2 AND agent_id = ?3",
                params![
                    runtime_id.0.clone(),
                    native_session_id.to_string(),
                    agent_id.to_string()
                ],
            )
            .await
            .map_err(store_err)?;
        match rows.next().await.map_err(store_err)? {
            Some(row) => Ok(Some(child_cursor_from_row(&row)?)),
            None => Ok(None),
        }
    }

    /// Where the next pass's directory window starts for this owned session (0 at first).
    pub async fn discovery_skip(
        &self,
        runtime_id: &SessionId,
        native_session_id: &str,
    ) -> Result<usize, NexusError> {
        self.ensure_schema().await?;
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT next_skip FROM claude_child_discovery
                 WHERE runtime_id = ?1 AND native_session_id = ?2",
                params![runtime_id.0.clone(), native_session_id.to_string()],
            )
            .await
            .map_err(store_err)?;
        match rows.next().await.map_err(store_err)? {
            Some(row) => Ok(usize::try_from(row.get::<i64>(0).map_err(store_err)?).unwrap_or(0)),
            None => Ok(0),
        }
    }

    pub async fn set_discovery_skip(
        &self,
        runtime_id: &SessionId,
        native_session_id: &str,
        next_skip: usize,
    ) -> Result<(), NexusError> {
        self.ensure_schema().await?;
        self.store
            .conn
            .execute(
                "INSERT INTO claude_child_discovery (runtime_id, native_session_id, next_skip, updated_at)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(runtime_id, native_session_id) DO UPDATE SET
                    next_skip = excluded.next_skip, updated_at = excluded.updated_at",
                params![
                    runtime_id.0.clone(),
                    native_session_id.to_string(),
                    i64::try_from(next_skip).unwrap_or(i64::MAX),
                    now()
                ],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Which of `agent_ids` already have a cursor row for this owned session. Queried in
    /// bounded chunks so a discovery window never builds an unbounded statement.
    pub async fn known_agent_ids(
        &self,
        runtime_id: &SessionId,
        native_session_id: &str,
        agent_ids: &[String],
    ) -> Result<std::collections::HashSet<String>, NexusError> {
        self.ensure_schema().await?;
        let mut known = std::collections::HashSet::new();
        for chunk in agent_ids.chunks(256) {
            let placeholders = (0..chunk.len())
                .map(|i| format!("?{}", i + 3))
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT agent_id FROM claude_child_streams
                 WHERE runtime_id = ?1 AND native_session_id = ?2 AND agent_id IN ({placeholders})"
            );
            let mut values: Vec<libsql::Value> = Vec::with_capacity(chunk.len() + 2);
            values.push(runtime_id.0.clone().into());
            values.push(native_session_id.to_string().into());
            values.extend(chunk.iter().map(|id| libsql::Value::from(id.clone())));
            let mut rows = self
                .store
                .conn
                .query(&sql, values)
                .await
                .map_err(store_err)?;
            while let Some(row) = rows.next().await.map_err(store_err)? {
                known.insert(get_text(&row, 0)?);
            }
        }
        Ok(known)
    }

    /// Register a discovered file with an untouched cursor unless a row already exists.
    pub async fn insert_if_absent(&self, cursor: &ClaudeChildCursor) -> Result<(), NexusError> {
        self.ensure_schema().await?;
        let ts = now();
        self.store
            .conn
            .execute(
                "INSERT OR IGNORE INTO claude_child_streams
                    (runtime_id, native_session_id, agent_id, path, epoch, generation, cursor, halted,
                     halt_reason, resolution, served_at, anchor_offset, anchor_uuid, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?14)",
                params![
                    cursor.runtime_id.0.clone(),
                    cursor.native_session_id.clone(),
                    cursor.agent_id.clone(),
                    path_to_text(&cursor.path),
                    cursor.epoch.clone(),
                    cursor.generation.clone(),
                    cursor.cursor,
                    i64::from(cursor.halted),
                    cursor.halt_reason.clone(),
                    cursor.resolution.clone(),
                    cursor.served_at,
                    cursor.anchor_offset,
                    cursor.anchor_uuid.clone(),
                    ts
                ],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// The `limit` cursors served least recently (then by owned session and agent id): one
    /// SQL page, never the whole table.
    pub async fn least_recently_served(
        &self,
        runtime_id: &SessionId,
        limit: usize,
    ) -> Result<Vec<ClaudeChildCursor>, NexusError> {
        self.ensure_schema().await?;
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT runtime_id, native_session_id, agent_id, path, epoch, generation, cursor, halted, halt_reason, resolution, served_at, anchor_offset, anchor_uuid, updated_at
                 FROM claude_child_streams WHERE runtime_id = ?1
                 ORDER BY served_at ASC, native_session_id ASC, agent_id ASC
                 LIMIT ?2",
                params![runtime_id.0.clone(), i64::try_from(limit).unwrap_or(i64::MAX)],
            )
            .await
            .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(child_cursor_from_row(&row)?);
        }
        Ok(out)
    }

    /// Record that a pass is about to attempt this file. Independent of whether the attempt
    /// then succeeds, so a persistently failing file loses its place in the rotation.
    pub async fn mark_attempt(
        &self,
        runtime_id: &SessionId,
        native_session_id: &str,
        agent_id: &str,
        served_at: i64,
    ) -> Result<(), NexusError> {
        self.ensure_schema().await?;
        self.store
            .conn
            .execute(
                "UPDATE claude_child_streams SET served_at = ?4, updated_at = ?4
                 WHERE runtime_id = ?1 AND native_session_id = ?2 AND agent_id = ?3",
                params![
                    runtime_id.0.clone(),
                    native_session_id.to_string(),
                    agent_id.to_string(),
                    served_at
                ],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Every child cursor of a runtime, in agent id order.
    pub async fn list(&self, runtime_id: &SessionId) -> Result<Vec<ClaudeChildCursor>, NexusError> {
        self.ensure_schema().await?;
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT runtime_id, native_session_id, agent_id, path, epoch, generation, cursor, halted, halt_reason, resolution, served_at, anchor_offset, anchor_uuid, updated_at
                 FROM claude_child_streams WHERE runtime_id = ?1
                 ORDER BY native_session_id ASC, agent_id ASC",
                params![runtime_id.0.clone()],
            )
            .await
            .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(child_cursor_from_row(&row)?);
        }
        Ok(out)
    }

    /// Insert or replace the cursor's path, epoch, generation, offset and halted flag.
    pub async fn upsert(&self, cursor: &ClaudeChildCursor) -> Result<(), NexusError> {
        self.ensure_schema().await?;
        let ts = now();
        self.store
            .conn
            .execute(
                "INSERT INTO claude_child_streams
                    (runtime_id, native_session_id, agent_id, path, epoch, generation, cursor, halted,
                     halt_reason, resolution, served_at, anchor_offset, anchor_uuid, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?14)
                 ON CONFLICT(runtime_id, native_session_id, agent_id) DO UPDATE SET
                    path = excluded.path,
                    epoch = excluded.epoch,
                    generation = excluded.generation,
                    cursor = excluded.cursor,
                    halted = excluded.halted,
                    halt_reason = excluded.halt_reason,
                    resolution = excluded.resolution,
                    served_at = excluded.served_at,
                    anchor_offset = excluded.anchor_offset,
                    anchor_uuid = excluded.anchor_uuid,
                    updated_at = excluded.updated_at",
                params![
                    cursor.runtime_id.0.clone(),
                    cursor.native_session_id.clone(),
                    cursor.agent_id.clone(),
                    path_to_text(&cursor.path),
                    cursor.epoch.clone(),
                    cursor.generation.clone(),
                    cursor.cursor,
                    i64::from(cursor.halted),
                    cursor.halt_reason.clone(),
                    cursor.resolution.clone(),
                    cursor.served_at,
                    cursor.anchor_offset,
                    cursor.anchor_uuid.clone(),
                    ts
                ],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }
}

fn child_cursor_from_row(row: &libsql::Row) -> Result<ClaudeChildCursor, NexusError> {
    Ok(ClaudeChildCursor {
        runtime_id: SessionId(get_text(row, 0)?),
        native_session_id: get_text(row, 1)?,
        agent_id: get_text(row, 2)?,
        path: PathBuf::from(get_text(row, 3)?),
        epoch: get_text(row, 4)?,
        generation: get_text(row, 5)?,
        cursor: row.get::<i64>(6).map_err(store_err)?,
        halted: row.get::<i64>(7).map_err(store_err)? != 0,
        halt_reason: get_text(row, 8)?,
        resolution: get_text(row, 9)?,
        served_at: row.get::<i64>(10).map_err(store_err)?,
        anchor_offset: row.get::<i64>(11).map_err(store_err)?,
        anchor_uuid: get_text(row, 12)?,
        updated_at: row.get::<i64>(13).map_err(store_err)?,
    })
}
