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
