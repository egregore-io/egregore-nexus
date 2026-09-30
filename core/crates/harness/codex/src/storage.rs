//! Codex-owned runtime state persisted in the shared Nexus store.
//!
//! Generic identity and active runtime rows live in `nexus-store`'s `agents` and
//! `agent_runtimes` tables. This module owns the Codex-specific sidecar state needed to revive a
//! headed Codex app-server session without guessing paths from a stale in-memory bridge.

use std::path::{Path, PathBuf};

use libsql::params;
use nexus_common::{now, NexusError};
use nexus_contracts::SessionId;
use nexus_store::repos::{NativeThreadBindings, NewNativeThreadBinding, Sessions};
use nexus_store::Store;

/// Runtime metadata written when a headed Codex app-server is launched or adopted.
#[derive(Debug, Clone, PartialEq)]
pub struct CodexRuntimeLaunch {
    pub runtime_id: SessionId,
    pub codex_thread_id: Option<String>,
    pub codex_home: PathBuf,
    pub app_server_sock: PathBuf,
    pub app_server_pid: Option<u32>,
    pub mcp_sidecar_pids_json: Option<String>,
    pub app_server_adopted: bool,
}

/// One row from `codex_runtime_state`.
#[derive(Debug, Clone, PartialEq)]
pub struct CodexRuntimeState {
    pub runtime_id: SessionId,
    pub codex_thread_id: Option<String>,
    pub codex_home: Option<PathBuf>,
    pub app_server_sock: Option<PathBuf>,
    pub app_server_pid: Option<i64>,
    pub mcp_sidecar_pids_json: Option<String>,
    pub app_server_adopted: bool,
    pub tmux_socket: Option<String>,
    pub tmux_session: Option<String>,
    pub rollout_path: Option<PathBuf>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// Revive inputs derived from Codex-specific runtime state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexRuntimeReviveSeed {
    pub state_dir: PathBuf,
    pub rollout_root: PathBuf,
    pub thread_id: Option<String>,
}

/// Repo for the Codex sidecar state table.
pub struct CodexRuntimeStateRepo<'a> {
    store: &'a Store,
}

impl<'a> CodexRuntimeStateRepo<'a> {
    pub fn new(store: &'a Store) -> Self {
        Self { store }
    }

    /// Create the Codex-owned sidecar table if this store predates the Codex runtime-state slice.
    pub async fn ensure_schema(&self) -> Result<(), NexusError> {
        self.store
            .conn
            .execute(
                "CREATE TABLE IF NOT EXISTS codex_runtime_state (
                    runtime_id TEXT PRIMARY KEY,
                    codex_thread_id TEXT,
                    codex_home TEXT,
                    app_server_sock TEXT,
                    app_server_pid INTEGER,
                    mcp_sidecar_pids_json TEXT,
                    app_server_adopted INTEGER NOT NULL DEFAULT 0,
                    tmux_socket TEXT,
                    tmux_session TEXT,
                    rollout_path TEXT,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                )",
                (),
            )
            .await
            .map_err(store_err)?;
        self.ensure_column("codex_runtime_state", "mcp_sidecar_pids_json", "TEXT")
            .await?;
        Ok(())
    }

    /// Insert or refresh launch/adoption metadata. A known resume thread is persisted immediately;
    /// otherwise the existing discovered thread/tmux fields survive.
    pub async fn upsert_launch(&self, launch: CodexRuntimeLaunch) -> Result<(), NexusError> {
        self.ensure_schema().await?;
        if let Some(thread_id) = launch.codex_thread_id.as_deref() {
            self.claim_native_thread_binding(&launch.runtime_id, thread_id)
                .await?;
        }
        let ts = now();
        self.store
            .conn
            .execute(
                "INSERT INTO codex_runtime_state (
                    runtime_id, codex_thread_id, codex_home, app_server_sock, app_server_pid,
                    mcp_sidecar_pids_json, app_server_adopted, tmux_socket, tmux_session,
                    rollout_path, created_at, updated_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, NULL, NULL, ?8, ?8)
                ON CONFLICT(runtime_id) DO UPDATE SET
                    codex_thread_id = COALESCE(excluded.codex_thread_id, codex_runtime_state.codex_thread_id),
                    codex_home = excluded.codex_home,
                    app_server_sock = excluded.app_server_sock,
                    app_server_pid = excluded.app_server_pid,
                    mcp_sidecar_pids_json = excluded.mcp_sidecar_pids_json,
                    app_server_adopted = excluded.app_server_adopted,
                    updated_at = excluded.updated_at",
                params![
                    launch.runtime_id.0,
                    launch.codex_thread_id,
                    path_to_text(&launch.codex_home),
                    path_to_text(&launch.app_server_sock),
                    launch.app_server_pid.map(i64::from),
                    launch.mcp_sidecar_pids_json,
                    launch.app_server_adopted as i64,
                    ts,
                ],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Persist Codex MCP sidecar process ids discovered by the app-server runtime.
    pub async fn set_mcp_sidecar_pids(
        &self,
        runtime_id: &SessionId,
        pids_json: Option<&str>,
    ) -> Result<(), NexusError> {
        self.ensure_schema().await?;
        self.store
            .conn
            .execute(
                "UPDATE codex_runtime_state
                 SET mcp_sidecar_pids_json = ?2, updated_at = ?3
                 WHERE runtime_id = ?1",
                params![runtime_id.0.clone(), pids_json, now()],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Persist the Codex thread discovered from rollout files.
    pub async fn set_thread(
        &self,
        runtime_id: &SessionId,
        codex_thread_id: &str,
        rollout_path: Option<PathBuf>,
    ) -> Result<(), NexusError> {
        self.ensure_schema().await?;
        self.claim_native_thread_binding(runtime_id, codex_thread_id)
            .await?;
        self.store
            .conn
            .execute(
                "UPDATE codex_runtime_state
                 SET codex_thread_id = ?2, rollout_path = COALESCE(?3, rollout_path), updated_at = ?4
                 WHERE runtime_id = ?1",
                params![
                    runtime_id.0.clone(),
                    codex_thread_id,
                    rollout_path.as_deref().map(path_to_text),
                    now(),
                ],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    async fn claim_native_thread_binding(
        &self,
        runtime_id: &SessionId,
        codex_thread_id: &str,
    ) -> Result<(), NexusError> {
        if codex_thread_id.is_empty() {
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
                provider: "codex".into(),
                kind: "harness".into(),
                native_thread_id: codex_thread_id.to_string(),
                agent_id,
                project: row.project,
                runtime_id: Some(runtime_id.0.clone()),
            })
            .await?;
        Ok(())
    }

    /// Mark an existing app-server socket as adopted rather than Nexus-owned.
    pub async fn mark_adopted(&self, runtime_id: &SessionId) -> Result<(), NexusError> {
        self.ensure_schema().await?;
        self.store
            .conn
            .execute(
                "UPDATE codex_runtime_state
                 SET app_server_adopted = 1, updated_at = ?2
                 WHERE runtime_id = ?1",
                params![runtime_id.0.clone(), now()],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Persist the tmux attachment target for the headed TUI side.
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
                "UPDATE codex_runtime_state
                 SET tmux_socket = ?2, tmux_session = ?3, updated_at = ?4
                 WHERE runtime_id = ?1",
                params![runtime_id.0.clone(), tmux_socket, tmux_session, now()],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Clear the legacy tmux viewer target when a Codex app-server runtime is viewed through a raw
    /// daemon-owned PTY instead. The app-server socket/thread metadata remains intact.
    pub async fn clear_tmux(&self, runtime_id: &SessionId) -> Result<(), NexusError> {
        self.ensure_schema().await?;
        self.store
            .conn
            .execute(
                "UPDATE codex_runtime_state
                 SET tmux_socket = NULL, tmux_session = NULL, updated_at = ?2
                 WHERE runtime_id = ?1",
                params![runtime_id.0.clone(), now()],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    pub async fn find_by_runtime_id(
        &self,
        runtime_id: &SessionId,
    ) -> Result<Option<CodexRuntimeState>, NexusError> {
        self.ensure_schema().await?;
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT runtime_id, codex_thread_id, codex_home, app_server_sock,
                    app_server_pid, mcp_sidecar_pids_json, app_server_adopted, tmux_socket, tmux_session,
                    rollout_path, created_at, updated_at
                 FROM codex_runtime_state
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

    /// Return Codex runtime sidecar rows that already claim a Codex thread id.
    pub async fn find_by_thread_id(
        &self,
        codex_thread_id: &str,
    ) -> Result<Vec<CodexRuntimeState>, NexusError> {
        self.ensure_schema().await?;
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT runtime_id, codex_thread_id, codex_home, app_server_sock,
                    app_server_pid, mcp_sidecar_pids_json, app_server_adopted, tmux_socket, tmux_session,
                    rollout_path, created_at, updated_at
                 FROM codex_runtime_state
                 WHERE codex_thread_id = ?1
                 ORDER BY created_at",
                params![codex_thread_id],
            )
            .await
            .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(row_to_state(&row)?);
        }
        Ok(out)
    }

    pub async fn delete(&self, runtime_id: &SessionId) -> Result<(), NexusError> {
        self.ensure_schema().await?;
        self.store
            .conn
            .execute(
                "DELETE FROM codex_runtime_state WHERE runtime_id = ?1",
                params![runtime_id.0.clone()],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }
}

fn row_to_state(row: &libsql::Row) -> Result<CodexRuntimeState, NexusError> {
    Ok(CodexRuntimeState {
        runtime_id: SessionId(get_text(row, 0)?),
        codex_thread_id: get_opt_text(row, 1)?,
        codex_home: get_opt_text(row, 2)?.map(PathBuf::from),
        app_server_sock: get_opt_text(row, 3)?.map(PathBuf::from),
        app_server_pid: get_opt_int(row, 4)?,
        mcp_sidecar_pids_json: get_opt_text(row, 5)?,
        app_server_adopted: get_opt_int(row, 6)?.unwrap_or(0) != 0,
        tmux_socket: get_opt_text(row, 7)?,
        tmux_session: get_opt_text(row, 8)?,
        rollout_path: get_opt_text(row, 9)?.map(PathBuf::from),
        created_at: get_opt_int(row, 10)?.unwrap_or(0),
        updated_at: get_opt_int(row, 11)?.unwrap_or(0),
    })
}

fn path_to_text(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

/// Derive the paths needed to revive a headed Codex runtime.
///
/// The Nexus session directory is taken from the stored app-server socket when available. That
/// keeps revive correct for explicit Codex resume launches where `codex_home` points at an external
/// home such as `~/.codex`, while the app-server socket still lives under Nexus state.
pub fn codex_runtime_revive_seed(
    home: &Path,
    runtime_id: &SessionId,
    state: Option<&CodexRuntimeState>,
    legacy_thread_id: Option<&str>,
) -> CodexRuntimeReviveSeed {
    let default_state_dir = home.join(".nexus");
    let stored_codex_home = state.and_then(|state| state.codex_home.clone());
    let stored_session_dir = state
        .and_then(|state| state.app_server_sock.as_ref())
        .filter(|endpoint| {
            let endpoint = endpoint.to_string_lossy();
            !endpoint.starts_with("ws://") && !endpoint.starts_with("wss://")
        })
        .and_then(|sock| sock.parent().map(|p| p.to_path_buf()));
    let session_dir = stored_session_dir
        .or_else(|| {
            stored_codex_home
                .as_ref()
                .and_then(|codex_home| codex_home.parent().map(|p| p.to_path_buf()))
        })
        .unwrap_or_else(|| {
            default_state_dir
                .join("codex-sessions")
                .join(runtime_id.0.as_str())
        });
    let state_dir = session_dir
        .parent()
        .and_then(|codex_sessions| codex_sessions.parent())
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| default_state_dir.clone());
    let rollout_root = stored_codex_home
        .as_ref()
        .map(|codex_home| codex_home.join("sessions"))
        .unwrap_or_else(|| session_dir.join("codex-home").join("sessions"));
    let thread_id = state
        .and_then(|state| state.codex_thread_id.clone())
        .or_else(|| legacy_thread_id.map(str::to_string));

    CodexRuntimeReviveSeed {
        state_dir,
        rollout_root,
        thread_id,
    }
}

/// Recover the headed viewer mode that created this Codex runtime.
///
/// Raw PTY is the safe default for legacy rows. A persisted tmux socket/session is written only by
/// an explicit tmux launch, so either field is sufficient to preserve that mode across a cold
/// revive even when the old tmux process itself is gone.
pub fn codex_runtime_viewer_backend(state: Option<&CodexRuntimeState>) -> &'static str {
    if state.is_some_and(|state| state.tmux_socket.is_some() || state.tmux_session.is_some()) {
        "tmux"
    } else {
        "pty"
    }
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

impl<'a> CodexRuntimeStateRepo<'a> {
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
