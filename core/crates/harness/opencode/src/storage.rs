//! Native runtime sidecar persistence, owned by the harness.

use libsql::params;
use nexus_common::{now, NexusError};
use nexus_contracts::SessionId;
use nexus_store::{
    repos::{IdentitySessions, NativeThreadBindings, NewNativeThreadBinding, Sessions},
    Store,
};
use std::path::{Path, PathBuf};

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
                provider: "opencode".into(),
                kind: "harness".into(),
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

fn is_duplicate_column(error: &libsql::Error) -> bool {
    error
        .to_string()
        .to_ascii_lowercase()
        .contains("duplicate column")
}
