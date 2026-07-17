//! Durable harness-native thread/session ownership.
//!
//! Runtime sidecar tables answer "what process is currently bound?" This repo answers "which
//! stable Nexus agent owns this native conversation?"

use libsql::params;

use nexus_common::{now, NexusError};

use crate::error::store_err;
use crate::repos::sessions::{get_opt_int, get_opt_text, get_text};
use crate::state::Store;
use crate::types::NativeThreadBindingRow;

#[derive(Debug, Clone)]
pub struct NewNativeThreadBinding {
    pub harness: String,
    pub native_thread_id: String,
    pub agent_id: String,
    pub project: String,
    pub runtime_id: Option<String>,
}

pub struct NativeThreadBindings<'a> {
    store: &'a Store,
}

impl<'a> NativeThreadBindings<'a> {
    pub fn new(store: &'a Store) -> Self {
        Self { store }
    }

    pub async fn find(
        &self,
        harness: &str,
        native_thread_id: &str,
    ) -> Result<Option<NativeThreadBindingRow>, NexusError> {
        let mut rows = self
            .store
            .identity_conn()
            .query(
                &format!("{SELECT} WHERE harness = ?1 AND native_thread_id = ?2"),
                params![harness, native_thread_id],
            )
            .await
            .map_err(store_err)?;
        match rows.next().await.map_err(store_err)? {
            Some(row) => Ok(Some(row_to_binding(&row)?)),
            None => Ok(None),
        }
    }

    /// Find the newest known native conversation owned by `agent_id` for one harness.
    ///
    /// Runtime sidecars are fallback evidence. Attach/resume paths use this identity-owned binding
    /// first so a stale compatibility row cannot replace the stable owner of a native conversation.
    pub async fn find_latest_for_agent(
        &self,
        harness: &str,
        agent_id: &str,
    ) -> Result<Option<NativeThreadBindingRow>, NexusError> {
        let mut rows = self
            .store
            .identity_conn()
            .query(
                &format!(
                    "{SELECT} WHERE harness = ?1 AND agent_id = ?2 \
                     ORDER BY released_at IS NULL DESC, updated_at DESC, native_thread_id ASC \
                     LIMIT 1"
                ),
                params![harness, agent_id],
            )
            .await
            .map_err(store_err)?;
        match rows.next().await.map_err(store_err)? {
            Some(row) => Ok(Some(row_to_binding(&row)?)),
            None => Ok(None),
        }
    }

    /// Find the native binding currently associated with one runtime row.
    pub async fn find_for_runtime(
        &self,
        harness: &str,
        runtime_id: &str,
    ) -> Result<Option<NativeThreadBindingRow>, NexusError> {
        let mut rows = self
            .store
            .identity_conn()
            .query(
                &format!(
                    "{SELECT} WHERE harness = ?1 AND last_runtime_id = ?2 \
                     ORDER BY released_at IS NULL DESC, updated_at DESC, native_thread_id ASC \
                     LIMIT 1"
                ),
                params![harness, runtime_id],
            )
            .await
            .map_err(store_err)?;
        match rows.next().await.map_err(store_err)? {
            Some(row) => Ok(Some(row_to_binding(&row)?)),
            None => Ok(None),
        }
    }

    pub async fn claim(
        &self,
        binding: NewNativeThreadBinding,
    ) -> Result<NativeThreadBindingRow, NexusError> {
        let ts = now();
        let harness = binding.harness.clone();
        let native_thread_id = binding.native_thread_id.clone();
        let agent_id = binding.agent_id.clone();
        self.store
            .identity_conn()
            .execute(
                "INSERT INTO native_thread_bindings (
                    harness, native_thread_id, agent_id, project, first_runtime_id,
                    last_runtime_id, created_at, updated_at, released_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?5, ?6, ?6, NULL)
                 ON CONFLICT(harness, native_thread_id) DO UPDATE SET
                    project = excluded.project,
                    last_runtime_id = COALESCE(excluded.last_runtime_id, last_runtime_id),
                    updated_at = excluded.updated_at,
                    released_at = NULL
                 WHERE native_thread_bindings.agent_id = excluded.agent_id",
                params![
                    binding.harness,
                    binding.native_thread_id,
                    binding.agent_id,
                    binding.project,
                    binding.runtime_id,
                    ts,
                ],
            )
            .await
            .map_err(store_err)?;
        let row = self
            .find(&harness, &native_thread_id)
            .await?
            .ok_or_else(|| NexusError::NotFound("native thread binding was not inserted".into()))?;
        if row.agent_id != agent_id {
            return Err(NexusError::Invalid(format!(
                "native thread {harness}:{native_thread_id} already bound to {}",
                row.agent_id
            )));
        }
        Ok(row)
    }

    pub async fn mark_released_for_runtime(
        &self,
        harness: &str,
        runtime_id: &str,
    ) -> Result<(), NexusError> {
        self.store
            .identity_conn()
            .execute(
                "UPDATE native_thread_bindings
                 SET released_at = COALESCE(released_at, ?3), updated_at = ?3
                 WHERE harness = ?1 AND last_runtime_id = ?2",
                params![harness, runtime_id, now()],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    pub async fn delete_for_agent(&self, agent_id: &str) -> Result<(), NexusError> {
        self.store
            .identity_conn()
            .execute(
                "DELETE FROM native_thread_bindings WHERE agent_id = ?1",
                params![agent_id],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }
}

const SELECT: &str = "SELECT harness, native_thread_id, agent_id, project, first_runtime_id,
    last_runtime_id, created_at, updated_at, released_at FROM native_thread_bindings";

fn row_to_binding(row: &libsql::Row) -> Result<NativeThreadBindingRow, NexusError> {
    Ok(NativeThreadBindingRow {
        harness: get_text(row, 0)?,
        native_thread_id: get_text(row, 1)?,
        agent_id: get_text(row, 2)?,
        project: get_text(row, 3)?,
        first_runtime_id: get_opt_text(row, 4)?,
        last_runtime_id: get_opt_text(row, 5)?,
        created_at: get_opt_int(row, 6)?.unwrap_or(0),
        updated_at: get_opt_int(row, 7)?.unwrap_or(0),
        released_at: get_opt_int(row, 8)?,
    })
}
