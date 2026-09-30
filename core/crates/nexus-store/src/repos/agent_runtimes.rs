//! Disposable agent runtime repo.
//!
//! `agent_runtimes` records the current process/session representing a durable agent. The generic
//! row deliberately stops at harness label, cwd, transport, presence, heartbeat, and a generic OS
//! process ids; harness-native resume/control state belongs in harness-owned storage.

use libsql::params;

use nexus_common::presence::presence_token;
use nexus_common::{now, NexusError, RuntimeProcessIds};
use nexus_contracts::enums::Presence;
use nexus_contracts::ids::SessionId;

use crate::error::store_err;
use crate::repos::sessions::{get_opt_int, get_opt_text, get_text};
use crate::repos::{DeveloperEvents, Sessions};
use crate::state::Store;
use crate::types::AgentRuntimeRow;

/// Fields needed to create a runtime row.
#[derive(Debug, Clone)]
pub struct NewAgentRuntime {
    pub runtime_id: String,
    pub agent_id: String,
    pub harness: String,
    pub cwd: Option<String>,
    pub transport: Option<String>,
    pub presence: Option<String>,
    pub active: bool,
}

/// Persistence for the `agent_runtimes` table.
pub struct AgentRuntimes<'a> {
    store: &'a Store,
}

impl<'a> AgentRuntimes<'a> {
    /// Bind a repo to the shared store connection.
    pub fn new(store: &'a Store) -> Self {
        AgentRuntimes { store }
    }

    /// Insert a runtime. Creating an active runtime deactivates any previous active runtime for
    /// the same stable agent.
    pub async fn create(&self, runtime: NewAgentRuntime) -> Result<String, NexusError> {
        let runtime_id = runtime.runtime_id.clone();
        let stopped_siblings = if runtime.active {
            self.active_sibling_runtime_ids(&runtime.agent_id, &runtime_id)
                .await?
        } else {
            Vec::new()
        };
        let ts = now();
        let tx = self
            .store
            .begin_identity_write_txn("agent_runtime_create")
            .await?;
        if runtime.active {
            tx.execute(
                "UPDATE agent_runtimes SET active = 0, stopped_at = COALESCE(stopped_at, ?2), \
                 os_pid = NULL, os_pgid = NULL \
                 WHERE agent_id = ?1 AND active = 1",
                params![runtime.agent_id.clone(), ts],
            )
            .await?;
        }
        tx.execute(
            "INSERT INTO agent_runtimes (runtime_id, agent_id, harness, cwd, transport, \
             presence, active, started_at, stopped_at, last_heartbeat) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, NULL, NULL)",
            params![
                runtime.runtime_id,
                runtime.agent_id,
                runtime.harness,
                runtime.cwd,
                runtime.transport,
                runtime.presence,
                runtime.active as i64,
                ts
            ],
        )
        .await?;
        tx.commit().await?;
        for stopped_runtime_id in stopped_siblings {
            self.append_stopped_lifecycle(&stopped_runtime_id, ts)
                .await?;
        }
        self.store.events().session_lifecycle_changed().signal();
        Ok(runtime_id)
    }

    /// Return the active runtime for a stable agent, if one exists.
    pub async fn active_for_agent(
        &self,
        agent_id: &str,
    ) -> Result<Option<AgentRuntimeRow>, NexusError> {
        self.query_one(
            "WHERE agent_id = ?1 AND active = 1 AND stopped_at IS NULL \
             ORDER BY started_at DESC LIMIT 1",
            params![agent_id],
        )
        .await
    }

    /// Find a runtime by its disposable runtime id.
    pub async fn find_by_runtime_id(
        &self,
        runtime_id: &str,
    ) -> Result<Option<AgentRuntimeRow>, NexusError> {
        self.query_one("WHERE runtime_id = ?1", params![runtime_id])
            .await
    }

    /// Remove an impossible runtime row owned by a non-agent compatibility session.
    ///
    /// This is intentionally keyed only by the disposable runtime id: callers must first prove
    /// that the authoritative `sessions` row has a non-agent kind. The durable agent identity and
    /// every sibling runtime remain untouched.
    pub async fn remove_non_agent_residue(&self, runtime_id: &str) -> Result<bool, NexusError> {
        let changed = self
            .store
            .identity_conn()
            .execute(
                "DELETE FROM agent_runtimes WHERE runtime_id = ?1",
                params![runtime_id],
            )
            .await
            .map_err(store_err)?;
        if changed > 0 {
            self.store.events().session_lifecycle_changed().signal();
        }
        Ok(changed > 0)
    }

    /// Roll back identity-side rows for a registration that never became externally visible.
    ///
    /// `runtime_id` is the freshly generated compatibility session id, so deleting that exact row
    /// cannot affect an older runtime. `generated_agent_id` is supplied only for the legacy
    /// name-only path, whose durable id is derived from that fresh session id. Existing explicit
    /// stable identities are never removed by this cleanup.
    pub async fn remove_staged_registration(
        &self,
        runtime_id: &str,
        generated_agent_id: Option<&str>,
    ) -> Result<(), NexusError> {
        let tx = self
            .store
            .begin_identity_write_txn("agent_registration_rollback")
            .await?;
        let result = async {
            tx.execute(
                "DELETE FROM agent_runtimes WHERE runtime_id = ?1",
                params![runtime_id],
            )
            .await?;
            if let Some(agent_id) = generated_agent_id {
                tx.execute(
                    "DELETE FROM agents WHERE agent_id = ?1
                     AND NOT EXISTS (
                       SELECT 1 FROM agent_runtimes WHERE agent_id = ?1
                     )
                     AND NOT EXISTS (
                       SELECT 1 FROM agent_credentials WHERE agent_id = ?1
                     )
                     AND NOT EXISTS (
                       SELECT 1 FROM native_thread_bindings WHERE agent_id = ?1
                     )",
                    params![agent_id],
                )
                .await?;
            }
            Ok(())
        }
        .await;
        match result {
            Ok(()) => {
                tx.commit().await?;
                self.store.events().session_lifecycle_changed().signal();
                Ok(())
            }
            Err(error) => {
                tx.rollback(&error).await?;
                Err(error)
            }
        }
    }

    /// Update runtime presence and heartbeat. Takes the [`Presence`] enum and serializes it to the
    /// stored lowercase token via the canonical [`nexus_common::presence::presence_token`].
    pub async fn set_presence(
        &self,
        runtime_id: &str,
        presence: Presence,
    ) -> Result<(), NexusError> {
        self.store
            .identity_conn()
            .execute(
                "UPDATE agent_runtimes SET presence = ?2, last_heartbeat = ?3 \
                 WHERE runtime_id = ?1",
                params![runtime_id, presence_token(presence), now()],
            )
            .await
            .map_err(store_err)?;
        self.store.events().session_lifecycle_changed().signal();
        Ok(())
    }

    /// Refresh an existing runtime as live, online, and freshly heartbeated.
    ///
    /// This is the durable-runtime twin of refreshing the compatibility `sessions` row for a live
    /// daemon-owned harness. It can recover this exact row when no sibling runtime is active, but it
    /// never steals `activeRuntime` away from a newer active sibling.
    pub async fn mark_live(&self, runtime_id: &str) -> Result<(), NexusError> {
        let runtime = self
            .find_by_runtime_id(runtime_id)
            .await?
            .ok_or_else(|| NexusError::NotFound(format!("runtime:{runtime_id}")))?;
        self.store
            .identity_conn()
            .execute(
                "UPDATE agent_runtimes SET active = 1, presence = 'online', stopped_at = NULL, \
                 last_heartbeat = ?2 WHERE runtime_id = ?1 AND NOT EXISTS ( \
                   SELECT 1 FROM agent_runtimes sibling \
                   WHERE sibling.agent_id = ?3 \
                     AND sibling.runtime_id <> ?1 \
                     AND sibling.active = 1 \
                     AND sibling.stopped_at IS NULL \
                 )",
                params![runtime_id, now(), runtime.agent_id],
            )
            .await
            .map_err(store_err)?;
        self.store.events().session_lifecycle_changed().signal();
        Ok(())
    }

    /// Toggle runtime active state. Activating one runtime deactivates any sibling runtime.
    pub async fn set_active(&self, runtime_id: &str, active: bool) -> Result<(), NexusError> {
        let runtime = self
            .find_by_runtime_id(runtime_id)
            .await?
            .ok_or_else(|| NexusError::NotFound(format!("runtime:{runtime_id}")))?;
        let ts = now();
        let stopped_siblings = if active {
            self.active_sibling_runtime_ids(&runtime.agent_id, runtime_id)
                .await?
        } else {
            Vec::new()
        };
        let tx = self
            .store
            .begin_identity_write_txn("agent_runtime_set_active")
            .await?;
        if active {
            tx.execute(
                "UPDATE agent_runtimes SET active = 0, stopped_at = COALESCE(stopped_at, ?2), \
                 os_pid = NULL, os_pgid = NULL \
                 WHERE agent_id = ?1 AND runtime_id <> ?3 AND active = 1",
                params![runtime.agent_id, ts, runtime_id],
            )
            .await?;
            tx.execute(
                "UPDATE agent_runtimes SET active = 1, stopped_at = NULL WHERE runtime_id = ?1",
                params![runtime_id],
            )
            .await?;
        } else {
            tx.execute(
                "UPDATE agent_runtimes SET active = 0, stopped_at = COALESCE(stopped_at, ?2), \
                 os_pid = NULL, os_pgid = NULL \
                 WHERE runtime_id = ?1",
                params![runtime_id, ts],
            )
            .await?;
        }
        tx.commit().await?;
        for stopped_runtime_id in stopped_siblings {
            self.append_stopped_lifecycle(&stopped_runtime_id, ts)
                .await?;
        }
        if !active && runtime.active && runtime.stopped_at.is_none() {
            self.append_stopped_lifecycle(runtime_id, ts).await?;
        }
        self.store.events().session_lifecycle_changed().signal();
        Ok(())
    }

    /// Stop a runtime and mark it offline.
    pub async fn stop(&self, runtime_id: &str) -> Result<(), NexusError> {
        let before = self.find_by_runtime_id(runtime_id).await?;
        let ts = now();
        self.store
            .identity_conn()
            .execute(
                "UPDATE agent_runtimes SET active = 0, presence = 'offline', \
                 stopped_at = COALESCE(stopped_at, ?2), \
                 os_pid = NULL, os_pgid = NULL \
                 WHERE runtime_id = ?1",
                params![runtime_id, ts],
            )
            .await
            .map_err(store_err)?;
        if before
            .as_ref()
            .map(|row| row.active && row.stopped_at.is_none())
            .unwrap_or(false)
        {
            self.append_stopped_lifecycle(runtime_id, ts).await?;
        }
        self.store.events().session_lifecycle_changed().signal();
        Ok(())
    }

    /// Stop active runtime rows whose effective heartbeat is missing or older than `ttl_ms`.
    ///
    /// Runtime rows can be heartbeated directly, but daemon-owned headed runtimes often rely on the
    /// compatibility `sessions.last_heartbeat` row. The reconcile rule therefore uses the freshest
    /// non-null heartbeat across both rows for the same `runtime_id`.
    pub async fn stop_stale(&self, now_ms: i64, ttl_ms: i64) -> Result<(), NexusError> {
        let stale = self.stale_active_runtime_ids(now_ms, ttl_ms).await?;
        if stale.is_empty() {
            return Ok(());
        }
        let stale_json =
            serde_json::to_string(&stale).map_err(|error| NexusError::Store(error.to_string()))?;
        let changed = self
            .store
            .identity_conn()
            .execute(
                "UPDATE agent_runtimes \
                 SET active = 0, presence = 'offline', stopped_at = COALESCE(stopped_at, ?1), \
                     os_pid = NULL, os_pgid = NULL \
                 WHERE active = 1 AND stopped_at IS NULL \
                   AND runtime_id IN (SELECT value FROM json_each(?2))",
                params![now_ms, stale_json],
            )
            .await
            .map_err(store_err)?;
        if changed > 0 {
            for runtime_id in stale {
                self.append_stopped_lifecycle(&runtime_id, now_ms).await?;
            }
            self.store.events().session_lifecycle_changed().signal();
        }
        Ok(())
    }

    /// Persist the OS process ids currently owned by this runtime.
    ///
    /// The tuple is generic process identity, not harness-private state. Daemon boot uses it to
    /// reap known orphaned process groups left behind by daemon crashes or forced exits.
    pub async fn set_process_ids(
        &self,
        runtime_id: &str,
        entry: RuntimeProcessIds,
    ) -> Result<(), NexusError> {
        let changed = self
            .store
            .identity_conn()
            .execute(
                "UPDATE agent_runtimes SET os_pid = ?2, os_pgid = ?3 \
                 WHERE runtime_id = ?1",
                params![
                    runtime_id,
                    i64::from(entry.os_pid),
                    i64::from(entry.os_pgid)
                ],
            )
            .await
            .map_err(store_err)?;
        if changed == 0 {
            return Err(NexusError::NotFound(format!("runtime:{runtime_id}")));
        }
        Ok(())
    }

    /// Clear the runtime's OS process ids after the process group has been verified dead.
    pub async fn clear_process_ids(&self, runtime_id: &str) -> Result<(), NexusError> {
        self.store
            .identity_conn()
            .execute(
                "UPDATE agent_runtimes SET os_pid = NULL, os_pgid = NULL WHERE runtime_id = ?1",
                params![runtime_id],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// List rows whose durable process ids should be considered for daemon boot reaping.
    ///
    /// Active ACP rows are daemon-owned stdio processes and cannot be adopted after daemon death,
    /// so a matching ledger tuple is an orphan. Stopped/offline rows are also candidates. Active
    /// headed/app-server rows remain adoptable and are deliberately excluded.
    pub async fn list_boot_process_reap_candidates(
        &self,
    ) -> Result<Vec<AgentRuntimeRow>, NexusError> {
        let mut rows = self
            .store
            .identity_conn()
            .query(
                &format!(
                    "{SELECT} WHERE os_pid IS NOT NULL \
                     AND os_pgid IS NOT NULL \
                     AND (transport = 'acp' OR active = 0 OR stopped_at IS NOT NULL \
                          OR COALESCE(presence, 'offline') = 'offline') \
                     ORDER BY started_at DESC"
                ),
                (),
            )
            .await
            .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(row_to_runtime(&row)?);
        }
        Ok(out)
    }

    /// List runtimes for one stable agent. By default only the active runtime is returned; set
    /// `include_stopped` to include historical and inactive rows.
    pub async fn list_for_agent(
        &self,
        agent_id: &str,
        include_stopped: bool,
    ) -> Result<Vec<AgentRuntimeRow>, NexusError> {
        let mut rows = if include_stopped {
            self.store
                .identity_conn()
                .query(
                    &format!("{SELECT} WHERE agent_id = ?1 ORDER BY started_at DESC"),
                    params![agent_id],
                )
                .await
        } else {
            self.store
                .identity_conn()
                .query(
                    &format!(
                        "{SELECT} WHERE agent_id = ?1 AND active = 1 AND stopped_at IS NULL \
                         ORDER BY started_at DESC"
                    ),
                    params![agent_id],
                )
                .await
        }
        .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(row_to_runtime(&row)?);
        }
        Ok(out)
    }

    /// List active runtime rows matching a harness and transport. This is used by daemon boot
    /// adoption for harness-owned sidecars, where the generic runtime row identifies which
    /// disposable sessions should have auxiliary observers reattached without relaunching them.
    pub async fn list_active_by_harness_transport(
        &self,
        harness: &str,
        transport: &str,
    ) -> Result<Vec<AgentRuntimeRow>, NexusError> {
        let mut rows = self
            .store
            .identity_conn()
            .query(
                &format!(
                    "{SELECT} WHERE harness = ?1 AND transport = ?2 AND active = 1 \
                     AND stopped_at IS NULL ORDER BY started_at DESC"
                ),
                params![harness, transport],
            )
            .await
            .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(row_to_runtime(&row)?);
        }
        Ok(out)
    }

    /// List active runtime rows matching a transport. Daemon boot adoption uses this to reattach
    /// loop/observer tasks for headed runtimes that survived the daemon process restart.
    pub async fn list_active_by_transport(
        &self,
        transport: &str,
    ) -> Result<Vec<AgentRuntimeRow>, NexusError> {
        let mut rows = self
            .store
            .identity_conn()
            .query(
                &format!(
                    "{SELECT} WHERE transport = ?1 AND active = 1 AND stopped_at IS NULL \
                     ORDER BY started_at DESC"
                ),
                params![transport],
            )
            .await
            .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(row_to_runtime(&row)?);
        }
        Ok(out)
    }

    async fn query_one(
        &self,
        where_clause: &str,
        p: impl libsql::params::IntoParams,
    ) -> Result<Option<AgentRuntimeRow>, NexusError> {
        let mut rows = self
            .store
            .identity_conn()
            .query(&format!("{SELECT} {where_clause}"), p)
            .await
            .map_err(store_err)?;
        match rows.next().await.map_err(store_err)? {
            Some(row) => Ok(Some(row_to_runtime(&row)?)),
            None => Ok(None),
        }
    }

    async fn stale_active_runtime_ids(
        &self,
        now_ms: i64,
        ttl_ms: i64,
    ) -> Result<Vec<String>, NexusError> {
        let mut rows = self
            .store
            .identity_conn()
            .query(
                &format!("{SELECT} WHERE active = 1 AND stopped_at IS NULL ORDER BY started_at"),
                (),
            )
            .await
            .map_err(store_err)?;
        let mut runtimes = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            runtimes.push(row_to_runtime(&row)?);
        }
        drop(rows);

        let sessions = Sessions::new(self.store);
        let mut out = Vec::new();
        for runtime in runtimes {
            let live = sessions
                .find_by_session_id(&SessionId(runtime.runtime_id.clone()))
                .await?;
            let effective_heartbeat = runtime
                .last_heartbeat
                .into_iter()
                .chain(live.as_ref().and_then(|row| row.last_heartbeat))
                .chain(std::iter::once(runtime.started_at))
                .max()
                .unwrap_or(runtime.started_at);
            let explicitly_offline = live
                .as_ref()
                .is_some_and(|row| row.presence.as_deref().unwrap_or("offline") == "offline");
            if explicitly_offline || now_ms.saturating_sub(effective_heartbeat) > ttl_ms {
                out.push(runtime.runtime_id);
            }
        }
        Ok(out)
    }

    async fn active_sibling_runtime_ids(
        &self,
        agent_id: &str,
        runtime_id: &str,
    ) -> Result<Vec<String>, NexusError> {
        let mut rows = self
            .store
            .identity_conn()
            .query(
                "SELECT runtime_id FROM agent_runtimes \
                 WHERE agent_id = ?1 AND runtime_id <> ?2 AND active = 1",
                params![agent_id, runtime_id],
            )
            .await
            .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(get_text(&row, 0)?);
        }
        Ok(out)
    }

    async fn append_stopped_lifecycle(
        &self,
        runtime_id: &str,
        created_at: i64,
    ) -> Result<(), NexusError> {
        let Some(agent_name) = self.agent_name_for_runtime(runtime_id).await? else {
            return Ok(());
        };
        DeveloperEvents::new(self.store)
            .append_agent_lifecycle(
                &agent_name,
                &SessionId(runtime_id.to_string()),
                "stopped",
                None,
                created_at,
            )
            .await?;
        Ok(())
    }

    async fn agent_name_for_runtime(&self, runtime_id: &str) -> Result<Option<String>, NexusError> {
        let mut rows = self
            .store
            .identity_conn()
            .query(
                "SELECT a.name FROM agent_runtimes ar \
                 LEFT JOIN agents a ON a.agent_id = ar.agent_id \
                 WHERE ar.runtime_id = ?1 LIMIT 1",
                params![runtime_id],
            )
            .await
            .map_err(store_err)?;
        let durable_name = match rows.next().await.map_err(store_err)? {
            Some(row) => get_opt_text(&row, 0)?,
            None => None,
        };
        if durable_name.is_some() {
            return Ok(durable_name);
        }
        Ok(Sessions::new(self.store)
            .find_by_session_id(&SessionId(runtime_id.to_string()))
            .await?
            .and_then(|row| row.name))
    }
}

const SELECT: &str = "SELECT runtime_id, agent_id, harness, cwd, transport, presence, active, \
     started_at, stopped_at, last_heartbeat, os_pid, os_pgid FROM agent_runtimes";

pub(crate) fn row_to_runtime(row: &libsql::Row) -> Result<AgentRuntimeRow, NexusError> {
    Ok(AgentRuntimeRow {
        runtime_id: get_text(row, 0)?,
        agent_id: get_text(row, 1)?,
        harness: get_text(row, 2)?,
        cwd: get_opt_text(row, 3)?,
        transport: get_opt_text(row, 4)?,
        presence: get_opt_text(row, 5)?,
        active: get_opt_int(row, 6)?.unwrap_or(0) != 0,
        started_at: get_opt_int(row, 7)?.unwrap_or(0),
        stopped_at: get_opt_int(row, 8)?,
        last_heartbeat: get_opt_int(row, 9)?,
        os_pid: get_opt_int(row, 10)?,
        os_pgid: get_opt_int(row, 11)?,
    })
}
