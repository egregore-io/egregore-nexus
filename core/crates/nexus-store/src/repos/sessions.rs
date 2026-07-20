//! The `sessions` repo — register-once identity rows, project-scoped reads. Higher-level
//! resume-vs-reject identity logic lives in `nexus-identity`; this repo is pure persistence.

use libsql::params;

use nexus_common::presence::presence_token;
use nexus_common::{now, NexusError};
use nexus_contracts::enums::Presence;
use nexus_contracts::ids::SessionId;

use crate::error::store_err;
use crate::repos::{AgentRuntimes, Agents, DeveloperEvents};
use crate::state::Store;
use crate::types::SessionRow;

/// Fields needed to create a session row. Named rows are globally addressable; `None` means this
/// is a staged identity that must be explicitly named later.
#[derive(Debug, Clone)]
pub struct NewSession {
    pub session_id: SessionId,
    pub name: Option<String>,
    pub agent: Option<String>,
    pub kind: String,
    pub role: Option<String>,
    pub tier: String,
    pub harness_session_id: Option<String>,
    pub client_key: Option<String>,
    pub cwd: Option<String>,
    pub project: String,
    /// Transport mode: `'pty'` | `'acp'` | `'codex-appserver'`. `None` → NULL (legacy row).
    pub transport: Option<String>,
}

/// Persistence for the `sessions` table.
pub struct Sessions<'a> {
    store: &'a Store,
}

impl<'a> Sessions<'a> {
    /// Bind a repo to the shared store connection.
    pub fn new(store: &'a Store) -> Self {
        Sessions { store }
    }

    /// Insert a new session row (presence `online`, not paused, `created_at = now()`).
    pub async fn create(&self, s: NewSession) -> Result<SessionId, NexusError> {
        let session_id = self.insert(s.clone()).await?;
        let ts = now();
        if s.kind == "agent" {
            if let Some(name) = s.name.as_deref() {
                self.append_lifecycle(name, &session_id, "started", None, ts)
                    .await?;
            }
        }
        self.store.events().session_lifecycle_changed().signal();
        Ok(session_id)
    }

    /// Insert the compatibility row for a registration without publishing lifecycle truth yet.
    ///
    /// Identity registration spans the transport and identity authorities in split-store mode.
    /// The service stages this row, binds the stable runtime, and only then calls
    /// [`finalize_staged_registration`](Self::finalize_staged_registration). If binding fails it
    /// removes the invisible row with
    /// [`remove_staged_registration`](Self::remove_staged_registration), so readers never receive
    /// a `started` fact for an identity that did not finish registering.
    pub async fn create_staged_registration(&self, s: NewSession) -> Result<SessionId, NexusError> {
        self.insert(s).await
    }

    /// Publish the compatibility lifecycle fact for a fully-bound staged registration.
    pub async fn finalize_staged_registration(&self, row: &SessionRow) -> Result<(), NexusError> {
        let result = if row.kind == "agent" {
            match row.name.as_deref() {
                Some(name) => {
                    self.append_lifecycle(name, &row.session_id, "started", None, now())
                        .await
                }
                None => Ok(()),
            }
        } else {
            Ok(())
        };
        self.store.events().session_lifecycle_changed().signal();
        result
    }

    /// Remove a registration row that never crossed the stable identity/runtime boundary.
    pub async fn remove_staged_registration(&self, session: &SessionId) -> Result<(), NexusError> {
        self.store
            .conn
            .execute(
                "DELETE FROM sessions WHERE session_id = ?1",
                params![session.0.clone()],
            )
            .await
            .map_err(store_err)?;
        self.store.events().session_lifecycle_changed().signal();
        Ok(())
    }

    async fn insert(&self, s: NewSession) -> Result<SessionId, NexusError> {
        let ts = now();
        let session_id = s.session_id.clone();
        self.store
            .conn
            .execute(
                "INSERT INTO sessions (session_id, name, agent, kind, role, tier, \
                 harness_session_id, client_key, cwd, project, presence, paused, created_at, \
                 transport) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 'online', 0, ?11, ?12)",
                params![
                    s.session_id.0.clone(),
                    s.name,
                    s.agent,
                    s.kind,
                    s.role,
                    s.tier,
                    s.harness_session_id,
                    s.client_key,
                    s.cwd,
                    s.project,
                    ts,
                    s.transport
                ],
            )
            .await
            .map_err(store_err)?;
        Ok(session_id)
    }

    /// Find a session by its idempotency `client_key` (the resume path).
    pub async fn find_by_client_key(
        &self,
        project: &str,
        client_key: &str,
    ) -> Result<Option<SessionRow>, NexusError> {
        self.query_one(
            "WHERE project = ?1 AND client_key = ?2",
            params![project, client_key],
        )
        .await
    }

    /// Find a session by its globally unique client key. Project is descriptive metadata and must
    /// not participate in caller authentication or transport routing.
    pub async fn find_by_client_key_any_project(
        &self,
        client_key: &str,
    ) -> Result<Option<SessionRow>, NexusError> {
        self.query_one("WHERE client_key = ?1", params![client_key])
            .await
    }

    /// Find a session by its unique `name`, scoped to the project.
    pub async fn find_by_name(
        &self,
        project: &str,
        name: &str,
    ) -> Result<Option<SessionRow>, NexusError> {
        self.query_one("WHERE project = ?1 AND name = ?2", params![project, name])
            .await
    }

    /// Update a session's presence. Takes the [`Presence`] enum and serializes it to the stored
    /// lowercase token (`online`|`busy`|`offline`) via the canonical
    /// [`nexus_common::presence::presence_token`].
    pub async fn set_presence(
        &self,
        session: &SessionId,
        presence: Presence,
    ) -> Result<(), NexusError> {
        let before = self.find_by_session_id(session).await?;
        self.store
            .conn
            .execute(
                "UPDATE sessions SET presence = ?2 WHERE session_id = ?1",
                params![session.0.clone(), presence_token(presence)],
            )
            .await
            .map_err(store_err)?;
        if let Some(row) = before.as_ref() {
            if let Some(lifecycle) = lifecycle_for_presence_transition(
                row.presence.as_deref().unwrap_or("offline"),
                presence_token(presence),
            ) {
                self.append_lifecycle(&row.display_name(), session, lifecycle, None, now())
                    .await?;
            }
        }
        self.store.events().session_lifecycle_changed().signal();
        Ok(())
    }

    /// Update the operator-visible work-state field and emit a metadata-only lifecycle event.
    ///
    /// `current_work` is the developer event counterpart to `nexus status --work`: tooling can
    /// watch `sys.agent.lifecycle` instead of polling roster rows, while no agent turn is woken.
    pub async fn set_current_work(
        &self,
        session: &SessionId,
        work: Option<&str>,
    ) -> Result<(), NexusError> {
        let before = self
            .find_by_session_id(session)
            .await?
            .ok_or_else(|| NexusError::NotFound(session.0.clone()))?;
        if before.current_work.as_deref() == work {
            return Ok(());
        }
        self.store
            .conn
            .execute(
                "UPDATE sessions SET current_work = ?2 WHERE session_id = ?1",
                params![session.0.clone(), work],
            )
            .await
            .map_err(store_err)?;
        self.append_lifecycle(&before.display_name(), session, "current_work", work, now())
            .await?;
        self.store.events().session_lifecycle_changed().signal();
        Ok(())
    }

    /// Update a session's durable transport label (`'pty'` | `'acp'`).
    pub async fn set_transport(
        &self,
        session: &SessionId,
        transport: &str,
    ) -> Result<(), NexusError> {
        self.store
            .conn
            .execute(
                "UPDATE sessions SET transport = ?2 WHERE session_id = ?1",
                params![session.0.clone(), transport],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Stamp the stable durable identity backing this compatibility/live session row.
    ///
    /// Launch/revive paths resolve an `agent_id` at the edge, then persist that id on `sessions`
    /// before the row can be used for later attach/revive reads. Fossil rows are intentionally left
    /// `NULL` until a path has resolved the identity explicitly.
    pub async fn set_agent_id(
        &self,
        session: &SessionId,
        agent_id: &str,
    ) -> Result<(), NexusError> {
        self.store
            .conn
            .execute(
                "UPDATE sessions SET agent_id = ?2 WHERE session_id = ?1",
                params![session.0.clone(), agent_id],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Reclaim an existing offline identity row for a daemon-owned launch that minted a new
    /// `session_id`. This preserves the display name and pending inbox while making `members` and
    /// `resolve(name)` point at the newly bound transport session.
    pub async fn rebind_name_to_session(&self, s: NewSession) -> Result<SessionId, NexusError> {
        let new_session_id = s.session_id.0.clone();
        let event_session_id = SessionId(new_session_id.clone());
        let is_agent = s.kind == "agent";
        let name = s
            .name
            .clone()
            .ok_or_else(|| NexusError::Invalid("cannot rebind an unnamed session".into()))?;
        let existing = self
            .find_by_name(&s.project, &name)
            .await?
            .ok_or_else(|| NexusError::NotFound(name.clone()))?;
        let ts = now();
        self.store
            .conn
            .execute(
                "UPDATE sessions SET session_id = ?1, agent = ?2, kind = ?3, role = ?4, \
                 tier = ?5, harness_session_id = ?6, client_key = ?7, cwd = ?8, project = ?9, \
                 presence = 'online', paused = 0, paused_by = NULL, last_heartbeat = ?10, \
                 transport = ?11 WHERE session_id = ?12",
                params![
                    new_session_id.clone(),
                    s.agent,
                    s.kind,
                    s.role,
                    s.tier,
                    s.harness_session_id,
                    s.client_key,
                    s.cwd,
                    s.project,
                    ts,
                    s.transport,
                    existing.session_id.0.clone()
                ],
            )
            .await
            .map_err(store_err)?;
        self.store
            .conn
            .execute(
                "UPDATE in_flight SET recipient_session = ?1 WHERE recipient_session = ?2",
                params![new_session_id, existing.session_id.0.clone()],
            )
            .await
            .map_err(store_err)?;
        if is_agent {
            self.append_lifecycle(&name, &event_session_id, "started", None, ts)
                .await?;
        }
        self.store.events().session_lifecycle_changed().signal();
        Ok(existing.session_id)
    }

    /// Persist the harness-native resume key. For ACP this is the ACP `session/load` id; for headed
    /// Codex app-server sessions this is the Codex thread id discovered from rollout metadata.
    pub async fn set_harness_session_id(
        &self,
        session: &SessionId,
        harness_session_id: &str,
    ) -> Result<(), NexusError> {
        self.store
            .conn
            .execute(
                "UPDATE sessions SET harness_session_id = ?2 WHERE session_id = ?1",
                params![session.0.clone(), harness_session_id],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Clear the harness-native resume key. `admin.remove` uses this to explicitly release native
    /// resume ownership (for example a headed Codex thread id) while retaining the compatibility
    /// session row and message history.
    pub async fn clear_harness_session_id(&self, session: &SessionId) -> Result<(), NexusError> {
        self.store
            .conn
            .execute(
                "UPDATE sessions SET harness_session_id = NULL WHERE session_id = ?1",
                params![session.0.clone()],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Clear a session's client key. Credential revocation uses this to make an already-running
    /// runtime fail its next authenticated command without deleting its history.
    pub async fn clear_client_key(&self, session: &SessionId) -> Result<(), NexusError> {
        self.store
            .conn
            .execute(
                "UPDATE sessions SET client_key = NULL WHERE session_id = ?1",
                params![session.0.clone()],
            )
            .await
            .map_err(store_err)?;
        if self.store.has_split_authority() {
            self.store
                .identity_conn()
                .execute(
                    "UPDATE identity_sessions SET client_key = NULL WHERE runtime_id = ?1",
                    params![session.0.clone()],
                )
                .await
                .map_err(store_err)?;
        }
        Ok(())
    }

    /// Rebind a session's display name (the `rename` op). The daemon is the sole writer; the caller
    /// guards name uniqueness in the project before calling.
    pub async fn set_name(&self, session: &SessionId, name: &str) -> Result<(), NexusError> {
        self.store
            .conn
            .execute(
                "UPDATE sessions SET name = ?2 WHERE session_id = ?1",
                params![session.0.clone(), name],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Set the operator-facing role label on the compatibility session row.
    ///
    /// Durable identity reads use `agents.role`; member/whoami compatibility views use
    /// `sessions.role`, so admin role assignment keeps both columns aligned.
    pub async fn set_role(&self, session: &SessionId, role: &str) -> Result<(), NexusError> {
        self.set_role_value(session, Some(role)).await
    }

    /// Set or clear the compatibility display role.
    ///
    /// The nullable form lets cross-database callers compensate to the exact prior state.
    pub async fn set_role_value(
        &self,
        session: &SessionId,
        role: Option<&str>,
    ) -> Result<(), NexusError> {
        self.store
            .conn
            .execute(
                "UPDATE sessions SET role = ?2 WHERE session_id = ?1",
                params![session.0.clone(), role],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Move one exact compatibility/live session row to another project.
    ///
    /// Id-aware admin assignment paths use the selected `session_id` instead of re-reading by name,
    /// which keeps rename/name-reuse cases from moving the wrong row.
    pub async fn set_project(&self, session: &SessionId, project: &str) -> Result<(), NexusError> {
        self.store
            .conn
            .execute(
                "UPDATE sessions SET project = ?2 WHERE session_id = ?1",
                params![session.0.clone(), project],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Set the authorization tier on a compatibility/live session row.
    ///
    /// Durable grants update `agents.tier` and this row so existing runtimes resolve the new
    /// privilege ceiling on their next command without requiring a re-register.
    pub async fn set_tier(&self, session: &SessionId, tier: &str) -> Result<(), NexusError> {
        self.store
            .conn
            .execute(
                "UPDATE sessions SET tier = ?2 WHERE session_id = ?1",
                params![session.0.clone(), tier],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// PURGE — erase a durable agent identity entirely (the `delete` op): the compatibility
    /// session row, stable runtime/credential rows, in-flight rows, thread memberships, ACL grant
    /// edges, and every message it sent or received. Caller kills the live process first.
    /// Best-effort per statement; a missing row is not an error.
    ///
    /// Legacy callers may only have `session_id` and the requested name, so this wrapper derives the
    /// stable id from the selected session/runtime before falling back to a name lookup.
    pub async fn purge(&self, session: &SessionId, name: &str) -> Result<(), NexusError> {
        let session_row = self.find_by_session_id(session).await?;
        let runtime = AgentRuntimes::new(self.store)
            .find_by_runtime_id(&session.0)
            .await?;
        let agent = Agents::new(self.store).find_by_name(name).await?;
        let agent_id = session_row
            .as_ref()
            .and_then(|row| row.agent_id.clone())
            .or_else(|| runtime.as_ref().map(|row| row.agent_id.clone()))
            .or_else(|| agent.as_ref().map(|row| row.agent_id.clone()));
        let project = session_row
            .as_ref()
            .map(|row| row.project.clone())
            .or_else(|| agent.as_ref().map(|row| row.project.clone()));
        self.purge_exact(
            session,
            project.as_deref().unwrap_or_default(),
            Some(name),
            agent_id.as_deref(),
        )
        .await
    }

    /// Purge using an already-selected session row.
    ///
    /// Id-aware admin delete paths call this after resolving the target row once. The row's own
    /// `agent_id`, project, name, and session id remain the deletion keys, so a stale request name
    /// cannot cause the purge to delete a different durable identity that now owns that name.
    pub async fn purge_selected(&self, row: &SessionRow) -> Result<(), NexusError> {
        let agent_id = match row.agent_id.as_deref() {
            Some(agent_id) => Some(agent_id.to_string()),
            None => match row.name.as_deref() {
                Some(name) => {
                    self.agent_id_for_selected_session(&row.session_id, &row.project, name)
                        .await?
                }
                None => None,
            },
        };
        self.purge_exact(
            &row.session_id,
            &row.project,
            row.name.as_deref(),
            agent_id.as_deref(),
        )
        .await
    }

    async fn agent_id_for_selected_session(
        &self,
        session: &SessionId,
        project: &str,
        name: &str,
    ) -> Result<Option<String>, NexusError> {
        if let Some(runtime) = AgentRuntimes::new(self.store)
            .find_by_runtime_id(&session.0)
            .await?
        {
            return Ok(Some(runtime.agent_id));
        }
        Ok(Agents::new(self.store)
            .find_by_project_name(project, name)
            .await?
            .map(|agent| agent.agent_id))
    }

    async fn purge_exact(
        &self,
        session: &SessionId,
        project: &str,
        name: Option<&str>,
        agent_id: Option<&str>,
    ) -> Result<(), NexusError> {
        let conn = &self.store.conn;
        let identity = self.store.identity_conn();
        let _ = crate::repos::AgentAccessGrants::new(self.store)
            .purge_deleted_agent(agent_id, project, name)
            .await;
        let _ = conn
            .execute(
                "DELETE FROM in_flight WHERE recipient_session = ?1 \
                 OR (?2 IS NOT NULL AND recipient_agent_id = ?2)",
                params![session.0.clone(), agent_id],
            )
            .await;
        let _ = conn
            .execute(
                "DELETE FROM thread_members WHERE (?1 IS NOT NULL AND session_name = ?1) \
                 OR (?2 IS NOT NULL AND agent_id = ?2)",
                params![name, agent_id],
            )
            .await;
        let _ = conn
            .execute(
                "DELETE FROM messages WHERE (?1 IS NOT NULL AND (from_name = ?1 OR to_name = ?1)) \
                 OR (?2 IS NOT NULL AND (from_agent_id = ?2 OR to_agent_id = ?2))",
                params![name, agent_id],
            )
            .await;
        let _ = identity
            .execute(
                "DELETE FROM agent_credentials WHERE ?1 IS NOT NULL AND agent_id = ?1",
                params![agent_id],
            )
            .await;
        let _ = identity
            .execute(
                "DELETE FROM agent_runtimes WHERE (?1 IS NOT NULL AND agent_id = ?1) \
                 OR runtime_id = ?2",
                params![agent_id, session.0.clone()],
            )
            .await;
        let _ = identity
            .execute(
                "DELETE FROM agents WHERE (?1 IS NOT NULL AND agent_id = ?1) \
                 OR (?1 IS NULL AND ?3 IS NOT NULL AND project = ?2 AND name = ?3)",
                params![agent_id, project, name],
            )
            .await;
        conn.execute(
            "DELETE FROM sessions WHERE session_id = ?1",
            params![session.0.clone()],
        )
        .await
        .map_err(store_err)?;
        if let Some(name) = name {
            self.append_lifecycle(name, session, "stopped", None, now())
                .await?;
        }
        self.store.events().session_lifecycle_changed().signal();
        Ok(())
    }

    /// Set (or clear) the paused hold + its source (self vs admin).
    pub async fn set_paused(
        &self,
        session: &SessionId,
        paused: bool,
        paused_by: Option<&str>,
    ) -> Result<(), NexusError> {
        self.store
            .conn
            .execute(
                "UPDATE sessions SET paused = ?2, paused_by = ?3 WHERE session_id = ?1",
                params![session.0.clone(), paused as i64, paused_by],
            )
            .await
            .map_err(store_err)?;
        self.store.events().session_lifecycle_changed().signal();
        Ok(())
    }

    /// Refresh `last_heartbeat` to now.
    pub async fn touch_heartbeat(&self, session: &SessionId) -> Result<(), NexusError> {
        self.store
            .conn
            .execute(
                "UPDATE sessions SET last_heartbeat = ?2 WHERE session_id = ?1",
                params![session.0.clone(), now()],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Flip an `offline` (non-paused) session back to `online` on proven activity.
    ///
    /// The boot presume-dead reconcile (N1) marks every non-transport-backed session offline;
    /// this is the promised RETURN path — an authenticated command from the session proves it
    /// alive again. Without it the offline flip is sticky for CLI/MCP peers (gate-caught
    /// 2026-07-10 night: every live actor showed offline after the phase-3 daemon bounce).
    pub async fn restore_online_on_activity(
        &self,
        session: &SessionId,
    ) -> Result<bool, NexusError> {
        let changed = self
            .store
            .conn
            .execute(
                "UPDATE sessions SET presence = 'online' \
                 WHERE session_id = ?1 AND COALESCE(presence, 'offline') = 'offline' \
                   AND paused = 0",
                params![session.0.clone()],
            )
            .await
            .map_err(store_err)?;
        if changed > 0 {
            self.store.events().session_lifecycle_changed().signal();
        }
        Ok(changed > 0)
    }

    /// Find a session by its unique `name` across **all** projects (LIMIT 1, insertion-order).
    /// Used by `assign_project` to locate the row before mutating its `project` column.
    pub async fn find_by_name_any_project(
        &self,
        name: &str,
    ) -> Result<Option<SessionRow>, NexusError> {
        self.query_one("WHERE name = ?1 LIMIT 1", params![name])
            .await
    }

    /// Resolve a legacy session-only name globally without guessing across duplicate metadata.
    /// Canonical durable-agent paths resolve through [`Agents`] first; this fallback exists only
    /// for fossils and non-agent principals that have no durable agent row.
    pub async fn find_unique_by_name_any_project(
        &self,
        name: &str,
    ) -> Result<Option<SessionRow>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                &format!("{SELECT} WHERE name = ?1 ORDER BY created_at LIMIT 2"),
                params![name],
            )
            .await
            .map_err(store_err)?;
        let Some(first) = rows.next().await.map_err(store_err)? else {
            return Ok(None);
        };
        let first = row_to_session(&first)?;
        if rows.next().await.map_err(store_err)?.is_some() {
            return Err(NexusError::Ambiguous(format!(
                "session name {name:?} matches multiple legacy identities; address a durable agent by a_* id"
            )));
        }
        Ok(Some(first))
    }

    /// Find a session by its `session_id` (any project). Used on boot re-spawn to resolve a
    /// pending recipient back to its `name`/`kind`/`project`.
    pub async fn find_by_session_id(
        &self,
        session: &SessionId,
    ) -> Result<Option<SessionRow>, NexusError> {
        self.query_one("WHERE session_id = ?1", params![session.0.clone()])
            .await
    }

    /// Find a session by its harness-native resume key across all projects.
    ///
    /// This is used by harnesses whose native session ids must be single-owner in Nexus, such as a
    /// headed Codex thread id. It deliberately returns insertion order so legacy duplicates can be
    /// detected and cleaned up by higher-level management tooling without changing this read.
    pub async fn find_by_harness_session_id(
        &self,
        project: &str,
        harness_session_id: &str,
    ) -> Result<Option<SessionRow>, NexusError> {
        self.query_one(
            "WHERE project = ?1 AND harness_session_id = ?2",
            params![project, harness_session_id],
        )
        .await
    }

    /// Find a session by its harness-native resume key across all projects.
    ///
    /// This is used by harnesses whose native session ids must be single-owner in Nexus, such as a
    /// headed Codex thread id. It deliberately returns insertion order so legacy duplicates can be
    /// detected and cleaned up by higher-level management tooling without changing this read.
    pub async fn find_by_harness_session_id_any_project(
        &self,
        harness_session_id: &str,
    ) -> Result<Vec<SessionRow>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                &format!("{SELECT} WHERE harness_session_id = ?1 ORDER BY created_at"),
                params![harness_session_id],
            )
            .await
            .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(row_to_session(&row)?);
        }
        Ok(out)
    }

    /// Find the newest compatibility session row bound to a durable identity.
    ///
    /// Identity-by-id slice 1: daemon paths that already hold an `agent_id` read the live/compat
    /// directory row without re-resolving the mutable name. Rows with NULL `agent_id` are fossils
    /// and are intentionally invisible to this read.
    pub async fn find_by_agent_id(&self, agent_id: &str) -> Result<Option<SessionRow>, NexusError> {
        self.query_one(
            "WHERE agent_id = ?1 ORDER BY created_at DESC LIMIT 1",
            params![agent_id],
        )
        .await
    }

    /// Resolve the session row backing an identity's ACTIVE runtime (`agent_runtimes.active = 1`).
    ///
    /// This is the revive/attach spine: runtime selection is keyed by `agent_id`, then the exact
    /// `runtime_id` picks the session row (`sessions.session_id == agent_runtimes.runtime_id`).
    /// Returns `None` when the identity has no active runtime or the runtime has no session row.
    pub async fn active_runtime_session_for_agent(
        &self,
        agent_id: &str,
    ) -> Result<Option<SessionRow>, NexusError> {
        let Some(runtime) = AgentRuntimes::new(self.store)
            .active_for_agent(agent_id)
            .await?
        else {
            return Ok(None);
        };
        let runtime_id = runtime.runtime_id;
        let Some(session) = self
            .find_by_session_id(&SessionId(runtime_id.clone()))
            .await?
        else {
            return Ok(None);
        };
        if session.agent_id.as_deref() != Some(agent_id) {
            return Err(NexusError::Invalid(format!(
                "active runtime {runtime_id} belongs to agent {agent_id}, but its session row belongs to {}",
                session.agent_id.as_deref().unwrap_or("<unbound>")
            )));
        }
        Ok(Some(session))
    }

    /// Check whether a session named `name` already exists in `project` (collision guard).
    pub async fn name_exists_in(&self, project: &str, name: &str) -> Result<bool, NexusError> {
        Ok(self.find_by_name(project, name).await?.is_some())
    }

    /// List every session in a project (the directory / presence source).
    pub async fn list(&self, project: &str) -> Result<Vec<SessionRow>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                &format!("{} WHERE project = ?1 ORDER BY created_at", SELECT),
                params![project],
            )
            .await
            .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(row_to_session(&row)?);
        }
        Ok(out)
    }

    /// List every session row across projects. Daemon lifecycle/status paths use this because they
    /// operate on process-owned transports, not a caller's project scope.
    pub async fn list_all(&self) -> Result<Vec<SessionRow>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(&format!("{SELECT} ORDER BY created_at"), ())
            .await
            .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(row_to_session(&row)?);
        }
        Ok(out)
    }

    async fn query_one(
        &self,
        where_clause: &str,
        p: impl libsql::params::IntoParams,
    ) -> Result<Option<SessionRow>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(&format!("{SELECT} {where_clause}"), p)
            .await
            .map_err(store_err)?;
        match rows.next().await.map_err(store_err)? {
            Some(row) => Ok(Some(row_to_session(&row)?)),
            None => Ok(None),
        }
    }

    /// Return raw non-offline rows whose heartbeat/birth timestamp is older than the TTL.
    /// Daemon reconciliation uses this list to route every transition through its complete
    /// presence writer, preserving ordered status emission as well as store convergence.
    pub async fn stale_online_rows(
        &self,
        now_ms: i64,
        ttl_ms: i64,
    ) -> Result<Vec<SessionRow>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                &format!(
                    "{SELECT} WHERE COALESCE(presence, 'offline') <> 'offline' \
                     AND ?1 - COALESCE(last_heartbeat, created_at) > ?2"
                ),
                params![now_ms, ttl_ms],
            )
            .await
            .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(row_to_session(&row)?);
        }
        Ok(out)
    }

    async fn append_lifecycle(
        &self,
        name: &str,
        session: &SessionId,
        lifecycle: &str,
        current_work: Option<&str>,
        created_at: i64,
    ) -> Result<(), NexusError> {
        DeveloperEvents::new(self.store)
            .append_agent_lifecycle(name, session, lifecycle, current_work, created_at)
            .await?;
        Ok(())
    }
}

const SELECT: &str = "SELECT session_id, name, agent, kind, role, tier, harness_session_id, \
     client_key, cwd, project, current_work, presence, paused, paused_by, callback_url, \
     last_heartbeat, created_at, transport, metadata_json, agent_id FROM sessions";

fn row_to_session(row: &libsql::Row) -> Result<SessionRow, NexusError> {
    Ok(SessionRow {
        session_id: SessionId(get_text(row, 0)?),
        name: get_opt_text(row, 1)?,
        agent: get_opt_text(row, 2)?,
        kind: get_text(row, 3)?,
        role: get_opt_text(row, 4)?,
        tier: get_text(row, 5)?,
        harness_session_id: get_opt_text(row, 6)?,
        client_key: get_opt_text(row, 7)?,
        cwd: get_opt_text(row, 8)?,
        project: get_text(row, 9)?,
        current_work: get_opt_text(row, 10)?,
        presence: get_opt_text(row, 11)?,
        paused: get_opt_int(row, 12)?.unwrap_or(0) != 0,
        paused_by: get_opt_text(row, 13)?,
        callback_url: get_opt_text(row, 14)?,
        last_heartbeat: get_opt_int(row, 15)?,
        created_at: get_opt_int(row, 16)?.unwrap_or(0),
        transport: get_opt_text(row, 17)?,
        metadata_json: get_opt_text(row, 18)?,
        agent_id: get_opt_text(row, 19)?,
    })
}

fn lifecycle_for_presence_transition(previous: &str, next: &str) -> Option<&'static str> {
    if previous == next {
        return None;
    }
    match next {
        "offline" => Some("offline"),
        "online" if previous == "offline" => Some("started"),
        _ => None,
    }
}

pub(crate) fn get_text(row: &libsql::Row, idx: i32) -> Result<String, NexusError> {
    Ok(get_opt_text(row, idx)?.unwrap_or_default())
}
pub(crate) fn get_opt_text(row: &libsql::Row, idx: i32) -> Result<Option<String>, NexusError> {
    match row.get_value(idx).map_err(store_err)? {
        libsql::Value::Text(s) => Ok(Some(s)),
        libsql::Value::Null => Ok(None),
        other => Ok(Some(format!("{other:?}"))),
    }
}
pub(crate) fn get_opt_int(row: &libsql::Row, idx: i32) -> Result<Option<i64>, NexusError> {
    match row.get_value(idx).map_err(store_err)? {
        libsql::Value::Integer(i) => Ok(Some(i)),
        libsql::Value::Null => Ok(None),
        _ => Ok(None),
    }
}
