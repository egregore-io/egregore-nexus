//! The [`Identity`] service: `impl IdentityPort` over the `nexus-store` identity/session repos +
//! an [`EventSink`] for `agent.spawned` (backend spec §5.1 register-once, §8 presence/tiers, §2.4
//! self-pause/resume). The service is the wire boundary — every method returns a
//! [`PortResult`], mapping the internal [`NexusError`] to a `ContractError`.
//!
//! During the identity/runtime migration, `sessions` remains the compatibility row while
//! `agents` carries durable identity and `agent_runtimes` carries the current process/session
//! representing that identity.

use std::sync::Arc;

use async_trait::async_trait;

use nexus_common::{hash_runtime_credential, now, Config, NexusError};
use nexus_contracts::admin::AssignProjectResponse;
use nexus_contracts::admin::AssignRoleResponse;
use nexus_contracts::admin::{
    AdminAssignRequest, AdminAssignResponse, AdminRenameRequest, AdminRenameResponse,
};
use nexus_contracts::enums::Presence;
use nexus_contracts::events::WsEvent;
use nexus_contracts::ids::{AgentId, SessionId};
use nexus_contracts::ports::{Caller, EventSink, IdentityPort, PortResult};
use nexus_contracts::register::{
    HeartbeatResponse, MemberListRequest, MemberListResponse, MemberSummary, RegisterRequest,
    RegisterResponse, RenameRequest, RenameResponse, StatusRequest, StatusResponse, StatusState,
    Whoami,
};
use nexus_store::repos::{
    AgentCredentials, AgentRuntimes, Agents, DeveloperEvents, IdentitySessions, NewAgent,
    NewAgentRuntime, NewDeveloperEvent, Sessions, AGENT_LIFECYCLE_TOPIC,
};
use nexus_store::types::{AgentRow, SessionRow};
use nexus_store::Store;

use crate::binding::{assert_project, caller_from_row, is_live, tier_str, whoami_from_row};
use crate::error::to_port;
use crate::presence::{is_stale, presence_for_state, presence_from_str};
use crate::registry::{resolve_register, RegisterOutcome};

/// The `<nexus>` startup directive returned on every register (backend §6): untagged content is
/// the agent's human, anything inside `<nexus …>` is bus traffic, and delivered batches carry
/// `from`/`target`/`receiver` attrs the agent reads instead of per-batch instruction prose
/// (addressee guidance is delivered once here, on wake or registration).
/// Operators can override the whole directive by writing `~/.nexus/startup-directive.txt`.
const STARTUP_DIRECTIVE: &str = "Untagged messages are from your human. Anything inside \
<nexus …> is bus traffic. Delivered batches name from, target, and receiver on every item; \
you are the session named by receiver= — act only on work addressed to you.";

/// Operator override file for the wake/register directive.
fn startup_directive_path() -> std::path::PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
    std::path::PathBuf::from(home)
        .join(".nexus")
        .join("startup-directive.txt")
}

/// Resolve the wake/register directive: if `~/.nexus/startup-directive.txt` exists and is
/// non-empty, its (trimmed) contents win; else the built-in text. Read per-register so an
/// operator can edit the file on a live daemon and every subsequent wake picks it up.
fn startup_directive() -> String {
    std::fs::read_to_string(startup_directive_path())
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| STARTUP_DIRECTIVE.to_string())
}

/// The identity service. Holds the shared store (sole writer) and a broadcast [`EventSink`] used to
/// emit `agent.spawned` on first register and `agent.status` on presence/pause changes.
pub struct Identity {
    store: Arc<Store>,
    events: Arc<dyn EventSink>,
    heartbeat_ttl_ms: i64,
}

impl Identity {
    /// Build an identity service over the shared store + event sink, taking the heartbeat TTL from
    /// [`Config`]. A stale heartbeat (older than the TTL) reports the member as `offline`.
    pub fn new(store: Arc<Store>, events: Arc<dyn EventSink>, config: &Config) -> Self {
        Identity {
            store,
            events,
            heartbeat_ttl_ms: config.heartbeat_ttl_ms,
        }
    }

    /// Load the caller's current row, asserting it still belongs to the caller's project.
    async fn row_for(&self, caller: &Caller) -> Result<SessionRow, NexusError> {
        if let Some(agent_id) = caller.agent_id.as_ref() {
            let row = self
                .session_for_agent_id(&agent_id.0)
                .await?
                .ok_or_else(|| NexusError::NotFound(format!("agent id {}", agent_id.0)))?;
            assert_project(&row, &caller.project)?;
            return Ok(row);
        }
        let repo = Sessions::new(&self.store);
        let row = repo
            .find_by_name(&caller.project, &caller.name)
            .await?
            .ok_or_else(|| NexusError::NotFound(caller.name.clone()))?;
        assert_project(&row, &caller.project)?;
        Ok(row)
    }

    /// Rebind a resumed session to a possibly-new `harness_session_id` (and refresh its
    /// `client_key`) and bring it back `online` (backend §5.1: resume rebinds, no duplicate).
    async fn rebind_resume(
        &self,
        row: &SessionRow,
        req: &RegisterRequest,
    ) -> Result<(), NexusError> {
        // Persist the (possibly new) harness binding + client_key + back-online presence in one
        // statement via the store connection (the daemon is the sole writer).
        self.store
            .conn
            .execute(
                "UPDATE sessions SET harness_session_id = ?2, client_key = ?3, agent = ?4, \
                 tier = ?5, presence = 'online', last_heartbeat = ?6 WHERE session_id = ?1",
                libsql::params![
                    row.session_id.0.clone(),
                    req.harness_session_id.clone(),
                    req.client_key.clone(),
                    req.harness.as_str().to_string(),
                    tier_str(req.tier).to_string(),
                    now()
                ],
            )
            .await
            .map_err(|e| NexusError::Store(e.to_string()))?;
        if let Some(name) = row.name.as_deref() {
            if let Err(error) = DeveloperEvents::new(&self.store)
                .append_agent_lifecycle(name, &row.session_id, "started", None, now())
                .await
            {
                tracing::warn!(
                    target: "nexus::identity",
                    name = name,
                    session = %row.session_id,
                    error = ?error,
                    "failed to append resume-started lifecycle telemetry"
                );
            } else {
                self.store.events().session_lifecycle_changed().signal();
            }
        }
        Ok(())
    }

    /// Resolve or create the durable agent row for a compatibility session row, then bind the
    /// current session id as that agent's active runtime.
    async fn bind_agent_runtime(
        &self,
        row: &SessionRow,
        req: &RegisterRequest,
        credential_verified: bool,
    ) -> Result<AgentId, NexusError> {
        let agents = Agents::new(&self.store);
        let row_name = row.require_name("agent runtime binding")?;
        let agent_id = if let Some(requested) = req.agent_id.as_ref() {
            if let Some(agent) = agents.find_by_id(&requested.0).await? {
                if agent.disabled_at.is_some() {
                    return Err(NexusError::Unauthorized);
                }
                if agent.name.as_deref() != Some(row_name) {
                    return Err(NexusError::Invalid(format!(
                        "agent_id {} is bound to {}, not {}",
                        requested.0,
                        agent.display_name(),
                        row_name
                    )));
                }
                agent.agent_id
            } else if let Some(existing_by_name) = agents.find_by_name(row_name).await? {
                if existing_by_name.agent_id != requested.0 {
                    return Err(NexusError::DuplicateName(row_name.to_string()));
                }
                existing_by_name.agent_id
            } else {
                agents
                    .create(NewAgent {
                        agent_id: requested.0.clone(),
                        project: row.project.clone(),
                        name: Some(row_name.to_string()),
                        default_harness: row.agent.clone(),
                        role: row.role.clone(),
                        tier: Some(row.tier.clone()),
                        owner: None,
                    })
                    .await?
            }
        } else if let Some(existing) = agents.find_by_name(row_name).await? {
            if existing.disabled_at.is_some() {
                return Err(NexusError::Unauthorized);
            }
            existing.agent_id
        } else {
            let generated = format!("a_{}", row.session_id.0);
            agents
                .create(NewAgent {
                    agent_id: generated,
                    project: row.project.clone(),
                    name: Some(row_name.to_string()),
                    default_harness: row.agent.clone(),
                    role: row.role.clone(),
                    tier: Some(row.tier.clone()),
                    owner: None,
                })
                .await?
        };

        if !credential_verified {
            self.enforce_runtime_credential(&agent_id, req).await?;
        }

        let runtimes = AgentRuntimes::new(&self.store);
        if let Some(existing_runtime) = runtimes.find_by_runtime_id(&row.session_id.0).await? {
            if existing_runtime.agent_id != agent_id {
                return Err(NexusError::Invalid(format!(
                    "runtime {} is bound to {}, not {}",
                    row.session_id.0, existing_runtime.agent_id, agent_id
                )));
            }
            runtimes.set_active(&row.session_id.0, true).await?;
            runtimes
                .set_presence(
                    &row.session_id.0,
                    // Preserve the row's current presence; an unset presence revives as online.
                    row.presence
                        .as_deref()
                        .map(|p| presence_from_str(Some(p)))
                        .unwrap_or(Presence::Online),
                )
                .await?;
        } else {
            runtimes
                .create(NewAgentRuntime {
                    runtime_id: row.session_id.0.clone(),
                    agent_id: agent_id.clone(),
                    harness: row.agent.clone().unwrap_or_else(|| "unknown".into()),
                    cwd: row.cwd.clone(),
                    transport: row.transport.clone(),
                    presence: Some(row.presence.clone().unwrap_or_else(|| "online".into())),
                    active: true,
                })
                .await?;
        }

        Ok(AgentId(agent_id))
    }

    async fn preflight_requested_agent(&self, req: &RegisterRequest) -> Result<bool, NexusError> {
        let Some(requested) = req.agent_id.as_ref() else {
            return Ok(false);
        };
        let agent = Agents::new(&self.store)
            .find_by_id(&requested.0)
            .await?
            .ok_or_else(|| NexusError::NotFound(format!("agent:{}", requested.0)))?;
        if agent.disabled_at.is_some() {
            return Err(NexusError::Unauthorized);
        }
        let req_name = req.name.as_deref().ok_or_else(|| {
            NexusError::Invalid("register name is required before whoami staging is active".into())
        })?;
        if agent.name.as_deref() != Some(req_name) {
            return Err(NexusError::Invalid(format!(
                "agent_id {} is bound to {}, not {}",
                requested.0,
                agent.display_name(),
                req_name
            )));
        }
        self.enforce_runtime_credential(&agent.agent_id, req)
            .await?;
        Ok(true)
    }

    async fn enforce_runtime_credential(
        &self,
        agent_id: &str,
        req: &RegisterRequest,
    ) -> Result<(), NexusError> {
        match req.runtime_credential.as_deref() {
            Some(secret) => self.verify_runtime_credential(agent_id, secret).await,
            None if req.agent_id.is_some() => Err(NexusError::Unauthorized),
            None => Ok(()),
        }
    }

    async fn verify_runtime_credential(
        &self,
        agent_id: &str,
        secret: &str,
    ) -> Result<(), NexusError> {
        let expected = hash_runtime_credential(secret);
        let credentials = AgentCredentials::new(&self.store)
            .find_active(agent_id)
            .await?;
        for credential in credentials {
            if credential.secret_hash == expected
                && scopes_allow_runtime_register(&credential.scopes_json)
            {
                AgentCredentials::new(&self.store)
                    .touch_last_used(&credential.credential_id)
                    .await?;
                return Ok(());
            }
        }
        Err(NexusError::Unauthorized)
    }

    async fn agent_id_for_name(&self, name: &str) -> Result<Option<AgentId>, NexusError> {
        Ok(Agents::new(&self.store)
            .find_by_name(name)
            .await?
            .map(|row| AgentId(row.agent_id)))
    }

    /// Atomically rename the compatibility session, durable agent identity, and legacy
    /// name-keyed membership rows that still power thread/topic/group read surfaces.
    async fn rename_identity_refs(
        &self,
        session: Option<&SessionId>,
        project: &str,
        previous: Option<&str>,
        new_name: &str,
        agent_id: Option<&AgentId>,
        evict_agent_id: Option<&str>,
    ) -> Result<(), NexusError> {
        let agent_id = agent_id.map(|id| id.0.clone());
        let previous = previous.map(str::to_string);

        if self.store.has_split_authority() {
            self.rename_durable_identity_refs(agent_id.as_deref(), evict_agent_id, new_name)
                .await?;
            if let Err(error) = self
                .rename_transport_identity_refs(
                    session,
                    project,
                    previous.as_deref(),
                    new_name,
                    agent_id.as_deref(),
                    evict_agent_id,
                )
                .await
            {
                if let Err(compensation) = self
                    .compensate_durable_identity_refs(
                        agent_id.as_deref(),
                        previous.as_deref(),
                        evict_agent_id,
                        new_name,
                    )
                    .await
                {
                    return Err(NexusError::Store(format!(
                        "{error}; durable rename compensation failed: {compensation}"
                    )));
                }
                return Err(error);
            }
            return Ok(());
        }

        let tx = self.store.begin_write_txn("identity_rename_refs").await?;

        // admin.assign eviction: the dead previous holder returns to the staged (unnamed)
        // surface in the SAME transaction that binds the name to the assignee, so the name is
        // never observably free in between (no register can steal it mid-assign).
        if let Some(evict) = evict_agent_id {
            tx.execute(
                "UPDATE agents SET name = NULL WHERE agent_id = ?1",
                libsql::params![evict],
            )
            .await?;
            // Also match by the name itself: legacy session rows may predate the agent_id
            // backfill, and the caller has already verified the name's owner is evictable.
            tx.execute(
                "UPDATE sessions SET name = NULL WHERE agent_id = ?1 OR name = ?2",
                libsql::params![evict, new_name],
            )
            .await?;
        }

        if let Some(session) = session {
            tx.execute(
                "UPDATE sessions SET name = ?2 WHERE session_id = ?1",
                libsql::params![session.0.clone(), new_name],
            )
            .await?;
        }

        if let Some(agent_id) = agent_id.as_ref() {
            tx.execute(
                "UPDATE agents SET name = ?2 WHERE agent_id = ?1",
                libsql::params![agent_id.clone(), new_name],
            )
            .await?;
        }

        tx.execute(
            "DELETE FROM thread_members \
             WHERE session_name = ?2 AND thread_id IN ( \
               SELECT thread_id FROM thread_members WHERE session_name = ?1 \
               UNION \
               SELECT thread_id FROM thread_members WHERE agent_id = ?3 \
             )",
            libsql::params![previous.clone(), new_name, agent_id.clone()],
        )
        .await?;
        tx.execute(
            "UPDATE thread_members \
             SET session_name = ?2, agent_id = COALESCE(agent_id, ?3) \
             WHERE session_name = ?1 OR agent_id = ?3",
            libsql::params![previous.clone(), new_name, agent_id.clone()],
        )
        .await?;

        tx.execute(
            "DELETE FROM subscriptions \
             WHERE subscriber_session = ?2 AND topic IN ( \
               SELECT topic FROM subscriptions WHERE subscriber_session = ?1 \
             )",
            libsql::params![previous.clone(), new_name],
        )
        .await?;
        tx.execute(
            "UPDATE subscriptions SET subscriber_session = ?2 WHERE subscriber_session = ?1",
            libsql::params![previous.clone(), new_name],
        )
        .await?;

        tx.execute(
            "UPDATE agent_group_members \
             SET agent_name = ?4 \
             WHERE project = ?1 AND (agent_name = ?2 OR agent_id = ?3)",
            libsql::params![project, previous, agent_id, new_name],
        )
        .await?;

        tx.commit().await?;
        Ok(())
    }

    async fn rename_durable_identity_refs(
        &self,
        agent_id: Option<&str>,
        evict_agent_id: Option<&str>,
        new_name: &str,
    ) -> Result<(), NexusError> {
        let tx = self
            .store
            .begin_identity_write_txn("identity_rename_durable")
            .await?;
        if let Some(evict) = evict_agent_id {
            tx.execute(
                "UPDATE agents SET name = NULL WHERE agent_id = ?1",
                libsql::params![evict],
            )
            .await?;
        }
        if let Some(agent_id) = agent_id {
            tx.execute(
                "UPDATE agents SET name = ?2 WHERE agent_id = ?1",
                libsql::params![agent_id, new_name],
            )
            .await?;
        }
        tx.commit().await
    }

    async fn rename_transport_identity_refs(
        &self,
        session: Option<&SessionId>,
        project: &str,
        previous: Option<&str>,
        new_name: &str,
        agent_id: Option<&str>,
        evict_agent_id: Option<&str>,
    ) -> Result<(), NexusError> {
        let tx = self
            .store
            .begin_write_txn("identity_rename_transport")
            .await?;
        if let Some(evict) = evict_agent_id {
            tx.execute(
                "UPDATE sessions SET name = NULL WHERE agent_id = ?1 OR name = ?2",
                libsql::params![evict, new_name],
            )
            .await?;
        }
        if let Some(session) = session {
            tx.execute(
                "UPDATE sessions SET name = ?2 WHERE session_id = ?1",
                libsql::params![session.0.clone(), new_name],
            )
            .await?;
        }
        tx.execute(
            "DELETE FROM thread_members
             WHERE session_name = ?2 AND thread_id IN (
               SELECT thread_id FROM thread_members WHERE session_name = ?1
               UNION
               SELECT thread_id FROM thread_members WHERE agent_id = ?3
             )",
            libsql::params![previous, new_name, agent_id],
        )
        .await?;
        tx.execute(
            "UPDATE thread_members
             SET session_name = ?2, agent_id = COALESCE(agent_id, ?3)
             WHERE session_name = ?1 OR agent_id = ?3",
            libsql::params![previous, new_name, agent_id],
        )
        .await?;
        tx.execute(
            "DELETE FROM subscriptions
             WHERE subscriber_session = ?2 AND topic IN (
               SELECT topic FROM subscriptions WHERE subscriber_session = ?1
             )",
            libsql::params![previous, new_name],
        )
        .await?;
        tx.execute(
            "UPDATE subscriptions SET subscriber_session = ?2 WHERE subscriber_session = ?1",
            libsql::params![previous, new_name],
        )
        .await?;
        tx.execute(
            "UPDATE agent_group_members
             SET agent_name = ?4
             WHERE project = ?1 AND (agent_name = ?2 OR agent_id = ?3)",
            libsql::params![project, previous, agent_id, new_name],
        )
        .await?;
        tx.commit().await
    }

    async fn compensate_durable_identity_refs(
        &self,
        agent_id: Option<&str>,
        previous: Option<&str>,
        evict_agent_id: Option<&str>,
        new_name: &str,
    ) -> Result<(), NexusError> {
        let tx = self
            .store
            .begin_identity_write_txn("identity_rename_compensate")
            .await?;
        if let Some(agent_id) = agent_id {
            tx.execute(
                "UPDATE agents SET name = ?2 WHERE agent_id = ?1",
                libsql::params![agent_id, previous],
            )
            .await?;
        }
        if let Some(evict) = evict_agent_id {
            tx.execute(
                "UPDATE agents SET name = ?2 WHERE agent_id = ?1",
                libsql::params![evict, new_name],
            )
            .await?;
        }
        tx.commit().await
    }

    /// Liveness gate for `admin.assign` takeover: a holder is live when it is not dead-marked
    /// AND has an online session or an active runtime. Dead-marked holders are takeable
    /// regardless of stale presence rows.
    async fn holder_is_live(&self, agent_id: &str) -> Result<bool, NexusError> {
        let (lifecycle, _) = Agents::new(&self.store).lifecycle_for_id(agent_id).await?;
        if lifecycle.as_deref() == Some("dead") {
            return Ok(false);
        }
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT 1 FROM sessions WHERE agent_id = ?1 AND presence = 'online' LIMIT 1",
                libsql::params![agent_id],
            )
            .await
            .map_err(|e| NexusError::Store(e.to_string()))?;
        if rows
            .next()
            .await
            .map_err(|e| NexusError::Store(e.to_string()))?
            .is_some()
        {
            return Ok(true);
        }
        Ok(AgentRuntimes::new(&self.store)
            .active_for_agent(agent_id)
            .await?
            .is_some())
    }

    async fn session_for_agent_id(&self, agent_id: &str) -> Result<Option<SessionRow>, NexusError> {
        let sessions = Sessions::new(&self.store);
        match sessions.active_runtime_session_for_agent(agent_id).await? {
            Some(row) => Ok(Some(row)),
            None => sessions.find_by_agent_id(agent_id).await,
        }
    }

    async fn resolve_admin_rename_source(
        &self,
        source: &str,
    ) -> Result<(Option<SessionRow>, AgentRow), NexusError> {
        let sessions = Sessions::new(&self.store);
        let agents = Agents::new(&self.store);
        if source.starts_with("s_") {
            let row = sessions
                .find_by_session_id(&SessionId(source.to_string()))
                .await?
                .ok_or_else(|| NexusError::NotFound(format!("session id {source}")))?;
            let agent_id = match row.agent_id.as_deref() {
                Some(agent_id) => agent_id.to_string(),
                None => {
                    let name = row.require_name("admin rename")?;
                    agents
                        .find_by_name(name)
                        .await?
                        .ok_or_else(|| NexusError::NotFound(format!("agent:{name}")))?
                        .agent_id
                }
            };
            let agent = agents
                .find_by_id(&agent_id)
                .await?
                .ok_or_else(|| NexusError::NotFound(format!("agent id {agent_id}")))?;
            return Ok((Some(row), agent));
        }

        if source.starts_with("a_") {
            let agent = agents
                .find_by_id(source)
                .await?
                .ok_or_else(|| NexusError::NotFound(format!("agent id {source}")))?;
            let row = self.session_for_agent_id(&agent.agent_id).await?;
            return Ok((row, agent));
        }

        if let Some(agent) = agents.find_by_name(source).await? {
            let row = self.session_for_agent_id(&agent.agent_id).await?;
            return Ok((row, agent));
        }

        let row = sessions
            .find_by_name_any_project(source)
            .await?
            .ok_or_else(|| NexusError::NotFound(source.to_string()))?;
        let agent_id = match row.agent_id.as_deref() {
            Some(agent_id) => agent_id.to_string(),
            None => {
                let name = row.require_name("admin rename")?;
                agents
                    .find_by_name(name)
                    .await?
                    .ok_or_else(|| NexusError::NotFound(format!("agent:{name}")))?
                    .agent_id
            }
        };
        let agent = agents
            .find_by_id(&agent_id)
            .await?
            .ok_or_else(|| NexusError::NotFound(format!("agent id {agent_id}")))?;
        Ok((Some(row), agent))
    }

    async fn ensure_rename_target_available(
        &self,
        target: &str,
        session_id: Option<&SessionId>,
        agent_id: Option<&AgentId>,
    ) -> Result<(), NexusError> {
        if let Some(agent) = Agents::new(&self.store).find_by_name(target).await? {
            let same_agent = agent_id.is_some_and(|agent_id| agent.agent_id == agent_id.0.as_str());
            if !same_agent {
                return Err(NexusError::DuplicateName(format!("{target} already bound")));
            }
        }
        if let Some(session) = Sessions::new(&self.store)
            .find_by_name_any_project(target)
            .await?
        {
            if !matches!(session_id, Some(session_id) if session.session_id == *session_id) {
                return Err(NexusError::DuplicateName(format!("{target} already bound")));
            }
        }
        Ok(())
    }

    async fn touch_runtime_presence(
        &self,
        session: &SessionId,
        presence: Presence,
    ) -> Result<(), NexusError> {
        let runtimes = AgentRuntimes::new(&self.store);
        if runtimes.find_by_runtime_id(&session.0).await?.is_some() {
            runtimes.set_presence(&session.0, presence).await?;
        }
        Ok(())
    }

    /// Emit a metadata-only developer lifecycle event for agent registrations/resumes. Human
    /// sessions remain outside the `sys.agent.lifecycle` stream.
    async fn append_agent_lifecycle(
        &self,
        row: &SessionRow,
        lifecycle: &str,
    ) -> Result<(), NexusError> {
        if row.kind != "agent" {
            return Ok(());
        }
        if let Some(name) = row.name.as_deref() {
            DeveloperEvents::new(&self.store)
                .append_agent_lifecycle(name, &row.session_id, lifecycle, None, now())
                .await?;
            self.store.events().session_lifecycle_changed().signal();
        }
        Ok(())
    }

    /// Best-effort wrapper for registration lifecycle telemetry. Registration/resume is the
    /// load-bearing bus path; a developer-event hiccup must never block it.
    async fn append_agent_lifecycle_best_effort(&self, row: &SessionRow, lifecycle: &str) {
        if let Err(error) = self.append_agent_lifecycle(row, lifecycle).await {
            tracing::warn!(
                target: "nexus::identity",
                name = %row.display_name(),
                session = %row.session_id,
                lifecycle,
                error = ?error,
                "failed to append identity lifecycle telemetry"
            );
        }
    }

    async fn append_agent_lifecycle_with_data_best_effort(
        &self,
        row: &SessionRow,
        agent_name: &str,
        lifecycle: &str,
        data: serde_json::Value,
    ) {
        if row.kind != "agent" {
            return;
        }
        let data_json = match serde_json::to_string(&data) {
            Ok(data_json) => data_json,
            Err(error) => {
                tracing::warn!(
                    target: "nexus::identity",
                    name = agent_name,
                    session = %row.session_id,
                    lifecycle,
                    error = ?error,
                    "failed to encode identity lifecycle telemetry"
                );
                return;
            }
        };
        if let Err(error) = DeveloperEvents::new(&self.store)
            .append_agent_lifecycle_with_data(
                agent_name,
                &row.session_id,
                lifecycle,
                None,
                Some(&data_json),
                now(),
            )
            .await
        {
            tracing::warn!(
                target: "nexus::identity",
                name = agent_name,
                session = %row.session_id,
                lifecycle,
                error = ?error,
                "failed to append identity lifecycle telemetry"
            );
            return;
        }
        self.store.events().session_lifecycle_changed().signal();
    }

    async fn append_admin_rename_lifecycle_best_effort(
        &self,
        row: Option<&SessionRow>,
        agent_name: &str,
        data: serde_json::Value,
    ) {
        if row.is_some_and(|row| row.kind != "agent") {
            return;
        }
        let data_json = match serde_json::to_string(&data) {
            Ok(data_json) => data_json,
            Err(error) => {
                tracing::warn!(
                    target: "nexus::identity",
                    name = agent_name,
                    error = ?error,
                    "failed to encode admin rename lifecycle telemetry"
                );
                return;
            }
        };
        if let Err(error) = DeveloperEvents::new(&self.store)
            .append(NewDeveloperEvent {
                topic: AGENT_LIFECYCLE_TOPIC.to_string(),
                kind: "agent_lifecycle".to_string(),
                message_id: None,
                thread_name: None,
                dm_name: None,
                from_name: None,
                agent_name: Some(agent_name.to_string()),
                session_id: row.map(|row| row.session_id.0.clone()),
                lifecycle: Some("rename".to_string()),
                current_work: None,
                data_json: Some(data_json),
                created_at: now(),
            })
            .await
        {
            tracing::warn!(
                target: "nexus::identity",
                name = agent_name,
                error = ?error,
                "failed to append admin rename lifecycle telemetry"
            );
            return;
        }
        self.store.events().session_lifecycle_changed().signal();
    }
}

fn scopes_allow_runtime_register(scopes_json: &str) -> bool {
    serde_json::from_str::<Vec<String>>(scopes_json)
        .map(|scopes| scopes.iter().any(|scope| scope == "runtime:register"))
        .unwrap_or(false)
}

#[async_trait]
impl IdentityPort for Identity {
    async fn register(&self, req: RegisterRequest) -> PortResult<RegisterResponse> {
        let result: Result<RegisterResponse, NexusError> = async {
            let _transition = self.store.lock_presence_transition().await;
            let credential_verified = self.preflight_requested_agent(&req).await?;
            let outcome = resolve_register(&self.store, &req).await?;
            let (session_id, lifecycle, publish_resume_status, publish_spawn) = match outcome {
                RegisterOutcome::Resumed(row) => {
                    let publish = row.presence.as_deref() != Some("online");
                    self.rebind_resume(&row, &req).await?;
                    (row.session_id, "resume", publish, false)
                }
                RegisterOutcome::Created(row) => (row.session_id, "register", false, true),
            };
            let row = Sessions::new(&self.store)
                .find_by_session_id(&session_id)
                .await?
                .ok_or_else(|| NexusError::NotFound(session_id.0.clone()))?;
            let agent_id = self
                .bind_agent_runtime(&row, &req, credential_verified)
                .await?;
            // Stamp the durable identity on the session row itself. Fan-out and resolve
            // paths fall back to `sessions.agent_id` when the runtime row is stopped or
            // flapping; a NULL here made that fallback structurally dead for CLI-registered
            // sessions and silently dropped them from thread fan-out (gate-caught
            // 2026-07-10, N17).
            if row.agent_id.as_deref() != Some(agent_id.0.as_str()) {
                Sessions::new(&self.store)
                    .set_agent_id(&row.session_id, &agent_id.0)
                    .await?;
            }
            if publish_spawn {
                // Publish the first spawn only after the stable identity and runtime binding have
                // committed. Gateway runtime projections require the durable agent id; emitting
                // the pre-bind session row would create an unprocessable ordered poison event.
                self.events
                    .emit(WsEvent::AgentSpawned {
                        session_id: SessionId(row.session_id.0.clone()),
                        name: row.name.clone(),
                        agent_id: Some(agent_id.0.clone()),
                    })
                    .await;
            }
            self.append_agent_lifecycle_best_effort(&row, lifecycle)
                .await;
            if publish_resume_status && row.kind == "agent" {
                self.events
                    .emit(WsEvent::AgentStatus {
                        session_id: row.session_id.clone(),
                        presence: Presence::Online,
                        paused: row.paused,
                    })
                    .await;
            }
            Ok(RegisterResponse {
                agent_id: Some(agent_id),
                session_id,
                directive: startup_directive(),
            })
        }
        .await;
        to_port(result)
    }

    async fn whoami(&self, caller: &Caller) -> PortResult<Whoami> {
        let result: Result<Whoami, NexusError> = async {
            let row = self.row_for(caller).await?;
            let mut who = whoami_from_row(&row);
            who.agent_id = caller.agent_id.clone();
            if who.agent_id.is_none() {
                if let Some(name) = row.name.as_deref() {
                    who.agent_id = self.agent_id_for_name(name).await?;
                }
            }
            Ok(who)
        }
        .await;
        to_port(result)
    }

    async fn resolve(&self, project: &str, name: &str) -> PortResult<Caller> {
        let result: Result<Caller, NexusError> = async {
            let repo = Sessions::new(&self.store);
            let row = repo
                .find_by_name(project, name)
                .await?
                .ok_or_else(|| NexusError::NotFound(name.to_string()))?;
            assert_project(&row, project)?;
            let mut caller = caller_from_row(&row);
            if let Some(agent_id) = self.agent_id_for_name(name).await? {
                if let Some(runtime) = AgentRuntimes::new(&self.store)
                    .active_for_agent(&agent_id.0)
                    .await?
                {
                    caller.session = SessionId(runtime.runtime_id);
                }
                caller.agent_id = Some(agent_id);
            }
            Ok(caller)
        }
        .await;
        to_port(result)
    }

    async fn members(
        &self,
        caller: &Caller,
        req: MemberListRequest,
    ) -> PortResult<MemberListResponse> {
        let result: Result<MemberListResponse, NexusError> = async {
            let repo = Sessions::new(&self.store);
            let rows = repo.list(&caller.project).await?;
            let ts = now();
            let include_offline = req.include_offline.unwrap_or(false);
            let include_dead = req.include_dead.unwrap_or(false);
            let mut members = Vec::new();
            for row in rows {
                // A stale heartbeat downgrades effective presence to offline (backend §8).
                // Birth counts as the first heartbeat (D9/N22): fresh launches must not
                // read as offline before their harness's first own heartbeat.
                let stale = is_stale(
                    row.last_heartbeat.or(Some(row.created_at)),
                    ts,
                    self.heartbeat_ttl_ms,
                );
                let presence = if stale {
                    Presence::Offline
                } else {
                    presence_from_str(row.presence.as_deref())
                };
                if presence == Presence::Offline && !include_offline {
                    continue;
                }
                let agent_id = if let Some(name) = row.name.as_deref() {
                    self.agent_id_for_name(name)
                        .await?
                        .or_else(|| row.agent_id.clone().map(AgentId))
                } else {
                    row.agent_id.clone().map(AgentId)
                };
                // Dead-marking is durable lifecycle state, separate
                // from presence. Dead rows leave the default roster; audit passes include_dead.
                let (lifecycle_state, dead_reason) = match agent_id.as_ref() {
                    Some(id) => {
                        nexus_store::repos::Agents::new(&self.store)
                            .lifecycle_for_id(&id.0)
                            .await?
                    }
                    None => (None, None),
                };
                if lifecycle_state.as_deref() == Some("dead") && !include_dead {
                    continue;
                }
                members.push(MemberSummary {
                    agent_id,
                    name: row.name,
                    session_id: row.session_id,
                    agent: row.agent,
                    role: row.role,
                    presence,
                    current_work: row.current_work,
                    lifecycle_state,
                    dead_reason,
                });
            }
            Ok(MemberListResponse { members })
        }
        .await;
        to_port(result)
    }

    async fn status(&self, caller: &Caller, req: StatusRequest) -> PortResult<StatusResponse> {
        let result: Result<StatusResponse, NexusError> = async {
            let _transition = self.store.lock_presence_transition().await;
            let repo = Sessions::new(&self.store);
            let row = self.row_for(caller).await?;

            // No fields set → just echo current status (backend §6).
            let mut presence = presence_from_str(row.presence.as_deref());
            let mut paused = row.paused;
            let mut current_work = req.work.clone().or(row.current_work.clone());
            let mut publish_status = false;

            if let Some(state) = req.state {
                presence = presence_for_state(state);
                // Self-pause / self-resume: both set the same `paused` flag, audited by source
                // (backend §2.4). The source here is the agent itself.
                let want_paused = matches!(state, StatusState::Paused);
                if want_paused != paused {
                    repo.set_paused(&row.session_id, want_paused, Some("self"))
                        .await?;
                    paused = want_paused;
                }
                repo.set_presence(&row.session_id, presence).await?;
                self.touch_runtime_presence(&row.session_id, presence)
                    .await?;
                publish_status =
                    presence_from_str(row.presence.as_deref()) != presence || row.paused != paused;
            }

            if let Some(work) = req.work.as_ref() {
                publish_status |= row.current_work.as_deref() != Some(work.as_str());
                repo.set_current_work(&row.session_id, Some(work)).await?;
                current_work = Some(work.clone());
            }

            if publish_status && row.kind == "agent" {
                self.events
                    .emit(WsEvent::AgentStatus {
                        session_id: SessionId(row.session_id.0.clone()),
                        presence,
                        paused,
                    })
                    .await;
            }

            Ok(StatusResponse {
                presence,
                current_work,
                paused,
            })
        }
        .await;
        to_port(result)
    }

    async fn heartbeat(&self, caller: &Caller) -> PortResult<HeartbeatResponse> {
        let result: Result<HeartbeatResponse, NexusError> = async {
            let _transition = self.store.lock_presence_transition().await;
            let repo = Sessions::new(&self.store);
            let row = self.row_for(caller).await?;
            repo.touch_heartbeat(&row.session_id).await?;
            // A heartbeat from an offline (but not paused) row brings it back online.
            let mut restored_online = false;
            if !is_live(&row) && !row.paused {
                repo.set_presence(&row.session_id, Presence::Online).await?;
                self.touch_runtime_presence(&row.session_id, Presence::Online)
                    .await?;
                restored_online = true;
            } else {
                self.touch_runtime_presence(
                    &row.session_id,
                    presence_from_str(row.presence.as_deref()),
                )
                .await?;
            }
            if restored_online && row.kind == "agent" {
                self.events
                    .emit(WsEvent::AgentStatus {
                        session_id: row.session_id,
                        presence: Presence::Online,
                        paused: false,
                    })
                    .await;
            }
            Ok(HeartbeatResponse { ok: true })
        }
        .await;
        to_port(result)
    }

    async fn assign_project(
        &self,
        name: &str,
        to_project: &str,
    ) -> PortResult<AssignProjectResponse> {
        let result: Result<AssignProjectResponse, NexusError> = async {
            let repo = Sessions::new(&self.store);

            // Locate the session across any project.
            let row = repo
                .find_by_name_any_project(name)
                .await?
                .ok_or_else(|| NexusError::NotFound(name.to_string()))?;

            if row.project == to_project {
                return Ok(AssignProjectResponse {
                    name: Some(name.to_string()),
                    project: to_project.to_string(),
                });
            }

            // Collision guard: the target project must not already hold this name.
            if repo.name_exists_in(to_project, name).await? {
                return Err(NexusError::DuplicateName(format!(
                    "{name} already bound in project {to_project}"
                )));
            }

            // Mutate the project column on both the compatibility session and the durable agent
            // row in a single transaction (same pattern as `rename_identity_refs`) so a crash
            // between the two updates can never leave sessions.project and agents.project
            // disagreeing. The daemon is the sole writer.
            let tx = self
                .store
                .begin_write_txn("identity_assign_project")
                .await?;
            tx.execute(
                "UPDATE sessions SET project = ?2 WHERE session_id = ?1",
                libsql::params![row.session_id.0.clone(), to_project],
            )
            .await?;
            tx.execute(
                "UPDATE agents SET project = ?2 WHERE name = ?1",
                libsql::params![name, to_project],
            )
            .await?;
            tx.commit().await?;

            // Emit a resync: AgentSpawned signals the new project's member list has changed.
            // (Same event used on first register — the UI reconciles the member roster on it.)
            self.events
                .emit(WsEvent::AgentSpawned {
                    session_id: SessionId(row.session_id.0.clone()),
                    name: Some(name.to_string()),
                    agent_id: row.agent_id.clone(),
                })
                .await;

            Ok(AssignProjectResponse {
                name: Some(name.to_string()),
                project: to_project.to_string(),
            })
        }
        .await;
        to_port(result)
    }

    async fn assign_role(&self, name: &str, role: &str) -> PortResult<AssignRoleResponse> {
        let result: Result<AssignRoleResponse, NexusError> = async {
            let role = role.trim();
            if role.is_empty() {
                return Err(NexusError::Invalid("role must not be empty".into()));
            }

            let agents = Agents::new(&self.store);
            let agent = agents
                .find_by_name(name)
                .await?
                .ok_or_else(|| NexusError::NotFound(name.to_string()))?;
            agents.set_role(&agent.agent_id, role).await?;

            if let Some(session) = Sessions::new(&self.store)
                .find_by_name(&agent.project, name)
                .await?
            {
                Sessions::new(&self.store)
                    .set_role(&session.session_id, role)
                    .await?;
            }

            Ok(AssignRoleResponse {
                name: agent.name,
                role: role.to_string(),
            })
        }
        .await;
        to_port(result)
    }

    async fn rename(&self, caller: &Caller, req: RenameRequest) -> PortResult<RenameResponse> {
        let result: Result<RenameResponse, NexusError> = async {
            let repo = Sessions::new(&self.store);

            // The caller renames ITSELF — locate by the caller's bound session id.
            let row = repo
                .find_by_session_id(&caller.session)
                .await?
                .ok_or_else(|| NexusError::NotFound(caller.name.clone()))?;

            let new_name = req.name.trim().to_string();
            if new_name.is_empty() {
                return Err(NexusError::Invalid("name must not be empty".into()));
            }
            // Staged (unnamed) callers may claim their FIRST name themselves, as long as the
            // name is free. `previous = None` is the self-service
            // first-naming path; the availability guard below is the only gate.
            let previous = row.name.clone();
            if previous.as_deref() == Some(new_name.as_str()) {
                return Ok(RenameResponse {
                    name: new_name,
                    previous,
                });
            }

            let agent_id = match caller.agent_id.clone() {
                Some(id) => Some(id),
                None => match row.agent_id.as_deref() {
                    Some(id) => Some(AgentId(id.to_string())),
                    None => match previous.as_deref() {
                        Some(previous) => self.agent_id_for_name(previous).await?,
                        None => None,
                    },
                },
            };
            self.ensure_rename_target_available(
                &new_name,
                Some(&row.session_id),
                agent_id.as_ref(),
            )
            .await?;
            self.rename_identity_refs(
                Some(&row.session_id),
                &row.project,
                previous.as_deref(),
                &new_name,
                agent_id.as_ref(),
                None,
            )
            .await?;
            self.append_agent_lifecycle_with_data_best_effort(
                &row,
                &new_name,
                "rename",
                serde_json::json!({
                    "oldName": previous.clone(),
                    "newName": new_name.clone(),
                }),
            )
            .await;

            // Resync the roster everywhere (same event the UI reconciles names on at register).
            self.events
                .emit(WsEvent::AgentSpawned {
                    session_id: SessionId(row.session_id.0.clone()),
                    name: Some(new_name.clone()),
                    agent_id: agent_id.as_ref().map(|id| id.0.clone()),
                })
                .await;

            Ok(RenameResponse {
                name: new_name,
                previous,
            })
        }
        .await;
        to_port(result)
    }

    async fn admin_rename(
        &self,
        _caller: &Caller,
        req: AdminRenameRequest,
    ) -> PortResult<AdminRenameResponse> {
        let result: Result<AdminRenameResponse, NexusError> = async {
            let source = req.source.trim();
            let new_name = req.target.trim().to_string();
            if source.is_empty() {
                return Err(NexusError::Invalid(
                    "source must not be empty (usage: nexus admin rename <source> <new-name>)"
                        .into(),
                ));
            }
            if new_name.is_empty() {
                return Err(NexusError::Invalid(
                    "target name must not be empty (usage: nexus admin rename <source> <new-name>)"
                        .into(),
                ));
            }

            let (row, agent) = self.resolve_admin_rename_source(source).await?;
            let agent_id = AgentId(agent.agent_id.clone());
            let session_id = row.as_ref().map(|row| row.session_id.clone());
            let previous = agent
                .name
                .clone()
                .or_else(|| row.as_ref().and_then(|row| row.name.clone()));
            if previous.as_deref() == Some(new_name.as_str()) {
                return Ok(AdminRenameResponse {
                    agent_id,
                    session_id,
                    name: new_name,
                    previous,
                });
            }

            self.ensure_rename_target_available(&new_name, session_id.as_ref(), Some(&agent_id))
                .await?;
            self.rename_identity_refs(
                session_id.as_ref(),
                &agent.project,
                previous.as_deref(),
                &new_name,
                Some(&agent_id),
                None,
            )
            .await?;
            self.append_admin_rename_lifecycle_best_effort(
                row.as_ref(),
                &new_name,
                serde_json::json!({
                    "oldName": previous.clone(),
                    "previous": previous.clone(),
                    "newName": new_name.clone(),
                }),
            )
            .await;

            if let Some(session_id) = session_id.as_ref() {
                self.events
                    .emit(WsEvent::AgentSpawned {
                        session_id: SessionId(session_id.0.clone()),
                        name: Some(new_name.clone()),
                        agent_id: Some(agent_id.0.clone()),
                    })
                    .await;
            }

            Ok(AdminRenameResponse {
                agent_id,
                session_id,
                name: new_name,
                previous,
            })
        }
        .await;
        to_port(result)
    }

    async fn admin_assign(
        &self,
        _caller: &Caller,
        req: AdminAssignRequest,
    ) -> PortResult<AdminAssignResponse> {
        let result: Result<AdminAssignResponse, NexusError> = async {
            let new_name = req.name.trim().to_string();
            if new_name.is_empty() {
                return Err(NexusError::Invalid("name must not be empty".into()));
            }
            let id = req.id.trim();
            // Names are ambiguous here by design (the assignee has none); ids only.
            if !(id.starts_with("a_") || id.starts_with("s_")) {
                return Err(NexusError::Invalid(
                    "assignee must be a stable a_* agent id or exact s_* session id \
                     (usage: nexus admin assign <id> <name> — the staged id comes first, \
                     then the name to assume)"
                        .into(),
                ));
            }

            let (row, agent) = self.resolve_admin_rename_source(id).await?;
            let already_named = agent
                .name
                .clone()
                .or_else(|| row.as_ref().and_then(|row| row.name.clone()));
            if let Some(current) = already_named {
                return Err(NexusError::Invalid(format!(
                    "assignee {id} is already named '{current}'; admin assign is the \
                     staged-identity surface — use admin rename"
                )));
            }
            let agent_id = AgentId(agent.agent_id.clone());
            let session_id = row.as_ref().map(|row| row.session_id.clone());

            // Takeover gate: a name held by a LIVE agent is never assignable. Dead-marked
            // holders, and holders with no online session and no active runtime, are evictable
            // back to the staged (unnamed) surface.
            let agents = Agents::new(&self.store);
            let mut evicted_agent_id: Option<AgentId> = None;
            if let Some(holder) = agents.find_by_name(&new_name).await? {
                if holder.agent_id == agent.agent_id {
                    return Err(NexusError::Invalid(
                        "assignee already holds this name".into(),
                    ));
                }
                if self.holder_is_live(&holder.agent_id).await? {
                    return Err(NexusError::DuplicateName(format!(
                        "{new_name} is held by a live agent; admin assign only takes over \
                         dead or fully-stopped owners"
                    )));
                }
                evicted_agent_id = Some(AgentId(holder.agent_id.clone()));
            } else if let Some(session) = Sessions::new(&self.store)
                .find_by_name_any_project(&new_name)
                .await?
            {
                // Legacy name-only session with no durable agents row: too ambiguous to evict
                // implicitly — free it explicitly first.
                return Err(NexusError::DuplicateName(format!(
                    "{new_name} is bound to session {} with no durable agent row; free it \
                     with admin rename first",
                    session.session_id.0
                )));
            }

            self.rename_identity_refs(
                session_id.as_ref(),
                &agent.project,
                None,
                &new_name,
                Some(&agent_id),
                evicted_agent_id.as_ref().map(|id| id.0.as_str()),
            )
            .await?;

            if let Some(evicted) = evicted_agent_id.as_ref() {
                self.append_admin_rename_lifecycle_best_effort(
                    None,
                    &new_name,
                    serde_json::json!({
                        "action": "assign.evict",
                        "oldName": new_name.clone(),
                        "newName": serde_json::Value::Null,
                        "agentId": evicted.0.clone(),
                    }),
                )
                .await;
            }
            self.append_admin_rename_lifecycle_best_effort(
                row.as_ref(),
                &new_name,
                serde_json::json!({
                    "action": "assign",
                    "oldName": serde_json::Value::Null,
                    "newName": new_name.clone(),
                    "agentId": agent_id.0.clone(),
                    "evictedAgentId": evicted_agent_id.as_ref().map(|id| id.0.clone()),
                }),
            )
            .await;

            if let Some(session_id) = session_id.as_ref() {
                self.events
                    .emit(WsEvent::AgentSpawned {
                        session_id: SessionId(session_id.0.clone()),
                        name: Some(new_name.clone()),
                        agent_id: Some(agent_id.0.clone()),
                    })
                    .await;
            }

            Ok(AdminAssignResponse {
                agent_id,
                session_id,
                name: new_name,
                evicted_agent_id,
            })
        }
        .await;
        to_port(result)
    }

    async fn set_offline(&self, session: &SessionId) -> PortResult<()> {
        // Identity-only callers keep this lightweight fallback. Daemon paths that also own
        // loop/runtime state use `AppState::mark_session_offline` so roster, runtime, and turn
        // state converge together.
        let result: Result<(), NexusError> = async {
            let _transition = self.store.lock_presence_transition().await;
            let sessions = Sessions::new(&self.store);
            let before = sessions.find_by_session_id(session).await?;
            sessions.set_presence(session, Presence::Offline).await?;
            AgentRuntimes::new(&self.store).stop(&session.0).await?;
            if before.as_ref().is_some_and(|row| {
                row.kind == "agent" && row.presence.as_deref() != Some("offline")
            }) {
                self.events
                    .emit(WsEvent::AgentStatus {
                        session_id: session.clone(),
                        presence: Presence::Offline,
                        paused: before.as_ref().is_some_and(|row| row.paused),
                    })
                    .await;
            }
            Ok(())
        }
        .await;
        to_port(result)
    }

    async fn set_resume_key(&self, session: &SessionId, resume_key: &str) -> PortResult<()> {
        // Persist the harness's real ACP session id onto the row so a not-fresh agent can be
        // re-spawned and resumed via `session/load`.
        let result: Result<(), NexusError> = async {
            Sessions::new(&self.store)
                .set_harness_session_id(session, resume_key)
                .await?;
            IdentitySessions::new(&self.store)
                .set_native_resume_key(&session.0, resume_key)
                .await?;
            Ok(())
        }
        .await;
        to_port(result)
    }
}
