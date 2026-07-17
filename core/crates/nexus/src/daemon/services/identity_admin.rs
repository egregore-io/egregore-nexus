//! Durable agent identity/admin verbs: groups, roles, projects, tiers, credentials,
//! delegated ACLs, runtime listing, and metadata writes. Extracted from AppState
//! verbatim (R5.1 Task 2); owns only the ports these paths use.

use std::sync::Arc;

use nexus_common::{hash_runtime_credential, now, NexusError};
use nexus_contracts::ids::{AgentId, CredentialId, SessionId};
use nexus_contracts::{
    AdminGroupAssignRequest, AdminGroupAssignResponse, AdminPort, AgentAccessGrantRequest,
    AgentAccessGrantResponse, AgentAccessRevokeRequest, AgentAccessRevokeResponse, AgentAccessRole,
    AgentCreateRequest, AgentCreateResponse, AgentCredentialCreateRequest,
    AgentCredentialCreateResponse, AgentCredentialRevokeRequest, AgentCredentialRevokeResponse,
    AgentListRequest, AgentListResponse, AgentOwnerTransferRequest, AgentOwnerTransferResponse,
    AgentRuntimeListRequest, AgentRuntimeListResponse, AgentRuntimeSummary, AgentShowRequest,
    AgentShowResponse, AgentSummary, AssignProjectRequest, AssignProjectResponse,
    AssignRoleRequest, AssignRoleResponse, Caller, ContractError, EventSink, GrantTierRequest,
    GrantTierResponse, Harness, Kind, MetadataEntityKind, MetadataResponse, MetadataSetRequest,
    Presence, Tier, WsEvent,
};
use nexus_store::repos::{
    AgentAccessGrants, AgentCredentials, AgentGroups, AgentOwner, AgentRef, AgentRuntimes, Agents,
    DeveloperEvents, Metadata, MetadataEntity, NewAgent, NewAgentAccessGrant, NewAgentCredential,
    Sessions, ROLE_CO_OWNER, ROLE_VIEWER,
};
use nexus_store::types::{AgentRow, AgentRuntimeRow, SessionRow};
use nexus_store::Store;

use crate::local_operator::LOCAL_OPERATOR_SESSION_ID;

struct AclPrincipalRefs {
    name: String,
    project: String,
    session_id: Option<String>,
    agent_id: Option<String>,
}

#[derive(Clone)]
pub(crate) struct IdentityAdminService {
    store: Arc<Store>,
    events: Arc<dyn EventSink>,
    admin: Arc<dyn AdminPort>,
}

impl IdentityAdminService {
    pub(crate) fn new(
        store: Arc<Store>,
        events: Arc<dyn EventSink>,
        admin: Arc<dyn AdminPort>,
    ) -> Self {
        Self {
            store,
            events,
            admin,
        }
    }

    pub(crate) fn random_suffix() -> String {
        nexus_common::new_source_token()
            .strip_prefix("src_")
            .unwrap_or_default()
            .to_string()
    }

    fn new_agent_id() -> AgentId {
        AgentId(format!("a_{}", Self::random_suffix()))
    }

    fn new_credential_id() -> CredentialId {
        CredentialId(format!("cred_{}", Self::random_suffix()))
    }

    fn new_runtime_secret() -> String {
        format!("nexus_rt_{}", Self::random_suffix())
    }

    pub(crate) fn harness_to_store(harness: Harness) -> String {
        serde_json::to_value(harness)
            .ok()
            .and_then(|v| v.as_str().map(str::to_string))
            .unwrap_or_else(|| "other".to_string())
    }

    fn harness_from_store(value: &str) -> Harness {
        serde_json::from_value(serde_json::Value::String(value.to_string()))
            .unwrap_or(Harness::Other)
    }

    fn presence_from_store(value: Option<&str>, active: bool) -> Presence {
        match value {
            Some(v) => nexus_common::presence::presence_from_token(Some(v)),
            None => {
                if active {
                    Presence::Online
                } else {
                    Presence::Offline
                }
            }
        }
    }

    fn row_to_runtime_summary(row: AgentRuntimeRow) -> AgentRuntimeSummary {
        AgentRuntimeSummary {
            runtime_id: SessionId(row.runtime_id),
            agent_id: AgentId(row.agent_id),
            harness: Self::harness_from_store(&row.harness),
            cwd: row.cwd,
            transport: row.transport,
            presence: Self::presence_from_store(row.presence.as_deref(), row.active),
            active: row.active,
            started_at: row.started_at,
            stopped_at: row.stopped_at,
            last_heartbeat: row.last_heartbeat,
        }
    }

    fn row_to_agent_summary(
        row: AgentRow,
        active_runtime: Option<AgentRuntimeRow>,
    ) -> AgentSummary {
        AgentSummary {
            agent_id: AgentId(row.agent_id),
            name: row.name,
            project: row.project,
            default_harness: row.default_harness.as_deref().map(Self::harness_from_store),
            role: row.role,
            tier: Some(row.tier),
            disabled: row.disabled_at.is_some(),
            active_runtime: active_runtime.map(Self::row_to_runtime_summary),
        }
    }

    fn invalid_agent_params(message: impl Into<String>) -> ContractError {
        NexusError::Invalid(message.into()).to_contract_error()
    }

    fn agent_not_found(id: impl Into<String>) -> ContractError {
        NexusError::NotFound(format!("agent:{}", id.into())).to_contract_error()
    }

    fn enforce_project_override(caller: &Caller, project: &str) -> Result<(), ContractError> {
        if caller.tier == Tier::Admin || project == caller.project {
            Ok(())
        } else {
            Err(NexusError::Unauthorized.to_contract_error())
        }
    }

    fn enforce_agent_read_scope(caller: &Caller, agent: &AgentRow) -> Result<(), ContractError> {
        if caller.tier == Tier::Admin || agent.project == caller.project {
            Ok(())
        } else {
            Err(Self::agent_not_found(agent.display_name()))
        }
    }

    async fn resolve_agent(
        &self,
        agent_id: Option<&AgentId>,
        name: Option<&str>,
    ) -> Result<AgentRow, ContractError> {
        let agents = Agents::new(&self.store);
        if let Some(agent_id) = agent_id {
            return agents
                .find_by_id(&agent_id.0)
                .await
                .map_err(|e| e.to_contract_error())?
                .ok_or_else(|| Self::agent_not_found(&agent_id.0));
        }
        let Some(name) = name else {
            return Err(Self::invalid_agent_params("agent_id or name is required"));
        };
        let name = name.trim();
        if name.is_empty() {
            return Err(Self::invalid_agent_params("agent name is required"));
        }
        agents
            .find_by_name(name)
            .await
            .map_err(|e| e.to_contract_error())?
            .ok_or_else(|| Self::agent_not_found(name))
    }

    pub(crate) async fn caller_is_human_admin(
        &self,
        caller: &Caller,
    ) -> Result<bool, ContractError> {
        if caller.tier != Tier::Admin {
            return Ok(false);
        }
        let sessions = Sessions::new(&self.store);
        match sessions
            .find_by_session_id(&caller.session)
            .await
            .map_err(|e| e.to_contract_error())?
        {
            Some(row) => Ok(row.kind == kind_token(Kind::Human)),
            None => Ok(caller.session.0 == LOCAL_OPERATOR_SESSION_ID),
        }
    }

    async fn require_human_admin(&self, caller: &Caller) -> Result<(), ContractError> {
        if self.caller_is_human_admin(caller).await? {
            Ok(())
        } else {
            Err(NexusError::Unauthorized.to_contract_error())
        }
    }

    async fn agent_acl_admin_override_allowed(
        &self,
        caller: &Caller,
        agent: &AgentRow,
    ) -> Result<bool, ContractError> {
        if caller.tier != Tier::Admin {
            return Ok(false);
        }
        if self.caller_is_human_admin(caller).await? {
            return Ok(true);
        }
        Ok(!self.protected_agent_acl_target(agent).await?)
    }

    async fn protected_agent_acl_target(&self, agent: &AgentRow) -> Result<bool, ContractError> {
        if agent.tier == tier_token(Tier::Admin)
            || agent
                .role
                .as_deref()
                .is_some_and(|role| role.eq_ignore_ascii_case("lead"))
        {
            return Ok(true);
        }
        let session = match agent.name.as_deref() {
            Some(name) => Sessions::new(&self.store)
                .find_by_name(&agent.project, name)
                .await
                .map_err(|e| e.to_contract_error())?,
            None => None,
        };
        Ok(session.as_ref().is_some_and(protected_remove_target))
    }

    async fn enforce_agent_acl_authority(
        &self,
        caller: &Caller,
        agent: &AgentRow,
    ) -> Result<(), ContractError> {
        if Self::caller_is_agent_owner(caller, agent) {
            return Ok(());
        }
        let grants = AgentAccessGrants::new(&self.store);
        if let Some(caller_agent_id) = caller.agent_id.as_ref() {
            if grants
                .is_co_owner_agent_id(&agent.agent_id, &caller_agent_id.0)
                .await
                .map_err(|e| e.to_contract_error())?
            {
                return Ok(());
            }
        }
        if grants
            .is_co_owner(&agent.agent_id, &caller.project, &caller.name)
            .await
            .map_err(|e| e.to_contract_error())?
        {
            return Ok(());
        }
        if self.agent_acl_admin_override_allowed(caller, agent).await? {
            return Ok(());
        }
        Err(NexusError::Unauthorized.to_contract_error())
    }

    /// Return whether `caller` owns `agent`, preferring durable ids when both sides have them.
    fn caller_is_agent_owner(caller: &Caller, agent: &AgentRow) -> bool {
        if let (Some(owner_agent_id), Some(caller_agent_id)) =
            (agent.owner_agent_id.as_deref(), caller.agent_id.as_ref())
        {
            return owner_agent_id == caller_agent_id.0;
        }
        agent.owner_name.as_deref() == Some(caller.name.as_str())
            && agent
                .owner_project
                .as_deref()
                .map_or(true, |project| project == caller.project)
    }

    async fn enforce_agent_owner_transfer_authority(
        &self,
        caller: &Caller,
        agent: &AgentRow,
    ) -> Result<(), ContractError> {
        if Self::caller_is_agent_owner(caller, agent)
            || self.agent_acl_admin_override_allowed(caller, agent).await?
        {
            Ok(())
        } else {
            Err(NexusError::Unauthorized.to_contract_error())
        }
    }

    async fn session_row_for_agent_id(
        &self,
        agent_id: &str,
    ) -> Result<Option<SessionRow>, ContractError> {
        let sessions = Sessions::new(&self.store);
        match sessions
            .active_runtime_session_for_agent(agent_id)
            .await
            .map_err(|e| e.to_contract_error())?
        {
            Some(row) => Ok(Some(row)),
            None => Ok(sessions
                .find_by_agent_id(agent_id)
                .await
                .map_err(|e| e.to_contract_error())?),
        }
    }

    async fn acl_session_id_for_agent(
        &self,
        agent_id: &str,
    ) -> Result<Option<String>, ContractError> {
        let session = self.session_row_for_agent_id(agent_id).await?;
        Ok(session.map(|row| row.session_id.0))
    }

    async fn resolve_acl_principal_refs(
        &self,
        project: &str,
        principal: &str,
        principal_agent_id: Option<&AgentId>,
    ) -> Result<AclPrincipalRefs, ContractError> {
        let agents = Agents::new(&self.store);
        let sessions = Sessions::new(&self.store);
        if let Some(principal_agent_id) = principal_agent_id {
            let agent = agents
                .find_by_id(&principal_agent_id.0)
                .await
                .map_err(|e| e.to_contract_error())?
                .ok_or_else(|| {
                    NexusError::NotFound(format!("principal:{}", principal_agent_id.0))
                        .to_contract_error()
                })?;
            return Ok(AclPrincipalRefs {
                name: agent
                    .require_name("ACL principal")
                    .map_err(|e| e.to_contract_error())?
                    .to_string(),
                project: agent.project,
                session_id: self.acl_session_id_for_agent(&agent.agent_id).await?,
                agent_id: Some(agent.agent_id),
            });
        }

        match agents
            .resolve_ref(project, &AgentRef::parse(principal), false)
            .await
        {
            Ok(agent) => {
                return Ok(AclPrincipalRefs {
                    name: principal.to_string(),
                    project: project.to_string(),
                    session_id: self.acl_session_id_for_agent(&agent.agent_id).await?,
                    agent_id: Some(agent.agent_id),
                });
            }
            Err(NexusError::NotFound(_)) => {}
            Err(e) => return Err(e.to_contract_error()),
        }

        let session_id = sessions
            .find_by_name(project, principal)
            .await
            .map_err(|e| e.to_contract_error())?
            .map(|row| row.session_id.0);
        let agent_id = agents
            .find_by_name(principal)
            .await
            .map_err(|e| e.to_contract_error())?
            .filter(|row| row.project == project)
            .map(|row| row.agent_id);
        Ok(AclPrincipalRefs {
            name: principal.to_string(),
            project: project.to_string(),
            session_id,
            agent_id,
        })
    }

    async fn resolve_transfer_owner(
        &self,
        project: &str,
        owner: &str,
        owner_agent_id: Option<&AgentId>,
    ) -> Result<AgentOwner, ContractError> {
        if let Some(owner_agent_id) = owner_agent_id {
            let agent = Agents::new(&self.store)
                .find_by_id(&owner_agent_id.0)
                .await
                .map_err(|e| e.to_contract_error())?
                .ok_or_else(|| {
                    NexusError::NotFound(format!("principal:{}", owner_agent_id.0))
                        .to_contract_error()
                })?;
            return Ok(AgentOwner {
                name: agent
                    .require_name("agent owner")
                    .map_err(|e| e.to_contract_error())?
                    .to_string(),
                project: agent.project,
                session_id: self.acl_session_id_for_agent(&agent.agent_id).await?,
                agent_id: Some(agent.agent_id),
            });
        }

        let refs = self
            .resolve_acl_principal_refs(project, owner, None)
            .await?;
        if refs.session_id.is_none() && refs.agent_id.is_none() {
            return Err(
                NexusError::NotFound(format!("principal:{project}/{owner}")).to_contract_error()
            );
        }
        Ok(AgentOwner {
            name: refs.name,
            project: refs.project,
            session_id: refs.session_id,
            agent_id: refs.agent_id,
        })
    }

    async fn record_agent_acl_audit(
        &self,
        agent: &AgentRow,
        event: serde_json::Value,
    ) -> Result<(), ContractError> {
        let metadata_repo = Metadata::new(&self.store);
        let mut metadata = metadata_repo
            .get(&agent.project, MetadataEntity::Agent, &agent.agent_id)
            .await
            .map_err(|e| e.to_contract_error())?
            .metadata;
        if !metadata.is_object() {
            metadata = serde_json::json!({});
        }
        let mut events = metadata
            .get("nexusAclAudit")
            .and_then(|audit| audit.get("events"))
            .and_then(|events| events.as_array())
            .cloned()
            .unwrap_or_default();
        events.push(event.clone());
        if events.len() > 20 {
            events = events.split_off(events.len() - 20);
        }
        metadata["nexusAclAudit"] = serde_json::json!({
            "last": event,
            "events": events,
        });
        metadata_repo
            .set(
                &agent.project,
                MetadataEntity::Agent,
                &agent.agent_id,
                &metadata,
            )
            .await
            .map_err(|e| e.to_contract_error())?;
        Ok(())
    }

    /// Admin-guarded: assign a durable agent identity to a project-scoped message-policy group.
    ///
    /// Group assignment is policy metadata only. It does not write messages, enqueue deliveries, or
    /// mutate thread membership; the Message Post policy evaluator reads the membership before
    /// canonical message rows are written.
    pub async fn assign_agent_group(
        &self,
        caller: &Caller,
        req: AdminGroupAssignRequest,
    ) -> Result<AdminGroupAssignResponse, ContractError> {
        nexus_identity::tier_guard(caller, Tier::Admin).map_err(|e| e.to_contract_error())?;
        let group = req.group.trim();
        if group.is_empty() {
            return Err(Self::invalid_agent_params("group name is required (usage: nexus admin group assign <group> <name> — group first)"));
        }
        let row = self
            .resolve_agent(req.agent_id.as_ref(), Some(&req.name))
            .await?;
        let project = req.project.unwrap_or_else(|| caller.project.clone());
        if row.project != project {
            return Err(Self::agent_not_found(&req.name));
        }
        let agent_id = AgentId(row.agent_id.clone());
        let row_name = row
            .require_name("agent group assignment")
            .map_err(|e| e.to_contract_error())?
            .to_string();
        AgentGroups::new(&self.store)
            .assign(&project, group, &agent_id, &row_name)
            .await
            .map_err(|e| e.to_contract_error())?;
        Ok(AdminGroupAssignResponse {
            project,
            group: group.to_string(),
            name: Some(row_name),
            agent_id,
        })
    }

    /// Admin-guarded: set a display-only role label by stable id when supplied.
    ///
    /// Legacy callers without `agent_id` still flow through the admin service. Id-capable callers
    /// resolve the target once and update the durable identity plus the selected compatibility
    /// session row directly, so a stale request name cannot retarget the mutation.
    pub async fn assign_agent_role(
        &self,
        caller: &Caller,
        req: AssignRoleRequest,
    ) -> Result<AssignRoleResponse, ContractError> {
        nexus_identity::tier_guard(caller, Tier::Admin).map_err(|e| e.to_contract_error())?;
        let Some(agent_id) = req.agent_id.as_ref() else {
            return self.admin.assign_role(caller, req).await;
        };

        let role = req.role.trim();
        if role.is_empty() {
            return Err(NexusError::Invalid(
                "role must not be empty (usage: nexus admin assign-role <name> <role>)".into(),
            )
            .to_contract_error());
        }
        let row = self.resolve_agent(Some(agent_id), Some(&req.name)).await?;
        Agents::new(&self.store)
            .set_role(&row.agent_id, role)
            .await
            .map_err(|e| e.to_contract_error())?;
        if let Some(session) = self.session_row_for_agent_id(&row.agent_id).await? {
            let sessions = Sessions::new(&self.store);
            if session.agent_id.as_deref() != Some(row.agent_id.as_str()) {
                sessions
                    .set_agent_id(&session.session_id, &row.agent_id)
                    .await
                    .map_err(|e| e.to_contract_error())?;
            }
            sessions
                .set_role(&session.session_id, role)
                .await
                .map_err(|e| e.to_contract_error())?;
        }
        Ok(AssignRoleResponse {
            name: row.name,
            role: role.to_string(),
        })
    }

    /// Admin-guarded: move an exact agent session to another project when `agent_id` is supplied.
    ///
    /// The compatibility session row is selected by `agent_id`, then both the session and durable
    /// identity rows are moved by stable keys. Legacy name-only callers continue to use the
    /// existing admin service path.
    pub async fn assign_agent_project(
        &self,
        caller: &Caller,
        req: AssignProjectRequest,
    ) -> Result<AssignProjectResponse, ContractError> {
        nexus_identity::tier_guard(caller, Tier::Admin).map_err(|e| e.to_contract_error())?;
        let Some(agent_id) = req.agent_id.as_ref() else {
            return self.admin.assign_project(caller, req).await;
        };

        let row = self.resolve_agent(Some(agent_id), Some(&req.name)).await?;
        let sessions = Sessions::new(&self.store);
        let session = self
            .session_row_for_agent_id(&row.agent_id)
            .await?
            .ok_or_else(|| NexusError::NotFound(row.display_name()).to_contract_error())?;
        if session.agent_id.as_deref() != Some(row.agent_id.as_str()) {
            sessions
                .set_agent_id(&session.session_id, &row.agent_id)
                .await
                .map_err(|e| e.to_contract_error())?;
        }

        if session.project == req.project {
            return Ok(AssignProjectResponse {
                name: session.name,
                project: req.project,
            });
        }
        if let Some(name) = session.name.as_deref() {
            if sessions
                .name_exists_in(&req.project, name)
                .await
                .map_err(|e| e.to_contract_error())?
            {
                return Err(NexusError::DuplicateName(format!(
                    "{} already bound in project {}",
                    name, req.project
                ))
                .to_contract_error());
            }
        }

        sessions
            .set_project(&session.session_id, &req.project)
            .await
            .map_err(|e| e.to_contract_error())?;
        Agents::new(&self.store)
            .set_project(&row.agent_id, &req.project)
            .await
            .map_err(|e| e.to_contract_error())?;
        self.events
            .emit(WsEvent::AgentSpawned {
                session_id: session.session_id,
                name: session.name.clone(),
                agent_id: Some(row.agent_id),
            })
            .await;

        Ok(AssignProjectResponse {
            name: session.name,
            project: req.project,
        })
    }

    /// Human-admin-only: set a durable agent identity's authorization tier.
    ///
    /// Agent-admins may use admin capability, but they cannot mint new admins. The grant updates
    /// both the stable identity row and any live compatibility session so the next resolved command
    /// sees the new ceiling without requiring a restart. Registered human admins remain scoped to
    /// their project; the implicit local operator is the machine owner and may administer a named
    /// agent in any local project because the CLI has no project selector for this verb.
    pub async fn grant_agent_tier(
        &self,
        caller: &Caller,
        req: GrantTierRequest,
    ) -> Result<GrantTierResponse, ContractError> {
        nexus_identity::tier_guard(caller, Tier::Admin).map_err(|e| e.to_contract_error())?;
        self.require_human_admin(caller).await?;

        let row = self
            .resolve_agent(req.agent_id.as_ref(), Some(&req.name))
            .await?;
        if row.project != caller.project && caller.session.0 != LOCAL_OPERATOR_SESSION_ID {
            return Err(Self::agent_not_found(&req.name));
        }

        let sessions = Sessions::new(&self.store);
        let session = sessions
            .find_by_name(
                &row.project,
                row.require_name("grant tier")
                    .map_err(|e| e.to_contract_error())?,
            )
            .await
            .map_err(|e| e.to_contract_error())?;
        if matches!(session.as_ref(), Some(s) if s.kind != kind_token(Kind::Agent)) {
            return Err(NexusError::Unauthorized.to_contract_error());
        }

        let tier = tier_token(req.tier);
        Agents::new(&self.store)
            .set_tier(&row.agent_id, tier)
            .await
            .map_err(|e| e.to_contract_error())?;
        if let Some(session) = session {
            sessions
                .set_tier(&session.session_id, tier)
                .await
                .map_err(|e| e.to_contract_error())?;
        }

        Ok(GrantTierResponse {
            name: row.name,
            tier: req.tier,
        })
    }

    /// Admin-guarded: create a durable agent identity.
    pub async fn create_agent_identity(
        &self,
        caller: &Caller,
        req: AgentCreateRequest,
    ) -> Result<AgentCreateResponse, ContractError> {
        nexus_identity::tier_guard(caller, Tier::Admin).map_err(|e| e.to_contract_error())?;
        let name = req.name.trim();
        if name.is_empty() {
            return Err(Self::invalid_agent_params("agent name is required"));
        }
        let project = req.project.unwrap_or_else(|| caller.project.clone());
        let default_harness = req.default_harness.map(Self::harness_to_store);
        let row_id = Agents::new(&self.store)
            .create(NewAgent {
                agent_id: Self::new_agent_id().0,
                project,
                name: Some(name.to_string()),
                default_harness,
                role: req.role,
                tier: Some("agent".to_string()),
                owner: None,
            })
            .await
            .map_err(|e| e.to_contract_error())?;
        let row = Agents::new(&self.store)
            .find_by_id(&row_id)
            .await
            .map_err(|e| e.to_contract_error())?
            .ok_or_else(|| Self::agent_not_found(row_id))?;
        Ok(AgentCreateResponse {
            agent: Self::row_to_agent_summary(row, None),
            credential: None,
        })
    }

    /// List durable agents, scoped to the caller's project unless an admin supplies an override.
    pub async fn list_agent_identities(
        &self,
        caller: &Caller,
        req: AgentListRequest,
    ) -> Result<AgentListResponse, ContractError> {
        let project = req.project.unwrap_or_else(|| caller.project.clone());
        Self::enforce_project_override(caller, &project)?;
        let runtimes = AgentRuntimes::new(&self.store);
        let mut agents = Vec::new();
        for row in Agents::new(&self.store)
            .list(Some(&project), req.include_disabled.unwrap_or(false))
            .await
            .map_err(|e| e.to_contract_error())?
        {
            let active = runtimes
                .active_for_agent(&row.agent_id)
                .await
                .map_err(|e| e.to_contract_error())?;
            agents.push(Self::row_to_agent_summary(row, active));
        }
        Ok(AgentListResponse { agents })
    }

    /// Show a durable agent identity and its runtime history.
    pub async fn show_agent_identity(
        &self,
        caller: &Caller,
        req: AgentShowRequest,
    ) -> Result<AgentShowResponse, ContractError> {
        let row = self
            .resolve_agent(req.agent_id.as_ref(), req.name.as_deref())
            .await?;
        Self::enforce_agent_read_scope(caller, &row)?;
        let runtimes_repo = AgentRuntimes::new(&self.store);
        let active = runtimes_repo
            .active_for_agent(&row.agent_id)
            .await
            .map_err(|e| e.to_contract_error())?;
        let runtimes = runtimes_repo
            .list_for_agent(&row.agent_id, true)
            .await
            .map_err(|e| e.to_contract_error())?
            .into_iter()
            .map(Self::row_to_runtime_summary)
            .collect();
        Ok(AgentShowResponse {
            agent: Self::row_to_agent_summary(row, active),
            runtimes,
        })
    }

    /// Grant delegated access to a managed agent session.
    ///
    /// Owners and co-owners may delegate `viewer`/`coOwner` access. Admin override follows the
    /// protected-target policy used by remove/delete: local/root human admins can override any
    /// target, while agent-admin override is limited to ordinary agent targets.
    pub async fn grant_agent_access(
        &self,
        caller: &Caller,
        req: AgentAccessGrantRequest,
    ) -> Result<AgentAccessGrantResponse, ContractError> {
        let target = self
            .resolve_agent(req.agent_id.as_ref(), Some(&req.name))
            .await?;
        self.enforce_agent_acl_authority(caller, &target).await?;
        let principal = req.principal.trim();
        if principal.is_empty() {
            return Err(Self::invalid_agent_params("principal name is required (usage: nexus agents grant-access <agent-name> <principal> <viewer|co_owner>)"));
        }
        let project = req.project.unwrap_or_else(|| caller.project.clone());
        let role = agent_access_role_to_store(req.role);
        let principal_refs = self
            .resolve_acl_principal_refs(&project, principal, req.principal_agent_id.as_ref())
            .await?;

        let grant = AgentAccessGrants::new(&self.store)
            .grant(NewAgentAccessGrant {
                agent_id: target.agent_id.clone(),
                principal_project: principal_refs.project.clone(),
                principal_name: principal_refs.name,
                principal_session_id: principal_refs.session_id,
                principal_agent_id: principal_refs.agent_id,
                role: role.to_string(),
                granted_by_name: caller.name.clone(),
                granted_by_project: caller.project.clone(),
            })
            .await
            .map_err(|e| e.to_contract_error())?;
        self.record_agent_acl_audit(
            &target,
            serde_json::json!({
                "op": "grant",
                "actor": caller.name,
                "actorProject": caller.project,
                "principal": grant.principal_name,
                "principalProject": grant.principal_project,
                "role": grant.role,
                "at": grant.updated_at,
            }),
        )
        .await?;

        Ok(AgentAccessGrantResponse {
            name: target.name,
            principal: grant.principal_name,
            project: grant.principal_project,
            role: store_agent_access_role(&grant.role)?,
        })
    }

    /// Revoke delegated access to a managed agent session.
    ///
    /// Revocation uses the same authority rule as grants. When a durable principal id resolves,
    /// revoke removes both the id-keyed grant and the requested legacy name-keyed grant so mixed
    /// compatibility rows do not keep authorizing the current label. Removing non-existent grants
    /// is idempotent and returns `revoked=false`.
    pub async fn revoke_agent_access(
        &self,
        caller: &Caller,
        req: AgentAccessRevokeRequest,
    ) -> Result<AgentAccessRevokeResponse, ContractError> {
        let target = self
            .resolve_agent(req.agent_id.as_ref(), Some(&req.name))
            .await?;
        self.enforce_agent_acl_authority(caller, &target).await?;
        let principal = req.principal.trim();
        if principal.is_empty() {
            return Err(Self::invalid_agent_params("principal name is required (usage: nexus agents grant-access <agent-name> <principal> <viewer|co_owner>)"));
        }
        let project = req.project.unwrap_or_else(|| caller.project.clone());
        let principal_refs = self
            .resolve_acl_principal_refs(&project, principal, req.principal_agent_id.as_ref())
            .await?;
        let grants = AgentAccessGrants::new(&self.store);
        let revoked = match principal_refs.agent_id {
            Some(principal_agent_id) => {
                let revoked_by_id = grants
                    .revoke_by_principal_agent_id(&target.agent_id, &principal_agent_id)
                    .await
                    .map_err(|e| e.to_contract_error())?;
                let revoked_by_name = grants
                    .revoke(
                        &target.agent_id,
                        &principal_refs.project,
                        &principal_refs.name,
                    )
                    .await
                    .map_err(|e| e.to_contract_error())?;
                revoked_by_id || revoked_by_name
            }
            None => grants
                .revoke(&target.agent_id, &project, principal)
                .await
                .map_err(|e| e.to_contract_error())?,
        };
        self.record_agent_acl_audit(
            &target,
            serde_json::json!({
                "op": "revoke",
                "actor": caller.name,
                "actorProject": caller.project,
                "principal": principal_refs.name.clone(),
                "principalProject": principal_refs.project.clone(),
                "revoked": revoked,
                "at": now(),
            }),
        )
        .await?;

        Ok(AgentAccessRevokeResponse {
            name: target.name,
            principal: principal_refs.name,
            project: principal_refs.project,
            revoked,
        })
    }

    /// Transfer durable ownership of a managed agent session.
    ///
    /// Only the current owner or an admin override may transfer ownership. Co-owner grants remain
    /// delegation rights and do not authorize replacing the `owner_*` fact.
    pub async fn transfer_agent_owner(
        &self,
        caller: &Caller,
        req: AgentOwnerTransferRequest,
    ) -> Result<AgentOwnerTransferResponse, ContractError> {
        let target = self
            .resolve_agent(req.agent_id.as_ref(), Some(&req.name))
            .await?;
        self.enforce_agent_owner_transfer_authority(caller, &target)
            .await?;
        let owner = req.owner.trim();
        if owner.is_empty() {
            return Err(Self::invalid_agent_params("owner name is required (usage: nexus agents transfer-owner <agent-name> <new-owner>)"));
        }
        let project = req.project.unwrap_or_else(|| caller.project.clone());
        let next_owner = self
            .resolve_transfer_owner(&project, owner, req.owner_agent_id.as_ref())
            .await?;
        let previous_owner = target.owner_name.clone();
        let previous_project = target.owner_project.clone();

        Agents::new(&self.store)
            .set_owner(&target.agent_id, &next_owner)
            .await
            .map_err(|e| e.to_contract_error())?;
        self.record_agent_acl_audit(
            &target,
            serde_json::json!({
                "op": "transfer",
                "actor": caller.name,
                "actorProject": caller.project,
                "previousOwner": previous_owner.clone(),
                "previousProject": previous_project.clone(),
                "owner": next_owner.name.clone(),
                "ownerProject": next_owner.project.clone(),
                "at": now(),
            }),
        )
        .await?;

        Ok(AgentOwnerTransferResponse {
            name: target.name,
            previous_owner,
            previous_project,
            owner: next_owner.name,
            project: next_owner.project,
        })
    }

    /// Admin-guarded: create a runtime credential and return the plaintext secret once.
    pub async fn create_agent_credential(
        &self,
        caller: &Caller,
        req: AgentCredentialCreateRequest,
    ) -> Result<AgentCredentialCreateResponse, ContractError> {
        nexus_identity::tier_guard(caller, Tier::Admin).map_err(|e| e.to_contract_error())?;
        let row = self
            .resolve_agent(req.agent_id.as_ref(), req.name.as_deref())
            .await?;
        let credential_id = Self::new_credential_id();
        let secret = Self::new_runtime_secret();
        AgentCredentials::new(&self.store)
            .create_hash(NewAgentCredential {
                credential_id: credential_id.0.clone(),
                agent_id: row.agent_id.clone(),
                secret_hash: hash_runtime_credential(&secret),
                purpose: req.purpose.clone(),
                label: req.label.clone(),
                scopes_json: serde_json::to_string(&req.scopes).map_err(|e| {
                    NexusError::Internal(format!("failed to encode credential scopes: {e}"))
                        .to_contract_error()
                })?,
                metadata_json: None,
            })
            .await
            .map_err(|e| e.to_contract_error())?;
        Ok(AgentCredentialCreateResponse {
            credential_id,
            agent_id: AgentId(row.agent_id),
            secret,
            label: req.label,
            purpose: req.purpose,
            scopes: req.scopes,
        })
    }

    /// Admin-guarded: revoke a runtime credential.
    pub async fn revoke_agent_credential(
        &self,
        caller: &Caller,
        req: AgentCredentialRevokeRequest,
    ) -> Result<AgentCredentialRevokeResponse, ContractError> {
        nexus_identity::tier_guard(caller, Tier::Admin).map_err(|e| e.to_contract_error())?;
        let credentials = AgentCredentials::new(&self.store);
        let credential = credentials
            .find_by_id(&req.credential_id.0)
            .await
            .map_err(|e| e.to_contract_error())?
            .ok_or_else(|| {
                NexusError::NotFound(format!("credential:{}", req.credential_id.0))
                    .to_contract_error()
            })?;
        credentials
            .revoke(&req.credential_id.0)
            .await
            .map_err(|e| e.to_contract_error())?;
        self.clear_active_runtime_client_key(&credential.agent_id)
            .await
            .map_err(|e| e.to_contract_error())?;
        Ok(AgentCredentialRevokeResponse {
            credential_id: req.credential_id,
            revoked: true,
        })
    }

    /// List runtimes for a durable agent identity.
    pub async fn list_agent_runtimes(
        &self,
        caller: &Caller,
        req: AgentRuntimeListRequest,
    ) -> Result<AgentRuntimeListResponse, ContractError> {
        let row = self
            .resolve_agent(req.agent_id.as_ref(), req.name.as_deref())
            .await?;
        Self::enforce_agent_read_scope(caller, &row)?;
        let runtimes = AgentRuntimes::new(&self.store)
            .list_for_agent(&row.agent_id, req.include_stopped.unwrap_or(false))
            .await
            .map_err(|e| e.to_contract_error())?
            .into_iter()
            .map(Self::row_to_runtime_summary)
            .collect();
        Ok(AgentRuntimeListResponse {
            agent_id: AgentId(row.agent_id),
            runtimes,
        })
    }

    /// Replace a core entity's opaque metadata JSON bag in the caller's project.
    ///
    /// Metadata is authenticated but intentionally ungated: it is integration state, not routing
    /// state, authorization state, or delivery state. The only domain check is that the target
    /// entity exists within the caller's project.
    pub async fn set_entity_metadata(
        &self,
        caller: &Caller,
        req: MetadataSetRequest,
    ) -> Result<MetadataResponse, ContractError> {
        let entity = metadata_entity(req.entity);
        let row = Metadata::new(&self.store)
            .set(&caller.project, entity, &req.id, &req.metadata)
            .await
            .map_err(|e| e.to_contract_error())?;
        self.append_metadata_action_best_effort(caller, row.entity, &row.id)
            .await;
        Ok(MetadataResponse {
            entity: metadata_entity_kind(row.entity),
            id: row.id,
            metadata: row.metadata,
        })
    }

    async fn append_metadata_action_best_effort(
        &self,
        caller: &Caller,
        entity: MetadataEntity,
        id: &str,
    ) {
        let token = entity.token();
        if let Err(error) = DeveloperEvents::new(&self.store)
            .append_action(
                &format!("sys.metadata.{token}"),
                "metadata.set",
                Some(&caller.name),
                matches!(entity, MetadataEntity::Thread).then_some(id),
                matches!(entity, MetadataEntity::Agent).then_some(id),
                matches!(entity, MetadataEntity::Session).then_some(id),
                serde_json::json!({
                    "entity": token,
                    "id": id,
                }),
                now(),
            )
            .await
        {
            tracing::warn!(
                target: "nexus::identity_admin",
                entity = token,
                id,
                error = ?error,
                "failed to append metadata action developer event"
            );
        }
    }

    /// Invalidate the active runtime's command client key after credential revocation. The process
    /// may still be alive, but its next command intent must fail caller verification instead of
    /// continuing under stale authority.
    async fn clear_active_runtime_client_key(&self, agent_id: &str) -> Result<(), NexusError> {
        let Some(runtime) = AgentRuntimes::new(&self.store)
            .active_for_agent(agent_id)
            .await?
        else {
            return Ok(());
        };
        Sessions::new(&self.store)
            .clear_client_key(&SessionId(runtime.runtime_id))
            .await
    }
}

/// The kind token stored on `sessions.kind`.
fn kind_token(k: Kind) -> &'static str {
    match k {
        Kind::Agent => "agent",
        Kind::Human => "human",
        Kind::Notification => "notification",
        Kind::App => "app",
    }
}

/// The tier token stored on `sessions.tier`.
fn tier_token(t: Tier) -> &'static str {
    match t {
        Tier::Agent => "agent",
        Tier::Admin => "admin",
    }
}

fn protected_remove_target(row: &SessionRow) -> bool {
    row.kind == kind_token(Kind::Human)
        || row.tier == tier_token(Tier::Admin)
        || row
            .role
            .as_deref()
            .is_some_and(|role| role.eq_ignore_ascii_case("lead"))
}

fn agent_access_role_to_store(role: AgentAccessRole) -> &'static str {
    match role {
        AgentAccessRole::Viewer => ROLE_VIEWER,
        AgentAccessRole::CoOwner => ROLE_CO_OWNER,
    }
}

fn store_agent_access_role(role: &str) -> Result<AgentAccessRole, ContractError> {
    match role {
        ROLE_VIEWER => Ok(AgentAccessRole::Viewer),
        ROLE_CO_OWNER => Ok(AgentAccessRole::CoOwner),
        other => Err(
            NexusError::Invalid(format!("unknown agent access role: {other}")).to_contract_error(),
        ),
    }
}

fn metadata_entity(kind: MetadataEntityKind) -> MetadataEntity {
    match kind {
        MetadataEntityKind::Message => MetadataEntity::Message,
        MetadataEntityKind::Session => MetadataEntity::Session,
        MetadataEntityKind::Thread => MetadataEntity::Thread,
        MetadataEntityKind::Agent => MetadataEntity::Agent,
    }
}

fn metadata_entity_kind(entity: MetadataEntity) -> MetadataEntityKind {
    match entity {
        MetadataEntity::Message => MetadataEntityKind::Message,
        MetadataEntity::Session => MetadataEntityKind::Session,
        MetadataEntity::Thread => MetadataEntityKind::Thread,
        MetadataEntity::Agent => MetadataEntityKind::Agent,
    }
}
