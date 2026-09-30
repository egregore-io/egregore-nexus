//! The register-once resolution (backend spec §5.1): idempotent on `client_key`, name↔session
//! binding, and duplicate-live-name rejection. This is the pure identity decision (resume vs
//! create vs reject); persistence is delegated to the `nexus-store` [`Sessions`] repo.

use nexus_common::{new_session_id, NexusError};
use nexus_contracts::register::RegisterRequest;
use nexus_store::repos::{AgentRuntimes, Agents, NativeThreadBindings, NewSession, Sessions};
use nexus_store::types::SessionRow;
use nexus_store::Store;

use crate::binding::{is_live, kind_str, tier_str};

/// The outcome of resolving a [`RegisterRequest`] against existing identity rows.
pub(crate) enum RegisterOutcome {
    /// A known `client_key` resumed an existing session (no row created, no event emitted).
    Resumed(SessionRow),
    /// A fresh session was created and bound (`agent.spawned` should be emitted by the caller).
    Created(SessionRow),
}

/// Resolve register-once over the store (backend §5.1):
///
/// 1. Known `harness_session_id` (global) → **resume** that session. (The service layer
///    rebinds `client_key`/runtime state and marks it online.)
/// 2. Else, known `client_key` (global) → **resume** that session.
/// 3. Else, if the `name` is held by a *different live* session → **reject**
///    ([`NexusError::DuplicateName`]). If the holding session is *offline*, adopting its name
///    (a rebind that overwrites `client_key`/harness/presence on the service layer) requires
///    **authenticating as that identity**: the caller must present `agent_id` +
///    `runtime_credential`, which the service verifies in preflight ([`preflight_requested_agent`])
///    *before* any write. A credential-less adoption attempt (`agent_id` absent) is rejected here
///    with [`NexusError::DuplicateName`] — closing an identity-takeover hole where any caller
///    could silently seize an offline identity's name with no authentication.
/// 4. Else → **create** a new session row, binding `name ↔ harness_session_id`.
///
/// [`preflight_requested_agent`]: crate::service — verified upfront in the register flow.
pub(crate) async fn resolve_register(
    store: &Store,
    req: &RegisterRequest,
) -> Result<RegisterOutcome, NexusError> {
    let repo = Sessions::new(store);
    let req_name = req.name.as_deref().ok_or_else(|| {
        NexusError::Invalid("register name is required before whoami staging is active".into())
    })?;

    if !req.harness_session_id.is_empty() {
        let harness = req.harness.as_str();
        if let Some(binding) = NativeThreadBindings::new(store)
            .find(harness, &req.harness_session_id)
            .await?
        {
            let owner = Agents::new(store)
                .find_by_id(&binding.agent_id)
                .await?
                .ok_or_else(|| {
                    NexusError::NotFound(format!(
                        "native {harness} session owner agent {}",
                        binding.agent_id
                    ))
                })?;
            let owner_name = owner.require_name("native register owner")?;
            let requested_agent_id = req.agent_id.as_ref().map(|id| id.0.as_str());
            if requested_agent_id.is_some_and(|id| id != binding.agent_id)
                || (requested_agent_id.is_none() && req_name != owner_name)
            {
                return Err(NexusError::Invalid(format!(
                    "native {harness} session {} belongs to {}:{}; requested {}:{}",
                    req.harness_session_id,
                    owner_name,
                    binding.agent_id,
                    req_name,
                    requested_agent_id.unwrap_or("<none>")
                )));
            }
            if let Some(runtime_id) = binding.last_runtime_id.as_deref() {
                if let Some(row) = repo
                    .find_by_session_id(&nexus_contracts::SessionId(runtime_id.to_string()))
                    .await?
                {
                    if row
                        .agent_id
                        .as_deref()
                        .is_some_and(|id| id != binding.agent_id)
                    {
                        return Err(NexusError::Invalid(format!(
                            "native {harness} session {} belongs to {}:{}, but last runtime {runtime_id} belongs to {}",
                            req.harness_session_id,
                            owner_name,
                            binding.agent_id,
                            row.agent_id.as_deref().unwrap_or("<unbound>")
                        )));
                    }
                    if let Some(runtime) = AgentRuntimes::new(store)
                        .find_by_runtime_id(runtime_id)
                        .await?
                    {
                        if runtime.agent_id != binding.agent_id {
                            return Err(NexusError::Invalid(format!(
                                "native {harness} session {} belongs to {}:{}, but last runtime {runtime_id} belongs to {}",
                                req.harness_session_id,
                                owner_name,
                                binding.agent_id,
                                runtime.agent_id
                            )));
                        }
                    }
                    require_matching_client_key(&row, req)?;
                    return Ok(RegisterOutcome::Resumed(row));
                }
            }
            if let Some(row) = repo.find_by_agent_id(&binding.agent_id).await? {
                require_matching_client_key(&row, req)?;
                return Ok(RegisterOutcome::Resumed(row));
            }
            return Err(NexusError::NotFound(format!(
                "native {harness} session {} owner runtime for {}:{}",
                req.harness_session_id, owner_name, binding.agent_id
            )));
        }
    }

    // (1) Resume on a globally known harness_session_id — ONLY when the requested name also
    // matches the bound identity. A harness_session_id recycled under a DIFFERENT name (e.g. a
    // Claude Code session id reused, or a session renamed then re-registered under a new
    // NEXUS_NAME) must NOT silently resume the prior identity — that is the "remy became enzo"
    // hijack (`harness_session_id` is non-unique, so two names can share one). On a name mismatch
    // we fall through to (2)/(3)/(4) so the new name gets its own identity and the prior row is
    // left untouched. A sanctioned rename goes through the `rename` verb, which updates the row's
    // name, so a post-rename reconnect still matches here and resumes correctly.
    let mut matching_harness_rows = repo
        .find_by_harness_session_id_any_project(&req.harness_session_id)
        .await?
        .into_iter()
        .filter(|existing| existing.name.as_deref() == Some(req_name));
    if let Some(existing) = matching_harness_rows.next() {
        if matching_harness_rows.next().is_some() {
            return Err(NexusError::Ambiguous(format!(
                "native session {:?} matches multiple legacy identities named {req_name:?}",
                req.harness_session_id
            )));
        }
        require_matching_client_key(&existing, req)?;
        return Ok(RegisterOutcome::Resumed(existing));
    }

    // (2) Resume on a globally known client_key. Project is descriptive metadata and cannot fork
    // one credential into a second identity.
    if let Some(existing) = repo.find_by_client_key_any_project(&req.client_key).await? {
        return Ok(RegisterOutcome::Resumed(existing));
    }

    // (3) Resolve the compatibility name globally and uniquely before any write. A project label
    // must not hide an existing owner or select one row from legacy duplicates.
    if let Some(by_name) = repo.find_unique_by_name_any_project(req_name).await? {
        if is_live(&by_name) {
            return Err(NexusError::DuplicateName(req_name.to_string()));
        }
        // A dead (offline) row with this name: the unique constraint still holds it, so we cannot
        // create a second row with the same name. Adopting it rebinds its client_key/harness on
        // the service layer — a takeover write. Gate that on authenticating as the identity: the
        // caller must present an `agent_id` (whose `runtime_credential` the service verifies in
        // preflight before any write). Without an `agent_id`, reject loudly rather than silently
        // handing the name to an unauthenticated caller.
        if req.agent_id.is_none() {
            return Err(NexusError::DuplicateName(format!(
                "'{}' is held by an offline session in project '{}'; re-register with its \
                 agent_id + runtime_credential to adopt it, or choose a different name",
                req_name, by_name.project
            )));
        }
        return Ok(RegisterOutcome::Resumed(by_name));
    }

    // Durable agent aliases are exclusive within the agent registration lane. Human/app sessions
    // occupy a separate authenticated principal namespace: sharing a display label must not turn
    // them into the agent, and command authentication explicitly strips any contaminated agent id.
    if req.kind.unwrap_or(nexus_contracts::Kind::Agent) == nexus_contracts::Kind::Agent {
        let durable_name_matches = Agents::new(store).find_all_by_name(req_name).await?;
        match durable_name_matches.as_slice() {
            [] => {}
            [owner]
                if req
                    .agent_id
                    .as_ref()
                    .is_some_and(|id| id.0.as_str() == owner.agent_id.as_str()) => {}
            [owner] if req.agent_id.is_none() => {
                return Err(NexusError::DuplicateName(format!(
                    "{req_name:?} is the alias of durable agent {}; register with its agent_id and runtime credential",
                    owner.agent_id
                )))
            }
            [owner] => {
                return Err(NexusError::DuplicateName(format!(
                    "{req_name:?} belongs to durable agent {}, not {}",
                    owner.agent_id,
                    req.agent_id.as_ref().expect("guarded above").0
                )))
            }
            matches => {
                return Err(NexusError::Ambiguous(format!(
                    "agent name {req_name:?} matches {} identities; address it by stable agent id",
                    matches.len()
                )))
            }
        }
    }

    // (4) Create + bind name ↔ harness_session_id.
    let new = NewSession {
        session_id: new_session_id(),
        name: Some(req_name.to_string()),
        agent: Some(req.harness.as_str().to_string()),
        kind: kind_str(req.locality, req.kind),
        role: req.role.clone(),
        tier: tier_str(req.tier).to_string(),
        harness_session_id: Some(req.harness_session_id.clone()),
        client_key: Some(req.client_key.clone()),
        cwd: req.cwd.clone(),
        project: req.project.clone(),
        transport: None, // legacy register path — transport unknown at registration time
    };
    let metadata_json = req
        .access
        .as_ref()
        .map(|access| serde_json::json!({"access": access}).to_string());
    let id = repo
        .create_staged_registration_with_metadata(new, metadata_json)
        .await?;
    match repo.find_by_session_id(&id).await {
        Ok(Some(created)) => Ok(RegisterOutcome::Created(created)),
        Ok(None) => {
            repo.remove_staged_registration(&id).await?;
            Err(NexusError::Internal(format!(
                "created session {id:?} not found"
            )))
        }
        Err(error) => {
            if let Err(cleanup) = repo.remove_staged_registration(&id).await {
                return Err(NexusError::Store(format!(
                    "{error}; staged session cleanup failed: {cleanup}"
                )));
            }
            Err(error)
        }
    }
}

fn require_matching_client_key(row: &SessionRow, req: &RegisterRequest) -> Result<(), NexusError> {
    if row.client_key.as_deref() == Some(req.client_key.as_str()) {
        return Ok(());
    }
    Err(NexusError::Unauthorized)
}
