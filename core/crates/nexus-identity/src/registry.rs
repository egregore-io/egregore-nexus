//! The register-once resolution (backend spec §5.1): idempotent on `client_key`, name↔session
//! binding, and duplicate-live-name rejection. This is the pure identity decision (resume vs
//! create vs reject); persistence is delegated to the `nexus-store` [`Sessions`] repo.

use nexus_common::{new_session_id, NexusError};
use nexus_contracts::register::RegisterRequest;
use nexus_store::repos::{Agents, NativeThreadBindings, NewSession, Sessions};
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
/// 1. Known `harness_session_id` (project-scoped) → **resume** that session. (The service layer
///    rebinds `client_key`/runtime state and marks it online.)
/// 2. Else, known `client_key` (project-scoped) → **resume** that session.
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
            if requested_agent_id.is_some_and(|id| id != binding.agent_id) || req_name != owner_name
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

    // (1) Resume on known harness_session_id (project-scoped) — ONLY when the requested name also
    // matches the bound identity. A harness_session_id recycled under a DIFFERENT name (e.g. a
    // Claude Code session id reused, or a session renamed then re-registered under a new
    // NEXUS_NAME) must NOT silently resume the prior identity — that is the "remy became enzo"
    // hijack (`harness_session_id` is non-unique, so two names can share one). On a name mismatch
    // we fall through to (2)/(3)/(4) so the new name gets its own identity and the prior row is
    // left untouched. A sanctioned rename goes through the `rename` verb, which updates the row's
    // name, so a post-rename reconnect still matches here and resumes correctly.
    if let Some(existing) = repo
        .find_by_harness_session_id(&req.project, &req.harness_session_id)
        .await?
    {
        if existing.name.as_deref() == Some(req_name) {
            require_matching_client_key(&existing, req)?;
            return Ok(RegisterOutcome::Resumed(existing));
        }
    }

    // (2) Resume on known client_key (project-scoped). This preserves legacy register-once
    // behavior for callers that only have the rotating client key.
    if let Some(existing) = repo
        .find_by_client_key(&req.project, &req.client_key)
        .await?
    {
        return Ok(RegisterOutcome::Resumed(existing));
    }

    // (3) Reject a different client_key claiming a live name (one name = one session).
    if let Some(by_name) = repo.find_by_name(&req.project, req_name).await? {
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
                req_name, req.project
            )));
        }
        return Ok(RegisterOutcome::Resumed(by_name));
    }

    // (4) Create + bind name ↔ harness_session_id.
    let new = NewSession {
        session_id: new_session_id(),
        name: Some(req_name.to_string()),
        agent: Some(req.harness.as_str().to_string()),
        kind: kind_str(req.kind).to_string(),
        role: req.role.clone(),
        tier: tier_str(req.tier).to_string(),
        harness_session_id: Some(req.harness_session_id.clone()),
        client_key: Some(req.client_key.clone()),
        cwd: req.cwd.clone(),
        project: req.project.clone(),
        transport: None, // legacy register path — transport unknown at registration time
    };
    let id = repo.create(new).await?;
    let created = repo
        .find_by_client_key(&req.project, &req.client_key)
        .await?
        .ok_or_else(|| NexusError::Internal(format!("created session {id:?} not found")))?;
    Ok(RegisterOutcome::Created(created))
}

fn require_matching_client_key(row: &SessionRow, req: &RegisterRequest) -> Result<(), NexusError> {
    if row.client_key.as_deref() == Some(req.client_key.as_str()) {
        return Ok(());
    }
    Err(NexusError::Unauthorized)
}
