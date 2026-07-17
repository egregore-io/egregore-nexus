//! The transport-agnostic **request router** (Task 14).
//!
//! [`route_request`] is the single entry point for RPC-style daemon operations: the store-backed command
//! worker builds a [`Request`] plus an optional resolved [`Caller`] and calls straight in here. It:
//!
//! 1. reads `req.method`,
//! 2. deserializes `req.params` into the registry type for that method (unit-param methods accept
//!    `null`/absent params),
//! 3. authenticates — `register` and `notify` are unauthenticated; every other method requires a
//!    resolved [`Caller`], and `admin.*` methods require an admin-tier caller before parsing the
//!    command-specific target,
//! 4. calls the matching port method on [`AppState`],
//! 5. serializes the result into `Response.result`, or maps a [`nexus_contracts::ContractError`]/routing
//!    failure into `Response.error` via [`nexus_contracts::codes`].
//!
//! serde lives here because the JSON-RPC envelope and command-intent rows store
//! `params`/`result` as `serde_json::Value`.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use nexus_common::NexusError;
use nexus_contracts::{
    validate_send_request, AckRequest, AckThreadsRequest, AdminAssignRequest,
    AdminGroupAssignRequest, AdminRenameRequest, AgentAccessGrantRequest, AgentAccessRevokeRequest,
    AgentCreateRequest, AgentCredentialCreateRequest, AgentCredentialRevokeRequest,
    AgentListRequest, AgentOwnerTransferRequest, AgentRuntimeListRequest, AgentShowRequest,
    AgentUpdateKind, ArchiveThreadRequest, AssignProjectRequest, AssignRoleRequest, Caller,
    ChannelRequest, ConsumeRequest, ContractError, CreateThreadRequest, DeleteThreadRequest,
    DlqEntry, DlqListRequest, DlqListResponse, DlqMutationResponse, DlqPurgeRequest,
    DlqRequeueRequest, GrantTierRequest, HistoryRequest, InboxSubscribeRequest,
    InboxSubscribeResponse, InboxSubscriptionAckRequest, InboxSubscriptionBatch,
    InboxSubscriptionNextRequest, InboxSubscriptionNextResponse, InboxSubscriptionStatusResponse,
    InboxUnsubscribeRequest, JoinThreadRequest, LeaveThreadRequest, MemberListRequest,
    MetadataSetRequest, MonitorRequest, NexusBatch, NotifyRequest, NotifySendRequest, NotifyTarget,
    PushRequest, ReadRequest, RegisterRequest, RemoveRequest, RemoveResponse, RenameThreadRequest,
    Request, Response, RouteForwardRequest, RpcError, SearchRequest, SendRequest, SourceRef,
    SourceRegisterRequest, SpawnRequest, StatusRequest, SteerRequest, SubscribeRequest,
    ThreadMembersRequest, Tier, UnsubscribeRequest, WsEvent,
};
use nexus_store::repos::{
    caller_subscription_id, subscription_now, Agents, DeadLetterFilter, DeadLetterMutation,
    DeadLetterSelector, DeveloperEvents, Inbox, InboxSubscriptionBatchRow, InboxSubscriptions,
    NewInboxSubscription, Sessions,
};
use nexus_store::types::SessionRow;

use crate::daemon::app::AppState;
use crate::daemon::slash_commands::{prompt_slash_action, PromptSlashAction};
use crate::error::{contract_to_rpc, invalid_params, method_not_found, unauthorized};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AttachLifecycleRequest {
    session_id: nexus_contracts::SessionId,
}

fn bad_request(message: impl Into<String>) -> ContractError {
    ContractError {
        code: nexus_contracts::codes::INVALID_PARAMS,
        message: message.into(),
    }
}

fn require_gateway_read(state: &AppState, method: &str) -> Result<(), RpcError> {
    if state.store.has_split_authority() {
        return Err(RpcError {
            code: nexus_contracts::codes::METHOD_NOT_FOUND,
            message: format!("{method} history is Gateway-owned; use the Nexus Gateway REST API"),
            data: None,
        });
    }
    Ok(())
}

/// Parse `params` (a `serde_json::Value`) into the registry type `T`. Absent params decode as
/// `null`, which `serde` accepts for unit-like / all-optional DTOs (the unit-param methods).
fn parse<T: DeserializeOwned>(params: &Option<Value>) -> Result<T, RpcError> {
    let v = params.clone().unwrap_or(Value::Null);
    serde_json::from_value(v).map_err(invalid_params)
}

/// Serialize a port result into `Response.result`.
fn ok<T: Serialize>(id: Option<nexus_contracts::RequestId>, value: T) -> Response {
    Response {
        jsonrpc: nexus_contracts::JSONRPC_VERSION.to_string(),
        id,
        result: Some(serde_json::to_value(value).unwrap_or(Value::Null)),
        error: None,
    }
}

/// Build an error `Response`.
fn err(id: Option<nexus_contracts::RequestId>, error: RpcError) -> Response {
    Response {
        jsonrpc: nexus_contracts::JSONRPC_VERSION.to_string(),
        id,
        result: None,
        error: Some(error),
    }
}

/// Require a resolved caller for an authenticated method.
fn require<'a>(caller: &'a Option<Caller>) -> Result<&'a Caller, RpcError> {
    caller
        .as_ref()
        .ok_or_else(|| unauthorized("authentication required"))
}

async fn notify_thread_add(
    state: &AppState,
    caller: &Caller,
    member: &str,
    thread: &str,
) -> Result<(), RpcError> {
    let body = json!({
        "event": "thread.added",
        "thread": thread,
        "member": member,
        "addedBy": caller.name,
    })
    .to_string();
    state
        .bus
        .notify(
            caller,
            NotifySendRequest {
                target: NotifyTarget::Name {
                    name: member.to_string(),
                },
                source: Some("nexus-thread".into()),
                body,
                idempotency_key: None,
            },
        )
        .await
        .map(|_| ())
        .map_err(|e| contract_to_rpc(&e))
}

async fn prompt_target_row(
    state: &AppState,
    project: &str,
    name: &str,
) -> Result<SessionRow, RpcError> {
    let sessions = Sessions::new(&state.store);
    match sessions
        .find_by_name(project, name)
        .await
        .map_err(|e| contract_to_rpc(&e.to_contract_error()))?
    {
        Some(row) => Ok(row),
        None => sessions
            .find_by_name_any_project(name)
            .await
            .map_err(|e| contract_to_rpc(&e.to_contract_error()))?
            .ok_or_else(|| {
                invalid_params(format!("prompt target {name} was not found after revive"))
            }),
    }
}

async fn prompt_target_row_by_session(
    state: &AppState,
    session: &nexus_contracts::SessionId,
) -> Result<SessionRow, RpcError> {
    Sessions::new(&state.store)
        .find_by_session_id(session)
        .await
        .map_err(|e| contract_to_rpc(&e.to_contract_error()))?
        .ok_or_else(|| {
            invalid_params(format!(
                "prompt target session {} was not found after revive",
                session.0
            ))
        })
}

async fn ensure_alive_agent_in_caller_project(
    state: &AppState,
    caller: &Caller,
    agent_id: &nexus_contracts::AgentId,
) -> Result<nexus_contracts::SessionId, RpcError> {
    let agent = Agents::new(&state.store)
        .find_by_id(&agent_id.0)
        .await
        .map_err(|e| contract_to_rpc(&e.to_contract_error()))?
        .ok_or_else(|| {
            contract_to_rpc(
                &NexusError::NotFound(format!("agent id {} does not exist", agent_id.0))
                    .to_contract_error(),
            )
        })?;
    if agent.project != caller.project {
        return Err(contract_to_rpc(
            &NexusError::NotFound(format!(
                "agent id {} does not exist in project {}",
                agent_id.0, caller.project
            ))
            .to_contract_error(),
        ));
    }
    state
        .ensure_alive_agent(&agent.agent_id)
        .await
        .map_err(|e| contract_to_rpc(&e))
}

/// Require an admin-tier caller before an `admin.*` operation parses or resolves its target.
///
/// Most admin operations delegate to [`nexus_admin::Admin`], which also tier-guards. The dispatch
/// guard is still load-bearing because a few daemon-local admin verbs (`remove`/`evict`/`delete`
/// and group assignment) call app-level helpers directly and must not leak lookup results to an
/// agent-tier caller.
fn require_admin<'a>(caller: &'a Option<Caller>) -> Result<&'a Caller, RpcError> {
    let c = require(caller)?;
    if c.tier == Tier::Admin {
        Ok(c)
    } else {
        Err(unauthorized("admin tier required"))
    }
}

fn parse_dlq_since(raw: Option<&str>) -> Result<Option<i64>, ContractError> {
    let Some(raw) = raw.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(None);
    };
    if let Ok(ts) = raw.parse::<i64>() {
        return Ok(Some(ts));
    }
    if raw.len() < 2 {
        return Err(bad_request("invalid --since value"));
    }
    let (num, unit) = raw.split_at(raw.len() - 1);
    let amount = num
        .parse::<i64>()
        .map_err(|_| bad_request("invalid --since value"))?;
    let millis = match unit {
        "s" => amount * 1_000,
        "m" => amount * 60_000,
        "h" => amount * 60 * 60_000,
        "d" => amount * 24 * 60 * 60_000,
        _ => return Err(bad_request("invalid --since value")),
    };
    Ok(Some(nexus_common::now().saturating_sub(millis)))
}

fn dlq_filter(
    for_target: Option<String>,
    since: Option<String>,
    limit: Option<u32>,
) -> Result<DeadLetterFilter, ContractError> {
    Ok(DeadLetterFilter {
        target: for_target,
        since: parse_dlq_since(since.as_deref())?,
        limit: limit.unwrap_or_default(),
    })
}

fn dlq_selector(
    in_flight_id: Option<String>,
    for_target: Option<String>,
    since: Option<String>,
) -> Result<DeadLetterSelector, ContractError> {
    if let Some(id) = in_flight_id {
        if id.trim().is_empty() {
            return Err(bad_request("inFlightId cannot be empty"));
        }
        return Ok(DeadLetterSelector::InFlightId(id));
    }
    if for_target.is_none() && since.is_none() {
        return Err(bad_request(
            "dlq batch mutation requires inFlightId or --for/--since filter",
        ));
    }
    Ok(DeadLetterSelector::Filter(dlq_filter(
        for_target, since, None,
    )?))
}

fn dlq_entry(row: nexus_store::repos::DeadLetterEntry) -> DlqEntry {
    DlqEntry {
        in_flight_id: row.in_flight_id,
        message_id: row.message_id,
        sender: row.sender,
        recipient_name: row.recipient_name,
        recipient_agent_id: row.recipient_agent_id.map(nexus_contracts::AgentId),
        recipient_session: row.recipient_session,
        created_at: row.created_at,
        dead_lettered_at: row.dead_lettered_at,
        attempt_count: row.attempt_count,
        error_code: row.error_code,
        error_reason: row.error_reason,
        error_details: row.error_details,
        body_preview: row.body_preview,
    }
}

fn dlq_mutation_response(mutation: DeadLetterMutation) -> DlqMutationResponse {
    DlqMutationResponse {
        count: mutation.count,
        in_flight_ids: mutation.in_flight_ids,
    }
}

async fn append_dlq_event(state: &AppState, caller: &Caller, lifecycle: &str, ids: &[String]) {
    let data = serde_json::json!({
        "count": ids.len(),
        "inFlightIds": ids,
    })
    .to_string();
    if let Err(error) = DeveloperEvents::new(&state.store)
        .append_agent_lifecycle_with_data(
            &caller.name,
            &caller.session,
            lifecycle,
            None,
            Some(&data),
            nexus_common::now(),
        )
        .await
    {
        tracing::warn!(
            error = %error,
            lifecycle,
            "dlq developer-event telemetry append failed"
        );
    }
}

async fn admin_dlq_list(
    state: &AppState,
    req: DlqListRequest,
) -> Result<DlqListResponse, ContractError> {
    let filter = dlq_filter(req.for_target, req.since, req.limit)?;
    let rows = Inbox::new(&state.store)
        .dead_letters(filter)
        .await
        .map_err(|e| e.to_contract_error())?;
    Ok(DlqListResponse {
        total: rows.len() as u64,
        rows: rows.into_iter().map(dlq_entry).collect(),
    })
}

async fn admin_dlq_requeue(
    state: &AppState,
    caller: &Caller,
    req: DlqRequeueRequest,
) -> Result<DlqMutationResponse, ContractError> {
    let selector = dlq_selector(req.in_flight_id, req.for_target, req.since)?;
    let mutation = Inbox::new(&state.store)
        .requeue_dead_letters(selector)
        .await
        .map_err(|e| e.to_contract_error())?;
    if mutation.count > 0 {
        append_dlq_event(state, caller, "dlq.requeue", &mutation.in_flight_ids).await;
        state.drive_explicit_delivery_requeue().await;
    }
    Ok(dlq_mutation_response(mutation))
}

async fn admin_dlq_purge(
    state: &AppState,
    caller: &Caller,
    req: DlqPurgeRequest,
) -> Result<DlqMutationResponse, ContractError> {
    let is_batch = req.in_flight_id.is_none();
    if is_batch && !req.yes {
        return Err(bad_request("batch dlq purge requires yes=true"));
    }
    let selector = dlq_selector(req.in_flight_id, req.for_target, req.since)?;
    let mutation = Inbox::new(&state.store)
        .purge_dead_letters(selector)
        .await
        .map_err(|e| e.to_contract_error())?;
    if mutation.count > 0 {
        append_dlq_event(state, caller, "dlq.purge", &mutation.in_flight_ids).await;
    }
    Ok(dlq_mutation_response(mutation))
}

const MAX_INBOX_SUBSCRIPTION_NEXT_TIMEOUT_MS: u32 = 1_000;

async fn inbox_subscribe(
    state: &AppState,
    caller: &Caller,
    req: InboxSubscribeRequest,
) -> Result<InboxSubscribeResponse, RpcError> {
    let subscription_id = caller_subscription_id(&caller.project, &caller.session.0);
    InboxSubscriptions::new(&state.store)
        .upsert_active(NewInboxSubscription {
            subscription_id: subscription_id.clone(),
            project: caller.project.clone(),
            caller_name: caller.name.clone(),
            caller_session_id: caller.session.0.clone(),
            caller_agent_id: caller.agent_id.as_ref().map(|id| id.0.clone()),
            caller_client_key: session_client_key(state, caller).await?,
            timeout_ms: req.timeout_ms.map(i64::from),
            max: req.max.map(i64::from),
            created_at: subscription_now(),
        })
        .await
        .map_err(|e| contract_to_rpc(&e.to_contract_error()))?;
    state.claim_inbox_subscription_delivery(&caller.session);
    Ok(InboxSubscribeResponse {
        subscription_id,
        active: true,
    })
}

async fn inbox_subscription_next(
    state: &AppState,
    caller: &Caller,
    req: InboxSubscriptionNextRequest,
) -> Result<InboxSubscriptionNextResponse, RpcError> {
    let repo = InboxSubscriptions::new(&state.store);
    let subscription = require_owned_subscription(&repo, caller, &req.subscription_id).await?;
    if let Some(row) = repo
        .pending_batch(&req.subscription_id)
        .await
        .map_err(|e| contract_to_rpc(&e.to_contract_error()))?
    {
        return Ok(InboxSubscriptionNextResponse {
            batch: Some(subscription_batch_from_row(row)?),
        });
    }

    let timeout_ms = req
        .timeout_ms
        .or_else(|| {
            subscription
                .timeout_ms
                .and_then(|value| u32::try_from(value).ok())
        })
        .map(|value| value.min(MAX_INBOX_SUBSCRIPTION_NEXT_TIMEOUT_MS));
    let max = subscription.max.and_then(|value| u32::try_from(value).ok());
    let batch = state
        .realtime
        .consume(caller, ConsumeRequest { timeout_ms, max })
        .await
        .map_err(|e| contract_to_rpc(&e))?;
    if batch.counts.total == 0 {
        return Ok(InboxSubscriptionNextResponse { batch: None });
    }
    let row = match repo
        .insert_pending_batch(&req.subscription_id, &batch, subscription_now())
        .await
        .map_err(|e| contract_to_rpc(&e.to_contract_error()))?
    {
        Some(row) => Some(row),
        None => repo
            .pending_batch(&req.subscription_id)
            .await
            .map_err(|e| contract_to_rpc(&e.to_contract_error()))?,
    };
    repo.mark_drained(&req.subscription_id, subscription_now())
        .await
        .map_err(|e| contract_to_rpc(&e.to_contract_error()))?;
    Ok(InboxSubscriptionNextResponse {
        batch: row.map(subscription_batch_from_row).transpose()?,
    })
}

async fn inbox_subscription_ack(
    state: &AppState,
    caller: &Caller,
    req: InboxSubscriptionAckRequest,
) -> Result<InboxSubscriptionStatusResponse, RpcError> {
    let repo = InboxSubscriptions::new(&state.store);
    let _ = require_owned_subscription(&repo, caller, &req.subscription_id).await?;
    let updated = repo
        .acknowledge_pull_batch(
            &req.subscription_id,
            &req.batch_id,
            &caller.session,
            subscription_now(),
        )
        .await
        .map_err(|e| contract_to_rpc(&e.to_contract_error()))?;
    if !updated {
        return Err(invalid_params(format!(
            "subscription batch {} is not pending for {}",
            req.batch_id, req.subscription_id
        )));
    }
    Ok(InboxSubscriptionStatusResponse {
        subscription_id: req.subscription_id,
        active: true,
    })
}

async fn inbox_unsubscribe(
    state: &AppState,
    caller: &Caller,
    req: InboxUnsubscribeRequest,
) -> Result<InboxSubscriptionStatusResponse, RpcError> {
    let repo = InboxSubscriptions::new(&state.store);
    let _ = require_owned_subscription(&repo, caller, &req.subscription_id).await?;
    repo.mark_inactive(&req.subscription_id, subscription_now())
        .await
        .map_err(|e| contract_to_rpc(&e.to_contract_error()))?;
    // Subscribing transfers this session's inbox from the daemon-owned harness loop to the
    // durable pull consumer. Once that claim is inactive, immediately restore the harness loop
    // (if this caller is an injectable agent) so pending and future messages auto-wake again.
    state.ensure_agent_loop(&caller.project, &caller.name).await;
    Ok(InboxSubscriptionStatusResponse {
        subscription_id: req.subscription_id,
        active: false,
    })
}

async fn session_client_key(state: &AppState, caller: &Caller) -> Result<Option<String>, RpcError> {
    Ok(Sessions::new(&state.store)
        .find_by_session_id(&caller.session)
        .await
        .map_err(|e| contract_to_rpc(&e.to_contract_error()))?
        .and_then(|row| row.client_key))
}

async fn require_owned_subscription(
    repo: &InboxSubscriptions<'_>,
    caller: &Caller,
    subscription_id: &str,
) -> Result<nexus_store::repos::InboxSubscriptionRow, RpcError> {
    let row = repo
        .get(subscription_id)
        .await
        .map_err(|e| contract_to_rpc(&e.to_contract_error()))?
        .ok_or_else(|| invalid_params(format!("subscription {subscription_id} not found")))?;
    if row.project != caller.project || row.caller_session_id != caller.session.0 {
        return Err(unauthorized("subscription belongs to a different caller"));
    }
    if row.status != "active" {
        return Err(invalid_params(format!(
            "subscription {subscription_id} is not active"
        )));
    }
    Ok(row)
}

fn subscription_batch_from_row(
    row: InboxSubscriptionBatchRow,
) -> Result<InboxSubscriptionBatch, RpcError> {
    let batch: NexusBatch = serde_json::from_str(&row.batch_json).map_err(invalid_params)?;
    Ok(InboxSubscriptionBatch {
        batch_id: row.batch_id,
        subscription_id: row.subscription_id,
        batch,
    })
}

/// The shared method registry. Daemon ingress paths call this with a parsed [`Request`] and the
/// caller resolved by that path (`None` until authenticated). Always returns a [`Response`] — port
/// errors become `Response.error`, never a panic.
pub async fn route_request(state: &AppState, caller: Option<Caller>, req: Request) -> Response {
    let id = req.id.clone();
    match route_request_inner(state, &caller, &req).await {
        Ok(resp_value) => ok(id, resp_value),
        Err(e) => err(id, e),
    }
}

/// Compatibility name for the pre-v0.1 internal request-router entry point.
///
/// New daemon code calls [`route_request`]. This alias preserves the existing Rust composition API
/// without retaining a second routing implementation or changing any CLI/wire method.
pub use route_request as dispatch;

/// Inner registry: returns the success `Value` or an `RpcError`. Splitting it keeps the `match`
/// arms terse (each ends in a serialized success value).
async fn route_request_inner(
    state: &AppState,
    caller: &Option<Caller>,
    req: &Request,
) -> Result<Value, RpcError> {
    let p = &req.params;
    match req.method.as_str() {
        // ---- Unauthenticated ----
        "register" => {
            let r: RegisterRequest = parse(p)?;
            let project = r.project.clone();
            let name = r.name.clone();
            let out = state
                .identity
                .register(r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            // Stand up the per-agent idle loop so any registered agent is wakeable by a dm
            // (spec: "idle loop stood up"). Idempotent across resume/re-register.
            if let Some(name) = name.as_deref() {
                state.ensure_agent_loop(&project, name).await;
            }
            Ok(serde_json::to_value(out).unwrap())
        }
        "notify" => {
            // Direct registry dispatch is never a public trust boundary. The store-backed command
            // worker owns signed-envelope verification; a bare RPC-shaped notify is recorded as
            // unverified and dropped without touching an agent path.
            let r: NotifyRequest = parse(p)?;
            let out = state
                .notify
                .ingest(r, false)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            // Revive any DEAD recipient so a routed notification wakes a dead agent — the SAME
            // mechanism as DMs/threads. `ensure_harness_live` resolves the name across projects and
            // its respawn re-rings the drain, so the just-ingested notification lands once the
            // harness is back. No-op for live agents / non-agents.
            for name in &out.routed_to {
                let _ = state.ensure_alive(name, "default").await;
            }
            Ok(serde_json::to_value(out).unwrap())
        }

        // ---- Identity (authenticated) ----
        "whoami" => {
            let c = require(caller)?;
            let out = state
                .identity
                .whoami(c)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }
        "members" => {
            let c = require(caller)?;
            let r: MemberListRequest = parse(p)?;
            let out = state
                .identity
                .members(c, r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }
        // Self-service rename: the caller (an agent, via its MCP/CLI) changes its own name.
        "rename" => {
            let c = require(caller)?;
            let r: nexus_contracts::RenameRequest = parse(p)?;
            let out = state
                .identity
                .rename(c, r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }
        "status" => {
            let c = require(caller)?;
            let r: StatusRequest = parse(p)?;
            let state_change = r.state;
            let out = state
                .identity
                .status(c, r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            // Bridge a pause/resume into the realtime wake state (spec §2.4): the persisted `paused`
            // flag alone does not hold the bus's in-memory wake decision.
            if let Some(s) = state_change {
                let paused = matches!(s, nexus_contracts::StatusState::Paused);
                state.apply_pause_state(&c.session, paused);
            }
            Ok(serde_json::to_value(out).unwrap())
        }
        "heartbeat" => {
            let c = require(caller)?;
            let out = state
                .identity
                .heartbeat(c)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }
        "identity.attach" => {
            let c = require(caller)?;
            let r: AttachLifecycleRequest = parse(p)?;
            state
                .record_attach_lifecycle(c, &r.session_id)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(json!({ "sessionId": r.session_id.0 }))
        }

        // ---- Freeform entity metadata (authenticated, intentionally ungated) ----
        "metadata.set" => {
            let c = require(caller)?;
            let r: MetadataSetRequest = parse(p)?;
            let out = state
                .set_entity_metadata(c, r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }

        // ---- Durable agent identity/runtime lifecycle ----
        "agent.create" => {
            let c = require(caller)?;
            let r: AgentCreateRequest = parse(p)?;
            let out = state
                .create_agent_identity(c, r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }
        "agent.list" => {
            let c = require(caller)?;
            let r: AgentListRequest = parse(p)?;
            let out = state
                .list_agent_identities(c, r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }
        "agent.show" => {
            let c = require(caller)?;
            let r: AgentShowRequest = parse(p)?;
            let out = state
                .show_agent_identity(c, r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }
        "agent.grantAccess" => {
            let c = require(caller)?;
            let r: AgentAccessGrantRequest = parse(p)?;
            let out = state
                .grant_agent_access(c, r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }
        "agent.revokeAccess" => {
            let c = require(caller)?;
            let r: AgentAccessRevokeRequest = parse(p)?;
            let out = state
                .revoke_agent_access(c, r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }
        "agent.transferOwner" => {
            let c = require(caller)?;
            let r: AgentOwnerTransferRequest = parse(p)?;
            let out = state
                .transfer_agent_owner(c, r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }
        "agent.credential.create" => {
            let c = require(caller)?;
            let r: AgentCredentialCreateRequest = parse(p)?;
            let out = state
                .create_agent_credential(c, r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }
        "agent.credential.revoke" => {
            let c = require(caller)?;
            let r: AgentCredentialRevokeRequest = parse(p)?;
            let out = state
                .revoke_agent_credential(c, r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }
        "agent.runtime.list" => {
            let c = require(caller)?;
            let r: AgentRuntimeListRequest = parse(p)?;
            let out = state
                .list_agent_runtimes(c, r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }

        // ---- Agent transport ----
        "launch" => {
            let c = require(caller)?;
            let r: SpawnRequest = parse(p)?;
            // `launch_agent` (not the bare `agent.launch`) is what makes the launched agent an
            // addressable, wakeable bus member: open+bind adapter → register identity → spawn loop.
            // Pass the CALLER'S project so the launched agent is registered in it (and thus visible to
            // the caller's project-scoped `members`/`resolve`) when the request omits a project — the
            // real `nexus launch <kind>` CLI invocation sends no `--project`.
            let out = state
                .launch_agent(r, &c.project, Some(c))
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }

        // ---- Bus / realtime ----
        "send" => {
            let c = require(caller)?;
            let r: SendRequest = parse(p)?;
            validate_send_request(&r).map_err(|e| contract_to_rpc(&e))?;
            // A thread post must reach its agent MEMBERS, not just sit in their inboxes. After the
            // bus writes the fan-out, wake every agent member (ensure adapter + drain loop) so the
            // just-posted message is drained + injected and they reply — the "everyone in the thread
            // sees it and responds" contract. DMs/topics keep their existing paths.
            let post_thread = match &r.to {
                nexus_contracts::SendTarget::Post { thread } => Some(thread.clone()),
                _ => None,
            };
            let dm_target = match &r.to {
                nexus_contracts::SendTarget::Dm { name, agent_id } => {
                    Some((name.clone(), agent_id.clone()))
                }
                _ => None,
            };
            if let Some((name, agent_id)) = &dm_target {
                state
                    .hold_cold_dm_target(name.as_deref(), agent_id.as_ref())
                    .await;
            }
            let out = state
                .bus
                .send(c, r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            if let Some((name, agent_id)) = dm_target {
                state.wake_dm_agent(name, agent_id, c.project.clone(), out.message_id.clone());
            }
            if let Some(thread) = post_thread {
                state
                    .wake_thread_agents(&thread, &c.project, &c.name, &out.message_id)
                    .await;
            }
            Ok(serde_json::to_value(out).unwrap())
        }
        // DIRECT operator→agent DM — inject straight into the agent's ACP session, NOT the bus.
        // Resolve the target name → its session (in the caller's project), then prompt it. The
        // reply is captured as `agent.update` rows (the gateway formats them to AG-UI via observe).
        "prompt" => {
            let c = require(caller)?;
            let r: nexus_contracts::PromptRequest = parse(p)?;
            // Revive the agent via the correct backend for its transport (pty or acp), then inject.
            let session = match r.agent_id.as_ref() {
                Some(agent_id) => ensure_alive_agent_in_caller_project(state, c, agent_id).await,
                None => state
                    .ensure_alive(&r.name, &c.project)
                    .await
                    .map_err(|e| contract_to_rpc(&e)),
            }?;
            let row = match r.agent_id.as_ref() {
                Some(_) => prompt_target_row_by_session(state, &session).await?,
                None => prompt_target_row(state, &c.project, &r.name).await?,
            };
            match prompt_slash_action(&row, &r.text).map_err(|e| contract_to_rpc(&e))? {
                Some(PromptSlashAction::NativeCompact) => {
                    state
                        .agent
                        .compact(&session)
                        .await
                        .map_err(|e| contract_to_rpc(&e))?;
                    return Ok(serde_json::to_value(nexus_contracts::PromptResponse {
                        delivered: true,
                    })
                    .unwrap());
                }
                Some(PromptSlashAction::PromptVerbatim) | None => {}
            }
            state
                .agent
                .prompt_observed(
                    &session,
                    r.text.clone(),
                    std::sync::Arc::new(state.ws.clone()),
                    WsEvent::AgentUpdate {
                        session_id: session.clone(),
                        kind: AgentUpdateKind::UserInput,
                        data: json!({
                            "text": r.text,
                            "clientMessageId": r.client_message_id,
                        }),
                    },
                )
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(nexus_contracts::PromptResponse { delivered: true }).unwrap())
        }
        // EXPLICIT active-turn redirect. This is intentionally a separate command from `prompt`:
        // normal `harness.prompt` rows retain their per-session boundary queue. The routed adapter
        // either performs native steer or interrupt-and-send and emits the user-input row only at
        // its accepted boundary.
        "steer" => {
            let c = require(caller)?;
            let r: SteerRequest = parse(p)?;
            let session = match r.agent_id.as_ref() {
                Some(agent_id) => ensure_alive_agent_in_caller_project(state, c, agent_id).await,
                None => state
                    .ensure_alive(&r.name, &c.project)
                    .await
                    .map_err(|e| contract_to_rpc(&e)),
            }?;
            let row = match r.agent_id.as_ref() {
                Some(_) => prompt_target_row_by_session(state, &session).await?,
                None => prompt_target_row(state, &c.project, &r.name).await?,
            };
            let response = state
                .agent
                .steer_observed(
                    &session,
                    r.text.clone(),
                    std::sync::Arc::new(state.ws.clone()),
                    WsEvent::AgentUpdate {
                        session_id: session.clone(),
                        kind: AgentUpdateKind::UserInput,
                        data: json!({
                            "text": r.text,
                            "clientMessageId": r.client_message_id,
                            "source": "steer",
                            "name": row.name,
                        }),
                    },
                )
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(response).unwrap())
        }
        // NATIVE context compaction — same resolve+revive spine as `prompt`, but the
        // transport runs the REAL operation (codex `thread/compact/start`; headed PTY
        // types `/compact`). Transports without a compact verb error loudly instead of
        // injecting literal text the model would just read as chat.
        "compact" => {
            let c = require(caller)?;
            let r: nexus_contracts::CompactRequest = parse(p)?;
            let session = match r.agent_id.as_ref() {
                Some(agent_id) => ensure_alive_agent_in_caller_project(state, c, agent_id).await,
                None => state
                    .ensure_alive(&r.name, &c.project)
                    .await
                    .map_err(|e| contract_to_rpc(&e)),
            }?;
            state
                .agent
                .compact(&session)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(nexus_contracts::CompactResponse { started: true }).unwrap())
        }
        // Pre-warm an agent's ACP session WITHOUT injecting — the AionUi "spawn at
        // conversation-open" move. The web console calls this when a DM pane opens (the `observe`
        // subscribe), so the harness is spawned + the session opened/resumed BEFORE the first
        // send. Without it, message #1 pays the full cold-start (`npx` + ACP initialize +
        // `session/new`) synchronously — the latency the operator notices vs AionUi.
        "warm" => {
            let c = require(caller)?;
            let r: nexus_contracts::WarmRequest = parse(p)?;
            match r.agent_id.as_ref() {
                Some(agent_id) => ensure_alive_agent_in_caller_project(state, c, agent_id).await,
                None => state
                    .ensure_alive(&r.name, &c.project)
                    .await
                    .map_err(|e| contract_to_rpc(&e)),
            }?;
            Ok(serde_json::to_value(nexus_contracts::WarmResponse { warm: true }).unwrap())
        }

        "consume" => {
            let c = require(caller)?;
            let r: ConsumeRequest = parse(p)?;
            let out = state
                .realtime
                .consume(c, r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }
        "inbox.subscribe" => {
            let c = require(caller)?;
            let r: InboxSubscribeRequest = parse(p)?;
            let out = inbox_subscribe(state, c, r).await?;
            Ok(serde_json::to_value(out).unwrap())
        }
        "inbox.next" => {
            let c = require(caller)?;
            let r: InboxSubscriptionNextRequest = parse(p)?;
            let out = inbox_subscription_next(state, c, r).await?;
            Ok(serde_json::to_value(out).unwrap())
        }
        "inbox.subscriptionAck" => {
            let c = require(caller)?;
            let r: InboxSubscriptionAckRequest = parse(p)?;
            let out = inbox_subscription_ack(state, c, r).await?;
            Ok(serde_json::to_value(out).unwrap())
        }
        "inbox.unsubscribe" => {
            let c = require(caller)?;
            let r: InboxUnsubscribeRequest = parse(p)?;
            let out = inbox_unsubscribe(state, c, r).await?;
            Ok(serde_json::to_value(out).unwrap())
        }
        "ack" => {
            let c = require(caller)?;
            let r: AckRequest = parse(p)?;
            let out = state
                .realtime
                .ack(c, r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }
        "ackThreads" => {
            let c = require(caller)?;
            let r: AckThreadsRequest = parse(p)?;
            let out = state
                .realtime
                .ack_threads(c, r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }

        // ---- Threads ----
        "thread.new" => {
            let c = require(caller)?;
            let r: CreateThreadRequest = parse(p)?;
            state
                .bus
                .create_thread(c, r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(Value::Null)
        }
        "thread.join" => {
            let c = require(caller)?;
            let r: JoinThreadRequest = parse(p)?;
            state
                .bus
                .join_thread(c, r.clone())
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            notify_thread_add(state, c, &c.name, &r.name).await?;
            Ok(Value::Null)
        }
        "thread.leave" => {
            let c = require(caller)?;
            let r: LeaveThreadRequest = parse(p)?;
            state
                .bus
                .leave_thread(c, r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(Value::Null)
        }
        "thread.archive" => {
            let c = require(caller)?;
            let r: ArchiveThreadRequest = parse(p)?;
            state
                .bus
                .archive_thread(c, r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(Value::Null)
        }
        "thread.delete" => {
            let c = require(caller)?;
            let r: DeleteThreadRequest = parse(p)?;
            state
                .bus
                .delete_thread(c, r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(Value::Null)
        }
        "thread.rename" => {
            let c = require_admin(caller)?;
            let r: RenameThreadRequest = parse(p)?;
            state
                .bus
                .rename_thread(c, r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(Value::Null)
        }
        "thread.addMember" => {
            let c = require(caller)?;
            let r: nexus_contracts::ThreadMemberRequest = parse(p)?;
            // The member must live in the thread's workspace or fan-out can't reach it (spec §12.4).
            state
                .ensure_in_workspace(&r.member, &c.project)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            state
                .bus
                .add_thread_member(c, r.clone())
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            notify_thread_add(state, c, &r.member, &r.name).await?;
            Ok(Value::Null)
        }
        "thread.removeMember" => {
            let c = require(caller)?;
            let r: nexus_contracts::ThreadMemberRequest = parse(p)?;
            state
                .bus
                .remove_thread_member(c, r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(Value::Null)
        }
        "thread.members" => {
            let c = require(caller)?;
            let r: ThreadMembersRequest = parse(p)?;
            let out = state
                .bus
                .thread_members(c, r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }
        "threads" => {
            let c = require(caller)?;
            let out = state
                .bus
                .threads(c)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }

        // ---- Notification sources ----
        // Source pushes arrive after the gateway has verified any remote HMAC and the command
        // worker has resolved the caller.
        "push" => {
            let c = require(caller)?;
            let r: PushRequest = parse(p)?;
            let out = state
                .push_as_source(c, r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }

        // ---- Source lifecycle ----
        "source.register" => {
            let c = require(caller)?;
            let r: SourceRegisterRequest = parse(p)?;
            let out = state
                .register_source(c, r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }
        "source.list" => {
            let _ = require(caller)?;
            let out = state
                .list_sources()
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }
        "source.show" => {
            let _ = require(caller)?;
            let r: SourceRef = parse(p)?;
            let out = state
                .show_source(&r.name)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }
        "source.enable" => {
            let _ = require(caller)?;
            let r: SourceRef = parse(p)?;
            let out = state
                .set_source_enabled(&r.name, true)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }
        "source.disable" => {
            let _ = require(caller)?;
            let r: SourceRef = parse(p)?;
            let out = state
                .set_source_enabled(&r.name, false)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }
        "source.rotate" => {
            let c = require(caller)?;
            let r: SourceRef = parse(p)?;
            let out = state
                .rotate_source(c, &r.name)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }
        "source.remove" => {
            let _ = require(caller)?;
            let r: SourceRef = parse(p)?;
            let out = state
                .delete_source(&r.name)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }
        "source.token" => {
            let c = require(caller)?;
            let r: SourceRef = parse(p)?;
            let out = state
                .source_token(c, &r.name)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }

        // ---- Topics ----
        "subscribe" => {
            let c = require(caller)?;
            let r: SubscribeRequest = parse(p)?;
            let out = state
                .bus
                .subscribe(c, r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }
        "unsubscribe" => {
            let c = require(caller)?;
            let r: UnsubscribeRequest = parse(p)?;
            state
                .bus
                .unsubscribe(c, r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(Value::Null)
        }
        "topics" => {
            let c = require(caller)?;
            let out = state.bus.topics(c).await.map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }

        // ---- Search ----
        "search" => {
            require_gateway_read(state, "search")?;
            let c = require(caller)?;
            let r: SearchRequest = parse(p)?;
            let out = state
                .search
                .search(c, r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }
        "history" => {
            require_gateway_read(state, "history")?;
            let c = require(caller)?;
            let r: HistoryRequest = parse(p)?;
            let out = state
                .search
                .history(c, r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }
        "read" => {
            require_gateway_read(state, "read")?;
            let c = require(caller)?;
            let r: ReadRequest = parse(p)?;
            let out = state
                .read_message(c, &r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }

        // ---- Admin (tier-gated before target parsing/lookup; AdminPort also guards) ----
        "admin.spawn" => {
            let c = require_admin(caller)?;
            let r: SpawnRequest = parse(p)?;
            // `admin.spawn` must use the same AppState-owned orchestration as `launch`: open/bind
            // the harness, register the member row, and start the wake loop. The AdminPort seam is
            // delegate-only and its `agent.launch` target is below that orchestration.
            let out = state
                .launch_agent(r, &c.project, Some(c))
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }
        "admin.remove" => {
            let c = require_admin(caller)?;
            let r: RemoveRequest = parse(p)?;
            let out = state
                .remove_agent(c, r.agent_id.as_ref(), &r.name, r.kill)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }
        // EVICT — remove the agent from every thread it's in (session + process untouched).
        "admin.evict" => {
            let c = require_admin(caller)?;
            let r: RemoveRequest = parse(p)?;
            state
                .evict_agent(&r.name, &c.project)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(RemoveResponse {
                name: Some(r.name),
                status: "evicted".into(),
            })
            .unwrap())
        }
        // DELETE — purge the agent entirely (kill process + erase all daemon-store rows).
        "admin.delete" => {
            let c = require_admin(caller)?;
            let r: RemoveRequest = parse(p)?;
            let out = state
                .delete_agent(c, r.agent_id.as_ref(), &r.name)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }
        "admin.rename" => {
            let c = require_admin(caller)?;
            let r: AdminRenameRequest = parse(p)?;
            let out = state
                .identity
                .admin_rename(c, r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }
        "admin.assign" => {
            let c = require_admin(caller)?;
            let r: AdminAssignRequest = parse(p)?;
            let out = state
                .identity
                .admin_assign(c, r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }
        "admin.assignRole" => {
            let c = require_admin(caller)?;
            let r: AssignRoleRequest = parse(p)?;
            let out = state
                .assign_agent_role(c, r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }
        "admin.assignProject" => {
            let c = require_admin(caller)?;
            let r: AssignProjectRequest = parse(p)?;
            let out = state
                .assign_agent_project(c, r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }
        "admin.grantTier" => {
            let c = require_admin(caller)?;
            let r: GrantTierRequest = parse(p)?;
            let out = state
                .grant_agent_tier(c, r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }
        "admin.group.assign" => {
            let c = require_admin(caller)?;
            let r: AdminGroupAssignRequest = parse(p)?;
            let out = state
                .assign_agent_group(c, r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }
        "admin.channel" => {
            let c = require_admin(caller)?;
            let r: ChannelRequest = parse(p)?;
            state
                .admin
                .channel(c, r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(Value::Null)
        }
        "admin.route" => {
            let c = require_admin(caller)?;
            let r: RouteForwardRequest = parse(p)?;
            state
                .admin
                .route(c, r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(Value::Null)
        }
        "admin.monitor" => {
            let c = require_admin(caller)?;
            let r: MonitorRequest = parse(p)?;
            state
                .admin
                .monitor(c, r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(Value::Null)
        }
        "admin.dlq.list" => {
            let _c = require_admin(caller)?;
            let r: DlqListRequest = parse(p)?;
            let out = admin_dlq_list(state, r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }
        "admin.dlq.requeue" => {
            let c = require_admin(caller)?;
            let r: DlqRequeueRequest = parse(p)?;
            let out = admin_dlq_requeue(state, c, r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }
        "admin.dlq.purge" => {
            let c = require_admin(caller)?;
            let r: DlqPurgeRequest = parse(p)?;
            let out = admin_dlq_purge(state, c, r)
                .await
                .map_err(|e| contract_to_rpc(&e))?;
            Ok(serde_json::to_value(out).unwrap())
        }

        other => Err(method_not_found(other)),
    }
}
