//! Store-backed command ingress worker.
//!
//! Producers submit rows to `command_intents`; this daemon-local worker claims those rows and
//! executes the real operation through the existing daemon services and dispatch registry. This
//! removes caller dependencies on daemon ingress transports while keeping the daemon as the only
//! writer of canonical delivery, identity, lifecycle, and control state.
//! `source.push` carries its command-envelope idempotency key through an internal service seam
//! because crash-reclaim identity is transaction context, not part of the public producer payload.
//!
//! Authenticated command rows resolve their caller from the registered session `client_key` only.
//! A bare `caller_name` or copied session id is not proof of identity. Verified registered
//! command activity refreshes that caller session's heartbeat and runtime presence.

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::time::Duration;

use futures::FutureExt;
use serde::Serialize;
use serde_json::Value;
use tokio::task::JoinHandle;

use nexus_common::{now, NexusError};
use nexus_contracts::{
    codes, AgentId, Caller, ContractError, MessageId, NotifyCommandRequest, NotifyRequest,
    PromptRequest, Request, SessionId, Tier, JSONRPC_VERSION,
};
use nexus_store::command_kinds;
use nexus_store::repos::{
    AgentRef, Agents, CommandIntentRow, CommandIntents, DaemonState, Inbox, Sessions,
};
use nexus_store::types::SessionRow;

use crate::daemon::app::{AppState, PROMPT_DEFERRED_FOR_SHUTDOWN};
use crate::daemon::retention_policy::{maybe_reap_operational_tables, RetentionPolicyState};
use crate::daemon::routing;
use crate::local_operator::LOCAL_OPERATOR_SESSION_ID;

const POLL_INTERVAL: Duration = Duration::from_millis(250);
const LEASE_MS: i64 = 15_000;
/// Prompt execution is guarded by a 45s timeout in production. The prompt lane may spawn multiple
/// executions, so its store lease must outlive that timeout; otherwise the same claimed row could
/// be reclaimed while the original execution task is still running.
const HARNESS_PROMPT_LEASE_MS: i64 = 60_000;
/// Non-native redirect adapters keep the command alive until the replacement turn finishes.
/// Match the ACP turn ceiling so an interrupt-and-send row cannot be reclaimed mid-turn.
const HARNESS_REDIRECT_LEASE_MS: i64 = 660_000;
/// One command-worker slice for held inbox consumes.
///
/// Client-facing listen loops can ask for a longer held-receive window, but the daemon command row
/// itself must complete well before its lease can be reclaimed. Keeping one consume to this slice
/// lets CLI/MCP callers re-issue after daemon restarts and prevents a dead long-poll from pinning
/// the inbox lane.
const INBOX_CONSUME_SLICE_MS: u32 = 1_000;
#[cfg(not(test))]
const HARNESS_PROMPT_EXECUTION_TIMEOUT: Duration = Duration::from_secs(45);
#[cfg(test)]
const HARNESS_PROMPT_EXECUTION_TIMEOUT: Duration = Duration::from_millis(50);

/// Spawn the daemon's command-intent worker loops.
///
/// `inbox.consume` is a held-receive operation: it may intentionally wait for a bell before
/// returning. Keeping it on the same serial lane as write/control commands lets one long poll block
/// unrelated sends. Harness launch/warm/prompt commands can also wait on process startup or a whole
/// model turn. The worker therefore keeps held inbox consumes and held harness operations off the
/// write/control lane while preserving the same command-intent dispatch spine. Prompt intents are
/// additionally spawned per claim and serialized only by target-session claim rules.
pub fn spawn(state: AppState) -> JoinHandle<()> {
    tokio::spawn(supervise_worker_lanes(state))
}

/// Fence new ingress and wake idle lanes so the daemon can drain every command accepted before
/// this boundary, plus the provider turns those commands start, before tearing down transports.
pub async fn begin_shutdown(state: &AppState) {
    state.begin_command_worker_shutdown().await;
}

/// Await every transport-reported active turn without inferring completion from rendered output.
///
/// A transport that reports an active session is required to expose its authoritative completion
/// boundary. If that contract is temporarily unavailable, keep the shutdown drain open; the
/// caller's outer timeout remains the only authority allowed to abandon the graceful path.
pub async fn wait_for_active_turns(state: &AppState) {
    loop {
        let sessions = state.agent.active_turn_sessions();
        if sessions.is_empty() {
            return;
        }
        for session in sessions {
            if let Err(error) = state.agent.wait_for_turn_completion(&session).await {
                tracing::warn!(
                    %error,
                    session_id = %session.0,
                    "active turn completion boundary unavailable during shutdown"
                );
                tokio::time::sleep(POLL_INTERVAL).await;
                break;
            }
        }
    }
}

/// Claim and execute at most one pending command. Returns `true` when a row was claimed.
pub async fn process_next(state: &AppState) -> Result<bool, NexusError> {
    process_next_for_lane(state, WorkerLane::Any).await
}

async fn supervise_worker_lanes(state: AppState) {
    let mut control = LaneHandle::spawn(state.clone(), WorkerLane::Control);
    let mut inbox = LaneHandle::spawn(state.clone(), WorkerLane::InboxConsume);
    let mut harness_prompt = LaneHandle::spawn(state.clone(), WorkerLane::HarnessPrompt);
    let mut harness_steer = LaneHandle::spawn(state.clone(), WorkerLane::HarnessSteer);
    let mut harness_warm = LaneHandle::spawn(state.clone(), WorkerLane::HarnessWarm);
    let mut harness_compact = LaneHandle::spawn(state.clone(), WorkerLane::HarnessCompact);
    let mut harness_launch = LaneHandle::spawn(state.clone(), WorkerLane::HarnessLaunch);

    loop {
        tokio::select! {
            result = &mut control.handle => {
                log_lane_exit(WorkerLane::Control, result);
                if !state.command_worker_is_shutting_down() {
                    control.restart(state.clone());
                }
            }
            result = &mut inbox.handle => {
                log_lane_exit(WorkerLane::InboxConsume, result);
                if !state.command_worker_is_shutting_down() {
                    inbox.restart(state.clone());
                }
            }
            result = &mut harness_prompt.handle => {
                log_lane_exit(WorkerLane::HarnessPrompt, result);
                if !state.command_worker_is_shutting_down() {
                    harness_prompt.restart(state.clone());
                }
            }
            result = &mut harness_steer.handle => {
                log_lane_exit(WorkerLane::HarnessSteer, result);
                if !state.command_worker_is_shutting_down() {
                    harness_steer.restart(state.clone());
                }
            }
            result = &mut harness_warm.handle => {
                log_lane_exit(WorkerLane::HarnessWarm, result);
                if !state.command_worker_is_shutting_down() {
                    harness_warm.restart(state.clone());
                }
            }
            result = &mut harness_compact.handle => {
                log_lane_exit(WorkerLane::HarnessCompact, result);
                if !state.command_worker_is_shutting_down() {
                    harness_compact.restart(state.clone());
                }
            }
            result = &mut harness_launch.handle => {
                log_lane_exit(WorkerLane::HarnessLaunch, result);
                if !state.command_worker_is_shutting_down() {
                    harness_launch.restart(state.clone());
                }
            }
        }
        if state.command_worker_is_shutting_down() {
            for lane in [
                &mut control,
                &mut inbox,
                &mut harness_prompt,
                &mut harness_steer,
                &mut harness_warm,
                &mut harness_compact,
                &mut harness_launch,
            ] {
                lane.wait_for_shutdown().await;
            }
            return;
        }
    }
}

struct LaneHandle {
    lane: WorkerLane,
    handle: JoinHandle<WorkerLane>,
}

impl LaneHandle {
    fn spawn(state: AppState, lane: WorkerLane) -> Self {
        Self {
            lane,
            handle: spawn_lane_task(state, lane),
        }
    }

    fn restart(&mut self, state: AppState) {
        self.handle = spawn_lane_task(state, self.lane);
    }

    async fn wait_for_shutdown(&mut self) {
        if !self.handle.is_finished() {
            let _ = (&mut self.handle).await;
        }
    }
}

impl Drop for LaneHandle {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

fn spawn_lane_task(state: AppState, lane: WorkerLane) -> JoinHandle<WorkerLane> {
    tokio::spawn(async move {
        let result = AssertUnwindSafe(run_worker_loop(state, lane))
            .catch_unwind()
            .await;
        match result {
            Ok(()) => tracing::warn!(?lane, "command worker lane exited unexpectedly"),
            Err(_) => tracing::error!(?lane, "command worker lane panicked; restarting"),
        }
        lane
    })
}

fn log_lane_exit(lane: WorkerLane, result: Result<WorkerLane, tokio::task::JoinError>) {
    match result {
        Ok(_) => tracing::warn!(?lane, "command worker lane stopped; restarting"),
        Err(err) if err.is_cancelled() => tracing::debug!(?lane, "command worker lane cancelled"),
        Err(err) => {
            tracing::error!(?lane, error = %err, "command worker lane join failed; restarting")
        }
    }
}

async fn run_worker_loop(state: AppState, lane: WorkerLane) {
    let mut retention_state = RetentionPolicyState::default();
    if matches!(lane, WorkerLane::HarnessPrompt) {
        run_harness_prompt_worker_loop(state).await;
        return;
    }

    loop {
        if matches!(lane, WorkerLane::Control) {
            if let Err(err) = maybe_reap_operational_tables(&state, &mut retention_state).await {
                tracing::warn!(error = %err, "operational retention reap failed");
            }
        }
        let idle_epoch = state.store.command_intents_epoch();
        match process_next_for_lane(&state, lane).await {
            Ok(true) => {}
            Ok(false) if state.command_worker_is_shutting_down() => {
                match lane_has_unsettled_commands(&state, lane).await {
                    Ok(false) => return,
                    Ok(true) => wait_for_command_intent_or_poll(&state, idle_epoch).await,
                    Err(err) => {
                        tracing::warn!(
                            error = %err,
                            ?lane,
                            "failed to verify command lane drain during shutdown"
                        );
                        wait_for_command_intent_or_poll(&state, idle_epoch).await;
                    }
                }
            }
            Ok(false) => wait_for_command_intent_or_poll(&state, idle_epoch).await,
            Err(err) => {
                tracing::warn!(error = %err, ?lane, "command worker tick failed");
                wait_for_command_intent_or_poll(&state, idle_epoch).await;
            }
        }
    }
}

async fn run_harness_prompt_worker_loop(state: AppState) {
    let mut actors = PromptActorSet::default();
    loop {
        actors.reap_finished();
        let idle_epoch = state.store.command_intents_epoch();
        let actor_sessions = actors.active_sessions();
        match claim_next_harness_prompt_serialized(&state, &actor_sessions).await {
            Ok(Some(row)) => match resolve_harness_prompt_target_session(&state, &row).await {
                Ok(Some(session_id)) => {
                    if let Err(row) = actors.spawn(state.clone(), session_id, row) {
                        if let Err(err) =
                            complete_claimed_row(&state, row, WorkerLane::HarnessPrompt).await
                        {
                            tracing::warn!(
                                error = %err,
                                "harness prompt worker execution task failed"
                            );
                        }
                    }
                }
                Ok(None) => {
                    if let Err(err) =
                        complete_claimed_row(&state, row, WorkerLane::HarnessPrompt).await
                    {
                        tracing::warn!(
                            error = %err,
                            "harness prompt worker execution task failed"
                        );
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        command_id = %row.command_id,
                        "failed to resolve harness prompt target session"
                    );
                    if let Err(err) =
                        complete_claimed_row(&state, row, WorkerLane::HarnessPrompt).await
                    {
                        tracing::warn!(
                            error = %err,
                            "harness prompt worker execution task failed"
                        );
                    }
                }
            },
            Ok(None) if state.command_worker_is_shutting_down() => {
                actors.wait_for_shutdown().await;
                wait_for_active_turns(&state).await;
                return;
            }
            Ok(None) => wait_for_command_intent_or_poll(&state, idle_epoch).await,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    lane = ?WorkerLane::HarnessPrompt,
                    "command worker tick failed"
                );
                wait_for_command_intent_or_poll(&state, idle_epoch).await;
            }
        }
    }
}

#[derive(Default)]
struct PromptActorSet {
    handles: HashMap<String, JoinHandle<()>>,
}

impl PromptActorSet {
    fn active_sessions(&self) -> Vec<String> {
        self.handles.keys().cloned().collect()
    }

    fn reap_finished(&mut self) {
        self.handles.retain(|session_id, handle| {
            let keep = !handle.is_finished();
            if !keep {
                tracing::debug!(
                    session_id = %session_id,
                    "harness prompt session actor finished"
                );
            }
            keep
        });
    }

    async fn wait_for_shutdown(&mut self) {
        let handles = self
            .handles
            .drain()
            .map(|(_, handle)| handle)
            .collect::<Vec<_>>();
        for handle in handles {
            let _ = handle.await;
        }
    }

    fn spawn(
        &mut self,
        state: AppState,
        session_id: String,
        row: CommandIntentRow,
    ) -> Result<(), CommandIntentRow> {
        if self.handles.contains_key(&session_id) {
            tracing::warn!(
                session_id = %session_id,
                command_id = %row.command_id,
                "harness prompt session actor already active for claimed row"
            );
            return Err(row);
        }
        let actor_session_id = session_id.clone();
        let handle = tokio::spawn(async move {
            run_harness_prompt_session_actor(state, actor_session_id, row).await;
        });
        self.handles.insert(session_id, handle);
        Ok(())
    }
}

impl Drop for PromptActorSet {
    fn drop(&mut self) {
        for handle in self.handles.values() {
            handle.abort();
        }
    }
}

async fn run_harness_prompt_session_actor(
    state: AppState,
    session_id: String,
    first_row: CommandIntentRow,
) {
    let mut next_row = Some(first_row);
    loop {
        let row = match next_row.take() {
            Some(row) => row,
            None => {
                if state
                    .agent
                    .active_turn_sessions()
                    .iter()
                    .any(|active| active.0 == session_id)
                {
                    return;
                }
                match claim_next_harness_prompt_for_session(&state, &session_id).await {
                    Ok(Some(row)) => row,
                    Ok(None) => return,
                    Err(err) => {
                        tracing::warn!(
                            error = %err,
                            session_id = %session_id,
                            "harness prompt session actor claim failed"
                        );
                        return;
                    }
                }
            }
        };

        if let Err(err) = complete_claimed_row(&state, row, WorkerLane::HarnessPrompt).await {
            tracing::warn!(
                error = %err,
                session_id = %session_id,
                "harness prompt session actor execution failed"
            );
        }
    }
}

async fn resolve_harness_prompt_target_session(
    state: &AppState,
    row: &CommandIntentRow,
) -> Result<Option<String>, NexusError> {
    let Ok(prompt) = serde_json::from_str::<PromptRequest>(&row.request_json) else {
        return Ok(None);
    };
    let sessions = Sessions::new(&state.store);
    if let Some(agent_id) = prompt.agent_id.as_ref() {
        if let Some(session) = sessions
            .active_runtime_session_for_agent(&agent_id.0)
            .await?
        {
            return Ok(Some(session.session_id.0));
        }
        return Ok(sessions
            .find_by_agent_id(&agent_id.0)
            .await?
            .map(|session| session.session_id.0));
    }
    let agents = Agents::new(&state.store);
    let parsed = AgentRef::parse(&prompt.name);
    let resolved = match agents.resolve_ref("", &parsed, true).await {
        Err(NexusError::NotFound(_)) if matches!(parsed, AgentRef::Id(_)) => {
            agents
                .resolve_ref("", &AgentRef::Name(prompt.name.clone()), true)
                .await
        }
        result => result,
    };
    let session = match resolved {
        Ok(agent) => match sessions
            .active_runtime_session_for_agent(&agent.agent_id)
            .await?
        {
            Some(session) => Some(session),
            None => sessions.find_by_agent_id(&agent.agent_id).await?,
        },
        Err(NexusError::NotFound(_)) => {
            sessions
                .find_unique_by_name_any_project(&prompt.name)
                .await?
        }
        Err(error) => return Err(error),
    };
    Ok(session.map(|session| session.session_id.0))
}

async fn wait_for_command_intent_or_poll(state: &AppState, idle_epoch: u64) {
    tokio::select! {
        _ = state.store.wait_for_command_intent_after(idle_epoch) => {}
        _ = tokio::time::sleep(POLL_INTERVAL) => {}
    }
}

async fn process_next_for_lane(state: &AppState, lane: WorkerLane) -> Result<bool, NexusError> {
    let Some(row) = claim_next_for_lane(state, lane).await? else {
        return Ok(false);
    };
    complete_claimed_row(state, row, lane).await?;
    Ok(true)
}

async fn claim_next_for_lane(
    state: &AppState,
    lane: WorkerLane,
) -> Result<Option<CommandIntentRow>, NexusError> {
    // Every claim is an UPDATE ... RETURNING statement. The worker lanes execute harness work
    // independently, but claims still serialize through the daemon's one embedded writer. Hold
    // the store's durable-write gate only while selecting/claiming a row; it is released
    // before any command execution, so long-running prompts, launches, and inbox waits remain
    // isolated by lane.
    let _claim_guard = state.store.write_lock().lock_owned().await;
    let repo = CommandIntents::new(&state.store);
    let row = match lane {
        WorkerLane::Any => repo.claim_next(now(), LEASE_MS).await?,
        WorkerLane::Control => {
            repo.claim_next_excluding_kind_set(
                now(),
                LEASE_MS,
                &[
                    command_kinds::inbox::CONSUME,
                    command_kinds::inbox::SUBSCRIPTION_NEXT,
                    command_kinds::harness::PROMPT,
                    command_kinds::harness::STEER,
                    command_kinds::harness::WARM,
                    command_kinds::harness::COMPACT,
                    command_kinds::harness::LAUNCH,
                ],
            )
            .await?
        }
        WorkerLane::InboxConsume => {
            repo.expire_stale_inbox_consumes(now(), state.heartbeat_ttl_ms())
                .await?;
            repo.claim_next_matching_kind_set(
                now(),
                LEASE_MS,
                &[
                    command_kinds::inbox::CONSUME,
                    command_kinds::inbox::SUBSCRIPTION_NEXT,
                ],
            )
            .await?
        }
        WorkerLane::HarnessPrompt => claim_next_harness_prompt(state, &[]).await?,
        WorkerLane::HarnessSteer => {
            repo.claim_next_ready_harness_command(
                now(),
                HARNESS_REDIRECT_LEASE_MS,
                command_kinds::harness::STEER,
            )
            .await?
        }
        WorkerLane::HarnessWarm => {
            repo.claim_next_ready_harness_command(now(), LEASE_MS, command_kinds::harness::WARM)
                .await?
        }
        WorkerLane::HarnessCompact => {
            repo.claim_next_ready_harness_command(now(), LEASE_MS, command_kinds::harness::COMPACT)
                .await?
        }
        WorkerLane::HarnessLaunch => {
            repo.claim_next_kind(now(), LEASE_MS, command_kinds::harness::LAUNCH)
                .await?
        }
    };
    Ok(row)
}

async fn claim_next_harness_prompt(
    state: &AppState,
    actor_sessions: &[String],
) -> Result<Option<CommandIntentRow>, NexusError> {
    if state.command_worker_is_shutting_down() {
        return Ok(None);
    }
    let mut busy_sessions = state
        .agent
        .active_turn_sessions()
        .into_iter()
        .map(|session| session.0)
        .chain(actor_sessions.iter().cloned())
        .collect::<Vec<_>>();
    busy_sessions.sort();
    busy_sessions.dedup();
    CommandIntents::new(&state.store)
        .claim_next_ready_harness_prompt(now(), HARNESS_PROMPT_LEASE_MS, &busy_sessions)
        .await
}

async fn claim_next_harness_prompt_serialized(
    state: &AppState,
    actor_sessions: &[String],
) -> Result<Option<CommandIntentRow>, NexusError> {
    let _claim_guard = state.store.write_lock().lock_owned().await;
    claim_next_harness_prompt(state, actor_sessions).await
}

async fn claim_next_harness_prompt_for_session(
    state: &AppState,
    session_id: &str,
) -> Result<Option<CommandIntentRow>, NexusError> {
    let _claim_guard = state.store.write_lock().lock_owned().await;
    if state.command_worker_is_shutting_down() {
        return Ok(None);
    }
    CommandIntents::new(&state.store)
        .claim_next_ready_harness_prompt_for_session(now(), HARNESS_PROMPT_LEASE_MS, session_id)
        .await
}

async fn complete_claimed_row(
    state: &AppState,
    row: CommandIntentRow,
    lane: WorkerLane,
) -> Result<(), NexusError> {
    let repo = CommandIntents::new(&state.store);
    let command_id = row.command_id.clone();
    let claimed_at = row.claimed_at;
    if let Some(claimed_at) = claimed_at {
        if !repo
            .mark_started_for_claim(&command_id, claimed_at, now())
            .await?
        {
            tracing::warn!(
                command_id = %command_id,
                claimed_at,
                "skipped command execution because its claim was no longer current"
            );
            return Ok(());
        }
    }
    match execute_for_lane(state, row, lane).await {
        Ok(result) => {
            if let Some(claimed_at) = claimed_at {
                if !repo
                    .mark_done_for_claim(&command_id, claimed_at, &json_string(&result)?, now())
                    .await?
                {
                    tracing::warn!(
                        command_id = %command_id,
                        claimed_at,
                        "skipped stale command completion; claim was no longer current"
                    );
                }
            } else {
                repo.mark_done(&command_id, &json_string(&result)?, now())
                    .await?;
            }
        }
        Err(error) => {
            if matches!(lane, WorkerLane::HarnessPrompt)
                && error.code == codes::INTERNAL_ERROR
                && error.message == PROMPT_DEFERRED_FOR_SHUTDOWN
            {
                if let Some(claimed_at) = claimed_at {
                    if !repo
                        .release_claim_for_shutdown_retry(&command_id, claimed_at)
                        .await?
                    {
                        tracing::warn!(
                            command_id = %command_id,
                            claimed_at,
                            "skipped shutdown retry release because its claim was no longer current"
                        );
                    }
                    return Ok(());
                }
            }
            if let Some(claimed_at) = claimed_at {
                if !repo
                    .mark_error_for_claim(&command_id, claimed_at, &json_string(&error)?, now())
                    .await?
                {
                    tracing::warn!(
                        command_id = %command_id,
                        claimed_at,
                        "skipped stale command error; claim was no longer current"
                    );
                }
            } else {
                repo.mark_error(&command_id, &json_string(&error)?, now())
                    .await?;
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum WorkerLane {
    Any,
    Control,
    InboxConsume,
    HarnessPrompt,
    HarnessSteer,
    HarnessWarm,
    HarnessCompact,
    HarnessLaunch,
}

impl WorkerLane {
    fn owns_kind(self, kind: &str) -> bool {
        match self {
            Self::Any => true,
            Self::InboxConsume => matches!(
                kind,
                command_kinds::inbox::CONSUME | command_kinds::inbox::SUBSCRIPTION_NEXT
            ),
            Self::HarnessPrompt => kind == command_kinds::harness::PROMPT,
            Self::HarnessSteer => kind == command_kinds::harness::STEER,
            Self::HarnessWarm => kind == command_kinds::harness::WARM,
            Self::HarnessCompact => kind == command_kinds::harness::COMPACT,
            Self::HarnessLaunch => kind == command_kinds::harness::LAUNCH,
            Self::Control => {
                !Self::InboxConsume.owns_kind(kind)
                    && !Self::HarnessPrompt.owns_kind(kind)
                    && !Self::HarnessSteer.owns_kind(kind)
                    && !Self::HarnessWarm.owns_kind(kind)
                    && !Self::HarnessCompact.owns_kind(kind)
                    && !Self::HarnessLaunch.owns_kind(kind)
            }
        }
    }
}

async fn lane_has_unsettled_commands(
    state: &AppState,
    lane: WorkerLane,
) -> Result<bool, NexusError> {
    Ok(CommandIntents::new(&state.store)
        .lane_depths()
        .await?
        .into_iter()
        .any(|depth| lane.owns_kind(&depth.kind) && depth.pending + depth.claimed > 0))
}

async fn execute(state: &AppState, row: CommandIntentRow) -> Result<Value, ContractError> {
    // Public notification trust terminates at the durable command worker. The ordinary `notify`
    // dispatch method deliberately remains unverified; only this command kind accepts the signed
    // raw-body envelope, independently re-verifies it, then enters verified ingest.
    if row.kind == command_kinds::notification::NOTIFY {
        return execute_verified_notification(state, &row).await;
    }
    let Some(method) = command_method(row.kind.as_str()) else {
        return Err(ContractError {
            code: codes::METHOD_NOT_FOUND,
            message: format!("unknown command intent kind: {}", row.kind),
        });
    };
    let mut params =
        serde_json::from_str::<Value>(&row.request_json).map_err(|e| ContractError {
            code: codes::INVALID_PARAMS,
            message: format!("invalid {} request_json: {e}", row.kind),
        })?;
    promote_message_post_idempotency_key(&row, &mut params);
    if row.kind == command_kinds::inbox::CONSUME
        || row.kind == command_kinds::inbox::SUBSCRIPTION_NEXT
    {
        clamp_inbox_consume_timeout(&mut params);
    }
    let caller = if command_requires_caller(row.kind.as_str()) {
        Some(resolve_command_caller(state, &row).await?)
    } else {
        None
    };
    if row.kind == command_kinds::notification::SEND {
        let caller = caller.as_ref().ok_or_else(|| ContractError {
            code: codes::UNAUTHORIZED,
            message: "notification.send requires a verified caller".into(),
        })?;
        let request = serde_json::from_value::<nexus_contracts::NotifySendRequest>(params)
            .map_err(|error| ContractError {
                code: codes::INVALID_PARAMS,
                message: format!("invalid notification.send request_json: {error}"),
            })?;
        // Resolve the immutable recipient, authorize policy, and execute `before_send` before the
        // runtime registry is touched. The returned preparation freezes that exact delivery set;
        // commit cannot re-resolve a renamed alias or execute the hook a second time.
        let prepared = state.bus.prepare_notify(caller, request).await?;
        if let Some(agent_id) = state.bus.prepared_hold_agent(caller, &prepared).await? {
            state.hold_cold_dm_target(None, Some(&agent_id)).await;
        }
        let response = state.bus.commit_prepared(caller, prepared).await?;
        state
            .wake_notification_recipients(&response.message_id)
            .await;
        return serde_json::to_value(response).map_err(|error| ContractError {
            code: codes::INTERNAL_ERROR,
            message: format!("failed to serialize notification.send response: {error}"),
        });
    }
    if row.kind == command_kinds::source::PUSH {
        let caller = caller.as_ref().ok_or_else(|| ContractError {
            code: codes::UNAUTHORIZED,
            message: "source.push command requires a verified caller".into(),
        })?;
        let request =
            serde_json::from_value::<nexus_contracts::PushRequest>(params).map_err(|e| {
                ContractError {
                    code: codes::INVALID_PARAMS,
                    message: format!("invalid source.push request_json: {e}"),
                }
            })?;
        let key = row
            .idempotency_key
            .clone()
            .filter(|key| !key.trim().is_empty())
            .or_else(|| Some(format!("source-command:{}", row.command_id)));
        let response = state
            .push_as_source_with_idempotency(caller, request, key)
            .await?;
        return serde_json::to_value(response).map_err(|e| ContractError {
            code: codes::INTERNAL_ERROR,
            message: format!("failed to serialize source.push response: {e}"),
        });
    }
    let response = routing::route_request(
        state,
        caller,
        Request {
            jsonrpc: JSONRPC_VERSION.to_string(),
            id: None,
            method: method.to_string(),
            params: Some(params),
        },
    )
    .await;
    if let Some(error) = response.error {
        return Err(ContractError {
            code: error.code,
            message: error.message,
        });
    }
    Ok(response.result.unwrap_or(Value::Null))
}

async fn execute_verified_notification(
    state: &AppState,
    row: &CommandIntentRow,
) -> Result<Value, ContractError> {
    if state.hmac_secret.trim().is_empty() {
        return Err(ContractError {
            code: codes::INTERNAL_ERROR,
            message: "notification HMAC secret is not configured".into(),
        });
    }
    let envelope: NotifyCommandRequest =
        serde_json::from_str(&row.request_json).map_err(|error| ContractError {
            code: codes::INVALID_PARAMS,
            message: format!("invalid notification command envelope: {error}"),
        })?;
    let timestamp_ms = envelope
        .timestamp
        .parse::<i64>()
        .ok()
        .filter(|timestamp| *timestamp >= 1_000_000_000_000)
        .ok_or_else(|| ContractError {
            code: codes::UNAUTHORIZED,
            message: "notification timestamp must be Unix milliseconds".into(),
        })?;
    if timestamp_ms.abs_diff(row.created_at) > 300_000 {
        return Err(ContractError {
            code: codes::UNAUTHORIZED,
            message: "notification timestamp is outside the command freshness window".into(),
        });
    }
    if !nexus_notify::verify_timestamped_hmac(
        &state.hmac_secret,
        &envelope.timestamp,
        envelope.raw_body.as_bytes(),
        &envelope.signature,
    ) {
        return Err(ContractError {
            code: codes::UNAUTHORIZED,
            message: "invalid notification signature".into(),
        });
    }
    let request: NotifyRequest =
        serde_json::from_str(&envelope.raw_body).map_err(|error| ContractError {
            code: codes::INVALID_PARAMS,
            message: format!("signed notification body is invalid: {error}"),
        })?;
    let idempotency_root = row
        .idempotency_key
        .as_deref()
        .map(str::trim)
        .filter(|key| !key.is_empty())
        .ok_or_else(|| ContractError {
            code: codes::INVALID_PARAMS,
            message: "verified notification command requires an idempotency key".into(),
        })?;
    let route_request = request.clone();
    let response = state
        .notify
        .ingest_verified(request, idempotency_root.to_string())
        .await?;
    drive_verified_notification_routes(
        state,
        &route_request,
        idempotency_root,
        &response.routed_to,
    )
    .await?;
    serde_json::to_value(response).map_err(|error| ContractError {
        code: codes::INTERNAL_ERROR,
        message: format!("serialize notification result: {error}"),
    })
}

/// Wake every verified notification route after its bus commit, or persist an exact terminal
/// delivery error when the target cannot be revived. The notification effect key identifies the
/// one canonical routed message produced by this command reclaim; no recipient-wide mutation is
/// used, so unrelated pending mail remains untouched.
async fn drive_verified_notification_routes(
    state: &AppState,
    request: &NotifyRequest,
    idempotency_root: &str,
    routed_to: &[String],
) -> Result<(), ContractError> {
    let sessions = Sessions::new(&state.store);
    let mut targets = Vec::with_capacity(routed_to.len());
    for recipient in routed_to {
        let Some(target) = sessions
            .find_unique_by_name_any_project(recipient)
            .await
            .map_err(|error| error.to_contract_error())?
        else {
            continue;
        };
        targets.push((recipient, target));
    }
    for (recipient, target) in targets {
        if state
            .inbox_subscription_owns_delivery(&target.session_id)
            .await
        {
            continue;
        }
        let revival = match target.agent_id.as_deref() {
            Some(agent_id) => state.ensure_alive_agent(agent_id).await,
            None => state.ensure_alive(recipient, &target.project).await,
        };
        let Err(revival_error) = revival else {
            continue;
        };

        let (effect, effect_target) = match request.topic.as_deref() {
            Some(topic) => ("topic", topic),
            None => ("dm", recipient.as_str()),
        };
        let effect_key =
            nexus_notify::notification_effect_key(Some(idempotency_root), effect, effect_target)
                .ok_or_else(|| ContractError {
                    code: codes::INTERNAL_ERROR,
                    message: "verified notification effect key was empty after validation".into(),
                })?;
        let message_id = verified_notification_delivery_message(
            state,
            &effect_key,
            &target.session_id,
            target.agent_id.as_deref(),
        )
        .await?
        .ok_or_else(|| ContractError {
            code: codes::INTERNAL_ERROR,
            message: format!(
                "verified notification delivery for {recipient} was committed without a canonical row"
            ),
        })?;
        let is_dead = match target.agent_id.as_deref() {
            Some(agent_id) => {
                Agents::new(&state.store)
                    .lifecycle_for_id(agent_id)
                    .await
                    .ok()
                    .and_then(|(lifecycle, _)| lifecycle)
                    .as_deref()
                    == Some("dead")
            }
            None => false,
        };
        let code = if is_dead {
            nexus_store::repos::inbox::TARGET_DEAD_ERROR_CODE
        } else {
            nexus_store::repos::inbox::TARGET_UNREACHABLE_ERROR_CODE
        };
        Inbox::new(&state.store)
            .mark_delivery_error(
                &message_id,
                &target.session_id,
                code,
                &format!(
                    "verified notification target could not be revived: {}",
                    revival_error.message
                ),
                Some("{\"source\":\"verified_notification_route\"}"),
            )
            .await
            .map_err(|error| error.to_contract_error())?;
    }
    Ok(())
}

async fn verified_notification_delivery_message(
    state: &AppState,
    effect_key: &str,
    recipient_session: &SessionId,
    recipient_agent_id: Option<&str>,
) -> Result<Option<MessageId>, ContractError> {
    let mut rows = state
        .store
        .conn
        .query(
            "SELECT m.message_id FROM messages m \
             JOIN in_flight f ON f.message_id = m.message_id \
             WHERE m.sender_session_id = 's_notify' AND m.idempotency_key = ?1 \
               AND (f.recipient_session = ?2 OR (?3 IS NOT NULL AND f.recipient_agent_id = ?3)) \
             ORDER BY m.created_at DESC LIMIT 1",
            libsql::params![
                effect_key,
                recipient_session.0.clone(),
                recipient_agent_id.map(str::to_string)
            ],
        )
        .await
        .map_err(|error| NexusError::Store(error.to_string()).to_contract_error())?;
    rows.next()
        .await
        .map_err(|error| NexusError::Store(error.to_string()).to_contract_error())?
        .map(|row| {
            row.get::<String>(0)
                .map(MessageId)
                .map_err(|error| NexusError::Store(error.to_string()).to_contract_error())
        })
        .transpose()
}

/// Carry a durable Message Post command key through to the canonical bus transaction.
///
/// A daemon can die after the bus commits but before the command receipt is marked done. The
/// expired command is then reclaimed and executed again. Store-backed producers are allowed to put
/// the retry key on the command envelope, so fill a missing nested `SendRequest.idempotencyKey`
/// before dispatch; an explicit request key remains authoritative.
fn promote_message_post_idempotency_key(row: &CommandIntentRow, params: &mut Value) {
    if row.kind != command_kinds::message_post::SEND {
        return;
    }
    let Some(key) = row
        .idempotency_key
        .as_deref()
        .filter(|key| !key.trim().is_empty())
    else {
        return;
    };
    let Some(object) = params.as_object_mut() else {
        return;
    };
    if object
        .get("idempotencyKey")
        .is_none_or(serde_json::Value::is_null)
    {
        object.insert("idempotencyKey".to_string(), Value::String(key.to_string()));
    }
}

async fn execute_for_lane(
    state: &AppState,
    row: CommandIntentRow,
    lane: WorkerLane,
) -> Result<Value, ContractError> {
    let WorkerLane::HarnessPrompt = lane else {
        return execute(state, row).await;
    };
    let command_id = row.command_id.clone();
    // The timeout REPORTS; it must never CANCEL. Dropping an execute future mid-store statement
    // can interrupt unrelated work on the daemon-owned connection. Run the execution on its own
    // task and detach on elapse: the prompt may still deliver late, and the 60s lease
    // (HARNESS_PROMPT_LEASE_MS) was already sized for executions that outlive the timeout.
    let owned_state = state.clone();
    let exec_task = tokio::spawn(async move { execute(&owned_state, row).await });
    match tokio::time::timeout(HARNESS_PROMPT_EXECUTION_TIMEOUT, exec_task).await {
        Ok(Ok(result)) => result,
        Ok(Err(join_error)) => Err(ContractError {
            code: codes::INTERNAL_ERROR,
            message: format!(
                "harness.prompt command {command_id} execution panicked: {join_error}"
            ),
        }),
        Err(_elapsed) => {
            tracing::warn!(
                command_id = %command_id,
                timeout_ms = HARNESS_PROMPT_EXECUTION_TIMEOUT.as_millis() as u64,
                "harness.prompt execution exceeded its report deadline; leaving it running \
                 detached (never cancel mid-statement) and marking the intent timed out"
            );
            Err(ContractError {
                code: codes::INTERNAL_ERROR,
                message: format!(
                    "harness.prompt command {command_id} timed out after {}ms",
                    HARNESS_PROMPT_EXECUTION_TIMEOUT.as_millis()
                ),
            })
        }
    }
}

fn clamp_inbox_consume_timeout(params: &mut Value) {
    let Some(obj) = params.as_object_mut() else {
        return;
    };
    let Some(current) = obj
        .get("timeoutMs")
        .and_then(|v| v.as_u64())
        .or_else(|| obj.get("timeout_ms").and_then(|v| v.as_u64()))
    else {
        return;
    };
    if current > u64::from(INBOX_CONSUME_SLICE_MS) {
        obj.insert("timeoutMs".to_string(), Value::from(INBOX_CONSUME_SLICE_MS));
        obj.remove("timeout_ms");
    }
}

fn command_method(kind: &str) -> Option<&'static str> {
    match kind {
        command_kinds::message_post::SEND => Some("send"),
        command_kinds::metadata::SET => Some("metadata.set"),
        command_kinds::notification::NOTIFY => Some("notify"),
        command_kinds::notification::SEND => Some("notification.send"),
        command_kinds::identity::REGISTER => Some("register"),
        command_kinds::identity::ATTACH => Some("identity.attach"),
        command_kinds::identity::RENAME => Some("rename"),
        command_kinds::inbox::CONSUME => Some("consume"),
        command_kinds::inbox::SUBSCRIBE => Some("inbox.subscribe"),
        command_kinds::inbox::SUBSCRIPTION_NEXT => Some("inbox.next"),
        command_kinds::inbox::SUBSCRIPTION_ACK => Some("inbox.subscriptionAck"),
        command_kinds::inbox::UNSUBSCRIBE => Some("inbox.unsubscribe"),
        command_kinds::inbox::ACK => Some("ack"),
        command_kinds::inbox::ACK_THREADS => Some("ackThreads"),
        command_kinds::presence::STATUS => Some("status"),
        command_kinds::presence::HEARTBEAT => Some("heartbeat"),
        command_kinds::topic::SUBSCRIBE => Some("subscribe"),
        command_kinds::topic::UNSUBSCRIBE => Some("unsubscribe"),
        command_kinds::thread::CREATE => Some("thread.new"),
        command_kinds::thread::JOIN => Some("thread.join"),
        command_kinds::thread::LEAVE => Some("thread.leave"),
        command_kinds::thread::ARCHIVE => Some("thread.archive"),
        command_kinds::thread::DELETE => Some("thread.delete"),
        command_kinds::thread::RENAME => Some("thread.rename"),
        command_kinds::thread::ADD_MEMBER => Some("thread.addMember"),
        command_kinds::thread::REMOVE_MEMBER => Some("thread.removeMember"),
        command_kinds::harness::LAUNCH => Some("launch"),
        command_kinds::harness::PROMPT => Some("prompt"),
        command_kinds::harness::STEER => Some("steer"),
        command_kinds::harness::INTERRUPT => Some("interrupt"),
        command_kinds::harness::COMPACT => Some("compact"),
        command_kinds::harness::WARM => Some("warm"),
        command_kinds::admin::SPAWN => Some("admin.spawn"),
        command_kinds::admin::REMOVE => Some("admin.remove"),
        command_kinds::admin::EVICT => Some("admin.evict"),
        command_kinds::admin::DELETE => Some("admin.delete"),
        command_kinds::admin::RENAME => Some("admin.rename"),
        command_kinds::admin::ASSIGN => Some("admin.assign"),
        command_kinds::admin::ASSIGN_ROLE => Some("admin.assignRole"),
        command_kinds::admin::ASSIGN_PROJECT => Some("admin.assignProject"),
        command_kinds::admin::GRANT_TIER => Some("admin.grantTier"),
        command_kinds::admin::GROUP_ASSIGN => Some("admin.group.assign"),
        command_kinds::admin::CHANNEL => Some("admin.channel"),
        command_kinds::admin::ROUTE => Some("admin.route"),
        command_kinds::admin::MONITOR => Some("admin.monitor"),
        command_kinds::admin::DLQ_LIST => Some("admin.dlq.list"),
        command_kinds::admin::DLQ_REQUEUE => Some("admin.dlq.requeue"),
        command_kinds::admin::DLQ_PURGE => Some("admin.dlq.purge"),
        command_kinds::agent::CREATE => Some("agent.create"),
        command_kinds::agent::GRANT_ACCESS => Some("agent.grantAccess"),
        command_kinds::agent::REVOKE_ACCESS => Some("agent.revokeAccess"),
        command_kinds::agent::TRANSFER_OWNER => Some("agent.transferOwner"),
        command_kinds::agent::credential::CREATE => Some("agent.credential.create"),
        command_kinds::agent::credential::REVOKE => Some("agent.credential.revoke"),
        command_kinds::source::REGISTER => Some("source.register"),
        command_kinds::source::ENABLE => Some("source.enable"),
        command_kinds::source::DISABLE => Some("source.disable"),
        command_kinds::source::ROTATE => Some("source.rotate"),
        command_kinds::source::REMOVE => Some("source.remove"),
        command_kinds::source::PUSH => Some("push"),
        _ => None,
    }
}

fn command_requires_caller(kind: &str) -> bool {
    !matches!(
        kind,
        command_kinds::identity::REGISTER | command_kinds::notification::NOTIFY
    )
}

async fn resolve_command_caller(
    state: &AppState,
    row: &CommandIntentRow,
) -> Result<Caller, ContractError> {
    if is_local_operator(row) {
        return Ok(Caller {
            agent_id: None,
            session: nexus_contracts::SessionId(
                row.caller_session_id
                    .clone()
                    .unwrap_or_else(|| LOCAL_OPERATOR_SESSION_ID.to_string()),
            ),
            name: row.caller_name.clone(),
            project: row.project.clone(),
            tier: nexus_contracts::Tier::Admin,
        });
    }

    if is_verified_source_push(row) {
        return Ok(Caller {
            agent_id: None,
            session: SessionId(format!("source:{}", row.caller_name)),
            name: row.caller_name.clone(),
            project: row.project.clone(),
            tier: Tier::Agent,
        });
    }

    // A human command accepted under the previous daemon boot owns the exact caller snapshot
    // stamped at ingress. A reconnect may legitimately bind the same client key to a fresh
    // session before the new worker drains that command; the new registration must not replace
    // or invalidate the already-accepted authority.
    if let Some(caller) = resolve_restart_validated_human_caller(state, row).await? {
        return Ok(caller);
    }

    let sessions = Sessions::new(&state.store);
    let Some(client_key) = row.caller_client_key.as_deref() else {
        return Err(unauthorized_command_caller(
            "command intent caller requires a verified client key",
        ));
    };

    let Some(session) = sessions
        .find_by_client_key_any_project(client_key)
        .await
        .map_err(|e| e.to_contract_error())?
    else {
        return Err(unauthorized_command_caller(
            "command intent caller client key is not registered",
        ));
    };
    let caller = if session.kind != "agent" {
        // The registered session kind is authoritative. Ignore pre-fix `agent_id` residue (and
        // mutable caller labels) so a durable human/app credential cannot inherit an agent alias.
        Caller {
            agent_id: None,
            session: session.session_id.clone(),
            name: session.display_name(),
            project: session.project.clone(),
            tier: tier_from_session(&session),
        }
    } else if let Some(agent_id) = session.agent_id.as_deref() {
        let agent = Agents::new(&state.store)
            .find_by_id(agent_id)
            .await
            .map_err(|error| error.to_contract_error())?
            .ok_or_else(|| {
                unauthorized_command_caller(
                    "command intent caller registered agent id is not a durable identity",
                )
            })?;
        Caller {
            agent_id: Some(AgentId(agent.agent_id)),
            session: session.session_id.clone(),
            name: agent.name.unwrap_or_else(|| session.display_name()),
            project: session.project.clone(),
            tier: tier_from_session(&session),
        }
    } else if session.kind == "agent" {
        let session_name = session.name.as_deref().ok_or_else(|| {
            unauthorized_command_caller(
                "command intent caller agent session has no durable agent identity",
            )
        })?;
        state
            .identity
            .resolve(&session.project, session_name)
            .await?
    } else {
        unreachable!("agent-session branch handles missing durable ids above")
    };
    validate_command_caller_row(row, &session, &caller)?;
    // Any verified registered command proves recent activity for presence purposes. Route the
    // refresh through the daemon presence writer so an offline→online transition also publishes
    // its ordered fleet-status fact; explicitly paused peers remain paused/offline.
    state
        .presence
        .restore_online_on_activity(&session.session_id)
        .await
        .map_err(|e| e.to_contract_error())?;

    Ok(caller)
}

async fn resolve_restart_validated_human_caller(
    state: &AppState,
    row: &CommandIntentRow,
) -> Result<Option<Caller>, ContractError> {
    let Some(validated_boot_epoch) = row
        .caller_validated_boot_epoch
        .as_deref()
        .filter(|epoch| !epoch.is_empty())
    else {
        return Ok(None);
    };
    let current_boot_epoch = DaemonState::new(&state.store)
        .boot_epoch()
        .await
        .map_err(|error| error.to_contract_error())?;
    if current_boot_epoch
        .as_deref()
        .is_none_or(|epoch| epoch.is_empty() || epoch == validated_boot_epoch)
    {
        return Ok(None);
    }
    if row.caller_kind.as_deref() != Some("human")
        || row.caller_agent_id.is_some()
        || row.caller_client_key.as_deref().is_none_or(str::is_empty)
        || row.caller_name.is_empty()
        || row.project.is_empty()
    {
        return Ok(None);
    }
    let Some(session_id) = row
        .caller_session_id
        .as_deref()
        .filter(|session_id| !session_id.is_empty())
    else {
        return Ok(None);
    };
    if row.caller_runtime_id.as_deref() != Some(session_id) {
        return Ok(None);
    }
    let tier = match row.caller_tier.as_deref() {
        Some("admin") => Tier::Admin,
        Some("agent") => Tier::Agent,
        _ => return Ok(None),
    };
    Ok(Some(Caller {
        agent_id: None,
        session: SessionId(session_id.to_string()),
        name: row.caller_name.clone(),
        project: row.project.clone(),
        tier,
    }))
}

fn is_local_operator(row: &CommandIntentRow) -> bool {
    row.caller_session_id.as_deref() == Some(LOCAL_OPERATOR_SESSION_ID)
        && matches!(
            row.caller_runtime_id.as_deref(),
            None | Some(LOCAL_OPERATOR_SESSION_ID)
        )
        && row.caller_client_key.is_none()
        && row.caller_kind.as_deref() == Some("human")
        && row.caller_tier.as_deref() == Some("admin")
}

fn is_verified_source_push(row: &CommandIntentRow) -> bool {
    row.kind == command_kinds::source::PUSH
        && row.caller_kind.as_deref() == Some("notification")
        && row.caller_tier.as_deref() == Some("agent")
        && row.caller_client_key.is_none()
}

fn unauthorized_command_caller(message: impl Into<String>) -> ContractError {
    ContractError {
        code: codes::UNAUTHORIZED,
        message: message.into(),
    }
}

fn tier_from_session(session: &SessionRow) -> Tier {
    match session.tier.as_str() {
        "admin" => Tier::Admin,
        _ => Tier::Agent,
    }
}

fn validate_command_caller_row(
    row: &CommandIntentRow,
    session: &SessionRow,
    caller: &Caller,
) -> Result<(), ContractError> {
    if let Some(caller_session_id) = row.caller_session_id.as_deref() {
        if !matches_session_id(caller_session_id, session, caller) {
            return Err(unauthorized_command_caller(
                "command intent caller session does not match registered client key",
            ));
        }
    }
    if let Some(caller_runtime_id) = row.caller_runtime_id.as_deref() {
        if !matches_session_id(caller_runtime_id, session, caller) {
            return Err(unauthorized_command_caller(
                "command intent caller runtime does not match registered client key",
            ));
        }
    }
    if session.kind == "agent" {
        if let Some(caller_agent_id) = row.caller_agent_id.as_deref() {
            if caller.agent_id.as_ref().map(|id| id.0.as_str()) != Some(caller_agent_id) {
                return Err(unauthorized_command_caller(
                    "command intent caller agent does not match registered client key",
                ));
            }
        }
    }
    if let Some(caller_kind) = row.caller_kind.as_deref() {
        if session.kind != caller_kind {
            return Err(unauthorized_command_caller(
                "command intent caller kind does not match registered client key",
            ));
        }
    }
    if let Some(caller_tier) = row.caller_tier.as_deref() {
        if session.tier != caller_tier {
            return Err(unauthorized_command_caller(
                "command intent caller tier does not match registered client key",
            ));
        }
    }
    Ok(())
}

fn matches_session_id(value: &str, session: &SessionRow, caller: &Caller) -> bool {
    value == session.session_id.0
        || value == caller.session.0
        || session.harness_session_id.as_deref() == Some(value)
}

fn json_string<T: Serialize>(value: &T) -> Result<String, NexusError> {
    serde_json::to_string(value).map_err(|e| NexusError::Internal(e.to_string()))
}

#[cfg(test)]
#[path = "../../tests/unit/command_worker.rs"]
mod tests;
