//! The per-agent event loop (spec §2.3 / §2.4). One long-lived task per `kind=agent` session:
//! park on the [`Bell`], on wake consult the [`WakePolicy`], and while not held drain the whole
//! pending queue as one batch → build the session-visible bus input event → inject it as a single
//! turn (via the [`AgentTurnExecutionPort`]) with that event emitted at the transport accepted
//! boundary → while awaiting completion, admit newly rung batches through non-destructive native
//! steer when the adapter supports it → on **observed** normal-turn completion mark its rows
//! delivered → emit `message.delivered` → drain again to absorb arrivals that could not be steered
//! (the turn-end window, §2.4) → re-park. If the bounded completion await elapses (the completion
//! signal was lost) the original batch is **dead-lettered** to the non-delivered `error` state,
//! never falsely marked delivered.
//!
//! Invariants:
//! - **Single consumer per session.** Exactly one loop task per session; it runs one turn at a
//!   time, so per-agent serialization is structural.
//! - **One in-flight turn.** Native steer appends a mid-turn batch to that same turn; it never starts
//!   a concurrent turn. Transports without non-destructive steer keep turn-end coalescing.
//! - **Crash-safety.** A missed bell is re-driven because the row is still `pending`; the loop
//!   re-rings its own bell on [`spawn`](EventLoop::spawn) to drive anything already queued at
//!   attach time.

use std::sync::Arc;

use nexus_common::render_injected_turn_for;
use nexus_contracts::ids::SessionId;
use nexus_contracts::{
    AgentTurnExecutionPort, AgentUpdateKind, ContractError, DeliveryTiming, EventSink, InjectError,
    NexusBatch, WsEvent,
};
use nexus_store::repos::inbox::{
    Inbox, COMPLETION_TIMEOUT_ERROR_CODE, CONTRACT_ERROR_CODE, OPERATOR_ACTION_ERROR_CODE,
    PROVIDER_ERROR_CODE, PROVIDER_LIMIT_ERROR_CODE, TARGET_UNREACHABLE_ERROR_CODE,
};
use nexus_store::Store;
use tokio::task::JoinHandle;

use crate::bell::Bell;
use crate::delivery_timing::{delivery_action, DeliveryAction, DeliveryTimingError};
use crate::drain::InboxDrainer;
use crate::wake_policy::{AgentState, WakeDecision, WakePolicy};

/// Daemon-to-agent delivery is model context, not a human preview. The CLI `listen` path still
/// applies `msg_preview_chars`; the per-agent loop must preserve full bodies so agents do not make
/// decisions from truncated thread/DM input.
const AGENT_DELIVERY_PREVIEW_CHARS: u32 = u32::MAX;

/// Last-resort backstop await for harness turn-completion. On elapse the batch is dead-lettered
/// (moved to the `error` state), NOT marked delivered. This MUST stay above every adapter's own
/// internal completion bound (codex app-server: 600s): elapsing CANCELS the inject future, and a
/// mid-statement drop interrupts the daemon's shared store connection — the 07-11 SQLITE_INTERRUPT
/// storms (N25) were this timeout (then 45s) firing on every codex turn longer than 45s, killing
/// unrelated command-worker/forwarder statements and dead-lettering mail codex had already
/// accepted. Adapters own the real turn-completion bound and return an ERROR (not cancellation)
/// when it elapses; this outer arm exists only for adapters that hang without one.
pub const DEFAULT_INJECT_COMPLETION_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(660);

/// Retained as a configuration compatibility constant. Provider failures are terminal and this
/// value no longer schedules an automatic retry.
pub const DEFAULT_PROVIDER_LIMIT_COOLDOWN: std::time::Duration =
    std::time::Duration::from_secs(300);

/// Everything one [`EventLoop`] task needs. Cloneable handles only, so the loop owns its own set.
#[derive(Clone)]
pub struct LoopDeps {
    /// The durable store (sole writer); repos are bound per drain.
    pub store: Arc<Store>,
    /// The per-session wake bell (parked on; re-rung on attach).
    pub bell: Bell,
    /// The agent-state registry (Idle/Busy/Paused/Offline) consulted via the [`WakePolicy`].
    pub registry: crate::state::AgentRegistry,
    /// Drives ACP `session/prompt` — injects one drained batch as one turn.
    pub turn_exec: Arc<dyn AgentTurnExecutionPort>,
    /// Broadcast sink for `message.delivered`.
    pub events: Arc<dyn EventSink>,
    /// Project scope.
    pub project: String,
    /// Per-drop cap.
    pub drain_limit: u32,
    /// Per-message preview budget.
    pub preview_chars: u32,
    /// Bounded await for harness turn-completion; on elapse the batch is dead-lettered, not delivered.
    pub completion_timeout: std::time::Duration,
    /// Deprecated compatibility field; retry is explicit and this duration is not scheduled.
    pub provider_limit_default_cooldown: std::time::Duration,
}

/// A spawned per-agent loop. Holds the task handle so the daemon can join/abort it on teardown.
pub struct EventLoop;

impl EventLoop {
    /// Spawn the long-lived loop for one `kind=agent` session. Immediately re-rings the session's
    /// bell so anything already `pending` at attach time (crash-safety) is drained on first park.
    pub fn spawn(session: SessionId, deps: LoopDeps) -> JoinHandle<()> {
        // Re-ring on attach: drive any pre-existing pending rows (a bell missed while detached).
        deps.bell.ring(&session);
        tokio::spawn(async move { run(session, deps).await })
    }
}

/// The loop body. Parks, then drains-injects until the queue is empty (coalescing window), then
/// re-parks. Runs until the task is aborted.
async fn run(session: SessionId, deps: LoopDeps) {
    tracing::info!(
        target: "nexus_dispatch::loop",
        session = %session,
        project = %deps.project,
        "event loop parked (per-agent consumer attached)"
    );
    loop {
        deps.bell.wait(&session).await;
        // Hop 1: the bell woke us. If a live run stops here with no later "drained" line, the loop
        // is waking but the drain comes back empty — almost always a project-scope mismatch
        // (`project` here != the in-flight row's project; see `drain_once`).
        tracing::debug!(
            target: "nexus_dispatch::loop",
            session = %session,
            "bell wake received"
        );

        // Consult the wake policy. `Hold` (paused/offline) → do not force a turn; re-park.
        // `Coalesce` should not normally be observed here (the loop is the only consumer and it is
        // Idle while parked), but treat it as a drain too — there is pending work either way.
        let state = deps.registry.get(&session);
        let decision = WakePolicy::should_wake(state, nexus_contracts::Kind::Agent);
        if decision == WakeDecision::Hold {
            tracing::debug!(
                target: "nexus_dispatch::loop",
                session = %session,
                ?state,
                "wake held (paused/offline); re-parking"
            );
            continue;
        }

        // Mark Busy for the whole turn window so mid-turn enqueues coalesce.
        deps.registry.set(&session, AgentState::Busy);

        // Drain → inject until the queue is empty. Each non-empty drain is one injected turn; a
        // drain that comes back empty closes the coalescing window.
        loop {
            let inbox = Inbox::new(&deps.store);
            // Each drain (including the coalesced turn-end drain) is a durable notification
            // boundary. Mid-turn enqueues are still `pending`, so advance them before reading.
            if let Err(error) = inbox.mark_notified(&session).await {
                tracing::error!(%error, %session, "mark_notified failed; refusing to inject");
                break;
            }
            let timed_batch = match InboxDrainer::drain_notified_timed_once(
                &inbox,
                &session,
                &deps.project,
                deps.drain_limit,
                AGENT_DELIVERY_PREVIEW_CHARS,
            )
            .await
            {
                Ok(Some(batch)) => batch,
                Ok(None) => break,
                Err(e) => {
                    tracing::error!(error = %e, session = %session, "drain failed");
                    break;
                }
            };

            // Hop 2: drain result. `count == 0` after a wake is THE signature of the project-scope
            // bug — the bell rang but the drain query (scoped to `project`) matched no in-flight row.
            tracing::debug!(
                target: "nexus_dispatch::loop",
                session = %session,
                project = %deps.project,
                count = timed_batch.batch.counts.total,
                "drain_once result"
            );
            let timing = timed_batch.timing;
            let batch = timed_batch.batch;
            if batch.counts.total == 0 {
                break;
            }

            // Hop 3: about to inject the drained batch as one turn.
            tracing::info!(
                target: "nexus_dispatch::loop",
                session = %session,
                count = batch.counts.total,
                "injecting turn"
            );

            let turn_active = has_active_turn(&session, &deps);
            match delivery_action(
                timing,
                turn_active,
                deps.turn_exec.steer_capability(&session),
            ) {
                Ok(DeliveryAction::WaitForTurnBoundary)
                | Ok(DeliveryAction::WaitForFinalTurnCompletion) => {
                    if let Err(error) = deps.turn_exec.wait_for_turn_completion(&session).await {
                        if !claim_batch(&session, &deps, &inbox, &batch).await {
                            break;
                        }
                        handle_inject_error(
                            &session,
                            &deps,
                            &inbox,
                            &batch,
                            InjectError::Contract(error),
                        )
                        .await;
                    }
                    continue;
                }
                Ok(DeliveryAction::NativeSteer) | Ok(DeliveryAction::InterruptAndSend) => {
                    if !claim_batch(&session, &deps, &inbox, &batch).await {
                        break;
                    }
                    match steer_claimed_batch(&session, &deps, &inbox, &batch).await {
                        SteerAttempt::Delivered | SteerAttempt::TurnEnded => continue,
                        SteerAttempt::Failed => break,
                    }
                }
                Err(error) => {
                    if !claim_batch(&session, &deps, &inbox, &batch).await {
                        break;
                    }
                    handle_inject_error(
                        &session,
                        &deps,
                        &inbox,
                        &batch,
                        InjectError::Contract(delivery_timing_contract_error(timing, error)),
                    )
                    .await;
                    continue;
                }
                Ok(DeliveryAction::StartTurn) => {}
            }

            // Persist the one permitted attempt before crossing the harness boundary. If the
            // daemon exits after this point, boot recovery turns `injecting` into the terminal
            // `delivery_outcome_unknown`; it never guesses that retrying is safe.
            if !claim_batch(&session, &deps, &inbox, &batch).await {
                break;
            }

            // Inject the whole batch as a single turn. The await is BOUNDED: adapters that wait
            // for harness turn-completion (codex app-server, opencode ACP) can hang forever when
            // the completion notification is lost (e.g. adopted transports after a daemon
            // restart — the 2026-07-04 cycle-3 outage). An unbounded await wedges this loop and
            // the session goes deaf while its heartbeat stays green. On timeout the completion
            // never arrived, so we CANNOT claim delivery: the batch is dead-lettered to the
            // non-delivered `error` state — neither falsely marked delivered (silent data loss)
            // nor left `pending` to re-inject the same rows on every future bell (the "repeating
            // batch" storm). Only the `Ok(Ok(()))` arm (observed completion) marks delivered.
            let accepted_event = bus_user_input_event(&session, &batch);
            let turn = deps.turn_exec.inject_turn_observed(
                &session,
                &batch,
                deps.events.clone(),
                accepted_event,
            );
            tokio::pin!(turn);
            let completion_timeout = tokio::time::sleep(deps.completion_timeout);
            tokio::pin!(completion_timeout);
            let mut redrive_after_interrupt = false;
            let stop_after_turn = loop {
                tokio::select! {
                    // Prefer a terminal completion when it races the bell. The post-completion
                    // drain can then start the newly queued batch normally instead of attempting a
                    // steer against a turn that is already gone.
                    biased;
                    result = &mut turn => {
                        match result {
                            Ok(()) => {
                                tracing::info!(
                                    target: "nexus_dispatch::loop",
                                    session = %session,
                                    count = batch.counts.total,
                                    "turn injected; marking delivered"
                                );
                                mark_batch_delivered(&session, &deps, &inbox, &batch).await;
                                break false;
                            }
                            Err(error) => {
                                handle_inject_error(&session, &deps, &inbox, &batch, error).await;
                                if redrive_after_interrupt {
                                    deps.bell.ring(&session);
                                }
                                break true;
                            }
                        }
                    }
                    _ = &mut completion_timeout => {
                        tracing::warn!(
                            target: "nexus_dispatch::loop",
                            session = %session,
                            count = batch.counts.total,
                            timeout_s = deps.completion_timeout.as_secs(),
                            "inject_turn completion await timed out; dead-lettering: marking delivery \
                             FAILED, not delivered (completion signal lost?)"
                        );
                        mark_batch_timeout(&session, &deps, &inbox, &batch).await;
                        if redrive_after_interrupt {
                            deps.bell.ring(&session);
                        }
                        break true;
                    }
                    _ = deps.bell.wait(&session) => {
                        // The current completion future stays alive while the new durable batch is
                        // admitted into the active turn. Dropping it here can block delivery for
                        // the full completion timeout.
                        if redrive_after_interrupt {
                            // Cancellation has already been accepted. Coalesce later arrivals at
                            // the same durable notified boundary without sending another cancel.
                            if let Err(error) = inbox.mark_notified(&session).await {
                                tracing::error!(%error, session = %session, "mid-turn coalesce after interrupt failed");
                            }
                        } else {
                            redrive_after_interrupt =
                                handle_pending_during_active_turn(&session, &deps, &inbox).await;
                        }
                    }
                }
            };
            if stop_after_turn {
                break;
            }
            // Loop: re-drain to pick up anything that arrived during the turn (coalescing).
        }

        // Turn window closed; back to Idle unless a structured adapter hold replaced Busy.
        if deps.registry.get(&session) == AgentState::Busy {
            deps.registry.set(&session, AgentState::Idle);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SteerAttempt {
    Delivered,
    TurnEnded,
    Failed,
}

fn has_active_turn(session: &SessionId, deps: &LoopDeps) -> bool {
    deps.turn_exec
        .active_turn_sessions()
        .iter()
        .any(|active| active == session)
}

async fn claim_batch(
    session: &SessionId,
    deps: &LoopDeps,
    inbox: &Inbox<'_>,
    batch: &NexusBatch,
) -> bool {
    let expected = batch.message_ids.len() as u64;
    match inbox
        .mark_batch_injecting(&batch.message_ids, session)
        .await
    {
        Ok(affected) if affected == expected => true,
        Ok(affected) => {
            tracing::error!(
                %session,
                expected,
                affected,
                "delivery batch claim was incomplete; no rows claimed and no harness injection attempted"
            );
            deps.bell.ring(session);
            false
        }
        Err(error) => {
            tracing::error!(%error, %session, "atomic delivery batch claim failed");
            deps.bell.ring(session);
            false
        }
    }
}

async fn mark_batch_delivered(
    session: &SessionId,
    deps: &LoopDeps,
    inbox: &Inbox<'_>,
    batch: &NexusBatch,
) {
    for id in &batch.message_ids {
        match inbox.mark_delivered(id, session).await {
            Ok(1) => {
                project_delivery_settlement(session, deps, inbox, id).await;
                deps.events
                    .emit(WsEvent::MessageDelivered {
                        message_id: id.clone(),
                        recipient: session.clone(),
                    })
                    .await;
            }
            Ok(affected) => tracing::error!(
                message = %id,
                affected,
                "mark_delivered did not settle exactly one injecting row"
            ),
            Err(error) => {
                tracing::error!(%error, message = %id, "mark_delivered failed");
            }
        }
    }
}

async fn mark_batch_timeout(
    session: &SessionId,
    deps: &LoopDeps,
    inbox: &Inbox<'_>,
    batch: &NexusBatch,
) {
    for id in &batch.message_ids {
        match inbox
            .mark_delivery_error(
                id,
                session,
                COMPLETION_TIMEOUT_ERROR_CODE,
                "harness completion await timed out",
                Some("{\"source\":\"realtime_outer_timeout\"}"),
            )
            .await
        {
            Ok(1) => project_delivery_settlement(session, deps, inbox, id).await,
            Ok(affected) => tracing::error!(
                message = %id,
                affected,
                "mark delivery timeout did not affect exactly one row"
            ),
            Err(error) => {
                tracing::error!(%error, message = %id, "mark delivery timeout failed");
            }
        }
    }
}

async fn handle_pending_during_active_turn(
    session: &SessionId,
    deps: &LoopDeps,
    inbox: &Inbox<'_>,
) -> bool {
    if let Err(error) = inbox.mark_notified(session).await {
        tracing::error!(%error, %session, "mid-turn mark_notified failed");
        return false;
    }
    let timed_batch = match InboxDrainer::drain_notified_timed_once(
        inbox,
        session,
        &deps.project,
        deps.drain_limit,
        AGENT_DELIVERY_PREVIEW_CHARS,
    )
    .await
    {
        Ok(Some(batch)) if batch.batch.counts.total > 0 => batch,
        Ok(_) => return false,
        Err(error) => {
            tracing::error!(%error, %session, "mid-turn drain failed");
            return false;
        }
    };
    let timing = timed_batch.timing;
    let batch = timed_batch.batch;
    match delivery_action(timing, true, deps.turn_exec.steer_capability(session)) {
        Ok(DeliveryAction::NativeSteer) => {
            if claim_batch(session, deps, inbox, &batch).await {
                let _ = steer_claimed_batch(session, deps, inbox, &batch).await;
            }
            false
        }
        Ok(DeliveryAction::InterruptAndSend) => {
            interrupt_and_redrive_notified_batch(session, deps, inbox, &batch).await
        }
        Ok(DeliveryAction::WaitForTurnBoundary)
        | Ok(DeliveryAction::WaitForFinalTurnCompletion) => {
            // The current turn future is still being polled by the caller. Leave these rows
            // notified; its terminal branch immediately re-drains them.
            false
        }
        Err(error) => {
            if claim_batch(session, deps, inbox, &batch).await {
                handle_inject_error(
                    session,
                    deps,
                    inbox,
                    &batch,
                    InjectError::Contract(delivery_timing_contract_error(timing, error)),
                )
                .await;
            }
            false
        }
        Ok(DeliveryAction::StartTurn) => unreachable!("the caller owns an active turn"),
    }
}

/// Cancel the active prompt while leaving the replacement batch at the durable `notified`
/// boundary. The caller still owns and polls the active turn future; once cancellation makes that
/// future terminal, the normal drain path atomically claims and starts the replacement prompt.
///
/// Do not call `steer_observed` inline here for interrupt-and-send adapters. Their replacement
/// prompt is serialized behind the active prompt. Awaiting it from this bell branch pauses polling
/// of the active future, so the cancelled prompt can never release the serialization boundary.
async fn interrupt_and_redrive_notified_batch(
    session: &SessionId,
    deps: &LoopDeps,
    inbox: &Inbox<'_>,
    batch: &NexusBatch,
) -> bool {
    if let Err(error) = deps.turn_exec.interrupt_active_turn(session).await {
        handle_inject_error(session, deps, inbox, batch, InjectError::Contract(error)).await;
        return false;
    }
    true
}

fn delivery_timing_contract_error(
    timing: DeliveryTiming,
    error: DeliveryTimingError,
) -> ContractError {
    let message = match error {
        DeliveryTimingError::InterruptUnsupported => format!(
            "delivery timing {} is unsupported by this harness while a turn is active",
            timing.as_str()
        ),
    };
    ContractError {
        code: nexus_contracts::codes::DELIVERY_TIMING_UNSUPPORTED,
        message,
    }
}

async fn steer_claimed_batch(
    session: &SessionId,
    deps: &LoopDeps,
    inbox: &Inbox<'_>,
    batch: &NexusBatch,
) -> SteerAttempt {
    let accepted_event = bus_user_input_event(session, batch);
    let text = render_injected_turn_for(batch, &session.0);
    match deps
        .turn_exec
        .steer_observed(session, text, deps.events.clone(), accepted_event)
        .await
    {
        Ok(_) => {
            tracing::info!(
                target: "nexus_dispatch::loop",
                session = %session,
                count = batch.counts.total,
                "active-turn delivery accepted; marking delivered"
            );
            mark_batch_delivered(session, deps, inbox, batch).await;
            SteerAttempt::Delivered
        }
        Err(error) if native_steer_was_not_admitted(&error) => {
            for id in &batch.message_ids {
                match inbox.restore_rejected_steer(id, session).await {
                    Ok(1) => {}
                    Ok(affected) => tracing::error!(
                        %session,
                        message = %id,
                        affected,
                        "rejected steer claim was not restored exactly once"
                    ),
                    Err(restore_error) => tracing::error!(
                        error = %restore_error,
                        %session,
                        message = %id,
                        "rejected steer claim restore failed"
                    ),
                }
            }
            SteerAttempt::TurnEnded
        }
        Err(error) => {
            handle_inject_error(session, deps, inbox, batch, InjectError::Contract(error)).await;
            SteerAttempt::Failed
        }
    }
}

fn native_steer_was_not_admitted(error: &nexus_contracts::ContractError) -> bool {
    error.code == nexus_contracts::codes::ACTIVE_TURN_REQUIRED
        || matches!(
            error.message.as_str(),
            "cannot steer a review turn" | "cannot steer a compact turn"
        )
}

async fn handle_inject_error(
    session: &SessionId,
    deps: &LoopDeps,
    inbox: &Inbox<'_>,
    batch: &NexusBatch,
    error: InjectError,
) {
    let (code, reason, details) = match error {
        InjectError::ProviderLimit(limit) => {
            tracing::error!(
                target: "nexus_dispatch::loop",
                session = %session,
                harness = ?limit.harness,
                reason = ?limit.reason,
                provider = limit.provider.as_deref().unwrap_or(""),
                model = limit.model.as_deref().unwrap_or(""),
                source = %limit.source,
                "provider limit; settling this delivery as terminal"
            );
            deps.registry.set(session, AgentState::ProviderLimited);
            (
                PROVIDER_LIMIT_ERROR_CODE,
                format!("provider limit: {:?}", limit.reason),
                serde_json::to_string(&limit).ok(),
            )
        }
        InjectError::ProviderError(error) => {
            tracing::error!(
                target: "nexus_dispatch::loop",
                session = %session,
                harness = ?error.harness,
                reason = %error.reason,
                provider = error.provider.as_deref().unwrap_or(""),
                model = error.model.as_deref().unwrap_or(""),
                retryable = error.retryable,
                source = %error.source,
                "provider error; settling this attempt as terminal for explicit retry"
            );
            (
                PROVIDER_ERROR_CODE,
                error.reason.clone(),
                serde_json::to_string(&error).ok(),
            )
        }
        InjectError::OperatorAction(action) => {
            tracing::error!(
                target: "nexus_dispatch::loop",
                session = %session,
                harness = ?action.harness,
                reason = %action.reason,
                provider = action.provider.as_deref().unwrap_or(""),
                model = action.model.as_deref().unwrap_or(""),
                source = %action.source,
                "operator action required; settling this delivery as terminal"
            );
            deps.registry.set(session, AgentState::ProviderLimited);
            (
                OPERATOR_ACTION_ERROR_CODE,
                action.reason.clone(),
                serde_json::to_string(&action).ok(),
            )
        }
        InjectError::CompletionTimeout { session: _, source } => {
            tracing::error!(%session, %source, "adapter completion timed out; terminal delivery error");
            (
                COMPLETION_TIMEOUT_ERROR_CODE,
                "harness accepted the prompt but model completion was not observed".to_string(),
                Some(serde_json::json!({ "source": source }).to_string()),
            )
        }
        InjectError::Contract(error) => {
            let unreachable = contract_is_target_unreachable(&error);
            tracing::error!(
                error = %error,
                session = %session,
                "inject_turn failed; settling this delivery as terminal"
            );
            (
                if unreachable {
                    TARGET_UNREACHABLE_ERROR_CODE
                } else {
                    CONTRACT_ERROR_CODE
                },
                error.message.clone(),
                Some(serde_json::json!({ "rpcCode": error.code }).to_string()),
            )
        }
    };

    for id in &batch.message_ids {
        match inbox
            .mark_delivery_error(id, session, code, &reason, details.as_deref())
            .await
        {
            Ok(1) => project_delivery_settlement(session, deps, inbox, id).await,
            Ok(affected) => tracing::error!(
                %session,
                message = %id,
                affected,
                error_code = code,
                "terminal delivery error did not settle exactly one row"
            ),
            Err(error) => tracing::error!(
                %error,
                %session,
                message = %id,
                error_code = code,
                "terminal delivery settlement failed"
            ),
        }
    }
}

async fn project_delivery_settlement(
    session: &SessionId,
    deps: &LoopDeps,
    inbox: &Inbox<'_>,
    message: &nexus_contracts::ids::MessageId,
) {
    match inbox.gateway_delivery_effect(message, session).await {
        Ok(Some(effect)) => {
            deps.events.project(effect).await;
            if let Err(error) = inbox.discard_settled_message_if_complete(message).await {
                tracing::error!(
                    %error,
                    %session,
                    message = %message,
                    "failed to discard completed boot-scoped delivery residue"
                );
            }
        }
        Ok(None) => tracing::error!(
            %session,
            message = %message,
            "terminal delivery row committed without a projection snapshot"
        ),
        Err(error) => tracing::error!(
            %error,
            %session,
            message = %message,
            "failed to build committed delivery projection"
        ),
    }
}

fn contract_is_target_unreachable(error: &nexus_contracts::ContractError) -> bool {
    let message = error.message.to_ascii_lowercase();
    message.contains("not receiving turns")
        || message.contains("not live")
        || message.contains("offline")
        || message.contains("unreachable")
        || message.contains("no transport")
}

fn bus_user_input_event(session: &SessionId, batch: &NexusBatch) -> WsEvent {
    let message_ids: Vec<String> = batch.message_ids.iter().map(|id| id.0.clone()).collect();
    let client_message_id = if message_ids.is_empty() {
        "bus:empty".to_string()
    } else {
        format!("bus:{}", message_ids.join(","))
    };
    WsEvent::AgentUpdate {
        session_id: session.clone(),
        kind: AgentUpdateKind::UserInput,
        data: serde_json::json!({
            "text": render_injected_turn_for(batch, &session.0),
            "source": "bus",
            "sourceLane": "message.created",
            "clientMessageId": client_message_id,
            "messageIds": message_ids,
            "counts": {
                "dms": batch.counts.dms,
                "thread": batch.counts.thread,
                "total": batch.counts.total,
            },
        }),
    }
}
