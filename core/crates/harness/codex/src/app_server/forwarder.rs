//! `spawn_codex_forwarder` — bridges a [`CodexAppServerClient`] notification
//! stream into the nexus [`EventSink`] as [`WsEvent::AgentUpdate`] events.
//!
//! # Coalescing
//! Consecutive `Text` deltas are coalesced into a single emit within a ~50 ms
//! window. When the FIRST Text delta is buffered, a fixed `flush_at` deadline
//! is set (`now + 50 ms`). Subsequent Text deltas extend the buffer but do NOT
//! re-arm the deadline. When `flush_at` is reached, the buffer is flushed
//! regardless of how many deltas are still arriving. This guarantees bounded
//! latency (~50 ms after the first delta) even under a steady stream where
//! inter-delta gaps are less than 50 ms. A non-Text event flushes buffered
//! text before emitting itself. `TurnEnd` does the same. No text is ever
//! dropped.
//!
//! Codex also sends `item/completed` snapshots for `agentMessage` items after
//! already streaming `item/agentMessage/delta`. The forwarder tracks streamed
//! item ids and suppresses those final snapshots so the durable Nexus stream
//! contains one assistant transcript, not streamed text plus a duplicate final.
//!
//! # Approval handling
//! When codex sends a server→client request (`id: Some(_)`) whose method is
//! one of {CMD_REQUEST_APPROVAL, FILE_REQUEST_APPROVAL,
//! PERMISSIONS_REQUEST_APPROVAL, TOOL_REQUEST_USER_INPUT}, the forwarder calls
//! `approvals.ask(session, method, params)` and immediately responds via
//! `client.respond(id, result)`. The notification path is unchanged.
//!
//! For an unrecognised server→client request (`id: Some(_)` + unknown method),
//! the forwarder responds with `{}` to avoid leaving codex hanging.

use std::collections::HashSet;
use std::sync::Arc;

use nexus_contracts::events::WsEvent;
use nexus_contracts::ids::SessionId;
use nexus_contracts::ports::EventSink;
use nexus_contracts::AgentUpdateKind;
use nexus_transcript::{ToolCallObservation, ToolCallPhase};
use serde_json::{json, Value};
use tokio::time::{Duration, Instant};

use super::approvals::ApprovalHandler;
use super::client::CodexAppServerClient;
use super::protocol::method;
use super::provider_limit::{turn_error_is_structured_hold, turn_error_will_retry};
use super::translate::{tool_call_observations, translate_codex, user_message_text};
use super::turn_completion::CodexTurnTracker;

/// Coalescing window: flush buffered Text deltas this long after the FIRST
/// delta in a run (fixed deadline — NOT re-armed on subsequent deltas).
const COALESCE_WINDOW: Duration = Duration::from_millis(50);

#[derive(Debug)]
struct BufferedText {
    text: String,
    item_id: Option<String>,
}

impl BufferedText {
    fn from_data(data: &Value) -> Self {
        Self {
            text: data["text"].as_str().unwrap_or("").to_string(),
            item_id: data
                .get("itemId")
                .or_else(|| data.get("item_id"))
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty())
                .map(str::to_string),
        }
    }

    fn into_data(self) -> Value {
        let mut data = json!({ "text": self.text });
        if let Some(item_id) = self.item_id {
            data["itemId"] = Value::String(item_id);
        }
        data
    }
}

/// Observation-only sink for Codex app-server tool-call phases.
///
/// Implementations must keep this side channel metadata-only: no store writes, no realtime bells,
/// no command intents, and no turn injection. The daemon implementation publishes into the bounded
/// gateway developer-event ring.
pub trait CodexToolObservationSink: Send + Sync {
    fn publish_tool_call(&self, session: &SessionId, observation: ToolCallObservation);
}

/// The approval method set — server→client requests that need a response.
fn is_approval_method(m: &str) -> bool {
    matches!(
        m,
        method::CMD_REQUEST_APPROVAL
            | method::FILE_REQUEST_APPROVAL
            | method::PERMISSIONS_REQUEST_APPROVAL
            | method::TOOL_REQUEST_USER_INPUT
    )
}

/// Spawn a task that reads `client`'s notification stream, translates every
/// codex notification via [`translate_codex`], coalesces consecutive `Text`
/// deltas with a fixed-deadline window, and emits the resulting
/// [`WsEvent`]s on `events`.
///
/// `client` must already be subscribed to the target thread. In the fresh headed bridge,
/// [`super::bridge::CodexBridge`] discovers the human TUI's rollout, calls `thread/resume` on this
/// second connection, binds the same connection for turn injection, and then passes it here.
///
/// Server→client approval requests (frames with `id: Some(_)`) are handled
/// inline via `approvals` before the notification path runs. The task exits
/// when the notifications channel closes.
pub fn spawn_codex_forwarder(
    session: SessionId,
    client: Arc<CodexAppServerClient>,
    events: Arc<dyn EventSink>,
    approvals: Arc<dyn ApprovalHandler>,
    turn_tracker: CodexTurnTracker,
) -> tokio::task::JoinHandle<()> {
    spawn_codex_forwarder_with_tool_observations(
        session,
        client,
        events,
        approvals,
        turn_tracker,
        None,
    )
}

/// Spawn a Codex app-server forwarder and optionally publish metadata-only tool-call observations.
///
/// The visible stream behavior is identical to [`spawn_codex_forwarder`]. The optional observation
/// sink receives `pre` events for command-output deltas once per item id and `post` events for
/// terminal tool item completions. Those observations are for developer-event subscribers only and
/// must never affect delivery, wake, or durable history.
pub fn spawn_codex_forwarder_with_tool_observations(
    session: SessionId,
    client: Arc<CodexAppServerClient>,
    events: Arc<dyn EventSink>,
    approvals: Arc<dyn ApprovalHandler>,
    turn_tracker: CodexTurnTracker,
    tool_observations: Option<Arc<dyn CodexToolObservationSink>>,
) -> tokio::task::JoinHandle<()> {
    let origin = client.origin_tracker();
    let native_ingress = origin.is_some();
    let turn_tracker = origin.unwrap_or(turn_tracker);
    tokio::spawn(async move {
        let mut notes = client.notifications();
        // Accumulated Text delta — None means no text is buffered.
        let mut text_buf: Option<BufferedText> = None;
        // Fixed flush deadline — set once when the FIRST Text delta is
        // buffered; NOT re-armed when subsequent deltas arrive.
        let mut flush_at: Option<Instant> = None;
        // Codex final `agentMessage` completions are snapshots, not deltas.
        // If we saw deltas for the same item id, skip the completion snapshot.
        let mut streamed_agent_messages: HashSet<String> = HashSet::new();
        // Codex command-output deltas repeat while a process streams. Publish the pre phase once
        // per native item id; terminal item/completed rows still publish their post phase.
        let mut started_tool_items: HashSet<String> = HashSet::new();

        loop {
            if let Some(deadline) = flush_at {
                // --- text is buffered ---
                let remaining = deadline.saturating_duration_since(Instant::now());

                if remaining.is_zero() {
                    // Deadline already passed — flush immediately without recv.
                    flush_text_buffer(&session, events.as_ref(), &mut text_buf, &mut flush_at)
                        .await;
                    continue;
                }

                // Wait for the next notification, but only up to `remaining`.
                match tokio::time::timeout(remaining, notes.recv()).await {
                    Err(_elapsed) => {
                        // Fixed deadline reached — flush buffered text.
                        flush_text_buffer(&session, events.as_ref(), &mut text_buf, &mut flush_at)
                            .await;
                        // Loop: flush_at is now None, so we go to the unbuffered path.
                    }
                    Ok(None) => {
                        // Channel closed — flush any remaining text then exit.
                        flush_text_buffer(&session, events.as_ref(), &mut text_buf, &mut flush_at)
                            .await;
                        break;
                    }
                    Ok(Some(n)) => {
                        log_wire_notification(&session, &n.method, &n.params);
                        // --- approval / server-request branch ---
                        if let Some(id) = n.id.clone() {
                            let result = if is_approval_method(&n.method) {
                                approvals.ask(&session, &n.method, &n.params).await
                            } else {
                                // Unknown server→client request: respond with {} to avoid hanging codex.
                                tracing::debug!(
                                    "codex forwarder: unrecognised server-request method={} — responding {{}}",
                                    n.method
                                );
                                json!({})
                            };
                            if let Err(e) = client.respond(id, result).await {
                                tracing::warn!("codex forwarder: respond failed: {e}");
                            }
                            continue;
                        }

                        publish_tool_observations(
                            &session,
                            tool_observations.as_deref(),
                            &mut started_tool_items,
                            &n.method,
                            &n.params,
                        );
                        if !native_ingress {
                            observe_turn_authority(&turn_tracker, &n.method, &n.params);
                        }

                        let Some(ev) = translate_codex(&n.method, &n.params) else {
                            emit_accepted_if_turn_notification(&turn_tracker, &n.method, &n.params)
                                .await;
                            continue;
                        };

                        emit_accepted_if_turn_notification(&turn_tracker, &n.method, &n.params)
                            .await;

                        mark_delivery_receipt_if_needed(&turn_tracker, &n.params, &ev);

                        if let Some(item_id) = agent_message_delta_id(&n.method, &n.params) {
                            streamed_agent_messages.insert(item_id.to_string());
                        }
                        if let Some(item_id) = completed_agent_message_id(&n.method, &n.params) {
                            if streamed_agent_messages.remove(item_id) {
                                flush_text_buffer(
                                    &session,
                                    events.as_ref(),
                                    &mut text_buf,
                                    &mut flush_at,
                                )
                                .await;
                                continue;
                            }
                        }
                        if suppress_accepted_user_input_echo(
                            &turn_tracker,
                            &n.method,
                            &n.params,
                            &ev,
                        )
                        .await
                        {
                            flush_text_buffer(
                                &session,
                                events.as_ref(),
                                &mut text_buf,
                                &mut flush_at,
                            )
                            .await;
                            continue;
                        }

                        if ev.kind == AgentUpdateKind::Text {
                            // Another Text delta — append to buffer.
                            // IMPORTANT: do NOT update flush_at (fixed deadline).
                            let incoming = BufferedText::from_data(&ev.data);
                            match &mut text_buf {
                                Some(buf) if buf.item_id == incoming.item_id => {
                                    buf.text.push_str(&incoming.text)
                                }
                                Some(_) => {
                                    flush_text_buffer(
                                        &session,
                                        events.as_ref(),
                                        &mut text_buf,
                                        &mut flush_at,
                                    )
                                    .await;
                                    text_buf = Some(incoming);
                                    flush_at = Some(Instant::now() + COALESCE_WINDOW);
                                }
                                None => {
                                    text_buf = Some(incoming);
                                    flush_at = Some(Instant::now() + COALESCE_WINDOW);
                                }
                            }
                        } else {
                            // Non-Text event — flush buffered text first, then emit event.
                            flush_text_buffer(
                                &session,
                                events.as_ref(),
                                &mut text_buf,
                                &mut flush_at,
                            )
                            .await;
                            events
                                .emit(WsEvent::AgentUpdate {
                                    session_id: session.clone(),
                                    kind: ev.kind,
                                    data: ev.data,
                                })
                                .await;
                            mark_turn_complete_if_needed(&turn_tracker, &n.method, &n.params);
                        }
                    }
                }
            } else {
                // --- no text buffered — block until the next notification ---
                let Some(n) = notes.recv().await else {
                    // Channel closed with no buffered text — just exit.
                    break;
                };
                log_wire_notification(&session, &n.method, &n.params);

                // --- approval / server-request branch ---
                if let Some(id) = n.id.clone() {
                    let result = if is_approval_method(&n.method) {
                        approvals.ask(&session, &n.method, &n.params).await
                    } else {
                        tracing::debug!(
                            "codex forwarder: unrecognised server-request method={} — responding {{}}",
                            n.method
                        );
                        json!({})
                    };
                    if let Err(e) = client.respond(id, result).await {
                        tracing::warn!("codex forwarder: respond failed: {e}");
                    }
                    continue;
                }

                publish_tool_observations(
                    &session,
                    tool_observations.as_deref(),
                    &mut started_tool_items,
                    &n.method,
                    &n.params,
                );
                if !native_ingress {
                    observe_turn_authority(&turn_tracker, &n.method, &n.params);
                }

                let Some(ev) = translate_codex(&n.method, &n.params) else {
                    emit_accepted_if_turn_notification(&turn_tracker, &n.method, &n.params).await;
                    continue;
                };

                emit_accepted_if_turn_notification(&turn_tracker, &n.method, &n.params).await;

                mark_delivery_receipt_if_needed(&turn_tracker, &n.params, &ev);

                if let Some(item_id) = agent_message_delta_id(&n.method, &n.params) {
                    streamed_agent_messages.insert(item_id.to_string());
                }
                if let Some(item_id) = completed_agent_message_id(&n.method, &n.params) {
                    if streamed_agent_messages.remove(item_id) {
                        flush_text_buffer(&session, events.as_ref(), &mut text_buf, &mut flush_at)
                            .await;
                        continue;
                    }
                }
                if suppress_accepted_user_input_echo(&turn_tracker, &n.method, &n.params, &ev).await
                {
                    continue;
                }

                if ev.kind == AgentUpdateKind::Text {
                    // First delta in a new run — buffer it and set the fixed deadline.
                    text_buf = Some(BufferedText::from_data(&ev.data));
                    flush_at = Some(Instant::now() + COALESCE_WINDOW);
                } else {
                    // Non-Text event with no buffer — emit directly.
                    events
                        .emit(WsEvent::AgentUpdate {
                            session_id: session.clone(),
                            kind: ev.kind,
                            data: ev.data,
                        })
                        .await;
                    mark_turn_complete_if_needed(&turn_tracker, &n.method, &n.params);
                }
            }
        }

        // Keep `client` alive for the task's lifetime so the connection stays open.
        drop(client);
    })
}

fn publish_tool_observations(
    session: &SessionId,
    sink: Option<&dyn CodexToolObservationSink>,
    started_tool_items: &mut HashSet<String>,
    method: &str,
    params: &Value,
) {
    let Some(sink) = sink else {
        return;
    };
    for observation in tool_call_observations(method, params) {
        if observation.phase == ToolCallPhase::Pre {
            if let Some(id) = observation.tool_call_id.as_deref() {
                if !started_tool_items.insert(id.to_string()) {
                    continue;
                }
            }
        }
        sink.publish_tool_call(session, observation);
    }
}

async fn flush_text_buffer(
    session: &SessionId,
    events: &dyn EventSink,
    text_buf: &mut Option<BufferedText>,
    flush_at: &mut Option<Instant>,
) {
    let Some(buf) = text_buf.take() else {
        *flush_at = None;
        return;
    };
    *flush_at = None;
    events
        .emit(WsEvent::AgentUpdate {
            session_id: session.clone(),
            kind: AgentUpdateKind::Text,
            data: buf.into_data(),
        })
        .await;
}

fn agent_message_delta_id<'a>(method: &str, params: &'a Value) -> Option<&'a str> {
    (method == method::AGENT_MESSAGE_DELTA)
        .then(|| params.get("itemId").and_then(Value::as_str))
        .flatten()
}

fn completed_agent_message_id<'a>(method: &str, params: &'a Value) -> Option<&'a str> {
    if method != method::ITEM_COMPLETED {
        return None;
    }
    let item = params.get("item")?;
    (item.get("type").and_then(Value::as_str) == Some("agentMessage"))
        .then(|| item.get("id").and_then(Value::as_str))
        .flatten()
}

/// Log one debug line per received notification with actual protocol methods and IDs.
/// Enable with
/// `RUST_LOG=nexus_harness_codex=debug`; silent otherwise.
fn log_wire_notification(session: &SessionId, method: &str, params: &Value) {
    // Field values are computed OUTSIDE the macro: `tracing`'s field syntax resolves
    // bare `Value` to its own trait inside the expansion.
    let thread_id = params
        .get("threadId")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    let turn_id = notification_turn_id(params).unwrap_or("");
    tracing::debug!(
        target: "nexus_harness_codex::wire",
        session = %session.0,
        method = %method,
        thread_id = %thread_id,
        turn_id = %turn_id,
        "codex notification"
    );
}

/// Extract the turn id from a server notification, tolerating both wire shapes:
/// `item/*` and `error` notifications carry a top-level `turnId`, while `turn/started`
/// and `turn/completed` nest it as `turn.id` (codex 0.144.x, ServerNotification.json).
/// Reading only the top-level field made every `turn/completed` invisible to the turn
/// tracker — codex deliveries then always burned the full completion timeout.
fn notification_turn_id(params: &Value) -> Option<&str> {
    params
        .get("turnId")
        .and_then(Value::as_str)
        .or_else(|| params.get("turn")?.get("id").and_then(Value::as_str))
}

fn mark_turn_complete_if_needed(tracker: &CodexTurnTracker, method: &str, params: &Value) {
    if method != method::TURN_COMPLETED && method != method::TURN_FAILED {
        return;
    }
    let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
        return;
    };
    let Some(turn_id) = notification_turn_id(params) else {
        return;
    };
    if method == method::TURN_FAILED {
        if turn_error_will_retry(params) {
            return;
        }
        if turn_error_is_structured_hold(params) {
            tracker.settle_failure(thread_id, turn_id, params.clone());
            return;
        }
    }
    tracker.settle_completion(thread_id, turn_id);
}

fn mark_delivery_receipt_if_needed(
    tracker: &CodexTurnTracker,
    params: &Value,
    event: &nexus_agent::StreamEvent,
) {
    if !matches!(
        event.kind,
        AgentUpdateKind::Text
            | AgentUpdateKind::Thinking
            | AgentUpdateKind::ToolCall
            | AgentUpdateKind::Plan
    ) {
        return;
    }
    let (Some(thread_id), Some(turn_id)) = (
        params.get("threadId").and_then(Value::as_str),
        notification_turn_id(params),
    ) else {
        return;
    };
    tracker.observe_delivery_receipt(thread_id, turn_id);
}

fn observe_turn_authority(tracker: &CodexTurnTracker, method: &str, params: &Value) {
    let (Some(thread_id), Some(turn_id)) = (
        params.get("threadId").and_then(Value::as_str),
        notification_turn_id(params),
    ) else {
        return;
    };
    if method == method::TURN_COMPLETED
        || (method == method::TURN_FAILED && !turn_error_will_retry(params))
    {
        // Routing truth follows the consumed native terminal, not the completion of display
        // writes. Receipt/waiter settlement remains after accepted events and ordered output.
        tracker.observe_terminal_turn(thread_id, turn_id);
    } else {
        tracker.observe_active_turn(thread_id, turn_id);
    }
}

async fn emit_accepted_if_turn_notification(
    tracker: &CodexTurnTracker,
    _method: &str,
    params: &Value,
) {
    let Some(turn_id) = notification_turn_id(params) else {
        return;
    };
    let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
        return;
    };
    tracker
        .emit_next_accepted_event_for_thread(thread_id, Some(turn_id))
        .await;
}

async fn suppress_accepted_user_input_echo(
    tracker: &CodexTurnTracker,
    method: &str,
    params: &Value,
    event: &nexus_agent::StreamEvent,
) -> bool {
    if method != method::ITEM_COMPLETED || event.kind != AgentUpdateKind::UserInput {
        return false;
    }
    let Some(item) = params.get("item") else {
        return false;
    };
    if item.get("type").and_then(Value::as_str) != Some("userMessage") {
        return false;
    }
    let (Some(thread_id), Some(turn_id)) = (
        params.get("threadId").and_then(Value::as_str),
        params.get("turnId").and_then(Value::as_str),
    ) else {
        return false;
    };
    // Projection sanitizes oversized user input to `[omitted]`, but internal delivery proof must
    // compare the exact native text Codex accepted. Comparing the projected text would make every
    // accepted batch above the projection ceiling wait until the delivery timeout and then false-
    // dead-letter even though the full input is already present in Codex's rollout.
    let text = user_message_text(item);
    if text.is_empty() {
        return false;
    }
    if tracker.take_cancelled_user_input_echo(thread_id, turn_id, &text) {
        return true;
    }
    if !tracker.take_accepted_user_input_echo(thread_id, turn_id, &text) {
        return false;
    }
    tracker
        .emit_native_user_input_accepted_event(thread_id, turn_id, &text)
        .await;
    tracker.observe_accepted_user_input_echo(thread_id, turn_id, &text);
    true
}
