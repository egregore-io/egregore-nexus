//! A hermetic **fake ACP agent** harness — a real subprocess that speaks the genuine
//! Agent Client Protocol wire format over stdio, built on the same `agent-client-protocol`
//! SDK the production adapters use. The integration tests in `tests/` spawn this binary in
//! place of a real `claude`/`codex` so the full `open → inject → stream` path is exercised
//! end to end **without a model and without network**.
//!
//! Protocol behaviour (deliberately tiny but spec-correct, and faithful to the real
//! codex-acp turn lifecycle):
//! - `initialize` — replies `InitializeResponse` advertising `load_session` so the resume path
//!   can be tested.
//! - `session/new` — replies a fixed `NewSessionResponse { session_id: "fake-session-1" }`.
//! - `session/load` — replies an empty `LoadSessionResponse` (resume handshake).
//! - `session/prompt` — reproduces the real agent turn lifecycle: it streams **several**
//!   `session/update` `AgentMessageChunk`s (partial text, one notification at a time, yielding
//!   between them so each is a distinct dispatch-loop iteration exactly like a live multi-chunk
//!   turn), interleaves an `AgentThoughtChunk` + a non-text `Plan` update (which the client must
//!   ignore, never mistake for content), and ONLY THEN replies
//!   `PromptResponse { stop_reason: EndTurn }`. The `PromptResponse`'s `StopReason` is the
//!   canonical ACP turn-end signal (the same one real codex-acp emits at `response.completed`);
//!   a client that does not key off it — e.g. one that drains its chunk buffer before the prompt
//!   request resolves — will either hang or surface an empty/partial reply. The reply text is
//!   selected by env var so the same harness exercises the inbound-relay path AND the OUTBOUND
//!   parse+route path:
//!     - default                       → `"echo: " + <prompt body>` (the original behaviour: lets a
//!       test assert the prompt was delivered *and* updates were relayed),
//!     - `FAKE_ACP_REPLY=envelope`     → an outbound envelope `"<nexus to=\"$FAKE_ACP_REPLY_TO\">"
//!       + $FAKE_ACP_REPLY_BODY + "</nexus>"` (default target `boss`, default body `pong`) — the
//!       daemon must parse this and `BusPort::send` it as a DM from the agent,
//!     - `FAKE_ACP_REPLY=bare`         → a bare `$FAKE_ACP_REPLY_BODY` (default `pong`) with NO
//!       envelope — the daemon must default-route it as a DM back to the turn's sender.
//!     - `FAKE_ACP_REPLY=coalesced`    → one `AgentMessageChunk` containing the entire reply body,
//!       matching real ACP bridges that coalesce a completed response.
//!     - `FAKE_ACP_REPLY=passthrough`  → the full renderable `session/update` stream, in order
//!       (`AvailableCommandsUpdate → AgentThoughtChunk → ToolCall → ToolCallUpdate(completed) →
//!       AgentMessageChunk → turn-end`), for the ACP pass-through e2e: the daemon must relay these
//!       as ordered tagged `agent.update` events (`Commands, Thinking, ToolCall, ToolCall, Text`).
//!
//!   Normal modes split the reply across **multiple** `AgentMessageChunk`s; `coalesced` deliberately
//!   emits one. Both are followed by the turn-end `PromptResponse`, covering provider framing on
//!   either side of Nexus's bounded text-delta normalization.
//!
//! Fault injection (drive the adapter's failure/liveness paths):
//! - `FAKE_ACP_FAIL_INIT=1` — make `initialize` never respond (the client's handshake then times
//!   out / sees the pipe close), which drives the adapter's init-failure path.
//! - `FAKE_ACP_FAIL_SESSION_NEW=1` — make `session/new` never respond after initialization.
//! - `FAKE_ACP_FAIL_SESSION_LOAD=1` — make `session/load` never respond after initialization.
//! - `FAKE_ACP_NO_TURN_END=1` — on `session/prompt`, stream the full turn's chunks as usual but
//!   then **never respond** to the prompt request (no `PromptResponse`/turn-end). This reproduces
//!   the live `codex-acp` failure mode where the model turn completes on the wire (chunks flow,
//!   the bridge goes idle) yet the turn-end signal never reaches the client. The engine must treat
//!   this as a bounded wait that errors out, NOT an unbounded hang that wedges the per-agent loop.
//! - `FAKE_ACP_NO_TURN_END_ONCE=<path>` — only the **first** turn omits the turn-end signal; every
//!   turn after it completes normally. Keyed off the marker file at `<path>` (created on the first
//!   prompt) so it is per-child and stable across this process's turns. Drives the **recovery**
//!   path: after the engine bounds + errors the wedged first turn and the per-agent loop re-parks,
//!   a subsequent turn must complete and route — proving the loop was not permanently wedged.
//! - `FAKE_ACP_PROMPT_ERROR_DATA=<json>` — return a structured ACP error from `session/prompt`
//!   before streaming. Adapter tests use this to prove provider-limit classification happens before
//!   `session/prompt failed: ...` stringification.
//! - `FAKE_ACP_PROMPT_ERROR_MESSAGE=<text>` — return an internal ACP error whose message is the
//!   exact supplied text. This models bridges that encode their diagnostic in `error.message`.
//! - `FAKE_ACP_PROMPT_ERROR_THEN_REPLY_MS=<milliseconds>` — when paired with
//!   `FAKE_ACP_PROMPT_ERROR_DATA`, return the scripted prompt error first, then emit the configured
//!   reply chunks after the delay. This reproduces a bridge handoff that appends the replacement
//!   prompt but answers the old request with a diagnostic before the replacement model turn emits.
//! - `FAKE_ACP_HERMES_QUEUE_PROMOTE_MS=<milliseconds>` — reproduce Hermes 0.17's busy-session
//!   behavior: answer the incoming prompt immediately with `EndTurn` plus a "Queued for the next
//!   turn" agent update, then later emit the exact prompt as a `UserMessageChunk` followed by a
//!   model reply. The immediate response is only an in-process queue acknowledgement and must not
//!   settle observed Nexus delivery before the promoted prompt reaches model context.
//! - `FAKE_ACP_USAGE=<input>,<output>` — attach end-turn token usage to the prompt response. This
//!   proves a silent harness turn can still carry provider-side evidence that input was consumed.
//! - `FAKE_ACP_SPAWN_CHILD=1` — spawn a long-lived child in the same process group. Engine kill
//!   tests use this to prove shutdown reaches the whole ACP process tree, not just this wrapper.
//! - `FAKE_ACP_CHILD_IGNORE_TERM=1` — make that child ignore `SIGTERM`. This exercises the
//!   process-group escalation path after the wrapper itself has already exited.

use agent_client_protocol::schema::v1::{
    AgentCapabilities, AvailableCommand, AvailableCommandsUpdate, ContentBlock, ContentChunk,
    InitializeRequest, InitializeResponse, LoadSessionRequest, LoadSessionResponse,
    NewSessionRequest, NewSessionResponse, Plan, PromptRequest, PromptResponse, SessionId,
    SessionNotification, SessionUpdate, StopReason, TextContent, ToolCall, ToolCallStatus,
    ToolCallUpdate, ToolCallUpdateFields, Usage,
};
use agent_client_protocol::{Agent, Client, ConnectionTo, Dispatch, Result};

const FAKE_SESSION_ID: &str = "fake-session-1";

fn end_turn_response() -> PromptResponse {
    let response = PromptResponse::new(StopReason::EndTurn);
    let Ok(raw) = std::env::var("FAKE_ACP_USAGE") else {
        return response;
    };
    let mut fields = raw.split(',');
    let input = fields
        .next()
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    let output = fields
        .next()
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    response.usage(Usage::new(input + output, input, output))
}

/// Pull the concatenated text out of a prompt's content blocks.
fn prompt_text(req: &PromptRequest) -> String {
    req.prompt
        .iter()
        .filter_map(|b| match b {
            ContentBlock::Text(t) => Some(t.text.clone()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("")
}

/// Build one `AgentMessageChunk` carrying `text` for `session_id` (the agent's streamed reply).
fn message_chunk(session_id: SessionId, text: &str) -> SessionNotification {
    SessionNotification::new(
        session_id,
        SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(TextContent::new(
            text,
        )))),
    )
}

/// Build one `AgentThoughtChunk` (internal reasoning) — a non-reply `session/update` the client
/// must NOT splice into the relayed answer. Real agents interleave these during a turn.
fn thought_chunk(session_id: SessionId, text: &str) -> SessionNotification {
    SessionNotification::new(
        session_id,
        SessionUpdate::AgentThoughtChunk(ContentChunk::new(ContentBlock::Text(TextContent::new(
            text,
        )))),
    )
}

/// Build a `Plan` `session/update` — another non-text variant the client must ignore. Its presence
/// mid-turn proves the turn-end detector keys off `StopReason`, not "any update arrived".
fn plan_update(session_id: SessionId) -> SessionNotification {
    SessionNotification::new(session_id, SessionUpdate::Plan(Plan::new(vec![])))
}

/// Build an `AvailableCommandsUpdate` advertising the harness's own slash commands — the discover
/// side of the pass-through. The daemon relays it as `agent.update` kind `commands`.
fn available_commands_update(session_id: SessionId) -> SessionNotification {
    SessionNotification::new(
        session_id,
        SessionUpdate::AvailableCommandsUpdate(AvailableCommandsUpdate::new(vec![
            AvailableCommand::new("compact", "Compact the conversation context"),
            AvailableCommand::new("clear", "Clear the conversation"),
        ])),
    )
}

/// Build the user-message echo Hermes emits when a prompt previously held in its private busy
/// queue is promoted into the next actual model turn.
fn user_message_chunk(session_id: SessionId, text: &str) -> SessionNotification {
    SessionNotification::new(
        session_id,
        SessionUpdate::UserMessageChunk(ContentChunk::new(ContentBlock::Text(TextContent::new(
            text,
        )))),
    )
}

/// Build a `ToolCall` `session/update` (a tool call starting) — relayed as `agent.update` kind
/// `tool_call`. Keyed by `tool_call_id` so the client can merge the later `ToolCallUpdate`.
fn tool_call_update(session_id: SessionId) -> SessionNotification {
    SessionNotification::new(
        session_id,
        SessionUpdate::ToolCall(
            ToolCall::new("tc_fake_1", "Read README.md").status(ToolCallStatus::InProgress),
        ),
    )
}

/// Build a `ToolCallUpdate` `session/update` (the same tool call, now completed) — relayed as a
/// second `agent.update` kind `tool_call` with the same id, which the UI merges in place.
fn tool_call_completed_update(session_id: SessionId) -> SessionNotification {
    SessionNotification::new(
        session_id,
        SessionUpdate::ToolCallUpdate(ToolCallUpdate::new(
            "tc_fake_1",
            ToolCallUpdateFields::new().status(ToolCallStatus::Completed),
        )),
    )
}

/// The scripted reply chunks for one turn, selected by the `FAKE_ACP_REPLY` env var (see the module
/// docs). Returned as a `Vec` of chunks so the harness streams the answer incrementally across
/// several `AgentMessageChunk`s — never as one clean blob.
fn scripted_reply(prompt_body: &str) -> Vec<String> {
    let body = std::env::var("FAKE_ACP_REPLY_BODY").unwrap_or_else(|_| "pong".to_string());
    match std::env::var("FAKE_ACP_REPLY").as_deref() {
        Ok("envelope") => {
            // An OUTBOUND `<nexus to="…">` envelope the daemon must parse + route as a DM.
            let to = std::env::var("FAKE_ACP_REPLY_TO").unwrap_or_else(|_| "boss".to_string());
            // Split across two chunks to prove the daemon reassembles the stream before parsing.
            vec![format!("<nexus to=\"{to}\">"), format!("{body}</nexus>")]
        }
        Ok("bare") => {
            // A bare reply (no envelope) → the daemon must default-route it to the turn's sender.
            // Still split into multiple chunks so the bare-reply path also reassembles a stream.
            split_into_chunks(&body)
        }
        Ok("coalesced") => vec![body],
        // Default: echo the prompt back (original behaviour; inbound-relay assertion).
        _ => vec!["echo: ".to_string(), prompt_body.to_string()],
    }
}

/// Split `text` into at most three non-empty chunks so a single-token reply still streams across
/// multiple `AgentMessageChunk`s. A real turn rarely arrives as one chunk; modelling that keeps the
/// client's stream-reassembly + turn-end detection honest.
fn split_into_chunks(text: &str) -> Vec<String> {
    if text.is_empty() {
        return vec![String::new()];
    }
    let chars: Vec<char> = text.chars().collect();
    let parts = chars.len().min(3).max(1);
    let per = chars.len().div_ceil(parts);
    chars
        .chunks(per)
        .map(|c| c.iter().collect::<String>())
        .collect()
}

#[tokio::main]
async fn main() -> Result<()> {
    use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
    let _spawned_child = if std::env::var("FAKE_ACP_SPAWN_CHILD").is_ok() {
        let mut command = if std::env::var("FAKE_ACP_CHILD_IGNORE_TERM").is_ok() {
            let mut command = std::process::Command::new("sh");
            command.args(["-c", "trap '' TERM; exec sleep 60"]);
            command
        } else {
            let mut command = std::process::Command::new("sleep");
            command.arg("60");
            command
        };
        command
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .ok()
    } else {
        None
    };

    // Byte transport over our own stdin/stdout — the parent adapter is the ACP client.
    let transport = agent_client_protocol::ByteStreams::new(
        tokio::io::stdout().compat_write(),
        tokio::io::stdin().compat(),
    );

    Agent
        .builder()
        .name("fake-acp-agent")
        .on_receive_request(
            async move |initialize: InitializeRequest, responder, _cx| {
                if std::env::var("FAKE_ACP_FAIL_INIT").is_ok() {
                    // Never respond: leave initialize unanswered so the client times out.
                    return Ok(());
                }
                let mut caps = AgentCapabilities::new();
                caps.load_session = true; // advertise resume support for the resume test
                responder.respond(
                    InitializeResponse::new(initialize.protocol_version).agent_capabilities(caps),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |_req: NewSessionRequest, responder, _cx| {
                if std::env::var("FAKE_ACP_FAIL_SESSION_NEW").is_ok() {
                    return Ok(());
                }
                responder.respond(NewSessionResponse::new(FAKE_SESSION_ID))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |req: LoadSessionRequest, responder, cx: ConnectionTo<Client>| {
                if std::env::var("FAKE_ACP_FAIL_SESSION_LOAD").is_ok() {
                    return Ok(());
                }
                // Fault injection: a harness whose stored resume key is stale (the live boot
                // reality after a daemon/process restart — the ACP session is gone) answers
                // `session/load` with an error ("Resource not found"). The engine must fall back to
                // a fresh `session/new` ON THE SAME, still-live connection. `session/new` keeps
                // working, so the fallback yields a usable session.
                if std::env::var("FAKE_ACP_FAIL_LOAD").is_ok() {
                    let _ = responder; // error is returned, not responded
                    return Err(agent_client_protocol::util::internal_error(
                        "Resource not found",
                    ));
                }
                let replay = std::env::var("FAKE_ACP_LOAD_REPLAY_BODY")
                    .ok()
                    .filter(|body| !body.is_empty());
                let replay_delay_ms = std::env::var("FAKE_ACP_LOAD_REPLAY_DELAY_MS")
                    .ok()
                    .and_then(|raw| raw.parse::<u64>().ok())
                    .unwrap_or(25);
                let session_id = req.session_id.clone();
                let result = responder.respond(LoadSessionResponse::new());
                if let Some(body) = replay {
                    tokio::spawn(async move {
                        tokio::time::sleep(std::time::Duration::from_millis(replay_delay_ms)).await;
                        let _ = cx.send_notification(message_chunk(session_id, &body));
                    });
                }
                result
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |req: PromptRequest, responder, cx: ConnectionTo<Client>| {
                let session_id = req.session_id.clone();
                let body = prompt_text(&req);
                if let Some(delay_ms) = std::env::var("FAKE_ACP_PROMPT_DELAY_MS")
                    .ok()
                    .and_then(|raw| raw.parse::<u64>().ok())
                {
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                }
                let prompt_error = std::env::var("FAKE_ACP_PROMPT_ERROR_MESSAGE")
                    .ok()
                    .map(|message| agent_client_protocol::Error::new(-32603, message))
                    .or_else(|| {
                        std::env::var("FAKE_ACP_PROMPT_ERROR_DATA").ok().map(|raw| {
                            let data = serde_json::from_str::<serde_json::Value>(&raw)
                                .unwrap_or_else(|_| serde_json::Value::String(raw));
                            agent_client_protocol::Error::internal_error().data(data)
                        })
                    });
                if let Some(error) = prompt_error {
                    let result = responder.respond_with_error(error);
                    if let Some(delay_ms) = std::env::var("FAKE_ACP_PROMPT_ERROR_THEN_REPLY_MS")
                        .ok()
                        .and_then(|raw| raw.parse::<u64>().ok())
                    {
                        let reply_chunks: Vec<String> = scripted_reply(&body)
                            .into_iter()
                            .filter(|chunk| !chunk.is_empty())
                            .collect();
                        tokio::spawn(async move {
                            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                            for chunk in reply_chunks {
                                let _ =
                                    cx.send_notification(message_chunk(session_id.clone(), &chunk));
                                tokio::task::yield_now().await;
                            }
                        });
                    }
                    return result;
                }

                if std::env::var("FAKE_ACP_PROMPT_ERROR").as_deref()
                    == Ok("opencode_provider_limit")
                {
                    return Err(agent_client_protocol::Error::internal_error().data(
                        serde_json::json!({
                            "providerError": {
                                "harness": "opencode",
                                "reason": "rate_limit",
                                "retryAfterMs": 1200,
                                "provider": "openrouter",
                                "model": "free-model"
                            }
                        }),
                    ));
                }

                if let Some(promote_after_ms) = std::env::var("FAKE_ACP_HERMES_QUEUE_PROMOTE_MS")
                    .ok()
                    .and_then(|raw| raw.parse::<u64>().ok())
                {
                    // Hermes 0.17 returns this response while the prompt exists only in its own
                    // process-local queue. If the daemon restarts now, that queue disappears. The
                    // later UserMessageChunk is the first causal signal that Hermes promoted the
                    // exact prompt into a new model turn.
                    let _ = cx.send_notification(message_chunk(
                        session_id.clone(),
                        "Queued for the next turn. (1 queued)",
                    ));
                    let result = responder.respond(end_turn_response());
                    tokio::spawn(async move {
                        tokio::time::sleep(std::time::Duration::from_millis(promote_after_ms))
                            .await;
                        let _ = cx.send_notification(user_message_chunk(session_id.clone(), &body));
                        tokio::task::yield_now().await;
                        let _ = cx
                            .send_notification(message_chunk(session_id, "promoted Hermes reply"));
                    });
                    return result;
                }

                // Pass-through e2e mode (backend plan 50, Task 9): stream the FULL renderable
                // session/update stream, in order, exactly as a real harness would over a turn —
                //   AvailableCommandsUpdate → AgentThoughtChunk → ToolCall → ToolCallUpdate(completed)
                //   → AgentMessageChunk → turn-end.
                // The daemon must relay these as ordered `agent.update` events
                // (Commands, Thinking, ToolCall, ToolCall(updated), Text). Yield between each so they
                // land as distinct dispatch-loop iterations like a live multi-event turn.
                if std::env::var("FAKE_ACP_REPLY").as_deref() == Ok("passthrough") {
                    let _ = cx.send_notification(available_commands_update(session_id.clone()));
                    tokio::task::yield_now().await;
                    let _ = cx.send_notification(thought_chunk(
                        session_id.clone(),
                        "considering the request",
                    ));
                    tokio::task::yield_now().await;
                    let _ = cx.send_notification(tool_call_update(session_id.clone()));
                    tokio::task::yield_now().await;
                    let _ = cx.send_notification(tool_call_completed_update(session_id.clone()));
                    tokio::task::yield_now().await;
                    let _ = cx.send_notification(message_chunk(session_id.clone(), "all done"));
                    tokio::task::yield_now().await;
                    return responder.respond(end_turn_response());
                }

                // Reproduce a real, multi-chunk model turn:
                //  1. interleave a non-reply thought + plan update (must be ignored by the client),
                //  2. stream the (env-selected) answer across SEVERAL AgentMessageChunks, yielding
                //     between each so they land as distinct dispatch-loop iterations,
                //  3. ONLY THEN respond with the PromptResponse — whose StopReason is the canonical
                //     ACP turn-end. A client that drains its buffer before this response resolves
                //     (instead of keying off the StopReason) will hang or relay a partial/empty reply.
                //
                // An EMPTY reply body must produce a turn that streams **nothing renderable at all**
                // — no chunks, and (under the full-stream pass-through, where thinking/plan ARE
                // renderable stream events) no decorative thought/plan either — so the no-content
                // liveness path stays exercisable: a turn with zero stream activity that never ends
                // must hit the hard ceiling, not be mistaken for a quiescent (content-then-idle)
                // turn. So the thought + plan are sent only when there is actual reply content.
                let reply_chunks: Vec<String> = scripted_reply(&body)
                    .into_iter()
                    .filter(|c| !c.is_empty())
                    .collect();
                let has_content = !reply_chunks.is_empty();

                if has_content {
                    let _ = cx.send_notification(thought_chunk(session_id.clone(), "(thinking…)"));
                    tokio::task::yield_now().await;
                }

                for chunk in &reply_chunks {
                    let _ = cx.send_notification(message_chunk(session_id.clone(), chunk));
                    // Yield so the client's dispatch loop processes each chunk separately, exactly
                    // as it would for a real turn streamed over the wire.
                    tokio::task::yield_now().await;
                }

                if has_content {
                    let _ = cx.send_notification(plan_update(session_id.clone()));
                    tokio::task::yield_now().await;
                }

                // Fault injection: a harness that STREAMS a full turn's chunks but never delivers
                // the turn-end signal (no `PromptResponse`). This reproduces the live `codex-acp`
                // failure mode — the model turn looks complete on the wire (chunks flowed) yet the
                // `session/prompt` request is left open forever. A client that keys turn-end off the
                // prompt response (correct) must time out and surface an error; a client with no
                // bound on that wait hangs the turn — and the per-agent loop — indefinitely.
                if std::env::var("FAKE_ACP_NO_TURN_END").is_ok() {
                    return Ok(());
                }

                // Turn-end: the PromptResponse carrying StopReason::EndTurn. This is what real
                // codex-acp sends at `response.completed`; it is the signal the client MUST use to
                // know the turn is over.
                responder.respond(end_turn_response())
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: Dispatch, cx: ConnectionTo<Client>| {
                message.respond_with_error(
                    agent_client_protocol::util::internal_error(
                        "fake-acp-agent: unhandled message",
                    ),
                    cx,
                )
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
}
