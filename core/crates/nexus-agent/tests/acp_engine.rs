//! Hermetic integration tests for the real ACP engine and built-in adapters.
//!
//! These spawn the **fake ACP agent harness** (`src/bin/fake_acp_agent.rs`, located via the
//! Cargo-provided `CARGO_BIN_EXE_fake_acp_agent`) — a real subprocess speaking the genuine ACP
//! wire protocol over stdio — so the full `open → inject → stream` path is exercised end to end
//! without a real model and without network. They pass by default (`cargo test -p nexus-agent`);
//! real harness spawns are covered separately and skipped when those binaries are absent.

use std::sync::Arc;

use nexus_agent::adapter::engine::{AcpEngine, HarnessCommand, LaunchCtx};
use nexus_agent::adapter::{
    Adapter, AdapterInjectError, HermesAdapter, OpenCodeAdapter, StreamEvent,
};
use nexus_contracts::{AgentUpdateKind, HarnessId, InjectError, ProviderLimitReason, SessionId};
use serde_json::json;

/// The compiled fake harness binary. `CARGO_BIN_EXE_<name>` is injected by Cargo because the
/// harness is a `[[bin]]` of this crate, so it is always built before these tests run.
const FAKE_HARNESS: &str = env!("CARGO_BIN_EXE_fake_acp_agent");
const CLAUDE_INTERRUPTED_HANDOFF: &str =
    "[ede_diagnostic] result_type=user last_content_type=n/a stop_reason=null";

/// Concatenate the reply text from a drained stream — i.e. the `text` of every `Text`-kind event,
/// in order. The full-stream pass-through also relays thinking / tool-call / plan events; the
/// **reply** is just the `Text` ones, so these turn-content assertions filter to those.
fn reply_text(events: Vec<StreamEvent>) -> String {
    events
        .into_iter()
        .filter(|e| e.kind == AgentUpdateKind::Text)
        .filter_map(|e| {
            e.data
                .get("text")
                .and_then(|t| t.as_str())
                .map(str::to_owned)
        })
        .collect()
}

/// The ordered list of `text` chunks from the `Text`-kind events of a drained stream (for asserting
/// the reply streamed as distinct chunks).
fn reply_chunks(events: Vec<StreamEvent>) -> Vec<String> {
    events
        .into_iter()
        .filter(|e| e.kind == AgentUpdateKind::Text)
        .filter_map(|e| {
            e.data
                .get("text")
                .and_then(|t| t.as_str())
                .map(str::to_owned)
        })
        .collect()
}

/// A [`HarnessCommand`] that launches the fake ACP agent (optionally with `FAKE_ACP_FAIL_INIT`
/// injected by the caller via env, since `HarnessCommand` carries no env of its own).
fn fake_command() -> HarnessCommand {
    HarnessCommand {
        program: FAKE_HARNESS.to_string(),
        args: vec![],
        cwd: None,
        ..Default::default()
    }
}

fn fake_prompt_error_command(data: serde_json::Value) -> HarnessCommand {
    HarnessCommand {
        program: FAKE_HARNESS.to_string(),
        args: vec![],
        cwd: None,
        env: vec![("FAKE_ACP_PROMPT_ERROR_DATA".to_string(), data.to_string())],
    }
}

fn fake_prompt_error_message_command(message: &str) -> HarnessCommand {
    HarnessCommand {
        program: FAKE_HARNESS.to_string(),
        args: vec![],
        cwd: None,
        env: vec![(
            "FAKE_ACP_PROMPT_ERROR_MESSAGE".to_string(),
            message.to_string(),
        )],
    }
}

#[cfg(target_os = "linux")]
fn process_group_of(pid: u32) -> Option<u32> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let close = stat.rfind(')')?;
    let fields = stat[close + 1..].split_whitespace().collect::<Vec<_>>();
    fields.get(2)?.parse::<u32>().ok()
}

#[cfg(target_os = "linux")]
fn process_group_members(pgrp: u32) -> Vec<u32> {
    let mut members = Vec::new();
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return members;
    };
    for entry in entries.flatten() {
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        if process_group_of(pid) == Some(pgrp) {
            members.push(pid);
        }
    }
    members.sort_unstable();
    members
}

#[tokio::test]
async fn engine_open_inject_stream_round_trip() {
    let engine = AcpEngine::new();
    engine
        .spawn_and_initialize(&fake_command())
        .await
        .expect("spawn + ACP initialize against the fake harness");
    assert!(
        engine.is_connected().await,
        "engine must be connected after initialize"
    );

    engine
        .new_session(None, &LaunchCtx::default())
        .await
        .expect("session/new");

    // The fake echoes the prompt back as two AgentMessageChunks: "echo: " then the body.
    engine
        .inject("ping from nexus".to_string())
        .await
        .expect("session/prompt turn completes");

    let chunks = reply_chunks(engine.take_updates());
    assert_eq!(
        chunks,
        vec!["echo: ".to_string(), "ping from nexus".to_string()],
        "the streamed reply chunks must be relayed in order"
    );

    // A second turn re-uses the same session and gets a fresh buffer.
    engine
        .inject("second turn".to_string())
        .await
        .expect("second prompt");
    let chunks2 = reply_chunks(engine.take_updates());
    assert_eq!(
        chunks2,
        vec!["echo: ".to_string(), "second turn".to_string()]
    );
}

#[tokio::test]
async fn session_new_timeout_is_terminal_and_reaps_the_harness() {
    let engine = AcpEngine::new().with_session_open_timeout(std::time::Duration::from_millis(100));
    let mut cmd = fake_command();
    cmd.env
        .push(("FAKE_ACP_FAIL_SESSION_NEW".to_string(), "1".to_string()));
    engine
        .spawn_and_initialize(&cmd)
        .await
        .expect("initialize succeeds before session/new stalls");
    let pid = engine.spawned_pid().expect("fake harness pid");

    let error = engine
        .new_session(None, &LaunchCtx::default())
        .await
        .expect_err("a silent session/new must terminate within the configured bound");

    assert!(
        error.to_string().contains("session/new timed out"),
        "timeout must identify the stuck ACP lifecycle boundary: {error}"
    );
    #[cfg(target_os = "linux")]
    assert!(
        !std::path::Path::new(&format!("/proc/{pid}")).exists(),
        "a timed-out session/new must not retain its ACP process"
    );
}

#[tokio::test]
async fn session_load_timeout_is_terminal_and_reaps_the_harness() {
    let engine = AcpEngine::new().with_session_open_timeout(std::time::Duration::from_millis(100));
    let mut cmd = fake_command();
    cmd.env
        .push(("FAKE_ACP_FAIL_SESSION_LOAD".to_string(), "1".to_string()));
    engine
        .spawn_and_initialize(&cmd)
        .await
        .expect("initialize succeeds before session/load stalls");
    let pid = engine.spawned_pid().expect("fake harness pid");

    let error = engine
        .load_session("stale-resume-key", None, &LaunchCtx::default())
        .await
        .expect_err("a silent session/load must terminate within the configured bound");

    assert!(
        error.to_string().contains("session/load timed out"),
        "timeout must identify the stuck ACP lifecycle boundary: {error}"
    );
    #[cfg(target_os = "linux")]
    assert!(
        !std::path::Path::new(&format!("/proc/{pid}")).exists(),
        "a timed-out session/load must not retain its ACP process"
    );
}

#[tokio::test]
async fn opencode_adapter_full_turn_over_acp() {
    // Drive the real OpenCodeAdapter (its ACP engine) against the fake harness — the full
    // Adapter contract over real ACP, no `opencode` binary required.
    let adapter = OpenCodeAdapter::with_command(fake_command());
    adapter
        .open_session()
        .await
        .expect("open_session (spawn + initialize + session/new)");

    adapter
        .inject("ping from nexus".to_string())
        .await
        .expect("inject one session/prompt turn");

    let chunks = reply_chunks(
        adapter
            .stream_updates()
            .await
            .expect("stream_updates drains the reply"),
    );
    assert_eq!(
        chunks,
        vec!["echo: ".to_string(), "ping from nexus".to_string()]
    );
}

#[tokio::test]
async fn opencode_adapter_maps_structured_acp_provider_limit_before_contract_collapse() {
    let adapter = OpenCodeAdapter::with_command(fake_prompt_error_command(json!({
        "providerError": {
            "reason": "rate_limit",
            "retryAfterMs": 1_200,
            "provider": "openrouter",
            "model": "free-model"
        }
    })));
    adapter
        .open_session()
        .await
        .expect("open_session (spawn + initialize + session/new)");

    let err = adapter
        .inject("trip structured provider limit".to_string())
        .await
        .expect_err("structured ACP provider error must not collapse into a generic adapter error");

    let AdapterInjectError::ProviderLimit(limit) = err else {
        panic!("expected ProviderLimit, got {err:?}");
    };
    assert_eq!(limit.harness, HarnessId::new("opencode").unwrap());
    assert_eq!(limit.reason, ProviderLimitReason::RateLimit);
    assert_eq!(limit.provider.as_deref(), Some("openrouter"));
    assert_eq!(limit.model.as_deref(), Some("free-model"));
    assert_eq!(limit.source, "opencode.acp.prompt_error");
    assert!(limit.reset_hint.is_some());
}

#[tokio::test]
async fn opencode_adapter_does_not_classify_visible_rate_limit_text() {
    let adapter = OpenCodeAdapter::with_command(HarnessCommand {
        env: vec![
            ("FAKE_ACP_REPLY".to_string(), "bare".to_string()),
            (
                "FAKE_ACP_REPLY_BODY".to_string(),
                "the visible text says rate limit but carries no structured error".to_string(),
            ),
        ],
        ..fake_command()
    });
    adapter
        .open_session()
        .await
        .expect("open_session (spawn + initialize + session/new)");

    adapter
        .inject("normal visible reply".to_string())
        .await
        .expect("visible text alone must not trip the provider-limit breaker");

    let reply = reply_text(
        adapter
            .stream_updates()
            .await
            .expect("stream_updates drains the reply"),
    );
    assert_eq!(
        reply,
        "the visible text says rate limit but carries no structured error"
    );
}

#[tokio::test]
async fn hermes_adapter_full_turn_over_acp() {
    let adapter = HermesAdapter::with_command(fake_command());
    adapter
        .open_session()
        .await
        .expect("open_session (spawn + initialize + session/new)");
    adapter
        .inject("ping from nexus".to_string())
        .await
        .expect("inject one session/prompt turn");
    let chunks = reply_chunks(
        adapter
            .stream_updates()
            .await
            .expect("stream_updates drains the reply"),
    );
    assert_eq!(
        chunks,
        vec!["echo: ".to_string(), "ping from nexus".to_string()]
    );
}

#[tokio::test]
async fn engine_classifies_structured_acp_provider_limit_before_string_collapse() {
    let engine = AcpEngine::for_harness(HarnessId::new("claude").unwrap());
    let command = fake_prompt_error_command(json!({
        "error": "rate_limit",
        "error_details": { "retry_after_ms": 5_000 },
        "provider": "anthropic",
        "model": "claude-opus"
    }));
    engine
        .spawn_and_initialize(&command)
        .await
        .expect("spawn + initialize");
    engine
        .new_session(None, &LaunchCtx::default())
        .await
        .expect("session/new");

    let err = engine
        .inject("trigger structured limit".to_string())
        .await
        .expect_err("structured ACP error must classify before string collapse");
    let AdapterInjectError::ProviderLimit(limit) = err else {
        panic!("expected provider limit, got {err:?}");
    };
    assert_eq!(limit.harness, HarnessId::new("claude").unwrap());
    assert_eq!(limit.reason, ProviderLimitReason::RateLimit);
    assert_eq!(limit.provider.as_deref(), Some("anthropic"));
    assert_eq!(limit.model.as_deref(), Some("claude-opus"));
    assert_eq!(limit.source, "claude.acp.prompt_error");
    assert!(
        limit.reset_hint.is_some(),
        "retry_after_ms must become an absolute reset hint"
    );
}

#[tokio::test]
async fn engine_preserves_structured_acp_server_error_as_retryable_provider_failure() {
    let engine = AcpEngine::for_harness(HarnessId::new("claude").unwrap());
    let command = fake_prompt_error_command(json!({
        "errorKind": "server_error"
    }));
    engine
        .spawn_and_initialize(&command)
        .await
        .expect("spawn + initialize");
    engine
        .new_session(None, &LaunchCtx::default())
        .await
        .expect("session/new");

    let err = engine
        .inject("trigger transient server failure".to_string())
        .await
        .expect_err("structured server error must not collapse into a generic contract error");
    let AdapterInjectError::ProviderError(error) = err else {
        panic!("expected provider error, got {err:?}");
    };
    assert_eq!(error.harness, HarnessId::new("claude").unwrap());
    assert_eq!(error.reason, "server_error");
    assert_eq!(error.source, "claude.acp.prompt_error");
    assert!(error.retryable);
}

/// Claude's ACP bridge can append an interrupt-and-send replacement prompt, return this exact
/// bridge diagnostic for the cancelled request, and then stream the replacement model turn. The
/// durable delivery must wait for that causal model output instead of recording an error or
/// retrying the prompt that is already present in Claude's transcript.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn claude_observed_interrupted_handoff_settles_only_after_model_output() {
    let engine = AcpEngine::for_harness(HarnessId::new("claude").unwrap());
    let mut command =
        fake_prompt_error_message_command(&format!("Internal error: {CLAUDE_INTERRUPTED_HANDOFF}"));
    command.env.extend([
        (
            "FAKE_ACP_PROMPT_ERROR_THEN_REPLY_MS".to_string(),
            "50".to_string(),
        ),
        ("FAKE_ACP_REPLY".to_string(), "bare".to_string()),
        ("FAKE_ACP_REPLY_BODY".to_string(), "r".to_string()),
    ]);
    engine
        .spawn_and_initialize(&command)
        .await
        .expect("spawn + initialize");
    engine
        .new_session(None, &LaunchCtx::default())
        .await
        .expect("session/new");

    tokio::time::timeout(
        std::time::Duration::from_secs(12),
        engine.inject_with_accepted_event(
            "interrupt-and-send replacement".to_string(),
            Some(StreamEvent {
                kind: AgentUpdateKind::UserInput,
                data: json!({"text": "accepted input"}),
            }),
        ),
    )
    .await
    .expect("Claude replacement handoff must settle after model output")
    .expect("causal replacement model output proves recipient delivery");

    assert_eq!(
        reply_text(engine.take_updates()),
        "r",
        "the replacement output remains available to the relay"
    );
}

/// The same diagnostic without a subsequent model event is not delivery evidence. Keep the
/// obligation pending for the engine's existing completion timeout; never accept the diagnostic
/// itself as success.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn claude_observed_interrupted_handoff_without_model_output_stays_unsettled() {
    let engine = AcpEngine::for_harness(HarnessId::new("claude").unwrap());
    let command = fake_prompt_error_command(json!(CLAUDE_INTERRUPTED_HANDOFF));
    engine
        .spawn_and_initialize(&command)
        .await
        .expect("spawn + initialize");
    engine
        .new_session(None, &LaunchCtx::default())
        .await
        .expect("session/new");

    let mut delivery = Box::pin(engine.inject_with_accepted_event(
        "replacement with no model output".to_string(),
        Some(StreamEvent::text("accepted input")),
    ));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(250), &mut delivery)
            .await
            .is_err(),
        "the bridge diagnostic alone must not settle or fail the durable delivery"
    );
}

#[tokio::test]
async fn hermes_adapter_classifies_structured_acp_provider_limit() {
    let adapter = HermesAdapter::with_command(fake_prompt_error_command(json!({
        "failureReason": "quotaExceeded",
        "provider": "openai",
        "model": "gpt-5"
    })));
    adapter
        .open_session()
        .await
        .expect("open hermes fake session");

    let err = adapter
        .inject("trigger hermes limit".to_string())
        .await
        .expect_err("structured Hermes ACP error must classify");
    let AdapterInjectError::ProviderLimit(limit) = err else {
        panic!("expected provider limit, got {err:?}");
    };
    assert_eq!(limit.harness, HarnessId::new("hermes").unwrap());
    assert_eq!(limit.reason, ProviderLimitReason::QuotaExhausted);
    assert_eq!(limit.provider.as_deref(), Some("openai"));
    assert_eq!(limit.model.as_deref(), Some("gpt-5"));
    assert_eq!(limit.source, "hermes.acp.prompt_error");
}

#[tokio::test]
async fn hermes_observed_empty_completion_fails_closed() {
    let adapter = HermesAdapter::with_command(HarnessCommand {
        env: vec![
            ("FAKE_ACP_REPLY".to_string(), "bare".to_string()),
            ("FAKE_ACP_REPLY_BODY".to_string(), String::new()),
        ],
        ..fake_command()
    });
    adapter
        .open_session()
        .await
        .expect("open hermes fake session");

    let err = adapter
        .inject_with_accepted_event(
            "prompt that the provider never processed".to_string(),
            Some(StreamEvent {
                kind: AgentUpdateKind::UserInput,
                data: json!({"text": "accepted by ACP"}),
            }),
        )
        .await
        .expect_err("an empty Hermes ACP EndTurn is not model-completion evidence");

    let AdapterInjectError::OperatorAction(action) = err else {
        panic!("expected operator-action settlement, got {err:?}");
    };
    assert_eq!(action.harness, HarnessId::new("hermes").unwrap());
    assert_eq!(action.source, "hermes.acp.empty_completion");
}

#[tokio::test]
async fn hermes_observed_empty_completion_with_input_usage_is_delivered() {
    let adapter = HermesAdapter::with_command(HarnessCommand {
        env: vec![
            ("FAKE_ACP_REPLY".to_string(), "bare".to_string()),
            ("FAKE_ACP_REPLY_BODY".to_string(), String::new()),
            ("FAKE_ACP_USAGE".to_string(), "17,0".to_string()),
        ],
        ..fake_command()
    });
    adapter
        .open_session()
        .await
        .expect("open hermes fake session");

    adapter
        .inject_with_accepted_event(
            "prompt processed without visible prose".to_string(),
            Some(StreamEvent {
                kind: AgentUpdateKind::UserInput,
                data: json!({"text": "accepted by ACP"}),
            }),
        )
        .await
        .expect("provider input usage proves the empty turn reached the model context");
}

#[tokio::test]
async fn init_failure_surfaces_error() {
    // FAKE_ACP_FAIL_INIT makes the harness never answer `initialize`; the engine must surface a
    // (timed-out / disconnected) error rather than hang or silently succeed. We shorten the wait
    // by killing via a child that exits: the harness leaves initialize unanswered and the pipe
    // stays open, so we rely on the engine's own timeout — but to keep the test fast we instead
    // point at a program that exits immediately, which closes the pipe and trips the
    // "closed before initialize" branch deterministically.
    let engine = AcpEngine::new();
    let cmd = HarnessCommand {
        // `true` exits 0 immediately → stdin/stdout close → initialize can never complete.
        program: "true".to_string(),
        args: vec![],
        cwd: None,
        ..Default::default()
    };
    let err = engine
        .spawn_and_initialize(&cmd)
        .await
        .expect_err("a harness that exits before initialize must surface an error");
    let msg = format!("{err}");
    assert!(
        msg.contains("initialize") || msg.contains("closed") || msg.contains("adapter"),
        "error should describe the failed handshake, got: {msg}"
    );
    assert!(
        !engine.is_connected().await,
        "no live connection after a failed init"
    );
}

#[tokio::test]
async fn spawn_failure_when_program_missing() {
    // A non-existent program must produce a spawn error, not a panic.
    let engine = AcpEngine::new();
    let cmd = HarnessCommand {
        program: "/nonexistent/nexus-no-such-harness".to_string(),
        args: vec![],
        cwd: None,
        ..Default::default()
    };
    let err = engine
        .spawn_and_initialize(&cmd)
        .await
        .expect_err("spawn must fail");
    assert!(
        format!("{err}").contains("spawn"),
        "error should mention spawn: {err}"
    );
}

/// Reproduction + fix of the live `codex-acp` turn-end bug at the engine layer.
///
/// The fake streams a full turn's `AgentMessageChunk`s and then **never** sends the turn-end
/// `PromptResponse` (`FAKE_ACP_NO_TURN_END`) — exactly the live failure: the reply flows, the
/// bridge goes idle, but the canonical turn-end signal (the prompt response's `StopReason`) never
/// arrives. The engine must NOT block forever on that response: once the reply has streamed and the
/// stream goes quiet for the quiescence window, `inject` infers turn-end, returns, and the buffered
/// chunks are drainable.
///
/// Fail-before/pass-after: before the fix, `inject`'s `block_task().await` waited for a prompt
/// response that never came, so it hung — the outer `tokio::time::timeout` below would fire and the
/// `take_updates` assertion would never run. After the fix, `inject` returns via quiescence and the
/// streamed reply is intact.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn engine_inject_completes_via_quiescence_when_prompt_response_never_arrives() {
    // Short quiescence window so the idle-detection path resolves quickly. Process-global, but no
    // other engine test sets it and they all complete in milliseconds.
    std::env::set_var("NEXUS_ACP_QUIESCENCE_MS", "400");

    let engine = AcpEngine::new();
    let cmd = HarnessCommand {
        program: FAKE_HARNESS.to_string(),
        args: vec![],
        cwd: None,
        // Per-child so this never races other tests on the process-global env. Stream a known reply
        // body across chunks, then never send the turn-end response.
        env: vec![
            ("FAKE_ACP_NO_TURN_END".to_string(), "1".to_string()),
            ("FAKE_ACP_REPLY".to_string(), "bare".to_string()),
            (
                "FAKE_ACP_REPLY_BODY".to_string(),
                "quiescent-reply".to_string(),
            ),
        ],
    };
    engine
        .spawn_and_initialize(&cmd)
        .await
        .expect("spawn + initialize");
    engine
        .new_session(None, &LaunchCtx::default())
        .await
        .expect("session/new");

    // `inject` must RETURN (via quiescence), not hang. Outer bound catches a regression to the old
    // unbounded `block_task` wait (which never returns when the prompt response never comes).
    tokio::time::timeout(
        std::time::Duration::from_secs(15),
        engine.inject("stream then go idle".to_string()),
    )
    .await
    .expect("inject must RETURN via stream-quiescence, not hang waiting for a turn-end that never comes")
    .expect("a streamed-then-idle turn is a completed turn (quiescence turn-end), not an error");

    // The streamed reply must be intact and drainable.
    let reply = reply_text(engine.take_updates());
    assert_eq!(
        reply, "quiescent-reply",
        "the reply streamed before the (missing) turn-end must be captured and relayed in full"
    );

    std::env::remove_var("NEXUS_ACP_QUIESCENCE_MS");
}

/// Bus delivery uses the observed injection seam and therefore requires an authoritative terminal
/// ACP response. Streamed content followed by silence proves only that bytes arrived; it must not
/// settle the durable delivery when the harness never returns `PromptResponse`/`StopReason`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn observed_engine_inject_rejects_quiescence_without_terminal_response() {
    std::env::set_var("NEXUS_ACP_TURN_TIMEOUT_SECS", "1");
    std::env::set_var("NEXUS_ACP_QUIESCENCE_MS", "100");

    let engine = AcpEngine::for_harness(HarnessId::new("opencode").unwrap());
    let cmd = HarnessCommand {
        program: FAKE_HARNESS.to_string(),
        args: vec![],
        cwd: None,
        env: vec![
            ("FAKE_ACP_NO_TURN_END".to_string(), "1".to_string()),
            ("FAKE_ACP_REPLY".to_string(), "bare".to_string()),
            (
                "FAKE_ACP_REPLY_BODY".to_string(),
                "content-is-not-terminal-proof".to_string(),
            ),
        ],
    };
    engine
        .spawn_and_initialize(&cmd)
        .await
        .expect("spawn + initialize");
    engine
        .new_session(None, &LaunchCtx::default())
        .await
        .expect("session/new");

    let error = engine
        .inject_with_accepted_event(
            "strict observed turn".to_string(),
            Some(StreamEvent::text("accepted input")),
        )
        .await
        .expect_err("observed bus delivery must require PromptResponse/StopReason");
    let error = error.into_inject_error(&SessionId("s_strict_acp".into()));
    assert!(
        matches!(error, InjectError::CompletionTimeout { ref source, .. } if source == "opencode.acp.turn_completion"),
        "missing terminal response must remain typed completion_timeout, got {error:?}"
    );

    std::env::remove_var("NEXUS_ACP_TURN_TIMEOUT_SECS");
    std::env::remove_var("NEXUS_ACP_QUIESCENCE_MS");
}

/// Hermes 0.17 can render a real assistant response and then leave `session/prompt` unanswered.
/// For a durable observed delivery, that non-empty harness output reaching quiescence is sufficient
/// recipient evidence: the target model demonstrably saw and answered the message. This fallback
/// is Hermes-specific; the OpenCode regression above remains strict.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hermes_observed_engine_settles_rendered_reply_via_quiescence() {
    std::env::set_var("NEXUS_ACP_TURN_TIMEOUT_SECS", "2");
    std::env::set_var("NEXUS_ACP_QUIESCENCE_MS", "100");

    let engine = AcpEngine::for_harness(HarnessId::new("hermes").unwrap());
    let cmd = HarnessCommand {
        program: FAKE_HARNESS.to_string(),
        args: vec![],
        cwd: None,
        env: vec![
            ("FAKE_ACP_NO_TURN_END".to_string(), "1".to_string()),
            ("FAKE_ACP_REPLY".to_string(), "bare".to_string()),
            (
                "FAKE_ACP_REPLY_BODY".to_string(),
                "hermes-rendered-reply".to_string(),
            ),
        ],
    };
    engine
        .spawn_and_initialize(&cmd)
        .await
        .expect("spawn + initialize");
    engine
        .new_session(None, &LaunchCtx::default())
        .await
        .expect("session/new");

    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        engine.inject_with_accepted_event(
            "Hermes must visibly receive this delivery".to_string(),
            Some(StreamEvent {
                kind: AgentUpdateKind::UserInput,
                data: json!({"text": "accepted input"}),
            }),
        ),
    )
    .await
    .expect("Hermes observed delivery must settle after rendered output goes quiet")
    .expect("rendered Hermes output is recipient-side delivery evidence");

    assert_eq!(
        reply_text(engine.take_updates()),
        "hermes-rendered-reply",
        "the rendered reply must remain available to the stream relay"
    );

    std::env::remove_var("NEXUS_ACP_TURN_TIMEOUT_SECS");
    std::env::remove_var("NEXUS_ACP_QUIESCENCE_MS");
}

/// Hermes 0.17 returns `EndTurn` immediately when a busy session puts an incoming prompt in its
/// private `queued_prompts` list. That response is not delivery: a daemon restart can kill the
/// Hermes process and lose the queued prompt before the model sees it. Nexus must keep the turn
/// unsettled until Hermes emits the exact queued prompt as a `UserMessageChunk` and subsequent
/// model output proves that promoted turn ran.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hermes_busy_queue_ack_waits_for_prompt_promotion_before_delivery() {
    std::env::set_var("NEXUS_ACP_QUIESCENCE_MS", "100");

    let engine = AcpEngine::for_harness(HarnessId::new("hermes").unwrap());
    let cmd = HarnessCommand {
        program: FAKE_HARNESS.to_string(),
        args: vec![],
        cwd: None,
        env: vec![(
            "FAKE_ACP_HERMES_QUEUE_PROMOTE_MS".to_string(),
            "400".to_string(),
        )],
    };
    engine
        .spawn_and_initialize(&cmd)
        .await
        .expect("spawn + initialize");
    engine
        .new_session(None, &LaunchCtx::default())
        .await
        .expect("session/new");

    let prompt = "delivery must survive until model context".to_string();
    let mut delivery = Box::pin(engine.inject_with_accepted_event(
        prompt,
        Some(StreamEvent {
            kind: AgentUpdateKind::UserInput,
            data: json!({"text": "accepted input"}),
        }),
    ));

    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(150), &mut delivery)
            .await
            .is_err(),
        "Hermes's immediate private-queue EndTurn must not settle observed delivery"
    );
    tokio::time::timeout(std::time::Duration::from_secs(3), &mut delivery)
        .await
        .expect("promoted Hermes turn must finish within the test bound")
        .expect("exact prompt promotion plus model output is delivery evidence");

    let reply = reply_text(engine.take_updates());
    assert!(
        reply.contains("promoted Hermes reply"),
        "the causally promoted model reply must remain in the relay buffer: {reply:?}"
    );

    std::env::remove_var("NEXUS_ACP_QUIESCENCE_MS");
}

/// A resumed Hermes ACP session may replay historical `session/update` rows after `session/load`
/// has already returned. Those rows belong to the old turn and must not count as recipient-side
/// evidence for the first new prompt. Otherwise Nexus can settle a durable delivery even though
/// that prompt never appeared in the target harness context.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hermes_resume_replay_cannot_settle_the_next_observed_prompt() {
    std::env::set_var("NEXUS_ACP_LOAD_REPLAY_SETTLE_MS", "100");

    let engine = AcpEngine::for_harness(HarnessId::new("hermes").unwrap());
    let cmd = HarnessCommand {
        program: FAKE_HARNESS.to_string(),
        args: vec![],
        cwd: None,
        env: vec![
            (
                "FAKE_ACP_LOAD_REPLAY_BODY".to_string(),
                "historical Hermes reply".to_string(),
            ),
            (
                "FAKE_ACP_LOAD_REPLAY_DELAY_MS".to_string(),
                "25".to_string(),
            ),
            ("FAKE_ACP_PROMPT_DELAY_MS".to_string(), "100".to_string()),
            ("FAKE_ACP_REPLY".to_string(), "bare".to_string()),
            ("FAKE_ACP_REPLY_BODY".to_string(), String::new()),
        ],
    };
    engine
        .spawn_and_initialize(&cmd)
        .await
        .expect("spawn + initialize");
    engine
        .load_session("resumed-hermes", None, &LaunchCtx::default())
        .await
        .expect("session/load");

    let error = engine
        .inject_with_accepted_event(
            "new delivery with no model response".to_string(),
            Some(StreamEvent {
                kind: AgentUpdateKind::UserInput,
                data: json!({"text": "accepted input"}),
            }),
        )
        .await
        .expect_err("historical replay must not prove delivery of the new prompt");
    assert!(
        matches!(error, AdapterInjectError::OperatorAction(_)),
        "a fresh Hermes prompt with no fresh model event must fail closed, got {error:?}"
    );

    std::env::remove_var("NEXUS_ACP_LOAD_REPLAY_SETTLE_MS");
}

/// The other liveness guard: a turn that produces **no content at all** and never signals turn-end
/// (no chunks, no prompt response) must NOT be mistaken for a (quiescent) completed turn — there is
/// nothing to complete. It must hit the hard ceiling and error, so the per-agent loop re-parks
/// instead of wedging forever. (Quiescence only ever fires *after* content streams.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn engine_inject_errors_on_silent_turn_with_no_content_and_no_turn_end() {
    std::env::set_var("NEXUS_ACP_TURN_TIMEOUT_SECS", "2");
    std::env::set_var("NEXUS_ACP_QUIESCENCE_MS", "400");

    let engine = AcpEngine::new();
    let cmd = HarnessCommand {
        program: FAKE_HARNESS.to_string(),
        args: vec![],
        cwd: None,
        // Empty reply body AND no turn-end → the turn streams nothing and never ends.
        env: vec![
            ("FAKE_ACP_NO_TURN_END".to_string(), "1".to_string()),
            ("FAKE_ACP_REPLY".to_string(), "bare".to_string()),
            ("FAKE_ACP_REPLY_BODY".to_string(), String::new()),
        ],
    };
    engine
        .spawn_and_initialize(&cmd)
        .await
        .expect("spawn + initialize");
    engine
        .new_session(None, &LaunchCtx::default())
        .await
        .expect("session/new");

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        engine.inject("silence".to_string()),
    )
    .await
    .expect("inject must RETURN (hard ceiling), not hang forever on a fully silent turn");

    let err =
        result.expect_err("a turn with neither content nor a turn-end must error at the ceiling");
    assert!(
        matches!(
            err,
            AdapterInjectError::CompletionTimeout { ref origin }
                if origin == "other.acp.turn_completion"
        ),
        "a missing terminal boundary must remain a typed completion timeout; got {err:?}"
    );

    std::env::remove_var("NEXUS_ACP_TURN_TIMEOUT_SECS");
    std::env::remove_var("NEXUS_ACP_QUIESCENCE_MS");
}

/// `Arc<dyn Adapter>` object-safety + concurrent reuse: the engine is `Send + Sync` and can be
/// shared as the trait object the registry hands out.
#[tokio::test]
async fn adapter_is_object_safe_and_shareable() {
    let adapter: Arc<dyn Adapter> = Arc::new(OpenCodeAdapter::with_command(fake_command()));
    adapter.open_session().await.expect("open via trait object");
    adapter
        .inject("hi".to_string())
        .await
        .expect("inject via trait object");
    let events = adapter
        .stream_updates()
        .await
        .expect("stream via trait object");
    // The full stream is relayed (thinking + reply chunks + plan); the reply itself is the two
    // Text chunks the fake echoes.
    assert_eq!(
        reply_chunks(events),
        vec!["echo: ".to_string(), "hi".to_string()]
    );
}

/// `AcpEngine::kill()` terminates a real spawned child process group.
///
/// This test exercises the FULL kill path including the race that was the original bug:
/// `run_connection` calls `child_arc.take()` as soon as it starts, so after initialization
/// completes the Arc holds `None`. The old `kill()` only inspected the Arc and therefore was
/// a no-op for any running agent. The fix adds a oneshot kill-signal channel; `run_connection`
/// selects on it and terminates the child process group. This test verifies that after a complete
/// `spawn_and_initialize` (where `run_connection` has ownership of the child), `engine.kill()`
/// actually terminates the OS process and the extra descendant the fake harness spawned.
///
/// Verification: `/proc/<pid>` disappears and the process group is empty (Linux).
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn engine_kill_escalates_after_wrapper_exit_to_terminate_stubborn_descendant() {
    let engine = AcpEngine::new();
    let mut command = fake_command();
    command
        .env
        .push(("FAKE_ACP_SPAWN_CHILD".to_string(), "1".to_string()));
    command
        .env
        .push(("FAKE_ACP_CHILD_IGNORE_TERM".to_string(), "1".to_string()));
    engine
        .spawn_and_initialize(&command)
        .await
        .expect("spawn + ACP initialize against the fake harness");
    assert!(engine.is_connected().await);

    // The PID was recorded at spawn time — even after run_connection takes the child out of
    // the Arc, the PID stays available here so we can verify process death.
    let pid = engine
        .spawned_pid()
        .expect("PID must be recorded after a successful spawn");

    // Assert the process IS alive before the kill (sanity check).
    assert!(
        std::path::Path::new(&format!("/proc/{pid}")).exists(),
        "process {pid} must be alive before kill()"
    );
    assert_eq!(
        process_group_of(pid),
        Some(pid),
        "ACP harness must be process-group leader"
    );
    let members_before = process_group_members(pid);
    assert!(
        members_before.len() >= 2,
        "fake harness must have spawned a descendant in process group {pid}, members: {members_before:?}"
    );

    // Call kill() — this fires the oneshot signal to run_connection, which terminates the whole
    // process group. The real fix: this now reaches descendants that direct-child kill leaked.
    engine.kill().await;

    // Give the kernel time to deliver the group signal, reap the zombie, and remove /proc/<pid>.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        if !std::path::Path::new(&format!("/proc/{pid}")).exists()
            && process_group_members(pid).is_empty()
        {
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!(
                "process group {pid} still has members {:?} after engine.kill()",
                process_group_members(pid)
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    // Idempotency: calling kill() again must be a no-op (signal already consumed).
    engine.kill().await; // must not panic
}

/// `AcpEngine::kill()` is a no-op on an engine that was never spawned.
#[tokio::test]
async fn engine_kill_noop_when_not_spawned() {
    let engine = AcpEngine::new();
    engine.kill().await; // must not panic
    engine.kill().await; // idempotent, still must not panic
}

/// A real adapter forwards to the engine's kill — smoke test that the trait object
/// dispatch path compiles and runs without panic.
#[tokio::test]
async fn adapter_kill_dispatches_to_engine() {
    let adapter = OpenCodeAdapter::with_command(fake_command());
    adapter.open_session().await.expect("open_session");
    // Kill the spawned process via the Adapter trait.
    adapter.kill().await; // must not panic
                          // Calling kill() a second time must also be a no-op.
    adapter.kill().await;
}
