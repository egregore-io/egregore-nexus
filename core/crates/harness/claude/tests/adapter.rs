use std::sync::Arc;

use nexus_agent::adapter::engine::{HarnessCommand, LaunchCtx};
use nexus_agent::{Adapter, StreamEvent};
use nexus_contracts::AgentUpdateKind;
use nexus_harness_claude::ClaudeAdapter;

const FAKE_HARNESS: &str = env!("CARGO_BIN_EXE_fake_claude_acp_agent");

fn fake_command() -> HarnessCommand {
    HarnessCommand {
        program: FAKE_HARNESS.to_string(),
        args: vec![],
        cwd: None,
        ..Default::default()
    }
}

fn fake_command_failing_load() -> HarnessCommand {
    HarnessCommand {
        program: FAKE_HARNESS.to_string(),
        args: vec![],
        cwd: None,
        env: vec![("FAKE_ACP_FAIL_LOAD".to_string(), "1".to_string())],
    }
}

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

#[tokio::test]
async fn claude_adapter_strict_observed_returns_owned_buffer() {
    let adapter = ClaudeAdapter::with_command(fake_command());
    adapter.open_session().await.expect("open_session");
    let events = adapter
        .inject_completion_observed("strict".to_string())
        .await
        .expect("strict completion delegates to ACP engine");
    assert_eq!(reply_chunks(events), vec!["echo: ", "strict"]);
    assert!(adapter
        .stream_updates()
        .await
        .expect("stream_updates")
        .is_empty());
    adapter.kill().await;
}

#[tokio::test]
async fn claude_adapter_full_turn_over_acp() {
    let adapter = ClaudeAdapter::with_command(fake_command());
    adapter
        .open_session()
        .await
        .expect("open_session over fake ACP");
    adapter
        .inject("take the auth refactor?".to_string())
        .await
        .expect("inject one session/prompt turn");

    let chunks = reply_chunks(adapter.stream_updates().await.expect("stream_updates"));
    assert_eq!(
        chunks,
        vec!["echo: ".to_string(), "take the auth refactor?".to_string()]
    );
}

#[tokio::test]
async fn adapter_resume_uses_session_load() {
    let adapter = ClaudeAdapter::with_command(fake_command());
    adapter
        .resume("fake-session-1")
        .await
        .expect("resume via session/load");
    adapter
        .inject("resumed turn".to_string())
        .await
        .expect("inject after resume");
    let chunks = reply_chunks(adapter.stream_updates().await.expect("stream_updates"));
    assert_eq!(
        chunks,
        vec!["echo: ".to_string(), "resumed turn".to_string()]
    );
}

#[tokio::test]
async fn resume_fail_falls_back_to_new_session_on_same_connection() {
    let adapter = ClaudeAdapter::with_command(fake_command_failing_load());
    let err = adapter
        .resume("dead-resume-key")
        .await
        .expect_err("session/load must fail when the harness rejects it");
    assert!(
        format!("{err}").contains("session/load failed"),
        "resume error should describe the failed session/load, got: {err}"
    );

    adapter
        .new_session_only()
        .await
        .expect("new_session_only must succeed on the still-live connection");
    adapter
        .inject("after fallback".to_string())
        .await
        .expect("inject must work against the freshly-opened session");
    let chunks = reply_chunks(adapter.stream_updates().await.expect("stream_updates"));
    assert_eq!(
        chunks,
        vec!["echo: ".to_string(), "after fallback".to_string()]
    );
}

#[tokio::test]
async fn builtin_real_adapter_resolves_documented_acp_command() {
    let claude = ClaudeAdapter::new(LaunchCtx::default());

    assert_eq!(
        claude.command().program,
        nexus_harness_core::native_npm_runner(nexus_harness_core::NativeProcessPlatform::current())
    );
    assert_eq!(
        claude.command().args,
        [
            "-y".to_string(),
            "@agentclientprotocol/claude-agent-acp@0.58.1".to_string(),
        ],
        "the production bridge must stay on the live-gated ACP version; npm latest is not a compatibility contract"
    );
}

#[tokio::test]
async fn adapter_is_object_safe_and_shareable() {
    let adapter: Arc<dyn Adapter> = Arc::new(ClaudeAdapter::with_command(fake_command()));
    adapter.open_session().await.expect("open via trait object");
    adapter
        .inject("hi".to_string())
        .await
        .expect("inject via trait object");
    let events = adapter
        .stream_updates()
        .await
        .expect("stream via trait object");
    assert_eq!(
        reply_chunks(events),
        vec!["echo: ".to_string(), "hi".to_string()]
    );
}

#[tokio::test]
async fn adapter_kill_dispatches_to_engine() {
    let adapter = ClaudeAdapter::with_command(fake_command());
    adapter.open_session().await.expect("open_session");
    adapter.kill().await;
    adapter.kill().await;
}
