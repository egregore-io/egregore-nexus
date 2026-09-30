//! Real-binary live smoke test for the Claude ACP adapter.

use std::process::Stdio;

use nexus_agent::adapter::engine::LaunchCtx;
use nexus_agent::Adapter;
use nexus_contracts::AgentUpdateKind;
use nexus_harness_claude::ClaudeAdapter;

fn program_available(program: &str) -> bool {
    std::process::Command::new(program)
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

#[tokio::test]
#[ignore = "requires a real Claude Code ACP bridge + credentials"]
async fn claude_live_one_turn() {
    let adapter = ClaudeAdapter::new(LaunchCtx::default());
    if !program_available(&adapter.command().program) {
        eprintln!("[skip] claude: `{}` not on PATH", adapter.command().program);
        return;
    }
    if let Err(e) = adapter.open_session().await {
        eprintln!("[skip] claude: open_session failed (binary/auth/network?): {e}");
        return;
    }
    adapter
        .inject("Reply with the single word: pong".to_string())
        .await
        .unwrap_or_else(|e| panic!("claude: inject failed after a successful open: {e}"));
    let events = adapter
        .stream_updates()
        .await
        .unwrap_or_else(|e| panic!("claude: stream_updates failed: {e}"));
    let reply: String = events
        .into_iter()
        .filter(|e| e.kind == AgentUpdateKind::Text)
        .filter_map(|e| {
            e.data
                .get("text")
                .and_then(|t| t.as_str())
                .map(str::to_owned)
        })
        .collect();
    eprintln!("[claude] reply: {reply:?}");
    assert!(!reply.trim().is_empty());
}
