//! Real-binary live smoke for the Codex ACP adapter (moved from nexus-agent).
//!
//! Drives the **actual** `codex` ACP bridge (a real model, real network), so this test is
//! `#[ignore]`d by default and additionally **skips if the binary/runtime is absent** —
//! it never fails a machine that simply doesn't have the harness installed. Run it on a
//! machine that has the binaries + credentials with:
//!
//! ```bash
//! cargo test -p nexus-harness-codex --test live_smoke -- --ignored --nocapture --test-threads=1
//! ```
//!
//! What it verifies on a real machine: spawn the bridge → ACP `initialize` → `session/new` →
//! one `session/prompt` → at least one `session/update` chunk relayed back.

use std::process::Stdio;

use nexus_agent::Adapter;
use nexus_contracts::AgentUpdateKind;
use nexus_harness_codex::CodexAdapter;

/// True if `program` resolves on PATH (best-effort: `program --version`). Used to skip the live
/// smoke when the harness runtime (e.g. `npx`/`node`) isn't installed.
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

/// Run one real turn through `adapter`, asserting a non-empty reply. Shared by both smokes.
async fn smoke_one_turn(adapter: impl Adapter, label: &str) {
    if let Err(e) = adapter.open_session().await {
        if open_failure_is_fatal(live_gate_required()) {
            panic!("{label}: required live gate could not open the adapter: {e}");
        }
        eprintln!("[skip] {label}: open_session failed (binary/auth/network?): {e}");
        return;
    }
    adapter
        .inject("Reply with the single word: pong".to_string())
        .await
        .unwrap_or_else(|e| panic!("{label}: inject failed after a successful open: {e}"));
    let events = adapter
        .stream_updates()
        .await
        .unwrap_or_else(|e| panic!("{label}: stream_updates failed: {e}"));
    // The reply text is the `Text`-kind events of the full pass-through stream.
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
    eprintln!("[{label}] reply: {reply:?}");
    assert!(
        !reply.trim().is_empty(),
        "{label}: expected a non-empty relayed reply"
    );
}

fn live_gate_required() -> bool {
    std::env::var("NEXUS_LIVE_E2E_REQUIRED").as_deref() == Ok("1")
}

fn open_failure_is_fatal(required: bool) -> bool {
    required
}

#[tokio::test]
#[ignore = "requires a real Codex ACP bridge + credentials"]
async fn codex_live_one_turn() {
    let adapter = CodexAdapter::new(nexus_agent::LaunchCtx::default());
    if !program_available(&adapter.command().program) {
        if live_gate_required() {
            panic!(
                "codex: required live gate cannot resolve `{}`",
                adapter.command().program
            );
        }
        eprintln!("[skip] codex: `{}` not on PATH", adapter.command().program);
        return;
    }
    smoke_one_turn(adapter, "codex").await;
}

#[test]
fn required_live_gate_treats_adapter_open_failure_as_fatal() {
    assert!(open_failure_is_fatal(true));
    assert!(!open_failure_is_fatal(false));
}
