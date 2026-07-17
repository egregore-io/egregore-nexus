//! Real-binary live smoke tests for built-in ACP adapters.
//!
//! These drive actual harness ACP bridges (a real model, real network), so
//! they are `#[ignore]`d by default and additionally **skip if the binary/runtime is absent** —
//! they never fail a machine that simply doesn't have the harness installed. Run them on a
//! machine that has the binaries + credentials with:
//!
//! ```bash
//! # build the live spawn path, then run the ignored smoke tests single-threaded
//! cargo test -p nexus-agent --test live_smoke -- --ignored --nocapture --test-threads=1
//! ```
//!
//! What they verify on a real machine: spawn the bridge → ACP `initialize` → `session/new` →
//! one `session/prompt` → at least one `session/update` chunk relayed back. That is the same
//! path two real instances use to talk on the Nexus bus.

use std::process::Stdio;

use nexus_agent::adapter::{Adapter, HermesAdapter, OpenCodeAdapter};
use nexus_contracts::AgentUpdateKind;

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
        // A missing binary / auth / network issue must skip, not fail.
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

#[tokio::test]
#[ignore = "requires a real `opencode` binary with ACP support"]
async fn opencode_live_one_turn() {
    let adapter = OpenCodeAdapter::new(nexus_agent::adapter::engine::LaunchCtx::default());
    if !program_available(&adapter.command().program) {
        eprintln!(
            "[skip] opencode: `{}` not on PATH",
            adapter.command().program
        );
        return;
    }
    smoke_one_turn(adapter, "opencode").await;
}

#[tokio::test]
#[ignore = "requires a real `opencode` binary with ACP support and credentials"]
async fn opencode_live_one_turn_with_nexus_config() {
    let dir = std::env::temp_dir().join(format!(
        "nexus-opencode-live-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("temp live opencode cwd");
    let cwd = dir.to_string_lossy().into_owned();
    let adapter = OpenCodeAdapter::new(nexus_agent::adapter::engine::LaunchCtx {
        cwd: Some(cwd),
        env: vec![
            ("NEXUS_NAME".into(), "live-opencode-probe".into()),
            ("NEXUS_CLIENT_KEY".into(), "nexus_ck_live_probe".into()),
            ("NEXUS_PROJECT".into(), "default".into()),
            ("NEXUS_AGENT".into(), "opencode".into()),
        ],
        bus_name: Some("live-opencode-probe".into()),
        bus_project: Some("default".into()),
        bus_client_key: Some("nexus_ck_live_probe".into()),
        bus_agent: Some("opencode".into()),
        ..Default::default()
    });
    if !program_available(&adapter.command().program) {
        eprintln!(
            "[skip] opencode: `{}` not on PATH",
            adapter.command().program
        );
        let _ = std::fs::remove_dir_all(&dir);
        return;
    }
    smoke_one_turn(adapter, "opencode+nexus-config").await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
#[ignore = "requires a real `hermes` binary with ACP support"]
async fn hermes_live_one_turn() {
    let adapter = HermesAdapter::new(nexus_agent::adapter::engine::LaunchCtx::default());
    if !program_available(&adapter.command().program) {
        eprintln!("[skip] hermes: `{}` not on PATH", adapter.command().program);
        return;
    }
    smoke_one_turn(adapter, "hermes").await;
}
