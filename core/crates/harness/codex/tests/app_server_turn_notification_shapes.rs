//! Regression guard for the REAL codex `turn/completed` wire shape (N25 root cause).
//!
//! codex 0.144.x (ServerNotification.json) sends `turn/started` / `turn/completed` with the
//! turn id NESTED as `turn.id` — there is no top-level `turnId` on those two methods (only
//! `item/*` and `error` carry it flat). The forwarder used to read only the flat field, so
//! every live `turn/completed` was invisible to the turn tracker: `wait_for_completion`
//! always burned its full timeout, deliveries were falsely dead-lettered, and the
//! `harness.prompt` scheduler stayed gated behind turns that had long since finished.
//! These tests script the fake app-server with the REAL nested shape (plus the legacy flat
//! shape for compat) and assert the tracker's waiter actually wakes.
//!
//! Run: `cargo test -p nexus-harness-codex --test app_server_turn_notification_shapes`

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use nexus_contracts::events::WsEvent;
use nexus_contracts::ids::SessionId;
use nexus_contracts::ports::EventSink;
use nexus_harness_codex::app_server::spawn_codex_forwarder;
use nexus_harness_codex::CodexTurnTracker;
use nexus_harness_codex::{AutoApprove, CodexAppServer, CodexAppServerClient, SupervisorOpts};
use serde_json::json;

const FAKE_BIN: &str = env!("CARGO_BIN_EXE_fake_codex_app_server");

fn tempdir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "nexus-turn-shape-test-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos(),
        tag,
    ));
    std::fs::create_dir_all(&dir).expect("create test tempdir");
    dir
}

#[derive(Default)]
struct NullSink;

#[async_trait]
impl EventSink for NullSink {
    async fn emit(&self, _e: WsEvent) {}
}

/// Drive one scripted turn and assert `wait_for_completion` resolves well inside the
/// deadline (i.e. the tracker OBSERVED the completion instead of timing out).
async fn assert_completion_observed(tag: &str, completed_params: serde_json::Value) {
    let dir = tempdir(tag);
    let script = serde_json::to_string(&json!([
        {
            "method": "item/agentMessage/delta",
            "params": { "delta": "hi", "threadId": "THREAD_ID", "turnId": "t1", "itemId": "i1" }
        },
        { "method": "turn/completed", "params": completed_params }
    ]))
    .expect("script serializes");

    let srv = CodexAppServer::start(SupervisorOpts {
        codex_exe: FAKE_BIN.to_string(),
        session_dir: dir.clone(),
        codex_home: None,
        model: None,
        bus_mcp: None,
        cwd: None,
        env: vec![("FAKE_CODEX_SCRIPT".into(), script)],
    })
    .await
    .expect("fake app-server should start");

    let cli = CodexAppServerClient::connect(srv.socket(), "nexus")
        .await
        .expect("nexus client should connect");
    let thread = cli
        .thread_start()
        .await
        .expect("thread_start should succeed");

    let tracker = CodexTurnTracker::default();
    let h = spawn_codex_forwarder(
        SessionId("s1".into()),
        Arc::new(cli),
        Arc::new(NullSink),
        Arc::new(AutoApprove),
        tracker.clone(),
    );
    tokio::time::sleep(Duration::from_millis(50)).await;

    let driver = CodexAppServerClient::connect(srv.socket(), "driver")
        .await
        .expect("driver client should connect");
    driver
        .turn_start(&thread, "go")
        .await
        .expect("turn_start should succeed");

    // The tracker must observe the scripted completion. 5s is the OBSERVATION bound —
    // if the shape is unreadable this only returns via the wait timeout, which we set
    // equal to the bound so an unobserved completion fails the test loudly.
    tracker
        .wait_for_completion(&thread, "t1", Duration::from_secs(5))
        .await
        .expect("completion must be OBSERVED (waiter woken), not timed out");

    h.abort();
    srv.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// The real 0.144.x wire shape: turn id nested under `turn.id`, no top-level `turnId`.
#[tokio::test]
async fn turn_completed_with_nested_turn_object_wakes_the_waiter() {
    assert_completion_observed(
        "nested",
        json!({
            "threadId": "THREAD_ID",
            "turn": { "id": "t1", "items": [], "status": "completed" }
        }),
    )
    .await;
}

/// An interrupted/aborted turn still ENDS the turn: the waiter must wake (and the
/// active-turn gate clear) instead of stalling the queue until the timeout.
#[tokio::test]
async fn turn_completed_with_interrupted_status_wakes_the_waiter() {
    assert_completion_observed(
        "interrupted",
        json!({
            "threadId": "THREAD_ID",
            "turn": { "id": "t1", "items": [], "status": "interrupted" }
        }),
    )
    .await;
}

/// Legacy/flat shape stays supported (older fixtures and any older app-server builds).
#[tokio::test]
async fn turn_completed_with_flat_turn_id_still_wakes_the_waiter() {
    assert_completion_observed("flat", json!({ "threadId": "THREAD_ID", "turnId": "t1" })).await;
}
