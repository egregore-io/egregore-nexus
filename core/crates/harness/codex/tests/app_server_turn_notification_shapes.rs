//! Regression guard for the REAL codex `turn/completed` wire shape.
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

#[derive(Default)]
struct BlockThinkingSink {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    blocked: Option<nexus_contracts::AgentUpdateKind>,
}

#[async_trait]
impl EventSink for BlockThinkingSink {
    async fn emit(&self, event: WsEvent) {
        if matches!(event, WsEvent::AgentUpdate { kind, .. } if kind == self.blocked.unwrap_or(nexus_contracts::AgentUpdateKind::Thinking))
        {
            self.entered.notify_one();
            self.release.notified().await;
        }
    }
}

#[tokio::test]
async fn native_terminal_ingress_precedes_an_earlier_blocked_thinking_projection() {
    assert_native_ingress_before_blocked_projection(nexus_contracts::AgentUpdateKind::Thinking)
        .await;
}

#[tokio::test]
async fn native_terminal_ingress_precedes_an_earlier_blocked_text_projection() {
    assert_native_ingress_before_blocked_projection(nexus_contracts::AgentUpdateKind::Text).await;
}

#[tokio::test]
async fn native_terminal_ingress_precedes_an_earlier_blocked_accepted_projection() {
    assert_native_ingress_before_blocked_projection(nexus_contracts::AgentUpdateKind::UserInput)
        .await;
}

async fn assert_native_ingress_before_blocked_projection(
    blocked: nexus_contracts::AgentUpdateKind,
) {
    let dir = tempdir("blocked-earlier-thinking");
    let script = json!([
        {"method": "item/reasoning/textDelta", "params": {"threadId": "THREAD_ID", "turnId": "t1", "delta": "thinking"}},
        {"method": "item/agentMessage/delta", "params": {"threadId": "THREAD_ID", "turnId": "t1", "itemId": "text", "delta": "text"}},
        {"method": "turn/completed", "params": {"threadId": "THREAD_ID", "turn": {"id": "t1", "status": "completed"}}},
        {"method": "turn/started", "params": {"threadId": "THREAD_ID", "turn": {"id": "t2"}}},
        {"method": "turn/completed", "params": {"threadId": "THREAD_ID", "turn": {"id": "t2", "status": "completed"}}}
    ]);
    let srv = CodexAppServer::start(SupervisorOpts {
        codex_exe: FAKE_BIN.into(),
        session_dir: dir.clone(),
        codex_home: Some(dir.join("codex-home")),
        model: None,
        bus_mcp: None,
        cwd: None,
        env: vec![("FAKE_CODEX_SCRIPT".into(), script.to_string())],
    })
    .await
    .unwrap();
    let client = Arc::new(
        CodexAppServerClient::connect(srv.socket(), "nexus")
            .await
            .unwrap(),
    );
    let thread = client.thread_start().await.unwrap();
    let transport = nexus_harness_codex::CodexAppServerTransport::new();
    let session = SessionId("blocked-earlier-thinking".into());
    transport.bind(session.clone(), client.clone(), thread.clone());
    let tracker = transport.turn_tracker();
    let sink = Arc::new(BlockThinkingSink {
        blocked: Some(blocked),
        ..Default::default()
    });
    if blocked == nexus_contracts::AgentUpdateKind::UserInput {
        tracker.queue_accepted_event(
            &thread,
            sink.clone(),
            WsEvent::AgentUpdate {
                session_id: session.clone(),
                kind: nexus_contracts::AgentUpdateKind::UserInput,
                data: json!({"text": "accepted"}),
            },
        );
    }
    let forwarder = spawn_codex_forwarder(
        session,
        client.clone(),
        sink.clone(),
        Arc::new(AutoApprove),
        tracker.clone(),
    );
    // The scripted server sends the terminal before this correlated response on the same socket.
    client.turn_start(&thread, "go").await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), sink.entered.notified())
        .await
        .unwrap();
    let active = tracker.active_turn_id(&thread);
    tracker.record_turn_start_acceptance(&thread, "t1");
    tracker.record_turn_start_acceptance(&thread, "t2");
    assert_eq!(tracker.active_turn_id(&thread), None);
    let mut completion =
        Box::pin(tracker.wait_for_completion(&thread, "t1", Duration::from_secs(3)));
    assert!(futures::poll!(&mut completion).is_pending());
    sink.release.notify_one();
    completion.await.unwrap();
    forwarder.abort();
    srv.shutdown().await;
    let _ = std::fs::remove_dir_all(dir);
    assert_eq!(
        active, None,
        "native terminal cannot wait behind earlier projected thinking"
    );
}

#[derive(Default)]
struct BlockTerminalSink {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    returned: tokio::sync::Notify,
    emitted: std::sync::Mutex<Vec<nexus_contracts::AgentUpdateKind>>,
}

#[async_trait]
impl EventSink for BlockTerminalSink {
    async fn emit(&self, event: WsEvent) {
        let WsEvent::AgentUpdate { kind, .. } = event else {
            return;
        };
        if kind == nexus_contracts::AgentUpdateKind::TurnEnd {
            self.entered.notify_one();
            self.release.notified().await;
            self.emitted.lock().unwrap().push(kind);
            self.returned.notify_one();
        } else {
            self.emitted.lock().unwrap().push(kind);
        }
    }
}

async fn assert_completion_authority_before_projection(with_text: bool) {
    let dir = tempdir("blocked-terminal-projection");
    let mut script = vec![json!({
        "method": "turn/started",
        "params": {"threadId": "THREAD_ID", "turn": {"id": "t1"}}
    })];
    if with_text {
        script.push(json!({
            "method": "item/agentMessage/delta",
            "params": {"delta": "hi", "threadId": "THREAD_ID", "turnId": "t1", "itemId": "i1"}
        }));
    }
    script.push(json!({
        "method": "turn/completed",
        "params": {"threadId": "THREAD_ID", "turn": {"id": "t1", "status": "completed"}}
    }));
    let srv = CodexAppServer::start(SupervisorOpts {
        codex_exe: FAKE_BIN.into(),
        session_dir: dir.clone(),
        codex_home: Some(dir.join("codex-home")),
        model: None,
        bus_mcp: None,
        cwd: None,
        env: vec![(
            "FAKE_CODEX_SCRIPT".into(),
            serde_json::to_string(&script).unwrap(),
        )],
    })
    .await
    .unwrap();
    let client = Arc::new(
        CodexAppServerClient::connect(srv.socket(), "nexus")
            .await
            .unwrap(),
    );
    let thread = client.thread_start().await.unwrap();
    let tracker = CodexTurnTracker::default();
    let sink = Arc::new(BlockTerminalSink::default());
    let task = spawn_codex_forwarder(
        SessionId("s1".into()),
        client.clone(),
        sink.clone(),
        Arc::new(AutoApprove),
        tracker.clone(),
    );
    client.turn_start(&thread, "go").await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), sink.entered.notified())
        .await
        .expect("forwarder must consume the native terminal and enter its projection sink");
    // The sink handshake proves the terminal was consumed, not merely queued behind an earlier
    // blocked display event. Keep it parked while sampling the scheduler's actual authority.
    let active_while_blocked = tracker.active_turn_id(&thread);
    assert!(!sink
        .emitted
        .lock()
        .unwrap()
        .contains(&nexus_contracts::AgentUpdateKind::TurnEnd));
    let mut completion =
        Box::pin(tracker.wait_for_completion(&thread, "t1", Duration::from_secs(5)));
    assert!(
        tokio::time::timeout(Duration::from_millis(25), &mut completion)
            .await
            .is_err(),
        "routing truth must not settle receipt/completion waiters before projection returns"
    );
    tracker.record_turn_start_acceptance(&thread, "t1");
    let active_after_late_acceptance = tracker.active_turn_id(&thread);
    tracker.observe_active_turn(&thread, "t2");
    sink.release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), sink.returned.notified())
        .await
        .unwrap();
    completion.await.unwrap();
    let active_after_release = tracker.active_turn_id(&thread);
    let emitted = sink.emitted.lock().unwrap().clone();
    assert_eq!(
        emitted.last(),
        Some(&nexus_contracts::AgentUpdateKind::TurnEnd)
    );
    assert_eq!(
        emitted
            .iter()
            .filter(|kind| **kind == nexus_contracts::AgentUpdateKind::Text)
            .count(),
        usize::from(with_text)
    );
    task.abort();
    srv.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(
        active_after_release.as_deref(),
        Some("t2"),
        "old projection completion must not clear the newer native turn"
    );
    assert_eq!(
        active_after_late_acceptance, None,
        "late acceptance must not resurrect the consumed terminal"
    );
    assert_eq!(
        active_while_blocked, None,
        "consumed native completion must not remain busy behind projection I/O"
    );
}

#[tokio::test]
async fn completion_authority_does_not_wait_for_terminal_projection() {
    assert_completion_authority_before_projection(false).await;
}

#[tokio::test]
async fn completion_authority_with_text_does_not_wait_for_terminal_projection() {
    assert_completion_authority_before_projection(true).await;
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
        codex_home: Some(dir.join("codex-home")),
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
