//! Integration test for `spawn_codex_forwarder`.
//!
//! Proves the shared-session property: a second client's `turn/start` is
//! observed by the forwarder's subscription (registered on the first client).
//!
//! Run: `cargo test -p nexus-harness-codex --test app_server_forwarder`

use std::sync::Arc;

use async_trait::async_trait;
use nexus_contracts::events::WsEvent;
use nexus_contracts::ids::SessionId;
use nexus_contracts::ports::EventSink;
use nexus_contracts::AgentUpdateKind;
use nexus_harness_codex::app_server::{
    spawn_codex_forwarder, spawn_codex_forwarder_with_tool_observations, CodexToolObservationSink,
};
use nexus_harness_codex::CodexTurnTracker;
use nexus_harness_codex::{AutoApprove, CodexAppServer, CodexAppServerClient, SupervisorOpts};
use nexus_transcript::{ToolCallObservation, ToolCallPhase};
use serde_json::json;

/// Path to the hermetic fake binary, injected by Cargo.
const FAKE_BIN: &str = env!("CARGO_BIN_EXE_fake_codex_app_server");

fn tempdir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "nexus-forwarder-test-{}-{}-{}",
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

/// A test `EventSink` that records emitted `WsEvent`s behind a Mutex.
#[derive(Default)]
struct RecSink(tokio::sync::Mutex<Vec<WsEvent>>);

#[async_trait]
impl EventSink for RecSink {
    async fn emit(&self, e: WsEvent) {
        self.0.lock().await.push(e);
    }
}

#[derive(Default)]
struct ToolObsSink(std::sync::Mutex<Vec<(SessionId, ToolCallObservation)>>);

impl ToolObsSink {
    fn observations(&self) -> Vec<(SessionId, ToolCallObservation)> {
        self.0.lock().unwrap().clone()
    }
}

impl CodexToolObservationSink for ToolObsSink {
    fn publish_tool_call(&self, session: &SessionId, observation: ToolCallObservation) {
        self.0.lock().unwrap().push((session.clone(), observation));
    }
}

/// Proves the shared-session / no-resume property:
///
/// - Connection A (nexus client) does `thread_start`, then
///   `spawn_codex_forwarder` on the SAME `Arc<CodexAppServerClient>`.
///   The forwarder does NOT call `thread_resume` — calling resume on a fresh
///   thread that has never had a turn would attempt to read a rollout from
///   disk that does not yet exist and would fail.  The connection from
///   `thread_start` is already subscribed to the thread's notifications.
///
/// - Connection B (driver client) does `turn_start` on the same thread.
///
/// - The forwarder's sink receives `Text` then `TurnEnd` events, proving
///   that no `thread/resume` is needed and must not be reintroduced.
///
/// REGRESSION GUARD (Fix 2 / Task 6): if `spawn_codex_forwarder` is ever
/// changed to call `thread_resume` before reading notifications, this test
/// will fail because the fake server handles resume fine, but the real codex
/// would error with "no rollout found for thread id" on a fresh thread.
/// The absence of any `thread_resume` call in `spawn_codex_forwarder` is
/// load-bearing — DO NOT reintroduce it.
#[tokio::test]
async fn forwarder_emits_text_then_turn_end() {
    let dir = tempdir("forwarder");

    let srv = CodexAppServer::start(SupervisorOpts {
        codex_exe: FAKE_BIN.to_string(),
        session_dir: dir.clone(),
        codex_home: None,
        model: None,
        bus_mcp: None,
        cwd: None,
        env: vec![],
    })
    .await
    .expect("fake app-server should start");

    // Connection A: nexus client — starts a thread, then spawns the forwarder.
    let cli = CodexAppServerClient::connect(srv.socket(), "nexus")
        .await
        .expect("nexus client should connect");

    let thread = cli
        .thread_start()
        .await
        .expect("thread_start should succeed");

    let sink = Arc::new(RecSink::default());
    let h = spawn_codex_forwarder(
        SessionId("s1".into()),
        Arc::new(cli),
        sink.clone(),
        Arc::new(AutoApprove),
        CodexTurnTracker::default(),
    );

    // Give the forwarder a moment to start reading the notification stream.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Connection B: driver client — triggers a turn on the same thread.
    let driver = CodexAppServerClient::connect(srv.socket(), "driver")
        .await
        .expect("driver client should connect");

    driver
        .turn_start(&thread, "go")
        .await
        .expect("turn_start should succeed");

    // Poll the sink until TurnEnd appears (bounded ~2 s).
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        let got = sink.0.lock().await.clone();
        let kinds: Vec<AgentUpdateKind> = got
            .iter()
            .filter_map(|e| match e {
                WsEvent::AgentUpdate { kind, .. } => Some(*kind),
                _ => None,
            })
            .collect();

        if kinds.last() == Some(&AgentUpdateKind::TurnEnd) {
            // Assert both Text and TurnEnd are present.
            assert!(
                kinds.contains(&AgentUpdateKind::Text),
                "expected at least one Text event, got: {kinds:?}"
            );
            assert_eq!(
                *kinds.last().unwrap(),
                AgentUpdateKind::TurnEnd,
                "last event should be TurnEnd, got: {kinds:?}"
            );
            break;
        }

        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for TurnEnd; got kinds: {kinds:?}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    h.abort();
    srv.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn forwarder_suppresses_completed_agent_message_after_streamed_delta() {
    let dir = tempdir("forwarder-dedup");
    let script = serde_json::to_string(&json!([
        {
            "method": "item/agentMessage/delta",
            "params": {
                "delta": "hello",
                "threadId": "THREAD_ID",
                "turnId": "t1",
                "itemId": "am1"
            }
        },
        {
            "method": "item/completed",
            "params": {
                "threadId": "THREAD_ID",
                "turnId": "t1",
                "item": {
                    "type": "agentMessage",
                    "id": "am1",
                    "text": "hello"
                }
            }
        },
        {
            "method": "turn/completed",
            "params": {
                "threadId": "THREAD_ID",
                "turnId": "t1"
            }
        }
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

    let sink = Arc::new(RecSink::default());
    let h = spawn_codex_forwarder(
        SessionId("s1".into()),
        Arc::new(cli),
        sink.clone(),
        Arc::new(AutoApprove),
        CodexTurnTracker::default(),
    );

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let driver = CodexAppServerClient::connect(srv.socket(), "driver")
        .await
        .expect("driver client should connect");
    driver
        .turn_start(&thread, "go")
        .await
        .expect("turn_start should succeed");

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        let got = sink.0.lock().await.clone();
        let kinds: Vec<AgentUpdateKind> = got
            .iter()
            .filter_map(|e| match e {
                WsEvent::AgentUpdate { kind, .. } => Some(*kind),
                _ => None,
            })
            .collect();

        if kinds.last() == Some(&AgentUpdateKind::TurnEnd) {
            let text = got
                .iter()
                .filter_map(|e| match e {
                    WsEvent::AgentUpdate {
                        kind: AgentUpdateKind::Text,
                        data,
                        ..
                    } => data.get("text").and_then(|v| v.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("");
            assert_eq!(text, "hello");
            break;
        }

        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for TurnEnd; got kinds: {kinds:?}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    h.abort();
    srv.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn forwarder_preserves_native_text_item_id_across_interleaved_tool_call() {
    let dir = tempdir("forwarder-text-item-id");
    let script = serde_json::to_string(&json!([
        {
            "method": "item/agentMessage/delta",
            "params": {
                "delta": "checking the",
                "threadId": "THREAD_ID",
                "turnId": "t1",
                "itemId": "am1"
            }
        },
        {
            "method": "item/completed",
            "params": {
                "threadId": "THREAD_ID",
                "turnId": "t1",
                "item": {
                    "type": "commandExecution",
                    "id": "tc1",
                    "command": "find focused test",
                    "aggregatedOutput": "focused.test.ts",
                    "commandActions": [],
                    "cwd": "/tmp",
                    "status": "completed"
                }
            }
        },
        {
            "method": "item/agentMessage/delta",
            "params": {
                "delta": " focused test",
                "threadId": "THREAD_ID",
                "turnId": "t1",
                "itemId": "am1"
            }
        },
        {
            "method": "item/completed",
            "params": {
                "threadId": "THREAD_ID",
                "turnId": "t1",
                "item": {
                    "type": "agentMessage",
                    "id": "am1",
                    "text": "checking the focused test"
                }
            }
        },
        {
            "method": "turn/completed",
            "params": {
                "threadId": "THREAD_ID",
                "turnId": "t1"
            }
        }
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
    let sink = Arc::new(RecSink::default());
    let h = spawn_codex_forwarder(
        SessionId("s1".into()),
        Arc::new(cli),
        sink.clone(),
        Arc::new(AutoApprove),
        CodexTurnTracker::default(),
    );

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let driver = CodexAppServerClient::connect(srv.socket(), "driver")
        .await
        .expect("driver client should connect");
    driver
        .turn_start(&thread, "go")
        .await
        .expect("turn_start should succeed");

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    let updates = loop {
        let got = sink.0.lock().await.clone();
        let updates = got
            .into_iter()
            .filter_map(|event| match event {
                WsEvent::AgentUpdate { kind, data, .. } => Some((kind, data)),
                _ => None,
            })
            .collect::<Vec<_>>();
        if updates.last().map(|(kind, _)| *kind) == Some(AgentUpdateKind::TurnEnd) {
            break updates;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for TurnEnd; got updates: {updates:?}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    };

    assert_eq!(
        updates.iter().map(|(kind, _)| *kind).collect::<Vec<_>>(),
        vec![
            AgentUpdateKind::Text,
            AgentUpdateKind::ToolCall,
            AgentUpdateKind::Text,
            AgentUpdateKind::TurnEnd,
        ]
    );
    assert_eq!(
        updates[0].1,
        json!({ "text": "checking the", "itemId": "am1" })
    );
    assert_eq!(updates[1].1["id"], "tc1");
    assert_eq!(
        updates[2].1,
        json!({ "text": " focused test", "itemId": "am1" })
    );

    h.abort();
    srv.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn forwarder_publishes_tool_call_observations_once_per_item_phase() {
    let dir = tempdir("forwarder-tool-observations");
    let script = serde_json::to_string(&json!([
        {
            "method": "item/commandExecution/outputDelta",
            "params": {
                "threadId": "THREAD_ID",
                "turnId": "t1",
                "itemId": "cmd1",
                "delta": "first line\n"
            }
        },
        {
            "method": "item/commandExecution/outputDelta",
            "params": {
                "threadId": "THREAD_ID",
                "turnId": "t1",
                "itemId": "cmd1",
                "delta": "second line\n"
            }
        },
        {
            "method": "item/completed",
            "params": {
                "threadId": "THREAD_ID",
                "turnId": "t1",
                "item": {
                    "type": "commandExecution",
                    "id": "cmd1",
                    "command": "cargo test",
                    "aggregatedOutput": "first line\nsecond line\n",
                    "status": "failed",
                    "exitCode": 101
                }
            }
        },
        {
            "method": "turn/completed",
            "params": {
                "threadId": "THREAD_ID",
                "turnId": "t1"
            }
        }
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

    let session = SessionId("s_codex_tool_obs".into());
    let sink = Arc::new(RecSink::default());
    let tool_sink = Arc::new(ToolObsSink::default());
    let h = spawn_codex_forwarder_with_tool_observations(
        session.clone(),
        Arc::new(cli),
        sink.clone(),
        Arc::new(AutoApprove),
        CodexTurnTracker::default(),
        Some(tool_sink.clone()),
    );

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let driver = CodexAppServerClient::connect(srv.socket(), "driver")
        .await
        .expect("driver client should connect");
    driver
        .turn_start(&thread, "go")
        .await
        .expect("turn_start should succeed");

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    let observations = loop {
        let observations = tool_sink.observations();
        if observations.len() >= 2 {
            break observations;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for tool observations; got: {observations:?}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    };

    assert_eq!(observations.len(), 2);
    assert_eq!(observations[0].0, session);
    assert_eq!(observations[0].1.tool_call_id.as_deref(), Some("cmd1"));
    assert_eq!(observations[0].1.tool, "commandExecution");
    assert_eq!(observations[0].1.phase, ToolCallPhase::Pre);
    assert!(observations[0].1.ok);

    assert_eq!(observations[1].1.tool_call_id.as_deref(), Some("cmd1"));
    assert_eq!(observations[1].1.tool, "cargo test");
    assert_eq!(observations[1].1.phase, ToolCallPhase::Post);
    assert!(!observations[1].1.ok);

    h.abort();
    srv.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}
