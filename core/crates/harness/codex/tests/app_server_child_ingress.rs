//! Composed boundary: the bridge-owned ingress tracker installed by the public
//! `CodexAppServerTransport::bind`, the scoped forwarder on the same client, and a child thread
//! sharing the parent's turn id over the real socket.
//!
//! The parent's turn is accepted through `transport.prompt` while the fake holds notifications,
//! so it is `NativeOpen`. A child turn then delivers text, a command item, a malformed identity,
//! a missing identity and a completion, all on the parent's turn id `t1`. The child-lane
//! terminal is the delivery witness. The bound root and owner must remain, the parent must stay
//! open and active, the child must have no tracker state, and the parent's lane, tool
//! observations and echo path must be untouched. The parent's own command item and terminal
//! must then settle it: same owner, newer revision, `VerifiedIdle`, no active session.
//!
//! Run: `cargo test -p nexus-harness-codex --test app_server_child_ingress`

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use nexus_contracts::events::WsEvent;
use nexus_contracts::ids::SessionId;
use nexus_contracts::ports::{AgentTurnExecutionPort, EventSink};
use nexus_contracts::{AgentUpdateKind, ChildResolution, TurnState};
use nexus_harness_codex::app_server::{
    spawn_codex_forwarder_scoped, CodexThreadScope, CodexToolObservationSink,
};
use nexus_harness_codex::{
    AutoApprove, CodexAppServer, CodexAppServerClient, CodexAppServerTransport, SupervisorOpts,
};
use nexus_transcript::ToolCallObservation;
use serde_json::json;

const FAKE_BIN: &str = env!("CARGO_BIN_EXE_fake_codex_app_server");
const MAIN: &str = "fake-thread";
const CHILD: &str = "child-thread-1";
const WATCHDOG: Duration = Duration::from_secs(10);

fn tempdir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "nexus-child-ingress-{}-{}-{}",
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

/// Bounded wait for a sink predicate; panics with the events seen on timeout.
async fn wait_for(sink: &RecSink, what: &str, pred: impl Fn(&[WsEvent]) -> bool) -> Vec<WsEvent> {
    let deadline = tokio::time::Instant::now() + WATCHDOG;
    loop {
        let got = sink.0.lock().await.clone();
        if pred(&got) {
            return got;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}; have {got:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn parent_kinds(events: &[WsEvent]) -> Vec<AgentUpdateKind> {
    events
        .iter()
        .filter_map(|e| match e {
            WsEvent::AgentUpdate { kind, .. } => Some(*kind),
            _ => None,
        })
        .collect()
}

fn parent_texts(events: &[WsEvent]) -> Vec<String> {
    events
        .iter()
        .filter_map(|e| match e {
            WsEvent::AgentUpdate {
                kind: AgentUpdateKind::Text,
                data,
                ..
            } => Some(data["text"].as_str().unwrap_or("").to_string()),
            _ => None,
        })
        .collect()
}

fn child_events(events: &[WsEvent]) -> Vec<&WsEvent> {
    events
        .iter()
        .filter(|e| matches!(e, WsEvent::ChildAgentUpdate { .. }))
        .collect()
}

fn child_turn_ends(events: &[WsEvent], id: &str) -> usize {
    child_events(events)
        .iter()
        .filter(|e| {
            matches!(e, WsEvent::ChildAgentUpdate { child, kind: AgentUpdateKind::TurnEnd, .. }
                if child.id.as_deref() == Some(id))
        })
        .count()
}

fn thread_scripts() -> String {
    json!({
        MAIN: [
            [
                { "method": "item/agentMessage/delta", "params": { "threadId": "THREAD_ID", "turnId": "t1", "itemId": "ip", "delta": "par" } }
            ],
            [
                { "method": "item/completed", "params": { "threadId": "THREAD_ID", "turnId": "t1", "item": { "id": "cp", "type": "commandExecution", "command": "ls", "status": "completed", "exitCode": 0 } } },
                { "method": "turn/completed", "params": { "threadId": "THREAD_ID", "turnId": "t1" } }
            ]
        ],
        CHILD: [
            [
                { "method": "item/agentMessage/delta", "params": { "threadId": "THREAD_ID", "turnId": "t1", "itemId": "ic", "delta": "kid" } },
                { "method": "item/completed", "params": { "threadId": "THREAD_ID", "turnId": "t1", "item": { "id": "cc", "type": "commandExecution", "command": "rm -rf x", "status": "completed", "exitCode": 0 } } },
                { "method": "item/agentMessage/delta", "params": { "threadId": 123, "thread": { "id": MAIN }, "turnId": "t1", "delta": "malformed" } },
                { "method": "item/agentMessage/delta", "params": { "turnId": "t1", "delta": "missing" } },
                { "method": "turn/completed", "params": { "threadId": "THREAD_ID", "turnId": "t1" } }
            ]
        ]
    })
    .to_string()
}

#[tokio::test]
async fn a_child_on_the_parents_turn_id_never_touches_the_bound_ingress_tracker() {
    let dir = tempdir("bound");
    let entered = dir.join("notifications-entered");
    let release = dir.join("notifications-release");
    let srv = CodexAppServer::start(SupervisorOpts {
        codex_exe: FAKE_BIN.to_string(),
        session_dir: dir.clone(),
        codex_home: Some(dir.join("codex-home")),
        model: None,
        bus_mcp: None,
        cwd: None,
        env: vec![
            ("FAKE_CODEX_REPLY_BEFORE_NOTIFICATIONS".into(), "1".into()),
            ("FAKE_CODEX_THREAD_SCRIPTS".into(), thread_scripts()),
            (
                "FAKE_CODEX_NOTIFICATION_ENTERED".into(),
                entered.to_string_lossy().into_owned(),
            ),
            (
                "FAKE_CODEX_NOTIFICATION_RELEASE".into(),
                release.to_string_lossy().into_owned(),
            ),
        ],
    })
    .await
    .expect("fake app-server should start");

    // One client: the main thread, the child thread attached on the same connection, then the
    // public bind that installs the bridge-owned ingress tracker and publishes the binding.
    let client = Arc::new(
        CodexAppServerClient::connect(srv.socket(), "nexus")
            .await
            .expect("nexus client should connect"),
    );
    let thread = client.thread_start().await.expect("thread_start");
    assert_eq!(thread, MAIN);
    client
        .thread_resume(CHILD)
        .await
        .expect("attach the child thread on the same connection");
    let session = SessionId("s_codex_child_ingress".into());
    let transport = CodexAppServerTransport::new();
    transport.bind(session.clone(), client.clone(), thread.clone());
    let sink = Arc::new(RecSink::default());
    let tools = Arc::new(ToolObsSink::default());
    let forwarder = spawn_codex_forwarder_scoped(
        session.clone(),
        client.clone(),
        sink.clone(),
        Arc::new(AutoApprove),
        transport.turn_tracker(),
        Some(tools.clone()),
        CodexThreadScope::bound(thread.clone(), None),
    );

    // Accept the parent's turn while the fake holds its notifications: NativeOpen, active.
    let acceptance = tokio::time::timeout(WATCHDOG, async {
        let (prompt, ()) = tokio::join!(
            transport.prompt(&session, "hold this turn".to_string()),
            async {
                let mut poll = tokio::time::interval(Duration::from_millis(5));
                while !entered.exists() {
                    poll.tick().await;
                }
            }
        );
        prompt
    })
    .await;
    assert!(
        matches!(&acceptance, Ok(Ok(()))),
        "parent prompt must be accepted while held; result={acceptance:?}; observation={:?}",
        transport.observe_turn(&session)
    );
    let accepted = transport.observe_turn(&session);
    assert_eq!(accepted.state, TurnState::NativeOpen);
    let accepted_stamp = accepted.stamp.clone().expect("owner stamp");
    assert_eq!(transport.active_turn_sessions(), vec![session.clone()]);
    std::fs::write(&release, b"release").expect("release notifications");
    let got = wait_for(&sink, "the parent's held text", |e| {
        parent_texts(e) == ["par"]
    })
    .await;
    assert_eq!(parent_kinds(&got), vec![AgentUpdateKind::Text]);

    // The child turn over the real socket, on the parent's turn id.
    let driver = CodexAppServerClient::connect(srv.socket(), "driver")
        .await
        .expect("driver client should connect");
    driver
        .turn_start(CHILD, "child go")
        .await
        .expect("child turn");
    let got = wait_for(&sink, "the child-lane terminal", |e| {
        child_turn_ends(e, CHILD) >= 1
    })
    .await;

    // Root and owner remain; the parent is still open and active; the child has no turn.
    let after_child = transport.observe_turn(&session);
    assert_eq!(after_child.state, TurnState::NativeOpen, "{after_child:?}");
    assert_eq!(
        after_child.stamp.as_ref().map(|s| s.owner.clone()),
        Some(accepted_stamp.owner.clone()),
        "the captured owner must survive the child turn"
    );
    assert_eq!(transport.active_turn_sessions(), vec![session.clone()]);
    assert_eq!(transport.turn_tracker().active_turn_id(CHILD), None);
    // Parent lane, tool observations and echo path untouched.
    assert_eq!(parent_kinds(&got), vec![AgentUpdateKind::Text]);
    assert_eq!(parent_texts(&got), vec!["par".to_string()]);
    assert!(
        tools.observations().is_empty(),
        "{:?}",
        tools.observations()
    );
    // Child lane: text, tool call, completion for the child; the shapeless frames unresolved.
    let mut child_kinds = Vec::new();
    let mut unresolved = Vec::new();
    for ev in child_events(&got) {
        let WsEvent::ChildAgentUpdate {
            child, kind, data, ..
        } = ev
        else {
            unreachable!()
        };
        assert_eq!(child.root, MAIN);
        if child.id.as_deref() == Some(CHILD) {
            child_kinds.push(*kind);
        } else {
            assert_eq!(child.id, None);
            assert_eq!(child.resolution, ChildResolution::Unresolved);
            unresolved.push(data["text"].as_str().unwrap_or("").to_string());
        }
    }
    assert_eq!(
        child_kinds,
        vec![
            AgentUpdateKind::Text,
            AgentUpdateKind::ToolCall,
            AgentUpdateKind::TurnEnd
        ]
    );
    unresolved.sort();
    assert_eq!(
        unresolved,
        vec!["malformed".to_string(), "missing".to_string()]
    );

    // The parent's own command item and terminal over the socket settle its turn.
    driver
        .turn_start(MAIN, "finish")
        .await
        .expect("parent finish");
    let got = wait_for(&sink, "the parent's terminal", |e| {
        parent_kinds(e).contains(&AgentUpdateKind::TurnEnd)
    })
    .await;
    assert_eq!(
        parent_kinds(&got),
        vec![
            AgentUpdateKind::Text,
            AgentUpdateKind::ToolCall,
            AgentUpdateKind::TurnEnd
        ]
    );
    let observations = tools.observations();
    assert!(
        observations
            .iter()
            .any(|(s, o)| s == &session && o.tool_call_id.as_deref() == Some("cp")),
        "{observations:?}"
    );
    let settled = tokio::time::timeout(WATCHDOG, async {
        let mut poll = tokio::time::interval(Duration::from_millis(5));
        loop {
            let observed = transport.observe_turn(&session);
            if observed.state == TurnState::VerifiedIdle
                && observed.stamp.as_ref().is_some_and(|stamp| {
                    stamp.owner == accepted_stamp.owner && stamp.revision > accepted_stamp.revision
                })
            {
                return observed;
            }
            poll.tick().await;
        }
    })
    .await;
    assert!(
        settled.is_ok(),
        "same-owner newer-revision VerifiedIdle expected; observation={:?}",
        transport.observe_turn(&session)
    );
    assert!(transport.active_turn_sessions().is_empty());
    assert_eq!(transport.turn_tracker().active_turn_id(MAIN), None);
    assert_eq!(transport.turn_tracker().active_turn_id(CHILD), None);
    forwarder.abort();
}
