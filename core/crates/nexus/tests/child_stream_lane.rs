//! Causal parent-isolation tests for the child lane through the real daemon sink.
//!
//! A `child_agent.update` must land only in `mem.child_stream_events` under the captured boot
//! epoch. The parent's `mem.stream_events`, the gateway publisher, the parent's turn state and
//! the durable `/agent` history must see nothing from it. Positive controls prove the same sink
//! still routes a parent update to every parent surface, including parent completion that
//! materializes durable history, so the isolation is discrimination, not a dead sink.

use std::sync::Arc;

use nexus::daemon::gateway_stream_socket::GatewayStreamPublisher;
use nexus::daemon::WsSink;
use nexus_common::now;
use nexus_contracts::{
    AgentUpdateKind, ChildResolution, ChildStream, EventSink, SessionId, WsEvent,
};
use nexus_store::repos::{
    ChildStreamEvents, DaemonState, LaneFilter, NewSession, Sessions, StreamEvents,
};
use nexus_store::Store;
use serde_json::json;
use tokio::sync::broadcast::error::TryRecvError;

const OWNER: &str = "s_owner";

async fn store_with_owner(boot_epoch: Option<&str>) -> Arc<Store> {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    if let Some(epoch) = boot_epoch {
        DaemonState::new(&store)
            .set_boot_epoch(epoch, now())
            .await
            .unwrap();
    }
    Sessions::new(&store)
        .create(NewSession {
            session_id: SessionId(OWNER.into()),
            name: Some("kestrel".into()),
            agent: Some("claude".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: None,
            client_key: Some("ck_owner".into()),
            cwd: None,
            project: "default".into(),
            transport: None,
        })
        .await
        .unwrap();
    store
}

fn child(id: &str, resolution: ChildResolution) -> ChildStream {
    ChildStream {
        harness: "claude".into(),
        root: "ses_root".into(),
        id: Some(id.into()),
        locator: format!("claude:subagents/{id}.jsonl#0"),
        parent: None,
        parent_ref: None,
        depth: None,
        resolution,
        evidence: Some("subagents_dir+sessionId+agentId".into()),
    }
}

fn child_update(kind: AgentUpdateKind, data: serde_json::Value, occurrence: u64) -> WsEvent {
    WsEvent::ChildAgentUpdate {
        session_id: SessionId(OWNER.into()),
        child: child("agent-abc", ChildResolution::RootVerified),
        kind,
        source_ref: format!("claude:agent-abc@uuid-first#{occurrence}"),
        data,
    }
}

fn parent_update(kind: AgentUpdateKind, data: serde_json::Value) -> WsEvent {
    WsEvent::AgentUpdate {
        session_id: SessionId(OWNER.into()),
        kind,
        data,
    }
}

async fn durable_history_rows(store: &Store) -> i64 {
    let mut rows = store
        .conn
        .query(
            "SELECT COUNT(*) FROM agent_session_messages WHERE session_id = ?1",
            libsql::params![OWNER.to_string()],
        )
        .await
        .unwrap();
    rows.next().await.unwrap().unwrap().get(0).unwrap()
}

async fn parent_lane_kinds(store: &Store) -> Vec<String> {
    StreamEvents::new(store)
        .since(&SessionId(OWNER.into()), 0)
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.kind)
        .collect()
}

async fn child_lane_len(store: &Store) -> usize {
    ChildStreamEvents::new(store)
        .page(&SessionId(OWNER.into()), &LaneFilter::default(), 0, 100)
        .await
        .unwrap()
        .rows
        .len()
}

#[tokio::test]
async fn child_completion_never_closes_the_open_parent_turn_and_parent_completion_still_does() {
    let store = store_with_owner(Some("boot_test")).await;
    let publisher = GatewayStreamPublisher::new(16);
    let mut frames = publisher.subscribe();
    let sink = WsSink::new(32, Some(store.clone())).with_gateway_stream(publisher);
    let mut notifications = sink.subscribe();

    // Parent turn opens: user input and a first text chunk reach the parent surfaces.
    sink.emit(parent_update(
        AgentUpdateKind::UserInput,
        json!({ "text": "delegate this" }),
    ))
    .await;
    sink.emit(parent_update(
        AgentUpdateKind::Text,
        json!({ "text": "on it" }),
    ))
    .await;
    assert_eq!(parent_lane_kinds(&store).await, vec!["user_input", "text"]);
    assert!(frames.try_recv().is_ok());
    assert!(frames.try_recv().is_ok());
    assert_eq!(notifications.try_recv().unwrap().method, "agent.update");
    assert_eq!(notifications.try_recv().unwrap().method, "agent.update");
    assert_eq!(durable_history_rows(&store).await, 0, "parent turn is open");

    // Child text, a child tool call, and a child completion while the parent turn is open.
    sink.emit(child_update(
        AgentUpdateKind::Text,
        json!({ "text": "child says" }),
        1,
    ))
    .await;
    sink.emit(child_update(
        AgentUpdateKind::ToolCall,
        json!({ "id": "toolu_1", "tool": "Read", "status": "in_progress" }),
        2,
    ))
    .await;
    sink.emit(child_update(AgentUpdateKind::TurnEnd, json!({}), 3))
        .await;

    // Parent lane unchanged, no gateway frame, parent turn still open, nothing materialized.
    assert_eq!(parent_lane_kinds(&store).await, vec!["user_input", "text"]);
    assert!(
        matches!(frames.try_recv(), Err(TryRecvError::Empty)),
        "child updates leaked into the gateway publisher"
    );
    assert_eq!(
        durable_history_rows(&store).await,
        0,
        "a child turn_end must not close the parent's open turn"
    );
    for _ in 0..3 {
        let note = notifications.try_recv().expect("child broadcast");
        assert_eq!(note.method, "child_agent.update");
        assert_eq!(note.params.as_ref().unwrap()["child"]["id"], "agent-abc");
    }

    // Child lane: all three, under the captured boot epoch, with their source refs.
    let page = ChildStreamEvents::new(&store)
        .page(&SessionId(OWNER.into()), &LaneFilter::default(), 0, 10)
        .await
        .unwrap();
    assert_eq!(page.rows.len(), 3);
    assert!(page.rows.iter().all(|row| row.epoch == "boot_test"));
    assert!(page.rows.iter().all(|row| row.child_key == "n:agent-abc"));
    assert_eq!(
        page.rows
            .iter()
            .map(|row| row.kind.as_str())
            .collect::<Vec<_>>(),
        vec!["text", "tool_call", "turn_end"]
    );
    assert_eq!(page.rows[2].source_ref, "claude:agent-abc@uuid-first#3");

    // Positive control: the parent's own completion closes the turn and materializes history.
    sink.emit(parent_update(AgentUpdateKind::TurnEnd, json!({})))
        .await;
    assert!(
        frames.try_recv().is_ok(),
        "parent turn_end reaches the gateway publisher"
    );
    assert_eq!(notifications.try_recv().unwrap().method, "agent.update");
    assert!(
        durable_history_rows(&store).await >= 2,
        "parent completion materializes the user and assistant messages"
    );
    // The parent completion left the child lane exactly as it was.
    assert_eq!(child_lane_len(&store).await, 3);
}

#[tokio::test]
async fn an_unresolved_child_update_is_kept_apart_from_the_parent_too() {
    let store = store_with_owner(Some("boot_test")).await;
    let sink = WsSink::new(32, Some(store.clone()));
    sink.emit(WsEvent::ChildAgentUpdate {
        session_id: SessionId(OWNER.into()),
        child: ChildStream {
            harness: "codex".into(),
            root: "thread_main".into(),
            id: None,
            locator: "codex:item/agentMessage/delta#41".into(),
            parent: None,
            parent_ref: None,
            depth: None,
            resolution: ChildResolution::Unresolved,
            evidence: None,
        },
        kind: AgentUpdateKind::Text,
        source_ref: "codex:item/agentMessage/delta@conn-1#41".into(),
        data: json!({ "text": "who owns this" }),
    })
    .await;
    assert!(parent_lane_kinds(&store).await.is_empty());
    let lanes = ChildStreamEvents::new(&store)
        .lanes(&SessionId(OWNER.into()), None, 10)
        .await
        .unwrap()
        .lanes;
    assert_eq!(lanes.len(), 1);
    assert_eq!(lanes[0].child_key, "l:codex:item/agentMessage/delta#41");
    assert_eq!(lanes[0].child.resolution, ChildResolution::Unresolved);
    assert_eq!(lanes[0].live_rows, 1);
}

#[tokio::test]
async fn without_a_captured_boot_epoch_the_child_update_is_not_recorded_under_a_fake_one() {
    let store = store_with_owner(None).await;
    let sink = WsSink::new(32, Some(store.clone()));
    let mut notifications = sink.subscribe();
    sink.emit(child_update(
        AgentUpdateKind::Text,
        json!({ "text": "child says" }),
        1,
    ))
    .await;
    // Nothing recorded anywhere in the store: no row under a placeholder epoch, no lane.
    assert_eq!(child_lane_len(&store).await, 0);
    assert!(ChildStreamEvents::new(&store)
        .lanes(&SessionId(OWNER.into()), None, 10)
        .await
        .unwrap()
        .lanes
        .is_empty());
    assert!(parent_lane_kinds(&store).await.is_empty());
    // The live broadcast still carries the event, as it does for a failed parent buffer write.
    assert_eq!(
        notifications.try_recv().unwrap().method,
        "child_agent.update"
    );

    // Once the daemon records its boot epoch, the same sink captures it and records under it.
    DaemonState::new(&store)
        .set_boot_epoch("boot_late", now())
        .await
        .unwrap();
    sink.emit(child_update(
        AgentUpdateKind::Text,
        json!({ "text": "child again" }),
        2,
    ))
    .await;
    let page = ChildStreamEvents::new(&store)
        .page(&SessionId(OWNER.into()), &LaneFilter::default(), 0, 10)
        .await
        .unwrap();
    assert_eq!(page.rows.len(), 1);
    assert_eq!(page.rows[0].epoch, "boot_late");
}
