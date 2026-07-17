use std::sync::Arc;
use std::time::Duration;

use nexus_contracts::ids::SessionId;
use nexus_store::repos::{
    AgentRuntimes, CommandIntents, NewAgentRuntime, NewCommandIntent, NewSession, Sessions,
    StreamEvents,
};
use nexus_store::Store;

fn now() -> i64 {
    nexus_common::now()
}

async fn memory_store() -> Arc<Store> {
    let store = Store::open(":memory:").await.unwrap();
    store.migrate().await.unwrap();
    Arc::new(store)
}

#[tokio::test]
async fn command_intent_insert_signals_topic_before_poll_fallback() {
    let store = memory_store().await;
    let epoch = store.events().command_intent_inserted().epoch();
    let waiter_store = store.clone();
    let wait = tokio::spawn(async move {
        waiter_store
            .events()
            .command_intent_inserted()
            .wait_after(epoch)
            .await;
    });

    tokio::time::sleep(Duration::from_millis(5)).await;
    CommandIntents::new(&store)
        .insert_pending(NewCommandIntent {
            command_id: "cmd_events".into(),
            kind: "identity.register".into(),
            project: "default".into(),
            caller_name: "events-test".into(),
            caller_session_id: None,
            caller_agent_id: None,
            caller_runtime_id: None,
            caller_client_key: None,
            caller_kind: None,
            caller_tier: None,
            idempotency_key: None,
            request_json: "{}".into(),
            created_at: now(),
        })
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_millis(75), wait)
        .await
        .expect("command intent topic should wake before fallback poll")
        .unwrap();
}

#[tokio::test]
async fn stream_append_and_turn_end_signal_separate_topics() {
    let store = memory_store().await;
    let session = SessionId("s_events_stream".into());
    let stream_epoch = store.events().stream_row_appended().epoch();
    let turn_epoch = store.events().turn_end_appended().epoch();

    let streams = StreamEvents::new(&store);
    streams
        .append(&session, "text", r#"{"text":"hello"}"#)
        .await
        .unwrap();

    store
        .events()
        .stream_row_appended()
        .wait_after(stream_epoch)
        .await;
    assert_eq!(
        store.events().turn_end_appended().epoch(),
        turn_epoch,
        "non-turn rows must not signal the turn_end topic"
    );

    let turn_wait_epoch = store.events().turn_end_appended().epoch();
    streams.append(&session, "turn_end", "{}").await.unwrap();
    store
        .events()
        .turn_end_appended()
        .wait_after(turn_wait_epoch)
        .await;
}

#[tokio::test]
async fn session_and_runtime_lifecycle_writes_signal_topic() {
    let store = memory_store().await;
    let session = SessionId("s_events_lifecycle".into());
    let create_epoch = store.events().session_lifecycle_changed().epoch();

    Sessions::new(&store)
        .create(NewSession {
            session_id: session.clone(),
            name: Some("events-lifecycle".into()),
            agent: Some("codex".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: None,
            client_key: None,
            cwd: None,
            project: "default".into(),
            transport: Some("codex-appserver".into()),
        })
        .await
        .unwrap();
    store
        .events()
        .session_lifecycle_changed()
        .wait_after(create_epoch)
        .await;

    let runtime_epoch = store.events().session_lifecycle_changed().epoch();
    AgentRuntimes::new(&store)
        .create(NewAgentRuntime {
            runtime_id: session.0,
            agent_id: "a_events_lifecycle".into(),
            harness: "codex".into(),
            cwd: None,
            transport: Some("codex-appserver".into()),
            presence: Some("online".into()),
            active: true,
        })
        .await
        .unwrap();
    store
        .events()
        .session_lifecycle_changed()
        .wait_after(runtime_epoch)
        .await;
}
