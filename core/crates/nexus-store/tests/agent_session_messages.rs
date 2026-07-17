use nexus_contracts::SessionId;
use nexus_store::repos::{AgentSessionMessages, NewAgentSessionMessage, StreamEvents};
use nexus_store::Store;

async fn migrated() -> Store {
    let store = Store::open(":memory:").await.unwrap();
    store.migrate().await.unwrap();
    store
}

fn session() -> SessionId {
    SessionId("s_agent_history".to_string())
}

fn message(
    turn_id: &str,
    ordinal: i64,
    content: &str,
    status: &str,
    first_event: i64,
    last_event: i64,
    ts: i64,
) -> NewAgentSessionMessage {
    NewAgentSessionMessage {
        id: format!("{turn_id}_m_{ordinal}"),
        session_id: session().0,
        turn_id: turn_id.to_string(),
        ordinal,
        role: "assistant".to_string(),
        author: Some("bianca".to_string()),
        content_json: content.to_string(),
        status: status.to_string(),
        first_stream_event_id: first_event,
        last_stream_event_id: last_event,
        created_at: ts,
        updated_at: ts,
        finalized_at: None,
    }
}

#[tokio::test]
async fn materialized_turns_messages_cursors_and_pruning_are_idempotent() {
    let store = migrated().await;
    let session = session();
    let streams = StreamEvents::new(&store);
    let repo = AgentSessionMessages::new(&store);

    let first_event = streams
        .append(&session, "text", r#"{"text":"hel"}"#)
        .await
        .unwrap();
    let second_event = streams
        .append(&session, "text", r#"{"text":"lo"}"#)
        .await
        .unwrap();
    let end_event = streams.append(&session, "turn_end", r#"{}"#).await.unwrap();

    let turn = repo
        .begin_or_get_open_turn(&session, first_event, 10)
        .await
        .unwrap();
    let replayed_turn = repo
        .begin_or_get_open_turn(&session, first_event, 11)
        .await
        .unwrap();
    assert_eq!(turn.id, replayed_turn.id);
    assert_eq!(replayed_turn.first_stream_event_id, first_event);

    repo.upsert_message(message(
        &turn.id,
        0,
        r#"{"schema":1,"blocks":[{"type":"text","text":"hel"}],"metadata":{}}"#,
        "streaming",
        first_event,
        first_event,
        10,
    ))
    .await
    .unwrap();
    repo.upsert_message(message(
        &turn.id,
        0,
        r#"{"schema":1,"blocks":[{"type":"text","text":"hello"}],"metadata":{}}"#,
        "streaming",
        first_event,
        second_event,
        20,
    ))
    .await
    .unwrap();

    let messages = repo.messages_for_session(&session, 10).await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].last_stream_event_id, second_event);
    assert!(messages[0].content_json.contains("hello"));

    let finalized = repo
        .finalize_turn(&session, end_event, 30, "final")
        .await
        .unwrap()
        .expect("turn finalized");
    assert_eq!(finalized.status, "final");
    assert_eq!(finalized.finalized_at, Some(30));
    assert_eq!(finalized.last_stream_event_id, end_event);

    let finalized_again = repo
        .finalize_turn(&session, end_event, 31, "final")
        .await
        .unwrap()
        .expect("latest turn returned");
    assert_eq!(finalized_again.id, turn.id);

    let messages = repo.messages_for_session(&session, 10).await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].status, "final");
    assert_eq!(messages[0].finalized_at, Some(30));

    assert_eq!(repo.cursor_for_session(&session).await.unwrap(), None);
    repo.set_cursor(&session, second_event).await.unwrap();
    repo.set_cursor(&session, first_event).await.unwrap();
    assert_eq!(
        repo.cursor_for_session(&session).await.unwrap(),
        Some(second_event)
    );
    repo.set_cursor(&session, end_event).await.unwrap();
    assert_eq!(
        repo.cursor_for_session(&session).await.unwrap(),
        Some(end_event)
    );

    let deleted = repo
        .prune_stream_events_before(&session, end_event)
        .await
        .unwrap();
    assert_eq!(deleted, 2);
    let retained = streams.since(&session, 0).await.unwrap();
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].id, end_event);
}

#[tokio::test]
async fn session_messages_read_latest_limit_in_chronological_order() {
    let store = migrated().await;
    let session = session();
    let repo = AgentSessionMessages::new(&store);

    let first_turn = repo.begin_or_get_open_turn(&session, 1, 10).await.unwrap();
    repo.upsert_message(message(
        &first_turn.id,
        0,
        r#"{"text":"one"}"#,
        "streaming",
        1,
        1,
        10,
    ))
    .await
    .unwrap();
    repo.finalize_turn(&session, 2, 20, "final").await.unwrap();

    let second_turn = repo.begin_or_get_open_turn(&session, 3, 30).await.unwrap();
    repo.upsert_message(message(
        &second_turn.id,
        0,
        r#"{"text":"two"}"#,
        "streaming",
        3,
        3,
        30,
    ))
    .await
    .unwrap();
    repo.finalize_turn(&session, 4, 40, "final").await.unwrap();

    let third_turn = repo.begin_or_get_open_turn(&session, 5, 50).await.unwrap();
    repo.upsert_message(message(
        &third_turn.id,
        0,
        r#"{"text":"three"}"#,
        "streaming",
        5,
        5,
        50,
    ))
    .await
    .unwrap();

    let latest_two = repo.messages_for_session(&session, 2).await.unwrap();
    assert_eq!(latest_two.len(), 2);
    assert_eq!(latest_two[0].turn_id, second_turn.id);
    assert_eq!(latest_two[1].turn_id, third_turn.id);
    assert!(latest_two[0].content_json.contains("two"));
    assert!(latest_two[1].content_json.contains("three"));
}
