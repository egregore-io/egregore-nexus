use std::sync::Arc;

use nexus::daemon::agent_session_materializer::{materialize_agent_update, materialize_ws_event};
use nexus::WsSink;
use nexus_contracts::{AgentUpdateKind, EventSink, MessageId, SessionId, WsEvent};
use nexus_store::repos::{AgentSessionMessages, StreamEvents};
use nexus_store::Store;
use serde_json::{json, Value};

async fn migrated() -> Store {
    let store = Store::open(":memory:").await.unwrap();
    store.migrate().await.unwrap();
    store
}

fn session() -> SessionId {
    SessionId("s_materializer".to_string())
}

async fn messages(store: &Store) -> Vec<nexus_store::repos::AgentSessionMessageRow> {
    AgentSessionMessages::new(store)
        .messages_for_session(&session(), 20)
        .await
        .unwrap()
}

fn content(row: &nexus_store::repos::AgentSessionMessageRow) -> Value {
    serde_json::from_str(&row.content_json).unwrap()
}

async fn append_update(
    store: &Store,
    session: &SessionId,
    kind: AgentUpdateKind,
    data: Value,
) -> i64 {
    let kind_str = serde_json::to_value(kind)
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    let id = StreamEvents::new(store)
        .append(session, &kind_str, &data.to_string())
        .await
        .unwrap();
    materialize_agent_update(store, session, id, kind, &data)
        .await
        .unwrap();
    id
}

async fn main_stream_events_exists(store: &Store) -> bool {
    let mut rows = store
        .conn
        .query(
            "SELECT 1 FROM main.sqlite_master WHERE type = 'table' AND name = 'stream_events'",
            (),
        )
        .await
        .unwrap();
    rows.next().await.unwrap().is_some()
}

async fn table_count(store: &Store, table: &str) -> i64 {
    let mut rows = store
        .conn
        .query(&format!("SELECT COUNT(*) FROM {table}"), ())
        .await
        .unwrap();
    rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap()
}

#[tokio::test]
async fn text_chunks_materialize_once_and_finalize_on_turn_end() {
    let store = migrated().await;
    let session = session();

    append_update(
        &store,
        &session,
        AgentUpdateKind::Text,
        json!({ "text": "hel" }),
    )
    .await;
    append_update(
        &store,
        &session,
        AgentUpdateKind::Text,
        json!({ "text": "lo" }),
    )
    .await;

    assert_eq!(messages(&store).await.len(), 0);
    assert!(!main_stream_events_exists(&store).await);

    let end = append_update(&store, &session, AgentUpdateKind::TurnEnd, json!({})).await;

    let rows = messages(&store).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].role, "assistant");
    assert_eq!(rows[0].status, "final");
    assert_eq!(rows[0].finalized_at.is_some(), true);
    assert_eq!(
        content(&rows[0])["blocks"][0],
        json!({ "type": "text", "text": "hello" })
    );
    assert_eq!(
        AgentSessionMessages::new(&store)
            .cursor_for_session(&session)
            .await
            .unwrap(),
        Some(end)
    );
    assert!(StreamEvents::new(&store)
        .since(&session, 0)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn chatty_turn_stays_volatile_until_one_final_recorded_fold() {
    let store = migrated().await;
    let session = session();

    for index in 0..250 {
        append_update(
            &store,
            &session,
            AgentUpdateKind::Text,
            json!({ "text": format!("{index:03},") }),
        )
        .await;
    }

    assert_eq!(
        StreamEvents::new(&store)
            .since(&session, 0)
            .await
            .unwrap()
            .len(),
        250
    );
    assert_eq!(
        messages(&store).await.len(),
        0,
        "non-final deltas must not write per-token durable history rows"
    );
    assert_eq!(table_count(&store, "agent_session_turns").await, 0);
    assert_eq!(table_count(&store, "agent_session_messages").await, 0);
    assert!(!main_stream_events_exists(&store).await);

    append_update(&store, &session, AgentUpdateKind::TurnEnd, json!({})).await;

    let rows = messages(&store).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(table_count(&store, "agent_session_turns").await, 1);
    assert_eq!(table_count(&store, "agent_session_messages").await, 1);
    assert_eq!(rows[0].status, "final");
    assert_eq!(
        content(&rows[0])["blocks"][0]["text"]
            .as_str()
            .unwrap()
            .len(),
        250 * 4
    );
    assert!(StreamEvents::new(&store)
        .since(&session, 0)
        .await
        .unwrap()
        .is_empty());
    assert!(!main_stream_events_exists(&store).await);
}

#[tokio::test]
async fn turn_end_prunes_completed_session_rows_but_keeps_other_active_turns() {
    let store = migrated().await;
    let completed = session();
    let active = SessionId("s_other_active_turn".to_string());

    append_update(
        &store,
        &completed,
        AgentUpdateKind::Text,
        json!({ "text": "done" }),
    )
    .await;
    append_update(
        &store,
        &active,
        AgentUpdateKind::Text,
        json!({ "text": "still-active" }),
    )
    .await;
    append_update(&store, &completed, AgentUpdateKind::TurnEnd, json!({})).await;

    let completed_rows = StreamEvents::new(&store)
        .since(&completed, 0)
        .await
        .unwrap();
    assert!(
        completed_rows.is_empty(),
        "turn_end should prune the completed session's live rows"
    );
    let active_rows = StreamEvents::new(&store).since(&active, 0).await.unwrap();
    assert_eq!(active_rows.len(), 1);
    assert_eq!(active_rows[0].kind, "text");
    assert_eq!(active_rows[0].data, r#"{"text":"still-active"}"#);
    assert!(!main_stream_events_exists(&store).await);
}

#[tokio::test]
async fn turn_end_fold_rolls_back_when_cursor_update_fails() {
    let store = migrated().await;
    let session = session();
    let streams = StreamEvents::new(&store);

    streams
        .append(&session, "text", &json!({ "text": "partial" }).to_string())
        .await
        .unwrap();
    let end = streams.append(&session, "turn_end", "{}").await.unwrap();

    store
        .conn
        .execute(
            "CREATE TEMP TRIGGER fail_materializer_cursor_insert \
             BEFORE INSERT ON agent_session_stream_cursors \
             BEGIN SELECT RAISE(FAIL, 'cursor write failed'); END",
            (),
        )
        .await
        .unwrap();

    let err = materialize_agent_update(&store, &session, end, AgentUpdateKind::TurnEnd, &json!({}))
        .await
        .expect_err("cursor failure should fail materialization");
    assert!(
        err.to_string().contains("cursor write failed"),
        "unexpected materializer error: {err}"
    );

    store
        .conn
        .execute("DROP TRIGGER fail_materializer_cursor_insert", ())
        .await
        .unwrap();

    assert!(
        messages(&store).await.is_empty(),
        "failed turn_end must not leave finalized message rows"
    );
    assert_eq!(
        AgentSessionMessages::new(&store)
            .cursor_for_session(&session)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        StreamEvents::new(&store)
            .since(&session, 0)
            .await
            .unwrap()
            .len(),
        2,
        "raw turn rows must remain available for retry"
    );

    materialize_agent_update(&store, &session, end, AgentUpdateKind::TurnEnd, &json!({}))
        .await
        .expect("connection should remain usable after rollback");

    let rows = messages(&store).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status, "final");
    assert!(StreamEvents::new(&store)
        .since(&session, 0)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn turn_end_fold_fails_loud_inside_a_foreign_transaction_and_recovers() {
    // Contract since the 2026-07-09 savepoint ban: the turn-end fold opens its OWN
    // `BEGIN IMMEDIATE` (Hrana/sqld drops savepoints across round-trips; savepoint error paths
    // were the file-mode write-wedge class). A caller-held open transaction on the shared
    // connection must fail the fold loudly, leave the raw rows intact for retry, and the fold
    // must succeed once the connection is back in autocommit.
    let store = migrated().await;
    let session = session();
    let streams = StreamEvents::new(&store);

    streams
        .append(
            &session,
            "text",
            &json!({ "text": "nested-safe" }).to_string(),
        )
        .await
        .unwrap();
    let end = streams.append(&session, "turn_end", "{}").await.unwrap();

    store.conn.execute("BEGIN", ()).await.unwrap();
    let result =
        materialize_agent_update(&store, &session, end, AgentUpdateKind::TurnEnd, &json!({})).await;
    assert!(
        result.is_err(),
        "turn_end fold inside a foreign open transaction must fail loud, not nest or wedge"
    );
    store.conn.execute("ROLLBACK", ()).await.unwrap();

    assert!(
        messages(&store).await.is_empty(),
        "failed fold must not leave finalized message rows"
    );
    assert_eq!(
        StreamEvents::new(&store)
            .since(&session, 0)
            .await
            .unwrap()
            .len(),
        2,
        "raw turn rows must remain available for retry"
    );

    materialize_agent_update(&store, &session, end, AgentUpdateKind::TurnEnd, &json!({}))
        .await
        .expect("fold succeeds once the connection is back in autocommit");

    let rows = messages(&store).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        content(&rows[0])["blocks"][0],
        json!({ "type": "text", "text": "nested-safe" })
    );
    assert_eq!(
        AgentSessionMessages::new(&store)
            .cursor_for_session(&session)
            .await
            .unwrap(),
        Some(end)
    );
    assert!(StreamEvents::new(&store)
        .since(&session, 0)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn thinking_text_and_tool_call_are_reconciled_into_blocks() {
    let store = migrated().await;
    let session = session();

    append_update(
        &store,
        &session,
        AgentUpdateKind::Thinking,
        json!({ "text": "plan" }),
    )
    .await;
    append_update(
        &store,
        &session,
        AgentUpdateKind::Text,
        json!({ "text": "answer" }),
    )
    .await;
    append_update(
        &store,
        &session,
        AgentUpdateKind::ToolCall,
        json!({ "id": "tc_1", "tool": "shell", "title": "ls -la", "status": "in_progress", "input": { "cmd": "ls" }, "output": "a" }),
    )
    .await;
    append_update(
        &store,
        &session,
        AgentUpdateKind::ToolCall,
        json!({ "id": "tc_1", "status": "completed", "output": "ab" }),
    )
    .await;
    append_update(&store, &session, AgentUpdateKind::TurnEnd, json!({})).await;

    let rows = messages(&store).await;
    assert_eq!(rows.len(), 1);
    let blocks = content(&rows[0])["blocks"].clone();
    assert_eq!(blocks[0], json!({ "type": "thinking", "text": "plan" }));
    assert_eq!(blocks[1], json!({ "type": "text", "text": "answer" }));
    assert_eq!(blocks[2]["type"], "tool_call");
    assert_eq!(blocks[2]["id"], "tc_1");
    assert_eq!(blocks[2]["name"], "ls -la");
    assert_eq!(
        blocks[2]["tool"], "shell",
        "C-TOOL: the canonical machine name is persisted, distinct from the display title"
    );
    assert_eq!(blocks[2]["status"], "completed");
    assert_eq!(
        blocks[2]["input"],
        json!({ "cmd": "ls" }),
        "structured input persists as an OBJECT — replay must never re-stringify it"
    );
    assert_eq!(blocks[2]["argsJson"], "{\n  \"cmd\": \"ls\"\n}");
    assert_eq!(blocks[2]["output"], "ab");
}

#[tokio::test]
async fn native_text_item_continues_in_one_block_across_interleaved_tool_call() {
    let store = migrated().await;
    let session = session();

    append_update(
        &store,
        &session,
        AgentUpdateKind::Text,
        json!({ "text": "checking the", "itemId": "am1" }),
    )
    .await;
    append_update(
        &store,
        &session,
        AgentUpdateKind::ToolCall,
        json!({
            "id": "tc_1",
            "tool": "shell",
            "title": "find focused test",
            "status": "completed",
            "output": "focused.test.ts"
        }),
    )
    .await;
    append_update(
        &store,
        &session,
        AgentUpdateKind::Text,
        json!({ "text": " focused test", "itemId": "am1" }),
    )
    .await;
    append_update(&store, &session, AgentUpdateKind::TurnEnd, json!({})).await;

    let rows = messages(&store).await;
    assert_eq!(rows.len(), 1);
    let blocks = content(&rows[0])["blocks"].clone();
    assert_eq!(
        blocks[0],
        json!({
            "type": "text",
            "text": "checking the focused test",
            "itemId": "am1"
        })
    );
    assert_eq!(blocks[1]["type"], "tool_call");
    assert_eq!(blocks[1]["id"], "tc_1");
}

#[tokio::test]
async fn user_input_and_assistant_reply_use_separate_rows() {
    let store = migrated().await;
    let session = session();

    append_update(
        &store,
        &session,
        AgentUpdateKind::UserInput,
        json!({ "text": "hello agent" }),
    )
    .await;
    append_update(
        &store,
        &session,
        AgentUpdateKind::Text,
        json!({ "text": "hello human" }),
    )
    .await;
    append_update(&store, &session, AgentUpdateKind::TurnEnd, json!({})).await;

    let rows = messages(&store).await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].role, "user");
    assert_eq!(
        content(&rows[0])["blocks"][0],
        json!({ "type": "text", "text": "hello agent" })
    );
    assert_eq!(rows[1].role, "assistant");
    assert_eq!(
        content(&rows[1])["blocks"][0],
        json!({ "type": "text", "text": "hello human" })
    );
}

#[tokio::test]
async fn duplicate_user_input_text_in_same_turn_is_idempotent() {
    let store = migrated().await;
    let session = session();

    append_update(
        &store,
        &session,
        AgentUpdateKind::UserInput,
        json!({ "text": "hello agent" }),
    )
    .await;
    append_update(
        &store,
        &session,
        AgentUpdateKind::UserInput,
        json!({ "text": "hello agent" }),
    )
    .await;
    append_update(
        &store,
        &session,
        AgentUpdateKind::Text,
        json!({ "text": "hello human" }),
    )
    .await;
    append_update(&store, &session, AgentUpdateKind::TurnEnd, json!({})).await;

    let rows = messages(&store).await;
    assert_eq!(rows.len(), 2);
    assert_eq!(
        content(&rows[0])["blocks"],
        json!([{ "type": "text", "text": "hello agent" }])
    );
    assert_eq!(rows[1].role, "assistant");
}

#[tokio::test]
async fn duplicate_user_input_echo_with_missing_client_id_is_idempotent() {
    let store = migrated().await;
    let session = session();

    append_update(
        &store,
        &session,
        AgentUpdateKind::UserInput,
        json!({ "text": "hello agent", "clientMessageId": "you:test:1" }),
    )
    .await;
    append_update(
        &store,
        &session,
        AgentUpdateKind::UserInput,
        json!({ "text": "hello agent" }),
    )
    .await;
    append_update(&store, &session, AgentUpdateKind::TurnEnd, json!({})).await;

    let rows = messages(&store).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        content(&rows[0])["blocks"],
        json!([{ "type": "text", "text": "hello agent", "id": "you:test:1", "clientMessageId": "you:test:1" }])
    );
}

#[tokio::test]
async fn initial_prompt_user_input_preserves_selected_metadata() {
    let store = migrated().await;
    let session = session();

    append_update(
        &store,
        &session,
        AgentUpdateKind::UserInput,
        json!({
            "text": "You are Ada.",
            "clientMessageId": "initial-prompt:s_agent",
            "source": "initial_prompt",
            "name": "ada",
            "harness": "codex",
            "runtimeId": "s_agent"
        }),
    )
    .await;
    append_update(&store, &session, AgentUpdateKind::TurnEnd, json!({})).await;

    let rows = messages(&store).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].role, "user");
    assert_eq!(
        content(&rows[0])["blocks"],
        json!([{
            "type": "text",
            "text": "You are Ada.",
            "id": "initial-prompt:s_agent",
            "clientMessageId": "initial-prompt:s_agent",
            "source": "initial_prompt",
            "name": "ada",
            "harness": "codex",
            "runtimeId": "s_agent"
        }])
    );
}

#[tokio::test]
async fn non_agent_ws_events_do_not_materialize() {
    let store = migrated().await;
    materialize_ws_event(
        &store,
        1,
        &WsEvent::MessageCreated {
            message_id: MessageId("m_1".to_string()),
        },
    )
    .await
    .unwrap();

    assert_eq!(messages(&store).await.len(), 0);
}

#[tokio::test]
async fn ws_sink_appends_raw_stream_and_materializes_history() {
    let store = Arc::new(migrated().await);
    let sink = WsSink::new(16, Some(store.clone()));
    let session = session();

    sink.emit(WsEvent::AgentUpdate {
        session_id: session.clone(),
        kind: AgentUpdateKind::Text,
        data: json!({ "text": "streamed" }),
    })
    .await;

    let raw = StreamEvents::new(&store).since(&session, 0).await.unwrap();
    assert_eq!(raw.len(), 1);
    assert_eq!(raw[0].kind, "text");

    let rows = messages(&store).await;
    assert_eq!(rows.len(), 0);
    assert!(!main_stream_events_exists(&store).await);

    sink.emit(WsEvent::AgentUpdate {
        session_id: session.clone(),
        kind: AgentUpdateKind::TurnEnd,
        data: json!({}),
    })
    .await;

    let raw = StreamEvents::new(&store).since(&session, 0).await.unwrap();
    assert!(raw.is_empty());

    let rows = messages(&store).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status, "final");
    assert_eq!(
        content(&rows[0])["blocks"][0],
        json!({ "type": "text", "text": "streamed" })
    );
}
