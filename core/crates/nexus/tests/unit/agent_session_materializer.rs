use std::sync::Arc;
use std::time::Duration;

use nexus_store::repos::StreamEvents;
use tokio::sync::{Mutex as TokioMutex, Notify, OwnedMutexGuard, Semaphore};

use super::*;
use std::sync::OnceLock;

static MATERIALIZER_TEST_LOCK: OnceLock<Arc<TokioMutex<()>>> = OnceLock::new();

async fn materializer_test_guard() -> OwnedMutexGuard<()> {
    MATERIALIZER_TEST_LOCK
        .get_or_init(|| Arc::new(TokioMutex::new(())))
        .clone()
        .lock_owned()
        .await
}

async fn migrated() -> Store {
    let store = Store::open(":memory:").await.unwrap();
    store.migrate().await.unwrap();
    store
}

#[test]
fn turn_end_fold_stays_on_the_canonical_pinned_write_transaction() {
    let source = include_str!("../../src/daemon/agent_session_materializer.rs");
    let fold = function_body(source, "commit_turn_end_fold");
    assert!(
        fold.contains("begin_write_txn"),
        "the fold must use Store::begin_write_txn so Hrana gets a private pinned connection"
    );
    assert!(
        !fold.contains("MaterializerSavepoint"),
        "the bespoke shared-connection transaction must not return"
    );
    for function in [
        "begin_or_get_open_turn_in_write_txn",
        "upsert_materialized_message_in_write_txn",
        "finalize_turn_in_write_txn",
        "set_cursor_in_write_txn",
    ] {
        let body = function_body(source, function);
        assert!(
            !body.contains("store.conn"),
            "{function} must use the pinned WriteTxn connection"
        );
    }
}

#[tokio::test]
async fn non_terminal_stream_updates_do_not_touch_the_durable_cursor() {
    let store = migrated().await;
    let session = SessionId("s_materializer_live_delta".to_string());
    let stream_event_id = StreamEvents::new(&store)
        .append(&session, "text", &json!({ "text": "partial" }).to_string())
        .await
        .unwrap();

    // A live delta belongs only to the volatile stream lane. Removing the durable cursor table
    // makes any accidental durable read observable without coupling this regression to timing
    // or a particular sqld implementation.
    store
        .conn
        .execute("DROP TABLE agent_session_stream_cursors", ())
        .await
        .unwrap();

    materialize_agent_update(
        &store,
        &session,
        stream_event_id,
        AgentUpdateKind::Text,
        &json!({ "text": "partial" }),
    )
    .await
    .expect("non-terminal deltas must remain entirely on the volatile stream lane");
}

#[tokio::test]
async fn aborting_turn_end_fold_after_transaction_start_leaves_writer_usable() {
    let _test_guard = materializer_test_guard().await;
    let store = Arc::new(migrated().await);
    let session = SessionId("s_materializer_abort".to_string());
    let streams = StreamEvents::new(&store);

    streams
        .append(&session, "text", &json!({ "text": "partial" }).to_string())
        .await
        .unwrap();
    let end = streams.append(&session, "turn_end", "{}").await.unwrap();

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Semaphore::new(0));
    let _guard =
        test_hooks::pause_after_next_transaction(&session, entered.clone(), release.clone());

    let task_store = store.clone();
    let task_session = session.clone();
    let fold = tokio::spawn(async move {
        materialize_agent_update(
            &task_store,
            &task_session,
            end,
            AgentUpdateKind::TurnEnd,
            &json!({}),
        )
        .await
    });

    tokio::time::timeout(Duration::from_secs(1), entered.notified())
        .await
        .expect("fold should reach the transaction pause");
    fold.abort();
    let _ = fold.await;

    wait_for_begin_immediate(&store).await;
    store.conn.execute("ROLLBACK", ()).await.unwrap();
}

#[tokio::test]
async fn concurrent_turn_end_folds_wait_for_the_open_transaction() {
    let _test_guard = materializer_test_guard().await;
    let store = Arc::new(migrated().await);
    let first_session = SessionId("s_materializer_first".to_string());
    let second_session = SessionId("s_materializer_second".to_string());
    let streams = StreamEvents::new(&store);

    streams
        .append(
            &first_session,
            "text",
            &json!({ "text": "first" }).to_string(),
        )
        .await
        .unwrap();
    let first_end = streams
        .append(&first_session, "turn_end", "{}")
        .await
        .unwrap();
    streams
        .append(
            &second_session,
            "text",
            &json!({ "text": "second" }).to_string(),
        )
        .await
        .unwrap();
    let second_end = streams
        .append(&second_session, "turn_end", "{}")
        .await
        .unwrap();

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Semaphore::new(0));
    let _guard =
        test_hooks::pause_after_next_transaction(&first_session, entered.clone(), release.clone());

    let first_store = store.clone();
    let first = tokio::spawn({
        let first_session = first_session.clone();
        async move {
            materialize_agent_update(
                &first_store,
                &first_session,
                first_end,
                AgentUpdateKind::TurnEnd,
                &json!({}),
            )
            .await
        }
    });

    tokio::time::timeout(Duration::from_secs(1), entered.notified())
        .await
        .expect("first fold should reach the transaction pause");

    let second_store = store.clone();
    let mut second = tokio::spawn({
        let second_session = second_session.clone();
        async move {
            materialize_agent_update(
                &second_store,
                &second_session,
                second_end,
                AgentUpdateKind::TurnEnd,
                &json!({}),
            )
            .await
        }
    });

    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut second)
            .await
            .is_err(),
        "a concurrent fold must not enter/finish while another fold owns the write transaction"
    );

    release.add_permits(1);
    first.await.unwrap().unwrap();
    second.await.unwrap().unwrap();
    wait_for_begin_immediate(&store).await;
    store.conn.execute("ROLLBACK", ()).await.unwrap();
}

async fn wait_for_begin_immediate(store: &Store) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    let mut last_err = None;
    loop {
        match store.conn.execute("BEGIN IMMEDIATE", ()).await {
            Ok(_) => return,
            Err(err) if tokio::time::Instant::now() < deadline => {
                last_err = Some(err);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(err) => {
                panic!(
                    "aborted fold must not strand an open transaction; last errors: {:?}, {err}",
                    last_err
                );
            }
        }
    }
}

fn function_body<'a>(source: &'a str, name: &str) -> &'a str {
    let signature = format!("fn {name}");
    let start = source.find(&signature).expect("function signature");
    let after_signature = &source[start..];
    let body_start = after_signature.find('{').expect("function body start");
    let mut depth = 0usize;
    for (index, ch) in after_signature[body_start..].char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return &after_signature[..body_start + index + ch.len_utf8()];
                }
            }
            _ => {}
        }
    }
    panic!("function body end")
}

mod hermes_tool_output_contract {
    use super::*;
    use nexus_agent::adapter::hermes::native::{
        translate_message_row, HermesForwardState, HermesMessageRow,
    };

    const CALL_ID: &str = "call_hermes_output_contract";

    fn input() -> Value {
        json!({ "command": "printf hello", "timeout": 10 })
    }

    fn native_events(result: &str) -> Vec<(AgentUpdateKind, Value)> {
        let opening = HermesMessageRow {
            id: 1,
            session_id: "hermes_output_contract_fixture".to_string(),
            role: "assistant".to_string(),
            content: None,
            tool_call_id: None,
            tool_calls: Some(json!([{
                "id": CALL_ID,
                "type": "function",
                "function": { "name": "terminal", "arguments": input().to_string() },
            }])),
            tool_name: None,
            timestamp: 1.0,
            finish_reason: Some("tool_calls".to_string()),
            active: true,
        };
        let terminal = HermesMessageRow {
            id: 2,
            role: "tool".to_string(),
            content: Some(result.to_string()),
            tool_call_id: Some(CALL_ID.to_string()),
            tool_calls: None,
            tool_name: Some("terminal".to_string()),
            timestamp: 2.0,
            finish_reason: None,
            ..opening.clone()
        };
        let mut state = HermesForwardState::default();
        let mut events = translate_message_row(&opening, &mut state);
        events.extend(translate_message_row(&terminal, &mut state));
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].kind, AgentUpdateKind::ToolCall);
        assert_eq!(events[0].data["status"], "in_progress");
        assert_eq!(events[1].kind, AgentUpdateKind::ToolCall);
        assert_eq!(events[1].data["id"], CALL_ID);
        assert_eq!(events[1].data["status"], "completed");
        events
            .into_iter()
            .map(|event| (event.kind, event.data))
            .collect()
    }

    async fn materialized_tool(events: &[(AgentUpdateKind, Value)]) -> Value {
        let store = migrated().await;
        let session = SessionId("s_hermes_output_contract_fixture".to_string());
        let streams = StreamEvents::new(&store);
        for (kind, data) in events {
            let kind_json = serde_json::to_value(kind).unwrap();
            let id = streams
                .append(&session, kind_json.as_str().unwrap(), &data.to_string())
                .await
                .unwrap();
            materialize_agent_update(&store, &session, id, *kind, data)
                .await
                .unwrap();
        }
        let end = streams.append(&session, "turn_end", "{}").await.unwrap();
        materialize_agent_update(&store, &session, end, AgentUpdateKind::TurnEnd, &json!({}))
            .await
            .unwrap();

        let messages = AgentSessionMessages::new(&store)
            .messages_for_session(&session, 20)
            .await
            .unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].role, "assistant");
        assert_eq!(messages[0].status, "final");
        let content: Value = serde_json::from_str(&messages[0].content_json).unwrap();
        let blocks = content["blocks"].as_array().unwrap();
        assert_eq!(blocks.len(), 1, "opening and result must merge by call id");
        let block = blocks[0].clone();
        assert_eq!(block["type"], "tool_call");
        assert_eq!(block["id"], CALL_ID);
        assert_eq!(block["tool"], "terminal");
        assert_eq!(block["input"], input());
        assert_eq!(block["status"], "completed");
        block
    }

    async fn assert_native_output(result: &str, expected: &str) {
        let events = native_events(result);
        let block = materialized_tool(&events).await;
        assert_eq!(
            block.get("output"),
            Some(&json!(expected)),
            "native tool result must survive the durable fold; emitted result: {}",
            events[1].1,
        );
    }

    #[tokio::test]
    async fn native_text_result_survives_materialization() {
        assert_native_output("hello\n", "hello\n").await;
    }

    #[tokio::test]
    async fn native_empty_result_remains_explicit_after_materialization() {
        assert_native_output("", "").await;
    }

    #[tokio::test]
    async fn native_json_result_survives_materialization() {
        let result = json!({ "stdout": "hello\n", "exit_code": 0, "lines": ["hello"] });
        assert_native_output(
            &result.to_string(),
            &serde_json::to_string_pretty(&result).unwrap(),
        )
        .await;
    }

    fn canonical_events(result_key: &str, output: Option<Value>) -> Vec<(AgentUpdateKind, Value)> {
        let mut terminal = json!({ "id": CALL_ID, "status": "completed" });
        if let Some(output) = output {
            terminal[result_key] = output;
        }
        vec![
            (
                AgentUpdateKind::ToolCall,
                json!({
                    "id": CALL_ID,
                    "tool": "terminal",
                    "title": "terminal",
                    "input": input(),
                    "status": "in_progress",
                }),
            ),
            (AgentUpdateKind::ToolCall, terminal),
        ]
    }

    #[tokio::test]
    async fn canonical_result_keys_preserve_text_empty_and_json_outputs() {
        let structured = json!({ "stdout": "hello\n", "exit_code": 0, "lines": ["hello"] });
        for key in ["output", "content"] {
            for (result, expected) in [
                (json!("hello\n"), "hello\n".to_string()),
                (json!(""), String::new()),
                (
                    structured.clone(),
                    serde_json::to_string_pretty(&structured).unwrap(),
                ),
            ] {
                let block = materialized_tool(&canonical_events(key, Some(result))).await;
                assert_eq!(block.get("output"), Some(&json!(expected)), "key: {key}");
            }
        }
    }

    #[tokio::test]
    async fn canonical_absent_result_does_not_invent_empty_output() {
        let block = materialized_tool(&canonical_events("output", None)).await;
        assert!(block.get("output").is_none());
    }
}
