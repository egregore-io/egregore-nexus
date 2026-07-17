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
