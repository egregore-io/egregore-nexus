use std::sync::Arc;
use std::time::{Duration, Instant};

use nexus::cli::commands::mcp::tools;
use nexus::cli::store_client::StoreClient;
use nexus_contracts::{Ack, Kind, MessageId, SendRequest, SendTarget, Tier};
use nexus_store::command_kinds;
use nexus_store::repos::CommandIntents;
use nexus_store::Store;
use serde_json::json;

async fn migrated_store() -> Arc<Store> {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    store
}

fn store_client(store: Arc<Store>) -> StoreClient {
    store_client_for(store, "ada", "ck_ada")
}

fn store_client_for(store: Arc<Store>, name: &str, client_key: &str) -> StoreClient {
    StoreClient::from_store_with_caller_for_tests(
        store,
        name,
        "demo",
        Some(client_key.into()),
        Tier::Agent,
        Kind::Agent,
    )
}

async fn wait_for_nth_command(store: &Store, count: usize) -> nexus_store::repos::CommandIntentRow {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let mut rows = store
            .conn
            .query(
                "SELECT command_id FROM command_intents ORDER BY created_at, command_id",
                (),
            )
            .await
            .unwrap();
        let mut ids = Vec::new();
        while let Some(row) = rows.next().await.unwrap() {
            ids.push(row.get::<String>(0).unwrap());
        }
        drop(rows);
        if ids.len() >= count {
            return CommandIntents::new(store)
                .get(&ids[count - 1])
                .await
                .unwrap()
                .unwrap();
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for command row"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn complete_next(store: Arc<Store>, count: usize) {
    let row = wait_for_nth_command(&store, count).await;
    let ack = Ack {
        message_id: MessageId(format!("m_mcp_{count}")),
        fanout: Some(count as u32),
    };
    CommandIntents::new(&store)
        .mark_done(
            &row.command_id,
            &serde_json::to_string(&ack).unwrap(),
            count as i64,
        )
        .await
        .unwrap();
}

async fn request_at(store: &Store, count: usize) -> SendRequest {
    let row = wait_for_nth_command(store, count).await;
    assert_eq!(row.kind, command_kinds::message_post::SEND);
    serde_json::from_str(&row.request_json).unwrap()
}

async fn command_count(store: &Store) -> usize {
    let mut rows = store
        .conn
        .query("SELECT COUNT(*) FROM command_intents", ())
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    row.get::<i64>(0).unwrap() as usize
}

#[tokio::test]
async fn mcp_dm_post_reply_publish_submit_message_post_send_intents() {
    let store = migrated_store().await;
    let store_cli = store_client(store.clone());

    let calls = [
        ("dm", json!({ "to": "ben", "message": "hello" })),
        (
            "post",
            json!({ "thread": "backend", "message": "plan", "mention": ["ben"] }),
        ),
        ("reply", json!({ "message": "on it" })),
        ("publish", json!({ "topic": "ci", "message": "green" })),
    ];

    for (idx, (name, args)) in calls.into_iter().enumerate() {
        let task = tokio::spawn({
            let store_cli = store_cli.clone();
            async move { tools::dispatch(None, Some(&store_cli), name, &args).await }
        });
        complete_next(store.clone(), idx + 1).await;
        assert!(task.await.unwrap().is_ok());
    }

    assert!(matches!(
        request_at(&store, 1).await.to,
        SendTarget::Dm { ref name, .. } if name.as_deref() == Some("ben")
    ));
    assert!(matches!(
        request_at(&store, 2).await.to,
        SendTarget::Post { ref thread } if thread == "backend"
    ));
    assert!(matches!(request_at(&store, 3).await.to, SendTarget::Reply));
    assert!(matches!(
        request_at(&store, 4).await.to,
        SendTarget::Publish { ref topic } if topic == "ci"
    ));
}

#[tokio::test]
async fn mcp_message_post_tools_reject_empty_or_whitespace_body_before_enqueue() {
    let store = migrated_store().await;
    let store_cli = store_client(store.clone()).with_timeout(Duration::from_millis(1));

    let cases = [
        ("dm", json!({ "to": "ben", "message": "" })),
        ("post", json!({ "thread": "backend", "message": " \n\t " })),
        ("reply", json!({ "message": "\n" })),
    ];
    for (name, args) in cases {
        let err = tools::dispatch(None, Some(&store_cli), name, &args)
            .await
            .unwrap_err();
        assert_eq!(err.code, nexus_contracts::codes::INVALID_PARAMS);
        assert!(err.message.contains("body"));
    }

    assert_eq!(command_count(&store).await, 0);
}

#[tokio::test]
async fn mcp_message_post_tools_return_ack_json_content() {
    let store = migrated_store().await;
    let store_cli = store_client(store.clone());

    let task = tokio::spawn({
        let store_cli = store_cli.clone();
        async move {
            tools::dispatch(
                None,
                Some(&store_cli),
                "dm",
                &json!({ "to": "ben", "message": "hello" }),
            )
            .await
        }
    });
    complete_next(store.clone(), 1).await;

    let value = task.await.unwrap().unwrap();
    assert_eq!(value["messageId"], "m_mcp_1");
    assert!(
        value.get("fanout").is_none(),
        "the model-facing MCP receipt must not expose internal recipient counts"
    );
}

#[tokio::test]
async fn mcp_message_post_tools_submit_through_store_ingress() {
    let store = migrated_store().await;
    let store_cli = store_client(store.clone());

    let task = tokio::spawn({
        let store_cli = store_cli.clone();
        async move {
            tools::dispatch(
                None,
                Some(&store_cli),
                "publish",
                &json!({ "topic": "ci", "message": "green" }),
            )
            .await
        }
    });
    complete_next(store.clone(), 1).await;

    assert!(task.await.unwrap().is_ok());
    let row = wait_for_nth_command(&store, 1).await;
    assert_eq!(row.project, "demo");
    assert_eq!(row.caller_name, "ada");
    assert_eq!(row.caller_client_key.as_deref(), Some("ck_ada"));
    assert_eq!(row.caller_kind.as_deref(), Some("local.agent"));
    assert_eq!(row.caller_tier.as_deref(), Some("agent"));
}

#[tokio::test]
async fn mcp_message_post_retry_with_same_idempotency_key_reuses_command_row() {
    let store = migrated_store().await;
    let store_cli = store_client(store.clone());

    let first = tokio::spawn({
        let store_cli = store_cli.clone();
        async move {
            tools::dispatch_with_idempotency(
                None,
                Some(&store_cli),
                "dm",
                &json!({ "to": "ben", "message": "hello" }),
                Some("mcp:ck_ada:dm:request-1"),
            )
            .await
        }
    });
    let second = tokio::spawn({
        let store_cli = store_cli.clone();
        async move {
            tools::dispatch_with_idempotency(
                None,
                Some(&store_cli),
                "dm",
                &json!({ "to": "ben", "message": "hello" }),
                Some("mcp:ck_ada:dm:request-1"),
            )
            .await
        }
    });

    complete_next(store.clone(), 1).await;

    let first = first.await.unwrap().unwrap();
    let second = second.await.unwrap().unwrap();
    assert_eq!(first["messageId"], "m_mcp_1");
    assert_eq!(second["messageId"], "m_mcp_1");

    let count = store
        .conn
        .query("SELECT COUNT(*) FROM command_intents", ())
        .await
        .unwrap()
        .next()
        .await
        .unwrap()
        .unwrap()
        .get::<i64>(0)
        .unwrap();
    assert_eq!(count, 1);
    let row = wait_for_nth_command(&store, 1).await;
    assert_eq!(
        row.idempotency_key.as_deref(),
        Some("mcp:ck_ada:dm:request-1")
    );
}

#[tokio::test]
async fn mcp_idempotency_keys_are_scoped_to_the_registered_caller() {
    let store = migrated_store().await;
    let ada = store_client_for(store.clone(), "ada", "ck_ada");
    let ben = store_client_for(store.clone(), "ben", "ck_ben");

    let first = tokio::spawn({
        let ada = ada.clone();
        async move {
            tools::dispatch_with_idempotency(
                None,
                Some(&ada),
                "post",
                &json!({ "thread": "ops", "message": "ada" }),
                Some("mcp:ck_ada:post:7"),
            )
            .await
        }
    });
    let second = tokio::spawn({
        let ben = ben.clone();
        async move {
            tools::dispatch_with_idempotency(
                None,
                Some(&ben),
                "post",
                &json!({ "thread": "ops", "message": "ben" }),
                Some("mcp:ck_ben:post:7"),
            )
            .await
        }
    });

    complete_next(store.clone(), 1).await;
    complete_next(store.clone(), 2).await;

    first.await.unwrap().unwrap();
    second.await.unwrap().unwrap();

    let count = store
        .conn
        .query("SELECT COUNT(*) FROM command_intents", ())
        .await
        .unwrap()
        .next()
        .await
        .unwrap()
        .unwrap()
        .get::<i64>(0)
        .unwrap();
    assert_eq!(count, 2);
    let first = wait_for_nth_command(&store, 1).await;
    let second = wait_for_nth_command(&store, 2).await;
    assert_eq!(first.idempotency_key.as_deref(), Some("mcp:ck_ada:post:7"));
    assert_eq!(second.idempotency_key.as_deref(), Some("mcp:ck_ben:post:7"));
}

#[tokio::test]
async fn mcp_message_post_missing_arguments_still_return_invalid_params() {
    let store = migrated_store().await;
    let store_cli = store_client(store);

    let err = tools::dispatch(None, Some(&store_cli), "dm", &json!({ "message": "hello" }))
        .await
        .unwrap_err();
    assert_eq!(err.code, nexus_contracts::codes::INVALID_PARAMS);
    assert!(err.message.contains("'to'"));
}
