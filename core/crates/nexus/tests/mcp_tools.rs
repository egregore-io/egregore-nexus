use std::sync::Arc;
use std::time::{Duration, Instant};

use nexus::cli::commands::mcp::tools::{dispatch, registry};
use nexus::cli::read_client::ReadClient;
use nexus::cli::store_client::StoreClient;
use nexus::daemon::AppState;
use nexus_common::{now, Config};
use nexus_contracts::{
    BatchCounts, CreateThreadRequest, Kind, MessageId, NexusBatch, RegisterRequest, SendRequest,
    SendTarget, Tier,
};
use nexus_store::repos::{CommandIntentRow, CommandIntents};
use nexus_store::Store;
use serde_json::json;

async fn state() -> AppState {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    AppState::wire(store, &Config::default())
}

fn reg(name: &str, client_key: &str) -> RegisterRequest {
    RegisterRequest {
        agent_id: None,
        name: Some(name.into()),
        harness: hid("claude"),
        harness_session_id: format!("hs_{client_key}"),
        project: "egregore".into(),
        client_key: client_key.into(),
        runtime_credential: None,
        tier: Tier::Agent,
        kind: Some(Kind::Agent),
        role: None,
        cwd: None,
    }
}

fn read_client(store: Arc<Store>, name: &str, session_id: String) -> ReadClient {
    ReadClient::from_store_with_caller_for_tests(
        store,
        name,
        "egregore",
        Some(session_id),
        Some(format!("ck_{name}")),
        Tier::Agent,
    )
}

fn store_client(store: Arc<Store>) -> StoreClient {
    StoreClient::from_store_with_caller_for_tests(
        store,
        "ben",
        "egregore",
        Some("ck_ben".into()),
        Tier::Agent,
        Kind::Agent,
    )
}

async fn wait_for_nth_command(store: &Store, count: usize) -> CommandIntentRow {
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

async fn complete_empty_batch(store: Arc<Store>, count: usize) {
    let row = wait_for_nth_command(&store, count).await;
    let batch = NexusBatch {
        counts: BatchCounts {
            dms: 0,
            thread: 0,
            total: 0,
        },
        dms: vec![],
        threads: vec![],
        dm_message_ids: vec![],
        thread_message_ids: vec![],
        message_ids: Vec::<MessageId>::new(),
    };
    CommandIntents::new(&store)
        .mark_done(
            &row.command_id,
            &serde_json::to_string(&batch).unwrap(),
            now(),
        )
        .await
        .unwrap();
}

#[test]
fn registry_has_expected_tools() {
    let tools = registry();
    let names: Vec<&str> = tools.iter().map(|t| t.name).collect();
    assert!(names.contains(&"dm"), "missing dm");
    assert!(names.contains(&"post"), "missing post");
    assert!(names.contains(&"reply"), "missing reply");
    assert!(names.contains(&"publish"), "missing publish");
    assert!(names.contains(&"members"), "missing members");
    assert!(names.contains(&"rename"), "missing rename");
    assert!(names.contains(&"threads"), "missing threads");
    assert!(names.contains(&"read"), "missing read");
    assert!(names.contains(&"history"), "missing history");
    assert!(names.contains(&"search"), "missing search");
    assert!(names.contains(&"inbox"), "missing inbox");
    assert_eq!(
        names.len(),
        11,
        "expected exactly 11 tools, got {}",
        names.len()
    );
}

#[test]
fn local_stdio_mcp_registry_has_no_gateway_auth_surface() {
    let tools = registry();
    assert!(
        tools.iter().any(|tool| tool.name == "dm"),
        "local stdio MCP must keep exposing bus tools without network auth"
    );
    assert!(
        tools
            .iter()
            .all(|tool| tool.input_schema.get("authorization").is_none()
                && tool.input_schema.get("bearer").is_none()),
        "local stdio MCP tools must not grow bearer-token arguments"
    );
}

#[test]
fn every_tool_has_input_schema() {
    for tool in registry() {
        assert!(
            tool.input_schema.is_object(),
            "tool '{}' input_schema is not an object",
            tool.name
        );
        assert_eq!(
            tool.input_schema.get("type").and_then(|v| v.as_str()),
            Some("object"),
            "tool '{}' input_schema must have type=object",
            tool.name
        );
    }
}

#[test]
fn every_tool_has_non_empty_description() {
    for tool in registry() {
        assert!(
            !tool.description.is_empty(),
            "tool '{}' has empty description",
            tool.name
        );
    }
}

#[tokio::test]
async fn dispatch_unknown_tool_returns_not_found_error() {
    let result = dispatch(None, None, "nonsuch_tool", &json!({})).await;
    assert!(result.is_err(), "expected Err for unknown tool, got Ok");
    assert_eq!(
        result.unwrap_err().code,
        nexus_contracts::codes::NOT_FOUND,
        "expected NOT_FOUND code for unknown tool"
    );
}

#[tokio::test]
async fn dispatch_members_end_to_end() {
    let state = state().await;
    let ada = state.identity.register(reg("ada", "ada")).await.unwrap();
    state.identity.register(reg("ben", "ben")).await.unwrap();
    let read = read_client(state.store.clone(), "ada", ada.session_id.0);

    let result = dispatch(Some(&read), None, "members", &json!({})).await;
    assert!(result.is_ok(), "members dispatch failed: {:?}", result);
    let val = result.unwrap();
    assert!(
        val.get("members").is_some(),
        "expected members array: {val}"
    );
}

#[tokio::test]
async fn dispatch_threads_end_to_end() {
    let state = state().await;
    let ada = state.identity.register(reg("ada", "ada")).await.unwrap();
    let caller = state.identity.resolve("egregore", "ada").await.unwrap();
    state
        .bus
        .create_thread(
            &caller,
            CreateThreadRequest {
                name: "backend".into(),
                members: vec![],
            },
        )
        .await
        .unwrap();
    let read = read_client(state.store.clone(), "ada", ada.session_id.0);

    let result = dispatch(Some(&read), None, "threads", &json!({})).await;
    assert!(result.is_ok(), "threads dispatch failed: {:?}", result);
    let val = result.unwrap();
    assert!(
        val.get("threads").is_some(),
        "expected threads array: {val}"
    );
}

#[tokio::test]
async fn dispatch_read_returns_full_message_by_id() {
    let state = state().await;
    let ben = state.identity.register(reg("ben", "ck_ben")).await.unwrap();
    state.identity.register(reg("ada", "ck_ada")).await.unwrap();
    let caller = state.identity.resolve("egregore", "ben").await.unwrap();
    let ack = state
        .bus
        .send(
            &caller,
            SendRequest {
                to: SendTarget::dm_name("ada"),
                summary: None,
                body: "full body available after truncation".into(),
                mention: vec![],
                metadata: None,
                idempotency_key: None,
            },
        )
        .await
        .unwrap();
    let read = read_client(state.store.clone(), "ben", ben.session_id.0);

    let result = dispatch(
        Some(&read),
        None,
        "read",
        &json!({ "id": ack.message_id.0 }),
    )
    .await;

    assert!(result.is_ok(), "read dispatch failed: {:?}", result);
    let val = result.unwrap();
    assert_eq!(val["body"], "full body available after truncation");
    assert_eq!(val["id"], ack.message_id.0);
}

#[tokio::test]
async fn dispatch_inbox_end_to_end() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let store_cli = store_client(store.clone());

    let task = tokio::spawn({
        let store_cli = store_cli.clone();
        async move {
            dispatch(
                None,
                Some(&store_cli),
                "inbox",
                &json!({ "timeout_ms": 0, "max": 10 }),
            )
            .await
        }
    });
    complete_empty_batch(store, 1).await;

    let result = task.await.unwrap();
    assert!(result.is_ok(), "inbox dispatch failed: {:?}", result);
    let val = result.unwrap();
    assert!(
        val.get("counts").is_some(),
        "expected counts in NexusBatch: {val}"
    );
}

/// A validated [`nexus_contracts::HarnessId`] from a literal (panics on invalid — test-only).
fn hid(s: &str) -> nexus_contracts::HarnessId {
    nexus_contracts::HarnessId::new(s).expect("valid harness id literal")
}
