//! Signed source-push crash-reclaim acceptance.
//!
//! The gateway verifies HMAC before writing a `source.push` command intent and stores a stable
//! idempotency key on that durable command. This test begins at that durable boundary, drives the
//! production command worker through the canonical bus transaction, cold-agent wake, actual ACP
//! injection, and terminal completion, then reclaims the same command as if the daemon died after
//! the bus commit but before command completion.

use std::sync::Arc;
use std::time::{Duration, Instant};

use nexus::daemon::{command_worker, AppState};
use nexus_agent::adapter::engine::HarnessCommand;
use nexus_agent::{Adapter, AdapterRegistry};
use nexus_common::Config;
use nexus_contracts::{Kind, PushRequest, RegisterRequest, SpawnRequest, Tier};
use nexus_harness_claude::ClaudeAdapter;
use nexus_store::command_kinds;
use nexus_store::repos::{CommandIntents, NewCommandIntent, Sources, Topics};
use nexus_store::Store;

const FAKE_HARNESS: &str = env!("CARGO_BIN_EXE_acceptance_fake_acp_agent");
const PROJECT: &str = "source-reclaim-project";
const SOURCE: &str = "signed-ci";
const TOPIC: &str = "signed-builds";
const COMMAND_ID: &str = "cmd_signed_source_reclaim";
const COMMAND_KEY: &str = "source-push:signed-ci:1777777777000:sha256=fixture";
const MARKER: &str = "SIGNED-SOURCE-RECLAIM-MARKER-91";

fn fake_command() -> HarnessCommand {
    HarnessCommand {
        program: FAKE_HARNESS.to_string(),
        args: vec![],
        cwd: None,
        env: vec![],
    }
}

async fn state_against_fake_acp() -> AppState {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let mut registry = AdapterRegistry::new();
    registry.register(
        &hid("claude"),
        Arc::new(|_ctx| Arc::new(ClaudeAdapter::with_command(fake_command())) as Arc<dyn Adapter>),
    );
    AppState::wire_with_registry(store, &Config::default(), registry)
}

fn operator_request() -> RegisterRequest {
    RegisterRequest {
        agent_id: None,
        name: Some("operator".into()),
        harness: hid("other"),
        harness_session_id: "source-reclaim-operator-native".into(),
        project: PROJECT.into(),
        client_key: "source-reclaim-operator-client".into(),
        runtime_credential: None,
        tier: Tier::Admin,
        kind: Some(Kind::Human),
        locality: Default::default(),
        access: None,
        role: None,
        cwd: None,
    }
}

fn spawn_request() -> SpawnRequest {
    SpawnRequest {
        kind: hid("claude"),
        name: Some("cold-source-target".into()),
        identity_policy: None,
        cwd: None,
        project: Some(PROJECT.into()),
        role: None,
        initial_prompt: None,
        resume: None,
        harness_args: Vec::new(),
        headless: true,
        backend: None,
    }
}

async fn wait_for_delivery(state: &AppState, message_id: &str) -> (String, i64) {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let mut rows = state
            .store
            .conn
            .query(
                "SELECT state, attempt_count FROM in_flight WHERE message_id = ?1",
                [message_id],
            )
            .await
            .unwrap();
        if let Some(row) = rows.next().await.unwrap() {
            let delivery_state = row.get::<String>(0).unwrap();
            let attempts = row.get::<i64>(1).unwrap();
            if delivery_state == "delivered" {
                return (delivery_state, attempts);
            }
        }
        assert!(
            Instant::now() < deadline,
            "source delivery {message_id} did not complete"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn command_result(state: &AppState) -> serde_json::Value {
    let row = CommandIntents::new(&state.store)
        .get(COMMAND_ID)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.status, "done");
    serde_json::from_str(row.result_json.as_deref().unwrap()).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reclaimed_signed_source_push_reuses_commit_and_injects_once() {
    let state = state_against_fake_acp().await;
    state.identity.register(operator_request()).await.unwrap();
    let operator = state.identity.resolve(PROJECT, "operator").await.unwrap();
    Sources::new(&state.store)
        .create(SOURCE, "fixture-token", TOPIC, 1)
        .await
        .unwrap();
    Topics::new(&state.store)
        .ensure(TOPIC, PROJECT)
        .await
        .unwrap();

    let target = state
        .launch_agent(spawn_request(), PROJECT, Some(&operator))
        .await
        .unwrap();
    Topics::new(&state.store)
        .subscribe(TOPIC, &target.session_id.0, None)
        .await
        .unwrap();
    state
        .mark_session_offline(&target.session_id)
        .await
        .unwrap();

    let request = PushRequest {
        source: SOURCE.into(),
        topic: None,
        summary: Some("signed build finished".into()),
        body: MARKER.into(),
        meta: None,
    };
    let intents = CommandIntents::new(&state.store);
    intents
        .insert_pending(NewCommandIntent {
            command_id: COMMAND_ID.into(),
            kind: command_kinds::source::PUSH.into(),
            project: PROJECT.into(),
            caller_name: SOURCE.into(),
            caller_session_id: Some(format!("source:{SOURCE}")),
            caller_agent_id: None,
            caller_runtime_id: Some(format!("source:{SOURCE}")),
            caller_client_key: None,
            caller_principal_id: None,
            caller_kind: Some("notification".into()),
            caller_tier: Some("agent".into()),
            idempotency_key: Some(COMMAND_KEY.into()),
            request_json: serde_json::to_string(&request).unwrap(),
            created_at: 1,
        })
        .await
        .unwrap();

    assert!(command_worker::process_next(&state).await.unwrap());
    let first_receipt = command_result(&state).await;
    let original_message_id = first_receipt["messageId"].as_str().unwrap().to_string();
    assert_eq!(
        wait_for_delivery(&state, &original_message_id).await,
        ("delivered".into(), 1)
    );

    // Move the canonical write outside the legacy duplicate-body window. Only the durable command
    // key may make the reclaimed execution resolve to the original bus transaction.
    state
        .store
        .conn
        .execute(
            "UPDATE messages SET created_at = created_at - 10 WHERE message_id = ?1",
            [original_message_id.as_str()],
        )
        .await
        .unwrap();
    // Crash boundary: bus commit is durable, command completion is not. Reclaim the expired lease.
    state
        .store
        .conn
        .execute(
            "UPDATE command_intents SET status = 'claimed', result_json = NULL, error_json = NULL, \
             claimed_at = 1, started_at = NULL, lease_until = 1, completed_at = NULL \
             WHERE command_id = ?1",
            [COMMAND_ID],
        )
        .await
        .unwrap();

    assert!(command_worker::process_next(&state).await.unwrap());
    let reclaimed_receipt = command_result(&state).await;
    assert_eq!(
        reclaimed_receipt, first_receipt,
        "reclaim must return the original source receipt and messageId"
    );
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut rows = state
        .store
        .conn
        .query(
            "SELECT COUNT(*), COUNT(DISTINCT message_id), COUNT(DISTINCT idempotency_key), \
                    MAX(idempotency_key) \
             FROM messages WHERE body = ?1",
            [MARKER],
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 1, "one canonical message");
    assert_eq!(row.get::<i64>(1).unwrap(), 1, "one canonical message id");
    assert_eq!(row.get::<i64>(2).unwrap(), 1, "one durable bus key");
    assert_eq!(
        row.get::<String>(3).unwrap(),
        COMMAND_KEY,
        "the signed command key must reach the canonical bus write"
    );

    let mut rows = state
        .store
        .conn
        .query(
            "SELECT COUNT(*), MAX(attempt_count), MAX(state) FROM in_flight \
             WHERE message_id = ?1",
            [original_message_id.as_str()],
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 1, "one delivery row");
    assert_eq!(row.get::<i64>(1).unwrap(), 1, "one automatic injection");
    assert_eq!(row.get::<String>(2).unwrap(), "delivered");
}

/// A validated [`nexus_contracts::HarnessId`] from a literal (panics on invalid — test-only).
fn hid(s: &str) -> nexus_contracts::HarnessId {
    nexus_contracts::HarnessId::new(s).expect("valid harness id literal")
}
