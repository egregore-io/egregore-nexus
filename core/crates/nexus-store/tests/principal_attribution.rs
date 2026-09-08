//! Durable caller-principal attribution across the public and split identity authorities.

use nexus_contracts::CommandQueueState;
use nexus_store::repos::{CommandIntents, CommandQueue, NewCommandIntent};
use nexus_store::Store;

const BASELINE_SCHEMA: &str = include_str!("../../../migrations/0001_init.sql");
const MESSAGE_HOOKS_SCHEMA: &str = include_str!("../../../migrations/0002_message_hooks.sql");
const DELIVERY_TIMING_SCHEMA: &str = include_str!("../../../migrations/0003_delivery_timing.sql");
const IDENTITY_SCHEMA: &str = include_str!("../../../migrations/identity/0001_identity.sql");

async fn migrated() -> Store {
    let store = Store::open(":memory:").await.expect("open store");
    store.migrate().await.expect("migrate store");
    store
}

fn command(command_id: &str, principal_id: Option<&str>, caller_kind: &str) -> NewCommandIntent {
    NewCommandIntent {
        command_id: command_id.into(),
        kind: nexus_store::command_kinds::harness::PROMPT.into(),
        project: "default".into(),
        caller_name: "outside".into(),
        caller_session_id: Some("s_external".into()),
        caller_agent_id: None,
        caller_runtime_id: None,
        caller_client_key: Some("external-client".into()),
        caller_principal_id: principal_id.map(str::to_string),
        caller_kind: Some(caller_kind.into()),
        caller_tier: Some("agent".into()),
        idempotency_key: Some(format!("idem-{command_id}")),
        request_json: serde_json::json!({
            "name": "target",
            "sessionId": "s_target",
            "clientMessageId": format!("cm-{command_id}"),
            "text": "hello"
        })
        .to_string(),
        created_at: 1,
    }
}

#[tokio::test]
async fn external_human_principal_is_durable_and_projected_on_completed_receipts() {
    let store = migrated().await;
    store.conn.execute(
        "INSERT INTO sessions (session_id, agent_id, name, agent, kind, project, created_at) VALUES ('s_target', 'a_target', 'target', 'other', 'agent', 'default', 1)",
        (),
    ).await.expect("seed owned legacy target binding");
    let commands = CommandIntents::new(&store);
    commands
        .insert_pending(command(
            "cmd_external",
            Some("x_external_abc"),
            "external.human",
        ))
        .await
        .expect("insert attributed command");

    let row = commands
        .get("cmd_external")
        .await
        .expect("read command")
        .expect("command row");
    assert_eq!(row.caller_principal_id.as_deref(), Some("x_external_abc"));
    assert_eq!(row.caller_kind.as_deref(), Some("external.human"));

    commands
        .mark_done("cmd_external", r#"{"ok":true}"#, 2)
        .await
        .expect("complete command");
    let page = CommandQueue::new(&store)
        .events_after(0)
        .await
        .expect("read projected receipts");
    let completed = page
        .events
        .iter()
        .find(|event| event.state == CommandQueueState::Completed)
        .expect("completed receipt projection");
    assert_eq!(
        completed.caller_principal_id.as_deref(),
        Some("x_external_abc")
    );
    assert_eq!(completed.caller_kind.as_deref(), Some("external.human"));
}

#[tokio::test]
async fn local_human_and_legacy_commands_round_trip_principal_or_null() {
    let store = migrated().await;
    let commands = CommandIntents::new(&store);
    commands
        .insert_pending(command("cmd_human", Some("h_human_abc"), "local.human"))
        .await
        .expect("insert human command");
    commands
        .insert_pending(command("cmd_legacy", None, "human"))
        .await
        .expect("insert legacy command");

    assert_eq!(
        commands
            .get("cmd_human")
            .await
            .unwrap()
            .unwrap()
            .caller_principal_id
            .as_deref(),
        Some("h_human_abc")
    );
    assert_eq!(
        commands
            .get("cmd_legacy")
            .await
            .unwrap()
            .unwrap()
            .caller_principal_id,
        None
    );

    let mut rows = store
        .identity_conn()
        .query(
            "SELECT caller_principal_id FROM command_intents WHERE command_id = 'cmd_legacy'",
            (),
        )
        .await
        .expect("query raw legacy attribution");
    assert!(rows
        .next()
        .await
        .expect("legacy row query")
        .expect("legacy row")
        .get::<Option<String>>(0)
        .expect("nullable principal")
        .is_none());
}

#[tokio::test]
async fn public_delivery_timing_store_upgrades_additively_without_losing_commands() {
    let store = Store::open(":memory:").await.expect("open raw store");
    store
        .conn
        .execute_batch(
            "CREATE TABLE schema_migrations (
               version INTEGER PRIMARY KEY,
               name TEXT NOT NULL,
               applied_at INTEGER NOT NULL
             );",
        )
        .await
        .unwrap();
    store.conn.execute_batch(BASELINE_SCHEMA).await.unwrap();
    store
        .conn
        .execute_batch(MESSAGE_HOOKS_SCHEMA)
        .await
        .unwrap();
    store
        .conn
        .execute_batch(DELIVERY_TIMING_SCHEMA)
        .await
        .unwrap();
    if column_exists(&store, "command_intents", "caller_principal_id").await {
        store
            .conn
            .execute(
                "ALTER TABLE command_intents DROP COLUMN caller_principal_id",
                (),
            )
            .await
            .unwrap();
    }
    if column_exists(&store, "command_intents", "caller_validated_boot_epoch").await {
        store
            .conn
            .execute(
                "ALTER TABLE command_intents DROP COLUMN caller_validated_boot_epoch",
                (),
            )
            .await
            .unwrap();
    }
    store
        .conn
        .execute_batch(
            "INSERT INTO schema_migrations VALUES (1, 'v0.1.0_baseline', 1);
             INSERT INTO schema_migrations VALUES (2, 'v0.1.5_message_hooks', 2);
             INSERT INTO schema_migrations VALUES (3, 'v0.1.5_delivery_timing', 3);
             INSERT INTO command_intents (
               command_id, kind, status, project, caller_name, request_json, created_at
             ) VALUES ('cmd_preserved', 'test.intent', 'pending', 'default', 'legacy', '{}', 4);",
        )
        .await
        .unwrap();

    store.migrate().await.expect("upgrade public store");

    assert!(column_exists(&store, "command_intents", "caller_principal_id").await);
    let row = CommandIntents::new(&store)
        .get("cmd_preserved")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.caller_principal_id, None);
    assert_eq!(schema_marker(&store).await, "v0.1.6_caller_authority");
}

#[tokio::test]
async fn split_identity_provider_store_upgrades_the_same_command_authority() {
    let store = migrated().await;
    store.conn.execute_batch(IDENTITY_SCHEMA).await.unwrap();
    store
        .conn
        .execute(
            "ALTER TABLE command_intents DROP COLUMN caller_principal_id",
            (),
        )
        .await
        .unwrap();
    store
        .conn
        .execute_batch(
            "DELETE FROM schema_migrations;
             INSERT INTO schema_migrations VALUES (1, 'v0.1.6_identity_provider', 1);",
        )
        .await
        .unwrap();

    store.migrate().await.expect("upgrade split identity store");

    assert!(column_exists(&store, "command_intents", "caller_principal_id").await);
    assert_eq!(
        schema_marker(&store).await,
        "v0.1.6_identity_caller_principal"
    );
}

async fn column_exists(store: &Store, table: &str, column: &str) -> bool {
    let mut rows = store
        .conn
        .query(&format!("PRAGMA table_info(\"{table}\")"), ())
        .await
        .unwrap();
    while let Some(row) = rows.next().await.unwrap() {
        if row.get::<String>(1).unwrap() == column {
            return true;
        }
    }
    false
}

async fn schema_marker(store: &Store) -> String {
    let mut rows = store
        .conn
        .query(
            "SELECT name FROM schema_migrations ORDER BY version DESC LIMIT 1",
            (),
        )
        .await
        .unwrap();
    rows.next()
        .await
        .unwrap()
        .unwrap()
        .get::<String>(0)
        .unwrap()
}
