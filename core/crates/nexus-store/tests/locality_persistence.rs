//! Persistence seam inventory for the v0.1.6 locality contract.
//!
//! - member directory / whoami caller evidence: `sessions.kind` plus `metadata_json.access`
//! - message and history rows: `messages.provenance`
//! - command intents and completed receipts: `command_intents.caller_kind`
//! - Gateway projection: the stored message provenance copied into `GatewayProjectionEffect`
//!
//! New writes use canonical dotted entity kinds. Legacy bare bytes remain unchanged in SQLite but
//! read as local. Unknown dotted locality/nature tokens fail closed at the repository boundary.

use nexus_common::NexusError;
use nexus_contracts::{
    entity_kind, Kind, Locality, Message, MessageId, ProjectId, Provenance, Scope,
};
use nexus_store::repos::{CommandIntents, Messages, NewCommandIntent, Sessions};
use nexus_store::Store;

async fn migrated() -> Store {
    let store = Store::open(":memory:").await.expect("open store");
    store.migrate().await.expect("migrate store");
    store
}

async fn insert_session(store: &Store, session_id: &str, kind: &str, access: Option<&str>) {
    let metadata = access.map(|access| serde_json::json!({"access": access}).to_string());
    store
        .conn
        .execute(
            "INSERT INTO sessions (session_id, name, kind, tier, project, presence, paused, \
             created_at, metadata_json) VALUES (?1, ?2, ?3, 'agent', 'metadata-only', \
             'online', 0, 1, ?4)",
            libsql::params![session_id, format!("name-{session_id}"), kind, metadata],
        )
        .await
        .expect("insert session fixture");
}

fn intent(command_id: &str, caller_kind: &str) -> NewCommandIntent {
    NewCommandIntent {
        command_id: command_id.into(),
        kind: "message.post.send".into(),
        project: "metadata-only".into(),
        caller_name: "outside".into(),
        caller_session_id: Some("s_external".into()),
        caller_agent_id: None,
        caller_runtime_id: None,
        caller_client_key: None,
        caller_principal_id: None,
        caller_kind: Some(caller_kind.into()),
        caller_tier: Some("agent".into()),
        idempotency_key: None,
        request_json: "{}".into(),
        created_at: 1,
    }
}

#[tokio::test]
async fn session_rows_read_canonical_locality_and_access_without_rewriting_legacy_bytes() {
    let store = migrated().await;
    insert_session(&store, "s_external", "external.human", Some("guest")).await;
    insert_session(&store, "s_legacy", "human", None).await;

    let external = Sessions::new(&store)
        .find_by_session_id(&"s_external".into())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(external.kind, "external.human");
    assert_eq!(external.access().unwrap().as_deref(), Some("guest"));

    let legacy = Sessions::new(&store)
        .find_by_session_id(&"s_legacy".into())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(legacy.kind, "local.human");
    assert_eq!(legacy.access().unwrap(), None);

    let mut raw = store
        .conn
        .query(
            "SELECT kind FROM sessions WHERE session_id = 's_legacy'",
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        raw.next().await.unwrap().unwrap().get::<String>(0).unwrap(),
        "human"
    );
}

#[tokio::test]
async fn message_history_and_gateway_projection_preserve_locality_and_access() {
    let store = migrated().await;
    let message = Message {
        id: MessageId("m_external".into()),
        project: ProjectId("metadata-only".into()),
        from: "outside".into(),
        scope: Scope::Dm,
        thread: None,
        topic: None,
        body: "hello".into(),
        summary: None,
        provenance: Provenance {
            from: "outside".into(),
            kind: Kind::Human,
            locality: Locality::External,
            access: Some("guest".into()),
            thread: None,
            topic: None,
            stamp: None,
        },
        created_at: 1,
    };
    let messages = Messages::new(&store);
    messages.insert(&message).await.unwrap();

    let read = messages
        .get("metadata-only", &message.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        entity_kind::dotted(read.provenance.locality, read.provenance.kind),
        "external.human"
    );
    assert_eq!(read.provenance.access.as_deref(), Some("guest"));

    let projection = messages
        .gateway_projection_effects(&message.id)
        .await
        .unwrap();
    assert_eq!(projection[0].payload["provenance"]["locality"], "external");
    assert_eq!(projection[0].payload["provenance"]["access"], "guest");
}

#[tokio::test]
async fn command_intent_and_completed_row_round_trip_dotted_caller_kind() {
    let store = migrated().await;
    let commands = CommandIntents::new(&store);
    commands
        .insert_pending(intent("cmd_external", "external.human"))
        .await
        .unwrap();
    commands
        .mark_done("cmd_external", r#"{"ok":true}"#, 2)
        .await
        .unwrap();

    let completed = commands.get("cmd_external").await.unwrap().unwrap();
    assert_eq!(completed.status, "done");
    assert_eq!(completed.caller_kind.as_deref(), Some("external.human"));
    assert_eq!(
        commands
            .receipt("cmd_external")
            .await
            .unwrap()
            .unwrap()
            .status,
        "done"
    );
}

#[tokio::test]
async fn legacy_caller_kind_reads_local_without_mutation_and_unknown_kind_is_typed_error() {
    let store = migrated().await;
    let commands = CommandIntents::new(&store);
    commands
        .insert_pending(intent("cmd_legacy", "local.human"))
        .await
        .unwrap();
    store
        .identity_conn()
        .execute(
            "UPDATE command_intents SET caller_kind = 'human' WHERE command_id = 'cmd_legacy'",
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        commands
            .get("cmd_legacy")
            .await
            .unwrap()
            .unwrap()
            .caller_kind
            .as_deref(),
        Some("local.human")
    );
    let mut raw = store
        .identity_conn()
        .query(
            "SELECT caller_kind FROM command_intents WHERE command_id = 'cmd_legacy'",
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        raw.next().await.unwrap().unwrap().get::<String>(0).unwrap(),
        "human"
    );

    commands
        .insert_pending(intent("cmd_unknown", "local.human"))
        .await
        .unwrap();
    store
        .identity_conn()
        .execute(
            "UPDATE command_intents SET caller_kind = 'remote.human' WHERE command_id = 'cmd_unknown'",
            (),
        )
        .await
        .unwrap();
    assert!(matches!(
        commands.get("cmd_unknown").await.unwrap_err(),
        NexusError::Invalid(_)
    ));
}

#[tokio::test]
async fn unknown_session_locality_is_a_typed_error() {
    let store = migrated().await;
    insert_session(&store, "s_unknown", "remote.human", None).await;
    assert!(matches!(
        Sessions::new(&store)
            .find_by_session_id(&"s_unknown".into())
            .await
            .unwrap_err(),
        NexusError::Invalid(_)
    ));
}
