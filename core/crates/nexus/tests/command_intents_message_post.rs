use std::sync::Arc;

use nexus::daemon::{command_worker, dispatch, AppState};
use nexus_agent::{Adapter, AdapterRegistry, MockAdapter};
use nexus_common::Config;
use nexus_contracts::ids::ThreadId;
use nexus_contracts::{
    ArchiveThreadRequest, CreateThreadRequest, DeleteThreadRequest, InboxSubscribeRequest,
    InboxSubscriptionAckRequest, InboxSubscriptionNextRequest, InboxSubscriptionNextResponse, Kind,
    MetadataEntityKind, MetadataSetRequest, NotifyCommandRequest, NotifyRequest, NotifySendRequest,
    NotifyTarget, Presence, PushRequest, RegisterRequest, RegisterResponse, Request, SendRequest,
    SendTarget, SpawnRequest, Tier, JSONRPC_VERSION,
};
use nexus_notify::{Notify, RouteRule, RoutingRules};
use nexus_store::command_kinds;
use nexus_store::repos::{
    AgentRuntimes, Agents, CommandIntents, NewAgent, NewCommandIntent, Notifications, Sessions,
    Sources, Threads, Topics,
};
use nexus_store::Store;

async fn test_state() -> AppState {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    AppState::wire(store, &Config::default())
}

async fn test_state_with_notify_secret() -> AppState {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let mut config = Config::default();
    config.hmac_secret = "notify-test-secret".into();
    AppState::wire(store, &config)
}

async fn test_state_with_notify_secret_and_mock() -> (AppState, MockAdapter) {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let mut config = Config::default();
    config.hmac_secret = "notify-test-secret".into();
    let mock = MockAdapter::new();
    let mut registry = AdapterRegistry::new();
    let adapter = mock.clone();
    registry.register(
        &hid("claude"),
        Arc::new(move |_cwd| Arc::new(adapter.clone()) as Arc<dyn Adapter>),
    );
    (AppState::wire_with_registry(store, &config, registry), mock)
}

fn human_register(name: &str, client_key: &str) -> RegisterRequest {
    RegisterRequest {
        agent_id: None,
        name: Some(name.into()),
        harness: hid("other"),
        harness_session_id: format!("hs_{client_key}"),
        project: "default".into(),
        client_key: client_key.into(),
        runtime_credential: None,
        tier: Tier::Admin,
        kind: Some(Kind::Human),
        role: None,
        cwd: None,
    }
}

fn agent_register(name: &str, client_key: &str) -> RegisterRequest {
    RegisterRequest {
        agent_id: None,
        name: Some(name.into()),
        harness: hid("other"),
        harness_session_id: format!("hs_{client_key}"),
        project: "default".into(),
        client_key: client_key.into(),
        runtime_credential: None,
        tier: Tier::Agent,
        kind: Some(Kind::Agent),
        role: None,
        cwd: None,
    }
}

#[tokio::test]
async fn message_post_command_intent_writes_canonical_bus_rows() {
    let state = test_state().await;
    let alex = state
        .identity
        .register(human_register("Alex Morgan", "ck_operator"))
        .await
        .unwrap();
    let blake = state
        .identity
        .register(human_register("Blake Human", "ck_blake"))
        .await
        .unwrap();
    assert_eq!(alex.agent_id, None);
    assert_eq!(blake.agent_id, None);
    let caller = state
        .identity
        .resolve("default", "Alex Morgan")
        .await
        .unwrap();
    state
        .bus
        .create_thread(
            &caller,
            CreateThreadRequest {
                name: "thread-smoke".into(),
                members: vec!["Blake Human".into()],
            },
        )
        .await
        .unwrap();

    let request = SendRequest {
        to: SendTarget::Post {
            thread: "thread-smoke".into(),
        },
        summary: Some("smoke".into()),
        body: "hello over command ingress".into(),
        mention: vec![],
        metadata: None,
        idempotency_key: None,
    };
    CommandIntents::new(&state.store)
        .insert_pending(NewCommandIntent {
            command_id: "cmd_message_post_1".into(),
            kind: command_kinds::message_post::SEND.into(),
            project: "default".into(),
            caller_name: "Alex Morgan".into(),
            caller_session_id: Some(alex.session_id.0.clone()),
            caller_agent_id: alex.agent_id.as_ref().map(|id| id.0.clone()),
            caller_runtime_id: Some(alex.session_id.0.clone()),
            caller_client_key: Some("ck_operator".into()),
            caller_kind: Some("human".into()),
            caller_tier: Some("admin".into()),
            idempotency_key: None,
            request_json: serde_json::to_string(&request).unwrap(),
            created_at: 1,
        })
        .await
        .unwrap();

    assert!(command_worker::process_next(&state).await.unwrap());

    let command = CommandIntents::new(&state.store)
        .get("cmd_message_post_1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(command.status, "done");
    let result: serde_json::Value =
        serde_json::from_str(command.result_json.as_deref().unwrap()).expect("command result json");
    let message_id = result["messageId"].as_str().expect("message id");

    let mut rows = state
        .store
        .conn
        .query(
            "SELECT from_name, kind, to_name, body, provenance, from_agent_id, sender_session_id \
             FROM messages \
             WHERE body = 'hello over command ingress'",
            (),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().expect("message row");
    assert_eq!(row.get::<String>(0).unwrap(), "Alex Morgan");
    assert_eq!(row.get::<String>(1).unwrap(), "thread");
    assert_eq!(row.get::<String>(2).unwrap(), "thread-smoke");
    assert_eq!(row.get::<String>(3).unwrap(), "hello over command ingress");
    let provenance: serde_json::Value =
        serde_json::from_str(&row.get::<String>(4).unwrap()).unwrap();
    assert_eq!(provenance["kind"], "human");
    assert_eq!(row.get::<Option<String>>(5).unwrap(), None);
    assert_eq!(
        row.get::<Option<String>>(6).unwrap().as_deref(),
        Some(alex.session_id.0.as_str())
    );

    let mut rows = state
        .store
        .conn
        .query(
            &format!(
                "SELECT COUNT(*), COUNT(recipient_agent_id) FROM in_flight WHERE message_id = '{}'",
                message_id.replace('\'', "''")
            ),
            (),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 1);
    assert_eq!(row.get::<i64>(1).unwrap(), 0);
    for table in ["agents", "agent_runtimes"] {
        let mut rows = state
            .store
            .identity_conn()
            .query(&format!("SELECT COUNT(*) FROM {table}"), ())
            .await
            .unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
            0,
            "human thread participants must not materialize {table} rows"
        );
    }
}

#[tokio::test]
async fn message_post_command_intent_keeps_dm_fanout_internal() {
    let state = test_state().await;
    let alex = state
        .identity
        .register(human_register("Alex Morgan", "ck_operator"))
        .await
        .unwrap();
    let bianca = state
        .identity
        .register(human_register("Bianca", "ck_bianca"))
        .await
        .unwrap();
    assert_eq!(alex.agent_id, None);
    assert_eq!(bianca.agent_id, None);

    let request = SendRequest {
        to: SendTarget::dm_name("Bianca"),
        summary: None,
        body: "fanout should count a live DM recipient".into(),
        mention: vec![],
        metadata: None,
        idempotency_key: None,
    };
    CommandIntents::new(&state.store)
        .insert_pending(NewCommandIntent {
            command_id: "cmd_message_post_dm_fanout".into(),
            kind: command_kinds::message_post::SEND.into(),
            project: "default".into(),
            caller_name: "Alex Morgan".into(),
            caller_session_id: Some(alex.session_id.0.clone()),
            caller_agent_id: alex.agent_id.as_ref().map(|id| id.0.clone()),
            caller_runtime_id: Some(alex.session_id.0.clone()),
            caller_client_key: Some("ck_operator".into()),
            caller_kind: Some("human".into()),
            caller_tier: Some("admin".into()),
            idempotency_key: None,
            request_json: serde_json::to_string(&request).unwrap(),
            created_at: 1,
        })
        .await
        .unwrap();

    assert!(command_worker::process_next(&state).await.unwrap());

    let command = CommandIntents::new(&state.store)
        .get("cmd_message_post_dm_fanout")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(command.status, "done");
    let result: serde_json::Value =
        serde_json::from_str(command.result_json.as_deref().unwrap()).expect("command result json");
    assert!(
        result.get("fanout").is_none(),
        "recipient fanout is daemon-owned bookkeeping, not a public result field"
    );
    let mut rows = state
        .store
        .conn
        .query(
            "SELECT from_name, from_agent_id, to_name, to_agent_id, provenance, \
                    sender_session_id \
             FROM messages WHERE body = 'fanout should count a live DM recipient'",
            (),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().expect("human DM message row");
    assert_eq!(row.get::<String>(0).unwrap(), "Alex Morgan");
    assert_eq!(row.get::<Option<String>>(1).unwrap(), None);
    assert_eq!(
        row.get::<Option<String>>(2).unwrap().as_deref(),
        Some("Bianca")
    );
    assert_eq!(row.get::<Option<String>>(3).unwrap(), None);
    let provenance: serde_json::Value =
        serde_json::from_str(&row.get::<String>(4).unwrap()).unwrap();
    assert_eq!(provenance["kind"], "human");
    assert_eq!(
        row.get::<Option<String>>(5).unwrap().as_deref(),
        Some(alex.session_id.0.as_str())
    );
}

#[tokio::test]
async fn one_shot_notify_command_uses_canonical_bus_and_keeps_fanout_internal() {
    let state = test_state().await;
    let alex = state
        .identity
        .register(human_register("Alex Morgan", "ck_operator"))
        .await
        .unwrap();
    let target = state
        .identity
        .register(agent_register("Alice", "ck_alice"))
        .await
        .unwrap();
    let request = NotifySendRequest {
        target: NotifyTarget::Agent {
            agent_id: target.agent_id.clone().unwrap(),
        },
        source: Some("release-watchdog".into()),
        body: "notify over daemon command ingress".into(),
        idempotency_key: Some("notify:test:1".into()),
    };
    CommandIntents::new(&state.store)
        .insert_pending(NewCommandIntent {
            command_id: "cmd_notify_send_1".into(),
            kind: command_kinds::notification::SEND.into(),
            project: "ignored-metadata".into(),
            caller_name: "Alex Morgan".into(),
            caller_session_id: Some(alex.session_id.0.clone()),
            caller_agent_id: alex.agent_id.as_ref().map(|id| id.0.clone()),
            caller_runtime_id: Some(alex.session_id.0.clone()),
            caller_client_key: Some("ck_operator".into()),
            caller_kind: Some("human".into()),
            caller_tier: Some("admin".into()),
            idempotency_key: request.idempotency_key.clone(),
            request_json: serde_json::to_string(&request).unwrap(),
            created_at: 1,
        })
        .await
        .unwrap();

    assert!(command_worker::process_next(&state).await.unwrap());
    let command = CommandIntents::new(&state.store)
        .get("cmd_notify_send_1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        command.status, "done",
        "notification command failed: {:?}",
        command.error_json
    );
    let result: serde_json::Value =
        serde_json::from_str(command.result_json.as_deref().unwrap()).unwrap();
    assert!(result.get("messageId").is_some());
    assert!(result.get("fanout").is_none());

    let mut rows = state
        .store
        .conn
        .query(
            "SELECT from_name, provenance, COUNT(*) OVER () FROM messages \
             WHERE body = 'notify over daemon command ingress'",
            (),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<String>(0).unwrap(), "release-watchdog");
    assert!(row
        .get::<String>(1)
        .unwrap()
        .contains(r#""kind":"notification""#));
    assert_eq!(row.get::<i64>(2).unwrap(), 1);
}

#[tokio::test]
async fn one_shot_notify_to_dead_agent_records_one_terminal_delivery_without_resubmit() {
    let state = test_state().await;
    let alex = state
        .identity
        .register(human_register("Alex Morgan", "ck_operator_dead_notify"))
        .await
        .unwrap();
    let target = state
        .identity
        .register(agent_register("Dead Alice", "ck_dead_alice"))
        .await
        .unwrap();
    let target_agent_id = target.agent_id.clone().unwrap();
    Sessions::new(&state.store)
        .set_presence(&target.session_id, Presence::Offline)
        .await
        .unwrap();
    AgentRuntimes::new(&state.store)
        .stop(&target.session_id.0)
        .await
        .unwrap();
    Agents::new(&state.store)
        .mark_dead(&target_agent_id.0, "test_dead_target")
        .await
        .unwrap();

    let request = NotifySendRequest {
        target: NotifyTarget::Agent {
            agent_id: target_agent_id,
        },
        source: Some("release-watchdog".into()),
        body: "dead target notification must settle once".into(),
        idempotency_key: Some("notify:dead-target:1".into()),
    };
    CommandIntents::new(&state.store)
        .insert_pending(NewCommandIntent {
            command_id: "cmd_notify_send_dead_1".into(),
            kind: command_kinds::notification::SEND.into(),
            project: "metadata-only".into(),
            caller_name: "Alex Morgan".into(),
            caller_session_id: Some(alex.session_id.0.clone()),
            caller_agent_id: alex.agent_id.as_ref().map(|id| id.0.clone()),
            caller_runtime_id: Some(alex.session_id.0.clone()),
            caller_client_key: Some("ck_operator_dead_notify".into()),
            caller_kind: Some("human".into()),
            caller_tier: Some("admin".into()),
            idempotency_key: request.idempotency_key.clone(),
            request_json: serde_json::to_string(&request).unwrap(),
            created_at: 1,
        })
        .await
        .unwrap();

    assert!(command_worker::process_next(&state).await.unwrap());
    for _ in 0..50 {
        let mut rows = state
            .store
            .conn
            .query(
                "SELECT state FROM in_flight f JOIN messages m ON m.message_id = f.message_id \
                 WHERE m.body = 'dead target notification must settle once'",
                (),
            )
            .await
            .unwrap();
        if rows
            .next()
            .await
            .unwrap()
            .is_some_and(|row| row.get::<String>(0).unwrap() == "error")
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    let mut rows = state
        .store
        .conn
        .query(
            "SELECT COUNT(*), MAX(f.state), MAX(f.attempt_count), MAX(f.error_code) \
             FROM in_flight f JOIN messages m ON m.message_id = f.message_id \
             WHERE m.body = 'dead target notification must settle once'",
            (),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 1, "body must remain canonical");
    assert_eq!(
        row.get::<Option<String>>(1).unwrap().as_deref(),
        Some("error")
    );
    assert_eq!(row.get::<Option<i64>>(2).unwrap(), Some(0));
    assert_eq!(
        row.get::<Option<String>>(3).unwrap().as_deref(),
        Some(nexus_store::repos::inbox::TARGET_DEAD_ERROR_CODE)
    );
}

#[tokio::test]
async fn duplicate_message_post_command_intents_return_original_ack() {
    let state = test_state().await;
    let alex = state
        .identity
        .register(human_register("Alex Morgan", "ck_operator"))
        .await
        .unwrap();
    state
        .identity
        .register(human_register("Blake Human", "ck_blake"))
        .await
        .unwrap();
    let caller = state
        .identity
        .resolve("default", "Alex Morgan")
        .await
        .unwrap();
    state
        .bus
        .create_thread(
            &caller,
            CreateThreadRequest {
                name: "thread-retry".into(),
                members: vec!["Blake Human".into()],
            },
        )
        .await
        .unwrap();

    let request = SendRequest {
        to: SendTarget::Post {
            thread: "thread-retry".into(),
        },
        summary: Some("retry-safe".into()),
        body: "same write submitted twice".into(),
        mention: vec![],
        metadata: None,
        idempotency_key: Some("web:thread-retry:client-1".into()),
    };
    let intents = CommandIntents::new(&state.store);
    for (command_id, created_at) in [
        ("cmd_message_post_retry_1", 1),
        ("cmd_message_post_retry_2", 2),
    ] {
        intents
            .insert_pending(NewCommandIntent {
                command_id: command_id.into(),
                kind: command_kinds::message_post::SEND.into(),
                project: "default".into(),
                caller_name: "Alex Morgan".into(),
                caller_session_id: Some(alex.session_id.0.clone()),
                caller_agent_id: alex.agent_id.as_ref().map(|id| id.0.clone()),
                caller_runtime_id: Some(alex.session_id.0.clone()),
                caller_client_key: Some("ck_operator".into()),
                caller_kind: Some("human".into()),
                caller_tier: Some("admin".into()),
                // Distinct command rows prove the daemon send path itself is idempotent.
                idempotency_key: None,
                request_json: serde_json::to_string(&request).unwrap(),
                created_at,
            })
            .await
            .unwrap();
    }

    assert!(command_worker::process_next(&state).await.unwrap());
    assert!(command_worker::process_next(&state).await.unwrap());

    let first = intents
        .get("cmd_message_post_retry_1")
        .await
        .unwrap()
        .unwrap();
    let second = intents
        .get("cmd_message_post_retry_2")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.status, "done");
    assert_eq!(second.status, "done");
    let first_result: serde_json::Value =
        serde_json::from_str(first.result_json.as_deref().unwrap()).unwrap();
    let second_result: serde_json::Value =
        serde_json::from_str(second.result_json.as_deref().unwrap()).unwrap();
    assert_eq!(first_result["messageId"], second_result["messageId"]);
    assert!(first_result.get("fanout").is_none());
    assert!(second_result.get("fanout").is_none());

    let mut rows = state
        .store
        .conn
        .query(
            "SELECT COUNT(*), COUNT(DISTINCT message_id) FROM messages \
             WHERE body = 'same write submitted twice' \
               AND idempotency_key = 'web:thread-retry:client-1'",
            (),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 1);
    assert_eq!(row.get::<i64>(1).unwrap(), 1);

    let message_id = first_result["messageId"].as_str().unwrap();
    let mut rows = state
        .store
        .conn
        .query(
            &format!(
                "SELECT COUNT(*) FROM in_flight WHERE message_id = '{}'",
                message_id.replace('\'', "''")
            ),
            (),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 1);
}

#[tokio::test]
async fn reclaimed_message_post_promotes_command_idempotency_key_into_bus_write() {
    let state = test_state().await;
    let alex = state
        .identity
        .register(human_register("Alex Morgan", "ck_operator_reclaim"))
        .await
        .unwrap();
    state
        .identity
        .register(human_register("Blake Human", "ck_blake_reclaim"))
        .await
        .unwrap();

    let request = SendRequest {
        to: SendTarget::dm_name("Blake Human"),
        summary: None,
        body: "commit survived before command receipt".into(),
        mention: vec![],
        metadata: None,
        // Store-backed callers may put the retry key on the durable command envelope only. The
        // worker must promote that key into the canonical Message Post write before dispatch.
        idempotency_key: None,
    };
    let intents = CommandIntents::new(&state.store);
    intents
        .insert_pending(NewCommandIntent {
            command_id: "cmd_message_post_reclaim".into(),
            kind: command_kinds::message_post::SEND.into(),
            project: "default".into(),
            caller_name: "Alex Morgan".into(),
            caller_session_id: Some(alex.session_id.0.clone()),
            caller_agent_id: alex.agent_id.as_ref().map(|id| id.0.clone()),
            caller_runtime_id: Some(alex.session_id.0.clone()),
            caller_client_key: Some("ck_operator_reclaim".into()),
            caller_kind: Some("human".into()),
            caller_tier: Some("admin".into()),
            idempotency_key: Some("gateway:dm:reclaim-1".into()),
            request_json: serde_json::to_string(&request).unwrap(),
            created_at: 1,
        })
        .await
        .unwrap();

    assert!(command_worker::process_next(&state).await.unwrap());
    let first = intents
        .get("cmd_message_post_reclaim")
        .await
        .unwrap()
        .unwrap();
    let first_result: serde_json::Value =
        serde_json::from_str(first.result_json.as_deref().unwrap()).unwrap();

    // A reclaim may happen well after the legacy two-second duplicate-body window. Age the
    // committed write so only the explicit durable idempotency contract can prevent duplication.
    state
        .store
        .conn
        .execute(
            "UPDATE messages SET created_at = created_at - 10 \
             WHERE body = 'commit survived before command receipt'",
            (),
        )
        .await
        .unwrap();

    // Model the only dangerous reclaim boundary: the bus transaction committed, then the daemon
    // died before the command receipt became durable. Restoring the command to an expired claim
    // preserves the committed message while forcing the worker to execute the same intent again.
    state
        .store
        .conn
        .execute(
            "UPDATE command_intents SET status = 'claimed', result_json = NULL, error_json = NULL, \
             claimed_at = 1, started_at = NULL, lease_until = 1, completed_at = NULL \
             WHERE command_id = 'cmd_message_post_reclaim'",
            (),
        )
        .await
        .unwrap();

    assert!(command_worker::process_next(&state).await.unwrap());
    let reclaimed = intents
        .get("cmd_message_post_reclaim")
        .await
        .unwrap()
        .unwrap();
    let reclaimed_result: serde_json::Value =
        serde_json::from_str(reclaimed.result_json.as_deref().unwrap()).unwrap();
    assert_eq!(reclaimed.status, "done");
    assert_eq!(first_result["messageId"], reclaimed_result["messageId"]);

    let mut rows = state
        .store
        .conn
        .query(
            "SELECT COUNT(*), COUNT(DISTINCT message_id), \
                    COUNT(DISTINCT idempotency_key) \
             FROM messages WHERE body = 'commit survived before command receipt'",
            (),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 1);
    assert_eq!(row.get::<i64>(1).unwrap(), 1);
    assert_eq!(row.get::<i64>(2).unwrap(), 1);
}

#[tokio::test]
async fn message_post_dm_to_local_operator_writes_operator_view_row() {
    let state = test_state().await;
    let roman = state
        .identity
        .register(agent_register("roman", "ck_roman"))
        .await
        .unwrap();

    let request = SendRequest {
        to: SendTarget::dm_name("operator"),
        summary: None,
        body: "reply visible to the local operator".into(),
        mention: vec![],
        metadata: None,
        idempotency_key: None,
    };
    CommandIntents::new(&state.store)
        .insert_pending(NewCommandIntent {
            command_id: "cmd_dm_operator".into(),
            kind: command_kinds::message_post::SEND.into(),
            project: "default".into(),
            caller_name: "roman".into(),
            caller_session_id: Some(roman.session_id.0.clone()),
            caller_agent_id: roman.agent_id.as_ref().map(|id| id.0.clone()),
            caller_runtime_id: Some(roman.session_id.0.clone()),
            caller_client_key: Some("ck_roman".into()),
            caller_kind: Some("agent".into()),
            caller_tier: Some("agent".into()),
            idempotency_key: None,
            request_json: serde_json::to_string(&request).unwrap(),
            created_at: 1,
        })
        .await
        .unwrap();

    assert!(command_worker::process_next(&state).await.unwrap());

    let command = CommandIntents::new(&state.store)
        .get("cmd_dm_operator")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(command.status, "done");
    let result: serde_json::Value =
        serde_json::from_str(command.result_json.as_deref().unwrap()).expect("command result json");
    assert!(result.get("fanout").is_none());

    let mut rows = state
        .store
        .conn
        .query(
            "SELECT message_id, from_name, kind, to_name, body FROM messages \
             WHERE body = 'reply visible to the local operator'",
            (),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().expect("operator DM row");
    let message_id: String = row.get(0).unwrap();
    assert_eq!(row.get::<String>(1).unwrap(), "roman");
    assert_eq!(row.get::<String>(2).unwrap(), "dm");
    assert_eq!(row.get::<String>(3).unwrap(), "operator");
    assert_eq!(
        row.get::<String>(4).unwrap(),
        "reply visible to the local operator"
    );

    let mut rows = state
        .store
        .conn
        .query(
            &format!(
                "SELECT COUNT(*) FROM in_flight WHERE message_id = '{}'",
                message_id.replace('\'', "''")
            ),
            (),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(
        row.get::<i64>(0).unwrap(),
        0,
        "local operator DM replies are rendered from messages, not a fake agent inbox"
    );
}

#[tokio::test]
async fn message_post_reply_to_local_operator_writes_operator_view_row() {
    let state = test_state().await;
    let roman = state
        .identity
        .register(agent_register("roman", "ck_roman"))
        .await
        .unwrap();

    CommandIntents::new(&state.store)
        .insert_pending(NewCommandIntent {
            command_id: "cmd_operator_to_roman".into(),
            kind: command_kinds::message_post::SEND.into(),
            project: "default".into(),
            caller_name: "operator".into(),
            caller_session_id: Some("local-operator".into()),
            caller_agent_id: None,
            caller_runtime_id: Some("local-operator".into()),
            caller_client_key: None,
            caller_kind: Some("human".into()),
            caller_tier: Some("admin".into()),
            idempotency_key: None,
            request_json: serde_json::to_string(&SendRequest {
                to: SendTarget::dm_name("roman"),
                summary: None,
                body: "human prompt from web".into(),
                mention: vec![],
                metadata: None,
                idempotency_key: None,
            })
            .unwrap(),
            created_at: 1,
        })
        .await
        .unwrap();

    assert!(command_worker::process_next(&state).await.unwrap());

    CommandIntents::new(&state.store)
        .insert_pending(NewCommandIntent {
            command_id: "cmd_roman_reply_operator".into(),
            kind: command_kinds::message_post::SEND.into(),
            project: "default".into(),
            caller_name: "roman".into(),
            caller_session_id: Some(roman.session_id.0.clone()),
            caller_agent_id: roman.agent_id.as_ref().map(|id| id.0.clone()),
            caller_runtime_id: Some(roman.session_id.0.clone()),
            caller_client_key: Some("ck_roman".into()),
            caller_kind: Some("agent".into()),
            caller_tier: Some("agent".into()),
            idempotency_key: None,
            request_json: serde_json::to_string(&SendRequest {
                to: SendTarget::Reply,
                summary: None,
                body: "reply visible in the web operator DM".into(),
                mention: vec![],
                metadata: None,
                idempotency_key: None,
            })
            .unwrap(),
            created_at: 2,
        })
        .await
        .unwrap();

    assert!(command_worker::process_next(&state).await.unwrap());

    let command = CommandIntents::new(&state.store)
        .get("cmd_roman_reply_operator")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(command.status, "done");
    let result: serde_json::Value =
        serde_json::from_str(command.result_json.as_deref().unwrap()).expect("command result json");
    assert!(result.get("fanout").is_none());

    let mut rows = state
        .store
        .conn
        .query(
            "SELECT from_name, kind, to_name, body FROM messages \
             WHERE body = 'reply visible in the web operator DM'",
            (),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().expect("operator reply row");
    assert_eq!(row.get::<String>(0).unwrap(), "roman");
    assert_eq!(row.get::<String>(1).unwrap(), "dm");
    assert_eq!(row.get::<String>(2).unwrap(), "operator");
    assert_eq!(
        row.get::<String>(3).unwrap(),
        "reply visible in the web operator DM"
    );
}

#[tokio::test]
async fn command_worker_dispatches_non_message_intents_through_daemon_registry() {
    let state = test_state().await;
    CommandIntents::new(&state.store)
        .insert_pending(NewCommandIntent {
            command_id: "cmd_register_1".into(),
            kind: command_kinds::identity::REGISTER.into(),
            project: "default".into(),
            caller_name: "operator".into(),
            caller_session_id: Some("local-operator".into()),
            caller_agent_id: None,
            caller_runtime_id: None,
            caller_client_key: None,
            caller_kind: Some("human".into()),
            caller_tier: Some("admin".into()),
            idempotency_key: None,
            request_json: serde_json::to_string(&human_register("Ada", "ck_ada")).unwrap(),
            created_at: 1,
        })
        .await
        .unwrap();

    assert!(command_worker::process_next(&state).await.unwrap());
    let command = CommandIntents::new(&state.store)
        .get("cmd_register_1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(command.status, "done");
    let registered: RegisterResponse =
        serde_json::from_str(command.result_json.as_deref().unwrap()).unwrap();

    CommandIntents::new(&state.store)
        .insert_pending(NewCommandIntent {
            command_id: "cmd_thread_1".into(),
            kind: command_kinds::thread::CREATE.into(),
            project: "default".into(),
            caller_name: "Ada".into(),
            caller_session_id: Some(registered.session_id.0.clone()),
            caller_agent_id: registered.agent_id.as_ref().map(|id| id.0.clone()),
            caller_runtime_id: Some(registered.session_id.0.clone()),
            caller_client_key: Some("ck_ada".into()),
            caller_kind: Some("human".into()),
            caller_tier: Some("admin".into()),
            idempotency_key: None,
            request_json: serde_json::to_string(&CreateThreadRequest {
                name: "store-control".into(),
                members: vec![],
            })
            .unwrap(),
            created_at: 2,
        })
        .await
        .unwrap();

    assert!(command_worker::process_next(&state).await.unwrap());
    let command = CommandIntents::new(&state.store)
        .get("cmd_thread_1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(command.status, "done");
    assert!(Threads::new(&state.store)
        .find_by_name("default", "store-control")
        .await
        .unwrap()
        .is_some());

    CommandIntents::new(&state.store)
        .insert_pending(NewCommandIntent {
            command_id: "cmd_metadata_1".into(),
            kind: command_kinds::metadata::SET.into(),
            project: "default".into(),
            caller_name: "Ada".into(),
            caller_session_id: Some(registered.session_id.0.clone()),
            caller_agent_id: registered.agent_id.as_ref().map(|id| id.0.clone()),
            caller_runtime_id: Some(registered.session_id.0.clone()),
            caller_client_key: Some("ck_ada".into()),
            caller_kind: Some("human".into()),
            caller_tier: Some("admin".into()),
            idempotency_key: None,
            request_json: serde_json::to_string(&MetadataSetRequest {
                entity: MetadataEntityKind::Thread,
                id: "store-control".into(),
                metadata: serde_json::json!({ "launch": true }),
            })
            .unwrap(),
            created_at: 3,
        })
        .await
        .unwrap();

    assert!(command_worker::process_next(&state).await.unwrap());
    let command = CommandIntents::new(&state.store)
        .get("cmd_metadata_1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(command.status, "done");
    let result: serde_json::Value =
        serde_json::from_str(command.result_json.as_deref().unwrap()).unwrap();
    assert_eq!(result["entity"], "thread");
    assert_eq!(result["id"], "store-control");
    assert_eq!(result["metadata"], serde_json::json!({ "launch": true }));

    CommandIntents::new(&state.store)
        .insert_pending(NewCommandIntent {
            command_id: "cmd_thread_archive_1".into(),
            kind: command_kinds::thread::ARCHIVE.into(),
            project: "default".into(),
            caller_name: "Ada".into(),
            caller_session_id: Some(registered.session_id.0.clone()),
            caller_agent_id: registered.agent_id.as_ref().map(|id| id.0.clone()),
            caller_runtime_id: Some(registered.session_id.0.clone()),
            caller_client_key: Some("ck_ada".into()),
            caller_kind: Some("human".into()),
            caller_tier: Some("admin".into()),
            idempotency_key: None,
            request_json: serde_json::to_string(&ArchiveThreadRequest {
                name: "store-control".into(),
            })
            .unwrap(),
            created_at: 3,
        })
        .await
        .unwrap();

    assert!(command_worker::process_next(&state).await.unwrap());
    let command = CommandIntents::new(&state.store)
        .get("cmd_thread_archive_1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(command.status, "done");
    assert!(Threads::new(&state.store)
        .find_by_name("default", "store-control")
        .await
        .unwrap()
        .is_some());
    assert!(Threads::new(&state.store)
        .find_active_by_name("default", "store-control")
        .await
        .unwrap()
        .is_none());

    CommandIntents::new(&state.store)
        .insert_pending(NewCommandIntent {
            command_id: "cmd_thread_delete_1".into(),
            kind: command_kinds::thread::DELETE.into(),
            project: "default".into(),
            caller_name: "Ada".into(),
            caller_session_id: Some(registered.session_id.0.clone()),
            caller_agent_id: registered.agent_id.as_ref().map(|id| id.0.clone()),
            caller_runtime_id: Some(registered.session_id.0.clone()),
            caller_client_key: Some("ck_ada".into()),
            caller_kind: Some("human".into()),
            caller_tier: Some("admin".into()),
            idempotency_key: None,
            request_json: serde_json::to_string(&DeleteThreadRequest {
                name: "store-control".into(),
            })
            .unwrap(),
            created_at: 4,
        })
        .await
        .unwrap();

    assert!(command_worker::process_next(&state).await.unwrap());
    let command = CommandIntents::new(&state.store)
        .get("cmd_thread_delete_1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(command.status, "done");
    assert!(Threads::new(&state.store)
        .find_by_name("default", "store-control")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn thread_delete_uses_global_name_identity_not_project_scope() {
    let state = test_state().await;
    let registered = state
        .identity
        .register(human_register("Ada", "ck_ada"))
        .await
        .unwrap();
    let thread_id = ThreadId("t_smoke_thread_1319".into());
    let threads = Threads::new(&state.store);
    threads
        .create(&thread_id, "smoke_thread_1319", "smoke", "smoke_s_1319")
        .await
        .unwrap();
    threads
        .add_member(&thread_id, "smoke_r_1319")
        .await
        .unwrap();
    threads
        .add_member(&thread_id, "smoke_s_1319")
        .await
        .unwrap();

    CommandIntents::new(&state.store)
        .insert_pending(NewCommandIntent {
            command_id: "cmd_thread_delete_global_identity".into(),
            kind: command_kinds::thread::DELETE.into(),
            project: "default".into(),
            caller_name: "Ada".into(),
            caller_session_id: Some(registered.session_id.0.clone()),
            caller_agent_id: registered.agent_id.as_ref().map(|id| id.0.clone()),
            caller_runtime_id: Some(registered.session_id.0.clone()),
            caller_client_key: Some("ck_ada".into()),
            caller_kind: Some("human".into()),
            caller_tier: Some("admin".into()),
            idempotency_key: None,
            request_json: serde_json::to_string(&DeleteThreadRequest {
                name: "smoke_thread_1319".into(),
            })
            .unwrap(),
            created_at: 5,
        })
        .await
        .unwrap();

    assert!(command_worker::process_next(&state).await.unwrap());
    let command = CommandIntents::new(&state.store)
        .get("cmd_thread_delete_global_identity")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(command.status, "done");
    assert!(Threads::new(&state.store)
        .find_any_by_name("smoke_thread_1319")
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        Threads::new(&state.store)
            .members(&thread_id)
            .await
            .unwrap(),
        Vec::<String>::new()
    );
}

#[tokio::test]
async fn command_worker_preserves_local_operator_fallback_for_store_ingress() {
    let state = test_state().await;
    CommandIntents::new(&state.store)
        .insert_pending(NewCommandIntent {
            command_id: "cmd_operator_thread".into(),
            kind: command_kinds::thread::CREATE.into(),
            project: "default".into(),
            caller_name: "operator".into(),
            caller_session_id: Some("local-operator".into()),
            caller_agent_id: None,
            caller_runtime_id: None,
            caller_client_key: None,
            caller_kind: Some("human".into()),
            caller_tier: Some("admin".into()),
            idempotency_key: None,
            request_json: serde_json::to_string(&CreateThreadRequest {
                name: "operator-created".into(),
                members: vec![],
            })
            .unwrap(),
            created_at: 1,
        })
        .await
        .unwrap();

    assert!(command_worker::process_next(&state).await.unwrap());
    let command = CommandIntents::new(&state.store)
        .get("cmd_operator_thread")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(command.status, "done");
    let thread = Threads::new(&state.store)
        .find_by_name("default", "operator-created")
        .await
        .unwrap()
        .expect("thread exists");
    assert_eq!(
        Threads::new(&state.store)
            .members(&thread.thread_id)
            .await
            .unwrap(),
        vec!["operator".to_string()]
    );
}

#[tokio::test]
async fn direct_daemon_notify_dispatch_remains_unverified_and_dropped() {
    let state = test_state().await;
    let request = NotifyRequest {
        source: "ci".into(),
        topic: Some("builds".into()),
        payload: serde_json::json!({ "status": "green" }),
    };
    let response = dispatch::dispatch(
        &state,
        None,
        Request {
            jsonrpc: JSONRPC_VERSION.into(),
            id: None,
            method: "notify".into(),
            params: Some(serde_json::to_value(request).unwrap()),
        },
    )
    .await;
    assert!(response.error.is_none());
    let result = response.result.unwrap();
    assert_eq!(result["hmacOk"], false);
    assert_eq!(result["routedTo"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn verified_notification_command_reclaim_reuses_the_routed_message_and_delivery() {
    const TIMESTAMP: i64 = 1_784_000_000_000;
    const RAW_BODY: &str = r#"{"source":"ci","payload":{"marker":"PUBLIC-NOTIFY-RECLAIM"}}"#;
    const SIGNATURE: &str =
        "sha256=885609237a82d763d6361c1b7310be67ba6c194be50142dbca3006fb81f34607";

    let (mut state, mock) = test_state_with_notify_secret_and_mock().await;
    Topics::new(&state.store)
        .ensure("pub", "nexus")
        .await
        .unwrap();
    let recipient = state
        .launch_agent(
            SpawnRequest {
                kind: hid("claude"),
                name: Some("notify-recipient".into()),
                identity_policy: None,
                cwd: None,
                project: Some("nexus".into()),
                role: None,
                initial_prompt: None,
                resume: None,
                harness_args: Vec::new(),
                headless: true,
                backend: None,
            },
            "nexus",
            None,
        )
        .await
        .unwrap();
    let recipient_agent_id = Sessions::new(&state.store)
        .find_by_session_id(&recipient.session_id)
        .await
        .unwrap()
        .unwrap()
        .agent_id
        .unwrap();
    state.notify = Arc::new(Notify::new(
        state.store.clone(),
        state.bus.clone(),
        Arc::new(state.ws.clone()),
        RoutingRules::new(vec![RouteRule {
            source: Some("ci".into()),
            topic: None,
            // String-configured notification recipients are names. The bus resolves the current
            // runtime and persists both its canonical ID and display-name metadata.
            to: "notify-recipient".into(),
        }]),
    ));

    let request = NotifyCommandRequest {
        raw_body: RAW_BODY.into(),
        timestamp: TIMESTAMP.to_string(),
        signature: SIGNATURE.into(),
    };
    let intents = CommandIntents::new(&state.store);
    intents
        .insert_pending(NewCommandIntent {
            command_id: "cmd_public_notify_reclaim".into(),
            kind: command_kinds::notification::NOTIFY.into(),
            project: "nexus".into(),
            caller_name: "ci".into(),
            caller_session_id: Some("source:ci".into()),
            caller_agent_id: None,
            caller_runtime_id: Some("source:ci".into()),
            caller_client_key: None,
            caller_kind: Some("notification".into()),
            caller_tier: Some("agent".into()),
            idempotency_key: Some(format!("notify:{TIMESTAMP}:signed-digest")),
            request_json: serde_json::to_string(&request).unwrap(),
            created_at: TIMESTAMP,
        })
        .await
        .unwrap();

    assert!(command_worker::process_next(&state).await.unwrap());
    let first = intents
        .get("cmd_public_notify_reclaim")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.status, "done");
    let first_result: serde_json::Value =
        serde_json::from_str(first.result_json.as_deref().unwrap()).unwrap();
    assert_eq!(first_result["hmacOk"], true);
    assert_eq!(first_result["routedTo"].as_array().unwrap().len(), 1);
    let delivery_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
    while !mock
        .injected_prompts()
        .iter()
        .any(|prompt| prompt.contains("PUBLIC-NOTIFY-RECLAIM"))
    {
        assert!(
            tokio::time::Instant::now() < delivery_deadline,
            "verified public notification never reached the fake harness"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    state
        .store
        .conn
        .execute(
            "UPDATE command_intents SET status = 'claimed', result_json = NULL, error_json = NULL, \
             claimed_at = 1, started_at = NULL, lease_until = 1, completed_at = NULL \
             WHERE command_id = 'cmd_public_notify_reclaim'",
            (),
        )
        .await
        .unwrap();
    assert!(command_worker::process_next(&state).await.unwrap());
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(
        mock.injected_prompts()
            .iter()
            .filter(|prompt| prompt.contains("PUBLIC-NOTIFY-RECLAIM"))
            .count(),
        1,
        "command reclaim must not create a second automatic harness injection"
    );

    let mut rows = state
        .store
        .conn
        .query(
            "SELECT COUNT(*), COUNT(DISTINCT m.message_id), COUNT(f.in_flight_id), \
                    MAX(f.recipient_agent_id), MAX(m.to_name) \
             FROM messages m LEFT JOIN in_flight f ON f.message_id = m.message_id \
             WHERE m.body = '{\"marker\":\"PUBLIC-NOTIFY-RECLAIM\"}' \
               AND m.kind = 'dm' AND m.idempotency_key IS NOT NULL",
            (),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(
        row.get::<i64>(0).unwrap(),
        1,
        "one routed canonical message"
    );
    assert_eq!(row.get::<i64>(1).unwrap(), 1);
    assert_eq!(row.get::<i64>(2).unwrap(), 1, "one recipient delivery row");
    assert_eq!(
        row.get::<Option<String>>(3).unwrap().as_deref(),
        Some(recipient_agent_id.as_str()),
        "ID-only DM must retain stable agent identity"
    );
    assert_eq!(
        row.get::<Option<String>>(4).unwrap().as_deref(),
        Some("notify-recipient"),
        "resolved display-name metadata must be preserved"
    );

    let mut rows = state
        .store
        .conn
        .query(
            "SELECT provenance FROM messages \
             WHERE body = '{\"marker\":\"PUBLIC-NOTIFY-RECLAIM\"}' AND kind = 'dm' LIMIT 1",
            (),
        )
        .await
        .unwrap();
    let provenance: String = rows.next().await.unwrap().unwrap().get(0).unwrap();
    let provenance: serde_json::Value = serde_json::from_str(&provenance).unwrap();
    assert_eq!(
        provenance["kind"], "notification",
        "routed public notifications must retain notification provenance"
    );

    let mut rows = state
        .store
        .conn
        .query("SELECT COUNT(*) FROM notifications WHERE hmac_ok = 1", ())
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
        1,
        "command reclaim must retain one idempotent notification audit"
    );
    let mut rows = state
        .store
        .conn
        .query(
            "SELECT COUNT(*) FROM messages \
             WHERE message_id LIKE 'm_notify_%' \
               AND body = '{\"marker\":\"PUBLIC-NOTIFY-RECLAIM\"}'",
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
        1,
        "command reclaim must retain one standalone notification message"
    );
}

#[tokio::test]
async fn verified_notification_to_unreachable_target_is_terminal_without_retry() {
    const TIMESTAMP: i64 = 1_784_000_000_000;
    const RAW_BODY: &str = r#"{"source":"dead-ci","payload":{"marker":"PUBLIC-NOTIFY-DEAD"}}"#;
    const SIGNATURE: &str =
        "sha256=4d0a64b97d5aa84576a6b4412f0f33d4a821dadb94ecad4b3ec66f4742b0aa72";

    let mut state = test_state_with_notify_secret().await;
    Topics::new(&state.store)
        .ensure("pub", "nexus")
        .await
        .unwrap();
    let recipient = state
        .identity
        .register(agent_register("dead-notify-recipient", "ck_dead_notify"))
        .await
        .unwrap();
    Sessions::new(&state.store)
        .set_presence(&recipient.session_id, Presence::Offline)
        .await
        .unwrap();
    AgentRuntimes::new(&state.store)
        .stop(&recipient.session_id.0)
        .await
        .unwrap();
    state.notify = Arc::new(Notify::new(
        state.store.clone(),
        state.bus.clone(),
        Arc::new(state.ws.clone()),
        RoutingRules::new(vec![RouteRule {
            source: Some("dead-ci".into()),
            topic: None,
            to: "dead-notify-recipient".into(),
        }]),
    ));
    let intents = CommandIntents::new(&state.store);
    intents
        .insert_pending(NewCommandIntent {
            command_id: "cmd_public_notify_dead_target".into(),
            kind: command_kinds::notification::NOTIFY.into(),
            project: "default".into(),
            caller_name: "dead-ci".into(),
            caller_session_id: Some("source:dead-ci".into()),
            caller_agent_id: None,
            caller_runtime_id: Some("source:dead-ci".into()),
            caller_client_key: None,
            caller_kind: Some("notification".into()),
            caller_tier: Some("agent".into()),
            idempotency_key: Some(format!("notify:{TIMESTAMP}:dead-target")),
            request_json: serde_json::to_string(&NotifyCommandRequest {
                raw_body: RAW_BODY.into(),
                timestamp: TIMESTAMP.to_string(),
                signature: SIGNATURE.into(),
            })
            .unwrap(),
            created_at: TIMESTAMP,
        })
        .await
        .unwrap();

    assert!(command_worker::process_next(&state).await.unwrap());
    let command = intents
        .get("cmd_public_notify_dead_target")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(command.status, "done", "{:?}", command.error_json);
    let mut rows = state
        .store
        .conn
        .query(
            "SELECT f.state, f.attempt_count, f.error_code, f.error_reason \
             FROM messages m JOIN in_flight f ON f.message_id = m.message_id \
             WHERE m.body = '{\"marker\":\"PUBLIC-NOTIFY-DEAD\"}' AND m.kind = 'dm'",
            (),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().expect("routed delivery row");
    assert_eq!(
        row.get::<String>(0).unwrap(),
        "error",
        "a known unreachable notification target must not stay automatically retryable"
    );
    assert_eq!(row.get::<i64>(1).unwrap(), 0, "no harness attempt occurred");
    assert!(matches!(
        row.get::<Option<String>>(2).unwrap().as_deref(),
        Some(nexus_store::repos::inbox::TARGET_DEAD_ERROR_CODE)
            | Some(nexus_store::repos::inbox::TARGET_UNREACHABLE_ERROR_CODE)
    ));
    assert!(row
        .get::<Option<String>>(3)
        .unwrap()
        .unwrap_or_default()
        .contains("could not be revived"));

    drop(rows);
    state
        .store
        .conn
        .execute(
            "UPDATE command_intents SET status = 'claimed', result_json = NULL, error_json = NULL, \
             claimed_at = 1, started_at = NULL, lease_until = 1, completed_at = NULL \
             WHERE command_id = 'cmd_public_notify_dead_target'",
            (),
        )
        .await
        .unwrap();
    assert!(command_worker::process_next(&state).await.unwrap());
    let mut rows = state
        .store
        .conn
        .query(
            "SELECT COUNT(*), MAX(f.state), MAX(f.attempt_count), MAX(f.error_code) \
             FROM messages m JOIN in_flight f ON f.message_id = m.message_id \
             WHERE m.body = '{\"marker\":\"PUBLIC-NOTIFY-DEAD\"}' AND m.kind = 'dm'",
            (),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 1, "reclaim reused one delivery");
    assert_eq!(
        row.get::<Option<String>>(1).unwrap().as_deref(),
        Some("error")
    );
    assert_eq!(row.get::<Option<i64>>(2).unwrap(), Some(0));
    assert!(matches!(
        row.get::<Option<String>>(3).unwrap().as_deref(),
        Some(nexus_store::repos::inbox::TARGET_DEAD_ERROR_CODE)
            | Some(nexus_store::repos::inbox::TARGET_UNREACHABLE_ERROR_CODE)
    ));
}

#[tokio::test]
async fn verified_notification_uses_pull_owner_and_reclaim_has_no_second_batch() {
    const TIMESTAMP: i64 = 1_784_000_000_000;
    const RAW_BODY: &str = r#"{"source":"pull-ci","payload":{"marker":"PUBLIC-NOTIFY-PULL"}}"#;
    const SIGNATURE: &str =
        "sha256=b221cf27dbd1fb000da3bac7bc8973490115623b0c7fdfa67a07e233952d6417";

    let mut state = test_state_with_notify_secret().await;
    Topics::new(&state.store)
        .ensure("pub", "nexus")
        .await
        .unwrap();
    let recipient = state
        .identity
        .register(agent_register("pull-notify-recipient", "ck_pull_notify"))
        .await
        .unwrap();
    let recipient_caller = state
        .identity
        .resolve("default", "pull-notify-recipient")
        .await
        .unwrap();
    let subscribe = dispatch::dispatch(
        &state,
        Some(recipient_caller.clone()),
        Request {
            jsonrpc: JSONRPC_VERSION.into(),
            id: None,
            method: "inbox.subscribe".into(),
            params: Some(
                serde_json::to_value(InboxSubscribeRequest {
                    timeout_ms: Some(0),
                    max: Some(10),
                })
                .unwrap(),
            ),
        },
    )
    .await;
    assert!(subscribe.error.is_none(), "{subscribe:?}");
    let subscription_id = subscribe.result.unwrap()["subscriptionId"]
        .as_str()
        .unwrap()
        .to_string();
    let recipient_agent_id = Sessions::new(&state.store)
        .find_by_session_id(&recipient.session_id)
        .await
        .unwrap()
        .unwrap()
        .agent_id
        .unwrap();
    state.notify = Arc::new(Notify::new(
        state.store.clone(),
        state.bus.clone(),
        Arc::new(state.ws.clone()),
        RoutingRules::new(vec![RouteRule {
            source: Some("pull-ci".into()),
            topic: None,
            to: "pull-notify-recipient".into(),
        }]),
    ));
    let intents = CommandIntents::new(&state.store);
    intents
        .insert_pending(NewCommandIntent {
            command_id: "cmd_public_notify_pull_owner".into(),
            kind: command_kinds::notification::NOTIFY.into(),
            project: "default".into(),
            caller_name: "pull-ci".into(),
            caller_session_id: Some("source:pull-ci".into()),
            caller_agent_id: None,
            caller_runtime_id: Some("source:pull-ci".into()),
            caller_client_key: None,
            caller_kind: Some("notification".into()),
            caller_tier: Some("agent".into()),
            idempotency_key: Some(format!("notify:{TIMESTAMP}:pull-owner")),
            request_json: serde_json::to_string(&NotifyCommandRequest {
                raw_body: RAW_BODY.into(),
                timestamp: TIMESTAMP.to_string(),
                signature: SIGNATURE.into(),
            })
            .unwrap(),
            created_at: TIMESTAMP,
        })
        .await
        .unwrap();

    assert!(command_worker::process_next(&state).await.unwrap());
    let next = dispatch::dispatch(
        &state,
        Some(recipient_caller.clone()),
        Request {
            jsonrpc: JSONRPC_VERSION.into(),
            id: None,
            method: "inbox.next".into(),
            params: Some(
                serde_json::to_value(InboxSubscriptionNextRequest {
                    subscription_id: subscription_id.clone(),
                    timeout_ms: Some(0),
                })
                .unwrap(),
            ),
        },
    )
    .await;
    assert!(next.error.is_none(), "{next:?}");
    let next: InboxSubscriptionNextResponse = serde_json::from_value(next.result.unwrap()).unwrap();
    let batch = next.batch.expect("notification pull batch");
    assert_eq!(batch.batch.counts.total, 1);
    assert_eq!(batch.batch.dms[0].kind, Kind::Notification);
    assert!(batch.batch.dms[0].body.contains("PUBLIC-NOTIFY-PULL"));

    let batch_ack = dispatch::dispatch(
        &state,
        Some(recipient_caller.clone()),
        Request {
            jsonrpc: JSONRPC_VERSION.into(),
            id: None,
            method: "inbox.subscriptionAck".into(),
            params: Some(
                serde_json::to_value(InboxSubscriptionAckRequest {
                    subscription_id: subscription_id.clone(),
                    batch_id: batch.batch_id.clone(),
                })
                .unwrap(),
            ),
        },
    )
    .await;
    assert!(batch_ack.error.is_none(), "{batch_ack:?}");

    state
        .store
        .conn
        .execute(
            "UPDATE command_intents SET status = 'claimed', result_json = NULL, error_json = NULL, \
             claimed_at = 1, started_at = NULL, lease_until = 1, completed_at = NULL \
             WHERE command_id = 'cmd_public_notify_pull_owner'",
            (),
        )
        .await
        .unwrap();
    assert!(command_worker::process_next(&state).await.unwrap());
    let after_reclaim = dispatch::dispatch(
        &state,
        Some(recipient_caller),
        Request {
            jsonrpc: JSONRPC_VERSION.into(),
            id: None,
            method: "inbox.next".into(),
            params: Some(
                serde_json::to_value(InboxSubscriptionNextRequest {
                    subscription_id,
                    timeout_ms: Some(0),
                })
                .unwrap(),
            ),
        },
    )
    .await;
    let after_reclaim: InboxSubscriptionNextResponse =
        serde_json::from_value(after_reclaim.result.unwrap()).unwrap();
    assert!(
        after_reclaim.batch.is_none(),
        "reclaim produced a second batch"
    );

    let mut rows = state
        .store
        .conn
        .query(
            "SELECT COUNT(*), MAX(f.state), MAX(f.recipient_agent_id), MAX(m.to_name) \
             FROM messages m JOIN in_flight f ON f.message_id = m.message_id \
             WHERE m.body = '{\"marker\":\"PUBLIC-NOTIFY-PULL\"}' AND m.kind = 'dm'",
            (),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 1);
    assert_eq!(
        row.get::<Option<String>>(1).unwrap().as_deref(),
        Some("acked")
    );
    assert_eq!(
        row.get::<Option<String>>(2).unwrap().as_deref(),
        Some(recipient_agent_id.as_str())
    );
    assert_eq!(
        row.get::<Option<String>>(3).unwrap().as_deref(),
        Some("pull-notify-recipient")
    );
}

#[tokio::test]
async fn invalid_verified_notification_envelope_creates_no_notification_state() {
    let state = test_state_with_notify_secret().await;
    let request = NotifyCommandRequest {
        raw_body: r#"{"source":"ci","payload":{"status":"green"}}"#.into(),
        timestamp: "1784000000000".into(),
        signature: format!("sha256={}", "0".repeat(64)),
    };
    CommandIntents::new(&state.store)
        .insert_pending(NewCommandIntent {
            command_id: "cmd_public_notify_invalid".into(),
            kind: command_kinds::notification::NOTIFY.into(),
            project: "nexus".into(),
            caller_name: "ci".into(),
            caller_session_id: Some("source:ci".into()),
            caller_agent_id: None,
            caller_runtime_id: Some("source:ci".into()),
            caller_client_key: None,
            caller_kind: Some("notification".into()),
            caller_tier: Some("agent".into()),
            idempotency_key: Some("notify:invalid".into()),
            request_json: serde_json::to_string(&request).unwrap(),
            created_at: 1_784_000_000_000,
        })
        .await
        .unwrap();

    assert!(command_worker::process_next(&state).await.unwrap());
    let command = CommandIntents::new(&state.store)
        .get("cmd_public_notify_invalid")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(command.status, "error");
    let counts = state
        .store
        .conn
        .query(
            "SELECT (SELECT COUNT(*) FROM notifications), \
                    (SELECT COUNT(*) FROM messages), \
                    (SELECT COUNT(*) FROM in_flight)",
            (),
        )
        .await
        .unwrap();
    let mut counts = counts;
    let row = counts.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 0);
    assert_eq!(row.get::<i64>(1).unwrap(), 0);
    assert_eq!(row.get::<i64>(2).unwrap(), 0);
    assert!(Notifications::new(&state.store)
        .get("does-not-exist")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn verified_notification_rejects_ambiguous_routes_before_pub_or_ingest() {
    const TIMESTAMP: i64 = 1_784_000_000_000;
    const RAW_BODY: &str = r#"{"source":"ci","payload":{"marker":"PUBLIC-NOTIFY-RECLAIM"}}"#;
    const SIGNATURE: &str =
        "sha256=885609237a82d763d6361c1b7310be67ba6c194be50142dbca3006fb81f34607";

    let mut state = test_state_with_notify_secret().await;
    Topics::new(&state.store)
        .ensure("pub", "nexus")
        .await
        .unwrap();
    state
        .store
        .identity_conn()
        .execute("DROP INDEX idx_agents_name_unique", ())
        .await
        .unwrap();
    for (agent_id, project) in [("a_route_one", "one"), ("a_route_two", "two")] {
        Agents::new(&state.store)
            .create(NewAgent {
                agent_id: agent_id.into(),
                project: project.into(),
                name: Some("ambiguous-route".into()),
                default_harness: Some("claude".into()),
                role: None,
                tier: Some("agent".into()),
                owner: None,
            })
            .await
            .unwrap();
    }
    state.notify = Arc::new(Notify::new(
        state.store.clone(),
        state.bus.clone(),
        Arc::new(state.ws.clone()),
        RoutingRules::new(vec![RouteRule {
            source: Some("ci".into()),
            topic: None,
            to: "ambiguous-route".into(),
        }]),
    ));
    let request = NotifyCommandRequest {
        raw_body: RAW_BODY.into(),
        timestamp: TIMESTAMP.to_string(),
        signature: SIGNATURE.into(),
    };
    CommandIntents::new(&state.store)
        .insert_pending(NewCommandIntent {
            command_id: "cmd_public_notify_ambiguous".into(),
            kind: command_kinds::notification::NOTIFY.into(),
            project: "metadata-only".into(),
            caller_name: "ci".into(),
            caller_session_id: Some("source:ci".into()),
            caller_agent_id: None,
            caller_runtime_id: Some("source:ci".into()),
            caller_client_key: None,
            caller_kind: Some("notification".into()),
            caller_tier: Some("agent".into()),
            idempotency_key: Some("notify:ambiguous-route".into()),
            request_json: serde_json::to_string(&request).unwrap(),
            created_at: TIMESTAMP,
        })
        .await
        .unwrap();

    assert!(command_worker::process_next(&state).await.unwrap());
    let command = CommandIntents::new(&state.store)
        .get("cmd_public_notify_ambiguous")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(command.status, "error");
    let error = command.error_json.unwrap();
    assert!(error.contains("ambiguous"), "unexpected error: {error}");
    let mut rows = state
        .store
        .conn
        .query(
            "SELECT (SELECT COUNT(*) FROM notifications), \
                    (SELECT COUNT(*) FROM messages), \
                    (SELECT COUNT(*) FROM in_flight)",
            (),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 0);
    assert_eq!(row.get::<i64>(1).unwrap(), 0);
    assert_eq!(row.get::<i64>(2).unwrap(), 0);
}

#[tokio::test]
async fn command_worker_dispatches_source_push_as_notification_principal_without_registered_caller()
{
    let state = test_state().await;
    Sources::new(&state.store)
        .create("ci-source", "src_token", "builds", 1)
        .await
        .unwrap();

    let request = PushRequest {
        source: "ci-source".into(),
        topic: None,
        summary: Some("Build finished".into()),
        body: "green".into(),
        meta: None,
    };
    CommandIntents::new(&state.store)
        .insert_pending(NewCommandIntent {
            command_id: "cmd_source_push_1".into(),
            kind: command_kinds::source::PUSH.into(),
            project: "default".into(),
            caller_name: "ci-source".into(),
            caller_session_id: Some("source:ci-source".into()),
            caller_agent_id: None,
            caller_runtime_id: Some("source:ci-source".into()),
            caller_client_key: None,
            caller_kind: Some("notification".into()),
            caller_tier: Some("agent".into()),
            idempotency_key: None,
            request_json: serde_json::to_string(&request).unwrap(),
            created_at: 1,
        })
        .await
        .unwrap();

    assert!(command_worker::process_next(&state).await.unwrap());
    let command = CommandIntents::new(&state.store)
        .get("cmd_source_push_1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(command.status, "done");
    let result: serde_json::Value =
        serde_json::from_str(command.result_json.as_deref().unwrap()).unwrap();
    assert_eq!(result["topic"], "builds");
    assert_eq!(result["queuedTo"], 0);
    assert!(result["messageId"].is_null());

    let source = Sources::new(&state.store)
        .find("ci-source")
        .await
        .unwrap()
        .unwrap();
    assert!(source.last_fired_at.is_some());
}

#[tokio::test]
async fn reclaimed_keyless_local_source_command_uses_its_command_id_once() {
    let state = test_state().await;
    let recipient = state
        .identity
        .register(human_register(
            "Local Recipient",
            "ck_local_source_recipient",
        ))
        .await
        .unwrap();
    Sources::new(&state.store)
        .create("local-source", "local-token", "local-builds", 1)
        .await
        .unwrap();
    Topics::new(&state.store)
        .ensure("local-builds", "default")
        .await
        .unwrap();
    Topics::new(&state.store)
        .subscribe("local-builds", &recipient.session_id.0, None)
        .await
        .unwrap();

    let request = PushRequest {
        source: "local-source".into(),
        topic: None,
        summary: Some("local build".into()),
        body: "LOCAL-SOURCE-RECLAIM".into(),
        meta: None,
    };
    let intents = CommandIntents::new(&state.store);
    intents
        .insert_pending(NewCommandIntent {
            command_id: "cmd_local_source_reclaim".into(),
            kind: command_kinds::source::PUSH.into(),
            project: "default".into(),
            caller_name: "operator".into(),
            caller_session_id: Some("local-operator".into()),
            caller_agent_id: None,
            caller_runtime_id: None,
            caller_client_key: None,
            caller_kind: Some("human".into()),
            caller_tier: Some("admin".into()),
            idempotency_key: None,
            request_json: serde_json::to_string(&request).unwrap(),
            created_at: 1,
        })
        .await
        .unwrap();

    assert!(command_worker::process_next(&state).await.unwrap());
    let first = intents
        .get("cmd_local_source_reclaim")
        .await
        .unwrap()
        .unwrap();
    let first_result: serde_json::Value =
        serde_json::from_str(first.result_json.as_deref().unwrap()).unwrap();
    let message_id = first_result["messageId"].as_str().unwrap();
    state
        .store
        .conn
        .execute(
            "UPDATE messages SET created_at = created_at - 10 WHERE message_id = ?1",
            [message_id],
        )
        .await
        .unwrap();
    state
        .store
        .conn
        .execute(
            "UPDATE command_intents SET status = 'claimed', result_json = NULL, error_json = NULL, \
             claimed_at = 1, started_at = NULL, lease_until = 1, completed_at = NULL \
             WHERE command_id = 'cmd_local_source_reclaim'",
            (),
        )
        .await
        .unwrap();

    assert!(command_worker::process_next(&state).await.unwrap());
    let reclaimed = intents
        .get("cmd_local_source_reclaim")
        .await
        .unwrap()
        .unwrap();
    let reclaimed_result: serde_json::Value =
        serde_json::from_str(reclaimed.result_json.as_deref().unwrap()).unwrap();
    assert_eq!(reclaimed_result, first_result);

    let mut rows = state
        .store
        .conn
        .query(
            "SELECT COUNT(*), COUNT(DISTINCT message_id), MAX(idempotency_key) \
             FROM messages WHERE body = 'LOCAL-SOURCE-RECLAIM'",
            (),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 1);
    assert_eq!(row.get::<i64>(1).unwrap(), 1);
    assert_eq!(
        row.get::<String>(2).unwrap(),
        "source-command:cmd_local_source_reclaim"
    );
}

/// A validated [`nexus_contracts::HarnessId`] from a literal (panics on invalid — test-only).
fn hid(s: &str) -> nexus_contracts::HarnessId {
    nexus_contracts::HarnessId::new(s).expect("valid harness id literal")
}
