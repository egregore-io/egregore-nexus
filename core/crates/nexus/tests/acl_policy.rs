use std::sync::Arc;

use nexus::daemon::{command_worker, AppState};
use nexus_common::Config;
use nexus_contracts::{
    AdminGroupAssignRequest, CreateThreadRequest, Kind, RegisterRequest, RegisterResponse,
    SendRequest, SendTarget, Tier,
};
use nexus_store::command_kinds;
use nexus_store::repos::{AgentGroups, CommandIntents, NewCommandIntent, Threads};
use nexus_store::Store;

async fn test_state() -> AppState {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let state = AppState::wire(store, &Config::default());
    state
        .wait_for_runtime_identity_ready()
        .await
        .expect("runtime identity ready");
    state
}

fn register_agent(name: &str, client_key: &str) -> RegisterRequest {
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
        locality: Default::default(),
        access: None,
        role: None,
        cwd: None,
    }
}

fn register_admin(name: &str, client_key: &str) -> RegisterRequest {
    RegisterRequest {
        tier: Tier::Admin,
        kind: Some(Kind::Human),
        locality: Default::default(),
        access: None,
        ..register_agent(name, client_key)
    }
}

async fn enqueue_send(
    state: &AppState,
    command_id: &str,
    caller: &RegisterResponse,
    caller_name: &str,
    caller_key: &str,
    request: SendRequest,
) {
    CommandIntents::new(&state.store)
        .insert_pending(NewCommandIntent {
            command_id: command_id.into(),
            kind: command_kinds::message_post::SEND.into(),
            project: "default".into(),
            caller_name: caller_name.into(),
            caller_session_id: Some(caller.session_id.0.clone()),
            caller_agent_id: caller.agent_id.as_ref().map(|id| id.0.clone()),
            caller_runtime_id: Some(caller.session_id.0.clone()),
            caller_client_key: Some(caller_key.into()),
            caller_principal_id: None,
            caller_kind: Some("agent".into()),
            caller_tier: Some("agent".into()),
            idempotency_key: None,
            request_json: serde_json::to_string(&request).unwrap(),
            created_at: 1,
        })
        .await
        .unwrap();
}

async fn message_count_by_body(state: &AppState, body: &str) -> i64 {
    let mut rows = state
        .store
        .conn
        .query(
            "SELECT COUNT(*) FROM messages WHERE body = ?1",
            libsql::params![body],
        )
        .await
        .unwrap();
    rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap()
}

#[tokio::test]
async fn admin_group_assign_command_creates_membership() {
    let state = test_state().await;
    state
        .identity
        .register(register_admin("Alex Morgan", "ck_operator"))
        .await
        .unwrap();
    let blake = state
        .identity
        .register(register_agent("blake", "ck_blake"))
        .await
        .unwrap();
    let admin = state
        .identity
        .resolve("default", "Alex Morgan")
        .await
        .unwrap();

    CommandIntents::new(&state.store)
        .insert_pending(NewCommandIntent {
            command_id: "cmd_group_assign".into(),
            kind: command_kinds::admin::GROUP_ASSIGN.into(),
            project: "default".into(),
            caller_name: "Alex Morgan".into(),
            caller_session_id: Some(admin.session.0.clone()),
            caller_agent_id: admin.agent_id.as_ref().map(|id| id.0.clone()),
            caller_runtime_id: Some(admin.session.0.clone()),
            caller_client_key: Some("ck_operator".into()),
            caller_principal_id: None,
            caller_kind: Some("human".into()),
            caller_tier: Some("admin".into()),
            idempotency_key: None,
            request_json: serde_json::to_string(&AdminGroupAssignRequest {
                group: "backend".into(),
                agent_id: None,
                name: "blake".into(),
                project: None,
            })
            .unwrap(),
            created_at: 1,
        })
        .await
        .unwrap();

    assert!(command_worker::process_next(&state).await.unwrap());
    let command = CommandIntents::new(&state.store)
        .get("cmd_group_assign")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(command.status, "done");
    assert!(AgentGroups::new(&state.store)
        .is_member("default", "backend", blake.agent_id.as_ref().unwrap())
        .await
        .unwrap());
}

#[tokio::test]
async fn policy_rejects_cross_group_agent_dm_before_message_write() {
    let state = test_state().await;
    let blake = state
        .identity
        .register(register_agent("blake", "ck_blake"))
        .await
        .unwrap();
    let bianca = state
        .identity
        .register(register_agent("bianca", "ck_bianca"))
        .await
        .unwrap();
    let groups = AgentGroups::new(&state.store);
    groups
        .assign(
            "default",
            "backend",
            blake.agent_id.as_ref().unwrap(),
            "blake",
        )
        .await
        .unwrap();
    groups
        .assign(
            "default",
            "runtime",
            bianca.agent_id.as_ref().unwrap(),
            "bianca",
        )
        .await
        .unwrap();

    enqueue_send(
        &state,
        "cmd_blocked_dm",
        &blake,
        "blake",
        "ck_blake",
        SendRequest {
            to: SendTarget::dm_name("bianca"),
            summary: None,
            body: "this should not cross groups".into(),
            mention: vec![],
            metadata: None,
            idempotency_key: None,
        },
    )
    .await;

    assert!(command_worker::process_next(&state).await.unwrap());
    let command = CommandIntents::new(&state.store)
        .get("cmd_blocked_dm")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(command.status, "error");
    let error: serde_json::Value = serde_json::from_str(command.error_json.as_deref().unwrap())
        .expect("structured command error");
    assert_eq!(error["code"], nexus_contracts::codes::UNAUTHORIZED);
    assert!(
        error["message"]
            .as_str()
            .unwrap()
            .contains("message policy denied"),
        "{error:?}"
    );

    let mut rows = state
        .store
        .conn
        .query(
            "SELECT COUNT(*) FROM messages WHERE body = 'this should not cross groups'",
            (),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 0);
}

#[tokio::test]
async fn policy_allows_cross_group_dm_for_current_active_thread_comembers() {
    let state = test_state().await;
    let blake = state
        .identity
        .register(register_agent("blake", "ck_blake"))
        .await
        .unwrap();
    let bianca = state
        .identity
        .register(register_agent("bianca", "ck_bianca"))
        .await
        .unwrap();
    let groups = AgentGroups::new(&state.store);
    groups
        .assign(
            "default",
            "backend",
            blake.agent_id.as_ref().unwrap(),
            "blake",
        )
        .await
        .unwrap();
    groups
        .assign(
            "default",
            "runtime",
            bianca.agent_id.as_ref().unwrap(),
            "bianca",
        )
        .await
        .unwrap();

    let caller = state.identity.resolve("default", "blake").await.unwrap();
    state
        .bus
        .create_thread(
            &caller,
            CreateThreadRequest {
                name: "shared-lane".into(),
                members: vec!["bianca".into()],
            },
        )
        .await
        .unwrap();

    enqueue_send(
        &state,
        "cmd_thread_comember_dm",
        &blake,
        "blake",
        "ck_blake",
        SendRequest {
            to: SendTarget::dm_name("bianca"),
            summary: None,
            body: "shared thread permits a direct follow-up".into(),
            mention: vec![],
            metadata: None,
            idempotency_key: None,
        },
    )
    .await;

    assert!(command_worker::process_next(&state).await.unwrap());
    let command = CommandIntents::new(&state.store)
        .get("cmd_thread_comember_dm")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(command.status, "done");
    assert_eq!(
        message_count_by_body(&state, "shared thread permits a direct follow-up").await,
        1
    );
}

#[tokio::test]
async fn policy_revokes_thread_comember_dm_after_member_removal() {
    let state = test_state().await;
    let blake = state
        .identity
        .register(register_agent("blake", "ck_blake"))
        .await
        .unwrap();
    let bianca = state
        .identity
        .register(register_agent("bianca", "ck_bianca"))
        .await
        .unwrap();
    let groups = AgentGroups::new(&state.store);
    groups
        .assign(
            "default",
            "backend",
            blake.agent_id.as_ref().unwrap(),
            "blake",
        )
        .await
        .unwrap();
    groups
        .assign(
            "default",
            "runtime",
            bianca.agent_id.as_ref().unwrap(),
            "bianca",
        )
        .await
        .unwrap();

    let caller = state.identity.resolve("default", "blake").await.unwrap();
    state
        .bus
        .create_thread(
            &caller,
            CreateThreadRequest {
                name: "removed-lane".into(),
                members: vec!["bianca".into()],
            },
        )
        .await
        .unwrap();
    let thread = Threads::new(&state.store)
        .find_by_name("default", "removed-lane")
        .await
        .unwrap()
        .unwrap();
    Threads::new(&state.store)
        .remove_member(&thread.thread_id, "bianca")
        .await
        .unwrap();

    enqueue_send(
        &state,
        "cmd_removed_thread_dm",
        &blake,
        "blake",
        "ck_blake",
        SendRequest {
            to: SendTarget::dm_name("bianca"),
            summary: None,
            body: "removed co-member cannot dm".into(),
            mention: vec![],
            metadata: None,
            idempotency_key: None,
        },
    )
    .await;

    assert!(command_worker::process_next(&state).await.unwrap());
    let command = CommandIntents::new(&state.store)
        .get("cmd_removed_thread_dm")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(command.status, "error");
    let error: serde_json::Value =
        serde_json::from_str(command.error_json.as_deref().unwrap()).unwrap();
    assert_eq!(error["code"], nexus_contracts::codes::UNAUTHORIZED);
    assert_eq!(
        message_count_by_body(&state, "removed co-member cannot dm").await,
        0
    );
}

#[tokio::test]
async fn policy_ignores_archived_threads_for_comember_dm_allowance() {
    let state = test_state().await;
    let blake = state
        .identity
        .register(register_agent("blake", "ck_blake"))
        .await
        .unwrap();
    let bianca = state
        .identity
        .register(register_agent("bianca", "ck_bianca"))
        .await
        .unwrap();
    let groups = AgentGroups::new(&state.store);
    groups
        .assign(
            "default",
            "backend",
            blake.agent_id.as_ref().unwrap(),
            "blake",
        )
        .await
        .unwrap();
    groups
        .assign(
            "default",
            "runtime",
            bianca.agent_id.as_ref().unwrap(),
            "bianca",
        )
        .await
        .unwrap();

    let caller = state.identity.resolve("default", "blake").await.unwrap();
    state
        .bus
        .create_thread(
            &caller,
            CreateThreadRequest {
                name: "archived-lane".into(),
                members: vec!["bianca".into()],
            },
        )
        .await
        .unwrap();
    Threads::new(&state.store)
        .archive("archived-lane")
        .await
        .unwrap();

    enqueue_send(
        &state,
        "cmd_archived_thread_dm",
        &blake,
        "blake",
        "ck_blake",
        SendRequest {
            to: SendTarget::dm_name("bianca"),
            summary: None,
            body: "archived co-member cannot dm".into(),
            mention: vec![],
            metadata: None,
            idempotency_key: None,
        },
    )
    .await;

    assert!(command_worker::process_next(&state).await.unwrap());
    let command = CommandIntents::new(&state.store)
        .get("cmd_archived_thread_dm")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(command.status, "error");
    let error: serde_json::Value =
        serde_json::from_str(command.error_json.as_deref().unwrap()).unwrap();
    assert_eq!(error["code"], nexus_contracts::codes::UNAUTHORIZED);
    assert_eq!(
        message_count_by_body(&state, "archived co-member cannot dm").await,
        0
    );
}

#[tokio::test]
async fn policy_keeps_authorized_thread_fanout_working() {
    let state = test_state().await;
    let blake = state
        .identity
        .register(register_agent("blake", "ck_blake"))
        .await
        .unwrap();
    let bianca = state
        .identity
        .register(register_agent("bianca", "ck_bianca"))
        .await
        .unwrap();
    let groups = AgentGroups::new(&state.store);
    groups
        .assign(
            "default",
            "backend",
            blake.agent_id.as_ref().unwrap(),
            "blake",
        )
        .await
        .unwrap();
    groups
        .assign(
            "default",
            "runtime",
            bianca.agent_id.as_ref().unwrap(),
            "bianca",
        )
        .await
        .unwrap();

    let caller = state.identity.resolve("default", "blake").await.unwrap();
    state
        .bus
        .create_thread(
            &caller,
            CreateThreadRequest {
                name: "lane".into(),
                members: vec!["bianca".into()],
            },
        )
        .await
        .unwrap();

    enqueue_send(
        &state,
        "cmd_thread_post",
        &blake,
        "blake",
        "ck_blake",
        SendRequest {
            to: SendTarget::Post {
                thread: "lane".into(),
            },
            summary: None,
            body: "thread membership is the authorization surface".into(),
            mention: vec![],
            metadata: None,
            idempotency_key: None,
        },
    )
    .await;

    assert!(command_worker::process_next(&state).await.unwrap());
    let command = CommandIntents::new(&state.store)
        .get("cmd_thread_post")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(command.status, "done");
    let result: serde_json::Value =
        serde_json::from_str(command.result_json.as_deref().unwrap()).expect("command result json");
    assert!(
        result.get("fanout").is_none(),
        "authorized fanout is enforced internally and must not leak into the public result"
    );

    let mut rows = state
        .store
        .conn
        .query(
            &format!(
                "SELECT COUNT(*) FROM in_flight WHERE recipient_agent_id = '{}'",
                bianca.agent_id.as_ref().unwrap().0.replace('\'', "''")
            ),
            (),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 1);
}

#[tokio::test]
async fn policy_rejects_non_member_thread_post_before_message_write() {
    let state = test_state().await;
    state
        .identity
        .register(register_agent("blake", "ck_blake"))
        .await
        .unwrap();
    state
        .identity
        .register(register_agent("bianca", "ck_bianca"))
        .await
        .unwrap();
    let roman = state
        .identity
        .register(register_agent("roman", "ck_roman"))
        .await
        .unwrap();

    let caller = state.identity.resolve("default", "blake").await.unwrap();
    state
        .bus
        .create_thread(
            &caller,
            CreateThreadRequest {
                name: "private-lane".into(),
                members: vec!["bianca".into()],
            },
        )
        .await
        .unwrap();

    enqueue_send(
        &state,
        "cmd_thread_blocked",
        &roman,
        "roman",
        "ck_roman",
        SendRequest {
            to: SendTarget::Post {
                thread: "private-lane".into(),
            },
            summary: None,
            body: "non-members cannot post into the lane".into(),
            mention: vec![],
            metadata: None,
            idempotency_key: None,
        },
    )
    .await;

    assert!(command_worker::process_next(&state).await.unwrap());
    let command = CommandIntents::new(&state.store)
        .get("cmd_thread_blocked")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(command.status, "error");
    let error: serde_json::Value =
        serde_json::from_str(command.error_json.as_deref().unwrap()).unwrap();
    assert_eq!(error["code"], nexus_contracts::codes::UNAUTHORIZED);
    assert!(
        error["message"]
            .as_str()
            .unwrap()
            .contains("is not a member of thread"),
        "{error:?}"
    );

    let mut rows = state
        .store
        .conn
        .query(
            "SELECT COUNT(*) FROM messages WHERE body = 'non-members cannot post into the lane'",
            (),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 0);
}

/// A validated [`nexus_contracts::HarnessId`] from a literal (panics on invalid — test-only).
fn hid(s: &str) -> nexus_contracts::HarnessId {
    nexus_contracts::HarnessId::new(s).expect("valid harness id literal")
}
