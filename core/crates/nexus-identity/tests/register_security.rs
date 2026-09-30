use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use libsql;
use nexus_common::{hash_runtime_credential, Config};
use nexus_contracts::events::WsEvent;
use nexus_contracts::ids::{AgentId, SessionId};
use nexus_contracts::ports::{Caller, EventSink, IdentityPort};
use nexus_contracts::register::{RegisterRequest, RenameRequest};
use nexus_contracts::{codes, Harness, Tier};
use nexus_identity::Identity;
use nexus_store::repos::{
    AgentCredentials, Agents, NativeThreadBindings, NewAgent, NewAgentCredential,
    NewNativeThreadBinding, Sessions,
};
use nexus_store::Store;

#[derive(Default)]
struct RecordingSink {
    events: Mutex<Vec<WsEvent>>,
}

#[async_trait]
impl EventSink for RecordingSink {
    async fn emit(&self, event: WsEvent) {
        self.events.lock().unwrap().push(event);
    }
}

async fn fixture() -> (Identity, Arc<Store>, Arc<RecordingSink>) {
    let store = Store::open(":memory:").await.unwrap();
    store.migrate().await.unwrap();
    let store = Arc::new(store);
    let sink = Arc::new(RecordingSink::default());
    let identity = Identity::new(store.clone(), sink.clone(), &Config::default());
    (identity, store, sink)
}

fn register_request(name: &str, client_key: &str) -> RegisterRequest {
    RegisterRequest {
        agent_id: None,
        name: Some(name.into()),
        harness: Harness::Claude,
        harness_session_id: format!("hs_{client_key}"),
        project: "p_demo".into(),
        client_key: client_key.into(),
        runtime_credential: None,
        tier: Tier::Agent,
        kind: None,
        role: None,
        cwd: None,
    }
}

async fn create_agent(store: &Store, agent_id: &str, name: &str) {
    Agents::new(store)
        .create(NewAgent {
            agent_id: agent_id.to_string(),
            project: "p_demo".to_string(),
            name: Some(name.to_string()),
            default_harness: Some("claude".to_string()),
            role: None,
            tier: Some("agent".to_string()),
            owner: None,
        })
        .await
        .unwrap();
}

async fn create_runtime_credential(
    store: &Store,
    agent_id: &str,
    secret: &str,
    credential_id: &str,
) -> String {
    AgentCredentials::new(store)
        .create_hash(NewAgentCredential {
            credential_id: credential_id.to_string(),
            agent_id: agent_id.to_string(),
            secret_hash: hash_runtime_credential(secret),
            purpose: Some("runtime".to_string()),
            label: Some("test".to_string()),
            scopes_json: r#"["runtime:register"]"#.to_string(),
            metadata_json: None,
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn register_rejects_revoked_runtime_credential_from_external_gate() {
    let (identity, store, _sink) = fixture().await;
    create_agent(&store, "a_ben", "ben").await;
    let credential_id = create_runtime_credential(&store, "a_ben", "secret", "cred_ben").await;
    AgentCredentials::new(&store)
        .revoke(&credential_id)
        .await
        .unwrap();

    let mut request = register_request("ben", "ck_revoked");
    request.agent_id = Some(AgentId("a_ben".into()));
    request.runtime_credential = Some("secret".into());

    let error = identity.register(request).await.unwrap_err();
    assert_eq!(error.code, codes::UNAUTHORIZED);
    assert!(
        Sessions::new(&store)
            .find_by_name("p_demo", "ben")
            .await
            .unwrap()
            .is_none(),
        "revoked runtime credentials must not create a session"
    );
}

#[tokio::test]
async fn register_same_client_key_resumes_without_duplicate_spawn_event() {
    let (identity, _store, sink) = fixture().await;
    let first = identity
        .register(register_request("ben", "ck_repeat"))
        .await
        .unwrap();
    let mut second_request = register_request("ben", "ck_repeat");
    second_request.harness_session_id = "hs_rebound".into();
    let second = identity.register(second_request).await.unwrap();

    assert_eq!(first.session_id, second.session_id);
    assert_eq!(first.agent_id, second.agent_id);
    let spawned = sink
        .events
        .lock()
        .unwrap()
        .iter()
        .filter(|event| matches!(event, WsEvent::AgentSpawned { .. }))
        .count();
    assert_eq!(spawned, 1, "idempotent register must not duplicate spawn");
}

#[tokio::test]
async fn register_stamps_agent_id_on_the_session_row() {
    // CLI-registered sessions carried agent_id = NULL, so the
    // fan-out fallback `sessions.agent_id = ?` could never match — members whose runtime row
    // was stopped at the send instant were silently dropped from thread delivery.
    let (identity, store, _sink) = fixture().await;
    let response = identity
        .register(register_request("ben", "ck_stamp"))
        .await
        .unwrap();

    let row = Sessions::new(&store)
        .find_by_session_id(&response.session_id)
        .await
        .unwrap()
        .expect("registered session row");
    assert_eq!(
        row.agent_id.as_deref(),
        response.agent_id.as_ref().map(|id| id.0.as_str()),
        "register must stamp the durable agent id on the session row"
    );
    assert!(row.agent_id.is_some());
}

async fn force_offline(store: &Store, name: &str) {
    store
        .conn
        .execute(
            "UPDATE sessions SET presence = 'offline' WHERE name = ?1 AND project = 'p_demo'",
            libsql::params![name],
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn offline_name_rejects_different_key_without_credential() {
    let (identity, store, _sink) = fixture().await;
    identity
        .register(register_request("ben", "ck_a"))
        .await
        .unwrap();
    force_offline(&store, "ben").await;

    let error = identity
        .register(register_request("ben", "ck_b"))
        .await
        .unwrap_err();
    assert_eq!(error.code, codes::DUPLICATE_NAME);

    // No takeover write happened: the stored row still holds the original client_key.
    let row = Sessions::new(&store)
        .find_by_name("p_demo", "ben")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        row.client_key.as_deref(),
        Some("ck_a"),
        "unauthenticated adoption must not rebind the client_key"
    );
}

#[tokio::test]
async fn offline_name_same_client_key_still_resumes() {
    let (identity, store, _sink) = fixture().await;
    let first = identity
        .register(register_request("ben", "ck_a"))
        .await
        .unwrap();
    force_offline(&store, "ben").await;

    let second = identity
        .register(register_request("ben", "ck_a"))
        .await
        .unwrap();
    assert_eq!(first.session_id, second.session_id);
}

#[tokio::test]
async fn register_rejects_native_resume_key_claimed_by_different_identity() {
    let (identity, store, _sink) = fixture().await;
    let kai = identity
        .register(register_request("kai", "ck_kai"))
        .await
        .unwrap();
    let kai_agent = kai.agent_id.as_ref().expect("agent id").0.clone();
    NativeThreadBindings::new(&store)
        .claim(NewNativeThreadBinding {
            harness: "claude".into(),
            native_thread_id: "claude-native-fork".into(),
            agent_id: kai_agent.clone(),
            project: "p_demo".into(),
            runtime_id: Some(kai.session_id.0.clone()),
        })
        .await
        .unwrap();

    let mut carl = register_request("carl", "ck_carl");
    carl.harness_session_id = "claude-native-fork".into();
    let error = identity
        .register(carl)
        .await
        .expect_err("native key owner must reject a different identity");

    assert_eq!(error.code, codes::INVALID_PARAMS);
    assert!(
        error.message.contains("belongs to kai:")
            && error.message.contains(&kai_agent)
            && error.message.contains("requested carl"),
        "unexpected error: {}",
        error.message
    );
    assert!(
        Sessions::new(&store)
            .find_by_name("p_demo", "carl")
            .await
            .unwrap()
            .is_none(),
        "rejected fork must not create a carl session"
    );
}

#[tokio::test]
async fn register_rejects_native_resume_key_with_mismatched_client_key() {
    let (identity, store, _sink) = fixture().await;
    let kai = identity
        .register(register_request("kai", "ck_kai"))
        .await
        .unwrap();
    let kai_agent = kai.agent_id.as_ref().expect("agent id").0.clone();
    NativeThreadBindings::new(&store)
        .claim(NewNativeThreadBinding {
            harness: "claude".into(),
            native_thread_id: "claude-native-kai".into(),
            agent_id: kai_agent,
            project: "p_demo".into(),
            runtime_id: Some(kai.session_id.0.clone()),
        })
        .await
        .unwrap();

    let mut leaked = register_request("kai", "ck_wrong_kai");
    leaked.harness_session_id = "claude-native-kai".into();
    let error = identity
        .register(leaked)
        .await
        .expect_err("native key owner must reject a mismatched client key");

    assert_eq!(error.code, codes::UNAUTHORIZED);
    let row = Sessions::new(&store)
        .find_by_session_id(&kai.session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.client_key.as_deref(), Some("ck_kai"));
}

#[tokio::test]
async fn offline_name_different_key_with_valid_credential_adopts() {
    let (identity, store, _sink) = fixture().await;
    create_agent(&store, "a_ben", "ben").await;
    create_runtime_credential(&store, "a_ben", "secret", "cred_ben").await;

    let mut first = register_request("ben", "ck_a");
    first.agent_id = Some(AgentId("a_ben".into()));
    first.runtime_credential = Some("secret".into());
    identity.register(first).await.unwrap();
    force_offline(&store, "ben").await;

    let mut second = register_request("ben", "ck_b");
    second.agent_id = Some(AgentId("a_ben".into()));
    second.runtime_credential = Some("secret".into());
    let resumed = identity.register(second).await.unwrap();

    let row = Sessions::new(&store)
        .find_by_name("p_demo", "ben")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        row.client_key.as_deref(),
        Some("ck_b"),
        "authorized adoption rebinds the client_key"
    );
    assert_eq!(resumed.agent_id, Some(AgentId("a_ben".into())));
}

#[tokio::test]
async fn register_same_harness_session_id_resumes_existing_session() {
    let (identity, store, sink) = fixture().await;
    let first = identity
        .register(register_request("ben", "ck_repeat"))
        .await
        .unwrap();
    let first_row = Sessions::new(&store)
        .find_by_name("p_demo", "ben")
        .await
        .unwrap()
        .unwrap();
    let mut second_request = register_request("ben", "ck_repeat");
    second_request.harness_session_id = first_row
        .harness_session_id
        .clone()
        .expect("first register stores a harness session id");

    let second = identity.register(second_request).await.unwrap();

    assert_eq!(first.session_id, second.session_id);
    assert_eq!(first.agent_id, second.agent_id);

    let row = Sessions::new(&store)
        .find_by_name("p_demo", "ben")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.session_id, first.session_id);
    assert_eq!(row.client_key.as_deref(), Some("ck_repeat"));
    assert_eq!(row.harness_session_id, first_row.harness_session_id);

    let spawned = sink
        .events
        .lock()
        .unwrap()
        .iter()
        .filter(|event| matches!(event, WsEvent::AgentSpawned { .. }))
        .count();
    assert_eq!(
        spawned, 1,
        "harness-session resume must not duplicate spawn"
    );
}

fn caller_for(session: &SessionId, name: &str) -> Caller {
    Caller {
        agent_id: None,
        session: session.clone(),
        name: name.into(),
        project: "p_demo".into(),
        tier: Tier::Agent,
    }
}

#[tokio::test]
async fn remy_does_not_become_enzo_when_a_claude_session_id_is_reused_under_a_new_name() {
    // Regression for the "remy became enzo" hijack: a Claude Code session id (harness_session_id)
    // first registered as "enzo", then re-registered under NEXUS_NAME=remy with the SAME id, must
    // NOT silently resume enzo. Step (1) is now name-gated, so remy gets its own identity.
    let (identity, store, _sink) = fixture().await;

    let mut enzo = register_request("enzo", "ck_enzo");
    enzo.harness_session_id = "hs_shared_claude".into();
    let enzo_resp = identity.register(enzo).await.unwrap();

    let mut remy = register_request("remy", "ck_remy");
    remy.harness_session_id = "hs_shared_claude".into(); // the recycled Claude session id
    let remy_resp = identity.register(remy).await.unwrap();

    assert_ne!(
        remy_resp.session_id, enzo_resp.session_id,
        "remy must not be bound to enzo's session"
    );
    let remy_row = Sessions::new(&store)
        .find_by_session_id(&remy_resp.session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(remy_row.name.as_deref(), Some("remy"));

    // enzo's row is left untouched.
    let enzo_row = Sessions::new(&store)
        .find_by_name("p_demo", "enzo")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(enzo_row.session_id, enzo_resp.session_id);
    assert_eq!(enzo_row.name.as_deref(), Some("enzo"));
}

#[tokio::test]
async fn same_name_and_harness_session_rejects_mismatched_client_key() {
    let (identity, store, _sink) = fixture().await;

    let mut first = register_request("remy", "ck_real_remy");
    first.harness_session_id = "hs_remy_runtime".into();
    let first_resp = identity.register(first).await.unwrap();

    let mut leaked = register_request("remy", "ck_wrong_remy");
    leaked.harness_session_id = "hs_remy_runtime".into();
    let error = identity.register(leaked).await.unwrap_err();
    assert_eq!(error.code, codes::UNAUTHORIZED);

    let row = Sessions::new(&store)
        .find_by_session_id(&first_resp.session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.client_key.as_deref(), Some("ck_real_remy"));
}

#[tokio::test]
async fn reconnect_after_sanctioned_rename_resumes_under_the_new_name() {
    // A sanctioned rename updates the row's name, so a later reconnect with the NEW name + the
    // same harness session id still matches the (now name-gated) step (1) and resumes the session.
    let (identity, store, _sink) = fixture().await;

    let mut first = register_request("enzo", "ck_enzo");
    first.harness_session_id = "hs_persist".into();
    let enzo_resp = identity.register(first).await.unwrap();

    identity
        .rename(
            &caller_for(&enzo_resp.session_id, "enzo"),
            RenameRequest {
                name: "remy".into(),
            },
        )
        .await
        .unwrap();

    let mut reconnect = register_request("remy", "ck_enzo");
    reconnect.harness_session_id = "hs_persist".into();
    let remy_resp = identity.register(reconnect).await.unwrap();

    assert_eq!(
        remy_resp.session_id, enzo_resp.session_id,
        "post-rename reconnect must resume the renamed session"
    );
    let row = Sessions::new(&store)
        .find_by_session_id(&remy_resp.session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.name.as_deref(), Some("remy"));
}
