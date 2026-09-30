use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use libsql;
use nexus_common::{hash_runtime_credential, Config};
use nexus_contracts::events::WsEvent;
use nexus_contracts::ids::{AgentId, SessionId};
use nexus_contracts::ports::{Caller, EventSink, IdentityPort};
use nexus_contracts::register::{RegisterRequest, RenameRequest};
use nexus_contracts::{codes, HarnessId, Tier};
use nexus_identity::Identity;
use nexus_store::repos::{
    AgentCredentials, AgentRuntimes, Agents, NativeThreadBindings, NewAgent, NewAgentCredential,
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
        harness: HarnessId::new("claude").unwrap(),
        harness_session_id: format!("hs_{client_key}"),
        project: "p_demo".into(),
        client_key: client_key.into(),
        runtime_credential: None,
        tier: Tier::Agent,
        kind: None,
        locality: Default::default(),
        access: None,
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
async fn register_same_client_key_resumes_across_project_metadata() {
    let (identity, store, _sink) = fixture().await;
    let first = identity
        .register(register_request("ben", "ck_global_resume"))
        .await
        .unwrap();

    let mut reconnect = register_request("ben", "ck_global_resume");
    reconnect.project = "stale-project-metadata".into();
    reconnect.harness_session_id = "hs_rebound_global".into();
    let resumed = identity.register(reconnect).await.unwrap();

    assert_eq!(resumed.session_id, first.session_id);
    let row = Sessions::new(&store)
        .find_by_session_id(&first.session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.project, "p_demo", "project is descriptive metadata");
    assert_eq!(row.harness_session_id.as_deref(), Some("hs_rebound_global"));
}

#[tokio::test]
async fn exact_agent_credential_reconnect_accepts_a_stale_pre_rename_name() {
    let (identity, store, _sink) = fixture().await;
    let first = identity
        .register(register_request("before-rename", "ck_rename_reconnect"))
        .await
        .unwrap();
    let agent_id = first.agent_id.clone().expect("durable agent id");
    create_runtime_credential(&store, &agent_id.0, "rename-secret", "cred_rename").await;
    Agents::new(&store)
        .rename(&agent_id.0, "after-rename")
        .await
        .unwrap();
    Sessions::new(&store)
        .set_name(&first.session_id, "after-rename")
        .await
        .unwrap();

    let mut reconnect = register_request("before-rename", "ck_rename_reconnect");
    reconnect.agent_id = Some(agent_id.clone());
    reconnect.runtime_credential = Some("rename-secret".into());
    reconnect.harness_session_id = "hs_after_rename".into();
    let resumed = identity.register(reconnect).await.unwrap();

    assert_eq!(resumed.session_id, first.session_id);
    assert_eq!(resumed.agent_id, Some(agent_id));
    let row = Sessions::new(&store)
        .find_by_session_id(&first.session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.name.as_deref(), Some("after-rename"));
    assert_eq!(row.harness_session_id.as_deref(), Some("hs_after_rename"));
}

#[tokio::test]
async fn native_exact_agent_reconnect_accepts_a_stale_pre_rename_name() {
    let (identity, store, _sink) = fixture().await;
    let first = identity
        .register(register_request("native-before", "ck_native_rename"))
        .await
        .unwrap();
    let agent_id = first.agent_id.clone().expect("durable agent id");
    create_runtime_credential(
        &store,
        &agent_id.0,
        "native-rename-secret",
        "cred_native_rename",
    )
    .await;
    NativeThreadBindings::new(&store)
        .claim(NewNativeThreadBinding {
            provider: "claude".into(),
            kind: "harness".into(),
            native_thread_id: "native-renamed-thread".into(),
            agent_id: agent_id.0.clone(),
            project: "p_demo".into(),
            runtime_id: Some(first.session_id.0.clone()),
        })
        .await
        .unwrap();
    Agents::new(&store)
        .rename(&agent_id.0, "native-after")
        .await
        .unwrap();
    Sessions::new(&store)
        .set_name(&first.session_id, "native-after")
        .await
        .unwrap();

    let mut reconnect = register_request("native-before", "ck_native_rename");
    reconnect.agent_id = Some(agent_id.clone());
    reconnect.runtime_credential = Some("native-rename-secret".into());
    reconnect.harness_session_id = "native-renamed-thread".into();
    let resumed = identity.register(reconnect).await.unwrap();

    assert_eq!(resumed.session_id, first.session_id);
    assert_eq!(resumed.agent_id, Some(agent_id));
    assert_eq!(
        Sessions::new(&store)
            .find_by_session_id(&first.session_id)
            .await
            .unwrap()
            .unwrap()
            .name
            .as_deref(),
        Some("native-after")
    );
}

#[tokio::test]
async fn exact_agent_resume_ignores_duplicate_legacy_aliases_for_an_idless_session() {
    let (identity, store, _sink) = fixture().await;
    let first = identity
        .register(register_request("shared-alias", "ck_exact_resume"))
        .await
        .unwrap();
    let exact_agent_id = first.agent_id.clone().expect("durable agent id");
    create_runtime_credential(
        &store,
        &exact_agent_id.0,
        "exact-resume-secret",
        "cred_exact_resume",
    )
    .await;

    Sessions::new(&store)
        .set_presence(&first.session_id, nexus_contracts::Presence::Offline)
        .await
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE sessions SET agent_id = NULL WHERE session_id = ?1",
            libsql::params![first.session_id.0.clone()],
        )
        .await
        .unwrap();
    store
        .conn
        .execute("DROP INDEX idx_agents_name_unique", ())
        .await
        .unwrap();
    create_agent(&store, "a_legacy_alias_collision", "shared-alias").await;

    let mut reconnect = register_request("shared-alias", "ck_exact_resume");
    reconnect.agent_id = Some(exact_agent_id.clone());
    reconnect.runtime_credential = Some("exact-resume-secret".into());
    reconnect.harness_session_id = "hs_exact_resume_after_alias_collision".into();
    let resumed = identity.register(reconnect).await.unwrap();

    assert_eq!(resumed.session_id, first.session_id);
    assert_eq!(resumed.agent_id, Some(exact_agent_id.clone()));
    let row = Sessions::new(&store)
        .find_by_session_id(&first.session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.agent_id.as_deref(), Some(exact_agent_id.0.as_str()));
    assert_eq!(
        row.harness_session_id.as_deref(),
        Some("hs_exact_resume_after_alias_collision")
    );
}

#[tokio::test]
async fn fresh_exact_agent_binding_canonicalizes_a_stale_pre_rename_name() {
    let (identity, store, _sink) = fixture().await;
    create_agent(&store, "a_renamed_exact", "canonical-name").await;
    create_runtime_credential(
        &store,
        "a_renamed_exact",
        "fresh-exact-secret",
        "cred_fresh_exact",
    )
    .await;

    let mut request = register_request("stale-pre-rename-name", "ck_fresh_exact");
    request.agent_id = Some(AgentId("a_renamed_exact".into()));
    request.runtime_credential = Some("fresh-exact-secret".into());
    let registered = identity.register(request).await.unwrap();

    assert_eq!(registered.agent_id, Some(AgentId("a_renamed_exact".into())));
    let row = Sessions::new(&store)
        .find_by_session_id(&registered.session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.name.as_deref(), Some("canonical-name"));
    assert!(Sessions::new(&store)
        .find_by_name_any_project("stale-pre-rename-name")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn failed_fresh_exact_agent_binding_leaves_no_registration_residue() {
    let (identity, store, sink) = fixture().await;
    create_agent(&store, "a_atomic_exact", "atomic-exact").await;
    create_runtime_credential(
        &store,
        "a_atomic_exact",
        "atomic-exact-secret",
        "cred_atomic_exact",
    )
    .await;
    store
        .conn
        .execute(
            "CREATE TRIGGER fail_atomic_exact_runtime
             BEFORE INSERT ON agent_runtimes
             BEGIN
               SELECT RAISE(ABORT, 'forced runtime bind failure');
             END",
            (),
        )
        .await
        .unwrap();

    let mut request = register_request("atomic-exact", "ck_atomic_exact");
    request.agent_id = Some(AgentId("a_atomic_exact".into()));
    request.runtime_credential = Some("atomic-exact-secret".into());
    let error = identity.register(request).await.unwrap_err();

    assert_eq!(error.code, codes::INTERNAL_ERROR);
    assert!(Sessions::new(&store)
        .find_by_client_key_any_project("ck_atomic_exact")
        .await
        .unwrap()
        .is_none());
    assert!(AgentRuntimes::new(&store)
        .active_for_agent("a_atomic_exact")
        .await
        .unwrap()
        .is_none());
    let credential = AgentCredentials::new(&store)
        .find_by_id("cred_atomic_exact")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(credential.last_used_at, None);
    let lifecycle_count: i64 = store
        .conn
        .query("SELECT COUNT(*) FROM developer_events", ())
        .await
        .unwrap()
        .next()
        .await
        .unwrap()
        .unwrap()
        .get(0)
        .unwrap();
    assert_eq!(lifecycle_count, 0);
    assert!(sink.events.lock().unwrap().is_empty());
}

#[tokio::test]
async fn name_only_registration_cannot_seize_an_offline_durable_agent() {
    let (identity, store, sink) = fixture().await;
    create_agent(&store, "a_reserved_offline", "reserved-offline").await;

    let error = identity
        .register(register_request("reserved-offline", "ck_name_only_seizure"))
        .await
        .unwrap_err();

    assert_eq!(error.code, codes::DUPLICATE_NAME);
    assert!(Sessions::new(&store)
        .find_by_client_key_any_project("ck_name_only_seizure")
        .await
        .unwrap()
        .is_none());
    assert!(AgentRuntimes::new(&store)
        .active_for_agent("a_reserved_offline")
        .await
        .unwrap()
        .is_none());
    assert!(sink.events.lock().unwrap().is_empty());
}

#[tokio::test]
async fn null_agent_legacy_resume_rejects_a_conflicting_runtime_owner_before_rebind() {
    let (identity, store, sink) = fixture().await;
    let beta = identity
        .register(register_request("beta", "ck_legacy_conflict"))
        .await
        .unwrap();
    let beta_agent = beta.agent_id.as_ref().unwrap().0.clone();
    create_agent(&store, "a_alpha", "alpha").await;
    create_runtime_credential(&store, "a_alpha", "alpha-secret", "cred_alpha").await;
    store
        .conn
        .execute(
            "UPDATE sessions SET agent_id = NULL, name = 'alpha', presence = 'offline' \
             WHERE session_id = ?1",
            libsql::params![beta.session_id.0.clone()],
        )
        .await
        .unwrap();
    let before = Sessions::new(&store)
        .find_by_session_id(&beta.session_id)
        .await
        .unwrap()
        .unwrap();
    let event_count = sink.events.lock().unwrap().len();

    let mut reconnect = register_request("alpha", "ck_legacy_conflict");
    reconnect.agent_id = Some(AgentId("a_alpha".into()));
    reconnect.runtime_credential = Some("alpha-secret".into());
    reconnect.harness_session_id = "must-not-rebind".into();
    let error = identity.register(reconnect).await.unwrap_err();

    assert_eq!(error.code, codes::INVALID_PARAMS);
    assert!(error.message.contains(&beta_agent));
    assert!(error.message.contains("a_alpha"));
    let after = Sessions::new(&store)
        .find_by_session_id(&beta.session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.client_key, before.client_key);
    assert_eq!(after.harness_session_id, before.harness_session_id);
    assert_eq!(after.presence, before.presence);
    assert_eq!(sink.events.lock().unwrap().len(), event_count);
}

#[tokio::test]
async fn native_binding_never_selects_a_last_runtime_owned_by_another_agent() {
    let (identity, store, sink) = fixture().await;
    let alpha = identity
        .register(register_request("alpha", "ck_alpha_native"))
        .await
        .unwrap();
    let beta = identity
        .register(register_request("beta", "ck_beta_native"))
        .await
        .unwrap();
    let alpha_agent = alpha.agent_id.as_ref().unwrap().0.clone();
    let beta_agent = beta.agent_id.as_ref().unwrap().0.clone();
    create_runtime_credential(
        &store,
        &alpha_agent,
        "alpha-native-secret",
        "cred_alpha_native",
    )
    .await;
    NativeThreadBindings::new(&store)
        .claim(NewNativeThreadBinding {
            provider: "claude".into(),
            kind: "harness".into(),
            native_thread_id: "native-cross-owner".into(),
            agent_id: alpha_agent.clone(),
            project: "p_demo".into(),
            runtime_id: Some(beta.session_id.0.clone()),
        })
        .await
        .unwrap();
    let before = Sessions::new(&store)
        .find_by_session_id(&beta.session_id)
        .await
        .unwrap()
        .unwrap();
    let event_count = sink.events.lock().unwrap().len();

    let mut reconnect = register_request("alpha", "ck_beta_native");
    reconnect.agent_id = Some(AgentId(alpha_agent.clone()));
    reconnect.runtime_credential = Some("alpha-native-secret".into());
    reconnect.harness_session_id = "native-cross-owner".into();
    let error = identity.register(reconnect).await.unwrap_err();

    assert_eq!(error.code, codes::INVALID_PARAMS);
    assert!(error.message.contains("native-cross-owner"));
    assert!(error.message.contains(&alpha_agent));
    assert!(error.message.contains(&beta_agent));
    assert_eq!(
        Sessions::new(&store)
            .find_by_session_id(&beta.session_id)
            .await
            .unwrap()
            .unwrap(),
        before
    );
    assert_eq!(sink.events.lock().unwrap().len(), event_count);
}

#[tokio::test]
async fn register_harness_resume_is_global_and_rejects_a_mismatched_client_key_before_writes() {
    let (identity, store, _sink) = fixture().await;
    let mut first = register_request("ben", "ck_global_harness");
    first.project = "first-project".into();
    first.harness_session_id = "hs_global_harness".into();
    let registered = identity.register(first).await.unwrap();

    let mut leaked = register_request("ben", "ck_wrong_harness");
    leaked.project = "second-project".into();
    leaked.harness_session_id = "hs_global_harness".into();
    let error = identity.register(leaked).await.unwrap_err();
    assert_eq!(error.code, codes::UNAUTHORIZED);

    let rows = Sessions::new(&store).list_all().await.unwrap();
    assert_eq!(
        rows.len(),
        1,
        "rejected global resume must not create a row"
    );
    assert_eq!(rows[0].session_id, registered.session_id);
    assert_eq!(rows[0].client_key.as_deref(), Some("ck_global_harness"));
}

#[tokio::test]
async fn register_treats_a_stored_agent_id_as_exclusive() {
    let (identity, store, _sink) = fixture().await;
    let first = identity
        .register(register_request("ben", "ck_stored_agent"))
        .await
        .unwrap();
    Sessions::new(&store)
        .set_agent_id(&first.session_id, "a_missing_stored_owner")
        .await
        .unwrap();
    create_agent(&store, "a_alias_owner", "a_missing_stored_owner").await;

    let mut reconnect = register_request("ben", "ck_stored_agent");
    reconnect.harness_session_id = "hs_must_not_persist".into();
    let error = identity.register(reconnect).await.unwrap_err();
    assert_eq!(error.code, codes::NOT_FOUND);
    let row = Sessions::new(&store)
        .find_by_session_id(&first.session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.agent_id.as_deref(), Some("a_missing_stored_owner"));
    assert_ne!(
        row.harness_session_id.as_deref(),
        Some("hs_must_not_persist")
    );
}

#[tokio::test]
async fn register_rejects_ambiguous_global_legacy_name_before_writes() {
    let (identity, store, _sink) = fixture().await;
    store
        .conn
        .execute("DROP INDEX idx_sessions_name", ())
        .await
        .unwrap();
    for (session_id, project) in [("s_legacy_one", "one"), ("s_legacy_two", "two")] {
        Sessions::new(&store)
            .create(nexus_store::repos::NewSession {
                session_id: SessionId(session_id.into()),
                name: Some("legacy-duplicate".into()),
                agent: Some("claude".into()),
                kind: "agent".into(),
                role: None,
                tier: "agent".into(),
                harness_session_id: None,
                client_key: None,
                cwd: None,
                project: project.into(),
                transport: Some("pty".into()),
            })
            .await
            .unwrap();
    }
    let before = Sessions::new(&store).list_all().await.unwrap().len();
    let mut request = register_request("legacy-duplicate", "ck_ambiguous_legacy");
    request.project = "three".into();

    let error = identity.register(request).await.unwrap_err();
    assert_eq!(error.code, codes::INVALID_PARAMS);
    assert!(error.message.contains("multiple legacy identities"));
    assert_eq!(
        Sessions::new(&store).list_all().await.unwrap().len(),
        before
    );
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
            provider: "claude".into(),
            kind: "harness".into(),
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
            provider: "claude".into(),
            kind: "harness".into(),
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
        locality: Default::default(),
        access: None,
        principal_id: None,
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
