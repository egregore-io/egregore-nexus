use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use nexus_common::{hash_runtime_credential, now, Config, NexusError};
use nexus_contracts::admin::{AdminAssignRequest, AdminRenameRequest};
use nexus_contracts::enums::{Presence, Tier};
use nexus_contracts::events::WsEvent;
use nexus_contracts::ids::{AgentId, SessionId, ThreadId};
use nexus_contracts::ports::{Caller, EventSink, IdentityPort};
use nexus_contracts::register::{RegisterRequest, RenameRequest, StatusRequest, StatusState};
use nexus_contracts::HarnessId;
use nexus_contracts::MemberListRequest;
use nexus_identity::{tier_guard, Identity};
use nexus_store::repos::{
    AgentCredentials, AgentGroups, Agents, DeveloperEvents, NewAgent, NewAgentCredential, Sessions,
    Threads, Topics, AGENT_LIFECYCLE_TOPIC,
};
use nexus_store::{DaemonStore, Store};

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

async fn split_fixture() -> (tempfile::TempDir, Identity, Arc<Store>, Arc<RecordingSink>) {
    let directory = tempfile::tempdir().unwrap();
    let identity_path = directory.path().join("identity.db");
    let daemon = DaemonStore::open(identity_path.to_str().unwrap())
        .await
        .unwrap();
    let store = Arc::new(daemon.compatibility_store());
    let sink = Arc::new(RecordingSink::default());
    let identity = Identity::new(store.clone(), sink.clone(), &Config::default());
    (directory, identity, store, sink)
}

async fn fail_register_resume_lifecycle_events(store: &Store) {
    store
        .conn
        .execute(
            "CREATE TRIGGER fail_register_resume_lifecycle
             BEFORE INSERT ON developer_events
             WHEN NEW.lifecycle IN ('register', 'resume')
             BEGIN
               SELECT RAISE(ABORT, 'forced lifecycle telemetry failure');
             END",
            (),
        )
        .await
        .unwrap();
}

async fn fail_rename_lifecycle_events(store: &Store) {
    store
        .conn
        .execute(
            "CREATE TRIGGER fail_rename_lifecycle
             BEFORE INSERT ON developer_events
             WHEN NEW.lifecycle = 'rename'
             BEGIN
               SELECT RAISE(ABORT, 'forced rename lifecycle telemetry failure');
             END",
            (),
        )
        .await
        .unwrap();
}

fn req(name: &str, client_key: &str) -> RegisterRequest {
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
        role: None,
        cwd: None,
    }
}

fn req_in(name: &str, client_key: &str, project: &str) -> RegisterRequest {
    RegisterRequest {
        project: project.into(),
        ..req(name, client_key)
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

async fn create_staged_agent(store: &Store, agent_id: &str) {
    store
        .conn
        .execute(
            "INSERT INTO agents (agent_id, project, name, default_harness, role, tier, created_at) \
             VALUES (?1, 'p_demo', NULL, 'claude', NULL, 'agent', ?2)",
            libsql::params![agent_id, now()],
        )
        .await
        .unwrap();
}

async fn create_runtime_credential(
    store: &Store,
    agent_id: &str,
    secret: &str,
    scopes_json: &str,
) -> String {
    AgentCredentials::new(store)
        .create_hash(NewAgentCredential {
            credential_id: format!("cred_{agent_id}"),
            agent_id: agent_id.to_string(),
            secret_hash: hash_runtime_credential(secret),
            purpose: Some("runtime".to_string()),
            label: Some("test".to_string()),
            scopes_json: scopes_json.to_string(),
            metadata_json: None,
        })
        .await
        .unwrap()
}

async fn create_staged_agent_session(store: &Store, agent_id: &str, session_id: &str) {
    let ts = now();
    store
        .conn
        .execute(
            "INSERT INTO agents (agent_id, project, name, default_harness, role, tier, created_at) \
             VALUES (?1, 'p_demo', NULL, 'claude', NULL, 'agent', ?2)",
            libsql::params![agent_id, ts],
        )
        .await
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO sessions (
               session_id, name, agent, kind, role, tier, harness_session_id, client_key, cwd,
               project, presence, paused, created_at, transport, agent_id
             ) VALUES (
               ?1, NULL, 'claude', 'agent', NULL, 'agent', ?2, ?3, '/tmp/staged',
               'p_demo', 'online', 0, ?4, 'pty', ?5
             )",
            libsql::params![
                session_id,
                format!("hs_{session_id}"),
                format!("ck_{session_id}"),
                ts,
                agent_id
            ],
        )
        .await
        .unwrap();
}

async fn caller_for(store: &Store, name: &str) -> Caller {
    let row = Sessions::new(store)
        .find_by_name("p_demo", name)
        .await
        .unwrap()
        .unwrap();
    let row_name = row.require_name("test caller").unwrap().to_string();
    Caller {
        agent_id: None,
        session: row.session_id,
        name: row_name,
        project: row.project,
        tier: Tier::Agent,
    }
}

fn caller(tier: Tier) -> Caller {
    Caller {
        agent_id: None,
        session: SessionId("s_a".into()),
        name: "agent".into(),
        project: "p".into(),
        tier,
    }
}

#[tokio::test]
async fn duplicate_live_name_is_rejected() {
    let (identity, _store, _sink) = fixture().await;
    identity.register(req("ben", "ck1")).await.unwrap();

    let error = identity.register(req("ben", "ck2")).await.unwrap_err();

    assert_eq!(error.code, nexus_contracts::codes::DUPLICATE_NAME);
    assert!(
        error.message.contains("ben"),
        "different client_key + live name -> DuplicateName naming the identity"
    );
}

#[tokio::test]
async fn register_binds_name_to_harness_session() {
    let (identity, store, _sink) = fixture().await;
    let response = identity.register(req("ben", "ck1")).await.unwrap();

    let row = Sessions::new(&store)
        .find_by_name("p_demo", "ben")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.session_id, response.session_id);
    assert_eq!(row.harness_session_id.as_deref(), Some("hs_ck1"));

    let caller = caller_for(&store, "ben").await;
    let who = identity.whoami(&caller).await.unwrap();
    assert_eq!(who.name.as_deref(), Some("ben"));
    assert_eq!(who.session_id, response.session_id);
    assert_eq!(who.agent_id, response.agent_id);
    assert_eq!(who.project, "p_demo");
}

#[tokio::test]
async fn register_and_resume_emit_lifecycle_events_after_runtime_bind() {
    let (identity, store, sink) = fixture().await;
    let registered = identity.register(req("ben", "ck1")).await.unwrap();
    let stable_agent_id = registered
        .agent_id
        .as_ref()
        .expect("register binds a durable agent")
        .0
        .clone();
    assert!(
        sink.events.lock().unwrap().iter().any(|event| matches!(
            event,
            WsEvent::AgentSpawned {
                session_id,
                name: Some(name),
                agent_id: Some(agent_id),
            } if *session_id == registered.session_id
                && name == "ben"
                && agent_id == &stable_agent_id
        )),
        "register must publish AgentSpawned only after the stable agent is bound"
    );
    let status_before_noop_resume = sink
        .events
        .lock()
        .unwrap()
        .iter()
        .filter(|event| matches!(event, WsEvent::AgentStatus { .. }))
        .count();
    identity.register(req("ben", "ck1")).await.unwrap();
    assert_eq!(
        sink.events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| matches!(event, WsEvent::AgentStatus { .. }))
            .count(),
        status_before_noop_resume,
        "already-online resume must not duplicate the fleet status fact"
    );

    let rows = DeveloperEvents::new(&store)
        .since(AGENT_LIFECYCLE_TOPIC, 0)
        .await
        .unwrap();
    assert_eq!(
        rows.iter()
            .filter_map(|row| row.lifecycle.as_deref())
            .collect::<Vec<_>>(),
        vec!["started", "register", "started", "resume"]
    );
    assert!(rows
        .iter()
        .all(|row| row.agent_name.as_deref() == Some("ben")));
    assert!(rows.iter().all(|row| row.kind == "agent_lifecycle"));
}

#[tokio::test]
async fn register_succeeds_when_register_lifecycle_telemetry_fails() {
    let (identity, store, _sink) = fixture().await;
    fail_register_resume_lifecycle_events(&store).await;

    let response = identity.register(req("ben", "ck1")).await.unwrap();

    let row = Sessions::new(&store)
        .find_by_session_id(&response.session_id)
        .await
        .unwrap()
        .expect("register keeps session addressable");
    assert_eq!(row.name.as_deref(), Some("ben"));
}

#[tokio::test]
async fn resume_succeeds_when_resume_lifecycle_telemetry_fails() {
    let (identity, store, _sink) = fixture().await;
    let first = identity.register(req("ben", "ck1")).await.unwrap();
    fail_register_resume_lifecycle_events(&store).await;

    let resumed = identity.register(req("ben", "ck1")).await.unwrap();

    assert_eq!(resumed.session_id, first.session_id);
    let row = Sessions::new(&store)
        .find_by_session_id(&resumed.session_id)
        .await
        .unwrap()
        .expect("resume keeps session addressable");
    assert_eq!(row.presence.as_deref(), Some("online"));
}

#[tokio::test]
async fn resolve_and_members_expose_stable_agent_id() {
    let (identity, store, _sink) = fixture().await;
    let response = identity.register(req("ben", "ck1")).await.unwrap();
    let resolved = identity.resolve("p_demo", "ben").await.unwrap();
    assert_eq!(resolved.agent_id, response.agent_id);
    assert_eq!(resolved.session, response.session_id);

    let caller = caller_for(&store, "ben").await;
    let members = identity
        .members(
            &caller,
            MemberListRequest {
                include_offline: Some(true),
                include_dead: None,
            },
        )
        .await
        .unwrap();
    let ben = members
        .members
        .iter()
        .find(|member| member.name.as_deref() == Some("ben"))
        .expect("ben appears in members");
    assert_eq!(ben.agent_id, response.agent_id);
    assert_eq!(ben.session_id, response.session_id);
}

#[tokio::test]
async fn rename_rekeys_legacy_membership_rows() {
    let (identity, store, _sink) = fixture().await;
    let registered = identity.register(req("olive", "ck_olive")).await.unwrap();
    let caller = caller_for(&store, "olive").await;
    let agent_id = registered.agent_id.expect("register binds durable agent");

    let threads = Threads::new(&store);
    let thread_id = ThreadId("t_egregore_back".into());
    threads
        .create(&thread_id, "egregore-back", "p_demo", "alex")
        .await
        .unwrap();
    threads.add_member(&thread_id, "olive").await.unwrap();
    Topics::new(&store)
        .subscribe("builds", "olive", Some("watchers"))
        .await
        .unwrap();
    AgentGroups::new(&store)
        .assign("p_demo", "ops", &agent_id, "olive")
        .await
        .unwrap();

    let renamed = identity
        .rename(
            &caller,
            RenameRequest {
                name: "oscar".into(),
            },
        )
        .await
        .unwrap();

    assert_eq!(renamed.previous.as_deref(), Some("olive"));
    assert_eq!(renamed.name, "oscar");
    assert_eq!(threads.members(&thread_id).await.unwrap(), vec!["oscar"]);
    assert!(
        threads.is_member(&thread_id, "oscar").await.unwrap(),
        "renamed agent must still be a thread member under the new name"
    );

    let thread_member_names = text_column(
        &store,
        "SELECT session_name FROM thread_members WHERE thread_id = 't_egregore_back' ORDER BY \
         session_name",
    )
    .await;
    assert_eq!(thread_member_names, vec!["oscar"]);

    let subscriber_names = text_column(
        &store,
        "SELECT subscriber_session FROM subscriptions WHERE topic = 'builds' ORDER BY \
         subscriber_session",
    )
    .await;
    assert_eq!(subscriber_names, vec!["oscar"]);

    let group_names = text_column(
        &store,
        "SELECT agent_name FROM agent_group_members WHERE project = 'p_demo' AND group_name = \
         'ops' ORDER BY agent_name",
    )
    .await;
    assert_eq!(group_names, vec!["oscar"]);
}

#[tokio::test]
async fn rename_updates_identity_and_transport_authorities_in_split_mode() {
    let (_directory, identity, store, _sink) = split_fixture().await;
    let registered = identity
        .register(req("olive", "ck_split_olive"))
        .await
        .unwrap();
    let caller = caller_for(&store, "olive").await;

    identity
        .rename(
            &caller,
            RenameRequest {
                name: "oscar".into(),
            },
        )
        .await
        .unwrap();

    assert_eq!(
        Agents::new(&store)
            .find_by_id(&registered.agent_id.unwrap().0)
            .await
            .unwrap()
            .unwrap()
            .name
            .as_deref(),
        Some("oscar")
    );
    assert!(Sessions::new(&store)
        .find_by_name("p_demo", "oscar")
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn rename_emits_lifecycle_event_with_old_and_new_names() {
    let (identity, store, _sink) = fixture().await;
    identity.register(req("olive", "ck_olive")).await.unwrap();
    let caller = caller_for(&store, "olive").await;

    identity
        .rename(
            &caller,
            RenameRequest {
                name: "oscar".into(),
            },
        )
        .await
        .unwrap();

    let rows = DeveloperEvents::new(&store)
        .since(AGENT_LIFECYCLE_TOPIC, 0)
        .await
        .unwrap();
    let rename = rows
        .iter()
        .find(|row| row.lifecycle.as_deref() == Some("rename"))
        .expect("rename lifecycle event");
    assert_eq!(rename.agent_name.as_deref(), Some("oscar"));
    assert_eq!(
        rename.session_id.as_deref(),
        Some(caller.session.0.as_str())
    );
    assert_eq!(
        rename
            .data_json
            .as_deref()
            .map(|data| serde_json::from_str::<serde_json::Value>(data).unwrap()),
        Some(serde_json::json!({"newName":"oscar","oldName":"olive"}))
    );
}

#[tokio::test]
async fn rename_noop_does_not_emit_lifecycle_event() {
    let (identity, store, _sink) = fixture().await;
    identity.register(req("olive", "ck_olive")).await.unwrap();
    let caller = caller_for(&store, "olive").await;

    identity
        .rename(
            &caller,
            RenameRequest {
                name: "olive".into(),
            },
        )
        .await
        .unwrap();

    let rows = DeveloperEvents::new(&store)
        .since(AGENT_LIFECYCLE_TOPIC, 0)
        .await
        .unwrap();
    assert!(
        rows.iter()
            .all(|row| row.lifecycle.as_deref() != Some("rename")),
        "no-op rename must not publish a lifecycle event"
    );
}

#[tokio::test]
async fn rename_succeeds_when_lifecycle_telemetry_fails() {
    let (identity, store, _sink) = fixture().await;
    identity.register(req("olive", "ck_olive")).await.unwrap();
    let caller = caller_for(&store, "olive").await;
    fail_rename_lifecycle_events(&store).await;

    let renamed = identity
        .rename(
            &caller,
            RenameRequest {
                name: "oscar".into(),
            },
        )
        .await
        .unwrap();

    assert_eq!(renamed.name, "oscar");
    assert!(Sessions::new(&store)
        .find_by_name("p_demo", "oscar")
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn admin_rename_renames_another_named_agent() {
    let (identity, store, sink) = fixture().await;
    let registered = identity.register(req("olive", "ck_olive")).await.unwrap();
    let agent_id = registered.agent_id.expect("register binds durable agent");

    let renamed = identity
        .admin_rename(
            &caller(Tier::Admin),
            AdminRenameRequest {
                source: "olive".into(),
                target: "oscar".into(),
            },
        )
        .await
        .unwrap();

    assert_eq!(renamed.previous.as_deref(), Some("olive"));
    assert_eq!(renamed.name, "oscar");
    assert_eq!(renamed.agent_id, agent_id);
    assert_eq!(renamed.session_id, Some(registered.session_id.clone()));
    assert!(Sessions::new(&store)
        .find_by_name("p_demo", "olive")
        .await
        .unwrap()
        .is_none());
    let row = Sessions::new(&store)
        .find_by_name("p_demo", "oscar")
        .await
        .unwrap()
        .expect("renamed session remains addressable");
    assert_eq!(row.session_id, registered.session_id);
    assert_eq!(
        Agents::new(&store)
            .find_by_id(&agent_id.0)
            .await
            .unwrap()
            .expect("renamed agent remains addressable")
            .name
            .as_deref(),
        Some("oscar")
    );
    let events = sink.events.lock().unwrap();
    assert!(events.iter().any(|event| {
        matches!(
            event,
            WsEvent::AgentSpawned {
                session_id,
                name: Some(name),
                agent_id: Some(event_agent_id),
            } if *session_id == registered.session_id
                && name == "oscar"
                && event_agent_id == &agent_id.0
        )
    }));
}

#[tokio::test]
async fn admin_rename_first_names_staged_session_by_session_id() {
    let (identity, store, _sink) = fixture().await;
    create_staged_agent_session(&store, "a_staged", "s_staged").await;

    let renamed = identity
        .admin_rename(
            &caller(Tier::Admin),
            AdminRenameRequest {
                source: "s_staged".into(),
                target: "nora".into(),
            },
        )
        .await
        .unwrap();

    assert_eq!(renamed.previous, None);
    assert_eq!(renamed.agent_id, AgentId("a_staged".into()));
    assert_eq!(renamed.session_id, Some(SessionId("s_staged".into())));
    assert_eq!(renamed.name, "nora");
    assert_eq!(
        Sessions::new(&store)
            .find_by_session_id(&SessionId("s_staged".into()))
            .await
            .unwrap()
            .expect("staged session still exists")
            .name
            .as_deref(),
        Some("nora")
    );
    assert_eq!(
        Agents::new(&store)
            .find_by_id("a_staged")
            .await
            .unwrap()
            .expect("staged agent still exists")
            .name
            .as_deref(),
        Some("nora")
    );

    let rows = DeveloperEvents::new(&store)
        .since(AGENT_LIFECYCLE_TOPIC, 0)
        .await
        .unwrap();
    let rename = rows
        .iter()
        .find(|row| row.lifecycle.as_deref() == Some("rename"))
        .expect("admin first-name publishes lifecycle event");
    assert_eq!(rename.agent_name.as_deref(), Some("nora"));
    assert_eq!(rename.session_id.as_deref(), Some("s_staged"));
    assert_eq!(
        rename
            .data_json
            .as_deref()
            .map(|data| serde_json::from_str::<serde_json::Value>(data).unwrap()),
        Some(serde_json::json!({
            "newName": "nora",
            "oldName": null,
            "previous": null,
        }))
    );
}

#[tokio::test]
async fn admin_rename_first_names_staged_agent_without_session() {
    let (identity, store, sink) = fixture().await;
    create_staged_agent(&store, "a_agent_only").await;

    let renamed = identity
        .admin_rename(
            &caller(Tier::Admin),
            AdminRenameRequest {
                source: "a_agent_only".into(),
                target: "zelda".into(),
            },
        )
        .await
        .unwrap();

    assert_eq!(renamed.previous, None);
    assert_eq!(renamed.agent_id, AgentId("a_agent_only".into()));
    assert_eq!(renamed.session_id, None);
    assert_eq!(renamed.name, "zelda");
    assert_eq!(
        Agents::new(&store)
            .find_by_id("a_agent_only")
            .await
            .unwrap()
            .expect("staged agent still exists")
            .name
            .as_deref(),
        Some("zelda")
    );

    let events = sink.events.lock().unwrap();
    assert!(
        events.is_empty(),
        "agent-only rename cannot emit session-scoped AgentSpawned"
    );
    let rows = DeveloperEvents::new(&store)
        .since(AGENT_LIFECYCLE_TOPIC, 0)
        .await
        .unwrap();
    let rename = rows
        .iter()
        .find(|row| row.lifecycle.as_deref() == Some("rename"))
        .expect("agent-only first-name publishes lifecycle event");
    assert_eq!(rename.agent_name.as_deref(), Some("zelda"));
    assert_eq!(rename.session_id, None);
}

#[tokio::test]
async fn admin_rename_rejects_collision_by_agent_id_source() {
    let (identity, _store, _sink) = fixture().await;
    let olive = identity.register(req("olive", "ck_olive")).await.unwrap();
    identity.register(req("oscar", "ck_oscar")).await.unwrap();
    let agent_id = olive.agent_id.expect("register binds durable agent");

    let error = identity
        .admin_rename(
            &caller(Tier::Admin),
            AdminRenameRequest {
                source: agent_id.0,
                target: "oscar".into(),
            },
        )
        .await
        .unwrap_err();

    assert_eq!(error.code, nexus_contracts::codes::DUPLICATE_NAME);
    assert!(error.message.contains("oscar"));
}

#[tokio::test]
async fn register_with_agent_id_requires_runtime_credential() {
    let (identity, store, _sink) = fixture().await;
    create_agent(&store, "a_ben", "ben").await;
    let mut request = req("ben", "ck1");
    request.agent_id = Some(AgentId("a_ben".into()));

    let error = identity.register(request).await.unwrap_err();

    assert_eq!(error.code, nexus_contracts::codes::UNAUTHORIZED);
    assert!(Sessions::new(&store)
        .find_by_name("p_demo", "ben")
        .await
        .unwrap()
        .is_none());
}

async fn text_column(store: &Store, sql: &str) -> Vec<String> {
    let mut rows = store.conn.query(sql, ()).await.unwrap();
    let mut out = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        out.push(row.get::<String>(0).unwrap());
    }
    out
}

#[tokio::test]
async fn register_with_agent_id_verifies_runtime_credential() {
    let (identity, store, _sink) = fixture().await;
    create_agent(&store, "a_ben", "ben").await;
    let credential_id =
        create_runtime_credential(&store, "a_ben", "secret", r#"["runtime:register"]"#).await;
    let mut request = req("ben", "ck1");
    request.agent_id = Some(AgentId("a_ben".into()));
    request.runtime_credential = Some("secret".into());

    let response = identity.register(request).await.unwrap();

    assert_eq!(response.agent_id, Some(AgentId("a_ben".into())));
    let active = AgentCredentials::new(&store)
        .find_active("a_ben")
        .await
        .unwrap();
    let credential = active
        .iter()
        .find(|credential| credential.credential_id == credential_id)
        .expect("credential remains active");
    assert!(
        credential.last_used_at.is_some(),
        "verified credential should be touched"
    );
}

#[tokio::test]
async fn register_rejects_runtime_credential_without_runtime_scope() {
    let (identity, store, _sink) = fixture().await;
    create_agent(&store, "a_ben", "ben").await;
    create_runtime_credential(&store, "a_ben", "secret", r#"["agent:read"]"#).await;
    let mut request = req("ben", "ck1");
    request.agent_id = Some(AgentId("a_ben".into()));
    request.runtime_credential = Some("secret".into());

    assert_eq!(
        identity.register(request).await.unwrap_err().code,
        nexus_contracts::codes::UNAUTHORIZED
    );
}

#[tokio::test]
async fn register_rejects_runtime_bound_to_another_agent() {
    let (identity, store, _sink) = fixture().await;
    let response = identity.register(req("ben", "ck1")).await.unwrap();
    store
        .conn
        .execute(
            "UPDATE agent_runtimes SET agent_id = 'a_other' WHERE runtime_id = ?1",
            libsql::params![response.session_id.0],
        )
        .await
        .unwrap();

    let error = identity.register(req("ben", "ck1")).await.unwrap_err();

    assert_eq!(error.code, nexus_contracts::codes::INVALID_PARAMS);
    assert!(
        error.message.contains("bound to a_other"),
        "error should explain the runtime ownership mismatch"
    );
}

#[tokio::test]
async fn self_pause_sets_paused_flag_audited_by_source() {
    let (identity, store, sink) = fixture().await;
    identity.register(req("ben", "ck1")).await.unwrap();
    let caller = caller_for(&store, "ben").await;

    let response = identity
        .status(
            &caller,
            StatusRequest {
                state: Some(StatusState::Paused),
                work: Some("compacting".into()),
            },
        )
        .await
        .unwrap();
    assert!(response.paused, "status Paused sets the paused flag");

    let row = Sessions::new(&store)
        .find_by_name("p_demo", "ben")
        .await
        .unwrap()
        .unwrap();
    assert!(row.paused);
    assert_eq!(row.paused_by.as_deref(), Some("self"));
    assert_eq!(response.presence, Presence::Online);
    assert_eq!(row.current_work.as_deref(), Some("compacting"));
    assert_eq!(
        sink.events.lock().unwrap().last(),
        Some(&WsEvent::AgentStatus {
            session_id: row.session_id,
            presence: Presence::Online,
            paused: true,
        })
    );
}

#[tokio::test]
async fn work_only_status_and_heartbeat_restore_emit_authoritative_status_facts() {
    let (identity, store, sink) = fixture().await;
    identity.register(req("ben", "ck1")).await.unwrap();
    let caller = caller_for(&store, "ben").await;

    let before_work = sink.events.lock().unwrap().len();
    identity
        .status(
            &caller,
            StatusRequest {
                state: None,
                work: Some("shipping presence".into()),
            },
        )
        .await
        .unwrap();
    let events = sink.events.lock().unwrap();
    assert_eq!(events.len(), before_work + 1);
    assert!(matches!(
        events.last(),
        Some(WsEvent::AgentStatus {
            session_id,
            presence: Presence::Online,
            paused: false,
        }) if session_id == &caller.session
    ));
    drop(events);

    let before_noop = sink.events.lock().unwrap().len();
    identity
        .status(
            &caller,
            StatusRequest {
                state: None,
                work: Some("shipping presence".into()),
            },
        )
        .await
        .unwrap();
    assert_eq!(sink.events.lock().unwrap().len(), before_noop);

    Sessions::new(&store)
        .set_presence(&caller.session, Presence::Offline)
        .await
        .unwrap();
    let before_heartbeat = sink.events.lock().unwrap().len();
    identity.heartbeat(&caller).await.unwrap();
    let events = sink.events.lock().unwrap();
    assert_eq!(events.len(), before_heartbeat + 1);
    assert!(matches!(
        events.last(),
        Some(WsEvent::AgentStatus {
            session_id,
            presence: Presence::Online,
            paused: false,
        }) if session_id == &caller.session
    ));
}

#[tokio::test]
async fn assign_project_moves_session_across_projects() {
    let (identity, store, sink) = fixture().await;
    identity
        .register(req_in("ben", "ck1", "nexus"))
        .await
        .unwrap();
    let events_before = sink.events.lock().unwrap().len();

    let response = identity.assign_project("ben", "lens").await.unwrap();

    assert_eq!(response.name.as_deref(), Some("ben"));
    assert_eq!(response.project, "lens");
    let repo = Sessions::new(&store);
    assert!(
        repo.find_by_name("lens", "ben").await.unwrap().is_some(),
        "ben must be in lens after move"
    );
    assert!(
        repo.find_by_name("nexus", "ben").await.unwrap().is_none(),
        "ben must not be in nexus after move"
    );

    let events = sink.events.lock().unwrap();
    let new_resync = events[events_before..]
        .iter()
        .filter(
            |event| matches!(event, WsEvent::AgentSpawned { name, .. } if name.as_deref() == Some("ben")),
        )
        .count();
    assert_eq!(new_resync, 1, "one AgentSpawned resync emitted on assign");
}

#[tokio::test]
async fn assign_project_to_current_project_is_noop() {
    let (identity, store, _sink) = fixture().await;
    identity
        .register(req_in("ben", "ck1", "nexus"))
        .await
        .unwrap();

    let response = identity.assign_project("ben", "nexus").await.unwrap();

    assert_eq!(response.name.as_deref(), Some("ben"));
    assert_eq!(response.project, "nexus");
    let repo = Sessions::new(&store);
    assert!(
        repo.find_by_name("nexus", "ben").await.unwrap().is_some(),
        "nexus ben must still exist after no-op move"
    );
}

#[test]
fn tier_guard_allows_agent_scope_and_admin_scope() {
    assert!(tier_guard(&caller(Tier::Agent), Tier::Agent).is_ok());
    assert!(tier_guard(&caller(Tier::Admin), Tier::Agent).is_ok());
    assert!(tier_guard(&caller(Tier::Admin), Tier::Admin).is_ok());
}

#[test]
fn tier_guard_blocks_agent_from_admin() {
    let agent = caller(Tier::Agent);
    let admin = Caller {
        tier: Tier::Admin,
        ..agent.clone()
    };

    assert!(matches!(
        tier_guard(&agent, Tier::Admin),
        Err(NexusError::Unauthorized)
    ));
    assert!(tier_guard(&admin, Tier::Admin).is_ok());
}

#[tokio::test]
async fn admin_assign_staged_assumes_dead_owners_name() {
    let (identity, store, sink) = fixture().await;
    let registered = identity.register(req("olive", "ck_olive")).await.unwrap();
    let dead_agent_id = registered.agent_id.expect("register binds durable agent");
    Agents::new(&store)
        .mark_dead(&dead_agent_id.0, "revive_exhausted")
        .await
        .unwrap();
    create_staged_agent_session(&store, "a_assignee", "s_assignee").await;

    let assigned = identity
        .admin_assign(
            &caller(Tier::Admin),
            AdminAssignRequest {
                id: "a_assignee".into(),
                name: "olive".into(),
            },
        )
        .await
        .unwrap();

    assert_eq!(assigned.name, "olive");
    assert_eq!(assigned.agent_id.0, "a_assignee");
    assert_eq!(
        assigned.evicted_agent_id.as_ref().map(|id| id.0.as_str()),
        Some(dead_agent_id.0.as_str())
    );
    // Assignee owns the name; the dead holder returned to the staged (unnamed) surface.
    assert_eq!(
        Agents::new(&store)
            .find_by_name("olive")
            .await
            .unwrap()
            .expect("name stays bound")
            .agent_id,
        "a_assignee"
    );
    assert!(Agents::new(&store)
        .find_by_id(&dead_agent_id.0)
        .await
        .unwrap()
        .expect("evicted agent row survives")
        .name
        .is_none());
    let events = sink.events.lock().unwrap();
    assert!(events.iter().any(|event| matches!(
        event,
        WsEvent::AgentSpawned { name: Some(name), agent_id: Some(agent_id), .. }
            if name == "olive" && agent_id == "a_assignee"
    )));
}

#[tokio::test]
async fn admin_assign_rejects_live_holder() {
    let (identity, store, _sink) = fixture().await;
    identity.register(req("olive", "ck_olive")).await.unwrap();
    create_staged_agent_session(&store, "a_assignee", "s_assignee").await;

    let err = identity
        .admin_assign(
            &caller(Tier::Admin),
            AdminAssignRequest {
                id: "a_assignee".into(),
                name: "olive".into(),
            },
        )
        .await
        .expect_err("live holder must not be takeable");
    assert!(err.message.contains("live"), "unexpected error: {err:?}");
}

#[tokio::test]
async fn admin_assign_rejects_named_assignee() {
    let (identity, _store, _sink) = fixture().await;
    let registered = identity.register(req("pearl", "ck_pearl")).await.unwrap();
    let agent_id = registered.agent_id.expect("register binds durable agent");

    let err = identity
        .admin_assign(
            &caller(Tier::Admin),
            AdminAssignRequest {
                id: agent_id.0.clone(),
                name: "quill".into(),
            },
        )
        .await
        .expect_err("named assignee must be rejected");
    assert!(
        err.message.contains("already named"),
        "unexpected error: {err:?}"
    );
}

#[tokio::test]
async fn admin_assign_first_names_when_free() {
    let (identity, store, _sink) = fixture().await;
    create_staged_agent_session(&store, "a_assignee", "s_assignee").await;

    let assigned = identity
        .admin_assign(
            &caller(Tier::Admin),
            AdminAssignRequest {
                id: "s_assignee".into(),
                name: "fresh".into(),
            },
        )
        .await
        .unwrap();

    assert_eq!(assigned.name, "fresh");
    assert!(assigned.evicted_agent_id.is_none());
    assert_eq!(
        Sessions::new(&store)
            .find_by_name("p_demo", "fresh")
            .await
            .unwrap()
            .expect("assigned session addressable")
            .session_id
            .0,
        "s_assignee"
    );
}

#[tokio::test]
async fn admin_assign_takes_fully_stopped_unmarked_holder() {
    let (identity, store, _sink) = fixture().await;
    let registered = identity.register(req("olive", "ck_olive")).await.unwrap();
    let holder_agent_id = registered.agent_id.expect("register binds durable agent");
    // Not dead-marked, but fully stopped: offline session, no active runtime.
    Sessions::new(&store)
        .set_presence(&registered.session_id, Presence::Offline)
        .await
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE agent_runtimes SET active = 0 WHERE agent_id = ?1",
            libsql::params![holder_agent_id.0.clone()],
        )
        .await
        .unwrap();
    create_staged_agent_session(&store, "a_assignee", "s_assignee").await;

    let assigned = identity
        .admin_assign(
            &caller(Tier::Admin),
            AdminAssignRequest {
                id: "a_assignee".into(),
                name: "olive".into(),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        assigned.evicted_agent_id.as_ref().map(|id| id.0.as_str()),
        Some(holder_agent_id.0.as_str())
    );
}

#[tokio::test]
async fn staged_caller_claims_its_first_name_when_free() {
    let (identity, store, sink) = fixture().await;
    create_staged_agent_session(&store, "a_selfname", "s_selfname").await;

    let renamed = identity
        .rename(
            &Caller {
                agent_id: Some(AgentId("a_selfname".into())),
                session: SessionId("s_selfname".into()),
                name: String::new(),
                project: "p_demo".into(),
                tier: Tier::Agent,
            },
            RenameRequest {
                name: "selby".into(),
            },
        )
        .await
        .unwrap();

    assert_eq!(renamed.name, "selby");
    assert_eq!(renamed.previous, None, "staged caller had no previous name");
    assert_eq!(
        Agents::new(&store)
            .find_by_id("a_selfname")
            .await
            .unwrap()
            .expect("agent row")
            .name
            .as_deref(),
        Some("selby")
    );
    let events = sink.events.lock().unwrap();
    assert!(events.iter().any(|event| matches!(
        event,
        WsEvent::AgentSpawned { name: Some(name), .. } if name == "selby"
    )));
}

#[tokio::test]
async fn staged_caller_cannot_claim_a_taken_name() {
    let (identity, store, _sink) = fixture().await;
    identity.register(req("olive", "ck_olive")).await.unwrap();
    create_staged_agent_session(&store, "a_selfname", "s_selfname").await;

    let err = identity
        .rename(
            &Caller {
                agent_id: Some(AgentId("a_selfname".into())),
                session: SessionId("s_selfname".into()),
                name: String::new(),
                project: "p_demo".into(),
                tier: Tier::Agent,
            },
            RenameRequest {
                name: "olive".into(),
            },
        )
        .await
        .expect_err("taken name must be rejected");
    assert!(
        err.message.contains("already bound"),
        "unexpected error: {err:?}"
    );
}
