use std::sync::Arc;
use std::time::{Duration, Instant};

use nexus::cli::store_client::StoreClient;
use nexus::daemon::{command_worker, AppState};
use nexus_common::{hash_runtime_credential, Config};
use nexus_contracts::{
    codes, AgentCredentialRevokeRequest, AgentId, Caller, ContractError, CreateThreadRequest, Kind,
    Presence, RegisterRequest, RenameRequest, Request, SendRequest, SendTarget, Tier, WsEvent,
    JSONRPC_VERSION,
};
use nexus_store::command_kinds;
use nexus_store::repos::{
    AgentCredentials, AgentRuntimes, Agents, CommandIntents, DeveloperEvents, NewAgent,
    NewAgentCredential, NewCommandIntent, NewSession, Sessions, Threads, AGENT_LIFECYCLE_TOPIC,
};
use nexus_store::Store;

async fn test_state() -> AppState {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    AppState::wire(store, &Config::default())
}

fn human_register(name: &str, client_key: &str) -> RegisterRequest {
    register_request(
        name,
        client_key,
        "default",
        hid("other"),
        Kind::Human,
        Tier::Admin,
    )
}

fn register_request(
    name: &str,
    client_key: &str,
    project: &str,
    harness: nexus_contracts::HarnessId,
    kind: Kind,
    tier: Tier,
) -> RegisterRequest {
    RegisterRequest {
        agent_id: None,
        name: Some(name.into()),
        harness,
        harness_session_id: format!("hs_{client_key}"),
        project: project.into(),
        client_key: client_key.into(),
        runtime_credential: None,
        tier,
        kind: Some(kind),
        locality: Default::default(),
        access: None,
        role: None,
        cwd: None,
    }
}

fn harness_token(harness: nexus_contracts::HarnessId) -> &'static str {
    match harness.as_str() {
        "claude" => "claude",
        "codex" => "codex",
        "opencode" => "opencode",
        "hermes" => "hermes",
        "pi" => "pi",
        "other" => "other",
        other => panic!("unexpected harness id in test: {other}"),
    }
}

fn create_thread_intent(
    command_id: &str,
    caller_name: &str,
    caller_session_id: Option<String>,
    caller_client_key: Option<String>,
    thread_name: &str,
) -> NewCommandIntent {
    NewCommandIntent {
        command_id: command_id.into(),
        kind: command_kinds::thread::CREATE.into(),
        project: "default".into(),
        caller_name: caller_name.into(),
        caller_session_id,
        caller_agent_id: None,
        caller_runtime_id: None,
        caller_client_key,
        caller_kind: Some("human".into()),
        caller_tier: Some("admin".into()),
        idempotency_key: None,
        request_json: serde_json::to_string(&CreateThreadRequest {
            name: thread_name.into(),
            members: vec![],
        })
        .unwrap(),
        created_at: 1,
    }
}

async fn assert_rejected(state: &AppState, command_id: &str, thread_name: &str) -> ContractError {
    assert!(command_worker::process_next(state).await.unwrap());
    let command = CommandIntents::new(&state.store)
        .get(command_id)
        .await
        .unwrap()
        .expect("command row exists");
    assert_eq!(command.status, "error");
    let error: ContractError =
        serde_json::from_str(command.error_json.as_deref().unwrap()).unwrap();
    assert_eq!(error.code, codes::UNAUTHORIZED);
    assert!(
        Threads::new(&state.store)
            .find_any_by_name(thread_name)
            .await
            .unwrap()
            .is_none(),
        "unauthenticated command must not execute"
    );
    error
}

async fn assert_executed(state: &AppState, command_id: &str, thread_name: &str) {
    assert!(command_worker::process_next(state).await.unwrap());
    let command = CommandIntents::new(&state.store)
        .get(command_id)
        .await
        .unwrap()
        .expect("command row exists");
    assert_eq!(
        command.status, "done",
        "command error was {:?}",
        command.error_json
    );
    assert!(
        Threads::new(&state.store)
            .find_any_by_name(thread_name)
            .await
            .unwrap()
            .is_some(),
        "authenticated command must execute"
    );
}

async fn create_agent_with_runtime_credential(
    state: &AppState,
    agent_id: &str,
    name: &str,
    secret: &str,
) -> String {
    Agents::new(&state.store)
        .create(NewAgent {
            agent_id: agent_id.to_string(),
            project: "default".to_string(),
            name: Some(name.to_string()),
            default_harness: Some("claude".to_string()),
            role: None,
            tier: Some("agent".to_string()),
            owner: None,
        })
        .await
        .unwrap();
    AgentCredentials::new(&state.store)
        .create_hash(NewAgentCredential {
            credential_id: format!("cred_{agent_id}"),
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

async fn process_next_command(state: &AppState) {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if command_worker::process_next(state).await.unwrap() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for pending command"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn attach_request(session: &nexus_contracts::SessionId) -> Request {
    Request {
        jsonrpc: JSONRPC_VERSION.to_string(),
        id: None,
        method: "identity.attach".into(),
        params: Some(serde_json::json!({ "sessionId": session.0 })),
    }
}

fn caller_for_session(
    name: &str,
    session: nexus_contracts::SessionId,
    agent_id: Option<AgentId>,
    tier: Tier,
) -> Caller {
    Caller {
        agent_id,
        session,
        name: name.into(),
        project: "default".into(),
        tier,
        locality: Default::default(),
        access: None,
        principal_id: None,
    }
}

async fn lifecycle_values(state: &AppState) -> Vec<String> {
    DeveloperEvents::new(&state.store)
        .since(AGENT_LIFECYCLE_TOPIC, 0)
        .await
        .unwrap()
        .into_iter()
        .filter_map(|row| row.lifecycle)
        .collect()
}

#[tokio::test]
async fn identity_attach_allows_session_owner_and_operator() {
    let state = test_state().await;
    let ada = state
        .identity
        .register(register_request(
            "ada",
            "ck_ada",
            "default",
            hid("claude"),
            Kind::Agent,
            Tier::Agent,
        ))
        .await
        .unwrap();

    let own_response = nexus::daemon::dispatch::dispatch(
        &state,
        Some(caller_for_session(
            "ada",
            ada.session_id.clone(),
            ada.agent_id.clone(),
            Tier::Agent,
        )),
        attach_request(&ada.session_id),
    )
    .await;
    assert!(
        own_response.error.is_none(),
        "own attach failed: {:?}",
        own_response.error
    );

    let operator_response = nexus::daemon::dispatch::dispatch(
        &state,
        Some(caller_for_session(
            "operator",
            nexus_contracts::SessionId("local-operator".into()),
            None,
            Tier::Admin,
        )),
        attach_request(&ada.session_id),
    )
    .await;
    assert!(
        operator_response.error.is_none(),
        "operator attach failed: {:?}",
        operator_response.error
    );

    let lifecycles = lifecycle_values(&state).await;
    assert_eq!(
        lifecycles
            .iter()
            .filter(|phase| phase.as_str() == "attach")
            .count(),
        2
    );
}

#[tokio::test]
async fn identity_attach_rejects_other_agent_session() {
    let state = test_state().await;
    let ada = state
        .identity
        .register(register_request(
            "ada",
            "ck_ada",
            "default",
            hid("claude"),
            Kind::Agent,
            Tier::Agent,
        ))
        .await
        .unwrap();
    let ben = state
        .identity
        .register(register_request(
            "ben",
            "ck_ben",
            "default",
            hid("claude"),
            Kind::Agent,
            Tier::Agent,
        ))
        .await
        .unwrap();

    let response = nexus::daemon::dispatch::dispatch(
        &state,
        Some(caller_for_session(
            "ada",
            ada.session_id,
            ada.agent_id,
            Tier::Agent,
        )),
        attach_request(&ben.session_id),
    )
    .await;

    let error = response.error.expect("forged attach is rejected");
    assert_eq!(error.code, codes::UNAUTHORIZED);
    assert!(
        !lifecycle_values(&state)
            .await
            .iter()
            .any(|phase| phase == "attach"),
        "rejected attach must not write a lifecycle event"
    );
}

#[tokio::test]
async fn command_worker_identity_attach_records_event_for_operator_command() {
    let state = test_state().await;
    let ada = state
        .identity
        .register(register_request(
            "ada",
            "ck_ada",
            "default",
            hid("claude"),
            Kind::Agent,
            Tier::Agent,
        ))
        .await
        .unwrap();

    CommandIntents::new(&state.store)
        .insert_pending(NewCommandIntent {
            command_id: "cmd_identity_attach".to_string(),
            kind: command_kinds::identity::ATTACH.to_string(),
            project: "default".to_string(),
            caller_name: "Alex Morgan".to_string(),
            caller_session_id: Some("local-operator".to_string()),
            caller_agent_id: None,
            caller_runtime_id: Some("local-operator".to_string()),
            caller_client_key: None,
            caller_kind: Some("human".to_string()),
            caller_tier: Some("admin".to_string()),
            idempotency_key: None,
            request_json: serde_json::json!({ "sessionId": ada.session_id.0 }).to_string(),
            created_at: nexus_common::now(),
        })
        .await
        .unwrap();

    process_next_command(&state).await;

    let command = CommandIntents::new(&state.store)
        .get("cmd_identity_attach")
        .await
        .unwrap()
        .expect("command row exists");
    assert_eq!(
        command.status, "done",
        "command error was {:?}",
        command.error_json
    );
    assert_eq!(
        lifecycle_values(&state)
            .await
            .iter()
            .filter(|phase| phase.as_str() == "attach")
            .count(),
        1,
        "operator attach command must write exactly one attach lifecycle event"
    );
}

#[tokio::test]
async fn command_worker_rejects_name_only_caller_claim() {
    let state = test_state().await;
    state
        .identity
        .register(human_register("victim", "ck_victim"))
        .await
        .unwrap();

    CommandIntents::new(&state.store)
        .insert_pending(create_thread_intent(
            "cmd_name_only",
            "victim",
            None,
            None,
            "forged-name-only",
        ))
        .await
        .unwrap();

    assert_rejected(&state, "cmd_name_only", "forged-name-only").await;
}

#[tokio::test]
async fn command_worker_accepts_local_operator_marker_with_display_name() {
    let state = test_state().await;
    let mut intent = create_thread_intent(
        "cmd_local_operator_display_name",
        "Alex Morgan",
        Some("local-operator".into()),
        None,
        "local-operator-display-name-proof",
    );
    intent.caller_runtime_id = Some("local-operator".into());

    CommandIntents::new(&state.store)
        .insert_pending(intent)
        .await
        .unwrap();

    assert_executed(
        &state,
        "cmd_local_operator_display_name",
        "local-operator-display-name-proof",
    )
    .await;
}

#[tokio::test]
async fn authenticated_command_refreshes_caller_heartbeat() {
    let state = test_state().await;
    let registered = state
        .identity
        .register(human_register("operator-human", "ck_operator_human"))
        .await
        .unwrap();

    let old_heartbeat = nexus_common::now() - 120_000;
    Sessions::new(&state.store)
        .set_presence(&registered.session_id, Presence::Online)
        .await
        .unwrap();
    state
        .store
        .conn
        .execute(
            "UPDATE sessions SET last_heartbeat = ?2 WHERE session_id = ?1",
            libsql::params![registered.session_id.0.clone(), old_heartbeat],
        )
        .await
        .unwrap();

    CommandIntents::new(&state.store)
        .insert_pending(create_thread_intent(
            "cmd_refresh_heartbeat",
            "operator-human",
            Some(registered.session_id.0.clone()),
            Some("ck_operator_human".to_string()),
            "heartbeat-refresh-proof",
        ))
        .await
        .unwrap();

    assert_executed(&state, "cmd_refresh_heartbeat", "heartbeat-refresh-proof").await;

    let row = Sessions::new(&state.store)
        .find_by_session_id(&registered.session_id)
        .await
        .unwrap()
        .expect("registered session");
    assert!(
        row.last_heartbeat.unwrap_or_default() > old_heartbeat,
        "verified command activity must refresh last_heartbeat"
    );
}

#[tokio::test]
async fn authenticated_command_refreshes_active_runtime_presence() {
    let state = test_state().await;
    let registered = state
        .identity
        .register(register_request(
            "active-agent",
            "ck_active_agent",
            "default",
            hid("claude"),
            Kind::Agent,
            Tier::Agent,
        ))
        .await
        .unwrap();

    Sessions::new(&state.store)
        .set_presence(&registered.session_id, Presence::Offline)
        .await
        .unwrap();
    AgentRuntimes::new(&state.store)
        .set_presence(&registered.session_id.0, Presence::Offline)
        .await
        .unwrap();
    AgentRuntimes::new(&state.store)
        .set_active(&registered.session_id.0, false)
        .await
        .unwrap();

    CommandIntents::new(&state.store)
        .insert_pending(NewCommandIntent {
            command_id: "cmd_runtime_presence".to_string(),
            kind: command_kinds::presence::HEARTBEAT.to_string(),
            project: "default".to_string(),
            caller_name: "active-agent".to_string(),
            caller_session_id: Some(registered.session_id.0.clone()),
            caller_agent_id: registered.agent_id.as_ref().map(|id| id.0.clone()),
            caller_runtime_id: Some(registered.session_id.0.clone()),
            caller_client_key: Some("ck_active_agent".to_string()),
            caller_kind: Some("agent".to_string()),
            caller_tier: Some("agent".to_string()),
            idempotency_key: None,
            request_json: "null".to_string(),
            created_at: nexus_common::now(),
        })
        .await
        .unwrap();

    let mut events = state.ws.subscribe();
    process_next_command(&state).await;

    let runtime = AgentRuntimes::new(&state.store)
        .find_by_runtime_id(&registered.session_id.0)
        .await
        .unwrap()
        .expect("runtime row");
    assert_eq!(runtime.presence.as_deref(), Some("online"));
    assert!(runtime.active);
    let notification = tokio::time::timeout(Duration::from_secs(1), events.recv())
        .await
        .expect("ordered status fact timeout")
        .expect("ordered status fact channel");
    let status: WsEvent =
        serde_json::from_value(notification.params.expect("status params")).expect("status event");
    assert!(matches!(
        status,
        WsEvent::AgentStatus {
            session_id,
            presence: Presence::Online,
            paused: false,
        } if session_id == registered.session_id
    ));
}

#[tokio::test]
async fn command_worker_rejects_forged_session_id_without_verified_client_key() {
    let state = test_state().await;
    let registered = state
        .identity
        .register(human_register("victim", "ck_victim"))
        .await
        .unwrap();

    CommandIntents::new(&state.store)
        .insert_pending(create_thread_intent(
            "cmd_bad_key",
            "victim",
            Some(registered.session_id.0),
            Some("wrong-client-key".into()),
            "forged-session-id",
        ))
        .await
        .unwrap();

    assert_rejected(&state, "cmd_bad_key", "forged-session-id").await;
}

#[tokio::test]
async fn command_worker_accepts_staged_unnamed_session_by_registered_key() {
    let state = test_state().await;
    Agents::new(&state.store)
        .create(NewAgent {
            agent_id: "a_s_staged".into(),
            project: "default".into(),
            name: None,
            default_harness: Some("codex".into()),
            role: None,
            tier: Some("agent".into()),
            owner: None,
        })
        .await
        .unwrap();
    let session_id = nexus_contracts::SessionId("s_staged".into());
    Sessions::new(&state.store)
        .create(NewSession {
            session_id: session_id.clone(),
            name: None,
            agent: Some("codex".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: Some("s_staged".into()),
            client_key: Some("ck_staged".into()),
            cwd: Some("/repo".into()),
            project: "default".into(),
            transport: Some("codex-appserver".into()),
        })
        .await
        .unwrap();
    Sessions::new(&state.store)
        .set_agent_id(&session_id, "a_s_staged")
        .await
        .unwrap();

    let mut intent = create_thread_intent(
        "cmd_staged_send",
        "a_s_staged",
        None,
        Some("ck_staged".into()),
        "staged-thread",
    );
    intent.caller_agent_id = Some("a_s_staged".into());
    intent.caller_kind = Some("agent".into());
    intent.caller_tier = Some("agent".into());
    CommandIntents::new(&state.store)
        .insert_pending(intent)
        .await
        .unwrap();

    assert_executed(&state, "cmd_staged_send", "staged-thread").await;
}

#[tokio::test]
async fn command_worker_rejects_missing_identity_for_authenticated_command() {
    let state = test_state().await;

    CommandIntents::new(&state.store)
        .insert_pending(create_thread_intent(
            "cmd_no_identity",
            "ghost",
            None,
            None,
            "missing-identity",
        ))
        .await
        .unwrap();

    let error = assert_rejected(&state, "cmd_no_identity", "missing-identity").await;
    assert!(error.message.contains("verified client key"));
}

#[tokio::test]
async fn command_worker_persists_structured_error_json_for_identity_reject() {
    let state = test_state().await;

    CommandIntents::new(&state.store)
        .insert_pending(create_thread_intent(
            "cmd_audit_identity_reject",
            "ghost",
            None,
            None,
            "identity-reject-audit",
        ))
        .await
        .unwrap();

    let error = assert_rejected(&state, "cmd_audit_identity_reject", "identity-reject-audit").await;
    assert!(error.message.contains("verified client key"));
    let command = CommandIntents::new(&state.store)
        .get("cmd_audit_identity_reject")
        .await
        .unwrap()
        .expect("command row exists after rejection");
    let error_json = command
        .error_json
        .as_deref()
        .expect("rejection is persisted as structured error_json");
    let persisted: serde_json::Value = serde_json::from_str(error_json).unwrap();
    assert_eq!(persisted["code"], codes::UNAUTHORIZED);
    assert!(persisted["message"]
        .as_str()
        .unwrap()
        .contains("verified client key"));
}

#[tokio::test]
async fn command_worker_rejects_runtime_client_key_after_credential_revoke() {
    let state = test_state().await;
    let credential_id =
        create_agent_with_runtime_credential(&state, "a_revoked", "revoked", "secret").await;
    let mut register = register_request(
        "revoked",
        "ck_revoked_runtime",
        "default",
        hid("claude"),
        Kind::Agent,
        Tier::Agent,
    );
    register.agent_id = Some(AgentId("a_revoked".into()));
    register.runtime_credential = Some("secret".into());
    state.identity.register(register).await.unwrap();

    let revoke = Request {
        jsonrpc: JSONRPC_VERSION.to_string(),
        id: None,
        method: "agent.credential.revoke".into(),
        params: Some(
            serde_json::to_value(AgentCredentialRevokeRequest {
                credential_id: credential_id.into(),
            })
            .unwrap(),
        ),
    };
    let response = nexus::daemon::dispatch::dispatch(
        &state,
        Some(nexus_contracts::Caller {
            agent_id: None,
            session: nexus_contracts::SessionId("s_admin".into()),
            name: "operator".into(),
            project: "default".into(),
            tier: Tier::Admin,
            locality: Default::default(),
            access: None,
            principal_id: None,
        }),
        revoke,
    )
    .await;
    assert!(
        response.error.is_none(),
        "revoke failed: {:?}",
        response.error
    );

    let mut intent = create_thread_intent(
        "cmd_after_revoke",
        "revoked",
        None,
        Some("ck_revoked_runtime".into()),
        "must-not-execute-after-revoke",
    );
    intent.caller_kind = Some("agent".into());
    intent.caller_tier = Some("agent".into());
    CommandIntents::new(&state.store)
        .insert_pending(intent)
        .await
        .unwrap();

    let error = assert_rejected(&state, "cmd_after_revoke", "must-not-execute-after-revoke").await;
    assert!(error.message.contains("not registered"));
}

#[tokio::test]
async fn command_worker_uses_the_client_key_owner_instead_of_a_submitted_name_claim() {
    let state = test_state().await;
    state
        .identity
        .register(human_register("victim", "ck_victim"))
        .await
        .unwrap();
    state
        .identity
        .register(human_register("attacker", "ck_attacker"))
        .await
        .unwrap();

    CommandIntents::new(&state.store)
        .insert_pending(create_thread_intent(
            "cmd_borrowed_key",
            "victim",
            None,
            Some("ck_attacker".into()),
            "forged-borrowed-key",
        ))
        .await
        .unwrap();

    assert_executed(&state, "cmd_borrowed_key", "forged-borrowed-key").await;
    let members = Threads::new(&state.store)
        .find_by_name("default", "forged-borrowed-key")
        .await
        .unwrap()
        .expect("created thread")
        .thread_id;
    assert_eq!(
        Threads::new(&state.store).members(&members).await.unwrap(),
        vec!["attacker"],
        "the immutable client key owner, not caller_name, owns the command"
    );
}

#[tokio::test]
async fn rename_keeps_mcp_and_cli_env_writes_live_and_canonically_attributed() {
    let state = test_state().await;
    let registered = state
        .identity
        .register(register_request(
            "before-rename",
            "ck_rename_continuity",
            "default",
            hid("claude"),
            Kind::Agent,
            Tier::Agent,
        ))
        .await
        .unwrap();

    let stale_intent = |command_id: &str, kind: &str, request_json: String| NewCommandIntent {
        command_id: command_id.into(),
        kind: kind.into(),
        project: "default".into(),
        caller_name: "before-rename".into(),
        caller_session_id: None,
        caller_agent_id: None,
        caller_runtime_id: None,
        caller_client_key: Some("ck_rename_continuity".into()),
        caller_kind: Some("agent".into()),
        caller_tier: Some("agent".into()),
        idempotency_key: None,
        request_json,
        created_at: nexus_common::now(),
    };

    CommandIntents::new(&state.store)
        .insert_pending(stale_intent(
            "cmd_rename_to_fable",
            command_kinds::identity::RENAME,
            serde_json::to_string(&RenameRequest {
                name: "fable".into(),
            })
            .unwrap(),
        ))
        .await
        .unwrap();
    process_next_command(&state).await;

    let mut cli_env_send = stale_intent(
        "cmd_send_after_rename",
        command_kinds::message_post::SEND,
        serde_json::to_string(&SendRequest {
            to: SendTarget::dm_name("operator"),
            summary: None,
            body: "identity continuity after rename".into(),
            mention: vec![],
            metadata: None,
            idempotency_key: Some("rename-continuity-send".into()),
        })
        .unwrap(),
    );
    // Some clients fall back to the immutable agent id when their cached display name becomes
    // stale. That label is metadata too; the registered client key remains the credential.
    cli_env_send.caller_name = registered.agent_id.as_ref().unwrap().0.clone();
    cli_env_send.caller_agent_id = registered.agent_id.as_ref().map(|id| id.0.clone());
    CommandIntents::new(&state.store)
        .insert_pending(cli_env_send)
        .await
        .unwrap();
    process_next_command(&state).await;

    let send_row = CommandIntents::new(&state.store)
        .get("cmd_send_after_rename")
        .await
        .unwrap()
        .expect("send command row");
    assert_eq!(
        send_row.status, "done",
        "a noncanonical caller label must not wedge a valid client key: {:?}",
        send_row.error_json
    );
    let mut rows = state
        .store
        .conn
        .query(
            "SELECT from_name, from_agent_id FROM messages \
             WHERE body = 'identity continuity after rename'",
            (),
        )
        .await
        .unwrap();
    let message = rows.next().await.unwrap().expect("canonical message row");
    assert_eq!(message.get::<String>(0).unwrap(), "fable");
    assert_eq!(
        message.get::<String>(1).unwrap(),
        registered.agent_id.as_ref().unwrap().0
    );

    CommandIntents::new(&state.store)
        .insert_pending(stale_intent(
            "cmd_rename_back",
            command_kinds::identity::RENAME,
            serde_json::to_string(&RenameRequest {
                name: "before-rename".into(),
            })
            .unwrap(),
        ))
        .await
        .unwrap();
    process_next_command(&state).await;

    let renamed_back = Sessions::new(&state.store)
        .find_by_session_id(&registered.session_id)
        .await
        .unwrap()
        .expect("renamed session");
    assert_eq!(renamed_back.name.as_deref(), Some("before-rename"));
}

#[tokio::test]
async fn command_worker_accepts_valid_client_key_across_project_metadata() {
    let state = test_state().await;
    state
        .identity
        .register(register_request(
            "victim",
            "ck_other_project",
            "other-project",
            hid("other"),
            Kind::Human,
            Tier::Admin,
        ))
        .await
        .unwrap();

    CommandIntents::new(&state.store)
        .insert_pending(create_thread_intent(
            "cmd_cross_project",
            "victim",
            None,
            Some("ck_other_project".into()),
            "cross-project-metadata",
        ))
        .await
        .unwrap();

    assert_executed(&state, "cmd_cross_project", "cross-project-metadata").await;
}

#[tokio::test]
async fn command_worker_rejects_mismatched_runtime_metadata() {
    let state = test_state().await;
    state
        .identity
        .register(human_register("victim", "ck_victim"))
        .await
        .unwrap();

    let mut intent = create_thread_intent(
        "cmd_bad_runtime",
        "victim",
        None,
        Some("ck_victim".into()),
        "forged-runtime",
    );
    intent.caller_runtime_id = Some("s_someone_else".into());
    CommandIntents::new(&state.store)
        .insert_pending(intent)
        .await
        .unwrap();

    let error = assert_rejected(&state, "cmd_bad_runtime", "forged-runtime").await;
    assert!(error.message.contains("runtime"));
}

#[tokio::test]
async fn command_worker_rejects_mismatched_kind_and_tier_metadata() {
    let state = test_state().await;
    state
        .identity
        .register(register_request(
            "agent",
            "ck_agent",
            "default",
            hid("claude"),
            Kind::Agent,
            Tier::Agent,
        ))
        .await
        .unwrap();

    let mut bad_kind = create_thread_intent(
        "cmd_bad_kind",
        "agent",
        None,
        Some("ck_agent".into()),
        "forged-kind",
    );
    bad_kind.caller_kind = Some("human".into());
    bad_kind.caller_tier = Some("agent".into());
    CommandIntents::new(&state.store)
        .insert_pending(bad_kind)
        .await
        .unwrap();
    let error = assert_rejected(&state, "cmd_bad_kind", "forged-kind").await;
    assert!(error.message.contains("kind"));

    let mut bad_tier = create_thread_intent(
        "cmd_bad_tier",
        "agent",
        None,
        Some("ck_agent".into()),
        "forged-tier",
    );
    bad_tier.caller_kind = Some("agent".into());
    bad_tier.caller_tier = Some("admin".into());
    CommandIntents::new(&state.store)
        .insert_pending(bad_tier)
        .await
        .unwrap();
    let error = assert_rejected(&state, "cmd_bad_tier", "forged-tier").await;
    assert!(error.message.contains("tier"));
}

#[tokio::test]
async fn command_worker_accepts_registered_client_key_for_each_harness() {
    let state = test_state().await;

    for harness in [
        hid("claude"),
        hid("codex"),
        hid("opencode"),
        hid("hermes"),
        hid("pi"),
        hid("other"),
    ] {
        let token = harness_token(harness.clone());
        let caller_name = format!("{token}_caller");
        let client_key = format!("ck_{token}");
        let command_id = format!("cmd_{token}");
        let thread_name = format!("{token}-thread");

        state
            .identity
            .register(register_request(
                &caller_name,
                &client_key,
                "default",
                harness,
                Kind::Agent,
                Tier::Admin,
            ))
            .await
            .unwrap();

        let mut intent = create_thread_intent(
            &command_id,
            &caller_name,
            None,
            Some(client_key),
            &thread_name,
        );
        intent.caller_kind = Some("agent".into());
        CommandIntents::new(&state.store)
            .insert_pending(intent)
            .await
            .unwrap();

        assert_executed(&state, &command_id, &thread_name).await;
    }
}

#[tokio::test]
async fn command_worker_accepts_mcp_store_client_row_with_registered_key() {
    let state = test_state().await;
    state
        .identity
        .register(register_request(
            "mcp-agent",
            "ck_mcp_agent",
            "default",
            hid("claude"),
            Kind::Agent,
            Tier::Agent,
        ))
        .await
        .unwrap();

    let client = StoreClient::from_store_with_caller_for_tests(
        state.store.clone(),
        "mcp-agent",
        "default",
        Some("ck_mcp_agent".into()),
        Tier::Agent,
        Kind::Agent,
    );
    let pending = tokio::spawn(async move {
        client
            .command::<_, ()>(
                command_kinds::thread::CREATE,
                &CreateThreadRequest {
                    name: "mcp-ingress".into(),
                    members: vec![],
                },
            )
            .await
    });

    process_next_command(&state).await;
    pending.await.unwrap().unwrap();
    assert!(
        Threads::new(&state.store)
            .find_by_name("default", "mcp-ingress")
            .await
            .unwrap()
            .is_some(),
        "registered MCP caller should execute through the command worker"
    );
}

#[tokio::test]
async fn command_worker_accepts_web_gateway_row_with_registered_key() {
    let state = test_state().await;
    let registered = state
        .identity
        .register(human_register("Alex", "ck_web_human"))
        .await
        .unwrap();

    CommandIntents::new(&state.store)
        .insert_pending(NewCommandIntent {
            command_id: "cmd_web_gateway".into(),
            kind: command_kinds::thread::CREATE.into(),
            project: "default".into(),
            caller_name: "Alex".into(),
            caller_session_id: Some(registered.session_id.0.clone()),
            caller_agent_id: registered.agent_id.as_ref().map(|id| id.0.clone()),
            caller_runtime_id: Some(registered.session_id.0),
            caller_client_key: Some("ck_web_human".into()),
            caller_kind: Some("human".into()),
            caller_tier: Some("admin".into()),
            idempotency_key: None,
            request_json: serde_json::to_string(&CreateThreadRequest {
                name: "web-ingress".into(),
                members: vec![],
            })
            .unwrap(),
            created_at: 1,
        })
        .await
        .unwrap();

    assert_executed(&state, "cmd_web_gateway", "web-ingress").await;
}

/// A validated [`nexus_contracts::HarnessId`] from a literal (panics on invalid — test-only).
fn hid(s: &str) -> nexus_contracts::HarnessId {
    nexus_contracts::HarnessId::new(s).expect("valid harness id literal")
}
