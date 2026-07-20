use std::sync::Arc;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::Serialize;

use nexus::daemon::gateway_stream_socket::{GatewayStreamFrame, GatewayStreamPublisher};
use nexus::daemon::{dispatch, AppState};
use nexus_common::{hash_runtime_credential, Config};
use nexus_contracts::{
    codes, AdminGroupAssignRequest, AgentAccessGrantRequest, AgentAccessGrantResponse,
    AgentAccessRevokeRequest, AgentAccessRevokeResponse, AgentAccessRole, AgentCreateRequest,
    AgentCreateResponse, AgentCredentialCreateRequest, AgentCredentialCreateResponse,
    AgentCredentialRevokeRequest, AgentCredentialRevokeResponse, AgentId, AgentListRequest,
    AgentListResponse, AgentOwnerTransferRequest, AgentOwnerTransferResponse,
    AgentRuntimeListRequest, AgentRuntimeListResponse, AgentShowRequest, AgentShowResponse,
    AssignProjectRequest, AssignProjectResponse, AssignRoleRequest, AssignRoleResponse, Caller,
    GrantTierRequest, GrantTierResponse, Kind, MemberListRequest, MemberListResponse,
    RegisterRequest, RegisterResponse, RemoveRequest, RemoveResponse, Request, RequestId, RpcError,
    SessionId, Tier,
};
use nexus_store::repos::{
    AgentAccessGrants, AgentCredentials, AgentOwner, AgentRuntimes, Agents, Metadata,
    MetadataEntity, NewSession, Sessions,
};
use nexus_store::{DaemonStore, Store};

async fn state() -> AppState {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    AppState::wire(store, &Config::default())
}

async fn split_state_with_gateway_stream() -> (
    tempfile::TempDir,
    AppState,
    tokio::sync::broadcast::Receiver<GatewayStreamFrame>,
) {
    let directory = tempfile::tempdir().unwrap();
    let identity_path = directory.path().join("identity.db");
    let daemon = DaemonStore::open(identity_path.to_str().unwrap())
        .await
        .unwrap();
    let store = Arc::new(daemon.compatibility_store());
    let publisher = GatewayStreamPublisher::new(128);
    let receiver = publisher.subscribe();
    (
        directory,
        AppState::wire_pty_with_gateway_stream(store, &Config::default(), Some(publisher)),
        receiver,
    )
}

async fn state_with_gateway_stream() -> (
    AppState,
    tokio::sync::broadcast::Receiver<GatewayStreamFrame>,
) {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let publisher = GatewayStreamPublisher::new(128);
    let receiver = publisher.subscribe();
    (
        AppState::wire_pty_with_gateway_stream(store, &Config::default(), Some(publisher)),
        receiver,
    )
}

async fn next_identity_projection(
    receiver: &mut tokio::sync::broadcast::Receiver<GatewayStreamFrame>,
    agent_id: &AgentId,
) -> serde_json::Value {
    let mut observed = Vec::new();
    let result = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let frame = receiver.recv().await.unwrap();
            observed.push(format!("{frame:?}"));
            if let GatewayStreamFrame::Projection { event } = frame {
                if event.kind == nexus_contracts::GatewayProjectionKind::IdentityUpserted
                    && event.payload["agentId"] == agent_id.0
                {
                    return event.payload;
                }
            }
        }
    })
    .await;
    result.unwrap_or_else(|_| {
        panic!("identity projection should be republished; observed {observed:#?}")
    })
}

fn caller(name: &str, project: &str, tier: Tier) -> Caller {
    let session = if name == "operator" && tier == Tier::Admin {
        "local-operator".to_string()
    } else {
        format!("s_{name}")
    };
    Caller {
        agent_id: None,
        session: SessionId(session),
        name: name.into(),
        project: project.into(),
        tier,
        locality: Default::default(),
        access: None,
        principal_id: None,
    }
}

fn req<T: Serialize>(method: &str, params: &T) -> Request {
    Request {
        jsonrpc: nexus_contracts::JSONRPC_VERSION.to_string(),
        id: Some(RequestId::Num(1)),
        method: method.into(),
        params: Some(serde_json::to_value(params).unwrap()),
    }
}

async fn call_ok<T, P>(state: &AppState, caller: Caller, method: &str, params: &P) -> T
where
    T: DeserializeOwned,
    P: Serialize,
{
    let resp = dispatch(state, Some(caller), req(method, params)).await;
    assert!(resp.error.is_none(), "unexpected error: {:?}", resp.error);
    serde_json::from_value(resp.result.unwrap()).unwrap()
}

async fn call_err<P>(state: &AppState, caller: Caller, method: &str, params: &P) -> RpcError
where
    P: Serialize,
{
    let resp = dispatch(state, Some(caller), req(method, params)).await;
    resp.error.expect("expected error")
}

async fn create_agent(state: &AppState, name: &str) -> AgentCreateResponse {
    call_ok(
        state,
        caller("operator", "demo", Tier::Admin),
        "agent.create",
        &AgentCreateRequest {
            name: name.into(),
            default_harness: Some(hid("codex")),
            project: None,
            role: Some("backend".into()),
        },
    )
    .await
}

async fn acl_rows_named(state: &AppState, name: &str) -> i64 {
    let mut rows = state
        .store
        .conn
        .query(
            "SELECT COUNT(*) FROM agent_acl_grants \
             WHERE principal_name = ?1 OR granted_by_name = ?1",
            libsql::params![name],
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().expect("acl count row");
    row.get(0).unwrap()
}

async fn register_agent_runtime(state: &AppState, name: &str, project: &str) -> RegisterResponse {
    register_agent_runtime_with_client_key(state, name, project, &format!("ck-{name}-runtime"))
        .await
}

async fn register_agent_runtime_with_client_key(
    state: &AppState,
    name: &str,
    project: &str,
    client_key: &str,
) -> RegisterResponse {
    let register = RegisterRequest {
        name: Some(name.into()),
        agent_id: None,
        harness: hid("codex"),
        harness_session_id: format!("{client_key}-native-session"),
        project: project.into(),
        client_key: client_key.into(),
        runtime_credential: None,
        tier: Tier::Agent,
        kind: Some(Kind::Agent),
        locality: Default::default(),
        access: None,
        role: Some("backend".into()),
        cwd: Some("/repo".into()),
    };
    let registered = dispatch(state, None, req("register", &register)).await;
    assert!(
        registered.error.is_none(),
        "unexpected register error: {:?}",
        registered.error
    );
    serde_json::from_value(registered.result.unwrap()).unwrap()
}

async fn register_human_runtime(
    state: &AppState,
    name: &str,
    project: &str,
    tier: Tier,
) -> RegisterResponse {
    let register = RegisterRequest {
        name: Some(name.into()),
        agent_id: None,
        harness: hid("codex"),
        harness_session_id: format!("{name}-human-session"),
        project: project.into(),
        client_key: format!("ck-{name}-human"),
        runtime_credential: None,
        tier,
        kind: Some(Kind::Human),
        locality: Default::default(),
        access: None,
        role: Some("operator".into()),
        cwd: Some("/repo".into()),
    };
    let registered = dispatch(state, None, req("register", &register)).await;
    assert!(
        registered.error.is_none(),
        "unexpected register error: {:?}",
        registered.error
    );
    serde_json::from_value(registered.result.unwrap()).unwrap()
}

#[tokio::test]
async fn agent_create_show_and_list_roundtrip_by_project() {
    let state = state().await;
    let created = create_agent(&state, "ember").await;

    assert!(created.agent.agent_id.0.starts_with("a_"));
    assert_eq!(created.agent.name.as_deref(), Some("ember"));
    assert_eq!(created.agent.project, "demo");
    assert_eq!(created.agent.default_harness, Some(hid("codex")));
    assert!(created.credential.is_none());

    let reader = caller("reader", "demo", Tier::Agent);
    let shown: AgentShowResponse = call_ok(
        &state,
        reader.clone(),
        "agent.show",
        &AgentShowRequest {
            agent_id: None,
            name: Some("ember".into()),
        },
    )
    .await;
    assert_eq!(shown.agent.agent_id, created.agent.agent_id);

    let listed: AgentListResponse = call_ok(
        &state,
        reader,
        "agent.list",
        &AgentListRequest {
            project: None,
            include_disabled: None,
        },
    )
    .await;
    assert_eq!(listed.agents.len(), 1);
    assert_eq!(listed.agents[0].agent_id, created.agent.agent_id);
}

#[tokio::test]
async fn agent_show_falls_back_to_an_unowned_id_shaped_alias() {
    let state = state().await;
    let created = create_agent(&state, "a_show_alias_only").await;

    let shown: AgentShowResponse = call_ok(
        &state,
        caller("reader", "different-project-metadata", Tier::Admin),
        "agent.show",
        &AgentShowRequest {
            agent_id: None,
            name: Some("a_show_alias_only".into()),
        },
    )
    .await;

    assert_eq!(shown.agent.agent_id, created.agent.agent_id);
}

#[tokio::test]
async fn name_only_id_shaped_tokens_prefer_the_exact_agent_id_over_an_alias_collision() {
    let state = state().await;
    let exact = create_agent(&state, "exact-owner").await;
    let alias = create_agent(&state, &exact.agent.agent_id.0).await;

    let shown: AgentShowResponse = call_ok(
        &state,
        caller("reader", "unrelated-project-metadata", Tier::Agent),
        "agent.show",
        &AgentShowRequest {
            agent_id: None,
            name: Some(exact.agent.agent_id.0.clone()),
        },
    )
    .await;

    assert_eq!(shown.agent.agent_id, exact.agent.agent_id);
    assert_ne!(shown.agent.agent_id, alias.agent.agent_id);
}

#[tokio::test]
async fn name_only_role_and_project_routes_prefer_exact_id_over_an_alias_collision() {
    let state = state().await;
    let exact = register_agent_runtime(&state, "exact-route-owner", "original-project").await;
    let exact_id = exact.agent_id.clone().unwrap();
    let alias = create_agent(&state, &exact_id.0).await;

    let role: AssignRoleResponse = call_ok(
        &state,
        caller("operator", "stale-project-metadata", Tier::Admin),
        "admin.assignRole",
        &AssignRoleRequest {
            agent_id: None,
            name: exact_id.0.clone(),
            role: "lead".into(),
        },
    )
    .await;
    assert_eq!(role.name.as_deref(), Some("exact-route-owner"));

    let project: AssignProjectResponse = call_ok(
        &state,
        caller("operator", "another-stale-project", Tier::Admin),
        "admin.assignProject",
        &AssignProjectRequest {
            agent_id: None,
            name: exact_id.0.clone(),
            project: "moved-project".into(),
        },
    )
    .await;
    assert_eq!(project.name.as_deref(), Some("exact-route-owner"));

    let exact_row = Agents::new(&state.store)
        .find_by_id(&exact_id.0)
        .await
        .unwrap()
        .unwrap();
    let alias_row = Agents::new(&state.store)
        .find_by_id(&alias.agent.agent_id.0)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(exact_row.role.as_deref(), Some("lead"));
    assert_eq!(exact_row.project, "moved-project");
    assert_eq!(alias_row.role.as_deref(), Some("backend"));
    assert_eq!(alias_row.project, "demo");
}

#[tokio::test]
async fn explicit_missing_agent_ids_never_fall_back_to_names_or_mutate_any_admin_surface() {
    let state = state().await;
    let target = create_agent(&state, "exclusive-target").await;
    create_agent(&state, "exclusive-principal").await;
    let admin = caller("operator", "stale-project-metadata", Tier::Admin);
    let missing = AgentId("a_explicit_missing".into());

    let errors = vec![
        call_err(
            &state,
            admin.clone(),
            "admin.group.assign",
            &AdminGroupAssignRequest {
                project: Some("policy-project".into()),
                group: "release".into(),
                agent_id: Some(missing.clone()),
                name: "exclusive-target".into(),
            },
        )
        .await,
        call_err(
            &state,
            admin.clone(),
            "admin.assignRole",
            &AssignRoleRequest {
                agent_id: Some(missing.clone()),
                name: "exclusive-target".into(),
                role: "lead".into(),
            },
        )
        .await,
        call_err(
            &state,
            admin.clone(),
            "admin.assignProject",
            &AssignProjectRequest {
                agent_id: Some(missing.clone()),
                name: "exclusive-target".into(),
                project: "moved".into(),
            },
        )
        .await,
        call_err(
            &state,
            admin.clone(),
            "admin.grantTier",
            &GrantTierRequest {
                agent_id: Some(missing.clone()),
                name: "exclusive-target".into(),
                tier: Tier::Admin,
            },
        )
        .await,
        call_err(
            &state,
            admin.clone(),
            "agent.show",
            &AgentShowRequest {
                agent_id: Some(missing.clone()),
                name: Some("exclusive-target".into()),
            },
        )
        .await,
        call_err(
            &state,
            admin.clone(),
            "agent.runtime.list",
            &AgentRuntimeListRequest {
                agent_id: Some(missing.clone()),
                name: Some("exclusive-target".into()),
                include_stopped: Some(true),
            },
        )
        .await,
        call_err(
            &state,
            admin.clone(),
            "agent.grantAccess",
            &AgentAccessGrantRequest {
                agent_id: Some(missing.clone()),
                name: "exclusive-target".into(),
                principal_agent_id: None,
                principal: "exclusive-principal".into(),
                project: Some("irrelevant-project".into()),
                role: AgentAccessRole::Viewer,
            },
        )
        .await,
        call_err(
            &state,
            admin.clone(),
            "agent.revokeAccess",
            &AgentAccessRevokeRequest {
                agent_id: Some(missing.clone()),
                name: "exclusive-target".into(),
                principal_agent_id: None,
                principal: "exclusive-principal".into(),
                project: Some("irrelevant-project".into()),
            },
        )
        .await,
        call_err(
            &state,
            admin.clone(),
            "agent.transferOwner",
            &AgentOwnerTransferRequest {
                agent_id: Some(missing.clone()),
                name: "exclusive-target".into(),
                owner_agent_id: None,
                owner: "exclusive-principal".into(),
                project: Some("irrelevant-project".into()),
            },
        )
        .await,
        call_err(
            &state,
            admin,
            "agent.credential.create",
            &AgentCredentialCreateRequest {
                agent_id: Some(missing),
                name: Some("exclusive-target".into()),
                label: None,
                purpose: None,
                scopes: vec![],
            },
        )
        .await,
    ];
    assert!(errors.iter().all(|error| error.code == codes::NOT_FOUND));

    let stored = Agents::new(&state.store)
        .find_by_id(&target.agent.agent_id.0)
        .await
        .unwrap()
        .expect("target identity remains");
    assert_eq!(stored.project, "demo");
    assert_eq!(stored.role.as_deref(), Some("backend"));
    assert_eq!(stored.tier, "agent");
    assert!(stored.owner_agent_id.is_none());
    assert!(AgentAccessGrants::new(&state.store)
        .list_for_agent(&target.agent.agent_id.0)
        .await
        .unwrap()
        .is_empty());
    assert!(AgentCredentials::new(&state.store)
        .find_active(&target.agent.agent_id.0)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn explicit_agent_ids_override_mismatched_names_across_admin_and_read_surfaces() {
    let state = state().await;
    let target = create_agent(&state, "id-authoritative-target").await;
    let imposter = create_agent(&state, "stale-request-name").await;
    let target_id = target.agent.agent_id.clone();
    let admin = caller("operator", "caller-project-metadata", Tier::Admin);

    let grouped: nexus_contracts::AdminGroupAssignResponse = call_ok(
        &state,
        admin.clone(),
        "admin.group.assign",
        &AdminGroupAssignRequest {
            project: Some("policy-metadata".into()),
            group: "release".into(),
            agent_id: Some(target_id.clone()),
            name: "stale-request-name".into(),
        },
    )
    .await;
    assert_eq!(grouped.agent_id, target_id);
    assert_eq!(grouped.name.as_deref(), Some("id-authoritative-target"));

    let granted: GrantTierResponse = call_ok(
        &state,
        admin.clone(),
        "admin.grantTier",
        &GrantTierRequest {
            agent_id: Some(target_id.clone()),
            name: "stale-request-name".into(),
            tier: Tier::Admin,
        },
    )
    .await;
    assert_eq!(granted.name.as_deref(), Some("id-authoritative-target"));

    let shown: AgentShowResponse = call_ok(
        &state,
        admin.clone(),
        "agent.show",
        &AgentShowRequest {
            agent_id: Some(target_id.clone()),
            name: Some("stale-request-name".into()),
        },
    )
    .await;
    assert_eq!(shown.agent.agent_id, target_id);

    let runtimes: AgentRuntimeListResponse = call_ok(
        &state,
        admin.clone(),
        "agent.runtime.list",
        &AgentRuntimeListRequest {
            agent_id: Some(target_id.clone()),
            name: Some("stale-request-name".into()),
            include_stopped: Some(true),
        },
    )
    .await;
    assert_eq!(runtimes.agent_id, target_id);

    let credential: AgentCredentialCreateResponse = call_ok(
        &state,
        admin,
        "agent.credential.create",
        &AgentCredentialCreateRequest {
            agent_id: Some(target_id.clone()),
            name: Some("stale-request-name".into()),
            label: Some("stable-id".into()),
            purpose: Some("runtime".into()),
            scopes: vec!["runtime:register".into()],
        },
    )
    .await;
    assert_eq!(credential.agent_id, target_id);

    let target_row = Agents::new(&state.store)
        .find_by_id(&target_id.0)
        .await
        .unwrap()
        .unwrap();
    let imposter_row = Agents::new(&state.store)
        .find_by_id(&imposter.agent.agent_id.0)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(target_row.tier, "admin");
    assert_eq!(imposter_row.tier, "agent");
    assert!(AgentCredentials::new(&state.store)
        .find_active(&imposter.agent.agent_id.0)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn admin_assign_role_updates_agent_show_read_model() {
    let state = state().await;
    create_agent(&state, "ember").await;

    let assigned: AssignRoleResponse = call_ok(
        &state,
        caller("operator", "demo", Tier::Admin),
        "admin.assignRole",
        &AssignRoleRequest {
            agent_id: None,
            name: "ember".into(),
            role: "lead".into(),
        },
    )
    .await;
    assert_eq!(assigned.role, "lead");

    let shown: AgentShowResponse = call_ok(
        &state,
        caller("reader", "demo", Tier::Agent),
        "agent.show",
        &AgentShowRequest {
            agent_id: None,
            name: Some("ember".into()),
        },
    )
    .await;
    assert_eq!(shown.agent.role.as_deref(), Some("lead"));
}

#[tokio::test]
async fn admin_assign_project_to_current_project_is_idempotent() {
    let state = state().await;
    register_agent_runtime(&state, "ember", "demo").await;

    let assigned: AssignProjectResponse = call_ok(
        &state,
        caller("operator", "demo", Tier::Admin),
        "admin.assignProject",
        &AssignProjectRequest {
            agent_id: None,
            name: "ember".into(),
            project: "demo".into(),
        },
    )
    .await;
    assert_eq!(assigned.name.as_deref(), Some("ember"));
    assert_eq!(assigned.project, "demo");
}

#[tokio::test]
async fn admin_assign_role_by_agent_id_survives_target_rename() {
    let state = state().await;
    let created = create_agent(&state, "ember").await;
    let agent_id = created.agent.agent_id.clone();

    Agents::new(&state.store)
        .rename(&agent_id.0, "ember-renamed")
        .await
        .unwrap();

    let assigned: AssignRoleResponse = call_ok(
        &state,
        caller("operator", "demo", Tier::Admin),
        "admin.assignRole",
        &AssignRoleRequest {
            agent_id: Some(agent_id.clone()),
            name: "ember".into(),
            role: "lead".into(),
        },
    )
    .await;
    assert_eq!(assigned.name.as_deref(), Some("ember-renamed"));
    assert_eq!(assigned.role, "lead");

    let shown: AgentShowResponse = call_ok(
        &state,
        caller("reader", "demo", Tier::Agent),
        "agent.show",
        &AgentShowRequest {
            agent_id: Some(agent_id),
            name: None,
        },
    )
    .await;
    assert_eq!(shown.agent.name.as_deref(), Some("ember-renamed"));
    assert_eq!(shown.agent.role.as_deref(), Some("lead"));
}

#[tokio::test]
async fn admin_assign_role_by_agent_id_is_atomic_with_the_compatibility_session() {
    let state = state().await;
    let registered = register_agent_runtime(&state, "role-atomic", "demo").await;
    let agent_id = registered.agent_id.clone().expect("agent id");
    let before_agent = Agents::new(&state.store)
        .find_by_id(&agent_id.0)
        .await
        .unwrap()
        .unwrap();
    let before_session = Sessions::new(&state.store)
        .find_by_session_id(&registered.session_id)
        .await
        .unwrap()
        .unwrap();
    state
        .store
        .conn
        .execute(
            "CREATE TRIGGER fail_admin_role_session_update \
             BEFORE UPDATE OF role ON sessions WHEN NEW.role = 'lead' BEGIN \
             SELECT RAISE(ABORT, 'forced admin role failure'); END",
            (),
        )
        .await
        .unwrap();

    let error = call_err(
        &state,
        caller("operator", "demo", Tier::Admin),
        "admin.assignRole",
        &AssignRoleRequest {
            agent_id: Some(agent_id.clone()),
            name: "stale-role-name".into(),
            role: "lead".into(),
        },
    )
    .await;

    assert!(error.message.contains("forced admin role failure"));
    assert_eq!(
        Agents::new(&state.store)
            .find_by_id(&agent_id.0)
            .await
            .unwrap()
            .unwrap()
            .role,
        before_agent.role
    );
    assert_eq!(
        Sessions::new(&state.store)
            .find_by_session_id(&registered.session_id)
            .await
            .unwrap()
            .unwrap()
            .role,
        before_session.role
    );
}

#[tokio::test]
async fn admin_assign_project_by_agent_id_survives_target_rename() {
    let state = state().await;
    let registered = register_agent_runtime(&state, "ember", "demo").await;
    let agent_id = registered.agent_id.as_ref().unwrap().clone();

    Agents::new(&state.store)
        .rename(&agent_id.0, "ember-renamed")
        .await
        .unwrap();
    Sessions::new(&state.store)
        .set_name(&registered.session_id, "ember-renamed")
        .await
        .unwrap();

    let assigned: AssignProjectResponse = call_ok(
        &state,
        caller("operator", "demo", Tier::Admin),
        "admin.assignProject",
        &AssignProjectRequest {
            agent_id: Some(agent_id.clone()),
            name: "ember".into(),
            project: "lens".into(),
        },
    )
    .await;
    assert_eq!(assigned.name.as_deref(), Some("ember-renamed"));
    assert_eq!(assigned.project, "lens");

    let session = Sessions::new(&state.store)
        .find_by_agent_id(&agent_id.0)
        .await
        .unwrap()
        .expect("renamed session remains selected by agent id");
    assert_eq!(session.name.as_deref(), Some("ember-renamed"));
    assert_eq!(session.project, "lens");
    let agent = Agents::new(&state.store)
        .find_by_id(&agent_id.0)
        .await
        .unwrap()
        .expect("durable row remains selected by agent id");
    assert_eq!(agent.name.as_deref(), Some("ember-renamed"));
    assert_eq!(agent.project, "lens");
}

#[tokio::test]
async fn split_admin_assign_project_by_id_compensates_when_durable_write_fails() {
    let (_directory, state, mut gateway) = split_state_with_gateway_stream().await;
    let registered = register_agent_runtime(&state, "ember", "demo").await;
    let agent_id = registered.agent_id.clone().unwrap();
    let _ = next_identity_projection(&mut gateway, &agent_id).await;
    while gateway.try_recv().is_ok() {}
    state
        .store
        .identity_conn()
        .execute(
            "CREATE TRIGGER fail_admin_durable_project_move \
             BEFORE UPDATE OF project ON agents WHEN NEW.project = 'lens' BEGIN \
             SELECT RAISE(ABORT, 'forced admin durable project failure'); END",
            (),
        )
        .await
        .unwrap();

    let error = call_err(
        &state,
        caller("operator", "caller-metadata", Tier::Admin),
        "admin.assignProject",
        &AssignProjectRequest {
            agent_id: Some(agent_id.clone()),
            name: "stale-label".into(),
            project: "lens".into(),
        },
    )
    .await;

    assert_eq!(error.code, codes::INTERNAL_ERROR);
    assert!(error
        .message
        .contains("forced admin durable project failure"));
    assert_eq!(
        Agents::new(&state.store)
            .find_by_id(&agent_id.0)
            .await
            .unwrap()
            .unwrap()
            .project,
        "demo"
    );
    assert_eq!(
        Sessions::new(&state.store)
            .find_by_session_id(&registered.session_id)
            .await
            .unwrap()
            .unwrap()
            .project,
        "demo",
        "transport metadata must be compensated before the error returns"
    );
    let published = tokio::time::timeout(Duration::from_millis(50), async {
        loop {
            let Ok(frame) = gateway.recv().await else {
                return false;
            };
            if matches!(
                frame,
                GatewayStreamFrame::Projection { ref event }
                    if event.kind == nexus_contracts::GatewayProjectionKind::IdentityUpserted
                        && event.payload["agentId"] == agent_id.0
            ) {
                return true;
            }
        }
    })
    .await
    .unwrap_or(false);
    assert!(
        !published,
        "a failed split move must not publish an identity projection"
    );
}

#[tokio::test]
async fn admin_role_and_project_fail_closed_on_runtime_session_owner_mismatch() {
    let state = state().await;
    let target = register_agent_runtime(&state, "target-owner", "demo").await;
    let other = register_agent_runtime(&state, "other-owner", "other-project").await;
    let target_id = target.agent_id.clone().unwrap();
    let other_id = other.agent_id.clone().unwrap();
    state
        .store
        .identity_conn()
        .execute(
            "DELETE FROM agent_runtimes WHERE runtime_id = ?1",
            libsql::params![other.session_id.0.clone()],
        )
        .await
        .unwrap();
    state
        .store
        .identity_conn()
        .execute(
            "UPDATE agent_runtimes SET runtime_id = ?2 WHERE agent_id = ?1 AND active = 1",
            libsql::params![target_id.0.clone(), other.session_id.0.clone()],
        )
        .await
        .unwrap();

    let role_error = call_err(
        &state,
        caller("operator", "caller-metadata", Tier::Admin),
        "admin.assignRole",
        &AssignRoleRequest {
            agent_id: Some(target_id.clone()),
            name: "stale-target-label".into(),
            role: "lead".into(),
        },
    )
    .await;
    let project_error = call_err(
        &state,
        caller("operator", "caller-metadata", Tier::Admin),
        "admin.assignProject",
        &AssignProjectRequest {
            agent_id: Some(target_id.clone()),
            name: "stale-target-label".into(),
            project: "lens".into(),
        },
    )
    .await;

    for error in [&role_error, &project_error] {
        assert_eq!(error.code, codes::INVALID_PARAMS);
        assert!(error.message.contains(&target_id.0));
        assert!(error.message.contains(&other_id.0));
    }
    let target_agent = Agents::new(&state.store)
        .find_by_id(&target_id.0)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(target_agent.role.as_deref(), Some("backend"));
    assert_eq!(target_agent.project, "demo");
    let other_session = Sessions::new(&state.store)
        .find_by_session_id(&other.session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(other_session.agent_id.as_deref(), Some(other_id.0.as_str()));
    assert_eq!(other_session.role.as_deref(), Some("backend"));
    assert_eq!(other_session.project, "other-project");
}

#[tokio::test]
async fn admin_remove_by_agent_id_survives_target_rename() {
    let state = state().await;
    let target = register_agent_runtime(&state, "target", "demo").await;
    let target_agent_id = target
        .agent_id
        .as_ref()
        .expect("target runtime should have durable agent id")
        .clone();

    Agents::new(&state.store)
        .rename(&target_agent_id.0, "target-renamed")
        .await
        .unwrap();
    Sessions::new(&state.store)
        .set_name(&target.session_id, "target-renamed")
        .await
        .unwrap();
    let imposter =
        register_agent_runtime_with_client_key(&state, "target", "demo", "ck-target-imposter")
            .await;
    let imposter_agent_id = imposter
        .agent_id
        .as_ref()
        .expect("imposter runtime should have durable agent id")
        .clone();

    let removed: RemoveResponse = call_ok(
        &state,
        caller("operator", "demo", Tier::Admin),
        "admin.remove",
        &RemoveRequest {
            agent_id: Some(target_agent_id.clone()),
            name: "target".into(),
            kill: false,
        },
    )
    .await;
    assert_eq!(removed.status, "removed");
    assert_eq!(removed.name.as_deref(), Some("target-renamed"));

    let selected = Sessions::new(&state.store)
        .find_by_agent_id(&target_agent_id.0)
        .await
        .unwrap()
        .expect("remove should retain the renamed target session row");
    assert_eq!(selected.session_id, target.session_id);
    assert_eq!(selected.name.as_deref(), Some("target-renamed"));
    assert_eq!(selected.presence.as_deref(), Some("offline"));

    let imposter_row = Sessions::new(&state.store)
        .find_by_session_id(&imposter.session_id)
        .await
        .unwrap()
        .expect("stale-name owner should not be removed");
    assert_eq!(imposter_row.session_id, imposter.session_id);
    assert_ne!(imposter_agent_id, target_agent_id);
    assert_eq!(imposter_row.name.as_deref(), Some("target"));
    assert_ne!(imposter_row.presence.as_deref(), Some("offline"));
}

async fn insert_thread_member(
    state: &AppState,
    thread_id: &str,
    thread_name: &str,
    member_name: &str,
    agent_id: &str,
) {
    state
        .store
        .conn
        .execute(
            "INSERT OR IGNORE INTO threads (thread_id, name, project, created_by, created_at) \
             VALUES (?1, ?2, 'metadata-project', 'operator', 1)",
            libsql::params![thread_id, thread_name],
        )
        .await
        .unwrap();
    state
        .store
        .conn
        .execute(
            "INSERT INTO thread_members (thread_id, session_name, agent_id, joined_at) \
             VALUES (?1, ?2, ?3, 1)",
            libsql::params![thread_id, member_name, agent_id],
        )
        .await
        .unwrap();
}

async fn thread_member_count(state: &AppState, thread_id: &str, agent_id: &str) -> i64 {
    let mut rows = state
        .store
        .conn
        .query(
            "SELECT COUNT(*) FROM thread_members WHERE thread_id = ?1 AND agent_id = ?2",
            libsql::params![thread_id, agent_id],
        )
        .await
        .unwrap();
    rows.next().await.unwrap().unwrap().get(0).unwrap()
}

#[tokio::test]
async fn admin_evict_by_agent_id_removes_only_that_identity_memberships() {
    let state = state().await;
    let target = register_agent_runtime(&state, "target", "target-project").await;
    let target_id = target.agent_id.unwrap();
    Agents::new(&state.store)
        .rename(&target_id.0, "target-renamed")
        .await
        .unwrap();
    Sessions::new(&state.store)
        .set_name(&target.session_id, "target-renamed")
        .await
        .unwrap();
    let alias = register_agent_runtime(&state, "stale-request-name", "alias-project").await;
    let alias_id = alias.agent_id.unwrap();
    insert_thread_member(
        &state,
        "t_evict_id",
        "evict-id",
        "target-old-label",
        &target_id.0,
    )
    .await;
    insert_thread_member(
        &state,
        "t_evict_id",
        "evict-id",
        "stale-request-name",
        &alias_id.0,
    )
    .await;

    let evicted: RemoveResponse = call_ok(
        &state,
        caller("operator", "stale-caller-project", Tier::Admin),
        "admin.evict",
        &RemoveRequest {
            agent_id: Some(target_id.clone()),
            name: "stale-request-name".into(),
            kill: false,
        },
    )
    .await;

    assert_eq!(evicted.name.as_deref(), Some("target-renamed"));
    assert_eq!(
        thread_member_count(&state, "t_evict_id", &target_id.0).await,
        0
    );
    assert_eq!(
        thread_member_count(&state, "t_evict_id", &alias_id.0).await,
        1
    );
}

#[tokio::test]
async fn admin_evict_missing_agent_id_does_not_fall_back_to_the_request_name() {
    let state = state().await;
    let survivor = register_agent_runtime(&state, "survivor", "metadata-project").await;
    let survivor_id = survivor.agent_id.unwrap();
    insert_thread_member(
        &state,
        "t_evict_missing",
        "evict-missing",
        "survivor",
        &survivor_id.0,
    )
    .await;

    let error = call_err(
        &state,
        caller("operator", "metadata-project", Tier::Admin),
        "admin.evict",
        &RemoveRequest {
            agent_id: Some(AgentId("a_missing_evict_target".into())),
            name: "survivor".into(),
            kill: false,
        },
    )
    .await;
    assert_eq!(error.code, codes::NOT_FOUND);
    assert_eq!(
        thread_member_count(&state, "t_evict_missing", &survivor_id.0).await,
        1
    );
}

#[tokio::test]
async fn admin_evict_rejects_an_ambiguous_global_alias_without_removing_memberships() {
    let state = state().await;
    state
        .store
        .conn
        .execute("DROP INDEX idx_agents_name_unique", ())
        .await
        .unwrap();
    for (agent_id, project, thread_id, created_at) in [
        (
            "a_evict_ambiguous_one",
            "one",
            "t_evict_ambiguous_one",
            1_i64,
        ),
        (
            "a_evict_ambiguous_two",
            "two",
            "t_evict_ambiguous_two",
            2_i64,
        ),
    ] {
        state
            .store
            .conn
            .execute(
                "INSERT INTO agents (agent_id, project, name, default_harness, tier, created_at) \
                 VALUES (?1, ?2, 'ambiguous-evict', 'claude', 'agent', ?3)",
                libsql::params![agent_id, project, created_at],
            )
            .await
            .unwrap();
        insert_thread_member(
            &state,
            thread_id,
            "evict-ambiguous",
            "ambiguous-evict",
            agent_id,
        )
        .await;
    }

    let error = call_err(
        &state,
        caller("operator", "unrelated-project", Tier::Admin),
        "admin.evict",
        &RemoveRequest {
            agent_id: None,
            name: "ambiguous-evict".into(),
            kill: false,
        },
    )
    .await;

    assert_eq!(error.code, codes::INVALID_PARAMS);
    for (agent_id, thread_id) in [
        ("a_evict_ambiguous_one", "t_evict_ambiguous_one"),
        ("a_evict_ambiguous_two", "t_evict_ambiguous_two"),
    ] {
        assert_eq!(thread_member_count(&state, thread_id, agent_id).await, 1);
    }
}

#[tokio::test]
async fn admin_delete_by_agent_id_deletes_renamed_target_not_stale_name_owner() {
    let state = state().await;
    let target = register_agent_runtime(&state, "target", "demo").await;
    let target_agent_id = target
        .agent_id
        .as_ref()
        .expect("target runtime should have durable agent id")
        .clone();

    Agents::new(&state.store)
        .rename(&target_agent_id.0, "target-renamed")
        .await
        .unwrap();
    Sessions::new(&state.store)
        .set_name(&target.session_id, "target-renamed")
        .await
        .unwrap();
    let imposter =
        register_agent_runtime_with_client_key(&state, "target", "demo", "ck-target-imposter")
            .await;
    let imposter_agent_id = imposter
        .agent_id
        .as_ref()
        .expect("imposter runtime should have durable agent id")
        .clone();

    let deleted: RemoveResponse = call_ok(
        &state,
        caller("operator", "demo", Tier::Admin),
        "admin.delete",
        &RemoveRequest {
            agent_id: Some(target_agent_id.clone()),
            name: "target".into(),
            kill: false,
        },
    )
    .await;
    assert_eq!(deleted.status, "deleted");
    assert_eq!(deleted.name.as_deref(), Some("target-renamed"));

    assert!(
        Sessions::new(&state.store)
            .find_by_agent_id(&target_agent_id.0)
            .await
            .unwrap()
            .is_none(),
        "delete should purge the renamed target session selected by id"
    );
    assert!(
        Agents::new(&state.store)
            .find_by_id(&target_agent_id.0)
            .await
            .unwrap()
            .is_none(),
        "delete should purge the renamed target durable identity selected by id"
    );

    let imposter_row = Sessions::new(&state.store)
        .find_by_session_id(&imposter.session_id)
        .await
        .unwrap()
        .expect("stale-name owner should not be deleted");
    assert_eq!(imposter_row.session_id, imposter.session_id);
    assert_ne!(imposter_agent_id, target_agent_id);
    assert_eq!(imposter_row.name.as_deref(), Some("target"));
    assert!(
        Agents::new(&state.store)
            .find_by_id(&imposter_agent_id.0)
            .await
            .unwrap()
            .is_some(),
        "stale-name owner durable identity should remain"
    );
}

#[tokio::test]
async fn admin_remove_and_delete_route_stable_ids_across_project_metadata() {
    let state = state().await;
    let remove_target = register_agent_runtime(&state, "remove-cross-project", "other").await;
    let delete_target = register_agent_runtime(&state, "delete-cross-project", "other").await;
    let remove_agent_id = remove_target.agent_id.expect("remove target agent id");
    let delete_agent_id = delete_target.agent_id.expect("delete target agent id");
    let admin = caller("operator", "stale-caller-project", Tier::Admin);

    let removed: RemoveResponse = call_ok(
        &state,
        admin.clone(),
        "admin.remove",
        &RemoveRequest {
            agent_id: Some(remove_agent_id.clone()),
            name: "stale-remove-name".into(),
            kill: false,
        },
    )
    .await;
    let deleted: RemoveResponse = call_ok(
        &state,
        admin,
        "admin.delete",
        &RemoveRequest {
            agent_id: Some(delete_agent_id.clone()),
            name: "stale-delete-name".into(),
            kill: false,
        },
    )
    .await;

    assert_eq!(removed.status, "removed");
    assert_eq!(removed.name.as_deref(), Some("remove-cross-project"));
    assert_eq!(deleted.status, "deleted");
    assert_eq!(deleted.name.as_deref(), Some("delete-cross-project"));
    assert!(
        Sessions::new(&state.store)
            .find_by_agent_id(&remove_agent_id.0)
            .await
            .unwrap()
            .is_some(),
        "remove retains the stable identity's compatibility session"
    );
    assert!(
        Sessions::new(&state.store)
            .find_by_agent_id(&delete_agent_id.0)
            .await
            .unwrap()
            .is_none(),
        "delete purges the session selected by stable id"
    );
}

#[tokio::test]
async fn agent_access_grant_revoke_and_co_owner_delegation() {
    let state = state().await;
    let created = create_agent(&state, "ember").await;
    Agents::new(&state.store)
        .set_owner_if_missing(
            &created.agent.agent_id.0,
            &AgentOwner {
                name: "owner".into(),
                project: "demo".into(),
                session_id: Some("s_owner".into()),
                agent_id: None,
            },
        )
        .await
        .unwrap();

    let granted: AgentAccessGrantResponse = call_ok(
        &state,
        caller("owner", "demo", Tier::Agent),
        "agent.grantAccess",
        &AgentAccessGrantRequest {
            agent_id: None,
            name: "ember".into(),
            principal_agent_id: None,
            principal: "alice".into(),
            project: None,
            role: AgentAccessRole::Viewer,
        },
    )
    .await;
    assert_eq!(granted.principal, "alice");
    assert_eq!(granted.role, AgentAccessRole::Viewer);

    let viewer_err = call_err(
        &state,
        caller("alice", "demo", Tier::Agent),
        "agent.grantAccess",
        &AgentAccessGrantRequest {
            agent_id: None,
            name: "ember".into(),
            principal_agent_id: None,
            principal: "blake".into(),
            project: None,
            role: AgentAccessRole::Viewer,
        },
    )
    .await;
    assert_eq!(viewer_err.code, codes::UNAUTHORIZED);

    let co_owner: AgentAccessGrantResponse = call_ok(
        &state,
        caller("operator", "demo", Tier::Admin),
        "agent.grantAccess",
        &AgentAccessGrantRequest {
            agent_id: None,
            name: "ember".into(),
            principal_agent_id: None,
            principal: "cara".into(),
            project: None,
            role: AgentAccessRole::CoOwner,
        },
    )
    .await;
    assert_eq!(co_owner.role, AgentAccessRole::CoOwner);

    let delegated: AgentAccessGrantResponse = call_ok(
        &state,
        caller("cara", "demo", Tier::Agent),
        "agent.grantAccess",
        &AgentAccessGrantRequest {
            agent_id: None,
            name: "ember".into(),
            principal_agent_id: None,
            principal: "drew".into(),
            project: None,
            role: AgentAccessRole::Viewer,
        },
    )
    .await;
    assert_eq!(delegated.principal, "drew");

    let revoked: AgentAccessRevokeResponse = call_ok(
        &state,
        caller("cara", "demo", Tier::Agent),
        "agent.revokeAccess",
        &AgentAccessRevokeRequest {
            agent_id: None,
            name: "ember".into(),
            principal_agent_id: None,
            principal: "drew".into(),
            project: None,
        },
    )
    .await;
    assert!(revoked.revoked);

    let revoked_again: AgentAccessRevokeResponse = call_ok(
        &state,
        caller("cara", "demo", Tier::Agent),
        "agent.revokeAccess",
        &AgentAccessRevokeRequest {
            agent_id: None,
            name: "ember".into(),
            principal_agent_id: None,
            principal: "drew".into(),
            project: None,
        },
    )
    .await;
    assert!(!revoked_again.revoked);

    let agent = nexus_store::repos::Agents::new(&state.store)
        .find_by_name("ember")
        .await
        .unwrap()
        .unwrap();
    let metadata = Metadata::new(&state.store)
        .get("demo", MetadataEntity::Agent, &agent.agent_id)
        .await
        .unwrap()
        .metadata;
    assert_eq!(
        metadata["nexusAclAudit"]["last"]["op"],
        serde_json::json!("revoke")
    );
    assert_eq!(
        metadata["nexusAclAudit"]["last"]["principal"],
        serde_json::json!("drew")
    );
}

#[tokio::test]
async fn agent_owner_authority_survives_owner_rename_by_agent_id() {
    let state = state().await;
    let target = create_agent(&state, "ember").await;
    let owner = register_agent_runtime(&state, "owner", "demo").await;
    let owner_agent_id = owner
        .agent_id
        .as_ref()
        .expect("owner runtime should have durable agent id")
        .0
        .clone();

    Agents::new(&state.store)
        .set_owner_if_missing(
            &target.agent.agent_id.0,
            &AgentOwner {
                name: "owner".into(),
                project: "demo".into(),
                session_id: Some(owner.session_id.0.clone()),
                agent_id: Some(owner_agent_id.clone()),
            },
        )
        .await
        .unwrap();
    Agents::new(&state.store)
        .rename(&owner_agent_id, "owner-renamed")
        .await
        .unwrap();
    Sessions::new(&state.store)
        .set_name(&owner.session_id, "owner-renamed")
        .await
        .unwrap();

    let renamed_owner = state
        .identity
        .resolve("demo", "owner-renamed")
        .await
        .unwrap();
    let granted: AgentAccessGrantResponse = call_ok(
        &state,
        renamed_owner,
        "agent.grantAccess",
        &AgentAccessGrantRequest {
            agent_id: None,
            name: "ember".into(),
            principal_agent_id: None,
            principal: "alice".into(),
            project: None,
            role: AgentAccessRole::Viewer,
        },
    )
    .await;

    assert_eq!(granted.principal, "alice");
    assert_eq!(granted.role, AgentAccessRole::Viewer);
}

#[tokio::test]
async fn agent_co_owner_authority_survives_principal_rename_by_agent_id() {
    let state = state().await;
    create_agent(&state, "ember").await;
    let cara = register_agent_runtime(&state, "cara", "demo").await;
    register_agent_runtime(&state, "drew", "demo").await;
    let cara_agent_id = cara
        .agent_id
        .as_ref()
        .expect("cara runtime should have durable agent id")
        .0
        .clone();

    let co_owner: AgentAccessGrantResponse = call_ok(
        &state,
        caller("operator", "demo", Tier::Admin),
        "agent.grantAccess",
        &AgentAccessGrantRequest {
            agent_id: None,
            name: "ember".into(),
            principal_agent_id: None,
            principal: "cara".into(),
            project: None,
            role: AgentAccessRole::CoOwner,
        },
    )
    .await;
    assert_eq!(co_owner.role, AgentAccessRole::CoOwner);

    Agents::new(&state.store)
        .rename(&cara_agent_id, "cara-renamed")
        .await
        .unwrap();
    Sessions::new(&state.store)
        .set_name(&cara.session_id, "cara-renamed")
        .await
        .unwrap();

    let renamed_cara = state
        .identity
        .resolve("demo", "cara-renamed")
        .await
        .unwrap();
    let delegated: AgentAccessGrantResponse = call_ok(
        &state,
        renamed_cara,
        "agent.grantAccess",
        &AgentAccessGrantRequest {
            agent_id: None,
            name: "ember".into(),
            principal_agent_id: None,
            principal: "drew".into(),
            project: None,
            role: AgentAccessRole::Viewer,
        },
    )
    .await;

    assert_eq!(delegated.principal, "drew");
    assert_eq!(delegated.role, AgentAccessRole::Viewer);
}

#[tokio::test]
async fn stable_co_owner_grant_never_falls_back_to_a_reused_principal_name() {
    let state = state().await;
    let target = create_agent(&state, "acl-target").await;
    let principal = register_agent_runtime(&state, "principal", "demo").await;
    let principal_id = principal.agent_id.clone().expect("principal id");
    register_agent_runtime(&state, "delegate", "demo").await;

    let _: AgentAccessGrantResponse = call_ok(
        &state,
        caller("operator", "demo", Tier::Admin),
        "agent.grantAccess",
        &AgentAccessGrantRequest {
            agent_id: Some(target.agent.agent_id.clone()),
            name: "stale-target".into(),
            principal_agent_id: Some(principal_id.clone()),
            principal: "principal".into(),
            project: None,
            role: AgentAccessRole::CoOwner,
        },
    )
    .await;
    Agents::new(&state.store)
        .rename(&principal_id.0, "principal-renamed")
        .await
        .unwrap();
    Sessions::new(&state.store)
        .set_name(&principal.session_id, "principal-renamed")
        .await
        .unwrap();
    let name_reuser = create_agent(&state, "principal").await;
    let name_reuser_caller = Caller {
        agent_id: Some(name_reuser.agent.agent_id.clone()),
        session: SessionId("s_name_reuser".into()),
        name: "principal".into(),
        project: "other-project".into(),
        tier: Tier::Agent,
        locality: Default::default(),
        access: None,
        principal_id: None,
    };
    assert_ne!(name_reuser_caller.agent_id, Some(principal_id));

    let error = call_err(
        &state,
        name_reuser_caller,
        "agent.grantAccess",
        &AgentAccessGrantRequest {
            agent_id: Some(target.agent.agent_id),
            name: "stale-target".into(),
            principal_agent_id: None,
            principal: "delegate".into(),
            project: None,
            role: AgentAccessRole::Viewer,
        },
    )
    .await;

    assert_eq!(error.code, codes::UNAUTHORIZED);
}

#[tokio::test]
async fn stable_owner_never_falls_back_to_an_idless_human_with_the_same_name() {
    let state = state().await;
    let target = create_agent(&state, "owned-target").await;
    let owner = register_agent_runtime(&state, "owner", "demo").await;
    let owner_id = owner.agent_id.clone().expect("owner id");
    register_agent_runtime(&state, "delegate", "demo").await;
    Agents::new(&state.store)
        .set_owner_if_missing(
            &target.agent.agent_id.0,
            &AgentOwner {
                name: "owner".into(),
                project: "demo".into(),
                session_id: Some(owner.session_id.0),
                agent_id: Some(owner_id.0),
            },
        )
        .await
        .unwrap();

    // Model a pre-fix/fossil duplicate label. The authenticated session kind/id, not the mutable
    // display label, must decide authority.
    state
        .store
        .conn
        .execute("DROP INDEX idx_sessions_name", ())
        .await
        .unwrap();
    let human_session = SessionId("s_idless_human_owner_reuser".into());
    Sessions::new(&state.store)
        .create(NewSession {
            session_id: human_session.clone(),
            name: Some("owner".into()),
            agent: Some("browser".into()),
            kind: "human".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: None,
            client_key: Some("ck_idless_human_owner_reuser".into()),
            cwd: None,
            project: "other-project".into(),
            transport: None,
        })
        .await
        .unwrap();

    let error = call_err(
        &state,
        Caller {
            agent_id: None,
            session: human_session,
            name: "owner".into(),
            project: "other-project".into(),
            tier: Tier::Agent,
            locality: Default::default(),
            access: None,
            principal_id: None,
        },
        "agent.grantAccess",
        &AgentAccessGrantRequest {
            agent_id: Some(target.agent.agent_id),
            name: "stale-target".into(),
            principal_agent_id: None,
            principal: "delegate".into(),
            project: None,
            role: AgentAccessRole::Viewer,
        },
    )
    .await;

    assert_eq!(error.code, codes::UNAUTHORIZED);
}

#[tokio::test]
async fn agent_access_revoke_survives_principal_rename_by_agent_id() {
    let state = state().await;
    let target = create_agent(&state, "ember").await;
    let principal_runtime = register_agent_runtime(&state, "principal", "demo").await;

    let _: AgentAccessGrantResponse = call_ok(
        &state,
        caller("operator", "demo", Tier::Admin),
        "agent.grantAccess",
        &AgentAccessGrantRequest {
            agent_id: None,
            name: "ember".into(),
            principal_agent_id: None,
            principal: "principal".into(),
            project: None,
            role: AgentAccessRole::Viewer,
        },
    )
    .await;

    let principal = state.identity.resolve("demo", "principal").await.unwrap();
    let principal_agent_id = principal
        .agent_id
        .as_ref()
        .expect("principal should resolve to durable agent id")
        .0
        .clone();

    Agents::new(&state.store)
        .rename(&principal_agent_id, "principal-renamed")
        .await
        .unwrap();
    Sessions::new(&state.store)
        .set_name(&principal_runtime.session_id, "principal-renamed")
        .await
        .unwrap();

    state
        .store
        .conn
        .execute(
            "INSERT INTO agent_acl_grants (agent_id, principal_project, principal_name, \
             principal_agent_id, role, granted_by_name, granted_by_project, created_at, \
             updated_at) VALUES (?1, 'demo', 'principal-renamed', NULL, 'viewer', \
             'operator', 'demo', 1, 1)",
            libsql::params![target.agent.agent_id.0.as_str()],
        )
        .await
        .unwrap();
    assert!(
        AgentAccessGrants::new(&state.store)
            .find_by_principal_agent_id(&target.agent.agent_id.0, &principal_agent_id)
            .await
            .unwrap()
            .is_some(),
        "test setup should include the id-keyed stale-label grant"
    );
    assert!(
        AgentAccessGrants::new(&state.store)
            .find(&target.agent.agent_id.0, "demo", "principal-renamed")
            .await
            .unwrap()
            .is_some(),
        "test setup should include the legacy current-label grant"
    );

    let revoked: AgentAccessRevokeResponse = call_ok(
        &state,
        caller("operator", "demo", Tier::Admin),
        "agent.revokeAccess",
        &AgentAccessRevokeRequest {
            agent_id: None,
            name: "ember".into(),
            principal_agent_id: None,
            principal: "principal-renamed".into(),
            project: None,
        },
    )
    .await;

    assert!(revoked.revoked);
    assert!(
        AgentAccessGrants::new(&state.store)
            .find_by_principal_agent_id(&target.agent.agent_id.0, &principal_agent_id)
            .await
            .unwrap()
            .is_none(),
        "renamed principal grant should be deleted by durable principal id"
    );
    assert!(
        AgentAccessGrants::new(&state.store)
            .find(&target.agent.agent_id.0, "demo", "principal-renamed")
            .await
            .unwrap()
            .is_none(),
        "requested legacy grant should be deleted by principal name"
    );
}

#[tokio::test]
async fn agent_access_request_ids_override_stale_names() {
    let state = state().await;
    let target = create_agent(&state, "ember").await;
    let alice = register_agent_runtime(&state, "alice", "demo").await;
    let alice_agent_id = alice
        .agent_id
        .as_ref()
        .expect("alice runtime should have durable agent id")
        .0
        .clone();

    let granted: AgentAccessGrantResponse = call_ok(
        &state,
        caller("operator", "demo", Tier::Admin),
        "agent.grantAccess",
        &AgentAccessGrantRequest {
            agent_id: Some(target.agent.agent_id.clone()),
            name: "stale-target".into(),
            principal_agent_id: Some(AgentId(alice_agent_id.clone())),
            principal: "stale-principal".into(),
            project: None,
            role: AgentAccessRole::Viewer,
        },
    )
    .await;

    assert_eq!(granted.name.as_deref(), Some("ember"));
    assert_eq!(granted.principal, "alice");

    let grants = AgentAccessGrants::new(&state.store);
    let grant = grants
        .find_by_principal_agent_id(&target.agent.agent_id.0, &alice_agent_id)
        .await
        .unwrap()
        .expect("grant should be keyed by the explicit principal agent id");
    assert_eq!(grant.principal_name, "alice");
    assert_eq!(
        grant.principal_agent_id.as_deref(),
        Some(alice_agent_id.as_str())
    );
    assert!(
        grants
            .find(&target.agent.agent_id.0, "demo", "stale-principal")
            .await
            .unwrap()
            .is_none(),
        "stale principal aliases must not be stored as a legacy name-keyed grant"
    );
}

#[tokio::test]
async fn agent_access_revoke_request_ids_delete_current_legacy_name() {
    let state = state().await;
    let target = create_agent(&state, "ember").await;
    let principal_runtime = register_agent_runtime(&state, "principal", "demo").await;
    let principal_agent_id = principal_runtime
        .agent_id
        .as_ref()
        .expect("principal runtime should have durable agent id")
        .0
        .clone();

    state
        .store
        .conn
        .execute(
            "INSERT INTO agent_acl_grants (agent_id, principal_project, principal_name, \
             principal_agent_id, role, granted_by_name, granted_by_project, created_at, \
             updated_at) VALUES (?1, 'demo', 'principal', NULL, 'viewer', \
             'operator', 'demo', 1, 1)",
            libsql::params![target.agent.agent_id.0.as_str()],
        )
        .await
        .unwrap();

    let grants = AgentAccessGrants::new(&state.store);
    assert!(
        grants
            .find(&target.agent.agent_id.0, "demo", "principal")
            .await
            .unwrap()
            .is_some(),
        "test setup should include the current-name legacy grant"
    );
    assert!(
        grants
            .find_by_principal_agent_id(&target.agent.agent_id.0, &principal_agent_id)
            .await
            .unwrap()
            .is_none(),
        "test setup should leave only a NULL-id legacy grant"
    );

    let revoked: AgentAccessRevokeResponse = call_ok(
        &state,
        caller("operator", "demo", Tier::Admin),
        "agent.revokeAccess",
        &AgentAccessRevokeRequest {
            agent_id: Some(target.agent.agent_id.clone()),
            name: "stale-target".into(),
            principal_agent_id: Some(AgentId(principal_agent_id.clone())),
            principal: "stale-principal".into(),
            project: None,
        },
    )
    .await;

    assert!(revoked.revoked);
    assert_eq!(revoked.name.as_deref(), Some("ember"));
    assert_eq!(revoked.principal, "principal");
    assert!(
        grants
            .find(&target.agent.agent_id.0, "demo", "principal")
            .await
            .unwrap()
            .is_none(),
        "explicit principal id revoke must delete the current-name legacy grant"
    );
}

#[tokio::test]
async fn agent_access_changes_republish_the_live_canonical_identity_snapshot() {
    let state = state().await;
    let target = register_agent_runtime(&state, "managed", "demo").await;
    let principal = register_agent_runtime(&state, "delegate", "demo").await;
    let target_agent_id = target.agent_id.clone().expect("managed agent id");
    let principal_agent_id = principal.agent_id.clone().expect("delegate agent id");
    let mut events = state.ws.subscribe();

    let _: AgentAccessGrantResponse = call_ok(
        &state,
        caller("operator", "demo", Tier::Admin),
        "agent.grantAccess",
        &AgentAccessGrantRequest {
            agent_id: Some(target_agent_id.clone()),
            name: "stale-managed".into(),
            principal_agent_id: Some(principal_agent_id),
            principal: "stale-delegate".into(),
            project: None,
            role: AgentAccessRole::Viewer,
        },
    )
    .await;

    let republished = tokio::time::timeout(Duration::from_millis(100), async {
        loop {
            let event = events.recv().await.unwrap();
            if matches!(
                event.params,
                Some(ref value)
                    if value["type"] == "agent.spawned"
                        && value["agentId"] == target_agent_id.0
                        && value["sessionId"] == target.session_id.0
            ) {
                break true;
            }
        }
    })
    .await
    .unwrap_or(false);
    assert!(republished, "grant should republish the live identity");
}

#[tokio::test]
async fn acl_and_owner_changes_republish_stopped_materialized_identity_descriptors() {
    let (state, mut projections) = state_with_gateway_stream().await;
    let target = register_agent_runtime(&state, "stopped-managed", "runtime-metadata").await;
    let principal = register_agent_runtime(&state, "offline-delegate", "principal-metadata").await;
    let target_agent_id = target.agent_id.clone().expect("target id");
    let principal_agent_id = principal.agent_id.clone().expect("principal id");
    AgentRuntimes::new(&state.store)
        .stop(&target.session_id.0)
        .await
        .unwrap();
    assert_eq!(
        Sessions::new(&state.store)
            .find_by_agent_id(&target_agent_id.0)
            .await
            .unwrap()
            .map(|row| row.session_id),
        Some(target.session_id.clone()),
        "a stopped runtime must retain its materialized compatibility session"
    );
    while projections.try_recv().is_ok() {}

    let _: AgentAccessGrantResponse = call_ok(
        &state,
        caller("operator", "caller-metadata", Tier::Admin),
        "agent.grantAccess",
        &AgentAccessGrantRequest {
            agent_id: Some(target_agent_id.clone()),
            name: "stale-target".into(),
            principal_agent_id: Some(principal_agent_id.clone()),
            principal: "stale-principal".into(),
            project: None,
            role: AgentAccessRole::Viewer,
        },
    )
    .await;
    let granted = next_identity_projection(&mut projections, &target_agent_id).await;
    assert_eq!(
        granted["accessGrants"][0]["principalAgentId"],
        principal_agent_id.0
    );

    let _: AgentAccessRevokeResponse = call_ok(
        &state,
        caller("operator", "caller-metadata", Tier::Admin),
        "agent.revokeAccess",
        &AgentAccessRevokeRequest {
            agent_id: Some(target_agent_id.clone()),
            name: "stale-target".into(),
            principal_agent_id: Some(principal_agent_id.clone()),
            principal: "stale-principal".into(),
            project: None,
        },
    )
    .await;
    let revoked = next_identity_projection(&mut projections, &target_agent_id).await;
    assert_eq!(revoked["accessGrants"], serde_json::json!([]));

    let _: AgentOwnerTransferResponse = call_ok(
        &state,
        caller("operator", "caller-metadata", Tier::Admin),
        "agent.transferOwner",
        &AgentOwnerTransferRequest {
            agent_id: Some(target_agent_id.clone()),
            name: "stale-target".into(),
            owner_agent_id: Some(principal_agent_id.clone()),
            owner: "stale-principal".into(),
            project: None,
        },
    )
    .await;
    let transferred = next_identity_projection(&mut projections, &target_agent_id).await;
    assert_eq!(transferred["ownerAgentId"], principal_agent_id.0);
    assert_eq!(transferred["name"], "stopped-managed");
}

#[tokio::test]
async fn agent_owner_transfer_request_ids_override_stale_names() {
    let state = state().await;
    let target = create_agent(&state, "ember").await;
    let new_owner = register_agent_runtime(&state, "new-owner", "demo").await;
    let new_owner_id = new_owner
        .agent_id
        .as_ref()
        .expect("new owner runtime should have durable agent id")
        .clone();

    let transferred: AgentOwnerTransferResponse = call_ok(
        &state,
        caller("operator", "demo", Tier::Admin),
        "agent.transferOwner",
        &AgentOwnerTransferRequest {
            agent_id: Some(target.agent.agent_id.clone()),
            name: "stale-target".into(),
            owner_agent_id: Some(new_owner_id.clone()),
            owner: "stale-owner".into(),
            project: None,
        },
    )
    .await;

    assert_eq!(transferred.name.as_deref(), Some("ember"));
    assert_eq!(transferred.owner, "new-owner");

    let stored = Agents::new(&state.store)
        .find_by_id(&target.agent.agent_id.0)
        .await
        .unwrap()
        .expect("target row should still exist after transfer");
    assert_eq!(
        stored.owner_agent_id.as_deref(),
        Some(new_owner_id.0.as_str())
    );
    assert_eq!(stored.owner_name.as_deref(), Some("new-owner"));
}

#[tokio::test]
async fn admin_delete_cascades_acl_grants_for_deleted_principal_and_grantor() {
    let state = state().await;
    let target = create_agent(&state, "ember").await;
    register_agent_runtime(&state, "principal", "demo").await;
    register_agent_runtime(&state, "grantor", "demo").await;
    register_agent_runtime(&state, "viewer", "demo").await;
    Agents::new(&state.store)
        .set_owner_if_missing(
            &target.agent.agent_id.0,
            &AgentOwner {
                name: "owner".into(),
                project: "demo".into(),
                session_id: Some("s_owner".into()),
                agent_id: None,
            },
        )
        .await
        .unwrap();

    let _: AgentAccessGrantResponse = call_ok(
        &state,
        caller("owner", "demo", Tier::Agent),
        "agent.grantAccess",
        &AgentAccessGrantRequest {
            agent_id: None,
            name: "ember".into(),
            principal_agent_id: None,
            principal: "principal".into(),
            project: None,
            role: AgentAccessRole::Viewer,
        },
    )
    .await;
    let _: AgentAccessGrantResponse = call_ok(
        &state,
        caller("owner", "demo", Tier::Agent),
        "agent.grantAccess",
        &AgentAccessGrantRequest {
            agent_id: None,
            name: "ember".into(),
            principal_agent_id: None,
            principal: "grantor".into(),
            project: None,
            role: AgentAccessRole::CoOwner,
        },
    )
    .await;
    let _: AgentAccessGrantResponse = call_ok(
        &state,
        caller("grantor", "demo", Tier::Agent),
        "agent.grantAccess",
        &AgentAccessGrantRequest {
            agent_id: None,
            name: "ember".into(),
            principal_agent_id: None,
            principal: "viewer".into(),
            project: None,
            role: AgentAccessRole::Viewer,
        },
    )
    .await;
    assert_eq!(acl_rows_named(&state, "principal").await, 1);
    assert_eq!(acl_rows_named(&state, "grantor").await, 2);

    let deleted_principal: RemoveResponse = call_ok(
        &state,
        caller("operator", "demo", Tier::Admin),
        "admin.delete",
        &RemoveRequest {
            agent_id: None,
            name: "principal".into(),
            kill: false,
        },
    )
    .await;
    assert_eq!(deleted_principal.status, "deleted");
    assert_eq!(
        acl_rows_named(&state, "principal").await,
        0,
        "admin.delete must purge grants where the deleted agent is the principal"
    );

    let deleted_grantor: RemoveResponse = call_ok(
        &state,
        caller("operator", "demo", Tier::Admin),
        "admin.delete",
        &RemoveRequest {
            agent_id: None,
            name: "grantor".into(),
            kill: false,
        },
    )
    .await;
    assert_eq!(deleted_grantor.status, "deleted");
    assert_eq!(
        acl_rows_named(&state, "grantor").await,
        0,
        "admin.delete must purge grants where the deleted agent is principal or grantor"
    );
}

#[tokio::test]
async fn admin_remove_cascades_acl_grants_for_removed_principal() {
    let state = state().await;
    let target = create_agent(&state, "ember").await;
    register_agent_runtime(&state, "principal", "demo").await;
    Agents::new(&state.store)
        .set_owner_if_missing(
            &target.agent.agent_id.0,
            &AgentOwner {
                name: "owner".into(),
                project: "demo".into(),
                session_id: Some("s_owner".into()),
                agent_id: None,
            },
        )
        .await
        .unwrap();

    let _: AgentAccessGrantResponse = call_ok(
        &state,
        caller("owner", "demo", Tier::Agent),
        "agent.grantAccess",
        &AgentAccessGrantRequest {
            agent_id: None,
            name: "ember".into(),
            principal_agent_id: None,
            principal: "principal".into(),
            project: None,
            role: AgentAccessRole::Viewer,
        },
    )
    .await;
    assert_eq!(acl_rows_named(&state, "principal").await, 1);

    let removed: RemoveResponse = call_ok(
        &state,
        caller("operator", "demo", Tier::Admin),
        "admin.remove",
        &RemoveRequest {
            agent_id: None,
            name: "principal".into(),
            kill: false,
        },
    )
    .await;
    assert_eq!(removed.status, "removed");
    assert_eq!(
        acl_rows_named(&state, "principal").await,
        0,
        "admin.remove must purge grants where the removed agent is the principal"
    );
}

#[tokio::test]
async fn agent_access_agent_admin_override_respects_protected_targets() {
    let state = state().await;
    register_agent_runtime(&state, "agent-admin", "demo").await;
    create_agent(&state, "ordinary").await;
    register_agent_runtime(&state, "admin-target", "demo").await;
    register_agent_runtime(&state, "lead-target", "demo").await;

    let _: GrantTierResponse = call_ok(
        &state,
        caller("operator", "demo", Tier::Admin),
        "admin.grantTier",
        &GrantTierRequest {
            agent_id: None,
            name: "agent-admin".into(),
            tier: Tier::Admin,
        },
    )
    .await;
    let _: GrantTierResponse = call_ok(
        &state,
        caller("operator", "demo", Tier::Admin),
        "admin.grantTier",
        &GrantTierRequest {
            agent_id: None,
            name: "admin-target".into(),
            tier: Tier::Admin,
        },
    )
    .await;
    let _: AssignRoleResponse = call_ok(
        &state,
        caller("operator", "demo", Tier::Admin),
        "admin.assignRole",
        &AssignRoleRequest {
            agent_id: None,
            name: "lead-target".into(),
            role: "lead".into(),
        },
    )
    .await;

    let agent_admin = state.identity.resolve("demo", "agent-admin").await.unwrap();
    assert_eq!(agent_admin.tier, Tier::Admin);

    let ordinary: AgentAccessGrantResponse = call_ok(
        &state,
        agent_admin.clone(),
        "agent.grantAccess",
        &AgentAccessGrantRequest {
            agent_id: None,
            name: "ordinary".into(),
            principal_agent_id: None,
            principal: "alice".into(),
            project: None,
            role: AgentAccessRole::Viewer,
        },
    )
    .await;
    assert_eq!(ordinary.principal, "alice");

    let admin_target = call_err(
        &state,
        agent_admin.clone(),
        "agent.grantAccess",
        &AgentAccessGrantRequest {
            agent_id: None,
            name: "admin-target".into(),
            principal_agent_id: None,
            principal: "alice".into(),
            project: None,
            role: AgentAccessRole::Viewer,
        },
    )
    .await;
    assert_eq!(admin_target.code, codes::UNAUTHORIZED);

    let lead_target = call_err(
        &state,
        agent_admin,
        "agent.grantAccess",
        &AgentAccessGrantRequest {
            agent_id: None,
            name: "lead-target".into(),
            principal_agent_id: None,
            principal: "alice".into(),
            project: None,
            role: AgentAccessRole::Viewer,
        },
    )
    .await;
    assert_eq!(lead_target.code, codes::UNAUTHORIZED);
}

#[tokio::test]
async fn agent_owner_transfer_moves_authority_without_granting_old_owner() {
    let state = state().await;
    let created = create_agent(&state, "ember").await;
    create_agent(&state, "new-owner").await;
    Agents::new(&state.store)
        .set_owner_if_missing(
            &created.agent.agent_id.0,
            &AgentOwner {
                name: "owner".into(),
                project: "demo".into(),
                session_id: Some("s_owner".into()),
                agent_id: None,
            },
        )
        .await
        .unwrap();

    let transferred: AgentOwnerTransferResponse = call_ok(
        &state,
        caller("owner", "stale-owner-project-metadata", Tier::Agent),
        "agent.transferOwner",
        &AgentOwnerTransferRequest {
            agent_id: None,
            name: "ember".into(),
            owner_agent_id: None,
            owner: "new-owner".into(),
            project: None,
        },
    )
    .await;
    assert_eq!(transferred.previous_owner.as_deref(), Some("owner"));
    assert_eq!(transferred.owner, "new-owner");

    let agent_after_transfer = nexus_store::repos::Agents::new(&state.store)
        .find_by_name("ember")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        agent_after_transfer.owner_name.as_deref(),
        Some("new-owner")
    );
    let metadata = Metadata::new(&state.store)
        .get(
            "demo",
            MetadataEntity::Agent,
            &agent_after_transfer.agent_id,
        )
        .await
        .unwrap()
        .metadata;
    assert_eq!(
        metadata["nexusAclAudit"]["last"]["op"],
        serde_json::json!("transfer")
    );
    assert_eq!(
        metadata["nexusAclAudit"]["last"]["owner"],
        serde_json::json!("new-owner")
    );

    let old_owner = call_err(
        &state,
        caller("owner", "demo", Tier::Agent),
        "agent.grantAccess",
        &AgentAccessGrantRequest {
            agent_id: None,
            name: "ember".into(),
            principal_agent_id: None,
            principal: "alice".into(),
            project: None,
            role: AgentAccessRole::Viewer,
        },
    )
    .await;
    assert_eq!(old_owner.code, codes::UNAUTHORIZED);

    let granted: AgentAccessGrantResponse = call_ok(
        &state,
        caller("new-owner", "demo", Tier::Agent),
        "agent.grantAccess",
        &AgentAccessGrantRequest {
            agent_id: None,
            name: "ember".into(),
            principal_agent_id: None,
            principal: "cara".into(),
            project: None,
            role: AgentAccessRole::CoOwner,
        },
    )
    .await;
    assert_eq!(granted.principal, "cara");
    assert_eq!(granted.role, AgentAccessRole::CoOwner);

    create_agent(&state, "third-owner").await;
    let co_owner_transfer = call_err(
        &state,
        caller("cara", "demo", Tier::Agent),
        "agent.transferOwner",
        &AgentOwnerTransferRequest {
            agent_id: None,
            name: "ember".into(),
            owner_agent_id: None,
            owner: "third-owner".into(),
            project: None,
        },
    )
    .await;
    assert_eq!(co_owner_transfer.code, codes::UNAUTHORIZED);

    let agent = nexus_store::repos::Agents::new(&state.store)
        .find_by_name("ember")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(agent.owner_name.as_deref(), Some("new-owner"));
}

#[tokio::test]
async fn legacy_co_owner_names_authorize_globally_without_project_authority() {
    let state = state().await;
    let target = create_agent(&state, "legacy-co-owned").await;
    Agents::new(&state.store)
        .set_owner_if_missing(
            &target.agent.agent_id.0,
            &AgentOwner {
                name: "owner".into(),
                project: "owner-metadata".into(),
                session_id: None,
                agent_id: None,
            },
        )
        .await
        .unwrap();
    AgentAccessGrants::new(&state.store)
        .grant(nexus_store::repos::NewAgentAccessGrant {
            agent_id: target.agent.agent_id.0.clone(),
            principal_project: "old-project-metadata".into(),
            principal_name: "legacy-co-owner".into(),
            principal_session_id: None,
            principal_agent_id: None,
            role: nexus_store::repos::ROLE_CO_OWNER.into(),
            granted_by_name: "owner".into(),
            granted_by_project: "owner-metadata".into(),
        })
        .await
        .unwrap();

    let granted: AgentAccessGrantResponse = call_ok(
        &state,
        caller("legacy-co-owner", "new-project-metadata", Tier::Agent),
        "agent.grantAccess",
        &AgentAccessGrantRequest {
            agent_id: Some(target.agent.agent_id.clone()),
            name: "stale-target".into(),
            principal_agent_id: None,
            principal: "viewer".into(),
            project: Some("presentation-only".into()),
            role: AgentAccessRole::Viewer,
        },
    )
    .await;
    assert_eq!(granted.principal, "viewer");
}

#[tokio::test]
async fn acl_name_principals_resolve_globally_and_snapshot_their_stable_id() {
    let state = state().await;
    let target = create_agent(&state, "managed-cross-project").await;
    let principal =
        register_agent_runtime(&state, "delegate-cross-project", "principal-home").await;
    let principal_agent_id = principal.agent_id.expect("principal id");

    let granted: AgentAccessGrantResponse = call_ok(
        &state,
        caller("operator", "caller-metadata", Tier::Admin),
        "agent.grantAccess",
        &AgentAccessGrantRequest {
            agent_id: Some(target.agent.agent_id.clone()),
            name: "stale-target".into(),
            principal_agent_id: None,
            principal: "delegate-cross-project".into(),
            project: Some("stale-principal-project".into()),
            role: AgentAccessRole::Viewer,
        },
    )
    .await;

    assert_eq!(granted.principal, "delegate-cross-project");
    assert_eq!(granted.project, "principal-home");
    let row = AgentAccessGrants::new(&state.store)
        .find_by_principal_agent_id(&target.agent.agent_id.0, &principal_agent_id.0)
        .await
        .unwrap()
        .expect("name-only principal should be canonicalized to its stable id");
    assert_eq!(row.principal_project, "principal-home");
}

#[tokio::test]
async fn agent_owner_transfer_admin_override_respects_protected_targets() {
    let state = state().await;
    register_agent_runtime(&state, "agent-admin", "demo").await;
    create_agent(&state, "ordinary").await;
    create_agent(&state, "new-owner").await;
    register_agent_runtime(&state, "admin-target", "demo").await;
    register_agent_runtime(&state, "lead-target", "demo").await;

    let _: GrantTierResponse = call_ok(
        &state,
        caller("operator", "demo", Tier::Admin),
        "admin.grantTier",
        &GrantTierRequest {
            agent_id: None,
            name: "agent-admin".into(),
            tier: Tier::Admin,
        },
    )
    .await;
    let _: GrantTierResponse = call_ok(
        &state,
        caller("operator", "demo", Tier::Admin),
        "admin.grantTier",
        &GrantTierRequest {
            agent_id: None,
            name: "admin-target".into(),
            tier: Tier::Admin,
        },
    )
    .await;
    let _: AssignRoleResponse = call_ok(
        &state,
        caller("operator", "demo", Tier::Admin),
        "admin.assignRole",
        &AssignRoleRequest {
            agent_id: None,
            name: "lead-target".into(),
            role: "lead".into(),
        },
    )
    .await;

    let agent_admin = state.identity.resolve("demo", "agent-admin").await.unwrap();
    assert_eq!(agent_admin.tier, Tier::Admin);

    let transferred: AgentOwnerTransferResponse = call_ok(
        &state,
        agent_admin.clone(),
        "agent.transferOwner",
        &AgentOwnerTransferRequest {
            agent_id: None,
            name: "ordinary".into(),
            owner_agent_id: None,
            owner: "new-owner".into(),
            project: None,
        },
    )
    .await;
    assert_eq!(transferred.owner, "new-owner");

    let admin_target = call_err(
        &state,
        agent_admin.clone(),
        "agent.transferOwner",
        &AgentOwnerTransferRequest {
            agent_id: None,
            name: "admin-target".into(),
            owner_agent_id: None,
            owner: "new-owner".into(),
            project: None,
        },
    )
    .await;
    assert_eq!(admin_target.code, codes::UNAUTHORIZED);

    let lead_target = call_err(
        &state,
        agent_admin,
        "agent.transferOwner",
        &AgentOwnerTransferRequest {
            agent_id: None,
            name: "lead-target".into(),
            owner_agent_id: None,
            owner: "new-owner".into(),
            project: None,
        },
    )
    .await;
    assert_eq!(lead_target.code, codes::UNAUTHORIZED);
}

#[tokio::test]
async fn human_admin_grants_agent_admin_tier_on_stable_identity_and_runtime() {
    let state = state().await;
    register_agent_runtime(&state, "ember", "demo").await;

    let granted: GrantTierResponse = call_ok(
        &state,
        caller("operator", "demo", Tier::Admin),
        "admin.grantTier",
        &GrantTierRequest {
            agent_id: None,
            name: "ember".into(),
            tier: Tier::Admin,
        },
    )
    .await;
    assert_eq!(granted.name.as_deref(), Some("ember"));
    assert_eq!(granted.tier, Tier::Admin);

    let shown: AgentShowResponse = call_ok(
        &state,
        caller("reader", "demo", Tier::Agent),
        "agent.show",
        &AgentShowRequest {
            agent_id: None,
            name: Some("ember".into()),
        },
    )
    .await;
    assert_eq!(shown.agent.tier.as_deref(), Some("admin"));

    let resolved = state.identity.resolve("demo", "ember").await.unwrap();
    assert_eq!(resolved.tier, Tier::Admin);
}

#[tokio::test]
async fn local_operator_grants_tier_to_agent_outside_default_project() {
    let state = state().await;
    register_agent_runtime(&state, "ember-cross-project", "release-project").await;

    let granted: GrantTierResponse = call_ok(
        &state,
        caller("operator", "default", Tier::Admin),
        "admin.grantTier",
        &GrantTierRequest {
            agent_id: None,
            name: "ember-cross-project".into(),
            tier: Tier::Admin,
        },
    )
    .await;

    assert_eq!(granted.name.as_deref(), Some("ember-cross-project"));
    assert_eq!(granted.tier, Tier::Admin);
    let resolved = state
        .identity
        .resolve("release-project", "ember-cross-project")
        .await
        .unwrap();
    assert_eq!(resolved.tier, Tier::Admin);
}

#[tokio::test]
async fn agent_cannot_self_escalate_or_grant_admin_to_another_agent() {
    let state = state().await;
    register_agent_runtime(&state, "ember", "demo").await;
    register_agent_runtime(&state, "nova", "demo").await;
    let ember = state.identity.resolve("demo", "ember").await.unwrap();

    let self_escalation = call_err(
        &state,
        ember.clone(),
        "admin.grantTier",
        &GrantTierRequest {
            agent_id: None,
            name: "ember".into(),
            tier: Tier::Admin,
        },
    )
    .await;
    assert_eq!(self_escalation.code, codes::UNAUTHORIZED);

    let other_escalation = call_err(
        &state,
        ember,
        "admin.grantTier",
        &GrantTierRequest {
            agent_id: None,
            name: "nova".into(),
            tier: Tier::Admin,
        },
    )
    .await;
    assert_eq!(other_escalation.code, codes::UNAUTHORIZED);

    let nova = state.identity.resolve("demo", "nova").await.unwrap();
    assert_eq!(nova.tier, Tier::Agent);
}

#[tokio::test]
async fn agent_admin_can_remove_agents_but_not_root_human_admin() {
    let state = state().await;
    register_agent_runtime(&state, "ember", "demo").await;
    register_agent_runtime(&state, "target", "demo").await;
    register_human_runtime(&state, "root-human", "demo", Tier::Admin).await;

    let _: GrantTierResponse = call_ok(
        &state,
        caller("operator", "demo", Tier::Admin),
        "admin.grantTier",
        &GrantTierRequest {
            agent_id: None,
            name: "ember".into(),
            tier: Tier::Admin,
        },
    )
    .await;
    let ember_admin = state.identity.resolve("demo", "ember").await.unwrap();
    assert_eq!(ember_admin.tier, Tier::Admin);

    let cannot_mint_admin = call_err(
        &state,
        ember_admin.clone(),
        "admin.grantTier",
        &GrantTierRequest {
            agent_id: None,
            name: "target".into(),
            tier: Tier::Admin,
        },
    )
    .await;
    assert_eq!(cannot_mint_admin.code, codes::UNAUTHORIZED);
    assert_eq!(
        state.identity.resolve("demo", "target").await.unwrap().tier,
        Tier::Agent
    );

    let removed: RemoveResponse = call_ok(
        &state,
        ember_admin.clone(),
        "admin.remove",
        &RemoveRequest {
            agent_id: None,
            name: "target".into(),
            kill: false,
        },
    )
    .await;
    assert_eq!(removed.status, "removed");
    let visible_members: MemberListResponse = call_ok(
        &state,
        ember_admin.clone(),
        "members",
        &MemberListRequest {
            project: None,
            include_offline: None,
            include_dead: None,
        },
    )
    .await;
    assert!(
        !visible_members
            .members
            .iter()
            .any(|member| member.name.as_deref() == Some("target")),
        "admin.remove must update presence so the removed agent leaves the live roster immediately"
    );
    let all_members: MemberListResponse = call_ok(
        &state,
        ember_admin.clone(),
        "members",
        &MemberListRequest {
            project: None,
            include_offline: Some(true),
            include_dead: None,
        },
    )
    .await;
    let removed_member = all_members
        .members
        .iter()
        .find(|member| member.name.as_deref() == Some("target"))
        .expect("remove retains the compatibility session row");
    assert_eq!(removed_member.presence, nexus_contracts::Presence::Offline);

    let protected = call_err(
        &state,
        ember_admin,
        "admin.remove",
        &RemoveRequest {
            agent_id: None,
            name: "root-human".into(),
            kill: false,
        },
    )
    .await;
    assert_eq!(protected.code, codes::UNAUTHORIZED);
}

#[tokio::test]
async fn agent_admin_cannot_remove_admin_or_lead_targets() {
    let state = state().await;
    register_agent_runtime(&state, "ember", "demo").await;
    register_agent_runtime(&state, "admin-target", "demo").await;
    register_agent_runtime(&state, "lead-target", "demo").await;

    let _: GrantTierResponse = call_ok(
        &state,
        caller("operator", "demo", Tier::Admin),
        "admin.grantTier",
        &GrantTierRequest {
            agent_id: None,
            name: "ember".into(),
            tier: Tier::Admin,
        },
    )
    .await;
    let _: GrantTierResponse = call_ok(
        &state,
        caller("operator", "demo", Tier::Admin),
        "admin.grantTier",
        &GrantTierRequest {
            agent_id: None,
            name: "admin-target".into(),
            tier: Tier::Admin,
        },
    )
    .await;
    let _: AssignRoleResponse = call_ok(
        &state,
        caller("operator", "demo", Tier::Admin),
        "admin.assignRole",
        &AssignRoleRequest {
            agent_id: None,
            name: "lead-target".into(),
            role: "lead".into(),
        },
    )
    .await;
    let ember_admin = state.identity.resolve("demo", "ember").await.unwrap();

    let admin_target = call_err(
        &state,
        ember_admin.clone(),
        "admin.remove",
        &RemoveRequest {
            agent_id: None,
            name: "admin-target".into(),
            kill: false,
        },
    )
    .await;
    assert_eq!(admin_target.code, codes::UNAUTHORIZED);

    let lead_target = call_err(
        &state,
        ember_admin,
        "admin.remove",
        &RemoveRequest {
            agent_id: None,
            name: "lead-target".into(),
            kill: false,
        },
    )
    .await;
    assert_eq!(lead_target.code, codes::UNAUTHORIZED);

    let removed_admin: RemoveResponse = call_ok(
        &state,
        caller("operator", "demo", Tier::Admin),
        "admin.remove",
        &RemoveRequest {
            agent_id: None,
            name: "admin-target".into(),
            kill: false,
        },
    )
    .await;
    assert_eq!(removed_admin.status, "removed");

    let removed_lead: RemoveResponse = call_ok(
        &state,
        caller("operator", "demo", Tier::Admin),
        "admin.remove",
        &RemoveRequest {
            agent_id: None,
            name: "lead-target".into(),
            kill: false,
        },
    )
    .await;
    assert_eq!(removed_lead.status, "removed");
}

#[tokio::test]
async fn agent_admin_cannot_delete_admin_or_lead_targets() {
    let state = state().await;
    register_agent_runtime(&state, "ember", "demo").await;
    register_agent_runtime(&state, "admin-target", "demo").await;
    register_agent_runtime(&state, "lead-target", "demo").await;

    let _: GrantTierResponse = call_ok(
        &state,
        caller("operator", "demo", Tier::Admin),
        "admin.grantTier",
        &GrantTierRequest {
            agent_id: None,
            name: "ember".into(),
            tier: Tier::Admin,
        },
    )
    .await;
    let _: GrantTierResponse = call_ok(
        &state,
        caller("operator", "demo", Tier::Admin),
        "admin.grantTier",
        &GrantTierRequest {
            agent_id: None,
            name: "admin-target".into(),
            tier: Tier::Admin,
        },
    )
    .await;
    let _: AssignRoleResponse = call_ok(
        &state,
        caller("operator", "demo", Tier::Admin),
        "admin.assignRole",
        &AssignRoleRequest {
            agent_id: None,
            name: "lead-target".into(),
            role: "lead".into(),
        },
    )
    .await;
    let ember_admin = state.identity.resolve("demo", "ember").await.unwrap();

    let admin_target = call_err(
        &state,
        ember_admin.clone(),
        "admin.delete",
        &RemoveRequest {
            agent_id: None,
            name: "admin-target".into(),
            kill: false,
        },
    )
    .await;
    assert_eq!(admin_target.code, codes::UNAUTHORIZED);

    let lead_target = call_err(
        &state,
        ember_admin,
        "admin.delete",
        &RemoveRequest {
            agent_id: None,
            name: "lead-target".into(),
            kill: false,
        },
    )
    .await;
    assert_eq!(lead_target.code, codes::UNAUTHORIZED);

    let deleted_admin: RemoveResponse = call_ok(
        &state,
        caller("operator", "demo", Tier::Admin),
        "admin.delete",
        &RemoveRequest {
            agent_id: None,
            name: "admin-target".into(),
            kill: false,
        },
    )
    .await;
    assert_eq!(deleted_admin.status, "deleted");

    let deleted_lead: RemoveResponse = call_ok(
        &state,
        caller("operator", "demo", Tier::Admin),
        "admin.delete",
        &RemoveRequest {
            agent_id: None,
            name: "lead-target".into(),
            kill: false,
        },
    )
    .await;
    assert_eq!(deleted_lead.status, "deleted");
}

#[tokio::test]
async fn credential_create_hashes_secret_and_revoke_removes_active_credential() {
    let state = state().await;
    let created = create_agent(&state, "ember").await;

    let credential: AgentCredentialCreateResponse = call_ok(
        &state,
        caller("operator", "demo", Tier::Admin),
        "agent.credential.create",
        &AgentCredentialCreateRequest {
            agent_id: Some(created.agent.agent_id.clone()),
            name: None,
            label: Some("local".into()),
            purpose: Some("runtime".into()),
            scopes: vec!["runtime:register".into()],
        },
    )
    .await;

    assert!(credential.credential_id.0.starts_with("cred_"));
    assert!(credential.secret.starts_with("nexus_rt_"));
    assert_eq!(credential.agent_id, created.agent.agent_id);

    let active = AgentCredentials::new(&state.store)
        .find_active(&created.agent.agent_id.0)
        .await
        .unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(
        active[0].secret_hash,
        hash_runtime_credential(&credential.secret)
    );
    assert_ne!(active[0].secret_hash, credential.secret);

    let revoked: AgentCredentialRevokeResponse = call_ok(
        &state,
        caller("operator", "demo", Tier::Admin),
        "agent.credential.revoke",
        &AgentCredentialRevokeRequest {
            credential_id: credential.credential_id.clone(),
        },
    )
    .await;
    assert!(revoked.revoked);
    assert_eq!(revoked.credential_id, credential.credential_id);
    assert!(AgentCredentials::new(&state.store)
        .find_active(&created.agent.agent_id.0)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn runtime_list_returns_registered_runtime_for_agent() {
    let state = state().await;
    let created = create_agent(&state, "ember").await;
    let credential: AgentCredentialCreateResponse = call_ok(
        &state,
        caller("operator", "demo", Tier::Admin),
        "agent.credential.create",
        &AgentCredentialCreateRequest {
            agent_id: Some(created.agent.agent_id.clone()),
            name: None,
            label: None,
            purpose: Some("runtime".into()),
            scopes: vec!["runtime:register".into()],
        },
    )
    .await;

    let register = RegisterRequest {
        name: Some("ember".into()),
        agent_id: Some(created.agent.agent_id.clone()),
        harness: hid("codex"),
        harness_session_id: "codex-session".into(),
        project: "demo".into(),
        client_key: "ck-ember-runtime".into(),
        runtime_credential: Some(credential.secret),
        tier: Tier::Agent,
        kind: Some(Kind::Agent),
        locality: Default::default(),
        access: None,
        role: Some("backend".into()),
        cwd: Some("/repo".into()),
    };
    let registered = dispatch(&state, None, req("register", &register)).await;
    assert!(
        registered.error.is_none(),
        "unexpected register error: {:?}",
        registered.error
    );
    let registered: RegisterResponse = serde_json::from_value(registered.result.unwrap()).unwrap();
    assert_eq!(registered.agent_id, Some(created.agent.agent_id.clone()));

    let runtimes: AgentRuntimeListResponse = call_ok(
        &state,
        caller("reader", "demo", Tier::Agent),
        "agent.runtime.list",
        &AgentRuntimeListRequest {
            agent_id: Some(created.agent.agent_id.clone()),
            name: None,
            include_stopped: Some(false),
        },
    )
    .await;
    assert_eq!(runtimes.agent_id, created.agent.agent_id);
    assert_eq!(runtimes.runtimes.len(), 1);
    assert_eq!(runtimes.runtimes[0].runtime_id, registered.session_id);
    assert_eq!(runtimes.runtimes[0].harness, hid("codex"));
    assert!(runtimes.runtimes[0].active);
}

#[tokio::test]
async fn agent_policy_gates_mutations_but_treats_project_as_an_explicit_list_filter_only() {
    let state = state().await;
    create_agent(&state, "ember").await;

    let denied_create = call_err(
        &state,
        caller("reader", "demo", Tier::Agent),
        "agent.create",
        &AgentCreateRequest {
            name: "unauthorized".into(),
            default_harness: Some(hid("codex")),
            project: None,
            role: None,
        },
    )
    .await;
    assert_eq!(denied_create.code, codes::UNAUTHORIZED);

    let listed: AgentListResponse = call_ok(
        &state,
        caller("reader", "other", Tier::Agent),
        "agent.list",
        &AgentListRequest {
            project: Some("demo".into()),
            include_disabled: None,
        },
    )
    .await;
    assert_eq!(listed.agents.len(), 1);
    assert_eq!(listed.agents[0].name.as_deref(), Some("ember"));

    let shown: AgentShowResponse = call_ok(
        &state,
        caller("reader", "other", Tier::Agent),
        "agent.show",
        &AgentShowRequest {
            agent_id: None,
            name: Some("ember".into()),
        },
    )
    .await;
    assert_eq!(shown.agent.name.as_deref(), Some("ember"));
}

/// A validated [`nexus_contracts::HarnessId`] from a literal (panics on invalid — test-only).
fn hid(s: &str) -> nexus_contracts::HarnessId {
    nexus_contracts::HarnessId::new(s).expect("valid harness id literal")
}
