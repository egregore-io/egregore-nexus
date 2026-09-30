use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::Serialize;

use nexus::daemon::{dispatch, AppState};
use nexus_common::{hash_runtime_credential, Config};
use nexus_contracts::{
    codes, AgentAccessGrantRequest, AgentAccessGrantResponse, AgentAccessRevokeRequest,
    AgentAccessRevokeResponse, AgentAccessRole, AgentCreateRequest, AgentCreateResponse,
    AgentCredentialCreateRequest, AgentCredentialCreateResponse, AgentCredentialRevokeRequest,
    AgentCredentialRevokeResponse, AgentId, AgentListRequest, AgentListResponse,
    AgentOwnerTransferRequest, AgentOwnerTransferResponse, AgentRuntimeListRequest,
    AgentRuntimeListResponse, AgentShowRequest, AgentShowResponse, AssignProjectRequest,
    AssignProjectResponse, AssignRoleRequest, AssignRoleResponse, Caller, GrantTierRequest,
    GrantTierResponse, Kind, MemberListRequest, MemberListResponse, RegisterRequest,
    RegisterResponse, RemoveRequest, RemoveResponse, Request, RequestId, RpcError, SessionId, Tier,
};
use nexus_store::repos::{
    AgentAccessGrants, AgentCredentials, AgentOwner, Agents, Metadata, MetadataEntity, Sessions,
};
use nexus_store::Store;

async fn state() -> AppState {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    AppState::wire(store, &Config::default())
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
        caller("owner", "demo", Tier::Agent),
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
async fn agent_policy_gates_mutations_and_cross_project_reads() {
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

    let denied_list = call_err(
        &state,
        caller("reader", "other", Tier::Agent),
        "agent.list",
        &AgentListRequest {
            project: Some("demo".into()),
            include_disabled: None,
        },
    )
    .await;
    assert_eq!(denied_list.code, codes::UNAUTHORIZED);

    let denied_show = call_err(
        &state,
        caller("reader", "other", Tier::Agent),
        "agent.show",
        &AgentShowRequest {
            agent_id: None,
            name: Some("ember".into()),
        },
    )
    .await;
    assert_eq!(denied_show.code, codes::NOT_FOUND);
}

/// A validated [`nexus_contracts::HarnessId`] from a literal (panics on invalid — test-only).
fn hid(s: &str) -> nexus_contracts::HarnessId {
    nexus_contracts::HarnessId::new(s).expect("valid harness id literal")
}
