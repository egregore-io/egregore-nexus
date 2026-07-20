//! Stable identity-by-id regressions.
//!
//! Durable identity is `agent_id`; names are edge-resolved labels. These tests pin the `AgentRef`
//! edge resolver and the id-keyed store reads that make renames label-only.

use libsql::params;
use nexus_common::NexusError;
use nexus_contracts::ids::{AgentId, SessionId};
use nexus_store::repos::{
    AgentAccessGrants, AgentGroups, AgentRef, AgentRuntimes, Agents, NewAgent, NewAgentAccessGrant,
    NewAgentRuntime, NewSession, Sessions, ROLE_CO_OWNER, ROLE_VIEWER,
};
use nexus_store::Store;

async fn migrated() -> Store {
    let store = Store::open(":memory:").await.unwrap();
    store.migrate().await.unwrap();
    store
}

fn new_agent(agent_id: &str, project: &str, name: &str) -> NewAgent {
    NewAgent {
        agent_id: agent_id.to_string(),
        project: project.to_string(),
        name: Some(name.to_string()),
        default_harness: Some("claude".to_string()),
        role: None,
        tier: None,
        owner: None,
    }
}

fn new_runtime(runtime_id: &str, agent_id: &str) -> NewAgentRuntime {
    NewAgentRuntime {
        runtime_id: runtime_id.to_string(),
        agent_id: agent_id.to_string(),
        harness: "claude".to_string(),
        cwd: None,
        transport: Some("pty".to_string()),
        presence: Some("online".to_string()),
        active: true,
    }
}

fn new_session(session_id: &str, project: &str, name: &str) -> NewSession {
    NewSession {
        session_id: SessionId(session_id.to_string()),
        name: Some(name.to_string()),
        agent: Some("claude".to_string()),
        kind: "agent".to_string(),
        role: None,
        tier: "agent".to_string(),
        harness_session_id: None,
        client_key: Some(format!("ck_{session_id}")),
        cwd: None,
        project: project.to_string(),
        transport: Some("pty".to_string()),
    }
}

async fn session_agent_id(store: &Store, session_id: &str) -> Option<String> {
    let mut rows = store
        .conn
        .query(
            "SELECT agent_id FROM sessions WHERE session_id = ?1",
            params![session_id],
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().expect("session row exists");
    match row.get_value(0).unwrap() {
        libsql::Value::Text(s) => Some(s),
        _ => None,
    }
}

#[tokio::test]
async fn agent_ref_parses_and_resolves_direct_id_without_names() {
    let store = migrated().await;
    let agents = Agents::new(&store);
    agents
        .create(new_agent("a_cara", "default", "cara"))
        .await
        .unwrap();

    assert_eq!(AgentRef::parse("a_cara"), AgentRef::Id("a_cara".into()));
    assert_eq!(AgentRef::parse("cara"), AgentRef::Name("cara".into()));
    assert_eq!(
        AgentRef::from_request(Some("a_cara"), "stale-label"),
        AgentRef::Id("a_cara".into()),
        "an explicit id always beats the display-name field"
    );

    let row = agents
        .resolve_ref("some-other-project", &AgentRef::Id("a_cara".into()), false)
        .await
        .unwrap();
    assert_eq!(row.agent_id, "a_cara");
    assert_eq!(row.name.as_deref(), Some("cara"));

    let missing = agents
        .resolve_ref("default", &AgentRef::Id("a_missing".into()), false)
        .await;
    assert!(
        matches!(missing, Err(NexusError::NotFound(_))),
        "a missing id is a loud NotFound, never a name fallback"
    );
}

#[tokio::test]
async fn name_resolution_is_project_scoped_with_explicit_any_project_opt_in() {
    let store = migrated().await;
    let agents = Agents::new(&store);
    agents
        .create(new_agent("a_dee", "proj-a", "dee"))
        .await
        .unwrap();

    let scoped = agents
        .resolve_ref("proj-a", &AgentRef::Name("dee".into()), false)
        .await
        .unwrap();
    assert_eq!(scoped.agent_id, "a_dee");

    let cross = agents
        .resolve_ref("proj-b", &AgentRef::Name("dee".into()), false)
        .await;
    assert!(
        matches!(cross, Err(NexusError::NotFound(_))),
        "cross-project names do not resolve without the explicit any-project opt-in"
    );

    let widened = agents
        .resolve_ref("proj-b", &AgentRef::Name("dee".into()), true)
        .await
        .unwrap();
    assert_eq!(widened.agent_id, "a_dee");

    let nowhere = agents
        .resolve_ref("proj-b", &AgentRef::Name("nobody".into()), true)
        .await;
    assert!(matches!(nowhere, Err(NexusError::NotFound(_))));
}

#[tokio::test]
async fn widened_name_resolution_rejects_cross_project_legacy_ambiguity() {
    let store = migrated().await;
    store
        .identity_conn()
        .execute("DROP INDEX idx_agents_name_unique", ())
        .await
        .unwrap();
    let agents = Agents::new(&store);
    agents
        .create(new_agent("a_dee_a", "proj-a", "dee"))
        .await
        .unwrap();
    agents
        .create(new_agent("a_dee_b", "proj-b", "dee"))
        .await
        .unwrap();

    let error = agents
        .resolve_ref("proj-a", &AgentRef::Name("dee".into()), true)
        .await
        .unwrap_err();

    assert!(matches!(error, NexusError::Ambiguous(_)));
}

#[tokio::test]
async fn rename_does_not_move_id_keyed_edges() {
    let store = migrated().await;
    let agents = Agents::new(&store);
    agents
        .create(new_agent("a_eve", "default", "eve"))
        .await
        .unwrap();
    AgentRuntimes::new(&store)
        .create(new_runtime("s_eve_rt", "a_eve"))
        .await
        .unwrap();
    let sessions = Sessions::new(&store);
    sessions
        .create(new_session("s_eve_rt", "default", "eve"))
        .await
        .unwrap();
    sessions
        .set_agent_id(&SessionId("s_eve_rt".into()), "a_eve")
        .await
        .unwrap();
    AgentAccessGrants::new(&store)
        .grant(NewAgentAccessGrant {
            agent_id: "a_target".to_string(),
            principal_project: "default".to_string(),
            principal_name: "eve".to_string(),
            principal_session_id: Some("s_eve_rt".to_string()),
            principal_agent_id: Some("a_eve".to_string()),
            role: ROLE_VIEWER.to_string(),
            granted_by_name: "alex".to_string(),
            granted_by_project: "default".to_string(),
        })
        .await
        .unwrap();
    AgentGroups::new(&store)
        .assign("default", "backend", &AgentId("a_eve".into()), "eve")
        .await
        .unwrap();
    assert_eq!(
        session_agent_id(&store, "s_eve_rt").await,
        Some("a_eve".to_string())
    );

    // Rename is label-only: agents.name + sessions.name change, ids stay put.
    agents.rename("a_eve", "yvette").await.unwrap();
    sessions
        .set_name(&SessionId("s_eve_rt".to_string()), "yvette")
        .await
        .unwrap();

    assert_eq!(
        session_agent_id(&store, "s_eve_rt").await,
        Some("a_eve".to_string()),
        "sessions.agent_id survives rename"
    );
    let runtime = AgentRuntimes::new(&store)
        .find_by_runtime_id("s_eve_rt")
        .await
        .unwrap()
        .expect("runtime row");
    assert_eq!(runtime.agent_id, "a_eve");
    let grant = AgentAccessGrants::new(&store)
        .find_by_principal_agent_id("a_target", "a_eve")
        .await
        .unwrap()
        .expect("grant still keyed to the principal id");
    assert_eq!(grant.principal_agent_id.as_deref(), Some("a_eve"));
    assert!(AgentGroups::new(&store)
        .is_member("default", "backend", &AgentId("a_eve".into()))
        .await
        .unwrap());
}

#[tokio::test]
async fn id_keyed_grant_reads_are_rename_proof() {
    let store = migrated().await;
    let grants = AgentAccessGrants::new(&store);
    grants
        .grant(NewAgentAccessGrant {
            agent_id: "a_target".to_string(),
            principal_project: "default".to_string(),
            principal_name: "old-label".to_string(),
            principal_session_id: None,
            principal_agent_id: Some("a_principal".to_string()),
            role: ROLE_CO_OWNER.to_string(),
            granted_by_name: "alex".to_string(),
            granted_by_project: "default".to_string(),
        })
        .await
        .unwrap();

    assert!(grants
        .can_view_agent_id("a_target", "a_principal")
        .await
        .unwrap());
    assert!(grants
        .is_co_owner_agent_id("a_target", "a_principal")
        .await
        .unwrap());
    assert!(
        !grants
            .can_view_agent_id("a_target", "a_other")
            .await
            .unwrap(),
        "no grant row, no access"
    );
    // A legacy row with NULL principal_agent_id must be invisible to the id-keyed read.
    grants
        .grant(NewAgentAccessGrant {
            agent_id: "a_target2".to_string(),
            principal_project: "default".to_string(),
            principal_name: "legacy".to_string(),
            principal_session_id: None,
            principal_agent_id: None,
            role: ROLE_VIEWER.to_string(),
            granted_by_name: "alex".to_string(),
            granted_by_project: "default".to_string(),
        })
        .await
        .unwrap();
    assert!(!grants
        .can_view_agent_id("a_target2", "a_anything")
        .await
        .unwrap());
}

#[tokio::test]
async fn access_grant_snapshot_lists_only_the_requested_stable_agent() {
    let store = migrated().await;
    let grants = AgentAccessGrants::new(&store);
    for (agent_id, principal, principal_agent_id, role) in [
        (
            "a_target",
            "delegate-one",
            Some("a_delegate_one"),
            ROLE_VIEWER,
        ),
        ("a_target", "delegate-two", None, ROLE_CO_OWNER),
        ("a_other", "unrelated", Some("a_unrelated"), ROLE_VIEWER),
    ] {
        grants
            .grant(NewAgentAccessGrant {
                agent_id: agent_id.to_string(),
                principal_project: "default".to_string(),
                principal_name: principal.to_string(),
                principal_session_id: None,
                principal_agent_id: principal_agent_id.map(str::to_string),
                role: role.to_string(),
                granted_by_name: "owner".to_string(),
                granted_by_project: "default".to_string(),
            })
            .await
            .unwrap();
    }

    let rows = grants.list_for_agent("a_target").await.unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].principal_name, "delegate-one");
    assert_eq!(rows[1].principal_name, "delegate-two");
    assert!(rows.iter().all(|row| row.agent_id == "a_target"));
}

#[tokio::test]
async fn active_runtime_session_resolves_by_agent_id() {
    let store = migrated().await;
    Agents::new(&store)
        .create(new_agent("a_fay", "default", "fay"))
        .await
        .unwrap();
    let runtimes = AgentRuntimes::new(&store);
    // A stopped historical runtime and the live one.
    runtimes
        .create(new_runtime("s_fay_old", "a_fay"))
        .await
        .unwrap();
    runtimes.stop("s_fay_old").await.unwrap();
    runtimes
        .create(new_runtime("s_fay_live", "a_fay"))
        .await
        .unwrap();
    let sessions = Sessions::new(&store);
    sessions
        .create(new_session("s_fay_old", "default", "fay-old"))
        .await
        .unwrap();
    sessions
        .set_agent_id(&SessionId("s_fay_old".into()), "a_fay")
        .await
        .unwrap();
    sessions
        .create(new_session("s_fay_live", "default", "fay"))
        .await
        .unwrap();
    sessions
        .set_agent_id(&SessionId("s_fay_live".into()), "a_fay")
        .await
        .unwrap();

    let live = sessions
        .active_runtime_session_for_agent("a_fay")
        .await
        .unwrap()
        .expect("live runtime session");
    assert_eq!(live.session_id.0, "s_fay_live");

    let compat = sessions
        .find_by_agent_id("a_fay")
        .await
        .unwrap()
        .expect("id-keyed session row");
    assert_eq!(compat.agent_id.as_deref(), Some("a_fay"));
}

#[tokio::test]
async fn active_runtime_session_rejects_a_session_owned_by_another_agent() {
    let store = migrated().await;
    let agents = Agents::new(&store);
    agents
        .create(new_agent("a_expected", "default", "expected"))
        .await
        .unwrap();
    agents
        .create(new_agent("a_other", "default", "other"))
        .await
        .unwrap();
    AgentRuntimes::new(&store)
        .create(new_runtime("s_shared", "a_expected"))
        .await
        .unwrap();
    let sessions = Sessions::new(&store);
    sessions
        .create(new_session("s_shared", "default", "other"))
        .await
        .unwrap();
    sessions
        .set_agent_id(&SessionId("s_shared".into()), "a_other")
        .await
        .unwrap();

    let error = sessions
        .active_runtime_session_for_agent("a_expected")
        .await
        .expect_err("runtime/session ownership mismatch must fail closed");

    assert!(matches!(error, NexusError::Invalid(_)));
    assert!(error.to_string().contains("s_shared"));
    assert!(error.to_string().contains("a_expected"));
    assert!(error.to_string().contains("a_other"));
}

#[tokio::test]
async fn message_row_recovers_sender_id_after_rename() {
    let store = migrated().await;
    let agents = Agents::new(&store);
    agents
        .create(new_agent("a_gus", "default", "gus"))
        .await
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO messages (message_id, from_name, kind, to_name, body, provenance, \
             project, created_at, from_agent_id) \
             VALUES ('m_1', 'gus', 'dm', 'alex', 'hi', '{}', 'default', 1, 'a_gus')",
            (),
        )
        .await
        .unwrap();

    agents.rename("a_gus", "gustavo").await.unwrap();

    let mut rows = store
        .conn
        .query(
            "SELECT from_agent_id FROM messages WHERE message_id = 'm_1'",
            (),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(
        row.get::<String>(0).unwrap(),
        "a_gus",
        "reply routing recovers the original sender identity, not whoever now holds the old name"
    );
}
