//! `agent.child_streams`: the owner-authorized lookup of a session's child lane through the
//! real dispatch path. Owner, admin and grantee callers read it; anyone else is refused; a
//! cursor from another boot is answered as `boot_mismatch` with a page from the start; a cursor
//! without an epoch is refused as malformed.
//!
//! Run: `cargo test -p egregore-nexus --test child_streams_method`

use std::sync::Arc;

use nexus::daemon::{dispatch, AppState};
use nexus_common::{now, Config};
use nexus_contracts::{
    codes, AgentId, Caller, ChildCursorStatus, ChildLaneCursor, ChildResolution, ChildStream,
    ChildStreamCursor, ChildStreamsRequest, ChildStreamsResponse, Request, RequestId, RpcError,
    SessionId, Tier,
};
use nexus_store::repos::{
    AgentAccessGrants, Agents, AppendOutcome, ChildStreamEvents, DaemonState, NewAgent,
    NewAgentAccessGrant, NewSession, Sessions,
};
use nexus_store::Store;
use serde::Serialize;
use serde_json::json;

const OWNER: &str = "s_owner";
const AGENT: &str = "agent_owner";

fn caller(name: &str, session: &str, tier: Tier) -> Caller {
    Caller {
        agent_id: None,
        session: SessionId(session.into()),
        name: name.into(),
        project: "demo".into(),
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

async fn call(
    state: &AppState,
    caller: Caller,
    params: &ChildStreamsRequest,
) -> Result<ChildStreamsResponse, RpcError> {
    let resp = dispatch(state, Some(caller), req("agent.child_streams", params)).await;
    match resp.error {
        Some(error) => Err(error),
        None => Ok(serde_json::from_value(resp.result.unwrap()).unwrap()),
    }
}

fn request(session: &str) -> ChildStreamsRequest {
    ChildStreamsRequest {
        session: SessionId(session.into()),
        harness: None,
        root: None,
        child: None,
        cursor: None,
        limit: None,
        lanes_after: None,
        lanes_limit: None,
    }
}

fn child(id: &str) -> ChildStream {
    ChildStream {
        harness: "claude".into(),
        root: "root".into(),
        id: Some(id.into()),
        locator: format!("claude:subagents/agent-{id}.jsonl@u1"),
        parent: None,
        parent_ref: None,
        depth: None,
        resolution: ChildResolution::RootVerified,
        evidence: Some("subagents_dir+sessionId+agentId".into()),
    }
}

/// A daemon state with an owner session bound to an agent, a stranger session, a boot epoch,
/// and three rows on the owner's child lane (two lanes, one with a coverage declaration).
async fn fixture() -> (AppState, Arc<Store>, String) {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let state = AppState::wire(store.clone(), &Config::default());
    state.wait_for_runtime_identity_ready().await.unwrap();
    DaemonState::new(&store)
        .set_boot_epoch("boot_a", now())
        .await
        .unwrap();
    let epoch = DaemonState::new(&store)
        .boot_epoch()
        .await
        .unwrap()
        .expect("boot epoch");
    Agents::new(&store)
        .create(NewAgent {
            agent_id: AGENT.into(),
            project: "demo".into(),
            name: Some("owner".into()),
            default_harness: Some("claude".into()),
            role: None,
            tier: Some("agent".into()),
            owner: None,
        })
        .await
        .unwrap();
    for (session, name) in [
        (OWNER, "owner"),
        ("s_stranger", "stranger"),
        ("s_viewer", "viewer"),
    ] {
        Sessions::new(&store)
            .create(NewSession {
                session_id: SessionId(session.into()),
                name: Some(name.into()),
                agent: Some("claude".into()),
                kind: "agent".into(),
                role: None,
                tier: "agent".into(),
                harness_session_id: None,
                client_key: Some(session.into()),
                cwd: None,
                project: "demo".into(),
                transport: Some("pty".into()),
            })
            .await
            .unwrap();
    }
    Sessions::new(&store)
        .set_agent_id(&SessionId(OWNER.into()), AGENT)
        .await
        .unwrap();
    let lane = ChildStreamEvents::new(&store);
    let bounds = store.child_stream_bounds();
    let owner = SessionId(OWNER.into());
    for (id, kind, text, source_ref) in [
        ("abc", "text", "first", "claude:abc@u1#10"),
        ("abc", "text", "second", "claude:abc@u1#20"),
        ("xyz", "user_input", "other child", "claude:xyz@u1#5"),
    ] {
        let outcome = lane
            .append(
                &owner,
                &epoch,
                &child(id),
                kind,
                source_ref,
                &json!({ "text": text }).to_string(),
                &bounds,
            )
            .await
            .unwrap();
        assert!(matches!(outcome, AppendOutcome::Stored(_)));
    }
    lane.set_coverage(
        &owner,
        &epoch,
        &child("xyz"),
        &json!({ "generation": "u1", "from": 5, "unknown_before": true, "reason": "source_truncated" }),
        &bounds,
    )
    .await
    .unwrap();
    (state, store, epoch)
}

#[tokio::test]
async fn the_owner_reads_its_lanes_rows_and_cursors_across_pages() {
    let (state, _store, epoch) = fixture().await;
    let owner = caller("owner", OWNER, Tier::Agent);

    // Fresh: from the start, both lanes listed, the coverage declaration visible.
    let out = call(&state, owner.clone(), &request(OWNER)).await.unwrap();
    assert_eq!(out.epoch, epoch);
    assert_eq!(out.cursor_status, ChildCursorStatus::Fresh);
    assert_eq!(out.page.rows.len(), 3, "{:?}", out.page);
    assert_eq!(out.page.rows[0].data["text"], "first");
    assert_eq!(out.page.rows[0].child.id.as_deref(), Some("abc"));
    assert_eq!(out.page.rows[0].source_ref, "claude:abc@u1#10");
    assert_eq!(out.page.rows[2].kind, "user_input");
    assert_eq!(out.page.next_after_id, None);
    let keys: Vec<&str> = out.lanes.iter().map(|l| l.child_key.as_str()).collect();
    assert_eq!(keys, vec!["n:abc", "n:xyz"]);
    assert_eq!(out.lanes[0].live_rows, 2);
    assert_eq!(out.lanes[0].coverage, serde_json::Value::Null);
    assert_eq!(out.lanes[1].coverage["reason"], "source_truncated");
    assert_eq!(out.lanes[1].child.resolution, ChildResolution::RootVerified);
    assert_eq!(out.session_loss, None);

    // Paging with a valid cursor continues after the given row; limits are honored.
    let mut paged = request(OWNER);
    paged.limit = Some(2);
    let first = call(&state, owner.clone(), &paged).await.unwrap();
    assert_eq!(first.page.rows.len(), 2);
    let after = first.page.next_after_id.expect("more rows");
    paged.cursor = Some(ChildStreamCursor {
        epoch: epoch.clone(),
        after_id: after,
    });
    let second = call(&state, owner.clone(), &paged).await.unwrap();
    assert_eq!(second.cursor_status, ChildCursorStatus::Valid);
    assert_eq!(second.page.rows.len(), 1);
    assert_eq!(second.page.rows[0].data["text"], "other child");
    assert_eq!(second.page.next_after_id, None);

    // Narrowing to one lane.
    let mut narrowed = request(OWNER);
    narrowed.child = Some("n:xyz".into());
    let out = call(&state, owner.clone(), &narrowed).await.unwrap();
    assert_eq!(out.page.rows.len(), 1);
    assert_eq!(out.page.rows[0].child.id.as_deref(), Some("xyz"));
}

#[tokio::test]
async fn a_cursor_from_another_boot_is_answered_as_boot_mismatch_from_the_start() {
    let (state, _store, epoch) = fixture().await;
    let owner = caller("owner", OWNER, Tier::Agent);
    let mut stale = request(OWNER);
    stale.cursor = Some(ChildStreamCursor {
        epoch: "boot_before".into(),
        after_id: 2,
    });
    let out = call(&state, owner.clone(), &stale).await.unwrap();
    assert_eq!(out.cursor_status, ChildCursorStatus::BootMismatch);
    assert_eq!(
        out.epoch, epoch,
        "the answer carries the current epoch to re-cursor with"
    );
    assert_eq!(
        out.page.rows.len(),
        3,
        "a page from the lane's start, never a silent empty page"
    );

    let mut malformed = request(OWNER);
    malformed.cursor = Some(ChildStreamCursor {
        epoch: "  ".into(),
        after_id: 0,
    });
    let error = call(&state, owner, &malformed).await.unwrap_err();
    assert_eq!(error.code, codes::INVALID_PARAMS);
}

#[tokio::test]
async fn only_the_owner_an_admin_or_a_grantee_may_read_the_lane() {
    let (state, store, _epoch) = fixture().await;
    let stranger = call(
        &state,
        caller("stranger", "s_stranger", Tier::Agent),
        &request(OWNER),
    )
    .await
    .unwrap_err();
    assert_eq!(stranger.code, codes::UNAUTHORIZED);

    let admin = call(
        &state,
        caller("operator", "local-operator", Tier::Admin),
        &request(OWNER),
    )
    .await
    .unwrap();
    assert_eq!(admin.page.rows.len(), 3);

    // A viewer without a grant is refused; with a grant on the owner's agent it reads.
    let viewer = caller("viewer", "s_viewer", Tier::Agent);
    let refused = call(&state, viewer.clone(), &request(OWNER))
        .await
        .unwrap_err();
    assert_eq!(refused.code, codes::UNAUTHORIZED);
    AgentAccessGrants::new(&store)
        .grant(NewAgentAccessGrant {
            agent_id: AGENT.into(),
            principal_project: "demo".into(),
            principal_name: "viewer".into(),
            principal_session_id: Some("s_viewer".into()),
            principal_agent_id: None,
            role: "viewer".into(),
            granted_by_name: "operator".into(),
            granted_by_project: "demo".into(),
        })
        .await
        .unwrap();
    let granted = call(&state, viewer, &request(OWNER)).await.unwrap();
    assert_eq!(granted.page.rows.len(), 3);

    // An unknown session is not found, even for an admin.
    let missing = call(
        &state,
        caller("operator", "local-operator", Tier::Admin),
        &request("s_nobody"),
    )
    .await
    .unwrap_err();
    assert_eq!(missing.code, codes::NOT_FOUND);
}

/// A lane cursor is epoch-bearing like the row cursor: one from another boot cannot hide the
/// current lanes (both cursors reset to the start and the status says so), one from the
/// current boot continues after its key, and one without an epoch is malformed. The returned
/// lane cursor carries the current epoch.
#[tokio::test]
async fn a_lane_cursor_from_another_boot_resets_lane_paging_too() {
    let (state, _store, epoch) = fixture().await;
    let owner = caller("owner", OWNER, Tier::Agent);
    let stale_lane = |epoch: &str| ChildLaneCursor {
        epoch: epoch.into(),
        child_key: "n:zzz".into(),
        harness: "claude".into(),
        root: "root".into(),
    };

    // Old-boot lane cursor beyond every current lane: mismatch, lanes from the start.
    let mut stale = request(OWNER);
    stale.lanes_after = Some(stale_lane("boot_before"));
    let out = call(&state, owner.clone(), &stale).await.unwrap();
    assert_eq!(out.cursor_status, ChildCursorStatus::BootMismatch);
    let keys: Vec<&str> = out.lanes.iter().map(|l| l.child_key.as_str()).collect();
    assert_eq!(keys, vec!["n:abc", "n:xyz"], "no current lane is hidden");
    assert_eq!(out.page.rows.len(), 3);

    // A current-boot lane cursor continues after its key, and the next cursor carries the
    // epoch so it can be handed back.
    let mut paged = request(OWNER);
    paged.lanes_limit = Some(1);
    let first = call(&state, owner.clone(), &paged).await.unwrap();
    assert_eq!(first.cursor_status, ChildCursorStatus::Fresh);
    assert_eq!(first.lanes.len(), 1);
    assert_eq!(first.lanes[0].child_key, "n:abc");
    let next = first.lanes_next_after.expect("more lanes");
    assert_eq!(next.epoch, epoch);
    assert_eq!(next.child_key, "n:abc");
    paged.lanes_after = Some(next);
    let second = call(&state, owner.clone(), &paged).await.unwrap();
    assert_eq!(second.cursor_status, ChildCursorStatus::Valid);
    assert_eq!(second.lanes.len(), 1);
    assert_eq!(second.lanes[0].child_key, "n:xyz");
    assert_eq!(second.lanes_next_after, None);

    // A lane cursor with no epoch is malformed; a valid row cursor next to an old-boot lane
    // cursor still counts as a mismatch for both.
    let mut malformed = request(OWNER);
    malformed.lanes_after = Some(stale_lane(""));
    let error = call(&state, owner.clone(), &malformed).await.unwrap_err();
    assert_eq!(error.code, codes::INVALID_PARAMS);
    let mut mixed = request(OWNER);
    mixed.cursor = Some(ChildStreamCursor {
        epoch: epoch.clone(),
        after_id: 2,
    });
    mixed.lanes_after = Some(stale_lane("boot_before"));
    let out = call(&state, owner, &mixed).await.unwrap();
    assert_eq!(out.cursor_status, ChildCursorStatus::BootMismatch);
    assert_eq!(
        out.page.rows.len(),
        3,
        "the row cursor is reset with the lane cursor"
    );
    assert_eq!(out.lanes.len(), 2);
}

/// A grant found by name and project authorizes only the principal it is bound to: a caller
/// with the same name but another session, or without the bound agent id, is refused; the
/// bound agent id authorizes even under a new name; a legacy grant with no bindings authorizes
/// by name and project alone.
#[tokio::test]
async fn a_named_grant_never_overrides_its_durable_principal_bindings() {
    let (state, store, _epoch) = fixture().await;
    for session in ["s_viewer2", "s_renamed", "s_legacy"] {
        Sessions::new(&store)
            .create(NewSession {
                session_id: SessionId(session.into()),
                name: Some(session.trim_start_matches("s_").into()),
                agent: Some("claude".into()),
                kind: "agent".into(),
                role: None,
                tier: "agent".into(),
                harness_session_id: None,
                client_key: Some(session.into()),
                cwd: None,
                project: "demo".into(),
                transport: Some("pty".into()),
            })
            .await
            .unwrap();
    }
    let grants = AgentAccessGrants::new(&store);
    grants
        .grant(NewAgentAccessGrant {
            agent_id: AGENT.into(),
            principal_project: "demo".into(),
            principal_name: "viewer".into(),
            principal_session_id: Some("s_viewer".into()),
            principal_agent_id: Some("agent_viewer".into()),
            role: "viewer".into(),
            granted_by_name: "operator".into(),
            granted_by_project: "demo".into(),
        })
        .await
        .unwrap();
    let with_agent = |name: &str, session: &str, agent: Option<&str>| {
        let mut c = caller(name, session, Tier::Agent);
        c.agent_id = agent.map(AgentId::from);
        c
    };

    // Same name, another session, no durable id: refused.
    let error = call(
        &state,
        with_agent("viewer", "s_viewer2", None),
        &request(OWNER),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, codes::UNAUTHORIZED);
    // Same name and session but without the bound agent id: refused.
    let error = call(
        &state,
        with_agent("viewer", "s_viewer", None),
        &request(OWNER),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, codes::UNAUTHORIZED);
    // Same name and session with another durable id: refused.
    let error = call(
        &state,
        with_agent("viewer", "s_viewer", Some("agent_other")),
        &request(OWNER),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, codes::UNAUTHORIZED);
    // The bound principal, exactly: reads.
    let out = call(
        &state,
        with_agent("viewer", "s_viewer", Some("agent_viewer")),
        &request(OWNER),
    )
    .await
    .unwrap();
    assert_eq!(out.page.rows.len(), 3);
    // The bound agent id under a new name and session: reads (durable identity wins).
    let out = call(
        &state,
        with_agent("renamed", "s_renamed", Some("agent_viewer")),
        &request(OWNER),
    )
    .await
    .unwrap();
    assert_eq!(out.page.rows.len(), 3);

    // A legacy grant with no bindings authorizes by name and project alone.
    grants
        .grant(NewAgentAccessGrant {
            agent_id: AGENT.into(),
            principal_project: "demo".into(),
            principal_name: "legacy".into(),
            principal_session_id: None,
            principal_agent_id: None,
            role: "viewer".into(),
            granted_by_name: "operator".into(),
            granted_by_project: "demo".into(),
        })
        .await
        .unwrap();
    let out = call(
        &state,
        with_agent("legacy", "s_legacy", None),
        &request(OWNER),
    )
    .await
    .unwrap();
    assert_eq!(out.page.rows.len(), 3);
    let error = call(
        &state,
        with_agent("legacy", "s_legacy", Some("agent_x")),
        &request(OWNER),
    )
    .await;
    assert!(error.is_ok(), "a legacy grant does not bind an agent id");
}
