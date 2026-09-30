//! `agent.child_streams`: owner-authorized lookup of one session's child lane.

use nexus_common::NexusError;
use nexus_contracts::codes;
use nexus_contracts::enums::Tier;
use nexus_contracts::ports::{Caller, ContractError};
use nexus_contracts::{
    ChildCursorStatus, ChildLaneCursor, ChildLaneSummary, ChildSessionLoss, ChildStreamPage,
    ChildStreamRow, ChildStreamsRequest, ChildStreamsResponse,
};
use nexus_store::repos::{
    AgentAccessGrants, ChildStreamEvents, DaemonState, LaneCursor, LaneFilter, Sessions,
};
use nexus_store::Store;

const DEFAULT_ROW_LIMIT: u32 = 200;
const DEFAULT_LANE_LIMIT: u32 = 100;
const MAX_LIMIT: u32 = 1000;

fn internal(error: NexusError) -> ContractError {
    ContractError {
        code: codes::INTERNAL_ERROR,
        message: format!("child streams: {error}"),
    }
}

/// The caller may read the owner session's child lane when it is that session, an admin, or
/// holds an access grant on the session's agent. A grant found by the caller's durable agent
/// id authorizes it under any name. A grant found by principal name and project authorizes
/// only when its explicit bindings, where present, name this caller: a bound principal agent
/// id must equal the caller's agent id and a bound principal session id must equal the
/// caller's session; a name never overrides a mismatching durable binding. A legacy grant with
/// neither binding authorizes by name and project alone.
async fn authorize(
    store: &Store,
    caller: &Caller,
    session: &nexus_contracts::SessionId,
) -> Result<(), ContractError> {
    let row = Sessions::new(store)
        .find_by_session_id(session)
        .await
        .map_err(internal)?
        .ok_or_else(|| ContractError {
            code: codes::NOT_FOUND,
            message: format!("child streams: unknown session {}", session.0),
        })?;
    if &caller.session == session || caller.tier == Tier::Admin {
        return Ok(());
    }
    let refused = || ContractError {
        code: codes::UNAUTHORIZED,
        message: "child streams: caller is not the owner session, an admin, or a grantee".into(),
    };
    let Some(agent_id) = row.agent_id.as_deref() else {
        return Err(refused());
    };
    let grants = AgentAccessGrants::new(store);
    let caller_agent = caller.agent_id.as_ref().map(|id| id.to_string());
    if let Some(principal) = caller_agent.as_deref() {
        if grants
            .find_by_principal_agent_id(agent_id, principal)
            .await
            .map_err(internal)?
            .is_some()
        {
            return Ok(());
        }
    }
    let Some(grant) = grants
        .find(agent_id, &caller.project, &caller.name)
        .await
        .map_err(internal)?
    else {
        return Err(refused());
    };
    let agent_bound_ok = match grant.principal_agent_id.as_deref() {
        Some(bound) => caller_agent.as_deref() == Some(bound),
        None => true,
    };
    let session_bound_ok = match grant.principal_session_id.as_deref() {
        Some(bound) => caller.session.0 == bound,
        None => true,
    };
    if agent_bound_ok && session_bound_ok {
        Ok(())
    } else {
        Err(ContractError {
            code: codes::UNAUTHORIZED,
            message: "child streams: the named grant is bound to another principal".into(),
        })
    }
}

pub async fn lookup(
    store: &Store,
    caller: &Caller,
    req: ChildStreamsRequest,
) -> Result<ChildStreamsResponse, ContractError> {
    authorize(store, caller, &req.session).await?;
    let epoch = DaemonState::new(store)
        .boot_epoch()
        .await
        .map_err(internal)?
        .unwrap_or_default();
    // Both cursors are epoch-bearing. A missing epoch on either is malformed; a mismatch on
    // either resets both to the start, so a lane cursor from an old boot can never hide the
    // current lanes while fresh rows are returned.
    for (name, cursor_epoch) in [
        ("cursor", req.cursor.as_ref().map(|c| c.epoch.as_str())),
        (
            "lanesAfter",
            req.lanes_after.as_ref().map(|c| c.epoch.as_str()),
        ),
    ] {
        if cursor_epoch.is_some_and(|value| value.trim().is_empty()) {
            return Err(ContractError {
                code: codes::INVALID_PARAMS,
                message: format!("child streams: {name}.epoch is required"),
            });
        }
    }
    let mismatch = req.cursor.as_ref().is_some_and(|c| c.epoch != epoch)
        || req.lanes_after.as_ref().is_some_and(|c| c.epoch != epoch);
    let cursor_status = if mismatch {
        ChildCursorStatus::BootMismatch
    } else if req.cursor.is_some() || req.lanes_after.is_some() {
        ChildCursorStatus::Valid
    } else {
        ChildCursorStatus::Fresh
    };
    let after_id = if mismatch {
        0
    } else {
        req.cursor.as_ref().map_or(0, |c| c.after_id)
    };
    let limit = req.limit.unwrap_or(DEFAULT_ROW_LIMIT).clamp(1, MAX_LIMIT);
    let lanes_limit = req
        .lanes_limit
        .unwrap_or(DEFAULT_LANE_LIMIT)
        .clamp(1, MAX_LIMIT);
    let filter = LaneFilter {
        harness: req.harness.clone(),
        root: req.root.clone(),
        child_key: req.child.clone(),
    };
    let repo = ChildStreamEvents::new(store);
    let page = repo
        .page(&req.session, &filter, after_id, limit)
        .await
        .map_err(internal)?;
    let lanes_after = if mismatch {
        None
    } else {
        req.lanes_after.as_ref().map(|c| LaneCursor {
            child_key: c.child_key.clone(),
            harness: c.harness.clone(),
            root: c.root.clone(),
        })
    };
    let lanes = repo
        .lanes(&req.session, lanes_after.as_ref(), lanes_limit)
        .await
        .map_err(internal)?;
    let session_loss = repo.session_loss(&req.session).await.map_err(internal)?;
    Ok(ChildStreamsResponse {
        epoch: epoch.clone(),
        cursor_status,
        lanes: lanes
            .lanes
            .into_iter()
            .map(|lane| ChildLaneSummary {
                child: lane.child,
                child_key: lane.child_key,
                epoch: lane.epoch,
                first_seen_id: lane.first_seen_id,
                live_first_id: lane.live_first_id,
                live_last_id: lane.live_last_id,
                live_rows: lane.live_rows,
                live_bytes: lane.live_bytes,
                evicted_through: lane.evicted_through,
                evicted_rows: lane.evicted_rows,
                evicted_bytes: lane.evicted_bytes,
                refused_rows: lane.refused_rows,
                coverage: lane.coverage.unwrap_or(serde_json::Value::Null),
            })
            .collect(),
        lanes_next_after: lanes.next_after.map(|c| ChildLaneCursor {
            epoch: epoch.clone(),
            child_key: c.child_key,
            harness: c.harness,
            root: c.root,
        }),
        page: ChildStreamPage {
            rows: page
                .rows
                .into_iter()
                .map(|row| ChildStreamRow {
                    id: row.id,
                    epoch: row.epoch,
                    child: row.child,
                    kind: row.kind,
                    source_ref: row.source_ref,
                    data: serde_json::from_str(&row.data)
                        .unwrap_or(serde_json::Value::String(row.data)),
                    bytes: row.bytes,
                    created_at: row.created_at,
                })
                .collect(),
            next_after_id: page.next_after_id,
        },
        session_loss: session_loss.map(|loss| ChildSessionLoss {
            epoch: loss.epoch,
            compacted_lanes: loss.compacted_lanes,
            evicted_rows: loss.evicted_rows,
            evicted_bytes: loss.evicted_bytes,
            refused_rows: loss.refused_rows,
            updated_at: loss.updated_at,
        }),
    })
}
