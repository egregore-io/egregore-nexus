//! `admin.remove` — tear down a session's loop (message store retained, spec §8). Tier-guarded,
//! then **delegates** to [`AgentTurnExecutionPort::remove`]. Command only; the message path is
//! untouched.

use std::sync::Arc;

use nexus_contracts::admin::{RemoveRequest, RemoveResponse};
use nexus_contracts::enums::Tier;
use nexus_contracts::ports::{AgentTurnExecutionPort, Caller, PortResult};

use crate::guard;

/// Guard on [`Tier::Admin`], then delegate to the agent-transport port's `remove`.
pub async fn remove(
    agent: &Arc<dyn AgentTurnExecutionPort>,
    caller: &Caller,
    req: RemoveRequest,
) -> PortResult<RemoveResponse> {
    guard(caller, Tier::Admin)?;
    agent.remove(req).await
}
