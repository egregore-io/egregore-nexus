//! `admin.spawn` — admin-initiated agent spawn (spec §8). Tier-guarded, then **delegates** to
//! [`AgentTurnExecutionPort::launch`]. Adds a command only: the spawned agent still self-registers;
//! nothing is inserted into the message path here.

use std::sync::Arc;

use nexus_contracts::admin::{SpawnRequest, SpawnResponse};
use nexus_contracts::enums::Tier;
use nexus_contracts::ports::{AgentTurnExecutionPort, Caller, PortResult};

use crate::guard;

/// Guard on [`Tier::Admin`], then delegate to the agent-transport port's `launch`.
pub async fn spawn(
    agent: &Arc<dyn AgentTurnExecutionPort>,
    caller: &Caller,
    req: SpawnRequest,
) -> PortResult<SpawnResponse> {
    guard(caller, Tier::Admin)?;
    agent.launch(req).await
}
