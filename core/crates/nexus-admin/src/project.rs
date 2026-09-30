//! `admin.assignProject` — move an agent session to a different project (spec §8). Tier-guarded.
//!
//! Delegates to [`IdentityPort::assign_project`] after enforcing [`Tier::Admin`].

use std::sync::Arc;

use nexus_contracts::admin::{AssignProjectRequest, AssignProjectResponse};
use nexus_contracts::enums::Tier;
use nexus_contracts::ports::{Caller, IdentityPort, PortResult};

use crate::guard;

/// Guard on [`Tier::Admin`], then delegate the project move to the identity port.
pub async fn assign_project(
    identity: &Arc<dyn IdentityPort>,
    caller: &Caller,
    req: AssignProjectRequest,
) -> PortResult<AssignProjectResponse> {
    guard(caller, Tier::Admin)?;
    identity.assign_project(&req.name, &req.project).await
}
