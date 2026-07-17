//! `admin.assignRole` — set a registered agent's role **label** (spec §8). Tier-guarded.
//!
//! **Roles are display labels only — no functional routing (de-orchestrated).** A role never gates,
//! reorders, or redirects the message path; it is metadata for display/addressing. [`IdentityPort`]
//! exposes a role-mutation method that persists the label into the durable identity read model while
//! keeping assignment inert with respect to routing.

use std::sync::Arc;

use nexus_contracts::admin::{AssignRoleRequest, AssignRoleResponse};
use nexus_contracts::enums::Tier;
use nexus_contracts::ports::{Caller, IdentityPort, PortResult};

use crate::guard;

/// Guard on [`Tier::Admin`], then record the role label (display-only).
///
/// Roles carry no functional behavior (anchor §5 — admin never becomes an orchestrator); the
/// identity layer only makes the label visible through `agents show`, `members`, and `whoami`.
pub async fn assign_role(
    identity: &Arc<dyn IdentityPort>,
    caller: &Caller,
    req: AssignRoleRequest,
) -> PortResult<AssignRoleResponse> {
    guard(caller, Tier::Admin)?;
    identity.assign_role(&req.name, &req.role).await
}
