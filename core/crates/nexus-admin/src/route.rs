//! `admin.route` — ad-hoc, one-shot notification forward (spec §7/§8). Tier-guarded, then
//! **delegates** to [`NotifyPort::forward`].
//!
//! **This is the closest admin gets to the message path, and it stays a guardrail:** forwarding goes
//! through the notify port's `forward` (a one-shot dispatch of an already-ingested notification), not
//! a direct `send`/`enqueue`. Admin never writes a `messages` or `in_flight` row itself; routing a
//! notification is the user/admin acting as dispatcher (anchor §5 — never a passive orchestrator).

use std::sync::Arc;

use nexus_contracts::admin::RouteForwardRequest;
use nexus_contracts::enums::Tier;
use nexus_contracts::ports::{Caller, NotifyPort, PortResult};

use crate::guard;

/// Guard on [`Tier::Admin`], then delegate to the notify port's one-shot `forward`.
pub async fn route(
    notify: &Arc<dyn NotifyPort>,
    caller: &Caller,
    req: RouteForwardRequest,
) -> PortResult<()> {
    guard(caller, Tier::Admin)?;
    notify.forward(caller, req).await
}
