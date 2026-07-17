//! `admin.monitor` — web console oversight (spec §8). Tier-guarded.
//!
//! Monitoring is **observe-only**: it grants the admin a view of the live [`WsEvent`] feed (the
//! [`EventSink`] broadcast the daemon's WS hub serves) — it writes nothing, sends nothing, and never
//! touches the message path. Per the registry decisions, the `admin.monitor` RPC returns a snapshot
//! acknowledgement; the live feed is the WS `Notification` stream the CLI's `--follow` subscribes to.
//! The [`EventSink`] handle is held by the [`Admin`](crate::Admin) service so the oversight seam is
//! explicit and so a snapshot can be sourced here later without changing the port surface.

use std::sync::Arc;

use nexus_contracts::admin::MonitorRequest;
use nexus_contracts::enums::Tier;
use nexus_contracts::ports::{Caller, EventSink, PortResult};

use crate::guard;

/// Guard on [`Tier::Admin`], then authorize oversight (observe-only; no message-path write).
///
/// The `_events` sink is the web console broadcast the admin observes via the WS feed; monitoring does
/// not emit into it (admin never injects), so the handle is intentionally not written to here.
pub async fn monitor(
    _events: &Arc<dyn EventSink>,
    caller: &Caller,
    _req: MonitorRequest,
) -> PortResult<()> {
    guard(caller, Tier::Admin)?;
    Ok(())
}
