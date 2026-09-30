//! `admin.channel` — manage topics / Pub-feed routing (spec §8). Tier-guarded, then **delegates**
//! to [`NotifyPort::channel`]. Command only: this configures channels/routing rules, it never writes
//! a message or in-flight row.

use std::sync::Arc;

use nexus_contracts::admin::ChannelRequest;
use nexus_contracts::enums::Tier;
use nexus_contracts::ports::{Caller, NotifyPort, PortResult};

use crate::guard;

/// Guard on [`Tier::Admin`], then delegate to the notify port's `channel`.
pub async fn channel(
    notify: &Arc<dyn NotifyPort>,
    caller: &Caller,
    req: ChannelRequest,
) -> PortResult<()> {
    guard(caller, Tier::Admin)?;
    notify.channel(caller, req).await
}
