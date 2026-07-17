//! Boot reconstruction for the daemon's minimal thread-routing directory.

use super::*;

impl AppState {
    /// Rehydrate only thread identity and membership edges needed for fan-out/auto-wake.
    /// Gateway remains authoritative for message history, presentation and search.
    pub(crate) async fn restore_minimal_thread_routing_once(&self) -> Result<usize, NexusError> {
        if !self.store.has_split_authority() {
            return Ok(0);
        }
        let routing = RoutingThreads::new(&self.store);
        let threads = Threads::new(&self.store);
        let mut restored = 0;
        for route in routing.list_active().await? {
            if threads.find_any_by_name(&route.name).await?.is_none() {
                threads
                    .create(
                        &route.thread_id,
                        &route.name,
                        &route.project,
                        route.created_by.as_deref().unwrap_or("nexus"),
                    )
                    .await?;
            }
            for member in routing.members(&route.thread_id).await? {
                threads
                    .restore_member_ref(
                        &route.thread_id,
                        &member.session_name,
                        member.agent_id.as_deref(),
                    )
                    .await?;
            }
            restored += 1;
        }
        Ok(restored)
    }
}
