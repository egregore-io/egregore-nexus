//! The launcher (backend spec §5): spawn/attach a harness and stand up its ACP session. It only
//! mints the adapter (from the [`AdapterRegistry`]) and opens the session — **self-registration is
//! the agent's job** (a startup hook/skill), and the per-agent event loop is stood up by realtime.
//! This keeps the daemon out of the message path.

use std::sync::Arc;

use nexus_contracts::SpawnRequest;

use crate::adapter::engine::LaunchCtx;
use crate::adapter::Adapter;
use crate::error::AgentError;
use crate::registry::AdapterRegistry;

/// Spawns harnesses by minting their adapter and opening the ACP session.
#[derive(Clone)]
pub struct Launcher {
    registry: AdapterRegistry,
}

impl Launcher {
    /// Build a launcher over the given registry.
    pub fn new(registry: AdapterRegistry) -> Self {
        Self { registry }
    }

    /// Mint the adapter for `req.kind` and open its ACP session. For the live `claude`/`codex`
    /// adapters, `open_session` spawns the harness's ACP bridge, completes the `initialize`
    /// handshake, and opens a `session/new` — so a successful return means a live, prompt-ready
    /// session. The live adapter handle is returned (the caller binds it to the new session and
    /// stands up the loop). An `open_session` failure is propagated so the caller can surface
    /// `agent.status=errored` and retain the session for retry (spec §11).
    pub async fn launch(&self, req: &SpawnRequest) -> Result<Arc<dyn Adapter>, AgentError> {
        let adapter = self.registry.get(
            &req.kind,
            LaunchCtx {
                cwd: req.cwd.clone(),
                ..Default::default()
            },
        )?;
        adapter
            .open_session()
            .await
            .map_err(|e| AgentError::Adapter(e.to_string()))?;
        Ok(adapter)
    }
}
