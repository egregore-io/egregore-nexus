//! The adapter registry (borrowed from Paperclip's pluggable pattern, backend spec §5): each
//! [`nexus_contracts::HarnessId`] maps to a factory that mints an [`Adapter`]. Built-ins cover
//! `hermes` and `opencode` over the SAME uniform ACP transport, plus the
//! [`crate::adapter::MockAdapter`] used by tests/acceptance. Claude and Codex are wired by the
//! composition root via their harness crates. New runtimes register a new factory.

use std::collections::HashMap;
use std::sync::Arc;

use nexus_contracts::HarnessId;

use crate::adapter::engine::LaunchCtx;
use crate::adapter::{Adapter, HermesAdapter, OpenCodeAdapter};
use crate::error::AgentError;

/// A factory that mints a fresh [`Adapter`] for one launch, given the [`LaunchCtx`] (working
/// directory + the per-agent env that lets the agent's shell `nexus` CLI act as itself).
pub type AdapterFactory = Arc<dyn Fn(LaunchCtx) -> Arc<dyn Adapter> + Send + Sync>;

/// Maps each [`nexus_contracts::HarnessId`] to its adapter factory. Wired once at startup by the
/// binary's `AppState`; tests register a [`crate::adapter::MockAdapter`] factory for the harness they
/// drive. Keys are the raw harness id tokens — the id IS the stable string key.
#[derive(Clone, Default)]
pub struct AdapterRegistry {
    factories: HashMap<String, AdapterFactory>,
}

impl AdapterRegistry {
    /// An empty registry (no harnesses registered).
    pub fn new() -> Self {
        Self::default()
    }

    /// A registry pre-wired with the **real** built-in `hermes` and `opencode` adapters.
    /// Mock factories are added per-test via [`AdapterRegistry::register`]. Claude and Codex are
    /// NOT included here; they are wired by the composition root via their harness crates.
    pub fn with_builtins() -> Self {
        let mut reg = Self::new();
        reg.register(
            &HarnessId::new("opencode").expect("builtin harness id is valid"),
            Arc::new(|ctx| Arc::new(OpenCodeAdapter::new(ctx)) as Arc<dyn Adapter>),
        );
        reg.register(
            &HarnessId::new("hermes").expect("builtin harness id is valid"),
            Arc::new(|ctx| Arc::new(HermesAdapter::new(ctx)) as Arc<dyn Adapter>),
        );
        reg
    }

    /// Register (or replace) the factory for a harness.
    pub fn register(&mut self, harness: &HarnessId, factory: AdapterFactory) {
        self.factories.insert(harness.as_str().to_owned(), factory);
    }

    /// Mint a fresh adapter for `harness` with the given [`LaunchCtx`] (cwd + per-agent env), or
    /// error if none is registered.
    pub fn get(&self, harness: &HarnessId, ctx: LaunchCtx) -> Result<Arc<dyn Adapter>, AgentError> {
        self.factories
            .get(harness.as_str())
            .map(|f| f(ctx))
            // Reaches users verbatim (via NotFound), so say what is missing instead of
            // returning an ambiguous bare name.
            .ok_or_else(|| {
                AgentError::NoAdapter(format!(
                    "no headless adapter is registered for harness {} on this daemon — \
                     this harness is not supported for headless launch yet",
                    harness
                ))
            })
    }

    /// True if a factory is registered for `harness`.
    pub fn has(&self, harness: &HarnessId) -> bool {
        self.factories.contains_key(harness.as_str())
    }
}
