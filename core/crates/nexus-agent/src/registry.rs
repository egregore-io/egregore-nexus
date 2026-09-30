//! The adapter registry (borrowed from Paperclip's pluggable pattern, backend spec §5): each
//! [`nexus_contracts::HarnessId`] maps to a factory that mints an [`Adapter`]. Built-ins cover
//! `hermes` and `opencode` over the SAME uniform ACP transport, plus the
//! [`crate::adapter::MockAdapter`] used by tests/acceptance. Claude and Codex are wired by the
//! composition root via their harness crates. New runtimes register a new factory.

use std::collections::HashMap;
use std::sync::Arc;

use nexus_contracts::HarnessId;

use crate::adapter::engine::LaunchCtx;
use crate::adapter::{Adapter, AdapterModelReportingProfile, HermesAdapter, OpenCodeAdapter};
use crate::error::AgentError;

/// A factory that mints a fresh [`Adapter`] for one launch, given the [`LaunchCtx`] (working
/// directory + the per-agent env that lets the agent's shell `nexus` CLI act as itself).
pub type AdapterFactory = Arc<dyn Fn(LaunchCtx) -> Arc<dyn Adapter> + Send + Sync>;

#[derive(Clone)]
struct AdapterRegistration {
    factory: AdapterFactory,
    profile: Option<AdapterModelReportingProfile>,
}

/// A factory/profile pair captured without constructing an adapter or looking it up again.
/// Consuming this non-cloneable capture permits one instantiation per selection, not global
/// uniqueness of the underlying factory. It does not establish daemon-owned launch freshness.
pub struct PreparedAdapterFactory {
    factory: AdapterFactory,
    profile: Option<AdapterModelReportingProfile>,
}

impl PreparedAdapterFactory {
    pub fn profile(&self) -> Option<&AdapterModelReportingProfile> {
        self.profile.as_ref()
    }

    /// Construct from the captured factory, even if the registry has since replaced its entry.
    pub fn instantiate(self, mut ctx: LaunchCtx) -> Arc<dyn Adapter> {
        // A caller cannot smuggle a different selection's observer through the legacy route.
        ctx.model_reporting = None;
        (self.factory)(ctx)
    }

    /// Pair the captured profile and observer before ANY factory/constructor side effects.
    /// Correspondence is not a native launch or runtime-liveness admission check.
    pub fn instantiate_observed(
        self,
        mut ctx: LaunchCtx,
        sink: Arc<dyn nexus_contracts::model_report::ModelObservationSink>,
    ) -> Result<Arc<dyn Adapter>, AgentError> {
        if ctx.model_reporting.is_some() {
            return Err(AgentError::Adapter(
                "launch context already contains reporting".into(),
            ));
        }
        let profile = self.profile.ok_or_else(|| {
            AgentError::Adapter("selected adapter has no reporting profile".into())
        })?;
        if !sink.accepts_profile(profile.identity()) {
            return Err(AgentError::Adapter(
                "selected adapter profile does not match an open observer".into(),
            ));
        }
        ctx.model_reporting = Some(crate::adapter::AdapterModelReporting::captured(
            profile, sink,
        ));
        Ok((self.factory)(ctx))
    }
}

/// Maps each [`nexus_contracts::HarnessId`] to its adapter factory. Wired once at startup by the
/// binary's `AppState`; tests register a [`crate::adapter::MockAdapter`] factory for the harness they
/// drive. Keys are the raw harness id tokens — the id IS the stable string key.
#[derive(Clone, Default)]
pub struct AdapterRegistry {
    factories: HashMap<String, AdapterRegistration>,
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
        reg.register_observed(
            &HarnessId::new("opencode").expect("builtin harness id is valid"),
            Arc::new(|ctx| Arc::new(OpenCodeAdapter::new(ctx)) as Arc<dyn Adapter>),
            OpenCodeAdapter::model_reporting_profile(),
        );
        reg.register_observed(
            &HarnessId::new("hermes").expect("builtin harness id is valid"),
            Arc::new(|ctx| Arc::new(HermesAdapter::new(ctx)) as Arc<dyn Adapter>),
            HermesAdapter::model_reporting_profile(),
        );
        reg
    }

    /// Register (or replace) the factory for a harness, clearing any previous reporting profile.
    pub fn register(&mut self, harness: &HarnessId, factory: AdapterFactory) {
        self.factories.insert(
            harness.as_str().to_owned(),
            AdapterRegistration {
                factory,
                profile: None,
            },
        );
    }

    /// Register (or replace) a factory and its validated, immutable reporting profile together.
    /// Registration alone does not enable model reporting.
    pub fn register_observed(
        &mut self,
        harness: &HarnessId,
        factory: AdapterFactory,
        profile: AdapterModelReportingProfile,
    ) {
        self.factories.insert(
            harness.as_str().to_owned(),
            AdapterRegistration {
                factory,
                profile: Some(profile),
            },
        );
    }

    /// Capture the current factory/profile pair without invoking the factory. Later replacement
    /// cannot change this selection; missing harnesses retain the legacy [`Self::get`] error.
    pub fn select(&self, harness: &HarnessId) -> Result<PreparedAdapterFactory, AgentError> {
        self.factories
            .get(harness.as_str())
            .map(|registration| PreparedAdapterFactory {
                factory: registration.factory.clone(),
                profile: registration.profile.clone(),
            })
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

    /// Mint a fresh adapter for `harness` with the given [`LaunchCtx`] (cwd + per-agent env), or
    /// error if none is registered.
    pub fn get(&self, harness: &HarnessId, ctx: LaunchCtx) -> Result<Arc<dyn Adapter>, AgentError> {
        Ok(self.select(harness)?.instantiate(ctx))
    }

    /// True if a factory is registered for `harness`.
    pub fn has(&self, harness: &HarnessId) -> bool {
        self.factories.contains_key(harness.as_str())
    }
}
