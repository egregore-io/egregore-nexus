//! `nexus-agent` — the agent transport layer (backend spec §2.3, §5, §6, §11).
//!
//! Turns in, replies out. This crate owns the runtime-agnostic [`adapter::Adapter`] seam and its
//! [`AdapterRegistry`] (built-in `hermes`/`opencode` factories + the test [`adapter::MockAdapter`];
//! Claude and Codex are wired by the composition root via their harness crates),
//! the [`Launcher`] that spawns/attaches a harness and opens its ACP session, and the [`Agent`]
//! service implementing [`nexus_contracts::AgentTurnExecutionPort`].
//!
//! The core mechanic: a drained [`nexus_contracts::NexusBatch`] is injected as **one** ACP
//! `session/prompt` turn — bus traffic wrapped in `<nexus-batch>` (`render_batch`), the core user's
//! single DM delivered **plain** (`is_plain_user_dm`, spec §6) — and the agent's reply is relayed
//! back as ordered `agent.update` events. An adapter init failure surfaces as `agent.status`
//! errored with the session retained for retry (spec §11), never silently dropped.

pub mod active_turns;
pub mod adapter;
pub mod error;
pub mod launcher;
pub mod registry;
pub mod service;

pub use active_turns::ActiveTurnTracker;
pub use adapter::engine::LaunchCtx;
pub use adapter::hermes::skill::write_hermes_mcp_config;
pub use adapter::opencode::harness::write_opencode_mcp_config;
pub use adapter::{
    Adapter, AdapterInjectError, AdapterProviderError, AdapterProviderLimit, HermesAdapter,
    MockAdapter, StreamEvent,
};
pub use error::AgentError;
pub use launcher::Launcher;
pub use registry::{AdapterFactory, AdapterRegistry};
pub use service::Agent;
