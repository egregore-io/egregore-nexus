//! # nexus-dispatch
//!
//! The durable-delivery dispatch core of the Nexus daemon (backend spec §2): the wake [`Bell`], the autonomous
//! [`WakePolicy`] (§2.4), the drain-once [`InboxDrainer`] (§2.3.1), the per-agent [`EventLoop`],
//! and the [`DispatchService`] service that implements [`nexus_contracts::DispatchPort`].
//!
//! ## The core mechanic (spec §2)
//!
//! `enqueue` writes a `pending` in-flight row (via the store) **then** rings the recipient's bell.
//! The recipient's long-lived [`EventLoop`] parks on that bell; on wake it consults the
//! [`WakePolicy`], and if the decision is `Wake` it [`InboxDrainer::drain_once`]s the whole pending
//! queue as one [`nexus_contracts::NexusBatch`], injects it as a single turn through the
//! `AgentTurnExecutionPort`, marks the rows delivered, and re-parks. Arrivals mid-turn **coalesce**:
//! the row is written before the bell rings, so they are picked up by the turn-end re-drain (and
//! re-rung on [`EventLoop::spawn`] for crash-safety).

pub mod bell;
pub mod drain;
pub mod error;
pub mod event_loop;
pub mod service;
pub mod state;
pub mod wake_policy;

pub use bell::Bell;
pub use drain::InboxDrainer;
pub use error::{DispatchResult, DispatchResult as RealtimeResult};
pub use event_loop::{
    EventLoop, LoopDeps, DEFAULT_INJECT_COMPLETION_TIMEOUT, DEFAULT_PROVIDER_LIMIT_COOLDOWN,
};
pub use service::{DispatchService, DispatchService as Realtime};
pub use state::{AgentRegistry, ServiceDeps};
pub use wake_policy::{AgentState, WakeDecision, WakePolicy};
