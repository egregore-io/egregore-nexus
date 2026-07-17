//! Shared dispatch state: the per-session [`AgentState`] registry and the dependency bundles the
//! [`DispatchService`](crate::DispatchService) service and [`EventLoop`](crate::EventLoop) are wired from.
//!
//! Single-consumer-per-session serialization (spec §2.4) is enforced structurally: there is one
//! [`EventLoop`](crate::EventLoop) task per `kind=agent` session, and the loop only ever runs one
//! turn at a time. The [`AgentRegistry`] records each session's current [`AgentState`] so the bus
//! (`enqueue`) can consult the [`WakePolicy`](crate::WakePolicy) before ringing.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use nexus_contracts::ids::SessionId;
use nexus_store::Store;

use crate::bell::Bell;
use crate::wake_policy::AgentState;

/// A thread-safe map of `SessionId -> AgentState`. Defaults an unknown session to
/// [`AgentState::Idle`] (a freshly-attached agent is ready to be woken). Cheap to clone (shared
/// behind an `Arc`).
#[derive(Clone, Default)]
pub struct AgentRegistry {
    states: Arc<Mutex<HashMap<SessionId, AgentState>>>,
}

impl AgentRegistry {
    /// New empty registry.
    pub fn new() -> Self {
        AgentRegistry {
            states: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Current state for a session (defaults to [`AgentState::Idle`]).
    pub fn get(&self, session: &SessionId) -> AgentState {
        *self
            .states
            .lock()
            .expect("state map poisoned")
            .get(session)
            .unwrap_or(&AgentState::Idle)
    }

    /// Set a session's state.
    pub fn set(&self, session: &SessionId, state: AgentState) {
        self.states
            .lock()
            .expect("state map poisoned")
            .insert(session.clone(), state);
    }
}

/// Dependencies shared by the [`DispatchService`](crate::DispatchService) service. Cloneable: every field is an
/// `Arc`/cheaply-clonable handle, so the service and its spawned loops share one set.
#[derive(Clone)]
pub struct ServiceDeps {
    /// The durable store (sole writer). Repos are bound per-call from this handle.
    pub store: Arc<Store>,
    /// The per-session wake bell.
    pub bell: Bell,
    /// The per-session agent-state registry.
    pub registry: AgentRegistry,
    /// Project scope all reads/writes are filtered by.
    pub project: String,
    /// Drain caps (`drain_limit`, `msg_preview_chars`).
    pub drain_limit: u32,
    /// Per-message preview budget.
    pub preview_chars: u32,
}
