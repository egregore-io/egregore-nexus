//! Cross-pass producer identity + duplicate suppression for native harness forwarders.
//!
//! The headed native forwarders each read agent output in two surfaces: a *streamed* surface
//! (incremental deltas — claude `MessageDisplay`, codex `agentMessage/delta`, opencode text parts)
//! and a *final* surface (a full snapshot of the same message — claude transcript assistant rows,
//! codex `item/completed`, opencode final rows). When a message is emitted incrementally and then
//! re-appears as a final snapshot, the forwarder must emit it exactly once.
//!
//! Today each forwarder solves this differently and one (claude) does it wrong: it compares *text*
//! within a single poll pass, so a streamed message in pass N and its final row in pass N+1 escape
//! the check and render twice. This module lifts the correct, id-keyed, *cross-pass* model (codex's
//! `streamed_agent_messages` set + claude's `streamed_message_ids` sidecar column, generalized) into
//! one shared tracker keyed on a stable producer id, so every adapter suppresses the same way.

use std::collections::{HashMap, HashSet, VecDeque};

/// Which surface a producer record arrived on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Surface {
    /// An incremental delta (live streamed text). Always emitted; records the id as pending.
    Streamed,
    /// A full final snapshot of a message. Suppressed iff the id was already streamed.
    Final,
}

/// Persistence for streamed-but-not-yet-finalized producer ids, keyed `(runtime_id, producer_id)`.
///
/// The in-memory implementation ([`InMemoryProducerIdentityStore`]) backs tests and non-durable
/// callers; the durable implementation (a `(runtime_id, producer_id)` table) lives in `nexus-store`
/// and is what makes suppression survive a forwarder restart. Both bound the pending set per runtime
/// so a streamed id whose final snapshot never arrives cannot grow unbounded.
pub trait ProducerIdentityStore {
    /// Record `producer_id` as pending for `runtime_id`. Bounded per runtime (oldest evicted).
    fn insert(&mut self, runtime_id: &str, producer_id: &str);
    /// Remove a pending id; returns `true` if it was present (→ suppress the final snapshot).
    fn remove(&mut self, runtime_id: &str, producer_id: &str) -> bool;
}

/// Cross-pass duplicate suppressor. Wraps a [`ProducerIdentityStore`] and decides, per record,
/// whether the forwarder should emit it.
pub struct ProducerIdentity<S> {
    store: S,
}

impl<S: ProducerIdentityStore> ProducerIdentity<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }

    /// Decide whether to emit a record for `(runtime_id, producer_id)` arriving on `surface`.
    ///
    /// - `Streamed` → always emit (it is the live text), and record the id as pending.
    /// - `Final` → emit only if the id was **not** already streamed; a match suppresses the
    ///   duplicate and one-shot-removes the pending id.
    pub fn admit(&mut self, runtime_id: &str, producer_id: &str, surface: Surface) -> bool {
        match surface {
            Surface::Streamed => {
                self.store.insert(runtime_id, producer_id);
                true
            }
            Surface::Final => !self.store.remove(runtime_id, producer_id),
        }
    }
}

/// In-memory bounded store: per-runtime FIFO of pending streamed ids, capped at `cap`.
pub struct InMemoryProducerIdentityStore {
    cap: usize,
    per_runtime: HashMap<String, (VecDeque<String>, HashSet<String>)>,
}

impl InMemoryProducerIdentityStore {
    pub fn new(cap: usize) -> Self {
        Self {
            cap,
            per_runtime: HashMap::new(),
        }
    }
}

impl ProducerIdentityStore for InMemoryProducerIdentityStore {
    fn insert(&mut self, runtime_id: &str, producer_id: &str) {
        let (order, set) = self
            .per_runtime
            .entry(runtime_id.to_string())
            .or_insert_with(|| (VecDeque::new(), HashSet::new()));
        if !set.insert(producer_id.to_string()) {
            return; // already pending; don't duplicate or reorder
        }
        order.push_back(producer_id.to_string());
        while order.len() > self.cap {
            if let Some(evicted) = order.pop_front() {
                set.remove(&evicted);
            }
        }
    }

    fn remove(&mut self, runtime_id: &str, producer_id: &str) -> bool {
        let Some((order, set)) = self.per_runtime.get_mut(runtime_id) else {
            return false;
        };
        if !set.remove(producer_id) {
            return false;
        }
        if let Some(pos) = order.iter().position(|id| id == producer_id) {
            order.remove(pos);
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const RT: &str = "s_runtime_1";

    fn tracker() -> ProducerIdentity<InMemoryProducerIdentityStore> {
        ProducerIdentity::new(InMemoryProducerIdentityStore::new(64))
    }

    /// The core cross-pass property (linus's claude fix, generalized): a message streamed in one
    /// pass and finalized in a LATER pass is emitted once — the final snapshot is suppressed by id.
    #[test]
    fn streamed_then_final_in_a_later_pass_suppresses_the_final() {
        let mut t = tracker();
        // pass N: streamed delta for msg1 → emitted.
        assert!(t.admit(RT, "msg1", Surface::Streamed));
        // pass N+1: the final transcript/snapshot for the SAME id → suppressed.
        assert!(!t.admit(RT, "msg1", Surface::Final));
    }

    /// The deliberate non-suppression case: same text under a DIFFERENT id is a real second message
    /// and must be emitted (proves we key on id, not text).
    #[test]
    fn final_with_a_different_id_is_emitted() {
        let mut t = tracker();
        assert!(t.admit(RT, "msg1", Surface::Streamed));
        assert!(t.admit(RT, "msg2", Surface::Final));
    }

    /// A message that only ever appears as a final snapshot (never streamed) must be emitted.
    #[test]
    fn final_without_a_prior_stream_is_emitted() {
        let mut t = tracker();
        assert!(t.admit(RT, "only_final", Surface::Final));
    }

    /// Suppression is one-shot: a second final snapshot for the same id (a re-emit) is NOT
    /// suppressed again — the pending id was consumed by the first final.
    #[test]
    fn suppression_is_one_shot() {
        let mut t = tracker();
        assert!(t.admit(RT, "msg1", Surface::Streamed));
        assert!(!t.admit(RT, "msg1", Surface::Final)); // suppressed, id consumed
        assert!(t.admit(RT, "msg1", Surface::Final)); // no longer pending → emitted
    }

    /// Identity is scoped per runtime: a streamed id for one runtime does not suppress a final for
    /// the same id under a different runtime.
    #[test]
    fn identity_is_scoped_per_runtime() {
        let mut t = tracker();
        assert!(t.admit("s_a", "msg1", Surface::Streamed));
        assert!(t.admit("s_b", "msg1", Surface::Final));
    }

    /// The pending set is bounded: streaming more than `cap` ids without finals evicts the oldest,
    /// so an orphaned streamed id (whose final never arrives) cannot grow memory without bound. The
    /// evicted id's late final is then emitted (acceptable: bounded memory over perfect suppression).
    #[test]
    fn pending_set_is_bounded_and_evicts_oldest() {
        let mut store = InMemoryProducerIdentityStore::new(2);
        store.insert(RT, "a");
        store.insert(RT, "b");
        store.insert(RT, "c"); // evicts "a"
        assert!(!store.remove(RT, "a"), "oldest was evicted → not pending");
        assert!(store.remove(RT, "b"));
        assert!(store.remove(RT, "c"));
    }
}
