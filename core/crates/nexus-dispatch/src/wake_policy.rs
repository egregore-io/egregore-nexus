//! The autonomous wake policy — the §2.4 state machine, the one policy that matters.
//!
//! Nexus relaxes AionCore's human-gated delivery: peers wake each other autonomously, no human in
//! the loop. An **idle** agent IS woken by a peer (no human gate). **busy** rings the same loop;
//! native-steer transports may admit the batch into the active context, while other transports
//! retain turn-end coalescing. **paused** is the only explicit hold (human- or self-set) — it holds
//! even a human's message until unpaused.
//! **provider-limited** and **offline** hold (the row stays `pending`, re-driven when the hold
//! clears or the loop attaches). See spec §2.4.

use nexus_contracts::Kind;

/// The recipient agent's current state (spec §2.4 rows).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentState {
    /// No turn in flight; ready to be woken.
    Idle,
    /// A turn is in flight — native steer may admit arrivals now; other transports coalesce.
    Busy,
    /// Explicit hold (human- or self-set). Only a resume lifts it.
    Paused,
    /// Harness/provider reported a structured retry/operator-action condition. Rows stay pending.
    ProviderLimited,
    /// No live loop attached. Rows stay `pending`, re-driven on attach.
    Offline,
}

/// What the bus should do with an inbound message given the recipient's [`AgentState`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WakeDecision {
    /// Ring the bell now → the loop drains and injects a turn.
    Wake,
    /// Ring the busy loop without starting a concurrent turn. Native-steer transports may drain
    /// into the active context; other transports leave the row for the turn-end drain.
    Coalesce,
    /// Leave the row queued; do not force a turn (paused/provider-limited/offline).
    Hold,
}

/// The wake policy — pure decision table, no state of its own.
pub struct WakePolicy;

impl WakePolicy {
    /// Decide what to do with an inbound message of `source` kind for an agent in `state`
    /// (the exact spec §2.4 table).
    ///
    /// - **idle** → always [`WakeDecision::Wake`] (the Nexus relaxation: a peer wakes an idle
    ///   agent with no human gate).
    /// - **busy** → always [`WakeDecision::Coalesce`] (a mid-turn arrival never starts a concurrent
    ///   turn; the event loop may use non-destructive native steer).
    /// - **paused** → always [`WakeDecision::Hold`] (paused holds even a human; a human *resumes*,
    ///   they do not punch through the hold).
    /// - **provider-limited** → always [`WakeDecision::Hold`] (stays `pending`, re-driven when
    ///   the hold clears).
    /// - **offline** → always [`WakeDecision::Hold`] (stays `pending`, re-driven on attach).
    ///
    /// The `source` [`Kind`] does not change the decision under the current table — it is part of
    /// the signature so future per-source routing (e.g. notification subscription gating) can be
    /// added without a contract change.
    pub fn should_wake(state: AgentState, source: Kind) -> WakeDecision {
        let _ = source;
        match state {
            AgentState::Idle => WakeDecision::Wake,
            AgentState::Busy => WakeDecision::Coalesce,
            AgentState::Paused => WakeDecision::Hold,
            AgentState::ProviderLimited => WakeDecision::Hold,
            AgentState::Offline => WakeDecision::Hold,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_contracts::Kind;

    #[test]
    fn idle_peer_wakes_with_no_human_gate() {
        // The Nexus relaxation vs AionCore (spec §2.4): an idle agent IS woken by a peer.
        assert_eq!(
            WakePolicy::should_wake(AgentState::Idle, Kind::Agent),
            WakeDecision::Wake
        );
    }
    #[test]
    fn busy_coalesces() {
        assert_eq!(
            WakePolicy::should_wake(AgentState::Busy, Kind::Agent),
            WakeDecision::Coalesce
        );
    }
    #[test]
    fn paused_holds_even_for_human() {
        assert_eq!(
            WakePolicy::should_wake(AgentState::Paused, Kind::Human),
            WakeDecision::Hold
        );
    }
    #[test]
    fn offline_holds() {
        assert_eq!(
            WakePolicy::should_wake(AgentState::Offline, Kind::Agent),
            WakeDecision::Hold
        );
    }

    #[test]
    fn provider_limited_holds() {
        assert_eq!(
            WakePolicy::should_wake(AgentState::ProviderLimited, Kind::Agent),
            WakeDecision::Hold
        );
    }

    #[test]
    fn idle_wakes_for_human_and_notification_too() {
        assert_eq!(
            WakePolicy::should_wake(AgentState::Idle, Kind::Human),
            WakeDecision::Wake
        );
        assert_eq!(
            WakePolicy::should_wake(AgentState::Idle, Kind::Notification),
            WakeDecision::Wake
        );
    }
}
