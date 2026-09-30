//! Harness-neutral scheduling decisions for accepted Message Post deliveries.
//!
//! This module uses only declared adapter capabilities and authoritative turn state. It never
//! infers a boundary from rendered text, tool names, or a count of tool calls.

use nexus_contracts::{DeliveryTiming, SteerCapability};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryAction {
    StartTurn,
    NativeSteer,
    NativeQueue,
    InterruptAndSend,
    WaitForTurnBoundary,
    WaitForFinalTurnCompletion,
}

/// Resolve one public timing value against authoritative active-turn state.
///
/// Infallible. A busy target whose backend cannot be interrupted waits for the turn boundary;
/// that is a scheduling outcome, never a delivery failure. Returning an error here dead-lettered
/// the batch, so an operator messaging a busy agent was silently ignored.
pub fn delivery_action(
    timing: DeliveryTiming,
    turn_active: bool,
    capability: SteerCapability,
) -> DeliveryAction {
    delivery_action_with_native_queue(timing, turn_active, capability, false)
}

/// Immediate mail prefers a backend-owned input queue over interrupting native work.
/// Explicit boundary timing remains authoritative; queuing is input acceptance, not completion.
pub fn delivery_action_with_native_queue(
    timing: DeliveryTiming,
    turn_active: bool,
    capability: SteerCapability,
    accepts_queue: bool,
) -> DeliveryAction {
    if !turn_active {
        return DeliveryAction::StartTurn;
    }

    if timing == DeliveryTiming::Interrupt && accepts_queue {
        return DeliveryAction::NativeQueue;
    }

    match timing {
        DeliveryTiming::Interrupt => match capability {
            SteerCapability::NativeSteer => DeliveryAction::NativeSteer,
            SteerCapability::InterruptAndSend => DeliveryAction::InterruptAndSend,
            SteerCapability::None => DeliveryAction::WaitForTurnBoundary,
        },
        DeliveryTiming::YieldTurn => DeliveryAction::WaitForTurnBoundary,
        DeliveryTiming::AfterToolLoop => DeliveryAction::WaitForFinalTurnCompletion,
    }
}
