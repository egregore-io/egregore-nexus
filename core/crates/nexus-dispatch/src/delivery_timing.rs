//! Harness-neutral scheduling decisions for accepted Message Post deliveries.
//!
//! This module uses only declared adapter capabilities and authoritative turn state. It never
//! infers a boundary from rendered text, tool names, or a count of tool calls.

use nexus_contracts::{DeliveryTiming, SteerCapability};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryAction {
    StartTurn,
    NativeSteer,
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
    if !turn_active {
        return DeliveryAction::StartTurn;
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
