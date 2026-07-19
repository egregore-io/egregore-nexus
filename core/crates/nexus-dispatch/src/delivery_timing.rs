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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryTimingError {
    InterruptUnsupported,
}

/// Resolve one public timing value against authoritative active-turn state.
pub fn delivery_action(
    timing: DeliveryTiming,
    turn_active: bool,
    capability: SteerCapability,
) -> Result<DeliveryAction, DeliveryTimingError> {
    if !turn_active {
        return Ok(DeliveryAction::StartTurn);
    }

    match timing {
        DeliveryTiming::Interrupt => match capability {
            SteerCapability::NativeSteer => Ok(DeliveryAction::NativeSteer),
            SteerCapability::InterruptAndSend => Ok(DeliveryAction::InterruptAndSend),
            SteerCapability::None => Err(DeliveryTimingError::InterruptUnsupported),
        },
        DeliveryTiming::YieldTurn => Ok(DeliveryAction::WaitForTurnBoundary),
        DeliveryTiming::AfterToolLoop => Ok(DeliveryAction::WaitForFinalTurnCompletion),
    }
}
