use nexus_contracts::{DeliveryTiming, SteerCapability};
use nexus_dispatch::{delivery_action, DeliveryAction, DeliveryTimingError};

#[test]
fn idle_targets_start_normally_for_every_public_timing() {
    for timing in [
        DeliveryTiming::Interrupt,
        DeliveryTiming::YieldTurn,
        DeliveryTiming::AfterToolLoop,
    ] {
        assert_eq!(
            delivery_action(timing, false, SteerCapability::None),
            Ok(DeliveryAction::StartTurn),
        );
    }
}

#[test]
fn interrupt_uses_native_steer_then_atomic_interrupt_and_send() {
    assert_eq!(
        delivery_action(
            DeliveryTiming::Interrupt,
            true,
            SteerCapability::NativeSteer,
        ),
        Ok(DeliveryAction::NativeSteer),
    );
    assert_eq!(
        delivery_action(
            DeliveryTiming::Interrupt,
            true,
            SteerCapability::InterruptAndSend,
        ),
        Ok(DeliveryAction::InterruptAndSend),
    );
    assert_eq!(
        delivery_action(DeliveryTiming::Interrupt, true, SteerCapability::None,),
        Err(DeliveryTimingError::InterruptUnsupported),
    );
}

#[test]
fn yield_and_tool_loop_policies_wait_for_an_authoritative_boundary() {
    for capability in [
        SteerCapability::NativeSteer,
        SteerCapability::InterruptAndSend,
        SteerCapability::None,
    ] {
        assert_eq!(
            delivery_action(DeliveryTiming::YieldTurn, true, capability),
            Ok(DeliveryAction::WaitForTurnBoundary),
        );
        assert_eq!(
            delivery_action(DeliveryTiming::AfterToolLoop, true, capability),
            Ok(DeliveryAction::WaitForFinalTurnCompletion),
        );
    }
}
