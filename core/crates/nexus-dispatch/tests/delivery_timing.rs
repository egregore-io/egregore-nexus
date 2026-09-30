use nexus_contracts::{DeliveryTiming, SteerCapability};
use nexus_dispatch::{delivery_action, delivery_action_with_native_queue, DeliveryAction};

#[test]
fn native_queue_is_opt_in_and_never_overrides_explicit_boundary_timing() {
    for capability in [
        SteerCapability::None,
        SteerCapability::NativeSteer,
        SteerCapability::InterruptAndSend,
    ] {
        for active in [false, true] {
            for timing in [
                DeliveryTiming::Interrupt,
                DeliveryTiming::YieldTurn,
                DeliveryTiming::AfterToolLoop,
            ] {
                assert_eq!(
                    delivery_action_with_native_queue(timing, active, capability, false),
                    delivery_action(timing, active, capability)
                );
                let expected = if active && timing == DeliveryTiming::Interrupt {
                    DeliveryAction::NativeQueue
                } else {
                    delivery_action(timing, active, capability)
                };
                assert_eq!(
                    delivery_action_with_native_queue(timing, active, capability, true),
                    expected
                );
            }
        }
    }
}

#[test]
fn idle_targets_start_normally_for_every_public_timing() {
    for timing in [
        DeliveryTiming::Interrupt,
        DeliveryTiming::YieldTurn,
        DeliveryTiming::AfterToolLoop,
    ] {
        assert_eq!(
            delivery_action(timing, false, SteerCapability::None),
            DeliveryAction::StartTurn,
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
        DeliveryAction::NativeSteer,
    );
    assert_eq!(
        delivery_action(
            DeliveryTiming::Interrupt,
            true,
            SteerCapability::InterruptAndSend,
        ),
        DeliveryAction::InterruptAndSend,
    );
}

/// The repair: a busy agent whose backend cannot be interrupted must WAIT for the turn
/// boundary. Returning an error here dead-lettered the message, so the operator never
/// received a reply at all.
#[test]
fn interrupt_on_busy_uninterruptible_agent_waits_for_boundary() {
    assert_eq!(
        delivery_action(DeliveryTiming::Interrupt, true, SteerCapability::None),
        DeliveryAction::WaitForTurnBoundary,
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
            DeliveryAction::WaitForTurnBoundary,
        );
        assert_eq!(
            delivery_action(DeliveryTiming::AfterToolLoop, true, capability),
            DeliveryAction::WaitForFinalTurnCompletion,
        );
    }
}
