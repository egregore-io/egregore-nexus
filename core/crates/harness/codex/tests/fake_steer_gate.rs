#[path = "support/fake_steer_gate.rs"]
mod steer_gate;

use steer_gate::{steer_gate_action, SteerGateAction};

#[test]
fn incomplete_or_unknown_gate_bytes_are_not_consumption() {
    for bytes in [
        b"".as_slice(),
        b"a",
        b"ab",
        b"abor",
        b"consum",
        b"ready",
        b"unknown",
    ] {
        assert_eq!(steer_gate_action(bytes), None, "gate bytes: {bytes:?}");
    }
}

#[test]
fn only_complete_abort_and_consume_commands_release_gate() {
    assert_eq!(steer_gate_action(b"abort"), Some(SteerGateAction::Abort));
    assert_eq!(
        steer_gate_action(b"consume"),
        Some(SteerGateAction::Consume)
    );
}
