//! Strict command parser shared by the fake server and its focused tests.

#[derive(Debug, PartialEq, Eq)]
pub(super) enum SteerGateAction {
    Abort,
    Consume,
}

pub(super) fn steer_gate_action(bytes: &[u8]) -> Option<SteerGateAction> {
    match bytes {
        b"abort" => Some(SteerGateAction::Abort),
        b"consume" => Some(SteerGateAction::Consume),
        _ => None,
    }
}
