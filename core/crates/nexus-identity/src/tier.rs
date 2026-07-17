//! Privilege-tier gating (backend spec §8). The only ordering that matters: `Agent < Admin`.
//! The human user sits *above* both tiers and is not represented here (the daemon authenticates
//! the human out of band).

use nexus_common::NexusError;
use nexus_contracts::enums::Tier;
use nexus_contracts::ports::Caller;

/// Rank a tier for comparison. `Agent = 0 < Admin = 1`.
fn rank(t: Tier) -> u8 {
    match t {
        Tier::Agent => 0,
        Tier::Admin => 1,
    }
}

/// Gate a call on the caller's tier. Returns [`NexusError::Unauthorized`] if
/// `caller.tier < required`; `Ok(())` when the caller meets or exceeds `required`.
///
/// This is the single authorization primitive admin commands wrap (backend §8: admin = extra
/// commands, gated by tier — never insertion into the message path).
pub fn tier_guard(caller: &Caller, required: Tier) -> Result<(), NexusError> {
    if rank(caller.tier) >= rank(required) {
        Ok(())
    } else {
        Err(NexusError::Unauthorized)
    }
}
