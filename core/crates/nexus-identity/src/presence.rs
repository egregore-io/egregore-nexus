//! Presence + heartbeat-staleness helpers (backend spec §8, §2.4). Presence is persisted as a
//! lowercase string on `sessions.presence`; this module is the single place that maps between the
//! store string, the [`Presence`] enum, and the self-set [`StatusState`].

use nexus_contracts::enums::Presence;
use nexus_contracts::register::StatusState;

/// Parse a stored presence string back into [`Presence`]. Unknown/`None` → `Offline` (the safe
/// default for a row with no presence written). Thin delegation to the canonical
/// [`nexus_common::presence::presence_from_token`].
pub(crate) fn presence_from_str(s: Option<&str>) -> Presence {
    nexus_common::presence::presence_from_token(s)
}

/// Map a self-set [`StatusState`] to the presence it implies. `Paused` keeps the agent reachable
/// (still `online`) — the *hold* is the separate `paused` flag, not an offline presence (§2.4:
/// paused is an explicit hold, distinct from offline where the loop has detached).
pub(crate) fn presence_for_state(state: StatusState) -> Presence {
    match state {
        StatusState::Active => Presence::Online,
        StatusState::Busy => Presence::Busy,
        StatusState::Paused => Presence::Online,
    }
}

/// Is a heartbeat stale? `None` (never beat) counts as stale. A row is stale once
/// `now - last_heartbeat > ttl_ms`. Thin delegation to the canonical
/// [`nexus_common::presence::is_stale`].
pub(crate) fn is_stale(last_heartbeat: Option<i64>, now: i64, ttl_ms: i64) -> bool {
    nexus_common::presence::is_stale(last_heartbeat, now, ttl_ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn presence_roundtrips_through_strings() {
        for p in [Presence::Online, Presence::Busy, Presence::Offline] {
            assert_eq!(
                presence_from_str(Some(nexus_common::presence::presence_token(p))),
                p
            );
        }
        assert_eq!(presence_from_str(None), Presence::Offline);
        assert_eq!(presence_from_str(Some("bogus")), Presence::Offline);
    }

    #[test]
    fn paused_stays_online() {
        assert_eq!(presence_for_state(StatusState::Paused), Presence::Online);
        assert_eq!(presence_for_state(StatusState::Active), Presence::Online);
        assert_eq!(presence_for_state(StatusState::Busy), Presence::Busy);
    }

    #[test]
    fn staleness_respects_ttl() {
        assert!(is_stale(None, 1000, 30));
        assert!(is_stale(Some(900), 1000, 30)); // 100 > 30
        assert!(!is_stale(Some(990), 1000, 30)); // 10 <= 30
    }
}
