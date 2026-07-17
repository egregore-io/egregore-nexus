//! The single canonical home for presence/staleness logic.
//!
//! Presence is persisted as a lowercase string (`sessions.presence`,
//! `agent_runtimes.presence`) matching the `#[serde(rename_all = "lowercase")]` on
//! [`Presence`]. Heartbeat staleness is a wall-clock TTL comparison. Every crate
//! (daemon, CLI, store, identity) calls the helpers here rather than re-deriving them, so the
//! rule lives in exactly one place.

use nexus_contracts::enums::Presence;

/// The single canonical staleness predicate: is a heartbeat stale relative to `now`?
///
/// `None` (never beat) → `true` (treated as stale). Otherwise a row is stale once
/// `now - last_heartbeat > ttl_ms`.
///
/// This is a **wall-clock** comparison, which has two known skew behaviors:
/// - A *backward* clock skew (a heartbeat stamped in the future, `hb > now`) yields a negative
///   delta, so the row reads as fresh — the **safe** direction (an agent is never wrongly marked
///   offline just because clocks disagree).
/// - A large *forward* daemon-clock jump can push `now` far ahead of every stored heartbeat and
///   mass-false-offline live agents — the **unsafe** direction.
///
/// This wall-clock limitation is deferred to R2.1b (monotonic / transport-derived presence).
pub fn is_stale(last_heartbeat: Option<i64>, now: i64, ttl_ms: i64) -> bool {
    match last_heartbeat {
        Some(hb) => now - hb > ttl_ms,
        None => true,
    }
}

/// The lowercase wire/store token for a [`Presence`] (matches its `serde` lowercase rename).
pub fn presence_token(p: Presence) -> &'static str {
    match p {
        Presence::Online => "online",
        Presence::Busy => "busy",
        Presence::Offline => "offline",
    }
}

/// Parse a stored presence token back into [`Presence`]. `Some("online")` → `Online`,
/// `Some("busy")` → `Busy`; any other value, including `None` and unknown tokens, → `Offline`
/// (the safe default for a row with no/unrecognized presence written).
pub fn presence_from_token(s: Option<&str>) -> Presence {
    match s {
        Some("online") => Presence::Online,
        Some("busy") => Presence::Busy,
        _ => Presence::Offline,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn never_beat_is_stale() {
        assert!(is_stale(None, 1000, 30));
    }

    #[test]
    fn heartbeat_far_past_is_stale() {
        assert!(is_stale(Some(900), 1000, 30)); // delta 100 > 30
    }

    #[test]
    fn recent_heartbeat_is_fresh() {
        assert!(!is_stale(Some(990), 1000, 30)); // delta 10 <= 30
    }

    #[test]
    fn future_heartbeat_is_skew_safe_and_fresh() {
        // hb in the FUTURE (hb > now) → negative delta → reads as fresh (safe direction).
        assert!(!is_stale(Some(5000), 1000, 30));
    }

    #[test]
    fn presence_token_maps_each_variant() {
        assert_eq!(presence_token(Presence::Online), "online");
        assert_eq!(presence_token(Presence::Busy), "busy");
        assert_eq!(presence_token(Presence::Offline), "offline");
    }

    #[test]
    fn presence_roundtrips_through_token() {
        for p in [Presence::Online, Presence::Busy, Presence::Offline] {
            assert_eq!(presence_from_token(Some(presence_token(p))), p);
        }
    }

    #[test]
    fn unknown_and_none_token_default_to_offline() {
        assert_eq!(presence_from_token(None), Presence::Offline);
        assert_eq!(presence_from_token(Some("bogus")), Presence::Offline);
    }
}
