//! Ingest helpers (backend §7 step 2): turn a verified [`NotifyRequest`] into the durable
//! `notification`-kind message and the synthetic system [`Caller`] used to drive the Pub-feed
//! publish and routed dispatch through the [`BusPort`].
//!
//! A notification has no registered session of its own — it originates outside the mesh. So the
//! service speaks to the bus as a synthetic, admin-tier **system caller** named after the producer
//! `source` (CI, GitHub, cron, …). The stored message carries `Kind::Notification` provenance so
//! the agent-side render is a `<nexus kind="notification" …>` turn (§7: "standard message,
//! standard delivery").
//!
//! [`NotifyRequest`]: nexus_contracts::notify::NotifyRequest
//! [`BusPort`]: nexus_contracts::ports::BusPort

use nexus_contracts::enums::Tier;
use nexus_contracts::ids::SessionId;
use nexus_contracts::notify::NotifyRequest;
use nexus_contracts::ports::Caller;

/// The session id used for the synthetic notification caller. Never a real registered session —
/// the bus only needs a `from`/`project`/`tier` to write provenance and pass the admin gate.
pub(crate) const NOTIFY_SESSION: &str = "s_notify";

/// The project a notification ingests under. The daemon runs one project per mesh in v4; the Pub
/// feed and route targets are resolved within it.
pub(crate) const NOTIFY_PROJECT: &str = "nexus";

/// Build the synthetic system caller for a notification, named after its producer `source`.
/// Admin tier: the bus must not reject the system's own Pub-publish/routed-dispatch as a
/// privilege violation — the *external* HMAC gate already happened upstream of this call.
pub(crate) fn system_caller(req: &NotifyRequest) -> Caller {
    Caller {
        agent_id: None,
        session: SessionId(NOTIFY_SESSION.into()),
        name: req.source.clone(),
        project: NOTIFY_PROJECT.into(),
        tier: Tier::Admin,
        locality: Default::default(),
        access: None,
        principal_id: None,
    }
}

/// Render the notification body for storage/delivery: the opaque producer payload, pretty-ish as
/// compact JSON. Subscribers and the Pub monitor render this verbatim.
pub(crate) fn render_body(req: &NotifyRequest) -> String {
    serde_json::to_string(&req.payload).unwrap_or_else(|_| req.payload.to_string())
}

/// A short index/summary line for the notification (the `source[/topic]` label).
pub(crate) fn render_summary(req: &NotifyRequest) -> String {
    match &req.topic {
        Some(topic) => format!("{}/{}", req.source, topic),
        None => req.source.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req() -> NotifyRequest {
        NotifyRequest {
            source: "ci".into(),
            topic: Some("ci".into()),
            payload: serde_json::json!({ "status": "green" }),
        }
    }

    #[test]
    fn system_caller_is_admin_named_for_source() {
        let c = system_caller(&req());
        assert_eq!(c.name, "ci");
        assert_eq!(c.tier, Tier::Admin);
        assert_eq!(c.session.0, NOTIFY_SESSION);
    }

    #[test]
    fn summary_includes_topic() {
        assert_eq!(render_summary(&req()), "ci/ci");
        let no_topic = NotifyRequest {
            topic: None,
            ..req()
        };
        assert_eq!(render_summary(&no_topic), "ci");
    }
}
