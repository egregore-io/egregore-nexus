//! The Pub monitor feed (backend §7). Every ingested, verified notification is **always**
//! appended to the `pub` topic — the frontend "Pub monitor" renders the live stream so a human
//! sees every notification arrive.
//!
//! **Pub = monitor, routing = dispatch.** Appending to Pub is a publish to a topic; it fans out
//! only to whoever explicitly *subscribes* to `pub` (a monitor view), and is deliberately distinct
//! from the route-by-source/topic *dispatch* set ([`crate::routing`]). Landing in Pub never, by
//! itself, pushes a notification into an agent's turn path.

/// The default Pub-monitor topic name (backend §7: "default `pub`").
pub const PUB_TOPIC: &str = "pub";

/// Resolve the Pub-feed topic for a notification. v4 keeps a single default `pub` monitor; a
/// `source`/`topic`-named feed is a future config knob (§7), so this is the seam for it.
pub(crate) fn pub_topic() -> &'static str {
    PUB_TOPIC
}
