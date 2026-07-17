//! External notification push-in (backend §7). Public v0.1 signs
//! `"<X-Nexus-Timestamp>.<raw-body>"` and carries the digest in `X-Nexus-Signature`. The gateway
//! verifies before enqueueing [`NotifyCommandRequest`], and the daemon worker re-verifies the
//! durable envelope before routing. Direct daemon [`NotifyRequest`] dispatch stays unverified.

use serde::{Deserialize, Serialize};
use typeshare::typeshare;

use crate::ids::{AgentId, MessageId};

/// One-shot notification target. `Auto` resolves an unqualified CLI value at daemon ingress;
/// explicit variants remove ambiguity without exposing the derived recipient fan-out.
#[typeshare(serialized_as = "NotifyTargetWire")]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum NotifyTarget {
    Auto {
        value: String,
    },
    Agent {
        #[serde(rename = "agentId")]
        agent_id: AgentId,
    },
    Name {
        name: String,
    },
    Group {
        group: String,
    },
    Thread {
        thread: String,
    },
}

/// Daemon-owned one-shot notification command. The body is stored once and recipient delivery
/// state is derived inside the daemon. `source` is attribution, never authentication.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NotifySendRequest {
    pub target: NotifyTarget,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    pub body: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
}

/// The HTTP header carrying the public v0.1 timestamped HMAC (backend §7). The gateway rejects a
/// bad or missing signature before command ingress; it never reaches notification audit or routing.
pub const NOTIFY_SIGNATURE_HEADER: &str = "X-Nexus-Signature";

/// Header carrying the Unix-millisecond timestamp included in the public v0.1 signature payload.
pub const NOTIFY_TIMESTAMP_HEADER: &str = "X-Nexus-Timestamp";

/// `POST /notify` body. `payload` is the opaque producer JSON (stored, rendered to subscribers).
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NotifyRequest {
    /// Producer source label (CI, GitHub, cron, …).
    pub source: String,
    /// Optional topic; absent → Pub feed only (no agent path).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    /// Opaque producer payload.
    pub payload: serde_json::Value,
}

/// Durable command-intent envelope produced only after the public gateway verifies a notification.
///
/// The daemon deliberately stores and re-verifies the exact signed bytes instead of trusting a
/// caller-supplied Boolean. `timestamp` remains a string so signature verification never rewrites
/// the producer's bytes. The worker parses `raw_body` into [`NotifyRequest`] only after verification.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NotifyCommandRequest {
    pub raw_body: String,
    pub timestamp: String,
    pub signature: String,
}

/// Ingest result. `notifId` is the id of the ingested `notification`-kind message.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NotifyResponse {
    pub notif_id: MessageId,
    /// Resolved recipients at ingest (recorded for the web console audit).
    pub routed_to: Vec<String>,
    /// Whether the HMAC verified.
    pub hmac_ok: bool,
}

/// A standing route rule (by source and/or topic) → a recipient name or thread.
/// Reused by admin `channel set-route` (CLI §8). At least one of `source`/`topic` is set.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RouteRule {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    /// Recipient: an agent name or a thread name.
    pub to: String,
}
