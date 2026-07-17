//! Notification-source registry and push wire types (backend §notification-sources).
//!
//! A **notification source** is a named, token-authenticated producer that pushes events onto the
//! bus. These types cover source registration, lifecycle management, and the push path.
//! Authentication headers are carried out-of-band (see the header constants below).

use serde::{Deserialize, Serialize};
use typeshare::typeshare;

use crate::ids::MessageId;

/// HTTP header carrying the HMAC signature over the raw `POST /push` body.
pub const SOURCE_SIGNATURE_HEADER: &str = "X-Nexus-Signature";

/// HTTP header carrying the Unix-millisecond timestamp of the push request (replay protection).
pub const SOURCE_TIMESTAMP_HEADER: &str = "X-Nexus-Timestamp";

/// A registered notification source.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Source {
    /// Unique human-readable name for this source (e.g. `"github-ci"`).
    pub name: String,
    /// Default topic pushes from this source are routed to.
    pub topic: String,
    /// Whether the source is currently active.
    pub enabled: bool,
    /// Unix milliseconds when the source was registered.
    #[typeshare(serialized_as = "number")]
    pub created_at: i64,
    /// Unix milliseconds of the most recent successful push; absent if never fired.
    #[typeshare(serialized_as = "Option<number>")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_fired_at: Option<i64>,
}

/// Request payload for the `"source.register"` method/command. Registers a new notification source.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SourceRegisterRequest {
    /// Desired name for the source — must be unique within the workspace.
    pub name: String,
    /// Override the default topic; absent → daemon assigns a default.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
}

/// Result of a successful `"source.register"` call. `token` is returned in plaintext once.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SourceRegisterResponse {
    /// The newly-created source record.
    pub source: Source,
    /// Plaintext bearer token — shown once; store it securely.
    pub token: String,
}

/// A minimal source reference used by show / enable / disable / rotate / remove operations.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SourceRef {
    pub name: String,
}

/// Response body for `"source.list"`.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SourceListResponse {
    pub sources: Vec<Source>,
}

/// Response body for `"source.rotate"` — returns the new token in plaintext, once.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SourceTokenResponse {
    pub name: String,
    pub token: String,
}

/// `POST /push` body. Sent by an authenticated source to push an event onto the bus.
/// The source token is carried via [`SOURCE_SIGNATURE_HEADER`]; it is NOT a JSON field.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PushRequest {
    /// Name of the registered source making the push.
    pub source: String,
    /// Override topic for this push; absent → source's default topic.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    /// Short human-readable summary (used as notification title).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    /// Full event body (rendered to subscribers).
    pub body: String,
    /// Opaque producer metadata (stored, not interpreted by the bus).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<serde_json::Value>,
}

/// Receipt for a committed source push. Fan-out means queued recipient rows, not model delivery.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PushResponse {
    /// The resolved topic the push was routed to.
    pub topic: String,
    /// Canonical message id, absent only when the topic has no subscribers and no message is made.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_id: Option<MessageId>,
    /// Number of committed recipient delivery rows. This does not claim harness completion.
    #[serde(alias = "deliveredTo")]
    pub queued_to: u32,
}
