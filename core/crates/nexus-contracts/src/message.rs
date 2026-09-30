//! The canonical message record + provenance (backend §4 `messages`, §6 provenance).
//!
//! Provenance is two things at once (backend §1 non-goals): the in-band `<nexus …>` tag
//! attributes the agent keys off (`from`/`kind`/`thread`/`topic`), AND a separately stored
//! cryptographic sender stamp recorded for audit (never injected in-band). Both live here so
//! the UI/audit can read the stamp while the daemon renders the tag.

use serde::{Deserialize, Serialize};
use typeshare::typeshare;

use crate::enums::{Kind, Scope};
use crate::ids::{MessageId, ProjectId, ThreadId, TopicId};

/// Cryptographic, verifiable sender stamp stored on every message (backend §1).
/// Not injected in-band; queryable for UI/audit/dispute.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ProvenanceStamp {
    /// Signature algorithm, e.g. "ed25519".
    pub algo: String,
    /// Base64/hex signature over the canonical message bytes.
    pub signature: String,
    /// Unix epoch millis when the stamp was produced.
    #[typeshare(serialized_as = "number")]
    pub signed_at: i64,
}

/// Provenance: the in-band tag attributes + the optional stored crypto stamp.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Provenance {
    /// Sender name (the `from` attr of `<nexus …>`).
    pub from: String,
    /// `agent` | `human` | `notification` (the `kind` attr).
    pub kind: Kind,
    /// Thread NAME (omitted for DMs) — the `thread` attr.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread: Option<String>,
    /// Topic name for pub/sub — the `topic` attr.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    /// Stored crypto stamp (audit only; never in-band). `None` until stamped.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stamp: Option<ProvenanceStamp>,
}

/// The canonical, immutable message record (one per send, even on fan-out).
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    pub id: MessageId,
    pub project: ProjectId,
    /// Sender name (`messages.from_name`).
    pub from: String,
    pub scope: Scope,
    /// Resolved internal thread id (hidden from agents; present for thread scope).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread: Option<ThreadId>,
    /// Resolved topic id (present for topic scope).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<TopicId>,
    pub body: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    pub provenance: Provenance,
    /// Unix epoch millis.
    #[typeshare(serialized_as = "number")]
    pub created_at: i64,
}

/// `read` request — fetch a full message by id (used to pull a truncated batch body,
/// backend §2.3.1; returns the full `Message`).
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ReadRequest {
    pub id: MessageId,
}
