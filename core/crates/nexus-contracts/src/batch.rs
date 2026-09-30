//! The drain-once delivery envelope (backend §2.3.1). One wake = one drain = one `NexusBatch`,
//! partitioned dms/threads. The daemon renders this into the `<nexus-batch>…</nexus-batch>`
//! in-band turn; this is the JSON/wire form.

use serde::{Deserialize, Serialize};
use typeshare::typeshare;

use crate::enums::{Kind, Scope};
use crate::ids::MessageId;

/// The shape line the agent sees: `dms=N thread=M total=T` (backend §2.3.1).
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BatchCounts {
    pub dms: u32,
    pub thread: u32,
    pub total: u32,
}

/// One message in a drop = one `<nexus …>` element. `truncated` marks a preview-capped body
/// (the agent fetches the full body with the MCP `read` tool, or CLI `nexus read <id>`).
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BatchMessage {
    pub id: MessageId,
    pub from: String,
    pub kind: Kind,
    pub scope: Scope,
    /// Thread name (present for thread scope).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread: Option<String>,
    /// Topic name (present for topic scope).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    /// The (possibly preview-truncated) body.
    pub body: String,
    /// True when the body was truncated to the preview budget.
    pub truncated: bool,
}

/// `consume` (drain-once) request — the held-receive window for one drain.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ConsumeRequest {
    /// Held-receive window in ms (None = daemon default).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u32>,
    /// Cap on messages drained in one drop (None = DRAIN_LIMIT).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<u32>,
}

/// Register a daemon-tracked durable inbox subscription for the caller.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct InboxSubscribeRequest {
    /// Held-receive window for follow-up `next` calls when no durable batch is ready.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u32>,
    /// Cap on messages drained per durable batch (None = DRAIN_LIMIT).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<u32>,
}

/// Response for a durable inbox subscription registration.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct InboxSubscribeResponse {
    pub subscription_id: String,
    pub active: bool,
}

/// Request the next durable batch for a subscription.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct InboxSubscriptionNextRequest {
    pub subscription_id: String,
    /// Optional override for this held receive only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u32>,
}

/// One durable subscription batch. It must be acknowledged separately from message split-acks.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct InboxSubscriptionBatch {
    pub batch_id: String,
    pub subscription_id: String,
    pub batch: NexusBatch,
}

/// Response for a durable subscription held receive.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct InboxSubscriptionNextResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch: Option<InboxSubscriptionBatch>,
}

/// Mark a durable subscription batch consumed after the caller has rendered and split-acked it.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct InboxSubscriptionAckRequest {
    pub subscription_id: String,
    pub batch_id: String,
}

/// Disable a durable inbox subscription.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct InboxUnsubscribeRequest {
    pub subscription_id: String,
}

/// Response for durable subscription ack/unsubscribe operations.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct InboxSubscriptionStatusResponse {
    pub subscription_id: String,
    pub active: bool,
}

/// One drain-once drop. `dms`/`threads` are the partitioned previews; the `*MessageIds` mirror
/// them for the split-ack (DMs per-message, threads bulk — backend §2 step 7).
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NexusBatch {
    pub counts: BatchCounts,
    pub dms: Vec<BatchMessage>,
    pub threads: Vec<BatchMessage>,
    pub dm_message_ids: Vec<MessageId>,
    pub thread_message_ids: Vec<MessageId>,
    /// All ids in the drop (dms + threads), for a whole-drop ack.
    pub message_ids: Vec<MessageId>,
}
