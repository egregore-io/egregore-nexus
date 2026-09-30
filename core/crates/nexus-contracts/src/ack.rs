//! Split-ack (backend §2 step 7): DMs per-message, threads bulk.

use serde::{Deserialize, Serialize};
use typeshare::typeshare;

use crate::ids::MessageId;

/// Per-message ack (DMs).
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AckRequest {
    pub message_id: MessageId,
}

/// Bulk ack (threads): ack all listed in one call.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AckThreadsRequest {
    pub message_ids: Vec<MessageId>,
}

/// How many in-flight rows moved to `acked`.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AckResponse {
    pub acked: u32,
}
