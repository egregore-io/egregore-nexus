//! Search + history (CLI §7, backend §9). Scope is enforced server-side (the requester's own
//! DMs + threads + subscriptions); the request only carries filters, never a scope override.

use serde::{Deserialize, Serialize};
use typeshare::typeshare;

use crate::ids::MessageId;

/// Search engine selector (CLI §7 `--semantic|--fts|--hybrid`).
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SearchMode {
    Fts,
    Semantic,
    Hybrid,
}

/// `GET /search` / `nexus search <query>`.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SearchRequest {
    pub query: String,
    pub mode: SearchMode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    /// Restrict to a thread by name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread: Option<String>,
    /// Restrict to a DM partner by name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub with: Option<String>,
    /// Only results at/after this epoch-millis.
    #[typeshare(serialized_as = "Option<number>")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub since: Option<i64>,
}

/// One search hit.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SearchHit {
    pub message_id: MessageId,
    pub from: String,
    /// Epoch millis.
    #[typeshare(serialized_as = "number")]
    pub when: i64,
    pub snippet: String,
    pub score: f32,
}

/// Search results.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SearchResponse {
    pub hits: Vec<SearchHit>,
}

/// `GET /history` / `nexus history` — chronological recall of a conversation.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HistoryRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub with: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    /// Only results before this epoch-millis.
    #[typeshare(serialized_as = "Option<number>")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before: Option<i64>,
}

/// One history line.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HistoryEntry {
    pub from: String,
    #[typeshare(serialized_as = "number")]
    pub when: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    pub body: String,
}

/// History results.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HistoryResponse {
    pub entries: Vec<HistoryEntry>,
}
