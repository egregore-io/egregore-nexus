//! Thread ops (CLI §5, backend §10). Threads are named; ids are hidden from agents.

use serde::{Deserialize, Serialize};
use typeshare::typeshare;

/// `nexus thread new <name>` — create a named thread, optional initial members.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CreateThreadRequest {
    pub name: String,
    #[serde(default)]
    pub members: Vec<String>,
}

/// `nexus thread join <name>` (caller joins).
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct JoinThreadRequest {
    pub name: String,
}

/// `nexus thread leave <name>` (caller leaves).
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct LeaveThreadRequest {
    pub name: String,
}

/// `nexus thread archive <name>` — hide a thread from active routing/read/search while preserving
/// its registry row and members.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ArchiveThreadRequest {
    pub name: String,
}

/// `nexus thread delete <name>` — remove a thread registry and its memberships.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DeleteThreadRequest {
    pub name: String,
}

/// `nexus thread rename <name> <new-name>` — rename a thread by its stable registry row, preserving
/// memberships and message history.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RenameThreadRequest {
    pub name: String,
    pub new_name: String,
}

/// `nexus thread members <name>` request.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ThreadMembersRequest {
    pub name: String,
}

/// One thread in a list (`nexus threads`). Name + members + last activity.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ThreadSummary {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub members: Vec<String>,
    /// Unix epoch millis of the last message, if any.
    #[typeshare(serialized_as = "Option<number>")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_at: Option<i64>,
    /// Latest metadata-only developer event sequence for `sys.message.thread.<name>`.
    /// Consumers can keep a per-thread last-seen sequence and compare it with this value for
    /// unread counters without replaying message bodies.
    #[typeshare(serialized_as = "Option<number>")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_seq: Option<i64>,
}

/// `GET /threads` / `nexus threads` response.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ThreadListResponse {
    pub threads: Vec<ThreadSummary>,
}

/// Members of one thread.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ThreadMembersResponse {
    pub name: String,
    pub members: Vec<String>,
}

/// One member row in a channel-open header read.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ThreadHeaderMember {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    pub presence: String,
    pub session_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_work: Option<String>,
}

/// `GET /api/v1/threads/:name/header` — the compact read needed to open a channel.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ThreadHeaderResponse {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Unix epoch millis of the last message, if any.
    #[typeshare(serialized_as = "Option<number>")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_at: Option<i64>,
    pub members: Vec<ThreadHeaderMember>,
    pub member_count: u32,
    pub active_sessions: u32,
}

/// Add/remove a SPECIFIC member to/from an existing thread (`thread.addMember` /
/// `thread.removeMember`) — the operator managing a channel's roster, distinct from `thread.join`
/// which only adds the caller itself.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ThreadMemberRequest {
    pub name: String,
    pub member: String,
}
