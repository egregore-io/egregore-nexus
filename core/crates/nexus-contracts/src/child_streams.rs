//! Owner-authorized lookup of one session's child lane: `agent.child_streams`.
//!
//! The lane is volatile and epoch-scoped, so every cursor carries the daemon boot epoch it was
//! taken under. A cursor from another boot is answered with `cursor_status: boot_mismatch` and
//! a page from the lane's start, never a silent empty page; a cursor without an epoch is
//! refused as malformed.

use serde::{Deserialize, Serialize};
use typeshare::typeshare;

use crate::events::ChildStream;
use crate::ids::SessionId;

/// Position in one owner session's child lane, valid only under `epoch`.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ChildStreamCursor {
    /// The daemon boot epoch the cursor was taken under.
    pub epoch: String,
    /// Continue after this lane row id.
    #[typeshare(serialized_as = "number")]
    pub after_id: i64,
}

/// Position in the lane enumeration: the full ordering key, valid only under `epoch`.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ChildLaneCursor {
    /// The daemon boot epoch the cursor was taken under.
    pub epoch: String,
    pub child_key: String,
    pub harness: String,
    pub root: String,
}

/// `agent.child_streams` parameters.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ChildStreamsRequest {
    /// The owner session whose child lane is read.
    pub session: SessionId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub harness: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root: Option<String>,
    /// Narrow rows to one lane by its child key (`n:<native id>` or `l:<locator>`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<ChildStreamCursor>,
    /// Rows per page (default 200, at most 1000).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lanes_after: Option<ChildLaneCursor>,
    /// Lanes per page (default 100, at most 1000).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lanes_limit: Option<u32>,
}

/// How the request's cursors (rows and lanes) related to the current daemon boot. A mismatch
/// on either resets both to the start.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChildCursorStatus {
    /// No cursor was given; the page starts at the lane's beginning.
    Fresh,
    /// The cursor belongs to the current boot and was honored.
    Valid,
    /// The cursor belongs to another boot; the page starts at the lane's beginning.
    BootMismatch,
}

/// One row of the child lane.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ChildStreamRow {
    #[typeshare(serialized_as = "number")]
    pub id: i64,
    pub epoch: String,
    pub child: ChildStream,
    pub kind: String,
    pub source_ref: String,
    pub data: serde_json::Value,
    #[typeshare(serialized_as = "number")]
    pub bytes: i64,
    #[typeshare(serialized_as = "number")]
    pub created_at: i64,
}

/// One SQL-limited page of lane rows.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ChildStreamPage {
    pub rows: Vec<ChildStreamRow>,
    /// Pass back as `cursor.afterId` for the next page; absent when this page was the last.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[typeshare(serialized_as = "Option<number>")]
    pub next_after_id: Option<i64>,
}

/// One lane's identity plus its live and lost accounting.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ChildLaneSummary {
    pub child: ChildStream,
    pub child_key: String,
    pub epoch: String,
    #[typeshare(serialized_as = "number")]
    pub first_seen_id: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[typeshare(serialized_as = "Option<number>")]
    pub live_first_id: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[typeshare(serialized_as = "Option<number>")]
    pub live_last_id: Option<i64>,
    #[typeshare(serialized_as = "number")]
    pub live_rows: i64,
    #[typeshare(serialized_as = "number")]
    pub live_bytes: i64,
    #[typeshare(serialized_as = "number")]
    pub evicted_through: i64,
    #[typeshare(serialized_as = "number")]
    pub evicted_rows: i64,
    #[typeshare(serialized_as = "number")]
    pub evicted_bytes: i64,
    #[typeshare(serialized_as = "number")]
    pub refused_rows: i64,
    /// What part of the native source the harness pass declared this lane covers; JSON null
    /// when nothing was declared.
    #[serde(default)]
    #[typeshare(serialized_as = "Value")]
    pub coverage: serde_json::Value,
}

/// Owner-level loss summary: what tombstone compaction removed from the lane table.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ChildSessionLoss {
    pub epoch: String,
    #[typeshare(serialized_as = "number")]
    pub compacted_lanes: i64,
    #[typeshare(serialized_as = "number")]
    pub evicted_rows: i64,
    #[typeshare(serialized_as = "number")]
    pub evicted_bytes: i64,
    #[typeshare(serialized_as = "number")]
    pub refused_rows: i64,
    #[typeshare(serialized_as = "number")]
    pub updated_at: i64,
}

/// `agent.child_streams` result.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ChildStreamsResponse {
    /// The current daemon boot epoch; cursors taken from this page carry it.
    pub epoch: String,
    pub cursor_status: ChildCursorStatus,
    pub lanes: Vec<ChildLaneSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lanes_next_after: Option<ChildLaneCursor>,
    pub page: ChildStreamPage,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_loss: Option<ChildSessionLoss>,
}
