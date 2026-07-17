//! Native OpenCode session event translation.
//!
//! Headed OpenCode writes structured records to its `opencode.db` `event` table. This module owns
//! the pure row-to-`agent.update` translation used by the daemon-side forwarder; it deliberately
//! does no process management and never reads terminal output. OpenCode `message.part.updated`
//! records carry cumulative text per part, so the translator keeps a small per-part cursor and emits
//! only the newly appended suffix.

use std::collections::HashMap;

use nexus_acp_stream::StreamEvent;
use nexus_contracts::AgentUpdateKind;
use nexus_transcript::{ToolCallObservation, ToolCallPhase};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// One row from OpenCode's native `event` table.
#[derive(Debug, Clone, PartialEq)]
pub struct OpenCodeEventRow {
    /// Monotonic sequence within the OpenCode aggregate/session.
    pub seq: i64,
    /// OpenCode event type, for example `message.updated.1` or `message.part.updated.1`.
    pub event_type: String,
    /// Parsed JSON payload from the `event.data` column.
    pub data: Value,
}

/// Incremental translation state for one OpenCode session.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct OpenCodeForwardState {
    /// The OpenCode native session id discovered from event payloads.
    pub opencode_session_id: Option<String>,
    /// Last OpenCode event sequence translated.
    pub last_seq: i64,
    message_roles: HashMap<String, MessageRole>,
    part_text: HashMap<String, String>,
}

impl OpenCodeForwardState {
    /// Create state seeded from a persisted OpenCode session id and event cursor.
    pub fn from_cursor(opencode_session_id: Option<String>, last_seq: i64) -> Self {
        Self::from_persisted(opencode_session_id, last_seq, None)
    }

    /// Restore state from the sidecar row persisted by the daemon forwarder.
    pub fn from_persisted(
        opencode_session_id: Option<String>,
        last_seq: i64,
        parser_state_json: Option<&str>,
    ) -> Self {
        let mut state = parser_state_json
            .filter(|raw| !raw.trim().is_empty())
            .and_then(|raw| serde_json::from_str::<Self>(raw).ok())
            .unwrap_or_default();
        if opencode_session_id.is_some() {
            state.opencode_session_id = opencode_session_id;
        }
        state.last_seq = state.last_seq.max(last_seq.max(0));
        state
    }

    /// Serialize the incremental parser state for the daemon sidecar row.
    pub fn to_persisted_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "{}".to_string())
    }
}

/// Stateless OpenCode native row codec.
///
/// The daemon owns polling, discovery, and cursor persistence. This adapter owns only the
/// harness-specific row-to-stream-event mapping plus the incremental parser state it receives.
#[derive(Debug, Clone, Copy, Default)]
pub struct OpenCodeNativeAdapter;

impl OpenCodeNativeAdapter {
    /// Decode one OpenCode event row into zero or more Nexus session-stream updates.
    pub fn decode(
        &self,
        row: &OpenCodeEventRow,
        state: &mut OpenCodeForwardState,
    ) -> Vec<StreamEvent> {
        decode_event_row(row, state)
    }

    /// Derive ephemeral tool-call developer-event observations from one native row.
    ///
    /// These observations are side-channel telemetry for `sys.agent.<name>.tool_call`; they never
    /// replace the visible `agent.update:tool_call` stream rows returned by [`Self::decode`].
    pub fn tool_call_observations(&self, row: &OpenCodeEventRow) -> Vec<ToolCallObservation> {
        tool_call_observations(row)
    }
}

/// Compatibility wrapper for the OpenCode native adapter.
pub fn translate_event_row(
    row: &OpenCodeEventRow,
    state: &mut OpenCodeForwardState,
) -> Vec<StreamEvent> {
    OpenCodeNativeAdapter.decode(row, state)
}

/// Compatibility wrapper for deriving OpenCode tool-call observations.
pub fn tool_call_observations(row: &OpenCodeEventRow) -> Vec<ToolCallObservation> {
    if row.event_type != "message.part.updated.1" {
        return Vec::new();
    }
    let Some(part) = row.data.get("part") else {
        return Vec::new();
    };
    if part.get("type").and_then(Value::as_str) != Some("tool") {
        return Vec::new();
    }
    vec![tool_part_observation(part)]
}

fn decode_event_row(row: &OpenCodeEventRow, state: &mut OpenCodeForwardState) -> Vec<StreamEvent> {
    remember_session_id(state, row.data.get("sessionID").and_then(Value::as_str));
    state.last_seq = state.last_seq.max(row.seq);

    match row.event_type.as_str() {
        "message.updated.1" => {
            remember_message_role(state, row.data.get("info"));
            Vec::new()
        }
        "message.part.updated.1" => translate_part(row.data.get("part"), state),
        _ => Vec::new(),
    }
}

fn remember_session_id(state: &mut OpenCodeForwardState, session_id: Option<&str>) {
    if let Some(session_id) = session_id.filter(|id| !id.is_empty()) {
        state.opencode_session_id = Some(session_id.to_string());
    }
}

fn remember_message_role(state: &mut OpenCodeForwardState, info: Option<&Value>) {
    let Some(info) = info else { return };
    let Some(id) = info.get("id").and_then(Value::as_str) else {
        return;
    };
    let Some(role) = info
        .get("role")
        .and_then(Value::as_str)
        .and_then(MessageRole::parse)
    else {
        return;
    };
    state.message_roles.insert(id.to_string(), role);
}

fn translate_part(part: Option<&Value>, state: &mut OpenCodeForwardState) -> Vec<StreamEvent> {
    let Some(part) = part else { return Vec::new() };
    let part_type = part.get("type").and_then(Value::as_str).unwrap_or_default();
    match part_type {
        "text" | "reasoning" => translate_text_part(part, state, part_type),
        "tool" => translate_tool_part(part),
        "step-finish" => vec![StreamEvent {
            kind: AgentUpdateKind::TurnEnd,
            data: json!({ "reason": part.get("reason").and_then(Value::as_str).unwrap_or("stop") }),
        }],
        _ => Vec::new(),
    }
}

fn translate_text_part(
    part: &Value,
    state: &mut OpenCodeForwardState,
    part_type: &str,
) -> Vec<StreamEvent> {
    let part_id = part.get("id").and_then(Value::as_str).unwrap_or_default();
    let message_id = part
        .get("messageID")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let text = part.get("text").and_then(Value::as_str).unwrap_or_default();
    if part_id.is_empty() || text.is_empty() {
        return Vec::new();
    }

    let previous = state
        .part_text
        .get(part_id)
        .map(String::as_str)
        .unwrap_or("");
    let delta = if let Some(delta) = text.strip_prefix(previous) {
        delta
    } else {
        text
    };
    state
        .part_text
        .insert(part_id.to_string(), text.to_string());
    if delta.is_empty() {
        return Vec::new();
    }

    match state.message_roles.get(message_id) {
        Some(MessageRole::User) => vec![StreamEvent {
            kind: AgentUpdateKind::UserInput,
            data: json!({ "text": delta }),
        }],
        Some(MessageRole::Assistant) if part_type == "reasoning" => vec![StreamEvent {
            kind: AgentUpdateKind::Thinking,
            data: json!({ "text": delta }),
        }],
        Some(MessageRole::Assistant) => vec![StreamEvent {
            kind: AgentUpdateKind::Text,
            data: json!({ "text": delta }),
        }],
        None => Vec::new(),
    }
}

fn translate_tool_part(part: &Value) -> Vec<StreamEvent> {
    let state = part.get("state").unwrap_or(&Value::Null);
    let id = part
        .get("callID")
        .or_else(|| part.get("id"))
        .and_then(Value::as_str)
        .unwrap_or("tool");
    let title = part.get("tool").and_then(Value::as_str).unwrap_or("tool");
    let status = state
        .get("status")
        .and_then(Value::as_str)
        .map(normalize_status)
        .unwrap_or("in_progress");

    // C-TOOL v1 (docs/tool-call-contract.md): opencode's registered tool name IS the machine
    // name; `input`/`output` are the structured values.
    let mut data = serde_json::Map::new();
    data.insert("id".to_string(), json!(id));
    data.insert("tool".to_string(), json!(title));
    data.insert("title".to_string(), json!(title));
    data.insert("kind".to_string(), json!("tool"));
    data.insert("status".to_string(), json!(status));
    if let Some(input) = state.get("input").filter(|value| !value.is_null()) {
        data.insert("input".to_string(), input.clone());
    }
    if let Some(output) = tool_output(state) {
        data.insert("output".to_string(), output);
    }

    vec![StreamEvent {
        kind: AgentUpdateKind::ToolCall,
        data: Value::Object(data),
    }]
}

fn tool_part_observation(part: &Value) -> ToolCallObservation {
    let state = part.get("state").unwrap_or(&Value::Null);
    let status = state
        .get("status")
        .and_then(Value::as_str)
        .map(normalize_status)
        .unwrap_or("in_progress");
    ToolCallObservation {
        tool_call_id: part
            .get("callID")
            .or_else(|| part.get("id"))
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .map(str::to_string),
        tool: part
            .get("tool")
            .and_then(Value::as_str)
            .filter(|tool| !tool.is_empty())
            .unwrap_or("tool")
            .to_string(),
        phase: if status == "in_progress" {
            ToolCallPhase::Pre
        } else {
            ToolCallPhase::Post
        },
        ok: status != "failed" && status != "cancelled",
    }
}

fn tool_output(state: &Value) -> Option<Value> {
    state
        .get("output")
        .cloned()
        .or_else(|| state.get("metadata").and_then(|m| m.get("output")).cloned())
}

fn normalize_status(status: &str) -> &'static str {
    match status {
        "completed" => "completed",
        "failed" | "error" => "failed",
        "cancelled" | "canceled" => "cancelled",
        _ => "in_progress",
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum MessageRole {
    User,
    Assistant,
}

impl MessageRole {
    fn parse(role: &str) -> Option<Self> {
        match role {
            "user" => Some(Self::User),
            "assistant" => Some(Self::Assistant),
            _ => None,
        }
    }
}
