//! Pure ACP `session/update` → tagged stream-event transform — the daemon's full-stream
//! pass-through. Mirrors AionCore's
//! `aionui-ai-agent/src/protocol/events/translate.rs`, but emits the loosely-typed
//! [`nexus_contracts::AgentUpdateKind`] + `serde_json::Value` shape the daemon persists as
//! agent-session activity.
//!
//! The contract is deliberately thin: every **renderable** `session/update` variant is translated
//! 1:1 into a [`StreamEvent`] (`None` for the non-renderable book-keeping variants —
//! mode/config/session-info/usage/user-echo). `data` is pass-through JSON; the UI interprets it per
//! `kind`. No modeling, no I/O, no async — just a `match` + light sanitization (oversized / base64
//! blobs replaced with `"[omitted]"`) so a multi-MB tool output or inline image never reaches the
//! event sink or the store.

use agent_client_protocol::schema::v1::{
    AvailableCommand, ContentBlock, SessionUpdate, ToolCall, ToolCallUpdate, ToolKind,
};
use nexus_contracts::{AgentUpdateKind, ToolCallData};
use serde_json::{json, Value};

/// One translated ACP `session/update`: the [`AgentUpdateKind`] tag + its pass-through `data`. The
/// daemon relays each through its event sink as agent-session activity.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamEvent {
    /// Which renderable ACP activity this carries.
    pub kind: AgentUpdateKind,
    /// The loosely-typed payload (e.g. `{"text": …}` for text/thinking, the sanitized tool-call
    /// object for tool calls, `{"entries": […]}` for plan, `{"commands": […]}` for commands).
    pub data: Value,
}

impl StreamEvent {
    /// A [`AgentUpdateKind::Text`] event carrying `{"text": s}` — the agent's reply chunk.
    pub fn text(s: impl Into<String>) -> Self {
        Self {
            kind: AgentUpdateKind::Text,
            data: json!({ "text": s.into() }),
        }
    }

    /// A [`AgentUpdateKind::Thinking`] event carrying `{"text": s}` — the agent's reasoning chunk.
    pub fn thinking(s: impl Into<String>) -> Self {
        Self {
            kind: AgentUpdateKind::Thinking,
            data: json!({ "text": s.into() }),
        }
    }
}

/// The byte ceiling above which a string field is replaced with [`OMITTED`] (mirrors AionCore's
/// 8 KB inline cutoff — large tool output / base64 image blobs must never reach the broadcast).
const MAX_FIELD_BYTES: usize = 8 * 1024;

/// The placeholder substituted for an oversized / base64 string field.
const OMITTED: &str = "[omitted]";

/// Map one ACP `session/update` notification → a tagged [`StreamEvent`]. Returns `None` for a
/// non-renderable variant (a `UserMessageChunk` echo — claude-agent-acp does not emit it for
/// injected prompts, and the daemon is the authoritative source of injected user turns anyway, see
/// `nexus-agent`'s `relay_turn` — or the mode/config/session-info/usage book-keeping updates).
///
/// Renderable mappings:
/// - `AgentMessageChunk` → [`AgentUpdateKind::Text`] `{"text"}`
/// - `AgentThoughtChunk` → [`AgentUpdateKind::Thinking`] `{"text"}`
/// - `ToolCall` / `ToolCallUpdate` → [`AgentUpdateKind::ToolCall`] (sanitized
///   `{id,title,status,kind,raw_input,raw_output,content,locations}`; the UI merges by `id`)
/// - `Plan` → [`AgentUpdateKind::Plan`] `{"entries"}`
/// - `AvailableCommandsUpdate` → [`AgentUpdateKind::Commands`] `{"commands":[{name,description}…]}`
pub fn translate(update: &SessionUpdate) -> Option<StreamEvent> {
    match update {
        SessionUpdate::AgentMessageChunk(chunk) => text_of(&chunk.content).map(StreamEvent::text),
        SessionUpdate::AgentThoughtChunk(chunk) => {
            text_of(&chunk.content).map(StreamEvent::thinking)
        }
        SessionUpdate::ToolCall(tc) => Some(StreamEvent {
            kind: AgentUpdateKind::ToolCall,
            data: tool_call_data(tc),
        }),
        SessionUpdate::ToolCallUpdate(tcu) => Some(StreamEvent {
            kind: AgentUpdateKind::ToolCall,
            data: tool_call_update_data(tcu),
        }),
        SessionUpdate::Plan(plan) => Some(StreamEvent {
            kind: AgentUpdateKind::Plan,
            data: json!({ "entries": sanitized(serde_json::to_value(&plan.entries).unwrap_or_default()) }),
        }),
        SessionUpdate::AvailableCommandsUpdate(update) => Some(StreamEvent {
            kind: AgentUpdateKind::Commands,
            data: json!({ "commands": commands_data(&update.available_commands) }),
        }),
        // Non-renderable: the mode/config/session-info/usage book-keeping updates. They keep the
        // stream alive (the engine still bumps activity) but carry no web console content, so they
        // are not forwarded as an `agent.update`.
        _ => None,
    }
}

/// Pull the text out of a `ContentBlock`, if it is a text block (the only kind that carries a
/// renderable chunk for message/thought updates).
fn text_of(content: &ContentBlock) -> Option<String> {
    match content {
        ContentBlock::Text(t) => Some(t.text.clone()),
        _ => None,
    }
}

/// ACP gives a human `title` and a semantic `kind`, never a machine name. C-TOOL v1: `tool` is the
/// kind's canonical machine name; `execute` → `shell` (the kind's wire string reads as a verb, the
/// contract wants the tool). The remaining kinds' wire strings ARE their canonical names.
fn acp_tool_name(kind: &ToolKind) -> String {
    match kind {
        ToolKind::Execute => "shell".into(),
        other => serde_json::to_value(other)
            .ok()
            .and_then(|v| v.as_str().map(str::to_owned))
            .unwrap_or_else(|| "tool".into()),
    }
}

/// Serialize an enum to its wire string (`ToolKind`/`ToolCallStatus` are plain snake_case enums).
fn wire_str<T: serde::Serialize>(v: &T) -> Option<String> {
    serde_json::to_value(v)
        .ok()
        .and_then(|v| v.as_str().map(str::to_owned))
}

/// Build the C-TOOL v1 JSON for a fresh [`ToolCall`] (docs/tool-call-contract.md). Keyed `id` (so
/// the UI can merge a later `ToolCallUpdate` with the same id); `tool` is the canonical machine
/// name; `input`/`output` are the structured values (sanitized), never pre-stringified.
fn tool_call_data(tc: &ToolCall) -> Value {
    let mut data = ToolCallData::start(tc.tool_call_id.0.as_ref(), acp_tool_name(&tc.kind));
    data.title = Some(tc.title.clone());
    data.kind = wire_str(&tc.kind);
    data.status = wire_str(&tc.status);
    data.input = tc.raw_input.as_ref().map(|v| sanitized(v.clone()));
    data.output = tc.raw_output.as_ref().map(|v| sanitized(v.clone()));
    let mut obj = match data.into_value() {
        Value::Object(obj) => obj,
        other => return other,
    };
    if !tc.content.is_empty() {
        obj.insert("content".into(), to_value_sanitized(&tc.content));
    }
    if !tc.locations.is_empty() {
        obj.insert("locations".into(), to_value_sanitized(&tc.locations));
    }
    Value::Object(obj)
}

/// Build the C-TOOL v1 JSON for a [`ToolCallUpdate`] — a PARTIAL patch of [`tool_call_data`]'s
/// shape: the same `id` merge key and contract key names, but only the fields the update actually
/// carries (`tool` only when the update carries a `kind` to derive it from).
fn tool_call_update_data(tcu: &ToolCallUpdate) -> Value {
    let fields = &tcu.fields;

    let mut obj = serde_json::Map::new();
    obj.insert("id".into(), json!(tcu.tool_call_id.0.as_ref()));
    if let Some(title) = &fields.title {
        obj.insert("title".into(), json!(title));
    }
    if let Some(status) = &fields.status {
        obj.insert("status".into(), to_value_sanitized(status));
    }
    if let Some(kind) = &fields.kind {
        obj.insert("tool".into(), json!(acp_tool_name(kind)));
        obj.insert("kind".into(), to_value_sanitized(kind));
    }
    insert_opt(&mut obj, "input", fields.raw_input.as_ref());
    insert_opt(&mut obj, "output", fields.raw_output.as_ref());
    if let Some(content) = &fields.content {
        obj.insert("content".into(), to_value_sanitized(content));
    }
    if let Some(locations) = &fields.locations {
        obj.insert("locations".into(), to_value_sanitized(locations));
    }
    Value::Object(obj)
}

/// Build the `commands` array: the harness's own slash commands, name + description only (the input
/// schema is dropped — the palette only needs name/description).
fn commands_data(commands: &[AvailableCommand]) -> Value {
    Value::Array(
        commands
            .iter()
            .map(|c| json!({ "name": c.name, "description": c.description }))
            .collect(),
    )
}

/// Serialize a value to JSON then sanitize it (oversized / base64 string fields → [`OMITTED`]).
fn to_value_sanitized<T: serde::Serialize>(v: &T) -> Value {
    sanitized(serde_json::to_value(v).unwrap_or(Value::Null))
}

/// Insert an optional already-`Value` field, sanitized, only when present.
fn insert_opt(obj: &mut serde_json::Map<String, Value>, key: &str, v: Option<&Value>) {
    if let Some(v) = v {
        obj.insert(key.into(), sanitized(v.clone()));
    }
}

/// Recursively replace any string > [`MAX_FIELD_BYTES`] or a `data:…;base64,` blob with [`OMITTED`].
/// Pure — returns a cleaned copy. Walks objects + arrays so a nested `raw_output.result` base64
/// image is caught wherever it sits (mirrors AionCore `translate.rs:66-69`, generalized to any
/// string field).
fn sanitized(value: Value) -> Value {
    match value {
        Value::String(s) => {
            if is_oversized_or_base64(&s) {
                Value::String(OMITTED.to_string())
            } else {
                Value::String(s)
            }
        }
        Value::Array(items) => Value::Array(items.into_iter().map(sanitized).collect()),
        Value::Object(map) => {
            Value::Object(map.into_iter().map(|(k, v)| (k, sanitized(v))).collect())
        }
        other => other,
    }
}

/// True if a string should be omitted: it exceeds the byte ceiling, or it is an inline base64 data
/// URI (`data:<mime>;base64,…`) — the multi-MB blobs that must never reach the broadcast / store.
fn is_oversized_or_base64(s: &str) -> bool {
    s.len() > MAX_FIELD_BYTES || s.starts_with("data:") && s.contains(";base64,")
}
