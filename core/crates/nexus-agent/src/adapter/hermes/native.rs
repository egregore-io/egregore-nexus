//! Native session row translation.
//!
//! Headed Hermes persists structured conversation rows in `~/.hermes/state.db`. This module owns the
//! pure row-to-`agent.update` translation used by the daemon-side forwarder; it deliberately does no
//! process management and never reads terminal output.

use nexus_acp_stream::StreamEvent;
use nexus_contracts::AgentUpdateKind;
use nexus_transcript::{ToolCallObservation, ToolCallPhase};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// Installed into the isolated profile's native gateway hook registry.
pub const MODEL_HOOK_SOURCE: &str = include_str!("model_hook.py");

/// Native session rows describe configured metadata, not a provider response.
pub fn model_profile() -> super::super::NativeModelReportingProfile {
    use nexus_contracts::{ModelEvidenceCapability as Capability, ModelReportBackend};
    super::super::NativeModelReportingProfile::new(
        ModelReportBackend::new("hermes.gateway").unwrap(),
        Capability::Supported,
        Capability::Unverified,
        Capability::Unverified,
    )
    .unwrap()
    .with_telemetry(super::super::AdapterTelemetryReportingProfile::new(
        super::super::AdapterTelemetryCapability::new(
            Capability::Supported,
            Some(
                nexus_contracts::ModelObservationSource::new(
                    nexus_harness_telemetry::HERMES_SESSION_USAGE_SOURCE,
                )
                .unwrap(),
            ),
        )
        .unwrap(),
        super::super::AdapterTelemetryCapability::new(Capability::Unsupported, None).unwrap(),
        super::super::AdapterTelemetryCapability::new(Capability::Unsupported, None).unwrap(),
    ))
}

/// Validate the captured native row before entering the harness-owned usage decoder.
pub fn session_usage(
    row: &Value,
    root: &str,
    observed_at: i64,
) -> Option<nexus_contracts::telemetry::NativeTelemetryUpdate> {
    configured_model(row, root, observed_at)?;
    nexus_harness_telemetry::decode_session_row_usage(row, root, observed_at)
}

/// Read only an exact root's configured model from its native persisted row.
pub fn configured_model(
    row: &Value,
    root: &str,
    observed_at: i64,
) -> Option<nexus_contracts::model_report::NativeModelUpdate> {
    use nexus_contracts::model_report::{
        ModelEvidenceField, ModelEvidenceValue, NativeModelUpdate,
    };
    use nexus_contracts::{
        ModelInvalidReason, ModelObservation, ModelObservationSource, ModelUnknownReason,
    };
    if root.trim().is_empty()
        || row.get("id").and_then(Value::as_str) != Some(root)
        || row.get("parent_session_id") != Some(&Value::Null)
    {
        return None;
    }
    // Native lineage metadata is not interchangeable with a shared cwd or display cursor.
    let config = match row.get("model_config") {
        None | Some(Value::Null) => json!({}),
        Some(Value::String(raw)) => serde_json::from_str::<Value>(raw).ok()?,
        _ => return None,
    };
    let config = config.as_object()?;
    if ["_delegate_from", "_branched_from"]
        .iter()
        .any(|key| config.get(*key).is_some_and(|value| !value.is_null()))
    {
        return None;
    }
    let invalid = || ModelEvidenceValue::Invalid(ModelInvalidReason::MalformedNativeMetadata);
    let value = match row.get("model") {
        None => ModelEvidenceValue::Unknown(ModelUnknownReason::AwaitingNativeMetadata),
        Some(Value::String(model)) => {
            let observation = ModelObservation {
                model_id: model.clone(),
                provider_id: None,
                source: ModelObservationSource::new("hermes.gateway.session").unwrap(),
                observed_at,
                native_session_id: Some(root.into()),
                native_turn_id: None,
                native_message_id: None,
                native_reported_at: None,
            };
            if observation.validate().is_ok() {
                ModelEvidenceValue::Observed(observation)
            } else {
                invalid()
            }
        }
        Some(_) => invalid(),
    };
    Some(NativeModelUpdate {
        native_session_id: root.into(),
        field: ModelEvidenceField::Configured,
        value,
    })
}

/// One active row from the native `messages` table.
#[derive(Debug, Clone, PartialEq)]
pub struct HermesMessageRow {
    /// Monotonic SQLite row id in `messages`.
    pub id: i64,
    /// Native Hermes session id.
    pub session_id: String,
    /// Hermes/OpenAI-style role: `user`, `assistant`, or `tool`.
    pub role: String,
    /// Stored message body. Tool result rows commonly store JSON text here.
    pub content: Option<String>,
    /// Tool result rows point back at the assistant call id.
    pub tool_call_id: Option<String>,
    /// Assistant rows store OpenAI-style tool call JSON in this column.
    pub tool_calls: Option<Value>,
    /// Tool result rows may include the human-readable tool name.
    pub tool_name: Option<String>,
    /// Hermes row timestamp.
    pub timestamp: f64,
    /// Assistant finish reason, for example `stop` or `tool_calls`.
    pub finish_reason: Option<String>,
    /// Hermes soft-delete flag. Inactive rows are ignored by the forwarder.
    pub active: bool,
}

/// Incremental translation state for one Hermes session.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct HermesForwardState {
    /// The Hermes native session id discovered from message rows.
    pub hermes_session_id: Option<String>,
    /// Last Hermes message row id translated.
    pub last_message_id: i64,
}

impl HermesForwardState {
    /// Create state seeded from a persisted Hermes session id and message cursor.
    pub fn from_cursor(hermes_session_id: Option<String>, last_message_id: i64) -> Self {
        Self {
            hermes_session_id,
            last_message_id: last_message_id.max(0),
        }
    }
}

/// Stateless Hermes native message-row codec.
///
/// The daemon owns polling, discovery, and cursor persistence. This adapter owns only the
/// harness-specific row-to-stream-event mapping plus the incremental parser state it receives.
#[derive(Debug, Clone, Copy, Default)]
pub struct HermesNativeAdapter;

impl HermesNativeAdapter {
    /// Decode one Hermes message row into zero or more Nexus session-stream updates.
    pub fn decode(
        &self,
        row: &HermesMessageRow,
        state: &mut HermesForwardState,
    ) -> Vec<StreamEvent> {
        decode_message_row(row, state)
    }

    /// Derive ephemeral tool-call developer-event observations from one native row.
    ///
    /// These observations are side-channel telemetry for `sys.agent.<name>.tool_call`; they never
    /// replace the visible `agent.update:tool_call` stream rows returned by [`Self::decode`].
    pub fn tool_call_observations(&self, row: &HermesMessageRow) -> Vec<ToolCallObservation> {
        tool_call_observations(row)
    }
}

/// Compatibility wrapper for the Hermes native adapter.
pub fn translate_message_row(
    row: &HermesMessageRow,
    state: &mut HermesForwardState,
) -> Vec<StreamEvent> {
    HermesNativeAdapter.decode(row, state)
}

/// Compatibility wrapper for deriving Hermes tool-call observations.
pub fn tool_call_observations(row: &HermesMessageRow) -> Vec<ToolCallObservation> {
    if !row.active {
        return Vec::new();
    }
    match row.role.as_str() {
        "assistant" => row
            .tool_calls
            .as_ref()
            .map(assistant_tool_call_observations)
            .unwrap_or_default(),
        "tool" => vec![tool_result_observation(row)],
        _ => Vec::new(),
    }
}

fn decode_message_row(row: &HermesMessageRow, state: &mut HermesForwardState) -> Vec<StreamEvent> {
    state.last_message_id = state.last_message_id.max(row.id);
    if !row.session_id.is_empty() {
        state.hermes_session_id = Some(row.session_id.clone());
    }
    if !row.active {
        return Vec::new();
    }

    match row.role.as_str() {
        "user" => text_event(AgentUpdateKind::UserInput, row.content.as_deref()),
        "assistant" => translate_assistant_row(row),
        "tool" => translate_tool_result_row(row),
        _ => Vec::new(),
    }
}

fn translate_assistant_row(row: &HermesMessageRow) -> Vec<StreamEvent> {
    let mut events = Vec::new();
    if let Some(tool_calls) = &row.tool_calls {
        events.extend(translate_tool_calls(tool_calls));
    }
    if let Some(content) = row.content.as_deref().filter(|text| !text.is_empty()) {
        events.push(StreamEvent {
            kind: AgentUpdateKind::Text,
            data: json!({ "text": content }),
        });
    }
    if let Some(reason) = row.finish_reason.as_deref() {
        if reason != "tool_calls" {
            events.push(StreamEvent {
                kind: AgentUpdateKind::TurnEnd,
                data: json!({ "reason": reason }),
            });
        }
    }
    events
}

fn translate_tool_calls(tool_calls: &Value) -> Vec<StreamEvent> {
    let calls: Vec<Value> = match tool_calls {
        Value::Array(items) => items.clone(),
        Value::Object(_) => vec![tool_calls.clone()],
        _ => Vec::new(),
    };
    calls
        .iter()
        .filter_map(|call| {
            let id = call
                .get("id")
                .or_else(|| call.get("call_id"))
                .and_then(Value::as_str)
                .unwrap_or("tool");
            let function = call.get("function").unwrap_or(&Value::Null);
            let title = function
                .get("name")
                .or_else(|| call.get("name"))
                .and_then(Value::as_str)
                .unwrap_or("tool");
            // C-TOOL v1 (docs/tool-call-contract.md): the OpenAI-style function name IS the
            // machine tool name; args are the structured `input` value.
            let mut data = serde_json::Map::new();
            data.insert("id".to_string(), json!(id));
            data.insert("tool".to_string(), json!(title));
            data.insert("title".to_string(), json!(title));
            data.insert("kind".to_string(), json!("tool"));
            data.insert("status".to_string(), json!("in_progress"));
            if let Some(arguments) = function.get("arguments").or_else(|| call.get("arguments")) {
                data.insert("input".to_string(), parse_jsonish(arguments));
            }
            Some(StreamEvent {
                kind: AgentUpdateKind::ToolCall,
                data: Value::Object(data),
            })
        })
        .collect()
}

fn assistant_tool_call_observations(tool_calls: &Value) -> Vec<ToolCallObservation> {
    tool_call_items(tool_calls)
        .iter()
        .map(|call| {
            let function = call.get("function").unwrap_or(&Value::Null);
            ToolCallObservation {
                tool_call_id: call
                    .get("id")
                    .or_else(|| call.get("call_id"))
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty())
                    .map(str::to_string),
                tool: function
                    .get("name")
                    .or_else(|| call.get("name"))
                    .and_then(Value::as_str)
                    .filter(|name| !name.is_empty())
                    .unwrap_or("tool")
                    .to_string(),
                phase: ToolCallPhase::Pre,
                ok: true,
            }
        })
        .collect()
}

fn tool_call_items(tool_calls: &Value) -> Vec<Value> {
    match tool_calls {
        Value::Array(items) => items.clone(),
        Value::Object(_) => vec![tool_calls.clone()],
        _ => Vec::new(),
    }
}

fn translate_tool_result_row(row: &HermesMessageRow) -> Vec<StreamEvent> {
    let id = row.tool_call_id.as_deref().unwrap_or("tool");
    let title = row.tool_name.as_deref().unwrap_or("tool");
    let mut data = serde_json::Map::new();
    data.insert("id".to_string(), json!(id));
    data.insert("title".to_string(), json!(title));
    data.insert("kind".to_string(), json!("tool"));
    data.insert("status".to_string(), json!("completed"));
    if let Some(content) = row.content.as_deref().filter(|text| !text.is_empty()) {
        data.insert("rawOutput".to_string(), parse_jsonish_str(content));
    }
    vec![StreamEvent {
        kind: AgentUpdateKind::ToolCall,
        data: Value::Object(data),
    }]
}

fn tool_result_observation(row: &HermesMessageRow) -> ToolCallObservation {
    ToolCallObservation {
        tool_call_id: row
            .tool_call_id
            .as_deref()
            .filter(|id| !id.is_empty())
            .map(str::to_string),
        tool: row
            .tool_name
            .as_deref()
            .filter(|name| !name.is_empty())
            .unwrap_or("tool")
            .to_string(),
        phase: ToolCallPhase::Post,
        ok: row.content.as_deref().map(tool_result_ok).unwrap_or(true),
    }
}

fn text_event(kind: AgentUpdateKind, text: Option<&str>) -> Vec<StreamEvent> {
    let Some(text) = text.filter(|text| !text.is_empty()) else {
        return Vec::new();
    };
    vec![StreamEvent {
        kind,
        data: json!({ "text": text }),
    }]
}

fn parse_jsonish(value: &Value) -> Value {
    match value {
        Value::String(raw) => parse_jsonish_str(raw),
        other => other.clone(),
    }
}

fn parse_jsonish_str(raw: &str) -> Value {
    serde_json::from_str(raw).unwrap_or_else(|_| json!(raw))
}

fn tool_result_ok(raw: &str) -> bool {
    let value = parse_jsonish_str(raw);
    let Value::Object(map) = value else {
        return true;
    };
    if let Some(ok) = map.get("ok").and_then(Value::as_bool) {
        return ok;
    }
    if let Some(success) = map.get("success").and_then(Value::as_bool) {
        return success;
    }
    if map.get("is_error").and_then(Value::as_bool) == Some(true)
        || map.get("isError").and_then(Value::as_bool) == Some(true)
    {
        return false;
    }
    if let Some(code) = map
        .get("exit_code")
        .or_else(|| map.get("exitCode"))
        .and_then(Value::as_i64)
    {
        return code == 0;
    }
    if let Some(error) = map.get("error") {
        if !error.is_null() && error.as_str().map(str::is_empty) != Some(true) {
            return false;
        }
    }
    if let Some(status) = map.get("status").and_then(Value::as_str) {
        return !matches!(
            status,
            "error"
                | "failed"
                | "failure"
                | "denied"
                | "rejected"
                | "cancelled"
                | "canceled"
                | "blocked"
        );
    }
    true
}
