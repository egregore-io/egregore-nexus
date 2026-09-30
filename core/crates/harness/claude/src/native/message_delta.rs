//! Pure parsing for Claude `MessageDisplay` hook object-stream records.
//!
//! The native bridge appends one JSON object per hook invocation with the hook input under
//! `payload`. Claude's hook stdin may be pretty-printed, so records are parsed as a whitespace
//! separated stream of JSON objects rather than by physical line.
//! These helpers also accept raw Claude hook input records so tests and future callers can
//! parse fixtures without recreating the bridge wrapper.

use serde_json::Value;

/// One assistant text chunk from a Claude `MessageDisplay` hook invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageDelta {
    /// Claude session id from the hook input, when present.
    pub session_id: Option<String>,
    /// Claude transcript path from the hook input, when present.
    pub transcript_path: Option<String>,
    /// Claude turn id for this streamed message, when present.
    pub turn_id: Option<String>,
    /// Claude prompt id that owns this displayed completion, when present.
    pub prompt_id: Option<String>,
    /// Stable Claude message id for this streamed message, when present.
    pub message_id: Option<String>,
    /// Zero-based position of this hook call within the message, when present.
    pub index: Option<u64>,
    /// Text newly displayed by this hook call.
    pub delta: String,
    /// Whether Claude marked this chunk as the final hook call for the message.
    pub final_chunk: bool,
}

/// Parse one JSONL line from `message_display.jsonl` into a [`MessageDelta`].
///
/// Returns `None` for invalid JSON, non-`MessageDisplay` hook records, or records without
/// a string `delta` field. Input order is not changed; callers that feed lines in append
/// order receive deltas in append order.
pub fn parse_message_display_line(line: &str) -> Option<MessageDelta> {
    let value: Value = serde_json::from_str(line).ok()?;
    parse_message_display_value(&value)
}

/// Parse many JSON object stream records into ordered [`MessageDelta`] values.
///
/// Non-`MessageDisplay` records are skipped. Parsing stops at the first malformed or partial
/// record so file-tail callers can retry it on a later pass without losing data. The returned
/// vector preserves append order.
pub fn parse_message_display_jsonl(jsonl: &str) -> Vec<MessageDelta> {
    json_stream_values(jsonl)
        .iter()
        .filter_map(parse_message_display_value)
        .collect()
}

/// Parse one JSON object into a [`MessageDelta`].
///
/// This is useful for tests that already have a `serde_json::Value` fixture.
pub fn parse_message_display_value(value: &Value) -> Option<MessageDelta> {
    let payload = event_payload(value, "MessageDisplay")?;
    let delta = string_field(payload, &["delta", "text"])?;

    Some(MessageDelta {
        session_id: string_field(payload, &["session_id", "sessionId"]),
        transcript_path: string_field(payload, &["transcript_path", "transcriptPath"]),
        turn_id: string_field(payload, &["turn_id", "turnId"]),
        prompt_id: string_field(payload, &["prompt_id", "promptId"]),
        message_id: string_field(payload, &["message_id", "messageId"]),
        index: u64_field(payload, &["index"]),
        delta,
        final_chunk: bool_field(payload, &["final", "final_chunk", "finalChunk"]).unwrap_or(false),
    })
}

fn event_payload<'a>(value: &'a Value, expected_event: &str) -> Option<&'a Value> {
    let payload = value
        .get("payload")
        .filter(|payload| payload.is_object())
        .unwrap_or(value);
    let root_event = string_field(value, &["event", "hook_event_name", "hookEventName"]);
    let payload_event = string_field(payload, &["hook_event_name", "hookEventName", "event"]);
    let event = payload_event.as_deref().or(root_event.as_deref());

    match event {
        Some(actual) if actual == expected_event => Some(payload),
        _ => None,
    }
}

fn string_field(value: &Value, names: &[&str]) -> Option<String> {
    names
        .iter()
        .find_map(|name| value.get(*name).and_then(Value::as_str))
        .map(ToOwned::to_owned)
}

fn bool_field(value: &Value, names: &[&str]) -> Option<bool> {
    names
        .iter()
        .find_map(|name| value.get(*name).and_then(Value::as_bool))
}

fn u64_field(value: &Value, names: &[&str]) -> Option<u64> {
    names.iter().find_map(|name| {
        let value = value.get(*name)?;
        value
            .as_u64()
            .or_else(|| value.as_i64().and_then(|n| u64::try_from(n).ok()))
    })
}

fn json_stream_values(text: &str) -> Vec<Value> {
    let mut values = Vec::new();
    let mut stream = serde_json::Deserializer::from_str(text).into_iter::<Value>();
    while let Some(next) = stream.next() {
        match next {
            Ok(value) => values.push(value),
            Err(_) => break,
        }
    }
    values
}
