use serde_json::{json, Value};

use crate::TranscriptEvent;

/// Parse one line of a claude session JSONL transcript into zero or more `TranscriptEvent`s, each
/// carrying an `AgentUpdateKind` wire string + `data` payload identical to the ACP `translate`
/// output, so the store/web render them the same regardless of source. Unrecognized / non-
/// conversation lines (attachments, skill/agent listings, hook records, deltas) yield `[]`.
pub fn parse_claude_line(line: &str) -> Vec<TranscriptEvent> {
    let Ok(v): Result<Value, _> = serde_json::from_str(line) else {
        return Vec::new();
    };
    let line_type = v.get("type").and_then(Value::as_str).unwrap_or("");
    let mut out = Vec::new();
    match line_type {
        "user" => {
            // Tool results ride user-role lines: each `tool_result` item is a C-TOOL patch
            // (merge by the originating tool_use id), NOT a user turn.
            if let Some(items) = v.pointer("/message/content").and_then(Value::as_array) {
                for item in items {
                    if item.get("type").and_then(Value::as_str) != Some("tool_result") {
                        continue;
                    }
                    let Some(id) = item.get("tool_use_id").and_then(Value::as_str) else {
                        continue;
                    };
                    let failed = item.get("is_error").and_then(Value::as_bool) == Some(true);
                    let mut data = json!({
                        "id": id,
                        "status": if failed { "failed" } else { "completed" },
                    });
                    if let Some(content) = item.get("content") {
                        data["output"] = sanitized(content.clone());
                    }
                    out.push(TranscriptEvent {
                        kind: "tool_call".into(),
                        data,
                    });
                }
            }
            if let Some(text) = first_text(&v) {
                out.push(TranscriptEvent {
                    kind: "user_input".into(),
                    data: json!({ "text": text }),
                });
            }
        }
        "assistant" => {
            let content = v.pointer("/message/content").and_then(Value::as_array);
            if let Some(items) = content {
                for item in items {
                    match item.get("type").and_then(Value::as_str) {
                        Some("text") => {
                            if let Some(t) = item.get("text").and_then(Value::as_str) {
                                out.push(TranscriptEvent {
                                    kind: "text".into(),
                                    data: json!({ "text": t }),
                                });
                            }
                        }
                        Some("thinking") | Some("reasoning") => {
                            if let Some(t) = item.get("text").and_then(Value::as_str) {
                                out.push(TranscriptEvent {
                                    kind: "thinking".into(),
                                    data: json!({ "text": t }),
                                });
                            }
                        }
                        Some("tool_use") => {
                            let id = item.get("id").and_then(Value::as_str).unwrap_or("");
                            // C-TOOL v1: the JSONL `name` IS the machine tool name (Read/Write/
                            // Bash/…); `input` is the structured args object and must be preserved.
                            let name = item.get("name").and_then(Value::as_str).unwrap_or("tool");
                            let mut data = json!({
                                "id": id,
                                "tool": name,
                                "title": name,
                                "status": "in_progress",
                            });
                            if let Some(input) = item.get("input") {
                                data["input"] = sanitized(input.clone());
                            }
                            out.push(TranscriptEvent {
                                kind: "tool_call".into(),
                                data,
                            });
                        }
                        _ => {}
                    }
                }
            }
            // A completed assistant message (has a stop_reason) closes the turn.
            if v.pointer("/message/stop_reason")
                .map(|s| !s.is_null())
                .unwrap_or(false)
            {
                out.push(TranscriptEvent {
                    kind: "turn_end".into(),
                    data: json!({}),
                });
            }
        }
        _ => {}
    }
    out
}

/// Byte ceiling above which a string field is replaced with [`OMITTED`] — mirrors
/// `nexus-acp-stream`'s cutoff so a multi-MB Write payload never reaches the event sink.
const MAX_FIELD_BYTES: usize = 8 * 1024;

/// Placeholder substituted for an oversized / base64 string field.
const OMITTED: &str = "[omitted]";

/// Recursively replace oversized or `data:…;base64,` string fields with [`OMITTED`]
/// (same semantics as `nexus-acp-stream::sanitized`).
fn sanitized(v: Value) -> Value {
    match v {
        Value::String(s) => {
            if s.len() > MAX_FIELD_BYTES || s.starts_with("data:") && s.contains(";base64,") {
                Value::String(OMITTED.into())
            } else {
                Value::String(s)
            }
        }
        Value::Array(items) => Value::Array(items.into_iter().map(sanitized).collect()),
        Value::Object(obj) => {
            Value::Object(obj.into_iter().map(|(k, v)| (k, sanitized(v))).collect())
        }
        other => other,
    }
}

/// First text chunk from a message's `content` array (used for user lines).
fn first_text(v: &Value) -> Option<String> {
    let content = v.pointer("/message/content")?;
    if let Some(s) = content.as_str() {
        return Some(s.to_string());
    }
    let items = content.as_array()?;
    for item in items {
        if item.get("type").and_then(Value::as_str) == Some("text") {
            if let Some(t) = item.get("text").and_then(Value::as_str) {
                return Some(t.to_string());
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assistant_text_line_maps_to_text_then_turn_end() {
        let line = r#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"ACK"}],"stop_reason":"end_turn"}}"#;
        let events = parse_claude_line(line);
        let kinds: Vec<&str> = events.iter().map(|e| e.kind.as_str()).collect();
        assert_eq!(
            kinds,
            vec!["text", "turn_end"],
            "assistant text + completed turn"
        );
        assert_eq!(events[0].data["text"], "ACK");
    }

    #[test]
    fn user_line_maps_to_user_input() {
        let line = r#"{"type":"user","message":{"role":"user","content":[{"type":"text","text":"hello"}]}}"#;
        let events = parse_claude_line(line);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, "user_input");
        assert_eq!(events[0].data["text"], "hello");
    }

    #[test]
    fn tool_use_line_maps_to_tool_call() {
        let line = r#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"tool_use","id":"tc_1","name":"Read"}]}}"#;
        let events = parse_claude_line(line);
        assert_eq!(events[0].kind, "tool_call");
        assert_eq!(events[0].data["id"], "tc_1");
        assert_eq!(events[0].data["title"], "Read");
    }

    // --- C-TOOL v1 (docs/tool-call-contract.md) ------------------------------

    #[test]
    fn tool_use_line_carries_tool_and_input() {
        let line = r#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"tool_use","id":"tc_1","name":"Write","input":{"file_path":"/tmp/a.md","content":"hi"}}]}}"#;
        let events = parse_claude_line(line);
        assert_eq!(events[0].kind, "tool_call");
        assert_eq!(
            events[0].data["tool"], "Write",
            "the JSONL name IS the machine name"
        );
        assert_eq!(events[0].data["input"]["file_path"], "/tmp/a.md");
        assert!(events[0].data["input"].is_object());
    }

    #[test]
    fn tool_result_user_line_patches_the_call_with_output() {
        let line = r#"{"type":"user","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"tc_1","content":"wrote 2 bytes"}]}}"#;
        let events = parse_claude_line(line);
        assert_eq!(
            events.len(),
            1,
            "a tool_result line is a tool patch, not a user turn"
        );
        assert_eq!(events[0].kind, "tool_call");
        assert_eq!(events[0].data["id"], "tc_1");
        assert_eq!(events[0].data["output"], "wrote 2 bytes");
        assert_eq!(events[0].data["status"], "completed");
    }

    #[test]
    fn errored_tool_result_maps_to_failed_status() {
        let line = r#"{"type":"user","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"tc_2","content":"boom","is_error":true}]}}"#;
        let events = parse_claude_line(line);
        assert_eq!(events[0].data["status"], "failed");
    }

    #[test]
    fn oversized_tool_input_field_is_omitted() {
        let big = "x".repeat(9 * 1024);
        let line = format!(
            r#"{{"type":"assistant","message":{{"role":"assistant","content":[{{"type":"tool_use","id":"tc_3","name":"Write","input":{{"file_path":"/tmp/a.md","content":"{big}"}}}}]}}}}"#
        );
        let events = parse_claude_line(&line);
        assert_eq!(events[0].data["input"]["content"], "[omitted]");
        assert_eq!(events[0].data["input"]["file_path"], "/tmp/a.md");
    }

    #[test]
    fn non_conversation_lines_are_skipped() {
        for line in [
            r#"{"type":"attachment","attachment":{}}"#,
            r#"{"type":"skill_listing"}"#,
            r#"{"type":"queue-operation","operation":"enqueue"}"#,
            "not json at all",
        ] {
            assert!(parse_claude_line(line).is_empty(), "skip: {line}");
        }
    }
}
