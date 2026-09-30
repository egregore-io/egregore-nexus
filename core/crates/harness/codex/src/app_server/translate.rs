//! `translate_codex` — maps codex app-server notifications to nexus [`StreamEvent`]s.
//!
//! The companion [`tool_call_observations`] helper maps the same app-server tool item facts into
//! ephemeral `ToolCallObservation` values. Those observations feed daemon developer-event streams
//! only; they do not write store rows, wake realtime, or alter the visible AG-UI stream.
//!
//! # Schema source of truth
//! Pinned from `codex-rs/app-server-protocol/schema/json/ServerNotification.json`
//!
//! ## Confirmed `item/completed` item shapes (schema lines 3278–3937)
//!
//! Emitted payloads follow C-TOOL v1 (docs/tool-call-contract.md): `tool` = canonical machine
//! name, `input`/`output` = structured values.
//!
//! | item.type          | id   | tool / title / input / output derivation                         |
//! |--------------------|------|------------------------------------------------------------------|
//! | commandExecution   | id   | tool = "shell"; title = item["command"]; input = {command,cwd}; output = item["aggregatedOutput"] |
//! | fileChange         | id   | tool = "edit"; title = "fileChange"; input = item["changes"]     |
//! | webSearch          | id   | tool = "search"; title = item["query"]; input = {query}          |
//! | mcpToolCall        | id   | tool = title = "{server}/{tool}"; input = item["arguments"]; output = item["result"] |
//! | agentMessage       | id   | text + itemId = item["text"] + item["id"] (durable Text)       |
//!
//! **Item field mapping:**
//! - `commandExecution` does NOT have `input`/`output` fields. Output is `aggregatedOutput`
//!   (string|null). Command string is `command` (used as title).
//! - `fileChange` does NOT have `input`/`output` fields. It has `changes` (array).
//! - `mcpToolCall` sources args from `arguments` and output from `result` (schema names).
//! - `webSearch` has no output field; `query` is used as title.
//!
//! ## Sanitizer
//! `nexus-acp-stream`'s `sanitized` is private — replicated here verbatim (same constants).

use nexus_agent::StreamEvent;
use nexus_contracts::AgentUpdateKind;
use nexus_transcript::{ToolCallObservation, ToolCallPhase};
use serde_json::{json, Value};

use super::protocol::method;

/// Byte ceiling above which a string field is replaced with `[omitted]`.
/// Mirrors `nexus-acp-stream`'s `MAX_FIELD_BYTES`.
const MAX_FIELD_BYTES: usize = 8 * 1024;

/// Placeholder for oversized / base64 blob fields.
const OMITTED: &str = "[omitted]";

/// Map one codex app-server notification to a nexus [`StreamEvent`].
///
/// Returns `None` for non-renderable notifications (thread/*, turn/started, item/started,
/// tokenUsage, approvals).
///
/// # Mapping
///
/// | method                                | StreamEvent.kind | StreamEvent.data                                         |
/// |---------------------------------------|------------------|----------------------------------------------------------|
/// | item/agentMessage/delta               | Text             | {"text": params["delta"], "itemId": params["itemId"]}   |
/// | item/reasoning/textDelta              | Thinking         | {"text": params["delta"]}                                |
/// | item/plan/delta                       | Plan             | {"text": params["delta"]}                                |
/// | item/commandExecution/outputDelta     | ToolCall         | {"id", "status":"in_progress", "output": delta}          |
/// | item/completed (commandExecution etc) | ToolCall         | {"id", "title", "status":"completed", "kind", ...}       |
/// | item/completed (agentMessage)         | Text             | {"text": item["text"], "itemId": item["id"]}            |
/// | item/completed (userMessage)          | UserInput        | {"text": joined text/placeholder content}                |
/// | turn/completed OR "error"             | TurnEnd          | {}                                                       |
/// | everything else                       | None             |                                                          |
pub fn translate_codex(method: &str, params: &Value) -> Option<StreamEvent> {
    match method {
        m if m == method::AGENT_MESSAGE_DELTA => {
            let delta = sanitize_str(params["delta"].as_str().unwrap_or(""));
            let item_id = params["itemId"].as_str().unwrap_or("");
            Some(StreamEvent {
                kind: AgentUpdateKind::Text,
                data: json!({ "text": delta, "itemId": item_id }),
            })
        }

        m if m == method::REASONING_TEXT_DELTA => {
            let delta = sanitize_str(params["delta"].as_str().unwrap_or(""));
            Some(StreamEvent {
                kind: AgentUpdateKind::Thinking,
                data: json!({ "text": delta }),
            })
        }

        m if m == method::PLAN_DELTA => {
            let delta = sanitize_str(params["delta"].as_str().unwrap_or(""));
            Some(StreamEvent {
                kind: AgentUpdateKind::Plan,
                data: json!({ "text": delta }),
            })
        }

        m if m == method::COMMAND_OUTPUT_DELTA => {
            let id = params["itemId"].as_str().unwrap_or("");
            let raw = sanitize_str(params["delta"].as_str().unwrap_or(""));
            Some(StreamEvent {
                kind: AgentUpdateKind::ToolCall,
                data: json!({
                    "id": id,
                    "status": "in_progress",
                    "output": raw,
                }),
            })
        }

        m if m == method::ITEM_COMPLETED => {
            let item = &params["item"];
            let item_type = item["type"].as_str().unwrap_or("");

            match item_type {
                "agentMessage" => {
                    // Durable final message — deltas already streamed.
                    let text = sanitize_str(item["text"].as_str().unwrap_or(""));
                    let item_id = item["id"].as_str().unwrap_or("");
                    Some(StreamEvent {
                        kind: AgentUpdateKind::Text,
                        data: json!({ "text": text, "itemId": item_id }),
                    })
                }

                "userMessage" => {
                    let text = user_message_text(item);
                    if text.is_empty() {
                        return None;
                    }
                    Some(StreamEvent {
                        kind: AgentUpdateKind::UserInput,
                        data: json!({ "text": sanitize_str(&text) }),
                    })
                }

                "commandExecution" => {
                    let id = item["id"].as_str().unwrap_or("");
                    // title derived from the `command` string (schema: required)
                    let title =
                        sanitize_str(item["command"].as_str().unwrap_or("commandExecution"));
                    // output from `aggregatedOutput` (string|null, schema line 3441)
                    let raw_output = item.get("aggregatedOutput").cloned().unwrap_or(Value::Null);
                    let mut data = json!({
                        "id": id,
                        "tool": "shell",
                        "title": title,
                        "status": "completed",
                        "kind": "commandExecution",
                    });
                    // The command (+cwd) ARE the tool call's arguments. Without `input` the
                    // materializer stores no argsJson and history replay after downtime comes
                    // back argument-less — the title alone is display-only.
                    let mut raw_input = serde_json::Map::new();
                    if let Some(command) = item["command"].as_str() {
                        raw_input
                            .insert("command".to_string(), Value::String(sanitize_str(command)));
                    }
                    if let Some(cwd) = item["cwd"].as_str() {
                        raw_input.insert("cwd".to_string(), Value::String(sanitize_str(cwd)));
                    }
                    if !raw_input.is_empty() {
                        data["input"] = Value::Object(raw_input);
                    }
                    if !raw_output.is_null() {
                        data["output"] = sanitized(raw_output);
                    }
                    Some(StreamEvent {
                        kind: AgentUpdateKind::ToolCall,
                        data,
                    })
                }

                "fileChange" => {
                    let id = item["id"].as_str().unwrap_or("");
                    // fileChange has no native input field; expose `changes` as the input
                    let changes = item.get("changes").cloned().unwrap_or(Value::Null);
                    let mut data = json!({
                        "id": id,
                        "tool": "edit",
                        "title": "fileChange",
                        "status": "completed",
                        "kind": "fileChange",
                    });
                    if !changes.is_null() {
                        data["input"] = sanitized(changes);
                    }
                    Some(StreamEvent {
                        kind: AgentUpdateKind::ToolCall,
                        data,
                    })
                }

                "webSearch" => {
                    let id = item["id"].as_str().unwrap_or("");
                    // title from `query` (schema: required field)
                    let title = sanitize_str(item["query"].as_str().unwrap_or("webSearch"));
                    let mut data = json!({
                        "id": id,
                        "tool": "search",
                        "title": title,
                        "status": "completed",
                        "kind": "webSearch",
                    });
                    // Same argument rule as commandExecution: the query is the argument.
                    if let Some(query) = item["query"].as_str() {
                        data["input"] = json!({ "query": sanitize_str(query) });
                    }
                    Some(StreamEvent {
                        kind: AgentUpdateKind::ToolCall,
                        data,
                    })
                }

                "mcpToolCall" => {
                    let id = item["id"].as_str().unwrap_or("");
                    // title = "{server}/{tool}" (both required fields in schema)
                    let server = item["server"].as_str().unwrap_or("");
                    let tool = item["tool"].as_str().unwrap_or("");
                    let title = format!("{server}/{tool}");
                    // input = arguments (schema: any type, required)
                    let raw_input = item.get("arguments").cloned().unwrap_or(Value::Null);
                    // output = result (McpToolCallResult|null, schema line 3584)
                    let raw_output = item.get("result").cloned().unwrap_or(Value::Null);
                    let mut data = json!({
                        "id": id,
                        "tool": title.clone(),
                        "title": title,
                        "status": "completed",
                        "kind": "mcpToolCall",
                    });
                    if !raw_input.is_null() {
                        data["input"] = sanitized(raw_input);
                    }
                    if !raw_output.is_null() {
                        data["output"] = sanitized(raw_output);
                    }
                    Some(StreamEvent {
                        kind: AgentUpdateKind::ToolCall,
                        data,
                    })
                }

                // All other item types (hookPrompt, plan, reasoning,
                // dynamicToolCall, collabAgentToolCall, imageView, imageGeneration,
                // enteredReviewMode, exitedReviewMode, contextCompaction) → not rendered.
                _ => None,
            }
        }

        // turn/completed or terminal "error" (= method::TURN_FAILED). Retrying errors are
        // progress, not a turn boundary; Codex will emit a later terminal notification.
        m if m == method::TURN_FAILED
            && params.get("willRetry").and_then(Value::as_bool) == Some(true) =>
        {
            None
        }
        m if m == method::TURN_COMPLETED || m == method::TURN_FAILED => Some(StreamEvent {
            kind: AgentUpdateKind::TurnEnd,
            data: json!({}),
        }),

        // Non-renderable: thread/*, turn/started, item/started, tokenUsage, approvals
        _ => None,
    }
}

/// Map one codex app-server notification to ephemeral tool-call observations.
///
/// This deliberately mirrors only the tool-producing subset of [`translate_codex`]. Callers are
/// responsible for per-item-id pre-phase suppression across repeated output deltas.
pub fn tool_call_observations(method: &str, params: &Value) -> Vec<ToolCallObservation> {
    match method {
        m if m == method::COMMAND_OUTPUT_DELTA => {
            let id = params["itemId"].as_str().unwrap_or("").to_string();
            if id.is_empty() {
                return Vec::new();
            }
            vec![ToolCallObservation {
                tool_call_id: Some(id),
                tool: "commandExecution".to_string(),
                phase: ToolCallPhase::Pre,
                ok: true,
            }]
        }
        m if m == method::ITEM_COMPLETED => completed_tool_observation(&params["item"])
            .into_iter()
            .collect(),
        _ => Vec::new(),
    }
}

fn completed_tool_observation(item: &Value) -> Option<ToolCallObservation> {
    let item_type = item["type"].as_str().unwrap_or("");
    let id = item["id"].as_str().unwrap_or("").to_string();
    if id.is_empty() {
        return None;
    }
    let tool = match item_type {
        "commandExecution" => sanitize_str(item["command"].as_str().unwrap_or("commandExecution")),
        "fileChange" => "fileChange".to_string(),
        "webSearch" => sanitize_str(item["query"].as_str().unwrap_or("webSearch")),
        "mcpToolCall" => {
            let server = item["server"].as_str().unwrap_or("");
            let tool = item["tool"].as_str().unwrap_or("");
            let title = format!("{server}/{tool}");
            if title == "/" {
                "mcpToolCall".to_string()
            } else {
                title
            }
        }
        _ => return None,
    };
    Some(ToolCallObservation {
        tool_call_id: Some(id),
        tool,
        phase: ToolCallPhase::Post,
        ok: tool_item_ok(item),
    })
}

fn tool_item_ok(item: &Value) -> bool {
    if let Some(ok) = item.get("ok").and_then(Value::as_bool) {
        return ok;
    }
    if let Some(success) = item.get("success").and_then(Value::as_bool) {
        return success;
    }
    if item.get("is_error").and_then(Value::as_bool) == Some(true)
        || item.get("isError").and_then(Value::as_bool) == Some(true)
    {
        return false;
    }
    if item
        .get("exit_code")
        .or_else(|| item.get("exitCode"))
        .and_then(Value::as_i64)
        .is_some_and(|code| code != 0)
    {
        return false;
    }
    if item
        .get("error")
        .is_some_and(|error| !error.is_null() && error.as_str() != Some(""))
    {
        return false;
    }
    let Some(status) = item.get("status").and_then(Value::as_str) else {
        return true;
    };
    !matches!(
        status.to_ascii_lowercase().as_str(),
        "error"
            | "errored"
            | "failed"
            | "failure"
            | "denied"
            | "rejected"
            | "cancelled"
            | "canceled"
            | "blocked"
    )
}

pub(super) fn user_message_text(item: &Value) -> String {
    let Some(content) = item.get("content").and_then(Value::as_array) else {
        return String::new();
    };
    content
        .iter()
        .filter_map(|part| match part.get("type").and_then(Value::as_str) {
            Some("text") => part
                .get("text")
                .and_then(Value::as_str)
                .filter(|text| !text.is_empty())
                .map(str::to_string),
            Some(kind) => Some(format!("[{kind} input]")),
            None => Some("[user input]".to_string()),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

// ─── Sanitizer (replicated from nexus-acp-stream — `sanitized` is private there) ─────────────

/// Recursively replace any string > [`MAX_FIELD_BYTES`] or a `data:…;base64,` blob with
/// [`OMITTED`]. Pure — returns a cleaned copy. Mirrors `nexus-acp-stream/src/lib.rs:sanitized`.
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

/// Sanitize a bare string value extracted from JSON params.
fn sanitize_str(s: &str) -> String {
    if is_oversized_or_base64(s) {
        OMITTED.to_string()
    } else {
        s.to_string()
    }
}

/// True if a string should be omitted: exceeds byte ceiling, or is an inline base64 data URI.
/// Mirrors `nexus-acp-stream/src/lib.rs:is_oversized_or_base64`.
fn is_oversized_or_base64(s: &str) -> bool {
    s.len() > MAX_FIELD_BYTES || (s.starts_with("data:") && s.contains(";base64,"))
}
