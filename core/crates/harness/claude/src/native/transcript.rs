//! Pure parsing for Claude transcript and native hook object-stream records.
//!
//! This module extracts only the facts the native bridge needs later: Claude session id,
//! transcript path, submitted user prompts, assistant text, tool-use/tool-result updates, native
//! tool hook observations, stop boundaries, stop-failure boundaries, and compaction markers. It
//! does not translate records into Nexus stream events, tail files, store cursors, or deduplicate.
//! Claude hook stdin can be pretty-printed, so bridge hook records are parsed as a whitespace
//! separated JSON object stream rather than by physical line.

use serde_json::Value;

/// Exact hook provenance, separate from transcript-derived display boundaries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeHookRecord {
    pub end_offset: u64,
    pub session_id: Option<String>,
    pub prompt_id: Option<String>,
    pub kind: Option<String>,
    pub valid: bool,
    pub prompt: Option<String>,
}

/// Parse authority only from native hook fields; conflicting aliases are not evidence.
pub fn parse_hook_record(value: &Value, end_offset: u64) -> ClaudeHookRecord {
    let payload = wrapped_payload(value);
    let mut valid = value.is_object();
    let mut field = |names: &[&str]| {
        let mut result: Option<String> = None;
        for source in [value, payload] {
            for name in names {
                if let Some(v) = source.get(*name) {
                    match v.as_str().filter(|s| !s.is_empty()) {
                        Some(s) if result.as_deref().is_none_or(|old| old == s) => {
                            result = Some(s.into())
                        }
                        _ => valid = false,
                    }
                }
            }
        }
        result
    };
    let kind = field(&["event", "hook_event_name", "hookEventName"]);
    let session_id = field(&["session_id", "sessionId"]);
    let prompt_id = field(&["prompt_id", "promptId"]);
    let prompt = user_prompt(payload, kind.as_deref()).map(|p| p.text);
    valid &= kind.is_some() && session_id.is_some();
    if kind.as_deref() == Some("UserPromptSubmit") {
        valid &= prompt.is_some();
    }
    if let Some(active) = payload.get("stop_hook_active") {
        valid &= active.as_bool() == Some(false);
    }
    ClaudeHookRecord {
        end_offset,
        session_id,
        prompt_id,
        kind,
        valid,
        prompt,
    }
}

/// Parsed facts from one Claude transcript or hook-like JSONL record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptRecord {
    /// Claude session id from `sessionId`, `session_id`, or nested hook payload fields.
    pub session_id: Option<String>,
    /// Claude transcript path from hook payloads or transcript records, when present.
    pub transcript_path: Option<String>,
    /// Claude prompt id carried by lifecycle/user hook records, when present.
    pub prompt_id: Option<String>,
    /// Claude model message id for an assistant transcript record, including thinking-only rows.
    pub assistant_message_id: Option<String>,
    /// Strict root response metadata, independent of display text or hook aliases.
    pub response_model: Option<crate::native::model_reporting::ClaudeResponseModel>,
    /// User prompt recovered from Claude's `UserPromptSubmit` native hook.
    pub user_prompt: Option<UserPrompt>,
    /// Assistant text fragments recovered from transcript message content.
    pub assistant_text: Vec<AssistantText>,
    /// Tool call starts and results recovered from Claude transcript content blocks or
    /// `PreToolUse`/`PostToolUse` hook records.
    pub tool_updates: Vec<ClaudeToolUpdate>,
    /// Stop or stop-failure boundary recovered from hook records or assistant stop reasons.
    pub boundary: Option<TurnBoundary>,
    /// Compaction marker recovered from pre/post compact hook records or transcript summaries.
    pub compaction: Option<CompactionMarker>,
    /// Native sidechain marks (`isSidechain`, `agentId`, `parentUuid`) when the record belongs
    /// to a subagent rather than the root conversation. `None` for root records.
    pub sidechain: Option<SidechainMark>,
}

/// Native marks Claude Code writes on subagent transcript records. `agent_id` is the native
/// subagent id (also the file name under `subagents/`); `parent_uuid` links records inside one
/// file and is not agent ancestry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SidechainMark {
    pub agent_id: Option<String>,
    pub parent_uuid: Option<String>,
}

/// One user prompt submitted into a Claude native session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserPrompt {
    /// Claude prompt id, when the hook supplies one.
    pub prompt_id: Option<String>,
    /// The prompt text as submitted to Claude.
    pub text: String,
}

/// One assistant text fragment from a transcript assistant message record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssistantText {
    /// Claude model message id, when present.
    pub message_id: Option<String>,
    /// Text content from a `text` content block or string content field.
    pub text: String,
}

/// One Claude tool-use or tool-result update from transcript content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaudeToolUpdate {
    /// Claude started a tool call.
    Start {
        /// Claude tool-use id.
        id: String,
        /// Tool name supplied by Claude.
        name: String,
        /// Tool input payload, when present.
        input: Option<Value>,
    },
    /// Claude returned a result for a previously started tool call.
    Result {
        /// Claude tool-use id this result belongs to.
        id: String,
        /// Renderable result content, when present.
        content: Option<String>,
        /// Whether Claude marked the tool result as an error.
        is_error: Option<bool>,
    },
}

/// A Claude turn boundary recovered from a hook or transcript record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TurnBoundary {
    /// Claude stopped normally. Hook records may include the last assistant message.
    Stop {
        /// Claude stop reason, when the source record provides one.
        reason: Option<String>,
        /// Last assistant message from a `Stop` hook record, when present.
        last_assistant_message: Option<String>,
    },
    /// Claude ended the turn because the API request failed.
    StopFailure {
        /// Claude error category, when present.
        error: Option<String>,
        /// Extra error detail, stringified when Claude supplies an object or array.
        error_details: Option<String>,
        /// Last assistant message from a `StopFailure` hook record, when present.
        last_assistant_message: Option<String>,
    },
}

/// A compaction marker recovered from hook or transcript records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionMarker {
    /// Whether this came before compaction, after compaction, or from a transcript summary record.
    pub phase: CompactionPhase,
    /// Claude compaction trigger such as `manual` or `auto`, when present.
    pub trigger: Option<String>,
    /// User instructions passed to manual compaction, when present.
    pub custom_instructions: Option<String>,
    /// Generated compact summary, when present.
    pub summary: Option<String>,
}

/// Source phase for a compaction marker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionPhase {
    /// `PreCompact` hook input.
    Pre,
    /// `PostCompact` hook input.
    Post,
    /// Transcript summary or compact record.
    Transcript,
}

/// Parse one Claude transcript or hook-like JSONL line.
///
/// Returns `None` for invalid JSON or records that contain none of the supported facts.
/// Hook bridge wrapper records of the form `{"event": "...", "payload": {...}}` and raw
/// Claude hook input records are both accepted.
pub fn parse_transcript_line(line: &str) -> Option<TranscriptRecord> {
    let value: Value = serde_json::from_str(line).ok()?;
    parse_transcript_value(&value)
}

/// Parse many JSON object stream records into [`TranscriptRecord`] values.
///
/// Irrelevant records are skipped. Parsing stops at the first malformed or partial record so
/// file-tail callers can retry it on a later pass without advancing past unread data. The returned
/// vector preserves append order.
pub fn parse_transcript_jsonl(jsonl: &str) -> Vec<TranscriptRecord> {
    json_stream_values(jsonl)
        .iter()
        .filter_map(parse_transcript_value)
        .collect()
}

/// Parse one JSON object into a [`TranscriptRecord`].
///
/// This is useful for tests that already have a `serde_json::Value` fixture.
pub fn parse_transcript_value(value: &Value) -> Option<TranscriptRecord> {
    let payload = wrapped_payload(value);
    let event = event_name(value, payload);
    let session_id = string_field(payload, &["session_id", "sessionId"])
        .or_else(|| string_field(value, &["session_id", "sessionId"]))
        .or_else(|| {
            value
                .pointer("/worktreeSession/sessionId")
                .and_then(string_value)
        });
    let transcript_path = string_field(payload, &["transcript_path", "transcriptPath"])
        .or_else(|| string_field(value, &["transcript_path", "transcriptPath"]));
    let prompt_id = string_field(payload, &["prompt_id", "promptId"])
        .or_else(|| string_field(value, &["prompt_id", "promptId"]));
    let assistant_message_id = assistant_message_id(payload);
    let user_prompt = user_prompt(payload, event.as_deref());
    let assistant_text = assistant_text(payload);
    let tool_updates = tool_updates(payload, event.as_deref());
    let boundary = boundary(payload, event.as_deref());
    let compaction = compaction(payload, event.as_deref());
    let sidechain = sidechain_mark(payload);

    if session_id.is_none()
        && transcript_path.is_none()
        && prompt_id.is_none()
        && assistant_message_id.is_none()
        && user_prompt.is_none()
        && assistant_text.is_empty()
        && tool_updates.is_empty()
        && boundary.is_none()
        && compaction.is_none()
    {
        return None;
    }

    Some(TranscriptRecord {
        session_id,
        transcript_path,
        prompt_id,
        assistant_message_id,
        response_model: crate::native::model_reporting::parse_response_model(value),
        user_prompt,
        assistant_text,
        tool_updates,
        boundary,
        compaction,
        sidechain,
    })
}

fn wrapped_payload(value: &Value) -> &Value {
    value
        .get("payload")
        .filter(|payload| payload.is_object())
        .unwrap_or(value)
}

/// The native sidechain marks of a transcript record: present when `isSidechain` is `true` or
/// `agentId` is a non-empty string. Root records (`isSidechain: false`, no agent id) yield
/// `None`.
fn sidechain_mark(value: &Value) -> Option<SidechainMark> {
    let flagged = value.get("isSidechain").and_then(Value::as_bool) == Some(true);
    let agent_id = value
        .get("agentId")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .map(str::to_owned);
    if !flagged && agent_id.is_none() {
        return None;
    }
    Some(SidechainMark {
        agent_id,
        parent_uuid: value
            .get("parentUuid")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .map(str::to_owned),
    })
}

fn event_name(value: &Value, payload: &Value) -> Option<String> {
    string_field(payload, &["hook_event_name", "hookEventName", "event"])
        .or_else(|| string_field(value, &["event", "hook_event_name", "hookEventName"]))
}

fn user_prompt(value: &Value, event: Option<&str>) -> Option<UserPrompt> {
    if event != Some("UserPromptSubmit") {
        return None;
    }
    let text = string_field(value, &["prompt", "text"])?;
    if text.is_empty() {
        return None;
    }
    Some(UserPrompt {
        prompt_id: string_field(value, &["prompt_id", "promptId"]),
        text,
    })
}

fn assistant_text(value: &Value) -> Vec<AssistantText> {
    let is_assistant = value.get("type").and_then(Value::as_str) == Some("assistant")
        || value.pointer("/message/role").and_then(Value::as_str) == Some("assistant");
    if !is_assistant {
        return Vec::new();
    }

    let message_id = assistant_message_id(value);
    let Some(content) = value
        .pointer("/message/content")
        .or_else(|| value.get("content"))
    else {
        return Vec::new();
    };

    if let Some(text) = content.as_str() {
        return vec![AssistantText {
            message_id,
            text: text.to_owned(),
        }];
    }

    let Some(items) = content.as_array() else {
        return Vec::new();
    };

    items
        .iter()
        .filter_map(|item| {
            let is_text = item.get("type").and_then(Value::as_str) == Some("text")
                || item.get("type").is_none();
            if !is_text {
                return None;
            }
            let text = item.get("text").and_then(Value::as_str)?;
            Some(AssistantText {
                message_id: message_id.clone(),
                text: text.to_owned(),
            })
        })
        .collect()
}

fn assistant_message_id(value: &Value) -> Option<String> {
    let is_assistant = value.get("type").and_then(Value::as_str) == Some("assistant")
        || value.pointer("/message/role").and_then(Value::as_str) == Some("assistant");
    if !is_assistant {
        return None;
    }
    string_field(value, &["message_id", "messageId"])
        .or_else(|| value.pointer("/message/id").and_then(string_value))
        .or_else(|| string_field(value, &["uuid"]))
}

fn tool_updates(value: &Value, event: Option<&str>) -> Vec<ClaudeToolUpdate> {
    match event {
        Some("PreToolUse") => pre_tool_use(value).into_iter().collect(),
        Some("PostToolUse") => post_tool_use(value).into_iter().collect(),
        _ => transcript_tool_updates(value),
    }
}

fn pre_tool_use(value: &Value) -> Option<ClaudeToolUpdate> {
    Some(ClaudeToolUpdate::Start {
        id: tool_call_id(value)?,
        name: tool_name(value)?,
        input: tool_input(value),
    })
}

fn post_tool_use(value: &Value) -> Option<ClaudeToolUpdate> {
    Some(ClaudeToolUpdate::Result {
        id: tool_call_id(value)?,
        content: tool_output(value),
        is_error: Some(!post_tool_ok(value)),
    })
}

fn transcript_tool_updates(value: &Value) -> Vec<ClaudeToolUpdate> {
    let Some(content) = value
        .pointer("/message/content")
        .or_else(|| value.get("content"))
    else {
        return Vec::new();
    };
    let Some(items) = content.as_array() else {
        return Vec::new();
    };

    items
        .iter()
        .filter_map(|item| match item.get("type").and_then(Value::as_str) {
            Some("tool_use") => {
                let id = item.get("id").and_then(Value::as_str)?.to_owned();
                let name = item.get("name").and_then(Value::as_str)?.to_owned();
                Some(ClaudeToolUpdate::Start {
                    id,
                    name,
                    input: item.get("input").cloned(),
                })
            }
            Some("tool_result") => {
                let id = item.get("tool_use_id").and_then(Value::as_str)?.to_owned();
                Some(ClaudeToolUpdate::Result {
                    id,
                    content: item.get("content").and_then(tool_result_content),
                    is_error: item.get("is_error").and_then(Value::as_bool),
                })
            }
            _ => None,
        })
        .collect()
}

fn tool_call_id(value: &Value) -> Option<String> {
    string_field(
        value,
        &[
            "tool_use_id",
            "toolUseId",
            "tool_call_id",
            "toolCallId",
            "id",
        ],
    )
    .or_else(|| value.pointer("/tool/id").and_then(string_value))
    .or_else(|| value.pointer("/tool_use/id").and_then(string_value))
    .or_else(|| value.pointer("/toolUse/id").and_then(string_value))
}

fn tool_name(value: &Value) -> Option<String> {
    string_field(value, &["tool_name", "toolName", "name"])
        .or_else(|| value.pointer("/tool/name").and_then(string_value))
        .or_else(|| value.pointer("/tool_use/name").and_then(string_value))
        .or_else(|| value.pointer("/toolUse/name").and_then(string_value))
}

fn tool_input(value: &Value) -> Option<Value> {
    ["input", "tool_input", "toolInput"]
        .iter()
        .find_map(|field| value.get(*field).cloned())
        .or_else(|| value.pointer("/tool/input").cloned())
        .or_else(|| value.pointer("/tool_use/input").cloned())
        .or_else(|| value.pointer("/toolUse/input").cloned())
}

fn tool_output(value: &Value) -> Option<String> {
    ["content", "result", "output"]
        .iter()
        .find_map(|field| value.get(*field).and_then(tool_result_content))
        .or_else(|| value.pointer("/tool/content").and_then(tool_result_content))
        .or_else(|| value.pointer("/tool/result").and_then(tool_result_content))
}

fn post_tool_ok(value: &Value) -> bool {
    if let Some(ok) = value.get("ok").and_then(Value::as_bool) {
        return ok;
    }
    if let Some(success) = value.get("success").and_then(Value::as_bool) {
        return success;
    }
    if value.get("is_error").and_then(Value::as_bool) == Some(true)
        || value.get("isError").and_then(Value::as_bool) == Some(true)
    {
        return false;
    }
    if value
        .get("exit_code")
        .or_else(|| value.get("exitCode"))
        .and_then(Value::as_i64)
        .is_some_and(|code| code != 0)
    {
        return false;
    }
    if value
        .get("error")
        .is_some_and(|error| !error.is_null() && error.as_str() != Some(""))
    {
        return false;
    }
    let Some(status) = string_field(value, &["status", "decision", "outcome"]) else {
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

fn tool_result_content(value: &Value) -> Option<String> {
    if let Some(text) = value.as_str() {
        return Some(text.to_owned());
    }
    if let Some(items) = value.as_array() {
        let text = items
            .iter()
            .filter_map(|item| item.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("");
        if !text.is_empty() {
            return Some(text);
        }
    }
    if value.is_null() {
        None
    } else {
        Some(value.to_string())
    }
}

fn boundary(value: &Value, event: Option<&str>) -> Option<TurnBoundary> {
    match event {
        Some("Stop") => Some(TurnBoundary::Stop {
            reason: string_field(value, &["reason", "stop_reason", "stopReason"]),
            last_assistant_message: string_field(
                value,
                &["last_assistant_message", "lastAssistantMessage"],
            ),
        }),
        Some("StopFailure") => Some(TurnBoundary::StopFailure {
            error: string_field(value, &["error"]),
            error_details: jsonish_field(value, &["error_details", "errorDetails"]),
            last_assistant_message: string_field(
                value,
                &["last_assistant_message", "lastAssistantMessage"],
            ),
        }),
        _ => {
            let reason = value
                .pointer("/message/stop_reason")
                .and_then(string_value)?;
            Some(TurnBoundary::Stop {
                reason: Some(reason),
                last_assistant_message: None,
            })
        }
    }
}

fn compaction(value: &Value, event: Option<&str>) -> Option<CompactionMarker> {
    match event {
        Some("PreCompact") => Some(CompactionMarker {
            phase: CompactionPhase::Pre,
            trigger: string_field(value, &["trigger"]),
            custom_instructions: string_field(
                value,
                &["custom_instructions", "customInstructions"],
            ),
            summary: None,
        }),
        Some("PostCompact") => Some(CompactionMarker {
            phase: CompactionPhase::Post,
            trigger: string_field(value, &["trigger"]),
            custom_instructions: None,
            summary: string_field(value, &["compact_summary", "compactSummary", "summary"]),
        }),
        _ => transcript_compaction(value),
    }
}

fn transcript_compaction(value: &Value) -> Option<CompactionMarker> {
    let record_type = value.get("type").and_then(Value::as_str)?;
    let is_compaction = matches!(record_type, "summary" | "compact" | "compaction");
    if !is_compaction {
        return None;
    }

    Some(CompactionMarker {
        phase: CompactionPhase::Transcript,
        trigger: string_field(value, &["trigger"]),
        custom_instructions: string_field(value, &["custom_instructions", "customInstructions"]),
        summary: string_field(value, &["summary", "compact_summary", "compactSummary"])
            .or_else(|| value.pointer("/message/content").and_then(string_value)),
    })
}

fn string_field(value: &Value, names: &[&str]) -> Option<String> {
    names
        .iter()
        .find_map(|name| value.get(*name).and_then(string_value))
}

fn string_value(value: &Value) -> Option<String> {
    value.as_str().map(ToOwned::to_owned)
}

fn jsonish_field(value: &Value, names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| {
        let value = value.get(*name)?;
        value
            .as_str()
            .map(ToOwned::to_owned)
            .or_else(|| Some(value.to_string()))
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
