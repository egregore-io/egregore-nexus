//! Claude-native record codec for the R3.1 headed forwarding seam.
//!
//! The headed Claude bridge still owns file tailing and cursor persistence in `forwarder.rs`, but
//! the record-to-agent-update mapping lives here so the later generic native forwarder can reuse
//! the same adapter. Records with a stable Claude message id also implement the shared
//! [`NativeForwardCodec`](nexus_transcript::NativeForwardCodec) seam, which lets the common
//! `ProducerIdentity` suppress streamed/final duplicates by id instead of by text. Claude tool
//! updates are also mapped into the seam's observation type so gateway developer-event transport can
//! surface tool phases without writing durable rows or waking agent turns.

use nexus_agent::adapter::provider_limit::classify_claude_stop_failure;
use nexus_contracts::{AgentUpdateKind, InjectError, SessionId};
use nexus_transcript::{
    DecodedRecord, NativeForwardCodec, Surface, ToolCallObservation, ToolCallPhase, TranscriptEvent,
};
use serde_json::{json, Value};

use crate::native::transcript::{
    ClaudeToolUpdate, CompactionMarker, CompactionPhase, TurnBoundary,
};

/// Renderable Claude-native event after harness-specific parsing.
#[derive(Debug, Clone, PartialEq)]
pub struct ClaudeNativeEvent {
    /// Agent-update kind emitted to Nexus.
    pub kind: AgentUpdateKind,
    /// Loose payload interpreted by AG-UI/gateway clients for this kind.
    pub data: Value,
}

/// A Claude-native record that carries a stable producer id and can participate in cross-pass
/// streamed/final suppression.
#[derive(Debug, Clone, PartialEq)]
pub struct ClaudeProducerRecord {
    producer_id: String,
    surface: Surface,
    event: ClaudeNativeEvent,
}

impl ClaudeProducerRecord {
    /// A streamed text chunk/group from Claude `MessageDisplay`.
    pub fn streamed_text(producer_id: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            producer_id: producer_id.into(),
            surface: Surface::Streamed,
            event: text_event(text),
        }
    }

    /// A final assistant transcript snapshot from Claude's durable transcript.
    pub fn final_text(producer_id: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            producer_id: producer_id.into(),
            surface: Surface::Final,
            event: text_event(text),
        }
    }

    /// Stable Claude producer identity.
    pub fn producer_id(&self) -> &str {
        &self.producer_id
    }

    /// Producer surface for duplicate suppression.
    pub fn surface(&self) -> Surface {
        self.surface
    }

    /// Renderable agent update for this record.
    pub fn event(&self) -> &ClaudeNativeEvent {
        &self.event
    }
}

/// Claude implementation of the R3.1 native-forward codec seam.
#[derive(Debug, Clone, Copy, Default)]
pub struct ClaudeNativeAdapter;

impl ClaudeNativeAdapter {
    /// Decode a stable-id text record into a renderable event plus producer metadata.
    pub fn decode_producer(&self, record: &ClaudeProducerRecord) -> DecodedRecord {
        self.decode(record)
            .expect("ClaudeProducerRecord always decodes to one event")
    }
}

impl NativeForwardCodec for ClaudeNativeAdapter {
    type Record = ClaudeProducerRecord;

    fn decode(&self, record: &Self::Record) -> Option<DecodedRecord> {
        if record.producer_id.is_empty() {
            return None;
        }
        Some(DecodedRecord {
            producer_id: record.producer_id.clone(),
            surface: record.surface,
            event: TranscriptEvent {
                kind: agent_update_kind_wire(record.event.kind).to_string(),
                data: record.event.data.clone(),
            },
        })
    }
}

/// A text update from streamed deltas or final transcript rows.
pub fn text_event(text: impl Into<String>) -> ClaudeNativeEvent {
    ClaudeNativeEvent {
        kind: AgentUpdateKind::Text,
        data: json!({ "text": text.into() }),
    }
}

/// A user prompt submitted into the native Claude session.
pub fn user_input_event(text: impl Into<String>, prompt_id: Option<&str>) -> ClaudeNativeEvent {
    ClaudeNativeEvent {
        kind: AgentUpdateKind::UserInput,
        data: json!({ "text": text.into(), "promptId": prompt_id }),
    }
}

/// A Claude tool-use/tool-result update.
pub fn tool_event(update: &ClaudeToolUpdate) -> ClaudeNativeEvent {
    let data = match update {
        ClaudeToolUpdate::Start { id, name, input } => {
            // C-TOOL v1 (docs/tool-call-contract.md): the Claude tool name IS the machine
            // name; args ride the structured `input` field.
            let mut data = json!({
                "id": id,
                "tool": name,
                "title": name,
                "status": "in_progress",
            });
            if let Some(input) = input {
                data["input"] = input.clone();
            }
            data
        }
        ClaudeToolUpdate::Result {
            id,
            content,
            is_error,
        } => {
            let status = if is_error.unwrap_or(false) {
                "failed"
            } else {
                "completed"
            };
            let mut data = json!({
                "id": id,
                "status": status,
            });
            if let Some(content) = content {
                data["content"] = Value::String(content.clone());
            }
            data
        }
    };
    ClaudeNativeEvent {
        kind: AgentUpdateKind::ToolCall,
        data,
    }
}

/// Convert a Claude-native tool update into an ephemeral developer-event observation.
///
/// The matching daemon service carries the tool name from `pre` to `post` by native id, so result
/// updates do not need to synthesize a name when Claude only reports `tool_use_id`.
pub fn tool_observation(update: &ClaudeToolUpdate) -> ToolCallObservation {
    match update {
        ClaudeToolUpdate::Start { id, name, .. } => ToolCallObservation {
            tool_call_id: Some(id.clone()),
            tool: name.clone(),
            phase: ToolCallPhase::Pre,
            ok: true,
        },
        ClaudeToolUpdate::Result { id, is_error, .. } => ToolCallObservation {
            tool_call_id: Some(id.clone()),
            tool: String::new(),
            phase: ToolCallPhase::Post,
            ok: !is_error.unwrap_or(false),
        },
    }
}

/// A native Claude turn boundary.
pub fn turn_boundary_event(boundary: &TurnBoundary) -> ClaudeNativeEvent {
    let data = match boundary {
        TurnBoundary::Stop { reason, .. } => {
            json!({ "reason": reason })
        }
        TurnBoundary::StopFailure {
            error,
            error_details,
            ..
        } => {
            json!({ "status": "failed", "error": error, "errorDetails": error_details })
        }
    };
    ClaudeNativeEvent {
        kind: AgentUpdateKind::TurnEnd,
        data,
    }
}

/// Convert Claude's structured native `StopFailure` hook fields into the shared breaker shape.
///
/// The classifier only uses `error` / `error_details` hook fields; `last_assistant_message` remains
/// visible transcript text and is intentionally ignored.
pub fn turn_boundary_inject_error(
    session: &SessionId,
    boundary: &TurnBoundary,
) -> Option<InjectError> {
    let TurnBoundary::StopFailure {
        error,
        error_details,
        ..
    } = boundary
    else {
        return None;
    };
    classify_claude_stop_failure(error.as_deref(), error_details.as_deref())
        .map(|error| error.into_inject_error(session))
}

/// A Claude compaction marker rendered as thinking/status activity.
pub fn compaction_event(compaction: &CompactionMarker) -> ClaudeNativeEvent {
    ClaudeNativeEvent {
        kind: AgentUpdateKind::Thinking,
        data: json!({
            "status": "compaction",
            "phase": compaction_phase_wire(compaction.phase),
            "trigger": compaction.trigger,
            "summary": compaction.summary,
        }),
    }
}

fn agent_update_kind_wire(kind: AgentUpdateKind) -> &'static str {
    match kind {
        AgentUpdateKind::Text => "text",
        AgentUpdateKind::Thinking => "thinking",
        AgentUpdateKind::ToolCall => "tool_call",
        AgentUpdateKind::Plan => "plan",
        AgentUpdateKind::Commands => "commands",
        AgentUpdateKind::UserInput => "user_input",
        AgentUpdateKind::TurnEnd => "turn_end",
    }
}

fn compaction_phase_wire(phase: CompactionPhase) -> &'static str {
    match phase {
        CompactionPhase::Pre => "pre",
        CompactionPhase::Post => "post",
        CompactionPhase::Transcript => "transcript",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_transcript::{forward_record, InMemoryProducerIdentityStore, ProducerIdentity};

    fn tracker() -> ProducerIdentity<InMemoryProducerIdentityStore> {
        ProducerIdentity::new(InMemoryProducerIdentityStore::new(64))
    }

    #[test]
    fn native_codec_suppresses_streamed_then_final_text_by_message_id() {
        let adapter = ClaudeNativeAdapter;
        let mut ids = tracker();

        let streamed = ClaudeProducerRecord::streamed_text("msg_1", "done");
        let first = forward_record(&adapter, &mut ids, "s_runtime", &streamed)
            .expect("streamed text emits");
        assert_eq!(first.kind, "text");
        assert_eq!(first.data["text"], "done");

        let final_snapshot = ClaudeProducerRecord::final_text("msg_1", "done");
        let duplicate = forward_record(&adapter, &mut ids, "s_runtime", &final_snapshot);
        assert!(
            duplicate.is_none(),
            "final snapshot with the same Claude message id is suppressed"
        );
    }

    #[test]
    fn native_codec_allows_same_text_with_different_message_id() {
        let adapter = ClaudeNativeAdapter;
        let mut ids = tracker();

        assert!(forward_record(
            &adapter,
            &mut ids,
            "s_runtime",
            &ClaudeProducerRecord::streamed_text("msg_1", "done")
        )
        .is_some());
        let distinct = forward_record(
            &adapter,
            &mut ids,
            "s_runtime",
            &ClaudeProducerRecord::final_text("msg_2", "done"),
        )
        .expect("different Claude message id is a real message");
        assert_eq!(distinct.kind, "text");
        assert_eq!(distinct.data["text"], "done");
    }

    #[test]
    fn tool_event_emits_the_ctool_contract_shape() {
        let event = tool_event(&ClaudeToolUpdate::Start {
            id: "toolu_read".to_string(),
            name: "Read".to_string(),
            input: Some(json!({ "file_path": "README.md" })),
        });

        assert_eq!(event.kind, AgentUpdateKind::ToolCall);
        assert_eq!(
            event.data,
            json!({
                "id": "toolu_read",
                "tool": "Read",
                "title": "Read",
                "status": "in_progress",
                "input": { "file_path": "README.md" },
            })
        );
    }

    #[test]
    fn stop_failure_rate_limit_surfaces_provider_limit() {
        let session = SessionId("s_claude_native".to_string());
        let err = turn_boundary_inject_error(
            &session,
            &TurnBoundary::StopFailure {
                error: Some("rate_limit".to_string()),
                error_details: Some(r#"{"retry_after_ms":1000}"#.to_string()),
                last_assistant_message: Some("partial visible text".to_string()),
            },
        )
        .expect("structured StopFailure should classify");

        let InjectError::ProviderLimit(limit) = err else {
            panic!("expected provider limit, got {err:?}");
        };
        assert_eq!(limit.harness, nexus_contracts::Harness::Claude);
        assert_eq!(limit.session, session);
        assert_eq!(
            limit.reason,
            nexus_contracts::ProviderLimitReason::RateLimit
        );
        assert!(limit.reset_hint.is_some());
        assert_eq!(limit.source, "claude.native.stop_failure");
    }
}
