//! The per-harness codec seam for native (headed) forwarders.
//!
//! Today claude/codex/opencode/hermes each hand-roll a full forwarder: a poll/notify loop, a cursor,
//! a translate step, and a bespoke duplicate-suppression scheme. The shared seam splits that into (1) a generic
//! forwarding *engine* (the async I/O loop + cursor, lives in the engine wiring) and (2) a small
//! per-harness *codec* — this trait — that only knows how to turn one raw record into an emittable
//! event tagged with its stable producer id + surface. The cross-pass duplicate suppression is owned
//! once by [`ProducerIdentity`](crate::ProducerIdentity), so every harness suppresses identically.
//!
//! This module holds the pure, synchronous half (codec + the per-record decision). The async I/O
//! half (`read_since`, session discovery) composes a `NativeForwardCodec` in the engine crate.
//!
//! A second, metadata-only observation side channel supports developer tooling. Observations
//! are not renderable transcript events: they are facts such as "a native tool call started" that a
//! daemon-side publisher can later expose on ephemeral `sys.*` topics. Keeping them separate from
//! [`TranscriptEvent`] prevents telemetry from being mistaken for conversation history.

use crate::producer_identity::{ProducerIdentity, ProducerIdentityStore, Surface};
use crate::TranscriptEvent;

/// One decoded record: the emittable event, its stable producer id, and the surface it arrived on.
#[derive(Debug, Clone, PartialEq)]
pub struct DecodedRecord {
    /// Stable semantic identity of the message (claude `message_id`, codex `itemId`, opencode
    /// `partID`). The suppression key — never the text.
    pub producer_id: String,
    /// Whether this record is an incremental delta or a full final snapshot.
    pub surface: Surface,
    /// The renderable event to emit if not suppressed.
    pub event: TranscriptEvent,
}

/// A non-renderable fact observed while decoding a native transcript record.
///
/// Observations are side-channel metadata for developer tooling. They must not be appended to
/// materialized conversation history, must not wake or inject agent turns, and may be dropped by
/// later bounded/ring-buffered transport layers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativeForwardObservation {
    /// A native tool call became visible or reached a terminal result.
    ToolCall(ToolCallObservation),
}

/// Metadata for one observed native tool-call phase.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCallObservation {
    /// Stable native tool-call id when the harness exposes one.
    pub tool_call_id: Option<String>,
    /// Human-readable tool name, already sanitized by the harness adapter if needed.
    pub tool: String,
    /// Whether this observation is before execution or after terminal result.
    pub phase: ToolCallPhase,
    /// Whether the phase is considered successful. `pre` observations normally set this to `true`.
    pub ok: bool,
}

/// Phase of a native tool-call observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolCallPhase {
    /// The tool call started or was about to execute.
    Pre,
    /// The tool call produced a terminal result.
    Post,
}

/// The per-harness codec: decode one raw record into a [`DecodedRecord`], or `None` for a
/// non-conversation row (metadata, tool bookkeeping, blank). Implemented per harness (claude JSONL
/// row, codex app-server item, opencode/hermes db row); the engine and suppressor are shared.
pub trait NativeForwardCodec {
    /// The harness's raw record type (a parsed JSONL value, a db row, an app-server item).
    type Record;

    fn decode(&self, record: &Self::Record) -> Option<DecodedRecord>;

    /// Return metadata-only facts observed in this raw record.
    ///
    /// The default is empty so existing codecs opt in explicitly. Engines should call this for
    /// every native record, including records whose [`decode`](Self::decode) result is `None`,
    /// because some hook rows are pure observations with no renderable conversation event.
    fn observations(&self, _record: &Self::Record) -> Vec<NativeForwardObservation> {
        Vec::new()
    }
}

/// The engine's per-record core: decode a raw record, then consult the cross-pass suppressor.
/// Returns `Some(event)` to emit, or `None` to skip (non-conversation row, or a suppressed
/// duplicate final snapshot of an already-streamed message).
pub fn forward_record<C, S>(
    codec: &C,
    ids: &mut ProducerIdentity<S>,
    runtime_id: &str,
    record: &C::Record,
) -> Option<TranscriptEvent>
where
    C: NativeForwardCodec,
    S: ProducerIdentityStore,
{
    let decoded = codec.decode(record)?;
    if ids.admit(runtime_id, &decoded.producer_id, decoded.surface) {
        Some(decoded.event)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::producer_identity::InMemoryProducerIdentityStore;
    use serde_json::json;

    /// A minimal claude-shaped codec: a raw record is `(producer_id, surface, text)`; a blank id
    /// means "non-conversation row" → `None`.
    struct MockCodec;
    impl NativeForwardCodec for MockCodec {
        type Record = (&'static str, Surface, &'static str);
        fn decode(&self, r: &Self::Record) -> Option<DecodedRecord> {
            let (id, surface, text) = *r;
            if id.is_empty() {
                return None;
            }
            Some(DecodedRecord {
                producer_id: id.to_string(),
                surface,
                event: TranscriptEvent {
                    kind: "text".to_string(),
                    data: json!({ "text": text }),
                },
            })
        }
    }

    fn tracker() -> ProducerIdentity<InMemoryProducerIdentityStore> {
        ProducerIdentity::new(InMemoryProducerIdentityStore::new(64))
    }

    /// End-to-end seam: a message streamed then finalized across passes emits exactly once through
    /// codec + suppressor — the final snapshot is dropped.
    #[test]
    fn streamed_then_final_emits_once_through_the_codec() {
        let codec = MockCodec;
        let mut ids = tracker();
        // pass N: streamed delta → emitted.
        let a = forward_record(&codec, &mut ids, "rt", &("m1", Surface::Streamed, "hi"));
        assert!(a.is_some());
        // pass N+1: final snapshot of the same id → suppressed (None).
        let b = forward_record(&codec, &mut ids, "rt", &("m1", Surface::Final, "hi"));
        assert!(b.is_none(), "duplicate final snapshot must be suppressed");
    }

    /// Non-conversation rows decode to `None` and are skipped without touching the suppressor.
    #[test]
    fn non_conversation_rows_are_skipped() {
        let codec = MockCodec;
        let mut ids = tracker();
        let out = forward_record(&codec, &mut ids, "rt", &("", Surface::Final, ""));
        assert!(out.is_none());
    }

    /// A final-only message (never streamed) is emitted.
    #[test]
    fn final_only_message_is_emitted() {
        let codec = MockCodec;
        let mut ids = tracker();
        let out = forward_record(&codec, &mut ids, "rt", &("m9", Surface::Final, "solo"));
        assert!(out.is_some());
    }

    #[test]
    fn codec_observations_default_to_empty() {
        let codec = MockCodec;
        assert!(codec
            .observations(&("m1", Surface::Streamed, "hi"))
            .is_empty());
    }

    #[derive(Debug, Clone)]
    struct ObservationRecord {
        producer_id: &'static str,
        text: &'static str,
        tool: Option<(&'static str, ToolCallPhase, bool)>,
    }

    struct ObservingCodec;
    impl NativeForwardCodec for ObservingCodec {
        type Record = ObservationRecord;

        fn decode(&self, record: &Self::Record) -> Option<DecodedRecord> {
            if record.producer_id.is_empty() {
                return None;
            }
            Some(DecodedRecord {
                producer_id: record.producer_id.to_string(),
                surface: Surface::Final,
                event: TranscriptEvent {
                    kind: "text".to_string(),
                    data: json!({ "text": record.text }),
                },
            })
        }

        fn observations(&self, record: &Self::Record) -> Vec<NativeForwardObservation> {
            let Some((tool, phase, ok)) = record.tool else {
                return Vec::new();
            };
            vec![NativeForwardObservation::ToolCall(ToolCallObservation {
                tool_call_id: Some(format!("tc-{tool}")),
                tool: tool.to_string(),
                phase,
                ok,
            })]
        }
    }

    #[test]
    fn non_renderable_records_can_emit_observations() {
        let codec = ObservingCodec;
        let record = ObservationRecord {
            producer_id: "",
            text: "",
            tool: Some(("grep", ToolCallPhase::Pre, true)),
        };

        assert!(forward_record(&codec, &mut tracker(), "rt", &record).is_none());
        assert_eq!(
            codec.observations(&record),
            vec![NativeForwardObservation::ToolCall(ToolCallObservation {
                tool_call_id: Some("tc-grep".to_string()),
                tool: "grep".to_string(),
                phase: ToolCallPhase::Pre,
                ok: true,
            })]
        );
    }

    #[test]
    fn renderable_records_can_emit_events_and_observations() {
        let codec = ObservingCodec;
        let record = ObservationRecord {
            producer_id: "m1",
            text: "done",
            tool: Some(("shell", ToolCallPhase::Post, false)),
        };

        let event = forward_record(&codec, &mut tracker(), "rt", &record)
            .expect("renderable record should emit");
        assert_eq!(event.kind, "text");
        assert_eq!(event.data, json!({ "text": "done" }));
        assert_eq!(
            codec.observations(&record),
            vec![NativeForwardObservation::ToolCall(ToolCallObservation {
                tool_call_id: Some("tc-shell".to_string()),
                tool: "shell".to_string(),
                phase: ToolCallPhase::Post,
                ok: false,
            })]
        );
    }
}
