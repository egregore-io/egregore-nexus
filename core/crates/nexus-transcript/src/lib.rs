//! Harness session-transcript parsing → the common `AgentUpdateKind` stream shape. A sibling of
//! `nexus-acp-stream::translate` for the PTY-native path (claude/codex write JSONL transcripts;
//! ACP is not in the loop).
mod claude;
pub mod native_forward;
pub mod producer_identity;
pub use claude::parse_claude_line;
pub use native_forward::{
    forward_record, DecodedRecord, NativeForwardCodec, NativeForwardObservation,
    ToolCallObservation, ToolCallPhase,
};
pub use producer_identity::{
    InMemoryProducerIdentityStore, ProducerIdentity, ProducerIdentityStore, Surface,
};

/// One renderable event parsed from a transcript line. `kind` is an `AgentUpdateKind` wire string
/// (`text`/`thinking`/`tool_call`/`user_input`/`turn_end`); `data` is the JSON payload.
#[derive(Debug, Clone, PartialEq)]
pub struct TranscriptEvent {
    pub kind: String,
    pub data: serde_json::Value,
}
