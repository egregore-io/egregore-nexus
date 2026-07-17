//! Shared ACP injection/stream vocabulary (backend spec §2.3, §5). ACP `session/prompt` is the
//! injection transport for all kinds and `session/update` is the reply stream; the per-harness
//! adapters ([`super::claude`] and the codex adapter in `nexus-harness-codex`) speak it (or the stream-json fallback). The wire
//! method names are centralized here so the adapters and the daemon's relay agree.

/// ACP method that injects one turn (the rendered `<nexus-batch>` / plain body) into a session.
pub const SESSION_PROMPT: &str = "session/prompt";

/// ACP method whose events carry the agent's chunked reply (relayed as `agent.update`).
pub const SESSION_UPDATE: &str = "session/update";
