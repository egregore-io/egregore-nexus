//! OpenCode harness adapter — self-contained module (mirrors the `claude/` and `codex/` templates).
//!
//! Public surface is identical to the old flat `adapter/opencode.rs`:
//! - `OpenCodeAdapter` — the adapter struct
//! - `opencode_command` — the command resolver

pub mod harness;
pub mod native;
pub mod skill;
/// Compatibility path; native policy is owned by the harness telemetry crate.
pub mod telemetry {
    pub use nexus_harness_telemetry::OpenCodeTelemetry;
}

pub use harness::classify_opencode_provider_error_payload;
pub use harness::opencode_command;
pub use harness::OpenCodeAdapter;
