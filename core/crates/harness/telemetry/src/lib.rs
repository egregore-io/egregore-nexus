//! Pure native telemetry policies. Transport and captured-owner admission remain with callers.
mod acp;
mod hermes;
mod opencode;
mod quota;

pub use acp::{AcpContextUsageBasis, AcpQuotaDialect};
pub use hermes::session_usage as decode_session_row_usage;
pub use opencode::OpenCodeTelemetry;
pub const OPENCODE_PLUGIN_SOURCE: &str = include_str!("opencode_plugin.mjs");
pub use quota::AcpQuotaSnapshot;

pub const HERMES_SESSION_USAGE_SOURCE: &str = "hermes.gateway.session.usage";
pub const HERMES_PROMPT_USAGE_SOURCE: &str = "hermes.acp.prompt.usage";
pub const HERMES_CONTEXT_SOURCE: &str = "hermes.acp.usage_update";
pub const OPENCODE_PROMPT_USAGE_SOURCE: &str = "opencode.acp.prompt.usage";
pub const OPENCODE_CONTEXT_SOURCE: &str = "opencode.acp.usage_update";
pub const OPENCODE_MESSAGE_USAGE_SOURCE: &str = "opencode.plugin.assistant.usage";
pub const OPENCODE_MESSAGE_CONTEXT_SOURCE: &str = "opencode.plugin.assistant.context";
