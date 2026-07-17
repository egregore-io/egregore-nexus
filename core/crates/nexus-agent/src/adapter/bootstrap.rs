//! Shared bootstrap test coordination.
//!
//! Harness-specific launch bootstrap lives in each harness module/crate. This
//! module only keeps the process-global lock used by tests that mutate the
//! `NEXUS_SKIP_AGENT_*` environment variables.

/// Process-global mutex for tests that mutate `NEXUS_SKIP_AGENT_*` env vars.
pub static SKIP_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
