//! Per-harness launch policy — the segregation seam.
//!
//! Launch cwd/argv quirks are owned by each harness crate behind
//! [`nexus_harness_core::Harness::launch_spec`]; this module only resolves the
//! per-agent root and dispatches through the registry. See
//! `nexus-harness-claude` for the claude transcript-slug quirk.

use crate::harness_registry::harness_registry_by_id;
use nexus_contracts::HarnessId;

pub use nexus_harness_core::HarnessLaunchSpec;

fn home() -> String {
    std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string())
}

fn agent_root(agent_id: &str) -> String {
    format!("{}/.nexus/agents/{agent_id}", home())
}

/// Resolve the launch policy for one (harness, agent, session) triple.
///
/// * `requested_cwd` — the folder the caller asked to work in (`req.cwd`).
/// * `is_resume` — a `--resume`/rollout-resume launch; harness policy is
///   bypassed and the requested cwd (the session's persisted original) is
///   used verbatim.
pub fn harness_launch_spec(
    harness: &HarnessId,
    agent_id: &str,
    session_id: &str,
    requested_cwd: Option<String>,
    is_resume: bool,
) -> HarnessLaunchSpec {
    harness_registry_by_id(harness).launch_spec(
        &agent_root(agent_id),
        session_id,
        requested_cwd,
        is_resume,
    )
}
