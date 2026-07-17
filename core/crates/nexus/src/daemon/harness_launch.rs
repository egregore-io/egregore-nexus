//! Per-harness launch policy — the segregation seam.
//!
//! Each harness has its own launch quirks; they live HERE, as one policy
//! function per launch, instead of inline branches scattered through
//! `services/launch.rs`.
//!
//! Claude's transcript JSONL is keyed by
//! the launch cwd's project slug (`~/.claude/projects/<slug>/<sid>.jsonl`),
//! and our streaming/observe capture reads that file. Spawning every session
//! in the SAME folder makes their transcripts collide under one slug, so a
//! fresh claude launch gets its own private per-SESSION folder, and the
//! folder the caller actually wanted to work in is granted via
//! `--add-dir <target>` instead of being the spawn cwd.
//!
//! Resume invariant: `--resume <sid>` must reuse the session's ORIGINAL cwd
//! (the transcript lives under that slug). Resume launches therefore keep
//! the caller-provided cwd verbatim — this policy applies to FRESH launches
//! only; callers pass `is_resume` from the harness args they already parse.

use nexus_contracts::Harness;

/// One launch's resolved cwd/argv policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HarnessLaunchSpec {
    /// The directory the harness process spawns in.
    pub cwd: String,
    /// Daemon-owned private folder: created 0700 (see `ensure_agent_launch_cwd`).
    pub private_cwd: bool,
    /// Appended to the harness argv (e.g. claude `--add-dir <target>`).
    pub extra_args: Vec<String>,
}

fn home() -> String {
    std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string())
}

fn agent_root(agent_id: &str) -> String {
    format!("{}/.nexus/agents/{agent_id}", home())
}

/// Resolve the launch policy for one (harness, agent, session) triple.
///
/// * `requested_cwd` — the folder the caller asked to work in (`req.cwd`).
/// * `is_resume` — a `--resume`/rollout-resume launch; policy is bypassed and
///   the requested cwd (the session's persisted original) is used verbatim.
pub fn harness_launch_spec(
    harness: Harness,
    agent_id: &str,
    session_id: &str,
    requested_cwd: Option<String>,
    is_resume: bool,
) -> HarnessLaunchSpec {
    match harness {
        Harness::Claude if !is_resume => {
            // Fresh claude: private per-session folder; the target folder (if
            // any) is granted, not inhabited.
            let cwd = format!("{}/sessions/{session_id}", agent_root(agent_id));
            let extra_args = match requested_cwd {
                Some(target) if !target.trim().is_empty() => {
                    vec!["--add-dir".to_string(), target]
                }
                _ => Vec::new(),
            };
            HarnessLaunchSpec {
                cwd,
                private_cwd: true,
                extra_args,
            }
        }
        // Status quo for everything else (and claude resumes): requested cwd
        // verbatim, else the per-agent private default.
        _ => match requested_cwd {
            Some(cwd) if !cwd.trim().is_empty() => HarnessLaunchSpec {
                cwd,
                private_cwd: false,
                extra_args: Vec::new(),
            },
            _ => HarnessLaunchSpec {
                cwd: agent_root(agent_id),
                private_cwd: true,
                extra_args: Vec::new(),
            },
        },
    }
}
