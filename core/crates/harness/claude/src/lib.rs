//! `nexus-harness-claude` -- the Claude harness as a standalone crate.
//!
//! Owns Claude-specific code over the shared `nexus-agent` substrate: the ACP
//! adapter ([`ClaudeAdapter`] / [`claude_command`]), headed command builders,
//! Claude launch bootstrap, and the registry factory installer ([`register`]).
//! The headless ACP behavior is identical to the former
//! `nexus_agent::adapter::claude` module.

pub mod harness;
pub mod headed;
pub mod native;
pub mod skill;
pub mod storage;

pub use harness::claude_command;
pub use harness::ClaudeAdapter;
pub use headed::{
    headed_cli_args, headed_cli_args_with_identity, headed_pty_args, headed_pty_args_with_identity,
};

use std::sync::Arc;

use nexus_harness_core::{
    native_harness_program, Harness, HarnessIdentity, HarnessLaunchSpec, HeadedCommand,
    HeadedRuntimeKind, NativeProcessPlatform, ResolvedTail, ResumeStyle, SlashCommand,
    SlashCommandAction,
};

/// Headed Claude harness contract implementation.
///
/// Claude uses the default Nexus rule for launch tails: every arg after
/// `nexus launch claude` is Claude-native argv and is forwarded verbatim.
#[derive(Debug, Clone, Copy)]
pub struct ClaudeHarness;

impl Harness for ClaudeHarness {
    fn program(&self) -> &'static str {
        native_harness_program("claude", NativeProcessPlatform::current())
            .expect("Claude has a native headed executable")
    }

    fn headed_runtime_kind(&self) -> HeadedRuntimeKind {
        HeadedRuntimeKind::ClaudeNative
    }

    fn resume_style(&self) -> ResumeStyle {
        ResumeStyle::Flag(&["--resume"])
    }

    fn display_name(&self) -> &'static str {
        "Claude"
    }

    fn attach_backend(&self) -> Option<&'static str> {
        Some("pty")
    }

    fn acp_attach_revivable(&self) -> bool {
        true
    }

    fn has_native_thread_binding(&self) -> bool {
        true
    }

    fn resume_key_description(&self) -> &'static str {
        "--resume session id"
    }

    fn resolve_tail(
        &self,
        tail: &[String],
    ) -> Result<ResolvedTail, nexus_harness_core::HarnessError> {
        Ok(ResolvedTail::passthrough(tail))
    }

    fn revive_tail(&self) -> Result<ResolvedTail, nexus_harness_core::HarnessError> {
        Err(nexus_harness_core::HarnessError::InvalidTail(
            "Claude revive requires an exact --resume <session_id>; refusing unsafe --continue"
                .into(),
        ))
    }

    fn headed_cli_command(
        &self,
        identity: &HarnessIdentity<'_>,
        nexus_exe: &str,
        tail: &[String],
    ) -> Result<HeadedCommand, nexus_harness_core::HarnessError> {
        let mut args = headed_cli_args_with_identity(
            identity.name,
            identity.project,
            nexus_exe,
            identity.client_key,
            identity.agent,
        );
        args.extend(tail.iter().cloned());
        Ok(HeadedCommand {
            program: self.program().to_string(),
            args,
        })
    }

    fn headed_pty_command(
        &self,
        identity: &HarnessIdentity<'_>,
        nexus_exe: &str,
        tail: &[String],
    ) -> Result<HeadedCommand, nexus_harness_core::HarnessError> {
        let mut args = headed_pty_args_with_identity(
            identity.name,
            identity.project,
            nexus_exe,
            identity.client_key,
            identity.agent,
        );
        args.extend(tail.iter().cloned());
        Ok(HeadedCommand {
            program: self.program().to_string(),
            args,
        })
    }

    fn translate_slash_command(
        &self,
        _command: &SlashCommand,
    ) -> Result<SlashCommandAction, nexus_harness_core::HarnessError> {
        Ok(SlashCommandAction::InjectVerbatim)
    }

    /// The claude quirk: claude's transcript jsonl is keyed
    /// by the launch cwd's project slug
    /// (`~/.claude/projects/<slug>/<sid>.jsonl`), and our streaming/observe
    /// capture reads that file. Spawning every session in the SAME folder makes
    /// their transcripts collide under one slug, so a fresh claude launch gets
    /// its own private per-SESSION folder, and the folder the caller actually
    /// wanted to work in is granted via `--add-dir <target>` instead of being
    /// the spawn cwd.
    ///
    /// Resume invariant: `--resume <sid>` must reuse the session's ORIGINAL
    /// cwd (the transcript lives under that slug), so resume launches keep the
    /// status-quo policy (caller-provided cwd verbatim).
    fn launch_spec(
        &self,
        agent_root: &str,
        session_id: &str,
        requested_cwd: Option<String>,
        is_resume: bool,
    ) -> HarnessLaunchSpec {
        if is_resume {
            return HarnessLaunchSpec::status_quo(agent_root, requested_cwd);
        }
        // Fresh claude: private per-session folder; the target folder (if
        // any) is granted, not inhabited.
        let cwd = format!("{agent_root}/sessions/{session_id}");
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
}

/// Install the real Claude adapter factory into a registry.
pub fn register(registry: &mut nexus_agent::AdapterRegistry) {
    registry.register(
        &nexus_contracts::HarnessId::new("claude").expect("builtin harness id is valid"),
        Arc::new(|ctx| Arc::new(ClaudeAdapter::new(ctx)) as Arc<dyn nexus_agent::Adapter>),
    );
}
