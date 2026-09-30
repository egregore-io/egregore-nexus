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
    native_harness_program, Harness, HarnessIdentity, HeadedCommand, HeadedRuntimeKind,
    NativeProcessPlatform, ResolvedTail, SlashCommand, SlashCommandAction,
};

/// Headed Claude harness contract implementation.
///
/// Claude uses the default Nexus rule for launch tails: every arg after
/// `nexus launch claude` is Claude-native argv and is forwarded verbatim.
#[derive(Debug, Clone, Copy)]
pub struct ClaudeHarness;

impl Harness for ClaudeHarness {
    fn kind(&self) -> nexus_contracts::Harness {
        nexus_contracts::Harness::Claude
    }

    fn program(&self) -> &'static str {
        native_harness_program(self.kind(), NativeProcessPlatform::current())
            .expect("Claude has a native headed executable")
    }

    fn headed_runtime_kind(&self) -> HeadedRuntimeKind {
        HeadedRuntimeKind::ClaudeNative
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
}

/// Install the real Claude adapter factory into a registry.
pub fn register(registry: &mut nexus_agent::AdapterRegistry) {
    registry.register(
        nexus_contracts::Harness::Claude,
        Arc::new(|ctx| Arc::new(ClaudeAdapter::new(ctx)) as Arc<dyn nexus_agent::Adapter>),
    );
}
