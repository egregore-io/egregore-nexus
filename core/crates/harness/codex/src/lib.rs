//! `nexus-harness-codex` — the Codex harness as a standalone crate.
//!
//! Owns ALL codex-specific code over the shared `nexus-agent` substrate: the ACP
//! adapter ([`CodexAdapter`] / [`codex_command`]), the headed (TUI) command
//! builders, the codex bootstrap, and the registry factory installer
//! ([`register`]). Behavior is identical to the former `nexus-agent::adapter::codex`.

pub mod app_server;
pub mod executable;
pub mod harness;
pub mod headed;
pub mod remote;
pub mod skill;
pub mod storage;

pub use app_server::approvals::{ApprovalHandler, AutoApprove};
pub use app_server::bridge::{
    latest_rollout_thread_id, BridgeLaunchOptions, CodexBridge, ThreadDiscovered,
};
pub use app_server::client::CodexAppServerClient;
pub use app_server::forwarder::{
    spawn_codex_forwarder, spawn_codex_forwarder_with_tool_observations, CodexToolObservationSink,
};
pub use app_server::jsonrpc::{CodexRpcError, JsonRpc, Notification};
pub use app_server::supervisor::{BusMcp, CodexAppServer, SupervisorOpts};
pub use app_server::transport::CodexAppServerTransport;
pub use app_server::turn_completion::{CodexTurnFailure, CodexTurnTracker, CodexTurnWaitError};
pub use executable::resolve_codex_executable;
pub use harness::codex_command;
pub use harness::CodexAdapter;
pub use headed::{
    headed_cli_args, headed_cli_args_with_identity, headed_pty_args, headed_pty_args_with_identity,
};
pub use remote::{remote_command_with_executable, remote_resume_command_with_executable};

use std::sync::Arc;

use nexus_harness_core::{
    native_harness_program, Harness, HarnessIdentity, HeadedCommand, HeadedRuntimeKind,
    NativeProcessPlatform, ResolvedTail, ResumeStyle, SlashCommand, SlashCommandAction,
};

/// Headed Codex harness contract implementation.
///
/// Codex forwards native tails verbatim except for the compatibility spelling
/// `resume <thread>`, which Nexus lifts into the app-server resume sidecar and leaves out of the
/// TUI argv.
#[derive(Debug, Clone, Copy)]
pub struct CodexHarness;

impl Harness for CodexHarness {
    fn program(&self) -> &'static str {
        native_harness_program("codex", NativeProcessPlatform::current())
            .expect("Codex has a native headed executable")
    }

    fn headed_runtime_kind(&self) -> HeadedRuntimeKind {
        HeadedRuntimeKind::CodexAppServer
    }

    fn resume_style(&self) -> ResumeStyle {
        ResumeStyle::Sidecar
    }

    fn display_name(&self) -> &'static str {
        "Codex"
    }

    fn has_native_thread_binding(&self) -> bool {
        true
    }

    fn resume_key_description(&self) -> &'static str {
        "thread id"
    }

    fn resolve_tail(
        &self,
        tail: &[String],
    ) -> Result<ResolvedTail, nexus_harness_core::HarnessError> {
        match tail {
            [cmd, thread_id] if cmd == "resume" => {
                Ok(ResolvedTail::sidecar_resume(thread_id.clone()))
            }
            args => Ok(ResolvedTail::passthrough(args)),
        }
    }

    fn headed_cli_command(
        &self,
        identity: &HarnessIdentity<'_>,
        nexus_exe: &str,
        tail: &[String],
    ) -> Result<HeadedCommand, nexus_harness_core::HarnessError> {
        let resolved = self.resolve_tail(tail)?;
        let mut args = headed_cli_args_with_identity(
            identity.name,
            identity.project,
            nexus_exe,
            identity.client_key,
            identity.agent,
        );
        args.extend(resolved.argv);
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
        let resolved = self.resolve_tail(tail)?;
        let mut args = headed_pty_args_with_identity(
            identity.name,
            identity.project,
            nexus_exe,
            identity.client_key,
            identity.agent,
        );
        args.extend(resolved.argv);
        Ok(HeadedCommand {
            program: self.program().to_string(),
            args,
        })
    }

    fn translate_slash_command(
        &self,
        command: &SlashCommand,
    ) -> Result<SlashCommandAction, nexus_harness_core::HarnessError> {
        if command.verb == "compact" && command.args.is_empty() {
            Ok(SlashCommandAction::NativeCompact)
        } else {
            Err(nexus_harness_core::HarnessError::UnsupportedSlashCommand {
                harness: self.agent_token().to_string(),
                command: command.display_name(),
            })
        }
    }
}

/// Install the real codex adapter factory into a registry (composition-root
/// replacement for the former `AdapterRegistry::with_builtins()` codex arm).
pub fn register(registry: &mut nexus_agent::AdapterRegistry) {
    registry.register(
        &nexus_contracts::HarnessId::new("codex").expect("builtin harness id is valid"),
        Arc::new(|ctx| Arc::new(CodexAdapter::new(ctx)) as Arc<dyn nexus_agent::Adapter>),
    );
}
