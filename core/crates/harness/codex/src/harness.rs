//! The Codex harness adapter (backend spec §5): injection over **ACP** — the one uniform
//! transport every harness uses. Nexus is the orchestrator and drives the agent over ACP itself;
//! there is deliberately NO Codex "app-server" delegation route.
//!
//! Codex speaks ACP through the maintained App Server-based adapter
//! (`@agentclientprotocol/codex-acp`), spawned as a Node subprocess over stdio. We `initialize`,
//! open a session with `session/new` (or re-attach with `session/load`), inject each turn as one
//! `session/prompt`, and relay the `session/update` `AgentMessageChunk`s back as [`UpdateChunk`]s.
//! All real-protocol work lives in [`AcpEngine`].
//!
//! The nexus-bus MCP server is wired into the headless ACP path through the ACP `session/new`
//! request itself (see [`super::super::engine::build_new_session_request`]) — i.e. over the
//! protocol, NOT via a codex CLI flag. Codex REJECTS claude's `--mcp-config`; the headed (TUI)
//! launch wires MCP through a codex `-c mcp_servers.nexus-bus={…}` TOML override instead (see
//! `nexus-pty::command::harness_command`). The headless bridge spawn here therefore carries NO
//! `--mcp-config` and needs no `-c` MCP override — the bus tools arrive over ACP.
//!
//! The **real-binary spawn** ([`CodexAdapter::new`] / the [`crate::AdapterRegistry`] built-in)
//! is gated behind the `live` cargo feature, like the router's Codex E2E: without `live`,
//! `open_session` returns [`NexusError::Adapter`] rather than launching a process. The engine
//! and the always-enabled [`CodexAdapter::with_command`] constructor let the hermetic tests
//! drive the full `open → inject → stream` path against the fake ACP harness without `live`.

use async_trait::async_trait;

use nexus_common::NexusError;
use nexus_contracts::{HarnessId, SteerCapability};

use nexus_agent::adapter::engine::{AcpEngine, HarnessCommand, LaunchCtx};
use nexus_agent::{Adapter, AdapterInjectError, StreamEvent};
use nexus_harness_core::{native_npm_runner, NativeProcessPlatform};

use super::skill;

/// Maintained App Server-based Codex ACP adapter, pinned for reproducible launches.
const DEFAULT_CODEX_ACP_PACKAGE: &str = "@agentclientprotocol/codex-acp@1.1.2";

/// Codex ACP adapter. Owns the resolved spawn command and the live ACP engine.
pub struct CodexAdapter {
    command: HarnessCommand,
    engine: AcpEngine,
    ctx: LaunchCtx,
}

impl CodexAdapter {
    /// Construct a Codex adapter that will spawn the **real** Codex ACP bridge for the given
    /// working directory. The exact invocation is [`codex_command`] (overridable via env).
    pub fn new(ctx: LaunchCtx) -> Self {
        // Install launch-local Nexus bootstrap files into Codex's project cwd: a SessionStart hook
        // that idempotently registers the daemon-minted session, plus the `nexus-bus` skill as
        // fallback/operator guidance. Scoped to this launch, not the user's global skills.
        if let Some(cwd) = &ctx.cwd {
            skill::install(cwd);
        }
        let mut command = codex_command(ctx.cwd.clone());
        command.env = ctx.env.clone();
        Self {
            command,
            engine: AcpEngine::for_harness(
                HarnessId::new("codex").expect("builtin harness id is valid"),
            ),
            ctx,
        }
    }

    /// Construct a Codex adapter over an explicit [`HarnessCommand`] (the hermetic tests point
    /// this at the fake ACP harness). The protocol path is identical to [`CodexAdapter::new`].
    pub fn with_command(command: HarnessCommand) -> Self {
        let ctx = LaunchCtx {
            cwd: command.cwd.clone(),
            ..Default::default()
        };
        Self {
            command,
            engine: AcpEngine::for_harness(
                HarnessId::new("codex").expect("builtin harness id is valid"),
            ),
            ctx,
        }
    }

    /// The resolved spawn command this adapter will run (program + args + cwd).
    pub fn command(&self) -> &HarnessCommand {
        &self.command
    }
}

/// Resolve the exact subprocess to put **Codex** into ACP mode.
///
/// Default: `npx -y @agentclientprotocol/codex-acp@1.1.2` (the maintained App Server-based
/// Codex ACP adapter, run over stdio).
/// Overrides (so an operator can pin a vendored bridge or a different runtime):
/// - `NEXUS_CODEX_ACP_CMD` — full program path (e.g. `node`); when set, `NEXUS_CODEX_ACP_ARGS`
///   (whitespace-split) supplies its args (e.g. the absolute `codex-acp.js` entrypoint).
/// - else `NEXUS_CODEX_ACP_PACKAGE` overrides just the npm package name passed to `npx -y`.
pub fn codex_command(cwd: Option<String>) -> HarnessCommand {
    if let Ok(program) = std::env::var("NEXUS_CODEX_ACP_CMD") {
        let args = std::env::var("NEXUS_CODEX_ACP_ARGS")
            .ok()
            .map(|s| s.split_whitespace().map(str::to_string).collect())
            .unwrap_or_default();
        return HarnessCommand {
            program,
            args,
            cwd,
            ..Default::default()
        };
    }
    let package = std::env::var("NEXUS_CODEX_ACP_PACKAGE")
        .unwrap_or_else(|_| DEFAULT_CODEX_ACP_PACKAGE.into());
    HarnessCommand {
        program: native_npm_runner(NativeProcessPlatform::current()).into(),
        args: vec!["-y".into(), package],
        cwd,
        ..Default::default()
    }
}

#[async_trait]
impl Adapter for CodexAdapter {
    async fn open_session(&self) -> Result<(), NexusError> {
        self.engine.spawn_and_initialize(&self.command).await?;
        self.engine
            .new_session(self.ctx.cwd.as_deref(), &self.ctx)
            .await
    }

    async fn resume(&self, resume_key: &str) -> Result<(), NexusError> {
        self.engine.spawn_and_initialize(&self.command).await?;
        self.engine
            .load_session(resume_key, self.ctx.cwd.as_deref(), &self.ctx)
            .await
    }

    async fn inject(&self, prompt: String) -> Result<(), AdapterInjectError> {
        self.engine.inject(prompt).await
    }

    fn steer_capability(&self) -> SteerCapability {
        // The ACP adapter has session/cancel but no native turn/steer. Headed Codex app-server
        // advertises NativeSteer from its separate transport implementation.
        SteerCapability::InterruptAndSend
    }

    async fn interrupt_active_turn(&self) -> Result<(), NexusError> {
        self.engine.cancel_active_turn().await
    }

    async fn compact(&self) -> Result<(), AdapterInjectError> {
        // The pinned codex-acp bridge recognizes this ACP session/prompt as a built-in command
        // and invokes Codex `thread/compact/start`; it never reaches the model as chat text.
        self.engine.inject("/compact".to_string()).await
    }

    async fn inject_with_accepted_event(
        &self,
        prompt: String,
        accepted_event: Option<StreamEvent>,
    ) -> Result<(), AdapterInjectError> {
        self.engine
            .inject_with_accepted_event(prompt, accepted_event)
            .await
    }

    async fn inject_completion_observed(
        &self,
        prompt: String,
    ) -> Result<Vec<StreamEvent>, AdapterInjectError> {
        self.engine.inject_completion_observed(prompt).await
    }

    async fn stream_updates(&self) -> Result<Vec<StreamEvent>, NexusError> {
        Ok(self.engine.take_updates())
    }

    async fn kill(&self) {
        self.engine.kill().await
    }

    fn install_live(&self) -> Option<tokio::sync::mpsc::UnboundedReceiver<StreamEvent>> {
        Some(self.engine.install_live())
    }

    fn clear_live(&self) {
        self.engine.clear_live()
    }

    fn runtime_process_ids(&self) -> Option<nexus_common::RuntimeProcessIds> {
        self.engine.runtime_process_ids()
    }

    async fn acp_session_id(&self) -> Option<String> {
        self.engine.acp_session_id().await
    }

    async fn new_session_only(&self) -> Result<(), NexusError> {
        // session/new on the already-connected engine (no re-spawn) — the resume-fail fallback.
        self.engine
            .new_session(self.ctx.cwd.as_deref(), &self.ctx)
            .await
    }
}
