//! The Claude harness adapter (backend spec §5).
//!
//! Claude Code speaks ACP through the official Claude Code ACP bridge
//! (`@agentclientprotocol/claude-agent-acp`), spawned as a Node subprocess over stdio — the
//! transport AionCore drives for the `claude` backend. We `initialize`, open a session with
//! `session/new` (Claude resumes via `session/load` when the bridge advertises it), inject each
//! turn as one `session/prompt`, and relay the `session/update` `AgentMessageChunk`s back as
//! [`StreamEvent`]s. All real-protocol work lives in [`AcpEngine`], so Claude and Codex talk an
//! identical ACP shape on the bus.
//!
//! Claude Code's own CLI also exposes a **stream-json** mode
//! (`claude --print --input-format stream-json --output-format stream-json`); that is the
//! documented fallback (see `RUN.md`) for environments without the ACP bridge, but the ACP
//! bridge is the canonical path here.
//!
//! The real-binary spawn ([`ClaudeAdapter::new`] / [`crate::register`]) resolves the
//! installed Claude ACP bridge. [`ClaudeAdapter::with_command`] lets the hermetic
//! tests drive the same `open -> inject -> stream` path against the fake ACP
//! harness without a real Claude installation.

use async_trait::async_trait;

use nexus_common::NexusError;
use nexus_contracts::{Harness, SteerCapability};

use nexus_agent::adapter::engine::{AcpEngine, HarnessCommand, LaunchCtx};
use nexus_agent::{Adapter, AdapterInjectError, StreamEvent};
use nexus_harness_core::{native_npm_runner, NativeProcessPlatform};

/// Live-gated Node package that bridges Claude Code to ACP over stdio.
///
/// Do not follow npm's moving `latest` tag here. Bridge 0.59.0 can complete the Claude SDK
/// initialization handshake and then wait indefinitely inside `session/new`; 0.58.1 is also the
/// version distributed by AionUi 2.1.35. Operators can still test a different bridge explicitly
/// through `NEXUS_CLAUDE_ACP_PACKAGE`.
const DEFAULT_CLAUDE_ACP_PACKAGE: &str = "@agentclientprotocol/claude-agent-acp@0.58.1";

/// Claude (ACP bridge) adapter. Owns the resolved spawn command and the live ACP engine.
pub struct ClaudeAdapter {
    command: HarnessCommand,
    engine: AcpEngine,
    ctx: LaunchCtx,
}

impl ClaudeAdapter {
    /// Construct a Claude adapter that will spawn the **real** Claude Code ACP bridge for the
    /// given working directory. The exact invocation is [`claude_command`] (overridable via
    /// env).
    pub fn new(ctx: LaunchCtx) -> Self {
        // Install launch-local Nexus bootstrap files into Claude Code's project cwd: a SessionStart
        // hook that idempotently registers the daemon-minted session, plus the `nexus-bus` skill as
        // fallback/operator guidance. Scoped to this launch, not the user's global skills.
        if let Some(cwd) = &ctx.cwd {
            crate::skill::install(cwd);
        }
        let mut command = claude_command(ctx.cwd.clone());
        command.env = ctx.env.clone();
        Self {
            command,
            engine: AcpEngine::for_harness(Harness::Claude),
            ctx,
        }
    }

    /// Construct a Claude adapter over an explicit [`HarnessCommand`] (the hermetic tests point
    /// this at the fake ACP harness). The protocol path is identical to [`ClaudeAdapter::new`].
    pub fn with_command(command: HarnessCommand) -> Self {
        let ctx = LaunchCtx {
            cwd: command.cwd.clone(),
            ..Default::default()
        };
        Self {
            command,
            engine: AcpEngine::for_harness(Harness::Claude),
            ctx,
        }
    }

    /// The resolved spawn command this adapter will run (program + args + cwd).
    pub fn command(&self) -> &HarnessCommand {
        &self.command
    }
}

/// Resolve the exact subprocess to put **Claude Code** into ACP mode.
///
/// Default: `npx -y @agentclientprotocol/claude-agent-acp@0.58.1` (the live-gated Claude Code ACP
/// bridge over stdio). Overrides (so an operator can pin a vendored bridge or the stream-json
/// fallback):
/// - `NEXUS_CLAUDE_ACP_CMD` — full program path (e.g. `node`); when set,
///   `NEXUS_CLAUDE_ACP_ARGS` (whitespace-split) supplies its args (e.g. the absolute bridge
///   `dist/index.js`).
/// - else `NEXUS_CLAUDE_ACP_PACKAGE` overrides just the npm package name passed to `npx -y`.
pub fn claude_command(cwd: Option<String>) -> HarnessCommand {
    if let Ok(program) = std::env::var("NEXUS_CLAUDE_ACP_CMD") {
        let args = std::env::var("NEXUS_CLAUDE_ACP_ARGS")
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
    let package = std::env::var("NEXUS_CLAUDE_ACP_PACKAGE")
        .unwrap_or_else(|_| DEFAULT_CLAUDE_ACP_PACKAGE.into());
    HarnessCommand {
        program: native_npm_runner(NativeProcessPlatform::current()).into(),
        args: vec!["-y".into(), package],
        cwd,
        ..Default::default()
    }
}

#[async_trait]
impl Adapter for ClaudeAdapter {
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
        SteerCapability::InterruptAndSend
    }

    async fn interrupt_active_turn(&self) -> Result<(), NexusError> {
        self.engine.cancel_active_turn().await
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
