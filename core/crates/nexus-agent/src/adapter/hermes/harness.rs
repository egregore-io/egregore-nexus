//! The Hermes harness adapter (backend spec §5): injection over **ACP** — the one uniform
//! transport every harness uses. Hermes (Nous Research) speaks ACP natively via its `hermes acp`
//! subcommand (stdio JSON-RPC), so — like opencode and unlike claude/codex which shell out to an
//! npm ACP bridge — the spawn command is just the `hermes` binary itself. All real-protocol work
//! lives in [`AcpEngine`]; this adapter mirrors the codex/opencode adapters.
//!
//! `--accept-hooks` is passed by default so a daemon-spawned (non-TTY) Hermes auto-approves shell
//! hooks instead of blocking on a TTY prompt (mirrors claude/codex bypassing interactive gates).
//!
//! **Why hermes previously never came online, and the fix.** The shared ACP `session/new` path
//! injects a *stdio* `nexus-bus` MCP server into the request (see
//! [`super::super::engine::build_new_session_request`]). hermes REJECTS that: its `initialize`
//! advertises NO `mcpCapabilities` at all, and its `session/new` Pydantic schema does not accept
//! the engine's stdio MCP wire form — so a forced stdio entry makes `session/new` fail with
//! `-32602 Invalid params`, the session never opens, and the agent never reports online (verified
//! live against `hermes 0.17.0`). The fix mirrors opencode and is two-sided:
//!   1. The adapter sets [`LaunchCtx::suppress_acp_mcp`] so the engine does NOT put the stdio MCP
//!      server into hermes's `session/new`. Without the bad MCP entry, `session/new` succeeds and
//!      hermes comes online and replies over ACP. This is the proven online + reply path.
//!   2. The bus is reached via the **`nexus` CLI** (on the agent's PATH), documented in the
//!      installed `nexus-bus` skill — exactly like the claude adapter. A `mcp_servers.nexus-bus`
//!      `config.yaml` entry the hermes way ([`super::skill::write_hermes_mcp_config`]) is available
//!      but **OPT-IN** (`NEXUS_HERMES_BUS_MCP=1`): hermes connects configured MCP servers eagerly at
//!      ACP startup and that blocks `initialize` (verified live — even with a healthy server), so
//!      wiring it on by default would re-introduce the "never came online" bug. See `skill.rs`.
//! The HEADED (TUI) launch builder (`nexus-pty::command::harness_command`) likewise gives hermes a
//! BARE `hermes` (no claude `--mcp-config`, which hermes parses as a positional command and dies).
//!
//! The **real-binary spawn** ([`HermesAdapter::new`] / the [`crate::AdapterRegistry`] built-in)
//! is gated behind the `live` cargo feature: without `live`, `open_session` returns
//! [`NexusError::Adapter`] rather than launching a process. The always-enabled
//! [`HermesAdapter::with_command`] constructor lets the hermetic tests drive the full
//! `open → inject → stream` path against the fake ACP harness without `live`.

use async_trait::async_trait;

use nexus_common::NexusError;
use nexus_contracts::{HarnessId, SteerCapability};
use nexus_harness_core::{native_harness_program, NativeProcessPlatform};

use super::super::engine::{AcpEngine, HarnessCommand, LaunchCtx};
use super::super::{Adapter, AdapterInjectError, StreamEvent};

/// Hermes ACP adapter. Owns the resolved spawn command and the live ACP engine.
pub struct HermesAdapter {
    command: HarnessCommand,
    engine: AcpEngine,
    ctx: LaunchCtx,
}

impl HermesAdapter {
    /// Configured model evidence from the adapter's ACP metadata surface. This does not
    /// advertise per-turn/response model selection. Usage follows the pinned ACP producer's
    /// explicit scope; context and account sources are independent capabilities.
    pub fn model_reporting_profile() -> crate::adapter::AdapterModelReportingProfile {
        use crate::adapter::{AcpModelMetadataDialect, AdapterModelReportingProfile};
        use nexus_contracts::{
            ModelEvidenceCapability, ModelObservationSource, ModelReportBackend,
        };
        let source = ModelObservationSource::new("acp.config_options").unwrap();
        AdapterModelReportingProfile::new(
            ModelReportBackend::new("hermes.acp").unwrap(),
            ModelEvidenceCapability::Supported,
            ModelEvidenceCapability::Unverified,
            ModelEvidenceCapability::Unverified,
            AcpModelMetadataDialect::ConfigOptionsAndLegacyModels {
                config_options_source: source,
                legacy_models_source: ModelObservationSource::new("acp.models").unwrap(),
            },
        )
        .expect("builtin ACP reporting profile is valid")
        .with_telemetry({
            use crate::adapter::{
                AcpPromptUsageScope, AdapterTelemetryCapability, AdapterTelemetryReportingProfile,
            };
            AdapterTelemetryReportingProfile::new(
                AdapterTelemetryCapability::new(
                    ModelEvidenceCapability::Supported,
                    Some(
                        ModelObservationSource::new(
                            nexus_harness_telemetry::HERMES_PROMPT_USAGE_SOURCE,
                        )
                        .unwrap(),
                    ),
                )
                .unwrap(),
                AdapterTelemetryCapability::new(
                    ModelEvidenceCapability::Supported,
                    Some(
                        ModelObservationSource::new(nexus_harness_telemetry::HERMES_CONTEXT_SOURCE)
                            .unwrap(),
                    ),
                )
                .unwrap(),
                AdapterTelemetryCapability::new(ModelEvidenceCapability::Unsupported, None)
                    .unwrap(),
            )
            .with_prompt_usage(AcpPromptUsageScope::SessionCumulative)
            .expect("builtin ACP usage semantics have a captured source")
            .with_context_usage(crate::adapter::AcpContextUsageBasis::HermesRequestEstimate)
            .expect("builtin ACP context semantics have a captured source")
        })
    }

    /// Construct a Hermes adapter that will spawn the **real** `hermes acp` process for the given
    /// working directory.
    pub fn new(mut ctx: LaunchCtx) -> Self {
        // Install launch-local bootstrap into hermes's project cwd: the `nexus-bus` skill (under
        // `.hermes/skills`), the bootstrap-register script, and — crucially — the `nexus-bus` MCP
        // server entry in hermes's `config.yaml` (hermes reads MCP from its config, NOT from ACP
        // `session/new`).
        if let Some(cwd) = &ctx.cwd {
            super::skill::install(cwd, &ctx);
        }
        // hermes rejects a stdio MCP server in ACP `session/new` (`-32602`), so suppress the
        // engine's stdio injection: the bus is wired via hermes's `config.yaml` written above.
        ctx.suppress_acp_mcp = true;
        let mut command = hermes_command(ctx.cwd.clone());
        command.env = ctx.env.clone();
        Self::with_command_and_context(command, ctx)
    }

    /// Construct a Hermes adapter over an explicit [`HarnessCommand`] (the hermetic tests point
    /// this at the fake ACP harness). The protocol path is identical to [`HermesAdapter::new`].
    pub fn with_command(command: HarnessCommand) -> Self {
        let ctx = LaunchCtx {
            cwd: command.cwd.clone(),
            suppress_acp_mcp: true,
            ..Default::default()
        };
        Self::with_command_and_context(command, ctx)
    }

    /// Construct over an explicit command with an already-captured launch context.
    /// The command is used verbatim; launch bootstrap/config preparation stays in `new`.
    /// Reporting is seeded before initialize through the same assembly as normal launches.
    pub fn with_command_and_context(command: HarnessCommand, mut ctx: LaunchCtx) -> Self {
        ctx.suppress_acp_mcp = true;
        Self {
            command,
            engine: AcpEngine::for_harness(
                HarnessId::new("hermes").expect("builtin harness id is valid"),
            )
            .with_reporting(ctx.model_reporting.clone()),
            ctx,
        }
    }

    /// The resolved spawn command this adapter will run (program + args + cwd).
    pub fn command(&self) -> &HarnessCommand {
        &self.command
    }
}

/// Resolve the exact subprocess to put **Hermes** into ACP mode.
///
/// Default: `hermes acp --accept-hooks` (ACP is built into the binary; `--accept-hooks` skips the
/// TTY shell-hook prompt for non-interactive daemon spawns). Overrides:
/// - `NEXUS_HERMES_ACP_CMD` — full program path (e.g. an absolute `hermes`); when set,
///   `NEXUS_HERMES_ACP_ARGS` (whitespace-split) supplies its args (default `acp --accept-hooks`).
pub fn hermes_command(cwd: Option<String>) -> HarnessCommand {
    if let Ok(program) = std::env::var("NEXUS_HERMES_ACP_CMD") {
        let args = std::env::var("NEXUS_HERMES_ACP_ARGS")
            .ok()
            .map(|s| s.split_whitespace().map(str::to_string).collect())
            .unwrap_or_else(|| vec!["acp".to_string(), "--accept-hooks".to_string()]);
        return HarnessCommand {
            program,
            args,
            cwd,
            ..Default::default()
        };
    }
    HarnessCommand {
        program: native_harness_program("hermes", NativeProcessPlatform::current())
            .expect("supported harness has a native executable")
            .into(),
        args: vec!["acp".into(), "--accept-hooks".into()],
        cwd,
        ..Default::default()
    }
}

#[async_trait]
impl Adapter for HermesAdapter {
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

    fn observe_turn(&self) -> nexus_contracts::TurnObservation {
        nexus_contracts::TurnObservation {
            steer_capability: self.steer_capability(),
            ..self.engine.observe_turn()
        }
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

    async fn inject_completion_observed(
        &self,
        prompt: String,
    ) -> Result<Vec<StreamEvent>, AdapterInjectError> {
        self.engine.inject_completion_observed(prompt).await
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
        self.engine
            .new_session(self.ctx.cwd.as_deref(), &self.ctx)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hermes_adapter_suppresses_acp_mcp() {
        // The adapter must mark its ctx so the engine drops the stdio MCP server from session/new —
        // hermes rejects a stdio MCP over ACP (`-32602`), so the bus is wired via config.yaml.
        let adapter = HermesAdapter::with_command(HarnessCommand {
            program: "hermes".into(),
            args: vec!["acp".into(), "--accept-hooks".into()],
            cwd: None,
            ..Default::default()
        });
        assert!(
            adapter.ctx.suppress_acp_mcp,
            "hermes must suppress ACP-injected MCP"
        );
    }
}
