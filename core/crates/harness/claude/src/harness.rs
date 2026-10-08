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
use nexus_contracts::{HarnessId, SteerCapability};

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
    preparation_error: Option<String>,
}

impl ClaudeAdapter {
    /// Configured model evidence from the adapter's ACP metadata surface. This does not
    /// advertise per-turn/response model selection. Usage follows the pinned ACP producer's
    /// explicit scope; context and account sources are independent capabilities.
    pub fn model_reporting_profile() -> nexus_agent::adapter::AdapterModelReportingProfile {
        use nexus_agent::adapter::{AcpModelMetadataDialect, AdapterModelReportingProfile};
        use nexus_contracts::{
            ModelEvidenceCapability, ModelObservationSource, ModelReportBackend,
        };
        let source = ModelObservationSource::new("claude.acp.config_options").unwrap();
        AdapterModelReportingProfile::new(
            ModelReportBackend::new("claude.acp").unwrap(),
            ModelEvidenceCapability::Supported,
            ModelEvidenceCapability::Unverified,
            ModelEvidenceCapability::Unverified,
            AcpModelMetadataDialect::ConfigOptions { source },
        )
        .expect("builtin ACP reporting profile is valid")
        .with_telemetry({
            use nexus_agent::adapter::{
                AcpPromptUsageScope, AdapterTelemetryCapability, AdapterTelemetryReportingProfile,
            };
            AdapterTelemetryReportingProfile::new(
                AdapterTelemetryCapability::new(
                    ModelEvidenceCapability::Supported,
                    Some(ModelObservationSource::new("claude.acp.prompt.usage").unwrap()),
                )
                .unwrap(),
                AdapterTelemetryCapability::new(
                    ModelEvidenceCapability::Supported,
                    Some(ModelObservationSource::new("claude.acp.usage_update").unwrap()),
                )
                .unwrap(),
                AdapterTelemetryCapability::new(
                    ModelEvidenceCapability::Supported,
                    Some(ModelObservationSource::new("claude.acp.rate_limit.selected").unwrap()),
                )
                .unwrap(),
            )
            .with_prompt_usage(AcpPromptUsageScope::LastPrompt)
            .expect("builtin ACP usage semantics have a captured source")
            .with_context_usage(
                nexus_agent::adapter::AcpContextUsageBasis::ClaudeAssistantOrCompactionProxy,
            )
            .expect("builtin ACP context semantics have a captured source")
            .with_quota_dialect(nexus_agent::adapter::AcpQuotaDialect::ClaudeSelectedRateLimit)
            .expect("builtin Claude account extension has a captured source")
        })
    }

    /// Construct a Claude adapter that will spawn the **real** Claude Code ACP bridge for the
    /// given working directory. The exact invocation is [`claude_command`] (overridable via
    /// env).
    /// Bootstrap augmentation of `session_meta.claudeCode.options.settings` accepts inline
    /// objects only, not the upstream SDK's settings-file path form. Provider-managed settings
    /// files remain in their normal layers, without copying their hooks into the overlay.
    pub fn new(mut ctx: LaunchCtx) -> Self {
        // Install launch-local Nexus bootstrap files into Claude Code's project cwd: a SessionStart
        // hook that idempotently registers the daemon-minted session, plus the `nexus-bus` skill as
        // fallback/operator guidance. Scoped to this launch, not the user's global skills.
        if let Some(cwd) = &ctx.cwd {
            crate::skill::install(cwd);
        }
        let preparation_error = if ctx.cwd.is_some() {
            add_launch_settings(&mut ctx).err()
        } else {
            None
        };
        let mut command = claude_command(ctx.cwd.clone());
        command.env = ctx.env.clone();
        let mut adapter = Self::with_command_and_context(command, ctx);
        adapter.preparation_error = preparation_error;
        adapter
    }

    /// Construct a Claude adapter over an explicit [`HarnessCommand`] (the hermetic tests point
    /// this at the fake ACP harness). The protocol path is identical to [`ClaudeAdapter::new`].
    pub fn with_command(command: HarnessCommand) -> Self {
        let ctx = LaunchCtx {
            cwd: command.cwd.clone(),
            ..Default::default()
        };
        Self::with_command_and_context(command, ctx)
    }

    /// Construct over an explicit command with an already-captured launch context.
    /// The command is used verbatim; launch bootstrap/config preparation stays in `new`.
    /// Reporting is seeded before initialize through the same assembly as normal launches.
    pub fn with_command_and_context(command: HarnessCommand, ctx: LaunchCtx) -> Self {
        Self {
            command,
            engine: AcpEngine::for_harness(
                HarnessId::new("claude").expect("builtin harness id is valid"),
            )
            .with_reporting(ctx.model_reporting.clone()),
            ctx,
            preparation_error: None,
        }
    }

    /// The resolved spawn command this adapter will run (program + args + cwd).
    pub fn command(&self) -> &HarnessCommand {
        &self.command
    }
}

fn add_launch_settings(ctx: &mut LaunchCtx) -> Result<(), String> {
    let Some(additions) = crate::skill::launch_settings() else {
        return Ok(());
    };
    let raw = ctx
        .env
        .iter()
        .rev()
        .find(|(key, _)| key == "CLAUDE_MODEL_CONFIG")
        .map(|(_, value)| value.clone())
        .or_else(|| std::env::var("CLAUDE_MODEL_CONFIG").ok());
    let model_config: serde_json::Value = match raw.filter(|value| !value.is_empty()) {
        Some(value) => {
            serde_json::from_str(&value).map_err(|_| "invalid CLAUDE_MODEL_CONFIG JSON")?
        }
        None => serde_json::json!({}),
    };
    let models = model_config
        .as_object()
        .ok_or("CLAUDE_MODEL_CONFIG must be an object")?;
    let meta = ctx.session_meta.get_or_insert_with(Default::default);
    let claude = meta
        .entry("claudeCode")
        .or_insert_with(|| serde_json::json!({}))
        .as_object_mut()
        .ok_or("Claude launch metadata must be an object")?;
    let options = claude
        .entry("options")
        .or_insert_with(|| serde_json::json!({}))
        .as_object_mut()
        .ok_or("Claude launch options must be an object")?;
    // The pinned bridge ignores its model-config fallback when settings are supplied.
    // Preserve that fallback unless the caller already owns an explicit settings overlay.
    if !options.contains_key("settings") {
        let fallback: serde_json::Map<String, serde_json::Value> = models
            .iter()
            .filter(|(key, _)| matches!(key.as_str(), "modelOverrides" | "availableModels"))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        options.insert("settings".into(), fallback.into());
    }
    let settings = options
        .get_mut("settings")
        .unwrap()
        .as_object_mut()
        .ok_or("Claude launch settings must be an object")?;
    let hooks = settings
        .entry("hooks")
        .or_insert_with(|| serde_json::json!({}))
        .as_object_mut()
        .ok_or("Claude launch hooks must be an object")?;
    let starts = hooks
        .entry("SessionStart")
        .or_insert_with(|| serde_json::json!([]))
        .as_array_mut()
        .ok_or("Claude launch SessionStart hooks must be an array")?;
    starts.extend(
        additions["hooks"]["SessionStart"]
            .as_array()
            .unwrap()
            .iter()
            .cloned(),
    );
    Ok(())
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
        if let Some(error) = &self.preparation_error {
            return Err(NexusError::Adapter(error.clone()));
        }
        self.engine.spawn_and_initialize(&self.command).await?;
        self.engine
            .new_session(self.ctx.cwd.as_deref(), &self.ctx)
            .await
    }

    async fn resume(&self, resume_key: &str) -> Result<(), NexusError> {
        if let Some(error) = &self.preparation_error {
            return Err(NexusError::Adapter(error.clone()));
        }
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

    fn observe_turn(&self) -> nexus_contracts::TurnObservation {
        nexus_contracts::TurnObservation {
            steer_capability: self.steer_capability(),
            ..self.engine.observe_turn()
        }
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
        if let Some(error) = &self.preparation_error {
            return Err(NexusError::Adapter(error.clone()));
        }
        // session/new on the already-connected engine (no re-spawn) — the resume-fail fallback.
        self.engine
            .new_session(self.ctx.cwd.as_deref(), &self.ctx)
            .await
    }
}
