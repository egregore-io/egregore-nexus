//! Generic ACP adapter for **spawn-spec** (manifest-defined) harnesses.
//!
//! An ACP-pure harness is one whose entire integration is "spawn this command; it speaks ACP on
//! stdio". For those, no per-harness Rust is needed: the spawn command comes from a TOML manifest
//! (parsed at the composition root — see the `nexus` crate's `spawn_spec` module), the contract
//! side is a [`nexus_harness_core::GenericHarness`], and all protocol work lives in [`AcpEngine`].
//! The adapter below is the runtime half: it owns the resolved [`HarnessCommand`] and drives the
//! identical ACP path the code-owned adapters use (`session/new` / `session/load` /
//! `session/prompt` / `session/cancel`).
//!
//! Deliberately **not** here: harness-specific bootstrap (hook installs, config injection, or
//! other bespoke environment setup). A runtime that needs bespoke setup has outgrown a manifest
//! and should become a harness crate.

use async_trait::async_trait;

use nexus_common::NexusError;
use nexus_contracts::{HarnessId, SteerCapability};

use super::engine::{AcpEngine, HarnessCommand, LaunchCtx};
use super::{Adapter, AdapterInjectError, StreamEvent};

/// Adapter for a manifest-defined ACP-pure harness. Owns the resolved spawn command and the live
/// ACP engine; the protocol path is identical to the code-owned ACP adapters.
pub struct SpawnSpecAdapter {
    command: HarnessCommand,
    engine: AcpEngine,
    ctx: LaunchCtx,
}

impl SpawnSpecAdapter {
    /// Build the adapter for `id` over the manifest's base `command`, layered with the launch
    /// context: the launch cwd applies when the manifest doesn't pin one, and the launch env
    /// (agent identity vars) is appended after the manifest env so identity always wins.
    pub fn new(id: HarnessId, mut command: HarnessCommand, ctx: LaunchCtx) -> Self {
        if command.cwd.is_none() {
            command.cwd = ctx.cwd.clone();
        }
        command.env.extend(ctx.env.iter().cloned());
        Self {
            command,
            engine: AcpEngine::for_harness(id),
            ctx,
        }
    }

    /// The resolved spawn command this adapter will run (program + args + cwd + env).
    pub fn command(&self) -> &HarnessCommand {
        &self.command
    }
}

#[async_trait]
impl Adapter for SpawnSpecAdapter {
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
        // session/new on the already-connected engine (no re-spawn) — the resume-fail fallback.
        self.engine
            .new_session(self.ctx.cwd.as_deref(), &self.ctx)
            .await
    }
}
