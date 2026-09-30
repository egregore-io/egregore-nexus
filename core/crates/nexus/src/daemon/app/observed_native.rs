//! Cold native reporting ownership extends through required daemon liveness work.
use super::*;
use crate::daemon::model_reporting::ModelObserverHandle;
use nexus_agent::adapter::{NativeModelReporting, NativeModelReportingProfile};
use nexus_contracts::model_report::ModelObservationSink;

struct GatewayRowObserver {
    handle: Arc<ModelObserverHandle>,
    coordinator: Arc<crate::daemon::model_reporting::ModelReporting>,
    ready: Arc<std::sync::atomic::AtomicBool>,
}
impl ModelObservationSink for GatewayRowObserver {
    fn accepts_profile(
        &self,
        profile: &nexus_contracts::model_report::ModelProfileIdentity,
    ) -> bool {
        self.handle.accepts_profile(profile)
    }
    fn bind_native_root(&self, root: &str) -> bool {
        self.handle.bind_native_root(root)
    }
    fn observe(&self, update: nexus_contracts::model_report::NativeModelUpdate) -> bool {
        use nexus_contracts::model_report::{ModelEvidenceField, ModelEvidenceValue};
        let activates = update.field == ModelEvidenceField::Configured
            && matches!(&update.value, ModelEvidenceValue::Observed(_));
        let root = update.native_session_id.clone();
        if !self.handle.observe(update) {
            return false;
        }
        if activates
            && self.ready.load(std::sync::atomic::Ordering::Acquire)
            && !self.coordinator.activate(&self.handle, &root)
        {
            self.handle.revoke();
            return false;
        }
        true
    }
    fn revoke(&self) {
        self.handle.revoke();
    }
    fn observe_telemetry(&self, update: nexus_contracts::telemetry::NativeTelemetryUpdate) -> bool {
        // Metrics do not activate a provisional runtime; the configured-row/liveness boundary
        // above remains the only activation authority. The captured handle fences root and close.
        self.handle.observe_telemetry(update)
    }
}

/// Response-only native evidence arrives later than forwarder attachment.
/// The completion invokes this sink while excluding replacement; no async work or current-owner
/// lookup is introduced at that callback. Durable settlement remains the coordinator's task.
struct ClaudeResponseObserver {
    handle: Arc<ModelObserverHandle>,
    coordinator: Arc<crate::daemon::model_reporting::ModelReporting>,
}

impl ModelObservationSink for ClaudeResponseObserver {
    fn accepts_profile(
        &self,
        profile: &nexus_contracts::model_report::ModelProfileIdentity,
    ) -> bool {
        self.handle.accepts_profile(profile)
    }
    fn bind_native_root(&self, root: &str) -> bool {
        self.handle.bind_native_root(root)
    }
    fn observe(&self, update: nexus_contracts::model_report::NativeModelUpdate) -> bool {
        use nexus_contracts::model_report::{ModelEvidenceField, ModelEvidenceValue};
        let activates = update.field == ModelEvidenceField::ResponseReported
            && matches!(&update.value, ModelEvidenceValue::Observed(_));
        let root = update.native_session_id.clone();
        if !self.handle.observe(update) {
            return false;
        }
        if activates && !self.coordinator.activate(&self.handle, &root) {
            self.handle.revoke();
            return false;
        }
        true
    }
    fn revoke(&self) {
        self.handle.revoke();
    }
    fn observe_telemetry(&self, update: nexus_contracts::telemetry::NativeTelemetryUpdate) -> bool {
        // Response usage is observation only; model/liveness remains activation authority.
        self.handle.observe_telemetry(update)
    }
}

pub(super) struct NativeObservation {
    handle: Arc<ModelObserverHandle>,
    reporting: NativeModelReporting,
    runtime: SessionId,
    agent: String,
    armed: bool,
    activation_ready: Option<Arc<std::sync::atomic::AtomicBool>>,
}

impl Drop for NativeObservation {
    fn drop(&mut self) {
        if self.armed {
            self.handle.revoke();
        }
    }
}

fn invalid(message: &str) -> ContractError {
    NexusError::Invalid(message.into()).to_contract_error()
}

impl NativeObservation {
    pub(super) fn defer_gateway_activation(
        &mut self,
        state: &AppState,
    ) -> Result<(), ContractError> {
        let ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
        self.reporting = self
            .reporting
            .profile()
            .clone()
            .capture(Arc::new(GatewayRowObserver {
                handle: self.handle.clone(),
                coordinator: state.model_reporting.clone(),
                ready: ready.clone(),
            }))
            .map_err(|e| e.to_contract_error())?;
        self.activation_ready = Some(ready);
        Ok(())
    }

    pub(super) async fn finish_gateway(
        &mut self,
        state: &AppState,
        supervisor: &PtySupervisor,
    ) -> Result<(), ContractError> {
        let _transition = state.store.lock_presence_transition().await;
        let runtime = AgentRuntimes::new(&state.store)
            .find_by_runtime_id(&self.runtime.0)
            .await
            .map_err(|e| e.to_contract_error())?
            .ok_or_else(|| invalid("captured native runtime disappeared"))?;
        if runtime.agent_id != self.agent || !runtime.active || runtime.stopped_at.is_some() {
            return Err(invalid(
                "captured native runtime is not the successfully live binding",
            ));
        }
        let ready = self
            .activation_ready
            .as_ref()
            .ok_or_else(|| invalid("native gateway activation was not prepared"))?;
        let accepted =
            supervisor.with_current_gateway_model(&self.runtime, &self.reporting, |root| {
                ready.store(true, std::sync::atomic::Ordering::Release);
                root.is_none_or(|root| {
                    self.handle.bind_native_root(root)
                        && state.model_reporting.activate(&self.handle, root)
                })
            });
        if accepted != Some(true) {
            return Err(invalid(
                "captured native gateway or observer was replaced before activation",
            ));
        }
        self.armed = false;
        Ok(())
    }
    pub(super) fn reporting(&self) -> NativeModelReporting {
        self.reporting.clone()
    }

    pub(super) async fn commit(&self, state: &AppState) -> Result<(), ContractError> {
        if state
            .model_reporting
            .commit_claim(&self.handle)
            .await
            .map_err(|e| e.to_contract_error())?
        {
            Ok(())
        } else {
            Err(invalid(
                "captured native model observer was replaced before setup",
            ))
        }
    }

    pub(super) async fn activate_codex(
        &mut self,
        state: &AppState,
        transport: &nexus_harness_codex::CodexAppServerTransport,
    ) -> Result<(), ContractError> {
        // Presence precedes the synchronous native binding callback. Never await under it.
        let _transition = state.store.lock_presence_transition().await;
        let runtime = AgentRuntimes::new(&state.store)
            .find_by_runtime_id(&self.runtime.0)
            .await
            .map_err(|e| e.to_contract_error())?
            .ok_or_else(|| invalid("captured native runtime disappeared"))?;
        if runtime.agent_id != self.agent || !runtime.active || runtime.stopped_at.is_some() {
            return Err(invalid(
                "captured native runtime is not the successfully live binding",
            ));
        }
        let accepted =
            transport.with_current_model_binding(&self.runtime, &self.reporting, |root| {
                self.handle.bind_native_root(root)
                    && state.model_reporting.activate(&self.handle, root)
            });
        if accepted != Some(true) {
            return Err(invalid(
                "captured native binding or observer was replaced before activation",
            ));
        }
        self.armed = false;
        Ok(())
    }

    pub(super) async fn activate_plugin(
        &mut self,
        state: &AppState,
        supervisor: &PtySupervisor,
    ) -> Result<(), ContractError> {
        let _transition = state.store.lock_presence_transition().await;
        let runtime = AgentRuntimes::new(&state.store)
            .find_by_runtime_id(&self.runtime.0)
            .await
            .map_err(|e| e.to_contract_error())?
            .ok_or_else(|| invalid("captured native runtime disappeared"))?;
        if runtime.agent_id != self.agent || !runtime.active || runtime.stopped_at.is_some() {
            return Err(invalid(
                "captured native runtime is not the successfully live binding",
            ));
        }
        let accepted =
            supervisor.with_current_plugin_model(&self.runtime, &self.reporting, |root| {
                self.handle.bind_native_root(root)
                    && state.model_reporting.activate(&self.handle, root)
            });
        if accepted != Some(true) {
            return Err(invalid(
                "captured native binding or observer was replaced before activation",
            ));
        }
        self.armed = false;
        Ok(())
    }
}

impl AppState {
    /// Preserve initial-prompt setup ordering, then claim optional native reporting before spawn.
    pub(super) async fn prepare_terminal_reporting(
        &self,
        descriptor: RegisteredRuntime,
        initial_prompt: Option<&str>,
        client_key: &str,
        owner: Option<&Caller>,
        profile: Option<NativeModelReportingProfile>,
    ) -> Result<
        (
            Option<(RegisteredRuntime, Option<PreparedInitialPrompt>)>,
            Option<NativeObservation>,
        ),
        ContractError,
    > {
        let registered = if let Some(template) = initial_prompt {
            let prepared = self.prepare_initial_prompt(&descriptor, template).await?;
            self.register_prepared_initial_prompt_runtime(&descriptor, client_key, "pty", owner)
                .await?;
            Some((descriptor, prepared))
        } else if profile.is_some() {
            self.register_prepared_runtime(&descriptor, client_key, "pty", owner)
                .await
                .map_err(|error| error.to_contract_error())?;
            Some((descriptor, None))
        } else {
            None
        };
        let observation = if let Some(profile) = profile {
            let descriptor = &registered
                .as_ref()
                .expect("observed terminal was registered")
                .0;
            let mut observation = self.reserve_native_observation(
                &descriptor.agent_id,
                &descriptor.session,
                profile,
            )?;
            observation.defer_gateway_activation(self)?;
            observation.commit(self).await?;
            Some(observation)
        } else {
            None
        };
        Ok((registered, observation))
    }

    /// Re-establish the stream observer, drain loop, and durable liveness rows for a headed PTY
    /// session after either deterministic tmux adoption or same-session respawn.
    ///
    /// Adoption after a daemon restart can race with stale-presence reconciliation: the tmux
    /// process is still real, but the durable `agent_runtimes` row may already be inactive/offline.
    /// Restamping both compatibility `sessions` and stable `agent_runtimes` before ringing the loop
    /// keeps stable-agent `in_flight` rows visible to the drain. Legacy callers keep best-effort
    /// behavior; observed resume calls the checked path before enabling its captured reporter.
    pub(crate) async fn attach_pty_session_machinery(
        &self,
        supervisor: &Arc<PtySupervisor>,
        session: &SessionId,
        project: &str,
        harness: Option<&str>,
        paused: bool,
    ) {
        let _ = self
            .attach_pty_session_machinery_checked(supervisor, session, project, harness, paused)
            .await;
    }

    /// Dispatch a captured resurrection descriptor without rereading or changing its ownership.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn respawn_stored_terminal(
        &self,
        supervisor: &PtySupervisor,
        row: &SessionRow,
        name: &str,
        client_key: &str,
        kind: &HarnessId,
        backend: &str,
        nexus_exe: &str,
        cwd: &str,
        tail: &[String],
        reporting: Option<NativeModelReporting>,
    ) -> Result<(), nexus_pty::PtyError> {
        let size = portable_pty::PtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        };
        let events = self.loop_wiring.as_ref().map(|w| w.events());
        let bell = self.loop_wiring.as_ref().map(|w| w.bell());
        if backend == "pty" {
            supervisor
                .launch_headed_raw_pty_observed(
                    &row.session_id,
                    kind,
                    row.agent_id.as_deref().unwrap_or(name),
                    Some(name),
                    &row.project,
                    client_key,
                    nexus_exe,
                    cwd,
                    size,
                    tail,
                    events,
                    bell,
                    reporting,
                )
                .await
        } else {
            supervisor
                .launch_headed_pty_observed(
                    &row.session_id,
                    kind,
                    row.agent_id.as_deref().unwrap_or(name),
                    Some(name),
                    &row.project,
                    client_key,
                    nexus_exe,
                    cwd,
                    size,
                    tail,
                    events,
                    bell,
                    reporting,
                )
                .await
        }
    }

    pub(super) async fn prepare_gateway_resume(
        &self,
        row: &SessionRow,
        profile: NativeModelReportingProfile,
    ) -> Result<NativeObservation, ContractError> {
        let runtime = AgentRuntimes::new(&self.store)
            .find_by_runtime_id(&row.session_id.0)
            .await
            .map_err(|e| e.to_contract_error())?
            .ok_or_else(|| invalid("observed gateway resume requires its durable runtime"))?;
        if row
            .agent_id
            .as_ref()
            .is_some_and(|agent| *agent != runtime.agent_id)
        {
            return Err(invalid(
                "captured session and runtime agent bindings disagree",
            ));
        }
        let report = runtime
            .model_report
            .as_ref()
            .filter(|report| report.backend == *profile.backend())
            .ok_or_else(|| {
                invalid("cannot resume observed gateway without captured native model source")
            })?;
        let nexus_contracts::ModelEvidenceSlot::Observed { observation, .. } = &report.configured
        else {
            return Err(invalid(
                "cannot resume observed gateway without exact native root evidence",
            ));
        };
        let root = observation
            .native_session_id
            .as_deref()
            .ok_or_else(|| invalid("captured native root is missing"))?;
        let mut captured =
            self.reserve_native_observation(&runtime.agent_id, &row.session_id, profile)?;
        if !captured.handle.bind_native_root(root) {
            return Err(invalid("captured native root changed before resume"));
        }
        captured.defer_gateway_activation(self)?;
        captured.commit(self).await?;
        Ok(captured)
    }

    pub(super) async fn attach_claude_model_reporting(
        &self,
        session: &SessionId,
        owner: &Arc<crate::daemon::claude_native_forwarder::ClaudeTurnCompletion>,
        paths: &nexus_harness_claude::native::bridge::ClaudeNativeBridgePaths,
    ) -> Result<(), ContractError> {
        let source_setup = owner.begin_model_source();
        // This floor is captured before claim waits and before the caller submits its prepared
        // initial/resume prompt. Unresolved sources retain this selection request; input remains
        // gated until the same completion admits its source, before native input handoff.
        let source = owner
            .capture_stored_model_source(&self.store, session, paths)
            .await
            .map_err(|e| e.to_contract_error())?;
        let _transition = self.store.lock_presence_transition().await;
        let runtime = AgentRuntimes::new(&self.store)
            .find_by_runtime_id(&session.0)
            .await
            .map_err(|e| e.to_contract_error())?
            .ok_or_else(|| {
                invalid("captured native runtime disappeared before reporting attachment")
            })?;
        if !runtime.active || runtime.stopped_at.is_some() {
            return Err(invalid(
                "captured native runtime is not live at reporting attachment",
            ));
        }
        let mut observation = self.reserve_native_observation(
            &runtime.agent_id,
            session,
            nexus_harness_claude::native::model_reporting::profile(),
        )?;
        observation.commit(self).await?;
        let reporting = observation
            .reporting
            .profile()
            .clone()
            .capture(Arc::new(ClaudeResponseObserver {
                handle: observation.handle.clone(),
                coordinator: self.model_reporting.clone(),
            }))
            .map_err(|e| e.to_contract_error())?;
        let accepted = self.pty.as_ref().and_then(|supervisor| {
            supervisor.with_claude_owner(session, owner, || {
                owner.attach_model_reporting(reporting, source)
            })
        });
        if accepted != Some(true) {
            return Err(invalid(
                "captured native owner or reporting source changed before attachment",
            ));
        }
        observation.armed = false;
        source_setup.disarm();
        Ok(())
    }

    pub(super) fn reserve_native_observation(
        &self,
        agent: &str,
        runtime: &SessionId,
        profile: NativeModelReportingProfile,
    ) -> Result<NativeObservation, ContractError> {
        let handle = Arc::new(
            self.model_reporting
                .reserve_native(AgentId(agent.into()), runtime.clone(), &profile)
                .map_err(|e| e.to_contract_error())?,
        );
        let reporting = match profile.capture(handle.clone()) {
            Ok(reporting) => reporting,
            Err(error) => {
                handle.revoke();
                return Err(error.to_contract_error());
            }
        };
        Ok(NativeObservation {
            handle,
            reporting,
            runtime: runtime.clone(),
            agent: agent.into(),
            armed: true,
            activation_ready: None,
        })
    }

    pub(super) async fn capture_model_reporting_agent(
        &self,
        row: &SessionRow,
    ) -> Result<String, ContractError> {
        let runtime = AgentRuntimes::new(&self.store)
            .find_by_runtime_id(&row.session_id.0)
            .await
            .map_err(|e| e.to_contract_error())?
            .ok_or_else(|| invalid("observed native resume requires its exact durable runtime"))?;
        if row
            .agent_id
            .as_ref()
            .is_some_and(|id| *id != runtime.agent_id)
        {
            return Err(invalid(
                "captured session and runtime agent bindings disagree",
            ));
        }
        Ok(runtime.agent_id)
    }
}
