//! Captured ACP launch reporting. No native decoder or provider capability is enabled here.
use super::*;
use crate::daemon::model_reporting::ModelObserverHandle;
use nexus_agent::adapter::AdapterModelReportingProfile;
use nexus_agent::service::OpenedSession;
use nexus_contracts::model_report::ModelObservationSink;

/// Extends cancellation ownership beyond Agent's successful open through required liveness and
/// final binding validation. Drop closes only the captured observer; its worker owns settlement.
pub(super) struct AdapterObservation {
    handle: Arc<ModelObserverHandle>,
    runtime: SessionId,
    agent: String,
    opened: Option<OpenedSession>,
    armed: bool,
}

impl Drop for AdapterObservation {
    fn drop(&mut self) {
        if self.armed {
            self.handle.revoke();
        }
    }
}

impl AdapterObservation {
    pub(super) fn sink(&self) -> Arc<dyn ModelObservationSink> {
        self.handle.clone()
    }

    pub(super) async fn commit(
        &self,
        state: &AppState,
    ) -> Result<(), nexus_contracts::ContractError> {
        if state
            .model_reporting
            .commit_claim(&self.handle)
            .await
            .map_err(|e| e.to_contract_error())?
        {
            Ok(())
        } else {
            Err(invalid(
                "captured model observer was replaced before native open",
            ))
        }
    }

    pub(super) fn opened(&mut self, receipt: OpenedSession) -> Option<RuntimeProcessIds> {
        let ids = receipt.process_ids();
        self.opened = Some(receipt);
        ids
    }

    pub(super) async fn activate(
        &mut self,
        state: &AppState,
        agent: &nexus_agent::Agent,
    ) -> Result<(), nexus_contracts::ContractError> {
        let receipt = self
            .opened
            .as_ref()
            .ok_or_else(|| invalid("no captured adapter open receipt"))?;
        let root = receipt
            .native_root()
            .ok_or_else(|| invalid("opened adapter supplied no native root for model reporting"))?;
        // Acquire presence before Agent's synchronous binding exclusion. Agent removal releases
        // its sessions lock before asking Identity to acquire presence. Never await in callback.
        let _transition = state.store.lock_presence_transition().await;
        let runtime = AgentRuntimes::new(&state.store)
            .find_by_runtime_id(&self.runtime.0)
            .await
            .map_err(|e| e.to_contract_error())?
            .ok_or_else(|| invalid("captured model runtime disappeared after native open"))?;
        if runtime.agent_id != self.agent || !runtime.active || runtime.stopped_at.is_some() {
            return Err(invalid(
                "captured model runtime is not the successfully live binding",
            ));
        }
        let accepted = agent.with_current_binding(receipt, || {
            self.handle.bind_native_root(root) && state.model_reporting.activate(&self.handle, root)
        });
        if accepted != Some(true) {
            return Err(invalid(
                "captured native adapter or model observer was replaced before activation",
            ));
        }
        self.armed = false;
        Ok(())
    }
}

fn invalid(message: &str) -> nexus_contracts::ContractError {
    NexusError::Invalid(message.into()).to_contract_error()
}

impl AppState {
    pub(super) async fn capture_adapter_reporting_agent(
        &self,
        row: &SessionRow,
        profile: Option<&AdapterModelReportingProfile>,
    ) -> Result<Option<String>, nexus_contracts::ContractError> {
        if profile.is_none() {
            return Ok(None);
        }
        self.capture_model_reporting_agent(row).await.map(Some)
    }

    /// Called only by cold winning branches, before launch-context filesystem/constructor work.
    /// None preserves legacy behavior; an opted-in profile never falls back after admission error.
    pub(super) fn reserve_adapter_observation(
        &self,
        agent: &str,
        runtime: &SessionId,
        profile: Option<&AdapterModelReportingProfile>,
    ) -> Result<Option<AdapterObservation>, nexus_contracts::ContractError> {
        let Some(profile) = profile else {
            return Ok(None);
        };
        let handle = self
            .model_reporting
            .reserve_adapter(AgentId(agent.into()), runtime.clone(), profile)
            .map_err(|e| e.to_contract_error())?;
        Ok(Some(AdapterObservation {
            handle: Arc::new(handle),
            runtime: runtime.clone(),
            agent: agent.into(),
            opened: None,
            armed: true,
        }))
    }
}
