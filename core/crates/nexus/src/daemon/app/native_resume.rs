//! Collect daemon-owned identity evidence without interpreting native resume grammar.

use super::*;
use nexus_harness_core::{Harness, NativeResumeEvidence, NativeResumePlan};
use nexus_store::repos::IdentitySessionRow;

impl AppState {
    /// Caller holds the runtime revival gate through validation and reuse or launch.
    /// The harness selects native semantics; this adapter enforces Nexus ownership.
    pub(super) async fn native_resume_plan_for_row(
        &self,
        row: &SessionRow,
        harness: &dyn Harness,
        agent_id: &str,
        capsule: Option<&IdentitySessionRow>,
        observed_key: Option<&str>,
        requested_key: Option<&str>,
    ) -> Result<NativeResumePlan, nexus_contracts::ContractError> {
        let bindings = NativeThreadBindings::new(&self.store);
        let binding = bindings
            .find_for_runtime(harness.agent_token(), &row.session_id.0)
            .await
            .map_err(|error| error.to_contract_error())?;
        let invalid = |reason: &str| {
            NexusError::Invalid(format!(
                "cannot revive runtime {}: {reason}",
                row.session_id
            ))
            .to_contract_error()
        };
        if binding
            .as_ref()
            .is_some_and(|binding| binding.agent_id != agent_id)
            || capsule.is_some_and(|capsule| {
                capsule.agent_id != agent_id || capsule.runtime_id != row.session_id.0
            })
        {
            return Err(invalid("native resume identity belongs to another owner"));
        }
        let plan = harness
            .native_resume_plan(NativeResumeEvidence {
                requested_key,
                binding_key: binding
                    .as_ref()
                    .map(|binding| binding.native_thread_id.as_str()),
                capsule_key: capsule.and_then(|capsule| capsule.native_resume_key.as_deref()),
                observed_key,
                legacy_key: row.harness_session_id.as_deref(),
            })
            .map_err(|error| invalid(&error.to_string()))?;
        if let Some(owner) = bindings
            .find(harness.agent_token(), &plan.native_key)
            .await
            .map_err(|error| error.to_contract_error())?
        {
            if owner.agent_id != agent_id
                || owner
                    .last_runtime_id
                    .as_deref()
                    .is_some_and(|runtime| runtime != row.session_id.0)
            {
                return Err(invalid("native resume identity belongs to another runtime"));
            }
        }
        Ok(plan)
    }
}
