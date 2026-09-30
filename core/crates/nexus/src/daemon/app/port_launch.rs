//! Launch and register runtimes through the generic agent port.

use super::*;

impl AppState {
    pub(super) async fn launch_agent_via_port(
        &self,
        req: SpawnRequest,
        project: &str,
        owner: Option<&Caller>,
    ) -> Result<SpawnResponse, nexus_contracts::ContractError> {
        let resp = self.agent.launch(req.clone()).await?;
        let identity = self
            .resolve_launch_identity_for_session(&req, project, &resp.session_id)
            .await
            .map_err(|e| e.to_contract_error())?;
        let client_key = Self::new_client_key();
        let registered = Self::runtime_descriptor(
            &resp.session_id,
            &identity.agent_id,
            identity.name.as_deref(),
            &identity.project,
            req.kind.clone(),
            req.role.clone(),
            req.cwd.clone(),
        );
        let prepared_initial_prompt = if let Some(template) = req.initial_prompt.as_deref() {
            Some(
                match self.prepare_initial_prompt(&registered, template).await? {
                    Some(prepared) => prepared,
                    None => {
                        self.register_prepared_runtime(&registered, &client_key, "acp", owner)
                            .await
                            .map_err(|e| e.to_contract_error())?;
                        self.wake_registered_runtime(&registered)
                            .await
                            .map_err(|e| e.to_contract_error())?;
                        self.persist_runtime_resurrection_descriptor(
                            &registered,
                            "headless",
                            "acp",
                            None,
                        )
                        .await
                        .map_err(|e| e.to_contract_error())?;
                        return Ok(resp);
                    }
                },
            )
        } else {
            None
        };
        if prepared_initial_prompt.is_some() {
            self.register_prepared_initial_prompt_runtime(&registered, &client_key, "acp", owner)
                .await?;
        } else {
            self.register_prepared_runtime(&registered, &client_key, "acp", owner)
                .await
                .map_err(|e| e.to_contract_error())?;
        }
        if let Some(prepared) = prepared_initial_prompt {
            if let Err(error) = self
                .deliver_prepared_initial_prompt(&registered, prepared)
                .await
            {
                self.fail_initial_prompt_runtime(&registered).await;
                return Err(error);
            }
        }
        self.wake_registered_runtime(&registered)
            .await
            .map_err(|e| e.to_contract_error())?;
        self.persist_runtime_resurrection_descriptor(&registered, "headless", "acp", None)
            .await
            .map_err(|e| e.to_contract_error())?;
        return Ok(resp);
    }
}
