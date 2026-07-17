//! `nexus resume <name|session-id>` — revive a headed harness without taking over its terminal.

use std::process::ExitCode;

use clap::Args;
use nexus_contracts::{ContractError, SessionId, SpawnResponse};
use nexus_store::command_kinds;
use serde::Serialize;

use crate::cli::read_client::ReadClient;
use crate::cli::render;
use crate::cli::store_client::StoreClient;

/// `nexus resume <name|session-id>` — bring a headed runtime back without attaching.
#[derive(Args, Debug)]
pub struct ResumeArgs {
    /// Agent name or Nexus session id for the headed runtime to revive.
    pub target: String,
}

/// Result printed after a successful detached revive.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResumeResponse {
    pub name: String,
    pub session_id: SessionId,
    pub attach_command: String,
}

/// Revive the target runtime through the stored native resume key and leave it detached.
pub async fn resume(store: &StoreClient, read: &ReadClient, a: ResumeArgs, json: bool) -> ExitCode {
    render::finish_with(
        resume_target(store, read, &a.target).await,
        json,
        render_resume,
    )
}

async fn resume_target(
    store: &StoreClient,
    read: &ReadClient,
    target: &str,
) -> Result<ResumeResponse, ContractError> {
    let plan = read.attach_revive_plan(target).await?;
    let resp: SpawnResponse = store
        .command(command_kinds::harness::LAUNCH, &plan.spawn)
        .await?;
    Ok(ResumeResponse {
        name: plan.name,
        attach_command: format!("nexus attach {}", resp.session_id.0),
        session_id: resp.session_id,
    })
}

fn render_resume(resp: &ResumeResponse) -> String {
    format!(
        "resumed {} as {}; attach with: {}",
        resp.name, resp.session_id.0, resp.attach_command
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resume_response_prints_session_and_attach_command() {
        let resp = ResumeResponse {
            name: "hugo".to_string(),
            session_id: SessionId("s_resumed_hugo".to_string()),
            attach_command: "nexus attach s_resumed_hugo".to_string(),
        };

        assert_eq!(
            render_resume(&resp),
            "resumed hugo as s_resumed_hugo; attach with: nexus attach s_resumed_hugo"
        );
    }
}
