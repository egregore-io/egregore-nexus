//! Agents commands: durable identity and runtime operator surface.
//!
//! `nexus agents list` keeps the existing roster behavior. Reads use store read views; mutations
//! use daemon-managed command intents backed by the stable agent repos.

use std::process::ExitCode;

use clap::Subcommand;
use nexus_contracts::{
    AgentAccessGrantRequest, AgentAccessGrantResponse, AgentAccessRevokeRequest,
    AgentAccessRevokeResponse, AgentAccessRole, AgentCreateRequest, AgentCreateResponse,
    AgentCredentialCreateRequest, AgentCredentialCreateResponse, AgentCredentialRevokeRequest,
    AgentCredentialRevokeResponse, AgentOwnerTransferRequest, AgentOwnerTransferResponse,
    AgentRuntimeListResponse, AgentShowResponse, Harness, MemberListRequest, MemberListResponse,
};

use crate::cli::commands::parse;
use crate::cli::read_client::ReadClient;
use crate::cli::render::finish_with;
use crate::cli::store_client::StoreClient;

/// `nexus agents <op>`.
#[derive(Subcommand, Debug)]
pub enum AgentsCmd {
    /// List agents in the caller's project (rows where the member has a harness).
    List,
    /// Create a durable agent identity.
    Create {
        name: String,
        #[arg(long, value_parser = parse::harness)]
        harness: Option<Harness>,
        #[arg(long)]
        project: Option<String>,
        #[arg(long)]
        role: Option<String>,
    },
    /// Show one durable agent identity.
    Show { name: String },
    /// Grant delegated `/agent` session access. Order: `<agent-name> <principal> <role>`,
    /// where role is `viewer|co_owner`.
    GrantAccess {
        name: String,
        principal: String,
        #[arg(value_parser = parse::agent_access_role, value_name = "viewer|co_owner")]
        role: AgentAccessRole,
        #[arg(long)]
        project: Option<String>,
    },
    /// Revoke delegated `/agent` session access. Order: `<agent-name> <principal>`.
    RevokeAccess {
        name: String,
        principal: String,
        #[arg(long)]
        project: Option<String>,
    },
    /// Transfer durable managed-agent ownership. Order: `<agent-name> <new-owner>`.
    TransferOwner {
        name: String,
        owner: String,
        #[arg(long)]
        project: Option<String>,
    },
    /// Manage runtime credentials.
    #[command(subcommand)]
    Credentials(AgentCredentialsCmd),
    /// List runtimes for one durable agent identity.
    Runtimes {
        name: String,
        #[arg(long)]
        include_stopped: bool,
    },
}

/// `nexus agents credentials <op>`.
#[derive(Subcommand, Debug)]
pub enum AgentCredentialsCmd {
    /// Create a runtime credential and print the plaintext secret once.
    Create {
        name: String,
        #[arg(long)]
        purpose: Option<String>,
        #[arg(long)]
        label: Option<String>,
        #[arg(long = "scope")]
        scopes: Vec<String>,
    },
    /// Revoke a runtime credential.
    Revoke { credential_id: String },
}

/// Dispatch an agents sub-command.
pub async fn run(store: &StoreClient, read: &ReadClient, cmd: AgentsCmd, json: bool) -> ExitCode {
    match cmd {
        AgentsCmd::List => list(read, json).await,
        AgentsCmd::Create {
            name,
            harness,
            project,
            role,
        } => create(store, name, harness, project, role, json).await,
        AgentsCmd::Show { name } => show(read, name, json).await,
        AgentsCmd::GrantAccess {
            name,
            principal,
            role,
            project,
        } => grant_access(store, name, principal, role, project, json).await,
        AgentsCmd::RevokeAccess {
            name,
            principal,
            project,
        } => revoke_access(store, name, principal, project, json).await,
        AgentsCmd::TransferOwner {
            name,
            owner,
            project,
        } => transfer_owner(store, name, owner, project, json).await,
        AgentsCmd::Credentials(cmd) => credentials(store, cmd, json).await,
        AgentsCmd::Runtimes {
            name,
            include_stopped,
        } => runtimes(read, name, include_stopped, json).await,
    }
}

async fn grant_access(
    client: &StoreClient,
    name: String,
    principal: String,
    role: AgentAccessRole,
    project: Option<String>,
    json: bool,
) -> ExitCode {
    let res: Result<AgentAccessGrantResponse, _> = client
        .command(
            nexus_store::command_kinds::agent::GRANT_ACCESS,
            &AgentAccessGrantRequest {
                agent_id: None,
                name,
                principal_agent_id: None,
                principal,
                project,
                role,
            },
        )
        .await;
    finish_with(res, json, |r| {
        format!(
            "{} -> {}:{} {}",
            r.name.as_deref().unwrap_or("<unnamed>"),
            r.project,
            r.principal,
            agent_access_role_label(r.role)
        )
    })
}

async fn revoke_access(
    client: &StoreClient,
    name: String,
    principal: String,
    project: Option<String>,
    json: bool,
) -> ExitCode {
    let res: Result<AgentAccessRevokeResponse, _> = client
        .command(
            nexus_store::command_kinds::agent::REVOKE_ACCESS,
            &AgentAccessRevokeRequest {
                agent_id: None,
                name,
                principal_agent_id: None,
                principal,
                project,
            },
        )
        .await;
    finish_with(res, json, |r| {
        format!(
            "{} -> {}:{} {}",
            r.name.as_deref().unwrap_or("<unnamed>"),
            r.project,
            r.principal,
            if r.revoked { "revoked" } else { "not_found" }
        )
    })
}

async fn transfer_owner(
    client: &StoreClient,
    name: String,
    owner: String,
    project: Option<String>,
    json: bool,
) -> ExitCode {
    let res: Result<AgentOwnerTransferResponse, _> = client
        .command(
            nexus_store::command_kinds::agent::TRANSFER_OWNER,
            &AgentOwnerTransferRequest {
                agent_id: None,
                name,
                owner_agent_id: None,
                owner,
                project,
            },
        )
        .await;
    finish_with(res, json, |r| {
        format!(
            "{} owner {}:{}",
            r.name.as_deref().unwrap_or("<unnamed>"),
            r.project,
            r.owner
        )
    })
}

async fn list(client: &ReadClient, json: bool) -> ExitCode {
    let req = MemberListRequest {
        include_offline: Some(true),
        include_dead: None,
    };
    let res: Result<MemberListResponse, _> = client.members(req).await;
    finish_with(res, json, |l| {
        let agents: Vec<_> = l.members.iter().filter(|m| m.agent.is_some()).collect();
        if agents.is_empty() {
            return "(no agents)".to_string();
        }
        let mut s = String::from("NAME       HARNESS   PRESENCE  WORK");
        for m in agents {
            s.push_str(&format!(
                "\n{:<10} {:<9} {:<9} {}",
                m.name.as_deref().unwrap_or("<unnamed>"),
                m.agent.as_deref().unwrap_or("-"),
                format!("{:?}", m.presence),
                m.current_work.as_deref().unwrap_or("-")
            ));
        }
        s
    })
}

async fn create(
    client: &StoreClient,
    name: String,
    default_harness: Option<Harness>,
    project: Option<String>,
    role: Option<String>,
    json: bool,
) -> ExitCode {
    let res: Result<AgentCreateResponse, _> = client
        .command(
            nexus_store::command_kinds::agent::CREATE,
            &AgentCreateRequest {
                name,
                default_harness,
                project,
                role,
            },
        )
        .await;
    finish_with(res, json, |r| format_agent_summary(&r.agent))
}

async fn show(client: &ReadClient, name: String, json: bool) -> ExitCode {
    let res: Result<AgentShowResponse, _> = client.agent_show(&name).await;
    finish_with(res, json, |r| {
        let mut out = format_agent_summary(&r.agent);
        if !r.runtimes.is_empty() {
            out.push_str("\nRUNTIMES");
            for runtime in &r.runtimes {
                out.push_str(&format!(
                    "\n{} {} {:?} {}",
                    runtime.runtime_id.0,
                    format!("{:?}", runtime.harness).to_lowercase(),
                    runtime.presence,
                    if runtime.active { "active" } else { "stopped" }
                ));
            }
        }
        out
    })
}

async fn credentials(client: &StoreClient, cmd: AgentCredentialsCmd, json: bool) -> ExitCode {
    match cmd {
        AgentCredentialsCmd::Create {
            name,
            purpose,
            label,
            scopes,
        } => {
            let scopes = if scopes.is_empty() {
                vec!["runtime:register".to_string()]
            } else {
                scopes
            };
            let res: Result<AgentCredentialCreateResponse, _> = client
                .command(
                    nexus_store::command_kinds::agent::credential::CREATE,
                    &AgentCredentialCreateRequest {
                        agent_id: None,
                        name: Some(name),
                        label,
                        purpose,
                        scopes,
                    },
                )
                .await;
            finish_with(res, json, |r| {
                format!(
                    "{} credential {} secret {}",
                    r.agent_id.0, r.credential_id.0, r.secret
                )
            })
        }
        AgentCredentialsCmd::Revoke { credential_id } => {
            let res: Result<AgentCredentialRevokeResponse, _> = client
                .command(
                    nexus_store::command_kinds::agent::credential::REVOKE,
                    &AgentCredentialRevokeRequest {
                        credential_id: credential_id.into(),
                    },
                )
                .await;
            finish_with(res, json, |r| {
                format!(
                    "{} {}",
                    r.credential_id.0,
                    if r.revoked { "revoked" } else { "not revoked" }
                )
            })
        }
    }
}

async fn runtimes(
    client: &ReadClient,
    name: String,
    include_stopped: bool,
    json: bool,
) -> ExitCode {
    let res: Result<AgentRuntimeListResponse, _> =
        client.agent_runtimes(&name, include_stopped).await;
    finish_with(res, json, |r| {
        if r.runtimes.is_empty() {
            return "(no runtimes)".to_string();
        }
        let mut out = String::from("RUNTIME     HARNESS   PRESENCE  STATE");
        for runtime in &r.runtimes {
            out.push_str(&format!(
                "\n{:<11} {:<9} {:<9} {}",
                runtime.runtime_id.0,
                format!("{:?}", runtime.harness).to_lowercase(),
                format!("{:?}", runtime.presence).to_lowercase(),
                if runtime.active { "active" } else { "stopped" }
            ));
        }
        out
    })
}

fn format_agent_summary(agent: &nexus_contracts::AgentSummary) -> String {
    format!(
        "{} {} {:?}",
        agent.agent_id.0,
        agent.name.as_deref().unwrap_or("<unnamed>"),
        agent.default_harness.unwrap_or(Harness::Other)
    )
}

fn agent_access_role_label(role: AgentAccessRole) -> &'static str {
    match role {
        AgentAccessRole::Viewer => "viewer",
        AgentAccessRole::CoOwner => "co_owner",
    }
}
