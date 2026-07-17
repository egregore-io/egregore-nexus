//! Admin commands (cli-spec §8): `route`, `spawn`, `remove`, `delete`, `channel`, `assign-role`,
//! `grant-tier`, `monitor`. Admin = extra commands only — never the message path. Mutations enter
//! through store-backed command intents, and the daemon worker still applies server-side
//! tier/project policy before executing the domain operation.

use std::process::ExitCode;

use clap::Subcommand;
use nexus_contracts::{
    AdminAssignRequest, AdminAssignResponse, AdminGroupAssignRequest, AdminGroupAssignResponse,
    AdminRenameRequest, AdminRenameResponse, AssignProjectRequest, AssignProjectResponse,
    AssignRoleRequest, AssignRoleResponse, ChannelOp, ChannelRequest, DlqListRequest,
    DlqListResponse, DlqMutationResponse, DlqPurgeRequest, DlqRequeueRequest, GrantTierRequest,
    GrantTierResponse, Harness, MessageId, MonitorRequest, RemoveRequest, RemoveResponse,
    RouteForwardRequest, SpawnRequest, SpawnResponse, Tier,
};

use crate::cli::commands::parse;
use crate::cli::render::{finish, finish_with};
use crate::cli::store_client::StoreClient;

/// `nexus admin <op>`.
#[derive(Subcommand, Debug)]
pub enum AdminCmd {
    /// Ad-hoc one-shot forward of a notification audit id to a name/thread.
    Route {
        /// Notification audit id returned by source push / notification ingest, e.g. n_...
        notif: String,
        #[arg(long)]
        to: String,
    },
    /// Admin-initiated agent spawn (the agent still self-registers).
    Spawn {
        #[arg(value_parser = parse::harness)]
        kind: Harness,
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        cwd: Option<String>,
        #[arg(long)]
        project: Option<String>,
        #[arg(long)]
        role: Option<String>,
    },
    /// Remove/detach a session (loop torn down; inbox retained). Pass `--kill` to also terminate
    /// the spawned harness OS process.
    Remove {
        name: String,
        /// Also send SIGKILL to the spawned harness process. Without this flag the agent is only
        /// evicted (detached) and its harness keeps running.
        #[arg(long)]
        kill: bool,
    },
    /// Delete a session and its daemon-owned identity/runtime rows.
    ///
    /// Unlike `remove`, delete is a purge path: history retained outside the daemon store is not
    /// part of this command surface, and protected targets still require local/root human admin.
    Delete { name: String },
    /// Rename or first-name any agent. Order: `<source> <new-name>` — source is a name,
    /// durable a_* agent id, or exact s_* session id; the second argument is the name to bind.
    Rename { source: String, target: String },
    /// A staged (unnamed) identity assumes a name whose previous owner died. `id` is a stable
    /// a_* agent id or exact s_* session id; the name's dead holder (if any) is evicted back to
    /// unnamed. Names held by live agents are never takeable.
    Assign {
        #[arg(value_name = "a_*|s_*")]
        id: String,
        name: String,
    },
    /// Manage topics / Pub-feed routing.
    Channel {
        #[arg(value_parser = parse::channel_op)]
        op: ChannelOp,
        topic: String,
        #[arg(long)]
        source: Option<String>,
    },
    /// Set an agent's registered label role (display/addressing only).
    AssignRole {
        /// Order: `<name> <role>` — the agent first, then the label role to set.
        name: String,
        role: String,
    },
    /// Assign the agent's active project.
    AssignProject {
        name: String,
        #[arg(long)]
        project: String,
    },
    /// Grant an agent a durable privilege tier. Order: `<name> <tier>`, tier is
    /// `agent|admin`. Only human/local admins may grant tiers.
    GrantTier {
        name: String,
        #[arg(value_parser = parse::tier, value_name = "agent|admin")]
        tier: Tier,
    },
    /// Manage Message Post policy groups.
    Group {
        #[command(subcommand)]
        cmd: AdminGroupCmd,
    },
    /// Show the web console event feed snapshot (`--follow` is not wired for store-backed monitor yet).
    Monitor {
        #[arg(long)]
        follow: bool,
        #[arg(long)]
        scope: Option<String>,
    },
    /// Inspect or repair dead-lettered Message Post deliveries.
    Dlq {
        #[command(subcommand)]
        cmd: AdminDlqCmd,
    },
}

/// `nexus admin group <op>`.
#[derive(Subcommand, Debug)]
pub enum AdminGroupCmd {
    /// Assign an agent to a Message Post policy group. Order: `<group> <name>` — the GROUP
    /// comes first, then the agent (note: inverted relative to `admin assign-role <name> <role>`).
    Assign {
        group: String,
        name: String,
        #[arg(long)]
        project: Option<String>,
    },
}

/// `nexus admin dlq <op>`.
#[derive(Subcommand, Debug)]
pub enum AdminDlqCmd {
    /// List terminal delivery failures.
    List {
        #[arg(long = "for")]
        for_target: Option<String>,
        #[arg(long)]
        since: Option<String>,
        #[arg(long)]
        limit: Option<u32>,
    },
    /// Move terminal delivery failures back to pending.
    Requeue {
        in_flight_id: Option<String>,
        #[arg(long = "for")]
        for_target: Option<String>,
        #[arg(long)]
        since: Option<String>,
    },
    /// Explicitly discard terminal delivery failures.
    Purge {
        in_flight_id: Option<String>,
        #[arg(long = "for")]
        for_target: Option<String>,
        #[arg(long)]
        since: Option<String>,
        #[arg(long)]
        yes: bool,
    },
}

/// Submit an admin op to the daemon command worker. Tier-gating is server-side: an agent-tier
/// caller is rejected with `UNAUTHORIZED`, which [`finish`]/[`finish_with`] render as a message + a
/// non-zero exit. The CLI never checks tier locally.
pub async fn run(client: &StoreClient, cmd: AdminCmd, json: bool) -> ExitCode {
    match cmd {
        AdminCmd::Route { notif, to } => {
            let res: Result<(), _> = client
                .command(
                    nexus_store::command_kinds::admin::ROUTE,
                    &RouteForwardRequest {
                        notif: MessageId(notif),
                        to,
                    },
                )
                .await;
            finish(res, json)
        }
        AdminCmd::Spawn {
            kind,
            name,
            cwd,
            project,
            role,
        } => {
            let cwd = super::lifecycle::resolve_launch_cwd_for(&kind, cwd, &[]);
            let res: Result<SpawnResponse, _> = client
                .command(
                    nexus_store::command_kinds::admin::SPAWN,
                    &SpawnRequest {
                        kind,
                        name,
                        identity_policy: None,
                        cwd,
                        project,
                        role,
                        initial_prompt: None,
                        resume: None,
                        harness_args: Vec::new(),
                        headless: false,
                        backend: None,
                    },
                )
                .await;
            finish_with(res, json, |r| format!("spawned session {}", r.session_id.0))
        }
        AdminCmd::Remove { name, kill } => {
            let res: Result<RemoveResponse, _> = client
                .command(
                    nexus_store::command_kinds::admin::REMOVE,
                    &RemoveRequest {
                        agent_id: None,
                        name,
                        kill,
                    },
                )
                .await;
            finish_with(res, json, |r| {
                format!("{}: {}", r.name.as_deref().unwrap_or("<unnamed>"), r.status)
            })
        }
        AdminCmd::Delete { name } => {
            let res: Result<RemoveResponse, _> = client
                .command(
                    nexus_store::command_kinds::admin::DELETE,
                    &RemoveRequest {
                        agent_id: None,
                        name,
                        kill: false,
                    },
                )
                .await;
            finish_with(res, json, |r| {
                format!("{}: {}", r.name.as_deref().unwrap_or("<unnamed>"), r.status)
            })
        }
        AdminCmd::Rename { source, target } => {
            let res: Result<AdminRenameResponse, _> = client
                .command(
                    nexus_store::command_kinds::admin::RENAME,
                    &AdminRenameRequest { source, target },
                )
                .await;
            finish_with(res, json, |r| {
                format!(
                    "{} -> {}",
                    r.previous.as_deref().unwrap_or("<unnamed>"),
                    r.name
                )
            })
        }
        AdminCmd::Assign { id, name } => {
            let res: Result<AdminAssignResponse, _> = client
                .command(
                    nexus_store::command_kinds::admin::ASSIGN,
                    &AdminAssignRequest { id, name },
                )
                .await;
            finish_with(res, json, |r| {
                let evicted = r
                    .evicted_agent_id
                    .as_ref()
                    .map(|id| format!(" (evicted {})", id.0))
                    .unwrap_or_default();
                format!("{} assigned {}{}", r.agent_id.0, r.name, evicted)
            })
        }
        AdminCmd::Channel { op, topic, source } => {
            let res: Result<(), _> = client
                .command(
                    nexus_store::command_kinds::admin::CHANNEL,
                    &ChannelRequest { op, topic, source },
                )
                .await;
            finish(res, json)
        }
        AdminCmd::AssignRole { name, role } => {
            let res: Result<AssignRoleResponse, _> = client
                .command(
                    nexus_store::command_kinds::admin::ASSIGN_ROLE,
                    &AssignRoleRequest {
                        agent_id: None,
                        name,
                        role,
                    },
                )
                .await;
            finish_with(res, json, |r| {
                format!("{} -> {}", r.name.as_deref().unwrap_or("<unnamed>"), r.role)
            })
        }
        AdminCmd::AssignProject { name, project } => {
            let res: Result<AssignProjectResponse, _> = client
                .command(
                    nexus_store::command_kinds::admin::ASSIGN_PROJECT,
                    &AssignProjectRequest {
                        agent_id: None,
                        name,
                        project,
                    },
                )
                .await;
            finish_with(res, json, |r| {
                format!(
                    "{} -> {}",
                    r.name.as_deref().unwrap_or("<unnamed>"),
                    r.project
                )
            })
        }
        AdminCmd::GrantTier { name, tier } => {
            let res: Result<GrantTierResponse, _> = client
                .command(
                    nexus_store::command_kinds::admin::GRANT_TIER,
                    &GrantTierRequest {
                        agent_id: None,
                        name,
                        tier,
                    },
                )
                .await;
            finish_with(res, json, |r| {
                format!(
                    "{} -> {}",
                    r.name.as_deref().unwrap_or("<unnamed>"),
                    tier_label(r.tier)
                )
            })
        }
        AdminCmd::Group {
            cmd:
                AdminGroupCmd::Assign {
                    group,
                    name,
                    project,
                },
        } => {
            let res: Result<AdminGroupAssignResponse, _> = client
                .command(
                    nexus_store::command_kinds::admin::GROUP_ASSIGN,
                    &AdminGroupAssignRequest {
                        project,
                        group,
                        agent_id: None,
                        name,
                    },
                )
                .await;
            finish_with(res, json, |r| {
                format!(
                    "{} -> {}:{}",
                    r.name.as_deref().unwrap_or("<unnamed>"),
                    r.project,
                    r.group
                )
            })
        }
        AdminCmd::Monitor { follow, scope } => {
            if follow {
                eprintln!(
                    "error: --follow is not wired for the store-backed admin monitor path yet"
                );
                return ExitCode::from(1);
            }
            let res: Result<(), _> = client
                .command(
                    nexus_store::command_kinds::admin::MONITOR,
                    &MonitorRequest { follow, scope },
                )
                .await;
            finish(res, json)
        }
        AdminCmd::Dlq { cmd } => run_dlq(client, cmd, json).await,
    }
}

async fn run_dlq(client: &StoreClient, cmd: AdminDlqCmd, json: bool) -> ExitCode {
    match cmd {
        AdminDlqCmd::List {
            for_target,
            since,
            limit,
        } => {
            let res: Result<DlqListResponse, _> = client
                .command(
                    nexus_store::command_kinds::admin::DLQ_LIST,
                    &DlqListRequest {
                        for_target,
                        since,
                        limit,
                    },
                )
                .await;
            finish_with(res, json, |r| {
                if r.total == 0 {
                    "dead-letters: none".to_string()
                } else {
                    let mut out = format!("dead-letters: {}", r.total);
                    for row in &r.rows {
                        out.push('\n');
                        let recipient = row
                            .recipient_name
                            .as_deref()
                            .or(row.recipient_session.as_ref().map(|s| s.0.as_str()))
                            .unwrap_or("-");
                        out.push_str(&format!(
                            "{} {} -> {} attempt={} code={} reason={} preview={}",
                            row.in_flight_id,
                            row.sender,
                            recipient,
                            row.attempt_count,
                            row.error_code.as_deref().unwrap_or("-"),
                            row.error_reason.as_deref().unwrap_or("-"),
                            row.body_preview
                        ));
                    }
                    out
                }
            })
        }
        AdminDlqCmd::Requeue {
            in_flight_id,
            for_target,
            since,
        } => {
            let res: Result<DlqMutationResponse, _> = client
                .command(
                    nexus_store::command_kinds::admin::DLQ_REQUEUE,
                    &DlqRequeueRequest {
                        in_flight_id,
                        for_target,
                        since,
                    },
                )
                .await;
            finish_with(res, json, |r| format!("requeued {}", r.count))
        }
        AdminDlqCmd::Purge {
            in_flight_id,
            for_target,
            since,
            yes,
        } => {
            if in_flight_id.is_none() && !yes {
                eprintln!("error: batch dlq purge requires --yes");
                return ExitCode::from(1);
            }
            let res: Result<DlqMutationResponse, _> = client
                .command(
                    nexus_store::command_kinds::admin::DLQ_PURGE,
                    &DlqPurgeRequest {
                        in_flight_id,
                        for_target,
                        since,
                        yes,
                    },
                )
                .await;
            finish_with(res, json, |r| format!("purged {}", r.count))
        }
    }
}

fn tier_label(tier: Tier) -> &'static str {
    match tier {
        Tier::Agent => "agent",
        Tier::Admin => "admin",
    }
}
