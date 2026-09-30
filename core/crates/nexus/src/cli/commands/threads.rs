//! Thread commands: `thread new|join|leave|rename|archive|delete|members`.
//!
//! Mutations are daemon-managed command intents; `thread members` remains a read operation and will
//! move to a store read view with the rest of the read-only surface.

use std::process::ExitCode;

use clap::Subcommand;
use nexus_contracts::{
    ArchiveThreadRequest, CreateThreadRequest, DeleteThreadRequest, JoinThreadRequest,
    LeaveThreadRequest, RenameThreadRequest, ThreadMembersResponse,
};

use crate::cli::read_client::ReadClient;
use crate::cli::render::{finish, finish_with};
use crate::cli::store_client::StoreClient;

/// `nexus thread <op>`.
#[derive(Subcommand, Debug)]
pub enum ThreadCmd {
    /// Create a named thread with optional initial members.
    New {
        name: String,
        #[arg(long)]
        member: Vec<String>,
        #[arg(long)]
        project: Option<String>,
    },
    /// Join a thread (caller).
    Join { name: String },
    /// Leave a thread (caller).
    Leave { name: String },
    /// Rename a thread. Order: `<name> <new-name>`. Admin-tier callers only; daemon policy
    /// enforces the gate.
    Rename { name: String, new_name: String },
    /// Archive a thread so it no longer receives posts or appears in active lists.
    Archive { name: String },
    /// Delete a thread registry and memberships. Old message rows remain in the store but are no
    /// longer reachable through scoped thread history.
    Delete { name: String },
    /// List a thread's members.
    Members { name: String },
}

/// Dispatch a mutating `thread` op through the store-backed command-intent ingress. Read-only
/// `thread members` uses the store read view.
pub async fn run_store(
    store: &StoreClient,
    read: &ReadClient,
    cmd: ThreadCmd,
    json: bool,
) -> ExitCode {
    match cmd {
        ThreadCmd::New { name, member, .. } => {
            let res: Result<(), _> = store
                .command(
                    nexus_store::command_kinds::thread::CREATE,
                    &CreateThreadRequest {
                        name,
                        members: member,
                    },
                )
                .await;
            finish(res, json)
        }
        ThreadCmd::Join { name } => {
            let res: Result<(), _> = store
                .command(
                    nexus_store::command_kinds::thread::JOIN,
                    &JoinThreadRequest { name },
                )
                .await;
            finish(res, json)
        }
        ThreadCmd::Leave { name } => {
            let res: Result<(), _> = store
                .command(
                    nexus_store::command_kinds::thread::LEAVE,
                    &LeaveThreadRequest { name },
                )
                .await;
            finish(res, json)
        }
        ThreadCmd::Rename { name, new_name } => {
            let res: Result<(), _> = store
                .command(
                    nexus_store::command_kinds::thread::RENAME,
                    &RenameThreadRequest { name, new_name },
                )
                .await;
            finish(res, json)
        }
        ThreadCmd::Archive { name } => {
            let res: Result<(), _> = store
                .command(
                    nexus_store::command_kinds::thread::ARCHIVE,
                    &ArchiveThreadRequest { name },
                )
                .await;
            finish(res, json)
        }
        ThreadCmd::Delete { name } => {
            let res: Result<(), _> = store
                .command(
                    nexus_store::command_kinds::thread::DELETE,
                    &DeleteThreadRequest { name },
                )
                .await;
            finish(res, json)
        }
        ThreadCmd::Members { name } => {
            let res: Result<ThreadMembersResponse, _> = read.thread_members(&name).await;
            finish_with(res, json, |m| {
                format!("{}: {}", m.name, m.members.join(", "))
            })
        }
    }
}
