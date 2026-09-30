//! Presence + subscription commands: `status`, `subscribe`, `unsubscribe`.
//!
//! These are daemon-managed command intents: the daemon still applies pause/wake state and topic
//! cursor rules, but the CLI no longer needs a daemon ingress transport.

use std::process::ExitCode;

use clap::Args;
use nexus_contracts::{
    StatusRequest, StatusResponse, StatusState, SubscribeRequest, SubscribeResponse,
    UnsubscribeRequest,
};

use crate::cli::commands::parse;
use crate::cli::render::{finish, finish_with};
use crate::cli::store_client::StoreClient;

/// `nexus status [<state>] [--work <text>]` — no state = show current status.
#[derive(Args, Debug)]
pub struct StatusArgs {
    /// `active`|`busy`|`paused`. Omit to show current status.
    #[arg(value_parser = parse::status_state)]
    pub state: Option<StatusState>,
    #[arg(long)]
    pub work: Option<String>,
}
/// Set self-status / pause / work, or (no state) show the current status.
pub async fn status(client: &StoreClient, a: StatusArgs, json: bool) -> ExitCode {
    let req = StatusRequest {
        state: a.state,
        work: a.work,
    };
    let res: Result<StatusResponse, _> = client
        .command(nexus_store::command_kinds::presence::STATUS, &req)
        .await;
    finish_with(res, json, |s| {
        format!(
            "presence={:?}  paused={}  work={}",
            s.presence,
            s.paused,
            s.current_work.as_deref().unwrap_or("-")
        )
    })
}

/// `nexus subscribe <topic> [--group <g>]`.
#[derive(Args, Debug)]
pub struct SubscribeArgs {
    pub topic: String,
    #[arg(long)]
    pub group: Option<String>,
}
/// Subscribe the caller to a topic (returns the resume cursor).
pub async fn subscribe(client: &StoreClient, a: SubscribeArgs, json: bool) -> ExitCode {
    let res: Result<SubscribeResponse, _> = client
        .command(
            nexus_store::command_kinds::topic::SUBSCRIBE,
            &SubscribeRequest {
                topic: a.topic,
                group: a.group,
            },
        )
        .await;
    finish_with(res, json, |r| format!("{} @ cursor {}", r.topic, r.cursor))
}

/// `nexus unsubscribe <topic>`.
#[derive(Args, Debug)]
pub struct UnsubscribeArgs {
    pub topic: String,
}
/// Unsubscribe the caller from a topic.
pub async fn unsubscribe(client: &StoreClient, a: UnsubscribeArgs, json: bool) -> ExitCode {
    let res: Result<(), _> = client
        .command(
            nexus_store::command_kinds::topic::UNSUBSCRIBE,
            &UnsubscribeRequest { topic: a.topic },
        )
        .await;
    finish(res, json)
}
