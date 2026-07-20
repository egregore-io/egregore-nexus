//! Discover commands: `members`, `threads`, `topics` (cli-spec §3).
//!
//! These are store-backed read views: the daemon still owns writes and wake side effects, but the
//! CLI does not need a daemon ingress transport to render the roster and channel directory.

use std::process::ExitCode;

use clap::Args;
use nexus_contracts::{
    MemberListRequest, MemberListResponse, ThreadListResponse, TopicListResponse,
};

use crate::cli::read_client::ReadClient;
use crate::cli::render::finish_with;

/// `nexus members` — directory + presence + current_work.
#[derive(Args, Debug)]
pub struct MembersArgs {
    /// Filter by project metadata. Omit to list the global directory.
    #[arg(long)]
    pub project: Option<String>,
    /// Include offline members (maps to `MemberListRequest.include_offline`).
    #[arg(long = "include-offline")]
    pub include_offline: bool,
    /// Include dead-marked members (audit view; maps to `MemberListRequest.include_dead`).
    #[arg(long = "include-dead")]
    pub include_dead: bool,
    /// Show the presence column in the human render (display-only; does NOT change the request).
    #[arg(long)]
    pub presence: bool,
}

#[doc(hidden)]
pub fn member_list_request(a: &MembersArgs) -> MemberListRequest {
    MemberListRequest {
        project: a.project.clone(),
        include_offline: Some(a.include_offline),
        include_dead: Some(a.include_dead),
    }
}

/// `nexus members` — project/offline/dead filters cross the read contract; `--presence` is a
/// render-only column toggle.
pub async fn members(client: &ReadClient, a: MembersArgs, json: bool) -> ExitCode {
    let req = member_list_request(&a);
    let show_presence = a.presence;
    let res: Result<MemberListResponse, _> = client.members(req).await;
    finish_with(res, json, move |l| {
        let mut s = if show_presence {
            String::from("NAME       PRESENCE  WORK")
        } else {
            String::from("NAME       WORK")
        };
        for m in &l.members {
            if show_presence {
                s.push_str(&format!(
                    "\n{:<10} {:<9} {}",
                    m.name.as_deref().unwrap_or("<unnamed>"),
                    format!("{:?}", m.presence),
                    m.current_work.as_deref().unwrap_or("-")
                ));
            } else {
                s.push_str(&format!(
                    "\n{:<10} {}",
                    m.name.as_deref().unwrap_or("<unnamed>"),
                    m.current_work.as_deref().unwrap_or("-")
                ));
            }
        }
        s
    })
}

/// `nexus threads` — names + members; `--mine` = threads the caller belongs to.
#[derive(Args, Debug)]
pub struct ThreadsArgs {
    #[arg(long)]
    pub project: Option<String>,
    #[arg(long)]
    pub mine: bool,
}

/// List threads. The daemon's `threads` method already scopes to the caller; `--mine`/`--project`
/// are reserved client-side filters (the wire DTO is unit) and do not alter the request today.
pub async fn threads(client: &ReadClient, _a: ThreadsArgs, json: bool) -> ExitCode {
    let res: Result<ThreadListResponse, _> = client.threads().await;
    finish_with(res, json, |l| {
        if l.threads.is_empty() {
            return "(no threads)".to_string();
        }
        l.threads
            .iter()
            .map(|t| format!("{}  [{}]", t.name, t.members.join(", ")))
            .collect::<Vec<_>>()
            .join("\n")
    })
}

/// `nexus topics` — topics incl. the Pub feed.
#[derive(Args, Debug)]
pub struct TopicsArgs {
    #[arg(long)]
    pub subscribed: bool,
}

/// List topics with subscriber counts.
pub async fn topics(client: &ReadClient, _a: TopicsArgs, json: bool) -> ExitCode {
    let res: Result<TopicListResponse, _> = client.topics().await;
    finish_with(res, json, |l| {
        if l.topics.is_empty() {
            return "(no topics)".to_string();
        }
        l.topics
            .iter()
            .map(|t| format!("{}  ({} subs)", t.topic, t.subscribers))
            .collect::<Vec<_>>()
            .join("\n")
    })
}
