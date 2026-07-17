//! Memory commands: `search`, `history`, and `read` (cli-spec §7).
//!
//! Scope is enforced by the store-backed read client using the same `nexus-search` scoped query
//! logic as the daemon. These reads do not need a daemon ingress transport.

use std::process::ExitCode;

use clap::Args;
use nexus_contracts::{
    HistoryRequest, HistoryResponse, Message, SearchMode, SearchRequest, SearchResponse,
};

use crate::cli::read_client::ReadClient;
use crate::cli::render::finish_with;

/// `nexus search <query> [--semantic|--fts|--hybrid] [--limit N] [--thread T] [--with <name>]`.
#[derive(Args, Debug)]
pub struct SearchArgs {
    pub query: String,
    #[arg(long, conflicts_with_all = ["fts", "hybrid"])]
    pub semantic: bool,
    #[arg(long, conflicts_with_all = ["semantic", "hybrid"])]
    pub fts: bool,
    #[arg(long, conflicts_with_all = ["semantic", "fts"])]
    pub hybrid: bool,
    #[arg(long)]
    pub limit: Option<u32>,
    #[arg(long)]
    pub thread: Option<String>,
    #[arg(long)]
    pub with: Option<String>,
}
/// Search the caller's scope. Engine: `--semantic`/`--fts`/`--hybrid` (hybrid is the default).
pub async fn search(client: &ReadClient, a: SearchArgs, json: bool) -> ExitCode {
    let mode = if a.semantic {
        SearchMode::Semantic
    } else if a.fts {
        SearchMode::Fts
    } else {
        SearchMode::Hybrid
    };
    let req = SearchRequest {
        query: a.query,
        mode,
        limit: a.limit,
        thread: a.thread,
        with: a.with,
        since: None,
    };
    let res: Result<SearchResponse, _> = client.search(req).await;
    finish_with(res, json, |r| {
        if r.hits.is_empty() {
            return "(no hits)".to_string();
        }
        r.hits
            .iter()
            .map(|h| format!("[{:.2}] {}  {}", h.score, h.from, h.snippet))
            .collect::<Vec<_>>()
            .join("\n")
    })
}

/// `nexus history [--thread T] [--with <name>] [--topic T] [--limit N] [--before TS]`.
#[derive(Args, Debug)]
pub struct HistoryArgs {
    #[arg(long)]
    pub thread: Option<String>,
    #[arg(long)]
    pub with: Option<String>,
    #[arg(long)]
    pub topic: Option<String>,
    #[arg(long)]
    pub limit: Option<u32>,
    #[arg(long)]
    pub before: Option<i64>,
}
/// Chronological recall of a conversation (DM partner / thread / topic).
pub async fn history(client: &ReadClient, a: HistoryArgs, json: bool) -> ExitCode {
    let req = HistoryRequest {
        thread: a.thread,
        with: a.with,
        topic: a.topic,
        limit: a.limit,
        before: a.before,
    };
    let res: Result<HistoryResponse, _> = client.history(req).await;
    finish_with(res, json, |r| {
        if r.entries.is_empty() {
            return "(no history)".to_string();
        }
        r.entries
            .iter()
            .map(|e| format!("{} @ {}: {}", e.from, e.when, e.body))
            .collect::<Vec<_>>()
            .join("\n")
    })
}

/// `nexus read <message-id>`.
#[derive(Args, Debug)]
pub struct ReadArgs {
    /// Message id to expand, for example the id printed in a truncated drain hint.
    pub message_id: String,
}

/// Fetch one committed message by id. Human output is the full body; `--json` returns the message.
pub async fn read(client: &ReadClient, a: ReadArgs, json: bool) -> ExitCode {
    let res: Result<Message, _> = client.message(&a.message_id).await;
    finish_with(res, json, |m| m.body.clone())
}
