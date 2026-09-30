//! The `nexus` CLI: the clap subcommand tree, global flags, and the `run` entrypoint. Local
//! write/control commands use [`store_client::StoreClient`] to enqueue daemon-executed command
//! intents, reads use [`read_client::ReadClient`], and terminal attach resolves local headed
//! runtime descriptors from store state. Identity is implicit — there is no `--from` and no ids.

pub mod ambient;
pub mod commands;
mod gateway_read_client;
pub mod read_client;
pub mod render;
pub mod store_client;

use std::process::ExitCode;

use clap::{Args, Parser, Subcommand};
use nexus_contracts::{Ack, ContractError};

use read_client::ReadClient;
use store_client::StoreClient;

const LONG_VERSION: &str = env!("CARGO_PKG_VERSION");

/// The `nexus` CLI root.
#[derive(Parser, Debug)]
#[command(
    name = "nexus",
    version,
    long_version = LONG_VERSION,
    about = "Nexus — the agent's deliberate outbound bus"
)]
pub struct Cli {
    #[command(flatten)]
    pub global: GlobalFlags,
    #[command(subcommand)]
    pub command: Command,
}

/// Flags available on every command.
#[derive(Args, Debug, Clone)]
pub struct GlobalFlags {
    /// Emit machine-readable JSON instead of a human table.
    #[arg(long, global = true)]
    pub json: bool,
    /// Suppress non-essential output.
    #[arg(short, long, global = true)]
    pub quiet: bool,
}

/// The full command surface. The `daemon` subcommand is owned by `main.rs` and is intentionally absent here.
#[derive(Subcommand, Debug)]
pub enum Command {
    // lifecycle (§2)
    Launch(commands::lifecycle::LaunchArgs),
    /// Internal: bind an ALREADY-RUNNING harness process to the bus. Registration alone creates
    /// no inbound delivery path (no adapter) — humans use `nexus launch`. Kept only for the
    /// daemon-installed `.nexus/bootstrap-register.sh` SessionStart hook, hence hidden.
    #[command(hide = true)]
    Register(commands::lifecycle::RegisterArgs),
    Whoami,
    /// Rename yourself on the bus (`nexus rename <new>`).
    Rename(commands::lifecycle::RenameArgs),
    // interactive attach
    /// Bring a headed runtime back using its stored native resume id, without attaching.
    Resume(commands::resume::ResumeArgs),
    Attach(commands::attach::AttachArgs),
    /// Attach the terminal (raw mode) to a daemon-owned PTY session (`nexus pty-attach <session>`),
    /// the operator-facing twin of `attach`, for the native harness TUI.
    PtyAttach(commands::pty_attach::PtyAttachArgs),
    /// Hidden plumbing: raw-mode socket terminal for a raw daemon-owned PTY runtime.
    #[command(hide = true, name = "terminal-client")]
    TerminalClient(commands::terminal_client::TerminalClientArgs),
    // discover (§3)
    Members(commands::discover::MembersArgs),
    Threads(commands::discover::ThreadsArgs),
    Topics(commands::discover::TopicsArgs),
    // send (§4)
    Dm(commands::send::DmArgs),
    Post(commands::send::PostArgs),
    Reply(commands::send::ReplyArgs),
    Publish(commands::send::PublishArgs),
    Send(commands::send::SendArgs),
    /// Deliver one notification to an agent id/name, group, or thread.
    Notify(commands::notify::NotifyArgs),
    // threads (§5)
    #[command(subcommand)]
    Thread(commands::threads::ThreadCmd),
    // notification sources (§notification-sources)
    #[command(subcommand)]
    Source(commands::source::SourceCmd),
    /// Push an event from a registered source (`nexus push <source> [-m body | --json | <stdin>]`).
    Push(commands::source::PushArgs),
    // presence (§6)
    Status(commands::presence::StatusArgs),
    Subscribe(commands::presence::SubscribeArgs),
    Unsubscribe(commands::presence::UnsubscribeArgs),
    // memory (§7)
    Search(commands::memory::SearchArgs),
    History(commands::memory::HistoryArgs),
    /// Read one full message body by id (`nexus read <message-id>`).
    Read(commands::memory::ReadArgs),
    // admin (§8)
    #[command(subcommand)]
    Admin(commands::admin::AdminCmd),
    // agents roster (§3-ext)
    #[command(subcommand)]
    Agents(commands::agents::AgentsCmd),
    /// Manage the separately installed Nexus REST/WebSocket gateway.
    #[command(subcommand)]
    Gateway(commands::gateway::GatewayCmd),
    /// Manage the on-demand Nexus browser console.
    #[command(subcommand)]
    Webconsole(commands::webconsole::WebconsoleCmd),
    /// Update the facets owned by this Nexus installation.
    Update(commands::update::UpdateArgs),
    // reach-the-daemon: the agent's drain loop (§9)
    Listen(commands::listen::ListenArgs),
    // MCP stdio server: exposes bus tools to agents (§10)
    Mcp(commands::mcp::McpArgs),
}

/// Parse → build DTO → dispatch through the store/read clients → render. Returns the process exit
/// code.
pub async fn run(cli: Cli) -> ExitCode {
    let json = cli.global.json;
    match cli.command {
        Command::Launch(a) => {
            let store = match resolve_store_client().await {
                Ok(store) => store,
                Err(e) => return render::finish::<serde_json::Value>(Err(e), json),
            };
            let read = match resolve_read_client().await {
                Ok(read) => read,
                Err(e) => return render::finish::<serde_json::Value>(Err(e), json),
            };
            commands::lifecycle::launch(&store, &read, a, json).await
        }
        Command::Register(a) => match resolve_store_client().await {
            Ok(store) => commands::lifecycle::register(&store, a, json).await,
            Err(e) => render::finish::<serde_json::Value>(Err(e), json),
        },
        Command::Whoami => match resolve_read_client().await {
            Ok(read) => commands::lifecycle::whoami(&read, json).await,
            Err(e) => render::finish::<serde_json::Value>(Err(e), json),
        },
        Command::Rename(a) => match resolve_store_client().await {
            Ok(store) => commands::lifecycle::rename(&store, a, json).await,
            Err(e) => render::finish::<serde_json::Value>(Err(e), json),
        },
        Command::Attach(a) => {
            let store = match resolve_store_client().await {
                Ok(store) => store,
                Err(e) => return render::finish::<serde_json::Value>(Err(e), json),
            };
            let read = match resolve_read_client().await {
                Ok(read) => read,
                Err(e) => return render::finish::<serde_json::Value>(Err(e), json),
            };
            commands::attach::attach(&store, &read, a, json).await
        }
        Command::Resume(a) => {
            let store = match resolve_store_client().await {
                Ok(store) => store,
                Err(e) => return render::finish::<serde_json::Value>(Err(e), json),
            };
            let read = match resolve_read_client().await {
                Ok(read) => read,
                Err(e) => return render::finish::<serde_json::Value>(Err(e), json),
            };
            commands::resume::resume(&store, &read, a, json).await
        }
        Command::TerminalClient(a) => commands::terminal_client::terminal_client(a).await,
        Command::PtyAttach(a) => {
            let store = match resolve_store_client().await {
                Ok(store) => store,
                Err(e) => return render::finish::<serde_json::Value>(Err(e), json),
            };
            let read = match resolve_read_client().await {
                Ok(read) => read,
                Err(e) => return render::finish::<serde_json::Value>(Err(e), json),
            };
            commands::pty_attach::pty_attach(&store, &read, a).await
        }
        Command::Members(a) => match resolve_read_client().await {
            Ok(read) => commands::discover::members(&read, a, json).await,
            Err(e) => render::finish::<serde_json::Value>(Err(e), json),
        },
        Command::Threads(a) => match resolve_read_client().await {
            Ok(read) => commands::discover::threads(&read, a, json).await,
            Err(e) => render::finish::<serde_json::Value>(Err(e), json),
        },
        Command::Topics(a) => match resolve_read_client().await {
            Ok(read) => commands::discover::topics(&read, a, json).await,
            Err(e) => render::finish::<serde_json::Value>(Err(e), json),
        },
        Command::Dm(a) => match resolve_store_client().await {
            Ok(store) => commands::send::dm(&store, a, json).await,
            Err(e) => render::finish::<Ack>(Err(e), json),
        },
        Command::Post(a) => match resolve_store_client().await {
            Ok(store) => commands::send::post(&store, a, json).await,
            Err(e) => render::finish::<Ack>(Err(e), json),
        },
        Command::Reply(a) => match resolve_store_client().await {
            Ok(store) => commands::send::reply(&store, a, json).await,
            Err(e) => render::finish::<Ack>(Err(e), json),
        },
        Command::Publish(a) => match resolve_store_client().await {
            Ok(store) => commands::send::publish(&store, a, json).await,
            Err(e) => render::finish::<Ack>(Err(e), json),
        },
        Command::Send(a) => match resolve_store_client().await {
            Ok(store) => commands::send::send(&store, a, json).await,
            Err(e) => render::finish::<Ack>(Err(e), json),
        },
        Command::Notify(a) => match resolve_store_client().await {
            Ok(store) => commands::notify::notify(&store, a, json).await,
            Err(e) => render::finish::<Ack>(Err(e), json),
        },
        Command::Thread(c) => {
            let store = match resolve_store_client().await {
                Ok(store) => store,
                Err(e) => return render::finish::<serde_json::Value>(Err(e), json),
            };
            let read = match resolve_read_client().await {
                Ok(read) => read,
                Err(e) => return render::finish::<serde_json::Value>(Err(e), json),
            };
            commands::threads::run_store(&store, &read, c, json).await
        }
        Command::Source(c) => {
            let store = match resolve_store_client().await {
                Ok(store) => store,
                Err(e) => return render::finish::<serde_json::Value>(Err(e), json),
            };
            let read = match resolve_read_client().await {
                Ok(read) => read,
                Err(e) => return render::finish::<serde_json::Value>(Err(e), json),
            };
            commands::source::run(&store, &read, c, json).await
        }
        Command::Push(a) => match resolve_store_client().await {
            Ok(store) => commands::source::push(&store, a, json).await,
            Err(e) => render::finish::<serde_json::Value>(Err(e), json),
        },
        Command::Status(a) => match resolve_store_client().await {
            Ok(store) => commands::presence::status(&store, a, json).await,
            Err(e) => render::finish::<serde_json::Value>(Err(e), json),
        },
        Command::Subscribe(a) => match resolve_store_client().await {
            Ok(store) => commands::presence::subscribe(&store, a, json).await,
            Err(e) => render::finish::<serde_json::Value>(Err(e), json),
        },
        Command::Unsubscribe(a) => match resolve_store_client().await {
            Ok(store) => commands::presence::unsubscribe(&store, a, json).await,
            Err(e) => render::finish::<serde_json::Value>(Err(e), json),
        },
        Command::Search(a) => match resolve_read_client().await {
            Ok(read) => commands::memory::search(&read, a, json).await,
            Err(e) => render::finish::<serde_json::Value>(Err(e), json),
        },
        Command::History(a) => match resolve_read_client().await {
            Ok(read) => commands::memory::history(&read, a, json).await,
            Err(e) => render::finish::<serde_json::Value>(Err(e), json),
        },
        Command::Read(a) => match resolve_read_client().await {
            Ok(read) => commands::memory::read(&read, a, json).await,
            Err(e) => render::finish::<serde_json::Value>(Err(e), json),
        },
        Command::Admin(c) => match resolve_store_client().await {
            Ok(store) => commands::admin::run(&store, c, json).await,
            Err(e) => render::finish::<serde_json::Value>(Err(e), json),
        },
        Command::Agents(c) => {
            let store = match resolve_store_client().await {
                Ok(store) => store,
                Err(e) => return render::finish::<serde_json::Value>(Err(e), json),
            };
            let read = match resolve_read_client().await {
                Ok(read) => read,
                Err(e) => return render::finish::<serde_json::Value>(Err(e), json),
            };
            commands::agents::run(&store, &read, c, json).await
        }
        Command::Gateway(c) => commands::gateway::run(c, json).await,
        Command::Webconsole(c) => commands::webconsole::run(c, json).await,
        Command::Update(a) => commands::update::run(a, json).await,
        Command::Listen(a) => match resolve_store_client().await {
            Ok(store) => commands::listen::listen(&store, a, json).await,
            Err(e) => render::finish::<serde_json::Value>(Err(e), json),
        },
        Command::Mcp(a) => commands::mcp::run_mcp(a).await,
    }
}

async fn resolve_store_client() -> Result<StoreClient, ContractError> {
    StoreClient::from_config().await
}

async fn resolve_read_client() -> Result<ReadClient, ContractError> {
    ReadClient::from_config().await
}

#[cfg(test)]
#[path = "../../tests/unit/cli_scaffold.rs"]
mod cli_scaffold_contracts;
