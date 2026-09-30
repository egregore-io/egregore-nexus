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

const LONG_VERSION: &str = concat!(
    env!("CARGO_PKG_VERSION"),
    " (revision ",
    env!("NEXUS_BUILD_REVISION"),
    ")"
);

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
mod scaffold_tests {
    use super::*;
    use clap::Parser;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(std::iter::once("nexus").chain(args.iter().copied())).unwrap()
    }

    #[test]
    fn clap_parses_every_documented_subcommand() {
        // lifecycle
        parse(&["launch", "claude"]);
        parse(&["launch", "claude", "--detach"]);
        parse(&["register", "--name", "ben"]);
        parse(&["whoami"]);
        parse(&["resume", "claude"]);
        parse(&["resume", "s_123"]);
        parse(&["attach", "claude"]);
        // discover
        parse(&["members"]);
        parse(&["members", "--include-offline", "--presence"]);
        parse(&["threads", "--mine"]);
        parse(&["topics", "--subscribed"]);
        // send
        parse(&["dm", "ben", "-m", "hi"]);
        parse(&["dm", "ben", "--stdin"]);
        parse(&["post", "backend", "-m", "plan"]);
        parse(&["post", "backend", "--stdin"]);
        parse(&["reply", "-m", "on it"]);
        parse(&["reply", "--stdin"]);
        parse(&["publish", "ci", "-m", "green"]);
        parse(&["publish", "ci", "--stdin"]);
        parse(&["send", "--to", "ben", "-m", "hi"]);
        parse(&["send", "--to", "ben", "--stdin"]);
        // threads
        parse(&["thread", "new", "backend", "--member", "dylan"]);
        parse(&["thread", "join", "backend"]);
        parse(&["thread", "leave", "backend"]);
        parse(&["thread", "archive", "backend"]);
        parse(&["thread", "delete", "backend"]);
        parse(&["thread", "members", "backend"]);
        // presence
        parse(&["status", "paused", "--work", "compacting"]);
        parse(&["status"]);
        parse(&["subscribe", "ci", "--group", "workers"]);
        parse(&["unsubscribe", "ci"]);
        // memory
        parse(&["search", "auth refactor", "--hybrid", "--with", "ben"]);
        parse(&["history", "--with", "ben", "--limit", "50"]);
        parse(&["read", "m_01"]);
        // admin
        parse(&["admin", "route", "n_01", "--to", "ben"]);
        parse(&["admin", "spawn", "codex", "--name", "dylan"]);
        parse(&["admin", "remove", "dylan"]);
        parse(&["admin", "delete", "dylan"]);
        parse(&["admin", "channel", "create", "ci"]);
        parse(&["admin", "assign-role", "ben", "lead"]);
        parse(&["admin", "assign-project", "ben", "--project", "lens"]);
        parse(&["admin", "grant-tier", "ben", "admin"]);
        parse(&["admin", "monitor", "--follow"]);
        // notification sources
        parse(&["source", "register", "gh-ci", "--topic", "ci.events"]);
        parse(&["source", "ls"]);
        parse(&["source", "show", "gh-ci"]);
        parse(&["source", "enable", "gh-ci"]);
        parse(&["source", "disable", "gh-ci"]);
        parse(&["source", "rotate", "gh-ci"]);
        parse(&["source", "rm", "gh-ci"]);
        // push
        parse(&["push", "myapp", "-m", "deploy done"]);
        parse(&[
            "push",
            "myapp",
            "--topic",
            "ci",
            "--summary",
            "s",
            "-m",
            "body",
        ]);
        parse(&["push", "myapp", "--json"]);
        // agents
        parse(&["agents", "list"]);
        // reach-the-daemon
        parse(&["listen"]);
        parse(&["listen", "--timeout-ms", "5000", "--max", "10", "--once"]);
        // mcp stdio server
        parse(&["mcp", "--as", "ada", "--project", "lens"]);
        // global flags
        parse(&["--json", "whoami"]);
    }

    #[test]
    fn assign_project_parses_name_and_project_flag() {
        let cli = parse(&["admin", "assign-project", "ben", "--project", "lens"]);
        match cli.command {
            Command::Admin(commands::admin::AdminCmd::AssignProject { name, project }) => {
                assert_eq!(name, "ben");
                assert_eq!(project, "lens");
            }
            other => panic!("expected AssignProject, got {:?}", other),
        }
    }

    #[test]
    fn admin_route_help_names_notification_audit_id() {
        use clap::CommandFactory;

        let mut command = Cli::command();
        let admin = command
            .find_subcommand_mut("admin")
            .expect("admin subcommand");
        let route = admin
            .find_subcommand_mut("route")
            .expect("admin route subcommand");
        let mut help = Vec::new();
        route.write_long_help(&mut help).unwrap();
        let help = String::from_utf8(help).unwrap();

        assert!(help.contains("notification audit id"), "{help}");
    }

    #[test]
    fn agents_list_parses_correctly() {
        let cli = parse(&["agents", "list"]);
        assert!(matches!(
            cli.command,
            Command::Agents(commands::agents::AgentsCmd::List)
        ));
    }

    #[test]
    fn json_flag_is_global_and_parsed() {
        let cli = parse(&["--json", "whoami"]);
        assert!(cli.global.json);
        // `--json` works after the subcommand too (global).
        let cli2 = parse(&["whoami", "--json"]);
        assert!(cli2.global.json);
    }
}
