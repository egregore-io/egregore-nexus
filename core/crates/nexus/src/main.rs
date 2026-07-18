//! The `nexus` binary entry point — **one binary = daemon + CLI**.
//!
//! `nexus daemon run` runs the always-on hub: it loads [`Config`], initializes tracing, opens +
//! migrates the [`Store`], reaps orphaned harness process groups from the durable runtime process
//! ledger (with a pre-ledger ACP env-scan fallback), builds the wired [`AppState`], and starts the
//! store-backed command worker. The daemon-owned local IPC endpoint is the only production ingress
//! for commands and read views; producers do not open the durable store.
//! `nexus daemon start|stop|restart|status|logs|doctor|install|uninstall` is the
//! operator-facing lifecycle surface. Every **other** subcommand is the agent's deliberate outbound
//! and is routed through [`nexus::cli::run`].
//! Identity is resolved server-side; canonical bus writes still happen through daemon services.

use std::process::ExitCode;
use std::sync::Arc;

use clap::Parser;

use nexus::cli::{self, Cli};
use nexus::daemon::AppState;
use nexus_common::{init_tracing, new_session_id, now, Config};
use nexus_store::repos::DaemonState;

#[tokio::main]
async fn main() -> ExitCode {
    // The daemon is its own pre-parse arm; every other subcommand is a CLI client call routed
    // through `cli::run`. We dispatch on `argv[1]` but still parse each arm through clap so `--help`
    // works for both `nexus daemon --help` and `nexus <client-cmd> --help`.
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("daemon") {
        // Parse through clap so `daemon --help`/bad-args behave correctly (clap exits for --help
        // and parse errors). Bare `nexus daemon` remains foreground-compatible and is equivalent to
        // `nexus daemon run`.
        let daemon = nexus::daemon::lifecycle::DaemonCli::parse().into_daemon_args();
        match daemon.command {
            None
            | Some(nexus::daemon::lifecycle::DaemonCommand::Run)
            | Some(nexus::daemon::lifecycle::DaemonCommand::Start(
                nexus::daemon::lifecycle::StartArgs {
                    foreground: true, ..
                },
            )) => {}
            Some(command) => return nexus::daemon::lifecycle::run_lifecycle_command(command).await,
        }
        return match run_daemon().await {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("{}", daemon_error_message(e.as_ref()));
                ExitCode::from(1)
            }
        };
    }

    let cli = Cli::parse();
    cli::run(cli).await
}

fn daemon_error_message(e: &(dyn std::error::Error + 'static)) -> String {
    if e.downcast_ref::<std::io::Error>()
        .is_some_and(|io| io.kind() == std::io::ErrorKind::AlreadyExists)
    {
        e.to_string()
    } else {
        format!("error: {e}")
    }
}

/// Boot the daemon: config -> tracing -> store (open + migrate) -> `AppState` -> command worker.
/// Gateway/CLI/MCP producers write command intents and read store projections.
async fn run_daemon() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::load();
    init_tracing();

    let lifecycle_paths = nexus::daemon::lifecycle::DaemonPaths::resolve();
    // Claim the local daemon state before binding public surfaces so a second daemon cannot start
    // against the same store and process-owned harness state.
    let state_dir = lifecycle_paths.home.to_string_lossy().to_string();
    let _daemon_singleton = nexus::daemon::process_guard::DaemonSingleton::acquire(&state_dir)?;
    if let Some(attribution) = nexus::daemon::lifecycle::read_shutdown_attribution(&lifecycle_paths)
    {
        tracing::warn!(%attribution, "daemon shutdown attribution recorded");
    }

    // Opening the daemon store applies the identity/continuity schema and creates a fresh anonymous
    // transport database. No second process opens either authority directly.
    let store = Arc::new(nexus::daemon::lifecycle::open_daemon_owned_store(&config).await?);
    let ledger_sweep = nexus::daemon::process_ledger::reap_runtime_process_ledger(&store).await?;
    if ledger_sweep.candidates > 0 || ledger_sweep.failures > 0 {
        tracing::warn!(
            candidates = ledger_sweep.candidates,
            groups_signalled = ledger_sweep.groups_signalled,
            cleared = ledger_sweep.cleared,
            failures = ledger_sweep.failures,
            "daemon boot swept runtime process groups from durable ledger"
        );
    }
    let acp_sweep = nexus_agent::adapter::engine::reap_orphaned_acp_harness_processes();
    if acp_sweep.candidates > 0 || acp_sweep.failures > 0 {
        tracing::warn!(
            scanned = acp_sweep.scanned,
            candidates = acp_sweep.candidates,
            groups_signalled = acp_sweep.groups_signalled,
            failures = acp_sweep.failures,
            "daemon boot fallback-swept pre-ledger ACP harness process groups"
        );
    }
    let boot_epoch = format!("boot_{}", new_session_id().0.trim_start_matches("s_"));
    DaemonState::new(&store)
        .set_boot_epoch(&boot_epoch, now())
        .await?;
    tracing::info!(%boot_epoch, "nexus daemon boot epoch recorded");

    let gateway_stream =
        nexus::daemon::gateway_stream_socket::GatewayStreamPublisher::new_with_projection_config(
            16_384,
            config.gateway_projection,
            boot_epoch.clone(),
        );
    let gateway_stream_socket = nexus::daemon::gateway_stream_socket::spawn_gateway_stream_socket(
        store.clone(),
        gateway_stream.clone(),
        boot_epoch.clone(),
    )
    .map_err(|error| {
        tracing::warn!(%error, "gateway stream push socket disabled");
        error
    })
    .ok();
    let state =
        AppState::wire_pty_with_gateway_stream(store.clone(), &config, Some(gateway_stream));

    // --- Store-backed ingress: producers submit command rows; the daemon executes them. ---
    state.wait_for_runtime_identity_ready().await;
    tracing::info!("nexus daemon runtime identity directory restored");
    let command_worker = nexus::daemon::command_worker::spawn(state.clone());
    tracing::info!("nexus daemon command worker running");
    let daemon_ipc = nexus::daemon::daemon_ipc::spawn_daemon_ipc(
        state.clone(),
        boot_epoch.clone(),
        &lifecycle_paths.home,
    )?;
    tracing::info!(
        endpoint = %daemon_ipc.endpoint().path.display(),
        "nexus daemon local IPC running"
    );

    // A daemon restart must preserve headed harness terminals. Existing tmux/app-server processes
    // are durable runtime attachments; boot revive/rebind reconciles liveness instead of treating
    // restart as a destructive lifecycle command.
    shutdown_signal().await;
    tracing::info!("nexus daemon tearing down recorded transports for shutdown");
    let transports = state.teardown_owned_transports_for_shutdown().await;
    tracing::info!(transports, "nexus daemon transport teardown complete");
    tracing::info!("nexus daemon draining command lanes for shutdown");
    let reaped = nexus::daemon::lifecycle::reap_claimed_command_intents(&store).await?;
    tracing::info!(
        reaped,
        "nexus daemon reaped in-flight command intents for shutdown"
    );
    drop(gateway_stream_socket);
    drop(daemon_ipc);
    command_worker.abort();
    // A failed final checkpoint must not turn a completed graceful shutdown into an error
    // exit — deploy tooling gates on this process's exit code (#29).
    if let Err(error) = nexus::daemon::lifecycle::checkpoint_wal(&store).await {
        tracing::warn!(%error, "final WAL checkpoint failed during shutdown");
    }
    Ok(())
}

/// Resolve when the process receives a shutdown signal: Ctrl-C (SIGINT) or, on Unix, SIGTERM (what
/// `kill <pid>` sends). Lifecycle stop/restart routes through this path and then tears down every
/// recorded daemon-owned transport before final WAL/discovery cleanup.
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {}
        _ = term => {}
    }
}

#[cfg(test)]
#[path = "../tests/unit/main_entrypoint.rs"]
mod main_entrypoint_contracts;
