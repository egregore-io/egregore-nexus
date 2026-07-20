//! Operator-facing lifecycle commands for the on-demand Nexus Webconsole.

use std::process::ExitCode;

use clap::{Args, Subcommand};

use crate::webconsole_lifecycle::{
    launch_webconsole, restart_webconsole, start_webconsole, stop_webconsole, webconsole_logs,
    webconsole_status, webconsole_url, WebconsoleLifecycleError, WebconsoleRuntimeStatus,
    WebconsoleStartOptions, WebconsoleStatusReport,
};

#[derive(Subcommand, Debug, Clone, PartialEq, Eq)]
pub enum WebconsoleCmd {
    /// Ensure dependencies, start or reuse the Webconsole, and open it in a browser.
    Launch(WebconsoleLaunchArgs),
    /// Start the Webconsole detached without opening a browser.
    Start(WebconsoleStartArgs),
    /// Stop only the Webconsole.
    Stop(WebconsoleStopArgs),
    /// Stop then start only the Webconsole.
    Restart(WebconsoleStartArgs),
    /// Show installation, process, URL, and dependency health.
    Status,
    /// Print or follow Webconsole logs.
    Logs(WebconsoleLogsArgs),
    /// Print the resolved Webconsole URL.
    Url,
}

#[derive(Args, Debug, Clone, PartialEq, Eq)]
pub struct WebconsoleLaunchArgs {
    /// Start the Webconsole without opening a browser.
    #[arg(long)]
    pub no_open: bool,
    #[command(flatten)]
    pub server: WebconsoleStartArgs,
}

#[derive(Args, Debug, Clone, PartialEq, Eq)]
pub struct WebconsoleStartArgs {
    /// Bind address. Defaults to loopback.
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,
    /// HTTP port.
    #[arg(long, default_value_t = 4200)]
    pub port: u16,
}

#[derive(Args, Debug, Clone, PartialEq, Eq)]
pub struct WebconsoleStopArgs {
    /// Force process termination after the graceful deadline.
    #[arg(long)]
    pub force: bool,
}

#[derive(Args, Debug, Clone, PartialEq, Eq)]
pub struct WebconsoleLogsArgs {
    /// Follow log output.
    #[arg(short, long)]
    pub follow: bool,
    /// Number of lines to print.
    #[arg(short = 'n', long, default_value_t = 80)]
    pub lines: usize,
}

pub async fn run(command: WebconsoleCmd, json: bool) -> ExitCode {
    match command {
        WebconsoleCmd::Launch(args) => {
            security_notice(&args.server.host);
            match launch_webconsole(options(args.server), args.no_open).await {
                Ok(report) => render_report(&report, json, false),
                Err(error) => render_error(&error, json),
            }
        }
        WebconsoleCmd::Start(args) => {
            security_notice(&args.host);
            match start_webconsole(options(args)).await {
                Ok(report) => render_report(&report, json, false),
                Err(error) => render_error(&error, json),
            }
        }
        WebconsoleCmd::Stop(args) => match stop_webconsole(args.force) {
            Ok(report) => render_report(&report, json, false),
            Err(error) => render_error(&error, json),
        },
        WebconsoleCmd::Restart(args) => {
            security_notice(&args.host);
            match restart_webconsole(options(args)).await {
                Ok(report) => render_report(&report, json, false),
                Err(error) => render_error(&error, json),
            }
        }
        WebconsoleCmd::Status => match webconsole_status() {
            Ok(report) => render_report(&report, json, true),
            Err(error) => render_error(&error, json),
        },
        WebconsoleCmd::Logs(args) => match webconsole_logs(args.lines, args.follow) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => render_error(&error, json),
        },
        WebconsoleCmd::Url => match webconsole_url() {
            Ok(url) => {
                if json {
                    println!("{}", serde_json::json!({ "url": url }));
                } else {
                    println!("{url}");
                }
                ExitCode::SUCCESS
            }
            Err(error) => render_error(&error, json),
        },
    }
}

fn options(args: WebconsoleStartArgs) -> WebconsoleStartOptions {
    WebconsoleStartOptions {
        host: args.host,
        port: args.port,
    }
}

fn security_notice(host: &str) {
    if !matches!(host, "127.0.0.1" | "localhost" | "::1") {
        eprintln!(
            "warning: Webconsole is binding to {host}; use a firewall or trusted private network"
        );
    }
}

fn render_report(report: &WebconsoleStatusReport, json: bool, status_mode: bool) -> ExitCode {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(report).unwrap_or_else(|_| "{}".into())
        );
    } else {
        match &report.runtime {
            WebconsoleRuntimeStatus::Live { url, pid, .. } => {
                println!("Nexus Webconsole live pid={pid} url={url}")
            }
            WebconsoleRuntimeStatus::Degraded { reason } => {
                println!("Nexus Webconsole degraded: {reason}")
            }
            WebconsoleRuntimeStatus::Stale { pid, .. } => {
                println!("Nexus Webconsole stale pid={pid:?}")
            }
            WebconsoleRuntimeStatus::Down => println!("Nexus Webconsole down"),
        }
    }
    ExitCode::from(if status_mode { report.exit_code() } else { 0 })
}

fn render_error(error: &WebconsoleLifecycleError, json: bool) -> ExitCode {
    if json {
        println!(
            "{}",
            serde_json::json!({
                "error": {
                    "code": error.code(),
                    "message": error.message(),
                    "hint": error.hint(),
                    "data": error.data(),
                }
            })
        );
    } else {
        eprintln!("error: {}", error.message());
        if let Some(hint) = error.hint() {
            eprintln!("hint: {hint}");
        }
    }
    ExitCode::from(error.exit_code())
}
