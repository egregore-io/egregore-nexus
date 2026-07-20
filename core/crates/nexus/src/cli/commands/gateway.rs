//! Operator-facing lifecycle commands for the independently installed Nexus Gateway.

use std::process::ExitCode;
use std::time::Duration;

use clap::{Args, Subcommand, ValueEnum};
use nexus_common::{
    persist_gateway_projection_delivery_mode, Config, GatewayProjectionDeliveryMode,
};
use nexus_contracts::{
    DaemonIpcCall, DaemonIpcCaller, DaemonIpcRequest, Kind, Tier, DAEMON_IPC_PROTOCOL_VERSION,
};

use crate::gateway_lifecycle::{
    gateway_logs, gateway_status, restart_gateway, start_gateway, stop_gateway,
    GatewayLifecycleError, GatewayRuntimeStatus, GatewayStatusReport,
};
use crate::gateway_service::{
    install_gateway_service, uninstall_gateway_service, GatewayServiceReport,
};

#[derive(Subcommand, Debug, Clone, PartialEq, Eq)]
pub enum GatewayCmd {
    /// Install and start the Gateway under the native per-user supervisor.
    Install,
    /// Start Nexus Core when needed, then start the installed gateway.
    Start,
    /// Stop only the gateway; Nexus Core remains running.
    Stop(GatewayStopArgs),
    /// Ensure Nexus Core is running, then restart only the gateway.
    Restart(GatewayStopArgs),
    /// Show installation, process, API, and daemon dependency status.
    Status,
    /// Print or follow gateway logs.
    Logs(GatewayLogsArgs),
    /// Inspect or change daemon-to-Gateway projection delivery policy.
    #[command(subcommand, name = "delivery-mode")]
    DeliveryMode(GatewayDeliveryModeCmd),
    /// Inspect Gateway-owned message hooks without mutating their manifests.
    #[command(subcommand)]
    Hooks(GatewayHooksCmd),
    /// Stop and remove the Gateway's native per-user service.
    Uninstall,
}

#[derive(Subcommand, Debug, Clone, PartialEq, Eq)]
pub enum GatewayHooksCmd {
    /// List the active hook generation and registered handlers.
    List,
}

#[derive(Subcommand, Debug, Clone, PartialEq, Eq)]
pub enum GatewayDeliveryModeCmd {
    /// Show the configured and live effective delivery mode.
    Show,
    /// Persist and hot-apply a delivery mode.
    Set(GatewayDeliveryModeSetArgs),
}

#[derive(Args, Debug, Clone, PartialEq, Eq)]
pub struct GatewayDeliveryModeSetArgs {
    #[arg(value_enum)]
    pub mode: GatewayDeliveryModeValue,
}

#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayDeliveryModeValue {
    Buffered,
    BestEffort,
}

impl From<GatewayDeliveryModeValue> for GatewayProjectionDeliveryMode {
    fn from(value: GatewayDeliveryModeValue) -> Self {
        match value {
            GatewayDeliveryModeValue::Buffered => Self::Buffered,
            GatewayDeliveryModeValue::BestEffort => Self::BestEffort,
        }
    }
}

#[derive(Args, Debug, Clone, PartialEq, Eq)]
pub struct GatewayStopArgs {
    /// Force process termination if the graceful stop deadline expires.
    #[arg(long)]
    pub force: bool,
}

#[derive(Args, Debug, Clone, PartialEq, Eq)]
pub struct GatewayLogsArgs {
    /// Follow log output.
    #[arg(short, long)]
    pub follow: bool,
    /// Number of lines to print.
    #[arg(short = 'n', long, default_value_t = 80)]
    pub lines: usize,
}

pub async fn run(command: GatewayCmd, json: bool) -> ExitCode {
    match command {
        GatewayCmd::Install => match install_gateway_service().await {
            Ok(report) => render_service_report(&report, json),
            Err(error) => render_error(&error, json),
        },
        GatewayCmd::Start => match start_gateway().await {
            Ok(report) => render_report(&report, json, false),
            Err(error) => render_error(&error, json),
        },
        GatewayCmd::Stop(args) => match stop_gateway(args.force) {
            Ok(report) => render_report(&report, json, false),
            Err(error) => render_error(&error, json),
        },
        GatewayCmd::Restart(args) => match restart_gateway(args.force).await {
            Ok(report) => render_report(&report, json, false),
            Err(error) => render_error(&error, json),
        },
        GatewayCmd::Status => match gateway_status() {
            Ok(report) => render_report(&report, json, true),
            Err(error) => render_error(&error, json),
        },
        GatewayCmd::Logs(args) => match gateway_logs(args.lines, args.follow) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => render_error(&error, json),
        },
        GatewayCmd::DeliveryMode(command) => match run_delivery_mode(command).await {
            Ok(report) => render_delivery_mode(&report, json),
            Err(error) => render_error(&error, json),
        },
        GatewayCmd::Hooks(GatewayHooksCmd::List) => match list_hooks().await {
            Ok(report) => render_hooks(&report, json),
            Err(error) => render_error(&error, json),
        },
        GatewayCmd::Uninstall => match uninstall_gateway_service() {
            Ok(report) => render_service_report(&report, json),
            Err(error) => render_error(&error, json),
        },
    }
}

async fn list_hooks() -> Result<serde_json::Value, GatewayLifecycleError> {
    let paths = crate::gateway_lifecycle::GatewayPaths::resolve();
    let client = crate::cli::gateway_read_client::GatewayReadClient::discover(&paths.home)
        .map_err(|error| GatewayLifecycleError::lifecycle(error.message))?;
    client
        .get_json("/api/v1/hooks")
        .await
        .map_err(|error| GatewayLifecycleError::lifecycle(error.message))
}

fn render_hooks(report: &serde_json::Value, json: bool) -> ExitCode {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(report)
                .unwrap_or_else(|_| "{\"error\":\"serialization failed\"}".into())
        );
        return ExitCode::SUCCESS;
    }
    println!(
        "gateway hooks: generation={} handlers={} errors={}",
        report["generation"].as_str().unwrap_or("unknown"),
        report["hooks"].as_array().map(Vec::len).unwrap_or(0),
        report["errors"].as_array().map(Vec::len).unwrap_or(0),
    );
    if let Some(hooks) = report["hooks"].as_array() {
        for hook in hooks {
            println!(
                "{}\t{}\t{}",
                hook["id"].as_str().unwrap_or("unknown"),
                hook["event"].as_str().unwrap_or("unknown"),
                if hook["enabled"].as_bool().unwrap_or(false) {
                    "enabled"
                } else {
                    "disabled"
                },
            );
        }
    }
    ExitCode::SUCCESS
}

fn render_service_report(report: &GatewayServiceReport, json: bool) -> ExitCode {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(report).unwrap_or_else(|_| "{}".into())
        );
    } else if report.installed {
        println!(
            "Nexus Gateway service installed and healthy ({})",
            report.supervisor
        );
    } else {
        println!("Nexus Gateway service uninstalled ({})", report.supervisor);
    }
    ExitCode::SUCCESS
}

async fn run_delivery_mode(
    command: GatewayDeliveryModeCmd,
) -> Result<serde_json::Value, GatewayLifecycleError> {
    let paths = crate::gateway_lifecycle::GatewayPaths::resolve();
    let configured = match &command {
        GatewayDeliveryModeCmd::Show => Config::load().gateway_projection.delivery_mode,
        GatewayDeliveryModeCmd::Set(args) => {
            let mode = GatewayProjectionDeliveryMode::from(args.mode);
            persist_gateway_projection_delivery_mode(&paths.home, mode).map_err(|error| {
                GatewayLifecycleError::lifecycle(format!(
                    "persist Gateway delivery mode in {}: {error}",
                    paths.home.display()
                ))
            })?;
            mode
        }
    };
    let Some(_) = crate::daemon::daemon_ipc::read_daemon_ipc_endpoint(&paths.home) else {
        return Ok(serde_json::json!({
            "configured": delivery_mode_token(configured),
            "effective": null,
            "daemonEpoch": null,
            "events": 0,
            "bytes": 0,
            "gaps": 0,
            "dropped": 0,
            "resyncRequired": configured == GatewayProjectionDeliveryMode::Buffered,
        }));
    };
    let method = match &command {
        GatewayDeliveryModeCmd::Show => "local.gateway.deliveryMode.show",
        GatewayDeliveryModeCmd::Set(_) => "local.gateway.deliveryMode.set",
    };
    let params = match &command {
        GatewayDeliveryModeCmd::Show => serde_json::Value::Null,
        GatewayDeliveryModeCmd::Set(_) => {
            serde_json::json!({ "deliveryMode": delivery_mode_token(configured) })
        }
    };
    let response = crate::daemon::daemon_ipc::call_daemon_ipc(
        &paths.home,
        DaemonIpcRequest {
            version: DAEMON_IPC_PROTOCOL_VERSION,
            token: String::new(),
            request_id: format!("gateway-delivery-mode-{}", uuid::Uuid::new_v4()),
            caller: Some(DaemonIpcCaller {
                name: Some(crate::local_operator::display_name()),
                project: "default".into(),
                session_id: Some(crate::local_operator::LOCAL_OPERATOR_SESSION_ID.into()),
                agent_id: None,
                runtime_id: Some(crate::local_operator::LOCAL_OPERATOR_SESSION_ID.into()),
                client_key: None,
                kind: Kind::Human,
                locality: Default::default(),
                access: None,
                principal_id: None,
                tier: Tier::Admin,
            }),
            call: DaemonIpcCall::Query {
                method: method.into(),
                params,
            },
        },
        Duration::from_secs(5),
    )
    .await
    .map_err(|error| GatewayLifecycleError::lifecycle(error.to_string()))?;
    if let Some(error) = response.error {
        return Err(GatewayLifecycleError::lifecycle(error.message));
    }
    let mut report = response.result.unwrap_or_else(|| serde_json::json!({}));
    report["configured"] = serde_json::Value::String(delivery_mode_token(configured).into());
    Ok(report)
}

fn delivery_mode_token(mode: GatewayProjectionDeliveryMode) -> &'static str {
    match mode {
        GatewayProjectionDeliveryMode::Buffered => "buffered",
        GatewayProjectionDeliveryMode::BestEffort => "best_effort",
    }
}

fn render_delivery_mode(report: &serde_json::Value, json: bool) -> ExitCode {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(report)
                .unwrap_or_else(|_| "{\"error\":\"serialization failed\"}".into())
        );
    } else {
        println!(
            "gateway delivery: configured={} effective={}",
            report["configured"].as_str().unwrap_or("unknown"),
            report["effective"].as_str().unwrap_or("daemon-down")
        );
        println!(
            "backlog: events={} bytes={} gaps={} dropped={}",
            report["events"].as_u64().unwrap_or(0),
            report["bytes"].as_u64().unwrap_or(0),
            report["gaps"].as_u64().unwrap_or(0),
            report["dropped"].as_u64().unwrap_or(0),
        );
    }
    ExitCode::SUCCESS
}

fn render_report(report: &GatewayStatusReport, json: bool, status_exit: bool) -> ExitCode {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(report)
                .unwrap_or_else(|_| "{\"error\":\"serialization failed\"}".into())
        );
    } else {
        let gateway = match &report.runtime {
            GatewayRuntimeStatus::Live { pid, url } => format!("live pid={pid} url={url}"),
            GatewayRuntimeStatus::Degraded { reason } => format!("degraded ({reason})"),
            GatewayRuntimeStatus::Stale { pid, url } => {
                format!("stale pid={pid:?} url={url:?}")
            }
            GatewayRuntimeStatus::Down => "down".into(),
        };
        println!("gateway: {gateway}");
        println!(
            "daemon: {}",
            if report.daemon_running {
                "running"
            } else {
                "down"
            }
        );
        println!("log: {}", report.log_path.display());
    }
    if status_exit {
        ExitCode::from(report.exit_code())
    } else {
        ExitCode::SUCCESS
    }
}

pub fn render_error(error: &GatewayLifecycleError, json: bool) -> ExitCode {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&error.envelope())
                .unwrap_or_else(|_| "{\"error\":{\"code\":\"SERIALIZATION_ERROR\"}}".into())
        );
    } else {
        eprintln!("error: {}", error.message());
        if error.hint().is_some() {
            eprintln!("tip: install it with `npm install -g @egregore/nexus-gateway`");
        }
    }
    ExitCode::from(error.exit_code())
}
