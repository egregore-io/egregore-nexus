//! Daemon lifecycle CLI support.
//!
//! `nexus daemon run` remains the foreground development loop. The rest of this module backs the
//! operator-facing lifecycle surface (`start`, `stop`, `restart`, `status`, `logs`, `doctor`, and
//! service installation) without exposing those supervision verbs to daemon-launched agents.

use std::env;
use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};
use std::thread;
use std::time::{Duration, SystemTime};

use clap::{Args, Parser, Subcommand};
use nexus_common::{config::expand_tilde, now, Config, NexusError};
use nexus_contracts::{
    DaemonIpcCall, DaemonIpcCaller, DaemonIpcRequest, Kind, Tier, DAEMON_IPC_PROTOCOL_VERSION,
};
use nexus_store::repos::CommandIntents;
use nexus_store::{DaemonStore, Store};
use sha2::{Digest, Sha256};

#[cfg(target_os = "linux")]
const SYSTEMD_SERVICE: &str = "nexus-daemon.service";
#[cfg(target_os = "linux")]
const LEGACY_SYSTEMD_SQLD_SERVICE: &str = "nexus-sqld.service";
const WINDOWS_TASK_NAME: &str = "EgregoreNexusDaemon";
const LAUNCHD_LABEL: &str = "io.egregore.nexus.daemon";
const OPERATOR_ONLY: &str = "daemon supervision is operator-only";
const SHUTDOWN_ATTRIBUTION: &str = "daemon-shutdown-request.json";
const DEFAULT_STOP_GRACE: Duration = Duration::from_secs(45);
const FORCE_STOP_GRACE: Duration = Duration::from_secs(10);
const SERVICE_START_GRACE: Duration = Duration::from_secs(5);
const SERVICE_STABILITY_GRACE: Duration = Duration::from_millis(500);

/// Parser for the daemon arm of the `nexus` binary.
#[derive(Parser, Debug)]
#[command(name = "nexus", about = "Nexus daemon lifecycle")]
pub struct DaemonCli {
    #[command(subcommand)]
    command: DaemonRootCommand,
}

#[derive(Subcommand, Debug)]
enum DaemonRootCommand {
    /// Manage the local Nexus daemon.
    Daemon(DaemonArgs),
}

impl DaemonCli {
    /// Return the parsed `daemon` arguments.
    pub fn into_daemon_args(self) -> DaemonArgs {
        match self.command {
            DaemonRootCommand::Daemon(args) => args,
        }
    }
}

/// Arguments under `nexus daemon`.
#[derive(Args, Debug)]
pub struct DaemonArgs {
    #[command(subcommand)]
    pub command: Option<DaemonCommand>,
}

/// `nexus daemon <verb>`.
#[derive(Subcommand, Debug)]
pub enum DaemonCommand {
    /// Run the daemon in the foreground. This is the development loop.
    Run,
    /// Start the daemon detached under the platform supervisor when installed, else self-daemonize.
    Start(StartArgs),
    /// Stop the daemon gracefully.
    Stop(StopArgs),
    /// Stop then start the daemon. This is the deploy primitive.
    Restart(RestartArgs),
    /// Print daemon health and exit with 0 healthy, 1 degraded, 2 down.
    Status,
    /// Tail daemon logs.
    Logs(LogsArgs),
    /// Read-only lifecycle preflight.
    Doctor,
    /// Install, enable, and start the platform supervisor unit/service.
    Install(InstallArgs),
    /// Stop and remove the platform supervisor unit/service.
    Uninstall,
}

/// `nexus daemon start` flags.
#[derive(Args, Debug)]
pub struct StartArgs {
    /// Alias for `nexus daemon run`.
    #[arg(long)]
    pub foreground: bool,
    /// Binary to execute when using the self-daemonizing fallback.
    #[arg(long)]
    pub binary: Option<PathBuf>,
}

/// `nexus daemon stop` flags.
#[derive(Args, Debug)]
pub struct StopArgs {
    /// Kill the daemon process if it does not exit after the 10s force grace.
    #[arg(long)]
    pub force: bool,
}

/// `nexus daemon restart` flags.
#[derive(Args, Debug)]
pub struct RestartArgs {
    /// Binary to use for the start half. Without this, restart uses the installed/current binary.
    #[arg(long)]
    pub binary: Option<PathBuf>,
    /// Kill the daemon if graceful stop exceeds the force grace.
    #[arg(long)]
    pub force: bool,
}

/// `nexus daemon logs` flags.
#[derive(Args, Debug)]
pub struct LogsArgs {
    /// Follow log output.
    #[arg(short, long)]
    pub follow: bool,
    /// Number of lines to print.
    #[arg(short = 'n', long, default_value_t = 80)]
    pub lines: usize,
}

/// `nexus daemon install` flags.
#[derive(Args, Debug)]
pub struct InstallArgs {
    /// Binary path recorded in the native service unit.
    #[arg(long)]
    pub binary: Option<PathBuf>,
}

/// Error type for lifecycle control commands.
#[derive(Debug, thiserror::Error)]
pub enum LifecycleError {
    #[error("{OPERATOR_ONLY}")]
    OperatorOnly,
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("{0}")]
    Store(#[from] NexusError),
    #[error("{0}")]
    Command(String),
}

/// Process paths owned by the daemon lifecycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonPaths {
    pub home: PathBuf,
    pub pid_file: PathBuf,
    pub lock_file: PathBuf,
    pub log_file: PathBuf,
    pub gateway_file: PathBuf,
    pub shutdown_attribution_file: PathBuf,
}

impl DaemonPaths {
    /// Resolve lifecycle paths from `NEXUS_HOME`, defaulting to `~/.nexus`.
    pub fn resolve() -> Self {
        let home = nexus_home();
        Self {
            pid_file: home.join("daemon.pid"),
            lock_file: home.join("daemon.lock"),
            log_file: home.join("daemon.log"),
            gateway_file: home.join("gateway.json"),
            shutdown_attribution_file: home.join(SHUTDOWN_ATTRIBUTION),
            home,
        }
    }
}

/// Return the daemon state root. All lifecycle-created files go through this helper.
pub fn nexus_home() -> PathBuf {
    env::var_os("NEXUS_HOME")
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(expand_tilde("~/.nexus")))
}

/// Execute a non-foreground lifecycle command.
pub async fn run_lifecycle_command(command: DaemonCommand) -> ExitCode {
    let result = match command {
        DaemonCommand::Run
        | DaemonCommand::Start(StartArgs {
            foreground: true, ..
        }) => {
            return ExitCode::SUCCESS;
        }
        DaemonCommand::Start(args) => start(args).await.map(|_| ExitCode::SUCCESS),
        DaemonCommand::Stop(args) => stop(args).await.map(|_| ExitCode::SUCCESS),
        DaemonCommand::Restart(args) => restart(args).await.map(|_| ExitCode::SUCCESS),
        DaemonCommand::Status => status().await,
        DaemonCommand::Logs(args) => logs(args).await.map(|_| ExitCode::SUCCESS),
        DaemonCommand::Doctor => doctor().await,
        DaemonCommand::Install(args) => install_service(args).await.map(|_| ExitCode::SUCCESS),
        DaemonCommand::Uninstall => uninstall_service().await.map(|_| ExitCode::SUCCESS),
    };
    match result {
        Ok(code) => code,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::from(1)
        }
    }
}

/// Compatibility no-op for callers compiled against the pre-v0.1 sqld preflight API.
///
/// The daemon now owns an embedded file and does not connect to `db_url`.
pub fn preflight_configured_db_url(config: &Config) -> Result<(), LifecycleError> {
    let _ = config;
    Ok(())
}

/// Open the daemon's exclusive embedded libSQL file.
///
/// Legacy `db_url`/auth settings remain parseable for pre-v0.1 config compatibility but are not
/// consulted here. CLI, MCP, and gateway producers reach this owner through daemon IPC instead of
/// opening a shared sqld/Hrana endpoint.
pub async fn open_daemon_owned_store(config: &Config) -> Result<Store, nexus_common::NexusError> {
    let stores = DaemonStore::open(&config.daemon_store_path()).await?;
    Ok(stores.compatibility_store())
}

async fn start(args: StartArgs) -> Result<(), LifecycleError> {
    start_with_announcement(args, true).await
}

async fn start_with_announcement(args: StartArgs, announce: bool) -> Result<(), LifecycleError> {
    ensure_operator_supervision_for("start")?;
    let paths = DaemonPaths::resolve();
    fs::create_dir_all(&paths.home)?;
    let binary = args.binary.unwrap_or(current_exe()?);
    let supervisor = select_control_supervisor(&paths, binary);
    supervisor.start()?;
    let status = collect_status().await;
    let pid = status
        .pid
        .map(|p| p.to_string())
        .unwrap_or_else(|| "unknown".into());
    if announce {
        println!(
            "nexus daemon started pid={} supervisor={} log={}",
            pid,
            supervisor.kind_label(),
            paths.log_file.display()
        );
    }
    Ok(())
}

/// Ensure Nexus Core is running for a separately managed local dependency such as Nexus Gateway.
/// This is idempotent and preserves the operator-only lifecycle boundary.
pub async fn ensure_running() -> Result<(), LifecycleError> {
    ensure_operator_supervision_for("gateway-start")?;
    if dependency_running() {
        return Ok(());
    }
    start_with_announcement(
        StartArgs {
            foreground: false,
            binary: None,
        },
        false,
    )
    .await
}

/// Cheap process-only daemon dependency probe used by gateway lifecycle status.
pub fn dependency_running() -> bool {
    let paths = DaemonPaths::resolve();
    read_pid(&paths.pid_file)
        .ok()
        .flatten()
        .is_some_and(process_alive)
}

/// Restart the daemon after a managed package update using the verified replacement binary.
pub async fn restart_after_update(binary: PathBuf) -> Result<(), LifecycleError> {
    restart(RestartArgs {
        binary: Some(binary),
        force: false,
    })
    .await
}

/// Ensure the native per-user daemon service exists and is healthy for a dependent facet.
pub async fn ensure_service_installed() -> Result<(), LifecycleError> {
    ensure_operator_supervision_for("install")?;
    let paths = DaemonPaths::resolve();
    let binary = current_exe()?;
    let supervisor = select_platform_supervisor(&paths, binary.clone());
    if !supervisor.installed() {
        return install_service(InstallArgs {
            binary: Some(binary),
        })
        .await;
    }
    if !dependency_running() {
        supervisor.start()?;
        if !wait_until_running(&paths.pid_file, SERVICE_START_GRACE) {
            return Err(LifecycleError::Command(
                "nexus daemon service did not become healthy".into(),
            ));
        }
    }
    Ok(())
}

/// Whether the native per-user daemon service definition is installed.
pub fn service_installed() -> bool {
    let paths = DaemonPaths::resolve();
    select_platform_supervisor(&paths, current_exe().unwrap_or_default()).installed()
}

/// Rewrite an installed daemon service to the verified replacement executable without changing
/// whether the daemon was running. The update transaction restores prior runtime state later.
pub fn rewrite_service_after_update(binary: PathBuf) -> Result<(), LifecycleError> {
    ensure_operator_supervision_for("update")?;
    let paths = DaemonPaths::resolve();
    let supervisor = select_platform_supervisor(&paths, binary);
    if supervisor.installed() {
        supervisor.install()?;
    }
    Ok(())
}

async fn stop(args: StopArgs) -> Result<(), LifecycleError> {
    ensure_operator_supervision_for("stop")?;
    let paths = DaemonPaths::resolve();
    fs::create_dir_all(&paths.home)?;
    write_shutdown_attribution(&paths, "stop")?;
    let supervisor = select_control_supervisor(&paths, current_exe()?);
    supervisor.stop(args.force)?;
    println!(
        "nexus daemon stop requested supervisor={}",
        supervisor.kind_label()
    );
    Ok(())
}

async fn restart(args: RestartArgs) -> Result<(), LifecycleError> {
    ensure_operator_supervision_for("restart")?;
    let paths = DaemonPaths::resolve();
    fs::create_dir_all(&paths.home)?;
    write_shutdown_attribution(&paths, "restart")?;
    let binary = args.binary.unwrap_or(current_exe()?);
    let supervisor = select_control_supervisor(&paths, binary);
    supervisor.restart(args.force)?;
    println!(
        "nexus daemon restarted supervisor={} log={}",
        supervisor.kind_label(),
        paths.log_file.display()
    );
    Ok(())
}

async fn install_service(args: InstallArgs) -> Result<(), LifecycleError> {
    ensure_operator_supervision_for("install")?;
    let paths = DaemonPaths::resolve();
    fs::create_dir_all(&paths.home)?;
    let binary = args.binary.unwrap_or(current_exe()?);
    let previous_supervisor = select_control_supervisor(&paths, binary.clone());
    let was_running = read_pid(&paths.pid_file)
        .ok()
        .flatten()
        .is_some_and(process_alive);
    let supervisor = select_platform_supervisor(&paths, binary);
    if was_running {
        write_shutdown_attribution(&paths, "install")?;
        previous_supervisor.stop(false)?;
        if !wait_until_down(&paths.pid_file, DEFAULT_STOP_GRACE) {
            return Err(LifecycleError::Command(
                "existing daemon did not stop during service installation".into(),
            ));
        }
    }
    supervisor.install()?;
    supervisor.start()?;
    if !wait_until_running(&paths.pid_file, SERVICE_START_GRACE) {
        return Err(LifecycleError::Command(format!(
            "nexus daemon service was installed but did not become healthy ({0}); inspect `nexus daemon logs` and the {0} service manager",
            supervisor.kind_label()
        )));
    }
    println!(
        "nexus daemon service installed and started ({})",
        supervisor.kind_label()
    );
    if crate::update::install_context::detect_install_context()
        .map(|context| {
            context
                .facets
                .contains(&crate::update::install_context::InstalledFacet::Gateway)
        })
        .unwrap_or(false)
        && crate::gateway_lifecycle::resolve_installed_gateway().is_ok()
    {
        crate::gateway_service::install_gateway_service()
            .await
            .map_err(|error| LifecycleError::Command(error.to_string()))?;
        println!("nexus gateway service installed and started");
    }
    Ok(())
}

async fn uninstall_service() -> Result<(), LifecycleError> {
    ensure_operator_supervision_for("uninstall")?;
    if crate::gateway_service::gateway_service_status()
        .map(|status| status.installed)
        .unwrap_or(false)
    {
        crate::gateway_service::uninstall_gateway_service()
            .map_err(|error| LifecycleError::Command(error.to_string()))?;
        println!("nexus gateway service uninstalled");
    }
    let paths = DaemonPaths::resolve();
    let supervisor = select_platform_supervisor(&paths, current_exe()?);
    supervisor.uninstall()?;
    println!(
        "nexus daemon service uninstalled ({})",
        supervisor.kind_label()
    );
    Ok(())
}

async fn status() -> Result<ExitCode, LifecycleError> {
    let status = collect_status().await;
    print_status(&status);
    Ok(status.exit_code())
}

async fn logs(args: LogsArgs) -> Result<(), LifecycleError> {
    let paths = DaemonPaths::resolve();
    let supervisor = select_control_supervisor(&paths, current_exe()?);
    if let Some(cmd) = supervisor.logs_command(args.lines, args.follow) {
        let status = cmd.run_inherit()?;
        if !status.success {
            eprintln!(
                "native log reader failed; falling back to {}",
                paths.log_file.display()
            );
            print_file_tail(&paths.log_file, args.lines)?;
        }
        return Ok(());
    }
    if args.follow {
        follow_file_tail(&paths.log_file, args.lines)?;
    } else {
        print_file_tail(&paths.log_file, args.lines)?;
    }
    Ok(())
}

async fn doctor() -> Result<ExitCode, LifecycleError> {
    let status = collect_status().await;
    let findings = doctor_findings(&status);
    if findings.is_empty() {
        println!("daemon doctor: no issues found");
        return Ok(ExitCode::SUCCESS);
    }
    println!("daemon doctor findings:");
    for finding in &findings {
        println!("- {finding}");
    }
    Ok(if status.running {
        ExitCode::from(1)
    } else {
        ExitCode::from(2)
    })
}

/// Reject daemon supervision commands from daemon-launched agents.
pub fn ensure_operator_supervision() -> Result<(), LifecycleError> {
    if ambient_supervision_identity().is_some() {
        return Err(LifecycleError::OperatorOnly);
    }
    Ok(())
}

fn ensure_operator_supervision_for(verb: &str) -> Result<(), LifecycleError> {
    if let Some(identity) = ambient_supervision_identity() {
        let paths = DaemonPaths::resolve();
        let label = format_ambient_identity(identity);
        if let Err(error) = fs::create_dir_all(&paths.home)
            .and_then(|_| write_shutdown_attribution_with_identity(&paths, verb, "refused", &label))
        {
            eprintln!("{OPERATOR_ONLY}: failed to record refused {verb}: {error}");
        }
        eprintln!("{OPERATOR_ONLY}: refused {verb} from {label}");
        return Err(LifecycleError::OperatorOnly);
    }
    Ok(())
}

/// Return the ambient agent identity variables that make supervision illegal.
pub fn ambient_supervision_identity() -> Option<Vec<(&'static str, String)>> {
    let mut found = Vec::new();
    for key in ["NEXUS_CLIENT_KEY", "NEXUS_SESSION_ID", "NEXUS_NAME"] {
        if let Ok(value) = env::var(key) {
            if !value.is_empty() {
                found.push((key, value));
            }
        }
    }
    (!found.is_empty()).then_some(found)
}

/// Log shutdown attribution prepared by the CLI tripwire path.
pub fn read_shutdown_attribution(paths: &DaemonPaths) -> Option<String> {
    let body = fs::read_to_string(&paths.shutdown_attribution_file).ok()?;
    let _ = fs::remove_file(&paths.shutdown_attribution_file);
    Some(body)
}

fn write_shutdown_attribution(paths: &DaemonPaths, verb: &str) -> io::Result<()> {
    let identity = ambient_supervision_identity()
        .map(format_ambient_identity)
        .unwrap_or_else(|| "operator-terminal".to_string());
    write_shutdown_attribution_with_identity(paths, verb, "requested", &identity)
}

fn write_shutdown_attribution_with_identity(
    paths: &DaemonPaths,
    verb: &str,
    outcome: &str,
    identity: &str,
) -> io::Result<()> {
    let uid = current_uid_label();
    let body = format!(
        "{{\"verb\":\"{}\",\"outcome\":\"{}\",\"uid\":\"{}\",\"identity\":\"{}\",\"pid\":{}}}\n",
        escape_json(verb),
        escape_json(outcome),
        escape_json(&uid),
        escape_json(&identity),
        std::process::id()
    );
    fs::write(&paths.shutdown_attribution_file, body)
}

fn format_ambient_identity(vars: Vec<(&'static str, String)>) -> String {
    vars.into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(",")
}

/// Lifecycle cleanup run by the foreground daemon after its shutdown signal fires.
pub async fn reap_claimed_command_intents(store: &Store) -> Result<u64, NexusError> {
    CommandIntents::new(store)
        .reap_claimed_for_shutdown(now())
        .await
}

/// Flush the daemon-owned embedded store's WAL during explicit lifecycle shutdown.
pub async fn checkpoint_wal(store: &Store) -> Result<(), NexusError> {
    let mut rows = store
        .conn
        .query("PRAGMA wal_checkpoint(TRUNCATE)", ())
        .await
        .map_err(nexus_store_error)?;
    while rows.next().await.map_err(nexus_store_error)?.is_some() {}
    Ok(())
}

fn nexus_store_error(error: libsql::Error) -> NexusError {
    NexusError::Store(error.to_string())
}

/// A supervisor backend. Native implementations wrap the OS supervisor; the fallback self-starts a
/// detached daemon process and is explicit about having no crash restart.
trait Supervisor {
    fn kind_label(&self) -> &'static str;
    fn installed(&self) -> bool;
    fn install(&self) -> Result<(), LifecycleError>;
    fn uninstall(&self) -> Result<(), LifecycleError>;
    fn start(&self) -> Result<(), LifecycleError>;
    fn stop(&self, force: bool) -> Result<(), LifecycleError>;
    fn restart(&self, force: bool) -> Result<(), LifecycleError> {
        self.stop(force)?;
        wait_until_down(&self.paths().pid_file, FORCE_STOP_GRACE);
        self.start()
    }
    fn logs_command(&self, _lines: usize, _follow: bool) -> Option<CommandSpec> {
        None
    }
    fn paths(&self) -> &DaemonPaths;
}

#[derive(Debug, Clone)]
struct SelfDaemonSupervisor {
    paths: DaemonPaths,
    binary: PathBuf,
}

impl Supervisor for SelfDaemonSupervisor {
    fn kind_label(&self) -> &'static str {
        "self (no crash restart)"
    }

    fn installed(&self) -> bool {
        false
    }

    fn install(&self) -> Result<(), LifecycleError> {
        Err(LifecycleError::Command(
            "self-daemonize fallback has no installable service".into(),
        ))
    }

    fn uninstall(&self) -> Result<(), LifecycleError> {
        Ok(())
    }

    fn start(&self) -> Result<(), LifecycleError> {
        if let Some(pid) = read_pid(&self.paths.pid_file)? {
            if process_alive(pid) {
                return Err(LifecycleError::Command(format!(
                    "nexus daemon already running (pid {pid})"
                )));
            }
        }
        fs::create_dir_all(&self.paths.home)?;
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.paths.log_file)?;
        let err = log.try_clone()?;
        let mut command = Command::new(&self.binary);
        command
            .arg("daemon")
            .arg("run")
            .env("NEXUS_HOME", &self.paths.home)
            .stdin(Stdio::null())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(err));
        detach_command(&mut command);
        let child = command.spawn()?;
        wait_for_pidfile(&self.paths.pid_file, child.id(), Duration::from_secs(3));
        Ok(())
    }

    fn stop(&self, force: bool) -> Result<(), LifecycleError> {
        let Some(pid) = read_pid(&self.paths.pid_file)? else {
            return Ok(());
        };
        if !process_alive(pid) {
            return Ok(());
        }
        signal_process(pid, false)?;
        let grace = if force {
            FORCE_STOP_GRACE
        } else {
            DEFAULT_STOP_GRACE
        };
        if !wait_pid_down(pid, grace) && force {
            signal_process(pid, true)?;
        }
        Ok(())
    }

    fn logs_command(&self, _lines: usize, _follow: bool) -> Option<CommandSpec> {
        None
    }

    fn paths(&self) -> &DaemonPaths {
        &self.paths
    }
}

#[cfg(target_os = "linux")]
#[derive(Debug, Clone)]
struct SystemdUserSupervisor {
    paths: DaemonPaths,
    binary: PathBuf,
}

#[cfg(target_os = "linux")]
impl SystemdUserSupervisor {
    fn unit_path(&self) -> PathBuf {
        env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(expand_tilde("~/.config")))
            .join(format!("systemd/user/{SYSTEMD_SERVICE}"))
    }

    fn sqld_unit_path(&self) -> PathBuf {
        env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(expand_tilde("~/.config")))
            .join(format!("systemd/user/{LEGACY_SYSTEMD_SQLD_SERVICE}"))
    }
}

#[cfg(target_os = "linux")]
impl Supervisor for SystemdUserSupervisor {
    fn kind_label(&self) -> &'static str {
        "systemd"
    }

    fn installed(&self) -> bool {
        self.unit_path().is_file()
    }

    fn install(&self) -> Result<(), LifecycleError> {
        let unit = self.unit_path();
        if let Some(parent) = unit.parent() {
            fs::create_dir_all(parent)?;
        }
        // Remove the obsolete companion service during an upgrade so an old sqld process cannot
        // keep running beside the daemon-owned embedded file.
        let _ = CommandSpec::new("systemctl", ["--user", "stop", LEGACY_SYSTEMD_SQLD_SERVICE])
            .run_capture();
        match fs::remove_file(self.sqld_unit_path()) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        fs::write(&unit, systemd_unit(&self.binary, &self.paths))?;
        CommandSpec::new("systemctl", ["--user", "daemon-reload"]).run_checked()?;
        CommandSpec::new("systemctl", ["--user", "enable", SYSTEMD_SERVICE]).run_checked()?;
        println!("note: run `loginctl enable-linger $USER` if the daemon should survive logout");
        Ok(())
    }

    fn uninstall(&self) -> Result<(), LifecycleError> {
        let _ = CommandSpec::new("systemctl", ["--user", "stop", SYSTEMD_SERVICE]).run_capture();
        let _ = CommandSpec::new("systemctl", ["--user", "disable", SYSTEMD_SERVICE]).run_capture();
        let _ = CommandSpec::new("systemctl", ["--user", "stop", LEGACY_SYSTEMD_SQLD_SERVICE])
            .run_capture();
        match fs::remove_file(self.unit_path()) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        match fs::remove_file(self.sqld_unit_path()) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let _ = CommandSpec::new("systemctl", ["--user", "daemon-reload"]).run_capture();
        Ok(())
    }

    fn start(&self) -> Result<(), LifecycleError> {
        CommandSpec::new("systemctl", ["--user", "start", SYSTEMD_SERVICE]).run_checked()
    }

    fn stop(&self, force: bool) -> Result<(), LifecycleError> {
        CommandSpec::new("systemctl", ["--user", "stop", SYSTEMD_SERVICE]).run_checked()?;
        if force && read_pid(&self.paths.pid_file)?.is_some_and(process_alive) {
            CommandSpec::new(
                "systemctl",
                ["--user", "kill", "-s", "SIGKILL", SYSTEMD_SERVICE],
            )
            .run_checked()?;
        }
        Ok(())
    }

    fn restart(&self, _force: bool) -> Result<(), LifecycleError> {
        CommandSpec::new("systemctl", ["--user", "restart", SYSTEMD_SERVICE]).run_checked()
    }

    fn logs_command(&self, lines: usize, follow: bool) -> Option<CommandSpec> {
        let mut args = vec![
            "--user".to_string(),
            "-u".to_string(),
            SYSTEMD_SERVICE.to_string(),
            "-n".to_string(),
            lines.to_string(),
        ];
        if follow {
            args.push("-f".to_string());
        }
        Some(CommandSpec::new_owned("journalctl", args))
    }

    fn paths(&self) -> &DaemonPaths {
        &self.paths
    }
}

#[cfg(target_os = "macos")]
#[derive(Debug, Clone)]
struct LaunchdSupervisor {
    paths: DaemonPaths,
    binary: PathBuf,
}

#[cfg(target_os = "macos")]
impl LaunchdSupervisor {
    fn plist_path(&self) -> PathBuf {
        PathBuf::from(expand_tilde(
            "~/Library/LaunchAgents/io.egregore.nexus.daemon.plist",
        ))
    }
}

#[cfg(target_os = "macos")]
impl Supervisor for LaunchdSupervisor {
    fn kind_label(&self) -> &'static str {
        "launchd"
    }

    fn installed(&self) -> bool {
        self.plist_path().is_file()
    }

    fn install(&self) -> Result<(), LifecycleError> {
        let path = self.plist_path();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let _ = CommandSpec::new_owned(
            "launchctl",
            vec![
                "bootout".into(),
                format!("gui/{}/{LAUNCHD_LABEL}", current_uid_label()),
            ],
        )
        .run_capture();
        fs::write(
            &path,
            launchd_service_plist(&self.binary, &self.paths.home, &self.paths.log_file),
        )?;
        Ok(())
    }

    fn uninstall(&self) -> Result<(), LifecycleError> {
        let _ = self.stop(false);
        match fs::remove_file(self.plist_path()) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn start(&self) -> Result<(), LifecycleError> {
        CommandSpec::new_owned(
            "launchctl",
            vec![
                "bootstrap".into(),
                format!("gui/{}", current_uid_label()),
                self.plist_path().display().to_string(),
            ],
        )
        .run_checked()
    }

    fn stop(&self, _force: bool) -> Result<(), LifecycleError> {
        CommandSpec::new_owned(
            "launchctl",
            vec![
                "bootout".into(),
                format!("gui/{}/{LAUNCHD_LABEL}", current_uid_label()),
            ],
        )
        .run_checked()
    }

    fn logs_command(&self, lines: usize, follow: bool) -> Option<CommandSpec> {
        let mut args = vec![
            "show".into(),
            "--style".into(),
            "compact".into(),
            "--last".into(),
            format!("{}m", std::cmp::max(1, lines / 10)),
            "--predicate".into(),
            "process == \"nexus\"".into(),
        ];
        if follow {
            args.push("--info".into());
        }
        Some(CommandSpec::new_owned("log", args))
    }

    fn paths(&self) -> &DaemonPaths {
        &self.paths
    }
}

#[cfg(target_os = "windows")]
#[derive(Debug, Clone)]
struct WindowsTaskSupervisor {
    paths: DaemonPaths,
    binary: PathBuf,
}

#[cfg(target_os = "windows")]
impl Supervisor for WindowsTaskSupervisor {
    fn kind_label(&self) -> &'static str {
        "task-scheduler"
    }

    fn installed(&self) -> bool {
        CommandSpec::new("schtasks.exe", ["/Query", "/TN", WINDOWS_TASK_NAME])
            .run_capture()
            .map(|out| out.success)
            .unwrap_or(false)
    }

    fn install(&self) -> Result<(), LifecycleError> {
        CommandSpec::new_owned(
            "powershell.exe",
            vec![
                "-NoProfile".into(),
                "-NonInteractive".into(),
                "-ExecutionPolicy".into(),
                "Bypass".into(),
                "-Command".into(),
                windows_task_registration_script(&self.binary, &self.paths.home),
            ],
        )
        .run_checked()
    }

    fn uninstall(&self) -> Result<(), LifecycleError> {
        if !self.installed() {
            let _ = fs::remove_file(self.paths.home.join("daemon-task.ps1"));
            return Ok(());
        }
        let _ = CommandSpec::new("schtasks.exe", ["/End", "/TN", WINDOWS_TASK_NAME]).run_capture();
        CommandSpec::new("schtasks.exe", ["/Delete", "/TN", WINDOWS_TASK_NAME, "/F"])
            .run_checked()?;
        match fs::remove_file(self.paths.home.join("daemon-task.ps1")) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn start(&self) -> Result<(), LifecycleError> {
        CommandSpec::new("schtasks.exe", ["/Run", "/TN", WINDOWS_TASK_NAME]).run_checked()
    }

    fn stop(&self, force: bool) -> Result<(), LifecycleError> {
        CommandSpec::new("schtasks.exe", ["/End", "/TN", WINDOWS_TASK_NAME]).run_checked()?;
        if force {
            if let Some(pid) = read_pid(&self.paths.pid_file)? {
                CommandSpec::new_owned(
                    "taskkill.exe",
                    vec!["/PID".into(), pid.to_string(), "/F".into()],
                )
                .run_checked()?;
            }
        }
        Ok(())
    }

    fn paths(&self) -> &DaemonPaths {
        &self.paths
    }
}

fn select_control_supervisor(paths: &DaemonPaths, binary: PathBuf) -> Box<dyn Supervisor> {
    let platform = select_platform_supervisor(paths, binary.clone());
    if platform.installed() {
        platform
    } else {
        Box::new(SelfDaemonSupervisor {
            paths: paths.clone(),
            binary,
        })
    }
}

fn select_platform_supervisor(paths: &DaemonPaths, binary: PathBuf) -> Box<dyn Supervisor> {
    #[cfg(target_os = "linux")]
    {
        return Box::new(SystemdUserSupervisor {
            paths: paths.clone(),
            binary,
        });
    }
    #[cfg(target_os = "macos")]
    {
        return Box::new(LaunchdSupervisor {
            paths: paths.clone(),
            binary,
        });
    }
    #[cfg(target_os = "windows")]
    {
        return Box::new(WindowsTaskSupervisor {
            paths: paths.clone(),
            binary,
        });
    }
    #[allow(unreachable_code)]
    Box::new(SelfDaemonSupervisor {
        paths: paths.clone(),
        binary,
    })
}

#[derive(Debug, Clone)]
struct CommandSpec {
    program: OsString,
    args: Vec<OsString>,
}

#[derive(Debug, Clone)]
struct CommandOutput {
    success: bool,
    stdout: String,
    stderr: String,
}

impl CommandSpec {
    fn new<I, S>(program: &str, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Self {
            program: OsString::from(program),
            args: args
                .into_iter()
                .map(|arg| OsString::from(arg.as_ref()))
                .collect(),
        }
    }

    fn new_owned(program: &str, args: Vec<String>) -> Self {
        Self {
            program: OsString::from(program),
            args: args.into_iter().map(OsString::from).collect(),
        }
    }

    fn run_capture(&self) -> Result<CommandOutput, LifecycleError> {
        let output = Command::new(&self.program).args(&self.args).output()?;
        Ok(CommandOutput {
            success: output.status.success(),
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
        })
    }

    fn run_checked(&self) -> Result<(), LifecycleError> {
        let output = self.run_capture()?;
        if output.success {
            return Ok(());
        }
        Err(LifecycleError::Command(format!(
            "{} failed: {}{}",
            self.display(),
            output.stderr,
            output.stdout
        )))
    }

    fn run_inherit(&self) -> Result<CommandOutput, LifecycleError> {
        let status = Command::new(&self.program)
            .args(&self.args)
            .stdin(Stdio::null())
            .status()?;
        Ok(CommandOutput {
            success: status.success(),
            stdout: String::new(),
            stderr: String::new(),
        })
    }

    fn display(&self) -> String {
        let mut parts = vec![self.program.to_string_lossy().to_string()];
        parts.extend(
            self.args
                .iter()
                .map(|arg| arg.to_string_lossy().to_string()),
        );
        parts.join(" ")
    }
}

/// Collected `nexus daemon status` state.
#[derive(Debug, Clone)]
pub struct DaemonStatus {
    pub paths: DaemonPaths,
    pub supervisor: String,
    pub running: bool,
    pub pid: Option<u32>,
    pub uptime_hint: Option<String>,
    pub restart_pending: bool,
    pub binary_current: Option<String>,
    pub binary_running: Option<String>,
    pub gateway: GatewayStatus,
    pub lane_depths: Vec<nexus_store::repos::CommandIntentDepth>,
    pub wedged_intents: i64,
    pub dead_letters: DeadLetterStatus,
    pub transport_pairs: Vec<TransportPairStatus>,
    pub store_error: Option<String>,
}

impl DaemonStatus {
    fn exit_code(&self) -> ExitCode {
        if !self.running {
            return ExitCode::from(2);
        }
        if self.restart_pending
            || !self.gateway.ok()
            || self.wedged_intents > 0
            || self.store_error.is_some()
        {
            ExitCode::from(1)
        } else {
            ExitCode::SUCCESS
        }
    }
}

/// Discovery-file liveness for the gateway.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GatewayStatus {
    Missing,
    Live {
        pid: Option<u32>,
        url: Option<String>,
    },
    Stale {
        pid: Option<u32>,
        url: Option<String>,
    },
    Invalid(String),
}

impl GatewayStatus {
    fn ok(&self) -> bool {
        matches!(self, GatewayStatus::Missing | GatewayStatus::Live { .. })
    }
}

/// Per-online-session transport count shown by status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportPairStatus {
    pub session: String,
    pub name: String,
    pub transport: String,
    pub count: usize,
}

/// Dead-letter count shown by status. This is visibility only and does not change status exit code.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeadLetterStatus {
    pub count: u64,
    pub oldest_age_hint: Option<String>,
}

async fn collect_status() -> DaemonStatus {
    let paths = DaemonPaths::resolve();
    let supervisor = select_control_supervisor(&paths, current_exe().unwrap_or_default());
    let pid = read_pid(&paths.pid_file).ok().flatten();
    let running = pid.is_some_and(process_alive);
    let gateway = gateway_status(&paths.gateway_file);
    let (lane_depths, wedged_intents, dead_letters, transport_pairs, store_error) =
        collect_store_status().await;
    let (binary_current, binary_running, restart_pending) = binary_status(pid);
    let uptime_hint = pid.and_then(|_| file_age_hint(&paths.pid_file));
    DaemonStatus {
        paths,
        supervisor: supervisor.kind_label().to_string(),
        running,
        pid,
        uptime_hint,
        restart_pending,
        binary_current,
        binary_running,
        gateway,
        lane_depths,
        wedged_intents,
        dead_letters,
        transport_pairs,
        store_error,
    }
}

async fn collect_store_status() -> (
    Vec<nexus_store::repos::CommandIntentDepth>,
    i64,
    DeadLetterStatus,
    Vec<TransportPairStatus>,
    Option<String>,
) {
    let response = crate::daemon::daemon_ipc::call_daemon_ipc(
        &nexus_home(),
        DaemonIpcRequest {
            version: DAEMON_IPC_PROTOCOL_VERSION,
            token: String::new(),
            request_id: format!("daemon-status-{}", uuid::Uuid::new_v4()),
            caller: Some(DaemonIpcCaller {
                name: Some(crate::local_operator::display_name()),
                project: env::var("NEXUS_PROJECT")
                    .ok()
                    .filter(|value| !value.is_empty())
                    .unwrap_or_else(|| "default".into()),
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
                method: "local.daemon.storeStatus".into(),
                params: serde_json::Value::Null,
            },
        },
        Duration::from_secs(2),
    )
    .await;
    let response = match response {
        Ok(response) => response,
        Err(error) => {
            return (
                Vec::new(),
                0,
                DeadLetterStatus::default(),
                Vec::new(),
                Some(format!("daemon IPC unavailable: {error}")),
            )
        }
    };
    if let Some(error) = response.error {
        return (
            Vec::new(),
            0,
            DeadLetterStatus::default(),
            Vec::new(),
            Some(error.message),
        );
    }
    let snapshot: crate::daemon::daemon_ipc::DaemonStoreStatusSnapshot =
        match serde_json::from_value(response.result.unwrap_or(serde_json::Value::Null)) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                return (
                    Vec::new(),
                    0,
                    DeadLetterStatus::default(),
                    Vec::new(),
                    Some(format!("invalid daemon status response: {error}")),
                )
            }
        };
    (
        snapshot
            .lane_depths
            .into_iter()
            .map(|depth| nexus_store::repos::CommandIntentDepth {
                kind: depth.kind,
                pending: depth.pending,
                claimed: depth.claimed,
            })
            .collect(),
        snapshot.wedged_intents,
        DeadLetterStatus {
            count: snapshot.dead_letter_count,
            oldest_age_hint: snapshot
                .dead_letter_oldest_created_at
                .and_then(dead_letter_age_hint),
        },
        snapshot
            .transport_pairs
            .into_iter()
            .map(|pair| TransportPairStatus {
                session: pair.session,
                name: pair.name,
                transport: pair.transport,
                count: pair.count,
            })
            .collect(),
        None,
    )
}

fn print_status(status: &DaemonStatus) {
    println!(
        "daemon: {}",
        if status.running { "running" } else { "down" }
    );
    println!("supervisor: {}", status.supervisor);
    println!(
        "pid: {}",
        status
            .pid
            .map(|pid| pid.to_string())
            .unwrap_or_else(|| "-".into())
    );
    if let Some(uptime) = &status.uptime_hint {
        println!("uptime: {uptime}");
    }
    println!(
        "binary: current={} running={}{}",
        status.binary_current.as_deref().unwrap_or("unknown"),
        status.binary_running.as_deref().unwrap_or("unknown"),
        if status.restart_pending {
            " RESTART PENDING"
        } else {
            ""
        }
    );
    println!("gateway: {}", gateway_label(&status.gateway));
    if let Some(error) = &status.store_error {
        println!("store: degraded ({error})");
    }
    println!("wedged_intents: {}", status.wedged_intents);
    match &status.dead_letters.oldest_age_hint {
        Some(age) if status.dead_letters.count > 0 => {
            println!(
                "dead_letters: {} (oldest: {age})",
                status.dead_letters.count
            )
        }
        _ => println!("dead_letters: {}", status.dead_letters.count),
    }
    println!("lanes:");
    if status.lane_depths.is_empty() {
        println!("  none");
    } else {
        for depth in &status.lane_depths {
            println!(
                "  {} pending={} claimed={}",
                depth.kind, depth.pending, depth.claimed
            );
        }
    }
    println!("transport_pairs:");
    if status.transport_pairs.is_empty() {
        println!("  none");
    } else {
        for pair in &status.transport_pairs {
            println!(
                "  {} {} transport={} count={}",
                pair.session, pair.name, pair.transport, pair.count
            );
        }
    }
}

fn doctor_findings(status: &DaemonStatus) -> Vec<String> {
    let mut findings = Vec::new();
    if !status.running {
        findings.push("daemon is down; run `nexus daemon start`".into());
    }
    if status.restart_pending {
        findings.push("running daemon binary differs from the installed/current binary; run `nexus daemon restart`".into());
    }
    match &status.gateway {
        GatewayStatus::Stale { .. } => {
            findings.push("gateway discovery file points at a dead pid; restart the daemon".into())
        }
        GatewayStatus::Invalid(error) => {
            findings.push(format!("gateway discovery invalid: {error}"))
        }
        _ => {}
    }
    if status.wedged_intents > 0 {
        findings.push(format!(
            "{} claimed command intents have expired leases",
            status.wedged_intents
        ));
    }
    if let Some(error) = &status.store_error {
        findings.push(format!("store status unavailable: {error}"));
    }
    for bin in ["node", "tmux", "codex", "opencode", "claude"] {
        if !path_has_executable(bin) {
            findings.push(format!("PATH does not resolve `{bin}`"));
        }
    }
    findings
}

fn gateway_label(status: &GatewayStatus) -> String {
    match status {
        GatewayStatus::Missing => "missing".into(),
        GatewayStatus::Live { pid, url } => format!(
            "live pid={} url={}",
            pid.map(|p| p.to_string())
                .unwrap_or_else(|| "unknown".into()),
            url.as_deref().unwrap_or("unknown")
        ),
        GatewayStatus::Stale { pid, url } => format!(
            "stale pid={} url={}",
            pid.map(|p| p.to_string())
                .unwrap_or_else(|| "unknown".into()),
            url.as_deref().unwrap_or("unknown")
        ),
        GatewayStatus::Invalid(error) => format!("invalid ({error})"),
    }
}

fn gateway_status(path: &Path) -> GatewayStatus {
    let body = match fs::read_to_string(path) {
        Ok(body) => body,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return GatewayStatus::Missing,
        Err(error) => return GatewayStatus::Invalid(error.to_string()),
    };
    let value: serde_json::Value = match serde_json::from_str(&body) {
        Ok(value) => value,
        Err(error) => return GatewayStatus::Invalid(error.to_string()),
    };
    let pid = value
        .get("pid")
        .and_then(|p| p.as_u64())
        .and_then(|p| u32::try_from(p).ok());
    let url = value
        .get("url")
        .and_then(|u| u.as_str())
        .map(str::to_string);
    if pid.is_some_and(process_alive) {
        GatewayStatus::Live { pid, url }
    } else {
        GatewayStatus::Stale { pid, url }
    }
}

fn binary_status(pid: Option<u32>) -> (Option<String>, Option<String>, bool) {
    let current_path = current_exe().ok();

    #[cfg(target_os = "linux")]
    if let (Some(current_path), Some(pid)) = (current_path.as_deref(), pid) {
        let running_path = PathBuf::from(format!("/proc/{pid}/exe"));
        if same_binary_file(current_path, &running_path) {
            let identity = binary_file_identity(current_path);
            return (identity.clone(), identity, false);
        }
    }

    let current = current_path
        .as_deref()
        .and_then(|path| sha256_file(path).ok());
    let running = pid.and_then(running_binary_hash);
    let restart_pending = current.is_some() && running.is_some() && current != running;
    (current, running, restart_pending)
}

#[cfg(target_os = "linux")]
fn same_binary_file(left: &Path, right: &Path) -> bool {
    use std::os::unix::fs::MetadataExt as _;

    let Ok(left) = fs::metadata(left) else {
        return false;
    };
    let Ok(right) = fs::metadata(right) else {
        return false;
    };
    left.dev() == right.dev() && left.ino() == right.ino()
}

#[cfg(target_os = "linux")]
fn binary_file_identity(path: &Path) -> Option<String> {
    use std::os::unix::fs::MetadataExt as _;

    let metadata = fs::metadata(path).ok()?;
    Some(format!(
        "file:{:x}:{:x}:{:x}:{:x}:{:x}",
        metadata.dev(),
        metadata.ino(),
        metadata.size(),
        metadata.mtime(),
        metadata.mtime_nsec()
    ))
}

fn running_binary_hash(pid: u32) -> Option<String> {
    #[cfg(target_os = "linux")]
    {
        sha256_file(Path::new(&format!("/proc/{pid}/exe"))).ok()
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = pid;
        None
    }
}

#[cfg(not(target_os = "linux"))]
fn running_binary_path(pid: u32) -> Option<PathBuf> {
    let _ = pid;
    None
}

fn sha256_file(path: &Path) -> io::Result<String> {
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn current_exe() -> io::Result<PathBuf> {
    env::current_exe()
}

fn file_age_hint(path: &Path) -> Option<String> {
    let modified = fs::metadata(path).ok()?.modified().ok()?;
    let elapsed = SystemTime::now().duration_since(modified).ok()?;
    Some(format_duration(elapsed))
}

fn dead_letter_age_hint(created_at_ms: i64) -> Option<String> {
    let age_ms = now().checked_sub(created_at_ms)?;
    Some(format_duration(Duration::from_millis(age_ms as u64)))
}

fn format_duration(duration: Duration) -> String {
    let secs = duration.as_secs();
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m{}s", secs / 60, secs % 60)
    } else {
        format!("{}h{}m", secs / 3600, (secs % 3600) / 60)
    }
}

fn print_file_tail(path: &Path, lines: usize) -> io::Result<()> {
    let body = match fs::read_to_string(path) {
        Ok(body) => body,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    let all = body.lines().collect::<Vec<_>>();
    let start = all.len().saturating_sub(lines);
    for line in &all[start..] {
        println!("{line}");
    }
    Ok(())
}

fn follow_file_tail(path: &Path, lines: usize) -> io::Result<()> {
    print_file_tail(path, lines)?;
    let mut printed = fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    loop {
        thread::sleep(Duration::from_millis(500));
        let Ok(mut file) = File::open(path) else {
            continue;
        };
        let len = file.metadata()?.len();
        if len < printed {
            printed = 0;
        }
        if len == printed {
            continue;
        }
        use std::io::Seek;
        file.seek(io::SeekFrom::Start(printed))?;
        let mut chunk = String::new();
        file.read_to_string(&mut chunk)?;
        print!("{chunk}");
        let _ = io::stdout().flush();
        printed = len;
    }
}

fn read_pid(path: &Path) -> io::Result<Option<u32>> {
    let body = match fs::read_to_string(path) {
        Ok(body) => body,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    Ok(body.split_whitespace().next().and_then(|s| s.parse().ok()))
}

fn wait_for_pidfile(path: &Path, fallback_pid: u32, timeout: Duration) {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if read_pid(path).ok().flatten().is_some_and(process_alive) {
            return;
        }
        thread::sleep(Duration::from_millis(100));
    }
    let _ = fs::write(path, format!("{fallback_pid}\n"));
}

fn wait_until_down(path: &Path, timeout: Duration) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if !read_pid(path).ok().flatten().is_some_and(process_alive) {
            return true;
        }
        thread::sleep(Duration::from_millis(100));
    }
    false
}

fn wait_until_running(path: &Path, timeout: Duration) -> bool {
    let start = std::time::Instant::now();
    let mut stable_since = None;
    while start.elapsed() < timeout {
        if read_pid(path).ok().flatten().is_some_and(process_alive) {
            let stable_since = stable_since.get_or_insert_with(std::time::Instant::now);
            if stable_since.elapsed() >= SERVICE_STABILITY_GRACE {
                return true;
            }
        } else {
            stable_since = None;
        }
        thread::sleep(Duration::from_millis(100));
    }
    false
}

fn wait_pid_down(pid: u32, timeout: Duration) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if !process_alive(pid) {
            return true;
        }
        thread::sleep(Duration::from_millis(100));
    }
    false
}

fn process_alive(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    #[cfg(unix)]
    {
        #[cfg(target_os = "linux")]
        if let Ok(stat) = fs::read_to_string(format!("/proc/{pid}/stat")) {
            if let Some(alive) = linux_proc_stat_is_alive(&stat) {
                return alive;
            }
        }
        Command::new("kill")
            .arg("-0")
            .arg(pid.to_string())
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }
    #[cfg(windows)]
    {
        CommandSpec::new_owned("tasklist.exe", vec!["/FI".into(), format!("PID eq {pid}")])
            .run_capture()
            .map(|out| out.success && out.stdout.contains(&pid.to_string()))
            .unwrap_or(false)
    }
}

/// Parse Linux `/proc/<pid>/stat` liveness without being confused by spaces in the process name.
///
/// `kill -0` succeeds for an unreaped zombie, but such a process cannot own a daemon transport or
/// service commands. Linux state `Z` (zombie) and `X` (dead) are therefore terminal; every other
/// valid state remains live. `None` lets callers fall back to the portable signal probe when the
/// proc record is malformed or unavailable.
pub fn linux_proc_stat_is_alive(stat: &str) -> Option<bool> {
    let (_, fields) = stat.rsplit_once(") ")?;
    let state = fields.split_whitespace().next()?.chars().next()?;
    Some(!matches!(state, 'Z' | 'X'))
}

fn signal_process(pid: u32, kill: bool) -> io::Result<()> {
    #[cfg(unix)]
    {
        let signal = if kill { "-KILL" } else { "-TERM" };
        let status = Command::new("kill")
            .arg(signal)
            .arg(pid.to_string())
            .status()?;
        if status.success() {
            Ok(())
        } else {
            Err(io::Error::other(format!("kill {signal} {pid} failed")))
        }
    }
    #[cfg(windows)]
    {
        let mut args = vec!["/PID".to_string(), pid.to_string()];
        if kill {
            args.push("/F".into());
        }
        let status = Command::new("taskkill.exe").args(args).status()?;
        if status.success() {
            Ok(())
        } else {
            Err(io::Error::other(format!("taskkill {pid} failed")))
        }
    }
}

fn detach_command(command: &mut Command) {
    #[cfg(unix)]
    unsafe {
        use std::os::unix::process::CommandExt;
        command.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
        const DETACHED_PROCESS: u32 = 0x0000_0008;
        command.creation_flags(CREATE_NEW_PROCESS_GROUP | DETACHED_PROCESS);
    }
}

/// Build the per-user Windows Task Scheduler registration script.
///
/// The task owns a small PowerShell wrapper so a non-default `NEXUS_HOME` survives logon and the
/// scheduler can observe the foreground daemon's exit status for restart-on-failure.
#[doc(hidden)]
pub fn windows_task_registration_script(binary: &Path, nexus_home: &Path) -> String {
    let wrapper_path =
        powershell_single_quote(&nexus_home.join("daemon-task.ps1").display().to_string());
    let binary = powershell_single_quote(&binary.display().to_string());
    let nexus_home = powershell_single_quote(&nexus_home.display().to_string());
    format!(
        "$ErrorActionPreference = 'Stop'\n\
$wrapperPath = '{wrapper_path}'\n\
$wrapper = @'\n\
$env:NEXUS_HOME = '{nexus_home}'\n\
& '{binary}' daemon run\n\
exit $LASTEXITCODE\n\
'@\n\
Set-Content -LiteralPath $wrapperPath -Value $wrapper -Encoding UTF8\n\
$identity = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name\n\
$actionArgs = '-NoProfile -NonInteractive -ExecutionPolicy Bypass -File \"' + $wrapperPath + '\"'\n\
$action = New-ScheduledTaskAction -Execute 'powershell.exe' -Argument $actionArgs\n\
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $identity\n\
$settings = New-ScheduledTaskSettingsSet -RestartCount 999 -RestartInterval (New-TimeSpan -Seconds 5) -StartWhenAvailable -ExecutionTimeLimit ([TimeSpan]::Zero)\n\
$principal = New-ScheduledTaskPrincipal -UserId $identity -LogonType Interactive -RunLevel Limited\n\
Register-ScheduledTask -TaskName '{WINDOWS_TASK_NAME}' -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Force | Out-Null\n"
    )
}

fn powershell_single_quote(value: &str) -> String {
    value.replace('\'', "''")
}

#[cfg(target_os = "linux")]
fn systemd_unit(binary: &Path, paths: &DaemonPaths) -> String {
    format!(
        "[Unit]\nDescription=Nexus daemon\n\n[Service]\nType=simple\nExecStart={} daemon run\nRestart=on-failure\nEnvironment=NEXUS_HOME={}\nWorkingDirectory={}\n\n[Install]\nWantedBy=default.target\n",
        binary.display(),
        paths.home.display(),
        env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .display()
    )
}

/// Build the per-user macOS LaunchAgent property list.
#[doc(hidden)]
pub fn launchd_service_plist(binary: &Path, nexus_home: &Path, log_file: &Path) -> String {
    let binary = escape_xml_text(&binary.display().to_string());
    let nexus_home = escape_xml_text(&nexus_home.display().to_string());
    let log_file = escape_xml_text(&log_file.display().to_string());
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>{LAUNCHD_LABEL}</string>
  <key>ProgramArguments</key>
  <array><string>{}</string><string>daemon</string><string>run</string></array>
  <key>EnvironmentVariables</key><dict><key>NEXUS_HOME</key><string>{}</string></dict>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><dict><key>SuccessfulExit</key><false/></dict>
  <key>StandardOutPath</key><string>{}</string>
  <key>StandardErrorPath</key><string>{}</string>
</dict>
</plist>
"#,
        binary, nexus_home, log_file, log_file
    )
}

fn escape_xml_text(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn path_has_executable(bin: &str) -> bool {
    let Some(path) = env::var_os("PATH") else {
        return false;
    };
    env::split_paths(&path).any(|dir| {
        let candidate = dir.join(bin);
        if candidate.is_file() {
            return true;
        }
        #[cfg(windows)]
        {
            return dir.join(format!("{bin}.exe")).is_file();
        }
        #[allow(unreachable_code)]
        false
    })
}

fn current_uid_label() -> String {
    #[cfg(unix)]
    {
        unsafe { libc::geteuid().to_string() }
    }
    #[cfg(windows)]
    {
        env::var("USERNAME").unwrap_or_else(|_| "unknown".into())
    }
}

fn escape_json(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(test)]
#[path = "../../tests/unit/daemon_lifecycle.rs"]
mod daemon_lifecycle_contracts;
