//! Lifecycle ownership for the independently installed Nexus REST/WebSocket gateway.

use std::env;
use std::ffi::{OsStr, OsString};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::daemon::lifecycle::{self, DaemonPaths};

pub const GATEWAY_INSTALL_HINT: &str = "Install it with npm install -g @egregore/nexus-gateway";
const GATEWAY_READY_TIMEOUT: Duration = Duration::from_secs(60);
const GATEWAY_HEALTH_IO_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayInstallation {
    pub executable: PathBuf,
    pub invocation: GatewayInvocation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayInvocation {
    Direct,
    WindowsCommandShim,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum GatewayRuntimeStatus {
    Live {
        pid: u32,
        url: String,
    },
    Degraded {
        reason: String,
    },
    Stale {
        pid: Option<u32>,
        url: Option<String>,
    },
    Down,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct GatewayLifecycleError {
    code: &'static str,
    message: String,
    hint: Option<String>,
    exit_code: u8,
}

impl GatewayLifecycleError {
    pub fn not_installed() -> Self {
        Self {
            code: "GATEWAY_NOT_INSTALLED",
            message: "Nexus Gateway is not installed".into(),
            hint: Some(GATEWAY_INSTALL_HINT.into()),
            exit_code: 3,
        }
    }

    pub(crate) fn lifecycle(message: impl Into<String>) -> Self {
        Self {
            code: "GATEWAY_LIFECYCLE_FAILED",
            message: message.into(),
            hint: None,
            exit_code: 1,
        }
    }

    pub fn code(&self) -> &'static str {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn hint(&self) -> Option<&str> {
        self.hint.as_deref()
    }

    pub fn exit_code(&self) -> u8 {
        self.exit_code
    }

    pub fn envelope(&self) -> GatewayErrorEnvelope<'_> {
        GatewayErrorEnvelope {
            error: GatewayErrorBody {
                code: self.code,
                message: &self.message,
                hint: self.hint.as_deref(),
            },
        }
    }
}

impl From<io::Error> for GatewayLifecycleError {
    fn from(error: io::Error) -> Self {
        Self::lifecycle(error.to_string())
    }
}

impl From<lifecycle::LifecycleError> for GatewayLifecycleError {
    fn from(error: lifecycle::LifecycleError) -> Self {
        Self::lifecycle(error.to_string())
    }
}

#[derive(Debug, Serialize)]
pub struct GatewayErrorEnvelope<'a> {
    error: GatewayErrorBody<'a>,
}

#[derive(Debug, Serialize)]
struct GatewayErrorBody<'a> {
    code: &'a str,
    message: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    hint: Option<&'a str>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GatewayStatusReport {
    pub installed: bool,
    pub daemon_running: bool,
    pub runtime: GatewayRuntimeStatus,
    pub log_path: PathBuf,
}

impl GatewayStatusReport {
    pub fn exit_code(&self) -> u8 {
        match self.runtime {
            GatewayRuntimeStatus::Live { .. } if self.daemon_running => 0,
            GatewayRuntimeStatus::Live { .. }
            | GatewayRuntimeStatus::Degraded { .. }
            | GatewayRuntimeStatus::Stale { .. } => 1,
            GatewayRuntimeStatus::Down => 2,
        }
    }
}

#[derive(Debug, Clone)]
pub struct GatewayPaths {
    pub home: PathBuf,
    pub discovery: PathBuf,
    pub log: PathBuf,
}

impl GatewayPaths {
    pub fn resolve() -> Self {
        let home = DaemonPaths::resolve().home;
        Self {
            discovery: home.join("gateway.json"),
            log: home.join("gateway.log"),
            home,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GatewayDiscovery {
    pid: u32,
    url: String,
}

#[async_trait::async_trait(?Send)]
trait GatewayBackend: Send + Sync {
    fn resolve(&self) -> Result<GatewayInstallation, GatewayLifecycleError>;
    async fn ensure_daemon(&self) -> Result<(), GatewayLifecycleError>;
    fn prepare_store(
        &self,
        installation: &GatewayInstallation,
    ) -> Result<(), GatewayLifecycleError>;
    fn status(&self) -> GatewayRuntimeStatus;
    fn clear_stale(&self) -> Result<(), GatewayLifecycleError>;
    fn spawn(&self, installation: &GatewayInstallation) -> Result<(), GatewayLifecycleError>;
    async fn wait_ready(&self) -> Result<GatewayRuntimeStatus, GatewayLifecycleError>;
    fn stop(&self, force: bool) -> Result<(), GatewayLifecycleError>;
}

async fn start_with<B: GatewayBackend>(
    backend: &B,
) -> Result<GatewayRuntimeStatus, GatewayLifecycleError> {
    let installation = backend.resolve()?;
    backend.ensure_daemon().await?;
    backend.prepare_store(&installation)?;
    match backend.status() {
        live @ GatewayRuntimeStatus::Live { .. } => return Ok(live),
        // A stale discovery PID can be reused after an OS/container restart. An unhealthy
        // endpoint does not prove that the live PID still belongs to Nexus, so ordinary start
        // must never signal it. Clear the stale locator and let the new gateway bind or fail
        // safely on its configured port.
        GatewayRuntimeStatus::Degraded { .. } => backend.clear_stale()?,
        GatewayRuntimeStatus::Stale { .. } => backend.clear_stale()?,
        GatewayRuntimeStatus::Down => {}
    }
    backend.spawn(&installation)?;
    backend.wait_ready().await
}

async fn restart_with<B: GatewayBackend>(
    backend: &B,
    force: bool,
) -> Result<GatewayRuntimeStatus, GatewayLifecycleError> {
    let installation = backend.resolve()?;
    backend.ensure_daemon().await?;
    backend.prepare_store(&installation)?;
    match backend.status() {
        GatewayRuntimeStatus::Live { .. } => backend.stop(force)?,
        GatewayRuntimeStatus::Degraded { reason } => {
            if !force {
                return Err(degraded_signal_requires_force("restart", &reason));
            }
            backend.stop(true)?;
        }
        GatewayRuntimeStatus::Stale { .. } => backend.clear_stale()?,
        GatewayRuntimeStatus::Down => {}
    }
    backend.spawn(&installation)?;
    backend.wait_ready().await
}

fn stop_with<B: GatewayBackend>(
    backend: &B,
    force: bool,
) -> Result<GatewayRuntimeStatus, GatewayLifecycleError> {
    backend.resolve()?;
    match backend.status() {
        GatewayRuntimeStatus::Live { .. } => backend.stop(force)?,
        GatewayRuntimeStatus::Degraded { reason } => {
            if !force {
                return Err(degraded_signal_requires_force("stop", &reason));
            }
            backend.stop(true)?;
        }
        GatewayRuntimeStatus::Stale { .. } => backend.clear_stale()?,
        GatewayRuntimeStatus::Down => {}
    }
    Ok(GatewayRuntimeStatus::Down)
}

fn degraded_signal_requires_force(action: &str, reason: &str) -> GatewayLifecycleError {
    GatewayLifecycleError::lifecycle(format!(
        "gateway is degraded ({reason}); refusing to {action} an unverified discovery PID; retry with --force"
    ))
}

struct SystemGatewayBackend {
    paths: GatewayPaths,
}

impl SystemGatewayBackend {
    fn new() -> Self {
        Self {
            paths: GatewayPaths::resolve(),
        }
    }
}

#[async_trait::async_trait(?Send)]
impl GatewayBackend for SystemGatewayBackend {
    fn resolve(&self) -> Result<GatewayInstallation, GatewayLifecycleError> {
        resolve_installed_gateway()
    }

    async fn ensure_daemon(&self) -> Result<(), GatewayLifecycleError> {
        lifecycle::ensure_running().await.map_err(Into::into)
    }

    fn prepare_store(
        &self,
        installation: &GatewayInstallation,
    ) -> Result<(), GatewayLifecycleError> {
        fs::create_dir_all(&self.paths.home)?;
        let mut command = gateway_command(installation);
        command
            .arg("--migrate-only")
            .env("NEXUS_HOME", &self.paths.home)
            .stdin(Stdio::null());
        let output = command.output()?;
        if output.status.success() {
            return Ok(());
        }
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        Err(GatewayLifecycleError::lifecycle(if stderr.is_empty() {
            format!("Gateway store migration failed with {}", output.status)
        } else {
            format!("Gateway store migration failed: {stderr}")
        }))
    }

    fn status(&self) -> GatewayRuntimeStatus {
        runtime_status(&self.paths)
    }

    fn clear_stale(&self) -> Result<(), GatewayLifecycleError> {
        if !matches!(
            runtime_status(&self.paths),
            GatewayRuntimeStatus::Live { .. }
        ) {
            match fs::remove_file(&self.paths.discovery) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    fn spawn(&self, installation: &GatewayInstallation) -> Result<(), GatewayLifecycleError> {
        fs::create_dir_all(&self.paths.home)?;
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.paths.log)?;
        let stderr = log.try_clone()?;
        let mut command = gateway_command(installation);
        command
            .env("NEXUS_HOME", &self.paths.home)
            .env("NEXUS_GATEWAY_DISCOVERY", "write")
            .stdin(Stdio::null())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(stderr));
        detach_command(&mut command);
        command.spawn().map(|_| ()).map_err(Into::into)
    }

    async fn wait_ready(&self) -> Result<GatewayRuntimeStatus, GatewayLifecycleError> {
        let deadline = Instant::now() + GATEWAY_READY_TIMEOUT;
        while Instant::now() < deadline {
            let status = runtime_status(&self.paths);
            if matches!(status, GatewayRuntimeStatus::Live { .. }) {
                return Ok(status);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Err(GatewayLifecycleError::lifecycle(format!(
            "gateway did not become healthy; inspect {}",
            self.paths.log.display()
        )))
    }

    fn stop(&self, force: bool) -> Result<(), GatewayLifecycleError> {
        let discovery = read_discovery(&self.paths.discovery).map_err(|error| {
            GatewayLifecycleError::lifecycle(format!("invalid gateway discovery: {error}"))
        })?;
        if !process_alive(discovery.pid) {
            return self.clear_stale();
        }
        signal_process(discovery.pid, false)?;
        if wait_pid_down(discovery.pid, Duration::from_secs(8)) {
            return self.clear_stale();
        }
        if !force {
            return Err(GatewayLifecycleError::lifecycle(format!(
                "gateway pid {} did not stop within 8s; retry with --force",
                discovery.pid
            )));
        }
        signal_process(discovery.pid, true)?;
        if !wait_pid_down(discovery.pid, Duration::from_secs(3)) {
            return Err(GatewayLifecycleError::lifecycle(format!(
                "gateway pid {} remained alive after forced stop",
                discovery.pid
            )));
        }
        self.clear_stale()
    }
}

pub async fn start_gateway() -> Result<GatewayStatusReport, GatewayLifecycleError> {
    lifecycle::ensure_operator_supervision()?;
    let backend = SystemGatewayBackend::new();
    let runtime = start_with(&backend).await?;
    Ok(report(&backend, runtime))
}

pub fn stop_gateway(force: bool) -> Result<GatewayStatusReport, GatewayLifecycleError> {
    lifecycle::ensure_operator_supervision()?;
    let backend = SystemGatewayBackend::new();
    let runtime = stop_with(&backend, force)?;
    Ok(report(&backend, runtime))
}

pub async fn restart_gateway(force: bool) -> Result<GatewayStatusReport, GatewayLifecycleError> {
    lifecycle::ensure_operator_supervision()?;
    let backend = SystemGatewayBackend::new();
    let runtime = restart_with(&backend, force).await?;
    Ok(report(&backend, runtime))
}

pub fn gateway_status() -> Result<GatewayStatusReport, GatewayLifecycleError> {
    let backend = SystemGatewayBackend::new();
    backend.resolve()?;
    let runtime = backend.status();
    Ok(report(&backend, runtime))
}

pub fn gateway_logs(lines: usize, follow: bool) -> Result<(), GatewayLifecycleError> {
    let backend = SystemGatewayBackend::new();
    backend.resolve()?;
    if follow {
        follow_file_tail(&backend.paths.log, lines)?;
    } else {
        print_file_tail(&backend.paths.log, lines)?;
    }
    Ok(())
}

fn report(backend: &SystemGatewayBackend, runtime: GatewayRuntimeStatus) -> GatewayStatusReport {
    GatewayStatusReport {
        installed: true,
        daemon_running: lifecycle::dependency_running(),
        runtime,
        log_path: backend.paths.log.clone(),
    }
}

fn runtime_status(paths: &GatewayPaths) -> GatewayRuntimeStatus {
    let discovery = match read_discovery(&paths.discovery) {
        Ok(discovery) => discovery,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return GatewayRuntimeStatus::Down,
        Err(error) => {
            return GatewayRuntimeStatus::Degraded {
                reason: format!("invalid discovery: {error}"),
            }
        }
    };
    if !process_alive(discovery.pid) {
        return GatewayRuntimeStatus::Stale {
            pid: Some(discovery.pid),
            url: Some(discovery.url),
        };
    }
    if health_ok(&discovery.url) {
        GatewayRuntimeStatus::Live {
            pid: discovery.pid,
            url: discovery.url,
        }
    } else {
        GatewayRuntimeStatus::Degraded {
            reason: format!(
                "gateway pid {} is alive but {} is unhealthy",
                discovery.pid, discovery.url
            ),
        }
    }
}

fn read_discovery(path: &Path) -> io::Result<GatewayDiscovery> {
    let body = fs::read_to_string(path)?;
    serde_json::from_str(&body).map_err(io::Error::other)
}

fn gateway_command(installation: &GatewayInstallation) -> Command {
    match installation.invocation {
        GatewayInvocation::Direct => Command::new(&installation.executable),
        GatewayInvocation::WindowsCommandShim => {
            let mut command = Command::new("cmd.exe");
            command
                .arg("/D")
                .arg("/S")
                .arg("/C")
                .arg(format!("\"{}\"", installation.executable.display()));
            command
        }
    }
}

fn health_ok(base_url: &str) -> bool {
    let Some(authority) = base_url.strip_prefix("http://") else {
        return false;
    };
    let authority = authority.split('/').next().unwrap_or(authority);
    let address = if authority.starts_with("localhost:") {
        authority.replacen("localhost", "127.0.0.1", 1)
    } else {
        authority.to_string()
    };
    let Ok(address) = address.parse::<SocketAddr>() else {
        return false;
    };
    let Ok(mut stream) = TcpStream::connect_timeout(&address, Duration::from_millis(500)) else {
        return false;
    };
    // Gateway status is an operator diagnostic, not a request-path latency metric. Under the
    // deliberately constrained twelve-harness gate, Node can remain healthy while its event loop
    // is unavailable for longer than the former 750 ms probe. Keep the check bounded, but use the
    // same three-second tolerance as the release gate so a loaded live Gateway does not flicker to
    // `degraded` between two successful readiness checks.
    let _ = stream.set_read_timeout(Some(GATEWAY_HEALTH_IO_TIMEOUT));
    let _ = stream.set_write_timeout(Some(GATEWAY_HEALTH_IO_TIMEOUT));
    let request =
        format!("GET /api/v1/health HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n\r\n");
    if stream.write_all(request.as_bytes()).is_err() {
        return false;
    }
    let mut response = [0_u8; 128];
    let Ok(read) = stream.read(&mut response) else {
        return false;
    };
    let head = String::from_utf8_lossy(&response[..read]);
    head.starts_with("HTTP/1.1 200") || head.starts_with("HTTP/1.0 200")
}

fn process_alive(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    #[cfg(unix)]
    {
        Command::new("kill")
            .arg("-0")
            .arg(pid.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }
    #[cfg(windows)]
    {
        let filter = format!("PID eq {pid}");
        Command::new("tasklist.exe")
            .args(["/FI", filter.as_str()])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .output()
            .map(|output| {
                output.status.success()
                    && String::from_utf8_lossy(&output.stdout).contains(&pid.to_string())
            })
            .unwrap_or(false)
    }
}

fn signal_process(pid: u32, force: bool) -> io::Result<()> {
    #[cfg(unix)]
    let status = Command::new("kill")
        .arg(if force { "-KILL" } else { "-TERM" })
        .arg(pid.to_string())
        .status()?;
    #[cfg(windows)]
    let status = {
        let mut command = Command::new("taskkill.exe");
        command.arg("/PID").arg(pid.to_string()).arg("/T");
        if force {
            command.arg("/F");
        }
        command.status()?
    };
    if status.success() {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "failed to terminate gateway pid {pid}"
        )))
    }
}

fn wait_pid_down(pid: u32, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if !process_alive(pid) {
            return true;
        }
        thread::sleep(Duration::from_millis(100));
    }
    false
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

fn print_file_tail(path: &Path, lines: usize) -> io::Result<()> {
    let body = fs::read_to_string(path)?;
    let selected = body.lines().rev().take(lines).collect::<Vec<_>>();
    for line in selected.into_iter().rev() {
        println!("{line}");
    }
    Ok(())
}

fn follow_file_tail(path: &Path, lines: usize) -> io::Result<()> {
    print_file_tail(path, lines)?;
    let mut printed = fs::metadata(path)
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    loop {
        thread::sleep(Duration::from_millis(250));
        let len = fs::metadata(path)?.len();
        if len < printed {
            printed = 0;
        }
        if len == printed {
            continue;
        }
        use std::io::{Seek, SeekFrom};
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(printed))?;
        let mut chunk = String::new();
        file.read_to_string(&mut chunk)?;
        print!("{chunk}");
        io::stdout().flush()?;
        printed = len;
    }
}

pub fn resolve_installed_gateway() -> Result<GatewayInstallation, GatewayLifecycleError> {
    resolve_gateway_installation(
        env::var_os("NEXUS_GATEWAY_BIN"),
        env::var_os("PATH"),
        cfg!(windows),
    )
}

pub fn resolve_gateway_installation(
    explicit: Option<OsString>,
    path: Option<OsString>,
    windows: bool,
) -> Result<GatewayInstallation, GatewayLifecycleError> {
    if let Some(explicit) = explicit.filter(|value| !value.is_empty()) {
        let executable = PathBuf::from(explicit);
        if is_runnable_file(&executable, windows) {
            return Ok(installation_for(executable, windows));
        }
        return Err(GatewayLifecycleError::not_installed());
    }

    let names: &[&str] = if windows {
        &[
            "nexus-gateway.exe",
            "nexus-gateway.cmd",
            "nexus-gateway.bat",
            "nexus-gateway",
        ]
    } else {
        &["nexus-gateway"]
    };
    if let Some(path) = path {
        for directory in env::split_paths(&path) {
            for name in names {
                let candidate = directory.join(name);
                if is_runnable_file(&candidate, windows) {
                    return Ok(installation_for(candidate, windows));
                }
            }
        }
    }
    Err(GatewayLifecycleError::not_installed())
}

fn installation_for(executable: PathBuf, windows: bool) -> GatewayInstallation {
    let is_shim = windows
        && executable
            .extension()
            .and_then(OsStr::to_str)
            .is_some_and(|extension| {
                extension.eq_ignore_ascii_case("cmd") || extension.eq_ignore_ascii_case("bat")
            });
    GatewayInstallation {
        executable,
        invocation: if is_shim {
            GatewayInvocation::WindowsCommandShim
        } else {
            GatewayInvocation::Direct
        },
    }
}

fn is_runnable_file(path: &Path, windows: bool) -> bool {
    let Ok(metadata) = fs::metadata(path) else {
        return false;
    };
    if !metadata.is_file() {
        return false;
    }
    if windows {
        return true;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}

#[path = "../tests/unit/gateway_lifecycle.rs"]
mod gateway_lifecycle_contracts;
