//! On-demand Nexus Webconsole lifecycle.

use std::env;
use std::ffi::{OsStr, OsString};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::daemon::lifecycle::{self, DaemonPaths};
use crate::gateway_lifecycle::{start_gateway, GatewayRuntimeStatus};

const READY_TIMEOUT: Duration = Duration::from_secs(60);
const STOP_TIMEOUT: Duration = Duration::from_secs(8);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebconsoleInvocation {
    Direct,
    WindowsCommandShim,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebconsoleInstallation {
    pub executable: PathBuf,
    pub invocation: WebconsoleInvocation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebconsoleStartOptions {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum WebconsoleRuntimeStatus {
    Live {
        pid: u32,
        url: String,
        host: String,
        port: u16,
        #[serde(rename = "gatewayUrl")]
        gateway_url: String,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WebconsoleStatusReport {
    pub installed: bool,
    pub gateway_healthy: bool,
    pub runtime: WebconsoleRuntimeStatus,
    pub log_path: PathBuf,
}

impl WebconsoleStatusReport {
    pub fn exit_code(&self) -> u8 {
        match self.runtime {
            WebconsoleRuntimeStatus::Live { .. } if self.gateway_healthy => 0,
            WebconsoleRuntimeStatus::Live { .. }
            | WebconsoleRuntimeStatus::Degraded { .. }
            | WebconsoleRuntimeStatus::Stale { .. } => 1,
            WebconsoleRuntimeStatus::Down => 2,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct WebconsoleLifecycleError {
    code: &'static str,
    message: String,
    hint: Option<String>,
    exit_code: u8,
}

impl WebconsoleLifecycleError {
    fn not_installed() -> Self {
        Self {
            code: "WEBCONSOLE_NOT_INSTALLED",
            message: "Nexus Webconsole is not installed".into(),
            hint: Some("Install it with npm install -g @egregore/nexus".into()),
            exit_code: 3,
        }
    }

    fn lifecycle(message: impl Into<String>) -> Self {
        Self {
            code: "WEBCONSOLE_LIFECYCLE_FAILED",
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
}

impl From<lifecycle::LifecycleError> for WebconsoleLifecycleError {
    fn from(error: lifecycle::LifecycleError) -> Self {
        Self::lifecycle(error.to_string())
    }
}

#[async_trait(?Send)]
#[doc(hidden)]
pub trait WebconsoleBackend {
    fn resolve(&mut self) -> Result<WebconsoleInstallation, String>;
    async fn ensure_gateway(&mut self) -> Result<String, String>;
    fn status(&mut self) -> WebconsoleRuntimeStatus;
    fn clear_stale(&mut self) -> Result<(), String>;
    fn recover(
        &mut self,
        installation: &WebconsoleInstallation,
        gateway_url: &str,
        options: &WebconsoleStartOptions,
    ) -> Result<Option<WebconsoleRuntimeStatus>, String>;
    fn spawn(
        &mut self,
        installation: &WebconsoleInstallation,
        gateway_url: &str,
        options: &WebconsoleStartOptions,
    ) -> Result<(), String>;
    async fn wait_ready(&mut self) -> Result<WebconsoleRuntimeStatus, String>;
    fn open_browser(&mut self, url: &str) -> Result<(), String>;
    fn stop(&mut self, force: bool) -> Result<(), String>;
}

pub async fn start_webconsole_with<B: WebconsoleBackend>(
    backend: &mut B,
    options: &WebconsoleStartOptions,
) -> Result<WebconsoleRuntimeStatus, String> {
    let installation = backend.resolve()?;
    let gateway_url = backend.ensure_gateway().await?;
    match backend.status() {
        live @ WebconsoleRuntimeStatus::Live { .. } => return Ok(live),
        WebconsoleRuntimeStatus::Degraded { .. } | WebconsoleRuntimeStatus::Stale { .. } => {
            backend.clear_stale()?;
        }
        WebconsoleRuntimeStatus::Down => {
            if let Some(runtime) = backend.recover(&installation, &gateway_url, options)? {
                return Ok(runtime);
            }
        }
    }
    backend.spawn(&installation, &gateway_url, options)?;
    backend.wait_ready().await
}

pub async fn launch_webconsole_with<B: WebconsoleBackend>(
    backend: &mut B,
    options: &WebconsoleStartOptions,
    no_open: bool,
) -> Result<WebconsoleRuntimeStatus, String> {
    let runtime = start_webconsole_with(backend, options).await?;
    if !no_open {
        let url = runtime_url(&runtime).ok_or_else(|| "Webconsole has no live URL".to_string())?;
        backend.open_browser(url)?;
    }
    Ok(runtime)
}

pub fn stop_webconsole_with<B: WebconsoleBackend>(
    backend: &mut B,
    force: bool,
) -> Result<WebconsoleRuntimeStatus, String> {
    match backend.status() {
        WebconsoleRuntimeStatus::Live { .. } | WebconsoleRuntimeStatus::Degraded { .. } => {
            backend.stop(force)?;
            backend.clear_stale()?;
        }
        WebconsoleRuntimeStatus::Stale { .. } => backend.clear_stale()?,
        WebconsoleRuntimeStatus::Down => {}
    }
    Ok(WebconsoleRuntimeStatus::Down)
}

pub async fn start_webconsole(
    options: WebconsoleStartOptions,
) -> Result<WebconsoleStatusReport, WebconsoleLifecycleError> {
    lifecycle::ensure_operator_supervision()?;
    let mut backend = SystemWebconsoleBackend::new();
    let runtime = start_webconsole_with(&mut backend, &options)
        .await
        .map_err(WebconsoleLifecycleError::lifecycle)?;
    Ok(backend.report(runtime))
}

pub async fn launch_webconsole(
    options: WebconsoleStartOptions,
    no_open: bool,
) -> Result<WebconsoleStatusReport, WebconsoleLifecycleError> {
    lifecycle::ensure_operator_supervision()?;
    let mut backend = SystemWebconsoleBackend::new();
    let runtime = launch_webconsole_with(&mut backend, &options, no_open)
        .await
        .map_err(WebconsoleLifecycleError::lifecycle)?;
    Ok(backend.report(runtime))
}

pub fn stop_webconsole(force: bool) -> Result<WebconsoleStatusReport, WebconsoleLifecycleError> {
    lifecycle::ensure_operator_supervision()?;
    let mut backend = SystemWebconsoleBackend::new();
    backend
        .resolve()
        .map_err(WebconsoleLifecycleError::lifecycle)?;
    let runtime =
        stop_webconsole_with(&mut backend, force).map_err(WebconsoleLifecycleError::lifecycle)?;
    Ok(backend.report(runtime))
}

pub async fn restart_webconsole(
    options: WebconsoleStartOptions,
) -> Result<WebconsoleStatusReport, WebconsoleLifecycleError> {
    lifecycle::ensure_operator_supervision()?;
    let mut backend = SystemWebconsoleBackend::new();
    backend
        .resolve()
        .map_err(WebconsoleLifecycleError::lifecycle)?;
    stop_webconsole_with(&mut backend, false).map_err(WebconsoleLifecycleError::lifecycle)?;
    let runtime = start_webconsole_with(&mut backend, &options)
        .await
        .map_err(WebconsoleLifecycleError::lifecycle)?;
    Ok(backend.report(runtime))
}

pub fn webconsole_status() -> Result<WebconsoleStatusReport, WebconsoleLifecycleError> {
    let mut backend = SystemWebconsoleBackend::new();
    backend
        .resolve()
        .map_err(|_| WebconsoleLifecycleError::not_installed())?;
    let runtime = backend.status();
    Ok(backend.report(runtime))
}

pub fn webconsole_url() -> Result<String, WebconsoleLifecycleError> {
    runtime_url(&webconsole_status()?.runtime)
        .map(str::to_owned)
        .ok_or_else(|| WebconsoleLifecycleError::lifecycle("Webconsole is not running"))
}

pub fn webconsole_logs(lines: usize, follow: bool) -> Result<(), WebconsoleLifecycleError> {
    let path = WebconsolePaths::resolve().log;
    if follow {
        follow_file_tail(&path, lines)
            .map_err(|error| WebconsoleLifecycleError::lifecycle(error.to_string()))
    } else {
        print_file_tail(&path, lines)
            .map_err(|error| WebconsoleLifecycleError::lifecycle(error.to_string()))
    }
}

fn runtime_url(runtime: &WebconsoleRuntimeStatus) -> Option<&str> {
    match runtime {
        WebconsoleRuntimeStatus::Live { url, .. } => Some(url),
        _ => None,
    }
}

#[derive(Debug, Clone)]
struct WebconsolePaths {
    discovery: PathBuf,
    log: PathBuf,
}

impl WebconsolePaths {
    fn resolve() -> Self {
        let home = DaemonPaths::resolve().home;
        Self {
            discovery: home.join("webconsole.json"),
            log: home.join("webconsole.log"),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct WebconsoleDiscovery {
    pid: u32,
    host: String,
    port: u16,
    url: String,
    gateway_url: String,
    executable: PathBuf,
}

#[derive(Debug, Deserialize)]
struct WebconsoleHealth {
    ok: bool,
    service: String,
    pid: u32,
    host: String,
    port: u16,
    url: String,
    gateway: String,
    executable: PathBuf,
}

struct SystemWebconsoleBackend {
    paths: WebconsolePaths,
    gateway_healthy: bool,
}

impl SystemWebconsoleBackend {
    fn new() -> Self {
        Self {
            paths: WebconsolePaths::resolve(),
            gateway_healthy: false,
        }
    }

    fn report(&self, runtime: WebconsoleRuntimeStatus) -> WebconsoleStatusReport {
        let discovered_gateway_healthy = match &runtime {
            WebconsoleRuntimeStatus::Live { gateway_url, .. } => {
                http_health_ok(gateway_url, "/api/v1/health")
            }
            _ => false,
        };
        WebconsoleStatusReport {
            installed: resolve_installed_webconsole().is_ok(),
            gateway_healthy: self.gateway_healthy || discovered_gateway_healthy,
            runtime,
            log_path: self.paths.log.clone(),
        }
    }
}

#[async_trait(?Send)]
impl WebconsoleBackend for SystemWebconsoleBackend {
    fn resolve(&mut self) -> Result<WebconsoleInstallation, String> {
        resolve_installed_webconsole().map_err(|error| error.to_string())
    }

    async fn ensure_gateway(&mut self) -> Result<String, String> {
        let report = start_gateway().await.map_err(|error| error.to_string())?;
        match report.runtime {
            GatewayRuntimeStatus::Live { url, .. } => {
                self.gateway_healthy = true;
                Ok(url)
            }
            other => Err(format!("Gateway is not healthy: {other:?}")),
        }
    }

    fn status(&mut self) -> WebconsoleRuntimeStatus {
        runtime_status(&self.paths)
    }

    fn clear_stale(&mut self) -> Result<(), String> {
        if !matches!(
            runtime_status(&self.paths),
            WebconsoleRuntimeStatus::Live { .. }
        ) {
            match fs::remove_file(&self.paths.discovery) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.to_string()),
            }
        }
        Ok(())
    }

    fn recover(
        &mut self,
        _installation: &WebconsoleInstallation,
        gateway_url: &str,
        options: &WebconsoleStartOptions,
    ) -> Result<Option<WebconsoleRuntimeStatus>, String> {
        let expected_url = webconsole_url_for(&options.host, options.port);
        let health = match read_webconsole_health(&options.host, options.port) {
            Ok(Some(health)) => health,
            Ok(None) => {
                return match TcpListener::bind((options.host.as_str(), options.port)) {
                    Ok(listener) => {
                        drop(listener);
                        Ok(None)
                    }
                    Err(error) => Err(format!(
                        "Webconsole port {} is occupied but no verified Nexus WebUI responded: {error}",
                        options.port
                    )),
                };
            }
            Err(error) => {
                return Err(format!(
                    "Webconsole port {} is occupied by an unverified process: {error}",
                    options.port
                ));
            }
        };
        if !health.ok
            || health.service != "nexus-webui"
            || health.host != options.host
            || health.port != options.port
            || health.url != expected_url
            || health.gateway.trim_end_matches('/') != gateway_url.trim_end_matches('/')
            || !process_alive(health.pid)
            || !process_matches(health.pid, &health.executable)
        {
            return Err(format!(
                "Webconsole port {} is occupied by a process that cannot be safely adopted",
                options.port
            ));
        }
        let discovery = WebconsoleDiscovery {
            pid: health.pid,
            host: health.host,
            port: health.port,
            url: health.url,
            gateway_url: health.gateway,
            executable: health.executable,
        };
        write_discovery(&self.paths.discovery, &discovery).map_err(|error| error.to_string())?;
        let runtime = runtime_status(&self.paths);
        match runtime {
            live @ WebconsoleRuntimeStatus::Live { .. } => Ok(Some(live)),
            other => Err(format!(
                "Webconsole discovery was recovered but verification failed: {other:?}"
            )),
        }
    }

    fn spawn(
        &mut self,
        installation: &WebconsoleInstallation,
        gateway_url: &str,
        options: &WebconsoleStartOptions,
    ) -> Result<(), String> {
        if let Some(parent) = self.paths.log.parent() {
            fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.paths.log)
            .map_err(|error| error.to_string())?;
        let stderr = log.try_clone().map_err(|error| error.to_string())?;
        let mut command = webconsole_command(installation);
        command
            .arg("--host")
            .arg(&options.host)
            .arg("--port")
            .arg(options.port.to_string())
            .arg("--gateway-url")
            .arg(gateway_url)
            .arg("--discovery")
            .arg(&self.paths.discovery)
            .stdin(Stdio::null())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(stderr));
        detach_command(&mut command);
        command
            .spawn()
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    async fn wait_ready(&mut self) -> Result<WebconsoleRuntimeStatus, String> {
        let deadline = Instant::now() + READY_TIMEOUT;
        while Instant::now() < deadline {
            let runtime = runtime_status(&self.paths);
            if matches!(runtime, WebconsoleRuntimeStatus::Live { .. }) {
                return Ok(runtime);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Err(format!(
            "Webconsole did not become healthy; inspect {}",
            self.paths.log.display()
        ))
    }

    fn open_browser(&mut self, url: &str) -> Result<(), String> {
        open_browser(url)
    }

    fn stop(&mut self, force: bool) -> Result<(), String> {
        let discovery = read_discovery(&self.paths.discovery).map_err(|error| error.to_string())?;
        if !process_alive(discovery.pid) {
            return Ok(());
        }
        signal_process(discovery.pid, false).map_err(|error| error.to_string())?;
        if wait_pid_down(discovery.pid, STOP_TIMEOUT) {
            return Ok(());
        }
        if !force {
            return Err(format!(
                "Webconsole pid {} did not stop within 8s; retry with --force",
                discovery.pid
            ));
        }
        signal_process(discovery.pid, true).map_err(|error| error.to_string())?;
        if wait_pid_down(discovery.pid, Duration::from_secs(3)) {
            Ok(())
        } else {
            Err(format!(
                "Webconsole pid {} remained alive after forced stop",
                discovery.pid
            ))
        }
    }
}

pub fn resolve_installed_webconsole() -> Result<WebconsoleInstallation, WebconsoleLifecycleError> {
    resolve_webconsole_installation(
        env::var_os("NEXUS_WEBUI_BIN"),
        env::var_os("PATH"),
        cfg!(windows),
    )
}

pub fn resolve_webconsole_installation(
    explicit: Option<OsString>,
    path: Option<OsString>,
    windows: bool,
) -> Result<WebconsoleInstallation, WebconsoleLifecycleError> {
    if let Some(explicit) = explicit.filter(|value| !value.is_empty()) {
        let executable = PathBuf::from(explicit);
        if executable.is_file() {
            return Ok(webconsole_installation(executable, windows));
        }
        return Err(WebconsoleLifecycleError::not_installed());
    }
    let names: &[&str] = if windows {
        &[
            "nexus-webui.exe",
            "nexus-webui.cmd",
            "nexus-webui.bat",
            "nexus-webui",
        ]
    } else {
        &["nexus-webui"]
    };
    if let Some(path) = path {
        for directory in env::split_paths(&path) {
            for name in names {
                let candidate = directory.join(name);
                if candidate.is_file() {
                    return Ok(webconsole_installation(candidate, windows));
                }
            }
        }
    }
    Err(WebconsoleLifecycleError::not_installed())
}

fn webconsole_installation(executable: PathBuf, windows: bool) -> WebconsoleInstallation {
    let invocation = if windows
        && executable
            .extension()
            .and_then(OsStr::to_str)
            .is_some_and(|extension| {
                extension.eq_ignore_ascii_case("cmd") || extension.eq_ignore_ascii_case("bat")
            }) {
        WebconsoleInvocation::WindowsCommandShim
    } else {
        WebconsoleInvocation::Direct
    };
    WebconsoleInstallation {
        executable,
        invocation,
    }
}

fn webconsole_command(installation: &WebconsoleInstallation) -> Command {
    match installation.invocation {
        WebconsoleInvocation::Direct => Command::new(&installation.executable),
        WebconsoleInvocation::WindowsCommandShim => {
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

fn runtime_status(paths: &WebconsolePaths) -> WebconsoleRuntimeStatus {
    let discovery = match read_discovery(&paths.discovery) {
        Ok(discovery) => discovery,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return WebconsoleRuntimeStatus::Down
        }
        Err(error) => {
            return WebconsoleRuntimeStatus::Degraded {
                reason: format!("invalid discovery: {error}"),
            }
        }
    };
    if !process_alive(discovery.pid) || !process_matches(discovery.pid, &discovery.executable) {
        return WebconsoleRuntimeStatus::Stale {
            pid: Some(discovery.pid),
            url: Some(discovery.url),
        };
    }
    if !http_health_ok(&discovery.url, "/health") {
        return WebconsoleRuntimeStatus::Degraded {
            reason: format!("Webconsole pid {} is alive but unhealthy", discovery.pid),
        };
    }
    WebconsoleRuntimeStatus::Live {
        pid: discovery.pid,
        url: discovery.url,
        host: discovery.host,
        port: discovery.port,
        gateway_url: discovery.gateway_url,
    }
}

fn read_discovery(path: &Path) -> io::Result<WebconsoleDiscovery> {
    serde_json::from_slice(&fs::read(path)?).map_err(io::Error::other)
}

fn write_discovery(path: &Path, discovery: &WebconsoleDiscovery) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let temporary = path.with_extension(format!("json.{}.tmp", std::process::id()));
    let body = serde_json::to_vec(discovery).map_err(io::Error::other)?;
    fs::write(&temporary, body)?;
    match fs::rename(&temporary, path) {
        Ok(()) => Ok(()),
        Err(error) => {
            let _ = fs::remove_file(&temporary);
            Err(error)
        }
    }
}

fn webconsole_url_for(host: &str, port: u16) -> String {
    format!("http://{}", webconsole_authority(host, port))
}

fn webconsole_authority(host: &str, port: u16) -> String {
    let display_host = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host.to_string()
    };
    format!("{display_host}:{port}")
}

fn read_webconsole_health(host: &str, port: u16) -> Result<Option<WebconsoleHealth>, String> {
    let probe_host = match host {
        "0.0.0.0" => "127.0.0.1",
        "::" => "::1",
        other => other,
    };
    let addresses = (probe_host, port)
        .to_socket_addrs()
        .map_err(|error| error.to_string())?;
    let mut stream = None;
    for address in addresses {
        if let Ok(connected) = TcpStream::connect_timeout(&address, Duration::from_millis(500)) {
            stream = Some(connected);
            break;
        }
    }
    let Some(mut stream) = stream else {
        return Ok(None);
    };
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .map_err(|error| error.to_string())?;
    let authority = webconsole_authority(host, port);
    let request = format!("GET /health HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .map_err(|error| error.to_string())?;
    let mut response = Vec::new();
    stream
        .take(16 * 1024)
        .read_to_end(&mut response)
        .map_err(|error| error.to_string())?;
    let boundary = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| "invalid HTTP response".to_string())?;
    let head = String::from_utf8_lossy(&response[..boundary]);
    if !head.starts_with("HTTP/1.1 200") && !head.starts_with("HTTP/1.0 200") {
        return Err(format!(
            "health endpoint returned {}",
            head.lines().next().unwrap_or("an invalid status")
        ));
    }
    serde_json::from_slice(&response[boundary + 4..])
        .map(Some)
        .map_err(|error| format!("invalid Nexus WebUI health response: {error}"))
}

fn http_health_ok(base_url: &str, path: &str) -> bool {
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
    let _ = stream.set_read_timeout(Some(Duration::from_secs(3)));
    let request = format!("GET {path} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n\r\n");
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

#[cfg(target_os = "linux")]
fn process_matches(pid: u32, executable: &Path) -> bool {
    fs::read(format!("/proc/{pid}/cmdline"))
        .map(|body| {
            let needle = executable.to_string_lossy();
            body.split(|byte| *byte == 0)
                .any(|part| String::from_utf8_lossy(part) == needle)
        })
        .unwrap_or(false)
}

#[cfg(target_os = "macos")]
fn process_matches(pid: u32, executable: &Path) -> bool {
    Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", "command="])
        .output()
        .map(|output| {
            output.status.success()
                && String::from_utf8_lossy(&output.stdout).contains(&*executable.to_string_lossy())
        })
        .unwrap_or(false)
}

#[cfg(target_os = "windows")]
fn process_matches(pid: u32, executable: &Path) -> bool {
    let script =
        format!("(Get-CimInstance Win32_Process -Filter \"ProcessId = {pid}\").CommandLine");
    Command::new("powershell.exe")
        .args(["-NoProfile", "-NonInteractive", "-Command", &script])
        .output()
        .map(|output| {
            output.status.success()
                && String::from_utf8_lossy(&output.stdout).contains(&*executable.to_string_lossy())
        })
        .unwrap_or(false)
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn process_matches(_pid: u32, _executable: &Path) -> bool {
    false
}

#[cfg(unix)]
fn process_alive(pid: u32) -> bool {
    let result = unsafe { libc::kill(pid as libc::pid_t, 0) };
    result == 0 || io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(windows)]
fn process_alive(pid: u32) -> bool {
    Command::new("tasklist.exe")
        .args(["/FI", &format!("PID eq {pid}")])
        .output()
        .map(|output| {
            output.status.success()
                && String::from_utf8_lossy(&output.stdout).contains(&pid.to_string())
        })
        .unwrap_or(false)
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
            "failed to terminate Webconsole pid {pid}"
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
        command.creation_flags(0x0000_0200 | 0x0000_0008);
    }
}

fn open_browser(url: &str) -> Result<(), String> {
    #[cfg(target_os = "linux")]
    let status = Command::new("xdg-open").arg(url).status();
    #[cfg(target_os = "macos")]
    let status = Command::new("open").arg(url).status();
    #[cfg(target_os = "windows")]
    let status = Command::new("cmd.exe")
        .args(["/C", "start", "", url])
        .status();
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    return Err("opening a browser is unsupported on this platform".into());
    #[allow(unreachable_code)]
    match status {
        Ok(status) if status.success() => Ok(()),
        Ok(_) => Err("browser command failed".into()),
        Err(error) => Err(error.to_string()),
    }
}

fn print_file_tail(path: &Path, lines: usize) -> io::Result<()> {
    let body = fs::read_to_string(path)?;
    for line in body
        .lines()
        .rev()
        .take(lines)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
    {
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
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(printed))?;
        let mut chunk = String::new();
        file.read_to_string(&mut chunk)?;
        print!("{chunk}");
        io::stdout().flush()?;
        printed = len;
    }
}
