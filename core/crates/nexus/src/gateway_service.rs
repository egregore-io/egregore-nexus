//! Native per-user Gateway service lifecycle.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::Serialize;

use crate::daemon::lifecycle;
use crate::gateway_lifecycle::{
    gateway_status, resolve_installed_gateway, GatewayLifecycleError, GatewayPaths,
    GatewayRuntimeStatus,
};
use crate::lifecycle_process;

const SYSTEMD_SERVICE: &str = "nexus-gateway.service";
const LAUNCHD_LABEL: &str = "io.egregore.nexus.gateway";
const WINDOWS_TASK_NAME: &str = "EgregoreNexusGateway";
const READY_TIMEOUT: Duration = Duration::from_secs(60);
const SERVICE_COMMAND_TIMEOUT: Duration = Duration::from_secs(5);
const SERVICE_COMMAND_OUTPUT_LIMIT: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayServiceSpec {
    pub executable: PathBuf,
    pub home: PathBuf,
    pub log: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GatewayServiceReport {
    pub installed: bool,
    pub running: bool,
    pub supervisor: String,
    pub definition: Option<PathBuf>,
}

#[async_trait(?Send)]
#[doc(hidden)]
pub trait GatewayServiceBackend {
    async fn ensure_daemon_service(&mut self) -> Result<(), String>;
    fn resolve_gateway(&mut self) -> Result<GatewayServiceSpec, String>;
    fn write_definition(&mut self, spec: &GatewayServiceSpec) -> Result<(), String>;
    fn enable(&mut self) -> Result<(), String>;
    fn start(&mut self) -> Result<(), String>;
    async fn wait_healthy(&mut self) -> Result<(), String>;
    fn stop(&mut self) -> Result<(), String>;
    fn disable(&mut self) -> Result<(), String>;
    fn remove_definition(&mut self) -> Result<(), String>;
    fn installed(&self) -> bool;
}

pub async fn install_gateway_service_with<B: GatewayServiceBackend>(
    backend: &mut B,
) -> Result<GatewayServiceReport, String> {
    backend.ensure_daemon_service().await?;
    let spec = backend.resolve_gateway()?;
    backend.write_definition(&spec)?;
    backend.enable()?;
    backend.start()?;
    backend.wait_healthy().await?;
    Ok(GatewayServiceReport {
        installed: true,
        running: true,
        supervisor: platform_label().into(),
        definition: None,
    })
}

pub fn uninstall_gateway_service_with<B: GatewayServiceBackend>(
    backend: &mut B,
) -> Result<GatewayServiceReport, String> {
    if !backend.installed() {
        return Ok(GatewayServiceReport {
            installed: false,
            running: false,
            supervisor: platform_label().into(),
            definition: None,
        });
    }
    backend.stop()?;
    backend.disable()?;
    backend.remove_definition()?;
    Ok(GatewayServiceReport {
        installed: false,
        running: false,
        supervisor: platform_label().into(),
        definition: None,
    })
}

pub async fn install_gateway_service() -> Result<GatewayServiceReport, GatewayLifecycleError> {
    lifecycle::ensure_operator_supervision()?;
    let mut backend = SystemGatewayServiceBackend::new();
    let mut report = install_gateway_service_with(&mut backend)
        .await
        .map_err(GatewayLifecycleError::lifecycle)?;
    report.definition = Some(backend.definition_path());
    Ok(report)
}

pub fn uninstall_gateway_service() -> Result<GatewayServiceReport, GatewayLifecycleError> {
    lifecycle::ensure_operator_supervision()?;
    let mut backend = SystemGatewayServiceBackend::new();
    let mut report =
        uninstall_gateway_service_with(&mut backend).map_err(GatewayLifecycleError::lifecycle)?;
    report.definition = Some(backend.definition_path());
    Ok(report)
}

pub fn rewrite_gateway_service() -> Result<(), GatewayLifecycleError> {
    let mut backend = SystemGatewayServiceBackend::new();
    if !backend.installed() {
        return Ok(());
    }
    let spec = backend
        .resolve_gateway()
        .map_err(GatewayLifecycleError::lifecycle)?;
    backend
        .write_definition(&spec)
        .map_err(GatewayLifecycleError::lifecycle)?;
    backend.enable().map_err(GatewayLifecycleError::lifecycle)
}

pub fn gateway_service_status() -> Result<GatewayServiceReport, GatewayLifecycleError> {
    let backend = SystemGatewayServiceBackend::new();
    let installed = backend.installed();
    let running = gateway_status()
        .map(|report| matches!(report.runtime, GatewayRuntimeStatus::Live { .. }))
        .unwrap_or(false);
    Ok(GatewayServiceReport {
        installed,
        running,
        supervisor: platform_label().into(),
        definition: Some(backend.definition_path()),
    })
}

struct SystemGatewayServiceBackend {
    paths: GatewayPaths,
}

impl SystemGatewayServiceBackend {
    fn new() -> Self {
        Self {
            paths: GatewayPaths::resolve(),
        }
    }

    fn definition_path(&self) -> PathBuf {
        #[cfg(target_os = "linux")]
        {
            return config_home().join("systemd/user").join(SYSTEMD_SERVICE);
        }
        #[cfg(target_os = "macos")]
        {
            return user_home()
                .join("Library/LaunchAgents")
                .join(format!("{LAUNCHD_LABEL}.plist"));
        }
        #[cfg(target_os = "windows")]
        {
            return self.paths.home.join("gateway-task.ps1");
        }
        #[allow(unreachable_code)]
        self.paths.home.join("gateway-service")
    }

    fn command_checked(&self, program: &str, args: &[String]) -> Result<(), String> {
        let mut command = Command::new(program);
        command.args(args);
        lifecycle_process::run_bounded(
            &mut command,
            "Gateway service-manager command",
            SERVICE_COMMAND_TIMEOUT,
            SERVICE_COMMAND_OUTPUT_LIMIT,
        )
        .map(|_| ())
        .map_err(|error| format!("{program} service command failed: {error}"))
    }
}

#[async_trait(?Send)]
impl GatewayServiceBackend for SystemGatewayServiceBackend {
    async fn ensure_daemon_service(&mut self) -> Result<(), String> {
        lifecycle::ensure_service_installed()
            .await
            .map_err(|error| error.to_string())
    }

    fn resolve_gateway(&mut self) -> Result<GatewayServiceSpec, String> {
        let installation = resolve_installed_gateway().map_err(|error| error.to_string())?;
        Ok(GatewayServiceSpec {
            executable: installation.executable,
            home: self.paths.home.clone(),
            log: self.paths.log.clone(),
        })
    }

    fn write_definition(&mut self, spec: &GatewayServiceSpec) -> Result<(), String> {
        let path = self.definition_path();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
        #[cfg(target_os = "linux")]
        fs::write(&path, systemd_unit(spec)).map_err(|error| error.to_string())?;
        #[cfg(target_os = "macos")]
        {
            let _ = self.command_checked(
                "launchctl",
                &[
                    "bootout".into(),
                    format!("gui/{}/{}", current_uid(), LAUNCHD_LABEL),
                ],
            );
            fs::write(&path, launchd_plist(spec)).map_err(|error| error.to_string())?;
        }
        #[cfg(target_os = "windows")]
        {
            let script = windows_runner_script(spec);
            fs::write(&path, script).map_err(|error| error.to_string())?;
            self.command_checked(
                "powershell.exe",
                &[
                    "-NoProfile".into(),
                    "-NonInteractive".into(),
                    "-ExecutionPolicy".into(),
                    "Bypass".into(),
                    "-Command".into(),
                    windows_registration_script(spec),
                ],
            )?;
        }
        Ok(())
    }

    fn enable(&mut self) -> Result<(), String> {
        #[cfg(target_os = "linux")]
        {
            self.command_checked("systemctl", &["--user".into(), "daemon-reload".into()])?;
            return self.command_checked(
                "systemctl",
                &["--user".into(), "enable".into(), SYSTEMD_SERVICE.into()],
            );
        }
        #[cfg(any(target_os = "macos", target_os = "windows"))]
        {
            return Ok(());
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
        {
            Err("Gateway service installation is unsupported on this platform".into())
        }
    }

    fn start(&mut self) -> Result<(), String> {
        #[cfg(target_os = "linux")]
        return self.command_checked(
            "systemctl",
            &["--user".into(), "start".into(), SYSTEMD_SERVICE.into()],
        );
        #[cfg(target_os = "macos")]
        return self.command_checked(
            "launchctl",
            &[
                "bootstrap".into(),
                format!("gui/{}", current_uid()),
                self.definition_path().display().to_string(),
            ],
        );
        #[cfg(target_os = "windows")]
        return self.command_checked(
            "schtasks.exe",
            &["/Run".into(), "/TN".into(), WINDOWS_TASK_NAME.into()],
        );
        #[allow(unreachable_code)]
        Err("Gateway service installation is unsupported on this platform".into())
    }

    async fn wait_healthy(&mut self) -> Result<(), String> {
        let deadline = Instant::now() + READY_TIMEOUT;
        while Instant::now() < deadline {
            if gateway_status()
                .map(|report| matches!(report.runtime, GatewayRuntimeStatus::Live { .. }))
                .unwrap_or(false)
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Err(format!(
            "Gateway service did not become healthy; inspect {}",
            self.paths.log.display()
        ))
    }

    fn stop(&mut self) -> Result<(), String> {
        #[cfg(target_os = "linux")]
        return ignore_absent(self.command_checked(
            "systemctl",
            &["--user".into(), "stop".into(), SYSTEMD_SERVICE.into()],
        ));
        #[cfg(target_os = "macos")]
        return ignore_absent(self.command_checked(
            "launchctl",
            &[
                "bootout".into(),
                format!("gui/{}/{}", current_uid(), LAUNCHD_LABEL),
            ],
        ));
        #[cfg(target_os = "windows")]
        return ignore_absent(self.command_checked(
            "schtasks.exe",
            &["/End".into(), "/TN".into(), WINDOWS_TASK_NAME.into()],
        ));
        #[allow(unreachable_code)]
        Ok(())
    }

    fn disable(&mut self) -> Result<(), String> {
        #[cfg(target_os = "linux")]
        return ignore_absent(self.command_checked(
            "systemctl",
            &["--user".into(), "disable".into(), SYSTEMD_SERVICE.into()],
        ));
        #[cfg(target_os = "windows")]
        return ignore_absent(self.command_checked(
            "schtasks.exe",
            &[
                "/Delete".into(),
                "/TN".into(),
                WINDOWS_TASK_NAME.into(),
                "/F".into(),
            ],
        ));
        #[cfg(not(any(target_os = "linux", target_os = "windows")))]
        Ok(())
    }

    fn remove_definition(&mut self) -> Result<(), String> {
        match fs::remove_file(self.definition_path()) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.to_string()),
        }
        #[cfg(target_os = "linux")]
        self.command_checked("systemctl", &["--user".into(), "daemon-reload".into()])?;
        Ok(())
    }

    fn installed(&self) -> bool {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        return self.definition_path().is_file();
        #[cfg(target_os = "windows")]
        {
            return self
                .command_checked(
                    "schtasks.exe",
                    &["/Query".into(), "/TN".into(), WINDOWS_TASK_NAME.into()],
                )
                .is_ok();
        }
        #[allow(unreachable_code)]
        false
    }
}

pub fn systemd_unit(spec: &GatewayServiceSpec) -> String {
    format!(
        "[Unit]\nDescription=Egregore Nexus Gateway\nRequires=nexus-daemon.service\nAfter=nexus-daemon.service\n\n[Service]\nType=simple\nExecStart={}\nEnvironment={}\nEnvironment=NEXUS_GATEWAY_DISCOVERY=write\nRestart=on-failure\nRestartSec=2\nStandardOutput={}\nStandardError={}\n\n[Install]\nWantedBy=default.target\n",
        systemd_quote(&spec.executable),
        systemd_quote_value(&format!("NEXUS_HOME={}", spec.home.display())),
        systemd_quote_value(&format!("append:{}", spec.log.display())),
        systemd_quote_value(&format!("append:{}", spec.log.display())),
    )
}

pub fn launchd_plist(spec: &GatewayServiceSpec) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\"><dict>\n<key>Label</key><string>{}</string>\n<key>ProgramArguments</key><array><string>{}</string></array>\n<key>EnvironmentVariables</key><dict><key>NEXUS_HOME</key><string>{}</string><key>NEXUS_GATEWAY_DISCOVERY</key><string>write</string></dict>\n<key>RunAtLoad</key><true/>\n<key>KeepAlive</key><true/>\n<key>StandardOutPath</key><string>{}</string>\n<key>StandardErrorPath</key><string>{}</string>\n</dict></plist>\n",
        LAUNCHD_LABEL,
        xml_escape(&spec.executable.to_string_lossy()),
        xml_escape(&spec.home.to_string_lossy()),
        xml_escape(&spec.log.to_string_lossy()),
        xml_escape(&spec.log.to_string_lossy()),
    )
}

pub fn windows_registration_script(spec: &GatewayServiceSpec) -> String {
    let runner = spec.home.join("gateway-task.ps1");
    format!(
        "$action = New-ScheduledTaskAction -Execute 'powershell.exe' -Argument '-NoProfile -NonInteractive -ExecutionPolicy Bypass -File {}'\n$trigger = New-ScheduledTaskTrigger -AtLogOn\n$settings = New-ScheduledTaskSettingsSet -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1) -ExecutionTimeLimit ([TimeSpan]::Zero)\n$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited\nRegister-ScheduledTask -TaskName '{}' -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Force | Out-Null\n# {}\n",
        powershell_literal(&runner.to_string_lossy()),
        WINDOWS_TASK_NAME,
        powershell_literal(&spec.executable.to_string_lossy()),
    )
}

/// Render the PowerShell runner the scheduled task executes.
///
/// PowerShell runs a `.cmd`/`.bat` shim directly. Routing it through `cmd.exe /S /C` instead
/// required hand-quoting the path, and PowerShell passes those quotes through literally, so `/S`
/// stripped them back off and any installation path containing a space was split.
pub fn windows_runner_script(spec: &GatewayServiceSpec) -> String {
    let invocation = format!(
        "& {}",
        powershell_literal(&spec.executable.to_string_lossy())
    );
    format!(
        "$env:NEXUS_HOME = {}\n$env:NEXUS_GATEWAY_DISCOVERY = 'write'\n{}\nexit $LASTEXITCODE\n",
        powershell_literal(&spec.home.to_string_lossy()),
        invocation,
    )
}

fn systemd_quote(path: &Path) -> String {
    systemd_quote_value(&path.to_string_lossy())
}

fn systemd_quote_value(value: &str) -> String {
    format!(
        "\"{}\"",
        value
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('%', "%%")
            .replace(['\n', '\r'], "")
    )
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn powershell_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''").replace(['\n', '\r'], ""))
}

fn ignore_absent(result: Result<(), String>) -> Result<(), String> {
    result.or(Ok(()))
}

fn platform_label() -> &'static str {
    if cfg!(target_os = "linux") {
        "systemd"
    } else if cfg!(target_os = "macos") {
        "launchd"
    } else if cfg!(target_os = "windows") {
        "task-scheduler"
    } else {
        "unsupported"
    }
}

fn user_home() -> PathBuf {
    env::var_os("HOME")
        .or_else(|| env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
}

fn config_home() -> PathBuf {
    env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| user_home().join(".config"))
}

#[cfg(target_os = "macos")]
fn current_uid() -> String {
    unsafe { libc::geteuid() }.to_string()
}
