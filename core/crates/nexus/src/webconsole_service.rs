//! Per-user background Webconsole service; never opens a browser.

use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};

use crate::{
    daemon::lifecycle, gateway_lifecycle, gateway_service, lifecycle_process, webconsole_lifecycle,
};

#[cfg(any(target_os = "linux", test))]
const UNIT: &str = "nexus-webconsole.service";
const LABEL: &str = "io.egregore.nexus.webconsole";
const TASK: &str = "EgregoreNexusWebconsole";

#[derive(Debug)]
#[doc(hidden)]
pub struct Spec {
    pub executable: PathBuf,
    pub home: PathBuf,
    pub gateway_url: String,
    pub path: String,
}

impl Spec {
    fn args(&self) -> Vec<String> {
        vec![
            "--host".into(),
            "127.0.0.1".into(),
            "--port".into(),
            "4200".into(),
            "--gateway-url".into(),
            self.gateway_url.clone(),
            "--discovery".into(),
            self.home.join("webconsole.json").display().to_string(),
        ]
    }
}

/// Detect service registration only; this is not proof of enablement or current health.
pub fn installed() -> bool {
    definition_path().is_file()
}

/// Enable the installed Webconsole server at login, and verify it is running now.
pub async fn install() -> Result<(), String> {
    lifecycle::ensure_operator_supervision().map_err(|e| e.to_string())?;
    let executable = webconsole_lifecycle::resolve_installed_webconsole()
        .map_err(|e| e.to_string())?
        .executable;
    // First-run establishes dependencies in order; explicit install must not silently
    // restart the daemon/Gateway or mix their human output into this command's JSON.
    if !gateway_service::gateway_service_status()
        .map(|s| s.installed && s.running)
        .unwrap_or(false)
    {
        return Err("install/start the Gateway service first: nexus gateway install".into());
    }
    let gateway = gateway_lifecycle::gateway_status().map_err(|e| e.to_string())?;
    let gateway_lifecycle::GatewayRuntimeStatus::Live { url, .. } = gateway.runtime else {
        return Err(
            "Gateway must be healthy before installing Webconsole background service".into(),
        );
    };
    let spec = Spec {
        executable,
        home: lifecycle::nexus_home(),
        gateway_url: url,
        path: std::env::var("PATH").unwrap_or_default(),
    };
    if [
        &spec.executable.to_string_lossy(),
        &spec.home.to_string_lossy(),
        &std::borrow::Cow::Borrowed(spec.path.as_str()),
    ]
    .iter()
    .any(|s| s.contains(['\n', '\r', '\0']))
    {
        return Err("service paths/PATH must not contain control characters".into());
    }
    if installed() {
        stop_service()?;
    }
    webconsole_lifecycle::stop_webconsole(false).map_err(|e| e.to_string())?;
    let definition = definition_path();
    fs::create_dir_all(definition.parent().ok_or("missing service directory")?)
        .map_err(|e| e.to_string())?;
    fs::create_dir_all(&spec.home).map_err(|e| e.to_string())?;
    write_and_start(&spec)?;
    wait_healthy().await
}

async fn wait_healthy() -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if webconsole_lifecycle::webconsole_status()
            .map(|report| {
                report.gateway_healthy
                    && matches!(
                        report.runtime,
                        webconsole_lifecycle::WebconsoleRuntimeStatus::Live { .. }
                    )
            })
            .unwrap_or(false)
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err("Webconsole service was registered but did not become healthy; inspect nexus webconsole logs".into())
}

/// Start the registered supervisor, retaining its saved URL, PATH and bind settings.
pub async fn start() -> Result<(), String> {
    lifecycle::ensure_operator_supervision().map_err(|e| e.to_string())?;
    if !installed() {
        return Err("Webconsole service is not installed".into());
    }
    #[cfg(target_os = "linux")]
    run("systemctl", &["--user", "start", UNIT])?;
    #[cfg(target_os = "macos")]
    {
        let domain = format!("gui/{}", unsafe { libc::geteuid() });
        gateway_service::launchd_start_with(
            &domain,
            LABEL,
            &definition_path().display().to_string(),
            false,
            gateway_service::launchd_command,
        )?;
    }
    #[cfg(target_os = "windows")]
    run("schtasks.exe", &["/Run", "/TN", TASK])?;
    wait_healthy().await
}

/// Stop the supervisor itself so KeepAlive cannot undo an explicit stop.
pub fn stop() -> Result<(), String> {
    lifecycle::ensure_operator_supervision().map_err(|e| e.to_string())?;
    if installed() {
        stop_service()?;
    }
    Ok(())
}

/// Stop and remove only the Webconsole's per-user service.
pub fn uninstall() -> Result<(), String> {
    lifecycle::ensure_operator_supervision().map_err(|e| e.to_string())?;
    if !installed() {
        return Ok(());
    }
    stop_service()?;
    #[cfg(target_os = "linux")]
    run("systemctl", &["--user", "disable", UNIT])?;
    #[cfg(target_os = "windows")]
    run("schtasks.exe", &["/Delete", "/TN", TASK, "/F"])?;
    fs::remove_file(definition_path()).map_err(|e| e.to_string())?;
    #[cfg(target_os = "linux")]
    run("systemctl", &["--user", "daemon-reload"])?;
    Ok(())
}

fn run(program: &str, args: &[&str]) -> Result<(), String> {
    lifecycle_process::run_bounded(
        Command::new(program).args(args),
        "Webconsole service manager",
        Duration::from_secs(5),
        64 * 1024,
    )
    .map(|_| ())
    .map_err(|e| e.to_string())
}

fn definition_path() -> PathBuf {
    #[cfg(target_os = "linux")]
    return std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| nexus_common::config::expand_tilde("~/.config").into())
        .join("systemd/user")
        .join(UNIT);
    #[cfg(target_os = "macos")]
    return PathBuf::from(nexus_common::config::expand_tilde("~/Library/LaunchAgents"))
        .join(format!("{LABEL}.plist"));
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    lifecycle::nexus_home().join("webconsole-task.ps1")
}

fn write_and_start(spec: &Spec) -> Result<(), String> {
    #[cfg(target_os = "linux")]
    {
        fs::write(definition_path(), systemd_unit(spec)).map_err(|e| e.to_string())?;
        run("systemctl", &["--user", "daemon-reload"])?;
        run("systemctl", &["--user", "enable", UNIT])?;
        return run("systemctl", &["--user", "start", UNIT]);
    }
    #[cfg(target_os = "macos")]
    {
        fs::write(definition_path(), launchd_plist(spec)).map_err(|e| e.to_string())?;
        return run(
            "launchctl",
            &[
                "bootstrap",
                &format!("gui/{}", unsafe { libc::geteuid() }),
                &definition_path().display().to_string(),
            ],
        );
    }
    #[cfg(target_os = "windows")]
    {
        fs::write(definition_path(), windows_runner(spec)).map_err(|e| e.to_string())?;
        run(
            "powershell.exe",
            &[
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                &windows_registration(spec),
            ],
        )?;
        return run("schtasks.exe", &["/Run", "/TN", TASK]);
    }
    #[allow(unreachable_code)]
    Err("Webconsole background services are unsupported on this platform".into())
}

fn stop_service() -> Result<(), String> {
    #[cfg(target_os = "linux")]
    return run("systemctl", &["--user", "stop", UNIT]);
    #[cfg(target_os = "macos")]
    {
        let domain = format!("gui/{}/{}", unsafe { libc::geteuid() }, LABEL);
        return gateway_service::launchd_stop_with(&domain, gateway_service::launchd_command);
    }
    #[cfg(target_os = "windows")]
    return run("powershell.exe", &["-NoProfile", "-NonInteractive", "-Command",
        &format!("$ErrorActionPreference='Stop'; $t=Get-ScheduledTask -TaskName '{TASK}'; if ($t.State -eq 'Running') {{ Stop-ScheduledTask -InputObject $t }}")]);
    #[allow(unreachable_code)]
    Err("Webconsole background services are unsupported on this platform".into())
}

#[doc(hidden)]
pub fn systemd_unit(spec: &Spec) -> String {
    let command = std::iter::once(spec.executable.display().to_string())
        .chain(spec.args())
        .map(|v| systemd_quote(&v).replace('$', "$$"))
        .collect::<Vec<_>>()
        .join(" ");
    let log = systemd_quote(&format!(
        "append:{}",
        spec.home.join("webconsole.log").display()
    ));
    format!("[Unit]\nDescription=Nexus Webconsole\nRequires=nexus-gateway.service\nAfter=nexus-gateway.service\nPartOf=nexus-gateway.service\n\n[Service]\nType=simple\nExecStart={command}\nEnvironment={}\nRestart=on-failure\nRestartSec=2\nStandardOutput={log}\nStandardError={log}\n\n[Install]\nWantedBy=default.target\n",
        systemd_quote(&format!("PATH={}", spec.path)))
}

#[doc(hidden)]
pub fn launchd_plist(spec: &Spec) -> String {
    let command = std::iter::once(spec.executable.display().to_string())
        .chain(spec.args())
        .map(|v| format!("<string>{}</string>", xml(&v)))
        .collect::<String>();
    let log = xml(&spec.home.join("webconsole.log").display().to_string());
    format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?><plist version=\"1.0\"><dict><key>Label</key><string>{LABEL}</string><key>ProgramArguments</key><array>{command}</array><key>RunAtLoad</key><true/><key>KeepAlive</key><true/><key>EnvironmentVariables</key><dict><key>PATH</key><string>{}</string></dict><key>StandardOutPath</key><string>{log}</string><key>StandardErrorPath</key><string>{log}</string></dict></plist>", xml(&spec.path))
}

#[doc(hidden)]
pub fn windows_runner(spec: &Spec) -> String {
    let args = spec
        .args()
        .iter()
        .map(|arg| ps(arg))
        .collect::<Vec<_>>()
        .join(" ");
    format!(
        "$env:PATH={}\n& {} {args} >> {} 2>&1\nexit $LASTEXITCODE\n",
        ps(&spec.path),
        ps(&spec.executable.display().to_string()),
        ps(&spec.home.join("webconsole.log").display().to_string())
    )
}

#[doc(hidden)]
pub fn windows_registration(spec: &Spec) -> String {
    let runner = spec.home.join("webconsole-task.ps1");
    let args = format!(
        "-NoProfile -NonInteractive -ExecutionPolicy Bypass -File \"{}\"",
        runner.display()
    );
    format!("$ErrorActionPreference='Stop'; $a=New-ScheduledTaskAction -Execute 'powershell.exe' -Argument {}; $t=New-ScheduledTaskTrigger -AtLogOn; $s=New-ScheduledTaskSettingsSet -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1) -ExecutionTimeLimit ([TimeSpan]::Zero); $p=New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited; Register-ScheduledTask -TaskName '{TASK}' -Action $a -Trigger $t -Settings $s -Principal $p -Force | Out-Null", ps(&args))
}

fn systemd_quote(value: &str) -> String {
    format!(
        "\"{}\"",
        value
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('%', "%%")
    )
}
fn xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
fn ps(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}
