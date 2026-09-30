use std::path::PathBuf;

use async_trait::async_trait;
use nexus::webconsole_lifecycle::{
    build_webconsole_spawn_command, classify_webconsole_start_error, launch_webconsole_with,
    resolve_webconsole_installation, start_webconsole_with, stop_webconsole_with,
    WebconsoleBackend, WebconsoleInstallation, WebconsoleInvocation, WebconsoleRuntimeStatus,
    WebconsoleStartOptions,
};

struct FakeBackend {
    calls: Vec<&'static str>,
    status: WebconsoleRuntimeStatus,
    recovery: Result<Option<WebconsoleRuntimeStatus>, String>,
}

impl FakeBackend {
    fn down() -> Self {
        Self {
            calls: Vec::new(),
            status: WebconsoleRuntimeStatus::Down,
            recovery: Ok(None),
        }
    }
}

#[async_trait(?Send)]
impl WebconsoleBackend for FakeBackend {
    fn resolve(&mut self) -> Result<WebconsoleInstallation, String> {
        self.calls.push("resolve");
        Ok(WebconsoleInstallation {
            executable: PathBuf::from("/usr/bin/nexus-webui"),
            invocation: WebconsoleInvocation::Direct,
        })
    }

    async fn ensure_gateway(&mut self) -> Result<String, String> {
        self.calls.push("ensure-gateway");
        Ok("http://127.0.0.1:4100".into())
    }

    fn status(&mut self) -> WebconsoleRuntimeStatus {
        self.calls.push("status");
        self.status.clone()
    }

    fn clear_stale(&mut self) -> Result<(), String> {
        self.calls.push("clear-stale");
        Ok(())
    }

    fn recover(
        &mut self,
        _installation: &WebconsoleInstallation,
        _gateway_url: &str,
        _options: &WebconsoleStartOptions,
    ) -> Result<Option<WebconsoleRuntimeStatus>, String> {
        self.calls.push("recover");
        self.recovery.clone()
    }

    fn spawn(
        &mut self,
        _installation: &WebconsoleInstallation,
        _gateway_url: &str,
        _options: &WebconsoleStartOptions,
    ) -> Result<(), String> {
        self.calls.push("spawn");
        Ok(())
    }

    async fn wait_ready(&mut self) -> Result<WebconsoleRuntimeStatus, String> {
        self.calls.push("health");
        Ok(live())
    }

    fn open_browser(&mut self, _url: &str) -> Result<(), String> {
        self.calls.push("open-browser");
        Ok(())
    }

    fn stop(&mut self, _force: bool) -> Result<(), String> {
        self.calls.push("stop");
        Ok(())
    }
}

#[tokio::test]
async fn start_ensures_gateway_before_spawning() {
    let mut backend = FakeBackend::down();
    let runtime = start_webconsole_with(&mut backend, &options())
        .await
        .unwrap();

    assert_eq!(runtime, live());
    assert_eq!(
        backend.calls,
        [
            "resolve",
            "ensure-gateway",
            "status",
            "recover",
            "spawn",
            "health"
        ]
    );
}

#[tokio::test]
async fn healthy_process_is_reused_without_duplicate_spawn() {
    let mut backend = FakeBackend {
        calls: Vec::new(),
        status: live(),
        recovery: Ok(None),
    };
    let runtime = start_webconsole_with(&mut backend, &options())
        .await
        .unwrap();

    assert_eq!(runtime, live());
    assert!(!backend.calls.contains(&"spawn"));
}

#[tokio::test]
async fn missing_discovery_adopts_a_verified_live_webconsole_without_spawning() {
    let mut backend = FakeBackend {
        calls: Vec::new(),
        status: WebconsoleRuntimeStatus::Down,
        recovery: Ok(Some(live())),
    };

    let runtime = start_webconsole_with(&mut backend, &options())
        .await
        .unwrap();

    assert_eq!(runtime, live());
    assert_eq!(
        backend.calls,
        ["resolve", "ensure-gateway", "status", "recover"]
    );
    assert!(!backend.calls.contains(&"spawn"));
}

#[tokio::test]
async fn occupied_port_diagnosis_is_immediate_and_never_spawns() {
    let mut backend = FakeBackend {
        calls: Vec::new(),
        status: WebconsoleRuntimeStatus::Down,
        recovery: Err("Webconsole port 4200 is occupied by an untracked process".into()),
    };

    let error = start_webconsole_with(&mut backend, &options())
        .await
        .unwrap_err();

    assert!(error.contains("occupied"));
    assert_eq!(
        backend.calls,
        ["resolve", "ensure-gateway", "status", "recover"]
    );
    assert!(!backend.calls.contains(&"spawn"));
}

#[test]
fn occupied_port_failure_has_a_stable_machine_code_and_endpoint_data() {
    let error = classify_webconsole_start_error(
        "Webconsole port 4200 is occupied by an unverified process",
        &options(),
    );

    assert_eq!(error.code(), "WEBCONSOLE_PORT_OCCUPIED");
    assert_eq!(
        error.data(),
        Some(&serde_json::json!({"host": "127.0.0.1", "port": 4200}))
    );
}

#[tokio::test]
async fn launch_opens_only_after_health_and_respects_no_open() {
    let mut open = FakeBackend::down();
    launch_webconsole_with(&mut open, &options(), false)
        .await
        .unwrap();
    assert_eq!(open.calls.last(), Some(&"open-browser"));

    let mut closed = FakeBackend::down();
    launch_webconsole_with(&mut closed, &options(), true)
        .await
        .unwrap();
    assert!(!closed.calls.contains(&"open-browser"));
}

#[test]
fn stop_never_touches_gateway() {
    let mut backend = FakeBackend {
        calls: Vec::new(),
        status: live(),
        recovery: Ok(None),
    };
    stop_webconsole_with(&mut backend, false).unwrap();
    assert_eq!(backend.calls, ["status", "stop", "clear-stale"]);
}

#[cfg(unix)]
#[test]
fn explicit_webconsole_symlink_resolves_to_the_spawned_executable() {
    use std::os::unix::fs::symlink;

    let directory = tempfile::tempdir().unwrap();
    let executable = directory.path().join("nexus-webui.mjs");
    let launcher = directory.path().join("nexus-webui");
    std::fs::write(&executable, "#!/usr/bin/env node\n").unwrap();
    symlink(&executable, &launcher).unwrap();

    let installation =
        resolve_webconsole_installation(Some(launcher.as_os_str().to_os_string()), None, false)
            .unwrap();

    assert_eq!(installation.executable, executable.canonicalize().unwrap());
    assert_eq!(installation.invocation, WebconsoleInvocation::Direct);
}

#[test]
fn windows_webconsole_command_files_use_the_native_command_shim() {
    for extension in ["cmd", "bat"] {
        let directory = tempfile::tempdir().unwrap();
        let launcher = directory.path().join(format!("nexus-webui.{extension}"));
        std::fs::write(&launcher, "@echo off\r\n").unwrap();

        let installation =
            resolve_webconsole_installation(Some(launcher.clone().into_os_string()), None, true)
                .unwrap();

        assert_eq!(installation.executable, launcher);
        assert_eq!(
            installation.invocation,
            WebconsoleInvocation::WindowsCommandShim
        );
    }
}

#[cfg(unix)]
#[test]
fn windows_command_shims_with_spaced_paths_preserve_the_start_argv() {
    use std::os::unix::fs::PermissionsExt;

    for extension in ["cmd", "bat"] {
        let directory = tempfile::tempdir().unwrap();
        let shim_dir = directory.path().join("command shim");
        let install_dir = directory.path().join("Program Files/Egregore Nexus");
        std::fs::create_dir_all(&shim_dir).unwrap();
        std::fs::create_dir_all(&install_dir).unwrap();
        let launcher = install_dir.join(format!("nexus-webui.{extension}"));
        std::fs::write(&launcher, "@echo off\r\n").unwrap();
        let capture = directory.path().join(format!("{extension}-argv.txt"));
        let cmd = shim_dir.join("cmd.exe");
        std::fs::write(
            &cmd,
            "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$CAPTURE_ARGS\"\n",
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&cmd).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&cmd, permissions).unwrap();

        let installation =
            resolve_webconsole_installation(Some(launcher.clone().into_os_string()), None, true)
                .unwrap();
        let discovery = directory.path().join("Nexus Home/webconsole.json");
        let mut command = build_webconsole_spawn_command(
            &installation,
            "http://127.0.0.1:4100",
            &options(),
            &discovery,
        );
        let status = command
            .env("PATH", &shim_dir)
            .env("CAPTURE_ARGS", &capture)
            .status()
            .unwrap();

        assert!(status.success());
        assert_eq!(
            std::fs::read_to_string(&capture)
                .unwrap()
                .lines()
                .collect::<Vec<_>>(),
            vec![
                "/D".to_string(),
                "/S".to_string(),
                "/C".to_string(),
                format!("\"{}\"", launcher.display()),
                "--host".to_string(),
                "127.0.0.1".to_string(),
                "--port".to_string(),
                "4200".to_string(),
                "--gateway-url".to_string(),
                "http://127.0.0.1:4100".to_string(),
                "--discovery".to_string(),
                discovery.display().to_string(),
            ]
        );
    }
}

#[cfg(windows)]
#[test]
fn windows_command_shims_execute_from_spaced_paths_with_intact_start_argv() {
    for extension in ["cmd", "bat"] {
        let directory = tempfile::tempdir().unwrap();
        let install_dir = directory.path().join("Program Files/Egregore Nexus");
        std::fs::create_dir_all(&install_dir).unwrap();
        let launcher = install_dir.join(format!("nexus-webui.{extension}"));
        let capture = directory.path().join(format!("{extension}-argv.txt"));
        std::fs::write(
            &launcher,
            "@echo off\r\n:next\r\nif \"%~1\"==\"\" goto done\r\n>>\"%CAPTURE_ARGS%\" echo %~1\r\nshift\r\ngoto next\r\n:done\r\nexit /b 0\r\n",
        )
        .unwrap();

        let installation =
            resolve_webconsole_installation(Some(launcher.into_os_string()), None, true).unwrap();
        let discovery = directory.path().join("Nexus Home/webconsole.json");
        let mut command = build_webconsole_spawn_command(
            &installation,
            "http://127.0.0.1:4100",
            &options(),
            &discovery,
        );
        let status = command.env("CAPTURE_ARGS", &capture).status().unwrap();

        assert!(status.success());
        assert_eq!(
            std::fs::read_to_string(&capture)
                .unwrap()
                .lines()
                .collect::<Vec<_>>(),
            vec![
                "--host",
                "127.0.0.1",
                "--port",
                "4200",
                "--gateway-url",
                "http://127.0.0.1:4100",
                "--discovery",
                discovery.to_str().unwrap(),
            ]
        );
    }
}

#[test]
fn windows_webconsole_executable_stays_a_direct_invocation() {
    let directory = tempfile::tempdir().unwrap();
    let launcher = directory.path().join("nexus-webui.exe");
    std::fs::write(&launcher, "fixture").unwrap();

    let installation =
        resolve_webconsole_installation(Some(launcher.clone().into_os_string()), None, true)
            .unwrap();

    assert_eq!(installation.executable, launcher);
    assert_eq!(installation.invocation, WebconsoleInvocation::Direct);
}

fn options() -> WebconsoleStartOptions {
    WebconsoleStartOptions {
        host: "127.0.0.1".into(),
        port: 4200,
    }
}

fn live() -> WebconsoleRuntimeStatus {
    WebconsoleRuntimeStatus::Live {
        pid: 42,
        url: "http://127.0.0.1:4200".into(),
        host: "127.0.0.1".into(),
        port: 4200,
        gateway_url: "http://127.0.0.1:4100".into(),
    }
}
