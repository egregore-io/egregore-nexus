#![cfg(test)]

use std::ffi::OsString;
use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

use tempfile::TempDir;

use super::*;

#[test]
fn explicit_gateway_binary_wins_over_path() {
    let temp = TempDir::new().unwrap();
    let explicit = temp.path().join("chosen-gateway");
    fs::write(&explicit, "fake").unwrap();
    make_executable(&explicit);

    let installation = resolve_gateway_installation(
        Some(explicit.clone().into_os_string()),
        Some(OsString::from("")),
        false,
    )
    .unwrap();

    assert_eq!(installation.executable, explicit);
    assert_eq!(installation.invocation, GatewayInvocation::Direct);
}

#[test]
fn missing_gateway_is_typed_and_actionable() {
    let error = resolve_gateway_installation(None, Some(OsString::from("")), false).unwrap_err();

    assert_eq!(error.code(), "GATEWAY_NOT_INSTALLED");
    assert!(error.hint().unwrap().contains("@egregore/nexus-gateway"));
}

#[test]
fn path_resolution_finds_gateway_command() {
    let temp = TempDir::new().unwrap();
    let executable = temp.path().join("nexus-gateway");
    fs::write(&executable, "fake").unwrap();
    make_executable(&executable);

    let installation = resolve_gateway_installation(
        None,
        Some(std::env::join_paths([temp.path()]).unwrap()),
        false,
    )
    .unwrap();

    assert_eq!(installation.executable, executable);
}

#[test]
fn windows_path_resolution_marks_cmd_shim() {
    let temp = TempDir::new().unwrap();
    let executable = temp.path().join("nexus-gateway.cmd");
    fs::write(&executable, "@echo off").unwrap();

    let installation = resolve_gateway_installation(
        None,
        Some(std::env::join_paths([temp.path()]).unwrap()),
        true,
    )
    .unwrap();

    assert_eq!(installation.executable, executable);
    assert_eq!(
        installation.invocation,
        GatewayInvocation::WindowsCommandShim
    );
}

#[test]
fn cold_gateway_start_has_a_full_minute_readiness_budget() {
    let source = include_str!("../../src/gateway_lifecycle.rs");

    assert!(
        source.contains("GATEWAY_READY_TIMEOUT: Duration = Duration::from_secs(60)"),
        "a capped cold gateway build can exceed the old 12-second readiness window"
    );
}

#[test]
fn gateway_runtime_receives_daemon_ipc_home_not_a_store_url() {
    let source = include_str!("../../src/gateway_lifecycle.rs");

    assert!(source.contains(".env(\"NEXUS_HOME\", &self.paths.home)"));
    assert!(
        !source.contains(".env(\"NEXUS_DB_URL\""),
        "the gateway must not receive a direct durable-store endpoint"
    );
}

#[test]
fn gateway_health_tolerates_one_loaded_event_loop_delay() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request = [0_u8; 256];
        let _ = stream.read(&mut request);
        std::thread::sleep(Duration::from_millis(900));
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
            .unwrap();
    });

    assert!(health_ok(&format!("http://{address}")));
    server.join().unwrap();
}

#[test]
fn gateway_uses_shared_detach_and_bounded_migration_boundaries() {
    let source = include_str!("../../src/gateway_lifecycle.rs");

    assert!(source.contains("lifecycle_process::spawn_detached"));
    assert!(source.contains("GATEWAY_MIGRATION_TIMEOUT"));
    assert!(source.contains("run_gateway_store_migration"));
    assert!(!source.contains("fn detach_command("));
}

#[cfg(unix)]
#[test]
fn gateway_store_migration_timeout_is_typed_and_bounded() {
    let mut command = std::process::Command::new("sh");
    command.args(["-c", "sleep 60"]);
    let started = std::time::Instant::now();

    let error = run_gateway_store_migration(&mut command, Duration::from_millis(150)).unwrap_err();

    assert!(error.message().contains("timed out"));
    assert!(started.elapsed() < Duration::from_secs(3));
}

struct FakeBackend {
    installed: bool,
    migration_fails: bool,
    state: Mutex<GatewayRuntimeStatus>,
    operations: Mutex<Vec<&'static str>>,
}

impl FakeBackend {
    fn installed(state: GatewayRuntimeStatus) -> Self {
        Self {
            installed: true,
            migration_fails: false,
            state: Mutex::new(state),
            operations: Mutex::new(Vec::new()),
        }
    }

    fn missing() -> Self {
        Self {
            installed: false,
            migration_fails: false,
            state: Mutex::new(GatewayRuntimeStatus::Down),
            operations: Mutex::new(Vec::new()),
        }
    }

    fn with_migration_failure(mut self) -> Self {
        self.migration_fails = true;
        self
    }

    fn operations(&self) -> Vec<&'static str> {
        self.operations.lock().unwrap().clone()
    }
}

#[async_trait::async_trait(?Send)]
impl GatewayBackend for FakeBackend {
    fn resolve(&self) -> Result<GatewayInstallation, GatewayLifecycleError> {
        self.operations.lock().unwrap().push("resolve");
        if !self.installed {
            return Err(GatewayLifecycleError::not_installed());
        }
        Ok(GatewayInstallation {
            executable: PathBuf::from("nexus-gateway"),
            invocation: GatewayInvocation::Direct,
        })
    }

    async fn ensure_daemon(&self) -> Result<(), GatewayLifecycleError> {
        self.operations.lock().unwrap().push("ensure_daemon");
        Ok(())
    }

    fn prepare_store(
        &self,
        _installation: &GatewayInstallation,
    ) -> Result<(), GatewayLifecycleError> {
        self.operations.lock().unwrap().push("prepare_store");
        if self.migration_fails {
            Err(GatewayLifecycleError::lifecycle("migration failed"))
        } else {
            Ok(())
        }
    }

    fn status(&self) -> GatewayRuntimeStatus {
        self.operations.lock().unwrap().push("status");
        self.state.lock().unwrap().clone()
    }

    fn clear_stale(&self) -> Result<(), GatewayLifecycleError> {
        self.operations.lock().unwrap().push("clear_stale");
        *self.state.lock().unwrap() = GatewayRuntimeStatus::Down;
        Ok(())
    }

    fn spawn(&self, _installation: &GatewayInstallation) -> Result<(), GatewayLifecycleError> {
        self.operations.lock().unwrap().push("spawn");
        Ok(())
    }

    async fn wait_ready(&self) -> Result<GatewayRuntimeStatus, GatewayLifecycleError> {
        self.operations.lock().unwrap().push("wait_ready");
        let live = GatewayRuntimeStatus::Live {
            pid: 41,
            url: "http://localhost:4100".into(),
        };
        *self.state.lock().unwrap() = live.clone();
        Ok(live)
    }

    fn stop(&self, _force: bool) -> Result<(), GatewayLifecycleError> {
        self.operations.lock().unwrap().push("stop");
        *self.state.lock().unwrap() = GatewayRuntimeStatus::Down;
        Ok(())
    }
}

#[tokio::test]
async fn missing_gateway_does_not_ensure_daemon() {
    let backend = FakeBackend::missing();

    let error = start_with(&backend).await.unwrap_err();

    assert_eq!(error.code(), "GATEWAY_NOT_INSTALLED");
    assert_eq!(backend.operations(), ["resolve"]);
}

#[tokio::test]
async fn start_ensures_daemon_before_gateway_spawn() {
    let backend = FakeBackend::installed(GatewayRuntimeStatus::Down);

    let result = start_with(&backend).await.unwrap();

    assert!(matches!(result, GatewayRuntimeStatus::Live { .. }));
    assert_eq!(
        backend.operations(),
        [
            "resolve",
            "ensure_daemon",
            "prepare_store",
            "status",
            "spawn",
            "wait_ready"
        ]
    );
}

#[tokio::test]
async fn failed_store_migration_prevents_gateway_spawn() {
    let backend = FakeBackend::installed(GatewayRuntimeStatus::Down).with_migration_failure();

    let error = start_with(&backend).await.unwrap_err();

    assert!(error.message().contains("migration failed"));
    assert_eq!(
        backend.operations(),
        ["resolve", "ensure_daemon", "prepare_store"]
    );
}

#[tokio::test]
async fn live_start_is_idempotent_but_still_ensures_daemon() {
    let backend = FakeBackend::installed(GatewayRuntimeStatus::Live {
        pid: 41,
        url: "http://localhost:4100".into(),
    });

    start_with(&backend).await.unwrap();

    assert_eq!(
        backend.operations(),
        ["resolve", "ensure_daemon", "prepare_store", "status"]
    );
}

#[tokio::test]
async fn start_never_signals_a_degraded_discovery_pid() {
    let backend = FakeBackend::installed(GatewayRuntimeStatus::Degraded {
        reason: "health probe timed out".into(),
    });

    start_with(&backend).await.unwrap();

    assert_eq!(
        backend.operations(),
        [
            "resolve",
            "ensure_daemon",
            "prepare_store",
            "status",
            "clear_stale",
            "spawn",
            "wait_ready"
        ]
    );
}

#[tokio::test]
async fn restart_stops_only_gateway_then_starts_it() {
    let backend = FakeBackend::installed(GatewayRuntimeStatus::Live {
        pid: 41,
        url: "http://localhost:4100".into(),
    });

    restart_with(&backend, false).await.unwrap();

    assert_eq!(
        backend.operations(),
        [
            "resolve",
            "ensure_daemon",
            "prepare_store",
            "status",
            "stop",
            "spawn",
            "wait_ready"
        ]
    );
}

#[tokio::test]
async fn restart_requires_force_before_signalling_a_degraded_discovery_pid() {
    let backend = FakeBackend::installed(GatewayRuntimeStatus::Degraded {
        reason: "health probe timed out".into(),
    });

    let error = restart_with(&backend, false).await.unwrap_err();

    assert!(error.message().contains("--force"));
    assert_eq!(
        backend.operations(),
        ["resolve", "ensure_daemon", "prepare_store", "status"]
    );
}

#[tokio::test]
async fn restart_stops_a_live_but_unhealthy_gateway_before_respawn() {
    let backend = FakeBackend::installed(GatewayRuntimeStatus::Degraded {
        reason: "health probe timed out".into(),
    });

    restart_with(&backend, true).await.unwrap();

    assert_eq!(
        backend.operations(),
        [
            "resolve",
            "ensure_daemon",
            "prepare_store",
            "status",
            "stop",
            "spawn",
            "wait_ready"
        ]
    );
}

#[test]
fn stop_terminates_a_live_but_unhealthy_gateway() {
    let backend = FakeBackend::installed(GatewayRuntimeStatus::Degraded {
        reason: "health probe timed out".into(),
    });

    stop_with(&backend, true).unwrap();

    assert_eq!(backend.operations(), ["resolve", "status", "stop"]);
}

#[test]
fn stop_requires_force_before_signalling_a_degraded_discovery_pid() {
    let backend = FakeBackend::installed(GatewayRuntimeStatus::Degraded {
        reason: "health probe timed out".into(),
    });

    let error = stop_with(&backend, false).unwrap_err();

    assert!(error.message().contains("--force"));
    assert_eq!(backend.operations(), ["resolve", "status"]);
}

#[cfg(unix)]
fn make_executable(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;
    let mut permissions = fs::metadata(path).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).unwrap();
}

#[cfg(windows)]
fn make_executable(_path: &std::path::Path) {}
