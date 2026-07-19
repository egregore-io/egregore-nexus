use std::path::PathBuf;

use async_trait::async_trait;
use nexus::webconsole_lifecycle::{
    launch_webconsole_with, start_webconsole_with, stop_webconsole_with, WebconsoleBackend,
    WebconsoleInstallation, WebconsoleInvocation, WebconsoleRuntimeStatus, WebconsoleStartOptions,
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
