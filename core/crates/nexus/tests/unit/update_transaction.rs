use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use async_trait::async_trait;

use nexus::update::transaction::{
    execute_transaction, InstallContext, InstallMethod, InstalledFacet, ServiceFacet,
    ServiceSnapshot, UpdatePort, UpdateStatus,
};

#[derive(Default)]
struct FakePort {
    calls: Vec<String>,
    current: String,
    target: String,
    running: BTreeSet<ServiceFacet>,
    fail: BTreeMap<String, String>,
    receipts: Vec<String>,
}

impl FakePort {
    fn ready(current: &str, target: &str, running: &[ServiceFacet]) -> Self {
        Self {
            current: current.into(),
            target: target.into(),
            running: running.iter().copied().collect(),
            ..Self::default()
        }
    }

    fn fail_at(mut self, operation: &str, message: &str) -> Self {
        self.fail.insert(operation.into(), message.into());
        self
    }

    fn operation(&mut self, name: &str) -> Result<(), String> {
        self.calls.push(name.into());
        self.fail.remove(name).map_or(Ok(()), Err)
    }
}

#[async_trait(?Send)]
impl UpdatePort for FakePort {
    fn acquire_lock(&mut self, target: Option<&str>) -> Result<(), String> {
        self.operation(&format!("lock:{}", target.unwrap_or("unknown")))
    }

    fn current_version(&mut self) -> Result<String, String> {
        self.calls.push("current".into());
        Ok(self.current.clone())
    }

    fn target_version(&mut self) -> Result<String, String> {
        self.calls.push("target".into());
        Ok(self.target.clone())
    }

    fn capture_services(&mut self) -> Result<ServiceSnapshot, String> {
        self.calls.push("snapshot".into());
        Ok(ServiceSnapshot::from_running(self.running.iter().copied()))
    }

    fn install_exact(&mut self, version: &str) -> Result<(), String> {
        self.operation(&format!("install:{version}"))
    }

    fn verify_installation(&mut self, version: &str) -> Result<(), String> {
        self.operation(&format!("verify:{version}"))
    }

    fn rewrite_services(&mut self) -> Result<(), String> {
        self.operation("rewrite")
    }

    async fn restore_service(&mut self, service: ServiceFacet) -> Result<(), String> {
        self.operation(&format!("restore:{service}"))
    }

    async fn verify_service(&mut self, service: ServiceFacet) -> Result<(), String> {
        self.operation(&format!("health:{service}"))
    }

    fn rollback_exact(&mut self, version: &str) -> Result<(), String> {
        self.operation(&format!("rollback:{version}"))
    }

    fn recovery_command(&self, version: &str) -> String {
        format!("recover {version}")
    }

    fn write_receipt(&mut self, report_json: &str) -> Result<(), String> {
        self.calls.push("receipt".into());
        self.receipts.push(report_json.into());
        Ok(())
    }
}

fn context() -> InstallContext {
    InstallContext {
        method: InstallMethod::Npm,
        managed_package: Some("@egregore/nexus".into()),
        managed_root: Some(PathBuf::from("/managed/nexus")),
        launcher_path: Some(PathBuf::from("/managed/nexus/bin/nexus.mjs")),
        executable: PathBuf::from("/managed/nexus/bin/nexus"),
        facets: [
            InstalledFacet::Cli,
            InstalledFacet::Daemon,
            InstalledFacet::Gateway,
            InstalledFacet::Webconsole,
        ]
        .into_iter()
        .collect(),
        wsl: false,
    }
}

#[tokio::test]
async fn current_version_is_a_locked_noop() {
    let mut port = FakePort::ready("0.1.2", "0.1.2", &[ServiceFacet::Daemon]);
    let result = execute_transaction(&context(), &mut port, false).await;

    assert_eq!(result.report.status, UpdateStatus::Current);
    assert_eq!(result.exit_code, 0);
    assert_eq!(port.calls, ["current", "lock:unknown", "target", "receipt"]);
}

#[tokio::test]
async fn check_reports_available_without_mutating() {
    let mut port = FakePort::ready("0.1.2", "0.1.3", &[ServiceFacet::Daemon]);
    let result = execute_transaction(&context(), &mut port, true).await;

    assert_eq!(result.report.status, UpdateStatus::Available);
    assert_eq!(result.exit_code, 10);
    assert!(!port.calls.iter().any(|call| call.starts_with("install:")));
}

#[tokio::test]
async fn success_restores_only_running_services_in_dependency_order() {
    let mut port = FakePort::ready(
        "0.1.2",
        "0.1.3",
        &[ServiceFacet::Webconsole, ServiceFacet::Daemon],
    );
    let result = execute_transaction(&context(), &mut port, false).await;

    assert_eq!(result.report.status, UpdateStatus::Updated);
    assert_eq!(result.exit_code, 0);
    let restores = port
        .calls
        .iter()
        .filter(|call| call.starts_with("restore:"))
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(restores, ["restore:daemon", "restore:webconsole"]);
    assert!(!port.calls.contains(&"restore:gateway".into()));
}

#[tokio::test]
async fn health_failure_rolls_back_exactly_and_restores_prior_state() {
    let mut port = FakePort::ready(
        "0.1.2",
        "0.1.3",
        &[ServiceFacet::Daemon, ServiceFacet::Gateway],
    )
    .fail_at("health:gateway", "gateway unhealthy");
    let result = execute_transaction(&context(), &mut port, false).await;

    assert_eq!(result.report.status, UpdateStatus::RolledBack);
    assert_eq!(result.exit_code, 1);
    assert!(result.report.rollback.attempted);
    assert!(result.report.rollback.succeeded);
    assert!(port.calls.contains(&"rollback:0.1.2".into()));
    assert_eq!(
        port.calls
            .iter()
            .filter(|call| call.as_str() == "restore:daemon")
            .count(),
        2
    );
}

#[tokio::test]
async fn rollback_failure_is_actionable_and_receipt_is_redacted() {
    let mut port = FakePort::ready("0.1.2", "0.1.3", &[ServiceFacet::Daemon])
        .fail_at("verify:0.1.3", "token=npm_secret")
        .fail_at("rollback:0.1.2", "registry password leaked");
    let result = execute_transaction(&context(), &mut port, false).await;

    assert_eq!(result.report.status, UpdateStatus::Failed);
    assert_eq!(result.exit_code, 1);
    let error = result.report.error.expect("actionable error");
    assert_eq!(error.recovery_command.as_deref(), Some("recover 0.1.2"));
    let receipt = port.receipts.last().expect("settled receipt");
    assert!(!receipt.contains("npm_secret"));
    assert!(!receipt.contains("password leaked"));
}
