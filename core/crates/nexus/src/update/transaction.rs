//! Transactional updater state machine.

use std::collections::BTreeSet;
use std::fmt;

use async_trait::async_trait;
use serde::Serialize;

pub use super::install_context::{InstallContext, InstallMethod, InstalledFacet};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceFacet {
    Daemon,
    Gateway,
    Webconsole,
}

impl fmt::Display for ServiceFacet {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Daemon => "daemon",
            Self::Gateway => "gateway",
            Self::Webconsole => "webconsole",
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ServiceSnapshot {
    running: BTreeSet<ServiceFacet>,
}

impl ServiceSnapshot {
    pub fn from_running(services: impl IntoIterator<Item = ServiceFacet>) -> Self {
        Self {
            running: services.into_iter().collect(),
        }
    }

    pub fn is_running(&self, service: ServiceFacet) -> bool {
        self.running.contains(&service)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UpdateStatus {
    Current,
    Available,
    Updated,
    RolledBack,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceAction {
    Restored,
    KeptStopped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceTransition {
    pub service: ServiceFacet,
    pub was_running: bool,
    pub action: ServiceAction,
    pub healthy: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RollbackReport {
    pub attempted: bool,
    pub succeeded: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateErrorReport {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failed_facet: Option<ServiceFacet>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recovery_command: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateReport {
    pub version: u8,
    pub status: UpdateStatus,
    pub install_method: InstallMethod,
    pub current_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_version: Option<String>,
    pub facets: Vec<InstalledFacet>,
    pub services: Vec<ServiceTransition>,
    pub rollback: RollbackReport,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<UpdateErrorReport>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateOutcome {
    pub report: UpdateReport,
    pub exit_code: u8,
}

#[async_trait(?Send)]
pub trait UpdatePort {
    fn acquire_lock(&mut self, target: Option<&str>) -> Result<(), String>;
    fn current_version(&mut self) -> Result<String, String>;
    fn target_version(&mut self) -> Result<String, String>;
    fn capture_services(&mut self) -> Result<ServiceSnapshot, String>;
    fn install_exact(&mut self, version: &str) -> Result<(), String>;
    fn verify_installation(&mut self, version: &str) -> Result<(), String>;
    fn rewrite_services(&mut self) -> Result<(), String>;
    async fn restore_service(&mut self, service: ServiceFacet) -> Result<(), String>;
    async fn verify_service(&mut self, service: ServiceFacet) -> Result<(), String>;
    fn rollback_exact(&mut self, version: &str) -> Result<(), String>;
    fn recovery_command(&self, version: &str) -> String;
    fn write_receipt(&mut self, report_json: &str) -> Result<(), String>;
}

pub async fn execute_transaction<P: UpdatePort>(
    context: &InstallContext,
    port: &mut P,
    check_only: bool,
) -> UpdateOutcome {
    let current = match port.current_version() {
        Ok(version) => version,
        Err(_) => return failed_outcome(context, "unknown", None, "CURRENT_VERSION_FAILED", None),
    };
    if port.acquire_lock(None).is_err() {
        return failed_outcome(context, &current, None, "UPDATE_LOCKED", None);
    }
    let target = match port.target_version() {
        Ok(version) => version,
        Err(_) => {
            let mut result = failed_outcome(
                context,
                &current,
                None,
                "UPDATE_CHECK_FAILED",
                Some(port.recovery_command(&current)),
            );
            settle_receipt(port, &mut result);
            return result;
        }
    };

    if current == target {
        let mut result = outcome(context, &current, Some(target), UpdateStatus::Current, 0);
        settle_receipt(port, &mut result);
        return result;
    }
    if check_only {
        let mut result = outcome(context, &current, Some(target), UpdateStatus::Available, 10);
        settle_receipt(port, &mut result);
        return result;
    }

    let snapshot = match port.capture_services() {
        Ok(snapshot) => snapshot,
        Err(_) => {
            let mut result = failed_outcome(
                context,
                &current,
                Some(target),
                "SERVICE_SNAPSHOT_FAILED",
                Some(port.recovery_command(&current)),
            );
            settle_receipt(port, &mut result);
            return result;
        }
    };
    if port.install_exact(&target).is_err() {
        let mut result = failed_outcome(
            context,
            &current,
            Some(target),
            "INSTALL_FAILED",
            Some(port.recovery_command(&current)),
        );
        settle_receipt(port, &mut result);
        return result;
    }

    let post_install = apply_and_restore(port, &snapshot, &target).await;
    let mut result = match post_install {
        Ok(services) => {
            let mut result = outcome(context, &current, Some(target), UpdateStatus::Updated, 0);
            result.report.services = services;
            result
        }
        Err(failed_facet) => {
            rollback(context, port, &snapshot, &current, &target, failed_facet).await
        }
    };
    settle_receipt(port, &mut result);
    result
}

async fn apply_and_restore<P: UpdatePort>(
    port: &mut P,
    snapshot: &ServiceSnapshot,
    version: &str,
) -> Result<Vec<ServiceTransition>, Option<ServiceFacet>> {
    port.verify_installation(version).map_err(|_| None)?;
    port.rewrite_services().map_err(|_| None)?;
    restore_snapshot(port, snapshot).await
}

async fn restore_snapshot<P: UpdatePort>(
    port: &mut P,
    snapshot: &ServiceSnapshot,
) -> Result<Vec<ServiceTransition>, Option<ServiceFacet>> {
    let mut transitions = Vec::new();
    for service in [
        ServiceFacet::Daemon,
        ServiceFacet::Gateway,
        ServiceFacet::Webconsole,
    ] {
        if !snapshot.is_running(service) {
            transitions.push(ServiceTransition {
                service,
                was_running: false,
                action: ServiceAction::KeptStopped,
                healthy: true,
            });
            continue;
        }
        port.restore_service(service)
            .await
            .map_err(|_| Some(service))?;
        port.verify_service(service)
            .await
            .map_err(|_| Some(service))?;
        transitions.push(ServiceTransition {
            service,
            was_running: true,
            action: ServiceAction::Restored,
            healthy: true,
        });
    }
    Ok(transitions)
}

async fn rollback<P: UpdatePort>(
    context: &InstallContext,
    port: &mut P,
    snapshot: &ServiceSnapshot,
    current: &str,
    target: &str,
    failed_facet: Option<ServiceFacet>,
) -> UpdateOutcome {
    let recovery = port.recovery_command(current);
    let rollback_succeeded = port.rollback_exact(current).is_ok()
        && port.verify_installation(current).is_ok()
        && port.rewrite_services().is_ok()
        && restore_snapshot(port, snapshot).await.is_ok();
    let mut result = outcome(
        context,
        current,
        Some(target.into()),
        if rollback_succeeded {
            UpdateStatus::RolledBack
        } else {
            UpdateStatus::Failed
        },
        1,
    );
    result.report.rollback = RollbackReport {
        attempted: true,
        succeeded: rollback_succeeded,
        version: Some(current.into()),
    };
    result.report.error = Some(UpdateErrorReport {
        code: if rollback_succeeded {
            "POST_INSTALL_HEALTH_FAILED"
        } else {
            "ROLLBACK_FAILED"
        }
        .into(),
        message: if rollback_succeeded {
            "the update failed verification; the previous version was restored"
        } else {
            "the update and automatic rollback failed; run the recovery command"
        }
        .into(),
        failed_facet,
        log_path: None,
        recovery_command: Some(recovery),
    });
    result
}

fn outcome(
    context: &InstallContext,
    current: &str,
    target: Option<String>,
    status: UpdateStatus,
    exit_code: u8,
) -> UpdateOutcome {
    UpdateOutcome {
        exit_code,
        report: UpdateReport {
            version: 1,
            status,
            install_method: context.method,
            current_version: current.into(),
            target_version: target,
            facets: context.facets.iter().copied().collect(),
            services: Vec::new(),
            rollback: RollbackReport::default(),
            error: None,
        },
    }
}

fn failed_outcome(
    context: &InstallContext,
    current: &str,
    target: Option<String>,
    code: &str,
    recovery_command: Option<String>,
) -> UpdateOutcome {
    let mut result = outcome(context, current, target, UpdateStatus::Failed, 1);
    result.report.error = Some(UpdateErrorReport {
        code: code.into(),
        message: "the update operation failed; no credential-bearing command output was retained"
            .into(),
        failed_facet: None,
        log_path: None,
        recovery_command,
    });
    result
}

fn settle_receipt<P: UpdatePort>(port: &mut P, result: &mut UpdateOutcome) {
    let json = serde_json::to_string_pretty(&result.report)
        .unwrap_or_else(|_| "{\"version\":1,\"status\":\"failed\"}".into());
    if port.write_receipt(&json).is_err() && result.exit_code == 0 {
        result.exit_code = 1;
        result.report.status = UpdateStatus::Failed;
        result.report.error = Some(UpdateErrorReport {
            code: "RECEIPT_WRITE_FAILED".into(),
            message: "the update settled but its local receipt could not be written".into(),
            failed_facet: None,
            log_path: None,
            recovery_command: None,
        });
    }
}
