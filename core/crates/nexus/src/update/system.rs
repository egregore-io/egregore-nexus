//! Operating-system adapter for the transactional updater.

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use async_trait::async_trait;

use crate::daemon::lifecycle::{self, DaemonPaths};
use crate::gateway_lifecycle::{gateway_status, restart_gateway, GatewayRuntimeStatus};
use crate::lifecycle_process::{self, BoundedProcessError};

use super::install_context::{InstallContext, InstallMethod, InstalledFacet};
use super::lock::UpdateLock;
use super::package_manager::{
    package_manager_plan, package_version_probe, CommandPlan, PackageManagerPlan, PackageTools,
};
use super::transaction::{
    execute_transaction, ServiceFacet, ServiceSnapshot, UpdateOutcome, UpdatePort,
};

const QUERY_TIMEOUT: Duration = Duration::from_secs(60);
const INSTALL_TIMEOUT: Duration = Duration::from_secs(15 * 60);
const VERIFY_TIMEOUT: Duration = Duration::from_secs(30);
const COMMAND_OUTPUT_LIMIT: usize = 256 * 1024;

pub async fn execute_system_update(context: &InstallContext, check_only: bool) -> UpdateOutcome {
    let mut port = SystemUpdatePort::new(context.clone());
    execute_transaction(context, &mut port, check_only).await
}

struct SystemUpdatePort {
    context: InstallContext,
    home: PathBuf,
    tools: PackageTools,
    environment: BTreeMap<String, String>,
    current: Option<String>,
    target: Option<String>,
    plan: Option<PackageManagerPlan>,
    lock: Option<UpdateLock>,
    webconsole_restore: Option<crate::webconsole_lifecycle::WebconsoleStartOptions>,
}

impl SystemUpdatePort {
    fn new(context: InstallContext) -> Self {
        Self {
            context,
            home: user_home(),
            tools: PackageTools::detect(),
            environment: env::vars().collect(),
            current: None,
            target: None,
            plan: None,
            lock: None,
            webconsole_restore: None,
        }
    }

    fn ensure_plan(&mut self, target: &str) -> Result<&PackageManagerPlan, String> {
        if self.plan.is_none() {
            let current = self
                .current
                .clone()
                .ok_or_else(|| "current version was not captured".to_string())?;
            self.plan = Some(
                package_manager_plan(
                    &self.context,
                    &self.tools,
                    self.home.clone(),
                    &current,
                    target,
                    &self.environment,
                )
                .map_err(|error| error.to_string())?,
            );
        }
        self.plan
            .as_ref()
            .ok_or_else(|| "package-manager plan unavailable".into())
    }
}

#[async_trait(?Send)]
impl UpdatePort for SystemUpdatePort {
    fn acquire_lock(&mut self, target: Option<&str>) -> Result<(), String> {
        let path = DaemonPaths::resolve().home.join("update.lock");
        self.lock = Some(
            UpdateLock::acquire(
                &path,
                self.context.method,
                self.context.managed_package.clone(),
                target.map(str::to_owned),
            )
            .map_err(|error| error.to_string())?,
        );
        Ok(())
    }

    fn current_version(&mut self) -> Result<String, String> {
        let version = match self.context.method {
            InstallMethod::Npm => read_npm_version(&self.context)?,
            InstallMethod::Cargo => env!("CARGO_PKG_VERSION").into(),
            InstallMethod::Manual | InstallMethod::Development => {
                return Err("automatic update requires an npm or Cargo installation".into())
            }
        };
        self.current = Some(version.clone());
        Ok(version)
    }

    fn target_version(&mut self) -> Result<String, String> {
        let probe = package_version_probe(
            &self.context,
            &self.tools,
            self.home.clone(),
            &self.environment,
        )
        .map_err(|error| error.to_string())?;
        let output = run_command(&probe, QUERY_TIMEOUT)?;
        let version = parse_target_version(self.context.method, &output)?;
        self.target = Some(version.clone());
        Ok(version)
    }

    fn capture_services(&mut self) -> Result<ServiceSnapshot, String> {
        let mut running = Vec::new();
        if lifecycle::dependency_running() {
            running.push(ServiceFacet::Daemon);
        }
        if self.context.facets.contains(&InstalledFacet::Gateway) {
            let status = gateway_status().map_err(|error| error.to_string())?;
            if matches!(status.runtime, GatewayRuntimeStatus::Live { .. }) {
                running.push(ServiceFacet::Gateway);
            }
        }
        if self.context.facets.contains(&InstalledFacet::Webconsole) {
            let status = crate::webconsole_lifecycle::webconsole_status()
                .map_err(|error| error.to_string())?;
            if let crate::webconsole_lifecycle::WebconsoleRuntimeStatus::Live {
                host, port, ..
            } = status.runtime
            {
                self.webconsole_restore =
                    Some(crate::webconsole_lifecycle::WebconsoleStartOptions { host, port });
                running.push(ServiceFacet::Webconsole);
            }
        }
        Ok(ServiceSnapshot::from_running(running))
    }

    fn install_exact(&mut self, version: &str) -> Result<(), String> {
        let command = self.ensure_plan(version)?.install.clone();
        run_command(&command, INSTALL_TIMEOUT).map(|_| ())
    }

    fn verify_installation(&mut self, version: &str) -> Result<(), String> {
        verify_binary(&self.context, version)
    }

    fn rewrite_services(&mut self) -> Result<(), String> {
        lifecycle::rewrite_service_after_update(self.context.executable.clone())
            .map_err(|error| error.to_string())?;
        if self.context.facets.contains(&InstalledFacet::Gateway) {
            crate::gateway_service::rewrite_gateway_service().map_err(|error| error.to_string())?;
        }
        Ok(())
    }

    async fn restore_service(&mut self, service: ServiceFacet) -> Result<(), String> {
        match service {
            ServiceFacet::Daemon => {
                lifecycle::restart_after_update(self.context.executable.clone())
                    .await
                    .map_err(|error| error.to_string())
            }
            ServiceFacet::Gateway => restart_gateway(false)
                .await
                .map(|_| ())
                .map_err(|error| error.to_string()),
            ServiceFacet::Webconsole => crate::webconsole_lifecycle::restart_webconsole(
                self.webconsole_restore.clone().unwrap_or(
                    crate::webconsole_lifecycle::WebconsoleStartOptions {
                        host: "127.0.0.1".into(),
                        port: 4200,
                    },
                ),
            )
            .await
            .map(|_| ())
            .map_err(|error| error.to_string()),
        }
    }

    async fn verify_service(&mut self, service: ServiceFacet) -> Result<(), String> {
        match service {
            ServiceFacet::Daemon if lifecycle::dependency_running() => Ok(()),
            ServiceFacet::Daemon => Err("daemon is down after restart".into()),
            ServiceFacet::Gateway => {
                match gateway_status().map_err(|error| error.to_string())?.runtime {
                    GatewayRuntimeStatus::Live { .. } => Ok(()),
                    other => Err(format!("gateway is not healthy: {other:?}")),
                }
            }
            ServiceFacet::Webconsole => match crate::webconsole_lifecycle::webconsole_status()
                .map_err(|error| error.to_string())?
                .runtime
            {
                crate::webconsole_lifecycle::WebconsoleRuntimeStatus::Live { .. } => Ok(()),
                other => Err(format!("Webconsole is not healthy: {other:?}")),
            },
        }
    }

    fn rollback_exact(&mut self, version: &str) -> Result<(), String> {
        let target = self.target.clone().unwrap_or_else(|| version.to_owned());
        let command = self.ensure_plan(&target)?.rollback.clone();
        run_command(&command, INSTALL_TIMEOUT).map(|_| ())
    }

    fn recovery_command(&self, version: &str) -> String {
        package_manager_plan(
            &self.context,
            &self.tools,
            self.home.clone(),
            version,
            self.target.as_deref().unwrap_or(version),
            &self.environment,
        )
        .map(|plan| plan.recovery_command)
        .unwrap_or_else(|_| "reinstall Nexus using the original package manager".into())
    }

    fn write_receipt(&mut self, report_json: &str) -> Result<(), String> {
        let home = DaemonPaths::resolve().home;
        fs::create_dir_all(&home).map_err(|error| error.to_string())?;
        let path = home.join("update-receipt.json");
        let temporary = home.join(format!(".update-receipt.{}.tmp", std::process::id()));
        fs::write(&temporary, report_json).map_err(|error| error.to_string())?;
        fs::rename(temporary, path).map_err(|error| error.to_string())
    }
}

fn read_npm_version(context: &InstallContext) -> Result<String, String> {
    let root = context
        .managed_root
        .as_deref()
        .ok_or_else(|| "npm managed root is missing".to_string())?;
    let bytes = fs::read(root.join("package.json")).map_err(|error| error.to_string())?;
    let value: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
    value
        .get("version")
        .and_then(serde_json::Value::as_str)
        .filter(|version| !version.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| "managed npm package has no version".into())
}

fn parse_target_version(method: InstallMethod, output: &str) -> Result<String, String> {
    match method {
        InstallMethod::Npm => serde_json::from_str::<String>(output.trim())
            .or_else(|_| Ok::<_, serde_json::Error>(output.trim().trim_matches('"').into()))
            .map_err(|error| error.to_string())
            .and_then(nonempty_version),
        InstallMethod::Cargo => output
            .lines()
            .find(|line| line.trim_start().starts_with("egregore-nexus "))
            .and_then(|line| line.split('"').nth(1))
            .map(str::to_owned)
            .ok_or_else(|| "Cargo registry response did not contain egregore-nexus".into())
            .and_then(nonempty_version),
        InstallMethod::Manual | InstallMethod::Development => {
            Err("automatic update requires an npm or Cargo installation".into())
        }
    }
}

fn nonempty_version(version: String) -> Result<String, String> {
    if version.trim().is_empty() {
        Err("package registry returned an empty version".into())
    } else {
        Ok(version)
    }
}

fn verify_binary(context: &InstallContext, expected_version: &str) -> Result<(), String> {
    let version = run_program(
        &context.executable,
        &["--version"],
        &user_home(),
        VERIFY_TIMEOUT,
    )?;
    if version.trim().is_empty() {
        return Err("replacement binary returned an empty version".into());
    }
    match context.method {
        InstallMethod::Npm if read_npm_version(context)? != expected_version => {
            return Err(
                "replacement npm package version does not match the requested version".into(),
            )
        }
        InstallMethod::Cargo if !version.contains(expected_version) => {
            return Err("replacement binary version does not match the requested version".into())
        }
        InstallMethod::Npm | InstallMethod::Cargo => {}
        InstallMethod::Manual | InstallMethod::Development => {
            return Err("automatic update requires an npm or Cargo installation".into())
        }
    }
    for arguments in [
        &["daemon", "install", "--help"][..],
        &["gateway", "--help"][..],
        &["webconsole", "--help"][..],
    ] {
        run_program(&context.executable, arguments, &user_home(), VERIFY_TIMEOUT)?;
    }
    Ok(())
}

fn run_program(
    program: &Path,
    args: &[&str],
    cwd: &Path,
    timeout: Duration,
) -> Result<String, String> {
    run_command(
        &CommandPlan {
            program: program.to_path_buf(),
            args: args.iter().map(|value| (*value).into()).collect(),
            cwd: cwd.to_path_buf(),
            env_overrides: BTreeMap::new(),
        },
        timeout,
    )
}

fn run_command(plan: &CommandPlan, timeout: Duration) -> Result<String, String> {
    let mut command = Command::new(&plan.program);
    command
        .args(&plan.args)
        .current_dir(&plan.cwd)
        .envs(&plan.env_overrides);
    match lifecycle_process::run_bounded(
        &mut command,
        "Nexus update helper",
        timeout,
        COMMAND_OUTPUT_LIMIT,
    ) {
        Ok(output) => String::from_utf8(output.stdout).map_err(|error| error.to_string()),
        Err(BoundedProcessError::Timeout { .. }) => {
            Err(format!("{} timed out", plan.redacted_display()))
        }
        Err(BoundedProcessError::Spawn { .. }) => {
            Err(format!("failed to start {}", plan.redacted_display()))
        }
        Err(BoundedProcessError::Exit { .. }) => Err(format!("{} failed", plan.redacted_display())),
        Err(_) => Err(format!("{} failed", plan.redacted_display())),
    }
}

fn user_home() -> PathBuf {
    env::var_os("HOME")
        .or_else(|| env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
}

#[cfg(test)]
#[path = "../../tests/unit/update_system.rs"]
mod update_system_contracts;
