//! Pure package-manager command planning for managed Nexus installations.

use std::collections::BTreeMap;
use std::env;
use std::ffi::{OsStr, OsString};
use std::path::PathBuf;

use serde::Serialize;

use super::install_context::{InstallContext, InstallMethod};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PackageTools {
    pub npm: Option<PathBuf>,
    pub cargo: Option<PathBuf>,
}

impl PackageTools {
    pub fn detect() -> Self {
        Self {
            npm: find_command("npm"),
            cargo: find_command("cargo"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CommandPlan {
    pub program: PathBuf,
    pub args: Vec<String>,
    pub cwd: PathBuf,
    pub env_overrides: BTreeMap<String, String>,
}

impl CommandPlan {
    pub fn redacted_display(&self) -> String {
        std::iter::once(self.program.to_string_lossy().into_owned())
            .chain(self.args.iter().cloned())
            .collect::<Vec<_>>()
            .join(" ")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageManagerPlan {
    pub probe: CommandPlan,
    pub install: CommandPlan,
    pub rollback: CommandPlan,
    pub recovery_command: String,
}

#[derive(Debug, thiserror::Error)]
pub enum PackageManagerError {
    #[error("required package manager is unavailable: {0}")]
    MissingTool(&'static str),
    #[error("Nexus is manually managed; automatic update will not overwrite this executable")]
    ManuallyManaged,
    #[error("npm install context is missing its managed package")]
    MissingManagedPackage,
}

pub fn package_version_probe(
    context: &InstallContext,
    tools: &PackageTools,
    home: PathBuf,
    inherited_environment: &BTreeMap<String, String>,
) -> Result<CommandPlan, PackageManagerError> {
    match context.method {
        InstallMethod::Npm => {
            let npm = tools
                .npm
                .clone()
                .ok_or(PackageManagerError::MissingTool("npm"))?;
            let package = context
                .managed_package
                .as_deref()
                .ok_or(PackageManagerError::MissingManagedPackage)?;
            Ok(CommandPlan {
                program: npm,
                args: vec![
                    "view".into(),
                    package.into(),
                    "version".into(),
                    "--json".into(),
                ],
                cwd: home,
                env_overrides: BTreeMap::new(),
            })
        }
        InstallMethod::Cargo => {
            let cargo = tools
                .cargo
                .clone()
                .ok_or(PackageManagerError::MissingTool("cargo"))?;
            let jobs = inherited_environment
                .get("CARGO_BUILD_JOBS")
                .cloned()
                .unwrap_or_else(|| "2".into());
            Ok(CommandPlan {
                program: cargo,
                args: vec![
                    "search".into(),
                    "egregore-nexus".into(),
                    "--limit".into(),
                    "1".into(),
                ],
                cwd: home,
                env_overrides: BTreeMap::from([("CARGO_BUILD_JOBS".into(), jobs)]),
            })
        }
        InstallMethod::Manual | InstallMethod::Development => {
            Err(PackageManagerError::ManuallyManaged)
        }
    }
}

pub fn package_manager_plan(
    context: &InstallContext,
    tools: &PackageTools,
    home: PathBuf,
    current_version: &str,
    target_version: &str,
    inherited_environment: &BTreeMap<String, String>,
) -> Result<PackageManagerPlan, PackageManagerError> {
    match context.method {
        InstallMethod::Npm => npm_plan(context, tools, home, current_version, target_version),
        InstallMethod::Cargo => cargo_plan(
            tools,
            home,
            current_version,
            target_version,
            inherited_environment,
        ),
        InstallMethod::Manual | InstallMethod::Development => {
            Err(PackageManagerError::ManuallyManaged)
        }
    }
}

fn npm_plan(
    context: &InstallContext,
    tools: &PackageTools,
    home: PathBuf,
    current_version: &str,
    target_version: &str,
) -> Result<PackageManagerPlan, PackageManagerError> {
    let npm = tools
        .npm
        .clone()
        .ok_or(PackageManagerError::MissingTool("npm"))?;
    let package = context
        .managed_package
        .as_deref()
        .ok_or(PackageManagerError::MissingManagedPackage)?;
    let command = |args: Vec<String>| CommandPlan {
        program: npm.clone(),
        args,
        cwd: home.clone(),
        env_overrides: BTreeMap::new(),
    };
    let install_args = |version: &str| {
        vec![
            "install".into(),
            "--global".into(),
            "--no-audit".into(),
            "--no-fund".into(),
            format!("{package}@{version}"),
        ]
    };
    Ok(PackageManagerPlan {
        probe: command(vec![
            "view".into(),
            package.into(),
            "version".into(),
            "--json".into(),
        ]),
        install: command(install_args(target_version)),
        rollback: command(install_args(current_version)),
        recovery_command: format!("npm install --global {package}@{current_version}"),
    })
}

fn cargo_plan(
    tools: &PackageTools,
    home: PathBuf,
    current_version: &str,
    target_version: &str,
    inherited_environment: &BTreeMap<String, String>,
) -> Result<PackageManagerPlan, PackageManagerError> {
    let cargo = tools
        .cargo
        .clone()
        .ok_or(PackageManagerError::MissingTool("cargo"))?;
    let jobs = inherited_environment
        .get("CARGO_BUILD_JOBS")
        .cloned()
        .unwrap_or_else(|| "2".into());
    let environment = BTreeMap::from([("CARGO_BUILD_JOBS".into(), jobs)]);
    let command = |args: Vec<String>| CommandPlan {
        program: cargo.clone(),
        args,
        cwd: home.clone(),
        env_overrides: environment.clone(),
    };
    let install_args = |version: &str| {
        vec![
            "install".into(),
            "egregore-nexus".into(),
            "--locked".into(),
            "--force".into(),
            "--version".into(),
            version.into(),
        ]
    };
    Ok(PackageManagerPlan {
        probe: command(vec![
            "search".into(),
            "egregore-nexus".into(),
            "--limit".into(),
            "1".into(),
        ]),
        install: command(install_args(target_version)),
        rollback: command(install_args(current_version)),
        recovery_command: format!(
            "CARGO_BUILD_JOBS={environment_jobs} cargo install egregore-nexus --locked --force --version {current_version}",
            environment_jobs = environment["CARGO_BUILD_JOBS"]
        ),
    })
}

fn find_command(name: &str) -> Option<PathBuf> {
    let candidates: Vec<OsString> = if cfg!(windows) {
        vec![format!("{name}.exe").into(), format!("{name}.cmd").into()]
    } else {
        vec![name.into()]
    };
    env::var_os("PATH").and_then(|value| {
        env::split_paths(&value)
            .flat_map(|directory| candidates.iter().map(move |name| directory.join(name)))
            .find(|candidate| candidate.is_file() && candidate.file_name() != Some(OsStr::new("")))
    })
}
