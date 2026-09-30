//! Detection of the installation that owns the running Nexus executable.

use std::collections::BTreeSet;
use std::env;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstallMethod {
    Npm,
    Cargo,
    Manual,
    Development,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InstalledFacet {
    Cli,
    Daemon,
    Gateway,
    Webconsole,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InstallContext {
    pub method: InstallMethod,
    pub managed_package: Option<String>,
    pub managed_root: Option<PathBuf>,
    pub launcher_path: Option<PathBuf>,
    pub executable: PathBuf,
    pub facets: BTreeSet<InstalledFacet>,
    pub wsl: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum InstallContextError {
    #[error("missing npm install metadata: {0}")]
    MissingMetadata(&'static str),
    #[error("unsupported managed package: {0}")]
    UnsupportedPackage(String),
    #[error("npm install metadata does not match the running executable")]
    ExecutableMismatch,
    #[error("npm launcher is outside the managed package root")]
    ManagedRootMismatch,
    #[error(
        "Windows npm installations are unsafe inside WSL; install and use Linux npm inside WSL"
    )]
    WslWindowsNpm,
    #[error("install-context filesystem error: {0}")]
    Io(#[from] io::Error),
}

pub fn detect_install_context() -> Result<InstallContext, InstallContextError> {
    detect_install_context_with(&SystemInstallEnvironment)
}

#[doc(hidden)]
pub fn detect_install_context_with(
    environment: &dyn InstallEnvironment,
) -> Result<InstallContext, InstallContextError> {
    let executable = environment.canonicalize(&environment.current_executable()?)?;
    let wsl = environment.is_wsl();
    if environment
        .var_os("NEXUS_INSTALL_METHOD")
        .is_some_and(|value| value == OsStr::new("npm"))
    {
        return detect_npm_context(environment, executable, wsl);
    }

    let cargo_home = environment
        .var_os("CARGO_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| environment.home_dir().join(".cargo"));
    let cargo_bin = cargo_home.join("bin");
    if executable.parent() == Some(cargo_bin.as_path())
        && environment.command_path("cargo").is_some()
    {
        return Ok(unmanaged_context(InstallMethod::Cargo, executable, wsl));
    }

    let normalized = executable.to_string_lossy().replace('\\', "/");
    let method = if normalized.contains("/target/debug/") || normalized.contains("/target/release/")
    {
        InstallMethod::Development
    } else {
        InstallMethod::Manual
    };
    Ok(unmanaged_context(method, executable, wsl))
}

fn detect_npm_context(
    environment: &dyn InstallEnvironment,
    executable: PathBuf,
    wsl: bool,
) -> Result<InstallContext, InstallContextError> {
    let package = required_text(environment, "NEXUS_MANAGED_PACKAGE")?;
    let managed_root = required_path(environment, "NEXUS_MANAGED_PACKAGE_ROOT")?;
    let launcher_path = required_path(environment, "NEXUS_LAUNCHER_PATH")?;
    let native_binary = required_path(environment, "NEXUS_NATIVE_BIN")?;
    let managed_root = environment.canonicalize(&managed_root)?;
    let launcher_path = environment.canonicalize(&launcher_path)?;
    let native_binary = environment.canonicalize(&native_binary)?;

    if native_binary != executable {
        return Err(InstallContextError::ExecutableMismatch);
    }
    if !launcher_path.starts_with(&managed_root) {
        return Err(InstallContextError::ManagedRootMismatch);
    }
    if wsl
        && ([&managed_root, &launcher_path, &native_binary]
            .into_iter()
            .any(|path| is_windows_mount(path))
            || environment
                .command_path("npm")
                .is_some_and(|path| is_windows_mount(&path)))
    {
        return Err(InstallContextError::WslWindowsNpm);
    }

    Ok(InstallContext {
        method: InstallMethod::Npm,
        facets: npm_facets(&package)?,
        managed_package: Some(package),
        managed_root: Some(managed_root),
        launcher_path: Some(launcher_path),
        executable,
        wsl,
    })
}

fn required_text(
    environment: &dyn InstallEnvironment,
    name: &'static str,
) -> Result<String, InstallContextError> {
    environment
        .var_os(name)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string_lossy().into_owned())
        .ok_or(InstallContextError::MissingMetadata(name))
}

fn required_path(
    environment: &dyn InstallEnvironment,
    name: &'static str,
) -> Result<PathBuf, InstallContextError> {
    environment
        .var_os(name)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .ok_or(InstallContextError::MissingMetadata(name))
}

fn npm_facets(package: &str) -> Result<BTreeSet<InstalledFacet>, InstallContextError> {
    let values: &[InstalledFacet] = match package {
        "@egregore/nexus-cli" => &[InstalledFacet::Cli, InstalledFacet::Daemon],
        "@egregore/nexus-gateway" | "@egregore/nexus" => &[
            InstalledFacet::Cli,
            InstalledFacet::Daemon,
            InstalledFacet::Gateway,
            InstalledFacet::Webconsole,
        ],
        other => return Err(InstallContextError::UnsupportedPackage(other.into())),
    };
    Ok(values.iter().copied().collect())
}

fn unmanaged_context(method: InstallMethod, executable: PathBuf, wsl: bool) -> InstallContext {
    InstallContext {
        method,
        managed_package: None,
        managed_root: None,
        launcher_path: None,
        executable,
        facets: [InstalledFacet::Cli, InstalledFacet::Daemon]
            .into_iter()
            .collect(),
        wsl,
    }
}

fn is_windows_mount(path: &Path) -> bool {
    let normalized = path.to_string_lossy().replace('\\', "/");
    normalized == "/mnt/c" || normalized.starts_with("/mnt/c/")
}

#[doc(hidden)]
pub trait InstallEnvironment {
    fn var_os(&self, name: &str) -> Option<OsString>;
    fn current_executable(&self) -> io::Result<PathBuf>;
    fn canonicalize(&self, path: &Path) -> io::Result<PathBuf>;
    fn command_path(&self, name: &str) -> Option<PathBuf>;
    fn home_dir(&self) -> PathBuf;
    fn is_wsl(&self) -> bool;
}

struct SystemInstallEnvironment;

impl InstallEnvironment for SystemInstallEnvironment {
    fn var_os(&self, name: &str) -> Option<OsString> {
        env::var_os(name)
    }

    fn current_executable(&self) -> io::Result<PathBuf> {
        env::current_exe()
    }

    fn canonicalize(&self, path: &Path) -> io::Result<PathBuf> {
        fs::canonicalize(path)
    }

    fn command_path(&self, name: &str) -> Option<PathBuf> {
        find_command(name)
    }

    fn home_dir(&self) -> PathBuf {
        env::var_os("HOME")
            .or_else(|| env::var_os("USERPROFILE"))
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."))
    }

    fn is_wsl(&self) -> bool {
        env::var_os("WSL_DISTRO_NAME").is_some()
            || fs::read_to_string("/proc/sys/kernel/osrelease")
                .map(|value| value.to_ascii_lowercase().contains("microsoft"))
                .unwrap_or(false)
    }
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
            .find(|candidate| candidate.is_file())
    })
}
