use nexus::update::install_context::{
    detect_install_context_with, InstallEnvironment, InstallMethod, InstalledFacet,
};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::io;
use std::path::{Path, PathBuf};

#[test]
fn npm_packages_map_to_their_owned_facets() {
    let cases = [
        (
            "@egregore/nexus-cli",
            facets(&[InstalledFacet::Cli, InstalledFacet::Daemon]),
        ),
        (
            "@egregore/nexus-gateway",
            facets(&[
                InstalledFacet::Cli,
                InstalledFacet::Daemon,
                InstalledFacet::Gateway,
                InstalledFacet::Webconsole,
            ]),
        ),
        (
            "@egregore/nexus",
            facets(&[
                InstalledFacet::Cli,
                InstalledFacet::Daemon,
                InstalledFacet::Gateway,
                InstalledFacet::Webconsole,
            ]),
        ),
    ];

    for (package, expected) in cases {
        let environment = FakeEnvironment::npm(package, "/home/ada/npm/nexus");
        let context = detect_install_context_with(&environment).unwrap();
        assert_eq!(context.method, InstallMethod::Npm);
        assert_eq!(context.managed_package.as_deref(), Some(package));
        assert_eq!(context.facets, expected);
    }
}

#[test]
fn npm_context_requires_the_running_native_binary() {
    let mut environment = FakeEnvironment::npm("@egregore/nexus", "/home/ada/npm/nexus");
    environment.executable = PathBuf::from("/home/ada/npm/other/nexus");

    let error = detect_install_context_with(&environment).unwrap_err();

    assert!(error.to_string().contains("running executable"), "{error}");
}

#[test]
fn npm_context_requires_the_launcher_to_live_under_the_managed_root() {
    let mut environment = FakeEnvironment::npm("@egregore/nexus", "/home/ada/npm/nexus");
    environment.vars.insert(
        "NEXUS_LAUNCHER_PATH".into(),
        "/home/ada/npm/different/bin/nexus.mjs".into(),
    );

    let error = detect_install_context_with(&environment).unwrap_err();

    assert!(
        error.to_string().contains("managed package root"),
        "{error}"
    );
}

#[test]
fn npm_context_rejects_unknown_top_level_packages() {
    let environment = FakeEnvironment::npm("@egregore/not-nexus", "/home/ada/npm/nexus");

    let error = detect_install_context_with(&environment).unwrap_err();

    assert!(
        error.to_string().contains("unsupported managed package"),
        "{error}"
    );
}

#[test]
fn wsl_rejects_a_windows_managed_install() {
    let mut environment = FakeEnvironment::npm("@egregore/nexus", "/mnt/c/npm/nexus");
    environment.wsl = true;

    let error = detect_install_context_with(&environment).unwrap_err();

    assert!(
        error.to_string().contains("Linux npm inside WSL"),
        "{error}"
    );
}

#[test]
fn wsl_accepts_a_linux_managed_install() {
    let mut environment = FakeEnvironment::npm("@egregore/nexus", "/home/ada/npm/nexus");
    environment.wsl = true;

    let context = detect_install_context_with(&environment).unwrap();

    assert_eq!(context.method, InstallMethod::Npm);
}

#[test]
fn cargo_install_is_recognized_only_under_the_active_cargo_home() {
    let environment = FakeEnvironment {
        executable: "/home/ada/.cargo/bin/nexus".into(),
        home: "/home/ada".into(),
        commands: BTreeMap::from([("cargo".into(), "/home/ada/.cargo/bin/cargo".into())]),
        vars: BTreeMap::from([("CARGO_HOME".into(), "/home/ada/.cargo".into())]),
        wsl: false,
    };

    let context = detect_install_context_with(&environment).unwrap();

    assert_eq!(context.method, InstallMethod::Cargo);
    assert_eq!(
        context.facets,
        facets(&[InstalledFacet::Cli, InstalledFacet::Daemon])
    );
}

#[test]
fn target_builds_are_development_and_copied_binaries_are_manual() {
    let development = FakeEnvironment::bare("/work/egregore-nexus/core/target/debug/nexus");
    let manual = FakeEnvironment::bare("/opt/egregore/bin/nexus");

    assert_eq!(
        detect_install_context_with(&development).unwrap().method,
        InstallMethod::Development
    );
    assert_eq!(
        detect_install_context_with(&manual).unwrap().method,
        InstallMethod::Manual
    );
}

fn facets(values: &[InstalledFacet]) -> BTreeSet<InstalledFacet> {
    values.iter().copied().collect()
}

struct FakeEnvironment {
    executable: PathBuf,
    home: PathBuf,
    commands: BTreeMap<String, PathBuf>,
    vars: BTreeMap<String, OsString>,
    wsl: bool,
}

impl FakeEnvironment {
    fn npm(package: &str, root: &str) -> Self {
        let native = "/home/ada/npm/nexus-cli/native/linux-x64-gnu/nexus";
        Self {
            executable: native.into(),
            home: "/home/ada".into(),
            commands: BTreeMap::from([("npm".into(), "/usr/bin/npm".into())]),
            vars: BTreeMap::from([
                ("NEXUS_INSTALL_METHOD".into(), "npm".into()),
                ("NEXUS_MANAGED_PACKAGE".into(), package.into()),
                ("NEXUS_MANAGED_PACKAGE_ROOT".into(), root.into()),
                (
                    "NEXUS_LAUNCHER_PATH".into(),
                    format!("{root}/bin/nexus.mjs").into(),
                ),
                ("NEXUS_NATIVE_BIN".into(), native.into()),
            ]),
            wsl: false,
        }
    }

    fn bare(executable: &str) -> Self {
        Self {
            executable: executable.into(),
            home: "/home/ada".into(),
            commands: BTreeMap::new(),
            vars: BTreeMap::new(),
            wsl: false,
        }
    }
}

impl InstallEnvironment for FakeEnvironment {
    fn var_os(&self, name: &str) -> Option<OsString> {
        self.vars.get(name).cloned()
    }

    fn current_executable(&self) -> io::Result<PathBuf> {
        Ok(self.executable.clone())
    }

    fn canonicalize(&self, path: &Path) -> io::Result<PathBuf> {
        Ok(path.to_path_buf())
    }

    fn command_path(&self, name: &str) -> Option<PathBuf> {
        self.commands.get(name).cloned()
    }

    fn home_dir(&self) -> PathBuf {
        self.home.clone()
    }

    fn is_wsl(&self) -> bool {
        self.wsl
    }
}
