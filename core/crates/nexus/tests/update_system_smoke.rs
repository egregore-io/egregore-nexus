#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

struct UpdateFixture {
    _root: tempfile::TempDir,
    home: PathBuf,
    nexus_home: PathBuf,
    managed_root: PathBuf,
    launcher: PathBuf,
    fake_bin: PathBuf,
    npm_log: PathBuf,
}

impl UpdateFixture {
    fn new() -> Self {
        let root = tempfile::tempdir().expect("temporary update fixture");
        let home = root.path().join("home");
        let nexus_home = home.join(".nexus");
        let managed_root = root.path().join("npm/@egregore/nexus-cli");
        let launcher = managed_root.join("bin/nexus.mjs");
        let fake_bin = root.path().join("bin");
        let npm_log = root.path().join("npm.log");
        fs::create_dir_all(launcher.parent().unwrap()).unwrap();
        fs::create_dir_all(&fake_bin).unwrap();
        fs::create_dir_all(&home).unwrap();
        fs::write(&launcher, "// package launcher fixture\n").unwrap();
        write_package_version(&managed_root, "0.1.2");

        let npm = fake_bin.join("npm");
        fs::write(
            &npm,
            r#"#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$NEXUS_UPDATE_SMOKE_LOG"
if [ "$1" = "view" ]; then
  printf '"0.1.3"\n'
  exit 0
fi
if [ "$1" = "install" ]; then
  package="$5"
  version="${package##*@}"
  if [ "${NEXUS_UPDATE_SMOKE_FAIL_TARGET:-0}" = "1" ] && [ "$version" = "0.1.3" ]; then
    exit 0
  fi
  printf '{"name":"@egregore/nexus-cli","version":"%s"}\n' "$version" \
    > "$NEXUS_MANAGED_PACKAGE_ROOT/package.json"
  exit 0
fi
exit 64
"#,
        )
        .unwrap();
        fs::set_permissions(&npm, fs::Permissions::from_mode(0o755)).unwrap();

        Self {
            _root: root,
            home,
            nexus_home,
            managed_root,
            launcher,
            fake_bin,
            npm_log,
        }
    }

    fn run(&self, fail_target_verification: bool) -> Output {
        let binary = fs::canonicalize(env!("CARGO_BIN_EXE_nexus")).unwrap();
        let mut command = Command::new(&binary);
        command
            .args(["--json", "update"])
            .env_clear()
            .env("PATH", &self.fake_bin)
            .env("HOME", &self.home)
            .env("XDG_CONFIG_HOME", self.home.join(".config"))
            .env("NEXUS_HOME", &self.nexus_home)
            .env("NEXUS_INSTALL_METHOD", "npm")
            .env("NEXUS_MANAGED_PACKAGE", "@egregore/nexus-cli")
            .env("NEXUS_MANAGED_PACKAGE_ROOT", &self.managed_root)
            .env("NEXUS_LAUNCHER_PATH", &self.launcher)
            .env("NEXUS_NATIVE_BIN", &binary)
            .env("NEXUS_UPDATE_SMOKE_LOG", &self.npm_log)
            .env("NEXUS_TEST_SECRET", "must-not-reach-receipt");
        if fail_target_verification {
            command.env("NEXUS_UPDATE_SMOKE_FAIL_TARGET", "1");
        }
        command.output().expect("run managed update")
    }

    fn version(&self) -> String {
        let manifest: serde_json::Value =
            serde_json::from_slice(&fs::read(self.managed_root.join("package.json")).unwrap())
                .unwrap();
        manifest["version"].as_str().unwrap().to_owned()
    }

    fn receipt(&self) -> String {
        fs::read_to_string(self.nexus_home.join("update-receipt.json")).unwrap()
    }
}

#[test]
fn managed_npm_update_applies_the_exact_target_and_writes_a_redacted_receipt() {
    let fixture = UpdateFixture::new();
    let output = fixture.run(false);

    assert!(
        output.status.success(),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["status"], "updated");
    assert_eq!(report["targetVersion"], "0.1.3");
    assert_eq!(fixture.version(), "0.1.3");
    assert!(!fixture.receipt().contains("must-not-reach-receipt"));
    let calls = fs::read_to_string(&fixture.npm_log).unwrap();
    assert!(calls.contains("view @egregore/nexus-cli version --json"));
    assert!(calls.contains("install --global --no-audit --no-fund @egregore/nexus-cli@0.1.3"));
    assert!(!calls.contains("@egregore/nexus-cli@0.1.2"));
}

#[test]
fn failed_target_verification_restores_the_exact_previous_npm_version() {
    let fixture = UpdateFixture::new();
    let output = fixture.run(true);

    assert_eq!(output.status.code(), Some(1));
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["status"], "rolled_back");
    assert_eq!(report["rollback"]["attempted"], true);
    assert_eq!(report["rollback"]["succeeded"], true);
    assert_eq!(report["error"]["code"], "POST_INSTALL_HEALTH_FAILED");
    assert_eq!(fixture.version(), "0.1.2");
    assert!(!fixture.receipt().contains("must-not-reach-receipt"));
    let calls = fs::read_to_string(&fixture.npm_log).unwrap();
    assert!(calls.contains("install --global --no-audit --no-fund @egregore/nexus-cli@0.1.3"));
    assert!(calls.contains("install --global --no-audit --no-fund @egregore/nexus-cli@0.1.2"));
}

fn write_package_version(root: &Path, version: &str) {
    fs::create_dir_all(root).unwrap();
    fs::write(
        root.join("package.json"),
        format!(r#"{{"name":"@egregore/nexus-cli","version":"{version}"}}"#),
    )
    .unwrap();
}
