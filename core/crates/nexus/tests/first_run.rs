//! Actual CLI/PTY acceptance with disposable homes and no real service manager.
#![cfg(target_os = "linux")]

#[path = "unit/first_run.rs"]
mod policy;

use std::fs;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Command, Output, Stdio};

fn command(home: &Path) -> Command {
    let mut command = Command::new("timeout");
    command.arg("10s");
    command
        .env("HOME", home)
        .env("NEXUS_HOME", home.join("nexus"))
        .env("XDG_CONFIG_HOME", home.join("config"))
        .env(
            "PATH",
            format!("{}:/usr/bin:/bin", home.join("bin").display()),
        );
    for key in [
        "NEXUS_NAME",
        "NEXUS_CLIENT_KEY",
        "NEXUS_SESSION_ID",
        "CI",
        "NEXUS_NO_SETUP",
        "NEXUS_NO_AUTOSTART",
        "NEXUS_INSTALL_METHOD",
        "NEXUS_GATEWAY_BIN",
        "NEXUS_WEBCONSOLE_BIN",
        "NEXUS_WEBUI_BIN",
    ] {
        command.env_remove(key);
    }
    command
}

fn pty(home: &Path, args: &str, input: &[u8], agent: bool) -> Output {
    let mut command = command(home);
    let binary = env!("CARGO_BIN_EXE_nexus").replace('\'', "'\\''");
    command.args(["script", "-qec", &format!("'{binary}' {args}"), "/dev/null"]);
    if agent {
        command.env("NEXUS_SESSION_ID", "s_test_agent");
    }
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(input).unwrap();
    child.wait_with_output().unwrap()
}

#[test]
fn interactive_bare_cli_prompts_once_and_remembers_decline() {
    let home = tempfile::tempdir().unwrap();
    let output = pty(home.path(), "", b"n\n", false);
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "{text}");
    assert!(text.contains("background now and at login?"), "{text}");
    assert!(text.contains("Webconsole:"), "{text}");
    let choice: serde_json::Value =
        serde_json::from_slice(&fs::read(home.path().join("nexus/first-run.json")).unwrap())
            .unwrap();
    assert_eq!(choice["background"], false);
    let again = pty(home.path(), "", b"", false);
    assert!(!String::from_utf8_lossy(&again.stdout).contains("background now and at login?"));
}

#[test]
fn scripts_help_version_and_agent_terminals_do_not_prompt_or_save_choices() {
    let home = tempfile::tempdir().unwrap();
    for args in [
        vec!["--help"],
        vec!["--version"],
        vec!["members", "--json"],
        vec!["webconsole", "install", "--help"],
    ] {
        let output = command(home.path())
            .arg(env!("CARGO_BIN_EXE_nexus"))
            .args(args)
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert_ne!(output.status.code(), Some(124));
        assert!(!String::from_utf8_lossy(&output.stderr).contains("background now and at login?"));
    }
    for (args, agent) in [("--help", false), ("--version", false), ("", true)] {
        let output = pty(home.path(), args, b"", agent);
        assert_ne!(output.status.code(), Some(124));
        assert!(!String::from_utf8_lossy(&output.stdout).contains("background now and at login?"));
    }
    assert!(!home.path().join("nexus/first-run.json").exists());
    assert!(!home.path().join("nexus/first-run.lock").exists());
}

#[test]
fn actual_service_failure_after_yes_does_not_remember_success() {
    let home = tempfile::tempdir().unwrap();
    fs::create_dir(home.path().join("bin")).unwrap();
    let manager = home.path().join("bin/systemctl");
    fs::write(
        &manager,
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$HOME/service-calls\"\nexit 1\n",
    )
    .unwrap();
    fs::set_permissions(&manager, fs::Permissions::from_mode(0o755)).unwrap();
    let output = pty(home.path(), "", b"yes\n", false);
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(!output.status.success());
    assert_ne!(output.status.code(), Some(124), "{text}");
    assert!(text.contains("background setup incomplete"), "{text}");
    assert!(fs::read_to_string(home.path().join("service-calls"))
        .unwrap()
        .contains("daemon-reload"));
    assert!(!home.path().join("nexus/first-run.json").exists());
}

#[test]
fn explicit_webconsole_service_commands_keep_json_machine_readable() {
    let home = tempfile::tempdir().unwrap();
    for (verb, agent) in [("uninstall", false), ("install", true)] {
        let mut cmd = command(home.path());
        cmd.arg(env!("CARGO_BIN_EXE_nexus"))
            .args(["webconsole", verb, "--json"]);
        if agent {
            cmd.env("NEXUS_SESSION_ID", "s_test_agent");
        }
        let output = cmd.output().unwrap();
        let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        if agent {
            assert!(!output.status.success());
            assert_eq!(value["error"]["code"], "WEBCONSOLE_SERVICE_FAILED");
        } else {
            assert!(output.status.success());
            assert_eq!(value["installed"], false);
        }
    }
    assert!(!home.path().join("nexus/first-run.json").exists());
}

#[test]
fn daemon_uninstall_removes_webconsole_before_its_dependencies() {
    let home = tempfile::tempdir().unwrap();
    fs::create_dir(home.path().join("bin")).unwrap();
    let units = home.path().join("config/systemd/user");
    fs::create_dir_all(&units).unwrap();
    for name in [
        "nexus-webconsole.service",
        "nexus-gateway.service",
        "nexus-daemon.service",
    ] {
        fs::write(units.join(name), "[Unit]\nDescription=disposable fixture\n").unwrap();
    }
    let manager = home.path().join("bin/systemctl");
    fs::write(
        &manager,
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$HOME/service-calls\"\nexit 0\n",
    )
    .unwrap();
    fs::set_permissions(&manager, fs::Permissions::from_mode(0o755)).unwrap();
    let output = command(home.path())
        .arg(env!("CARGO_BIN_EXE_nexus"))
        .args(["daemon", "uninstall"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let calls = fs::read_to_string(home.path().join("service-calls")).unwrap();
    let web = calls
        .find("--user stop nexus-webconsole.service")
        .expect("Webconsole was not stopped");
    let gateway = calls.find("--user stop nexus-gateway.service").unwrap();
    let daemon = calls.find("--user stop nexus-daemon.service").unwrap();
    assert!(web < gateway && gateway < daemon, "{calls}");
    assert!(!units.join("nexus-webconsole.service").exists());
}

#[test]
fn webconsole_stop_controls_the_registered_supervisor() {
    let home = tempfile::tempdir().unwrap();
    fs::create_dir(home.path().join("bin")).unwrap();
    let units = home.path().join("config/systemd/user");
    fs::create_dir_all(&units).unwrap();
    fs::write(units.join("nexus-webconsole.service"), "fixture").unwrap();
    let manager = home.path().join("bin/systemctl");
    fs::write(
        &manager,
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$HOME/service-calls\"\nexit 0\n",
    )
    .unwrap();
    fs::set_permissions(&manager, fs::Permissions::from_mode(0o755)).unwrap();
    let output = command(home.path())
        .arg(env!("CARGO_BIN_EXE_nexus"))
        .args(["webconsole", "stop", "--json"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    let calls = fs::read_to_string(home.path().join("service-calls")).unwrap();
    assert!(
        calls.contains("--user stop nexus-webconsole.service"),
        "{calls}"
    );
    assert!(
        units.join("nexus-webconsole.service").exists(),
        "stop must retain login configuration"
    );
}

#[test]
fn registered_gateway_lifecycle_uses_supervisor_and_propagates_its_failure() {
    for verb in ["start", "stop", "restart"] {
        let home = tempfile::tempdir().unwrap();
        fs::create_dir(home.path().join("bin")).unwrap();
        let units = home.path().join("config/systemd/user");
        fs::create_dir_all(&units).unwrap();
        for name in ["nexus-gateway.service", "nexus-daemon.service"] {
            fs::write(units.join(name), "fixture").unwrap();
        }
        fs::create_dir(home.path().join("nexus")).unwrap();
        // Read-only dependency liveness; the fake manager cannot signal this PID.
        fs::write(
            home.path().join("nexus/daemon.pid"),
            std::process::id().to_string(),
        )
        .unwrap();
        let manager = home.path().join("bin/systemctl");
        fs::write(&manager, "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$HOME/service-calls\"\necho fixture-manager-refused >&2\nexit 1\n").unwrap();
        fs::set_permissions(&manager, fs::Permissions::from_mode(0o755)).unwrap();
        let output = command(home.path())
            .arg(env!("CARGO_BIN_EXE_nexus"))
            .args(["gateway", verb, "--json"])
            .output()
            .unwrap();
        assert!(!output.status.success());
        let calls = fs::read_to_string(home.path().join("service-calls")).unwrap_or_default();
        assert!(
            calls.contains(&format!("--user {verb} nexus-gateway.service")),
            "{verb} bypassed supervisor: {calls}"
        );
        let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(value["error"]["code"], "GATEWAY_LIFECYCLE_FAILED");
        // The bounded process wrapper reports the native exit status, not raw stderr.
        assert!(value["error"]["message"].as_str().unwrap().contains(
            "systemctl service command failed: Gateway service-manager command exited unsuccessfully"
        ), "{verb}: {value}");
    }
}
