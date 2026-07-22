use std::process::Command;

use nexus::daemon::lifecycle::{launchd_service_plist, windows_task_registration_script};

#[cfg(target_os = "linux")]
use std::fs;
#[cfg(target_os = "linux")]
use std::os::unix::fs::PermissionsExt;

#[cfg(target_os = "linux")]
fn stop_fake_service(home: &std::path::Path) {
    let Ok(pid) = fs::read_to_string(home.join("daemon.pid")) else {
        return;
    };
    let _ = Command::new("kill").arg(pid.trim()).status();
}

fn nexus() -> Command {
    Command::new(env!("CARGO_BIN_EXE_nexus"))
}

#[test]
fn daemon_install_and_uninstall_are_the_canonical_service_commands() {
    let install = nexus()
        .args(["daemon", "install", "--help"])
        .output()
        .unwrap();
    assert!(
        install.status.success(),
        "{}",
        String::from_utf8_lossy(&install.stderr)
    );

    let uninstall = nexus()
        .args(["daemon", "uninstall", "--help"])
        .output()
        .unwrap();
    assert!(
        uninstall.status.success(),
        "{}",
        String::from_utf8_lossy(&uninstall.stderr)
    );

    for legacy in ["install-service", "uninstall-service"] {
        let output = nexus().args(["daemon", legacy, "--help"]).output().unwrap();
        assert!(
            !output.status.success(),
            "legacy command remained public: {legacy}"
        );
    }
}

#[test]
fn windows_install_plan_is_user_scoped_and_restarts_failures() {
    let script = windows_task_registration_script(
        std::path::Path::new(r"C:\Users\nexus-user\AppData\Roaming\npm\nexus.exe"),
        std::path::Path::new(r"C:\Users\nexus-user\.nexus"),
    );

    assert!(script.contains("New-ScheduledTaskAction"));
    assert!(script.contains(r"C:\Users\nexus-user\AppData\Roaming\npm\nexus.exe"));
    assert!(script.contains("daemon run"));
    assert!(script.contains(r"C:\Users\nexus-user\.nexus"));
    assert!(script.contains("New-ScheduledTaskTrigger -AtLogOn"));
    assert!(script.contains("-RestartCount 999"));
    assert!(script.contains("-RestartInterval"));
    assert!(script.contains("-RunLevel Limited"));
    assert!(script.contains("Register-ScheduledTask"));
    assert!(!script.contains("RunLevel Highest"));
    assert!(!script.contains("sc.exe"));
}

#[test]
fn macos_install_plan_runs_at_login_and_restarts_failures() {
    let plist = launchd_service_plist(
        std::path::Path::new("/Applications/Egregore & Co/nexus"),
        std::path::Path::new("/Users/nexus-user/.nexus"),
        std::path::Path::new("/Users/nexus-user/.nexus/daemon.log"),
    );

    assert!(plist.contains("<string>io.egregore.nexus.daemon</string>"));
    assert!(plist.contains("/Applications/Egregore &amp; Co/nexus"));
    assert!(plist.contains("<key>RunAtLoad</key><true/>"));
    assert!(plist.contains("<key>KeepAlive</key>"));
    assert!(plist.contains("<key>SuccessfulExit</key><false/>"));
    assert!(plist.contains("<key>NEXUS_HOME</key><string>/Users/nexus-user/.nexus</string>"));
}

#[cfg(target_os = "linux")]
#[test]
fn daemon_install_enables_and_starts_the_systemd_user_service() {
    let root = tempfile::tempdir().unwrap();
    let fake_bin = root.path().join("bin");
    let config = root.path().join("config");
    let home = root.path().join("nexus-home");
    let calls = root.path().join("systemctl.log");
    fs::create_dir_all(&fake_bin).unwrap();

    let systemctl = fake_bin.join("systemctl");
    fs::write(
        &systemctl,
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$NEXUS_TEST_SYSTEMCTL_LOG\"\nif [ \"$*\" = \"--user start nexus-daemon.service\" ]; then\n  setsid /bin/sleep 30 </dev/null >/dev/null 2>&1 &\n  printf '%s\\n' \"$!\" > \"$NEXUS_HOME/daemon.pid\"\nfi\n",
    )
    .unwrap();
    fs::set_permissions(&systemctl, fs::Permissions::from_mode(0o755)).unwrap();

    let output = nexus()
        .args(["daemon", "install", "--binary", env!("CARGO_BIN_EXE_nexus")])
        .env("PATH", format!("{}:/usr/bin:/bin", fake_bin.display()))
        .env("HOME", root.path())
        .env("XDG_CONFIG_HOME", &config)
        .env("NEXUS_HOME", &home)
        .env("NEXUS_TEST_SYSTEMCTL_LOG", &calls)
        .env_remove("NEXUS_NAME")
        .env_remove("NEXUS_CLIENT_KEY")
        .env_remove("NEXUS_SESSION_ID")
        .output()
        .unwrap();

    stop_fake_service(&home);

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("service installed and started (systemd)")
    );

    let calls = fs::read_to_string(calls).unwrap();
    assert!(calls.contains("--user daemon-reload"), "{calls}");
    assert!(
        calls.contains("--user enable nexus-daemon.service"),
        "{calls}"
    );
    assert!(
        calls.contains("--user start nexus-daemon.service"),
        "{calls}"
    );

    let unit = fs::read_to_string(config.join("systemd/user/nexus-daemon.service")).unwrap();
    assert!(unit.contains(&format!(
        "ExecStart={} daemon run",
        env!("CARGO_BIN_EXE_nexus")
    )));
    assert!(unit.contains("Restart=on-failure"));
    assert!(unit.contains(&format!("Environment=NEXUS_HOME={}", home.display())));
}

#[cfg(target_os = "linux")]
#[test]
fn daemon_install_rejects_a_service_that_never_becomes_healthy() {
    let root = tempfile::tempdir().unwrap();
    let fake_bin = root.path().join("bin");
    let config = root.path().join("config");
    let home = root.path().join("nexus-home");
    let calls = root.path().join("systemctl.log");
    fs::create_dir_all(&fake_bin).unwrap();

    let systemctl = fake_bin.join("systemctl");
    fs::write(
        &systemctl,
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$NEXUS_TEST_SYSTEMCTL_LOG\"\n",
    )
    .unwrap();
    fs::set_permissions(&systemctl, fs::Permissions::from_mode(0o755)).unwrap();

    let output = nexus()
        .args(["daemon", "install", "--binary", env!("CARGO_BIN_EXE_nexus")])
        .env("PATH", format!("{}:/usr/bin:/bin", fake_bin.display()))
        .env("HOME", root.path())
        .env("XDG_CONFIG_HOME", &config)
        .env("NEXUS_HOME", &home)
        .env("NEXUS_TEST_SYSTEMCTL_LOG", &calls)
        .env_remove("NEXUS_NAME")
        .env_remove("NEXUS_CLIENT_KEY")
        .env_remove("NEXUS_SESSION_ID")
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(!String::from_utf8_lossy(&output.stdout).contains("service installed and started"));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("did not become healthy"), "{stderr}");
    assert!(stderr.contains("nexus daemon logs"), "{stderr}");
}

#[cfg(target_os = "linux")]
#[test]
fn daemon_uninstall_stops_disables_and_removes_the_systemd_user_service() {
    let root = tempfile::tempdir().unwrap();
    let fake_bin = root.path().join("bin");
    let config = root.path().join("config");
    let home = root.path().join("nexus-home");
    let calls = root.path().join("systemctl.log");
    let unit = config.join("systemd/user/nexus-daemon.service");
    fs::create_dir_all(&fake_bin).unwrap();
    fs::create_dir_all(unit.parent().unwrap()).unwrap();
    fs::write(&unit, "[Service]\n").unwrap();

    let systemctl = fake_bin.join("systemctl");
    fs::write(
        &systemctl,
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$NEXUS_TEST_SYSTEMCTL_LOG\"\n",
    )
    .unwrap();
    fs::set_permissions(&systemctl, fs::Permissions::from_mode(0o755)).unwrap();

    let output = nexus()
        .args(["daemon", "uninstall"])
        .env("PATH", &fake_bin)
        .env("HOME", root.path())
        .env("XDG_CONFIG_HOME", &config)
        .env("NEXUS_HOME", &home)
        .env("NEXUS_TEST_SYSTEMCTL_LOG", &calls)
        .env_remove("NEXUS_NAME")
        .env_remove("NEXUS_CLIENT_KEY")
        .env_remove("NEXUS_SESSION_ID")
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let calls = fs::read_to_string(calls).unwrap();
    assert!(
        calls.contains("--user stop nexus-daemon.service"),
        "{calls}"
    );
    assert!(
        calls.contains("--user disable nexus-daemon.service"),
        "{calls}"
    );
    assert!(calls.contains("--user daemon-reload"), "{calls}");
    assert!(!unit.exists());
}

#[cfg(target_os = "linux")]
#[test]
fn daemon_uninstall_removes_gateway_service_first() {
    let root = tempfile::tempdir().unwrap();
    let fake_bin = root.path().join("bin");
    let config = root.path().join("config");
    let home = root.path().join("nexus-home");
    let calls = root.path().join("systemctl.log");
    let units = config.join("systemd/user");
    fs::create_dir_all(&fake_bin).unwrap();
    fs::create_dir_all(&units).unwrap();
    fs::write(units.join("nexus-daemon.service"), "[Service]\n").unwrap();
    fs::write(units.join("nexus-gateway.service"), "[Service]\n").unwrap();
    let systemctl = fake_bin.join("systemctl");
    fs::write(
        &systemctl,
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$NEXUS_TEST_SYSTEMCTL_LOG\"\n",
    )
    .unwrap();
    fs::set_permissions(&systemctl, fs::Permissions::from_mode(0o755)).unwrap();

    let output = nexus()
        .args(["daemon", "uninstall"])
        .env("PATH", &fake_bin)
        .env("HOME", root.path())
        .env("XDG_CONFIG_HOME", &config)
        .env("NEXUS_HOME", &home)
        .env("NEXUS_TEST_SYSTEMCTL_LOG", &calls)
        .env_remove("NEXUS_NAME")
        .env_remove("NEXUS_CLIENT_KEY")
        .env_remove("NEXUS_SESSION_ID")
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let calls = fs::read_to_string(calls).unwrap();
    let gateway = calls.find("--user stop nexus-gateway.service").unwrap();
    let daemon = calls.find("--user stop nexus-daemon.service").unwrap();
    assert!(gateway < daemon, "{calls}");
    assert!(!units.join("nexus-gateway.service").exists());
    assert!(!units.join("nexus-daemon.service").exists());
}

#[cfg(target_os = "linux")]
#[test]
fn wsl_install_transfers_a_running_self_daemon_to_systemd() {
    let root = tempfile::tempdir().unwrap();
    let fake_bin = root.path().join("bin");
    let config = root.path().join("config");
    let home = root.path().join("nexus-home");
    let calls = root.path().join("systemctl.log");
    fs::create_dir_all(&fake_bin).unwrap();
    fs::create_dir_all(&home).unwrap();

    let systemctl = fake_bin.join("systemctl");
    fs::write(
        &systemctl,
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$NEXUS_TEST_SYSTEMCTL_LOG\"\nif [ \"$*\" = \"--user start nexus-daemon.service\" ]; then\n  setsid /bin/sleep 30 </dev/null >/dev/null 2>&1 &\n  printf '%s\\n' \"$!\" > \"$NEXUS_HOME/daemon.pid\"\nfi\n",
    )
    .unwrap();
    fs::set_permissions(&systemctl, fs::Permissions::from_mode(0o755)).unwrap();

    let mut old_daemon = Command::new("/bin/sleep").arg("30").spawn().unwrap();
    fs::write(home.join("daemon.pid"), format!("{}\n", old_daemon.id())).unwrap();

    let output = nexus()
        .args(["daemon", "install", "--binary", env!("CARGO_BIN_EXE_nexus")])
        .env("PATH", format!("{}:/usr/bin:/bin", fake_bin.display()))
        .env("HOME", root.path())
        .env("XDG_CONFIG_HOME", &config)
        .env("NEXUS_HOME", &home)
        .env("NEXUS_TEST_SYSTEMCTL_LOG", &calls)
        .env("WSL_DISTRO_NAME", "Ubuntu")
        .env("WSL_INTEROP", "/run/WSL/1_interop")
        .env_remove("NEXUS_NAME")
        .env_remove("NEXUS_CLIENT_KEY")
        .env_remove("NEXUS_SESSION_ID")
        .output()
        .unwrap();

    stop_fake_service(&home);

    let old_daemon_still_running = old_daemon.try_wait().unwrap().is_none();
    if old_daemon_still_running {
        old_daemon.kill().unwrap();
    }
    old_daemon.wait().unwrap();

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !old_daemon_still_running,
        "the self-daemon remained live after supervisor installation"
    );
    let calls = fs::read_to_string(calls).unwrap();
    assert!(
        calls.contains("--user start nexus-daemon.service"),
        "{calls}"
    );
}
