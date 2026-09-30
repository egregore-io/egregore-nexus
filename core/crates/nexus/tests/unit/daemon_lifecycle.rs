#![cfg(test)]

use super::*;
use crate::cli::ambient::TestEnvGuard;
use clap::Parser;

#[test]
fn daemon_cli_parses_lifecycle_verbs() {
    let parse = |args: &[&str]| {
        DaemonCli::try_parse_from(std::iter::once("nexus").chain(args.iter().copied()))
            .unwrap()
            .into_daemon_args()
    };
    assert!(matches!(
        parse(&["daemon", "run"]).command,
        Some(DaemonCommand::Run)
    ));
    assert!(matches!(
        parse(&["daemon", "start"]).command,
        Some(DaemonCommand::Start(_))
    ));
    assert!(matches!(
        parse(&["daemon", "stop", "--force"]).command,
        Some(DaemonCommand::Stop(StopArgs { force: true }))
    ));
    assert!(matches!(
        parse(&["daemon", "restart", "--binary", "/bin/nexus"]).command,
        Some(DaemonCommand::Restart(_))
    ));
    assert!(matches!(
        parse(&["daemon", "install"]).command,
        Some(DaemonCommand::Install(_))
    ));
    assert!(matches!(
        parse(&["daemon", "uninstall"]).command,
        Some(DaemonCommand::Uninstall)
    ));
}

#[test]
fn ambient_tripwire_refuses_agent_supervision() {
    let _env = TestEnvGuard::new(&[
        ("NEXUS_NAME", Some("demoa")),
        ("NEXUS_CLIENT_KEY", Some("ck_demoa")),
        ("NEXUS_SESSION_ID", Some("s_demoa")),
    ]);

    assert!(matches!(
        ensure_operator_supervision(),
        Err(LifecycleError::OperatorOnly)
    ));
}

#[test]
fn refused_supervision_attempt_records_attribution() {
    let home = tempfile::tempdir().unwrap();
    let home_s = home.path().to_string_lossy().to_string();
    let _env = TestEnvGuard::new(&[
        ("NEXUS_HOME", Some(home_s.as_str())),
        ("NEXUS_NAME", Some("demoa")),
        ("NEXUS_CLIENT_KEY", Some("ck_demoa")),
        ("NEXUS_SESSION_ID", Some("s_demoa")),
    ]);

    assert!(matches!(
        ensure_operator_supervision_for("stop"),
        Err(LifecycleError::OperatorOnly)
    ));
    let body = fs::read_to_string(home.path().join(SHUTDOWN_ATTRIBUTION)).unwrap();
    assert!(body.contains(r#""verb":"stop""#));
    assert!(body.contains(r#""outcome":"refused""#));
    assert!(body.contains("NEXUS_NAME=demoa"));
    assert!(body.contains("NEXUS_CLIENT_KEY=ck_demoa"));
    assert!(body.contains("NEXUS_SESSION_ID=s_demoa"));
}

#[test]
fn ambient_tripwire_allows_bare_operator_terminal() {
    let _env = TestEnvGuard::new(&[
        ("NEXUS_NAME", None),
        ("NEXUS_CLIENT_KEY", None),
        ("NEXUS_SESSION_ID", None),
    ]);

    assert!(ensure_operator_supervision().is_ok());
}

#[test]
fn status_exit_codes_are_scriptable() {
    let home = tempfile::tempdir().unwrap();
    let paths = DaemonPaths {
        home: home.path().to_path_buf(),
        pid_file: home.path().join("daemon.pid"),
        lock_file: home.path().join("daemon.lock"),
        log_file: home.path().join("daemon.log"),
        gateway_file: home.path().join("gateway.json"),
        shutdown_attribution_file: home.path().join("shutdown.json"),
    };
    let base = DaemonStatus {
        paths,
        supervisor: "self (no crash restart)".into(),
        running: true,
        pid: Some(1),
        uptime_hint: None,
        restart_pending: false,
        binary_current: None,
        binary_running: None,
        gateway: GatewayStatus::Missing,
        lane_depths: Vec::new(),
        wedged_intents: 0,
        dead_letters: DeadLetterStatus {
            count: 0,
            oldest_age_hint: None,
        },
        transport_pairs: Vec::new(),
        store_error: None,
    };
    assert_eq!(base.exit_code(), ExitCode::SUCCESS);
    let mut with_dead_letters = base.clone();
    with_dead_letters.dead_letters.count = 2;
    assert_eq!(
        with_dead_letters.exit_code(),
        ExitCode::SUCCESS,
        "dead-letter count is an operator visibility signal, not a daemon health failure"
    );
    let mut degraded = base.clone();
    degraded.restart_pending = true;
    assert_eq!(degraded.exit_code(), ExitCode::from(1));
    let mut down = base;
    down.running = false;
    assert_eq!(down.exit_code(), ExitCode::from(2));
}

#[test]
fn service_path_preserves_install_path_and_adds_stable_fallbacks_once() {
    let rendered = build_service_execution_path(
        Path::new("/opt/nexus/bin/nexus"),
        [
            PathBuf::from("/home/user/.local/bin"),
            PathBuf::new(),
            PathBuf::from("/custom/bin"),
            PathBuf::from("/home/user/.local/bin"),
        ],
        [
            PathBuf::from("/home/user/.local/bin"),
            PathBuf::from("/usr/local/bin"),
            PathBuf::from("/usr/bin"),
        ],
        ':',
    );

    assert_eq!(
        rendered,
        "/opt/nexus/bin:/home/user/.local/bin:/custom/bin:/usr/local/bin:/usr/bin"
    );
}

#[test]
fn service_path_falls_back_when_install_path_is_missing() {
    let rendered = build_service_execution_path(
        Path::new("/opt/nexus/bin/nexus"),
        Vec::<PathBuf>::new(),
        [PathBuf::from("/usr/local/bin"), PathBuf::from("/usr/bin")],
        ':',
    );

    assert_eq!(rendered, "/opt/nexus/bin:/usr/local/bin:/usr/bin");
}

#[cfg(target_os = "linux")]
#[test]
fn running_current_binary_uses_fast_file_identity() {
    let pid = std::process::id();
    let (current, running, restart_pending) = binary_status(Some(pid));

    assert_eq!(current, running);
    assert!(current.unwrap().starts_with("file:"));
    assert!(!restart_pending);
}

#[cfg(target_os = "linux")]
#[test]
fn same_binary_file_recognizes_a_hard_link_without_a_second_read() {
    let directory = tempfile::tempdir().unwrap();
    let binary = directory.path().join("nexus");
    let running = directory.path().join("nexus-running");
    fs::write(&binary, b"release candidate").unwrap();
    fs::hard_link(&binary, &running).unwrap();

    assert!(same_binary_file(&binary, &running));
}

#[cfg(target_os = "linux")]
#[test]
fn systemd_unit_records_run_command_and_nexus_home() {
    let paths = DaemonPaths {
        home: PathBuf::from("/home/user/.nexus"),
        pid_file: PathBuf::from("/home/user/.nexus/daemon.pid"),
        lock_file: PathBuf::from("/home/user/.nexus/daemon.lock"),
        log_file: PathBuf::from("/home/user/.nexus/daemon.log"),
        gateway_file: PathBuf::from("/home/user/.nexus/gateway.json"),
        shutdown_attribution_file: PathBuf::from("/home/user/.nexus/shutdown.json"),
    };
    let unit = systemd_unit(Path::new("/usr/bin/nexus"), &paths);
    assert!(unit.contains("ExecStart=/usr/bin/nexus daemon run"));
    assert!(unit.contains("Restart=on-failure"));
    assert!(unit.contains("Environment=NEXUS_HOME=/home/user/.nexus"));
    assert!(!unit.contains("NEXUS_GATEWAY_DIR"));
}

#[cfg(target_os = "linux")]
#[test]
fn systemd_unit_pins_the_install_time_service_path() {
    let paths = DaemonPaths {
        home: PathBuf::from("/home/user/.nexus"),
        pid_file: PathBuf::from("/home/user/.nexus/daemon.pid"),
        lock_file: PathBuf::from("/home/user/.nexus/daemon.lock"),
        log_file: PathBuf::from("/home/user/.nexus/daemon.log"),
        gateway_file: PathBuf::from("/home/user/.nexus/gateway.json"),
        shutdown_attribution_file: PathBuf::from("/home/user/.nexus/shutdown.json"),
    };
    let unit = systemd_unit_with_path(
        Path::new("/opt/nexus/bin/nexus"),
        &paths,
        "/opt/nexus/bin:/home/user/.local/bin:/usr/bin",
    );

    assert!(unit.contains("Environment=\"PATH=/opt/nexus/bin:/home/user/.local/bin:/usr/bin\""));
}

#[test]
fn launchd_plist_pins_and_xml_escapes_the_service_path() {
    let plist = launchd_service_plist_with_path(
        Path::new("/Applications/Nexus/nexus"),
        Path::new("/Users/user/.nexus"),
        Path::new("/Users/user/.nexus/daemon.log"),
        "/Users/user/a&b:/usr/bin",
    );

    assert!(plist.contains("<key>PATH</key><string>/Users/user/a&amp;b:/usr/bin</string>"));
}

#[test]
fn windows_wrapper_pins_and_powershell_escapes_the_service_path() {
    let script = windows_task_registration_script_with_path(
        Path::new(r"C:\Tools\nexus.exe"),
        Path::new(r"C:\Users\e\.nexus"),
        r"C:\Users\e\AppData\Roaming\npm;C:\Program Files\Tool's Bin",
    );

    assert!(script.contains(
        "$env:Path = 'C:\\Users\\e\\AppData\\Roaming\\npm;C:\\Program Files\\Tool''s Bin'"
    ));
}

#[cfg(target_os = "linux")]
#[test]
fn systemd_unit_ignores_legacy_sqld_configuration() {
    let paths = DaemonPaths {
        home: PathBuf::from("/home/user/.nexus"),
        pid_file: PathBuf::from("/home/user/.nexus/daemon.pid"),
        lock_file: PathBuf::from("/home/user/.nexus/daemon.lock"),
        log_file: PathBuf::from("/home/user/.nexus/daemon.log"),
        gateway_file: PathBuf::from("/home/user/.nexus/gateway.json"),
        shutdown_attribution_file: PathBuf::from("/home/user/.nexus/shutdown.json"),
    };
    let unit = systemd_unit(Path::new("/usr/bin/nexus"), &paths);

    assert!(!unit.contains("nexus-sqld.service"));
    assert!(!unit.contains("NEXUS_DB_URL"));
    assert!(unit.contains("ExecStart=/usr/bin/nexus daemon run"));
}

#[test]
fn db_url_preflight_is_a_noop_after_embedded_store_cutover() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let config = Config {
        db_url: Some(format!("http://{addr}")),
        ..Config::default()
    };

    preflight_configured_db_url(&config).expect("legacy db_url must not gate daemon startup");
}

#[test]
fn self_daemon_supervisor_uses_the_shared_detach_boundary() {
    let source = include_str!("../../src/daemon/lifecycle.rs");

    assert!(source.contains("lifecycle_process::spawn_detached"));
    assert!(!source.contains("fn detach_command("));
}

#[test]
fn daemon_service_manager_commands_use_the_shared_bounded_process_boundary() {
    let source = include_str!("../../src/daemon/lifecycle.rs");

    assert!(source.contains("lifecycle_process::run_bounded"));
    assert!(!source.contains(".args(&self.args).output()"));
}
