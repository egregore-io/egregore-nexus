//! Integration test for the Codex app-server supervisor.
//!
//! Uses the hermetic `fake_codex_app_server` binary (built from
//! `src/bin/fake_codex_app_server.rs`) in place of a real `codex` binary.
//! No network, no model, no real credentials needed.
//!
//! Run: `cargo test -p nexus-harness-codex --test app_server_supervisor`

use nexus_harness_codex::{CodexAppServer, SupervisorOpts};

/// Path to the hermetic fake app-server binary, injected by Cargo because
/// `fake_codex_app_server` is a `[[bin]]` of `nexus-harness-codex`.
const FAKE_BIN: &str = env!("CARGO_BIN_EXE_fake_codex_app_server");

#[cfg(target_os = "linux")]
fn process_group_of(pid: u32) -> Option<u32> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let close = stat.rfind(')')?;
    let fields = stat[close + 1..].split_whitespace().collect::<Vec<_>>();
    fields.get(2)?.parse::<u32>().ok()
}

#[cfg(target_os = "linux")]
fn process_group_members(pgrp: u32) -> Vec<u32> {
    let mut members = Vec::new();
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return members;
    };
    for entry in entries.flatten() {
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        if process_group_of(pid) == Some(pgrp) {
            members.push(pid);
        }
    }
    members.sort_unstable();
    members
}

#[cfg(target_os = "linux")]
fn cleanup_process_group_for_failed_drop(pid: u32, pgid: u32) {
    const SIGTERM: i32 = 15;
    const SIGKILL: i32 = 9;

    extern "C" {
        fn kill(pid: i32, sig: i32) -> i32;
        fn waitpid(pid: i32, status: *mut i32, options: i32) -> i32;
    }

    // SAFETY: The test-created process group is isolated by the supervisor's process_group(0).
    unsafe {
        let _ = kill(-(pgid as i32), SIGTERM);
    }
    std::thread::sleep(std::time::Duration::from_millis(100));
    if !process_group_members(pgid).is_empty() {
        // SAFETY: same isolated test process group as above.
        unsafe {
            let _ = kill(-(pgid as i32), SIGKILL);
        }
    }
    let mut status = 0;
    // SAFETY: `pid` is the direct child spawned by this test process. If it has already exited
    // or was reaped by the production drop path, waitpid will simply fail and the cleanup remains
    // best-effort.
    unsafe {
        let _ = waitpid(pid as i32, &mut status, 0);
    }
}

/// Build a unique temp directory path for this test run.
fn tempdir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "nexus-supervisor-test-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos(),
        tag,
    ));
    std::fs::create_dir_all(&dir).expect("create test tempdir");
    dir
}

/// Clean up a temp directory created by `tempdir()`.
fn cleanup(dir: &std::path::Path) {
    let _ = std::fs::remove_dir_all(dir);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn dropping_supervisor_reaps_owned_app_server_process_group() {
    let dir = tempdir("drop-reaps-process-group");

    let srv = CodexAppServer::start(SupervisorOpts {
        codex_exe: FAKE_BIN.to_string(),
        session_dir: dir.clone(),
        codex_home: None,
        model: None,
        bus_mcp: None,
        cwd: None,
        env: vec![],
    })
    .await
    .expect("fake app-server should start");

    let ledger = srv
        .process_ledger()
        .expect("spawned app-server should expose process ids");
    assert_eq!(
        ledger.os_pid, ledger.os_pgid,
        "process_group(0) should make the app-server the process-group leader"
    );
    assert!(
        process_group_members(ledger.os_pgid).contains(&ledger.os_pid),
        "process group {} should contain spawned fake app-server {} before drop",
        ledger.os_pgid,
        ledger.os_pid
    );

    drop(srv);

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        let members = process_group_members(ledger.os_pgid);
        if members.is_empty() {
            break;
        }
        if std::time::Instant::now() >= deadline {
            cleanup_process_group_for_failed_drop(ledger.os_pid, ledger.os_pgid);
            panic!(
                "dropping CodexAppServer leaked fake app-server process group {} with members {members:?}",
                ledger.os_pgid
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Test: start + readiness poll + assertions + shutdown
// ---------------------------------------------------------------------------

#[tokio::test]
async fn start_waits_until_initialize_round_trips() {
    let dir = tempdir("supervisor");

    let opts = SupervisorOpts {
        // The fake binary accepts the same native app-server listen shape as real Codex.
        codex_exe: FAKE_BIN.to_string(),
        session_dir: dir.clone(),
        codex_home: None,
        model: Some("gpt-5".to_string()),
        bus_mcp: None,
        cwd: None,
        env: vec![],
    };

    let srv = CodexAppServer::start(opts)
        .await
        .expect("supervisor should start and complete initialize round-trip");

    // Unix exposes a socket file; Windows exposes a loopback WebSocket descriptor.
    #[cfg(unix)]
    assert!(
        srv.socket().exists(),
        "socket {:?} should exist after start()",
        srv.socket()
    );
    #[cfg(windows)]
    {
        let endpoint = srv.socket().to_string_lossy();
        assert!(
            endpoint.starts_with("ws://127.0.0.1:"),
            "Windows app-server endpoint must be loopback-only: {endpoint}"
        );
        let client = nexus_harness_codex::CodexAppServerClient::connect(
            srv.socket(),
            "windows-readiness-probe",
        )
        .await
        .expect("Windows loopback endpoint should accept a fresh initialized client");
        drop(client);
    }

    // config.toml is copied into CODEX_HOME by the supervisor. Nexus runtime settings such as the
    // model are passed as argv overrides, not persisted into this file.
    let config_path = dir.join("codex-home").join("config.toml");
    assert!(
        config_path.exists(),
        "config.toml should have been seeded at {:?}",
        config_path
    );

    let pid_path = dir.join("app-server.pid");
    let pid_line = std::fs::read_to_string(&pid_path).expect("read app-server.pid");
    let pid_json: serde_json::Value =
        serde_json::from_str(pid_line.trim()).expect("pid sidecar must be JSON");
    let process_ledger = srv.process_ledger().expect("spawned process ledger");
    assert_eq!(pid_json["pid"], process_ledger.os_pid);
    assert_eq!(pid_json["pgid"], process_ledger.os_pgid);

    // Clean shutdown.
    srv.shutdown().await;
    assert!(
        !pid_path.exists(),
        "app-server.pid should be removed during shutdown"
    );

    cleanup(&dir);
}

#[cfg(unix)]
#[tokio::test]
async fn start_adopts_existing_app_server_socket_without_owning_process() {
    let dir = tempdir("adopt-existing");

    let opts = SupervisorOpts {
        codex_exe: FAKE_BIN.to_string(),
        session_dir: dir.clone(),
        codex_home: None,
        model: Some("gpt-5".to_string()),
        bus_mcp: None,
        cwd: None,
        env: vec![],
    };

    let owner = CodexAppServer::start(opts.clone())
        .await
        .expect("owner start");
    let adopted = CodexAppServer::start(opts).await.expect("adopt existing");

    adopted.shutdown().await;
    let client = nexus_harness_codex::CodexAppServerClient::connect(owner.socket(), "probe")
        .await
        .expect("adopted handle shutdown must not kill existing app-server");
    drop(client);

    owner.shutdown().await;
    cleanup(&dir);
}

#[tokio::test]
async fn start_runs_app_server_in_requested_cwd() {
    let dir = tempdir("cwd-session");
    let launch_cwd = tempdir("launch-cwd");
    let probe = dir.join("cwd-probe.txt");

    let srv = CodexAppServer::start(SupervisorOpts {
        codex_exe: FAKE_BIN.to_string(),
        session_dir: dir.clone(),
        codex_home: None,
        model: None,
        bus_mcp: None,
        cwd: Some(launch_cwd.clone()),
        env: vec![(
            "FAKE_CODEX_CWD_PROBE".to_string(),
            probe.to_string_lossy().into_owned(),
        )],
    })
    .await
    .expect("start with cwd");

    let observed = std::fs::read_to_string(&probe).expect("read cwd probe");
    assert_eq!(std::path::Path::new(observed.trim()), launch_cwd.as_path());

    srv.shutdown().await;
    cleanup(&dir);
    cleanup(&launch_cwd);
}

#[tokio::test]
async fn start_with_existing_codex_home_does_not_rewrite_user_config() {
    let dir = tempdir("external-home-session");
    let external_home = tempdir("external-codex-home");

    let srv = CodexAppServer::start(SupervisorOpts {
        codex_exe: FAKE_BIN.to_string(),
        session_dir: dir.clone(),
        codex_home: Some(external_home.clone()),
        model: Some("gpt-5".to_string()),
        bus_mcp: Some(nexus_harness_codex::BusMcp {
            command: "nexus".to_string(),
            args: vec!["mcp".to_string()],
        }),
        cwd: None,
        env: vec![],
    })
    .await
    .expect("start with external codex home");

    assert!(
        !external_home.join("config.toml").exists(),
        "external CODEX_HOME config.toml must not be created or rewritten"
    );
    assert!(
        dir.join("app-server.stderr.log").exists(),
        "app-server stderr log should live under the Nexus session dir"
    );

    srv.shutdown().await;
    cleanup(&dir);
    cleanup(&external_home);
}
