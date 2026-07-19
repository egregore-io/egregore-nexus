//! Codex app-server bridge integration coverage from the daemon package.
//!
//! This stays hermetic: it launches the fake app-server, creates the rollout a fresh human TUI
//! would write after its first turn, and proves the bridge resumes that thread, binds injection,
//! and forwards notifications. No tmux and no real codex binary are required here.

use std::sync::Arc;

use async_trait::async_trait;
use nexus::daemon::pty_supervisor::PtySupervisor;
use nexus::daemon::AppState;
use nexus_common::Config;
use nexus_contracts::events::WsEvent;
use nexus_contracts::ids::SessionId;
use nexus_contracts::ports::EventSink;
use nexus_contracts::{AgentUpdateKind, HarnessId, SpawnRequest};
use nexus_harness_codex::storage::CodexRuntimeStateRepo;
use nexus_harness_codex::{CodexAppServerClient, CodexBridge, SupervisorOpts};
use nexus_store::repos::IdentitySessions;
use nexus_store::{DaemonStore, Store};
use portable_pty::PtySize;

const FAKE_BIN: &str = env!("CARGO_BIN_EXE_nexus_fake_codex_app_server");

#[derive(Default)]
struct RecSink(tokio::sync::Mutex<Vec<WsEvent>>);

#[async_trait]
impl EventSink for RecSink {
    async fn emit(&self, event: WsEvent) {
        self.0.lock().await.push(event);
    }
}

fn tempdir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "nexus-bridge-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos(),
        tag,
    ));
    std::fs::create_dir_all(&dir).expect("create tempdir");
    dir
}

fn write_rollout(session_dir: &std::path::Path, thread_id: &str) {
    let rollout_dir = session_dir
        .join("codex-home")
        .join("sessions")
        .join("2026")
        .join("06");
    std::fs::create_dir_all(&rollout_dir).expect("create rollout dir");
    let first_line = serde_json::json!({
        "type": "session_meta",
        "payload": { "id": thread_id }
    });
    std::fs::write(
        rollout_dir.join("rollout-test.jsonl"),
        format!("{first_line}\n"),
    )
    .expect("write rollout");
}

#[cfg(unix)]
fn write_fake_codex_tui(dir: &std::path::Path) -> std::path::PathBuf {
    use std::os::unix::fs::PermissionsExt as _;

    let path = dir.join("codex");
    std::fs::write(
        &path,
        "#!/bin/sh\nprintf 'fake codex tui ready\\n'\nwhile true; do sleep 1; done\n",
    )
    .expect("write fake codex tui");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
        .expect("chmod fake codex tui");
    path
}

#[cfg(unix)]
struct EnvGuard {
    key: &'static str,
    previous: Option<std::ffi::OsString>,
}

#[cfg(unix)]
impl EnvGuard {
    fn set(key: &'static str, value: impl AsRef<std::ffi::OsStr>) -> Self {
        let previous = std::env::var_os(key);
        std::env::set_var(key, value);
        Self { key, previous }
    }
}

#[cfg(unix)]
impl Drop for EnvGuard {
    fn drop(&mut self) {
        match self.previous.take() {
            Some(value) => std::env::set_var(self.key, value),
            None => std::env::remove_var(self.key),
        }
    }
}

#[cfg(unix)]
fn write_fake_codex_command(dir: &std::path::Path) -> std::path::PathBuf {
    use std::os::unix::fs::PermissionsExt as _;

    let path = dir.join("codex");
    std::fs::write(
        &path,
        format!(
            "#!/bin/sh\nif [ \"$1\" = app-server ]; then exec '{}' \"$@\"; fi\nprintf 'fake codex tui ready\\n'\nwhile true; do sleep 1; done\n",
            FAKE_BIN
        ),
    )
    .expect("write fake codex command");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
        .expect("chmod fake codex command");
    path
}

#[cfg(unix)]
#[tokio::test]
async fn fresh_headed_codex_launch_persists_discovered_thread_in_resurrection_capsule() {
    let root = std::env::temp_dir().join(format!(
        "nx-cap-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos()
    ));
    let fake_bin_dir = root.join("bin");
    let work = root.join("work");
    let home = root.join("home");
    std::fs::create_dir_all(&fake_bin_dir).expect("create fake bin dir");
    std::fs::create_dir_all(&work).expect("create work dir");
    std::fs::create_dir_all(&home).expect("create home dir");
    write_fake_codex_command(&fake_bin_dir);

    let path = format!(
        "{}:{}",
        fake_bin_dir.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let _path = EnvGuard::set("PATH", path);
    let _home = EnvGuard::set("HOME", &home);

    let store_path = root.join("nexus.db");
    let daemon = DaemonStore::open(store_path.to_string_lossy().as_ref())
        .await
        .expect("split store");
    let store = Arc::new(daemon.compatibility_store());
    let state = AppState::wire_pty(store.clone(), &Config::default());

    let spawned = state
        .launch_agent_with_program(
            SpawnRequest {
                kind: HarnessId::new("codex").unwrap(),
                name: Some("headed-codex".into()),
                identity_policy: None,
                cwd: Some(work.to_string_lossy().into_owned()),
                project: Some("default".into()),
                role: None,
                initial_prompt: Some("boot".into()),
                resume: None,
                harness_args: Vec::new(),
                headless: false,
                backend: Some("pty".into()),
            },
            "default",
            "codex",
            None,
        )
        .await
        .expect("fresh headed Codex launch");

    let descriptor = IdentitySessions::new(&store)
        .find(&spawned.session_id.0)
        .await
        .expect("descriptor lookup")
        .expect("resurrection descriptor");
    assert_eq!(
        descriptor.native_resume_key.as_deref(),
        Some("fake-thread"),
        "the first daemon restart must not be the event that makes a headed Codex native thread durable"
    );

    state.teardown_owned_transports_for_shutdown().await;
    drop(state);
    drop(store);
    drop(daemon);
    let _ = std::fs::remove_dir_all(root);
}

#[cfg(unix)]
#[tokio::test]
async fn codex_remote_viewer_runs_in_daemon_owned_raw_pty() {
    let root = tempdir("codex-appserver-raw-viewer");
    let fake_bin_dir = root.join("bin");
    let cwd = root.join("work");
    std::fs::create_dir_all(&fake_bin_dir).unwrap();
    std::fs::create_dir_all(&cwd).unwrap();
    let fake_codex = write_fake_codex_tui(&fake_bin_dir);
    let supervisor = PtySupervisor::new();
    let session = SessionId("s_codex_raw_viewer".into());

    supervisor
        .launch_codex_remote_raw_pty(
            &session,
            fake_codex.to_str().unwrap(),
            &["--remote".into(), "unix:///tmp/fake-codex.sock".into()],
            cwd.to_str().unwrap(),
            PtySize {
                rows: 24,
                cols: 80,
                pixel_width: 0,
                pixel_height: 0,
            },
            &[],
        )
        .expect("raw Codex viewer should spawn");

    assert_eq!(supervisor.pty_backend_kind(&session), Some("raw"));
    assert!(
        supervisor.terminal_endpoint(&session).is_some(),
        "raw Codex viewer should expose the local terminal socket"
    );

    supervisor.kill(&session);
    let _ = std::fs::remove_dir_all(root);
}

#[cfg(unix)]
#[tokio::test]
async fn codex_restart_adopts_live_app_server_without_spawning_a_duplicate() {
    let root = std::env::temp_dir().join(format!(
        "nx-codex-restart-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos()
    ));
    let fake_bin_dir = root.join("bin");
    let cwd = root.join("work");
    let state_dir = root.join("state");
    std::fs::create_dir_all(&fake_bin_dir).unwrap();
    std::fs::create_dir_all(&cwd).unwrap();
    let fake_codex = write_fake_codex_command(&fake_bin_dir);

    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let owner = PtySupervisor::with_runtime_store(store.clone());
    let restarted = PtySupervisor::with_runtime_store(store.clone());
    let session = SessionId("s_codex_restart_adopts_live_server".into());
    let sink = Arc::new(RecSink::default());
    let size = PtySize {
        rows: 24,
        cols: 80,
        pixel_width: 0,
        pixel_height: 0,
    };

    owner
        .launch_codex_appserver(
            &session,
            "a_codex_restart_adopts_live_server",
            Some("restart-codex"),
            "default",
            "client-key",
            "nexus",
            cwd.to_str().unwrap(),
            size,
            sink.clone(),
            fake_codex.to_str().unwrap(),
            None,
            state_dir.to_str().unwrap(),
            None,
            Vec::new(),
            &[],
            None,
            "pty",
        )
        .await
        .expect("initial headed Codex launch");

    let repo = CodexRuntimeStateRepo::new(&store);
    let owner_state = repo
        .find_by_runtime_id(&session)
        .await
        .unwrap()
        .expect("owner runtime state");
    let owner_pid = owner_state
        .app_server_pid
        .expect("the original supervisor owns the app-server process");
    let owner_home = owner_state.codex_home.clone().expect("owner Codex home");
    let session_dir = state_dir.join("codex-sessions").join(&session.0);
    write_rollout(&session_dir, "fake-thread");

    restarted
        .respawn_codex_appserver(
            &session,
            "a_codex_restart_adopts_live_server",
            "restart-codex",
            "default",
            "client-key",
            "nexus",
            cwd.to_str().unwrap(),
            size,
            sink,
            fake_codex.to_str().unwrap(),
            None,
            state_dir.to_str().unwrap(),
            Some("fake-thread".into()),
            vec![owner_home],
            None,
            "pty",
        )
        .await
        .expect("restart should adopt the live app-server");

    let restarted_state = repo
        .find_by_runtime_id(&session)
        .await
        .unwrap()
        .expect("restarted runtime state");
    assert!(
        restarted_state.app_server_adopted,
        "daemon restart must adopt a live app-server instead of replacing it"
    );
    assert_eq!(
        restarted_state.app_server_pid, None,
        "an adopted app-server remains owned by the process that launched it"
    );
    assert!(
        nexus_common::process_ids::runtime_process_ids_for_pid(
            u32::try_from(owner_pid).expect("app-server pid fits u32"),
        )
        .is_some(),
        "adoption must leave the original app-server process alive"
    );

    let transport = restarted.codex_transport();
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while transport.bound_thread_id(&session).as_deref() != Some("fake-thread") {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("adopted bridge should bind the durable native thread");
    transport
        .prompt(&session, "after restart".into())
        .await
        .expect("adopted bridge should accept a turn");
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while !transport.active_turn_sessions().is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("adopted completion forwarder should observe turn completion");

    restarted.kill(&session);
    owner.kill(&session);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn bridge_emits_text_update_after_rollout_discovery() {
    let session_dir = tempdir("codex-appserver-launch");
    let sink = Arc::new(RecSink::default());
    let bridge = CodexBridge::new();
    let session = SessionId("test-codex-daemon-bridge".into());

    let sock_path = bridge
        .launch(
            session.clone(),
            SupervisorOpts {
                codex_exe: FAKE_BIN.to_string(),
                session_dir: session_dir.clone(),
                codex_home: None,
                model: None,
                bus_mcp: None,
                cwd: None,
                env: vec![],
            },
            sink.clone() as Arc<dyn EventSink>,
        )
        .await
        .expect("bridge launch should start fake app-server");

    write_rollout(&session_dir, "fake-thread");

    let transport = bridge.transport();
    let bind_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    while !transport.is_bound(&session) {
        assert!(
            tokio::time::Instant::now() < bind_deadline,
            "bridge did not bind transport after rollout discovery"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    let driver = CodexAppServerClient::connect(&sock_path, "driver")
        .await
        .expect("driver client should connect");
    driver
        .turn_start("fake-thread", "go")
        .await
        .expect("turn_start should succeed");

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let got = sink.0.lock().await.clone();
        if got.iter().any(|e| {
            matches!(
                e,
                WsEvent::AgentUpdate {
                    kind: AgentUpdateKind::Text,
                    ..
                }
            )
        }) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for Text AgentUpdate; got events: {got:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    assert!(bridge.kill(&session));
    let _ = std::fs::remove_dir_all(&session_dir);
}
