//! Hermetic native-boundary regressions: no provider, operator home, or live daemon.

use std::{
    os::unix::{fs::PermissionsExt, process::CommandExt},
    path::Path,
    process::{Child, Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{cli::read_client::ReadClient, daemon::AppState};
use nexus_contracts::{AgentTurnExecutionPort, SessionId, SpawnRequest, Tier};
use nexus_store::{
    repos::{
        AgentRuntimes, Agents, CommandIntentRow, CommandIntents, IdentitySessions,
        NativeThreadBindings, NewAgent, NewCommandIntent, NewNativeThreadBinding, Sessions,
    },
    DaemonStore,
};

use crate::daemon::opencode_native_forwarder::OpenCodeRuntimeStateRepo;

const NAME: &str = "revival-owner";
const ROOT: &str = "ses_fixture_exact";

// Substitute only the external native viewer. Production still generates the plugin files,
// captures the ready owner, launches a real PTY, persists bindings, and attaches its transport.
// This shim follows the generated serve wrapper's exact session/isolation selection rules and
// stores native roots in SQLite, so an accidental fresh launch creates a detectably different ID.
const NATIVE_VIEWER: &str = r#"#!/usr/bin/env python3
import json, os, pathlib, sqlite3, sys, time
home = pathlib.Path(os.environ['HOME'])
args = sys.argv[2:]
resume = None
for i, arg in enumerate(args):
    if arg in ('-s', '--session'): resume = args[i + 1]
    elif arg.startswith('--session='): resume = arg.split('=', 1)[1]
isolated = not resume or os.environ.get('NEXUS_OPENCODE_RESUME_ISOLATED') == '1'
db_path = pathlib.Path(os.environ['NEXUS_OPENCODE_HOME']) / 'opencode.db' if isolated else home / 'operator-opencode.db'
record = {'argv': sys.argv[1:], 'session': os.environ['NEXUS_SESSION_ID'], 'agent': os.environ['NEXUS_AGENT_ID'], 'pid': os.getpid(), 'resume': resume, 'isolated': isolated, 'db': str(db_path), 'root': None}
log = home / 'native-launches.jsonl'
if (home / 'fail-next').exists():
    with log.open('a') as out: out.write(json.dumps(record) + '\n')
    print('fixture native startup rejected', flush=True)
    sys.exit(42)
db = sqlite3.connect(db_path)
db.execute('CREATE TABLE IF NOT EXISTS session (id TEXT PRIMARY KEY)')
if resume:
    if not db.execute('SELECT id FROM session WHERE id = ?', (resume,)).fetchone():
        with log.open('a') as out: out.write(json.dumps(record) + '\n')
        print('fixture exact native root absent from selected database', flush=True)
        sys.exit(43)
    root = resume
else:
    count = db.execute('SELECT COUNT(*) FROM session').fetchone()[0]
    root = 'ses_fixture_exact' if count == 0 else 'ses_accidental_fresh_' + str(count)
    db.execute('INSERT INTO session VALUES (?)', (root,))
    db.commit()
db.close()
record['root'] = root
with log.open('a') as out: out.write(json.dumps(record) + '\n')
pathlib.Path(os.environ['NEXUS_OPENCODE_READY_PATH']).write_text(json.dumps({'sessionId': root, 'pid': os.getpid(), 'readyOwner': os.environ['NEXUS_NATIVE_READY_OWNER']}))
# Even a failed pre-bind launch cannot leave a permanent fixture process behind.
deadline = time.monotonic() + 30
while time.monotonic() < deadline: time.sleep(.05)
"#;

struct Fixture<E = crate::cli::ambient::TestEnvGuard> {
    state: Option<AppState>,
    _env: E,
    dir: tempfile::TempDir,
}

impl Fixture {
    async fn new() -> Self {
        Self::with_dir(tempfile::tempdir().unwrap()).await
    }

    async fn with_dir(dir: tempfile::TempDir) -> Self {
        let env = Self::configure(&dir);
        let state = Self::open(dir.path(), None).await;
        Self {
            state: Some(state),
            _env: env,
            dir,
        }
    }

    fn configure(dir: &tempfile::TempDir) -> crate::cli::ambient::TestEnvGuard {
        let native = dir.path().join("native-viewer");
        std::fs::write(&native, NATIVE_VIEWER).unwrap();
        std::fs::set_permissions(&native, std::fs::Permissions::from_mode(0o755)).unwrap();
        crate::cli::ambient::TestEnvGuard::new(&[
            ("HOME", dir.path().to_str()),
            ("NEXUS_HOME", dir.path().join(".nexus").to_str()),
            ("XDG_CONFIG_HOME", dir.path().join("config").to_str()),
            ("XDG_DATA_HOME", dir.path().join("data").to_str()),
            ("NEXUS_NODE_BIN", native.to_str()),
            ("NEXUS_OPENCODE_BIN", native.to_str()),
            ("NEXUS_OPENCODE_RESUME_ISOLATED", None),
            ("OPENCODE_DB", None),
            ("NEXUS_SKIP_AGENT_BOOTSTRAP_INSTALL", Some("1")),
            ("NEXUS_SKIP_AGENT_HOOK_INSTALL", Some("1")),
        ])
    }
}

impl<E> Fixture<E> {
    async fn open(home: &Path, absent_session: Option<&SessionId>) -> AppState {
        let daemon = DaemonStore::open(home.join("identity.db").to_str().unwrap())
            .await
            .unwrap();
        let store = Arc::new(daemon.compatibility_store());
        if let Some(session) = absent_session {
            // Check transport noninheritance at the store-open boundary. wire_pty starts
            // concurrent adoption, and ingress readiness does not wait for that work;
            // this assertion must not add lazy sidecar DDL racing those boot queries.
            assert!(
                OpenCodeRuntimeStateRepo::new(&store)
                    .find_by_runtime_id(session)
                    .await
                    .unwrap()
                    .is_none(),
                "reopened boot must not inherit transient OpenCode sidecars"
            );
        }
        let state = AppState::wire_pty(store, &nexus_common::Config::default());
        state.wait_for_runtime_identity_ready().await.unwrap();
        state
    }

    fn state(&self) -> &AppState {
        self.state.as_ref().unwrap()
    }

    fn records(&self) -> Vec<serde_json::Value> {
        std::fs::read_to_string(self.dir.path().join("native-launches.jsonl"))
            .unwrap_or_default()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    fn read(&self) -> ReadClient {
        ReadClient::from_store_with_caller_for_tests(
            self.state().store.clone(),
            "operator",
            "default",
            None,
            None,
            Tier::Admin,
        )
    }

    async fn launch(&self) -> SessionId {
        self.launch_with_backend("pty").await
    }

    async fn launch_with_backend(&self, backend: &str) -> SessionId {
        let request: SpawnRequest = serde_json::from_value(serde_json::json!({
            "kind": "opencode", "name": NAME, "backend": backend, "headless": false,
            "cwd": self.dir.path().to_str().unwrap()
        }))
        .unwrap();
        tokio::time::timeout(
            Duration::from_secs(8),
            self.state().launch_agent(request, "default", None),
        )
        .await
        .expect("bounded fresh native launch")
        .unwrap()
        .session_id
    }

    async fn stop(&self, session: &SessionId) {
        self.state().pty_supervisor().unwrap().kill(session);
        self.state()
            .presence
            .materialize_offline(session)
            .await
            .unwrap();
    }

    async fn reopen(&mut self, session: &SessionId) {
        self.stop(session).await;
        self.state()
            .drain_model_reporting_for_shutdown(Duration::from_secs(2))
            .await
            .unwrap();
        self.state().teardown_owned_transports_for_shutdown().await;
        drop(self.state.take());
        // Ready files, unlike the launch-local native DB, are transient startup evidence.
        let ready = self
            .dir
            .path()
            .join(".nexus/opencode-plugin-sessions")
            .join(&session.0)
            .join("ready.json");
        let _ = std::fs::remove_file(ready);
        self.state = Some(Self::open(self.dir.path(), Some(session)).await);
    }

    async fn clear_capsule_key(&self, session: &SessionId) {
        self.state()
            .store
            .identity_conn()
            .execute(
                "UPDATE identity_sessions SET native_resume_key = NULL WHERE runtime_id = ?1",
                libsql::params![session.0.clone()],
            )
            .await
            .unwrap();
    }

    async fn attach(
        &self,
    ) -> Result<nexus_contracts::SpawnResponse, nexus_contracts::ContractError> {
        let plan = self.read().attach_revive_plan(NAME).await?;
        tokio::time::timeout(
            Duration::from_secs(8),
            self.state().launch_agent(plan.spawn, &plan.project, None),
        )
        .await
        .expect("bounded attach native launch")
    }

    async fn assert_rebound(&self, session: &SessionId, first: &serde_json::Value) {
        let records = self.records();
        assert_eq!(
            records.len(),
            2,
            "offline owner must actually start a second native process"
        );
        let resumed = &records[1];
        assert_eq!(resumed["session"], session.0);
        assert_eq!(resumed["agent"], first["agent"]);
        assert_eq!(
            resumed["resume"], ROOT,
            "native resume must use the exact durable root"
        );
        assert_eq!(resumed["root"], ROOT);
        assert_eq!(
            resumed["db"], first["db"],
            "revival must use the original launch-local DB"
        );
        assert_eq!(resumed["isolated"], true);
        assert_ne!(resumed["pid"], first["pid"]);
        let supervisor = self.state().pty_supervisor().unwrap();
        assert!(supervisor.has_opencode_plugin(session));
        assert!(supervisor.transport().is_bound(session));
        assert_eq!(supervisor.transport().is_harness_alive(session), Some(true));
        let runtime = AgentRuntimes::new(&self.state().store)
            .find_by_runtime_id(&session.0)
            .await
            .unwrap()
            .unwrap();
        assert!(runtime.active);
        assert_eq!(runtime.agent_id, first["agent"].as_str().unwrap());
        assert_eq!(runtime.presence.as_deref(), Some("online"));
        assert_eq!(
            NativeThreadBindings::new(&self.state().store)
                .find_for_runtime("opencode", &session.0)
                .await
                .unwrap()
                .unwrap()
                .native_thread_id,
            ROOT
        );
    }
}

impl<E> Drop for Fixture<E> {
    fn drop(&mut self) {
        if let Some(state) = &self.state {
            state.pty_supervisor().unwrap().kill_all();
        }
        cleanup_native_viewers(self.dir.path());
    }
}

fn cleanup_native_viewers(home: &Path) {
    // Persistence rejection or a worker abort can precede supervisor cleanup. Inspect only
    // PIDs recorded by this fixture, and require its unique executable before sending a signal.
    #[cfg(target_os = "linux")]
    for line in std::fs::read_to_string(home.join("native-launches.jsonl"))
        .unwrap_or_default()
        .lines()
    {
        if let Ok(record) = serde_json::from_str::<serde_json::Value>(line) {
            let Some(pid) = record["pid"]
                .as_i64()
                .and_then(|pid| i32::try_from(pid).ok())
            else {
                continue;
            };
            if pid <= 0 {
                continue;
            }
            let owned = std::fs::read(format!("/proc/{pid}/cmdline")).is_ok_and(|argv| {
                argv.split(|byte| *byte == 0)
                    .any(|arg| arg == home.join("native-viewer").as_os_str().as_encoded_bytes())
            });
            if owned {
                unsafe {
                    libc::kill(pid, libc::SIGKILL);
                }
            }
        }
    }
}

const WORKER_CHILD_HOME: &str = "NEXUS_OPENCODE_WORKER_TEST_HOME";

struct WorkerChild {
    process: Child,
    home: tempfile::TempDir,
}

impl Drop for WorkerChild {
    fn drop(&mut self) {
        if self.process.try_wait().ok().flatten().is_none() {
            let _ = self.process.kill();
        }
        let _ = self.process.wait();
        for entry in std::fs::read_dir(self.home.path())
            .into_iter()
            .flatten()
            .flatten()
        {
            if entry.file_type().is_ok_and(|kind| kind.is_dir()) {
                cleanup_native_viewers(&entry.path());
            }
        }
    }
}

// A native stack overflow aborts instead of unwinding. Keep the actual production worker in
// a bounded child so the failure remains an assertion and cannot take down the whole suite.
#[test]
fn attach_revival_through_command_worker_fits_two_mib_stack() {
    worker_stack_case(
        "attach_revival_through_command_worker_fits_two_mib_stack",
        nexus_store::command_kinds::harness::LAUNCH,
        "pty",
    );
}

#[test]
fn admin_revival_through_command_worker_fits_two_mib_stack() {
    worker_stack_case(
        "admin_revival_through_command_worker_fits_two_mib_stack",
        nexus_store::command_kinds::admin::SPAWN,
        "pty",
    );
}

#[test]
fn tmux_revival_through_command_worker_fits_two_mib_stack() {
    if !Command::new("tmux")
        .arg("-V")
        .output()
        .is_ok_and(|output| output.status.success())
    {
        eprintln!("tmux worker regression not exercised: tmux is unavailable");
        return;
    }
    worker_stack_case(
        "tmux_revival_through_command_worker_fits_two_mib_stack",
        nexus_store::command_kinds::harness::LAUNCH,
        "tmux",
    );
}

fn worker_stack_case(test_name: &str, command_kind: &'static str, backend: &'static str) {
    worker_runtime_case(test_name, command_kind, backend, 1);
}

#[test]
fn attach_revival_through_command_worker_with_concurrent_boot() {
    worker_runtime_case(
        "attach_revival_through_command_worker_with_concurrent_boot",
        nexus_store::command_kinds::harness::LAUNCH,
        "pty",
        4,
    );
}

fn worker_runtime_case(
    test_name: &str,
    command_kind: &'static str,
    backend: &'static str,
    worker_threads: usize,
) {
    if let Some(home) = std::env::var_os(WORKER_CHILD_HOME) {
        let dir = tempfile::tempdir_in(home).unwrap();
        // Keep the thread-bound environment lock on this calling thread for the whole
        // child runtime lifetime; the async fixture owns only Send state and its directory.
        let _env = Fixture::configure(&dir);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .thread_stack_size(2 * 1024 * 1024)
            .enable_all()
            .build()
            .unwrap();
        // Keep setup and the command task on configured 2 MiB workers. The four-worker
        // case intentionally races boot repositories and runtime-sidecar schema checks;
        // the one-worker cases remain separate stack-size regression controls.
        runtime.block_on(async {
            tokio::spawn(async move {
                let fixture = Fixture {
                    state: Some(Fixture::<()>::open(dir.path(), None).await),
                    _env: (),
                    dir,
                };
                assert_worker_revival(fixture, command_kind, backend).await;
            })
            .await
            .unwrap();
        });
        return;
    }

    let home = tempfile::tempdir().unwrap();
    let output_path = home.path().join("child-output.log");
    let output = std::fs::File::create(&output_path).unwrap();
    let test_name = format!(
        "{}::{test_name}",
        module_path!().split_once("::").unwrap().1
    );
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", &test_name, "--nocapture", "--test-threads=1"])
        .env(WORKER_CHILD_HOME, home.path())
        .env_remove("RUST_MIN_STACK")
        .stdin(Stdio::null())
        .stdout(output.try_clone().unwrap())
        .stderr(output);
    unsafe {
        command.pre_exec(|| {
            let limit = libc::rlimit {
                rlim_cur: 0,
                rlim_max: 0,
            };
            if libc::setrlimit(libc::RLIMIT_CORE, &limit) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = WorkerChild {
        process: command.spawn().unwrap(),
        home,
    };
    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = child.process.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= deadline {
            panic!(
                "OpenCode worker child exceeded 30s:\n{}",
                std::fs::read_to_string(&output_path).unwrap_or_default()
            );
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let output = std::fs::read_to_string(output_path).unwrap();
    assert!(
        status.success(),
        "OpenCode revival must settle on a 2 MiB production command-worker stack; child {status}:\n{output}"
    );
    let success_marker = format!("worker revival and hot reuse verified: {command_kind}/{backend}");
    assert!(
        output.contains(&success_marker),
        "child must execute the regression, not silently select zero tests:\n{output}"
    );
    eprintln!("{success_marker} (2 MiB production worker)");
}

async fn assert_worker_revival(mut fixture: Fixture<()>, command_kind: &str, backend: &str) {
    use crate::daemon::command_worker;

    let session = fixture.launch_with_backend(backend).await;
    let first = fixture.records()[0].clone();
    fixture.clear_capsule_key(&session).await;
    fixture.reopen(&session).await;
    assert!(IdentitySessions::new(&fixture.state().store)
        .find(&session.0)
        .await
        .unwrap()
        .unwrap()
        .native_resume_key
        .is_none());
    assert!(Sessions::new(&fixture.state().store)
        .find_by_session_id(&session)
        .await
        .unwrap()
        .unwrap()
        .harness_session_id
        .is_none());
    assert_eq!(
        NativeThreadBindings::new(&fixture.state().store)
            .find_for_runtime("opencode", &session.0)
            .await
            .unwrap()
            .unwrap()
            .native_thread_id,
        ROOT
    );

    let plan = fixture.read().attach_revive_plan(NAME).await.unwrap();
    eprintln!("fixture reopened with exact durable binding; entering production command worker");
    let worker = command_worker::spawn(fixture.state().clone());
    // The same lane must settle malformed native arguments before any launch, then
    // remain available for the valid request. Existing identity tests cover key conflicts.
    let mut invalid = plan.spawn.clone();
    invalid.harness_args = vec!["-s".into()];
    let rejected = launch_through_worker(
        fixture.state(),
        "worker-argument-error",
        command_kind,
        &invalid,
    )
    .await;
    assert_eq!(rejected.status, "error", "{:?}", rejected.result_json);
    assert_eq!(fixture.records().len(), 1, "argument error must not spawn");
    for command_id in ["worker-offline-revival", "worker-hot-reuse"] {
        let row =
            launch_through_worker(fixture.state(), command_id, command_kind, &plan.spawn).await;
        assert_eq!(row.status, "done", "{command_id}: {:?}", row.error_json);
        let response: nexus_contracts::SpawnResponse =
            serde_json::from_str(row.result_json.as_deref().unwrap()).unwrap();
        assert_eq!(response.session_id, session);
        fixture.assert_rebound(&session, &first).await;
    }
    command_worker::begin_shutdown(fixture.state()).await;
    tokio::time::timeout(Duration::from_secs(3), worker)
        .await
        .unwrap()
        .unwrap();
    fixture
        .state()
        .drain_model_reporting_for_shutdown(Duration::from_secs(2))
        .await
        .unwrap();
    fixture
        .state()
        .teardown_owned_transports_for_shutdown()
        .await;
    eprintln!("worker revival and hot reuse verified: {command_kind}/{backend}");
}

async fn launch_through_worker(
    state: &AppState,
    command_id: &str,
    command_kind: &str,
    request: &SpawnRequest,
) -> CommandIntentRow {
    CommandIntents::new(&state.store)
        .insert_pending(NewCommandIntent {
            command_id: command_id.into(),
            kind: command_kind.into(),
            project: "default".into(),
            caller_name: "Local Operator".into(),
            caller_session_id: Some("local-operator".into()),
            caller_agent_id: None,
            caller_runtime_id: Some("local-operator".into()),
            caller_client_key: None,
            caller_principal_id: None,
            caller_kind: Some("local.human".into()),
            caller_tier: Some("admin".into()),
            idempotency_key: None,
            request_json: serde_json::to_string(request).unwrap(),
            created_at: nexus_common::now(),
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let row = CommandIntents::new(&state.store)
                .get(command_id)
                .await
                .unwrap()
                .unwrap();
            if row.status == "done" || row.status == "error" {
                break row;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("bounded production command-worker launch")
}

#[tokio::test(flavor = "current_thread")]
async fn fresh_ready_native_id_survives_in_identity_capsule_without_resume_argv() {
    let fixture = Fixture::new().await;
    let session = fixture.launch().await;
    assert!(fixture.records()[0]["resume"].is_null());
    let capsule = IdentitySessions::new(&fixture.state().store)
        .find(&session.0)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        capsule.native_resume_key.as_deref(),
        Some(ROOT),
        "fresh readiness ID must survive boot-local sidecar loss, even without -s argv"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn attach_plan_restarts_offline_owner_and_hot_reattach_does_not_spawn_twice() {
    let fixture = Fixture::new().await;
    let session = fixture.launch().await;
    let first = fixture.records()[0].clone();
    assert_eq!(fixture.attach().await.unwrap().session_id, session);
    assert_eq!(
        fixture.records().len(),
        1,
        "hot attach must reuse the live native process"
    );
    fixture.stop(&session).await;
    assert_eq!(fixture.attach().await.unwrap().session_id, session);
    fixture.assert_rebound(&session, &first).await;
    assert_eq!(fixture.attach().await.unwrap().session_id, session);
    assert_eq!(
        fixture.records().len(),
        2,
        "hot reattach must not spawn again"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn autowake_recovers_null_capsule_from_exact_durable_binding_after_reopen() {
    let mut fixture = Fixture::new().await;
    let session = fixture.launch().await;
    let first = fixture.records()[0].clone();
    NativeThreadBindings::new(&fixture.state().store)
        .claim(NewNativeThreadBinding {
            provider: "opencode".into(),
            kind: "harness".into(),
            native_thread_id: "ses_unrelated_newer".into(),
            agent_id: first["agent"].as_str().unwrap().into(),
            project: "default".into(),
            runtime_id: Some("s_other_runtime".into()),
        })
        .await
        .unwrap();
    fixture.state().store.identity_conn().execute(
        "UPDATE native_thread_bindings SET updated_at = updated_at + 10000 WHERE native_thread_id = 'ses_unrelated_newer'", (),
    ).await.unwrap();
    fixture.clear_capsule_key(&session).await;
    fixture.reopen(&session).await;
    assert!(Sessions::new(&fixture.state().store)
        .find_by_session_id(&session)
        .await
        .unwrap()
        .unwrap()
        .harness_session_id
        .is_none());
    let revived = tokio::time::timeout(
        Duration::from_secs(8),
        fixture.state().ensure_opencode_plugin_live(NAME, "default"),
    )
    .await
    .expect("bounded autowake")
    .unwrap();
    assert_eq!(revived, session);
    fixture.assert_rebound(&session, &first).await;
}

#[tokio::test(flavor = "current_thread")]
async fn autowake_without_any_native_id_fails_before_process_or_new_conversation() {
    let mut fixture = Fixture::new().await;
    let session = fixture.launch().await;
    fixture.clear_capsule_key(&session).await;
    fixture
        .state()
        .store
        .identity_conn()
        .execute(
            "DELETE FROM native_thread_bindings WHERE last_runtime_id = ?1",
            libsql::params![session.0.clone()],
        )
        .await
        .unwrap();
    fixture.reopen(&session).await;
    assert!(
        fixture.read().attach_revive_plan(NAME).await.is_err(),
        "CLI must refuse missing native identity"
    );
    let result = tokio::time::timeout(
        Duration::from_secs(8),
        fixture.state().ensure_opencode_plugin_live(NAME, "default"),
    )
    .await
    .expect("bounded missing-ID autowake");
    assert!(
        result.is_err(),
        "missing exact native identity must fail closed, got {result:?}"
    );
    assert_eq!(
        fixture.records().len(),
        1,
        "missing identity must be rejected before launching native code"
    );
    assert!(!fixture
        .state()
        .pty_supervisor()
        .unwrap()
        .has_opencode_plugin(&session));
}

#[tokio::test(flavor = "current_thread")]
async fn attach_offline_owner_propagates_native_startup_failure() {
    let fixture = Fixture::new().await;
    let session = fixture.launch().await;
    fixture.stop(&session).await;
    std::fs::write(fixture.dir.path().join("fail-next"), b"").unwrap();
    let result = fixture.attach().await;
    assert!(
        result.is_err(),
        "attach must propagate failed startup, not return dead owner: {result:?}"
    );
    assert_eq!(
        fixture.records().len(),
        2,
        "failure must originate at the attempted native restart"
    );
    assert!(!fixture
        .state()
        .pty_supervisor()
        .unwrap()
        .has_opencode_plugin(&session));
}

#[tokio::test(flavor = "current_thread")]
async fn attach_restarts_dead_viewer_even_when_plugin_and_transport_are_still_mapped() {
    let fixture = Fixture::new().await;
    let session = fixture.launch().await;
    let first = fixture.records()[0].clone();
    // Simulate native crash, not administrative teardown: retain both supervisor maps.
    assert_eq!(
        unsafe { libc::kill(first["pid"].as_i64().unwrap() as i32, libc::SIGKILL) },
        0
    );
    let supervisor = fixture.state().pty_supervisor().unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while supervisor.transport().is_harness_alive(&session) != Some(false) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("native process exit becomes observable");
    assert!(supervisor.has_opencode_plugin(&session));
    assert!(supervisor.transport().is_bound(&session));
    assert_eq!(fixture.attach().await.unwrap().session_id, session);
    fixture.assert_rebound(&session, &first).await;
}

#[tokio::test(flavor = "current_thread")]
async fn autowake_rejects_capsule_binding_conflict_before_native_process() {
    let mut fixture = Fixture::new().await;
    let session = fixture.launch().await;
    IdentitySessions::new(&fixture.state().store)
        .set_native_resume_key(&session.0, "ses_conflicting_capsule")
        .await
        .unwrap();
    fixture.reopen(&session).await;
    let result = tokio::time::timeout(
        Duration::from_secs(8),
        fixture.state().ensure_opencode_plugin_live(NAME, "default"),
    )
    .await
    .expect("bounded conflicting-evidence autowake");
    assert!(
        result.is_err(),
        "conflicting exact roots must not be guessed: {result:?}"
    );
    assert_eq!(
        fixture.records().len(),
        1,
        "conflicting capsule/binding must fail before native process"
    );
    assert!(!fixture
        .state()
        .pty_supervisor()
        .unwrap()
        .has_opencode_plugin(&session));
}

#[tokio::test(flavor = "current_thread")]
async fn autowake_rejects_foreign_agent_binding_before_native_process() {
    let mut fixture = Fixture::new().await;
    let session = fixture.launch().await;
    IdentitySessions::new(&fixture.state().store)
        .set_native_resume_key(&session.0, ROOT)
        .await
        .unwrap();
    Agents::new(&fixture.state().store)
        .create(NewAgent {
            agent_id: "a_foreign_owner".into(),
            project: "default".into(),
            name: None,
            default_harness: None,
            role: None,
            tier: None,
            owner: None,
        })
        .await
        .unwrap();
    fixture.state().store.identity_conn().execute(
        "UPDATE native_thread_bindings SET agent_id = 'a_foreign_owner' WHERE last_runtime_id = ?1",
        libsql::params![session.0.clone()],
    ).await.unwrap();
    fixture.reopen(&session).await;
    let result = tokio::time::timeout(
        Duration::from_secs(8),
        fixture.state().ensure_opencode_plugin_live(NAME, "default"),
    )
    .await
    .expect("bounded foreign-binding autowake");
    assert!(
        result.is_err(),
        "native root ownership mismatch must fail: {result:?}"
    );
    assert_eq!(
        fixture.records().len(),
        1,
        "foreign agent binding must fail before native process"
    );
    assert!(!fixture
        .state()
        .pty_supervisor()
        .unwrap()
        .has_opencode_plugin(&session));
}

#[tokio::test(flavor = "current_thread")]
async fn autowake_rejects_malformed_harness_metadata_before_native_process() {
    let fixture = Fixture::new().await;
    let session = fixture.launch().await;
    fixture.stop(&session).await;
    fixture
        .state()
        .store
        .conn
        .execute(
            "UPDATE sessions SET agent = 'claude' WHERE session_id = ?1",
            libsql::params![session.0.clone()],
        )
        .await
        .unwrap();
    let malformed = Sessions::new(&fixture.state().store)
        .find_by_session_id(&session)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(malformed.agent.as_deref(), Some("claude"));
    assert_eq!(malformed.transport.as_deref(), Some("opencode-plugin"));

    let result = tokio::time::timeout(
        Duration::from_secs(8),
        fixture.state().ensure_opencode_plugin_live(NAME, "default"),
    )
    .await
    .expect("bounded malformed-metadata autowake");
    assert!(
        result.is_err(),
        "OpenCode transport with another harness must fail closed: {result:?}"
    );
    assert_eq!(
        fixture.records().len(),
        1,
        "malformed harness metadata must fail before native process"
    );
    assert!(!fixture
        .state()
        .pty_supervisor()
        .unwrap()
        .has_opencode_plugin(&session));
}

#[tokio::test(flavor = "current_thread")]
async fn explicit_native_request_rejects_other_root_without_reusing_hot_owner() {
    assert_explicit_native_request_preserves_owner(false, false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn explicit_native_request_rejects_other_root_without_reviving_offline_owner() {
    assert_explicit_native_request_preserves_owner(true, false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn agent_id_request_rejects_other_root_without_reusing_hot_owner() {
    assert_explicit_native_request_preserves_owner(false, true).await;
}

async fn assert_explicit_native_request_preserves_owner(offline: bool, target_by_agent_id: bool) {
    let fixture = Fixture::new().await;
    let session = fixture.launch().await;
    let first = fixture.records()[0].clone();
    let requested_root = "ses_other_owned_root";
    let missing_runtime = SessionId("s_absent_older_runtime".into());
    NativeThreadBindings::new(&fixture.state().store)
        .claim(NewNativeThreadBinding {
            provider: "opencode".into(),
            kind: "harness".into(),
            native_thread_id: requested_root.into(),
            agent_id: first["agent"].as_str().unwrap().into(),
            project: "default".into(),
            runtime_id: Some(missing_runtime.0.clone()),
        })
        .await
        .unwrap();
    assert!(Sessions::new(&fixture.state().store)
        .find_by_session_id(&missing_runtime)
        .await
        .unwrap()
        .is_none());
    if offline {
        fixture.stop(&session).await;
    }
    let capsule_before = IdentitySessions::new(&fixture.state().store)
        .find(&session.0)
        .await
        .unwrap()
        .unwrap();
    let native_before = OpenCodeRuntimeStateRepo::new(&fixture.state().store)
        .find_by_runtime_id(&session)
        .await
        .unwrap()
        .unwrap();
    let mut request: SpawnRequest = serde_json::from_value(serde_json::json!({
        "kind": "opencode", "name": if target_by_agent_id { first["agent"].as_str().unwrap() } else { NAME },
        "backend": "pty", "headless": false,
        "cwd": fixture.dir.path().to_str().unwrap()
    }))
    .unwrap();
    request.harness_args = vec!["-s".into(), requested_root.into()];

    let result = tokio::time::timeout(
        Duration::from_secs(8),
        fixture.state().launch_agent(request, "default", None),
    )
    .await
    .expect("bounded explicit native-root launch");
    assert!(
        result.is_err(),
        "explicit -s {requested_root} must not silently select {ROOT} (offline={offline}, agent_id={target_by_agent_id}): {result:?}"
    );
    assert_eq!(
        fixture.records().len(),
        1,
        "conflicting explicit root must be rejected before any native process attempt"
    );
    assert_eq!(
        IdentitySessions::new(&fixture.state().store)
            .find(&session.0)
            .await
            .unwrap()
            .unwrap(),
        capsule_before,
        "rejected root request must preserve the original resurrection capsule"
    );
    assert_eq!(
        OpenCodeRuntimeStateRepo::new(&fixture.state().store)
            .find_by_runtime_id(&session)
            .await
            .unwrap()
            .unwrap(),
        native_before,
        "rejected root request must preserve the original launch-local DB and native root"
    );
    assert_eq!(
        NativeThreadBindings::new(&fixture.state().store)
            .find_for_runtime("opencode", &session.0)
            .await
            .unwrap()
            .unwrap()
            .native_thread_id,
        ROOT
    );
    let supervisor = fixture.state().pty_supervisor().unwrap();
    assert_eq!(supervisor.has_opencode_plugin(&session), !offline);
    if !offline {
        assert_eq!(
            supervisor.transport().is_harness_alive(&session),
            Some(true)
        );
    }
    assert_eq!(
        AgentRuntimes::new(&fixture.state().store)
            .find_by_runtime_id(&session.0)
            .await
            .unwrap()
            .unwrap()
            .active,
        !offline,
        "rejected root request must not change the original owner's liveness"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn agent_id_request_rejects_malformed_resume_flag_before_effects() {
    let fixture = Fixture::new().await;
    let session = fixture.launch().await;
    let first = fixture.records()[0].clone();
    let mut request: SpawnRequest = serde_json::from_value(serde_json::json!({
        "kind": "opencode", "name": first["agent"], "backend": "pty", "headless": false,
        "cwd": fixture.dir.path().to_str().unwrap()
    }))
    .unwrap();
    request.harness_args = vec!["-s".into()];
    let result = tokio::time::timeout(
        Duration::from_secs(8),
        fixture.state().launch_agent(request, "default", None),
    )
    .await
    .expect("bounded malformed agent-ID resume request");
    assert!(
        result.is_err(),
        "agent-ID shortcut must not bypass native flag validation: {result:?}"
    );
    assert_eq!(
        fixture.records().len(),
        1,
        "malformed flag must not launch another process"
    );
    assert_eq!(
        fixture
            .state()
            .pty_supervisor()
            .unwrap()
            .transport()
            .is_harness_alive(&session),
        Some(true)
    );
    assert_eq!(
        IdentitySessions::new(&fixture.state().store)
            .find(&session.0)
            .await
            .unwrap()
            .unwrap()
            .native_resume_key
            .as_deref(),
        Some(ROOT)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn agent_id_request_reuses_matching_native_root_without_new_process() {
    let fixture = Fixture::new().await;
    let session = fixture.launch().await;
    let first = fixture.records()[0].clone();
    let mut request: SpawnRequest = serde_json::from_value(serde_json::json!({
        "kind": "opencode", "name": first["agent"], "backend": "pty", "headless": false,
        "cwd": fixture.dir.path().to_str().unwrap()
    }))
    .unwrap();
    request.harness_args = vec!["-s".into(), ROOT.into()];
    let result = tokio::time::timeout(
        Duration::from_secs(8),
        fixture.state().launch_agent(request, "default", None),
    )
    .await
    .expect("bounded matching agent-ID resume request")
    .unwrap();
    assert_eq!(result.session_id, session);
    assert_eq!(
        fixture.records(),
        [first],
        "matching root must retain the original native process"
    );
    assert_eq!(
        fixture
            .state()
            .pty_supervisor()
            .unwrap()
            .transport()
            .is_harness_alive(&session),
        Some(true)
    );
    assert_eq!(
        NativeThreadBindings::new(&fixture.state().store)
            .find_for_runtime("opencode", &session.0)
            .await
            .unwrap()
            .unwrap()
            .native_thread_id,
        ROOT
    );
}
