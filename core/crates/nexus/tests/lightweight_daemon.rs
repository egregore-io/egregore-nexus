use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use nexus::daemon::gateway_stream_socket::{GatewayStreamFrame, GatewayStreamPublisher};
use nexus::daemon::{daemon_ipc, AppState};
use nexus_agent::{Adapter, AdapterInjectError, AdapterRegistry, MockAdapter, StreamEvent};
use nexus_common::{Config, NexusError};
use nexus_contracts::{
    ConsumeRequest, DaemonIpcCall, DaemonIpcCaller, DaemonIpcRequest, GatewayProjectionKind,
    HarnessId, Kind, Message, MessageId, ProjectId, Provenance, RegisterRequest, Request, Scope,
    SendRequest, SendTarget, SessionId, SpawnRequest, ThreadId, Tier, DAEMON_IPC_PROTOCOL_VERSION,
};
use nexus_store::repos::{
    AgentRuntimes, Agents, DeliveryObligations, IdentitySessions, Inbox, NewAgent, NewAgentRuntime,
    NewDeliveryObligation, NewIdentitySession, NewSession, Sessions, StreamEvents, Threads,
};
use nexus_store::DaemonStore;

fn unique_store_path(label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "nexus-lightweight-{label}-{}-{nonce}.db",
        std::process::id()
    ))
}

fn passive_pty_program() -> &'static str {
    #[cfg(windows)]
    {
        "cmd.exe"
    }
    #[cfg(not(windows))]
    {
        "cat"
    }
}

async fn boot_model_fixture() -> (tempfile::TempDir, Arc<nexus_store::Store>) {
    let dir = tempfile::tempdir().unwrap();
    let daemon = DaemonStore::open(dir.path().join("identity.db").to_str().unwrap())
        .await
        .unwrap();
    let store = Arc::new(daemon.compatibility_store());
    Agents::new(&store)
        .create(NewAgent {
            agent_id: "a_boot_model".into(),
            project: "default".into(),
            name: Some("boot-model".into()),
            default_harness: Some("other".into()),
            role: None,
            tier: None,
            owner: None,
        })
        .await
        .unwrap();
    AgentRuntimes::new(&store)
        .create(NewAgentRuntime {
            runtime_id: "s_boot_model".into(),
            agent_id: "a_boot_model".into(),
            harness: "other".into(),
            cwd: None,
            transport: None,
            presence: Some("offline".into()),
            active: true,
        })
        .await
        .unwrap();
    IdentitySessions::new(&store)
        .upsert(NewIdentitySession {
            runtime_id: "s_boot_model".into(),
            agent_id: "a_boot_model".into(),
            project: "default".into(),
            harness: "other".into(),
            mode: "headless".into(),
            backend: None,
            cwd: None,
            native_resume_key: None,
            client_key: Some("boot-model-key".into()),
        })
        .await
        .unwrap();
    (dir, store)
}

#[tokio::test]
async fn corrupt_model_authority_blocks_valid_boot_directory_restoration() {
    let (_dir, store) = boot_model_fixture().await;
    store.identity_conn().execute(
        "UPDATE agent_runtimes SET model_observer_token='', model_report_revision=1 WHERE runtime_id='s_boot_model'",
        (),
    ).await.unwrap();
    let state = AppState::wire(store.clone(), &Config::default());
    let mut current = Box::pin(state.wait_for_runtime_identity_ready());
    assert!(futures::poll!(&mut current).is_pending());
    let failed = tokio::time::timeout(std::time::Duration::from_secs(2), current)
        .await
        .expect("terminal boot outcome")
        .unwrap_err();
    let later = state.wait_for_runtime_identity_ready().await.unwrap_err();
    assert_eq!(failed.to_string(), later.to_string());
    assert!(
        Sessions::new(&store)
            .find_by_session_id(&SessionId("s_boot_model".into()))
            .await
            .unwrap()
            .is_none(),
        "failed model authority must prevent directory restoration"
    );
}

async fn claim_boot_model_owner(store: &nexus_store::Store, token: &str) {
    let report = serde_json::from_value(serde_json::json!({
        "backend": "fixture/opaque", "observerActive": true, "reportRevision": 1,
        "configured": {"status": "unknown", "capability": "unverified"},
        "turnSelected": {"status": "unknown", "capability": "unverified"},
        "responseReported": {"status": "unknown", "capability": "unsupported"}
    }))
    .unwrap();
    assert!(AgentRuntimes::new(store)
        .claim_model_observer("s_boot_model", "a_boot_model", None, token, &report)
        .await
        .unwrap());
}

#[tokio::test]
async fn boot_invalidates_surviving_model_owner_before_directory_insert_failure() {
    let (_dir, store) = boot_model_fixture().await;
    claim_boot_model_owner(&store, "surviving-owner").await;
    store.conn.execute_batch(
        "CREATE TRIGGER reject_boot_directory BEFORE INSERT ON sessions BEGIN SELECT RAISE(ABORT, 'boot directory insertion rejected'); END;"
    ).await.unwrap();
    let state = AppState::wire(store.clone(), &Config::default());
    let mut current = Box::pin(state.wait_for_runtime_identity_ready());
    assert!(futures::poll!(&mut current).is_pending());
    let error = tokio::time::timeout(std::time::Duration::from_secs(2), current)
        .await
        .unwrap()
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("boot directory insertion rejected"));
    assert_eq!(
        state
            .wait_for_runtime_identity_ready()
            .await
            .unwrap_err()
            .to_string(),
        error.to_string()
    );
    let row = AgentRuntimes::new(&store)
        .find_by_runtime_id("s_boot_model")
        .await
        .unwrap()
        .unwrap();
    assert!(
        row.model_observer_token.is_none(),
        "model invalidation precedes attempted directory insert"
    );
    assert_eq!(row.model_report_revision, 2);
    assert!(!row.model_report.unwrap().observer_active);
    assert!(Sessions::new(&store)
        .find_by_session_id(&SessionId("s_boot_model".into()))
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn boot_invalidates_surviving_owner_before_successful_directory_read() {
    let (_dir, store) = boot_model_fixture().await;
    claim_boot_model_owner(&store, "surviving-owner").await;
    let state = AppState::wire(store.clone(), &Config::default());
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        state.wait_for_runtime_identity_ready(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(Sessions::new(&store)
        .find_by_session_id(&SessionId("s_boot_model".into()))
        .await
        .unwrap()
        .is_some());
    let row = AgentRuntimes::new(&store)
        .find_by_runtime_id("s_boot_model")
        .await
        .unwrap()
        .unwrap();
    assert!(row.model_observer_token.is_none());
    assert_eq!(row.model_report_revision, 2);
    assert!(!row.model_report.unwrap().observer_active);
    // Cloning state and later readiness waits do not construct another coordinator or resweep.
    for _ in 0..3 {
        state
            .clone()
            .wait_for_runtime_identity_ready()
            .await
            .unwrap();
    }
    assert_eq!(
        AgentRuntimes::new(&store)
            .find_by_runtime_id("s_boot_model")
            .await
            .unwrap()
            .unwrap()
            .model_report_revision,
        2
    );
}

#[derive(Clone)]
struct ResumeKeyAdapter {
    inner: MockAdapter,
    resume_key: String,
}

#[derive(Clone, Default)]
struct ShutdownOrderingAdapter {
    inject_started: Arc<AtomicBool>,
    inject_cancelled: Arc<AtomicBool>,
    kill_saw_cancelled_inject: Arc<AtomicBool>,
    killed: Arc<tokio::sync::Notify>,
}

#[derive(Clone, Default)]
struct BlockingResumeAdapter {
    inner: MockAdapter,
    resume_attempts: Arc<AtomicUsize>,
    release: Arc<tokio::sync::Notify>,
}

struct InjectCancellationGuard(Arc<AtomicBool>);

impl Drop for InjectCancellationGuard {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl Adapter for ShutdownOrderingAdapter {
    async fn open_session(&self) -> Result<(), NexusError> {
        Ok(())
    }

    async fn resume(&self, _resume_key: &str) -> Result<(), NexusError> {
        Ok(())
    }

    async fn inject(&self, _prompt: String) -> Result<(), AdapterInjectError> {
        self.inject_started.store(true, Ordering::SeqCst);
        let _guard = InjectCancellationGuard(self.inject_cancelled.clone());
        self.killed.notified().await;
        Err(AdapterInjectError::Contract(NexusError::Adapter(
            "adapter closed for daemon shutdown".into(),
        )))
    }

    async fn stream_updates(&self) -> Result<Vec<StreamEvent>, NexusError> {
        Ok(Vec::new())
    }

    async fn kill(&self) {
        self.kill_saw_cancelled_inject.store(
            self.inject_cancelled.load(Ordering::SeqCst),
            Ordering::SeqCst,
        );
        self.killed.notify_waiters();
    }
}

#[async_trait::async_trait]
impl Adapter for BlockingResumeAdapter {
    async fn open_session(&self) -> Result<(), NexusError> {
        self.resume_attempts.fetch_add(1, Ordering::SeqCst);
        self.release.notified().await;
        Ok(())
    }

    async fn resume(&self, _resume_key: &str) -> Result<(), NexusError> {
        self.open_session().await
    }

    async fn inject(&self, prompt: String) -> Result<(), AdapterInjectError> {
        self.inner.inject(prompt).await
    }

    async fn stream_updates(&self) -> Result<Vec<StreamEvent>, NexusError> {
        self.inner.stream_updates().await
    }
}

#[async_trait::async_trait]
impl Adapter for ResumeKeyAdapter {
    async fn open_session(&self) -> Result<(), NexusError> {
        self.inner.open_session().await
    }

    async fn resume(&self, resume_key: &str) -> Result<(), NexusError> {
        self.inner.resume(resume_key).await
    }

    async fn inject(&self, prompt: String) -> Result<(), AdapterInjectError> {
        self.inner.inject(prompt).await
    }

    async fn stream_updates(&self) -> Result<Vec<StreamEvent>, NexusError> {
        self.inner.stream_updates().await
    }

    async fn acp_session_id(&self) -> Option<String> {
        Some(self.resume_key.clone())
    }
}

#[tokio::test]
async fn daemon_owned_launch_persists_its_resurrection_descriptor() {
    let path = unique_store_path("launch-resurrection");
    let daemon = DaemonStore::open(path.to_string_lossy().as_ref())
        .await
        .expect("split store");
    let store = Arc::new(daemon.compatibility_store());
    let mock = MockAdapter::new();
    let mut registry = AdapterRegistry::new();
    registry.register(
        &hid("codex"),
        Arc::new(move |_cwd| Arc::new(mock.clone()) as Arc<dyn Adapter>),
    );
    let state = AppState::wire_with_registry(store.clone(), &Config::default(), registry);
    state
        .wait_for_runtime_identity_ready()
        .await
        .expect("runtime identity ready");

    let spawned = state
        .launch_agent(
            SpawnRequest {
                kind: hid("codex"),
                name: Some("persistent-codex".into()),
                identity_policy: None,
                cwd: Some("/work/repo".into()),
                project: Some("default".into()),
                role: None,
                initial_prompt: None,
                resume: None,
                harness_args: Vec::new(),
                headless: true,
                backend: None,
            },
            "default",
            None,
        )
        .await
        .expect("launch");

    let descriptor = IdentitySessions::new(&store)
        .find(&spawned.session_id.0)
        .await
        .expect("descriptor lookup")
        .expect("daemon-owned launch must persist a resurrection descriptor");
    assert_eq!(descriptor.agent_id, format!("a_{}", spawned.session_id.0));
    assert_eq!(descriptor.harness, "codex");
    assert_eq!(descriptor.mode, "headless");
    assert_eq!(descriptor.backend.as_deref(), Some("acp"));
    assert_eq!(descriptor.cwd.as_deref(), Some("/work/repo"));
    let runtime = Sessions::new(&store)
        .find_by_session_id(&spawned.session_id)
        .await
        .expect("runtime lookup")
        .expect("runtime row");
    assert_eq!(descriptor.client_key, runtime.client_key);
    assert!(descriptor.client_key.is_some());

    drop(state);
    drop(store);
    drop(daemon);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}

#[tokio::test]
async fn graceful_shutdown_cancels_delivery_loops_before_killing_adapters() {
    let path = unique_store_path("shutdown-ordering");
    let daemon = DaemonStore::open(path.to_string_lossy().as_ref())
        .await
        .expect("split store");
    let store = Arc::new(daemon.compatibility_store());
    let adapter = ShutdownOrderingAdapter::default();
    let observed = adapter.clone();
    let mut registry = AdapterRegistry::new();
    registry.register(
        &hid("codex"),
        Arc::new(move |_cwd| Arc::new(adapter.clone()) as Arc<dyn Adapter>),
    );
    let state = AppState::wire_with_registry(store.clone(), &Config::default(), registry);
    // Match production ingress: direct launch must not run ahead of model/identity readiness.
    state
        .wait_for_runtime_identity_ready()
        .await
        .expect("boot ingress readiness");

    let spawned = state
        .launch_agent(
            SpawnRequest {
                kind: hid("codex"),
                name: Some("shutdown-order-target".into()),
                identity_policy: None,
                cwd: Some("/work/shutdown-order".into()),
                project: Some("default".into()),
                role: None,
                initial_prompt: None,
                resume: None,
                harness_args: Vec::new(),
                headless: true,
                backend: None,
            },
            "default",
            None,
        )
        .await
        .expect("launch");

    let message = Message {
        id: MessageId("m_shutdown_order".into()),
        project: ProjectId("default".into()),
        from: "operator".into(),
        scope: Scope::Dm,
        thread: None,
        topic: None,
        body: "hold this turn across shutdown".into(),
        summary: None,
        provenance: Provenance {
            from: "operator".into(),
            kind: Kind::Human,
            locality: Default::default(),
            access: None,
            thread: None,
            topic: None,
            stamp: None,
        },
        created_at: nexus_common::now(),
    };
    nexus_store::repos::Messages::new(&store)
        .insert(&message)
        .await
        .expect("message");
    state
        .realtime
        .enqueue(&spawned.session_id, &message.id)
        .await
        .expect("enqueue");

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    while !observed.inject_started.load(Ordering::SeqCst) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "delivery loop must enter the adapter before shutdown"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    state.teardown_owned_transports_for_shutdown().await;

    assert!(
        observed.kill_saw_cancelled_inject.load(Ordering::SeqCst),
        "graceful shutdown must cancel and join delivery loops before adapter kill can turn cancellation into a terminal contract error"
    );
    let mut delivery = store
        .conn
        .query(
            "SELECT state, error_code FROM in_flight WHERE message_id = ?1",
            [message.id.0.clone()],
        )
        .await
        .expect("delivery state query");
    let delivery = delivery
        .next()
        .await
        .expect("delivery state row")
        .expect("delivery state");
    assert_eq!(delivery.get::<String>(0).expect("state"), "injecting");
    assert_eq!(delivery.get::<Option<String>>(1).expect("error code"), None);

    // A wake task may already be racing with shutdown. Once teardown has crossed the loop fence,
    // a late ensure-alive must not create a replacement drainer against adapters being closed.
    observed.inject_started.store(false, Ordering::SeqCst);
    let runtime = Sessions::new(&store)
        .find_by_session_id(&spawned.session_id)
        .await
        .expect("runtime lookup")
        .expect("runtime row");
    state
        .ensure_alive_agent(runtime.agent_id.as_deref().expect("agent id"))
        .await
        .expect("late wake may rebind metadata but must remain fenced from delivery");

    let late_message = Message {
        id: MessageId("m_shutdown_order_late".into()),
        body: "late wake after shutdown fence".into(),
        created_at: nexus_common::now(),
        ..message.clone()
    };
    nexus_store::repos::Messages::new(&store)
        .insert(&late_message)
        .await
        .expect("late message");
    state
        .realtime
        .enqueue(&spawned.session_id, &late_message.id)
        .await
        .expect("late enqueue");
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(
        !observed.inject_started.load(Ordering::SeqCst),
        "shutdown must fence a concurrent wake from spawning a replacement delivery loop"
    );

    drop(state);
    drop(store);
    drop(daemon);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}

#[tokio::test]
async fn daemon_owned_headless_launch_persists_the_adapter_resume_key() {
    let path = unique_store_path("launch-native-resume");
    let daemon = DaemonStore::open(path.to_string_lossy().as_ref())
        .await
        .expect("split store");
    let store = Arc::new(daemon.compatibility_store());
    let adapter = ResumeKeyAdapter {
        inner: MockAdapter::new(),
        resume_key: "native-acp-session".into(),
    };
    let mut registry = AdapterRegistry::new();
    registry.register(
        &hid("opencode"),
        Arc::new(move |_cwd| Arc::new(adapter.clone()) as Arc<dyn Adapter>),
    );
    let state = AppState::wire_with_registry(store.clone(), &Config::default(), registry);
    state
        .wait_for_runtime_identity_ready()
        .await
        .expect("runtime identity ready");

    let spawned = state
        .launch_agent(
            SpawnRequest {
                kind: hid("opencode"),
                name: Some("persistent-opencode".into()),
                identity_policy: None,
                cwd: Some("/work/opencode".into()),
                project: Some("default".into()),
                role: None,
                initial_prompt: None,
                resume: None,
                harness_args: Vec::new(),
                headless: true,
                backend: None,
            },
            "default",
            None,
        )
        .await
        .expect("launch");

    assert_eq!(
        IdentitySessions::new(&store)
            .find(&spawned.session_id.0)
            .await
            .expect("descriptor lookup")
            .expect("descriptor")
            .native_resume_key
            .as_deref(),
        Some("native-acp-session")
    );

    drop(state);
    drop(store);
    drop(daemon);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}

#[tokio::test]
async fn daemon_owned_resume_key_updates_the_opaque_resurrection_capsule() {
    let path = unique_store_path("resume-key");
    let daemon = DaemonStore::open(path.to_string_lossy().as_ref())
        .await
        .expect("split store");
    let store = Arc::new(daemon.compatibility_store());
    let mock = MockAdapter::new();
    let mut registry = AdapterRegistry::new();
    registry.register(
        &hid("codex"),
        Arc::new(move |_cwd| Arc::new(mock.clone()) as Arc<dyn Adapter>),
    );
    let state = AppState::wire_with_registry(store.clone(), &Config::default(), registry);
    state
        .wait_for_runtime_identity_ready()
        .await
        .expect("runtime identity ready");
    let spawned = state
        .launch_agent(
            SpawnRequest {
                kind: hid("codex"),
                name: Some("resume-codex".into()),
                identity_policy: None,
                cwd: Some("/work/resume".into()),
                project: Some("default".into()),
                role: None,
                initial_prompt: None,
                resume: None,
                harness_args: Vec::new(),
                headless: true,
                backend: None,
            },
            "default",
            None,
        )
        .await
        .expect("launch");

    state
        .identity
        .set_resume_key(&spawned.session_id, "opaque-native-resume-key")
        .await
        .expect("resume key");
    assert_eq!(
        IdentitySessions::new(&store)
            .find(&spawned.session_id.0)
            .await
            .expect("descriptor lookup")
            .expect("descriptor")
            .native_resume_key
            .as_deref(),
        Some("opaque-native-resume-key")
    );

    drop(state);
    drop(store);
    drop(daemon);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}

#[tokio::test]
async fn daemon_owned_headed_launch_persists_its_exact_mode_and_backend() {
    let path = unique_store_path("headed-resurrection");
    let daemon = DaemonStore::open(path.to_string_lossy().as_ref())
        .await
        .expect("split store");
    let store = Arc::new(daemon.compatibility_store());
    let state = AppState::wire_pty(store.clone(), &Config::default());
    state
        .wait_for_runtime_identity_ready()
        .await
        .expect("runtime identity ready");

    let spawned = state
        .launch_agent_with_program(
            SpawnRequest {
                kind: hid("hermes"),
                name: Some("persistent-hermes-pty".into()),
                identity_policy: None,
                cwd: Some("/work/hermes".into()),
                project: Some("default".into()),
                role: None,
                initial_prompt: None,
                resume: None,
                harness_args: Vec::new(),
                headless: false,
                backend: Some("pty".into()),
            },
            "default",
            passive_pty_program(),
            None,
        )
        .await
        .expect("headed launch");

    let descriptor = IdentitySessions::new(&store)
        .find(&spawned.session_id.0)
        .await
        .expect("descriptor lookup")
        .expect("headed launch must persist a resurrection descriptor");
    assert_eq!(descriptor.harness, "hermes");
    assert_eq!(descriptor.mode, "headed");
    assert_eq!(descriptor.backend.as_deref(), Some("pty"));
    assert_eq!(descriptor.cwd.as_deref(), Some("/work/hermes"));
    let runtime = Sessions::new(&store)
        .find_by_session_id(&spawned.session_id)
        .await
        .expect("runtime lookup")
        .expect("runtime row");
    assert_eq!(descriptor.client_key, runtime.client_key);
    assert!(descriptor.client_key.is_some());

    state.teardown_owned_transports_for_shutdown().await;
    drop(state);
    drop(store);
    drop(daemon);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}

#[tokio::test]
async fn daemon_boot_rehydrates_the_nexus_runtime_identity_from_its_capsule() {
    let path = unique_store_path("boot-resurrection");
    {
        let daemon = DaemonStore::open(path.to_string_lossy().as_ref())
            .await
            .expect("first boot");
        let store = daemon.compatibility_store();
        Agents::new(&store)
            .create(NewAgent {
                agent_id: "a_boot_codex".into(),
                project: "default".into(),
                name: Some("boot-codex".into()),
                default_harness: Some("codex".into()),
                role: None,
                tier: Some("agent".into()),
                owner: None,
            })
            .await
            .expect("agent identity");
        IdentitySessions::new(&store)
            .upsert(NewIdentitySession {
                runtime_id: "s_boot_codex".into(),
                agent_id: "a_boot_codex".into(),
                project: "default".into(),
                harness: "codex".into(),
                mode: "headless".into(),
                backend: Some("acp".into()),
                cwd: Some("/work/boot".into()),
                native_resume_key: Some("codex-acp-native".into()),
                client_key: Some("stable-boot-client-key".into()),
            })
            .await
            .expect("resurrection capsule");
    }

    let daemon = DaemonStore::open(path.to_string_lossy().as_ref())
        .await
        .expect("second boot");
    let store = Arc::new(daemon.compatibility_store());
    let mock = MockAdapter::new();
    let mut registry = AdapterRegistry::new();
    registry.register(
        &hid("codex"),
        Arc::new(move |_cwd| Arc::new(mock.clone()) as Arc<dyn Adapter>),
    );
    let state = AppState::wire_with_registry(store.clone(), &Config::default(), registry);

    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        state.wait_for_runtime_identity_ready(),
    )
    .await
    .expect("boot identity readiness")
    .expect("successful model and identity boot");
    let row = Sessions::new(&store)
        .find_by_session_id(&SessionId("s_boot_codex".into()))
        .await
        .expect("live lookup")
        .expect("boot must rebuild the boot-scoped session row from identity_sessions");
    assert_eq!(row.agent_id.as_deref(), Some("a_boot_codex"));
    assert_eq!(row.name.as_deref(), Some("boot-codex"));
    assert_eq!(row.agent.as_deref(), Some("codex"));
    assert_eq!(row.transport.as_deref(), Some("acp"));
    assert_eq!(row.harness_session_id.as_deref(), Some("codex-acp-native"));
    assert_eq!(row.cwd.as_deref(), Some("/work/boot"));
    assert_eq!(row.client_key.as_deref(), Some("stable-boot-client-key"));

    drop(state);
    drop(store);
    drop(daemon);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}

#[tokio::test]
async fn daemon_boot_projects_only_the_newest_runtime_capsule_per_agent() {
    let path = unique_store_path("boot-latest-runtime-per-agent");
    {
        let daemon = DaemonStore::open(path.to_string_lossy().as_ref())
            .await
            .expect("first boot");
        let store = daemon.compatibility_store();
        for (agent_id, name) in [
            ("a_relaunched", "relaunched-agent"),
            ("a_following", "following-agent"),
        ] {
            Agents::new(&store)
                .create(NewAgent {
                    agent_id: agent_id.into(),
                    project: "default".into(),
                    name: Some(name.into()),
                    default_harness: Some("codex".into()),
                    role: None,
                    tier: Some("agent".into()),
                    owner: None,
                })
                .await
                .expect("agent identity");
        }
        for (runtime_id, agent_id, client_key) in [
            ("s_following", "a_following", "following-key"),
            ("s_relaunched_old", "a_relaunched", "old-key"),
            ("s_relaunched_new", "a_relaunched", "new-key"),
        ] {
            IdentitySessions::new(&store)
                .upsert(NewIdentitySession {
                    runtime_id: runtime_id.into(),
                    agent_id: agent_id.into(),
                    project: "default".into(),
                    harness: "codex".into(),
                    mode: "headless".into(),
                    backend: Some("acp".into()),
                    cwd: Some(format!("/work/{runtime_id}")),
                    native_resume_key: Some(format!("native-{runtime_id}")),
                    client_key: Some(client_key.into()),
                })
                .await
                .expect("resurrection capsule");
            AgentRuntimes::new(&store)
                .create(NewAgentRuntime {
                    runtime_id: runtime_id.into(),
                    agent_id: agent_id.into(),
                    harness: "codex".into(),
                    cwd: Some(format!("/work/{runtime_id}")),
                    transport: Some("acp".into()),
                    presence: Some("online".into()),
                    active: true,
                })
                .await
                .expect("durable runtime descriptor");
        }
        for (runtime_id, updated_at) in [
            ("s_following", 10_i64),
            ("s_relaunched_old", 20_i64),
            ("s_relaunched_new", 30_i64),
        ] {
            store
                .identity_conn()
                .execute(
                    "UPDATE identity_sessions SET updated_at = ?2 WHERE runtime_id = ?1",
                    libsql::params![runtime_id, updated_at],
                )
                .await
                .expect("ordered resurrection capsule");
        }
    }

    let daemon = DaemonStore::open(path.to_string_lossy().as_ref())
        .await
        .expect("second boot");
    let store = Arc::new(daemon.compatibility_store());
    let publisher = GatewayStreamPublisher::new(32);
    let mut gateway_frames = publisher.subscribe();
    let state =
        AppState::wire_pty_with_gateway_stream(store.clone(), &Config::default(), Some(publisher));
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        state.wait_for_runtime_identity_ready(),
    )
    .await
    .expect("boot identity readiness")
    .expect("successful model and identity boot");

    let sessions = Sessions::new(&store);
    let latest = sessions
        .find_by_session_id(&SessionId("s_relaunched_new".into()))
        .await
        .expect("latest runtime lookup")
        .expect("latest runtime must be projected");
    assert_eq!(latest.agent_id.as_deref(), Some("a_relaunched"));
    assert_eq!(latest.client_key.as_deref(), Some("new-key"));
    assert!(
        sessions
            .find_by_session_id(&SessionId("s_relaunched_old".into()))
            .await
            .expect("old runtime lookup")
            .is_none(),
        "an older capsule for the same stable agent must remain history, not a live projection"
    );
    assert!(
        sessions
            .find_by_session_id(&SessionId("s_following".into()))
            .await
            .expect("following runtime lookup")
            .is_some(),
        "one stale capsule must not abort restoration for later agents"
    );

    let mut identities = HashMap::new();
    let mut runtimes = HashMap::new();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
    while (identities.len() < 2 || runtimes.len() < 2) && tokio::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let Ok(Ok(frame)) = tokio::time::timeout(remaining, gateway_frames.recv()).await else {
            break;
        };
        let GatewayStreamFrame::Projection { event } = frame else {
            continue;
        };
        match event.kind {
            GatewayProjectionKind::IdentityUpserted => {
                if let Some(agent_id) = event.payload["agentId"].as_str() {
                    identities.insert(agent_id.to_string(), event.payload);
                }
            }
            GatewayProjectionKind::RuntimeUpserted => {
                if let Some(runtime_id) = event.payload["runtimeId"].as_str() {
                    runtimes.insert(runtime_id.to_string(), event.payload);
                }
            }
            _ => {}
        }
    }
    assert_eq!(
        identities
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>(),
        ["a_following".to_string(), "a_relaunched".to_string()]
            .into_iter()
            .collect()
    );
    assert_eq!(
        runtimes
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>(),
        ["s_following".to_string(), "s_relaunched_new".to_string()]
            .into_iter()
            .collect()
    );
    let identity = &identities["a_relaunched"];
    assert_eq!(identity["name"], "relaunched-agent");
    assert_eq!(identity["project"], "default");
    assert_eq!(identity["defaultHarness"], "codex");
    assert_eq!(identity["tier"], "agent");
    let runtime = &runtimes["s_relaunched_new"];
    assert_eq!(runtime["agentId"], "a_relaunched");
    assert_eq!(runtime["harness"], "codex");
    assert_eq!(runtime["cwd"], "/work/s_relaunched_new");
    assert_eq!(runtime["transport"], "acp");
    assert_eq!(runtime["presence"], "online");
    assert_eq!(runtime["active"], true);
    assert_eq!(runtime["nativeResumeKey"], "native-s_relaunched_new");

    drop(state);
    drop(store);
    drop(daemon);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}

#[tokio::test]
async fn concurrent_acp_revives_open_one_adapter_for_the_runtime() {
    let path = unique_store_path("concurrent-acp-revive");
    {
        let daemon = DaemonStore::open(path.to_string_lossy().as_ref())
            .await
            .expect("seed store");
        let store = daemon.compatibility_store();
        Agents::new(&store)
            .create(NewAgent {
                agent_id: "a_concurrent_hermes".into(),
                project: "default".into(),
                name: Some("concurrent-hermes".into()),
                default_harness: Some("hermes".into()),
                role: None,
                tier: Some("agent".into()),
                owner: None,
            })
            .await
            .expect("agent identity");
        IdentitySessions::new(&store)
            .upsert(NewIdentitySession {
                runtime_id: "s_concurrent_hermes".into(),
                agent_id: "a_concurrent_hermes".into(),
                project: "default".into(),
                harness: "hermes".into(),
                mode: "headless".into(),
                backend: Some("acp".into()),
                cwd: Some("/work/concurrent-hermes".into()),
                native_resume_key: Some("hermes-native-session".into()),
                client_key: Some("concurrent-hermes-key".into()),
            })
            .await
            .expect("resurrection capsule");
    }

    let daemon = DaemonStore::open(path.to_string_lossy().as_ref())
        .await
        .expect("runtime store");
    let store = Arc::new(daemon.compatibility_store());
    let adapter = BlockingResumeAdapter::default();
    let observed = adapter.clone();
    let mut registry = AdapterRegistry::new();
    registry.register(
        &hid("hermes"),
        Arc::new(move |_ctx| Arc::new(adapter.clone()) as Arc<dyn Adapter>),
    );
    let state = AppState::wire_with_registry(store.clone(), &Config::default(), registry);
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        state.wait_for_runtime_identity_ready(),
    )
    .await
    .expect("runtime identity readiness")
    .expect("successful model and identity boot");

    let first_state = state.clone();
    let first =
        tokio::spawn(async move { first_state.ensure_alive_agent("a_concurrent_hermes").await });
    let second_state = state.clone();
    let second =
        tokio::spawn(async move { second_state.ensure_alive_agent("a_concurrent_hermes").await });

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
    while observed.resume_attempts.load(Ordering::SeqCst) == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "one revive must enter the adapter"
        );
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(
        observed.resume_attempts.load(Ordering::SeqCst),
        1,
        "overlapping wake paths must share one ACP resume attempt"
    );

    observed.release.notify_waiters();
    first.await.expect("first join").expect("first revive");
    second.await.expect("second join").expect("second revive");
    assert_eq!(observed.resume_attempts.load(Ordering::SeqCst), 1);

    state.teardown_owned_transports_for_shutdown().await;
    drop(state);
    drop(store);
    drop(daemon);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}

#[tokio::test]
async fn claude_revive_uses_the_capsule_key_as_an_opaque_best_effort_hint() {
    let path = unique_store_path("claude-opaque-resume");
    let daemon = DaemonStore::open(path.to_string_lossy().as_ref())
        .await
        .expect("split store");
    let store = Arc::new(daemon.compatibility_store());
    Agents::new(&store)
        .create(NewAgent {
            agent_id: "a_claude_opaque".into(),
            project: "default".into(),
            name: Some("claude-opaque".into()),
            default_harness: Some("claude".into()),
            role: None,
            tier: Some("agent".into()),
            owner: None,
        })
        .await
        .expect("agent identity");
    let runtime_id = SessionId("s_claude_opaque".into());
    Sessions::new(&store)
        .create(NewSession {
            session_id: runtime_id.clone(),
            name: Some("claude-opaque".into()),
            agent: Some("claude".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: Some("claude-native-opaque".into()),
            client_key: Some("boot-scoped-key".into()),
            cwd: Some("/work/claude".into()),
            project: "default".into(),
            transport: Some("pty".into()),
        })
        .await
        .expect("runtime row");
    Sessions::new(&store)
        .set_agent_id(&runtime_id, "a_claude_opaque")
        .await
        .expect("stable agent binding");
    IdentitySessions::new(&store)
        .upsert(NewIdentitySession {
            runtime_id: runtime_id.0.clone(),
            agent_id: "a_claude_opaque".into(),
            project: "default".into(),
            harness: "claude".into(),
            mode: "headed".into(),
            backend: Some("pty".into()),
            cwd: Some("/work/claude".into()),
            native_resume_key: Some("claude-native-opaque".into()),
            client_key: Some("boot-scoped-key".into()),
        })
        .await
        .expect("resurrection capsule");
    let row = Sessions::new(&store)
        .find_by_session_id(&runtime_id)
        .await
        .expect("runtime lookup")
        .expect("runtime row");
    let state = AppState::wire_pty(store.clone(), &Config::default());

    let tail = state
        .headed_revive_tail_for_row(&row, &hid("claude"))
        .await
        .expect("Claude resume should consume the opaque capsule without native identity checks");
    assert_eq!(tail, ["--resume", "claude-native-opaque"]);

    state.teardown_owned_transports_for_shutdown().await;
    drop(state);
    drop(store);
    drop(daemon);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}

#[tokio::test]
async fn daemon_boot_replays_one_unsettled_delivery_and_auto_wakes_its_target() {
    let path = unique_store_path("boot-delivery");
    let marker = "RESTART-CONTINUITY-MARKER";
    {
        let daemon = DaemonStore::open(path.to_string_lossy().as_ref())
            .await
            .expect("first boot");
        let store = daemon.compatibility_store();
        Agents::new(&store)
            .create(NewAgent {
                agent_id: "a_restart_target".into(),
                project: "default".into(),
                name: Some("restart-target".into()),
                default_harness: Some("codex".into()),
                role: None,
                tier: Some("agent".into()),
                owner: None,
            })
            .await
            .expect("agent identity");
        IdentitySessions::new(&store)
            .upsert(NewIdentitySession {
                runtime_id: "s_restart_target".into(),
                agent_id: "a_restart_target".into(),
                project: "default".into(),
                harness: "codex".into(),
                mode: "headless".into(),
                backend: Some("acp".into()),
                cwd: Some("/work/restart".into()),
                native_resume_key: Some("codex-restart-native".into()),
                client_key: Some("restart-target-key".into()),
            })
            .await
            .expect("resurrection capsule");
        let message = Message {
            id: MessageId("m_restart_delivery".into()),
            project: ProjectId("default".into()),
            from: "operator".into(),
            scope: Scope::Dm,
            thread: None,
            topic: None,
            body: marker.into(),
            summary: None,
            provenance: Provenance {
                from: "operator".into(),
                kind: Kind::Human,
                locality: Default::default(),
                access: None,
                thread: None,
                topic: None,
                stamp: None,
            },
            created_at: 10,
        };
        DeliveryObligations::new(&store)
            .insert(NewDeliveryObligation {
                message_id: message.id.0.clone(),
                recipient_agent_id: "a_restart_target".into(),
                recipient_runtime_id: Some("s_restart_target".into()),
                payload_json: serde_json::to_string(&message).expect("message json"),
                dedupe_key: "delivery:m_restart_delivery:a_restart_target".into(),
                attempt: 0,
                state: "pending".into(),
                created_at: message.created_at,
            })
            .await
            .expect("delivery continuity");
    }

    let daemon = DaemonStore::open(path.to_string_lossy().as_ref())
        .await
        .expect("second boot");
    let store = Arc::new(daemon.compatibility_store());
    let mock = MockAdapter::new();
    let observed = mock.clone();
    let mut registry = AdapterRegistry::new();
    registry.register(
        &hid("codex"),
        Arc::new(move |_cwd| Arc::new(mock.clone()) as Arc<dyn Adapter>),
    );
    let state = AppState::wire_with_registry(store.clone(), &Config::default(), registry);

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        if observed
            .injected_prompts()
            .iter()
            .any(|prompt| prompt.contains(marker))
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "boot must materialize the pending capsule and auto-wake its target; prompts={:?}",
            observed.injected_prompts()
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(
        observed
            .injected_prompts()
            .iter()
            .filter(|prompt| prompt.contains(marker))
            .count(),
        1,
        "restart continuity must inject once"
    );

    drop(state);
    drop(store);
    drop(daemon);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}

#[tokio::test]
async fn daemon_boot_restores_minimal_thread_routing_and_wakes_the_same_agent_identity() {
    let path = unique_store_path("boot-thread-routing");
    let thread_id = ThreadId("t_restart_routing".into());
    for (agent_id, name) in [
        ("a_restart_sender", "restart-sender"),
        ("a_restart_member", "restart-member"),
    ] {
        let daemon = DaemonStore::open(path.to_string_lossy().as_ref())
            .await
            .expect("seed store");
        let store = daemon.compatibility_store();
        if Agents::new(&store)
            .find_by_id(agent_id)
            .await
            .expect("agent lookup")
            .is_none()
        {
            Agents::new(&store)
                .create(NewAgent {
                    agent_id: agent_id.into(),
                    project: "default".into(),
                    name: Some(name.into()),
                    default_harness: Some("codex".into()),
                    role: None,
                    tier: Some("agent".into()),
                    owner: None,
                })
                .await
                .expect("agent identity");
            IdentitySessions::new(&store)
                .upsert(NewIdentitySession {
                    runtime_id: format!("s_{name}"),
                    agent_id: agent_id.into(),
                    project: "default".into(),
                    harness: "codex".into(),
                    mode: "headless".into(),
                    backend: Some("acp".into()),
                    cwd: Some("/work/restart-routing".into()),
                    native_resume_key: Some(format!("native-{name}")),
                    client_key: Some(format!("key-{name}")),
                })
                .await
                .expect("runtime capsule");
        }
        drop(store);
        drop(daemon);
    }
    {
        let daemon = DaemonStore::open(path.to_string_lossy().as_ref())
            .await
            .expect("first boot");
        let store = daemon.compatibility_store();
        Threads::new(&store)
            .create(&thread_id, "restart-room", "default", "restart-sender")
            .await
            .expect("thread route");
        Threads::new(&store)
            .add_member(&thread_id, "restart-sender")
            .await
            .expect("creator membership");
        Threads::new(&store)
            .add_member(&thread_id, "restart-member")
            .await
            .expect("target membership");
    }

    let daemon = DaemonStore::open(path.to_string_lossy().as_ref())
        .await
        .expect("second boot");
    let store = Arc::new(daemon.compatibility_store());
    let mock = MockAdapter::new();
    let observed = mock.clone();
    let mut registry = AdapterRegistry::new();
    registry.register(
        &hid("codex"),
        Arc::new(move |_cwd| Arc::new(mock.clone()) as Arc<dyn Adapter>),
    );
    let state = AppState::wire_with_registry(store.clone(), &Config::default(), registry);

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        if Threads::new(&store)
            .find_any_by_name("restart-room")
            .await
            .expect("thread lookup")
            .is_some()
            && Sessions::new(&store)
                .find_by_agent_id("a_restart_sender")
                .await
                .expect("sender lookup")
                .is_some()
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "boot must restore the minimal thread route before interaction"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let caller = state
        .identity
        .resolve("default", "restart-sender")
        .await
        .expect("sender caller");
    let response = nexus::daemon::routing::route_request(
        &state,
        Some(caller),
        Request {
            jsonrpc: "2.0".into(),
            id: None,
            method: "send".into(),
            params: Some(
                serde_json::to_value(SendRequest {
                    to: SendTarget::Post {
                        thread: "restart-room".into(),
                    },
                    summary: None,
                    body: "THREAD-RESTART-WAKE".into(),
                    mention: Vec::new(),
                    metadata: None,
                    idempotency_key: Some("thread-restart-wake".into()),
                })
                .expect("send request json"),
            ),
        },
    )
    .await;
    assert!(
        response.error.is_none(),
        "thread send after restart failed: {:?}",
        response.error
    );
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        if observed
            .injected_prompts()
            .iter()
            .any(|prompt| prompt.contains("THREAD-RESTART-WAKE"))
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "restored thread routing must auto-wake the stable member identity"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    drop(state);
    drop(store);
    drop(daemon);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}

#[tokio::test]
async fn split_daemon_exposes_no_arbitrary_product_history_query_surface() {
    let path = unique_store_path("no-history-read");
    let daemon = DaemonStore::open(path.to_string_lossy().as_ref())
        .await
        .expect("split store");
    let state = AppState::wire(Arc::new(daemon.compatibility_store()), &Config::default());
    let response = daemon_ipc::handle_request(
        &state,
        "boot-token",
        DaemonIpcRequest {
            version: DAEMON_IPC_PROTOCOL_VERSION,
            token: "boot-token".into(),
            request_id: "no-history-read".into(),
            caller: Some(DaemonIpcCaller {
                name: Some("Local Operator".into()),
                project: "default".into(),
                session_id: Some("local-operator".into()),
                agent_id: None,
                runtime_id: Some("local-operator".into()),
                client_key: None,
                kind: Kind::Human,
                locality: Default::default(),
                access: None,
                principal_id: None,
                tier: Tier::Admin,
            }),
            call: DaemonIpcCall::Query {
                method: "local.store.read".into(),
                params: serde_json::json!({
                    "sql": "SELECT * FROM messages",
                    "args": []
                }),
            },
        },
    )
    .await;
    let error = response.error.expect("history read must be rejected");
    assert_eq!(error.code, nexus_contracts::codes::METHOD_NOT_FOUND);
    assert!(error.message.contains("Gateway REST"));

    let response = daemon_ipc::handle_request(
        &state,
        "boot-token",
        DaemonIpcRequest {
            version: DAEMON_IPC_PROTOCOL_VERSION,
            token: "boot-token".into(),
            request_id: "no-history-rpc".into(),
            caller: Some(DaemonIpcCaller {
                name: Some("Local Operator".into()),
                project: "default".into(),
                session_id: Some("local-operator".into()),
                agent_id: None,
                runtime_id: Some("local-operator".into()),
                client_key: None,
                kind: Kind::Human,
                locality: Default::default(),
                access: None,
                principal_id: None,
                tier: Tier::Admin,
            }),
            call: DaemonIpcCall::Query {
                method: "history".into(),
                params: serde_json::json!({
                    "with": "target",
                    "limit": 10
                }),
            },
        },
    )
    .await;
    let error = response.error.expect("daemon history RPC must be rejected");
    assert_eq!(error.code, nexus_contracts::codes::METHOD_NOT_FOUND);
    assert!(error.message.contains("Gateway-owned"));

    drop(state);
    drop(daemon);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}

fn agent(name: &str, client_key: &str) -> RegisterRequest {
    RegisterRequest {
        agent_id: None,
        name: Some(name.into()),
        harness: hid("other"),
        harness_session_id: format!("native-{client_key}"),
        project: "default".into(),
        client_key: client_key.into(),
        runtime_credential: None,
        tier: Tier::Agent,
        kind: Some(Kind::Agent),
        locality: Default::default(),
        access: None,
        role: None,
        cwd: Some("/work/repo".into()),
    }
}

#[tokio::test]
async fn gateway_absent_delivery_uses_memory_transport_and_only_unsettled_continuity_persists() {
    let path = unique_store_path("delivery");
    let daemon = DaemonStore::open(path.to_string_lossy().as_ref())
        .await
        .expect("split store");
    let store = Arc::new(daemon.compatibility_store());
    let state = AppState::wire(store.clone(), &Config::default());
    state
        .wait_for_runtime_identity_ready()
        .await
        .expect("runtime identity ready");

    state
        .identity
        .register(agent("sender", "sender-client"))
        .await
        .expect("sender register");
    let target = state
        .identity
        .register(agent("target", "target-client"))
        .await
        .expect("target register");
    let caller = state
        .identity
        .resolve("default", "sender")
        .await
        .expect("sender caller");

    let ack = state
        .bus
        .send(
            &caller,
            SendRequest {
                to: SendTarget::dm_name("target"),
                summary: None,
                body: "transport without Gateway".into(),
                mention: Vec::new(),
                metadata: None,
                idempotency_key: Some("lightweight-send-1".into()),
            },
        )
        .await
        .expect("Gateway absence does not block transport");
    assert_eq!(ack.fanout, Some(1));
    assert_eq!(
        DeliveryObligations::new(&store)
            .pending()
            .await
            .expect("pending continuity")
            .len(),
        1,
        "accepted but unsettled delivery must survive a daemon restart"
    );

    let batch = state
        .realtime
        .consume(
            &state
                .identity
                .resolve("default", "target")
                .await
                .expect("target caller"),
            ConsumeRequest {
                timeout_ms: Some(0),
                max: None,
            },
        )
        .await
        .expect("consume");
    assert_eq!(batch.dms[0].body, "transport without Gateway");
    let inbox = Inbox::new(&store);
    inbox
        .mark_notified(&target.session_id)
        .await
        .expect("notify");
    assert_eq!(
        inbox
            .mark_injecting(&ack.message_id, &target.session_id)
            .await
            .expect("injecting"),
        1
    );
    assert_eq!(
        inbox
            .mark_delivered(&ack.message_id, &target.session_id)
            .await
            .expect("delivered"),
        1
    );
    assert!(DeliveryObligations::new(&store)
        .pending()
        .await
        .expect("settled continuity")
        .is_empty());

    drop(state);
    drop(store);
    drop(daemon);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}

#[tokio::test]
async fn ttl_terminal_delivery_is_not_restored_from_split_store_continuity() {
    ttl_delivery_reopen_case(false).await;
}

#[tokio::test]
async fn ttl_explicit_requeue_survives_split_store_reopen() {
    ttl_delivery_reopen_case(true).await;
}

async fn ttl_delivery_reopen_case(explicit_requeue: bool) {
    let (dir, store) = boot_model_fixture().await;
    let session = SessionId("s_boot_model".into());
    // No registered adapters: this exercises real daemon/store recovery without
    // launching a provider, native process, or external service.
    let state =
        AppState::wire_with_registry(store.clone(), &Config::default(), AdapterRegistry::new());
    state.wait_for_runtime_identity_ready().await.unwrap();
    state
        .identity
        .register(agent("ttl-sender", "ttl-sender-key"))
        .await
        .unwrap();
    let caller = state
        .identity
        .resolve("default", "ttl-sender")
        .await
        .unwrap();
    let send = |body: &str| SendRequest {
        to: SendTarget::dm_name("boot-model"),
        summary: None,
        body: body.into(),
        mention: Vec::new(),
        metadata: None,
        idempotency_key: None,
    };
    let expired = state
        .bus
        .send(&caller, send("expire before restart"))
        .await
        .unwrap();
    assert_eq!(expired.fanout, Some(1));
    assert_eq!(
        DeliveryObligations::new(&store)
            .pending()
            .await
            .unwrap()
            .len(),
        1
    );
    let inbox = Inbox::new(&store);
    inbox.mark_notified(&session).await.unwrap();
    let ttl = nexus_store::repos::inbox::DELIVERY_TIMEOUT_TTL_MS;
    let mutation = inbox
        .dead_letter_expired_deliveries(nexus_common::now() + ttl, ttl)
        .await
        .unwrap();
    assert_eq!(
        mutation.count, 1,
        "the real sweep must terminalize accepted mail"
    );
    let mut rows = store
        .conn
        .query(
            "SELECT state, attempt_count, error_code FROM in_flight WHERE message_id = ?1",
            libsql::params![expired.message_id.0.clone()],
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<String>(0).unwrap(), "error");
    assert_eq!(row.get::<i64>(1).unwrap(), 0);
    assert_eq!(row.get::<String>(2).unwrap(), "delivery_timeout");
    drop(row);
    drop(rows);
    let outstanding_after_expiry = DeliveryObligations::new(&store)
        .pending()
        .await
        .unwrap()
        .len();
    assert_eq!(outstanding_after_expiry, 0);
    if explicit_requeue {
        // Completed attempts are historical audit data, not a new attempt.
        store
            .conn
            .execute(
                "UPDATE in_flight SET attempt_count = 3 WHERE message_id = ?1",
                libsql::params![expired.message_id.0.clone()],
            )
            .await
            .unwrap();
        let requeued = inbox
            .requeue_dead_letters(nexus_store::repos::inbox::DeadLetterSelector::InFlightId(
                mutation.in_flight_ids[0].clone(),
            ))
            .await
            .unwrap();
        assert_eq!(requeued.count, 1);
        let obligations = DeliveryObligations::new(&store).pending().await.unwrap();
        assert_eq!(obligations.len(), 1);
        assert_eq!(
            obligations[0].attempt, 3,
            "requeue must retain completed attempt history"
        );
        assert_eq!(obligations[0].message_id, expired.message_id.0);
        assert_eq!(
            obligations[0].recipient_runtime_id.as_deref(),
            Some("s_boot_model")
        );
    }

    // A separately accepted, unswept obligation is the positive recovery control.
    let pending = state
        .bus
        .send(&caller, send("still unsettled at restart"))
        .await
        .unwrap();
    drop(state);
    drop(store);

    let reopened = DaemonStore::open(dir.path().join("identity.db").to_str().unwrap())
        .await
        .unwrap();
    let store = Arc::new(reopened.compatibility_store());
    let state =
        AppState::wire_with_registry(store.clone(), &Config::default(), AdapterRegistry::new());
    state.wait_for_runtime_identity_ready().await.unwrap();
    // Ingress readiness precedes backlog recovery; explicitly await the same
    // idempotent production recovery method rather than racing its boot task.
    state
        .restore_unsettled_delivery_obligations_once()
        .await
        .unwrap();
    let mut rows = store
        .conn
        .query(
            "SELECT message_id, state, attempt_count FROM in_flight WHERE recipient_session = ?1",
            libsql::params![session.0],
        )
        .await
        .unwrap();
    let mut recovered = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        recovered.push((
            row.get::<String>(0).unwrap(),
            row.get::<String>(1).unwrap(),
            row.get::<i64>(2).unwrap(),
        ));
    }
    assert!(
        recovered
            .iter()
            .any(|(id, _, _)| id == &pending.message_id.0),
        "control must prove that actual continuity recovery ran: {recovered:?}"
    );
    assert_eq!(
        recovered
            .iter()
            .any(|(id, _, _)| id == &expired.message_id.0),
        explicit_requeue,
        "only an explicit requeue may restore TTL-terminal mail; recovered={recovered:?}"
    );
}

#[tokio::test]
async fn ten_thousand_session_frames_remain_boot_scoped_and_create_no_transcript_history() {
    let path = unique_store_path("frames");
    let session = SessionId("s_frames".into());
    {
        let daemon = DaemonStore::open(path.to_string_lossy().as_ref())
            .await
            .expect("split store");
        let store = daemon.compatibility_store();
        let stream = StreamEvents::new(&store);
        for index in 0..10_000 {
            stream
                .append(&session, "text", &format!(r#"{{"delta":{index}}}"#))
                .await
                .expect("boot-scoped frame");
        }
        assert_eq!(
            stream.since(&session, 0).await.expect("stream page").len(),
            10_000
        );
        for authority in [daemon.identity(), daemon.transport()] {
            let mut rows = authority
                .conn
                .query(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' \
                     AND name IN ('agent_session_messages','agent_session_turns','transcript_archive')",
                    (),
                )
                .await
                .expect("schema query");
            let count: i64 = rows
                .next()
                .await
                .expect("schema row")
                .expect("count row")
                .get(0)
                .expect("count");
            assert_eq!(count, 0, "daemon must own no transcript history tables");
        }
    }

    let identity_bytes = std::fs::metadata(&path).expect("identity file").len();
    assert!(
        identity_bytes < 2 * 1024 * 1024,
        "ephemeral frame burst leaked into identity file: {identity_bytes} bytes"
    );
    let reopened = DaemonStore::open(path.to_string_lossy().as_ref())
        .await
        .expect("restart");
    let store = reopened.compatibility_store();
    assert!(StreamEvents::new(&store)
        .since(&session, 0)
        .await
        .expect("fresh stream")
        .is_empty());

    drop(store);
    drop(reopened);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}

fn isolated_ttl_boot<T>(future: impl std::future::Future<Output = T>) -> T {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let result =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| runtime.block_on(future)));
    runtime.shutdown_timeout(std::time::Duration::from_secs(1));
    result.unwrap_or_else(|error| std::panic::resume_unwind(error))
}

#[test]
fn external_recipient_without_runtime_capsule_expires_after_restart() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("identity.db");
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let (message_id, agent_id, runtime_id, created_at) = isolated_ttl_boot(async {
            let daemon = DaemonStore::open(path.to_str().unwrap()).await.unwrap();
            let store = Arc::new(daemon.compatibility_store());
            let state = AppState::wire_with_registry(
                store.clone(),
                &Config::default(),
                AdapterRegistry::new(),
            );
            state.wait_for_runtime_identity_ready().await.unwrap();
            state
                .identity
                .register(agent("external-ttl-sender", "external-ttl-sender-key"))
                .await
                .unwrap();
            let target = state
                .identity
                .register(agent("external-ttl-ghost", "external-ttl-ghost-key"))
                .await
                .unwrap();
            let caller = state
                .identity
                .resolve("default", "external-ttl-sender")
                .await
                .unwrap();
            let ack = state
                .bus
                .send(
                    &caller,
                    SendRequest {
                        to: SendTarget::dm_name("external-ttl-ghost"),
                        summary: None,
                        body: "accepted before restart without runtime capsule".into(),
                        mention: vec![],
                        metadata: None,
                        idempotency_key: None,
                    },
                )
                .await
                .unwrap();
            assert_eq!(ack.fanout, Some(1));
            assert!(IdentitySessions::new(&store)
                .list()
                .await
                .unwrap()
                .is_empty());
            let obligations = DeliveryObligations::new(&store).pending().await.unwrap();
            assert_eq!(obligations.len(), 1);
            assert_eq!(obligations[0].attempt, 0);
            let message = nexus_store::repos::Messages::new(&store)
                .get("default", &ack.message_id)
                .await
                .unwrap()
                .unwrap();
            (
                ack.message_id,
                obligations[0].recipient_agent_id.clone(),
                target.session_id,
                message.created_at,
            )
        });
        isolated_ttl_boot(async {
            let daemon = DaemonStore::open(path.to_str().unwrap()).await.unwrap();
            let store = Arc::new(daemon.compatibility_store());
            use nexus::daemon::gateway_stream_socket::{
                GatewayStreamFrame, GatewayStreamPublisher,
            };
            let publisher = GatewayStreamPublisher::new(128);
            let mut projected = publisher.subscribe();
            let state = AppState::wire_with_registry_and_gateway_stream(
                store.clone(),
                &Config::default(),
                AdapterRegistry::new(),
                Some(publisher),
            );
            state.wait_for_runtime_identity_ready().await.unwrap();
            assert!(Sessions::new(&store)
                .find_by_session_id(&runtime_id)
                .await
                .unwrap()
                .is_none());
            // Readiness precedes the boot task's recovery. Observe that real task's completed
            // edge before exercising sequential idempotence, not a second concurrent restore.
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                loop {
                    let mut rows = store
                        .conn
                        .query(
                            "SELECT COUNT(*) FROM in_flight WHERE message_id=?1",
                            libsql::params![message_id.0.clone()],
                        )
                        .await
                        .unwrap();
                    if rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap() == 1 {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("boot must reconstruct a durable external recipient edge");
            state
                .restore_unsettled_delivery_obligations_once()
                .await
                .unwrap();
            let mutation = nexus::daemon::retention_policy::sweep_delivery_timeouts(
                &state,
                created_at + 1_001,
                1_000,
            )
            .await
            .unwrap();
            let pending = DeliveryObligations::new(&store).pending().await.unwrap();
            eprintln!(
                "external TTL recovery: mutation_count={} durable_pending={} runtime_present=false",
                mutation.count,
                pending.len()
            );
            assert_eq!(
                mutation.count, 1,
                "accepted external obligation must expire without re-registering its recipient"
            );
            assert!(pending.is_empty());
            let mut settlements = Vec::new();
            while let Ok(frame) = projected.try_recv() {
                if let GatewayStreamFrame::Projection { event } = frame {
                    if event.kind == nexus_contracts::GatewayProjectionKind::DeliverySettled {
                        settlements.push(event);
                    }
                }
            }
            assert_eq!(
                settlements.len(),
                1,
                "expiry must publish its committed terminal fact"
            );
            assert_eq!(settlements[0].payload["messageId"], message_id.0);
            assert_eq!(settlements[0].payload["recipientAgentId"], agent_id);
            assert_eq!(settlements[0].payload["state"], "error");
            assert_eq!(settlements[0].payload["attempt"], 0);
            assert_eq!(settlements[0].payload["errorCode"], "delivery_timeout");
            let mut rows = store.conn.query(
                "SELECT recipient_agent_id,state,attempt_count,error_code,delivery_timing FROM in_flight WHERE message_id=?1",
                libsql::params![message_id.0.clone()],
            ).await.unwrap();
            let row = rows.next().await.unwrap().unwrap();
            assert_eq!(row.get::<String>(0).unwrap(), agent_id);
            assert_eq!(row.get::<String>(1).unwrap(), "error");
            assert_eq!(row.get::<i64>(2).unwrap(), 0);
            assert_eq!(row.get::<String>(3).unwrap(), "delivery_timeout");
            assert_eq!(row.get::<String>(4).unwrap(), "interrupt");
            assert!(rows.next().await.unwrap().is_none());
            assert!(
                Sessions::new(&store)
                    .find_by_session_id(&runtime_id)
                    .await
                    .unwrap()
                    .is_none(),
                "reconstruction must not invent runtime presence"
            );
        });
        isolated_ttl_boot(async {
            let daemon = DaemonStore::open(path.to_str().unwrap()).await.unwrap();
            let store = Arc::new(daemon.compatibility_store());
            let state = AppState::wire_with_registry(
                store.clone(),
                &Config::default(),
                AdapterRegistry::new(),
            );
            state.wait_for_runtime_identity_ready().await.unwrap();
            assert_eq!(
                state
                    .restore_unsettled_delivery_obligations_once()
                    .await
                    .unwrap(),
                0
            );
            assert!(DeliveryObligations::new(&store)
                .pending()
                .await
                .unwrap()
                .is_empty());
            assert!(nexus_store::repos::Messages::new(&store)
                .get("default", &message_id)
                .await
                .unwrap()
                .is_none());
        });
    }));
    let cleanup = directory.close();
    assert!(cleanup.is_ok(), "disposable TTL store cleanup: {cleanup:?}");
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}

#[test]
fn external_ttl_recovery_preserves_owner_timing_and_rejects_invalid_capsules() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("identity.db");
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        isolated_ttl_boot(async {
            let daemon = DaemonStore::open(path.to_str().unwrap()).await.unwrap();
            let store = Arc::new(daemon.compatibility_store());
            Agents::new(&store)
                .create(NewAgent {
                    agent_id: "a_stable_ttl".into(),
                    project: "default".into(),
                    name: Some("stable-ttl".into()),
                    default_harness: Some("other".into()),
                    role: None,
                    tier: None,
                    owner: None,
                })
                .await
                .unwrap();
            let legacy = SessionId("s_legacy_ttl".into());
            Sessions::new(&store)
                .create(NewSession {
                    session_id: legacy.clone(),
                    name: Some("legacy-ttl".into()),
                    agent: Some("other".into()),
                    kind: "agent".into(),
                    role: None,
                    tier: "agent".into(),
                    harness_session_id: None,
                    client_key: None,
                    cwd: None,
                    project: "default".into(),
                    transport: None,
                })
                .await
                .unwrap();
            // The last sorted valid edge is a rendezvous for the real boot recovery loop.
            let cases = [
                ("m_bad_json", "a_stable_ttl", None, "bad", false),
                ("m_bad_id", "a_stable_ttl", None, "mismatch", false),
                (
                    "m_unknown_owner",
                    "a_missing",
                    Some("s_legacy_ttl"),
                    "interrupt",
                    false,
                ),
                (
                    "m_legacy_absent",
                    "s_absent",
                    Some("s_absent"),
                    "interrupt",
                    false,
                ),
                (
                    "m_legacy_valid",
                    "s_legacy_ttl",
                    Some("s_legacy_ttl"),
                    "interrupt",
                    true,
                ),
                (
                    "m_stable_no_runtime",
                    "a_stable_ttl",
                    None,
                    "yield_turn",
                    true,
                ),
                (
                    "m_zz_stable_absent",
                    "a_stable_ttl",
                    Some("s_absent"),
                    "after_tool_loop",
                    true,
                ),
            ];
            for (id, owner, runtime, timing, _) in cases {
                let message = Message {
                    id: MessageId(
                        if timing == "mismatch" {
                            "m_not_the_capsule"
                        } else {
                            id
                        }
                        .into(),
                    ),
                    project: ProjectId("default".into()),
                    from: "operator".into(),
                    scope: Scope::Dm,
                    thread: None,
                    topic: None,
                    body: "retained delivery".into(),
                    summary: None,
                    provenance: Provenance {
                        from: "operator".into(),
                        kind: Kind::Human,
                        locality: Default::default(),
                        access: None,
                        thread: None,
                        topic: None,
                        stamp: None,
                    },
                    created_at: 17,
                };
                let payload_json = if timing == "bad" {
                    "{".into()
                } else {
                    serde_json::json!({"message": message, "deliveryTiming":
                        if timing == "mismatch" { "interrupt" } else { timing }})
                    .to_string()
                };
                DeliveryObligations::new(&store)
                    .insert(NewDeliveryObligation {
                        message_id: id.into(),
                        recipient_agent_id: owner.into(),
                        recipient_runtime_id: runtime.map(str::to_owned),
                        payload_json,
                        dedupe_key: format!("delivery:{id}:{owner}"),
                        attempt: 0,
                        state: "pending".into(),
                        created_at: 17,
                    })
                    .await
                    .unwrap();
            }
            let constructions = Arc::new(AtomicUsize::new(0));
            let observed = constructions.clone();
            let mut registry = AdapterRegistry::new();
            registry.register(
                &hid("other"),
                Arc::new(move |_cwd| {
                    observed.fetch_add(1, Ordering::SeqCst);
                    Arc::new(MockAdapter::new()) as Arc<dyn Adapter>
                }),
            );
            let state = AppState::wire_with_registry(store.clone(), &Config::default(), registry);
            state.wait_for_runtime_identity_ready().await.unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                loop {
                    let mut rows = store
                        .conn
                        .query(
                            "SELECT COUNT(*) FROM in_flight WHERE message_id='m_zz_stable_absent'",
                            (),
                        )
                        .await
                        .unwrap();
                    if rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap() == 1 {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("boot recovery must reach the valid last edge");
            state
                .restore_unsettled_delivery_obligations_once()
                .await
                .unwrap();
            state.respawn_pending_agents().await;
            assert_eq!(
                constructions.load(Ordering::SeqCst),
                0,
                "recovery cannot launch a native adapter"
            );
            for (id, owner, _, timing, valid) in cases {
                let message = nexus_store::repos::Messages::new(&store)
                    .get("default", &MessageId(id.into()))
                    .await
                    .unwrap();
                assert_eq!(message.is_some(), valid, "capsule {id}");
                let mut rows = store.conn.query(
                    "SELECT recipient_agent_id,recipient_session,delivery_timing,attempt_count FROM in_flight WHERE message_id=?1",
                    libsql::params![id],
                ).await.unwrap();
                let row = rows.next().await.unwrap();
                assert_eq!(row.is_some(), valid, "recipient edge {id}");
                if let Some(row) = row {
                    assert_eq!(
                        message.unwrap().created_at,
                        17,
                        "retain original TTL origin"
                    );
                    assert_eq!(
                        row.get::<Option<String>>(0).unwrap().as_deref(),
                        if owner == "a_stable_ttl" {
                            Some(owner)
                        } else {
                            None
                        }
                    );
                    assert_eq!(
                        row.get::<Option<String>>(1).unwrap().as_deref(),
                        if owner == "s_legacy_ttl" {
                            Some(owner)
                        } else {
                            None
                        }
                    );
                    assert_eq!(row.get::<String>(2).unwrap(), timing);
                    assert_eq!(row.get::<i64>(3).unwrap(), 0);
                }
            }
            assert!(nexus_store::repos::Messages::new(&store)
                .get("default", &MessageId("m_not_the_capsule".into()))
                .await
                .unwrap()
                .is_none());
            assert!(Sessions::new(&store)
                .find_by_session_id(&SessionId("s_absent".into()))
                .await
                .unwrap()
                .is_none());
            assert!(AgentRuntimes::new(&store)
                .active_for_agent("a_stable_ttl")
                .await
                .unwrap()
                .is_none());
            assert!(IdentitySessions::new(&store)
                .list()
                .await
                .unwrap()
                .is_empty());
            assert_eq!(
                nexus::daemon::retention_policy::sweep_delivery_timeouts(&state, 1_016, 1_000)
                    .await
                    .unwrap()
                    .count,
                0
            );
            assert_eq!(
                nexus::daemon::retention_policy::sweep_delivery_timeouts(&state, 1_017, 1_000)
                    .await
                    .unwrap()
                    .count,
                3
            );
            let pending = DeliveryObligations::new(&store).pending().await.unwrap();
            assert_eq!(
                pending.len(),
                4,
                "unverifiable capsules stay retained, not falsely settled"
            );
            for row in pending {
                assert!(
                    !cases
                        .iter()
                        .find(|case| case.0 == row.message_id)
                        .unwrap()
                        .4
                );
            }
            // Unlike mere runtime absence, explicit identity death retains its terminal path.
            let mut rows = store
                .conn
                .query(
                    "SELECT in_flight_id FROM in_flight WHERE message_id='m_stable_no_runtime'",
                    (),
                )
                .await
                .unwrap();
            let id = rows
                .next()
                .await
                .unwrap()
                .unwrap()
                .get::<String>(0)
                .unwrap();
            drop(rows);
            assert_eq!(
                Inbox::new(&store)
                    .requeue_dead_letters(
                        nexus_store::repos::inbox::DeadLetterSelector::InFlightId(id.clone()),
                    )
                    .await
                    .unwrap()
                    .count,
                1
            );
            assert!(Agents::new(&store)
                .mark_dead("a_stable_ttl", "explicit test death")
                .await
                .unwrap());
            assert_eq!(
                Inbox::new(&store)
                    .dead_letter_undeliverable_for_dead_recipients()
                    .await
                    .unwrap(),
                1
            );
            let effect = Inbox::new(&store)
                .gateway_delivery_effects_for_ids(&[id])
                .await
                .unwrap();
            assert_eq!(effect.len(), 1);
            assert_eq!(effect[0].payload["errorCode"], "target_dead");
            assert_eq!(effect[0].payload["recipientAgentId"], "a_stable_ttl");
        });
    }));
    let cleanup = directory.close();
    assert!(
        cleanup.is_ok(),
        "disposable TTL control cleanup: {cleanup:?}"
    );
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}

/// A validated [`HarnessId`] from a literal (panics on invalid — test-only).
fn hid(s: &str) -> HarnessId {
    HarnessId::new(s).expect("valid harness id literal")
}
