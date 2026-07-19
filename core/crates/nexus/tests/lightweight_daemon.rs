use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use nexus::daemon::{daemon_ipc, AppState};
use nexus_agent::{Adapter, AdapterInjectError, AdapterRegistry, MockAdapter, StreamEvent};
use nexus_common::{Config, NexusError};
use nexus_contracts::{
    ConsumeRequest, DaemonIpcCall, DaemonIpcCaller, DaemonIpcRequest, HarnessId, Kind, Message,
    MessageId, ProjectId, Provenance, RegisterRequest, Request, Scope, SendRequest, SendTarget,
    SessionId, SpawnRequest, ThreadId, Tier, DAEMON_IPC_PROTOCOL_VERSION,
};
use nexus_store::repos::{
    Agents, DeliveryObligations, IdentitySessions, Inbox, NewAgent, NewDeliveryObligation,
    NewIdentitySession, NewSession, Sessions, StreamEvents, Threads,
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
    .expect("boot identity readiness");
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
    .expect("runtime identity readiness");

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

/// A validated [`HarnessId`] from a literal (panics on invalid — test-only).
fn hid(s: &str) -> HarnessId {
    HarnessId::new(s).expect("valid harness id literal")
}
