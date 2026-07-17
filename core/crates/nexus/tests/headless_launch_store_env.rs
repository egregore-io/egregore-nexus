use std::sync::{Arc, Mutex};

use nexus::daemon::AppState;
use nexus_agent::{Adapter, AdapterRegistry, LaunchCtx, MockAdapter};
use nexus_common::Config;
use nexus_contracts::{Harness, SpawnRequest};
use nexus_store::Store;

#[tokio::test]
async fn headless_launch_pins_daemon_ipc_home_without_store_credentials() {
    let stem = format!("nexus-headless-launch-store-env-{}", std::process::id());
    let nexus_home = std::env::temp_dir().join(format!("{stem}-home"));
    let _nexus_home_env = EnvRestore::set("NEXUS_HOME", nexus_home.to_string_lossy().as_ref());
    let _tokio_env = EnvRestore::set("TOKIO_WORKER_THREADS", "2");
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();

    let captured = Arc::new(Mutex::new(None::<LaunchCtx>));
    let captured_for_factory = captured.clone();
    let mut registry = AdapterRegistry::new();
    registry.register(
        Harness::Claude,
        Arc::new(move |ctx| {
            *captured_for_factory.lock().unwrap() = Some(ctx);
            Arc::new(MockAdapter::new()) as Arc<dyn Adapter>
        }),
    );

    let config = Config {
        db_url: Some("http://127.0.0.1:4141".into()),
        db_auth_token: "test-store-token".into(),
        ..Config::default()
    };
    let state = AppState::wire_with_registry(store, &config, registry);

    state
        .launch_agent(
            SpawnRequest {
                kind: Harness::Claude,
                name: Some("store-env-probe".into()),
                identity_policy: None,
                cwd: Some("/tmp/nexus-store-env-probe".into()),
                project: Some("release-test".into()),
                role: None,
                initial_prompt: None,
                resume: None,
                harness_args: Vec::new(),
                headless: true,
                backend: None,
            },
            "release-test",
            None,
        )
        .await
        .unwrap();

    let ctx = captured
        .lock()
        .unwrap()
        .clone()
        .expect("adapter factory must receive launch context");
    let env = ctx
        .env
        .into_iter()
        .collect::<std::collections::HashMap<_, _>>();

    assert_eq!(
        env.get("NEXUS_HOME").map(String::as_str),
        nexus_home.to_str(),
        "the child harness and every MCP process it launches must discover daemon IPC"
    );
    assert_eq!(
        env.get("NEXUS_CLI").map(String::as_str),
        std::env::current_exe()
            .ok()
            .as_deref()
            .and_then(|path| path.to_str()),
        "headless harnesses need a launch-pinned CLI path even when their shell rewrites PATH"
    );
    assert_eq!(
        env.get("TOKIO_WORKER_THREADS").map(String::as_str),
        Some("2")
    );
    assert!(!env.contains_key("NEXUS_DB_URL"));
    assert!(!env.contains_key("NEXUS_DB_AUTH_TOKEN"));
    assert!(!env.contains_key("NEXUS_STREAM_DB_PATH"));
}

#[tokio::test]
async fn headless_codex_launch_pins_the_daemons_resolved_home_in_child_env() {
    let stem = format!("nexus-headless-codex-home-env-{}", std::process::id());
    let codex_home = std::env::temp_dir().join(format!("{stem}-home"));
    let _codex_env = EnvRestore::set("CODEX_HOME", codex_home.to_string_lossy().as_ref());
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();

    let captured = Arc::new(Mutex::new(None::<LaunchCtx>));
    let captured_for_factory = captured.clone();
    let mut registry = AdapterRegistry::new();
    registry.register(
        Harness::Codex,
        Arc::new(move |ctx| {
            *captured_for_factory.lock().unwrap() = Some(ctx);
            Arc::new(MockAdapter::new()) as Arc<dyn Adapter>
        }),
    );

    let state = AppState::wire_with_registry(store, &Config::default(), registry);
    state
        .launch_agent(
            SpawnRequest {
                kind: Harness::Codex,
                name: Some("codex-home-probe".into()),
                identity_policy: None,
                cwd: Some("/tmp/nexus-codex-home-probe".into()),
                project: Some("release-test".into()),
                role: None,
                initial_prompt: None,
                resume: None,
                harness_args: Vec::new(),
                headless: true,
                backend: None,
            },
            "release-test",
            None,
        )
        .await
        .unwrap();

    let ctx = captured
        .lock()
        .unwrap()
        .clone()
        .expect("adapter factory must receive launch context");
    let env = ctx
        .env
        .into_iter()
        .collect::<std::collections::HashMap<_, _>>();
    assert_eq!(
        env.get("CODEX_HOME").map(String::as_str),
        codex_home.to_str(),
        "Codex must receive the daemon-resolved home after inherited harness state is scrubbed"
    );
}

#[tokio::test]
async fn headless_hermes_launch_pins_the_daemons_resolved_home_in_child_env() {
    let stem = format!("nexus-headless-hermes-home-env-{}", std::process::id());
    let hermes_home = std::env::temp_dir().join(format!("{stem}-home"));
    let _hermes_env = EnvRestore::set("HERMES_HOME", hermes_home.to_string_lossy().as_ref());
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();

    let captured = Arc::new(Mutex::new(None::<LaunchCtx>));
    let captured_for_factory = captured.clone();
    let mut registry = AdapterRegistry::new();
    registry.register(
        Harness::Hermes,
        Arc::new(move |ctx| {
            *captured_for_factory.lock().unwrap() = Some(ctx);
            Arc::new(MockAdapter::new()) as Arc<dyn Adapter>
        }),
    );

    let state = AppState::wire_with_registry(store, &Config::default(), registry);
    state
        .launch_agent(
            SpawnRequest {
                kind: Harness::Hermes,
                name: Some("hermes-home-probe".into()),
                identity_policy: None,
                cwd: Some("/tmp/nexus-hermes-home-probe".into()),
                project: Some("release-test".into()),
                role: None,
                initial_prompt: None,
                resume: None,
                harness_args: Vec::new(),
                headless: true,
                backend: None,
            },
            "release-test",
            None,
        )
        .await
        .unwrap();

    let ctx = captured
        .lock()
        .unwrap()
        .clone()
        .expect("adapter factory must receive launch context");
    let env = ctx
        .env
        .into_iter()
        .collect::<std::collections::HashMap<_, _>>();
    assert_eq!(
        env.get("HERMES_HOME").map(String::as_str),
        hermes_home.to_str(),
        "Hermes must receive the daemon-resolved home after inherited harness state is scrubbed"
    );
}

struct EnvRestore {
    key: &'static str,
    previous: Option<String>,
}

impl EnvRestore {
    fn set(key: &'static str, value: &str) -> Self {
        let previous = std::env::var(key).ok();
        std::env::set_var(key, value);
        Self { key, previous }
    }
}

impl Drop for EnvRestore {
    fn drop(&mut self) {
        match self.previous.take() {
            Some(value) => std::env::set_var(self.key, value),
            None => std::env::remove_var(self.key),
        }
    }
}
