use std::path::{Path, PathBuf};

use nexus_contracts::SessionId;
use nexus_harness_codex::storage::{
    codex_runtime_revive_seed, codex_runtime_viewer_backend, CodexRuntimeLaunch, CodexRuntimeState,
    CodexRuntimeStateRepo,
};
use nexus_store::{
    repos::{Agents, NativeThreadBindings, NewAgent, NewSession, Sessions},
    Store,
};

async fn store() -> Store {
    let store = Store::open(":memory:").await.unwrap();
    store.migrate().await.unwrap();
    store
}

#[test]
fn revive_seed_prefers_persisted_codex_thread_and_home() {
    let runtime = SessionId("s_live_codex".into());
    let state = CodexRuntimeState {
        runtime_id: runtime.clone(),
        codex_thread_id: Some("persisted-thread".into()),
        codex_home: Some(PathBuf::from(
            "/tmp/nexus-state/codex-sessions/s_live_codex/codex-home",
        )),
        app_server_sock: None,
        app_server_pid: None,
        mcp_sidecar_pids_json: None,
        app_server_adopted: false,
        tmux_socket: None,
        tmux_session: None,
        rollout_path: None,
        created_at: 1,
        updated_at: 1,
    };

    let seed = codex_runtime_revive_seed(
        Path::new("/home/user"),
        &runtime,
        Some(&state),
        Some("legacy-thread"),
    );

    assert_eq!(seed.thread_id.as_deref(), Some("persisted-thread"));
    assert_eq!(seed.state_dir, PathBuf::from("/tmp/nexus-state"));
    assert_eq!(
        seed.rollout_root,
        PathBuf::from("/tmp/nexus-state/codex-sessions/s_live_codex/codex-home/sessions")
    );
}

#[test]
fn revive_seed_ignores_loopback_endpoint_as_a_filesystem_path() {
    let runtime = SessionId("s_windows_codex".into());
    let state = CodexRuntimeState {
        runtime_id: runtime.clone(),
        codex_thread_id: Some("persisted-thread".into()),
        codex_home: Some(PathBuf::from(
            "/nexus-state/codex-sessions/s_windows_codex/codex-home",
        )),
        app_server_sock: Some(PathBuf::from("ws://127.0.0.1:43127")),
        app_server_pid: Some(4242),
        mcp_sidecar_pids_json: None,
        app_server_adopted: false,
        tmux_socket: None,
        tmux_session: None,
        rollout_path: None,
        created_at: 1,
        updated_at: 1,
    };

    let seed = codex_runtime_revive_seed(Path::new("/home/user"), &runtime, Some(&state), None);

    assert_eq!(seed.state_dir, PathBuf::from("/nexus-state"));
    assert_eq!(
        seed.rollout_root,
        PathBuf::from("/nexus-state/codex-sessions/s_windows_codex/codex-home/sessions")
    );
}

#[test]
fn revive_seed_falls_back_to_legacy_thread_and_default_paths() {
    let runtime = SessionId("s_legacy_codex".into());

    let seed = codex_runtime_revive_seed(
        Path::new("/home/user"),
        &runtime,
        None,
        Some("legacy-thread"),
    );

    assert_eq!(seed.thread_id.as_deref(), Some("legacy-thread"));
    assert_eq!(seed.state_dir, PathBuf::from("/home/user/.nexus"));
    assert_eq!(
        seed.rollout_root,
        PathBuf::from("/home/user/.nexus/codex-sessions/s_legacy_codex/codex-home/sessions")
    );
}

#[test]
fn revive_viewer_backend_preserves_the_persisted_spawn_mode() {
    let mut state = CodexRuntimeState {
        runtime_id: SessionId("s_backend_codex".into()),
        codex_thread_id: Some("thread".into()),
        codex_home: None,
        app_server_sock: None,
        app_server_pid: None,
        mcp_sidecar_pids_json: None,
        app_server_adopted: false,
        tmux_socket: None,
        tmux_session: None,
        rollout_path: None,
        created_at: 1,
        updated_at: 1,
    };

    assert_eq!(codex_runtime_viewer_backend(Some(&state)), "pty");
    assert_eq!(codex_runtime_viewer_backend(None), "pty");

    state.tmux_socket = Some("/tmp/nexus-codex.sock".into());
    state.tmux_session = Some("nexus-s_backend_codex".into());
    assert_eq!(codex_runtime_viewer_backend(Some(&state)), "tmux");
}

#[tokio::test]
async fn codex_runtime_state_records_launch_thread_tmux_and_delete() {
    let store = store().await;
    let repo = CodexRuntimeStateRepo::new(&store);
    let runtime = SessionId("s_codex_runtime".into());

    repo.upsert_launch(CodexRuntimeLaunch {
        runtime_id: runtime.clone(),
        codex_thread_id: None,
        codex_home: PathBuf::from("/tmp/codex-home"),
        app_server_sock: PathBuf::from("/tmp/codex.sock"),
        app_server_pid: Some(4242),
        mcp_sidecar_pids_json: Some("[111,222]".to_string()),
        app_server_adopted: false,
    })
    .await
    .unwrap();

    let launched = repo
        .find_by_runtime_id(&runtime)
        .await
        .unwrap()
        .expect("runtime state");
    assert_eq!(launched.runtime_id, runtime);
    assert_eq!(launched.codex_home, Some(PathBuf::from("/tmp/codex-home")));
    assert_eq!(
        launched.app_server_sock,
        Some(PathBuf::from("/tmp/codex.sock"))
    );
    assert_eq!(launched.app_server_pid, Some(4242));
    assert_eq!(launched.mcp_sidecar_pids_json.as_deref(), Some("[111,222]"));
    assert!(!launched.app_server_adopted);
    assert!(launched.codex_thread_id.is_none());
    assert!(launched.created_at > 0);
    assert!(launched.updated_at >= launched.created_at);

    repo.set_mcp_sidecar_pids(&runtime, Some("[333]"))
        .await
        .unwrap();
    let refreshed = repo
        .find_by_runtime_id(&runtime)
        .await
        .unwrap()
        .expect("runtime state");
    assert_eq!(refreshed.mcp_sidecar_pids_json.as_deref(), Some("[333]"));

    repo.mark_adopted(&runtime).await.unwrap();
    repo.set_thread(
        &runtime,
        "codex-thread-1",
        Some(PathBuf::from("/tmp/codex-home/sessions/rollout-1.jsonl")),
    )
    .await
    .unwrap();
    repo.set_tmux(&runtime, Some("/tmp/tmux.sock"), "nexus-s_codex_runtime")
        .await
        .unwrap();

    let updated = repo
        .find_by_runtime_id(&runtime)
        .await
        .unwrap()
        .expect("updated runtime state");
    assert!(updated.app_server_adopted);
    assert_eq!(updated.codex_thread_id.as_deref(), Some("codex-thread-1"));
    assert_eq!(
        updated.rollout_path,
        Some(PathBuf::from("/tmp/codex-home/sessions/rollout-1.jsonl"))
    );
    assert_eq!(updated.tmux_socket.as_deref(), Some("/tmp/tmux.sock"));
    assert_eq!(
        updated.tmux_session.as_deref(),
        Some("nexus-s_codex_runtime")
    );

    repo.delete(&runtime).await.unwrap();
    assert!(repo.find_by_runtime_id(&runtime).await.unwrap().is_none());
}

#[tokio::test]
async fn upsert_launch_records_known_resume_thread_id() {
    let store = store().await;
    let repo = CodexRuntimeStateRepo::new(&store);
    let runtime = SessionId("s_codex_resume_spawn".into());

    repo.upsert_launch(CodexRuntimeLaunch {
        runtime_id: runtime.clone(),
        codex_thread_id: Some("codex-thread-resume-id".into()),
        codex_home: PathBuf::from("/tmp/codex-home"),
        app_server_sock: PathBuf::from("/tmp/codex.sock"),
        app_server_pid: Some(4242),
        mcp_sidecar_pids_json: None,
        app_server_adopted: false,
    })
    .await
    .unwrap();

    let launched = repo
        .find_by_runtime_id(&runtime)
        .await
        .unwrap()
        .expect("runtime state");
    assert_eq!(
        launched.codex_thread_id.as_deref(),
        Some("codex-thread-resume-id")
    );
}

#[tokio::test]
async fn set_thread_claims_native_thread_binding_for_identity() {
    let store = store().await;
    let repo = CodexRuntimeStateRepo::new(&store);
    let runtime = SessionId("s_codex_binding".into());
    seed_identity_session(
        &store,
        &runtime,
        "codex",
        "agent_codex_binding",
        "codex-binding",
    )
    .await;

    repo.upsert_launch(CodexRuntimeLaunch {
        runtime_id: runtime.clone(),
        codex_thread_id: None,
        codex_home: PathBuf::from("/tmp/codex-home"),
        app_server_sock: PathBuf::from("/tmp/codex.sock"),
        app_server_pid: Some(4242),
        mcp_sidecar_pids_json: None,
        app_server_adopted: false,
    })
    .await
    .unwrap();
    repo.set_thread(&runtime, "codex-thread-binding", None)
        .await
        .unwrap();

    let binding = NativeThreadBindings::new(&store)
        .find("codex", "codex-thread-binding")
        .await
        .unwrap()
        .expect("native binding");
    assert_eq!(binding.agent_id, "agent_codex_binding");
    assert_eq!(binding.last_runtime_id.as_deref(), Some(runtime.0.as_str()));
}

#[tokio::test]
async fn set_thread_preserves_existing_rollout_path_when_new_path_is_none() {
    let store = store().await;
    let repo = CodexRuntimeStateRepo::new(&store);
    let runtime = SessionId("s_codex_runtime_preserve_rollout".into());
    let rollout = PathBuf::from("/tmp/codex-home/sessions/rollout-1.jsonl");

    repo.upsert_launch(CodexRuntimeLaunch {
        runtime_id: runtime.clone(),
        codex_thread_id: None,
        codex_home: PathBuf::from("/tmp/codex-home"),
        app_server_sock: PathBuf::from("/tmp/codex.sock"),
        app_server_pid: Some(4242),
        mcp_sidecar_pids_json: None,
        app_server_adopted: false,
    })
    .await
    .unwrap();
    repo.set_thread(&runtime, "codex-thread-1", Some(rollout.clone()))
        .await
        .unwrap();

    repo.set_thread(&runtime, "codex-thread-1", None)
        .await
        .unwrap();

    let row = repo
        .find_by_runtime_id(&runtime)
        .await
        .unwrap()
        .expect("runtime state");
    assert_eq!(row.rollout_path, Some(rollout));
}

#[tokio::test]
async fn upsert_launch_preserves_discovered_thread() {
    let store = store().await;
    let repo = CodexRuntimeStateRepo::new(&store);
    let runtime = SessionId("s_codex_runtime".into());

    repo.upsert_launch(CodexRuntimeLaunch {
        runtime_id: runtime.clone(),
        codex_thread_id: None,
        codex_home: PathBuf::from("/tmp/first-home"),
        app_server_sock: PathBuf::from("/tmp/first.sock"),
        app_server_pid: Some(1),
        mcp_sidecar_pids_json: None,
        app_server_adopted: false,
    })
    .await
    .unwrap();
    repo.set_thread(&runtime, "codex-thread-1", None)
        .await
        .unwrap();

    repo.upsert_launch(CodexRuntimeLaunch {
        runtime_id: runtime.clone(),
        codex_thread_id: None,
        codex_home: PathBuf::from("/tmp/second-home"),
        app_server_sock: PathBuf::from("/tmp/second.sock"),
        app_server_pid: Some(2),
        mcp_sidecar_pids_json: None,
        app_server_adopted: true,
    })
    .await
    .unwrap();

    let row = repo
        .find_by_runtime_id(&runtime)
        .await
        .unwrap()
        .expect("runtime state");
    assert_eq!(row.codex_thread_id.as_deref(), Some("codex-thread-1"));
    assert_eq!(row.codex_home, Some(PathBuf::from("/tmp/second-home")));
    assert_eq!(row.app_server_sock, Some(PathBuf::from("/tmp/second.sock")));
    assert_eq!(row.app_server_pid, Some(2));
    assert!(row.app_server_adopted);
}

async fn seed_identity_session(
    store: &Store,
    runtime: &SessionId,
    harness: &str,
    agent_id: &str,
    name: &str,
) {
    Agents::new(store)
        .create(NewAgent {
            agent_id: agent_id.to_string(),
            project: "default".to_string(),
            name: Some(name.to_string()),
            default_harness: Some(harness.to_string()),
            role: None,
            tier: Some("agent".to_string()),
            owner: None,
        })
        .await
        .unwrap();
    Sessions::new(store)
        .create(NewSession {
            session_id: runtime.clone(),
            name: Some(name.to_string()),
            agent: Some(harness.to_string()),
            kind: "agent".to_string(),
            role: None,
            tier: "agent".to_string(),
            harness_session_id: None,
            client_key: Some(runtime.0.clone()),
            cwd: Some("/work/project".to_string()),
            project: "default".to_string(),
            transport: Some("pty".to_string()),
        })
        .await
        .unwrap();
    Sessions::new(store)
        .set_agent_id(runtime, agent_id)
        .await
        .unwrap();
}
