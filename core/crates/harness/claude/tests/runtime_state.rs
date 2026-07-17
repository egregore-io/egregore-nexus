use std::path::{Path, PathBuf};

use libsql::params;
use nexus_contracts::SessionId;
use nexus_harness_claude::storage::{
    claude_runtime_revive_seed, ClaudeRuntimeCursor, ClaudeRuntimeLaunch, ClaudeRuntimeState,
    ClaudeRuntimeStateRepo,
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
fn revive_seed_prefers_persisted_claude_paths_and_session() {
    let runtime = SessionId("s_live_claude".into());
    let state = ClaudeRuntimeState {
        runtime_id: runtime.clone(),
        bridge_dir: Some(PathBuf::from(
            "/tmp/nexus-state/claude-sessions/s_live_claude/bridge",
        )),
        claude_session_id: Some("claude-session-1".into()),
        transcript_path: Some(PathBuf::from("/tmp/claude/transcript.jsonl")),
        launch_cwd: Some(PathBuf::from("/work/project")),
        tmux_socket: None,
        tmux_session: None,
        bridge_pid: None,
        hook_pids_json: None,
        hook_cursor: 0,
        transcript_cursor: 0,
        message_delta_cursor: 0,
        created_at: 1,
        updated_at: 1,
    };

    let seed = claude_runtime_revive_seed(Path::new("/home/user"), &runtime, Some(&state));

    assert_eq!(seed.claude_session_id.as_deref(), Some("claude-session-1"));
    assert_eq!(seed.state_dir, PathBuf::from("/tmp/nexus-state"));
    assert_eq!(
        seed.bridge_dir,
        PathBuf::from("/tmp/nexus-state/claude-sessions/s_live_claude/bridge")
    );
    assert_eq!(seed.launch_cwd, PathBuf::from("/work/project"));
    assert_eq!(
        seed.transcript_path,
        PathBuf::from("/tmp/claude/transcript.jsonl")
    );
}

#[test]
fn revive_seed_uses_deterministic_defaults_when_state_is_missing() {
    let runtime = SessionId("s_missing_claude".into());

    let seed = claude_runtime_revive_seed(Path::new("/home/user"), &runtime, None);

    assert!(seed.claude_session_id.is_none());
    assert_eq!(seed.state_dir, PathBuf::from("/home/user/.nexus"));
    assert_eq!(
        seed.bridge_dir,
        PathBuf::from("/home/user/.nexus/claude-sessions/s_missing_claude/bridge")
    );
    assert_eq!(
        seed.launch_cwd,
        PathBuf::from("/home/user/.nexus/claude-sessions/s_missing_claude/cwd")
    );
    assert_eq!(
        seed.transcript_path,
        PathBuf::from("/home/user/.nexus/claude-sessions/s_missing_claude/bridge/transcript.jsonl")
    );
}

#[tokio::test]
async fn claude_runtime_state_records_launch_session_tmux_cursors_and_delete() {
    let store = store().await;
    let repo = ClaudeRuntimeStateRepo::new(&store);
    let runtime = SessionId("s_claude_runtime".into());

    repo.upsert_launch(ClaudeRuntimeLaunch {
        runtime_id: runtime.clone(),
        bridge_dir: PathBuf::from("/tmp/bridge"),
        claude_session_id: None,
        launch_cwd: PathBuf::from("/work/project"),
        transcript_path: None,
        bridge_pid: Some(4321),
        hook_pids_json: Some("[101,102]".to_string()),
    })
    .await
    .unwrap();

    let launched = repo
        .find_by_runtime_id(&runtime)
        .await
        .unwrap()
        .expect("runtime state");
    assert_eq!(launched.runtime_id, runtime);
    assert_eq!(launched.bridge_dir, Some(PathBuf::from("/tmp/bridge")));
    assert_eq!(launched.launch_cwd, Some(PathBuf::from("/work/project")));
    assert!(launched.claude_session_id.is_none());
    assert!(launched.transcript_path.is_none());
    assert_eq!(launched.bridge_pid, Some(4321));
    assert_eq!(launched.hook_pids_json.as_deref(), Some("[101,102]"));
    assert_eq!(launched.hook_cursor, 0);
    assert_eq!(launched.transcript_cursor, 0);
    assert_eq!(launched.message_delta_cursor, 0);
    assert!(launched.created_at > 0);
    assert!(launched.updated_at >= launched.created_at);

    repo.set_process_detail(&runtime, Some(9876), Some("[201]"))
        .await
        .unwrap();
    let with_process_detail = repo
        .find_by_runtime_id(&runtime)
        .await
        .unwrap()
        .expect("runtime state");
    assert_eq!(with_process_detail.bridge_pid, Some(9876));
    assert_eq!(with_process_detail.hook_pids_json.as_deref(), Some("[201]"));

    repo.set_claude_session(
        &runtime,
        "claude-session-1",
        Some(PathBuf::from("/tmp/bridge/transcript.jsonl")),
    )
    .await
    .unwrap();
    repo.set_transcript_path(&runtime, PathBuf::from("/tmp/bridge/transcript-2.jsonl"))
        .await
        .unwrap();
    repo.set_tmux(&runtime, Some("/tmp/tmux.sock"), "nexus-s_claude_runtime")
        .await
        .unwrap();

    repo.set_hook_cursor(&runtime, 12).await.unwrap();
    repo.set_hook_cursor(&runtime, 7).await.unwrap();
    repo.set_cursor(&runtime, ClaudeRuntimeCursor::Transcript, 64)
        .await
        .unwrap();
    repo.set_cursors(&runtime, None, Some(61), Some(128))
        .await
        .unwrap();

    let updated = repo
        .find_by_runtime_id(&runtime)
        .await
        .unwrap()
        .expect("updated runtime state");
    assert_eq!(
        updated.claude_session_id.as_deref(),
        Some("claude-session-1")
    );
    assert_eq!(
        updated.transcript_path,
        Some(PathBuf::from("/tmp/bridge/transcript-2.jsonl"))
    );
    assert_eq!(updated.tmux_socket.as_deref(), Some("/tmp/tmux.sock"));
    assert_eq!(
        updated.tmux_session.as_deref(),
        Some("nexus-s_claude_runtime")
    );
    assert_eq!(updated.hook_cursor, 12);
    assert_eq!(updated.transcript_cursor, 64);
    assert_eq!(updated.message_delta_cursor, 128);

    repo.set_transcript_path_and_cursor(&runtime, PathBuf::from("/tmp/claude/real.jsonl"), 4)
        .await
        .unwrap();
    let switched = repo
        .find_by_runtime_id(&runtime)
        .await
        .unwrap()
        .expect("switched runtime state");
    assert_eq!(
        switched.transcript_path,
        Some(PathBuf::from("/tmp/claude/real.jsonl"))
    );
    assert_eq!(
        switched.transcript_cursor, 4,
        "switching transcript files replaces the old file cursor"
    );

    repo.delete(&runtime).await.unwrap();
    assert!(repo.find_by_runtime_id(&runtime).await.unwrap().is_none());
}

#[tokio::test]
async fn list_missing_claude_session_id_is_read_only_when_table_is_absent() {
    let store = Store::open(":memory:").await.unwrap();
    let missing = ClaudeRuntimeStateRepo::new(&store)
        .list_missing_claude_session_id()
        .await
        .unwrap();

    assert!(missing.is_empty());
    let mut rows = store
        .conn
        .query(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'claude_runtime_state'",
            (),
        )
        .await
        .unwrap();
    assert!(rows.next().await.unwrap().is_none());
}

#[tokio::test]
async fn upsert_launch_records_known_native_resume_id() {
    let store = store().await;
    let repo = ClaudeRuntimeStateRepo::new(&store);
    let runtime = SessionId("s_claude_resume_spawn".into());

    repo.upsert_launch(ClaudeRuntimeLaunch {
        runtime_id: runtime.clone(),
        bridge_dir: PathBuf::from("/tmp/bridge"),
        claude_session_id: Some("claude-native-resume-id".into()),
        launch_cwd: PathBuf::from("/work/project"),
        transcript_path: Some(PathBuf::from("/tmp/bridge/transcript.jsonl")),
        bridge_pid: None,
        hook_pids_json: None,
    })
    .await
    .unwrap();

    let launched = repo
        .find_by_runtime_id(&runtime)
        .await
        .unwrap()
        .expect("runtime state");
    assert_eq!(
        launched.claude_session_id.as_deref(),
        Some("claude-native-resume-id")
    );
}

#[tokio::test]
async fn set_claude_session_does_not_claim_native_identity_for_nexus() {
    let store = store().await;
    let repo = ClaudeRuntimeStateRepo::new(&store);
    let runtime = SessionId("s_claude_binding".into());
    seed_identity_session(
        &store,
        &runtime,
        "claude",
        "agent_claude_binding",
        "claude-binding",
    )
    .await;

    repo.upsert_launch(ClaudeRuntimeLaunch {
        runtime_id: runtime.clone(),
        bridge_dir: PathBuf::from("/tmp/bridge"),
        claude_session_id: None,
        launch_cwd: PathBuf::from("/work/project"),
        transcript_path: None,
        bridge_pid: None,
        hook_pids_json: None,
    })
    .await
    .unwrap();
    repo.set_claude_session(&runtime, "claude-native-binding", None)
        .await
        .unwrap();

    let binding = NativeThreadBindings::new(&store)
        .find("claude", "claude-native-binding")
        .await
        .unwrap();
    assert!(
        binding.is_none(),
        "Claude Code's native session key is an opaque hint, not a Nexus identity claim"
    );
}

#[tokio::test]
async fn upsert_launch_preserves_discovered_claude_runtime_fields() {
    let store = store().await;
    let repo = ClaudeRuntimeStateRepo::new(&store);
    let runtime = SessionId("s_claude_runtime".into());

    repo.upsert_launch(ClaudeRuntimeLaunch {
        runtime_id: runtime.clone(),
        bridge_dir: PathBuf::from("/tmp/first-bridge"),
        claude_session_id: None,
        launch_cwd: PathBuf::from("/work/first"),
        transcript_path: None,
        bridge_pid: None,
        hook_pids_json: None,
    })
    .await
    .unwrap();
    repo.set_session(
        &runtime,
        "claude-session-1",
        Some(PathBuf::from("/tmp/first-bridge/transcript.jsonl")),
    )
    .await
    .unwrap();
    repo.set_tmux(&runtime, None, "nexus-s_claude_runtime")
        .await
        .unwrap();
    repo.set_cursors(&runtime, Some(10), Some(20), Some(30))
        .await
        .unwrap();

    repo.upsert_launch(ClaudeRuntimeLaunch {
        runtime_id: runtime.clone(),
        bridge_dir: PathBuf::from("/tmp/second-bridge"),
        claude_session_id: None,
        launch_cwd: PathBuf::from("/work/second"),
        transcript_path: None,
        bridge_pid: None,
        hook_pids_json: None,
    })
    .await
    .unwrap();

    let row = repo
        .find_by_runtime_id(&runtime)
        .await
        .unwrap()
        .expect("runtime state");
    assert_eq!(row.bridge_dir, Some(PathBuf::from("/tmp/second-bridge")));
    assert_eq!(row.launch_cwd, Some(PathBuf::from("/work/second")));
    assert_eq!(row.claude_session_id.as_deref(), Some("claude-session-1"));
    assert_eq!(
        row.transcript_path,
        Some(PathBuf::from("/tmp/first-bridge/transcript.jsonl"))
    );
    assert_eq!(row.tmux_session.as_deref(), Some("nexus-s_claude_runtime"));
    assert_eq!(row.hook_cursor, 10);
    assert_eq!(row.transcript_cursor, 20);
    assert_eq!(row.message_delta_cursor, 30);
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

#[tokio::test]
async fn find_by_claude_session_id_resolves_persisted_runtime() {
    let store = store().await;
    let repo = ClaudeRuntimeStateRepo::new(&store);
    let runtime = SessionId("s_claude_runtime".into());

    repo.upsert_launch(ClaudeRuntimeLaunch {
        runtime_id: runtime.clone(),
        bridge_dir: PathBuf::from("/tmp/bridge"),
        claude_session_id: None,
        launch_cwd: PathBuf::from("/work/project"),
        transcript_path: None,
        bridge_pid: None,
        hook_pids_json: None,
    })
    .await
    .unwrap();
    repo.set_session(
        &runtime,
        "claude-native-session",
        Some(PathBuf::from("/tmp/bridge/transcript.jsonl")),
    )
    .await
    .unwrap();

    let row = repo
        .find_by_claude_session_id("claude-native-session")
        .await
        .unwrap()
        .expect("runtime state");

    assert_eq!(row.runtime_id, runtime);
    assert_eq!(
        row.claude_session_id.as_deref(),
        Some("claude-native-session")
    );
    assert!(repo
        .find_by_claude_session_id("missing-session")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn set_claude_session_keeps_native_identity_opaque_to_nexus() {
    let store = store().await;
    let repo = ClaudeRuntimeStateRepo::new(&store);
    let first = SessionId("s_claude_first".into());
    let second = SessionId("s_claude_second".into());

    for runtime in [&first, &second] {
        repo.upsert_launch(ClaudeRuntimeLaunch {
            runtime_id: runtime.clone(),
            bridge_dir: PathBuf::from(format!("/tmp/{}/bridge", runtime.0)),
            claude_session_id: None,
            launch_cwd: PathBuf::from("/work/project"),
            transcript_path: None,
            bridge_pid: None,
            hook_pids_json: None,
        })
        .await
        .unwrap();
    }
    repo.set_session(
        &first,
        "claude-native-session",
        Some(PathBuf::from("/tmp/first/transcript.jsonl")),
    )
    .await
    .unwrap();

    repo.set_session(
        &second,
        "claude-native-session",
        Some(PathBuf::from("/tmp/second/transcript.jsonl")),
    )
    .await
    .expect("Nexus must not validate or claim Claude Code's native identity space");

    let second_row = repo
        .find_by_runtime_id(&second)
        .await
        .unwrap()
        .expect("second runtime row");
    assert_eq!(
        second_row.claude_session_id.as_deref(),
        Some("claude-native-session"),
        "the native key is an opaque best-effort resume hint, not a Nexus-owned identity"
    );
    assert_eq!(
        repo.find_all_by_claude_session_id("claude-native-session")
            .await
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn diagnostic_lookup_reports_duplicate_claude_resume_hints() {
    let store = store().await;
    let repo = ClaudeRuntimeStateRepo::new(&store);
    repo.ensure_schema().await.unwrap();
    store
        .conn
        .execute(
            "INSERT INTO claude_runtime_state (
                runtime_id, claude_session_id, hook_cursor, transcript_cursor,
                message_delta_cursor, created_at, updated_at
            ) VALUES
                (?1, ?3, 0, 0, 0, 1, 1),
                (?2, ?3, 0, 0, 0, 2, 2)",
            params!["s_claude_first", "s_claude_second", "claude-native-session",],
        )
        .await
        .unwrap();

    let err = repo
        .find_by_claude_session_id("claude-native-session")
        .await
        .expect_err("a singular diagnostic lookup cannot choose between duplicate hints");

    assert!(
        err.to_string().contains("claude-native-session")
            && err.to_string().contains("s_claude_first")
            && err.to_string().contains("s_claude_second"),
        "unexpected error: {err}"
    );
}
