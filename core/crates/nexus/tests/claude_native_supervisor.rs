use std::sync::Arc;

use nexus::daemon::pty_supervisor::PtySupervisor;
use nexus_contracts::SessionId;
use nexus_harness_claude::storage::ClaudeRuntimeStateRepo;
use nexus_store::Store;

fn temp_dir(label: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("nexus-claude-supervisor-{label}-{nanos}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[tokio::test]
async fn prepare_claude_native_runtime_writes_settings_and_state() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let supervisor = PtySupervisor::with_runtime_store(store.clone());
    let session = SessionId("s_claude_prepare".into());
    let state_dir = temp_dir("state");
    let cwd = temp_dir("cwd");

    let paths = supervisor
        .prepare_claude_native_runtime(
            &session,
            "blake",
            Some("blake"),
            "nexus",
            "nexus_ck_blake",
            "/usr/bin/nexus",
            cwd.to_str().unwrap(),
            &state_dir,
            None,
        )
        .await
        .unwrap();

    assert!(paths.settings_path.exists());
    let settings: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&paths.settings_path).unwrap()).unwrap();
    assert!(settings["hooks"]["MessageDisplay"].is_array());
    assert_eq!(
        settings["mcpServers"]["nexus-bus"]["args"],
        serde_json::json!([
            "mcp",
            "--as",
            "blake",
            "--project",
            "nexus",
            "--client-key",
            "nexus_ck_blake",
            "--agent",
            "claude"
        ])
    );
    assert!(paths.settings_path.starts_with(&state_dir));
    let bus_skill = cwd.join(".claude/skills/nexus-bus/SKILL.md");
    assert!(
        bus_skill.exists(),
        "headed Claude must receive the same trusted Nexus bus contract as headless Claude"
    );
    let bus_skill_body = std::fs::read_to_string(bus_skill).unwrap();
    assert!(bus_skill_body.contains("That is bus traffic from another agent"));

    let row = ClaudeRuntimeStateRepo::new(&store)
        .find_by_runtime_id(&session)
        .await
        .unwrap()
        .expect("runtime state");
    assert_eq!(row.bridge_dir, Some(paths.bridge_dir.clone()));
    assert_eq!(row.launch_cwd, Some(cwd));
    assert!(row.claude_session_id.is_none());
    assert_eq!(
        row.transcript_path,
        Some(paths.bridge_dir.join("transcript.jsonl"))
    );
}

#[tokio::test]
async fn prepare_claude_native_runtime_persists_known_resume_id() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let supervisor = PtySupervisor::with_runtime_store(store.clone());
    let session = SessionId("s_claude_resume_prepare".into());
    let state_dir = temp_dir("state-resume");
    let cwd = temp_dir("cwd-resume");

    supervisor
        .prepare_claude_native_runtime(
            &session,
            "enzo",
            Some("enzo"),
            "nexus",
            "nexus_ck_enzo",
            "/usr/bin/nexus",
            cwd.to_str().unwrap(),
            &state_dir,
            Some("claude-native-enzo"),
        )
        .await
        .unwrap();

    let row = ClaudeRuntimeStateRepo::new(&store)
        .find_by_runtime_id(&session)
        .await
        .unwrap()
        .expect("runtime state");
    assert_eq!(row.claude_session_id.as_deref(), Some("claude-native-enzo"));
}
