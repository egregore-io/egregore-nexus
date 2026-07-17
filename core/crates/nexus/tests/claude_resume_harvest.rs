use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use nexus::daemon::claude_resume_harvest::{
    harvest_claude_resume_id_for_runtime, harvest_missing_claude_resume_ids,
    ClaudeResumeHarvestStatus,
};
use nexus::daemon::AppState;
use nexus_common::Config;
use nexus_contracts::SessionId;
use nexus_harness_claude::storage::{ClaudeRuntimeLaunch, ClaudeRuntimeStateRepo};
use nexus_store::Store;
use tempfile::tempdir;

async fn migrated_store() -> Store {
    let store = Store::open(":memory:").await.unwrap();
    store.migrate().await.unwrap();
    store
}

async fn seed_missing_claude(store: &Store, runtime_id: &str, bridge_dir: &Path) -> SessionId {
    let runtime = SessionId(runtime_id.to_string());
    ClaudeRuntimeStateRepo::new(store)
        .upsert_launch(ClaudeRuntimeLaunch {
            runtime_id: runtime.clone(),
            bridge_dir: bridge_dir.to_path_buf(),
            claude_session_id: None,
            launch_cwd: PathBuf::from("/work/project"),
            transcript_path: None,
            bridge_pid: None,
            hook_pids_json: None,
        })
        .await
        .unwrap();
    runtime
}

fn write_hooks(bridge_dir: &Path, body: &str) {
    fs::create_dir_all(bridge_dir).unwrap();
    fs::write(bridge_dir.join("hooks.jsonl"), body).unwrap();
}

#[tokio::test]
async fn claude_resume_harvest_persists_resume_id_through_guarded_repo() {
    let store = migrated_store().await;
    let tmp = tempdir().unwrap();
    let bridge_dir = tmp.path().join("claude-sessions/s_claude/bridge");
    let runtime = seed_missing_claude(&store, "s_claude", &bridge_dir).await;
    write_hooks(
        &bridge_dir,
        "\
{\"event\":\"SessionStart\",\"payload\":{\"session_id\":\"claude-native-2\"}}\n\
}\n\
{\"event\":\"Stop\",\"payload\":{\"session_id\":\"claude-native-2\",\"transcript_path\":\"/tmp/claude-native-2.jsonl\"}}\n",
    );

    let report = harvest_missing_claude_resume_ids(&store, tmp.path())
        .await
        .unwrap();

    assert_eq!(report.scanned, 1);
    assert_eq!(report.updated, 1);
    assert_eq!(report.items[0].status, ClaudeResumeHarvestStatus::Updated);
    let state = ClaudeRuntimeStateRepo::new(&store)
        .find_by_runtime_id(&runtime)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state.claude_session_id.as_deref(), Some("claude-native-2"));
    assert_eq!(
        state.transcript_path,
        Some(PathBuf::from("/tmp/claude-native-2.jsonl"))
    );
}

#[tokio::test]
async fn claude_resume_harvest_refuses_ambiguous_hook_log() {
    let store = migrated_store().await;
    let tmp = tempdir().unwrap();
    let bridge_dir = tmp.path().join("claude-sessions/s_claude/bridge");
    let runtime = seed_missing_claude(&store, "s_claude", &bridge_dir).await;
    write_hooks(
        &bridge_dir,
        "\
{\"event\":\"SessionStart\",\"payload\":{\"session_id\":\"claude-native-a\"}}\n\
{\"event\":\"Stop\",\"payload\":{\"session_id\":\"claude-native-b\"}}\n",
    );

    let report = harvest_missing_claude_resume_ids(&store, tmp.path())
        .await
        .unwrap();

    assert_eq!(report.scanned, 1);
    assert_eq!(report.updated, 0);
    assert_eq!(report.skipped, 1);
    assert_eq!(
        report.items[0].status,
        ClaudeResumeHarvestStatus::AmbiguousNativeSessionId
    );
    let state = ClaudeRuntimeStateRepo::new(&store)
        .find_by_runtime_id(&runtime)
        .await
        .unwrap()
        .unwrap();
    assert!(state.claude_session_id.is_none());
}

#[tokio::test]
async fn claude_resume_harvest_reads_hook_log_even_when_sidecar_row_is_missing() {
    let store = migrated_store().await;
    let tmp = tempdir().unwrap();
    let runtime = SessionId("s_missing_sidecar".to_string());
    let bridge_dir = tmp
        .path()
        .join(".nexus/claude-sessions/s_missing_sidecar/bridge");
    write_hooks(
        &bridge_dir,
        r#"{"event":"SessionStart","payload":{"session_id":"claude-native-missing-row","transcript_path":"/tmp/missing-row.jsonl"}}"#,
    );

    let item = harvest_claude_resume_id_for_runtime(&store, tmp.path(), &runtime)
        .await
        .unwrap();

    assert_eq!(item.status, ClaudeResumeHarvestStatus::MissingRuntimeState);
    assert_eq!(
        item.native_session_id.as_deref(),
        Some("claude-native-missing-row")
    );
    assert_eq!(
        item.reason.as_deref(),
        Some(
            "no Claude runtime sidecar row exists; bridge hook log has native session evidence but there is no row to update"
        )
    );
}

#[tokio::test]
async fn daemon_boot_harvests_missing_claude_resume_ids() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let tmp = tempdir().unwrap();
    let bridge_dir = tmp.path().join("claude-sessions/s_boot_claude/bridge");
    let runtime = seed_missing_claude(&store, "s_boot_claude", &bridge_dir).await;
    write_hooks(
        &bridge_dir,
        r#"{"event":"UserPromptSubmit","payload":{"session_id":"claude-native-boot","transcript_path":"/tmp/claude-native-boot.jsonl","prompt":"hello"}}"#,
    );

    let _state = AppState::wire(store.clone(), &Config::default());
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let state = ClaudeRuntimeStateRepo::new(&store)
            .find_by_runtime_id(&runtime)
            .await
            .unwrap()
            .unwrap();
        if state.claude_session_id.as_deref() == Some("claude-native-boot") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "boot harvest did not fill Claude resume id"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
