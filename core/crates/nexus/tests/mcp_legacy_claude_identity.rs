use std::path::PathBuf;

use nexus::cli::ambient::TestEnvGuard;
use nexus::cli::commands::mcp::{mcp_identity_for_store_with_claude_session, McpArgs};
use nexus_contracts::SessionId;
use nexus_harness_claude::storage::{ClaudeRuntimeLaunch, ClaudeRuntimeStateRepo};
use nexus_store::repos::{NewSession, Sessions};
use nexus_store::Store;

async fn migrated_store() -> Store {
    let store = Store::open(":memory:").await.unwrap();
    store.migrate().await.unwrap();
    store
}

async fn seed_claude_runtime(
    store: &Store,
    name: &str,
    project: &str,
    runtime_id: &str,
    client_key: &str,
    claude_session_id: &str,
) {
    let runtime = SessionId(runtime_id.to_string());
    Sessions::new(store)
        .create(NewSession {
            session_id: runtime.clone(),
            name: Some(name.to_string()),
            agent: Some("claude".to_string()),
            kind: "agent".to_string(),
            role: None,
            tier: "agent".to_string(),
            harness_session_id: Some(claude_session_id.to_string()),
            client_key: Some(client_key.to_string()),
            cwd: Some("/work/project".to_string()),
            project: project.to_string(),
            transport: Some("pty".to_string()),
        })
        .await
        .unwrap();

    let repo = ClaudeRuntimeStateRepo::new(store);
    repo.upsert_launch(ClaudeRuntimeLaunch {
        runtime_id: runtime.clone(),
        bridge_dir: PathBuf::from("/tmp/nexus/claude/bridge"),
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
        claude_session_id,
        Some(PathBuf::from("/tmp/nexus/claude/transcript.jsonl")),
    )
    .await
    .unwrap();
}

fn mcp_args(name: &str, project: &str) -> McpArgs {
    McpArgs {
        name: name.to_string(),
        project: project.to_string(),
        client_key: None,
        agent: None,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn claude_native_session_never_authenticates_a_nexus_mcp_identity() {
    let _env = TestEnvGuard::cleared(&["NEXUS_SESSION_ID", "CLAUDE_CODE_SESSION_ID"]);
    let store = migrated_store().await;
    seed_claude_runtime(
        &store,
        "hugo",
        "default",
        "s_hugo_runtime",
        "nexus_ck_hugo",
        "claude-native-session",
    )
    .await;

    let identity = mcp_identity_for_store_with_claude_session(
        &mcp_args("hugo", "default"),
        &store,
        Some("claude-native-session"),
    )
    .await;

    assert_eq!(identity.name.as_deref(), Some("hugo"));
    assert_eq!(identity.project, "default");
    assert_eq!(identity.client_key, "mcp:hugo");
    assert_eq!(identity.harness_session_id, "mcp:hugo");
    assert_eq!(identity.harness.as_str(), "other");
}

#[tokio::test(flavor = "current_thread")]
async fn claude_native_session_does_not_override_the_explicit_mcp_boundary() {
    let _env = TestEnvGuard::new(&[
        ("NEXUS_SESSION_ID", Some("stable-nexus-session")),
        ("CLAUDE_CODE_SESSION_ID", Some("claude-native-session")),
    ]);

    let store = migrated_store().await;
    seed_claude_runtime(
        &store,
        "hugo",
        "default",
        "s_hugo_runtime",
        "nexus_ck_hugo",
        "claude-native-session",
    )
    .await;

    let identity = mcp_identity_for_store_with_claude_session(
        &mcp_args("hugo", "default"),
        &store,
        Some("claude-native-session"),
    )
    .await;

    assert_eq!(identity.client_key, "mcp:hugo");
    assert_eq!(identity.harness_session_id, "mcp:hugo");
}

#[tokio::test]
async fn legacy_claude_mcp_does_not_adopt_key_when_name_or_project_do_not_match() {
    let store = migrated_store().await;
    seed_claude_runtime(
        &store,
        "hugo",
        "default",
        "s_hugo_runtime",
        "nexus_ck_hugo",
        "claude-native-session",
    )
    .await;

    let wrong_name = mcp_identity_for_store_with_claude_session(
        &mcp_args("bianca", "default"),
        &store,
        Some("claude-native-session"),
    )
    .await;
    let wrong_project = mcp_identity_for_store_with_claude_session(
        &mcp_args("hugo", "other"),
        &store,
        Some("claude-native-session"),
    )
    .await;
    let missing_proof = mcp_identity_for_store_with_claude_session(
        &mcp_args("hugo", "default"),
        &store,
        Some("missing-session"),
    )
    .await;

    assert_eq!(wrong_name.client_key, "mcp:bianca");
    assert_eq!(wrong_project.client_key, "mcp:hugo");
    assert_eq!(missing_proof.client_key, "mcp:hugo");
}
