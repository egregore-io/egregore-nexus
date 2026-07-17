use std::sync::Arc;

use nexus::daemon::pty_supervisor::tmux_session_name;
use nexus::daemon::AppState;
use nexus_common::Config;
use nexus_contracts::{Harness, Kind, Message, MessageId, ProjectId, Provenance, Scope, SessionId};
use nexus_harness_claude::storage::{ClaudeRuntimeLaunch, ClaudeRuntimeStateRepo};
use nexus_pty::TmuxHarness;
use nexus_store::repos::{
    AgentRuntimes, Agents, Inbox, NewAgent, NewAgentRuntime, NewSession, Sessions,
};
use nexus_store::Store;

const PROJECT: &str = "default";
const AGENT_NAME: &str = "warm-claude";
const AGENT_ID: &str = "agent_warm_claude";
const CODEX_AGENT_NAME: &str = "warm-codex";
const CODEX_AGENT_ID: &str = "agent_warm_codex";
const CLAUDE_NATIVE_SESSION: &str = "claude-native-warm-owner";

#[tokio::test]
async fn warm_adopts_existing_tmux_runtime_without_respawn() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let state = AppState::wire_pty(store.clone(), &Config::default());
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let session = SessionId("s_warm_adopt_existing_tmux".into());
    let cwd = temp_dir("cwd");
    seed_active_pty_runtime(
        store.clone(),
        session.clone(),
        cwd.to_string_lossy().into_owned(),
    )
    .await;

    let tmux_session = tmux_session_name(&session);
    let harness = TmuxHarness::launch(
        &tmux_session,
        "sleep",
        &["30".to_string()],
        cwd.to_string_lossy().as_ref(),
        80,
        24,
    )
    .unwrap();

    let ensured = state.ensure_harness_live(AGENT_NAME, PROJECT).await;
    let _ = harness.kill();

    assert_eq!(ensured.unwrap(), session);
    assert!(
        state
            .pty_supervisor()
            .unwrap()
            .transport()
            .is_bound(&SessionId("s_warm_adopt_existing_tmux".into())),
        "warm should adopt the already-running deterministic tmux session"
    );
}

#[tokio::test]
async fn warm_adoption_restamps_runtime_active_before_stable_recipient_drain() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let state = AppState::wire_pty(store.clone(), &Config::default());
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let session = SessionId("s_warm_adopt_stable_recipient".into());
    let cwd = temp_dir("cwd-stable");
    seed_active_pty_runtime(
        store.clone(),
        session.clone(),
        cwd.to_string_lossy().into_owned(),
    )
    .await;
    AgentRuntimes::new(&store).stop(&session.0).await.unwrap();

    let message_id = MessageId("m_stable_pending".into());
    seed_stable_agent_pending_row(store.clone(), message_id.clone()).await;

    let tmux_session = tmux_session_name(&session);
    let harness = TmuxHarness::launch(
        &tmux_session,
        "sleep",
        &["30".to_string()],
        cwd.to_string_lossy().as_ref(),
        80,
        24,
    )
    .unwrap();

    let ensured = state.ensure_harness_live(AGENT_NAME, PROJECT).await;
    let pending = Inbox::new(&store)
        .pending_for(&session, PROJECT, 50)
        .await
        .unwrap();
    let runtime = AgentRuntimes::new(&store)
        .find_by_runtime_id(&session.0)
        .await
        .unwrap()
        .unwrap();
    let _ = harness.kill();

    assert_eq!(ensured.unwrap(), session);
    assert!(
        runtime.active,
        "warm adoption must reactivate the runtime row"
    );
    assert_eq!(runtime.presence.as_deref(), Some("online"));
    assert_eq!(
        pending
            .iter()
            .map(|(_, msg)| msg.id.clone())
            .collect::<Vec<_>>(),
        vec![message_id],
        "stable-agent pending rows must be visible to the drain after adoption"
    );
}

#[tokio::test]
async fn warm_codex_does_not_adopt_surviving_tmux_as_plain_pty() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let state = AppState::wire_pty(store.clone(), &Config::default());
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let session = SessionId("s_warm_codex_no_plain_pty_adopt".into());
    let cwd = temp_dir("cwd-codex");
    seed_active_codex_appserver_runtime(
        store.clone(),
        session.clone(),
        cwd.to_string_lossy().into_owned(),
    )
    .await;

    let tmux_session = tmux_session_name(&session);
    let harness = TmuxHarness::launch(
        &tmux_session,
        "sleep",
        &["30".to_string()],
        cwd.to_string_lossy().as_ref(),
        80,
        24,
    )
    .unwrap();

    let err = state
        .ensure_harness_live(CODEX_AGENT_NAME, PROJECT)
        .await
        .unwrap_err();
    let _ = harness.kill();

    assert!(
        err.message.contains("cannot revive headed codex session"),
        "Codex warm must route to structured app-server revive; got {err:?}"
    );
    assert!(
        !state
            .pty_supervisor()
            .unwrap()
            .transport()
            .is_bound(&session),
        "generic warm must not adopt a surviving Codex remote TUI as plain PTY input"
    );
}

#[tokio::test]
async fn warm_treats_duplicate_claude_native_session_as_an_opaque_resume_hint() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let state = AppState::wire_pty(store.clone(), &Config::default());
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let stale_session = SessionId("s_warm_claude_stale".into());
    let owner_session = SessionId("s_hugo_native_owner".into());
    let stale_cwd = temp_dir("cwd-stale-claude");
    let owner_cwd = temp_dir("cwd-owner-claude");
    seed_active_pty_runtime(
        store.clone(),
        stale_session.clone(),
        stale_cwd.to_string_lossy().into_owned(),
    )
    .await;
    Sessions::new(&store)
        .create(NewSession {
            session_id: owner_session.clone(),
            name: Some("hugo".to_string()),
            agent: Some("claude".to_string()),
            kind: "agent".to_string(),
            role: None,
            tier: "agent".to_string(),
            harness_session_id: Some(CLAUDE_NATIVE_SESSION.to_string()),
            client_key: Some(owner_session.0.clone()),
            cwd: Some(owner_cwd.to_string_lossy().into_owned()),
            project: PROJECT.to_string(),
            transport: Some("pty".to_string()),
        })
        .await
        .unwrap();

    let sidecar = ClaudeRuntimeStateRepo::new(&store);
    for (runtime_id, cwd) in [
        (owner_session.clone(), owner_cwd),
        (stale_session.clone(), stale_cwd),
    ] {
        sidecar
            .upsert_launch(ClaudeRuntimeLaunch {
                runtime_id: runtime_id.clone(),
                bridge_dir: cwd.join("bridge"),
                claude_session_id: None,
                launch_cwd: cwd,
                transcript_path: None,
                bridge_pid: None,
                hook_pids_json: None,
            })
            .await
            .unwrap();
    }
    sidecar
        .set_session(&owner_session, CLAUDE_NATIVE_SESSION, None)
        .await
        .unwrap();
    sidecar
        .set_session(&stale_session, CLAUDE_NATIVE_SESSION, None)
        .await
        .unwrap();

    let stale_row = Sessions::new(&store)
        .find_by_session_id(&stale_session)
        .await
        .unwrap()
        .unwrap();
    let tail = state
        .headed_revive_tail_for_row(&stale_row, Harness::Claude)
        .await
        .unwrap();

    assert_eq!(tail, ["--resume", CLAUDE_NATIVE_SESSION]);
}

async fn seed_active_pty_runtime(store: Arc<Store>, session: SessionId, cwd: String) {
    Agents::new(&store)
        .create(NewAgent {
            agent_id: AGENT_ID.to_string(),
            project: PROJECT.to_string(),
            name: Some(AGENT_NAME.to_string()),
            default_harness: Some("claude".to_string()),
            role: None,
            tier: Some("agent".to_string()),
            owner: None,
        })
        .await
        .unwrap();
    AgentRuntimes::new(&store)
        .create(NewAgentRuntime {
            runtime_id: session.0.clone(),
            agent_id: AGENT_ID.to_string(),
            harness: "claude".to_string(),
            cwd: Some(cwd.clone()),
            transport: Some("pty".to_string()),
            presence: Some("online".to_string()),
            active: true,
        })
        .await
        .unwrap();
    Sessions::new(&store)
        .create(NewSession {
            session_id: session.clone(),
            name: Some(AGENT_NAME.to_string()),
            agent: Some("claude".to_string()),
            kind: "agent".to_string(),
            role: None,
            tier: "agent".to_string(),
            harness_session_id: None,
            client_key: Some(session.0.clone()),
            cwd: Some(cwd),
            project: PROJECT.to_string(),
            transport: Some("pty".to_string()),
        })
        .await
        .unwrap();
}

async fn seed_active_codex_appserver_runtime(store: Arc<Store>, session: SessionId, cwd: String) {
    Agents::new(&store)
        .create(NewAgent {
            agent_id: CODEX_AGENT_ID.to_string(),
            project: PROJECT.to_string(),
            name: Some(CODEX_AGENT_NAME.to_string()),
            default_harness: Some("codex".to_string()),
            role: None,
            tier: Some("agent".to_string()),
            owner: None,
        })
        .await
        .unwrap();
    AgentRuntimes::new(&store)
        .create(NewAgentRuntime {
            runtime_id: session.0.clone(),
            agent_id: CODEX_AGENT_ID.to_string(),
            harness: "codex".to_string(),
            cwd: Some(cwd.clone()),
            transport: Some("codex-appserver".to_string()),
            presence: Some("online".to_string()),
            active: true,
        })
        .await
        .unwrap();
    Sessions::new(&store)
        .create(NewSession {
            session_id: session.clone(),
            name: Some(CODEX_AGENT_NAME.to_string()),
            agent: Some("codex".to_string()),
            kind: "agent".to_string(),
            role: None,
            tier: "agent".to_string(),
            harness_session_id: None,
            client_key: Some(session.0.clone()),
            cwd: Some(cwd),
            project: PROJECT.to_string(),
            transport: Some("codex-appserver".to_string()),
        })
        .await
        .unwrap();
}

async fn seed_stable_agent_pending_row(store: Arc<Store>, message_id: MessageId) {
    nexus_store::repos::Messages::new(&store)
        .insert(&Message {
            id: message_id.clone(),
            project: ProjectId(PROJECT.to_string()),
            from: "operator".to_string(),
            scope: Scope::Dm,
            thread: None,
            topic: None,
            body: "wake from stable row".to_string(),
            summary: None,
            provenance: Provenance {
                from: "operator".to_string(),
                kind: Kind::Human,
                thread: None,
                topic: None,
                stamp: None,
            },
            created_at: nexus_common::now(),
        })
        .await
        .unwrap();
    Inbox::new(&store)
        .enqueue_for_agent(&message_id, AGENT_ID)
        .await
        .unwrap();
}

fn temp_dir(label: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("nexus-harness-warm-{label}-{nanos}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}
