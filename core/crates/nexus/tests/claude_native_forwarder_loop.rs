use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use nexus::daemon::claude_native_forwarder::{
    spawn_claude_native_forwarder, spawn_claude_native_forwarder_with_tool_events,
    ClaudeTurnCompletion,
};
use nexus::daemon::{AppState, WsSink};
use nexus_common::Config;
use nexus_contracts::{
    AgentTurnExecutionPort, Kind, Message, NexusBatch, PortResult, ProjectId, Provenance,
    RemoveRequest, RemoveResponse, Scope, SessionId, SpawnRequest, SpawnResponse,
};
use nexus_dispatch::Bell;
use nexus_harness_claude::native::bridge::{write_launch_settings, ClaudeNativeBridgePaths};
use nexus_harness_claude::storage::{ClaudeRuntimeLaunch, ClaudeRuntimeStateRepo};
use nexus_store::repos::{
    AgentRuntimes, Agents, Inbox, Messages, NewAgent, NewAgentRuntime, NewSession, Sessions,
    StreamEvents, TranscriptArchive,
};
use nexus_store::Store;

fn temp_dir(label: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("nexus-claude-loop-{label}-{nanos}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn unique_session(label: &str) -> SessionId {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    SessionId(format!("s_{label}_{nanos}"))
}

#[tokio::test]
async fn claude_turn_completion_waits_for_a_later_terminal_boundary() {
    let completion = Arc::new(ClaudeTurnCompletion::default());
    let observed = completion.snapshot();
    let waiting = {
        let completion = completion.clone();
        tokio::spawn(async move {
            completion
                .wait_after(observed, Duration::from_secs(1))
                .await
        })
    };
    tokio::task::yield_now().await;
    assert!(!waiting.is_finished());
    completion.signal();
    waiting
        .await
        .unwrap()
        .expect("terminal boundary should wake waiter");
}

#[tokio::test]
async fn claude_turn_completion_waits_for_a_later_submission_boundary() {
    let completion = Arc::new(ClaudeTurnCompletion::default());
    let observed = completion.submission_snapshot();
    let waiting = {
        let completion = completion.clone();
        tokio::spawn(async move {
            completion
                .wait_for_submission_after(observed, Duration::from_secs(1))
                .await
        })
    };
    tokio::task::yield_now().await;
    assert!(!waiting.is_finished());
    completion.signal_submission();
    waiting
        .await
        .unwrap()
        .expect("submitted-input boundary should wake waiter");
}

#[tokio::test]
async fn claude_forwarder_signals_a_structured_user_prompt_submission() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let session = unique_session("claude_submit_signal");
    let paths = seed_active_claude_runtime(store.clone(), session.clone()).await;
    let completion = Arc::new(ClaudeTurnCompletion::default());
    let observed = completion.submission_snapshot();
    let handle = spawn_claude_native_forwarder_with_tool_events(
        store,
        session,
        paths.clone(),
        Arc::new(WsSink::new(16, None)),
        Bell::new(),
        10,
        None,
        Some(completion.clone()),
    );

    std::fs::write(
        &paths.hook_log_path,
        r#"{"event":"UserPromptSubmit","payload":{"hook_event_name":"UserPromptSubmit","session_id":"claude-real","prompt":"queued during a tool loop"}}"#,
    )
    .unwrap();

    completion
        .wait_for_submission_after(observed, Duration::from_secs(1))
        .await
        .expect("Claude UserPromptSubmit should prove input acceptance");
    handle.abort();
}

#[tokio::test]
async fn boot_adoption_forwards_active_claude_hook_records_to_stream_events() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let session = SessionId("s_claude_loop".into());
    let paths = seed_active_claude_runtime(store.clone(), session.clone()).await;
    std::fs::write(
        &paths.message_delta_log_path,
        r#"{"event":"MessageDisplay","payload":{"hook_event_name":"MessageDisplay","delta":"hello from hook"}}"#,
    )
    .unwrap();

    let state = AppState::wire_pty(store.clone(), &Config::default());
    state.adopt_active_claude_forwarders().await;

    let rows = wait_for_stream_events(store.clone(), &session).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].kind, "text");
    assert!(
        rows[0].data.contains("hello from hook"),
        "unexpected stream event payload: {}",
        rows[0].data
    );
}

#[tokio::test]
async fn boot_adoption_archives_claude_lifecycle_transcript_without_delivery_input() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let session = unique_session("claude_archive_loop");
    let paths = seed_active_claude_runtime(store.clone(), session.clone()).await;
    cleanup_archive(
        default_archive_path_for_test("agent_claude_loop", &session)
            .to_string_lossy()
            .as_ref(),
    );
    let transcript =
        r#"{"type":"assistant","message":{"content":[{"type":"text","text":"archived"}]}}"#;
    std::fs::write(paths.bridge_dir.join("transcript.jsonl"), transcript).unwrap();
    std::fs::write(
        &paths.hook_log_path,
        r#"{"event":"SessionStart","payload":{"hook_event_name":"SessionStart","session_id":"claude-real"}}"#,
    )
    .unwrap();

    let state = AppState::wire_pty(store.clone(), &Config::default());
    state.adopt_active_claude_forwarders().await;

    let row = wait_for_archive(store.clone(), &session).await;
    assert_eq!(row.bytes_archived, transcript.len() as i64);
    assert_eq!(row.last_event.as_deref(), Some("SessionStart"));
    let archived = std::fs::read_to_string(&row.archive_path).unwrap();
    assert_eq!(archived, transcript);
    cleanup_archive(&row.archive_path);
}

#[cfg(unix)]
#[tokio::test]
async fn blocked_lifecycle_archive_append_does_not_delay_delivery_rearm() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let session = unique_session("claude_archive_block");
    let paths = seed_active_claude_runtime(store.clone(), session.clone()).await;
    std::fs::write(
        paths.bridge_dir.join("transcript.jsonl"),
        r#"{"type":"assistant","text":"still wakes"}"#,
    )
    .unwrap();
    std::fs::write(
        &paths.hook_log_path,
        r#"{"event":"SessionStart","payload":{"hook_event_name":"SessionStart","session_id":"claude-real"}}"#,
    )
    .unwrap();

    let archive_path = default_archive_path_for_test("agent_claude_loop", &session);
    if let Some(parent) = archive_path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    cleanup_archive(archive_path.to_string_lossy().as_ref());
    if let Some(parent) = archive_path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    let status = std::process::Command::new("mkfifo")
        .arg(&archive_path)
        .status()
        .expect("run mkfifo");
    assert!(status.success(), "mkfifo failed with {status}");

    let bell = Bell::new();
    let handle = spawn_claude_native_forwarder(
        store,
        session.clone(),
        paths,
        Arc::new(WsSink::new(16, None)),
        bell.clone(),
        10,
    );
    let woke = tokio::time::timeout(Duration::from_millis(500), bell.wait(&session)).await;
    let _unblock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&archive_path);
    handle.abort();
    cleanup_archive(archive_path.to_string_lossy().as_ref());

    assert!(
        woke.is_ok(),
        "Claude lifecycle archive append must not block delivery re-arm"
    );
}

#[tokio::test]
async fn boot_adoption_restarts_drain_loop_for_active_claude_pty_runtime() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let session = SessionId("s_claude_drain".into());
    seed_active_claude_runtime(store.clone(), session.clone()).await;
    let message_id = nexus_contracts::MessageId("m_pending_hugo".into());
    Messages::new(&store)
        .insert(&Message {
            id: message_id.clone(),
            project: ProjectId("default".into()),
            from: "operator".into(),
            scope: Scope::Dm,
            thread: None,
            topic: None,
            body: "wake up".into(),
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
        })
        .await
        .unwrap();
    Inbox::new(&store)
        .enqueue(&message_id, &session)
        .await
        .unwrap();
    let turn_exec = Arc::new(CaptureTurnExec::default());
    let state = AppState::wire_with_turn_exec(store.clone(), &Config::default(), turn_exec.clone());

    state.adopt_active_pty_runtimes().await;

    let batch = wait_for_injected_batch(turn_exec).await;
    assert_eq!(batch.counts.total, 1);
    assert_eq!(batch.message_ids, vec![message_id]);
}

async fn seed_active_claude_runtime(
    store: Arc<Store>,
    session: SessionId,
) -> ClaudeNativeBridgePaths {
    let agent_id = "agent_claude_loop".to_string();
    Agents::new(&store)
        .create(NewAgent {
            agent_id: agent_id.clone(),
            project: "default".to_string(),
            name: Some("claude-loop".to_string()),
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
            agent_id,
            harness: "claude".to_string(),
            cwd: Some(temp_dir("cwd").to_string_lossy().into_owned()),
            transport: Some("pty".to_string()),
            presence: Some("online".to_string()),
            active: true,
        })
        .await
        .unwrap();
    Sessions::new(&store)
        .create(NewSession {
            session_id: session.clone(),
            name: Some("claude-loop".to_string()),
            agent: Some("claude".to_string()),
            kind: "agent".to_string(),
            role: None,
            tier: "agent".to_string(),
            harness_session_id: None,
            client_key: Some(session.0.clone()),
            cwd: Some(temp_dir("session-cwd").to_string_lossy().into_owned()),
            project: "default".to_string(),
            transport: Some("pty".to_string()),
        })
        .await
        .unwrap();
    let heartbeat = nexus_common::now();
    store
        .conn
        .execute(
            "UPDATE sessions SET last_heartbeat = ?2 WHERE session_id = ?1",
            (session.0.clone(), heartbeat),
        )
        .await
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE agent_runtimes SET last_heartbeat = ?2 WHERE runtime_id = ?1",
            (session.0.clone(), heartbeat),
        )
        .await
        .unwrap();

    let state_root = temp_dir("state");
    let paths = ClaudeNativeBridgePaths::new(&state_root, &session);
    write_launch_settings(&paths, "claude-loop", "default", "/usr/bin/nexus").unwrap();
    ClaudeRuntimeStateRepo::new(&store)
        .upsert_launch(ClaudeRuntimeLaunch {
            runtime_id: session,
            bridge_dir: paths.bridge_dir.clone(),
            claude_session_id: None,
            launch_cwd: temp_dir("launch-cwd"),
            transcript_path: Some(paths.bridge_dir.join("transcript.jsonl")),
            bridge_pid: None,
            hook_pids_json: None,
        })
        .await
        .unwrap();
    paths
}

#[derive(Default)]
struct CaptureTurnExec {
    batches: Mutex<Vec<NexusBatch>>,
}

#[async_trait]
impl AgentTurnExecutionPort for CaptureTurnExec {
    async fn inject_turn(&self, _recipient: &SessionId, batch: &NexusBatch) -> PortResult<()> {
        self.batches.lock().unwrap().push(batch.clone());
        Ok(())
    }

    async fn launch(&self, req: SpawnRequest) -> PortResult<SpawnResponse> {
        Ok(SpawnResponse {
            session_id: req
                .name
                .map(SessionId)
                .unwrap_or_else(|| SessionId("s_capture".into())),
        })
    }

    async fn remove(&self, req: RemoveRequest) -> PortResult<RemoveResponse> {
        Ok(RemoveResponse {
            name: Some(req.name),
            status: "removed".into(),
        })
    }

    fn is_harness_alive(&self, _recipient: &SessionId) -> Option<bool> {
        Some(true)
    }
}

async fn wait_for_injected_batch(turn_exec: Arc<CaptureTurnExec>) -> NexusBatch {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        if let Some(batch) = turn_exec.batches.lock().unwrap().first().cloned() {
            return batch;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for active PTY adoption to drain pending inbox"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

async fn wait_for_stream_events(
    store: Arc<Store>,
    session: &SessionId,
) -> Vec<nexus_store::repos::StreamEventRow> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        let rows = StreamEvents::new(&store).since(session, 0).await.unwrap();
        if !rows.is_empty() {
            return rows;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for Claude native forwarder stream rows"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

async fn wait_for_archive(
    store: Arc<Store>,
    session: &SessionId,
) -> nexus_store::repos::TranscriptArchiveRow {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        if let Some(row) = TranscriptArchive::new(&store)
            .find_by_runtime_id(&session.0)
            .await
            .unwrap()
        {
            if row.bytes_archived > 0 {
                return row;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for Claude transcript archive row"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

fn cleanup_archive(path: &str) {
    let path = std::path::PathBuf::from(path);
    let _ = std::fs::remove_file(&path);
    if let Some(parent) = path.parent() {
        let _ = std::fs::remove_dir_all(parent);
    }
}

fn default_archive_path_for_test(agent_id: &str, session: &SessionId) -> std::path::PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
    std::path::PathBuf::from(home)
        .join(".nexus")
        .join("archives")
        .join(agent_id)
        .join(&session.0)
        .join("transcript.jsonl")
}
