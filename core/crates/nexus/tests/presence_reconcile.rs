use std::sync::Arc;
use std::time::Duration;

use nexus::daemon::AppState;
use nexus_common::Config;
use nexus_contracts::{
    AgentRuntimeListRequest, AgentTurnExecutionPort, Caller, Kind, Message, MessageId, NexusBatch,
    PortResult, Presence, ProjectId, Provenance, RemoveRequest, RemoveResponse, Scope, SessionId,
    SpawnRequest, SpawnResponse, Tier, WsEvent,
};
use nexus_harness_claude::storage::{ClaudeRuntimeLaunch, ClaudeRuntimeStateRepo};
use nexus_harness_codex::storage::{CodexRuntimeLaunch, CodexRuntimeStateRepo};
use nexus_store::repos::{
    AgentRuntimes, AgentSessionMessages, Agents, Inbox, Messages, NewAgent, NewAgentRuntime,
    NewAgentSessionMessage, NewSession, Sessions, TranscriptArchive,
};
use nexus_store::types::AgentRuntimeRow;
use nexus_store::Store;

const PROJECT: &str = "default";

#[tokio::test]
async fn reconcile_marks_stale_sessions_and_runtimes_offline() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let mut config = Config::default();
    config.heartbeat_ttl_ms = 30_000;
    let state = AppState::wire(store.clone(), &config);
    state.wait_for_runtime_identity_ready().await.unwrap();

    seed_agent_runtime(
        &store,
        "stale",
        "s_stale",
        "a_stale",
        1_700_000_000_000 - 60_000,
    )
    .await;
    seed_agent_runtime(&store, "fresh", "s_fresh", "a_fresh", nexus_common::now()).await;
    seed_agent_runtime(
        &store,
        "offline-stale",
        "s_offline_stale",
        "a_offline_stale",
        1_700_000_000_000 - 60_000,
    )
    .await;
    Sessions::new(&store)
        .set_presence(&SessionId("s_offline_stale".to_string()), Presence::Offline)
        .await
        .unwrap();
    seed_orphan_runtime(&store, "orphan-stale", "s_orphan_stale", "a_orphan_stale").await;
    seed_open_agent_turn(&store, "s_stale", 10, "half-written stale turn").await;
    seed_open_agent_turn(&store, "s_fresh", 20, "active fresh turn").await;

    let mut events = state.ws.subscribe();
    state.reconcile_stale_presence_once().await.unwrap();

    let notification = tokio::time::timeout(Duration::from_secs(1), events.recv())
        .await
        .expect("stale status fact timeout")
        .expect("stale status fact channel");
    let status: WsEvent =
        serde_json::from_value(notification.params.expect("status params")).expect("status event");
    assert!(matches!(
        status,
        WsEvent::AgentStatus {
            session_id,
            presence: Presence::Offline,
            paused: false,
        } if session_id.0 == "s_stale"
    ));

    let sessions = Sessions::new(&store);
    let stale = sessions
        .find_by_session_id(&SessionId("s_stale".to_string()))
        .await
        .unwrap()
        .expect("stale session");
    assert_eq!(stale.presence.as_deref(), Some("offline"));

    let fresh = sessions
        .find_by_session_id(&SessionId("s_fresh".to_string()))
        .await
        .unwrap()
        .expect("fresh session");
    assert_eq!(fresh.presence.as_deref(), Some("online"));

    let runtimes = AgentRuntimes::new(&store);
    let stale_runtime = runtimes
        .find_by_runtime_id("s_stale")
        .await
        .unwrap()
        .expect("stale runtime");
    assert!(!stale_runtime.active);
    assert_eq!(stale_runtime.presence.as_deref(), Some("offline"));
    assert!(stale_runtime.stopped_at.is_some());

    let fresh_runtime = runtimes
        .find_by_runtime_id("s_fresh")
        .await
        .unwrap()
        .expect("fresh runtime");
    assert!(fresh_runtime.active);
    assert_eq!(fresh_runtime.presence.as_deref(), Some("online"));

    for runtime_id in ["s_offline_stale", "s_orphan_stale"] {
        let runtime = runtimes
            .find_by_runtime_id(runtime_id)
            .await
            .unwrap()
            .expect("runtime sweep row");
        assert!(
            !runtime.active,
            "{runtime_id} must be stopped by runtime sweep"
        );
        assert_eq!(runtime.presence.as_deref(), Some("offline"));
    }

    let reader = admin_caller();
    let stale_visible = state
        .list_agent_runtimes(
            &reader,
            AgentRuntimeListRequest {
                agent_id: None,
                name: Some("stale".to_string()),
                include_stopped: Some(false),
            },
        )
        .await
        .unwrap();
    assert!(
        stale_visible.runtimes.is_empty(),
        "reconcile must keep stale agent_runtimes out of active runtime reads"
    );

    let stale_audit = state
        .list_agent_runtimes(
            &reader,
            AgentRuntimeListRequest {
                agent_id: None,
                name: Some("stale".to_string()),
                include_stopped: Some(true),
            },
        )
        .await
        .unwrap();
    let stale_summary = stale_audit
        .runtimes
        .iter()
        .find(|runtime| runtime.runtime_id.0 == "s_stale")
        .expect("include_stopped keeps the stale runtime visible for audit");
    assert!(!stale_summary.active);
    assert_eq!(stale_summary.presence, Presence::Offline);

    let fresh_visible = state
        .list_agent_runtimes(
            &reader,
            AgentRuntimeListRequest {
                agent_id: None,
                name: Some("fresh".to_string()),
                include_stopped: Some(false),
            },
        )
        .await
        .unwrap();
    assert_eq!(fresh_visible.runtimes.len(), 1);
    assert_eq!(fresh_visible.runtimes[0].runtime_id.0, "s_fresh");
    assert!(fresh_visible.runtimes[0].active);
    assert_eq!(fresh_visible.runtimes[0].presence, Presence::Online);

    let stale_turn = turn_row(&store, "s_stale").await;
    assert_eq!(stale_turn.status, "aborted");
    assert!(stale_turn.finalized_at.is_some());
    let stale_message = message_row(&store, "s_stale").await;
    assert_eq!(stale_message.status, "aborted");
    assert!(stale_message.finalized_at.is_some());

    let fresh_turn = turn_row(&store, "s_fresh").await;
    assert_eq!(fresh_turn.status, "streaming");
    assert_eq!(fresh_turn.finalized_at, None);
    let fresh_message = message_row(&store, "s_fresh").await;
    assert_eq!(fresh_message.status, "streaming");
    assert_eq!(fresh_message.finalized_at, None);
}

#[tokio::test]
async fn boot_registry_reconcile_offlines_transport_owned_rows_without_handles() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let state = AppState::wire(store.clone(), &Config::default());
    state.wait_for_runtime_identity_ready().await.unwrap();

    seed_agent_runtime(
        &store,
        "transport_owned",
        "s_transport_owned",
        "a_transport_owned",
        nexus_common::now(),
    )
    .await;
    seed_agent_runtime_with_harness(
        &store,
        "heartbeat_owned",
        "s_heartbeat_owned",
        "a_heartbeat_owned",
        nexus_common::now(),
        "codex",
        "mcp",
    )
    .await;
    store
        .conn
        .execute(
            "UPDATE sessions SET transport = NULL WHERE session_id = ?1",
            libsql::params!["s_heartbeat_owned"],
        )
        .await
        .unwrap();

    state
        .reconcile_transport_presence_from_registry_once()
        .await
        .unwrap();

    let sessions = Sessions::new(&store);
    let transport_owned = sessions
        .find_by_session_id(&SessionId("s_transport_owned".to_string()))
        .await
        .unwrap()
        .expect("transport-owned session");
    assert_eq!(transport_owned.presence.as_deref(), Some("offline"));
    let heartbeat_owned = sessions
        .find_by_session_id(&SessionId("s_heartbeat_owned".to_string()))
        .await
        .unwrap()
        .expect("heartbeat-owned session");
    assert_eq!(heartbeat_owned.presence.as_deref(), Some("online"));

    let runtimes = AgentRuntimes::new(&store);
    let transport_runtime = runtimes
        .find_by_runtime_id("s_transport_owned")
        .await
        .unwrap()
        .expect("transport-owned runtime");
    assert!(!transport_runtime.active);
    assert_eq!(transport_runtime.presence.as_deref(), Some("offline"));
    assert!(transport_runtime.stopped_at.is_some());
    let heartbeat_runtime = runtimes
        .find_by_runtime_id("s_heartbeat_owned")
        .await
        .unwrap()
        .expect("heartbeat-owned runtime");
    assert!(heartbeat_runtime.active);
    assert_eq!(heartbeat_runtime.presence.as_deref(), Some("online"));
}

#[tokio::test]
async fn mark_session_offline_stops_runtime_and_aborts_open_turn() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let mut config = Config::default();
    config.heartbeat_ttl_ms = 30_000;
    let state = AppState::wire(store.clone(), &config);
    state.wait_for_runtime_identity_ready().await.unwrap();

    seed_agent_runtime(
        &store,
        "doomed",
        "s_doomed",
        "a_doomed",
        nexus_common::now(),
    )
    .await;
    seed_open_agent_turn(&store, "s_doomed", 10, "half-written doomed turn").await;

    state
        .mark_session_offline(&SessionId("s_doomed".to_string()))
        .await
        .unwrap();

    let session = Sessions::new(&store)
        .find_by_session_id(&SessionId("s_doomed".to_string()))
        .await
        .unwrap()
        .expect("session row remains for audit");
    assert_eq!(session.presence.as_deref(), Some("offline"));

    let runtime = AgentRuntimes::new(&store)
        .find_by_runtime_id("s_doomed")
        .await
        .unwrap()
        .expect("runtime row remains for audit");
    assert!(!runtime.active);
    assert_eq!(runtime.presence.as_deref(), Some("offline"));
    assert!(runtime.stopped_at.is_some());

    let turn = turn_row(&store, "s_doomed").await;
    assert_eq!(turn.status, "aborted");
    assert!(turn.finalized_at.is_some());
    let message = message_row(&store, "s_doomed").await;
    assert_eq!(message.status, "aborted");
    assert!(message.finalized_at.is_some());
}

#[tokio::test]
async fn heartbeat_keeper_dead_probe_path_uses_complete_offline_transition() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let mut config = Config::default();
    config.heartbeat_ttl_ms = 30_000;
    let state = AppState::wire(store.clone(), &config);
    state.wait_for_runtime_identity_ready().await.unwrap();

    seed_agent_runtime(
        &store,
        "dead-probe",
        "s_dead_probe",
        "a_dead_probe",
        nexus_common::now(),
    )
    .await;
    seed_open_agent_turn(&store, "s_dead_probe", 10, "half-written dead probe turn").await;

    state
        .handle_dead_harness_probe(&SessionId("s_dead_probe".to_string()))
        .await
        .unwrap();

    let runtime = AgentRuntimes::new(&store)
        .find_by_runtime_id("s_dead_probe")
        .await
        .unwrap()
        .expect("runtime row remains for audit");
    assert!(!runtime.active);

    let turn = turn_row(&store, "s_dead_probe").await;
    assert_eq!(turn.status, "aborted");
}

#[tokio::test]
async fn heartbeat_dead_probe_terminalizes_owned_injecting_delivery() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let state = AppState::wire(store.clone(), &Config::default());
    state.wait_for_runtime_identity_ready().await.unwrap();
    let session = SessionId("s_dead_injecting".to_string());
    let message = MessageId("m_dead_injecting".to_string());

    seed_agent_runtime(
        &store,
        "dead-injecting",
        &session.0,
        "a_dead_injecting",
        nexus_common::now(),
    )
    .await;
    Messages::new(&store)
        .insert(&Message {
            id: message.clone(),
            project: ProjectId(PROJECT.to_string()),
            from: "sender".to_string(),
            scope: Scope::Dm,
            thread: None,
            topic: None,
            body: "crossed the harness boundary".to_string(),
            summary: None,
            provenance: Provenance {
                from: "sender".to_string(),
                kind: Kind::Agent,
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
    let inbox = Inbox::new(&store);
    inbox.enqueue(&message, &session).await.unwrap();
    inbox.mark_notified(&session).await.unwrap();
    assert_eq!(inbox.mark_injecting(&message, &session).await.unwrap(), 1);

    state.handle_dead_harness_probe(&session).await.unwrap();

    let mut rows = store
        .conn
        .query(
            "SELECT state, error_code, error_reason FROM in_flight WHERE message_id = ?1",
            libsql::params![message.0],
        )
        .await
        .unwrap();
    let row = rows
        .next()
        .await
        .unwrap()
        .expect("delivery row remains for audit");
    assert_eq!(row.get::<String>(0).unwrap(), "error");
    assert_eq!(
        row.get::<Option<String>>(1).unwrap().as_deref(),
        Some("delivery_outcome_unknown")
    );
    assert!(row
        .get::<Option<String>>(2)
        .unwrap()
        .as_deref()
        .is_some_and(|reason| reason.contains("harness exited")));
}

#[tokio::test]
async fn heartbeat_keeper_restamps_live_acp_runtime_projection() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let mut config = Config::default();
    config.heartbeat_ttl_ms = 20;
    let state = AppState::wire_with_turn_exec(store.clone(), &config, Arc::new(AcpNoProbeExec));
    state.wait_for_runtime_identity_ready().await.unwrap();
    let session = SessionId("s_acp_projection".to_string());

    state
        .register_and_wake(
            &session,
            "a_acp_projection",
            Some("acp-projection"),
            PROJECT,
            hid("claude"),
            None,
            "ck_acp_projection",
            None,
            "acp",
            None,
        )
        .await
        .unwrap();
    AgentRuntimes::new(&store).stop(&session.0).await.unwrap();

    let runtime = wait_for_runtime(&store, &session.0, |runtime| {
        runtime.active && runtime.presence.as_deref() == Some("online")
    })
    .await;
    assert_eq!(runtime.stopped_at, None);
    assert!(
        runtime.last_heartbeat.is_some(),
        "live ACP keeper must refresh agent_runtimes.last_heartbeat, not only sessions.last_heartbeat"
    );
}

#[tokio::test]
async fn stopped_offline_stable_agent_remains_revivable_by_thread_wake_path() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let mut config = Config::default();
    config.heartbeat_ttl_ms = 30_000;
    let state = AppState::wire(store.clone(), &config);
    state.wait_for_runtime_identity_ready().await.unwrap();

    seed_agent_runtime(
        &store,
        "thread-member",
        "s_thread_member",
        "a_thread_member",
        nexus_common::now(),
    )
    .await;

    state
        .mark_session_offline(&SessionId("s_thread_member".to_string()))
        .await
        .unwrap();

    let revived = state
        .ensure_alive_agent("a_thread_member")
        .await
        .expect("thread wake can still revive a stopped stable-agent runtime");
    assert_eq!(revived, SessionId("s_thread_member".to_string()));
}

#[derive(Default)]
struct AcpNoProbeExec;

#[async_trait::async_trait]
impl AgentTurnExecutionPort for AcpNoProbeExec {
    async fn inject_turn(&self, _recipient: &SessionId, _batch: &NexusBatch) -> PortResult<()> {
        Ok(())
    }

    async fn launch(&self, req: SpawnRequest) -> PortResult<SpawnResponse> {
        Ok(SpawnResponse {
            session_id: req
                .name
                .map(SessionId)
                .unwrap_or_else(|| SessionId("s_acp_projection".to_string())),
        })
    }

    async fn remove(&self, req: RemoveRequest) -> PortResult<RemoveResponse> {
        Ok(RemoveResponse {
            name: Some(req.name),
            status: "removed".to_string(),
        })
    }
}

async fn wait_for_runtime(
    store: &Store,
    runtime_id: &str,
    predicate: impl Fn(&AgentRuntimeRow) -> bool,
) -> AgentRuntimeRow {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(400);
    loop {
        let runtime = AgentRuntimes::new(store)
            .find_by_runtime_id(runtime_id)
            .await
            .unwrap()
            .expect("runtime row");
        if predicate(&runtime) {
            return runtime;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for runtime {runtime_id} to satisfy predicate; last row: {runtime:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn reconcile_final_flushes_stale_claude_transcript_before_stopping_runtime() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let mut config = Config::default();
    config.heartbeat_ttl_ms = 30_000;
    let state = AppState::wire(store.clone(), &config);
    state.wait_for_runtime_identity_ready().await.unwrap();

    seed_agent_runtime(
        &store,
        "stale-claude",
        "s_stale_claude",
        "a_stale_claude",
        1_700_000_000_000 - 60_000,
    )
    .await;
    let transcript_path = temp_dir("stale-claude").join("transcript.jsonl");
    let transcript = r#"{"type":"assistant","text":"stale archive"}"#;
    std::fs::write(&transcript_path, transcript).unwrap();
    ClaudeRuntimeStateRepo::new(&store)
        .upsert_launch(ClaudeRuntimeLaunch {
            runtime_id: SessionId("s_stale_claude".to_string()),
            bridge_dir: temp_dir("bridge"),
            claude_session_id: None,
            launch_cwd: temp_dir("cwd"),
            transcript_path: Some(transcript_path),
            bridge_pid: None,
            hook_pids_json: None,
        })
        .await
        .unwrap();

    state.reconcile_stale_presence_once().await.unwrap();

    let archived = TranscriptArchive::new(&store)
        .find_by_runtime_id("s_stale_claude")
        .await
        .unwrap()
        .expect("stale reconcile final-flushes Claude transcript before stopping runtime");
    assert_eq!(archived.bytes_archived, transcript.len() as i64);
    assert_eq!(archived.last_event.as_deref(), Some("stale_reconcile"));
    assert_eq!(
        std::fs::read_to_string(&archived.archive_path).unwrap(),
        transcript
    );
    cleanup_archive(&archived.archive_path);
}

#[tokio::test]
async fn reconcile_final_flushes_stale_codex_rollout_before_stopping_runtime() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let mut config = Config::default();
    config.heartbeat_ttl_ms = 30_000;
    let state = AppState::wire(store.clone(), &config);
    state.wait_for_runtime_identity_ready().await.unwrap();

    let session_id = SessionId("s_stale_codex".to_string());
    seed_agent_runtime_with_harness(
        &store,
        "stale-codex",
        &session_id.0,
        "a_stale_codex",
        1_700_000_000_000 - 60_000,
        "codex",
        "codex-appserver",
    )
    .await;
    let rollout_path = temp_dir("stale-codex").join("rollout.jsonl");
    let rollout = r#"{"type":"response_item","text":"stale codex archive"}"#;
    std::fs::write(&rollout_path, rollout).unwrap();
    let sock_dir = temp_dir("codex-sock");
    let codex = CodexRuntimeStateRepo::new(&store);
    codex
        .upsert_launch(CodexRuntimeLaunch {
            runtime_id: session_id.clone(),
            codex_thread_id: None,
            codex_home: temp_dir("codex-home"),
            app_server_sock: sock_dir.join("codex.sock"),
            app_server_pid: None,
            mcp_sidecar_pids_json: None,
            app_server_adopted: true,
        })
        .await
        .unwrap();
    codex
        .set_thread(
            &session_id,
            "codex-thread-stale",
            Some(rollout_path.clone()),
        )
        .await
        .unwrap();

    state.reconcile_stale_presence_once().await.unwrap();

    let archived = TranscriptArchive::new(&store)
        .find_by_runtime_id(&session_id.0)
        .await
        .unwrap()
        .expect("stale reconcile final-flushes Codex rollout before stopping runtime");
    assert_eq!(archived.harness, "codex");
    assert_eq!(archived.source_kind, "file");
    assert_eq!(archived.source_path, rollout_path.to_string_lossy());
    assert_eq!(archived.bytes_archived, rollout.len() as i64);
    assert_eq!(archived.last_event.as_deref(), Some("stale_reconcile"));
    assert_eq!(
        std::fs::read_to_string(&archived.archive_path).unwrap(),
        rollout
    );
    cleanup_archive(&archived.archive_path);
}

#[derive(Debug)]
struct MaterializedRow {
    status: String,
    finalized_at: Option<i64>,
}

async fn turn_row(store: &Store, session_id: &str) -> MaterializedRow {
    let sql = format!(
        "SELECT status, finalized_at FROM agent_session_turns WHERE session_id = '{}'",
        session_id
    );
    let mut rows = store.conn.query(&sql, ()).await.unwrap();
    let row = rows.next().await.unwrap().expect("turn row");
    MaterializedRow {
        status: row.get::<String>(0).unwrap(),
        finalized_at: row.get::<Option<i64>>(1).unwrap(),
    }
}

async fn message_row(store: &Store, session_id: &str) -> MaterializedRow {
    let sql = format!(
        "SELECT status, finalized_at FROM agent_session_messages WHERE session_id = '{}'",
        session_id
    );
    let mut rows = store.conn.query(&sql, ()).await.unwrap();
    let row = rows.next().await.unwrap().expect("message row");
    MaterializedRow {
        status: row.get::<String>(0).unwrap(),
        finalized_at: row.get::<Option<i64>>(1).unwrap(),
    }
}

async fn seed_open_agent_turn(store: &Store, session_id: &str, event_id: i64, text: &str) {
    let session = SessionId(session_id.to_string());
    let ts = nexus_common::now();
    let repo = AgentSessionMessages::new(store);
    let turn = repo
        .begin_or_get_open_turn(&session, event_id, ts)
        .await
        .unwrap();
    repo.upsert_message(NewAgentSessionMessage {
        id: format!("msg_{}_0", turn.id),
        session_id: session.0,
        turn_id: turn.id,
        ordinal: 0,
        role: "assistant".to_string(),
        author: None,
        content_json: format!(
            r#"{{"schema":1,"blocks":[{{"type":"text","text":{}}}],"metadata":{{}}}}"#,
            serde_json::to_string(text).unwrap()
        ),
        status: "streaming".to_string(),
        first_stream_event_id: event_id,
        last_stream_event_id: event_id,
        created_at: ts,
        updated_at: ts,
        finalized_at: None,
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn boot_presume_dead_marks_online_agents_offline_regardless_of_heartbeat() {
    // After a daemon restart NOTHING in the store is transport-backed — even a
    // fresh heartbeat predates the boot (the shutdown sweep can lose the write race).
    // The presume-dead pass must flip every liveness-claiming AGENT row offline;
    // humans and already-offline rows stay untouched.
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let state = AppState::wire(store.clone(), &Config::default());
    state.wait_for_runtime_identity_ready().await.unwrap();

    // PTY-backed agent with a FRESH heartbeat.
    seed_agent_runtime(
        &store,
        "phantom",
        "s_phantom",
        "a_phantom",
        nexus_common::now(),
    )
    .await;
    // CLI/MCP peer: no transport label at all.
    Sessions::new(&store)
        .create(NewSession {
            session_id: SessionId("s_cli_peer".to_string()),
            name: Some("cli-peer".to_string()),
            agent: Some("claude".to_string()),
            kind: "agent".to_string(),
            role: None,
            tier: "agent".to_string(),
            harness_session_id: None,
            client_key: Some("ck_cli_peer".to_string()),
            cwd: None,
            project: PROJECT.to_string(),
            transport: None,
        })
        .await
        .unwrap();
    // Human console session: boot reconcile must not touch it.
    Sessions::new(&store)
        .create(NewSession {
            session_id: SessionId("s_human".to_string()),
            name: Some("alex-test".to_string()),
            agent: None,
            kind: "human".to_string(),
            role: None,
            tier: "admin".to_string(),
            harness_session_id: None,
            client_key: Some("ck_human".to_string()),
            cwd: None,
            project: PROJECT.to_string(),
            transport: None,
        })
        .await
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE sessions SET presence = 'online', last_heartbeat = ?1",
            libsql::params![nexus_common::now()],
        )
        .await
        .unwrap();

    state.reconcile_boot_presume_dead_once().await.unwrap();

    let sessions = Sessions::new(&store);
    for id in ["s_phantom", "s_cli_peer"] {
        let row = sessions
            .find_by_session_id(&SessionId(id.to_string()))
            .await
            .unwrap()
            .expect("agent session");
        assert_eq!(
            row.presence.as_deref(),
            Some("offline"),
            "{id} must be presumed dead at boot"
        );
    }
    let human = sessions
        .find_by_session_id(&SessionId("s_human".to_string()))
        .await
        .unwrap()
        .expect("human session");
    assert_eq!(
        human.presence.as_deref(),
        Some("online"),
        "human console sessions are not the daemon's to presume dead"
    );

    let runtime = AgentRuntimes::new(&store)
        .find_by_runtime_id("s_phantom")
        .await
        .unwrap()
        .expect("phantom runtime");
    assert!(!runtime.active, "presumed-dead runtime row must be stopped");
}

async fn seed_agent_runtime(
    store: &Store,
    name: &str,
    session_id: &str,
    agent_id: &str,
    heartbeat: i64,
) {
    seed_agent_runtime_with_harness(
        store, name, session_id, agent_id, heartbeat, "claude", "pty",
    )
    .await;
}

async fn seed_orphan_runtime(store: &Store, name: &str, runtime_id: &str, agent_id: &str) {
    Agents::new(store)
        .create(NewAgent {
            agent_id: agent_id.to_string(),
            project: PROJECT.to_string(),
            name: Some(name.to_string()),
            default_harness: Some("claude".to_string()),
            role: None,
            tier: Some("agent".to_string()),
            owner: None,
        })
        .await
        .unwrap();
    AgentRuntimes::new(store)
        .create(NewAgentRuntime {
            runtime_id: runtime_id.to_string(),
            agent_id: agent_id.to_string(),
            harness: "claude".to_string(),
            cwd: None,
            transport: Some("pty".to_string()),
            presence: Some("online".to_string()),
            active: true,
        })
        .await
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE agent_runtimes SET last_heartbeat = ?2, started_at = ?2 WHERE runtime_id = ?1",
            (runtime_id.to_string(), 1_700_000_000_000_i64 - 60_000),
        )
        .await
        .unwrap();
}

async fn seed_agent_runtime_with_harness(
    store: &Store,
    name: &str,
    session_id: &str,
    agent_id: &str,
    heartbeat: i64,
    harness: &str,
    transport: &str,
) {
    Agents::new(store)
        .create(NewAgent {
            agent_id: agent_id.to_string(),
            project: PROJECT.to_string(),
            name: Some(name.to_string()),
            default_harness: Some(harness.to_string()),
            role: None,
            tier: Some("agent".to_string()),
            owner: None,
        })
        .await
        .unwrap();
    AgentRuntimes::new(store)
        .create(NewAgentRuntime {
            runtime_id: session_id.to_string(),
            agent_id: agent_id.to_string(),
            harness: harness.to_string(),
            cwd: None,
            transport: Some(transport.to_string()),
            presence: Some("online".to_string()),
            active: true,
        })
        .await
        .unwrap();
    Sessions::new(store)
        .create(NewSession {
            session_id: SessionId(session_id.to_string()),
            name: Some(name.to_string()),
            agent: Some(harness.to_string()),
            kind: "agent".to_string(),
            role: None,
            tier: "agent".to_string(),
            harness_session_id: None,
            client_key: Some(format!("ck_{session_id}")),
            cwd: None,
            project: PROJECT.to_string(),
            transport: Some(transport.to_string()),
        })
        .await
        .unwrap();
    Sessions::new(store)
        .set_agent_id(&SessionId(session_id.to_string()), agent_id)
        .await
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE sessions SET presence = 'online', last_heartbeat = ?2 WHERE session_id = ?1",
            (session_id.to_string(), heartbeat),
        )
        .await
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE agent_runtimes SET presence = 'online', last_heartbeat = ?2 WHERE runtime_id = ?1",
            (session_id.to_string(), heartbeat),
        )
        .await
        .unwrap();
}

fn admin_caller() -> Caller {
    Caller {
        agent_id: None,
        session: SessionId("s_ws2_admin".to_string()),
        name: "ws2-admin".to_string(),
        project: PROJECT.to_string(),
        tier: Tier::Admin,
        locality: Default::default(),
        access: None,
        principal_id: None,
    }
}

fn temp_dir(label: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("nexus-presence-reconcile-{label}-{nanos}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn cleanup_archive(path: &str) {
    let path = std::path::PathBuf::from(path);
    let _ = std::fs::remove_file(&path);
    if let Some(parent) = path.parent() {
        let _ = std::fs::remove_dir_all(parent);
    }
}

/// A validated [`nexus_contracts::HarnessId`] from a literal (panics on invalid — test-only).
fn hid(s: &str) -> nexus_contracts::HarnessId {
    nexus_contracts::HarnessId::new(s).expect("valid harness id literal")
}
