use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use nexus::daemon::hermes_native_forwarder::{
    forward_once, HermesRuntimeLaunch, HermesRuntimeStateRepo,
};
use nexus::daemon::AppState;
use nexus_common::Config;
use nexus_contracts::{AgentUpdateKind, EventSink, SessionId, WsEvent};
use nexus_store::repos::{
    AgentRuntimes, AgentSessionMessages, Agents, NativeThreadBindings, NewAgent, NewAgentRuntime,
    NewSession, Sessions, StreamEvents,
};
use nexus_store::Store;

#[derive(Clone, Default)]
struct CaptureSink {
    events: Arc<Mutex<Vec<WsEvent>>>,
}

#[async_trait]
impl EventSink for CaptureSink {
    async fn emit(&self, event: WsEvent) {
        self.events.lock().unwrap().push(event);
    }
}

fn temp_db_path(label: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir()
        .join(format!("nexus-hermes-forwarder-{label}-{nanos}.db"))
        .to_string_lossy()
        .into_owned()
}

#[tokio::test]
async fn hermes_forwarder_reads_message_rows_and_persists_cursor() {
    let nexus_store = Arc::new(Store::open(":memory:").await.unwrap());
    nexus_store.migrate().await.unwrap();
    let session = SessionId("s_hermes_forwarder".into());
    let db_path = temp_db_path("messages");
    seed_hermes_db(&db_path).await;
    HermesRuntimeStateRepo::new(&nexus_store)
        .upsert_launch(HermesRuntimeLaunch {
            runtime_id: session.clone(),
            hermes_db_path: db_path.clone().into(),
            hermes_session_id: None,
            launch_cwd: "/tmp/nexus-hermes".into(),
            acp_child_pid: Some(2468),
            viewer_backend: "pty".into(),
        })
        .await
        .unwrap();
    let sink = CaptureSink::default();

    let stats = forward_once(nexus_store.clone(), session.clone(), Arc::new(sink.clone()))
        .await
        .unwrap();

    assert_eq!(stats.user_input_events, 1);
    assert_eq!(stats.text_events, 1);
    assert_eq!(stats.tool_call_events, 2);
    assert_eq!(stats.turn_end_events, 1);
    let events = sink.events.lock().unwrap().clone();
    assert_eq!(events.len(), 5);
    assert_agent_update(&events[0], AgentUpdateKind::UserInput, "hello");
    assert_eq!(agent_update_data(&events[1])["id"], "call_1");
    assert_eq!(agent_update_data(&events[2])["status"], "completed");
    assert_agent_update(&events[3], AgentUpdateKind::Text, "hi back");
    assert!(matches!(
        &events[4],
        WsEvent::AgentUpdate {
            kind: AgentUpdateKind::TurnEnd,
            ..
        }
    ));

    let state = HermesRuntimeStateRepo::new(&nexus_store)
        .find_by_runtime_id(&session)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state.hermes_session_id.as_deref(), Some("ses_native"));
    assert_eq!(state.acp_child_pid, Some(2468));
    assert_eq!(state.viewer_backend, "pty");
    assert_eq!(state.message_id, 4);

    HermesRuntimeStateRepo::new(&nexus_store)
        .set_acp_child_pid(&session, Some(8642))
        .await
        .unwrap();
    let refreshed = HermesRuntimeStateRepo::new(&nexus_store)
        .find_by_runtime_id(&session)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(refreshed.acp_child_pid, Some(8642));

    sink.events.lock().unwrap().clear();
    let stats = forward_once(nexus_store, session, Arc::new(sink.clone()))
        .await
        .unwrap();
    assert_eq!(stats.total_events(), 0);
    assert!(sink.events.lock().unwrap().is_empty());
}

#[tokio::test]
async fn boot_adoption_materializes_active_hermes_message_rows() {
    let nexus_store = Arc::new(Store::open(":memory:").await.unwrap());
    nexus_store.migrate().await.unwrap();
    let session = SessionId("s_hermes_adopt".into());
    let db_path = temp_db_path("adopt");
    seed_hermes_db(&db_path).await;
    seed_active_hermes_runtime(nexus_store.clone(), session.clone(), db_path.into()).await;
    let state = AppState::wire_pty(nexus_store.clone(), &Config::default());

    state.adopt_active_hermes_forwarders().await;

    let rows = wait_for_agent_session_messages(nexus_store.clone(), &session).await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].role, "user");
    assert!(rows[0].content_json.contains("hello"));
    assert_eq!(rows[1].role, "assistant");
    assert_eq!(rows[1].status, "final");
    assert!(rows[1].content_json.contains("tool_call"));
    assert!(rows[1].content_json.contains("hi back"));
    assert!(
        StreamEvents::new(&nexus_store)
            .since(&session, 0)
            .await
            .unwrap()
            .is_empty(),
        "turn_end should evict volatile Hermes rows after final materialization"
    );
    let binding = NativeThreadBindings::new(&nexus_store)
        .find("hermes", "ses_native")
        .await
        .unwrap()
        .expect("native binding");
    assert_eq!(binding.agent_id, "agent_hermes_loop");
    assert_eq!(binding.last_runtime_id.as_deref(), Some(session.0.as_str()));
}

async fn seed_active_hermes_runtime(
    store: Arc<Store>,
    session: SessionId,
    db_path: std::path::PathBuf,
) {
    let agent_id = "agent_hermes_loop".to_string();
    Agents::new(&store)
        .create(NewAgent {
            agent_id: agent_id.clone(),
            project: "default".to_string(),
            name: Some("hermes-loop".to_string()),
            default_harness: Some("hermes".to_string()),
            role: None,
            tier: Some("agent".to_string()),
            owner: None,
        })
        .await
        .unwrap();
    AgentRuntimes::new(&store)
        .create(NewAgentRuntime {
            runtime_id: session.0.clone(),
            agent_id: agent_id.clone(),
            harness: "hermes".to_string(),
            cwd: Some("/tmp/nexus-hermes".to_string()),
            transport: Some("pty".to_string()),
            presence: Some("online".to_string()),
            active: true,
        })
        .await
        .unwrap();
    Sessions::new(&store)
        .create(NewSession {
            session_id: session.clone(),
            name: Some("hermes-loop".to_string()),
            agent: Some("hermes".to_string()),
            kind: "agent".to_string(),
            role: None,
            tier: "agent".to_string(),
            harness_session_id: None,
            client_key: Some(session.0.clone()),
            cwd: Some("/tmp/nexus-hermes".to_string()),
            project: "default".to_string(),
            transport: Some("pty".to_string()),
        })
        .await
        .unwrap();
    Sessions::new(&store)
        .set_agent_id(&session, &agent_id)
        .await
        .unwrap();
    HermesRuntimeStateRepo::new(&store)
        .upsert_launch(HermesRuntimeLaunch {
            runtime_id: session,
            hermes_db_path: db_path,
            hermes_session_id: None,
            launch_cwd: "/tmp/nexus-hermes".into(),
            acp_child_pid: None,
            viewer_backend: "pty".into(),
        })
        .await
        .unwrap();
}

async fn seed_hermes_db(path: &str) {
    seed_empty_hermes_db(path).await;
    let db = Store::open(path).await.unwrap();
    insert_hermes_message(&db, 1, "user", "hello", None, None, None, None).await;
    insert_hermes_message(
        &db,
        2,
        "assistant",
        "",
        Some(r#"[{"id":"call_1","type":"function","function":{"name":"terminal","arguments":"{\"command\":\"pwd\"}"}}]"#),
        None,
        None,
        Some("tool_calls"),
    )
    .await;
    insert_hermes_message(
        &db,
        3,
        "tool",
        r#"{"output":"/tmp\n","exit_code":0}"#,
        None,
        Some("call_1"),
        Some("terminal"),
        None,
    )
    .await;
    insert_hermes_message(
        &db,
        4,
        "assistant",
        "hi back",
        None,
        None,
        None,
        Some("stop"),
    )
    .await;
}

async fn seed_empty_hermes_db(path: &str) {
    let db = Store::open(path).await.unwrap();
    db.conn
        .execute_batch(
            "
            CREATE TABLE sessions (
                id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                model_config TEXT,
                started_at REAL NOT NULL,
                ended_at REAL,
                cwd TEXT
            );
            CREATE TABLE messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL REFERENCES sessions(id),
                role TEXT NOT NULL,
                content TEXT,
                tool_call_id TEXT,
                tool_calls TEXT,
                tool_name TEXT,
                timestamp REAL NOT NULL,
                finish_reason TEXT,
                active INTEGER NOT NULL DEFAULT 1
            );
            ",
        )
        .await
        .unwrap();
    db.conn
        .execute(
            "INSERT INTO sessions (id, source, model_config, started_at, cwd)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            libsql::params![
                "ses_native",
                "cli",
                r#"{"cwd":"/tmp/nexus-hermes"}"#,
                1.0,
                "/tmp/nexus-hermes"
            ],
        )
        .await
        .unwrap();
}

async fn insert_hermes_message(
    db: &Store,
    id: i64,
    role: &str,
    content: &str,
    tool_calls: Option<&str>,
    tool_call_id: Option<&str>,
    tool_name: Option<&str>,
    finish_reason: Option<&str>,
) {
    db.conn
        .execute(
            "INSERT INTO messages (
                id, session_id, role, content, tool_calls, tool_call_id,
                tool_name, timestamp, finish_reason, active
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 1)",
            libsql::params![
                id,
                "ses_native",
                role,
                content,
                tool_calls,
                tool_call_id,
                tool_name,
                id as f64,
                finish_reason
            ],
        )
        .await
        .unwrap();
}

async fn wait_for_agent_session_messages(
    store: Arc<Store>,
    session: &SessionId,
) -> Vec<nexus_store::repos::AgentSessionMessageRow> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        let rows = AgentSessionMessages::new(&store)
            .messages_for_session(session, 10)
            .await
            .unwrap();
        if !rows.is_empty() {
            return rows;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for Hermes native forwarder materialized rows"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

fn assert_agent_update(event: &WsEvent, expected_kind: AgentUpdateKind, expected_text: &str) {
    match event {
        WsEvent::AgentUpdate { kind, data, .. } => {
            assert_eq!(*kind, expected_kind);
            assert_eq!(data["text"], expected_text);
        }
        other => panic!("expected agent.update, got {other:?}"),
    }
}

fn agent_update_data(event: &WsEvent) -> &serde_json::Value {
    match event {
        WsEvent::AgentUpdate { data, .. } => data,
        other => panic!("expected agent.update, got {other:?}"),
    }
}
