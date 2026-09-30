use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use nexus::daemon::opencode_native_forwarder::{
    forward_once, OpenCodeRuntimeLaunch, OpenCodeRuntimeStateRepo,
};
use nexus::daemon::AppState;
use nexus_common::Config;
use nexus_contracts::{AgentUpdateKind, EventSink, SessionId, WsEvent};
use nexus_store::repos::{
    AgentRuntimes, AgentSessionMessages, Agents, NativeThreadBindings, NewAgent, NewAgentRuntime,
    NewSession, Sessions, StreamEvents,
};
use nexus_store::Store;
use serde_json::json;

#[derive(Clone, Default)]
struct CaptureSink {
    events: Arc<Mutex<Vec<WsEvent>>>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_sidecar_schema_checks_preserve_lookup_and_bad_schema_errors() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    let bad_store = Arc::new(Store::open(":memory:").await.unwrap());
    bad_store
        .conn
        .execute(
            "CREATE VIEW opencode_runtime_state AS SELECT 1 AS runtime_id",
            (),
        )
        .await
        .unwrap();
    let barrier = Arc::new(tokio::sync::Barrier::new(4));
    let mut tasks = Vec::new();
    for lane in 0..4 {
        let store = store.clone();
        let bad_store = bad_store.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            for _ in 0..200 {
                let repo = OpenCodeRuntimeStateRepo::new(&store);
                repo.ensure_schema().await.unwrap();
                assert!(repo
                    .find_by_runtime_id(&SessionId(format!("missing_{lane}")))
                    .await
                    .unwrap()
                    .is_none());
                let error = OpenCodeRuntimeStateRepo::new(&bad_store)
                    .ensure_schema()
                    .await
                    .unwrap_err();
                assert!(
                    error.to_string().contains("view") || error.to_string().contains("not a table"),
                    "bad schema must retain its own error: {error}"
                );
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
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
        .join(format!("nexus-opencode-forwarder-{label}-{nanos}.db"))
        .to_string_lossy()
        .into_owned()
}

#[tokio::test]
async fn plugin_runtime_db_poll_retains_tool_metadata_without_second_display_authority() {
    use nexus::daemon::opencode_native_forwarder::{
        forward_once_with_tool_observations, OpenCodeToolObservationSink,
    };
    #[derive(Default)]
    struct Tools(Mutex<Vec<nexus_transcript::ToolCallObservation>>);
    impl OpenCodeToolObservationSink for Tools {
        fn publish_tool_call(
            &self,
            _: &SessionId,
            observation: nexus_transcript::ToolCallObservation,
        ) {
            self.0.lock().unwrap().push(observation);
        }
    }
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let session = SessionId("s_plugin_display_owner".into());
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("native.db");
    seed_opencode_db(db_path.to_str().unwrap()).await;
    seed_active_opencode_runtime(store.clone(), session.clone(), db_path.clone()).await;
    store
        .conn
        .execute(
            "UPDATE agent_runtimes SET transport = 'opencode-plugin' WHERE runtime_id = ?1",
            libsql::params![session.0.clone()],
        )
        .await
        .unwrap();
    let native = Store::open(db_path.to_str().unwrap()).await.unwrap();
    insert_opencode_event(
        &native,
        6,
        "message.part.updated.1",
        json!({"sessionID":"ses_native",
        "part":{"id":"part_tool", "callID":"call_native", "messageID":"msg_agent", "type":"tool",
        "tool":"bash", "state":{"status":"error", "error":"native failure"}}}),
    )
    .await;
    let events = CaptureSink::default();
    let tools = Arc::new(Tools::default());
    let stats = forward_once_with_tool_observations(
        store.clone(),
        session.clone(),
        Arc::new(events.clone()),
        Some(tools.clone()),
    )
    .await
    .unwrap();
    assert_eq!(tools.0.lock().unwrap().len(), 1);
    assert!(!tools.0.lock().unwrap()[0].ok);
    assert_eq!(
        stats.total_events(),
        0,
        "plugin already owns visible text/tools/completion"
    );
    assert!(events.events.lock().unwrap().is_empty());
    assert_eq!(
        OpenCodeRuntimeStateRepo::new(&store)
            .find_by_runtime_id(&session)
            .await
            .unwrap()
            .unwrap()
            .event_seq,
        6
    );
    forward_once_with_tool_observations(
        store,
        session,
        Arc::new(events.clone()),
        Some(tools.clone()),
    )
    .await
    .unwrap();
    assert_eq!(
        tools.0.lock().unwrap().len(),
        1,
        "metadata cursor still advances"
    );
}

#[tokio::test]
async fn opencode_forwarder_reads_event_rows_and_persists_seq_cursor() {
    let nexus_store = Arc::new(Store::open(":memory:").await.unwrap());
    nexus_store.migrate().await.unwrap();
    let session = SessionId("s_opencode_forwarder".into());
    let db_path = temp_db_path("events");
    seed_opencode_db(&db_path).await;
    OpenCodeRuntimeStateRepo::new(&nexus_store)
        .upsert_launch(OpenCodeRuntimeLaunch {
            runtime_id: session.clone(),
            opencode_db_path: db_path.clone().into(),
            opencode_session_id: None,
            launch_cwd: "/tmp/nexus-opencode".into(),
            plugin_bridge_pid: Some(4321),
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
    assert_eq!(stats.turn_end_events, 1);
    let events = sink.events.lock().unwrap().clone();
    assert_eq!(events.len(), 3);
    assert_agent_update(&events[0], AgentUpdateKind::UserInput, "hello");
    assert_agent_update(&events[1], AgentUpdateKind::Text, "hi back");
    assert!(matches!(
        &events[2],
        WsEvent::AgentUpdate {
            kind: AgentUpdateKind::TurnEnd,
            ..
        }
    ));

    let state = OpenCodeRuntimeStateRepo::new(&nexus_store)
        .find_by_runtime_id(&session)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state.opencode_session_id.as_deref(), Some("ses_native"));
    assert_eq!(state.plugin_bridge_pid, Some(4321));
    assert_eq!(state.viewer_backend, "pty");
    assert_eq!(state.event_seq, 5);

    OpenCodeRuntimeStateRepo::new(&nexus_store)
        .set_plugin_bridge_pid(&session, Some(8765))
        .await
        .unwrap();
    let refreshed = OpenCodeRuntimeStateRepo::new(&nexus_store)
        .find_by_runtime_id(&session)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(refreshed.plugin_bridge_pid, Some(8765));

    sink.events.lock().unwrap().clear();
    let stats = forward_once(nexus_store, session, Arc::new(sink.clone()))
        .await
        .unwrap();
    assert_eq!(stats.total_events(), 0);
    assert!(sink.events.lock().unwrap().is_empty());
}

#[tokio::test]
async fn opencode_forwarder_persists_parser_state_between_polls() {
    let nexus_store = Arc::new(Store::open(":memory:").await.unwrap());
    nexus_store.migrate().await.unwrap();
    let session = SessionId("s_opencode_forwarder_state".into());
    let db_path = temp_db_path("state");
    seed_empty_opencode_db(&db_path).await;
    let db = Store::open(&db_path).await.unwrap();
    insert_opencode_event(
        &db,
        1,
        "message.updated.1",
        json!({ "sessionID": "ses_native", "info": { "id": "msg_agent", "role": "assistant" } }),
    )
    .await;
    insert_opencode_event(
        &db,
        2,
        "message.part.updated.1",
        json!({ "sessionID": "ses_native", "part": { "id": "part_agent", "messageID": "msg_agent", "type": "text", "text": "hel" } }),
    )
    .await;
    OpenCodeRuntimeStateRepo::new(&nexus_store)
        .upsert_launch(OpenCodeRuntimeLaunch {
            runtime_id: session.clone(),
            opencode_db_path: db_path.clone().into(),
            opencode_session_id: Some("ses_native".to_string()),
            launch_cwd: "/tmp/nexus-opencode".into(),
            plugin_bridge_pid: None,
            viewer_backend: "tmux".into(),
        })
        .await
        .unwrap();
    let sink = CaptureSink::default();

    let stats = forward_once(nexus_store.clone(), session.clone(), Arc::new(sink.clone()))
        .await
        .unwrap();
    assert_eq!(stats.text_events, 1);
    assert_agent_update(
        sink.events.lock().unwrap().last().unwrap(),
        AgentUpdateKind::Text,
        "hel",
    );

    sink.events.lock().unwrap().clear();
    insert_opencode_event(
        &db,
        3,
        "message.part.updated.1",
        json!({ "sessionID": "ses_native", "part": { "id": "part_agent", "messageID": "msg_agent", "type": "text", "text": "hello" } }),
    )
    .await;

    let stats = forward_once(nexus_store, session, Arc::new(sink.clone()))
        .await
        .unwrap();

    assert_eq!(stats.text_events, 1);
    let events = sink.events.lock().unwrap().clone();
    assert_eq!(events.len(), 1);
    assert_agent_update(&events[0], AgentUpdateKind::Text, "lo");
}

#[tokio::test]
async fn boot_adoption_materializes_active_opencode_event_rows() {
    let nexus_store = Arc::new(Store::open(":memory:").await.unwrap());
    nexus_store.migrate().await.unwrap();
    let session = SessionId("s_opencode_adopt".into());
    let db_path = temp_db_path("adopt");
    seed_opencode_db(&db_path).await;
    seed_active_opencode_runtime(nexus_store.clone(), session.clone(), db_path.into()).await;
    let state = AppState::wire_pty(nexus_store.clone(), &Config::default());

    state.adopt_active_opencode_forwarders().await;

    let rows = wait_for_agent_session_messages(nexus_store.clone(), &session).await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].role, "user");
    assert!(rows[0].content_json.contains("hello"));
    assert_eq!(rows[1].role, "assistant");
    assert_eq!(rows[1].status, "final");
    assert!(rows[1].content_json.contains("hi back"));
    assert!(
        StreamEvents::new(&nexus_store)
            .since(&session, 0)
            .await
            .unwrap()
            .is_empty(),
        "turn_end should evict volatile OpenCode rows after final materialization"
    );
    let binding = NativeThreadBindings::new(&nexus_store)
        .find("opencode", "ses_native")
        .await
        .unwrap()
        .expect("native binding");
    assert_eq!(binding.agent_id, "agent_opencode_loop");
    assert_eq!(binding.last_runtime_id.as_deref(), Some(session.0.as_str()));
}

async fn seed_active_opencode_runtime(
    store: Arc<Store>,
    session: SessionId,
    db_path: std::path::PathBuf,
) {
    let agent_id = "agent_opencode_loop".to_string();
    Agents::new(&store)
        .create(NewAgent {
            agent_id: agent_id.clone(),
            project: "default".to_string(),
            name: Some("opencode-loop".to_string()),
            default_harness: Some("opencode".to_string()),
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
            harness: "opencode".to_string(),
            cwd: Some("/tmp/nexus-opencode".to_string()),
            transport: Some("pty".to_string()),
            presence: Some("online".to_string()),
            active: true,
        })
        .await
        .unwrap();
    Sessions::new(&store)
        .create(NewSession {
            session_id: session.clone(),
            name: Some("opencode-loop".to_string()),
            agent: Some("opencode".to_string()),
            kind: "agent".to_string(),
            role: None,
            tier: "agent".to_string(),
            harness_session_id: None,
            client_key: Some(session.0.clone()),
            cwd: Some("/tmp/nexus-opencode".to_string()),
            project: "default".to_string(),
            transport: Some("pty".to_string()),
        })
        .await
        .unwrap();
    Sessions::new(&store)
        .set_agent_id(&session, &agent_id)
        .await
        .unwrap();
    OpenCodeRuntimeStateRepo::new(&store)
        .upsert_launch(OpenCodeRuntimeLaunch {
            runtime_id: session,
            opencode_db_path: db_path,
            opencode_session_id: None,
            launch_cwd: "/tmp/nexus-opencode".into(),
            plugin_bridge_pid: None,
            viewer_backend: "tmux".into(),
        })
        .await
        .unwrap();
}

async fn seed_opencode_db(path: &str) {
    seed_empty_opencode_db(path).await;
    let db = Store::open(path).await.unwrap();
    for (seq, event_type, data) in [
        (
            1,
            "message.updated.1",
            json!({ "sessionID": "ses_native", "info": { "id": "msg_user", "role": "user" } }),
        ),
        (
            2,
            "message.part.updated.1",
            json!({ "sessionID": "ses_native", "part": { "id": "part_user", "messageID": "msg_user", "type": "text", "text": "hello" } }),
        ),
        (
            3,
            "message.updated.1",
            json!({ "sessionID": "ses_native", "info": { "id": "msg_agent", "role": "assistant" } }),
        ),
        (
            4,
            "message.part.updated.1",
            json!({ "sessionID": "ses_native", "part": { "id": "part_agent", "messageID": "msg_agent", "type": "text", "text": "hi back" } }),
        ),
        (
            5,
            "message.part.updated.1",
            json!({ "sessionID": "ses_native", "part": { "id": "part_done", "messageID": "msg_agent", "type": "step-finish", "reason": "stop" } }),
        ),
    ] {
        insert_opencode_event(&db, seq, event_type, data).await;
    }
}

async fn seed_empty_opencode_db(path: &str) {
    let db = Store::open(path).await.unwrap();
    db.conn
        .execute_batch(
            "
            CREATE TABLE event (
                id TEXT PRIMARY KEY,
                aggregate_id TEXT NOT NULL,
                seq INTEGER NOT NULL,
                type TEXT NOT NULL,
                data TEXT NOT NULL
            );
            CREATE TABLE session (
                id TEXT PRIMARY KEY,
                directory TEXT NOT NULL,
                time_created INTEGER NOT NULL,
                time_updated INTEGER NOT NULL
            );
            ",
        )
        .await
        .unwrap();
    db.conn
        .execute(
            "INSERT INTO session (id, directory, time_created, time_updated) VALUES (?1, ?2, ?3, ?4)",
            libsql::params!["ses_native", "/tmp/nexus-opencode", 1, 2],
        )
        .await
        .unwrap();
}

async fn insert_opencode_event(db: &Store, seq: i64, event_type: &str, data: serde_json::Value) {
    db.conn
        .execute(
            "INSERT INTO event (id, aggregate_id, seq, type, data) VALUES (?1, ?2, ?3, ?4, ?5)",
            libsql::params![
                format!("evt_{seq}"),
                "ses_native",
                seq,
                event_type,
                data.to_string()
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
            "timed out waiting for OpenCode native forwarder materialized rows"
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
