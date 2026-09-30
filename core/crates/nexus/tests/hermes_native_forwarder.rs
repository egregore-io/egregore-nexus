use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use nexus::daemon::hermes_native_forwarder::{
    forward_once, HermesChildStreamsRepo, HermesRuntimeLaunch, HermesRuntimeStateRepo,
};
use nexus::daemon::AppState;
use nexus_common::{now, Config};
use nexus_contracts::{AgentUpdateKind, ChildResolution, EventSink, SessionId, WsEvent};
use nexus_store::repos::{
    AgentRuntimes, AgentSessionMessages, Agents, ChildStreamEvents, DaemonState, LaneFilter,
    NativeThreadBindings, NewAgent, NewAgentRuntime, NewSession, Sessions, StreamEvents,
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

/// One seeded message row: id, session, role, content, tool_calls, tool_call_id, tool_name,
/// finish_reason.
type SeedRow<'a> = (
    i64,
    &'a str,
    &'a str,
    &'a str,
    Option<&'a str>,
    Option<&'a str>,
    Option<&'a str>,
    Option<&'a str>,
);

/// A store shaped like a Hermes release that records native ancestry: a root, a child of it,
/// a nested child, and a child of some other root, all sharing the launch cwd and each newer
/// than the root, with one message set per session.
async fn seed_hermes_db_with_children(path: &str) {
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
                cwd TEXT,
                parent_session_id TEXT
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
            INSERT INTO sessions (id, source, model_config, started_at, cwd, parent_session_id) VALUES
                ('ses_native', 'cli', '{\"cwd\":\"/tmp/nexus-hermes\"}', 1.0, '/tmp/nexus-hermes', NULL),
                ('ses_child', 'cli', NULL, 2.0, '/tmp/nexus-hermes', 'ses_native'),
                ('ses_nested', 'cli', NULL, 3.0, '/tmp/nexus-hermes', 'ses_child'),
                ('ses_stranger', 'cli', NULL, 4.0, '/tmp/nexus-hermes', 'ses_other_root');
            ",
        )
        .await
        .unwrap();
    let rows: [SeedRow; 8] = [
        (1, "ses_native", "user", "hello", None, None, None, None),
        (
            2,
            "ses_native",
            "assistant",
            "",
            Some(
                r#"[{"id":"call_1","type":"function","function":{"name":"terminal","arguments":"{\"command\":\"pwd\"}"}}]"#,
            ),
            None,
            None,
            Some("tool_calls"),
        ),
        (
            3,
            "ses_native",
            "tool",
            r#"{"output":"/tmp\n","exit_code":0}"#,
            None,
            Some("call_1"),
            Some("terminal"),
            None,
        ),
        (
            4,
            "ses_native",
            "assistant",
            "hi back",
            None,
            None,
            None,
            Some("stop"),
        ),
        (
            5,
            "ses_child",
            "user",
            "child hello",
            None,
            None,
            None,
            None,
        ),
        (
            6,
            "ses_child",
            "assistant",
            "child hi",
            None,
            None,
            None,
            Some("stop"),
        ),
        (
            7,
            "ses_nested",
            "assistant",
            "nested hi",
            None,
            None,
            None,
            Some("stop"),
        ),
        (
            8,
            "ses_stranger",
            "assistant",
            "stranger",
            None,
            None,
            None,
            Some("stop"),
        ),
    ];
    for (id, session, role, content, tool_calls, tool_call_id, tool_name, finish) in rows {
        insert_hermes_message_for(
            &db,
            session,
            id,
            role,
            content,
            tool_calls,
            tool_call_id,
            tool_name,
            finish,
        )
        .await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn insert_hermes_message_for(
    db: &Store,
    session: &str,
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
                session,
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

/// One child lane event as captured: the child, the kind, the source ref and the data.
type ChildEvent = (
    nexus_contracts::ChildStream,
    AgentUpdateKind,
    String,
    serde_json::Value,
);

fn child_events(events: &[WsEvent]) -> Vec<ChildEvent> {
    events
        .iter()
        .filter_map(|e| match e {
            WsEvent::ChildAgentUpdate {
                child,
                kind,
                source_ref,
                data,
                ..
            } => Some((child.clone(), *kind, source_ref.clone(), data.clone())),
            _ => None,
        })
        .collect()
}

/// Root discovery never binds a child even when every child is newer and shares the cwd; the
/// root's rows stay the parent lane; descendants stream into the child lane with native
/// lineage (parent, depth) and a child of another root never appears.
#[tokio::test]
async fn hermes_children_stream_into_the_child_lane_with_native_lineage() {
    let nexus_store = Arc::new(Store::open(":memory:").await.unwrap());
    nexus_store.migrate().await.unwrap();
    DaemonState::new(&nexus_store)
        .set_boot_epoch("boot_a", now())
        .await
        .unwrap();
    let session = SessionId("s_hermes_children".into());
    let db_path = temp_db_path("children");
    seed_hermes_db_with_children(&db_path).await;
    HermesRuntimeStateRepo::new(&nexus_store)
        .upsert_launch(HermesRuntimeLaunch {
            runtime_id: session.clone(),
            hermes_db_path: db_path.clone().into(),
            hermes_session_id: None,
            launch_cwd: "/tmp/nexus-hermes".into(),
            acp_child_pid: None,
            viewer_backend: "pty".into(),
        })
        .await
        .unwrap();
    let sink = CaptureSink::default();

    let stats = forward_once(nexus_store.clone(), session.clone(), Arc::new(sink.clone()))
        .await
        .unwrap();

    let state = HermesRuntimeStateRepo::new(&nexus_store)
        .find_by_runtime_id(&session)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        state.hermes_session_id.as_deref(),
        Some("ses_native"),
        "the root is bound although every child is newer"
    );
    assert_eq!(stats.user_input_events, 1);
    assert_eq!(stats.text_events, 1);
    assert_eq!(stats.tool_call_events, 2);
    assert_eq!(stats.turn_end_events, 1);
    assert_eq!(stats.child_errors, 0);
    assert_eq!(stats.child_events, 5, "{stats:?}");

    let events = sink.events.lock().unwrap().clone();
    let parent: Vec<&WsEvent> = events
        .iter()
        .filter(|e| matches!(e, WsEvent::AgentUpdate { .. }))
        .collect();
    assert_eq!(parent.len(), 5);
    assert_agent_update(parent[0], AgentUpdateKind::UserInput, "hello");
    assert_agent_update(parent[3], AgentUpdateKind::Text, "hi back");
    for event in &parent {
        if let WsEvent::AgentUpdate { data, .. } = event {
            let text = data.to_string();
            assert!(
                !text.contains("child") && !text.contains("nested") && !text.contains("stranger"),
                "child content in the parent lane: {text}"
            );
        }
    }

    let children = child_events(&events);
    let child: Vec<_> = children
        .iter()
        .filter(|(c, ..)| c.id.as_deref() == Some("ses_child"))
        .collect();
    assert_eq!(
        child.iter().map(|(_, k, ..)| *k).collect::<Vec<_>>(),
        vec![
            AgentUpdateKind::UserInput,
            AgentUpdateKind::Text,
            AgentUpdateKind::TurnEnd
        ]
    );
    assert_eq!(child[0].3["text"], "child hello");
    assert_eq!(child[1].3["text"], "child hi");
    for (c, _, source_ref, _) in &child {
        assert_eq!(c.harness, "hermes");
        assert_eq!(c.root, "ses_native");
        assert_eq!(c.parent.as_deref(), Some("ses_native"));
        assert_eq!(c.depth, Some(1));
        assert_eq!(c.resolution, ChildResolution::LineageVerified);
        assert_eq!(
            c.evidence.as_deref(),
            Some("sessions.parent_session_id chain to root")
        );
        assert_eq!(c.locator, "hermes:sessions/ses_child");
        assert!(
            source_ref.starts_with("hermes:ses_child@2#"),
            "{source_ref}"
        );
    }
    let nested: Vec<_> = children
        .iter()
        .filter(|(c, ..)| c.id.as_deref() == Some("ses_nested"))
        .collect();
    assert_eq!(
        nested.iter().map(|(_, k, ..)| *k).collect::<Vec<_>>(),
        vec![AgentUpdateKind::Text, AgentUpdateKind::TurnEnd]
    );
    assert_eq!(nested[0].0.parent.as_deref(), Some("ses_child"));
    assert_eq!(nested[0].0.depth, Some(2));
    assert_eq!(nested[0].0.resolution, ChildResolution::LineageVerified);
    assert!(
        children
            .iter()
            .all(|(c, ..)| c.id.as_deref() != Some("ses_stranger")),
        "a child of another root is nobody's descendant here"
    );

    // Durable cursors: nothing is forwarded twice.
    let cursors = HermesChildStreamsRepo::new(&nexus_store)
        .list(&session)
        .await
        .unwrap();
    assert_eq!(cursors.len(), 2);
    assert_eq!(cursors[0].child_session_id, "ses_child");
    assert_eq!(cursors[0].cursor, 6);
    assert_eq!(cursors[0].generation, "2");
    assert_eq!(cursors[1].child_session_id, "ses_nested");
    assert_eq!(cursors[1].cursor, 7);
    sink.events.lock().unwrap().clear();
    let stats = forward_once(nexus_store.clone(), session.clone(), Arc::new(sink.clone()))
        .await
        .unwrap();
    assert_eq!(stats.total_events(), 0);
    assert_eq!(stats.child_events, 0);
    assert!(sink.events.lock().unwrap().is_empty());
    assert!(StreamEvents::new(&nexus_store)
        .since(&session, 0)
        .await
        .unwrap()
        .is_empty());
}

/// A consumed child cursor from another daemon epoch declares bounded unknown coverage on the
/// child's lane and halts; nothing is replayed or skipped, and later passes stay halted.
#[tokio::test]
async fn a_hermes_child_cursor_from_another_epoch_declares_coverage_and_halts() {
    let nexus_store = Arc::new(Store::open(":memory:").await.unwrap());
    nexus_store.migrate().await.unwrap();
    DaemonState::new(&nexus_store)
        .set_boot_epoch("boot_a", now())
        .await
        .unwrap();
    let session = SessionId("s_hermes_child_epoch".into());
    let db_path = temp_db_path("child-epoch");
    seed_hermes_db_with_children(&db_path).await;
    HermesRuntimeStateRepo::new(&nexus_store)
        .upsert_launch(HermesRuntimeLaunch {
            runtime_id: session.clone(),
            hermes_db_path: db_path.clone().into(),
            hermes_session_id: Some("ses_native".into()),
            launch_cwd: "/tmp/nexus-hermes".into(),
            acp_child_pid: None,
            viewer_backend: "pty".into(),
        })
        .await
        .unwrap();
    let sink = CaptureSink::default();
    let stats = forward_once(nexus_store.clone(), session.clone(), Arc::new(sink.clone()))
        .await
        .unwrap();
    assert_eq!(stats.child_events, 5);

    // The daemon restarts; the child keeps talking meanwhile.
    DaemonState::new(&nexus_store)
        .set_boot_epoch("boot_b", now())
        .await
        .unwrap();
    let db = Store::open(&db_path).await.unwrap();
    insert_hermes_message_for(
        &db,
        "ses_child",
        9,
        "assistant",
        "after the restart",
        None,
        None,
        None,
        Some("stop"),
    )
    .await;
    sink.events.lock().unwrap().clear();
    let stats = forward_once(nexus_store.clone(), session.clone(), Arc::new(sink.clone()))
        .await
        .unwrap();
    assert_eq!(
        stats.child_events, 0,
        "nothing is skipped or replayed without a restart recovery policy"
    );
    assert_eq!(stats.child_errors, 0);
    let cursor = HermesChildStreamsRepo::new(&nexus_store)
        .find(&session, "ses_native", "ses_child")
        .await
        .unwrap()
        .unwrap();
    assert!(cursor.halted);
    assert_eq!(cursor.halt_reason, "daemon_restart_policy_undecided");
    assert_eq!(cursor.epoch, "boot_b");
    assert_eq!(cursor.cursor, 6, "the cursor did not advance");
    let lanes = ChildStreamEvents::new(&nexus_store)
        .lanes(&session, None, 10)
        .await
        .unwrap()
        .lanes;
    let lane = lanes
        .iter()
        .find(|l| l.child_key == "n:ses_child")
        .expect("coverage declared on the child's lane");
    let coverage = lane.coverage.as_ref().unwrap();
    assert_eq!(coverage["unknown_before"], true);
    assert_eq!(coverage["reason"], "daemon_restart_policy_undecided");
    assert_eq!(coverage["generation"], "2");
    assert_eq!(coverage["from"], 6);
    assert_eq!(lane.epoch, "boot_b");
    assert_eq!(lane.child.resolution, ChildResolution::LineageVerified);
    assert_eq!(lane.child.depth, Some(1));

    let stats = forward_once(nexus_store.clone(), session.clone(), Arc::new(sink.clone()))
        .await
        .unwrap();
    assert_eq!(stats.child_events, 0);
    assert!(ChildStreamEvents::new(&nexus_store)
        .page(&session, &LaneFilter::default(), 0, 10)
        .await
        .unwrap()
        .rows
        .is_empty());
}

/// A store whose sessions table records native ancestry, with the caller choosing the rows.
async fn seed_hermes_lineage_db(path: &str, sessions: &[(&str, f64, Option<&str>)]) -> Store {
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
                cwd TEXT,
                parent_session_id TEXT
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
    for (id, started_at, parent) in sessions {
        db.conn
            .execute(
                "INSERT INTO sessions (id, source, model_config, started_at, cwd, parent_session_id)
                 VALUES (?1, 'cli', NULL, ?2, '/tmp/nexus-hermes', ?3)",
                libsql::params![*id, *started_at, *parent],
            )
            .await
            .unwrap();
    }
    db
}

async fn hermes_runtime(store: &Store, session: &SessionId, db_path: &str, root: Option<&str>) {
    HermesRuntimeStateRepo::new(store)
        .upsert_launch(HermesRuntimeLaunch {
            runtime_id: session.clone(),
            hermes_db_path: db_path.into(),
            hermes_session_id: root.map(str::to_owned),
            launch_cwd: "/tmp/nexus-hermes".into(),
            acp_child_pid: None,
            viewer_backend: "pty".into(),
        })
        .await
        .unwrap();
}

async fn memory_store(epoch: &str) -> Arc<Store> {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    DaemonState::new(&store)
        .set_boot_epoch(epoch, now())
        .await
        .unwrap();
    store
}

/// A file-backed store reopened like a daemon restart: fresh volatile lane, new epoch.
async fn file_store(path: &str, epoch: &str) -> Arc<Store> {
    let store = Arc::new(Store::open(path).await.unwrap());
    store.migrate().await.unwrap();
    DaemonState::new(&store)
        .set_boot_epoch(epoch, now())
        .await
        .unwrap();
    store
}

fn child_ids(events: &[WsEvent]) -> Vec<String> {
    child_events(events)
        .into_iter()
        .filter_map(|(c, ..)| c.id)
        .collect()
}

/// The root a child was verified under is part of its identity: after the runtime's bound
/// root changes from A to B, A's child keeps streaming and declaring under A while B's own
/// descendants stream under B, and nothing of A's is relabelled as B's.
#[tokio::test]
async fn a_rebound_root_never_relabels_earlier_roots_children() {
    let store = memory_store("boot_a").await;
    let session = SessionId("s_hermes_rebind".into());
    let db_path = temp_db_path("rebind");
    let db = seed_hermes_lineage_db(
        &db_path,
        &[
            ("root_a", 1.0, None),
            ("child_a", 2.0, Some("root_a")),
            ("root_b", 3.0, None),
            ("child_b", 4.0, Some("root_b")),
        ],
    )
    .await;
    insert_hermes_message_for(
        &db,
        "child_a",
        1,
        "assistant",
        "a says",
        None,
        None,
        None,
        Some("stop"),
    )
    .await;
    insert_hermes_message_for(
        &db,
        "child_b",
        2,
        "assistant",
        "b says",
        None,
        None,
        None,
        Some("stop"),
    )
    .await;
    hermes_runtime(&store, &session, &db_path, Some("root_a")).await;
    let sink = CaptureSink::default();

    let stats = forward_once(store.clone(), session.clone(), Arc::new(sink.clone()))
        .await
        .unwrap();
    assert_eq!(stats.child_events, 2);
    let events = sink.events.lock().unwrap().clone();
    let a: Vec<_> = child_events(&events);
    assert!(a
        .iter()
        .all(|(c, ..)| c.root == "root_a" && c.id.as_deref() == Some("child_a")));

    // The runtime's bound root moves to B; A's child talks again.
    HermesRuntimeStateRepo::new(&store)
        .set_hermes_session_id(&session, "root_b")
        .await
        .unwrap();
    insert_hermes_message_for(
        &db,
        "child_a",
        3,
        "assistant",
        "a again",
        None,
        None,
        None,
        Some("stop"),
    )
    .await;
    sink.events.lock().unwrap().clear();
    let stats = forward_once(store.clone(), session.clone(), Arc::new(sink.clone()))
        .await
        .unwrap();
    assert_eq!(stats.child_events, 4, "{stats:?}");
    let events = sink.events.lock().unwrap().clone();
    let by_child = |id: &str| -> Vec<(
        nexus_contracts::ChildStream,
        AgentUpdateKind,
        String,
        serde_json::Value,
    )> {
        child_events(&events)
            .into_iter()
            .filter(|(c, ..)| c.id.as_deref() == Some(id))
            .collect()
    };
    let a_again = by_child("child_a");
    assert_eq!(a_again.len(), 2);
    assert!(a_again
        .iter()
        .all(|(c, ..)| c.root == "root_a" && c.parent.as_deref() == Some("root_a")));
    assert_eq!(a_again[0].3["text"], "a again");
    let b = by_child("child_b");
    assert_eq!(b.len(), 2);
    assert!(b.iter().all(|(c, ..)| c.root == "root_b"
        && c.parent.as_deref() == Some("root_b")
        && c.depth == Some(1)));
    assert_eq!(b[0].3["text"], "b says");
    let cursors = HermesChildStreamsRepo::new(&store)
        .list(&session)
        .await
        .unwrap();
    assert_eq!(
        cursors
            .iter()
            .map(|c| (c.root.as_str(), c.child_session_id.as_str()))
            .collect::<Vec<_>>(),
        vec![("root_a", "child_a"), ("root_b", "child_b")]
    );
}

/// Discovery is keyset-resumable and fair: 300 direct children are all registered and served
/// across passes with no repeat, the cut pages being counted rather than hidden; a deeper
/// frontier (40 children with 3 grandchildren each) is fully reached as the frontier rotates.
#[tokio::test]
async fn discovery_resumes_past_full_pages_and_reaches_the_whole_tree() {
    let store = memory_store("boot_a").await;
    let session = SessionId("s_hermes_wide".into());
    let db_path = temp_db_path("wide");
    let mut sessions: Vec<(String, f64, Option<String>)> = vec![("root".into(), 1.0, None)];
    for i in 0..300 {
        sessions.push((format!("d{i:03}"), 2.0 + i as f64, Some("root".into())));
    }
    for i in 0..40 {
        for j in 0..3 {
            sessions.push((
                format!("g{i:02}_{j}"),
                400.0 + (i * 3 + j) as f64,
                Some(format!("d{i:03}")),
            ));
        }
    }
    let borrowed: Vec<(&str, f64, Option<&str>)> = sessions
        .iter()
        .map(|(id, t, p)| (id.as_str(), *t, p.as_deref()))
        .collect();
    let db = seed_hermes_lineage_db(&db_path, &borrowed).await;
    let mut next_id = 1;
    for (id, _, parent) in &sessions {
        if parent.is_some() {
            insert_hermes_message_for(
                &db,
                id,
                next_id,
                "assistant",
                "hi",
                None,
                None,
                None,
                Some("stop"),
            )
            .await;
            next_id += 1;
        }
    }
    hermes_runtime(&store, &session, &db_path, Some("root")).await;
    let sink = CaptureSink::default();

    let first = forward_once(store.clone(), session.clone(), Arc::new(sink.clone()))
        .await
        .unwrap();
    assert!(
        first.child_discovery_truncated > 0,
        "the first pass says it was cut: {first:?}"
    );
    let total = 300 + 120;
    let mut passes = 1;
    let mut seen: std::collections::BTreeSet<String> = child_ids(&sink.events.lock().unwrap())
        .into_iter()
        .collect();
    while seen.len() < total && passes < 80 {
        forward_once(store.clone(), session.clone(), Arc::new(sink.clone()))
            .await
            .unwrap();
        passes += 1;
        seen = child_ids(&sink.events.lock().unwrap())
            .into_iter()
            .collect();
    }
    assert_eq!(
        seen.len(),
        total,
        "every descendant served after {passes} passes"
    );
    let all = child_ids(&sink.events.lock().unwrap());
    assert_eq!(
        all.len(),
        total * 2,
        "each child's two events once, none repeated"
    );
    let cursors = HermesChildStreamsRepo::new(&store)
        .list(&session)
        .await
        .unwrap();
    assert_eq!(cursors.len(), total);
    assert!(cursors
        .iter()
        .filter(|c| c.child_session_id.starts_with('g'))
        .all(|c| c.depth == 2));
    // Once the tree is registered, a pass no longer reports cuts that could hide descendants;
    // it still reports that the wide first level is explored a bounded frontier at a time.
    let settled = forward_once(store.clone(), session.clone(), Arc::new(sink.clone()))
        .await
        .unwrap();
    assert_eq!(settled.child_discovery_truncated, 0, "{settled:?}");
    assert!(settled.child_discovery_deferred > 0, "{settled:?}");
    assert_eq!(settled.child_events, 0);
}

/// A child record over the per-record byte budget is never materialized: the rows before it
/// are forwarded, the child halts with a declaration naming the row, and the parent and a
/// sibling child are untouched.
#[tokio::test]
async fn an_oversized_child_record_halts_that_child_only_without_being_materialized() {
    let store = memory_store("boot_a").await;
    let session = SessionId("s_hermes_bytes".into());
    let db_path = temp_db_path("bytes");
    let db = seed_hermes_lineage_db(
        &db_path,
        &[
            ("root", 1.0, None),
            ("big", 2.0, Some("root")),
            ("small", 3.0, Some("root")),
        ],
    )
    .await;
    insert_hermes_message_for(&db, "root", 1, "user", "root hello", None, None, None, None).await;
    insert_hermes_message_for(
        &db,
        "big",
        2,
        "assistant",
        "before",
        None,
        None,
        None,
        Some("stop"),
    )
    .await;
    let huge = "x".repeat(300 * 1024);
    insert_hermes_message_for(
        &db,
        "big",
        3,
        "tool",
        &huge,
        None,
        Some("call_9"),
        Some("terminal"),
        None,
    )
    .await;
    insert_hermes_message_for(
        &db,
        "big",
        4,
        "assistant",
        "after",
        None,
        None,
        None,
        Some("stop"),
    )
    .await;
    insert_hermes_message_for(
        &db,
        "small",
        5,
        "assistant",
        "sibling",
        None,
        None,
        None,
        Some("stop"),
    )
    .await;
    hermes_runtime(&store, &session, &db_path, Some("root")).await;
    let sink = CaptureSink::default();

    let stats = forward_once(store.clone(), session.clone(), Arc::new(sink.clone()))
        .await
        .unwrap();
    assert_eq!(stats.user_input_events, 1, "the parent is untouched");
    assert_eq!(stats.child_errors, 0);
    let events = sink.events.lock().unwrap().clone();
    let big: Vec<_> = child_events(&events)
        .into_iter()
        .filter(|(c, ..)| c.id.as_deref() == Some("big"))
        .collect();
    assert_eq!(
        big.iter().map(|(_, k, ..)| *k).collect::<Vec<_>>(),
        vec![AgentUpdateKind::Text, AgentUpdateKind::TurnEnd]
    );
    assert_eq!(big[0].3["text"], "before");
    assert!(
        events
            .iter()
            .all(|e| !serde_json::to_string(e).unwrap().contains(&huge[..1024])),
        "nothing of the oversized record was forwarded"
    );
    let small: Vec<_> = child_events(&events)
        .into_iter()
        .filter(|(c, ..)| c.id.as_deref() == Some("small"))
        .collect();
    assert_eq!(small.len(), 2, "the sibling is untouched");
    let cursor = HermesChildStreamsRepo::new(&store)
        .find(&session, "root", "big")
        .await
        .unwrap()
        .unwrap();
    assert!(cursor.halted);
    assert_eq!(cursor.halt_reason, "record_exceeds_byte_budget");
    assert_eq!(
        cursor.cursor, 2,
        "advanced through the rows before the oversized one"
    );
    let lanes = ChildStreamEvents::new(&store)
        .lanes(&session, None, 10)
        .await
        .unwrap()
        .lanes;
    let lane = lanes.iter().find(|l| l.child_key == "n:big").unwrap();
    let coverage = lane.coverage.as_ref().unwrap();
    assert_eq!(coverage["reason"], "record_exceeds_byte_budget");
    assert_eq!(coverage["row"], 3);
    assert_eq!(coverage["from"], 2);
    let stats = forward_once(store.clone(), session.clone(), Arc::new(sink.clone()))
        .await
        .unwrap();
    assert_eq!(stats.child_events, 0, "stays halted");
}

/// Durable continuity through real store reopens: a child cursor persists across a
/// file-backed reopen, halts under the new epoch with coverage on the fresh volatile lane,
/// and re-declares that coverage under a second reopen; a refused declaration leaves the
/// cursor unhalted and retrying.
#[tokio::test]
async fn child_cursors_survive_real_reopens_and_re_declare_after_each_boot() {
    let dir = std::env::temp_dir().join(format!(
        "nexus-hermes-reopen-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let store_path = dir.join("state.db").to_string_lossy().into_owned();
    let session = SessionId("s_hermes_reopen".into());
    let db_path = temp_db_path("reopen");
    let db =
        seed_hermes_lineage_db(&db_path, &[("root", 1.0, None), ("kid", 2.0, Some("root"))]).await;
    insert_hermes_message_for(
        &db,
        "kid",
        1,
        "assistant",
        "first boot",
        None,
        None,
        None,
        Some("stop"),
    )
    .await;

    let store = file_store(&store_path, "boot_a").await;
    hermes_runtime(&store, &session, &db_path, Some("root")).await;
    let sink = CaptureSink::default();
    let stats = forward_once(store.clone(), session.clone(), Arc::new(sink.clone()))
        .await
        .unwrap();
    assert_eq!(stats.child_events, 2);
    drop(store);

    // Restart one: the cursor is durable, the lane is fresh, the halt is declared under boot_b.
    let store = file_store(&store_path, "boot_b").await;
    assert!(ChildStreamEvents::new(&store)
        .lanes(&session, None, 10)
        .await
        .unwrap()
        .lanes
        .is_empty());
    insert_hermes_message_for(
        &db,
        "kid",
        2,
        "assistant",
        "after restart",
        None,
        None,
        None,
        Some("stop"),
    )
    .await;
    let stats = forward_once(store.clone(), session.clone(), Arc::new(sink.clone()))
        .await
        .unwrap();
    assert_eq!(stats.child_events, 0);
    let cursor = HermesChildStreamsRepo::new(&store)
        .find(&session, "root", "kid")
        .await
        .unwrap()
        .unwrap();
    assert!(cursor.halted);
    assert_eq!(cursor.halt_reason, "daemon_restart_policy_undecided");
    assert_eq!(cursor.epoch, "boot_b");
    assert_eq!(cursor.cursor, 1);
    let lane = ChildStreamEvents::new(&store)
        .lanes(&session, None, 10)
        .await
        .unwrap()
        .lanes
        .into_iter()
        .find(|l| l.child_key == "n:kid")
        .expect("declared under boot_b");
    assert_eq!(lane.epoch, "boot_b");
    assert_eq!(lane.coverage.as_ref().unwrap()["from"], 1);
    drop(store);

    // Restart two: the halted cursor re-declares under boot_c on the again-fresh lane.
    let store = file_store(&store_path, "boot_c").await;
    assert!(ChildStreamEvents::new(&store)
        .lanes(&session, None, 10)
        .await
        .unwrap()
        .lanes
        .is_empty());
    let stats = forward_once(store.clone(), session.clone(), Arc::new(sink.clone()))
        .await
        .unwrap();
    assert_eq!(stats.child_events, 0);
    let cursor = HermesChildStreamsRepo::new(&store)
        .find(&session, "root", "kid")
        .await
        .unwrap()
        .unwrap();
    assert!(cursor.halted);
    assert_eq!(cursor.epoch, "boot_c");
    assert_eq!(cursor.cursor, 1);
    let lane = ChildStreamEvents::new(&store)
        .lanes(&session, None, 10)
        .await
        .unwrap()
        .lanes
        .into_iter()
        .find(|l| l.child_key == "n:kid")
        .expect("re-declared under boot_c");
    assert_eq!(lane.epoch, "boot_c");
    assert_eq!(
        lane.coverage.as_ref().unwrap()["reason"],
        "daemon_restart_policy_undecided"
    );
    assert_eq!(lane.child.resolution, ChildResolution::LineageVerified);
    assert_eq!(lane.child.root, "root");
}

#[tokio::test]
async fn a_refused_coverage_declaration_leaves_the_child_cursor_retrying() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    store.configure_child_stream_bounds(nexus_store::repos::ChildStreamBounds {
        max_lanes_per_session: 0,
        ..nexus_store::repos::ChildStreamBounds::default()
    });
    DaemonState::new(&store)
        .set_boot_epoch("boot_a", now())
        .await
        .unwrap();
    let session = SessionId("s_hermes_refused".into());
    let db_path = temp_db_path("refused");
    let db =
        seed_hermes_lineage_db(&db_path, &[("root", 1.0, None), ("kid", 2.0, Some("root"))]).await;
    insert_hermes_message_for(
        &db,
        "kid",
        1,
        "assistant",
        "hello",
        None,
        None,
        None,
        Some("stop"),
    )
    .await;
    hermes_runtime(&store, &session, &db_path, Some("root")).await;
    let sink = CaptureSink::default();
    assert_eq!(
        forward_once(store.clone(), session.clone(), Arc::new(sink.clone()))
            .await
            .unwrap()
            .child_events,
        2
    );

    DaemonState::new(&store)
        .set_boot_epoch("boot_b", now())
        .await
        .unwrap();
    for _ in 0..2 {
        let stats = forward_once(store.clone(), session.clone(), Arc::new(sink.clone()))
            .await
            .unwrap();
        assert_eq!(stats.child_events, 0);
        assert_eq!(stats.child_errors, 0);
        let cursor = HermesChildStreamsRepo::new(&store)
            .find(&session, "root", "kid")
            .await
            .unwrap()
            .unwrap();
        assert!(
            !cursor.halted,
            "a refused declaration is not recorded as halted"
        );
        assert_eq!(
            cursor.epoch, "boot_a",
            "the epoch is unchanged so the pass retries"
        );
        assert_eq!(cursor.cursor, 1);
    }
    let lanes = ChildStreamEvents::new(&store)
        .lanes(&session, None, 10)
        .await
        .unwrap()
        .lanes;
    assert!(
        lanes.iter().all(|l| l.child_key != "n:kid"),
        "the lane refused the declaration"
    );
}

/// A three-level fanout wider than the per-level frontier: 16 children, each with 16
/// grandchildren, each with one great-grandchild. Exploration must rotate between parent
/// groups, so every great-grandchild is registered and served; deferrals are counted while the
/// frontier is spread over passes.
#[tokio::test]
async fn the_frontier_rotates_fairly_between_parent_groups_across_depths() {
    let store = memory_store("boot_a").await;
    let session = SessionId("s_hermes_deep".into());
    let db_path = temp_db_path("deep");
    let mut sessions: Vec<(String, f64, Option<String>)> = vec![("root".into(), 1.0, None)];
    let mut t = 2.0;
    for c in 0..16 {
        sessions.push((format!("c{c:02}"), t, Some("root".into())));
        t += 1.0;
    }
    for c in 0..16 {
        for g in 0..16 {
            sessions.push((format!("g{c:02}_{g:02}"), t, Some(format!("c{c:02}"))));
            t += 1.0;
        }
    }
    for c in 0..16 {
        for g in 0..16 {
            sessions.push((
                format!("h{c:02}_{g:02}"),
                t,
                Some(format!("g{c:02}_{g:02}")),
            ));
            t += 1.0;
        }
    }
    let borrowed: Vec<(&str, f64, Option<&str>)> = sessions
        .iter()
        .map(|(id, t, p)| (id.as_str(), *t, p.as_deref()))
        .collect();
    let db = seed_hermes_lineage_db(&db_path, &borrowed).await;
    let mut next_id = 1;
    for (id, _, parent) in &sessions {
        if parent.is_some() {
            insert_hermes_message_for(
                &db,
                id,
                next_id,
                "assistant",
                "hi",
                None,
                None,
                None,
                Some("stop"),
            )
            .await;
            next_id += 1;
        }
    }
    hermes_runtime(&store, &session, &db_path, Some("root")).await;
    let sink = CaptureSink::default();

    let total = 16 + 256 + 256;
    let mut passes = 0;
    let mut deferred_seen = false;
    let mut seen: std::collections::BTreeSet<String> = Default::default();
    while seen.len() < total && passes < 120 {
        let stats = forward_once(store.clone(), session.clone(), Arc::new(sink.clone()))
            .await
            .unwrap();
        deferred_seen |= stats.child_discovery_deferred > 0;
        passes += 1;
        seen = child_ids(&sink.events.lock().unwrap())
            .into_iter()
            .collect();
    }
    assert_eq!(
        seen.len(),
        total,
        "every descendant served after {passes} passes"
    );
    assert!(
        deferred_seen,
        "the grandchild frontier was spread over passes"
    );
    let cursors = HermesChildStreamsRepo::new(&store)
        .list(&session)
        .await
        .unwrap();
    let great: Vec<_> = cursors.iter().filter(|c| c.depth == 3).collect();
    assert_eq!(great.len(), 256);
    let parents_of_great: std::collections::BTreeSet<&str> =
        great.iter().map(|c| c.parent_session_id.as_str()).collect();
    assert_eq!(
        parents_of_great.len(),
        256,
        "every grandchild, of every child, was explored"
    );
    let all = child_ids(&sink.events.lock().unwrap());
    assert_eq!(all.len(), total * 2, "no record forwarded twice");
}

/// Byte budgets use exact byte lengths: a multibyte payload whose character count is under the
/// budget but whose byte count is over it is withheld, and a payload with an embedded NUL is
/// measured in full instead of stopping at the NUL; a small payload containing a NUL is
/// forwarded normally.
#[tokio::test]
async fn payload_budgets_measure_bytes_not_characters_and_see_past_an_embedded_nul() {
    let store = memory_store("boot_a").await;
    let session = SessionId("s_hermes_bytes_exact".into());
    let db_path = temp_db_path("bytes-exact");
    let db = seed_hermes_lineage_db(
        &db_path,
        &[
            ("root", 1.0, None),
            ("wide", 2.0, Some("root")),
            ("nul", 3.0, Some("root")),
            ("okay", 4.0, Some("root")),
        ],
    )
    .await;
    // 200 000 characters, 400 000 bytes: under the budget in characters, over it in bytes.
    let multibyte = "\u{e9}".repeat(200_000);
    insert_hermes_message_for(
        &db,
        "wide",
        1,
        "assistant",
        "before wide",
        None,
        None,
        None,
        Some("stop"),
    )
    .await;
    insert_hermes_message_for(
        &db,
        "wide",
        2,
        "tool",
        &multibyte,
        None,
        Some("call_w"),
        Some("terminal"),
        None,
    )
    .await;
    // One byte, a NUL, then a huge tail: TEXT length would report 1.
    let nul_tail = format!("x\0{}", "y".repeat(300 * 1024));
    insert_hermes_message_for(
        &db,
        "nul",
        3,
        "assistant",
        "before nul",
        None,
        None,
        None,
        Some("stop"),
    )
    .await;
    insert_hermes_message_for(
        &db,
        "nul",
        4,
        "tool",
        &nul_tail,
        None,
        Some("call_n"),
        Some("terminal"),
        None,
    )
    .await;
    // A small NUL-containing payload is within budget and is forwarded.
    insert_hermes_message_for(
        &db,
        "okay",
        5,
        "assistant",
        "a\0b",
        None,
        None,
        None,
        Some("stop"),
    )
    .await;
    hermes_runtime(&store, &session, &db_path, Some("root")).await;
    let sink = CaptureSink::default();

    let stats = forward_once(store.clone(), session.clone(), Arc::new(sink.clone()))
        .await
        .unwrap();
    assert_eq!(stats.child_errors, 0, "{stats:?}");
    let events = sink.events.lock().unwrap().clone();
    let by = |id: &str| -> Vec<ChildEvent> {
        child_events(&events)
            .into_iter()
            .filter(|(c, ..)| c.id.as_deref() == Some(id))
            .collect()
    };
    for (child, expected_bytes, row) in [("wide", 400_000, 2), ("nul", 300 * 1024 + 2, 4)] {
        let forwarded = by(child);
        assert_eq!(
            forwarded.len(),
            2,
            "{child}: the row before the oversized one"
        );
        assert_eq!(forwarded[0].3["text"], format!("before {child}"));
        let cursor = HermesChildStreamsRepo::new(&store)
            .find(&session, "root", child)
            .await
            .unwrap()
            .unwrap();
        assert!(cursor.halted, "{child}");
        assert_eq!(cursor.halt_reason, "record_exceeds_byte_budget");
        let lanes = ChildStreamEvents::new(&store)
            .lanes(&session, None, 10)
            .await
            .unwrap()
            .lanes;
        let lane = lanes
            .iter()
            .find(|l| l.child_key == format!("n:{child}"))
            .unwrap();
        let coverage = lane.coverage.as_ref().unwrap();
        assert_eq!(coverage["row"], row);
        assert_eq!(coverage["bytes"], expected_bytes);
    }
    let serialized = serde_json::to_string(&events).unwrap();
    assert!(
        !serialized.contains(&multibyte[..64]),
        "nothing of the multibyte payload was forwarded"
    );
    assert!(
        !serialized.contains(&"y".repeat(64)),
        "nothing of the NUL-tailed payload was forwarded"
    );
    let okay = by("okay");
    assert_eq!(okay.len(), 2, "{okay:?}");
    assert_eq!(okay[0].3["text"], "a\0b");
    assert!(
        !HermesChildStreamsRepo::new(&store)
            .find(&session, "root", "okay")
            .await
            .unwrap()
            .unwrap()
            .halted
    );
}

/// Invalid UTF-8 is isolated to the child that owns it: neither replacement text nor a
/// replacement-bearing tool call is emitted, the cursor stays before the bad row, and healthy
/// parent and sibling traffic keeps moving. Replacing the exact native rows makes the next pass
/// retry them once without replaying already-consumed traffic.
#[tokio::test]
async fn invalid_utf8_child_payloads_retry_without_substitution_or_cross_child_stalls() {
    let store = memory_store("boot_a").await;
    let session = SessionId("s_hermes_invalid_utf8".into());
    let db_path = temp_db_path("invalid-utf8");
    let db = seed_hermes_lineage_db(
        &db_path,
        &[
            ("root", 1.0, None),
            ("bad_content", 2.0, Some("root")),
            ("bad_tools", 3.0, Some("root")),
            ("healthy", 4.0, Some("root")),
        ],
    )
    .await;
    insert_hermes_message_for(&db, "root", 1, "user", "parent", None, None, None, None).await;
    insert_hermes_message_for(
        &db,
        "bad_content",
        2,
        "assistant",
        "placeholder",
        None,
        None,
        None,
        Some("stop"),
    )
    .await;
    insert_hermes_message_for(
        &db,
        "bad_tools",
        3,
        "assistant",
        "",
        None,
        None,
        None,
        Some("tool_calls"),
    )
    .await;
    insert_hermes_message_for(
        &db,
        "healthy",
        4,
        "assistant",
        "sibling",
        None,
        None,
        None,
        Some("stop"),
    )
    .await;

    let invalid_content = vec![b'b', 0xff, b'd'];
    db.conn
        .execute(
            "UPDATE messages SET content = CAST(?2 AS TEXT) WHERE id = ?1",
            libsql::params![2_i64, invalid_content],
        )
        .await
        .unwrap();
    let mut invalid_tool_json =
        br#"[{"id":"call_bad","type":"function","function":{"name":"term"#.to_vec();
    invalid_tool_json.push(0xff);
    invalid_tool_json.extend_from_slice(br#"inal","arguments":"{}"}}]"#);
    db.conn
        .execute(
            "UPDATE messages SET tool_calls = CAST(?2 AS TEXT) WHERE id = ?1",
            libsql::params![3_i64, invalid_tool_json],
        )
        .await
        .unwrap();
    hermes_runtime(&store, &session, &db_path, Some("root")).await;
    let sink = CaptureSink::default();

    let stats = forward_once(store.clone(), session.clone(), Arc::new(sink.clone()))
        .await
        .unwrap();
    assert_eq!(stats.user_input_events, 1, "parent traffic progresses");
    assert_eq!(stats.child_errors, 2, "one error per invalid child");
    let events = sink.events.lock().unwrap().clone();
    assert_agent_update(&events[0], AgentUpdateKind::UserInput, "parent");
    let children = child_events(&events);
    assert_eq!(children.len(), 2, "only the healthy sibling emits");
    assert!(children
        .iter()
        .all(|(child, ..)| child.id.as_deref() == Some("healthy")));
    assert_eq!(children[0].3["text"], "sibling");
    assert!(!serde_json::to_string(&events).unwrap().contains('\u{fffd}'));
    for child in ["bad_content", "bad_tools"] {
        let cursor = HermesChildStreamsRepo::new(&store)
            .find(&session, "root", child)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cursor.cursor, 0, "{child} stays before its invalid row");
        assert!(!cursor.halted, "{child} is retryable");
    }

    db.conn
        .execute(
            "UPDATE messages SET content = CAST(?2 AS TEXT) WHERE id = ?1",
            libsql::params![2_i64, b"repaired content".to_vec()],
        )
        .await
        .unwrap();
    let repaired_tool_json =
        br#"[{"id":"call_repaired","type":"function","function":{"name":"terminal","arguments":"{}"}}]"#;
    db.conn
        .execute(
            "UPDATE messages SET tool_calls = CAST(?2 AS TEXT) WHERE id = ?1",
            libsql::params![3_i64, repaired_tool_json.to_vec()],
        )
        .await
        .unwrap();
    sink.events.lock().unwrap().clear();

    let stats = forward_once(store.clone(), session.clone(), Arc::new(sink.clone()))
        .await
        .unwrap();
    assert_eq!(stats.total_events(), 0, "the parent is not replayed");
    assert_eq!(stats.child_errors, 0);
    assert_eq!(stats.child_events, 3);
    let events = sink.events.lock().unwrap().clone();
    let children = child_events(&events);
    assert!(children
        .iter()
        .all(|(child, ..)| child.id.as_deref() != Some("healthy")));
    let repaired_content: Vec<_> = children
        .iter()
        .filter(|(child, ..)| child.id.as_deref() == Some("bad_content"))
        .collect();
    assert_eq!(repaired_content.len(), 2);
    assert_eq!(repaired_content[0].3["text"], "repaired content");
    let repaired_tools: Vec<_> = children
        .iter()
        .filter(|(child, ..)| child.id.as_deref() == Some("bad_tools"))
        .collect();
    assert_eq!(repaired_tools.len(), 1);
    assert_eq!(repaired_tools[0].1, AgentUpdateKind::ToolCall);
    assert_eq!(repaired_tools[0].3["id"], "call_repaired");
    for (child, row) in [("bad_content", 2), ("bad_tools", 3), ("healthy", 4)] {
        assert_eq!(
            HermesChildStreamsRepo::new(&store)
                .find(&session, "root", child)
                .await
                .unwrap()
                .unwrap()
                .cursor,
            row
        );
    }

    sink.events.lock().unwrap().clear();
    let settled = forward_once(store, session, Arc::new(sink.clone()))
        .await
        .unwrap();
    assert_eq!(settled.total_events(), 0);
    assert_eq!(settled.child_events, 0, "repaired rows emit exactly once");
    assert!(sink.events.lock().unwrap().is_empty());
}

/// The tool-call column has the same byte guard as content. Valid multibyte JSON and malformed
/// NUL-tailed text over the limit are withheld before materialization or JSON parsing, while an
/// exact-limit valid JSON value, an empty tool-call value, the parent, and a sibling all proceed.
#[tokio::test]
async fn oversized_tool_calls_use_blob_byte_lengths_and_admit_the_exact_limit() {
    const RECORD_LIMIT: usize = 256 * 1024;

    let store = memory_store("boot_a").await;
    let session = SessionId("s_hermes_tool_bytes".into());
    let db_path = temp_db_path("tool-bytes");
    let db = seed_hermes_lineage_db(
        &db_path,
        &[
            ("root", 1.0, None),
            ("multi", 2.0, Some("root")),
            ("nul", 3.0, Some("root")),
            ("exact", 4.0, Some("root")),
            ("healthy", 5.0, Some("root")),
        ],
    )
    .await;
    insert_hermes_message_for(&db, "root", 1, "user", "parent", None, None, None, None).await;

    let multibyte_tool_calls = format!(
        r#"[{{"id":"call_multi","type":"function","function":{{"name":"terminal","arguments":"{}"}}}}]"#,
        "\u{e9}".repeat(200_000)
    );
    assert!(multibyte_tool_calls.len() > RECORD_LIMIT);
    insert_hermes_message_for(
        &db,
        "multi",
        2,
        "assistant",
        "",
        Some(&multibyte_tool_calls),
        None,
        None,
        Some("tool_calls"),
    )
    .await;

    // Invalid JSON by construction; the byte guard must reject it before JSON parsing.
    let nul_tool_calls = format!("[\0{}", "y".repeat(300 * 1024));
    assert!(nul_tool_calls.len() > RECORD_LIMIT);
    insert_hermes_message_for(
        &db,
        "nul",
        3,
        "assistant",
        "",
        Some(&nul_tool_calls),
        None,
        None,
        Some("tool_calls"),
    )
    .await;

    let mut exact_tool_calls =
        r#"[{"id":"call_exact","type":"function","function":{"name":"terminal","arguments":"{}"}}]"#
            .to_string();
    exact_tool_calls.push_str(&" ".repeat(RECORD_LIMIT - exact_tool_calls.len()));
    assert_eq!(exact_tool_calls.len(), RECORD_LIMIT);
    insert_hermes_message_for(
        &db,
        "exact",
        4,
        "assistant",
        "",
        Some(&exact_tool_calls),
        None,
        None,
        Some("tool_calls"),
    )
    .await;
    insert_hermes_message_for(
        &db,
        "healthy",
        5,
        "assistant",
        "sibling",
        Some(""),
        None,
        None,
        Some("stop"),
    )
    .await;
    hermes_runtime(&store, &session, &db_path, Some("root")).await;
    let sink = CaptureSink::default();

    let stats = forward_once(store.clone(), session.clone(), Arc::new(sink.clone()))
        .await
        .unwrap();
    assert_eq!(stats.user_input_events, 1, "parent traffic progresses");
    assert_eq!(stats.child_errors, 0, "oversized JSON is never parsed");
    assert_eq!(
        stats.child_events, 3,
        "exact-limit and sibling controls emit"
    );
    let events = sink.events.lock().unwrap().clone();
    let children = child_events(&events);
    assert!(children
        .iter()
        .all(|(child, ..)| { matches!(child.id.as_deref(), Some("exact") | Some("healthy")) }));
    let exact: Vec<_> = children
        .iter()
        .filter(|(child, ..)| child.id.as_deref() == Some("exact"))
        .collect();
    assert_eq!(exact.len(), 1);
    assert_eq!(exact[0].1, AgentUpdateKind::ToolCall);
    assert_eq!(exact[0].3["id"], "call_exact");
    let healthy: Vec<_> = children
        .iter()
        .filter(|(child, ..)| child.id.as_deref() == Some("healthy"))
        .collect();
    assert_eq!(healthy.len(), 2);
    assert_eq!(healthy[0].3["text"], "sibling");
    let serialized = serde_json::to_string(&events).unwrap();
    assert!(!serialized.contains(&"\u{e9}".repeat(32)));
    assert!(!serialized.contains(&"y".repeat(64)));

    let lanes = ChildStreamEvents::new(&store)
        .lanes(&session, None, 10)
        .await
        .unwrap()
        .lanes;
    for (child, row, bytes) in [
        ("multi", 2, multibyte_tool_calls.len()),
        ("nul", 3, nul_tool_calls.len()),
    ] {
        let cursor = HermesChildStreamsRepo::new(&store)
            .find(&session, "root", child)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cursor.cursor, 0, "{child} stays before the rejected row");
        assert!(cursor.halted, "{child}");
        assert_eq!(cursor.halt_reason, "record_exceeds_byte_budget");
        let coverage = lanes
            .iter()
            .find(|lane| lane.child_key == format!("n:{child}"))
            .unwrap()
            .coverage
            .as_ref()
            .unwrap();
        assert_eq!(coverage["reason"], "record_exceeds_byte_budget");
        assert_eq!(coverage["row"], row);
        assert_eq!(coverage["from"], 0);
        assert_eq!(coverage["bytes"], bytes);
        assert_eq!(coverage["budget"], RECORD_LIMIT);
    }
    for (child, row) in [("exact", 4), ("healthy", 5)] {
        let cursor = HermesChildStreamsRepo::new(&store)
            .find(&session, "root", child)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cursor.cursor, row);
        assert!(!cursor.halted);
    }
}
