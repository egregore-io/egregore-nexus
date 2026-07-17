//! Integration coverage formerly embedded in daemon/app.rs.
//!
//! Keep this target behavior-oriented: app.rs contains production wiring only, while these tests
//! exercise the same launch, revive, removal, and source-lifecycle contracts through exported seams.

use nexus::daemon::app::{
    harness_from_token, headed_runtime_from_agent_token, launch_route, pending_respawn_delay_ms,
    revive_route, teardown_route, AppState, LaunchRoute, ReviveRoute, TeardownRoute,
};
use nexus_common::{now, Config};
use nexus_contracts::{Caller, Harness, Kind, Message, SessionId, SpawnRequest, Tier};
use nexus_harness_core::HeadedRuntimeKind;
use nexus_store::repos::{Inbox, Messages, NativeThreadBindings, NewSession, Sessions};
use nexus_store::types::SessionRow;
use nexus_store::Store;
use std::path::PathBuf;
use std::sync::Arc;

const PENDING_RESPAWN_BASE_BACKOFF_MS: i64 = 30_000;
const PENDING_RESPAWN_MAX_BACKOFF_MS: i64 = 5 * 60_000;
const PENDING_RESPAWN_TOMBSTONE_FAILURES: u32 = 3;

fn kind_token(kind: Kind) -> &'static str {
    match kind {
        Kind::Agent => "agent",
        Kind::Human => "human",
        Kind::Notification => "notification",
        Kind::App => "app",
    }
}

fn tier_token(tier: Tier) -> &'static str {
    match tier {
        Tier::Admin => "admin",
        Tier::Agent => "agent",
    }
}

/// Build a minimal in-memory store suitable for wiring tests (no live I/O).
async fn mem_store() -> Arc<Store> {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    store
}

fn owner_row(name: &str, project: &str, transport: &str) -> SessionRow {
    SessionRow {
        session_id: SessionId(format!("s_{name}")),
        name: Some(name.to_string()),
        agent: Some("claude".to_string()),
        kind: "agent".to_string(),
        role: None,
        tier: "agent".to_string(),
        harness_session_id: Some("claude-native-1".to_string()),
        client_key: Some(format!("ck_{name}")),
        cwd: Some("/tmp/claude".to_string()),
        project: project.to_string(),
        current_work: None,
        presence: Some("online".to_string()),
        paused: false,
        paused_by: None,
        callback_url: None,
        last_heartbeat: None,
        created_at: 0,
        transport: Some(transport.to_string()),
        metadata_json: None,
        agent_id: None,
    }
}

#[tokio::test]
async fn launch_identity_without_name_stages_stable_id_only() {
    let store = mem_store().await;
    let state = AppState::wire(store, &Config::default());
    let session = SessionId("s_stage_launch".into());
    let req = SpawnRequest {
        kind: Harness::Codex,
        name: None,
        identity_policy: None,
        cwd: None,
        project: None,
        role: None,
        initial_prompt: None,
        resume: None,
        harness_args: Vec::new(),
        headless: true,
        backend: None,
    };

    let identity = state
        .resolve_launch_identity_for_session(&req, "default", &session)
        .await
        .unwrap();

    assert_eq!(identity.agent_id, "a_s_stage_launch");
    assert_eq!(identity.name, None);
    assert_eq!(identity.project, "default");
}

#[tokio::test]
async fn claude_daemon_revive_tail_uses_stored_native_resume_id() {
    let store = mem_store().await;
    let state = AppState::wire_pty(store.clone(), &Config::default());
    let row = owner_row("clara", "default", "pty");
    let repo = nexus_harness_claude::storage::ClaudeRuntimeStateRepo::new(&store);
    repo.upsert_launch(nexus_harness_claude::storage::ClaudeRuntimeLaunch {
        runtime_id: row.session_id.clone(),
        bridge_dir: PathBuf::from("/tmp/claude-bridge-clara"),
        claude_session_id: None,
        launch_cwd: PathBuf::from("/tmp/claude"),
        transcript_path: None,
        bridge_pid: None,
        hook_pids_json: None,
    })
    .await
    .unwrap();
    repo.set_claude_session_id(&row.session_id, "claude-native-clara")
        .await
        .unwrap();

    let tail = state
        .headed_revive_tail_for_row(&row, Harness::Claude)
        .await
        .unwrap();

    assert_eq!(tail, ["--resume", "claude-native-clara"]);
}

#[tokio::test]
async fn claude_daemon_revive_tail_rejects_missing_native_resume_id() {
    let store = mem_store().await;
    let state = AppState::wire_pty(store, &Config::default());
    let mut row = owner_row("clara", "default", "pty");
    row.harness_session_id = None;

    let err = state
        .headed_revive_tail_for_row(&row, Harness::Claude)
        .await
        .unwrap_err();

    assert_eq!(err.code, nexus_contracts::codes::INVALID_PARAMS);
    assert!(
        err.message.contains("--resume"),
        "error should explain exact Claude resume requirement: {}",
        err.message
    );
}

#[tokio::test]
async fn codex_resume_finish_ignores_lifecycle_telemetry_failure() {
    let store = mem_store().await;
    let session = SessionId("s_otto".into());
    Sessions::new(&store)
        .create(NewSession {
            session_id: session.clone(),
            name: Some("otto".into()),
            agent: Some("codex".into()),
            kind: kind_token(Kind::Agent).into(),
            role: None,
            tier: tier_token(Tier::Agent).into(),
            harness_session_id: Some("codex-thread-1".into()),
            client_key: Some("ck_otto".into()),
            cwd: Some("/tmp/otto".into()),
            project: "default".into(),
            transport: Some("codex-appserver".into()),
        })
        .await
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE sessions SET presence = 'offline' WHERE session_id = ?1",
            libsql::params![session.0.clone()],
        )
        .await
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO messages (
                message_id, from_name, kind, to_name, body, provenance, project, created_at
             ) VALUES (
                'm_codex_resume_pending', 'operator', 'dm', 'otto', 'resume me',
                '{\"from\":\"operator\",\"kind\":\"agent\",\"thread\":null,\"topic\":null,\"stamp\":null}',
                'default', 1
             )",
            (),
        )
        .await
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO in_flight (
                in_flight_id, message_id, recipient_session, state
             ) VALUES (
                'if_codex_resume_pending', 'm_codex_resume_pending', ?1, 'pending'
             )",
            libsql::params![session.0.clone()],
        )
        .await
        .unwrap();
    store
        .conn
        .execute("DROP TABLE developer_events", ())
        .await
        .unwrap();
    let state = AppState::wire(store.clone(), &Config::default());

    let returned = state
        .finish_codex_appserver_resume("otto", &session, "default", false)
        .await
        .unwrap();

    assert_eq!(returned, session);
    let row = Sessions::new(&store)
        .find_by_session_id(&session)
        .await
        .unwrap()
        .expect("codex session remains addressable");
    assert_eq!(row.presence.as_deref(), Some("online"));
    assert!(row.last_heartbeat.is_some());

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(500);
    loop {
        let state: String = store
            .conn
            .query(
                "SELECT state FROM in_flight WHERE in_flight_id = 'if_codex_resume_pending'",
                (),
            )
            .await
            .unwrap()
            .next()
            .await
            .unwrap()
            .unwrap()
            .get(0)
            .unwrap();
        if state != "pending" {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "Codex resume must materialize online before ringing pending mail"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn pending_respawn_backoff_caps_and_tombstones_after_threshold() {
    assert_eq!(pending_respawn_delay_ms(1), PENDING_RESPAWN_BASE_BACKOFF_MS);
    assert_eq!(
        pending_respawn_delay_ms(3),
        PENDING_RESPAWN_BASE_BACKOFF_MS * 4
    );
    assert_eq!(pending_respawn_delay_ms(99), PENDING_RESPAWN_MAX_BACKOFF_MS);

    let store = mem_store().await;
    let state = AppState::wire(store, &Config::default());
    let session = SessionId("s_backoff".into());
    let first = state.record_pending_respawn_failure(&session, 1_000);
    assert_eq!(first.failures, 1);
    assert!(state.pending_respawn_backoff_for(&session, 1_001).is_some());
    assert!(state
        .pending_respawn_backoff_for(&session, first.next_retry_at)
        .is_none());

    let second = state.record_pending_respawn_failure(&session, first.next_retry_at);
    let third = state.record_pending_respawn_failure(&session, second.next_retry_at);
    assert_eq!(third.failures, PENDING_RESPAWN_TOMBSTONE_FAILURES);

    state.clear_pending_respawn_backoff(&session);
    assert!(state
        .pending_respawn_backoff_for(&session, third.next_retry_at - 1)
        .is_none());
}

#[tokio::test]
async fn first_failed_bootstrap_revive_keeps_delivery_pending() {
    let store = mem_store().await;
    let state = AppState::wire_with_registry(
        store.clone(),
        &Config::default(),
        nexus_agent::AdapterRegistry::new(),
    );
    let session = SessionId("s_bootstrap_pull_consumer".into());
    Sessions::new(&store)
        .create(NewSession {
            session_id: session.clone(),
            name: Some("bootstrap-pull-consumer".into()),
            agent: Some("codex".into()),
            kind: kind_token(Kind::Agent).into(),
            role: None,
            tier: tier_token(Tier::Agent).into(),
            harness_session_id: None,
            client_key: Some("ck_bootstrap_pull_consumer".into()),
            cwd: None,
            project: "default".into(),
            transport: Some("acp".into()),
        })
        .await
        .unwrap();
    let message = Message {
        id: nexus_contracts::ids::MessageId("m_bootstrap_pull_consumer".into()),
        project: nexus_contracts::ids::ProjectId("default".into()),
        from: "operator".into(),
        scope: nexus_contracts::enums::Scope::Dm,
        thread: None,
        topic: None,
        body: "arrived while listener was bootstrapping".into(),
        summary: None,
        provenance: nexus_contracts::message::Provenance {
            from: "operator".into(),
            kind: Kind::Human,
            thread: None,
            topic: None,
            stamp: None,
        },
        created_at: now(),
    };
    Messages::new(&store).insert(&message).await.unwrap();
    Inbox::new(&store)
        .enqueue(&message.id, &session)
        .await
        .unwrap();

    state.wake_dm_agent(
        Some("bootstrap-pull-consumer".into()),
        None,
        "default".into(),
        message.id.clone(),
    );

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        if state.pending_respawn_backoff_for(&session, now()).is_some() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the first failed revive must enter backoff instead of terminalizing the row"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    let mut rows = store
        .conn
        .query(
            "SELECT state, error_code, attempt_count FROM in_flight WHERE message_id = ?1",
            libsql::params![message.id.0],
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().expect("delivery row");
    assert_eq!(row.get::<String>(0).unwrap(), "pending");
    assert_eq!(row.get::<Option<String>>(1).unwrap(), None);
    assert_eq!(row.get::<i64>(2).unwrap(), 0);
}

#[tokio::test]
async fn third_failed_bootstrap_revive_moves_delivery_to_dlq() {
    let store = mem_store().await;
    let state = AppState::wire_with_registry(
        store.clone(),
        &Config::default(),
        nexus_agent::AdapterRegistry::new(),
    );
    let session = SessionId("s_exhausted_pull_consumer".into());
    Sessions::new(&store)
        .create(NewSession {
            session_id: session.clone(),
            name: Some("exhausted-pull-consumer".into()),
            agent: Some("codex".into()),
            kind: kind_token(Kind::Agent).into(),
            role: None,
            tier: tier_token(Tier::Agent).into(),
            harness_session_id: None,
            client_key: Some("ck_exhausted_pull_consumer".into()),
            cwd: None,
            project: "default".into(),
            transport: Some("acp".into()),
        })
        .await
        .unwrap();
    let message = Message {
        id: nexus_contracts::ids::MessageId("m_exhausted_pull_consumer".into()),
        project: nexus_contracts::ids::ProjectId("default".into()),
        from: "operator".into(),
        scope: nexus_contracts::enums::Scope::Dm,
        thread: None,
        topic: None,
        body: "cannot remain pending forever".into(),
        summary: None,
        provenance: nexus_contracts::message::Provenance {
            from: "operator".into(),
            kind: Kind::Human,
            thread: None,
            topic: None,
            stamp: None,
        },
        created_at: now(),
    };
    Messages::new(&store).insert(&message).await.unwrap();
    Inbox::new(&store)
        .enqueue(&message.id, &session)
        .await
        .unwrap();
    state.record_pending_respawn_failure(&session, 0);
    state.record_pending_respawn_failure(&session, 0);

    state.wake_dm_agent(
        Some("exhausted-pull-consumer".into()),
        None,
        "default".into(),
        message.id.clone(),
    );

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        let mut rows = store
            .conn
            .query(
                "SELECT state, error_code, attempt_count FROM in_flight WHERE message_id = ?1",
                libsql::params![message.id.0.clone()],
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().expect("delivery row");
        let delivery_state = row.get::<String>(0).unwrap();
        if delivery_state == "error" {
            assert_eq!(
                row.get::<Option<String>>(1).unwrap().as_deref(),
                Some(nexus_store::repos::inbox::TARGET_UNREACHABLE_ERROR_CODE)
            );
            assert_eq!(row.get::<i64>(2).unwrap(), 0);
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the third failed revive must terminalize the delivery; state={delivery_state}; backoff={:?}",
            state.pending_respawn_backoff_for(&session, now())
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn respawn_pending_agents_preserves_dead_recipient_failure_in_dlq() {
    let store = mem_store().await;
    let state = AppState::wire(store.clone(), &Config::default());
    let message = Message {
        id: nexus_contracts::ids::MessageId("m_orphan_respawn".into()),
        project: nexus_contracts::ids::ProjectId("default".into()),
        from: "operator".into(),
        scope: nexus_contracts::enums::Scope::Dm,
        thread: None,
        topic: None,
        body: "cannot deliver".into(),
        summary: None,
        provenance: nexus_contracts::message::Provenance {
            from: "operator".into(),
            kind: Kind::Human,
            thread: None,
            topic: None,
            stamp: None,
        },
        created_at: now(),
    };
    nexus_store::repos::Messages::new(&store)
        .insert(&message)
        .await
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO in_flight (in_flight_id, message_id, recipient_session, \
             recipient_agent_id, state) VALUES \
             ('if_orphan_respawn', 'm_orphan_respawn', 's_missing_respawn', NULL, 'pending')",
            (),
        )
        .await
        .unwrap();

    state.respawn_pending_agents().await;

    let mut rows = store
        .conn
        .query(
            "SELECT state, error_code FROM in_flight \
             WHERE in_flight_id = 'if_orphan_respawn'",
            (),
        )
        .await
        .unwrap();
    let row = rows
        .next()
        .await
        .unwrap()
        .expect("dead-letter row retained");
    assert_eq!(row.get::<String>(0).unwrap(), "error");
    assert_eq!(row.get::<String>(1).unwrap(), "target_dead");
}

/// After `wire_pty`, both the PTY supervisor (headed path) AND the ACP `Agent` (headless path)
/// must be mounted so the daemon can serve both launch modes in the same process.
///
/// - `pty_supervisor().is_some()` — the supervisor is present for headed/tmux launches (Task 7)
/// - `agent_concrete.is_some()`  — the ACP `Agent` is present for headless ACP launches (Task 6)
///
/// Before this task, `wire_pty` delegated to `wire_with_turn_exec`, which hard-wired
/// `agent_concrete = None`, making headless launches impossible even though the `RoutingTurnExec`
/// was ready to dispatch them.
#[tokio::test]
async fn wire_pty_mounts_both_pty_supervisor_and_acp_agent() {
    let store = mem_store().await;
    let config = Config::default();
    let state = AppState::wire_pty(store, &config);

    assert!(
        state.pty_supervisor().is_some(),
        "wire_pty must install a PtySupervisor for headed (PTY/tmux) launches"
    );
    assert!(
        state.has_acp_agent(),
        "wire_pty must install the ACP Agent (agent_concrete) for headless launches"
    );
}

/// Regression guard: `wire_with_registry` and `wire_with_turn_exec` callers are unchanged.
///
/// - `wire_with_registry` must still have `agent_concrete = Some` (its own ACP agent) and no PTY
///   supervisor (`pty = None`).
/// - `wire_with_turn_exec` must still have `agent_concrete = None` and no PTY supervisor.
///
/// These assert that our Task 5 changes to `wire_pty` did NOT accidentally mutate the behaviour
/// of the other two wiring paths.
#[tokio::test]
async fn wire_with_registry_and_wire_with_turn_exec_unchanged_by_task5() {
    use nexus_contracts::AgentTurnExecutionPort;

    // wire_with_registry: ACP agent present, no PTY supervisor
    let store_a = mem_store().await;
    let state_a = AppState::wire_with_registry(
        store_a,
        &Config::default(),
        nexus_agent::AdapterRegistry::with_builtins(),
    );
    assert!(
        state_a.has_acp_agent(),
        "wire_with_registry must still set agent_concrete"
    );
    assert!(
        state_a.pty_supervisor().is_none(),
        "wire_with_registry must not install a PTY supervisor"
    );

    // wire_with_turn_exec: no ACP agent, no PTY supervisor
    struct NoopTurnExec;
    #[async_trait::async_trait]
    impl AgentTurnExecutionPort for NoopTurnExec {
        async fn inject_turn(
            &self,
            _r: &nexus_contracts::ids::SessionId,
            _b: &nexus_contracts::NexusBatch,
        ) -> nexus_contracts::ports::PortResult<()> {
            Ok(())
        }
        async fn launch(
            &self,
            _r: nexus_contracts::SpawnRequest,
        ) -> nexus_contracts::ports::PortResult<nexus_contracts::SpawnResponse> {
            unimplemented!()
        }
        async fn remove(
            &self,
            _r: nexus_contracts::RemoveRequest,
        ) -> nexus_contracts::ports::PortResult<nexus_contracts::RemoveResponse> {
            unimplemented!()
        }
        async fn prompt(
            &self,
            _r: &nexus_contracts::ids::SessionId,
            _t: String,
        ) -> nexus_contracts::ports::PortResult<()> {
            Ok(())
        }
    }
    let store_b = mem_store().await;
    let state_b =
        AppState::wire_with_turn_exec(store_b, &Config::default(), Arc::new(NoopTurnExec));
    assert!(
        !state_b.has_acp_agent(),
        "wire_with_turn_exec must still leave agent_concrete = None"
    );
    assert!(
        state_b.pty_supervisor().is_none(),
        "wire_with_turn_exec must not install a PTY supervisor"
    );
}

#[tokio::test]
async fn codex_thread_discovery_callback_waits_for_session_registration() {
    use nexus_contracts::AgentTurnExecutionPort;
    use nexus_store::repos::{NewSession, Sessions};

    struct CallbackNoopTurnExec;
    #[async_trait::async_trait]
    impl AgentTurnExecutionPort for CallbackNoopTurnExec {
        async fn inject_turn(
            &self,
            _r: &nexus_contracts::ids::SessionId,
            _b: &nexus_contracts::NexusBatch,
        ) -> nexus_contracts::ports::PortResult<()> {
            Ok(())
        }
        async fn launch(
            &self,
            _r: nexus_contracts::SpawnRequest,
        ) -> nexus_contracts::ports::PortResult<nexus_contracts::SpawnResponse> {
            unimplemented!()
        }
        async fn remove(
            &self,
            _r: nexus_contracts::RemoveRequest,
        ) -> nexus_contracts::ports::PortResult<nexus_contracts::RemoveResponse> {
            unimplemented!()
        }
        async fn prompt(
            &self,
            _r: &nexus_contracts::ids::SessionId,
            _t: String,
        ) -> nexus_contracts::ports::PortResult<()> {
            Ok(())
        }
    }

    let store = mem_store().await;
    let state = AppState::wire_with_turn_exec(
        store.clone(),
        &Config::default(),
        Arc::new(CallbackNoopTurnExec),
    );
    let callback = state.codex_thread_persistence_callback();
    let session = SessionId("s_codex_discovery_race".into());
    let thread_id = "codex-thread-race".to_string();

    callback(session.clone(), thread_id.clone());
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    Sessions::new(&store)
        .create(NewSession {
            session_id: session.clone(),
            name: Some("otto".into()),
            agent: Some("codex".into()),
            kind: kind_token(Kind::Agent).into(),
            role: None,
            tier: tier_token(Tier::Agent).into(),
            harness_session_id: None,
            client_key: Some("ck_otto".into()),
            cwd: Some("/tmp/otto".into()),
            project: "default".into(),
            transport: Some("codex-appserver".into()),
        })
        .await
        .unwrap();
    Sessions::new(&store)
        .set_agent_id(&session, "a_otto")
        .await
        .unwrap();

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
    let binding = loop {
        if let Some(binding) = NativeThreadBindings::new(&store)
            .find("codex", &thread_id)
            .await
            .unwrap()
        {
            break binding;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "callback did not claim durable Codex thread binding after registration"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    };

    let row = Sessions::new(&store)
        .find_by_session_id(&session)
        .await
        .unwrap()
        .expect("session row should exist");
    assert_eq!(row.harness_session_id.as_deref(), Some(thread_id.as_str()));
    assert_eq!(binding.agent_id, "a_otto");
    assert_eq!(binding.last_runtime_id.as_deref(), Some(session.0.as_str()));
}

// ── revive_route truth table ─────────────────────────────────────────────────────────────────
// Pure/hermetic — no I/O.

#[test]
fn revive_route_acp_transport() {
    assert_eq!(revive_route(Some("acp")), ReviveRoute::Acp);
}

#[test]
fn revive_route_pty_transport() {
    assert_eq!(revive_route(Some("pty")), ReviveRoute::Pty);
}

#[test]
fn revive_route_none_transport_is_acp() {
    // A NULL row (self-registered agent or legacy) has no daemon-owned tmux harness to respawn,
    // so revive goes over ACP — the daemon can always synthesize a fresh ACP session. Defaulting
    // to Pty here stranded a registered agent's boot-pending DM (the regression this guards).
    assert_eq!(revive_route(None), ReviveRoute::Acp);
}

#[test]
fn revive_route_unknown_transport_is_acp() {
    // Only the explicit "pty" token has a tmux harness; any unknown future token revives over ACP.
    assert_eq!(revive_route(Some("weird")), ReviveRoute::Acp);
}

#[test]
fn revive_route_codex_appserver_uses_codex_appserver_backend() {
    // Headed codex sessions own a Codex app-server + remote TUI, not ACP or PTY scraping.
    assert_eq!(
        revive_route(Some("codex-appserver")),
        ReviveRoute::CodexAppServer
    );
}

#[test]
fn revive_route_opencode_plugin_uses_plugin_backend() {
    // Headed OpenCode native-plugin sessions own a daemon-local loopback bridge. After a daemon
    // restart, reviving them as ACP (or adopting tmux as the input) leaves pending delivery
    // disconnected from the plugin completion callback.
    assert_eq!(
        revive_route(Some("opencode-plugin")),
        ReviveRoute::OpenCodePlugin
    );
}

// ── teardown_route truth table ────────────────────────────────────────────────────────────────

#[test]
fn teardown_route_acp_transport() {
    assert_eq!(teardown_route(Some("acp")), TeardownRoute::Acp);
}

#[test]
fn teardown_route_pty_transport() {
    assert_eq!(teardown_route(Some("pty")), TeardownRoute::Pty);
}

#[test]
fn teardown_route_none_transport_is_pty() {
    assert_eq!(teardown_route(None), TeardownRoute::Pty);
}

#[test]
fn teardown_route_unknown_transport_is_pty() {
    assert_eq!(teardown_route(Some("weird")), TeardownRoute::Pty);
}

/// Regression: the revive path maps the stored `agent` label back to a Harness to re-resolve
/// the adapter — opencode/hermes must NOT fall through to the Claude default (which would
/// respawn them as claude on revive).
#[test]
fn harness_from_token_resolves_each_runtime() {
    assert_eq!(harness_from_token(Some("codex")), Harness::Codex);
    assert_eq!(harness_from_token(Some("opencode")), Harness::OpenCode);
    assert_eq!(harness_from_token(Some("hermes")), Harness::Hermes);
    assert_eq!(harness_from_token(Some("pi")), Harness::Pi);
    assert_eq!(harness_from_token(Some("other")), Harness::Other);
    // Unknown / legacy / NULL → Claude (historical default for pre-label rows).
    assert_eq!(harness_from_token(None), Harness::Claude);
    assert_eq!(harness_from_token(Some("claude")), Harness::Claude);
}

#[test]
fn headed_runtime_from_stored_agent_token_preserves_legacy_attach_behavior() {
    assert_eq!(
        headed_runtime_from_agent_token(Some("claude")),
        HeadedRuntimeKind::ClaudeNative
    );
    assert_eq!(
        headed_runtime_from_agent_token(Some("opencode")),
        HeadedRuntimeKind::OpenCodePlugin
    );
    assert_eq!(
        headed_runtime_from_agent_token(Some("hermes")),
        HeadedRuntimeKind::HermesGateway
    );
    assert_eq!(
        headed_runtime_from_agent_token(Some("codex")),
        HeadedRuntimeKind::CodexAppServer
    );
    assert_eq!(
        headed_runtime_from_agent_token(None),
        HeadedRuntimeKind::Screen
    );
    assert_eq!(
        headed_runtime_from_agent_token(Some("legacy-cat")),
        HeadedRuntimeKind::Screen
    );
}

// ── launch_route truth table ─────────────────────────────────────────────────────────────────
// These are pure/hermetic — no spawning, no I/O, no async.

/// Headed claude with PTY present → spawn with the "claude" binary.
#[test]
fn launch_route_headed_claude_pty_present() {
    assert_eq!(
        launch_route(false, true, Harness::Claude),
        LaunchRoute::Headed("claude"),
    );
}

/// Headed codex with PTY present → spawn with the "codex" binary.
#[test]
fn launch_route_headed_codex_pty_present() {
    assert_eq!(
        launch_route(false, true, Harness::Codex),
        LaunchRoute::Headed("codex"),
    );
}

/// Headed Pi with PTY present → error: no TUI binary.
#[test]
fn launch_route_headed_pi_no_tui_binary() {
    assert_eq!(
        launch_route(false, true, Harness::Pi),
        LaunchRoute::NoTuiBinary,
    );
}

/// Headless claude with PTY present → ACP path (the core fix: headless beats the pty branch).
#[test]
fn launch_route_headless_claude_pty_present() {
    assert_eq!(
        launch_route(true, true, Harness::Claude),
        LaunchRoute::Headless,
    );
}

/// Headed claude with NO PTY → degrade to ACP (preserves mock-port behaviour).
#[test]
fn launch_route_headed_claude_no_pty() {
    assert_eq!(
        launch_route(false, false, Harness::Claude),
        LaunchRoute::Headless,
    );
}

/// Headless claude with NO PTY → still ACP.
#[test]
fn launch_route_headless_claude_no_pty() {
    assert_eq!(
        launch_route(true, false, Harness::Claude),
        LaunchRoute::Headless,
    );
}

#[tokio::test]
async fn admin_remove_kill_targets_the_resolved_pty_session_only() {
    use nexus_store::repos::{NewSession, Sessions};
    use portable_pty::PtySize;

    let store = mem_store().await;
    let state = AppState::wire_pty(store.clone(), &Config::default());
    let supervisor = state
        .pty_supervisor()
        .expect("wire_pty must install a PTY supervisor")
        .clone();
    let cwd = std::env::temp_dir().join(format!("nexus-admin-remove-kill-{}", nexus_common::now()));
    std::fs::create_dir_all(&cwd).unwrap();
    let cwd = cwd.to_string_lossy().into_owned();
    let size = PtySize {
        rows: 24,
        cols: 80,
        pixel_width: 0,
        pixel_height: 0,
    };
    let target = SessionId("s_remove_target_pty".into());
    let survivor = SessionId("s_keep_survivor_pty".into());

    for (session, name) in [(&target, "remove-target"), (&survivor, "keep-survivor")] {
        Sessions::new(&store)
            .create(NewSession {
                session_id: session.clone(),
                name: Some(name.to_string()),
                agent: Some("claude".to_string()),
                kind: kind_token(Kind::Agent).to_string(),
                role: None,
                tier: tier_token(Tier::Agent).to_string(),
                harness_session_id: None,
                client_key: Some(format!("ck_{name}")),
                cwd: Some(cwd.clone()),
                project: "default".to_string(),
                transport: Some("pty".to_string()),
            })
            .await
            .unwrap();
        supervisor
            .launch(
                session,
                "cat",
                name,
                Some(name),
                "default",
                &format!("ck_{name}"),
                "/usr/bin/nexus",
                &cwd,
                size,
            )
            .await
            .unwrap();
    }

    let removed = state
        .remove_agent(&admin_caller(), None, "remove-target", true)
        .await
        .unwrap();

    assert_eq!(removed.status, "removed");
    assert!(
        supervisor.pty_backend_kind(&target).is_none(),
        "admin.remove --kill must terminate the resolved target session"
    );
    assert!(
        supervisor.pty_backend_kind(&survivor).is_some(),
        "admin.remove --kill must not re-resolve and kill another live session"
    );
    let target_row = Sessions::new(&store)
        .find_by_session_id(&target)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(target_row.presence.as_deref(), Some("offline"));

    supervisor.kill(&survivor);
}

// ── Source lifecycle tests ────────────────────────────────────────────────────────────────────

fn admin_caller() -> Caller {
    Caller {
        agent_id: None,
        session: nexus_contracts::SessionId("s_test_admin".into()),
        name: "operator".into(),
        project: "default".into(),
        tier: Tier::Admin,
    }
}

fn agent_caller() -> Caller {
    Caller {
        agent_id: None,
        session: nexus_contracts::SessionId("s_test_agent".into()),
        name: "bot".into(),
        project: "default".into(),
        tier: Tier::Agent,
    }
}

async fn source_state() -> AppState {
    let store = mem_store().await;
    AppState::wire_with_registry(
        store,
        &Config::default(),
        nexus_agent::AdapterRegistry::with_builtins(),
    )
}

#[tokio::test]
async fn register_source_returns_token_and_stored_row_matches() {
    let state = source_state().await;
    let caller = admin_caller();
    let resp = state
        .register_source(
            &caller,
            nexus_contracts::SourceRegisterRequest {
                name: "gh-ci".into(),
                topic: Some("ci.events".into()),
            },
        )
        .await
        .unwrap();

    // Response carries the plaintext token.
    assert!(
        resp.token.starts_with("src_"),
        "token must have src_ prefix"
    );
    assert_eq!(resp.source.name, "gh-ci");
    assert_eq!(resp.source.topic, "ci.events");
    assert!(resp.source.enabled);
    assert_eq!(resp.source.last_fired_at, None);

    // Stored token equals the returned plaintext.
    let row = nexus_store::repos::Sources::new(&state.store)
        .find("gh-ci")
        .await
        .unwrap()
        .expect("row must exist");
    assert_eq!(
        row.token, resp.token,
        "stored token must equal returned plaintext"
    );
}

#[tokio::test]
async fn register_source_topic_defaults_to_name() {
    let state = source_state().await;
    let resp = state
        .register_source(
            &admin_caller(),
            nexus_contracts::SourceRegisterRequest {
                name: "cron".into(),
                topic: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        resp.source.topic, "cron",
        "default topic must equal the name"
    );
}

#[tokio::test]
async fn show_and_list_reflect_registered_source_without_token() {
    let state = source_state().await;
    let caller = admin_caller();
    state
        .register_source(
            &caller,
            nexus_contracts::SourceRegisterRequest {
                name: "src-a".into(),
                topic: Some("t".into()),
            },
        )
        .await
        .unwrap();

    // show returns the source (no token field).
    let shown = state.show_source("src-a").await.unwrap();
    assert_eq!(shown.name, "src-a");

    // list contains it.
    let list = state.list_sources().await.unwrap();
    assert_eq!(list.sources.len(), 1);
    assert_eq!(list.sources[0].name, "src-a");
}

#[tokio::test]
async fn enable_disable_flips_enabled_field() {
    let state = source_state().await;
    state
        .register_source(
            &admin_caller(),
            nexus_contracts::SourceRegisterRequest {
                name: "toggle".into(),
                topic: None,
            },
        )
        .await
        .unwrap();

    let disabled = state.set_source_enabled("toggle", false).await.unwrap();
    assert!(!disabled.enabled, "should be disabled");

    let enabled = state.set_source_enabled("toggle", true).await.unwrap();
    assert!(enabled.enabled, "should be re-enabled");
}

#[tokio::test]
async fn rotate_changes_token_and_returns_new_plaintext() {
    let state = source_state().await;
    let caller = admin_caller();
    let original = state
        .register_source(
            &caller,
            nexus_contracts::SourceRegisterRequest {
                name: "rotor".into(),
                topic: None,
            },
        )
        .await
        .unwrap();

    let rotated = state.rotate_source(&caller, "rotor").await.unwrap();
    assert_ne!(
        rotated.token, original.token,
        "rotated token must differ from original"
    );
    assert!(
        rotated.token.starts_with("src_"),
        "rotated token must have src_ prefix"
    );

    // Stored token now equals the rotated value.
    let row = nexus_store::repos::Sources::new(&state.store)
        .find("rotor")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.token, rotated.token);
}

#[tokio::test]
async fn source_token_returns_current_plaintext() {
    let state = source_state().await;
    let caller = admin_caller();
    let orig = state
        .register_source(
            &caller,
            nexus_contracts::SourceRegisterRequest {
                name: "vault".into(),
                topic: None,
            },
        )
        .await
        .unwrap();

    let tok_resp = state.source_token(&caller, "vault").await.unwrap();
    assert_eq!(tok_resp.token, orig.token);
    assert_eq!(tok_resp.name, "vault");
}

#[tokio::test]
async fn delete_source_removes_it() {
    let state = source_state().await;
    state
        .register_source(
            &admin_caller(),
            nexus_contracts::SourceRegisterRequest {
                name: "gone".into(),
                topic: None,
            },
        )
        .await
        .unwrap();

    state.delete_source("gone").await.unwrap();

    let not_found = state.show_source("gone").await;
    assert!(not_found.is_err());
    assert_eq!(
        not_found.unwrap_err().code,
        nexus_contracts::codes::NOT_FOUND
    );
}

#[tokio::test]
async fn non_admin_caller_cannot_register_rotate_or_get_token() {
    let state = source_state().await;
    let agent = agent_caller();

    // register → UNAUTHORIZED
    let e = state
        .register_source(
            &agent,
            nexus_contracts::SourceRegisterRequest {
                name: "denied".into(),
                topic: None,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(e.code, nexus_contracts::codes::UNAUTHORIZED);

    // First register with admin so rotate/token have a target.
    state
        .register_source(
            &admin_caller(),
            nexus_contracts::SourceRegisterRequest {
                name: "allowed".into(),
                topic: None,
            },
        )
        .await
        .unwrap();

    // rotate → UNAUTHORIZED
    let e2 = state.rotate_source(&agent, "allowed").await.unwrap_err();
    assert_eq!(e2.code, nexus_contracts::codes::UNAUTHORIZED);

    // source_token → UNAUTHORIZED
    let e3 = state.source_token(&agent, "allowed").await.unwrap_err();
    assert_eq!(e3.code, nexus_contracts::codes::UNAUTHORIZED);
}

#[tokio::test]
async fn register_duplicate_name_returns_mapped_error() {
    let state = source_state().await;
    let caller = admin_caller();
    state
        .register_source(
            &caller,
            nexus_contracts::SourceRegisterRequest {
                name: "dup".into(),
                topic: None,
            },
        )
        .await
        .unwrap();

    let e = state
        .register_source(
            &caller,
            nexus_contracts::SourceRegisterRequest {
                name: "dup".into(),
                topic: None,
            },
        )
        .await
        .unwrap_err();
    // DuplicateName maps to DUPLICATE_NAME (or similar non-success code).
    assert_ne!(e.code, 0, "duplicate must produce an error code");
}

#[tokio::test]
async fn show_and_token_on_missing_source_return_not_found() {
    let state = source_state().await;
    let caller = admin_caller();

    let e1 = state.show_source("no-such").await.unwrap_err();
    assert_eq!(e1.code, nexus_contracts::codes::NOT_FOUND);

    let e2 = state.source_token(&caller, "no-such").await.unwrap_err();
    assert_eq!(e2.code, nexus_contracts::codes::NOT_FOUND);

    let e3 = state.rotate_source(&caller, "no-such").await.unwrap_err();
    assert_eq!(e3.code, nexus_contracts::codes::NOT_FOUND);
}
