use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

use nexus::cli::commands::resume::{self, ResumeArgs};
use nexus::cli::read_client::ReadClient;
use nexus::cli::store_client::StoreClient;
use nexus::daemon::AppState;
use nexus_common::Config;
use nexus_contracts::{
    codes, Kind, Presence, RegisterRequest, SessionId, SpawnRequest, SpawnResponse, Tier,
};
use nexus_harness_codex::storage::{CodexRuntimeLaunch, CodexRuntimeStateRepo};
use nexus_store::command_kinds;
use nexus_store::repos::{CommandIntents, Sessions};
use nexus_store::Store;
use std::path::PathBuf;

async fn state() -> AppState {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    AppState::wire(store, &Config::default())
}

fn agent(
    name: &str,
    client_key: &str,
    harness: nexus_contracts::HarnessId,
    cwd: &str,
) -> RegisterRequest {
    RegisterRequest {
        agent_id: None,
        name: Some(name.into()),
        harness,
        harness_session_id: format!("hs_{client_key}"),
        project: "default".into(),
        client_key: client_key.into(),
        runtime_credential: None,
        tier: Tier::Agent,
        kind: Some(Kind::Agent),
        role: None,
        cwd: Some(cwd.into()),
    }
}

fn store_client(store: Arc<Store>) -> StoreClient {
    StoreClient::from_store_with_caller_for_tests(
        store,
        "operator",
        "default",
        Some("ck_operator".into()),
        Tier::Admin,
        Kind::Human,
    )
}

fn read_client(store: Arc<Store>) -> ReadClient {
    ReadClient::from_store_with_caller_for_tests(
        store,
        "operator",
        "default",
        Some("local-operator".into()),
        None,
        Tier::Admin,
    )
}

async fn wait_for_nth_command(store: &Store, count: usize) -> nexus_store::repos::CommandIntentRow {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let mut rows = store
            .conn
            .query(
                "SELECT command_id FROM command_intents ORDER BY created_at, command_id",
                (),
            )
            .await
            .unwrap();
        let mut ids = Vec::new();
        while let Some(row) = rows.next().await.unwrap() {
            ids.push(row.get::<String>(0).unwrap());
        }
        drop(rows);
        if ids.len() >= count {
            return CommandIntents::new(store)
                .get(&ids[count - 1])
                .await
                .unwrap()
                .unwrap();
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for command row"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn command_count(store: &Store, kind: Option<&str>) -> usize {
    let (sql, args): (&str, Vec<String>) = if let Some(kind) = kind {
        (
            "SELECT COUNT(*) FROM command_intents WHERE kind = ?1",
            vec![kind.to_string()],
        )
    } else {
        ("SELECT COUNT(*) FROM command_intents", Vec::new())
    };
    let mut rows = if args.is_empty() {
        store.conn.query(sql, ()).await.unwrap()
    } else {
        store.conn.query(sql, [args[0].as_str()]).await.unwrap()
    };
    let row = rows.next().await.unwrap().unwrap();
    row.get::<i64>(0).unwrap() as usize
}

async fn seed_codex_resume(state: &AppState, name: &str, thread_id: &str) -> SessionId {
    let registered = state
        .identity
        .register(agent(name, "ck_codex", hid("codex"), "/work/codex"))
        .await
        .unwrap();
    let sessions = Sessions::new(&state.store);
    sessions
        .set_transport(&registered.session_id, "codex-appserver")
        .await
        .unwrap();
    sessions
        .set_presence(&registered.session_id, Presence::Offline)
        .await
        .unwrap();
    let codex_state = CodexRuntimeStateRepo::new(&state.store);
    codex_state
        .upsert_launch(CodexRuntimeLaunch {
            runtime_id: registered.session_id.clone(),
            codex_thread_id: None,
            codex_home: PathBuf::from("/tmp/codex-home"),
            app_server_sock: PathBuf::from("/tmp/codex.sock"),
            app_server_pid: None,
            mcp_sidecar_pids_json: None,
            app_server_adopted: false,
        })
        .await
        .unwrap();
    codex_state
        .set_thread(&registered.session_id, thread_id, None)
        .await
        .unwrap();
    registered.session_id
}

#[tokio::test]
async fn cli_resume_submits_detached_launch_without_attach_event() {
    let state = state().await;
    seed_codex_resume(&state, "rex", "codex-thread-rex").await;
    let store = store_client(state.store.clone());
    let read = read_client(state.store.clone());

    let task = tokio::spawn(async move {
        resume::resume(
            &store,
            &read,
            ResumeArgs {
                target: "rex".into(),
            },
            false,
        )
        .await
    });

    let row = wait_for_nth_command(&state.store, 1).await;
    assert_eq!(row.kind, command_kinds::harness::LAUNCH);
    let req: SpawnRequest = serde_json::from_str(&row.request_json).unwrap();
    assert_eq!(req.name.as_deref(), Some("rex"));
    assert_eq!(req.kind, hid("codex"));
    assert_eq!(req.resume.as_deref(), Some("codex-thread-rex"));
    assert!(req.harness_args.is_empty());
    assert!(!req.headless);
    assert!(req.initial_prompt.is_none());

    CommandIntents::new(&state.store)
        .mark_done(
            &row.command_id,
            &serde_json::to_string(&SpawnResponse {
                session_id: SessionId("s_resumed_rex".into()),
            })
            .unwrap(),
            2,
        )
        .await
        .unwrap();

    assert_eq!(task.await.unwrap(), ExitCode::SUCCESS);
    assert_eq!(
        command_count(&state.store, Some(command_kinds::identity::ATTACH)).await,
        0
    );
}

#[tokio::test]
async fn cli_resume_rejects_missing_resume_id_before_enqueue() {
    let state = state().await;
    let registered = state
        .identity
        .register(agent("hugo", "ck_hugo", hid("claude"), "/work/claude"))
        .await
        .unwrap();
    let sessions = Sessions::new(&state.store);
    sessions
        .set_transport(&registered.session_id, "pty")
        .await
        .unwrap();
    sessions
        .set_harness_session_id(&registered.session_id, "")
        .await
        .unwrap();
    let store = store_client(state.store.clone());
    let read = read_client(state.store.clone());

    let code = resume::resume(
        &store,
        &read,
        ResumeArgs {
            target: "hugo".into(),
        },
        false,
    )
    .await;

    assert_ne!(code, ExitCode::SUCCESS);
    assert_eq!(command_count(&state.store, None).await, 0);
    let err = read.attach_revive_plan("hugo").await.unwrap_err();
    assert_eq!(err.code, codes::INVALID_PARAMS);
    assert!(err.message.contains("--resume"));
}

/// A validated [`nexus_contracts::HarnessId`] from a literal (panics on invalid — test-only).
fn hid(s: &str) -> nexus_contracts::HarnessId {
    nexus_contracts::HarnessId::new(s).expect("valid harness id literal")
}
