//! Store/read parity and actual AppState boot/offline wiring; no native collector or Gateway
//! projection is enabled. The boot-failure fixture has no transport handles to mutate.
use nexus::{cli::read_client::ReadClient, AppState};
use nexus_common::Config;
use nexus_contracts::model_report::RuntimeModelReport;
use nexus_contracts::{AgentRuntimeListRequest, AgentShowRequest, Caller, SessionId, Tier};
use nexus_store::{
    repos::{AgentRuntimes, Agents, NewAgent, NewAgentRuntime, NewSession, Sessions},
    DaemonStore,
};
use std::sync::Arc;

#[tokio::test]
async fn boot_failed_app_offline_preserves_compatibility_presence() {
    let home = tempfile::tempdir().unwrap();
    let path = home.path().join("identity.db");
    let daemon = DaemonStore::open(path.to_str().unwrap()).await.unwrap();
    let store = Arc::new(daemon.compatibility_store());
    let session = SessionId("s_failed_model_boot".into());
    Sessions::new(&store)
        .create(NewSession {
            session_id: session.clone(),
            name: Some("failed-model-boot".into()),
            agent: Some("other".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: None,
            client_key: Some("ck_failed_model_boot".into()),
            cwd: None,
            project: "default".into(),
            transport: None,
        })
        .await
        .unwrap();
    Sessions::new(&store)
        .set_presence(&session, nexus_contracts::Presence::Online)
        .await
        .unwrap();
    // An empty owner is corrupt authority, not merely invalid display JSON. Boot cannot admit
    // observers; an unmanaged writer would mutate compatibility presence before stop detects it.
    store.identity_conn().execute_batch("INSERT INTO agent_runtimes (runtime_id, agent_id, harness, active, presence, started_at, model_observer_token) VALUES ('s_failed_model_boot', 'a_missing', 'other', 1, 'online', 0, '');").await.unwrap();
    let before = Sessions::new(&store)
        .find_by_session_id(&session)
        .await
        .unwrap()
        .unwrap();
    let config = Config {
        db_path: path.to_string_lossy().into_owned(),
        ..Config::default()
    };
    let state = AppState::wire(store.clone(), &config);
    assert!(state.wait_for_runtime_identity_ready().await.is_err());
    assert!(state.mark_session_offline(&session).await.is_err());
    let after = Sessions::new(&store)
        .find_by_session_id(&session)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        after.presence, before.presence,
        "production offline must use the boot-failed coordinator before compatibility writes"
    );
    assert_eq!(after.paused, before.paused);
    assert_eq!(after.last_heartbeat, before.last_heartbeat);
    let mut rows = store.identity_conn().query("SELECT active, presence, stopped_at, model_observer_token, model_report_revision FROM agent_runtimes WHERE runtime_id = 's_failed_model_boot'", ()).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 1);
    assert_eq!(row.get::<String>(1).unwrap(), "online");
    assert_eq!(row.get::<Option<i64>>(2).unwrap(), None);
    assert_eq!(row.get::<String>(3).unwrap(), "");
    assert_eq!(row.get::<i64>(4).unwrap(), 0);
}

#[tokio::test]
async fn populated_canonical_store_report_matches_cli_and_daemon_reads() {
    let home = tempfile::tempdir().unwrap();
    let path = home.path().join("identity.db");
    let daemon = DaemonStore::open(path.to_str().unwrap()).await.unwrap();
    let store = Arc::new(daemon.compatibility_store());
    let config = Config {
        db_path: path.to_string_lossy().into_owned(),
        ..Config::default()
    };
    let state = AppState::wire(store.clone(), &config);
    // Boot correctly invalidates pre-existing observers. Seed this positive read fixture only
    // after that sweep, so parity still compares populated opaque evidence, not two None values.
    state.wait_for_runtime_identity_ready().await.unwrap();
    Agents::new(&store)
        .create(NewAgent {
            agent_id: "a_model_fixture".into(),
            project: "default".into(),
            name: Some("model-fixture".into()),
            default_harness: Some("other".into()),
            role: None,
            tier: Some("agent".into()),
            owner: None,
        })
        .await
        .unwrap();
    let repo = AgentRuntimes::new(&store);
    repo.create(NewAgentRuntime {
        runtime_id: "s_model_fixture".into(),
        agent_id: "a_model_fixture".into(),
        harness: "other".into(),
        cwd: None,
        transport: None,
        presence: Some("online".into()),
        active: true,
    })
    .await
    .unwrap();
    let snapshot:RuntimeModelReport=serde_json::from_value(serde_json::json!({
        "backend":"opaque.new-backend/β", "observerActive":true, "reportRevision":55,
        "configured":{"status":"observed","capability":"supported","observation":{"modelId":" model:opaque/value[mode] ","source":"opaque.new-source/β","observedAt":42}},
        "turnSelected":{"status":"unknown","capability":"unverified"},
        "responseReported":{"status":"unknown","capability":"unsupported"}
    })).unwrap();
    assert!(repo
        .claim_model_observer(
            "s_model_fixture",
            "a_model_fixture",
            None,
            "owner",
            &snapshot
        )
        .await
        .unwrap());
    // The public low-level constructor remains a no-background-boot compatibility seam. Reuse
    // these prebuilt ports; construction/readiness calls none of them and must not sweep owner.
    let unmanaged = AppState::new(
        store.clone(),
        state.ws.clone(),
        state.identity.clone(),
        state.agent.clone(),
        state.realtime.clone(),
        state.bus.clone(),
        state.search.clone(),
        state.notify.clone(),
        state.admin.clone(),
        "default".into(),
    );
    let mut compatibility_ready = Box::pin(unmanaged.wait_for_runtime_identity_ready());
    assert!(matches!(
        futures::poll!(&mut compatibility_ready),
        std::task::Poll::Ready(Ok(()))
    ));
    assert_eq!(
        repo.find_by_runtime_id("s_model_fixture")
            .await
            .unwrap()
            .unwrap()
            .model_observer_token
            .as_deref(),
        Some("owner")
    );
    let read = ReadClient::from_store_with_caller_for_tests(
        store.clone(),
        "reader",
        "default",
        None,
        None,
        Tier::Admin,
    );
    let caller = Caller {
        agent_id: None,
        session: SessionId("reader".into()),
        name: "reader".into(),
        project: "default".into(),
        tier: Tier::Admin,
        locality: Default::default(),
        access: None,
        principal_id: None,
    };
    for inactive in [false, true] {
        if inactive {
            repo.revoke_model_observer("s_model_fixture", "a_model_fixture", "owner")
                .await
                .unwrap();
        }
        let canonical = repo
            .find_by_runtime_id("s_model_fixture")
            .await
            .unwrap()
            .unwrap()
            .model_report
            .unwrap();
        assert_eq!(canonical.observer_active, !inactive);
        assert_eq!(
            serde_json::to_value(&canonical).unwrap()["configured"]["observation"]["modelId"],
            " model:opaque/value[mode] "
        );
        let cli = read.agent_runtimes("model-fixture", true).await.unwrap();
        let daemon = state
            .list_agent_runtimes(
                &caller,
                AgentRuntimeListRequest {
                    agent_id: None,
                    name: Some("model-fixture".into()),
                    include_stopped: Some(true),
                },
            )
            .await
            .unwrap();
        assert_eq!(cli.runtimes[0].model_report.as_ref(), Some(&canonical));
        assert_eq!(daemon.runtimes[0].model_report.as_ref(), Some(&canonical));
        assert_eq!(
            serde_json::to_value(&cli).unwrap(),
            serde_json::to_value(&daemon).unwrap()
        );
        let cli = read.agent_show("model-fixture").await.unwrap();
        let daemon = state
            .show_agent_identity(
                &caller,
                AgentShowRequest {
                    agent_id: None,
                    name: Some("model-fixture".into()),
                },
            )
            .await
            .unwrap();
        assert_eq!(
            cli.agent.active_runtime.unwrap().model_report.as_ref(),
            Some(&canonical)
        );
        assert_eq!(
            daemon.agent.active_runtime.unwrap().model_report.as_ref(),
            Some(&canonical)
        );
    }
}
