//! Registration must repair canonical Gateway snapshots even when compatibility presence
//! is already online. These use production registration, WsSink, and buffered replay.
use std::{sync::Arc, time::Duration};

use nexus::daemon::{
    gateway_stream_socket::{
        serve_gateway_stream_connection, write_gateway_client_frame, GatewayStreamClientFrame,
        GatewayStreamFrame, GatewayStreamPublisher,
    },
    AppState,
};
use nexus_common::Config;
use nexus_contracts::{
    GatewayProjectionEvent, GatewayProjectionKind, HarnessId, RegisterRequest, SessionId, Tier,
};
use nexus_store::{
    repos::{AgentRuntimes, Sessions},
    DaemonStore, Store,
};
use tokio::io::AsyncReadExt;

#[derive(Clone, Copy, Debug)]
enum Path {
    Daemon,
    Identity,
}

fn request() -> RegisterRequest {
    RegisterRequest {
        agent_id: None,
        name: Some("resume-probe".into()),
        harness: HarnessId::new("codex").unwrap(),
        harness_session_id: "native-probe".into(),
        project: "default".into(),
        client_key: "ck_resume_probe".into(),
        runtime_credential: None,
        tier: Tier::Agent,
        kind: None,
        locality: Default::default(),
        access: None,
        role: None,
        cwd: None,
    }
}

async fn register(
    state: &AppState,
    path: Path,
    sid: &SessionId,
    aid: &str,
) -> Result<SessionId, String> {
    match path {
        Path::Daemon => state
            .register_and_wake(
                sid,
                aid,
                Some("resume-probe"),
                "default",
                HarnessId::new("codex").unwrap(),
                None,
                "ck_resume_probe",
                None,
                "codex-appserver",
                None,
            )
            .await
            .map(|()| sid.clone())
            .map_err(|e| e.to_string()),
        Path::Identity => state
            .identity
            .register(request())
            .await
            .map(|r| r.session_id)
            .map_err(|e| format!("{e:?}")),
    }
}

async fn fixture(
    path: Path,
) -> (
    tempfile::TempDir,
    Arc<Store>,
    AppState,
    GatewayStreamPublisher,
    SessionId,
    String,
) {
    // Production split authority: persistent identity/runtime DB and boot-scoped sessions.
    let dir = tempfile::tempdir().unwrap();
    let daemon = DaemonStore::open(dir.path().join("identity.db").to_str().unwrap())
        .await
        .unwrap();
    let store = Arc::new(daemon.compatibility_store());
    let publisher = GatewayStreamPublisher::new(64);
    let state = AppState::wire_pty_with_gateway_stream(
        store.clone(),
        &Config::default(),
        Some(publisher.clone()),
    );
    state
        .wait_for_runtime_identity_ready()
        .await
        .expect("runtime identity ready");
    let sid = register(
        &state,
        path,
        &SessionId("s_resume_probe".into()),
        "a_resume_probe",
    )
    .await
    .unwrap();
    let aid = Sessions::new(&store)
        .find_by_session_id(&sid)
        .await
        .unwrap()
        .unwrap()
        .agent_id
        .unwrap();
    (dir, store, state, publisher, sid, aid)
}

fn projections(
    rx: &mut tokio::sync::broadcast::Receiver<GatewayStreamFrame>,
) -> Vec<GatewayProjectionEvent> {
    let mut result = Vec::new();
    while let Ok(frame) = rx.try_recv() {
        if let GatewayStreamFrame::Projection { event } = frame {
            result.push(event);
        }
    }
    result
}

fn assert_bound<'a>(
    events: &'a [GatewayProjectionEvent],
    sid: &SessionId,
    aid: &str,
) -> &'a GatewayProjectionEvent {
    assert!(
        events.windows(2).all(|pair| pair[0].seq < pair[1].seq),
        "repair projections must retain strictly increasing sequence order"
    );
    let mut saw_identity = false;
    let mut latest_runtime = None;
    for event in events {
        match event.kind {
            GatewayProjectionKind::IdentityUpserted => {
                assert_eq!(event.payload["agentId"], aid);
                saw_identity = true;
            }
            GatewayProjectionKind::RuntimeUpserted => {
                assert_eq!(event.payload["agentId"], aid);
                assert_eq!(event.payload["sessionId"], sid.0);
                assert_eq!(event.payload["runtimeId"], sid.0);
                latest_runtime = Some(event);
            }
            _ => {}
        }
    }
    assert!(
        saw_identity,
        "resume must publish committed identity even without a presence transition"
    );
    // Selection can publish an active/offline snapshot before presence materialization.
    // The caller's final publication must expose the completed canonical repair.
    let runtime = latest_runtime
        .expect("resume must upsert the canonical runtime, not only a presence patch");
    assert_eq!(runtime.payload["presence"], "online");
    assert_eq!(runtime.payload["active"], true);
    assert!(runtime.payload["stoppedAt"].is_null());
    runtime
}

async fn check_repairs(path: Path) {
    for damage in ["stopped", "missing", "missing-session-agent-id"] {
        let (_dir, store, state, publisher, sid, aid) = fixture(path).await;
        match damage {
            "stopped" => AgentRuntimes::new(&store).stop(&sid.0).await.unwrap(),
            "missing" => {
                store
                    .identity_conn()
                    .execute(
                        "DELETE FROM agent_runtimes WHERE runtime_id = ?1",
                        [sid.0.clone()],
                    )
                    .await
                    .unwrap();
            }
            _ => {
                store
                    .conn
                    .execute(
                        "UPDATE sessions SET agent_id = NULL WHERE session_id = ?1",
                        [sid.0.clone()],
                    )
                    .await
                    .unwrap();
            }
        }
        assert_eq!(
            Sessions::new(&store)
                .find_by_session_id(&sid)
                .await
                .unwrap()
                .unwrap()
                .presence
                .as_deref(),
            Some("online")
        );
        let mut rx = publisher.subscribe();
        let mut lifecycle = state.ws.subscribe();
        assert_eq!(register(&state, path, &sid, &aid).await.unwrap(), sid);
        let events = projections(&mut rx);
        let latest = assert_bound(&events, &sid, &aid);
        let runtime = AgentRuntimes::new(&store)
            .find_by_runtime_id(&sid.0)
            .await
            .unwrap()
            .expect("successful repair must retain the canonical runtime");
        assert_eq!(latest.payload["agentId"], runtime.agent_id);
        assert_eq!(latest.payload["sessionId"], runtime.runtime_id);
        assert_eq!(latest.payload["runtimeId"], runtime.runtime_id);
        assert_eq!(latest.payload["active"], runtime.active);
        assert_eq!(
            latest.payload["presence"],
            serde_json::json!(runtime.presence)
        );
        assert_eq!(
            latest.payload["stoppedAt"],
            serde_json::json!(runtime.stopped_at)
        );
        while let Ok(event) = lifecycle.try_recv() {
            assert_ne!(event.method, "agent.spawned", "resume is not a new spawn");
            assert_ne!(
                event.method, "agent.status",
                "already-online resume is not a compatibility presence transition"
            );
        }
    }
}

#[tokio::test]
async fn same_session_daemon_bind_projects_repaired_canonical_binding() {
    check_repairs(Path::Daemon).await;
}

#[tokio::test]
async fn resumed_identity_registration_projects_repaired_canonical_binding() {
    check_repairs(Path::Identity).await;
}

#[tokio::test]
async fn invalid_runtime_identity_does_not_publish_or_materialize_online() {
    for path in [Path::Daemon, Path::Identity] {
        for damage in ["runtime-owner", "session-owner"] {
            let (_dir, store, state, publisher, sid, aid) = fixture(path).await;
            AgentRuntimes::new(&store).stop(&sid.0).await.unwrap();
            if damage == "runtime-owner" {
                store
                    .identity_conn()
                    .execute(
                        "UPDATE agent_runtimes SET agent_id = 'a_foreign' WHERE runtime_id = ?1",
                        [sid.0.clone()],
                    )
                    .await
                    .unwrap();
            } else {
                store
                    .conn
                    .execute(
                        "UPDATE sessions SET agent_id = 'a_foreign' WHERE session_id = ?1",
                        [sid.0.clone()],
                    )
                    .await
                    .unwrap();
            }
            store
                .conn
                .execute(
                    "UPDATE sessions SET presence = 'offline' WHERE session_id = ?1",
                    [sid.0.clone()],
                )
                .await
                .unwrap();
            let mut rx = publisher.subscribe();
            let mut lifecycle = state.ws.subscribe();
            assert!(register(&state, path, &sid, &aid).await.is_err());
            assert!(projections(&mut rx).is_empty());
            assert!(lifecycle.try_recv().is_err());
            let runtime = AgentRuntimes::new(&store)
                .find_by_runtime_id(&sid.0)
                .await
                .unwrap()
                .unwrap();
            assert!(
                !runtime.active,
                "{path:?}: a rejected binding must not revive a foreign runtime"
            );
            assert_eq!(runtime.presence.as_deref(), Some("offline"));
            assert_eq!(
                Sessions::new(&store)
                    .find_by_session_id(&sid)
                    .await
                    .unwrap()
                    .unwrap()
                    .presence
                    .as_deref(),
                Some("offline")
            );
        }
    }
}

#[tokio::test]
async fn late_gateway_connection_replays_post_resume_online_snapshot() {
    for path in [Path::Daemon, Path::Identity] {
        // No receiver exists during registration or resume.
        let (_dir, store, state, publisher, sid, aid) = fixture(path).await;
        AgentRuntimes::new(&store).stop(&sid.0).await.unwrap();
        // Persist the offline canonical descriptor in the backlog, then repair it while absent.
        state
            .ws
            .emit(nexus_contracts::WsEvent::AgentSpawned {
                session_id: sid.clone(),
                name: Some("resume-probe".into()),
                agent_id: Some(aid.clone()),
            })
            .await;
        register(&state, path, &sid, &aid).await.unwrap();
        let count = publisher.projection_stats().events;
        let (mut client, server) = tokio::io::duplex(65536);
        let task = tokio::spawn(serve_gateway_stream_connection(
            server,
            store,
            publisher,
            "probe-token".into(),
            "probe-boot".into(),
        ));
        write_gateway_client_frame(
            &mut client,
            &GatewayStreamClientFrame::Hello {
                version: 1,
                token: "probe-token".into(),
                subscriptions: vec![],
                hooks: None,
            },
        )
        .await
        .unwrap();
        let mut received = Vec::new();
        while received.len() < count {
            let frame = tokio::time::timeout(Duration::from_secs(2), async {
                let len = client.read_u32().await.unwrap();
                let mut bytes = vec![0; len as usize];
                client.read_exact(&mut bytes).await.unwrap();
                serde_json::from_slice::<GatewayStreamFrame>(&bytes).unwrap()
            })
            .await
            .unwrap();
            if let GatewayStreamFrame::Projection { event } = frame {
                received.push(event);
            }
        }
        task.abort();
        let _ = task.await;
        assert_eq!(
            received.iter().map(|event| event.seq).collect::<Vec<_>>(),
            (1..=count as i64).collect::<Vec<_>>(),
            "late Gateway must be able to commit every replay event in sequence"
        );
        // Latest canonical runtime, not the earlier stopped snapshot, must win on replay.
        let latest = received
            .iter()
            .rev()
            .find(|e| e.kind == GatewayProjectionKind::RuntimeUpserted)
            .unwrap();
        assert_bound(
            &[
                received
                    .iter()
                    .find(|e| e.kind == GatewayProjectionKind::IdentityUpserted)
                    .unwrap()
                    .clone(),
                latest.clone(),
            ],
            &sid,
            &aid,
        );
    }
}

use nexus_contracts::EventSink;
