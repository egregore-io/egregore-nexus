use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use nexus::daemon::AppState;
use nexus_common::Config;
use nexus_contracts::{
    AgentTurnExecutionPort, CreateThreadRequest, Kind, NexusBatch, PortResult, RegisterRequest,
    RemoveRequest, RemoveResponse, SendRequest, SendTarget, SessionId, SpawnRequest, SpawnResponse,
    Tier,
};
use nexus_store::Store;
use tokio::sync::mpsc;

const PROJECT: &str = "default";

fn human(name: &str, client_key: &str) -> RegisterRequest {
    RegisterRequest {
        agent_id: None,
        name: Some(name.into()),
        harness: hid("other"),
        harness_session_id: format!("hs_{client_key}"),
        project: PROJECT.into(),
        client_key: client_key.into(),
        runtime_credential: None,
        tier: Tier::Admin,
        kind: Some(Kind::Human),
        role: None,
        cwd: None,
    }
}

struct RecordingTurnExec {
    tx: mpsc::UnboundedSender<(SessionId, NexusBatch)>,
}

#[async_trait]
impl AgentTurnExecutionPort for RecordingTurnExec {
    async fn inject_turn(&self, recipient: &SessionId, batch: &NexusBatch) -> PortResult<()> {
        self.tx
            .send((recipient.clone(), batch.clone()))
            .expect("test receiver is alive");
        Ok(())
    }

    async fn launch(&self, _req: SpawnRequest) -> PortResult<SpawnResponse> {
        Ok(SpawnResponse {
            session_id: SessionId("s_unused_launch".into()),
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

#[tokio::test]
async fn fresh_codex_launch_receives_thread_fanout_without_self_register_or_bus_warm() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let state = AppState::wire_with_turn_exec(
        store,
        &Config::default(),
        Arc::new(RecordingTurnExec { tx }),
    );

    state
        .identity
        .register(human("alex", "ck_operator"))
        .await
        .unwrap();
    let caller = state.identity.resolve(PROJECT, "alex").await.unwrap();
    let codex_session = SessionId("s_codex_fresh_thread_fanout".into());

    state
        .register_and_wake(
            &codex_session,
            "a_codex_fresh_thread_fanout",
            Some("codex-fresh-thread-fanout"),
            PROJECT,
            hid("codex"),
            None,
            "ck_codex_fresh_thread_fanout",
            Some("/tmp/nexus-codex-fresh-thread-fanout".into()),
            "codex-appserver",
            None,
        )
        .await
        .unwrap();
    state.mark_session_offline(&codex_session).await.unwrap();

    // A live fresh-launch/re-register path must make the session wakeable again. Before the fix,
    // the durable rows came back online but the in-memory realtime registry stayed Offline, so
    // thread fanout wrote pending rows without ringing/draining them.
    state
        .register_and_wake(
            &codex_session,
            "a_codex_fresh_thread_fanout",
            Some("codex-fresh-thread-fanout"),
            PROJECT,
            hid("codex"),
            None,
            "ck_codex_fresh_thread_fanout",
            Some("/tmp/nexus-codex-fresh-thread-fanout".into()),
            "codex-appserver",
            None,
        )
        .await
        .unwrap();

    state
        .bus
        .create_thread(
            &caller,
            CreateThreadRequest {
                name: "codex-fresh-thread-fanout".into(),
                members: vec!["codex-fresh-thread-fanout".into()],
            },
        )
        .await
        .unwrap();
    state
        .bus
        .send(
            &caller,
            SendRequest {
                to: SendTarget::Post {
                    thread: "codex-fresh-thread-fanout".into(),
                },
                summary: None,
                body: "thread fanout should drain after fresh codex reregister".into(),
                mention: vec![],
                idempotency_key: None,
            },
        )
        .await
        .unwrap();

    let (recipient, batch) = tokio::time::timeout(Duration::from_millis(500), rx.recv())
        .await
        .expect("thread fanout did not wake and drain the fresh Codex session")
        .expect("turn executor channel closed unexpectedly");

    assert_eq!(recipient, codex_session);
    assert_eq!(batch.counts.thread, 1);
    assert_eq!(batch.counts.total, 1);
    assert_eq!(
        batch.threads[0].body,
        "thread fanout should drain after fresh codex reregister"
    );
}

/// A validated [`nexus_contracts::HarnessId`] from a literal (panics on invalid — test-only).
fn hid(s: &str) -> nexus_contracts::HarnessId {
    nexus_contracts::HarnessId::new(s).expect("valid harness id literal")
}
