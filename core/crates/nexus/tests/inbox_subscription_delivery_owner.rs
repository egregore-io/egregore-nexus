use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use nexus::daemon::{dispatch, AppState};
use nexus_agent::AdapterRegistry;
use nexus_common::Config;
use nexus_contracts::{
    AgentTurnExecutionPort, BatchCounts, Caller, ConsumeRequest, CreateThreadRequest,
    InboxSubscribeRequest, InboxSubscribeResponse, InboxUnsubscribeRequest, Kind, NexusBatch,
    PortResult, Presence, RegisterRequest, RemoveRequest, RemoveResponse, Request, RequestId,
    SendRequest, SendTarget, SessionId, SpawnRequest, SpawnResponse, Tier,
};
use nexus_store::repos::{AgentRuntimes, Sessions};
use nexus_store::{DaemonStore, Store};

const PROJECT: &str = "pull-owner";

struct CountingTurnExec {
    calls: AtomicUsize,
}

#[async_trait]
impl AgentTurnExecutionPort for CountingTurnExec {
    async fn inject_turn(&self, _recipient: &SessionId, _batch: &NexusBatch) -> PortResult<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn launch(&self, _request: SpawnRequest) -> PortResult<SpawnResponse> {
        unreachable!("test registers an existing runtime")
    }

    async fn remove(&self, _request: RemoveRequest) -> PortResult<RemoveResponse> {
        unreachable!("test does not remove a runtime")
    }
}

fn register(
    name: &str,
    client_key: &str,
    kind: Kind,
    harness: nexus_contracts::HarnessId,
) -> RegisterRequest {
    RegisterRequest {
        agent_id: None,
        name: Some(name.into()),
        harness,
        harness_session_id: format!("native-{name}"),
        project: PROJECT.into(),
        client_key: client_key.into(),
        runtime_credential: None,
        tier: if kind == Kind::Human {
            Tier::Admin
        } else {
            Tier::Agent
        },
        kind: Some(kind),
        role: None,
        cwd: None,
    }
}

#[tokio::test]
async fn active_inbox_subscription_owns_delivery_instead_of_harness_event_loop() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let exec = Arc::new(CountingTurnExec {
        calls: AtomicUsize::new(0),
    });
    let state = AppState::wire_with_turn_exec(store.clone(), &Config::default(), exec.clone());
    tokio::time::sleep(Duration::from_millis(50)).await;

    state
        .identity
        .register(register("sender", "ck_sender", Kind::Human, hid("other")))
        .await
        .unwrap();
    state
        .identity
        .register(register(
            "receiver",
            "ck_receiver",
            Kind::Agent,
            hid("claude"),
        ))
        .await
        .unwrap();
    state.ensure_agent_loop(PROJECT, "receiver").await;

    let sender = state.identity.resolve(PROJECT, "sender").await.unwrap();
    let receiver = state.identity.resolve(PROJECT, "receiver").await.unwrap();
    let response = dispatch(
        &state,
        Some(receiver.clone()),
        Request {
            jsonrpc: nexus_contracts::JSONRPC_VERSION.into(),
            id: Some(RequestId::Num(1)),
            method: "inbox.subscribe".into(),
            params: Some(
                serde_json::to_value(InboxSubscribeRequest {
                    timeout_ms: Some(100),
                    max: Some(10),
                })
                .unwrap(),
            ),
        },
    )
    .await;
    assert!(response.error.is_none(), "subscribe failed: {response:?}");
    Sessions::new(&state.store)
        .set_presence(&receiver.session, Presence::Online)
        .await
        .unwrap();
    AgentRuntimes::new(&state.store)
        .mark_live(&receiver.session.0)
        .await
        .unwrap();

    let ack = state
        .bus
        .send(
            &sender,
            SendRequest {
                to: SendTarget::dm_name("receiver"),
                summary: None,
                body: "pull-owned-message".into(),
                mention: vec![],
                idempotency_key: Some("pull-owned-message".into()),
            },
        )
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        exec.calls.load(Ordering::SeqCst),
        0,
        "an active subscription must be the only consumer for its inbox"
    );

    let mut rows = state
        .store
        .conn
        .query(
            "SELECT state, COALESCE(error_code, ''), recipient_session FROM in_flight \
             WHERE message_id = ?1",
            [ack.message_id.0.as_str()],
        )
        .await
        .unwrap();
    let delivery = rows.next().await.unwrap().map(|row| {
        (
            row.get::<String>(0).unwrap(),
            row.get::<String>(1).unwrap(),
            row.get::<String>(2).unwrap(),
        )
    });

    let batch = state
        .realtime
        .consume(
            &Caller {
                agent_id: receiver.agent_id,
                ..receiver
            },
            ConsumeRequest {
                timeout_ms: Some(0),
                max: Some(10),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        batch.counts,
        BatchCounts {
            dms: 1,
            thread: 0,
            total: 1
        },
        "delivery before pull: {delivery:?}"
    );
    assert_eq!(batch.dms[0].body, "pull-owned-message");
}

#[tokio::test]
async fn inbox_unsubscribe_restores_daemon_owned_harness_delivery() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let exec = Arc::new(CountingTurnExec {
        calls: AtomicUsize::new(0),
    });
    let state = AppState::wire_with_turn_exec(store, &Config::default(), exec.clone());
    tokio::time::sleep(Duration::from_millis(50)).await;

    state
        .identity
        .register(register("sender", "ck_sender", Kind::Human, hid("other")))
        .await
        .unwrap();
    state
        .identity
        .register(register(
            "receiver",
            "ck_receiver",
            Kind::Agent,
            hid("claude"),
        ))
        .await
        .unwrap();
    state.ensure_agent_loop(PROJECT, "receiver").await;

    let sender = state.identity.resolve(PROJECT, "sender").await.unwrap();
    let receiver = state.identity.resolve(PROJECT, "receiver").await.unwrap();
    let subscribed = dispatch(
        &state,
        Some(receiver.clone()),
        Request {
            jsonrpc: nexus_contracts::JSONRPC_VERSION.into(),
            id: Some(RequestId::Num(1)),
            method: "inbox.subscribe".into(),
            params: Some(
                serde_json::to_value(InboxSubscribeRequest {
                    timeout_ms: Some(100),
                    max: Some(10),
                })
                .unwrap(),
            ),
        },
    )
    .await;
    assert!(
        subscribed.error.is_none(),
        "subscribe failed: {subscribed:?}"
    );
    let subscription: InboxSubscribeResponse =
        serde_json::from_value(subscribed.result.unwrap()).unwrap();

    let unsubscribed = dispatch(
        &state,
        Some(receiver.clone()),
        Request {
            jsonrpc: nexus_contracts::JSONRPC_VERSION.into(),
            id: Some(RequestId::Num(2)),
            method: "inbox.unsubscribe".into(),
            params: Some(
                serde_json::to_value(InboxUnsubscribeRequest {
                    subscription_id: subscription.subscription_id,
                })
                .unwrap(),
            ),
        },
    )
    .await;
    assert!(
        unsubscribed.error.is_none(),
        "unsubscribe failed: {unsubscribed:?}"
    );

    state
        .bus
        .send(
            &sender,
            SendRequest {
                to: SendTarget::dm_name("receiver"),
                summary: None,
                body: "daemon-owned-again".into(),
                mention: vec![],
                idempotency_key: Some("daemon-owned-again".into()),
            },
        )
        .await
        .unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while exec.calls.load(Ordering::SeqCst) == 0 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        exec.calls.load(Ordering::SeqCst),
        1,
        "unsubscribing must hand delivery ownership back to the daemon harness loop"
    );
}

#[tokio::test]
async fn external_other_thread_member_is_pull_delivered_and_never_revived() {
    let identity_path = std::env::temp_dir().join(format!(
        "nexus-external-pull-owner-{}-{}.db",
        std::process::id(),
        nexus_common::now()
    ));
    let daemon = DaemonStore::open(identity_path.to_string_lossy().as_ref())
        .await
        .unwrap();
    let store = Arc::new(daemon.compatibility_store());
    let state =
        AppState::wire_with_registry(store.clone(), &Config::default(), AdapterRegistry::new());
    tokio::time::sleep(Duration::from_millis(50)).await;

    state
        .identity
        .register(register("sender", "ck_sender", Kind::Human, hid("other")))
        .await
        .unwrap();
    state
        .identity
        .register(register(
            "external-controller",
            "ck_external_controller",
            Kind::Agent,
            hid("other"),
        ))
        .await
        .unwrap();
    let sender = state.identity.resolve(PROJECT, "sender").await.unwrap();
    let external = state
        .identity
        .resolve(PROJECT, "external-controller")
        .await
        .unwrap();

    state
        .bus
        .create_thread(
            &sender,
            CreateThreadRequest {
                name: "external-controller-thread".into(),
                members: vec!["external-controller".into()],
            },
        )
        .await
        .unwrap();
    let mut message_ids = Vec::new();
    for index in 0..4 {
        let ack = state
            .bus
            .send(
                &sender,
                SendRequest {
                    to: SendTarget::Post {
                        thread: "external-controller-thread".into(),
                    },
                    summary: None,
                    body: format!("wait for external pull {index}"),
                    mention: vec![],
                    idempotency_key: Some(format!("external-controller-thread-message-{index}")),
                },
            )
            .await
            .unwrap();
        state
            .wake_thread_agents(
                "external-controller-thread",
                PROJECT,
                "sender",
                &ack.message_id,
            )
            .await;
        message_ids.push(ack.message_id);
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut error_rows = state
        .store
        .conn
        .query(
            "SELECT COUNT(*) FROM in_flight WHERE recipient_session = ?1 AND state = 'error'",
            [external.session.0.as_str()],
        )
        .await
        .unwrap();
    let error_count = error_rows
        .next()
        .await
        .unwrap()
        .unwrap()
        .get::<i64>(0)
        .unwrap();
    assert_eq!(
        error_count, 0,
        "external pull deliveries must never be terminalized by harness revival"
    );
    assert_eq!(message_ids.len(), 4);

    drop(state);
    drop(store);
    drop(daemon);
    let _ = std::fs::remove_file(identity_path);
}

/// A validated [`nexus_contracts::HarnessId`] from a literal (panics on invalid — test-only).
fn hid(s: &str) -> nexus_contracts::HarnessId {
    nexus_contracts::HarnessId::new(s).expect("valid harness id literal")
}
