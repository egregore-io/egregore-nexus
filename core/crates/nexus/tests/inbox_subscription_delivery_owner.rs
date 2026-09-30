use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use nexus::daemon::{dispatch, AppState};
use nexus_agent::AdapterRegistry;
use nexus_common::Config;
use nexus_contracts::{
    AgentTurnExecutionPort, CreateThreadRequest, InboxSubscribeRequest, InboxSubscribeResponse,
    InboxSubscriptionAckRequest, InboxSubscriptionNextRequest, InboxSubscriptionNextResponse,
    InboxUnsubscribeRequest, Kind, NexusBatch, PortResult, Presence, RegisterRequest,
    RemoveRequest, RemoveResponse, Request, RequestId, SendRequest, SendTarget, SessionId,
    SpawnRequest, SpawnResponse, Tier,
};
use nexus_store::repos::{AgentRuntimes, InboxSubscriptions, Sessions};
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
        locality: Default::default(),
        access: None,
        role: None,
        cwd: None,
    }
}

#[tokio::test]
async fn daemon_managed_agents_cannot_transfer_delivery_to_a_pull_subscription() {
    for (harness, transport) in [
        ("claude", "pty"),
        ("codex", "codex-appserver"),
        ("opencode", "opencode-plugin"),
        ("hermes", "acp"),
    ] {
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
                hid(harness),
            ))
            .await
            .unwrap();

        let sender = state.identity.resolve(PROJECT, "sender").await.unwrap();
        let receiver = state.identity.resolve(PROJECT, "receiver").await.unwrap();
        Sessions::new(&state.store)
            .set_transport(&receiver.session, transport)
            .await
            .unwrap();
        state.ensure_agent_loop(PROJECT, "receiver").await;
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
        let error = response.error.unwrap_or_else(|| {
            panic!("daemon-managed {harness}/{transport} inbox accepted pull ownership")
        });
        assert_eq!(error.code, nexus_contracts::codes::INVALID_PARAMS);
        assert!(
            error.message.contains("managed agent"),
            "typed refusal must explain why listen cannot own {harness}/{transport}: {error:?}"
        );
        assert!(
            !InboxSubscriptions::new(&state.store)
                .has_active_for_session(&receiver.session.0)
                .await
                .unwrap(),
            "refused {harness}/{transport} listener left an active delivery owner"
        );
        Sessions::new(&state.store)
            .set_presence(&receiver.session, Presence::Online)
            .await
            .unwrap();
        AgentRuntimes::new(&state.store)
            .mark_live(&receiver.session.0)
            .await
            .unwrap();

        state
            .bus
            .send(
                &sender,
                SendRequest {
                    to: SendTarget::dm_name("receiver"),
                    summary: None,
                    body: format!("managed-{harness}-delivery"),
                    mention: vec![],
                    metadata: None,
                    idempotency_key: Some(format!("managed-{harness}-delivery")),
                },
            )
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            exec.calls.load(Ordering::SeqCst),
            1,
            "refusing pull ownership must leave the {harness}/{transport} event loop wakeable"
        );
    }
}

#[tokio::test]
async fn external_other_can_take_and_release_pull_delivery_ownership() {
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
            hid("other"),
        ))
        .await
        .unwrap();

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
                metadata: None,
                idempotency_key: Some("daemon-owned-again".into()),
            },
        )
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        exec.calls.load(Ordering::SeqCst),
        0,
        "an external pull client must not acquire a daemon harness loop after unsubscribe"
    );
}

#[tokio::test]
async fn managed_rebind_retires_the_same_sessions_external_pull_owner_before_delivery() {
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
            hid("other"),
        ))
        .await
        .unwrap();
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
    let subscription: InboxSubscribeResponse =
        serde_json::from_value(subscribed.result.expect("external subscription result")).unwrap();

    // The stable session is now owned by a daemon-managed transport. Registration/rebind paths
    // converge through ensure_agent_loop after updating this authoritative descriptor.
    Sessions::new(&state.store)
        .set_transport(&receiver.session, "pty")
        .await
        .unwrap();
    state.ensure_agent_loop(PROJECT, "receiver").await;

    let row = InboxSubscriptions::new(&state.store)
        .get(&subscription.subscription_id)
        .await
        .unwrap()
        .expect("subscription row retained for audit");
    assert_eq!(
        row.status, "inactive",
        "managed rebind must atomically retire the old external delivery owner"
    );

    state
        .bus
        .send(
            &sender,
            SendRequest {
                to: SendTarget::dm_name("receiver"),
                summary: None,
                body: "managed-after-external-rebind".into(),
                mention: vec![],
                metadata: None,
                idempotency_key: Some("managed-after-external-rebind".into()),
            },
        )
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        exec.calls.load(Ordering::SeqCst),
        1,
        "the daemon loop must receive delivery after the stale pull owner is retired"
    );
}

#[tokio::test]
async fn subscription_ack_uses_stable_session_authority_across_project_drift() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let exec = Arc::new(CountingTurnExec {
        calls: AtomicUsize::new(0),
    });
    let state = AppState::wire_with_turn_exec(store, &Config::default(), exec);
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
            hid("other"),
        ))
        .await
        .unwrap();
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
                    timeout_ms: Some(0),
                    max: Some(10),
                })
                .unwrap(),
            ),
        },
    )
    .await;
    let subscription: InboxSubscribeResponse =
        serde_json::from_value(subscribed.result.expect("subscription result")).unwrap();

    state
        .bus
        .send(
            &sender,
            SendRequest {
                to: SendTarget::dm_name("receiver"),
                summary: None,
                body: "project-labels-are-not-ack-authority".into(),
                mention: vec![],
                metadata: None,
                idempotency_key: Some("project-labels-are-not-ack-authority".into()),
            },
        )
        .await
        .unwrap();
    let next = dispatch(
        &state,
        Some(receiver.clone()),
        Request {
            jsonrpc: nexus_contracts::JSONRPC_VERSION.into(),
            id: Some(RequestId::Num(2)),
            method: "inbox.next".into(),
            params: Some(
                serde_json::to_value(InboxSubscriptionNextRequest {
                    subscription_id: subscription.subscription_id.clone(),
                    timeout_ms: Some(0),
                })
                .unwrap(),
            ),
        },
    )
    .await;
    let next: InboxSubscriptionNextResponse =
        serde_json::from_value(next.result.expect("subscription batch result")).unwrap();
    let batch = next.batch.expect("one pull delivery batch");

    Sessions::new(&state.store)
        .set_project(&receiver.session, "renamed-project-label")
        .await
        .unwrap();
    let mut drifted_caller = receiver;
    drifted_caller.project = "renamed-project-label".into();
    let ack = dispatch(
        &state,
        Some(drifted_caller.clone()),
        Request {
            jsonrpc: nexus_contracts::JSONRPC_VERSION.into(),
            id: Some(RequestId::Num(3)),
            method: "inbox.subscriptionAck".into(),
            params: Some(
                serde_json::to_value(InboxSubscriptionAckRequest {
                    subscription_id: subscription.subscription_id.clone(),
                    batch_id: batch.batch_id,
                })
                .unwrap(),
            ),
        },
    )
    .await;
    assert!(
        ack.error.is_none(),
        "same authenticated session must ACK after project metadata drift: {ack:?}"
    );

    let replay = dispatch(
        &state,
        Some(drifted_caller),
        Request {
            jsonrpc: nexus_contracts::JSONRPC_VERSION.into(),
            id: Some(RequestId::Num(4)),
            method: "inbox.next".into(),
            params: Some(
                serde_json::to_value(InboxSubscriptionNextRequest {
                    subscription_id: subscription.subscription_id,
                    timeout_ms: Some(0),
                })
                .unwrap(),
            ),
        },
    )
    .await;
    let replay: InboxSubscriptionNextResponse =
        serde_json::from_value(replay.result.expect("post-ack result")).unwrap();
    assert!(
        replay.batch.is_none(),
        "ACKed batch replayed after project drift"
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
                    metadata: None,
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
