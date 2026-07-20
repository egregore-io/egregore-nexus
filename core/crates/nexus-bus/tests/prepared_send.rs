use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use nexus_bus::service::{PREPARED_SEND_CAPACITY, PREPARED_SEND_TTL_MS};
use nexus_bus::Bus;
use nexus_contracts::ack::{AckRequest, AckResponse, AckThreadsRequest};
use nexus_contracts::batch::{ConsumeRequest, NexusBatch};
use nexus_contracts::events::WsEvent;
use nexus_contracts::hooks::{
    DeliveryTiming, HookAction, HookBeforeSendRequest, HookBeforeSendResult,
};
use nexus_contracts::notify::{NotifySendRequest, NotifyTarget};
use nexus_contracts::ports::{
    BusPort, Caller, DispatchPort, EventSink, IdentityPort, MessageHookPort, PortResult,
    PreparedBusSend,
};
use nexus_contracts::register::{
    HeartbeatResponse, MemberListRequest, MemberListResponse, RegisterRequest, RegisterResponse,
    StatusRequest, StatusResponse, Whoami,
};
use nexus_contracts::{AgentId, MessageId, SessionId, Tier};
use nexus_store::repos::{AgentRuntimes, Agents, NewAgent, NewAgentRuntime, NewSession, Sessions};
use nexus_store::{DaemonStore, Store};

const PROJECT: &str = "prepared-send";

#[derive(Default)]
struct RecordingDispatch {
    recipients: Mutex<Vec<SessionId>>,
}

#[derive(Default)]
struct BlockingDispatch {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[async_trait]
impl DispatchPort for BlockingDispatch {
    async fn enqueue(&self, _recipient: &SessionId, _message: &MessageId) -> PortResult<()> {
        self.entered.notify_one();
        self.release.notified().await;
        Ok(())
    }

    async fn consume(&self, _caller: &Caller, _req: ConsumeRequest) -> PortResult<NexusBatch> {
        unreachable!()
    }

    async fn ack(&self, _caller: &Caller, _req: AckRequest) -> PortResult<AckResponse> {
        unreachable!()
    }

    async fn ack_threads(
        &self,
        _caller: &Caller,
        _req: AckThreadsRequest,
    ) -> PortResult<AckResponse> {
        unreachable!()
    }
}

#[async_trait]
impl DispatchPort for RecordingDispatch {
    async fn enqueue(&self, recipient: &SessionId, _message: &MessageId) -> PortResult<()> {
        self.recipients.lock().unwrap().push(recipient.clone());
        Ok(())
    }

    async fn consume(&self, _caller: &Caller, _req: ConsumeRequest) -> PortResult<NexusBatch> {
        unreachable!()
    }

    async fn ack(&self, _caller: &Caller, _req: AckRequest) -> PortResult<AckResponse> {
        unreachable!()
    }

    async fn ack_threads(
        &self,
        _caller: &Caller,
        _req: AckThreadsRequest,
    ) -> PortResult<AckResponse> {
        unreachable!()
    }
}

struct NoopEvents;

#[async_trait]
impl EventSink for NoopEvents {
    async fn emit(&self, _event: WsEvent) {}
}

struct AliasIdentity {
    aliases: Mutex<HashMap<String, Caller>>,
}

impl AliasIdentity {
    fn point(&self, alias: &str, caller: Caller) {
        self.aliases
            .lock()
            .unwrap()
            .insert(alias.to_string(), caller);
    }
}

#[async_trait]
impl IdentityPort for AliasIdentity {
    async fn register(&self, _req: RegisterRequest) -> PortResult<RegisterResponse> {
        unreachable!()
    }

    async fn whoami(&self, _caller: &Caller) -> PortResult<Whoami> {
        unreachable!()
    }

    async fn resolve(&self, _project: &str, name: &str) -> PortResult<Caller> {
        self.aliases
            .lock()
            .unwrap()
            .get(name)
            .cloned()
            .ok_or_else(|| nexus_contracts::ContractError {
                code: nexus_contracts::codes::NOT_FOUND,
                message: format!("unknown alias: {name}"),
            })
    }

    async fn members(
        &self,
        _caller: &Caller,
        _req: MemberListRequest,
    ) -> PortResult<MemberListResponse> {
        unreachable!()
    }

    async fn status(&self, _caller: &Caller, _req: StatusRequest) -> PortResult<StatusResponse> {
        unreachable!()
    }

    async fn heartbeat(&self, _caller: &Caller) -> PortResult<HeartbeatResponse> {
        unreachable!()
    }

    async fn assign_project(
        &self,
        _name: &str,
        _to_project: &str,
    ) -> PortResult<nexus_contracts::AssignProjectResponse> {
        unreachable!()
    }
}

struct CountingHook {
    calls: AtomicUsize,
    reject_call: Option<usize>,
}

#[async_trait]
impl MessageHookPort for CountingHook {
    async fn before_send(
        &self,
        request: HookBeforeSendRequest,
    ) -> PortResult<HookBeforeSendResult> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(HookBeforeSendResult {
            evaluation_id: request.evaluation_id,
            action: if self.reject_call == Some(call) {
                HookAction::Reject
            } else {
                HookAction::Continue
            },
            message: request.message,
            timing: Some(DeliveryTiming::Interrupt),
            executed_by: Vec::new(),
        })
    }
}

async fn unified_store() -> Arc<Store> {
    let store = Store::open(":memory:").await.unwrap();
    store.migrate().await.unwrap();
    Arc::new(store)
}

async fn split_store() -> (tempfile::TempDir, Arc<Store>) {
    let directory = tempfile::tempdir().unwrap();
    let daemon = DaemonStore::open(directory.path().join("identity.db").to_str().unwrap())
        .await
        .unwrap();
    (directory, Arc::new(daemon.compatibility_store()))
}

fn agent_caller(agent_id: &str, name: &str, session_id: &str) -> Caller {
    Caller {
        agent_id: Some(AgentId(agent_id.to_string())),
        session: SessionId(session_id.to_string()),
        name: name.to_string(),
        project: PROJECT.to_string(),
        tier: Tier::Agent,
        locality: Default::default(),
        access: None,
        principal_id: None,
    }
}

fn sender() -> Caller {
    Caller {
        tier: Tier::Admin,
        locality: Default::default(),
        access: None,
        principal_id: None,
        ..agent_caller("a_sender", "sender", "s_sender")
    }
}

async fn seed_agent(store: &Store, agent_id: &str, name: &str, session_id: &str) {
    Agents::new(store)
        .create(NewAgent {
            agent_id: agent_id.to_string(),
            project: PROJECT.to_string(),
            name: Some(name.to_string()),
            default_harness: Some("codex".to_string()),
            role: None,
            tier: None,
            owner: None,
        })
        .await
        .unwrap();
    AgentRuntimes::new(store)
        .create(NewAgentRuntime {
            runtime_id: session_id.to_string(),
            agent_id: agent_id.to_string(),
            harness: "codex".to_string(),
            cwd: None,
            transport: Some("pty".to_string()),
            presence: Some("online".to_string()),
            active: true,
        })
        .await
        .unwrap();
    let session = SessionId(session_id.to_string());
    Sessions::new(store)
        .create(NewSession {
            session_id: session.clone(),
            name: Some(name.to_string()),
            agent: Some("codex".to_string()),
            kind: "agent".to_string(),
            role: None,
            tier: "agent".to_string(),
            harness_session_id: None,
            client_key: Some(format!("ck_{session_id}")),
            cwd: None,
            project: PROJECT.to_string(),
            transport: Some("pty".to_string()),
        })
        .await
        .unwrap();
    Sessions::new(store)
        .set_agent_id(&session, agent_id)
        .await
        .unwrap();
}

async fn assert_prepared_notify_freezes_recipient(store: Arc<Store>) {
    seed_agent(&store, "a_first", "first", "s_first").await;
    seed_agent(&store, "a_second", "second", "s_second").await;
    let identity = Arc::new(AliasIdentity {
        aliases: Mutex::new(HashMap::from([(
            "target".to_string(),
            agent_caller("a_first", "first", "s_first"),
        )])),
    });
    let dispatch = Arc::new(RecordingDispatch::default());
    let hook = Arc::new(CountingHook {
        calls: AtomicUsize::new(0),
        reject_call: None,
    });
    let bus = Bus::new_with_message_hooks(
        store.clone(),
        dispatch.clone(),
        identity.clone(),
        Arc::new(NoopEvents),
        hook.clone(),
    );

    let request = NotifySendRequest {
        target: NotifyTarget::Name {
            name: "target".to_string(),
        },
        source: Some("fixture".to_string()),
        body: "prepared notification".to_string(),
        idempotency_key: Some("prepared-notify".to_string()),
    };
    let prepared = bus
        .prepare_notify(&sender(), request.clone())
        .await
        .unwrap();
    assert_eq!(
        bus.prepared_hold_agent(&sender(), &prepared).await.unwrap(),
        Some(AgentId("a_first".into()))
    );
    assert_eq!(hook.calls.load(Ordering::SeqCst), 1);
    assert_table_empty(&store, "messages").await;
    assert_table_empty(&store, "in_flight").await;

    identity.point("target", agent_caller("a_second", "second", "s_second"));
    let first_ack = bus.commit_prepared(&sender(), prepared).await.unwrap();

    assert_eq!(hook.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        dispatch.recipients.lock().unwrap().as_slice(),
        &[SessionId("s_first".into())]
    );
    let mut rows = store
        .conn
        .query(
            "SELECT recipient_agent_id, recipient_session FROM in_flight",
            (),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<String>(0).unwrap(), "a_first");
    assert_eq!(row.get::<String>(1).unwrap(), "s_first");

    let replay = bus.prepare_notify(&sender(), request).await.unwrap();
    assert_eq!(
        bus.prepared_hold_agent(&sender(), &replay).await.unwrap(),
        None,
        "an already-accepted idempotent effect needs no new lifecycle hold"
    );
    let replay_ack = bus.commit_prepared(&sender(), replay).await.unwrap();
    assert_eq!(replay_ack, first_ack);
    assert_eq!(hook.calls.load(Ordering::SeqCst), 1);
    assert_eq!(dispatch.recipients.lock().unwrap().len(), 1);
}

async fn assert_retry_reuses_abandoned_preparation(store: Arc<Store>) {
    seed_agent(&store, "a_first", "first", "s_first").await;
    seed_agent(&store, "a_second", "second", "s_second").await;
    let identity = Arc::new(AliasIdentity {
        aliases: Mutex::new(HashMap::from([(
            "target".to_string(),
            agent_caller("a_first", "first", "s_first"),
        )])),
    });
    let dispatch = Arc::new(RecordingDispatch::default());
    let hook = Arc::new(CountingHook {
        calls: AtomicUsize::new(0),
        reject_call: None,
    });
    let bus = Bus::new_with_message_hooks(
        store.clone(),
        dispatch.clone(),
        identity.clone(),
        Arc::new(NoopEvents),
        hook.clone(),
    );
    let request = NotifySendRequest {
        target: NotifyTarget::Name {
            name: "target".to_string(),
        },
        source: Some("fixture".to_string()),
        body: "frozen across command reclaim".to_string(),
        idempotency_key: Some("prepared-reclaim".to_string()),
    };

    let abandoned = bus
        .prepare_notify(&sender(), request.clone())
        .await
        .unwrap();
    assert_eq!(
        bus.prepared_hold_agent(&sender(), &abandoned)
            .await
            .unwrap(),
        Some(AgentId("a_first".into()))
    );
    drop(abandoned); // models cancellation after prepare and before the lifecycle hold completes.

    identity.point("target", agent_caller("a_second", "second", "s_second"));
    let reclaimed = bus.prepare_notify(&sender(), request).await.unwrap();

    assert_eq!(
        hook.calls.load(Ordering::SeqCst),
        1,
        "reclaim must reuse the accepted hook result instead of evaluating a new send"
    );
    assert_eq!(
        bus.prepared_hold_agent(&sender(), &reclaimed)
            .await
            .unwrap(),
        Some(AgentId("a_first".into())),
        "reclaim must preserve the originally resolved durable target"
    );
    bus.commit_prepared(&sender(), reclaimed).await.unwrap();
    assert_eq!(
        dispatch.recipients.lock().unwrap().as_slice(),
        &[SessionId("s_first".into())]
    );
}

async fn assert_hold_target_is_authenticated_by_the_bus(store: Arc<Store>) {
    seed_agent(&store, "a_first", "first", "s_first").await;
    seed_agent(&store, "a_second", "second", "s_second").await;
    let identity = Arc::new(AliasIdentity {
        aliases: Mutex::new(HashMap::from([(
            "target".to_string(),
            agent_caller("a_first", "first", "s_first"),
        )])),
    });
    let dispatch = Arc::new(RecordingDispatch::default());
    let bus = Bus::new(store, dispatch.clone(), identity, Arc::new(NoopEvents));
    let prepared = bus
        .prepare_notify(
            &sender(),
            NotifySendRequest {
                target: NotifyTarget::Name {
                    name: "target".to_string(),
                },
                source: None,
                body: "authenticate the lifecycle hold".to_string(),
                idempotency_key: Some("prepared-hold-authority".to_string()),
            },
        )
        .await
        .unwrap();
    let reconstructed = PreparedBusSend::resolved(prepared.preparation_id().to_string());

    assert_eq!(
        bus.prepared_hold_agent(&sender(), &reconstructed)
            .await
            .unwrap(),
        Some(AgentId("a_first".into())),
        "the lifecycle hold must come from the authenticated preparation ledger"
    );
    bus.commit_prepared(&sender(), reconstructed).await.unwrap();
    assert_eq!(
        dispatch.recipients.lock().unwrap().as_slice(),
        &[SessionId("s_first".into())]
    );
}

async fn assert_cancelled_commit_remains_reclaimable(store: Arc<Store>) {
    seed_agent(&store, "a_first", "first", "s_first").await;
    seed_agent(&store, "a_second", "second", "s_second").await;
    let identity = Arc::new(AliasIdentity {
        aliases: Mutex::new(HashMap::from([(
            "target".to_string(),
            agent_caller("a_first", "first", "s_first"),
        )])),
    });
    let dispatch = Arc::new(BlockingDispatch::default());
    let hook = Arc::new(CountingHook {
        calls: AtomicUsize::new(0),
        reject_call: None,
    });
    let bus = Arc::new(Bus::new_with_message_hooks(
        store.clone(),
        dispatch.clone(),
        identity.clone(),
        Arc::new(NoopEvents),
        hook.clone(),
    ));
    let caller = sender();
    let request = NotifySendRequest {
        target: NotifyTarget::Name {
            name: "target".to_string(),
        },
        source: Some("fixture".to_string()),
        body: "accepted before cancellation".to_string(),
        idempotency_key: Some("prepared-cancelled-commit".to_string()),
    };
    let prepared = bus.prepare_notify(&caller, request.clone()).await.unwrap();
    let token = prepared.preparation_id().to_string();
    let committing_bus = bus.clone();
    let committing_caller = caller.clone();
    let commit = tokio::spawn(async move {
        committing_bus
            .commit_prepared(&committing_caller, prepared)
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), dispatch.entered.notified())
        .await
        .expect("commit must reach the post-durable bell boundary");
    commit.abort();
    assert!(commit.await.unwrap_err().is_cancelled());

    let retained = PreparedBusSend::resolved(token.clone());
    assert_eq!(
        bus.prepared_hold_agent(&caller, &retained).await.unwrap(),
        Some(AgentId("a_first".into())),
        "task cancellation must not consume the accepted preparation"
    );

    identity.point("target", agent_caller("a_second", "second", "s_second"));
    let reclaimed = bus.prepare_notify(&caller, request).await.unwrap();
    assert_eq!(
        hook.calls.load(Ordering::SeqCst),
        1,
        "reclaim must not re-run the accepted hook"
    );
    let ack = bus.commit_prepared(&caller, reclaimed).await.unwrap();
    let mut rows = store
        .conn
        .query(
            "SELECT body, to_agent_id FROM messages WHERE message_id = ?1",
            libsql::params![ack.message_id.0],
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(
        row.get::<String>(0).unwrap(),
        "accepted before cancellation"
    );
    assert_eq!(row.get::<String>(1).unwrap(), "a_first");
    assert!(
        bus.prepared_hold_agent(&caller, &PreparedBusSend::resolved(token))
            .await
            .is_err(),
        "successful reclaim must consume the retained preparation"
    );
}

async fn assert_rejected_prepare_has_no_effects(store: Arc<Store>) {
    seed_agent(&store, "a_target", "target", "s_target").await;
    let identity = Arc::new(AliasIdentity {
        aliases: Mutex::new(HashMap::from([(
            "target".to_string(),
            agent_caller("a_target", "target", "s_target"),
        )])),
    });
    let dispatch = Arc::new(RecordingDispatch::default());
    let hook = Arc::new(CountingHook {
        calls: AtomicUsize::new(0),
        reject_call: Some(1),
    });
    let bus = Bus::new_with_message_hooks(
        store.clone(),
        dispatch.clone(),
        identity,
        Arc::new(NoopEvents),
        hook.clone(),
    );

    let error = bus
        .prepare_notify(
            &sender(),
            NotifySendRequest {
                target: NotifyTarget::Name {
                    name: "target".to_string(),
                },
                source: None,
                body: "must reject".to_string(),
                idempotency_key: Some("prepared-reject".to_string()),
            },
        )
        .await
        .expect_err("hook rejection must fail preparation");

    assert_eq!(error.code, nexus_contracts::codes::HOOK_REJECTED);
    assert_eq!(hook.calls.load(Ordering::SeqCst), 1);
    assert!(dispatch.recipients.lock().unwrap().is_empty());
    assert_table_empty(&store, "messages").await;
    assert_table_empty(&store, "in_flight").await;
}

async fn assert_prepared_token_is_caller_bound_and_consumed_once(store: Arc<Store>) {
    seed_agent(&store, "a_target", "target", "s_target").await;
    let identity = Arc::new(AliasIdentity {
        aliases: Mutex::new(HashMap::from([(
            "target".to_string(),
            agent_caller("a_target", "target", "s_target"),
        )])),
    });
    let dispatch = Arc::new(RecordingDispatch::default());
    let bus = Bus::new(
        store.clone(),
        dispatch.clone(),
        identity,
        Arc::new(NoopEvents),
    );
    let authorized = sender();
    let prepared = bus
        .prepare_notify(
            &authorized,
            NotifySendRequest {
                target: NotifyTarget::Name {
                    name: "target".to_string(),
                },
                source: None,
                body: "caller-bound preparation".to_string(),
                idempotency_key: Some("prepared-caller-bound".to_string()),
            },
        )
        .await
        .unwrap();

    let mut unauthorized = authorized.clone();
    unauthorized.session = SessionId("s_other_sender".into());
    let token = prepared.preparation_id().to_string();
    let error = bus
        .commit_prepared(&unauthorized, PreparedBusSend::resolved(token.clone()))
        .await
        .expect_err("another caller must not consume the preparation");
    assert_eq!(error.code, nexus_contracts::codes::UNAUTHORIZED);
    let error = bus
        .discard_prepared(&unauthorized, PreparedBusSend::resolved(token.clone()))
        .await
        .expect_err("another caller must not discard the preparation");
    assert_eq!(error.code, nexus_contracts::codes::UNAUTHORIZED);
    assert_table_empty(&store, "messages").await;
    assert_table_empty(&store, "in_flight").await;

    bus.commit_prepared(&authorized, PreparedBusSend::resolved(token.clone()))
        .await
        .unwrap();
    let error = bus
        .commit_prepared(&authorized, PreparedBusSend::resolved(token))
        .await
        .expect_err("a preparation must commit at most once");
    assert_eq!(error.code, nexus_contracts::codes::INVALID_PARAMS);
    assert_eq!(dispatch.recipients.lock().unwrap().len(), 1);
}

async fn assert_discard_consumes_preparation_without_effects(store: Arc<Store>) {
    seed_agent(&store, "a_target", "target", "s_target").await;
    let identity = Arc::new(AliasIdentity {
        aliases: Mutex::new(HashMap::from([(
            "target".to_string(),
            agent_caller("a_target", "target", "s_target"),
        )])),
    });
    let dispatch = Arc::new(RecordingDispatch::default());
    let bus = Bus::new(
        store.clone(),
        dispatch.clone(),
        identity,
        Arc::new(NoopEvents),
    );
    let authorized = sender();
    let prepared = bus
        .prepare_notify(
            &authorized,
            NotifySendRequest {
                target: NotifyTarget::Name {
                    name: "target".to_string(),
                },
                source: None,
                body: "discarded preparation".to_string(),
                idempotency_key: Some("prepared-discard".to_string()),
            },
        )
        .await
        .unwrap();

    let token = prepared.preparation_id().to_string();
    bus.discard_prepared(&authorized, PreparedBusSend::resolved(token.clone()))
        .await
        .unwrap();
    let error = bus
        .commit_prepared(&authorized, PreparedBusSend::resolved(token))
        .await
        .expect_err("discard must consume the preparation");
    assert_eq!(error.code, nexus_contracts::codes::INVALID_PARAMS);
    assert!(dispatch.recipients.lock().unwrap().is_empty());
    assert_table_empty(&store, "messages").await;
    assert_table_empty(&store, "in_flight").await;
}

async fn assert_table_empty(store: &Store, table: &str) {
    let mut rows = store
        .conn
        .query(&format!("SELECT COUNT(*) FROM {table}"), ())
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
        0
    );
}

#[tokio::test]
async fn abandoned_preparations_are_bounded_and_expire_without_effects() {
    let store = unified_store().await;
    seed_agent(&store, "a_target", "target", "s_target").await;
    let identity = Arc::new(AliasIdentity {
        aliases: Mutex::new(HashMap::from([(
            "target".to_string(),
            agent_caller("a_target", "target", "s_target"),
        )])),
    });
    let dispatch = Arc::new(RecordingDispatch::default());
    let clock = Arc::new(AtomicI64::new(10_000));
    let now = {
        let clock = clock.clone();
        Arc::new(move || clock.load(Ordering::SeqCst))
    };
    let bus = Bus::new_with_clock(
        store.clone(),
        dispatch.clone(),
        identity,
        Arc::new(NoopEvents),
        now,
    );
    let caller = sender();

    for index in 0..PREPARED_SEND_CAPACITY {
        let abandoned = bus
            .prepare_notify(
                &caller,
                NotifySendRequest {
                    target: NotifyTarget::Name {
                        name: "target".to_string(),
                    },
                    source: None,
                    body: "abandoned preparation".to_string(),
                    idempotency_key: Some(format!("abandoned:{index}")),
                },
            )
            .await
            .unwrap();
        drop(abandoned);
    }
    let error = bus
        .prepare_notify(
            &caller,
            NotifySendRequest {
                target: NotifyTarget::Name {
                    name: "target".to_string(),
                },
                source: None,
                body: "capacity must fail closed".to_string(),
                idempotency_key: Some("abandoned:capacity".to_string()),
            },
        )
        .await
        .expect_err("abandoned preparations must not grow without a bound");
    assert_eq!(error.code, nexus_contracts::codes::INTERNAL_ERROR);

    clock.fetch_add(PREPARED_SEND_TTL_MS + 1, Ordering::SeqCst);
    let after_expiry = bus
        .prepare_notify(
            &caller,
            NotifySendRequest {
                target: NotifyTarget::Name {
                    name: "target".to_string(),
                },
                source: None,
                body: "reaped preparation".to_string(),
                idempotency_key: Some("abandoned:after-expiry".to_string()),
            },
        )
        .await
        .unwrap();
    bus.discard_prepared(&caller, after_expiry).await.unwrap();

    assert!(dispatch.recipients.lock().unwrap().is_empty());
    assert_table_empty(&store, "messages").await;
    assert_table_empty(&store, "in_flight").await;
}

#[tokio::test]
async fn unified_prepared_notify_freezes_recipient_and_hook_result() {
    assert_prepared_notify_freezes_recipient(unified_store().await).await;
}

#[tokio::test]
async fn split_prepared_notify_freezes_recipient_and_hook_result() {
    let (_directory, store) = split_store().await;
    assert_prepared_notify_freezes_recipient(store).await;
}

#[tokio::test]
async fn unified_reclaim_reuses_abandoned_preparation() {
    assert_retry_reuses_abandoned_preparation(unified_store().await).await;
}

#[tokio::test]
async fn split_reclaim_reuses_abandoned_preparation() {
    let (_directory, store) = split_store().await;
    assert_retry_reuses_abandoned_preparation(store).await;
}

#[tokio::test]
async fn unified_hold_target_is_authenticated_by_the_bus() {
    assert_hold_target_is_authenticated_by_the_bus(unified_store().await).await;
}

#[tokio::test]
async fn split_hold_target_is_authenticated_by_the_bus() {
    let (_directory, store) = split_store().await;
    assert_hold_target_is_authenticated_by_the_bus(store).await;
}

#[tokio::test]
async fn unified_cancelled_commit_remains_reclaimable() {
    assert_cancelled_commit_remains_reclaimable(unified_store().await).await;
}

#[tokio::test]
async fn split_cancelled_commit_remains_reclaimable() {
    let (_directory, store) = split_store().await;
    assert_cancelled_commit_remains_reclaimable(store).await;
}

#[tokio::test]
async fn unified_hook_rejection_has_no_prepared_send_effects() {
    assert_rejected_prepare_has_no_effects(unified_store().await).await;
}

#[tokio::test]
async fn split_hook_rejection_has_no_prepared_send_effects() {
    let (_directory, store) = split_store().await;
    assert_rejected_prepare_has_no_effects(store).await;
}

#[tokio::test]
async fn unified_preparation_is_caller_bound_and_one_shot() {
    assert_prepared_token_is_caller_bound_and_consumed_once(unified_store().await).await;
}

#[tokio::test]
async fn split_preparation_is_caller_bound_and_one_shot() {
    let (_directory, store) = split_store().await;
    assert_prepared_token_is_caller_bound_and_consumed_once(store).await;
}

#[tokio::test]
async fn unified_discard_consumes_preparation_without_effects() {
    assert_discard_consumes_preparation_without_effects(unified_store().await).await;
}

#[tokio::test]
async fn split_discard_consumes_preparation_without_effects() {
    let (_directory, store) = split_store().await;
    assert_discard_consumes_preparation_without_effects(store).await;
}
