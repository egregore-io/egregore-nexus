use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use nexus_contracts::ports::{
    BusPort, Caller, ContractError, EventSink, NotifyPort, PortResult, PreparedBusSend,
};
use nexus_contracts::send::{Ack, SendRequest, SendTarget};
use nexus_contracts::threads::{
    CreateThreadRequest, JoinThreadRequest, LeaveThreadRequest, ThreadListResponse,
    ThreadMemberRequest, ThreadMembersRequest, ThreadMembersResponse,
};
use nexus_contracts::topics::{
    SubscribeRequest, SubscribeResponse, TopicListResponse, UnsubscribeRequest,
};
use nexus_contracts::{
    GatewayProjectionEffect, GatewayProjectionKind, MessageId, NotifyRequest, WsEvent,
};
use nexus_notify::{Notify, RouteRule, RoutingRules};
use nexus_store::repos::Topics;
use nexus_store::{DaemonStore, Store};

#[derive(Default)]
struct BusCalls {
    targets: Vec<SendTarget>,
    idempotency_keys: Vec<Option<String>>,
}

struct MockBus {
    calls: Mutex<BusCalls>,
    known: Vec<String>,
    prepare_calls: AtomicUsize,
    reject_prepare_call: Option<usize>,
    commit_calls: AtomicUsize,
    reject_commit_call: Option<usize>,
    prepared: Mutex<HashMap<String, (Caller, SendRequest)>>,
    accepted: Mutex<HashMap<String, Ack>>,
}

impl MockBus {
    fn new(known: &[&str]) -> Self {
        Self {
            calls: Mutex::new(BusCalls::default()),
            known: known.iter().map(|name| (*name).to_string()).collect(),
            prepare_calls: AtomicUsize::new(0),
            reject_prepare_call: None,
            commit_calls: AtomicUsize::new(0),
            reject_commit_call: None,
            prepared: Mutex::new(HashMap::new()),
            accepted: Mutex::new(HashMap::new()),
        }
    }

    fn rejecting_prepare(known: &[&str], call: usize) -> Self {
        Self {
            reject_prepare_call: Some(call),
            ..Self::new(known)
        }
    }

    fn rejecting_commit_once(known: &[&str], call: usize) -> Self {
        Self {
            reject_commit_call: Some(call),
            ..Self::new(known)
        }
    }

    fn targets(&self) -> Vec<SendTarget> {
        self.calls.lock().unwrap().targets.clone()
    }

    fn idempotency_keys(&self) -> Vec<Option<String>> {
        self.calls.lock().unwrap().idempotency_keys.clone()
    }

    fn validate(&self, request: &SendRequest) -> PortResult<()> {
        if let SendTarget::Dm { name, agent_id } = &request.to {
            let recipient = agent_id
                .as_ref()
                .map(|id| id.0.as_str())
                .or(name.as_deref())
                .unwrap_or("");
            if !self.known.iter().any(|known| known == recipient) {
                return Err(ContractError {
                    code: nexus_contracts::codes::NOT_FOUND,
                    message: format!("no such recipient: {recipient}"),
                });
            }
        }
        Ok(())
    }
}

#[async_trait]
impl BusPort for MockBus {
    async fn preflight_send(&self, _caller: &Caller, request: &SendRequest) -> PortResult<()> {
        self.validate(request)
    }

    async fn prepare_send(
        &self,
        caller: &Caller,
        request: SendRequest,
        _sender_kind: Option<nexus_contracts::Kind>,
    ) -> PortResult<PreparedBusSend> {
        self.validate(&request)?;
        if let Some(idempotency_key) = request.idempotency_key.as_deref() {
            if let Some(token) =
                self.prepared
                    .lock()
                    .unwrap()
                    .iter()
                    .find_map(|(token, (authorized, accepted))| {
                        (authorized == caller
                            && accepted.idempotency_key.as_deref() == Some(idempotency_key))
                        .then(|| token.clone())
                    })
            {
                return Ok(PreparedBusSend::resolved(token));
            }
            if self.accepted.lock().unwrap().contains_key(idempotency_key) {
                let token = format!("mock:accepted:{idempotency_key}");
                self.prepared
                    .lock()
                    .unwrap()
                    .insert(token.clone(), (caller.clone(), request));
                return Ok(PreparedBusSend::resolved(token));
            }
        }
        let call = self.prepare_calls.fetch_add(1, Ordering::SeqCst) + 1;
        if self.reject_prepare_call == Some(call) {
            return Err(ContractError {
                code: nexus_contracts::codes::HOOK_REJECTED,
                message: "before_send hook rejected the prepared effect".into(),
            });
        }
        let token = format!("mock:{call}");
        self.prepared
            .lock()
            .unwrap()
            .insert(token.clone(), (caller.clone(), request));
        Ok(PreparedBusSend::resolved(token))
    }

    async fn commit_prepared(&self, caller: &Caller, prepared: PreparedBusSend) -> PortResult<Ack> {
        let token = prepared.preparation_id();
        let (authorized, request) = self
            .prepared
            .lock()
            .unwrap()
            .get(token)
            .cloned()
            .ok_or_else(|| ContractError {
                code: nexus_contracts::codes::INVALID_PARAMS,
                message: "unknown or consumed mock preparation".into(),
            })?;
        if &authorized != caller {
            return Err(ContractError {
                code: nexus_contracts::codes::UNAUTHORIZED,
                message: "mock preparation belongs to another caller".into(),
            });
        }
        let call = self.commit_calls.fetch_add(1, Ordering::SeqCst) + 1;
        if self.reject_commit_call == Some(call) {
            return Err(ContractError {
                code: nexus_contracts::codes::INTERNAL_ERROR,
                message: "injected prepared commit failure".into(),
            });
        }
        self.prepared.lock().unwrap().remove(token);
        self.send(caller, request).await
    }

    async fn discard_prepared(&self, caller: &Caller, prepared: PreparedBusSend) -> PortResult<()> {
        let token = prepared.preparation_id();
        let authorized = self
            .prepared
            .lock()
            .unwrap()
            .get(token)
            .map(|(authorized, _)| authorized.clone())
            .ok_or_else(|| ContractError {
                code: nexus_contracts::codes::INVALID_PARAMS,
                message: "unknown or consumed mock preparation".into(),
            })?;
        if &authorized != caller {
            return Err(ContractError {
                code: nexus_contracts::codes::UNAUTHORIZED,
                message: "mock preparation belongs to another caller".into(),
            });
        }
        self.prepared.lock().unwrap().remove(token);
        Ok(())
    }

    async fn preflight_notify_target(
        &self,
        _caller: &Caller,
        target: &nexus_contracts::NotifyTarget,
    ) -> PortResult<Option<nexus_contracts::AgentId>> {
        Ok(match target {
            nexus_contracts::NotifyTarget::Agent { agent_id } => Some(agent_id.clone()),
            _ => None,
        })
    }

    async fn send(&self, _caller: &Caller, request: SendRequest) -> PortResult<Ack> {
        self.validate(&request)?;

        if let Some(key) = request.idempotency_key.as_ref() {
            if let Some(ack) = self.accepted.lock().unwrap().get(key).cloned() {
                return Ok(ack);
            }
        }

        let mut calls = self.calls.lock().unwrap();
        calls.targets.push(request.to);
        calls.idempotency_keys.push(request.idempotency_key.clone());
        let ack = Ack {
            message_id: MessageId(format!("m_sent_{}", calls.targets.len())),
            fanout: None,
        };
        drop(calls);
        if let Some(key) = request.idempotency_key {
            self.accepted.lock().unwrap().insert(key, ack.clone());
        }
        Ok(ack)
    }

    async fn create_thread(
        &self,
        _caller: &Caller,
        _request: CreateThreadRequest,
    ) -> PortResult<()> {
        Ok(())
    }

    async fn join_thread(&self, _caller: &Caller, _request: JoinThreadRequest) -> PortResult<()> {
        Ok(())
    }

    async fn leave_thread(&self, _caller: &Caller, _request: LeaveThreadRequest) -> PortResult<()> {
        Ok(())
    }

    async fn add_thread_member(
        &self,
        _caller: &Caller,
        _request: ThreadMemberRequest,
    ) -> PortResult<()> {
        Ok(())
    }

    async fn remove_thread_member(
        &self,
        _caller: &Caller,
        _request: ThreadMemberRequest,
    ) -> PortResult<()> {
        Ok(())
    }

    async fn threads(&self, _caller: &Caller) -> PortResult<ThreadListResponse> {
        Ok(ThreadListResponse { threads: vec![] })
    }

    async fn thread_members(
        &self,
        _caller: &Caller,
        _request: ThreadMembersRequest,
    ) -> PortResult<ThreadMembersResponse> {
        Ok(ThreadMembersResponse {
            name: String::new(),
            members: vec![],
        })
    }

    async fn subscribe(
        &self,
        _caller: &Caller,
        _request: SubscribeRequest,
    ) -> PortResult<SubscribeResponse> {
        Ok(SubscribeResponse {
            topic: String::new(),
            cursor: 0,
        })
    }

    async fn unsubscribe(&self, _caller: &Caller, _request: UnsubscribeRequest) -> PortResult<()> {
        Ok(())
    }

    async fn topics(&self, _caller: &Caller) -> PortResult<TopicListResponse> {
        Ok(TopicListResponse { topics: vec![] })
    }
}

struct NoopEvents;

#[async_trait]
impl EventSink for NoopEvents {
    async fn emit(&self, _event: WsEvent) {}
}

#[derive(Default)]
struct RecordingEvents {
    projected: Mutex<Vec<GatewayProjectionEffect>>,
}

#[async_trait]
impl EventSink for RecordingEvents {
    async fn emit(&self, _event: WsEvent) {}

    async fn project(&self, effect: GatewayProjectionEffect) {
        self.projected.lock().unwrap().push(effect);
    }
}

async fn migrated_store() -> Arc<Store> {
    let store = Store::open(":memory:").await.unwrap();
    store.migrate().await.unwrap();
    Arc::new(store)
}

async fn split_store() -> Arc<Store> {
    let daemon = DaemonStore::open(":memory:").await.unwrap();
    Arc::new(daemon.compatibility_store())
}

async fn assert_table_count(store: &Store, table: &str, expected: i64) {
    let mut rows = store
        .conn
        .query(&format!("SELECT COUNT(*) FROM {table}"), ())
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
        expected,
        "unexpected {table} row count"
    );
}

#[tokio::test]
async fn verified_ingest_derives_stable_distinct_keys_for_pub_and_each_dm() {
    let store = migrated_store().await;
    let bus = Arc::new(MockBus::new(&["ana", "a_unnamed"]));
    let notify = Notify::new(
        store.clone(),
        bus.clone(),
        Arc::new(NoopEvents),
        RoutingRules::new(vec![
            RouteRule {
                source: Some("pager".into()),
                topic: None,
                to: "ana".into(),
            },
            RouteRule {
                source: Some("pager".into()),
                topic: None,
                to: "a_unnamed".into(),
            },
        ]),
    );
    let request = NotifyRequest {
        source: "pager".into(),
        topic: None,
        payload: serde_json::json!({"incident": "INC-1"}),
    };

    let first = notify
        .ingest_verified(request.clone(), "notify:root-command".into())
        .await
        .unwrap();
    let replay = notify
        .ingest_verified(request, "notify:root-command".into())
        .await
        .unwrap();
    assert_eq!(replay, first);

    let keys = bus.idempotency_keys();
    assert_eq!(keys.len(), 3, "reclaim must not commit any effect twice");
    assert!(keys.iter().all(Option::is_some));
    let mut unique = keys
        .iter()
        .map(|key| key.as_deref().unwrap())
        .collect::<Vec<_>>();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(unique.len(), 3, "pub and each DM require distinct keys");

    let targets = bus.targets();
    assert!(targets.iter().any(|target| matches!(
        target,
        SendTarget::Dm { name, agent_id }
            if name.as_deref() == Some("ana") && agent_id.is_none()
    )));
    assert_table_count(&store, "messages", 1).await;
    assert_table_count(&store, "notifications", 1).await;
    assert!(targets.iter().any(|target| matches!(
        target,
        SendTarget::Dm { name, agent_id }
            if name.as_deref() == Some("a_unnamed") && agent_id.is_none()
    )));
}

#[tokio::test]
async fn verified_topic_ingest_separates_pub_and_routed_topic_keys() {
    let store = migrated_store().await;
    Topics::new(&store).ensure("ci", "nexus").await.unwrap();
    Topics::new(&store)
        .subscribe("ci", "ben", None)
        .await
        .unwrap();
    let bus = Arc::new(MockBus::new(&["ben"]));
    let notify = Notify::new(
        store.clone(),
        bus.clone(),
        Arc::new(NoopEvents),
        RoutingRules::default(),
    );
    let request = NotifyRequest {
        source: "github".into(),
        topic: Some("ci".into()),
        payload: serde_json::json!({"status": "green"}),
    };

    let first = notify
        .ingest_verified(request.clone(), "notify:topic-root".into())
        .await
        .unwrap();
    let replay = notify
        .ingest_verified(request, "notify:topic-root".into())
        .await
        .unwrap();
    assert_eq!(replay, first);

    let keys = bus.idempotency_keys();
    assert_eq!(keys.len(), 2, "reclaim must not commit any effect twice");
    assert_ne!(keys[0], keys[1], "Pub and routed topic effects must differ");
    assert!(matches!(
        &bus.targets()[..],
        [
            SendTarget::Publish { topic: first },
            SendTarget::Publish { topic: second }
        ] if first == "pub" && second == "ci"
    ));
    assert_table_count(&store, "messages", 1).await;
    assert_table_count(&store, "notifications", 1).await;
}

async fn assert_verified_reclaim_resumes_partial_commit_once(store: Arc<Store>) {
    let split = store.has_split_authority();
    let bus = Arc::new(MockBus::rejecting_commit_once(&["ana", "ben"], 2));
    let events = Arc::new(RecordingEvents::default());
    let notify = Notify::new(
        store.clone(),
        bus.clone(),
        events.clone(),
        RoutingRules::new(vec![
            RouteRule {
                source: Some("pager".into()),
                topic: None,
                to: "ana".into(),
            },
            RouteRule {
                source: Some("pager".into()),
                topic: None,
                to: "ben".into(),
            },
        ]),
    );
    let request = NotifyRequest {
        source: "pager".into(),
        topic: None,
        payload: serde_json::json!({"incident": "resume-partial"}),
    };

    let error = notify
        .ingest_verified(request.clone(), "notify:partial-root".into())
        .await
        .expect_err("the injected second-effect failure must leave the ingest reclaimable");
    assert_eq!(error.code, nexus_contracts::codes::INTERNAL_ERROR);
    assert_table_count(&store, "messages", 1).await;
    if !split {
        assert_table_count(&store, "notifications", 0).await;
    }

    let recovered = notify
        .ingest_verified(request.clone(), "notify:partial-root".into())
        .await
        .unwrap();
    let replay = notify
        .ingest_verified(request, "notify:partial-root".into())
        .await
        .unwrap();

    assert_eq!(replay, recovered);
    assert_table_count(&store, "messages", 1).await;
    if !split {
        assert_table_count(&store, "notifications", 1).await;
    }
    assert_eq!(
        bus.targets().len(),
        3,
        "Pub and both frozen DMs must each commit exactly once across failure and reclaim"
    );
    assert_eq!(
        bus.idempotency_keys().len(),
        3,
        "reclaim must not create duplicate bus effects"
    );
    assert_eq!(
        bus.prepare_calls.load(Ordering::SeqCst),
        3,
        "reclaim must reuse every accepted preparation instead of re-running later hooks"
    );
    let notification_event_ids = events
        .projected
        .lock()
        .unwrap()
        .iter()
        .filter(|effect| effect.kind == GatewayProjectionKind::NotificationEmitted)
        .map(|effect| effect.event_id.clone())
        .collect::<Vec<_>>();
    assert!(!notification_event_ids.is_empty());
    assert!(
        notification_event_ids
            .iter()
            .all(|event_id| event_id == &notification_event_ids[0]),
        "reclaim may republish only the same idempotent Gateway audit fact"
    );
    assert_eq!(
        recovered.notif_id.0,
        notification_event_ids[0]
            .strip_prefix("notification:")
            .expect("notification projection id"),
        "response, standalone message, and Gateway audit must share one logical id"
    );
}

#[tokio::test]
async fn unified_verified_reclaim_resumes_partial_commit_once() {
    assert_verified_reclaim_resumes_partial_commit_once(migrated_store().await).await;
}

#[tokio::test]
async fn split_verified_reclaim_resumes_partial_commit_once() {
    assert_verified_reclaim_resumes_partial_commit_once(split_store().await).await;
}

#[tokio::test]
async fn verified_ingest_preflights_every_route_before_any_durable_side_effect() {
    let store = migrated_store().await;
    let bus = Arc::new(MockBus::new(&[]));
    let notify = Notify::new(
        store.clone(),
        bus.clone(),
        Arc::new(NoopEvents),
        RoutingRules::new(vec![RouteRule {
            source: Some("pager".into()),
            topic: None,
            to: "ambiguous-or-missing".into(),
        }]),
    );

    let error = notify
        .ingest_verified(
            NotifyRequest {
                source: "pager".into(),
                topic: None,
                payload: serde_json::json!({"incident": "must-not-commit"}),
            },
            "notify:preflight-root".into(),
        )
        .await
        .expect_err("an unresolved route must reject the complete verified ingest");

    assert_eq!(error.code, nexus_contracts::codes::NOT_FOUND);
    assert!(bus.targets().is_empty(), "no Pub or routed send may begin");
    for table in ["messages", "in_flight", "notifications"] {
        let mut rows = store
            .conn
            .query(&format!("SELECT COUNT(*) FROM {table}"), ())
            .await
            .unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
            0,
            "verified preflight failure must leave {table} empty"
        );
    }
}

async fn assert_second_hook_rejection_aborts_every_verified_effect(store: Arc<Store>) {
    let bus = Arc::new(MockBus::rejecting_prepare(&["ana"], 2));
    let notify = Notify::new(
        store.clone(),
        bus.clone(),
        Arc::new(NoopEvents),
        RoutingRules::new(vec![RouteRule {
            source: Some("pager".into()),
            topic: None,
            to: "ana".into(),
        }]),
    );

    let error = notify
        .ingest_verified(
            NotifyRequest {
                source: "pager".into(),
                topic: None,
                payload: serde_json::json!({"incident": "reject-second-effect"}),
            },
            "notify:reject-second".into(),
        )
        .await
        .expect_err("the second prepared hook must reject the entire ingest");

    assert_eq!(error.code, nexus_contracts::codes::HOOK_REJECTED);
    assert_eq!(bus.prepare_calls.load(Ordering::SeqCst), 2);
    assert!(bus.targets().is_empty(), "no prepared effect may commit");
    for table in ["messages", "in_flight", "notifications"] {
        let mut table_rows = store
            .conn
            .query(
                &format!(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = '{table}'"
                ),
                (),
            )
            .await
            .unwrap();
        if table_rows
            .next()
            .await
            .unwrap()
            .unwrap()
            .get::<i64>(0)
            .unwrap()
            == 0
        {
            continue;
        }
        let mut rows = store
            .conn
            .query(&format!("SELECT COUNT(*) FROM {table}"), ())
            .await
            .unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
            0,
            "hook rejection must leave {table} empty"
        );
    }
}

#[tokio::test]
async fn unified_second_hook_rejection_aborts_every_verified_effect_before_commit() {
    assert_second_hook_rejection_aborts_every_verified_effect(migrated_store().await).await;
}

#[tokio::test]
async fn split_second_hook_rejection_aborts_every_verified_effect_before_commit() {
    assert_second_hook_rejection_aborts_every_verified_effect(split_store().await).await;
}
