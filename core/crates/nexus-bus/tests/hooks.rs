use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use nexus_bus::Bus;
use nexus_contracts::ack::{AckRequest, AckResponse, AckThreadsRequest};
use nexus_contracts::batch::{ConsumeRequest, NexusBatch};
use nexus_contracts::events::WsEvent;
use nexus_contracts::notify::{NotifySendRequest, NotifyTarget};
use nexus_contracts::ports::{
    BusPort, Caller, DispatchPort, EventSink, IdentityPort, MessageHookPort, PortResult,
};
use nexus_contracts::register::{
    HeartbeatResponse, MemberListRequest, MemberListResponse, RegisterRequest, RegisterResponse,
    StatusRequest, StatusResponse, Whoami,
};
use nexus_contracts::{
    codes, AgentId, DeliveryTiming, HookAction, HookBeforeSendRequest, HookBeforeSendResult,
    HookMessage, MessageId, SendRequest, SendTarget, SessionId, Tier,
};
use nexus_store::Store;

#[derive(Clone, Copy)]
enum HookBehavior {
    Transform,
    Reject,
    Unavailable,
}

struct RecordingHook {
    behavior: HookBehavior,
    calls: AtomicUsize,
    evaluation_ids: Mutex<Vec<String>>,
}

impl RecordingHook {
    fn new(behavior: HookBehavior) -> Self {
        Self {
            behavior,
            calls: AtomicUsize::new(0),
            evaluation_ids: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl MessageHookPort for RecordingHook {
    async fn before_send(
        &self,
        request: HookBeforeSendRequest,
    ) -> PortResult<HookBeforeSendResult> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.evaluation_ids
            .lock()
            .expect("evaluation ids")
            .push(request.evaluation_id.clone());
        match self.behavior {
            HookBehavior::Transform => {
                let mut message = request.message;
                message.body = "transformed by hook".into();
                message.summary = Some("hook summary".into());
                message.mention = vec!["ben".into()];
                message
                    .metadata
                    .insert("reviewed".into(), serde_json::json!(true));
                Ok(HookBeforeSendResult {
                    evaluation_id: request.evaluation_id,
                    action: HookAction::Continue,
                    message,
                    timing: Some(DeliveryTiming::AfterToolLoop),
                    executed_by: Vec::new(),
                })
            }
            HookBehavior::Reject => Ok(HookBeforeSendResult {
                evaluation_id: request.evaluation_id,
                action: HookAction::Reject,
                message: request.message,
                timing: None,
                executed_by: Vec::new(),
            }),
            HookBehavior::Unavailable => Err(nexus_contracts::ContractError {
                code: codes::HOOK_GATEWAY_UNAVAILABLE,
                message: "hook-capable Gateway is unavailable".into(),
            }),
        }
    }
}

#[derive(Default)]
struct NoopDispatch;

#[async_trait]
impl DispatchPort for NoopDispatch {
    async fn enqueue(&self, _recipient: &SessionId, _message: &MessageId) -> PortResult<()> {
        Ok(())
    }

    async fn consume(&self, _caller: &Caller, _req: ConsumeRequest) -> PortResult<NexusBatch> {
        unimplemented!()
    }

    async fn ack(&self, _caller: &Caller, _req: AckRequest) -> PortResult<AckResponse> {
        unimplemented!()
    }

    async fn ack_threads(
        &self,
        _caller: &Caller,
        _req: AckThreadsRequest,
    ) -> PortResult<AckResponse> {
        unimplemented!()
    }
}

struct NoopSink;

#[async_trait]
impl EventSink for NoopSink {
    async fn emit(&self, _event: WsEvent) {}
}

struct MockIdentity {
    sessions: HashMap<String, SessionId>,
}

#[async_trait]
impl IdentityPort for MockIdentity {
    async fn register(&self, _req: RegisterRequest) -> PortResult<RegisterResponse> {
        unimplemented!()
    }

    async fn whoami(&self, _caller: &Caller) -> PortResult<Whoami> {
        unimplemented!()
    }

    async fn resolve(&self, project: &str, name: &str) -> PortResult<Caller> {
        Ok(Caller {
            agent_id: None,
            session: self.sessions[name].clone(),
            name: name.into(),
            project: project.into(),
            tier: Tier::Agent,
            locality: Default::default(),
            access: None,
            principal_id: None,
        })
    }

    async fn members(
        &self,
        _caller: &Caller,
        _req: MemberListRequest,
    ) -> PortResult<MemberListResponse> {
        unimplemented!()
    }

    async fn status(&self, _caller: &Caller, _req: StatusRequest) -> PortResult<StatusResponse> {
        unimplemented!()
    }

    async fn heartbeat(&self, _caller: &Caller) -> PortResult<HeartbeatResponse> {
        unimplemented!()
    }

    async fn assign_project(
        &self,
        _name: &str,
        _to_project: &str,
    ) -> PortResult<nexus_contracts::AssignProjectResponse> {
        unimplemented!()
    }
}

async fn build(hook: Arc<RecordingHook>) -> (Arc<Store>, Bus) {
    let store = Arc::new(Store::open(":memory:").await.expect("store"));
    store.migrate().await.expect("migrate");
    let identity = Arc::new(MockIdentity {
        sessions: HashMap::from([("ben".into(), SessionId("s_ben".into()))]),
    });
    let bus = Bus::new_with_message_hooks(
        store.clone(),
        Arc::new(NoopDispatch),
        identity,
        Arc::new(NoopSink),
        hook,
    );
    (store, bus)
}

fn caller() -> Caller {
    Caller {
        agent_id: Some(AgentId("a_ana".into())),
        session: SessionId("s_ana".into()),
        name: "ana".into(),
        project: "default".into(),
        tier: Tier::Agent,
        locality: Default::default(),
        access: None,
        principal_id: None,
    }
}

fn request(key: &str) -> SendRequest {
    SendRequest {
        to: SendTarget::dm_name("ben"),
        summary: None,
        body: "original body".into(),
        mention: Vec::new(),
        metadata: Some(serde_json::Map::from_iter([(
            "source".into(),
            serde_json::json!("test"),
        )])),
        idempotency_key: Some(key.into()),
    }
}

#[tokio::test]
async fn before_send_transforms_message_and_metadata_in_one_canonical_commit() {
    let hook = Arc::new(RecordingHook::new(HookBehavior::Transform));
    let (store, bus) = build(hook.clone()).await;

    let ack = bus
        .send(&caller(), request("hook-transform-1"))
        .await
        .unwrap();

    let mut rows = store
        .conn
        .query(
            "SELECT body, summary, metadata_json, mention_json
             FROM messages WHERE message_id = ?1",
            libsql::params![ack.message_id.0.clone()],
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().expect("message row");
    assert_eq!(row.get::<String>(0).unwrap(), "transformed by hook");
    assert_eq!(row.get::<String>(1).unwrap(), "hook summary");
    let metadata: serde_json::Value = serde_json::from_str(&row.get::<String>(2).unwrap()).unwrap();
    assert_eq!(
        metadata,
        serde_json::json!({"reviewed": true, "source": "test"})
    );
    let mention: Vec<String> = serde_json::from_str(&row.get::<String>(3).unwrap()).unwrap();
    assert_eq!(mention, vec!["ben"]);
    let mut delivery = store
        .conn
        .query(
            "SELECT delivery_timing FROM in_flight WHERE message_id = ?1",
            libsql::params![ack.message_id.0],
        )
        .await
        .unwrap();
    assert_eq!(
        delivery
            .next()
            .await
            .unwrap()
            .expect("delivery row")
            .get::<String>(0)
            .unwrap(),
        "after_tool_loop"
    );
    assert_eq!(hook.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn accepted_idempotency_retry_returns_existing_ack_without_rerunning_hooks() {
    let hook = Arc::new(RecordingHook::new(HookBehavior::Transform));
    let (_store, bus) = build(hook.clone()).await;

    let first = bus
        .send(&caller(), request("hook-idempotent-1"))
        .await
        .unwrap();
    let second = bus
        .send(&caller(), request("hook-idempotent-1"))
        .await
        .unwrap();

    assert_eq!(first.message_id, second.message_id);
    assert_eq!(hook.calls.load(Ordering::SeqCst), 1);
    let ids = hook.evaluation_ids.lock().unwrap();
    assert_eq!(ids.len(), 1);
    assert!(ids[0].starts_with("he_"));
}

#[tokio::test]
async fn hook_rejection_creates_no_message_or_delivery_rows() {
    let hook = Arc::new(RecordingHook::new(HookBehavior::Reject));
    let (store, bus) = build(hook).await;

    let error = bus
        .send(&caller(), request("hook-reject-1"))
        .await
        .expect_err("hook must reject before acceptance");
    assert_eq!(error.code, codes::HOOK_REJECTED);
    for table in ["messages", "in_flight"] {
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
}

#[tokio::test]
async fn required_hook_gateway_failure_is_typed_and_pre_accept() {
    let hook = Arc::new(RecordingHook::new(HookBehavior::Unavailable));
    let (store, bus) = build(hook).await;

    let error = bus
        .send(&caller(), request("hook-required-1"))
        .await
        .expect_err("required Gateway must fail closed");
    assert_eq!(error.code, codes::HOOK_GATEWAY_UNAVAILABLE);
    let mut rows = store
        .conn
        .query("SELECT COUNT(*) FROM messages", ())
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
        0
    );
}

#[tokio::test]
async fn notification_source_uses_the_same_before_send_boundary() {
    let hook = Arc::new(RecordingHook::new(HookBehavior::Transform));
    let (store, bus) = build(hook.clone()).await;

    let ack = bus
        .notify(
            &caller(),
            NotifySendRequest {
                target: NotifyTarget::Name { name: "ben".into() },
                source: Some("buildbot".into()),
                body: "build finished".into(),
                idempotency_key: Some("hook-notify-1".into()),
            },
        )
        .await
        .unwrap();

    let mut rows = store
        .conn
        .query(
            "SELECT from_name, provenance, body FROM messages WHERE message_id = ?1",
            libsql::params![ack.message_id.0],
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().expect("notification row");
    assert_eq!(row.get::<String>(0).unwrap(), "buildbot");
    let provenance: serde_json::Value =
        serde_json::from_str(&row.get::<String>(1).unwrap()).unwrap();
    assert_eq!(provenance["kind"], "notification");
    assert_eq!(row.get::<String>(2).unwrap(), "transformed by hook");
    assert_eq!(hook.calls.load(Ordering::SeqCst), 1);
}

#[allow(dead_code)]
fn assert_hook_message_is_public(_message: HookMessage) {}
