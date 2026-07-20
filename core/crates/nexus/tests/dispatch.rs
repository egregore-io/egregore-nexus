//! Task 14 tests: the transport-agnostic method-registry `dispatch`.
//!
//! Builds an `AppState` from mock ports + an in-memory store, then drives `dispatch` directly:
//! `send` routes to the bus and returns an `Ack`; an unknown method → -32601; bad params → -32602;
//! an admin method from an agent → -32001; an unauthenticated `send` is rejected.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use nexus::daemon::{dispatch, AppState, WsSink};
use nexus_contracts::{
    codes, Ack, AckRequest, AckResponse, AckThreadsRequest, AgentUpdateKind, AssignProjectRequest,
    AssignProjectResponse, AssignRoleRequest, AssignRoleResponse, Caller, ChannelOp,
    ChannelRequest, ConsumeRequest, ContractError, CreateThreadRequest, EventSink,
    GrantTierRequest, HeartbeatResponse, HistoryRequest, HistoryResponse, JoinThreadRequest,
    LeaveThreadRequest, MemberListRequest, MemberListResponse, MessageId, MetadataEntityKind,
    MetadataResponse, MonitorRequest, NexusBatch, NotifyRequest, NotifyResponse, NotifySendRequest,
    NotifyTarget, PortResult, RegisterRequest, RegisterResponse, RemoveRequest, RemoveResponse,
    Request, RequestId, RouteForwardRequest, SearchRequest, SearchResponse, SendRequest,
    SendTarget, SessionId, SpawnRequest, SpawnResponse, StatusRequest, StatusResponse,
    SubscribeRequest, SubscribeResponse, ThreadListResponse, ThreadMemberRequest,
    ThreadMembersRequest, ThreadMembersResponse, Tier, TopicListResponse, UnsubscribeRequest,
    Whoami, WsEvent,
};
use nexus_store::repos::{
    Agents, DeveloperEvents, NewAgent, NewSession, Sessions, AGENT_LIFECYCLE_TOPIC,
};
use nexus_store::Store;

#[derive(Clone, Default)]
struct SlowOpenAdapter {
    opens: Arc<AtomicUsize>,
}

#[derive(Default)]
struct DefinitelyOfflineTurnExec {
    injects: AtomicUsize,
}

#[async_trait]
impl nexus_contracts::AgentTurnExecutionPort for DefinitelyOfflineTurnExec {
    async fn inject_turn(&self, _r: &SessionId, _b: &NexusBatch) -> PortResult<()> {
        self.injects.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn inject_turn_observed(
        &self,
        _recipient: &SessionId,
        _batch: &NexusBatch,
        _events: Arc<dyn nexus_contracts::EventSink>,
        _accepted_event: nexus_contracts::WsEvent,
    ) -> nexus_contracts::InjectResult<()> {
        self.injects.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn prompt(&self, _r: &SessionId, _t: String) -> PortResult<()> {
        Ok(())
    }

    async fn launch(&self, _req: SpawnRequest) -> PortResult<SpawnResponse> {
        unreachable!("fixture registers the target directly")
    }

    async fn remove(&self, _req: RemoveRequest) -> PortResult<RemoveResponse> {
        unreachable!("fixture does not remove through the turn executor")
    }

    fn is_harness_alive(&self, _recipient: &SessionId) -> Option<bool> {
        Some(false)
    }
}

#[async_trait]
impl nexus_agent::Adapter for SlowOpenAdapter {
    async fn open_session(&self) -> Result<(), nexus_common::NexusError> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        Ok(())
    }

    async fn resume(&self, _resume_key: &str) -> Result<(), nexus_common::NexusError> {
        self.open_session().await
    }

    async fn inject(&self, _prompt: String) -> Result<(), nexus_agent::AdapterInjectError> {
        Ok(())
    }

    async fn stream_updates(
        &self,
    ) -> Result<Vec<nexus_agent::StreamEvent>, nexus_common::NexusError> {
        Ok(Vec::new())
    }
}

// ---- Mock ports ----

struct MockIdentity;
#[async_trait]
impl nexus_contracts::IdentityPort for MockIdentity {
    async fn register(&self, _req: RegisterRequest) -> PortResult<RegisterResponse> {
        Ok(RegisterResponse {
            agent_id: None,
            session_id: SessionId("s_mock".into()),
            directive: "untagged = your human, <nexus> = the bus".into(),
        })
    }
    async fn whoami(&self, _caller: &Caller) -> PortResult<Whoami> {
        unimplemented!()
    }
    async fn resolve(&self, _project: &str, _name: &str) -> PortResult<Caller> {
        unimplemented!()
    }
    async fn members(&self, _c: &Caller, _r: MemberListRequest) -> PortResult<MemberListResponse> {
        unimplemented!()
    }
    async fn status(&self, _c: &Caller, _r: StatusRequest) -> PortResult<StatusResponse> {
        unimplemented!()
    }
    async fn heartbeat(&self, _c: &Caller) -> PortResult<HeartbeatResponse> {
        unimplemented!()
    }
    async fn assign_project(
        &self,
        _name: &str,
        _to_project: &str,
    ) -> PortResult<AssignProjectResponse> {
        unimplemented!()
    }
}

#[derive(Default)]
struct MockBus {
    notifications: Mutex<Vec<(Caller, NotifySendRequest)>>,
}

impl MockBus {
    fn notification_calls(&self) -> Vec<(Caller, NotifySendRequest)> {
        self.notifications.lock().unwrap().clone()
    }
}

#[async_trait]
impl nexus_contracts::BusPort for MockBus {
    async fn preflight_send(&self, _caller: &Caller, _req: &SendRequest) -> PortResult<()> {
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
    async fn send(&self, _caller: &Caller, _req: SendRequest) -> PortResult<Ack> {
        Ok(Ack {
            message_id: MessageId("m_mock".into()),
            fanout: None,
        })
    }
    async fn notify(&self, caller: &Caller, req: NotifySendRequest) -> PortResult<Ack> {
        self.notifications
            .lock()
            .unwrap()
            .push((caller.clone(), req));
        Ok(Ack {
            message_id: MessageId("m_notification".into()),
            fanout: Some(1),
        })
    }
    async fn create_thread(&self, _c: &Caller, _r: CreateThreadRequest) -> PortResult<()> {
        Ok(())
    }
    async fn join_thread(&self, _c: &Caller, _r: JoinThreadRequest) -> PortResult<()> {
        Ok(())
    }
    async fn leave_thread(&self, _c: &Caller, _r: LeaveThreadRequest) -> PortResult<()> {
        Ok(())
    }
    async fn add_thread_member(&self, _c: &Caller, _r: ThreadMemberRequest) -> PortResult<()> {
        Ok(())
    }
    async fn remove_thread_member(&self, _c: &Caller, _r: ThreadMemberRequest) -> PortResult<()> {
        Ok(())
    }
    async fn threads(&self, _c: &Caller) -> PortResult<ThreadListResponse> {
        unimplemented!()
    }
    async fn thread_members(
        &self,
        _c: &Caller,
        _r: ThreadMembersRequest,
    ) -> PortResult<ThreadMembersResponse> {
        unimplemented!()
    }
    async fn subscribe(&self, _c: &Caller, _r: SubscribeRequest) -> PortResult<SubscribeResponse> {
        unimplemented!()
    }
    async fn unsubscribe(&self, _c: &Caller, _r: UnsubscribeRequest) -> PortResult<()> {
        Ok(())
    }
    async fn topics(&self, _c: &Caller) -> PortResult<TopicListResponse> {
        unimplemented!()
    }
}

struct MockRealtime;
#[async_trait]
impl nexus_contracts::DispatchPort for MockRealtime {
    async fn enqueue(&self, _r: &SessionId, _m: &MessageId) -> PortResult<()> {
        Ok(())
    }
    async fn consume(&self, _c: &Caller, _r: ConsumeRequest) -> PortResult<NexusBatch> {
        unimplemented!()
    }
    async fn ack(&self, _c: &Caller, _r: AckRequest) -> PortResult<AckResponse> {
        unimplemented!()
    }
    async fn ack_threads(&self, _c: &Caller, _r: AckThreadsRequest) -> PortResult<AckResponse> {
        unimplemented!()
    }
}

struct MockAgent;
#[async_trait]
impl nexus_contracts::AgentTurnExecutionPort for MockAgent {
    async fn inject_turn(&self, _r: &SessionId, _b: &NexusBatch) -> PortResult<()> {
        Ok(())
    }
    async fn prompt(&self, _r: &SessionId, _t: String) -> PortResult<()> {
        Ok(())
    }
    async fn steer_observed(
        &self,
        _recipient: &SessionId,
        _text: String,
        events: Arc<dyn nexus_contracts::EventSink>,
        accepted_event: nexus_contracts::WsEvent,
    ) -> PortResult<nexus_contracts::SteerResponse> {
        events.emit(accepted_event).await;
        Ok(nexus_contracts::SteerResponse {
            accepted: true,
            delivery: nexus_contracts::SteerDelivery::Steered,
            turn_id: Some("turn_native".into()),
        })
    }
    async fn compact(&self, _recipient: &SessionId) -> PortResult<()> {
        Ok(())
    }
    async fn launch(&self, _req: SpawnRequest) -> PortResult<SpawnResponse> {
        Ok(SpawnResponse {
            session_id: SessionId("s_launched".into()),
        })
    }
    async fn remove(&self, _req: RemoveRequest) -> PortResult<RemoveResponse> {
        unimplemented!()
    }
}

#[derive(Default)]
struct RecipientRecordingAgent {
    recipients: Mutex<Vec<SessionId>>,
}

impl RecipientRecordingAgent {
    fn recipients(&self) -> Vec<SessionId> {
        self.recipients.lock().unwrap().clone()
    }
}

#[async_trait]
impl nexus_contracts::AgentTurnExecutionPort for RecipientRecordingAgent {
    async fn inject_turn(&self, _r: &SessionId, _b: &NexusBatch) -> PortResult<()> {
        Ok(())
    }

    async fn prompt(&self, recipient: &SessionId, _text: String) -> PortResult<()> {
        self.recipients.lock().unwrap().push(recipient.clone());
        Ok(())
    }

    async fn steer_observed(
        &self,
        recipient: &SessionId,
        _text: String,
        events: Arc<dyn nexus_contracts::EventSink>,
        accepted_event: nexus_contracts::WsEvent,
    ) -> PortResult<nexus_contracts::SteerResponse> {
        self.recipients.lock().unwrap().push(recipient.clone());
        events.emit(accepted_event).await;
        Ok(nexus_contracts::SteerResponse {
            accepted: true,
            delivery: nexus_contracts::SteerDelivery::Steered,
            turn_id: Some("turn_recorded".into()),
        })
    }

    async fn compact(&self, recipient: &SessionId) -> PortResult<()> {
        self.recipients.lock().unwrap().push(recipient.clone());
        Ok(())
    }

    async fn launch(&self, _req: SpawnRequest) -> PortResult<SpawnResponse> {
        unreachable!("routing fixtures pre-register every target")
    }

    async fn remove(&self, _req: RemoveRequest) -> PortResult<RemoveResponse> {
        unreachable!("routing fixtures do not remove agents")
    }
}

#[derive(Default)]
struct RecordingAgent {
    launches: AtomicUsize,
}

#[async_trait]
impl nexus_contracts::AgentTurnExecutionPort for RecordingAgent {
    async fn inject_turn(&self, _r: &SessionId, _b: &NexusBatch) -> PortResult<()> {
        Ok(())
    }

    async fn prompt(&self, _r: &SessionId, _t: String) -> PortResult<()> {
        Ok(())
    }

    async fn launch(&self, _req: SpawnRequest) -> PortResult<SpawnResponse> {
        self.launches.fetch_add(1, Ordering::SeqCst);
        Ok(SpawnResponse {
            session_id: SessionId("s_admin_spawned".into()),
        })
    }

    async fn remove(&self, _req: RemoveRequest) -> PortResult<RemoveResponse> {
        unimplemented!()
    }
}

struct MockSearch;
#[async_trait]
impl nexus_contracts::SearchPort for MockSearch {
    async fn search(&self, _c: &Caller, _r: SearchRequest) -> PortResult<SearchResponse> {
        unimplemented!()
    }
    async fn history(&self, _c: &Caller, _r: HistoryRequest) -> PortResult<HistoryResponse> {
        unimplemented!()
    }
}

struct MockNotify;
#[async_trait]
impl nexus_contracts::NotifyPort for MockNotify {
    async fn ingest(&self, _req: NotifyRequest, _hmac_ok: bool) -> PortResult<NotifyResponse> {
        unimplemented!()
    }
    async fn forward(&self, _c: &Caller, _r: RouteForwardRequest) -> PortResult<()> {
        Ok(())
    }
    async fn channel(&self, _c: &Caller, _r: ChannelRequest) -> PortResult<()> {
        Ok(())
    }
}

/// Admin mock that gates on tier exactly like the real `nexus-admin` (agent → UNAUTHORIZED).
struct MockAdmin;
#[async_trait]
impl nexus_contracts::AdminPort for MockAdmin {
    async fn spawn(&self, caller: &Caller, _r: SpawnRequest) -> PortResult<SpawnResponse> {
        gate(caller)?;
        Ok(SpawnResponse {
            session_id: SessionId("s_spawn".into()),
        })
    }
    async fn remove(&self, caller: &Caller, _r: RemoveRequest) -> PortResult<RemoveResponse> {
        gate(caller)?;
        unimplemented!()
    }
    async fn assign_role(
        &self,
        caller: &Caller,
        _r: AssignRoleRequest,
    ) -> PortResult<AssignRoleResponse> {
        gate(caller)?;
        unimplemented!()
    }
    async fn channel(&self, caller: &Caller, _r: ChannelRequest) -> PortResult<()> {
        gate(caller)
    }
    async fn route(&self, caller: &Caller, _r: RouteForwardRequest) -> PortResult<()> {
        gate(caller)
    }
    async fn monitor(&self, caller: &Caller, _r: MonitorRequest) -> PortResult<()> {
        gate(caller)
    }
    async fn assign_project(
        &self,
        caller: &Caller,
        _r: AssignProjectRequest,
    ) -> PortResult<AssignProjectResponse> {
        gate(caller)?;
        unimplemented!()
    }
}

fn gate(caller: &Caller) -> PortResult<()> {
    if caller.tier == Tier::Admin {
        Ok(())
    } else {
        Err(ContractError {
            code: codes::UNAUTHORIZED,
            message: "admin tier required".into(),
        })
    }
}

async fn mock_state() -> AppState {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    AppState::new(
        store,
        WsSink::new(16, None),
        Arc::new(MockIdentity),
        Arc::new(MockAgent),
        Arc::new(MockRealtime),
        Arc::new(MockBus::default()),
        Arc::new(MockSearch),
        Arc::new(MockNotify),
        Arc::new(MockAdmin),
        "proj".into(),
    )
}

fn agent_caller() -> Caller {
    Caller {
        agent_id: None,
        session: SessionId("s_a".into()),
        name: "ben".into(),
        project: "proj".into(),
        tier: Tier::Agent,
        locality: Default::default(),
        access: None,
        principal_id: None,
    }
}

fn admin_caller() -> Caller {
    Caller {
        agent_id: None,
        session: SessionId("s_admin".into()),
        name: "operator".into(),
        project: "proj".into(),
        tier: Tier::Admin,
        locality: Default::default(),
        access: None,
        principal_id: None,
    }
}

async fn register_admin_human(store: &Store) {
    Sessions::new(store)
        .create(NewSession {
            session_id: SessionId("s_admin".into()),
            name: Some("operator".into()),
            agent: None,
            kind: "human".into(),
            role: Some("operator".into()),
            tier: "admin".into(),
            harness_session_id: None,
            client_key: Some("ck_operator".into()),
            cwd: None,
            project: "proj".into(),
            transport: None,
        })
        .await
        .unwrap();
}

fn req(method: &str, params: Option<serde_json::Value>) -> Request {
    Request {
        jsonrpc: nexus_contracts::JSONRPC_VERSION.to_string(),
        id: Some(RequestId::Num(1)),
        method: method.into(),
        params,
    }
}

fn send_params() -> serde_json::Value {
    serde_json::to_value(SendRequest {
        to: SendTarget::dm_name("dylan"),
        summary: None,
        body: "hi".into(),
        mention: vec![],
        metadata: None,
        idempotency_key: None,
    })
    .unwrap()
}

#[tokio::test]
async fn dispatch_routes_send_to_bus_and_returns_ack() {
    let state = mock_state().await;
    let resp = dispatch(
        &state,
        Some(agent_caller()),
        req("send", Some(send_params())),
    )
    .await;
    assert!(resp.error.is_none(), "unexpected error: {:?}", resp.error);
    let ack: Ack = serde_json::from_value(resp.result.unwrap()).unwrap();
    assert_eq!(ack.message_id, MessageId("m_mock".into()));
}

#[tokio::test]
async fn send_commits_before_a_cold_target_finishes_reviving() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let slow = SlowOpenAdapter::default();
    let opens = slow.opens.clone();
    let mut registry = nexus_agent::AdapterRegistry::new();
    registry.register(
        &hid("claude"),
        Arc::new(move |_cwd| Arc::new(slow.clone()) as Arc<dyn nexus_agent::Adapter>),
    );
    let state = AppState::wire_with_registry(store, &nexus_common::Config::default(), registry);
    state
        .identity
        .register(RegisterRequest {
            agent_id: None,
            name: Some("operator".into()),
            harness: hid("other"),
            harness_session_id: "native-operator".into(),
            project: "proj".into(),
            client_key: "ck-operator".into(),
            runtime_credential: None,
            tier: Tier::Admin,
            kind: Some(nexus_contracts::Kind::Human),
            locality: Default::default(),
            access: None,
            role: None,
            cwd: None,
        })
        .await
        .unwrap();
    let target = state
        .identity
        .register(RegisterRequest {
            agent_id: None,
            name: Some("cold-claude".into()),
            harness: hid("claude"),
            harness_session_id: "native-cold-claude".into(),
            project: "proj".into(),
            client_key: "ck-cold-claude".into(),
            runtime_credential: None,
            tier: Tier::Agent,
            kind: Some(nexus_contracts::Kind::Agent),
            locality: Default::default(),
            access: None,
            role: None,
            cwd: None,
        })
        .await
        .unwrap();
    state
        .mark_session_offline(&target.session_id)
        .await
        .unwrap();
    let caller = state.identity.resolve("proj", "operator").await.unwrap();

    let response = tokio::time::timeout(
        std::time::Duration::from_millis(250),
        dispatch(
            &state,
            Some(caller),
            req(
                "send",
                Some(
                    serde_json::to_value(SendRequest {
                        to: SendTarget::dm_name("cold-claude"),
                        summary: None,
                        body: "durable before revive".into(),
                        mention: vec![],
                        metadata: None,
                        idempotency_key: None,
                    })
                    .unwrap(),
                ),
            ),
        ),
    )
    .await
    .expect("send acknowledgement must not wait for a cold harness to open");

    assert!(
        response.error.is_none(),
        "unexpected error: {:?}",
        response.error
    );
    for _ in 0..25 {
        if opens.load(Ordering::SeqCst) == 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(opens.load(Ordering::SeqCst), 1, "revival still starts");

    // A daemon-owned harness registers its Nexus MCP while ACP open/resume is still in progress.
    // That registration must not stand up the drain loop until the adapter is actually bound.
    state.ensure_agent_loop("proj", "cold-claude").await;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let mut rows = state
        .store
        .conn
        .query(
            "SELECT state FROM in_flight ORDER BY rowid DESC LIMIT 1",
            (),
        )
        .await
        .unwrap();
    let delivery_state: String = rows.next().await.unwrap().unwrap().get(0).unwrap();
    assert_eq!(
        delivery_state, "pending",
        "MCP registration during adapter open must leave durable mail held"
    );
}

#[tokio::test]
async fn registration_does_not_start_a_drain_loop_before_the_harness_is_live() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let exec = Arc::new(DefinitelyOfflineTurnExec::default());
    let state =
        AppState::wire_with_turn_exec(store, &nexus_common::Config::default(), exec.clone());
    state
        .identity
        .register(RegisterRequest {
            agent_id: None,
            name: Some("operator".into()),
            harness: hid("other"),
            harness_session_id: "native-operator".into(),
            project: "proj".into(),
            client_key: "ck-operator".into(),
            runtime_credential: None,
            tier: Tier::Admin,
            kind: Some(nexus_contracts::Kind::Human),
            locality: Default::default(),
            access: None,
            role: None,
            cwd: None,
        })
        .await
        .unwrap();
    state
        .identity
        .register(RegisterRequest {
            agent_id: None,
            name: Some("opening-claude".into()),
            harness: hid("claude"),
            harness_session_id: "native-opening".into(),
            project: "proj".into(),
            client_key: "ck-opening".into(),
            runtime_credential: None,
            tier: Tier::Agent,
            kind: Some(nexus_contracts::Kind::Agent),
            locality: Default::default(),
            access: None,
            role: None,
            cwd: None,
        })
        .await
        .unwrap();
    let caller = state.identity.resolve("proj", "operator").await.unwrap();
    state
        .bus
        .send(
            &caller,
            SendRequest {
                to: SendTarget::dm_name("opening-claude"),
                summary: None,
                body: "hold until the adapter binds".into(),
                mention: vec![],
                metadata: None,
                idempotency_key: None,
            },
        )
        .await
        .unwrap();

    state.ensure_agent_loop("proj", "opening-claude").await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    assert_eq!(
        exec.injects.load(Ordering::SeqCst),
        0,
        "registration during harness open must not inject through a dead transport"
    );
    let mut rows = state
        .store
        .conn
        .query(
            "SELECT state FROM in_flight ORDER BY rowid DESC LIMIT 1",
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        rows.next()
            .await
            .unwrap()
            .unwrap()
            .get::<String>(0)
            .unwrap(),
        "pending"
    );
}

#[tokio::test]
async fn metadata_set_writes_action_event() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    store
        .conn
        .execute(
            "INSERT INTO threads (thread_id, name, project, created_at) \
             VALUES ('t_meta', 'meta-thread', 'proj', 1)",
            (),
        )
        .await
        .unwrap();
    let state = AppState::new(
        store.clone(),
        WsSink::new(16, None),
        Arc::new(MockIdentity),
        Arc::new(MockAgent),
        Arc::new(MockRealtime),
        Arc::new(MockBus::default()),
        Arc::new(MockSearch),
        Arc::new(MockNotify),
        Arc::new(MockAdmin),
        "proj".into(),
    );

    let resp = dispatch(
        &state,
        Some(admin_caller()),
        req(
            "metadata.set",
            Some(serde_json::json!({
                "entity": "thread",
                "id": "meta-thread",
                "metadata": { "owner": "qa" }
            })),
        ),
    )
    .await;

    assert!(resp.error.is_none(), "unexpected error: {:?}", resp.error);
    let out: MetadataResponse = serde_json::from_value(resp.result.unwrap()).unwrap();
    assert_eq!(out.entity, MetadataEntityKind::Thread);
    let rows = DeveloperEvents::new(&store)
        .since("sys.metadata.thread", 0)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].kind, "action");
    assert_eq!(rows[0].from_name.as_deref(), Some("operator"));
    assert_eq!(rows[0].thread_name.as_deref(), Some("meta-thread"));
    let data: serde_json::Value =
        serde_json::from_str(rows[0].data_json.as_deref().unwrap()).unwrap();
    assert_eq!(data["action"], "metadata.set");
    assert_eq!(data["entity"], "thread");
    assert_eq!(data["id"], "meta-thread");
}

#[tokio::test]
async fn metadata_set_continues_when_action_event_append_fails() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    store
        .conn
        .execute(
            "INSERT INTO threads (thread_id, name, project, created_at) \
             VALUES ('t_meta_fail', 'meta-fail', 'proj', 1)",
            (),
        )
        .await
        .unwrap();
    store
        .conn
        .execute(
            "CREATE TRIGGER fail_metadata_action_events
             BEFORE INSERT ON developer_events
             WHEN NEW.kind = 'action'
             BEGIN
               SELECT RAISE(ABORT, 'forced metadata telemetry failure');
             END",
            (),
        )
        .await
        .unwrap();
    let state = AppState::new(
        store.clone(),
        WsSink::new(16, None),
        Arc::new(MockIdentity),
        Arc::new(MockAgent),
        Arc::new(MockRealtime),
        Arc::new(MockBus::default()),
        Arc::new(MockSearch),
        Arc::new(MockNotify),
        Arc::new(MockAdmin),
        "proj".into(),
    );

    let resp = dispatch(
        &state,
        Some(admin_caller()),
        req(
            "metadata.set",
            Some(serde_json::json!({
                "entity": "thread",
                "id": "meta-fail",
                "metadata": { "owner": "qa" }
            })),
        ),
    )
    .await;

    assert!(
        resp.error.is_none(),
        "metadata.set should ignore telemetry failure: {:?}",
        resp.error
    );
    let metadata = nexus_store::repos::Metadata::new(&store)
        .get(
            "proj",
            nexus_store::repos::MetadataEntity::Thread,
            "meta-fail",
        )
        .await
        .unwrap();
    assert_eq!(metadata.metadata, serde_json::json!({ "owner": "qa" }));
    assert!(DeveloperEvents::new(&store)
        .since("sys.metadata.thread", 0)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn dispatch_rejects_empty_or_whitespace_send_body() {
    for body in ["", " \n\t "] {
        let state = mock_state().await;
        let mut params = send_params();
        params["body"] = serde_json::Value::String(body.into());

        let resp = dispatch(&state, Some(agent_caller()), req("send", Some(params))).await;

        let error = resp.error.expect("empty send body should be rejected");
        assert_eq!(error.code, codes::INVALID_PARAMS);
        assert!(error.message.contains("body"));
    }
}

#[tokio::test]
async fn unknown_method_returns_method_not_found() {
    let state = mock_state().await;
    let resp = dispatch(&state, Some(agent_caller()), req("does.not.exist", None)).await;
    assert_eq!(resp.error.unwrap().code, codes::METHOD_NOT_FOUND);
}

#[tokio::test]
async fn bad_params_returns_invalid_params() {
    let state = mock_state().await;
    // `send` with garbage params (a bare string, not a SendRequest object)
    let resp = dispatch(
        &state,
        Some(agent_caller()),
        req("send", Some(serde_json::json!("not a send request"))),
    )
    .await;
    assert_eq!(resp.error.unwrap().code, codes::INVALID_PARAMS);
}

#[tokio::test]
async fn admin_method_from_agent_returns_unauthorized() {
    let state = mock_state().await;
    let params = serde_json::to_value(SpawnRequest {
        kind: hid("claude"),
        name: Some("x".into()),
        identity_policy: None,
        cwd: None,
        project: None,
        role: None,
        initial_prompt: None,
        resume: None,
        harness_args: Vec::new(),
        headless: false,
        backend: None,
    })
    .unwrap();
    let resp = dispatch(
        &state,
        Some(agent_caller()),
        req("admin.spawn", Some(params)),
    )
    .await;
    assert_eq!(resp.error.unwrap().code, codes::UNAUTHORIZED);
}

#[tokio::test]
async fn admin_spawn_uses_appstate_launch_path_not_admin_delegate() {
    let state = mock_state().await;
    let params = serde_json::to_value(SpawnRequest {
        kind: hid("other"),
        name: Some("admin-spawned".into()),
        identity_policy: None,
        cwd: None,
        project: None,
        role: Some("raw-op".into()),
        initial_prompt: None,
        resume: None,
        harness_args: Vec::new(),
        headless: true,
        backend: None,
    })
    .unwrap();

    let resp = dispatch(
        &state,
        Some(admin_caller()),
        req("admin.spawn", Some(params)),
    )
    .await;

    assert!(resp.error.is_none(), "unexpected error: {:?}", resp.error);
    let spawned: SpawnResponse = serde_json::from_value(resp.result.unwrap()).unwrap();
    assert_eq!(
        spawned.session_id,
        SessionId("s_launched".into()),
        "admin.spawn must use AppState::launch_agent/self.agent, not AdminPort::spawn"
    );

    let row = Sessions::new(&state.store)
        .find_by_name("proj", "admin-spawned")
        .await
        .unwrap()
        .expect("admin.spawn should register the launched member");
    assert_eq!(row.session_id, SessionId("s_launched".into()));
    assert_eq!(row.role.as_deref(), Some("raw-op"));
}

#[tokio::test]
async fn thread_add_member_notifies_added_agent() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let bus = Arc::new(MockBus::default());
    let state = AppState::new(
        store,
        WsSink::new(16, None),
        Arc::new(MockIdentity),
        Arc::new(MockAgent),
        Arc::new(MockRealtime),
        bus.clone(),
        Arc::new(MockSearch),
        Arc::new(MockNotify),
        Arc::new(MockAdmin),
        "proj".into(),
    );

    let resp = dispatch(
        &state,
        Some(admin_caller()),
        req(
            "thread.addMember",
            Some(
                serde_json::to_value(ThreadMemberRequest {
                    name: "backend".into(),
                    member: "ana".into(),
                })
                .unwrap(),
            ),
        ),
    )
    .await;

    assert!(resp.error.is_none(), "unexpected error: {:?}", resp.error);
    let calls = bus.notification_calls();
    assert_eq!(calls.len(), 1);
    let (caller, notification) = &calls[0];
    assert_eq!(caller.project, "proj");
    assert_eq!(notification.source.as_deref(), Some("nexus-thread"));
    assert_eq!(
        notification.target,
        NotifyTarget::Name { name: "ana".into() }
    );
    let payload: serde_json::Value = serde_json::from_str(&notification.body).unwrap();
    assert_eq!(payload["event"], "thread.added");
    assert_eq!(payload["thread"], "backend");
    assert_eq!(payload["member"], "ana");
    assert_eq!(payload["addedBy"], "operator");
}

#[tokio::test]
async fn thread_join_notifies_joining_agent() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let bus = Arc::new(MockBus::default());
    let state = AppState::new(
        store,
        WsSink::new(16, None),
        Arc::new(MockIdentity),
        Arc::new(MockAgent),
        Arc::new(MockRealtime),
        bus.clone(),
        Arc::new(MockSearch),
        Arc::new(MockNotify),
        Arc::new(MockAdmin),
        "proj".into(),
    );

    let resp = dispatch(
        &state,
        Some(agent_caller()),
        req(
            "thread.join",
            Some(
                serde_json::to_value(JoinThreadRequest {
                    name: "backend".into(),
                })
                .unwrap(),
            ),
        ),
    )
    .await;

    assert!(resp.error.is_none(), "unexpected error: {:?}", resp.error);
    let calls = bus.notification_calls();
    assert_eq!(calls.len(), 1);
    let (_, notification) = &calls[0];
    assert_eq!(
        notification.target,
        NotifyTarget::Name { name: "ben".into() }
    );
    let payload: serde_json::Value = serde_json::from_str(&notification.body).unwrap();
    assert_eq!(payload["thread"], "backend");
    assert_eq!(payload["member"], "ben");
    assert_eq!(payload["addedBy"], "ben");
}

#[tokio::test]
async fn every_admin_method_from_agent_returns_unauthorized_before_lookup() {
    let state = mock_state().await;
    let cases = [
        (
            "admin.spawn",
            serde_json::to_value(SpawnRequest {
                kind: hid("claude"),
                name: Some("missing-agent".into()),
                identity_policy: None,
                cwd: None,
                project: None,
                role: None,
                initial_prompt: None,
                resume: None,
                harness_args: Vec::new(),
                headless: false,
                backend: None,
            })
            .unwrap(),
        ),
        (
            "admin.remove",
            serde_json::to_value(RemoveRequest {
                agent_id: None,
                name: "missing-agent".into(),
                kill: false,
            })
            .unwrap(),
        ),
        (
            "admin.evict",
            serde_json::to_value(RemoveRequest {
                agent_id: None,
                name: "missing-agent".into(),
                kill: false,
            })
            .unwrap(),
        ),
        (
            "admin.delete",
            serde_json::to_value(RemoveRequest {
                agent_id: None,
                name: "missing-agent".into(),
                kill: false,
            })
            .unwrap(),
        ),
        (
            "admin.rename",
            serde_json::to_value(nexus_contracts::AdminRenameRequest {
                source: "s_staged".into(),
                target: "nora".into(),
            })
            .unwrap(),
        ),
        (
            "admin.assignRole",
            serde_json::to_value(AssignRoleRequest {
                agent_id: None,
                name: "missing-agent".into(),
                role: "lead".into(),
            })
            .unwrap(),
        ),
        (
            "admin.assignProject",
            serde_json::to_value(AssignProjectRequest {
                agent_id: None,
                name: "missing-agent".into(),
                project: "other".into(),
            })
            .unwrap(),
        ),
        (
            "admin.grantTier",
            serde_json::to_value(GrantTierRequest {
                agent_id: None,
                name: "missing-agent".into(),
                tier: Tier::Admin,
            })
            .unwrap(),
        ),
        (
            "admin.group.assign",
            serde_json::to_value(nexus_contracts::AdminGroupAssignRequest {
                project: None,
                group: "ops".into(),
                agent_id: None,
                name: "missing-agent".into(),
            })
            .unwrap(),
        ),
        (
            "admin.channel",
            serde_json::to_value(ChannelRequest {
                op: ChannelOp::Create,
                topic: "ops.alerts".into(),
                source: None,
            })
            .unwrap(),
        ),
        (
            "admin.route",
            serde_json::to_value(RouteForwardRequest {
                notif: MessageId("m_missing".into()),
                to: "missing-agent".into(),
            })
            .unwrap(),
        ),
        (
            "admin.monitor",
            serde_json::to_value(MonitorRequest {
                follow: false,
                scope: None,
            })
            .unwrap(),
        ),
        (
            "admin.dlq.list",
            serde_json::to_value(nexus_contracts::DlqListRequest {
                for_target: Some("missing-agent".into()),
                since: None,
                limit: Some(10),
            })
            .unwrap(),
        ),
        (
            "admin.dlq.requeue",
            serde_json::to_value(nexus_contracts::DlqRequeueRequest {
                in_flight_id: Some("if_missing".into()),
                for_target: None,
                since: None,
            })
            .unwrap(),
        ),
        (
            "admin.dlq.purge",
            serde_json::to_value(nexus_contracts::DlqPurgeRequest {
                in_flight_id: Some("if_missing".into()),
                for_target: None,
                since: None,
                yes: false,
            })
            .unwrap(),
        ),
    ];

    for (method, params) in cases {
        let resp = dispatch(&state, Some(agent_caller()), req(method, Some(params))).await;
        let error = resp
            .error
            .unwrap_or_else(|| panic!("{method} should reject an agent-tier caller"));
        assert_eq!(
            error.code,
            codes::UNAUTHORIZED,
            "{method} must tier-gate before lookup/delegation"
        );
    }
}

#[tokio::test]
async fn admin_remove_detaches_session_without_routing_through_agent_port() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    Sessions::new(&store)
        .create(NewSession {
            session_id: SessionId("s_remove_me".into()),
            name: Some("remove-me".into()),
            agent: Some("claude".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: None,
            client_key: Some("ck_remove_me".into()),
            cwd: None,
            project: "proj".into(),
            transport: Some("acp".into()),
        })
        .await
        .unwrap();
    let state = AppState::new(
        store.clone(),
        WsSink::new(16, None),
        Arc::new(MockIdentity),
        Arc::new(MockAgent),
        Arc::new(MockRealtime),
        Arc::new(MockBus::default()),
        Arc::new(MockSearch),
        Arc::new(MockNotify),
        Arc::new(MockAdmin),
        "proj".into(),
    );

    let resp = dispatch(
        &state,
        Some(admin_caller()),
        req(
            "admin.remove",
            Some(serde_json::json!({ "name": "remove-me" })),
        ),
    )
    .await;

    assert!(resp.error.is_none(), "unexpected error: {:?}", resp.error);
    let removed: RemoveResponse = serde_json::from_value(resp.result.unwrap()).unwrap();
    assert_eq!(removed.name.as_deref(), Some("remove-me"));
    assert_eq!(removed.status, "removed");
    let row = Sessions::new(&store)
        .find_by_name("proj", "remove-me")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.presence.as_deref(), Some("offline"));
    let rows = DeveloperEvents::new(&store)
        .since(AGENT_LIFECYCLE_TOPIC, 0)
        .await
        .unwrap();
    assert!(rows.iter().any(|row| {
        row.agent_name.as_deref() == Some("remove-me")
            && row.session_id.as_deref() == Some("s_remove_me")
            && row.lifecycle.as_deref() == Some("removed")
    }));
}

#[tokio::test]
async fn admin_delete_records_deleted_lifecycle_before_purge() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    Sessions::new(&store)
        .create(NewSession {
            session_id: SessionId("s_delete_me".into()),
            name: Some("delete-me".into()),
            agent: Some("claude".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: None,
            client_key: Some("ck_delete_me".into()),
            cwd: None,
            project: "proj".into(),
            transport: Some("acp".into()),
        })
        .await
        .unwrap();
    let state = AppState::new(
        store.clone(),
        WsSink::new(16, None),
        Arc::new(MockIdentity),
        Arc::new(MockAgent),
        Arc::new(MockRealtime),
        Arc::new(MockBus::default()),
        Arc::new(MockSearch),
        Arc::new(MockNotify),
        Arc::new(MockAdmin),
        "proj".into(),
    );

    let resp = dispatch(
        &state,
        Some(admin_caller()),
        req(
            "admin.delete",
            Some(serde_json::json!({ "name": "delete-me" })),
        ),
    )
    .await;

    assert!(resp.error.is_none(), "unexpected error: {:?}", resp.error);
    let deleted: RemoveResponse = serde_json::from_value(resp.result.unwrap()).unwrap();
    assert_eq!(deleted.name.as_deref(), Some("delete-me"));
    assert_eq!(deleted.status, "deleted");
    assert!(Sessions::new(&store)
        .find_by_name("proj", "delete-me")
        .await
        .unwrap()
        .is_none());
    let rows = DeveloperEvents::new(&store)
        .since(AGENT_LIFECYCLE_TOPIC, 0)
        .await
        .unwrap();
    assert!(rows.iter().any(|row| {
        row.agent_name.as_deref() == Some("delete-me")
            && row.session_id.as_deref() == Some("s_delete_me")
            && row.lifecycle.as_deref() == Some("deleted")
    }));
}

#[tokio::test]
async fn admin_remove_and_delete_continue_when_lifecycle_telemetry_fails() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    for (session, name) in [
        ("s_remove_telemetry_fail", "remove-telemetry-fail"),
        ("s_delete_telemetry_fail", "delete-telemetry-fail"),
    ] {
        Sessions::new(&store)
            .create(NewSession {
                session_id: SessionId(session.into()),
                name: Some(name.into()),
                agent: Some("claude".into()),
                kind: "agent".into(),
                role: None,
                tier: "agent".into(),
                harness_session_id: None,
                client_key: Some(format!("ck_{session}")),
                cwd: None,
                project: "proj".into(),
                transport: Some("acp".into()),
            })
            .await
            .unwrap();
    }
    store
        .conn
        .execute(
            "CREATE TRIGGER fail_admin_lifecycle
             BEFORE INSERT ON developer_events
             WHEN NEW.lifecycle IN ('removed', 'deleted')
             BEGIN
               SELECT RAISE(ABORT, 'forced admin lifecycle telemetry failure');
             END",
            (),
        )
        .await
        .unwrap();
    let state = AppState::new(
        store.clone(),
        WsSink::new(16, None),
        Arc::new(MockIdentity),
        Arc::new(MockAgent),
        Arc::new(MockRealtime),
        Arc::new(MockBus::default()),
        Arc::new(MockSearch),
        Arc::new(MockNotify),
        Arc::new(MockAdmin),
        "proj".into(),
    );

    let remove = dispatch(
        &state,
        Some(admin_caller()),
        req(
            "admin.remove",
            Some(serde_json::json!({ "name": "remove-telemetry-fail" })),
        ),
    )
    .await;
    assert!(
        remove.error.is_none(),
        "remove should ignore telemetry failure: {:?}",
        remove.error
    );
    let delete = dispatch(
        &state,
        Some(admin_caller()),
        req(
            "admin.delete",
            Some(serde_json::json!({ "name": "delete-telemetry-fail" })),
        ),
    )
    .await;
    assert!(
        delete.error.is_none(),
        "delete should ignore telemetry failure: {:?}",
        delete.error
    );
}

#[tokio::test]
async fn admin_dlq_list_requeue_and_purge_use_store_and_emit_events() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    Sessions::new(&store)
        .create(NewSession {
            session_id: SessionId("s_dlq".into()),
            name: Some("dlq-agent".into()),
            agent: Some("codex".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: None,
            client_key: Some("ck_dlq".into()),
            cwd: None,
            project: "proj".into(),
            transport: Some("codex-appserver".into()),
        })
        .await
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO messages (message_id, from_name, to_name, kind, body, project, \
             created_at) VALUES ('m_dlq', 'operator', 'dlq-agent', 'dm', 'needs replay', \
             'proj', 100)",
            (),
        )
        .await
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO in_flight (in_flight_id, message_id, recipient_session, state, \
             attempt_count, error_code, error_reason, error_details_json) VALUES \
             ('if_dlq', 'm_dlq', 's_dlq', 'error', 2, 'provider_error', \
             'provider server error', '{\"retryable\":true,\"source\":\"claude.acp.prompt_error\"}')",
            (),
        )
        .await
        .unwrap();
    let state = AppState::new(
        store.clone(),
        WsSink::new(16, None),
        Arc::new(MockIdentity),
        Arc::new(MockAgent),
        Arc::new(MockRealtime),
        Arc::new(MockBus::default()),
        Arc::new(MockSearch),
        Arc::new(MockNotify),
        Arc::new(MockAdmin),
        "proj".into(),
    );

    let list = dispatch(
        &state,
        Some(admin_caller()),
        req(
            "admin.dlq.list",
            Some(serde_json::json!({
                "for": "dlq-agent",
                "limit": 5,
            })),
        ),
    )
    .await;
    assert!(list.error.is_none(), "unexpected error: {:?}", list.error);
    let listed: nexus_contracts::DlqListResponse =
        serde_json::from_value(list.result.unwrap()).unwrap();
    assert_eq!(listed.total, 1);
    assert_eq!(listed.rows[0].in_flight_id, "if_dlq");
    assert_eq!(listed.rows[0].attempt_count, 2);
    assert_eq!(listed.rows[0].error_code.as_deref(), Some("provider_error"));
    assert_eq!(
        listed.rows[0].error_reason.as_deref(),
        Some("provider server error")
    );
    assert_eq!(
        listed.rows[0].error_details,
        Some(serde_json::json!({
            "retryable": true,
            "source": "claude.acp.prompt_error",
        }))
    );

    let requeue = dispatch(
        &state,
        Some(admin_caller()),
        req(
            "admin.dlq.requeue",
            Some(serde_json::json!({ "inFlightId": "if_dlq" })),
        ),
    )
    .await;
    assert!(
        requeue.error.is_none(),
        "unexpected error: {:?}",
        requeue.error
    );
    let requeued: nexus_contracts::DlqMutationResponse =
        serde_json::from_value(requeue.result.unwrap()).unwrap();
    assert_eq!(requeued.count, 1);
    let mut rows = store
        .conn
        .query(
            "SELECT state, error_reason FROM in_flight WHERE in_flight_id = 'if_dlq'",
            (),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<String>(0).unwrap(), "pending");
    assert!(row.get::<Option<String>>(1).unwrap().is_none());

    store
        .conn
        .execute(
            "UPDATE in_flight SET state = 'error', error_reason = 'operator purge test' \
             WHERE in_flight_id = 'if_dlq'",
            (),
        )
        .await
        .unwrap();
    let purge = dispatch(
        &state,
        Some(admin_caller()),
        req(
            "admin.dlq.purge",
            Some(serde_json::json!({ "inFlightId": "if_dlq" })),
        ),
    )
    .await;
    assert!(purge.error.is_none(), "unexpected error: {:?}", purge.error);
    let purged: nexus_contracts::DlqMutationResponse =
        serde_json::from_value(purge.result.unwrap()).unwrap();
    assert_eq!(purged.count, 1);
    let mut rows = store
        .conn
        .query(
            "SELECT COUNT(*) FROM in_flight WHERE in_flight_id = 'if_dlq'",
            (),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 0);

    let events = DeveloperEvents::new(&store)
        .since(AGENT_LIFECYCLE_TOPIC, 0)
        .await
        .unwrap();
    assert!(events.iter().any(|row| {
        row.lifecycle.as_deref() == Some("dlq.requeue")
            && row
                .data_json
                .as_deref()
                .is_some_and(|data| data.contains("if_dlq"))
    }));
    assert!(events.iter().any(|row| {
        row.lifecycle.as_deref() == Some("dlq.purge")
            && row
                .data_json
                .as_deref()
                .is_some_and(|data| data.contains("if_dlq"))
    }));
}

#[tokio::test]
async fn admin_spawn_uses_daemon_launch_path_and_registers_member() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let agent = Arc::new(RecordingAgent::default());
    let state = AppState::wire_with_turn_exec(
        store.clone(),
        &nexus_common::Config::default(),
        agent.clone(),
    );

    let resp = dispatch(
        &state,
        Some(admin_caller()),
        req(
            "admin.spawn",
            Some(
                serde_json::to_value(SpawnRequest {
                    kind: hid("other"),
                    name: Some("admin-spawned".into()),
                    identity_policy: None,
                    cwd: None,
                    project: None,
                    role: Some("raw-op".into()),
                    initial_prompt: None,
                    resume: None,
                    harness_args: Vec::new(),
                    headless: true,
                    backend: None,
                })
                .unwrap(),
            ),
        ),
    )
    .await;

    assert!(resp.error.is_none(), "unexpected error: {:?}", resp.error);
    let spawned: SpawnResponse = serde_json::from_value(resp.result.unwrap()).unwrap();
    assert_eq!(spawned.session_id, SessionId("s_admin_spawned".into()));
    assert_eq!(agent.launches.load(Ordering::SeqCst), 1);

    let row = Sessions::new(&store)
        .find_by_name("proj", "admin-spawned")
        .await
        .unwrap()
        .expect("admin.spawn should register an addressable member");
    assert_eq!(row.session_id, SessionId("s_admin_spawned".into()));
    assert_eq!(row.project, "proj");
    assert_eq!(row.role.as_deref(), Some("raw-op"));

    let agent = Agents::new(&store)
        .find_by_name("admin-spawned")
        .await
        .unwrap()
        .expect("admin.spawn should bind a durable agent identity");
    assert_eq!(agent.owner_name.as_deref(), Some("operator"));
    assert_eq!(agent.owner_project.as_deref(), Some("proj"));
    assert_eq!(agent.owner_session_id.as_deref(), Some("s_admin"));
}

#[tokio::test]
async fn unauthenticated_send_is_rejected() {
    let state = mock_state().await;
    let resp = dispatch(&state, None, req("send", Some(send_params()))).await;
    assert_eq!(resp.error.unwrap().code, codes::UNAUTHORIZED);
}

#[tokio::test]
async fn prompt_preserves_authenticated_human_provenance_in_stream_and_replay() {
    use nexus_store::repos::{AgentSessionMessages, NewSession, Sessions, StreamEvents};

    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let session = SessionId("s_hugo".into());
    Sessions::new(&store)
        .create(NewSession {
            session_id: session.clone(),
            name: Some("hugo".into()),
            agent: Some("claude".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: None,
            client_key: Some("ck_hugo".into()),
            cwd: None,
            project: "proj".into(),
            transport: Some("pty".into()),
        })
        .await
        .unwrap();
    register_admin_human(&store).await;
    let state = AppState::new(
        store.clone(),
        WsSink::new(16, Some(store.clone())),
        Arc::new(MockIdentity),
        Arc::new(MockAgent),
        Arc::new(MockRealtime),
        Arc::new(MockBus::default()),
        Arc::new(MockSearch),
        Arc::new(MockNotify),
        Arc::new(MockAdmin),
        "proj".into(),
    );

    let resp = dispatch(
        &state,
        Some(admin_caller()),
        req(
            "prompt",
            Some(serde_json::json!({
                "name": "hugo",
                "text": "hello session",
                "clientMessageId": "you:test:1",
            })),
        ),
    )
    .await;

    assert!(resp.error.is_none(), "unexpected error: {:?}", resp.error);
    let rows = StreamEvents::new(&store).since(&session, 0).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].kind, "user_input");
    let data: serde_json::Value = serde_json::from_str(&rows[0].data).unwrap();
    assert_eq!(
        data,
        serde_json::json!({
            "text": "hello session",
            "clientMessageId": "you:test:1",
            "name": "operator",
            "kind": "local.human",
        })
    );
    assert_ne!(data["name"], "hugo");

    state
        .ws
        .emit(WsEvent::AgentUpdate {
            session_id: session.clone(),
            kind: AgentUpdateKind::TurnEnd,
            data: serde_json::json!({}),
        })
        .await;
    let durable = AgentSessionMessages::new(&store)
        .messages_for_session(&session, 10)
        .await
        .unwrap();
    assert_eq!(durable.len(), 1);
    let content: serde_json::Value = serde_json::from_str(&durable[0].content_json).unwrap();
    assert_eq!(
        content["blocks"][0],
        serde_json::json!({
            "type": "text",
            "text": "hello session",
            "id": "you:test:1",
            "clientMessageId": "you:test:1",
            "name": "operator",
            "kind": "local.human",
        })
    );
}

#[tokio::test]
async fn steer_emits_accepted_user_input_with_native_source() {
    use nexus_store::repos::{AgentSessionMessages, NewSession, Sessions, StreamEvents};

    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let session = SessionId("s_codex_steer".into());
    Sessions::new(&store)
        .create(NewSession {
            session_id: session.clone(),
            name: Some("codex-steer".into()),
            agent: Some("codex".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: None,
            client_key: Some("ck_codex_steer".into()),
            cwd: None,
            project: "proj".into(),
            transport: Some("codex-app-server".into()),
        })
        .await
        .unwrap();
    register_admin_human(&store).await;
    let state = AppState::new(
        store.clone(),
        WsSink::new(16, Some(store.clone())),
        Arc::new(MockIdentity),
        Arc::new(MockAgent),
        Arc::new(MockRealtime),
        Arc::new(MockBus::default()),
        Arc::new(MockSearch),
        Arc::new(MockNotify),
        Arc::new(MockAdmin),
        "proj".into(),
    );

    let response = dispatch(
        &state,
        Some(admin_caller()),
        req(
            "steer",
            Some(serde_json::json!({
                "name": "codex-steer",
                "text": "focus tests",
                "clientMessageId": "steer:1",
            })),
        ),
    )
    .await;

    assert!(
        response.error.is_none(),
        "unexpected error: {:?}",
        response.error
    );
    let result: nexus_contracts::SteerResponse =
        serde_json::from_value(response.result.unwrap()).unwrap();
    assert_eq!(result.delivery, nexus_contracts::SteerDelivery::Steered);
    let rows = StreamEvents::new(&store).since(&session, 0).await.unwrap();
    assert_eq!(rows.len(), 1);
    let data: serde_json::Value = serde_json::from_str(&rows[0].data).unwrap();
    assert_eq!(data["source"], "steer");
    assert_eq!(data["clientMessageId"], "steer:1");
    assert_eq!(data["name"], "operator");
    assert_eq!(data["kind"], "local.human");
    assert_ne!(data["name"], "codex-steer");

    state
        .ws
        .emit(WsEvent::AgentUpdate {
            session_id: session.clone(),
            kind: AgentUpdateKind::TurnEnd,
            data: serde_json::json!({}),
        })
        .await;
    let durable = AgentSessionMessages::new(&store)
        .messages_for_session(&session, 10)
        .await
        .unwrap();
    assert_eq!(durable.len(), 1);
    let content: serde_json::Value = serde_json::from_str(&durable[0].content_json).unwrap();
    assert_eq!(content["blocks"][0]["name"], "operator");
    assert_eq!(content["blocks"][0]["kind"], "local.human");
}

#[tokio::test]
async fn stable_agent_id_routes_prompt_steer_compact_and_warm_across_project_metadata() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    Agents::new(&store)
        .create(NewAgent {
            agent_id: "a_other".into(),
            project: "other".into(),
            name: Some("hugo".into()),
            default_harness: Some("claude".into()),
            role: None,
            tier: Some("agent".into()),
            owner: None,
        })
        .await
        .unwrap();
    let session = SessionId("s_other_hugo".into());
    Sessions::new(&store)
        .create(NewSession {
            session_id: session.clone(),
            name: Some("hugo".into()),
            agent: Some("claude".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: None,
            client_key: Some("ck_other_hugo".into()),
            cwd: None,
            project: "other".into(),
            transport: Some("pty".into()),
        })
        .await
        .unwrap();
    Sessions::new(&store)
        .set_agent_id(&session, "a_other")
        .await
        .unwrap();
    let state = AppState::new(
        store,
        WsSink::new(16, None),
        Arc::new(MockIdentity),
        Arc::new(MockAgent),
        Arc::new(MockRealtime),
        Arc::new(MockBus::default()),
        Arc::new(MockSearch),
        Arc::new(MockNotify),
        Arc::new(MockAdmin),
        "proj".into(),
    );

    for (method, params) in [
        (
            "prompt",
            serde_json::json!({
                "name": "stale-hugo",
                "agentId": "a_other",
                "text": "route by stable id",
            }),
        ),
        (
            "steer",
            serde_json::json!({
                "name": "stale-hugo",
                "agentId": "a_other",
                "text": "route by stable id",
            }),
        ),
        (
            "compact",
            serde_json::json!({
                "name": "stale-hugo",
                "agentId": "a_other",
            }),
        ),
        (
            "warm",
            serde_json::json!({
                "name": "stale-hugo",
                "agentId": "a_other",
            }),
        ),
    ] {
        let response = dispatch(&state, Some(agent_caller()), req(method, Some(params))).await;

        assert!(
            response.error.is_none(),
            "{method} must route the stable id across project metadata: {:?}",
            response.error,
        );
    }
}

#[tokio::test]
async fn name_only_id_shaped_alias_routes_prompt_steer_compact_and_warm() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    Agents::new(&store)
        .create(NewAgent {
            agent_id: "a_alias_owner".into(),
            project: "identity-metadata".into(),
            name: Some("a_route_alias".into()),
            default_harness: Some("claude".into()),
            role: None,
            tier: Some("agent".into()),
            owner: None,
        })
        .await
        .unwrap();
    let alias_session = SessionId("s_route_alias".into());
    Sessions::new(&store)
        .create(NewSession {
            session_id: alias_session.clone(),
            name: Some("a_route_alias".into()),
            agent: Some("claude".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: None,
            client_key: Some("ck_route_alias".into()),
            cwd: None,
            project: "runtime-metadata".into(),
            transport: Some("pty".into()),
        })
        .await
        .unwrap();
    Sessions::new(&store)
        .set_agent_id(&alias_session, "a_alias_owner")
        .await
        .unwrap();
    let agent = Arc::new(RecipientRecordingAgent::default());
    let state = AppState::new(
        store,
        WsSink::new(16, None),
        Arc::new(MockIdentity),
        agent.clone(),
        Arc::new(MockRealtime),
        Arc::new(MockBus::default()),
        Arc::new(MockSearch),
        Arc::new(MockNotify),
        Arc::new(MockAdmin),
        "caller-metadata".into(),
    );

    for (method, params) in [
        (
            "prompt",
            serde_json::json!({"name":"a_route_alias","text":"alias prompt"}),
        ),
        (
            "steer",
            serde_json::json!({"name":"a_route_alias","text":"alias steer"}),
        ),
        ("compact", serde_json::json!({"name":"a_route_alias"})),
        ("warm", serde_json::json!({"name":"a_route_alias"})),
    ] {
        let response = dispatch(&state, Some(agent_caller()), req(method, Some(params))).await;
        assert!(
            response.error.is_none(),
            "{method} must preserve an unowned id-shaped alias: {:?}",
            response.error,
        );
    }
    assert_eq!(
        agent.recipients(),
        vec![alias_session.clone(), alias_session.clone(), alias_session,],
    );
}

#[tokio::test]
async fn name_only_id_shaped_collision_routes_every_command_to_the_exact_agent_id() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    for (agent_id, name) in [
        ("a_route_exact", "exact-display"),
        ("a_route_alias_owner", "a_route_exact"),
    ] {
        Agents::new(&store)
            .create(NewAgent {
                agent_id: agent_id.into(),
                project: "identity-metadata".into(),
                name: Some(name.into()),
                default_harness: Some("claude".into()),
                role: None,
                tier: Some("agent".into()),
                owner: None,
            })
            .await
            .unwrap();
    }
    let sessions = Sessions::new(&store);
    for (session_id, agent_id, name) in [
        ("s_route_exact", "a_route_exact", "exact-display"),
        ("s_route_alias", "a_route_alias_owner", "a_route_exact"),
    ] {
        let session = SessionId(session_id.into());
        sessions
            .create(NewSession {
                session_id: session.clone(),
                name: Some(name.into()),
                agent: Some("claude".into()),
                kind: "agent".into(),
                role: None,
                tier: "agent".into(),
                harness_session_id: None,
                client_key: Some(format!("ck_{session_id}")),
                cwd: None,
                project: "runtime-metadata".into(),
                transport: Some("pty".into()),
            })
            .await
            .unwrap();
        sessions.set_agent_id(&session, agent_id).await.unwrap();
    }
    let agent = Arc::new(RecipientRecordingAgent::default());
    let state = AppState::new(
        store,
        WsSink::new(16, None),
        Arc::new(MockIdentity),
        agent.clone(),
        Arc::new(MockRealtime),
        Arc::new(MockBus::default()),
        Arc::new(MockSearch),
        Arc::new(MockNotify),
        Arc::new(MockAdmin),
        "caller-metadata".into(),
    );

    for (method, params) in [
        (
            "prompt",
            serde_json::json!({"name":"a_route_exact","text":"exact prompt"}),
        ),
        (
            "steer",
            serde_json::json!({"name":"a_route_exact","text":"exact steer"}),
        ),
        ("compact", serde_json::json!({"name":"a_route_exact"})),
        ("warm", serde_json::json!({"name":"a_route_exact"})),
    ] {
        let response = dispatch(&state, Some(agent_caller()), req(method, Some(params))).await;
        assert!(
            response.error.is_none(),
            "{method} must prefer the exact stable id over the alias collision: {:?}",
            response.error,
        );
    }
    assert_eq!(
        agent.recipients(),
        vec![
            SessionId("s_route_exact".into()),
            SessionId("s_route_exact".into()),
            SessionId("s_route_exact".into()),
        ],
    );
    assert_eq!(
        state
            .ensure_alive("a_route_exact", "unrelated-project")
            .await
            .unwrap(),
        SessionId("s_route_exact".into()),
    );
}

#[tokio::test]
async fn prompt_rejects_ambiguous_legacy_names_instead_of_preferring_caller_project() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    store
        .conn
        .execute("DROP INDEX idx_sessions_name", ())
        .await
        .unwrap();
    for (session_id, project) in [("s_legacy_local", "proj"), ("s_legacy_other", "other")] {
        Sessions::new(&store)
            .create(NewSession {
                session_id: SessionId(session_id.into()),
                name: Some("legacy-duplicate".into()),
                agent: Some("claude".into()),
                kind: "agent".into(),
                role: None,
                tier: "agent".into(),
                harness_session_id: None,
                client_key: Some(format!("ck_{session_id}")),
                cwd: None,
                project: project.into(),
                transport: Some("acp".into()),
            })
            .await
            .unwrap();
    }
    let state = AppState::new(
        store,
        WsSink::new(16, None),
        Arc::new(MockIdentity),
        Arc::new(MockAgent),
        Arc::new(MockRealtime),
        Arc::new(MockBus::default()),
        Arc::new(MockSearch),
        Arc::new(MockNotify),
        Arc::new(MockAdmin),
        "proj".into(),
    );

    let response = dispatch(
        &state,
        Some(agent_caller()),
        req(
            "prompt",
            Some(serde_json::json!({
                "name": "legacy-duplicate",
                "text": "must not guess",
            })),
        ),
    )
    .await;

    let error = response.error.expect("ambiguous fossil names must fail");
    assert_eq!(error.code, codes::INVALID_PARAMS);
    assert!(error.message.contains("ambiguous"));
}

#[tokio::test]
async fn prompt_does_not_hide_ambiguous_durable_agents_behind_one_fossil_session() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    store
        .conn
        .execute("DROP INDEX idx_agents_name_unique", ())
        .await
        .unwrap();
    for agent_id in ["a_ambiguous_one", "a_ambiguous_two"] {
        Agents::new(&store)
            .create(NewAgent {
                agent_id: agent_id.into(),
                project: "identity-metadata".into(),
                name: Some("ambiguous-agent".into()),
                default_harness: Some("claude".into()),
                role: None,
                tier: Some("agent".into()),
                owner: None,
            })
            .await
            .unwrap();
    }
    Sessions::new(&store)
        .create(NewSession {
            session_id: SessionId("s_unique_fossil".into()),
            name: Some("ambiguous-agent".into()),
            agent: Some("claude".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: None,
            client_key: Some("ck_unique_fossil".into()),
            cwd: None,
            project: "transport-metadata".into(),
            transport: Some("acp".into()),
        })
        .await
        .unwrap();
    let state = AppState::new(
        store,
        WsSink::new(16, None),
        Arc::new(MockIdentity),
        Arc::new(MockAgent),
        Arc::new(MockRealtime),
        Arc::new(MockBus::default()),
        Arc::new(MockSearch),
        Arc::new(MockNotify),
        Arc::new(MockAdmin),
        "proj".into(),
    );

    let response = dispatch(
        &state,
        Some(agent_caller()),
        req(
            "prompt",
            Some(serde_json::json!({
                "name": "ambiguous-agent",
                "text": "must not fossil-fallback",
            })),
        ),
    )
    .await;

    let error = response
        .error
        .expect("durable ambiguity must remain visible");
    assert_eq!(error.code, codes::INVALID_PARAMS);
    assert!(error.message.contains("ambiguous"));
}

/// Adding a thread member who lives in another workspace must MOVE its session into the thread's
/// workspace (never silently drop it; spec §12.4). Uses the real wired `AppState` over an in-memory
/// store so identity + store behavior is exercised end-to-end.
#[tokio::test]
async fn ensure_in_workspace_moves_a_cross_workspace_agent() {
    use nexus_common::Config;
    use nexus_contracts::RegisterRequest;

    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let state = AppState::wire(store, &Config::default());

    // Seed an agent registered in project "team".
    state
        .identity
        .register(RegisterRequest {
            agent_id: None,
            name: Some("carol".into()),
            harness: hid("claude"),
            harness_session_id: "hs_carol".into(),
            project: "team".into(),
            client_key: "ck_carol".into(),
            runtime_credential: None,
            tier: Tier::Agent,
            kind: None,
            locality: Default::default(),
            access: None,
            role: None,
            cwd: None,
        })
        .await
        .unwrap();

    state.ensure_in_workspace("carol", "default").await.unwrap();

    let row = nexus_store::repos::Sessions::new(&state.store)
        .find_by_name_any_project("carol")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.project, "default");
}

/// A validated [`nexus_contracts::HarnessId`] from a literal (panics on invalid — test-only).
fn hid(s: &str) -> nexus_contracts::HarnessId {
    nexus_contracts::HarnessId::new(s).expect("valid harness id literal")
}
