use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use nexus::daemon::gateway_stream_socket::{GatewayStreamFrame, GatewayStreamPublisher};
use nexus::daemon::WsSink;
use nexus_bus::Bus;
use nexus_contracts::ack::{AckRequest, AckResponse, AckThreadsRequest};
use nexus_contracts::batch::{ConsumeRequest, NexusBatch};
use nexus_contracts::register::{
    HeartbeatResponse, MemberListRequest, MemberListResponse, RegisterRequest, RegisterResponse,
    StatusRequest, StatusResponse, Whoami,
};
use nexus_contracts::{
    AgentId, BusPort, Caller, DispatchPort, EventSink, GatewayProjectionEffect,
    GatewayProjectionKind, IdentityPort, MessageId, NotifySendRequest, NotifyTarget, PortResult,
    SendRequest, SendTarget, SessionId, SubscribeRequest, Tier, WsEvent,
};
use nexus_store::repos::{
    AgentRuntimes, Agents, Inbox, NewAgent, NewAgentRuntime, NewSession, Sessions, Threads,
};
use nexus_store::Store;

#[derive(Default)]
struct CaptureEvents(Mutex<Vec<GatewayProjectionEffect>>);

impl CaptureEvents {
    fn effects(&self) -> Vec<GatewayProjectionEffect> {
        self.0.lock().unwrap().clone()
    }
}

#[async_trait]
impl EventSink for CaptureEvents {
    async fn emit(&self, _event: WsEvent) {}

    async fn project(&self, effect: GatewayProjectionEffect) {
        self.0.lock().unwrap().push(effect);
    }
}

#[derive(Default)]
struct CaptureDispatch(Mutex<Vec<(SessionId, MessageId)>>);

#[async_trait]
impl DispatchPort for CaptureDispatch {
    async fn enqueue(&self, recipient: &SessionId, message: &MessageId) -> PortResult<()> {
        self.0
            .lock()
            .unwrap()
            .push((recipient.clone(), message.clone()));
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

struct TestIdentity(HashMap<String, (AgentId, SessionId)>);

#[async_trait]
impl IdentityPort for TestIdentity {
    async fn register(&self, _req: RegisterRequest) -> PortResult<RegisterResponse> {
        unreachable!()
    }

    async fn whoami(&self, _caller: &Caller) -> PortResult<Whoami> {
        unreachable!()
    }

    async fn resolve(&self, project: &str, name: &str) -> PortResult<Caller> {
        let (agent_id, session) = self.0.get(name).expect("known test identity");
        Ok(Caller {
            agent_id: Some(agent_id.clone()),
            session: session.clone(),
            name: name.into(),
            project: project.into(),
            tier: Tier::Agent,
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

async fn seed_agent(store: &Store, id: &str, name: Option<&str>, session: &str) {
    Agents::new(store)
        .create(NewAgent {
            agent_id: id.into(),
            project: "identity-metadata".into(),
            name: name.map(str::to_string),
            default_harness: Some("codex".into()),
            role: None,
            tier: None,
            owner: None,
        })
        .await
        .unwrap();
    let session_id = Sessions::new(store)
        .create(NewSession {
            session_id: SessionId(session.into()),
            name: name.map(str::to_string),
            agent: Some("codex".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: None,
            client_key: Some(format!("ck_{session}")),
            cwd: None,
            project: "runtime-metadata".into(),
            transport: Some("app-server".into()),
        })
        .await
        .unwrap();
    Sessions::new(store)
        .set_agent_id(&session_id, id)
        .await
        .unwrap();
    AgentRuntimes::new(store)
        .create(NewAgentRuntime {
            runtime_id: session.into(),
            agent_id: id.into(),
            harness: "codex".into(),
            cwd: None,
            transport: Some("app-server".into()),
            presence: Some("online".into()),
            active: true,
        })
        .await
        .unwrap();
}

fn caller() -> Caller {
    Caller {
        agent_id: Some(AgentId("a_sender".into())),
        session: SessionId("s_sender".into()),
        name: "sender".into(),
        project: "caller-metadata".into(),
        tier: Tier::Admin,
    }
}

#[tokio::test]
async fn committed_notification_projects_once_with_id_only_target_and_no_fanout_field() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    seed_agent(&store, "a_sender", Some("sender"), "s_sender").await;
    seed_agent(&store, "a_unnamed", None, "s_unnamed").await;
    let events = Arc::new(CaptureEvents::default());
    let bus = Bus::new(
        store.clone(),
        Arc::new(CaptureDispatch::default()),
        Arc::new(TestIdentity(HashMap::new())),
        events.clone(),
    );

    let request = NotifySendRequest {
        target: NotifyTarget::Agent {
            agent_id: AgentId("a_unnamed".into()),
        },
        source: Some("build-watch".into()),
        body: "ready".into(),
        idempotency_key: Some("notify:ready".into()),
    };
    let first = bus.notify(&caller(), request.clone()).await.unwrap();
    let replay = bus.notify(&caller(), request).await.unwrap();
    assert_eq!(first.message_id, replay.message_id);

    let effects = events.effects();
    assert_eq!(
        effects.len(),
        2,
        "one accepted fact plus one notification fact"
    );
    assert_eq!(effects[0].kind, GatewayProjectionKind::MessageAccepted);
    assert_eq!(effects[1].kind, GatewayProjectionKind::NotificationEmitted);
    assert_eq!(effects[0].payload["messageId"], first.message_id.0);
    assert_eq!(effects[1].payload["messageId"], first.message_id.0);
    assert_eq!(effects[0].payload["toAgentId"], "a_unnamed");
    assert!(effects[0].payload["toName"].is_null());
    assert_eq!(effects[0].payload["project"], "caller-metadata");
    assert!(effects[0].payload.get("fanout").is_none());
}

#[tokio::test]
async fn thread_fanout_is_daemon_resolved_but_projects_one_message_fact() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    seed_agent(&store, "a_sender", Some("sender"), "s_sender").await;
    seed_agent(&store, "a_one", Some("one"), "s_one").await;
    seed_agent(&store, "a_two", Some("two"), "s_two").await;
    let thread_id = nexus_contracts::ThreadId("t_review".into());
    Threads::new(&store)
        .create(&thread_id, "review", "thread-metadata", "sender")
        .await
        .unwrap();
    for member in ["sender", "one", "two"] {
        Threads::new(&store)
            .add_member(&thread_id, member)
            .await
            .unwrap();
    }
    let events = Arc::new(CaptureEvents::default());
    let bus = Bus::new(
        store,
        Arc::new(CaptureDispatch::default()),
        Arc::new(TestIdentity(HashMap::from([
            (
                "one".into(),
                (AgentId("a_one".into()), SessionId("s_one".into())),
            ),
            (
                "two".into(),
                (AgentId("a_two".into()), SessionId("s_two".into())),
            ),
        ]))),
        events.clone(),
    );
    let ack = bus
        .send(
            &caller(),
            SendRequest {
                to: SendTarget::Post {
                    thread: "review".into(),
                },
                summary: None,
                body: "one durable post".into(),
                mention: Vec::new(),
                metadata: None,
                idempotency_key: Some("thread:one".into()),
            },
        )
        .await
        .unwrap();
    assert_eq!(ack.fanout, Some(2));
    let effects = events.effects();
    assert_eq!(effects.len(), 1);
    assert_eq!(effects[0].kind, GatewayProjectionKind::MessageAccepted);
    assert_eq!(effects[0].payload["messageId"], ack.message_id.0);
    assert_eq!(effects[0].payload["toName"], "review");
    assert_eq!(effects[0].payload["threadId"], "t_review");
    assert_eq!(effects[0].payload["project"], "thread-metadata");
    assert!(effects[0].payload.get("fanout").is_none());
}

#[tokio::test]
async fn terminal_delivery_projection_is_stable_and_failed_mutation_emits_nothing() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    seed_agent(&store, "a_sender", Some("sender"), "s_sender").await;
    seed_agent(&store, "a_target", Some("target"), "s_target").await;
    let events = Arc::new(CaptureEvents::default());
    let bus = Bus::new(
        store.clone(),
        Arc::new(CaptureDispatch::default()),
        Arc::new(TestIdentity(HashMap::from([(
            "target".into(),
            (AgentId("a_target".into()), SessionId("s_target".into())),
        )]))),
        events.clone(),
    );
    let before = events.effects().len();
    assert!(bus
        .send(
            &caller(),
            SendRequest {
                to: SendTarget::dm_name("target"),
                summary: None,
                body: "   ".into(),
                mention: Vec::new(),
                metadata: None,
                idempotency_key: Some("invalid".into()),
            },
        )
        .await
        .is_err());
    assert_eq!(events.effects().len(), before);

    let ack = bus
        .send(
            &caller(),
            SendRequest {
                to: SendTarget::dm_name("target"),
                summary: None,
                body: "settle me".into(),
                mention: Vec::new(),
                metadata: None,
                idempotency_key: Some("settle".into()),
            },
        )
        .await
        .unwrap();
    let inbox = Inbox::new(&store);
    inbox
        .mark_notified(&SessionId("s_target".into()))
        .await
        .unwrap();
    assert_eq!(
        inbox
            .mark_injecting(&ack.message_id, &SessionId("s_target".into()))
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        inbox
            .mark_delivery_error(
                &ack.message_id,
                &SessionId("s_target".into()),
                "target_dead",
                "target exited",
                None,
            )
            .await
            .unwrap(),
        1
    );
    let first = inbox
        .gateway_delivery_effect(&ack.message_id, &SessionId("s_target".into()))
        .await
        .unwrap()
        .unwrap();
    let replay = inbox
        .gateway_delivery_effect(&ack.message_id, &SessionId("s_target".into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.event_id, replay.event_id);
    assert_eq!(first.kind, GatewayProjectionKind::DeliverySettled);
    assert_eq!(first.payload["state"], "error");
    assert_eq!(first.payload["errorCode"], "target_dead");
    assert_eq!(
        inbox
            .mark_delivery_error(
                &ack.message_id,
                &SessionId("s_target".into()),
                "target_dead",
                "automatic retry must not happen",
                None,
            )
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn thread_and_topic_mutations_project_declared_current_state() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    seed_agent(&store, "a_sender", Some("sender"), "s_sender").await;
    let events = Arc::new(CaptureEvents::default());
    let bus = Bus::new(
        store,
        Arc::new(CaptureDispatch::default()),
        Arc::new(TestIdentity(HashMap::new())),
        events.clone(),
    );
    bus.create_thread(
        &caller(),
        nexus_contracts::CreateThreadRequest {
            name: "release".into(),
            members: vec![],
        },
    )
    .await
    .unwrap();
    bus.subscribe(
        &caller(),
        SubscribeRequest {
            topic: "builds".into(),
            group: None,
        },
    )
    .await
    .unwrap();
    let kinds = events
        .effects()
        .into_iter()
        .map(|effect| effect.kind)
        .collect::<Vec<_>>();
    assert!(kinds.contains(&GatewayProjectionKind::ThreadDeclared));
    assert!(kinds.contains(&GatewayProjectionKind::ThreadMembershipChanged));
    assert!(kinds.contains(&GatewayProjectionKind::TopicDeclared));
    assert!(kinds.contains(&GatewayProjectionKind::TopicSubscriptionChanged));
}

#[tokio::test]
async fn committed_lifecycle_events_project_identity_runtime_and_presence() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    seed_agent(&store, "a_sender", Some("sender"), "s_sender").await;
    let publisher = GatewayStreamPublisher::new(32);
    let mut rx = publisher.subscribe();
    let sink = WsSink::new(32, Some(store)).with_gateway_stream(publisher);
    sink.emit(WsEvent::AgentSpawned {
        session_id: SessionId("s_sender".into()),
        name: Some("sender".into()),
        agent_id: Some("a_sender".into()),
    })
    .await;

    match rx.recv().await.unwrap() {
        GatewayStreamFrame::DeveloperEvent { event, .. } => {
            assert_eq!(event.lifecycle.as_deref(), Some("spawned"));
        }
        other => panic!("expected established fleet-status wake first, got {other:?}"),
    }
    let first = rx.recv().await.unwrap();
    let second = rx.recv().await.unwrap();
    let kinds = [first, second]
        .into_iter()
        .map(|frame| match frame {
            GatewayStreamFrame::Projection { event } => event.kind,
            other => panic!("expected lifecycle projection, got {other:?}"),
        })
        .collect::<Vec<_>>();
    assert!(kinds.contains(&GatewayProjectionKind::IdentityUpserted));
    assert!(kinds.contains(&GatewayProjectionKind::RuntimeUpserted));

    sink.emit(WsEvent::AgentStatus {
        session_id: SessionId("s_sender".into()),
        presence: nexus_contracts::Presence::Busy,
        paused: false,
    })
    .await;
    match rx.recv().await.unwrap() {
        GatewayStreamFrame::DeveloperEvent { event, .. } => {
            assert_eq!(event.lifecycle.as_deref(), Some("status"));
        }
        other => panic!("expected established fleet-status wake first, got {other:?}"),
    }
    match rx.recv().await.unwrap() {
        GatewayStreamFrame::Projection { event } => {
            assert_eq!(event.kind, GatewayProjectionKind::PresenceChanged)
        }
        other => panic!("expected presence projection, got {other:?}"),
    }
}
