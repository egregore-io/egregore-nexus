use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use libsql::params;
use nexus_bus::Bus;
use nexus_contracts::ack::{AckRequest, AckResponse, AckThreadsRequest};
use nexus_contracts::batch::{ConsumeRequest, NexusBatch};
use nexus_contracts::register::{
    HeartbeatResponse, MemberListRequest, MemberListResponse, RegisterRequest, RegisterResponse,
    StatusRequest, StatusResponse, Whoami,
};
use nexus_contracts::{
    AgentId, BusPort, Caller, DispatchPort, EventSink, MessageId, NotifySendRequest, NotifyTarget,
    PortResult, SendRequest, SendTarget, SessionId, Tier, WsEvent,
};
use nexus_store::repos::{
    AgentGroups, AgentRuntimes, Agents, NewAgent, NewAgentRuntime, NewSession, Sessions, Threads,
};
use nexus_store::Store;

struct CaptureDispatch(Mutex<Vec<SessionId>>);

#[async_trait]
impl DispatchPort for CaptureDispatch {
    async fn enqueue(&self, recipient: &SessionId, _message: &MessageId) -> PortResult<()> {
        self.0.lock().unwrap().push(recipient.clone());
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

#[tokio::test]
async fn positional_dm_target_accepts_a_stable_agent_id_without_a_name() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    seed_agent(&store, "a_unnamed", None, "s_unnamed").await;

    let dispatch = Arc::new(CaptureDispatch(Mutex::new(Vec::new())));
    let bus = Bus::new(
        store.clone(),
        dispatch.clone(),
        Arc::new(TestIdentity(HashMap::new())),
        Arc::new(NullEvents),
    );
    let caller = Caller {
        agent_id: None,
        session: SessionId("s_operator".into()),
        name: "operator".into(),
        project: "default".into(),
        tier: Tier::Admin,
    };

    let ack = bus
        .send(
            &caller,
            SendRequest {
                to: SendTarget::dm_name("a_unnamed"),
                summary: None,
                body: "id-only delivery".into(),
                mention: vec![],
                metadata: None,
                idempotency_key: Some("id-only-delivery".into()),
            },
        )
        .await
        .unwrap();

    assert_eq!(ack.fanout, Some(1));
    assert_eq!(
        dispatch.0.lock().unwrap().as_slice(),
        &[SessionId("s_unnamed".into())]
    );
    let mut rows = store
        .conn
        .query(
            "SELECT to_agent_id FROM messages WHERE body = 'id-only delivery'",
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        rows.next()
            .await
            .unwrap()
            .expect("message row")
            .get::<String>(0)
            .unwrap(),
        "a_unnamed"
    );
}

#[tokio::test]
async fn named_agent_dm_resolves_the_alias_then_routes_by_stable_agent_id() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    seed_agent(&store, "a_target", Some("target"), "s_current").await;

    let dispatch = Arc::new(CaptureDispatch(Mutex::new(Vec::new())));
    let bus = Bus::new(
        store,
        dispatch.clone(),
        Arc::new(TestIdentity(HashMap::from([(
            "target".into(),
            (AgentId("a_target".into()), SessionId("s_stale".into())),
        )]))),
        Arc::new(NullEvents),
    );
    let caller = Caller {
        agent_id: None,
        session: SessionId("s_operator".into()),
        name: "operator".into(),
        project: "default".into(),
        tier: Tier::Admin,
    };

    bus.send(
        &caller,
        SendRequest {
            to: SendTarget::dm_name("target"),
            summary: None,
            body: "alias routes through identity".into(),
            mention: vec![],
            metadata: None,
            idempotency_key: Some("alias-to-id".into()),
        },
    )
    .await
    .unwrap();

    assert_eq!(
        dispatch.0.lock().unwrap().as_slice(),
        &[SessionId("s_current".into())],
        "the alias resolver's session is not a routing key"
    );
}

struct NullEvents;

#[async_trait]
impl EventSink for NullEvents {
    async fn emit(&self, _event: WsEvent) {}
}

struct TestIdentity(HashMap<String, (AgentId, SessionId)>);

#[async_trait]
impl nexus_contracts::IdentityPort for TestIdentity {
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
            project: "metadata-only".into(),
            name: name.map(str::to_string),
            default_harness: Some("claude".into()),
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
            agent: Some("claude".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: None,
            client_key: Some(format!("ck_{session}")),
            cwd: None,
            project: "different-metadata".into(),
            transport: Some("acp".into()),
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
            harness: "claude".into(),
            cwd: None,
            transport: Some("acp".into()),
            presence: Some("online".into()),
            active: true,
        })
        .await
        .unwrap();
}

async fn count_for_body(store: &Store, table: &str, body: &str) -> i64 {
    let sql = match table {
        "messages" => "SELECT COUNT(*) FROM messages WHERE body = ?1",
        "in_flight" => {
            "SELECT COUNT(*) FROM in_flight f JOIN messages m ON m.message_id = f.message_id WHERE m.body = ?1"
        }
        _ => unreachable!(),
    };
    let mut rows = store.conn.query(sql, params![body]).await.unwrap();
    rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap()
}

#[tokio::test]
async fn one_shot_notify_resolves_group_thread_and_unnamed_id_with_one_body_each() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    seed_agent(&store, "a_alice", Some("alice"), "s_alice").await;
    seed_agent(&store, "a_blake", Some("blake"), "s_blake").await;
    seed_agent(&store, "a_unnamed", None, "s_unnamed").await;

    for (id, name) in [("a_alice", "alice"), ("a_blake", "blake")] {
        AgentGroups::new(&store)
            .assign("ignored-a", "reviewers", &AgentId(id.into()), name)
            .await
            .unwrap();
    }
    let thread_id = nexus_contracts::ThreadId("t_release".into());
    Threads::new(&store)
        .create(&thread_id, "release", "ignored-b", "operator")
        .await
        .unwrap();
    Threads::new(&store)
        .add_member(&thread_id, "alice")
        .await
        .unwrap();
    Threads::new(&store)
        .add_member(&thread_id, "blake")
        .await
        .unwrap();

    let bus = Bus::new(
        store.clone(),
        Arc::new(CaptureDispatch(Mutex::new(Vec::new()))),
        Arc::new(TestIdentity(HashMap::from([
            (
                "alice".into(),
                (AgentId("a_alice".into()), SessionId("s_alice".into())),
            ),
            (
                "blake".into(),
                (AgentId("a_blake".into()), SessionId("s_blake".into())),
            ),
        ]))),
        Arc::new(NullEvents),
    );
    let caller = Caller {
        agent_id: None,
        session: SessionId("s_operator".into()),
        name: "operator".into(),
        project: "default".into(),
        tier: Tier::Admin,
    };

    let cases = [
        (
            NotifyTarget::Group {
                group: "reviewers".into(),
            },
            "group checkpoint",
            2,
        ),
        (
            NotifyTarget::Thread {
                thread: "release".into(),
            },
            "thread checkpoint",
            2,
        ),
        (
            NotifyTarget::Agent {
                agent_id: AgentId("a_unnamed".into()),
            },
            "unnamed checkpoint",
            1,
        ),
    ];

    for (target, body, recipients) in cases {
        let ack = bus
            .notify(
                &caller,
                NotifySendRequest {
                    target,
                    source: Some("watchdog".into()),
                    body: body.into(),
                    idempotency_key: Some(format!("test:{body}")),
                },
            )
            .await
            .unwrap();
        assert_eq!(ack.fanout, Some(recipients));
        assert_eq!(count_for_body(&store, "messages", body).await, 1);
        assert_eq!(
            count_for_body(&store, "in_flight", body).await,
            i64::from(recipients)
        );
    }

    let mut rows = store
        .conn
        .query(
            "SELECT from_name, provenance FROM messages WHERE body = 'group checkpoint'",
            (),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<String>(0).unwrap(), "watchdog");
    assert!(row
        .get::<String>(1)
        .unwrap()
        .contains(r#""kind":"notification""#));
}
