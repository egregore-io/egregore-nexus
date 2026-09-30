use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use nexus_bus::Bus;
use nexus_contracts::ack::{AckRequest, AckResponse, AckThreadsRequest};
use nexus_contracts::batch::{ConsumeRequest, NexusBatch};
use nexus_contracts::enums::Tier;
use nexus_contracts::events::WsEvent;
use nexus_contracts::ids::{MessageId, SessionId, ThreadId};
use nexus_contracts::ports::{BusPort, Caller, DispatchPort, EventSink, IdentityPort, PortResult};
use nexus_contracts::register::{
    HeartbeatResponse, MemberListRequest, MemberListResponse, RegisterRequest, RegisterResponse,
    StatusRequest, StatusResponse, Whoami,
};
use nexus_contracts::send::{SendRequest, SendTarget};
use nexus_contracts::threads::ThreadMembersRequest;
use nexus_store::repos::Threads;
use nexus_store::Store;

const PROJECT: &str = "default";

struct MockRealtime;

#[async_trait]
impl DispatchPort for MockRealtime {
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
        let session = self.sessions.get(name).unwrap_or_else(|| {
            panic!("test identity missing session for {name}");
        });
        Ok(Caller {
            agent_id: None,
            session: session.clone(),
            name: name.to_string(),
            project: project.to_string(),
            tier: Tier::Agent,
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

fn caller(name: &str, session: &str) -> Caller {
    Caller {
        agent_id: None,
        session: SessionId(session.into()),
        name: name.into(),
        project: PROJECT.into(),
        tier: Tier::Agent,
    }
}

#[tokio::test]
async fn thread_post_stores_thread_name_as_to_name() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();

    let threads = Threads::new(&store);
    let thread_id = ThreadId("t_backend".into());
    threads
        .create(&thread_id, "backend", PROJECT, "ben")
        .await
        .unwrap();
    for member in ["ben", "ana"] {
        threads.add_member(&thread_id, member).await.unwrap();
    }

    let identity = Arc::new(MockIdentity {
        sessions: HashMap::from([
            ("ben".to_string(), SessionId("s_ben".into())),
            ("ana".to_string(), SessionId("s_ana".into())),
        ]),
    });
    let bus = Bus::new(
        store.clone(),
        Arc::new(MockRealtime),
        identity,
        Arc::new(NoopSink),
    );

    bus.send(
        &caller("ben", "s_ben"),
        SendRequest {
            to: SendTarget::Post {
                thread: "backend".into(),
            },
            summary: None,
            body: "thread target check".into(),
            mention: vec![],
            metadata: None,
            idempotency_key: None,
        },
    )
    .await
    .unwrap();

    let mut rows = store
        .conn
        .query("SELECT to_name FROM messages WHERE kind = 'thread'", ())
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    let to_name: String = row.get(0).unwrap();
    assert_eq!(to_name, "backend");
}

#[tokio::test]
async fn archived_or_deleted_threads_do_not_route_as_thread_targets() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();

    let threads = Threads::new(&store);
    let archived_id = ThreadId("t_archived".into());
    threads
        .create(&archived_id, "archived", PROJECT, "ben")
        .await
        .unwrap();
    threads.add_member(&archived_id, "ben").await.unwrap();
    threads.archive("archived").await.unwrap();

    let deleted_id = ThreadId("t_deleted".into());
    threads
        .create(&deleted_id, "deleted", PROJECT, "ben")
        .await
        .unwrap();
    threads.add_member(&deleted_id, "ben").await.unwrap();
    threads.delete("deleted").await.unwrap();

    let identity = Arc::new(MockIdentity {
        sessions: HashMap::from([("ben".to_string(), SessionId("s_ben".into()))]),
    });
    let bus = Bus::new(
        store.clone(),
        Arc::new(MockRealtime),
        identity,
        Arc::new(NoopSink),
    );

    for thread in ["archived", "deleted"] {
        let err = bus
            .send(
                &caller("ben", "s_ben"),
                SendRequest {
                    to: SendTarget::Post {
                        thread: thread.to_string(),
                    },
                    summary: None,
                    body: "should not route".into(),
                    mention: vec![],
                    metadata: None,
                    idempotency_key: None,
                },
            )
            .await
            .expect_err("inactive thread should not route");
        assert_eq!(err.code, nexus_contracts::codes::NOT_FOUND);
        assert!(err.message.contains("thread:"));

        let err = bus
            .thread_members(
                &caller("ben", "s_ben"),
                ThreadMembersRequest {
                    name: thread.to_string(),
                },
            )
            .await
            .expect_err("inactive thread should not expose members");
        assert_eq!(err.code, nexus_contracts::codes::NOT_FOUND);
        assert!(err.message.contains("thread:"));
    }

    let mut rows = store
        .conn
        .query(
            "SELECT COUNT(*) FROM messages WHERE body = 'should not route'",
            (),
        )
        .await
        .unwrap();
    let count: i64 = rows.next().await.unwrap().unwrap().get(0).unwrap();
    assert_eq!(count, 0, "inactive thread sends must fail before write");
}
