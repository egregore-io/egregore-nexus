use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

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
use nexus_store::repos::Threads;
use nexus_store::Store;

const PROJECT: &str = "default";

#[derive(Default)]
struct MockRealtime {
    count: AtomicUsize,
    enqueued: Mutex<Vec<SessionId>>,
}

struct ParallelBellRealtime {
    entered: AtomicUsize,
    barrier: tokio::sync::Barrier,
}

impl ParallelBellRealtime {
    fn new(participants: usize) -> Self {
        Self {
            entered: AtomicUsize::new(0),
            barrier: tokio::sync::Barrier::new(participants),
        }
    }
}

#[async_trait]
impl DispatchPort for ParallelBellRealtime {
    async fn enqueue(&self, recipient: &SessionId, _message: &MessageId) -> PortResult<()> {
        self.entered.fetch_add(1, Ordering::SeqCst);
        self.barrier.wait().await;
        if recipient.0 == "s_etan" {
            return Err(nexus_contracts::ports::ContractError {
                code: nexus_contracts::codes::INTERNAL_ERROR,
                message: "injected bell failure".into(),
            });
        }
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

#[async_trait]
impl DispatchPort for MockRealtime {
    async fn enqueue(&self, recipient: &SessionId, _message: &MessageId) -> PortResult<()> {
        self.count.fetch_add(1, Ordering::SeqCst);
        self.enqueued.lock().unwrap().push(recipient.clone());
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

impl MockRealtime {
    fn enqueue_count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }

    fn recipients(&self) -> Vec<SessionId> {
        self.enqueued.lock().unwrap().clone()
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
        let session = self
            .sessions
            .get(name)
            .unwrap_or_else(|| panic!("test identity missing session for {name}"));
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

fn build(store: Arc<Store>) -> (Bus, Arc<MockRealtime>) {
    let realtime = Arc::new(MockRealtime::default());
    let identity = Arc::new(MockIdentity {
        sessions: HashMap::from([
            ("ana".to_string(), SessionId("s_ana".into())),
            ("ben".to_string(), SessionId("s_ben".into())),
            ("etan".to_string(), SessionId("s_etan".into())),
            ("zed".to_string(), SessionId("s_zed".into())),
        ]),
    });
    let bus = Bus::new(
        store,
        realtime.clone() as Arc<dyn DispatchPort>,
        identity,
        Arc::new(NoopSink),
    );
    (bus, realtime)
}

#[tokio::test]
async fn reply_broadcast_owns_its_transaction_and_fails_loud_inside_a_foreign_one() {
    // Contract since the 2026-07-09 savepoint ban: broadcast opens its OWN `BEGIN IMMEDIATE`
    // (Hrana/sqld drops savepoints across round-trips, and savepoint error paths were the
    // file-mode write-wedge class). A caller-held open transaction on the shared connection is a
    // programming error and must fail loudly — never wedge, never silently join.
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let (bus, realtime) = build(store.clone());

    bus.send(
        &caller("etan", "s_etan"),
        SendRequest {
            to: SendTarget::dm_name("ben"),
            summary: None,
            body: "first inbound".into(),
            mention: vec![],
            metadata: None,
            idempotency_key: None,
        },
    )
    .await
    .unwrap();

    store.conn.execute("BEGIN", ()).await.unwrap();
    let inside = bus
        .send(
            &caller("ben", "s_ben"),
            SendRequest {
                to: SendTarget::Reply,
                summary: None,
                body: "reply inside caller transaction".into(),
                mention: vec![],
                metadata: None,
                idempotency_key: None,
            },
        )
        .await;
    assert!(
        inside.is_err(),
        "send inside a foreign open transaction must fail loud, not nest or wedge"
    );
    // The foreign transaction must still be intact and releasable after the failure.
    store.conn.execute("ROLLBACK", ()).await.unwrap();

    let ack = bus
        .send(
            &caller("ben", "s_ben"),
            SendRequest {
                to: SendTarget::Reply,
                summary: None,
                body: "reply after caller transaction released".into(),
                mention: vec![],
                metadata: None,
                idempotency_key: None,
            },
        )
        .await
        .expect("send succeeds once the connection is back in autocommit");

    assert_eq!(ack.fanout, Some(1));
    assert_eq!(realtime.enqueue_count(), 2);
    assert_eq!(
        realtime.recipients(),
        vec![SessionId("s_ben".into()), SessionId("s_etan".into())]
    );

    let mut rows = store
        .conn
        .query(
            "SELECT COUNT(*) FROM messages WHERE body = 'reply after caller transaction released'",
            (),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().expect("count row");
    let count: i64 = row.get(0).unwrap();
    assert_eq!(count, 1);

    let mut rows = store
        .conn
        .query(
            "SELECT COUNT(*) FROM messages WHERE body = 'reply inside caller transaction'",
            (),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().expect("count row");
    let count: i64 = row.get(0).unwrap();
    assert_eq!(count, 0, "failed send must leave no partial rows");
}

#[tokio::test]
async fn fanout_statement_failure_rolls_back_message_fts_and_every_recipient_before_bells() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let (bus, realtime) = build(store.clone());
    let ana = caller("ana", "s_ana");

    bus.create_thread(
        &ana,
        nexus_contracts::CreateThreadRequest {
            name: "atomic-failure".into(),
            members: vec!["ben".into(), "etan".into()],
        },
    )
    .await
    .unwrap();
    store
        .conn
        .execute(
            "CREATE TRIGGER force_second_recipient_failure \
             BEFORE INSERT ON in_flight \
             WHEN NEW.recipient_session = 's_etan' \
             BEGIN SELECT RAISE(ABORT, 'forced fanout failure'); END",
            (),
        )
        .await
        .unwrap();

    let result = bus
        .send(
            &ana,
            SendRequest {
                to: SendTarget::Post {
                    thread: "atomic-failure".into(),
                },
                summary: Some("must roll back".into()),
                body: "no partial broadcast may survive".into(),
                mention: vec![],
                metadata: None,
                idempotency_key: Some("atomic-failure-1".into()),
            },
        )
        .await;
    assert!(
        result.is_err(),
        "the injected recipient failure must fail the send"
    );
    assert_eq!(
        realtime.enqueue_count(),
        0,
        "bells must not ring when the durable batch rolls back"
    );

    for (table, predicate) in [
        ("messages", "body = 'no partial broadcast may survive'"),
        ("in_flight", "1 = 1"),
        ("messages_fts", "body = 'no partial broadcast may survive'"),
    ] {
        let mut rows = store
            .conn
            .query(
                &format!("SELECT COUNT(*) FROM {table} WHERE {predicate}"),
                (),
            )
            .await
            .unwrap();
        let count = rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap();
        assert_eq!(count, 0, "{table} retained a partial broadcast row");
    }
}

#[tokio::test]
async fn large_quoted_unicode_body_is_bound_once_and_round_trips_through_fts() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let (bus, _) = build(store.clone());
    let marker = "parameterized64kmarker";
    let body = format!("{marker} 'quotes' 雪 λ 🧪\n{}", "x".repeat(65_536));

    let ack = bus
        .send(
            &caller("ana", "s_ana"),
            SendRequest {
                to: SendTarget::dm_name("ben"),
                summary: Some("résumé 'quoted' 雪".into()),
                body: body.clone(),
                mention: vec![],
                metadata: None,
                idempotency_key: Some("parameterized-large-body".into()),
            },
        )
        .await
        .unwrap();

    let mut rows = store
        .conn
        .query(
            "SELECT body, summary FROM messages WHERE message_id = ?1",
            libsql::params![ack.message_id.0.clone()],
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().expect("message row");
    assert_eq!(row.get::<String>(0).unwrap(), body);
    assert_eq!(row.get::<String>(1).unwrap(), "résumé 'quoted' 雪");

    let mut rows = store
        .conn
        .query(
            "SELECT COUNT(*) FROM messages_fts WHERE messages_fts MATCH ?1",
            libsql::params![marker],
        )
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
        1
    );
}

#[tokio::test]
async fn recipient_bells_run_in_parallel_and_one_failure_does_not_reject_durable_send() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let thread_id = ThreadId("t_parallel_bells".into());
    let threads = Threads::new(&store);
    threads
        .create(&thread_id, "parallel-bells", PROJECT, "ana")
        .await
        .unwrap();
    for member in ["ana", "ben", "etan", "zed"] {
        threads.add_member(&thread_id, member).await.unwrap();
    }
    let realtime = Arc::new(ParallelBellRealtime::new(3));
    let identity = Arc::new(MockIdentity {
        sessions: HashMap::from([
            ("ana".to_string(), SessionId("s_ana".into())),
            ("ben".to_string(), SessionId("s_ben".into())),
            ("etan".to_string(), SessionId("s_etan".into())),
            ("zed".to_string(), SessionId("s_zed".into())),
        ]),
    });
    let bus = Bus::new(store, realtime.clone(), identity, Arc::new(NoopSink));
    let ana = caller("ana", "s_ana");

    let ack = tokio::time::timeout(
        Duration::from_secs(1),
        bus.send(
            &ana,
            SendRequest {
                to: SendTarget::Post {
                    thread: "parallel-bells".into(),
                },
                summary: None,
                body: "all bells enter together".into(),
                mention: vec![],
                metadata: None,
                idempotency_key: Some("parallel-bells-1".into()),
            },
        ),
    )
    .await
    .expect("sequential bells would deadlock at the barrier")
    .expect("a post-commit bell failure must not reject the durable send");

    assert_eq!(ack.fanout, Some(3));
    assert_eq!(realtime.entered.load(Ordering::SeqCst), 3);
}
