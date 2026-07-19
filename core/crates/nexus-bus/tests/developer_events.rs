use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
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
use nexus_contracts::threads::{
    ArchiveThreadRequest, CreateThreadRequest, DeleteThreadRequest, JoinThreadRequest,
    LeaveThreadRequest, RenameThreadRequest,
};
use nexus_contracts::topics::{SubscribeRequest, UnsubscribeRequest};
use nexus_store::repos::{DeveloperEvents, Threads};
use nexus_store::Store;

const PROJECT: &str = "default";

#[derive(Default)]
struct CountingRealtime {
    enqueues: AtomicUsize,
}

#[async_trait]
impl DispatchPort for CountingRealtime {
    async fn enqueue(&self, _recipient: &SessionId, _message: &MessageId) -> PortResult<()> {
        self.enqueues.fetch_add(1, Ordering::SeqCst);
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

fn admin_caller(name: &str, session: &str) -> Caller {
    Caller {
        tier: Tier::Admin,
        ..caller(name, session)
    }
}

fn bus(store: Arc<Store>, realtime: Arc<CountingRealtime>) -> Bus {
    let identity = Arc::new(MockIdentity {
        sessions: HashMap::from([
            ("ada".to_string(), SessionId("s_ada".into())),
            ("ben".to_string(), SessionId("s_ben".into())),
            ("cy".to_string(), SessionId("s_cy".into())),
        ]),
    });
    Bus::new(store, realtime, identity, Arc::new(NoopSink))
}

async fn action_names(store: &Store, topic: &str) -> Vec<String> {
    DeveloperEvents::new(store)
        .since(topic, 0)
        .await
        .unwrap()
        .into_iter()
        .map(|row| {
            let data: serde_json::Value =
                serde_json::from_str(row.data_json.as_deref().unwrap_or("{}")).unwrap();
            data["action"].as_str().unwrap().to_string()
        })
        .collect()
}

#[tokio::test]
async fn thread_post_writes_metadata_events_without_extra_turn_wakes() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let thread_id = ThreadId("t_ops".into());
    let threads = Threads::new(&store);
    threads
        .create(&thread_id, "ops", PROJECT, "ada")
        .await
        .unwrap();
    for member in ["ada", "ben", "cy"] {
        threads.add_member(&thread_id, member).await.unwrap();
    }
    let realtime = Arc::new(CountingRealtime::default());
    let bus = bus(store.clone(), realtime.clone());

    let ack = bus
        .send(
            &caller("ada", "s_ada"),
            SendRequest {
                to: SendTarget::Post {
                    thread: "ops".into(),
                },
                summary: None,
                body: "status".into(),
                mention: vec![],
                metadata: None,
                idempotency_key: None,
            },
        )
        .await
        .unwrap();

    assert_eq!(ack.fanout, Some(2));
    assert_eq!(
        realtime.enqueues.load(Ordering::SeqCst),
        2,
        "developer events must not add turn wakes"
    );

    let events = DeveloperEvents::new(&store);
    assert_eq!(
        events.latest_seq("sys.message.thread.ops").await.unwrap(),
        1
    );
    assert_eq!(events.latest_seq("sys.inbox.ada").await.unwrap(), 1);
    assert_eq!(events.latest_seq("sys.inbox.ben").await.unwrap(), 1);
    assert_eq!(events.latest_seq("sys.inbox.cy").await.unwrap(), 1);

    let rows = events.since("sys.message.thread.ops", 0).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].message_id.as_deref(),
        Some(ack.message_id.0.as_str())
    );
    assert_eq!(rows[0].thread_name.as_deref(), Some("ops"));
    assert_eq!(rows[0].from_name.as_deref(), Some("ada"));

    let listed = bus.threads(&caller("ada", "s_ada")).await.unwrap();
    assert_eq!(listed.threads[0].latest_seq, Some(1));
}

#[tokio::test]
async fn dm_writes_private_per_party_sequences() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let realtime = Arc::new(CountingRealtime::default());
    let bus = bus(store.clone(), realtime.clone());

    let ack = bus
        .send(
            &caller("ada", "s_ada"),
            SendRequest {
                to: SendTarget::dm_name("ben"),
                summary: None,
                body: "private".into(),
                mention: vec![],
                metadata: None,
                idempotency_key: None,
            },
        )
        .await
        .unwrap();

    assert_eq!(ack.fanout, Some(1));
    assert_eq!(realtime.enqueues.load(Ordering::SeqCst), 1);

    let events = DeveloperEvents::new(&store);
    assert_eq!(events.latest_seq("sys.dm.ada").await.unwrap(), 1);
    assert_eq!(events.latest_seq("sys.dm.ben").await.unwrap(), 1);
    assert_eq!(events.latest_seq("sys.dm.cy").await.unwrap(), 0);
    assert_eq!(events.latest_seq("sys.inbox.ada").await.unwrap(), 1);
    assert_eq!(events.latest_seq("sys.inbox.ben").await.unwrap(), 1);

    let ben_rows = events.since("sys.dm.ben", 0).await.unwrap();
    assert_eq!(ben_rows.len(), 1);
    assert_eq!(
        ben_rows[0].message_id.as_deref(),
        Some(ack.message_id.0.as_str())
    );
    assert_eq!(ben_rows[0].dm_name.as_deref(), Some("ada"));
    assert_eq!(ben_rows[0].from_name.as_deref(), Some("ada"));
}

#[tokio::test]
async fn thread_create_writes_action_event() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let realtime = Arc::new(CountingRealtime::default());
    let bus = bus(store.clone(), realtime.clone());

    bus.create_thread(
        &caller("ada", "s_ada"),
        CreateThreadRequest {
            name: "launch".into(),
            members: vec!["ben".into()],
        },
    )
    .await
    .unwrap();

    let rows = DeveloperEvents::new(&store)
        .since("sys.thread.launch", 0)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].kind, "action");
    assert_eq!(rows[0].thread_name.as_deref(), Some("launch"));
    assert_eq!(rows[0].from_name.as_deref(), Some("ada"));
    let data: serde_json::Value =
        serde_json::from_str(rows[0].data_json.as_deref().unwrap()).unwrap();
    assert_eq!(data["action"], "thread.create");
    assert_eq!(data["members"], serde_json::json!(["ada", "ben"]));
    assert_eq!(realtime.enqueues.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn thread_join_leave_archive_delete_and_rename_write_action_events() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let thread_id = ThreadId("t_ops".into());
    let threads = Threads::new(&store);
    threads
        .create(&thread_id, "ops", PROJECT, "ada")
        .await
        .unwrap();
    threads.add_member(&thread_id, "ada").await.unwrap();
    let realtime = Arc::new(CountingRealtime::default());
    let bus = bus(store.clone(), realtime.clone());

    bus.join_thread(
        &caller("ben", "s_ben"),
        JoinThreadRequest { name: "ops".into() },
    )
    .await
    .unwrap();
    bus.leave_thread(
        &caller("ben", "s_ben"),
        LeaveThreadRequest { name: "ops".into() },
    )
    .await
    .unwrap();
    bus.archive_thread(
        &admin_caller("ada", "s_ada"),
        ArchiveThreadRequest { name: "ops".into() },
    )
    .await
    .unwrap();

    assert_eq!(
        action_names(&store, "sys.thread.ops").await,
        vec!["thread.join", "thread.leave", "thread.archive"]
    );

    let rename_id = ThreadId("t_rename".into());
    threads
        .create(&rename_id, "rename-me", PROJECT, "ada")
        .await
        .unwrap();
    bus.rename_thread(
        &admin_caller("ada", "s_ada"),
        RenameThreadRequest {
            name: "rename-me".into(),
            new_name: "renamed".into(),
        },
    )
    .await
    .unwrap();
    assert_eq!(
        action_names(&store, "sys.thread.renamed").await,
        vec!["thread.rename"]
    );

    let delete_id = ThreadId("t_delete".into());
    threads
        .create(&delete_id, "delete-me", PROJECT, "ada")
        .await
        .unwrap();
    bus.delete_thread(
        &admin_caller("ada", "s_ada"),
        DeleteThreadRequest {
            name: "delete-me".into(),
        },
    )
    .await
    .unwrap();
    assert_eq!(
        action_names(&store, "sys.thread.delete-me").await,
        vec!["thread.delete"]
    );
}

#[tokio::test]
async fn topic_subscribe_and_unsubscribe_write_action_events() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let realtime = Arc::new(CountingRealtime::default());
    let bus = bus(store.clone(), realtime.clone());

    bus.subscribe(
        &caller("ada", "s_ada"),
        SubscribeRequest {
            topic: "builds".into(),
            group: Some("workers".into()),
        },
    )
    .await
    .unwrap();
    bus.unsubscribe(
        &caller("ada", "s_ada"),
        UnsubscribeRequest {
            topic: "builds".into(),
        },
    )
    .await
    .unwrap();

    let rows = DeveloperEvents::new(&store)
        .since("sys.topic.builds", 0)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].kind, "action");
    assert_eq!(rows[0].from_name.as_deref(), Some("ada"));
    assert_eq!(rows[0].session_id.as_deref(), Some("s_ada"));
    let first: serde_json::Value =
        serde_json::from_str(rows[0].data_json.as_deref().unwrap()).unwrap();
    let second: serde_json::Value =
        serde_json::from_str(rows[1].data_json.as_deref().unwrap()).unwrap();
    assert_eq!(first["action"], "topic.subscribe");
    assert_eq!(first["group"], "workers");
    assert_eq!(second["action"], "topic.unsubscribe");
}

#[tokio::test]
async fn bus_action_events_are_best_effort_when_append_fails() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    store
        .conn
        .execute(
            "CREATE TRIGGER fail_action_events
             BEFORE INSERT ON developer_events
             WHEN NEW.kind = 'action'
             BEGIN
               SELECT RAISE(ABORT, 'forced action telemetry failure');
             END",
            (),
        )
        .await
        .unwrap();
    let realtime = Arc::new(CountingRealtime::default());
    let bus = bus(store.clone(), realtime.clone());

    bus.create_thread(
        &caller("ada", "s_ada"),
        CreateThreadRequest {
            name: "fail-open".into(),
            members: vec![],
        },
    )
    .await
    .unwrap();
    bus.subscribe(
        &caller("ada", "s_ada"),
        SubscribeRequest {
            topic: "fail-topic".into(),
            group: None,
        },
    )
    .await
    .unwrap();

    assert!(Threads::new(&store)
        .find_any_by_name("fail-open")
        .await
        .unwrap()
        .is_some());
    assert!(DeveloperEvents::new(&store)
        .since("sys.thread.fail-open", 0)
        .await
        .unwrap()
        .is_empty());
    assert!(DeveloperEvents::new(&store)
        .since("sys.topic.fail-topic", 0)
        .await
        .unwrap()
        .is_empty());
}
