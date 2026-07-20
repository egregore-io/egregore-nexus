//! Thread fan-out durability regressions (integration surface: `Bus::send`).
//!
//! N17 (gate-caught 2026-07-10): a thread member whose runtime row is stopped/flapping at
//! the send instant — and whose session row carries no `agent_id` stamp — was silently
//! dropped from fan-out: no `in_flight` row, durable mail lost. Membership is durable, so
//! resolution must fall back to the member's stored name before giving up.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use nexus_bus::Bus;
use nexus_contracts::ack::{AckRequest, AckResponse, AckThreadsRequest};
use nexus_contracts::batch::{ConsumeRequest, NexusBatch};
use nexus_contracts::enums::Tier;
use nexus_contracts::events::WsEvent;
use nexus_contracts::ids::{AgentId, MessageId, SessionId, ThreadId};
use nexus_contracts::ports::{
    BusPort, Caller, ContractError, DispatchPort, EventSink, IdentityPort, PortResult,
};
use nexus_contracts::register::{
    HeartbeatResponse, MemberListRequest, MemberListResponse, RegisterRequest, RegisterResponse,
    StatusRequest, StatusResponse, Whoami,
};
use nexus_contracts::send::{SendRequest, SendTarget};
use nexus_contracts::threads::JoinThreadRequest;
use nexus_store::repos::{
    AgentRuntimes, Agents, NewAgent, NewAgentRuntime, NewSession, Sessions, Threads, Topics,
};
use nexus_store::{DaemonStore, Store};

const PROJECT: &str = "p_demo";

#[derive(Default)]
struct MockRealtime {
    count: AtomicUsize,
    enqueued: Mutex<Vec<(SessionId, MessageId)>>,
}

#[async_trait]
impl DispatchPort for MockRealtime {
    async fn enqueue(&self, recipient: &SessionId, message: &MessageId) -> PortResult<()> {
        self.count.fetch_add(1, Ordering::SeqCst);
        self.enqueued
            .lock()
            .unwrap()
            .push((recipient.clone(), message.clone()));
        Ok(())
    }
    async fn consume(&self, _caller: &Caller, _req: ConsumeRequest) -> PortResult<NexusBatch> {
        unreachable!("not used by fan-out tests")
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

impl MockRealtime {
    fn recipients(&self) -> Vec<SessionId> {
        self.enqueued
            .lock()
            .unwrap()
            .iter()
            .map(|(r, _)| r.clone())
            .collect()
    }
}

/// Name → session map. Names absent from the map do NOT resolve — the test relies on that
/// to prove fan-out survives on store truth alone.
struct MockIdentity {
    project: String,
    names: HashMap<String, (SessionId, Option<AgentId>)>,
}

#[async_trait]
impl IdentityPort for MockIdentity {
    async fn register(&self, _req: RegisterRequest) -> PortResult<RegisterResponse> {
        unreachable!()
    }
    async fn whoami(&self, _caller: &Caller) -> PortResult<Whoami> {
        unreachable!()
    }
    async fn resolve(&self, project: &str, name: &str) -> PortResult<Caller> {
        if project != self.project {
            return Err(ContractError {
                code: nexus_contracts::codes::PROJECT_SCOPE_VIOLATION,
                message: "scope".into(),
            });
        }
        match self.names.get(name) {
            Some((session, agent_id)) => Ok(Caller {
                agent_id: agent_id.clone(),
                session: session.clone(),
                name: name.to_string(),
                project: project.to_string(),
                tier: Tier::Agent,
            }),
            None => Err(ContractError {
                code: nexus_contracts::codes::NOT_FOUND,
                message: name.to_string(),
            }),
        }
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

struct NullSink;
#[async_trait]
impl EventSink for NullSink {
    async fn emit(&self, _event: WsEvent) {}
}

/// Batch routing must resolve store-backed fossil members without turning fan-out into one
/// `IdentityPort::resolve` call per member. Any call is a regression in the bounded read path.
struct RejectResolveIdentity;

#[async_trait]
impl IdentityPort for RejectResolveIdentity {
    async fn register(&self, _req: RegisterRequest) -> PortResult<RegisterResponse> {
        unreachable!()
    }

    async fn whoami(&self, _caller: &Caller) -> PortResult<Whoami> {
        unreachable!()
    }

    async fn resolve(&self, _project: &str, name: &str) -> PortResult<Caller> {
        panic!("batch-resolvable member unexpectedly used IdentityPort::resolve: {name}")
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

async fn migrated_store() -> Arc<Store> {
    let s = Store::open(":memory:").await.unwrap();
    s.migrate().await.unwrap();
    Arc::new(s)
}

async fn split_store() -> (tempfile::TempDir, Arc<Store>) {
    let directory = tempfile::tempdir().unwrap();
    let identity_path = directory.path().join("identity.db");
    let daemon = DaemonStore::open(identity_path.to_str().unwrap())
        .await
        .unwrap();
    (directory, Arc::new(daemon.compatibility_store()))
}

fn caller_with_agent(name: &str, session: &str, agent_id: &str) -> Caller {
    Caller {
        agent_id: Some(AgentId(agent_id.into())),
        session: SessionId(session.into()),
        name: name.into(),
        project: PROJECT.into(),
        tier: Tier::Agent,
    }
}

fn new_agent(agent_id: &str, name: &str) -> NewAgent {
    NewAgent {
        agent_id: agent_id.to_string(),
        project: PROJECT.to_string(),
        name: Some(name.to_string()),
        default_harness: Some("claude".to_string()),
        role: None,
        tier: None,
        owner: None,
    }
}

fn new_runtime(runtime_id: &str, agent_id: &str) -> NewAgentRuntime {
    NewAgentRuntime {
        runtime_id: runtime_id.to_string(),
        agent_id: agent_id.to_string(),
        harness: "claude".to_string(),
        cwd: None,
        transport: Some("pty".to_string()),
        presence: Some("online".to_string()),
        active: true,
    }
}

fn new_session(session_id: &str, name: &str) -> NewSession {
    NewSession {
        session_id: SessionId(session_id.to_string()),
        name: Some(name.to_string()),
        agent: Some("claude".to_string()),
        kind: "agent".to_string(),
        role: None,
        tier: "agent".to_string(),
        harness_session_id: None,
        client_key: Some(format!("ck_{session_id}")),
        cwd: None,
        project: PROJECT.to_string(),
        transport: Some("pty".to_string()),
    }
}

async fn seed_agent_runtime(store: &Store, agent_id: &str, name: &str, session_id: &str) {
    Agents::new(store)
        .create(new_agent(agent_id, name))
        .await
        .unwrap();
    AgentRuntimes::new(store)
        .create(new_runtime(session_id, agent_id))
        .await
        .unwrap();
    let session = SessionId(session_id.to_string());
    Sessions::new(store)
        .create(new_session(session_id, name))
        .await
        .unwrap();
    Sessions::new(store)
        .set_agent_id(&session, agent_id)
        .await
        .unwrap();
}

async fn assert_ambiguous_fossil_thread_member_fails_before_fanout(store: Arc<Store>) {
    seed_agent_runtime(&store, "a_sender", "sender", "s_sender").await;
    store
        .identity_conn()
        .execute("DROP INDEX idx_agents_name_unique", ())
        .await
        .unwrap();
    store
        .conn
        .execute("DROP INDEX idx_sessions_name", ())
        .await
        .unwrap();
    seed_agent_runtime(&store, "a_fossil_one", "fossil", "s_fossil_one").await;
    seed_agent_runtime(&store, "a_fossil_two", "fossil", "s_fossil_two").await;

    let thread_id = ThreadId("t_ambiguous_fossil".into());
    Threads::new(&store)
        .create(&thread_id, "ambiguous-fossil", PROJECT, "sender")
        .await
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO thread_members (thread_id, session_name, agent_id, joined_at) \
             VALUES (?1, 'sender', 'a_sender', 1), (?1, 'fossil', NULL, 2)",
            libsql::params![thread_id.0.clone()],
        )
        .await
        .unwrap();

    let realtime = Arc::new(MockRealtime::default());
    let bus = Bus::new(
        store.clone(),
        realtime.clone(),
        Arc::new(RejectResolveIdentity),
        Arc::new(NullSink),
    );
    let error = bus
        .send(
            &caller_with_agent("sender", "s_sender", "a_sender"),
            SendRequest {
                to: SendTarget::Post {
                    thread: "ambiguous-fossil".into(),
                },
                summary: None,
                body: "must not commit ambiguous fossil fanout".into(),
                mention: vec![],
                metadata: None,
                idempotency_key: Some("ambiguous-fossil-send".into()),
            },
        )
        .await
        .expect_err("a fossil alias must resolve globally and uniquely");

    assert_eq!(error.code, nexus_contracts::codes::INVALID_PARAMS);
    assert!(error.message.contains("ambiguous"));
    assert_eq!(realtime.count.load(Ordering::SeqCst), 0);
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
async fn thread_fossil_aliases_must_be_globally_unique_before_fanout() {
    assert_ambiguous_fossil_thread_member_fails_before_fanout(migrated_store().await).await;
}

#[tokio::test]
async fn split_thread_fossil_aliases_must_be_globally_unique_before_fanout() {
    let (_directory, store) = split_store().await;
    assert_ambiguous_fossil_thread_member_fails_before_fanout(store).await;
}

async fn assert_stable_thread_edge_never_alias_hops(store: Arc<Store>) {
    seed_agent_runtime(&store, "a_sender", "sender", "s_sender").await;
    Agents::new(&store)
        .create(NewAgent {
            agent_id: "a_offline_target".into(),
            project: PROJECT.into(),
            name: Some("current-offline-name".into()),
            default_harness: Some("claude".into()),
            role: None,
            tier: Some("agent".into()),
            owner: None,
        })
        .await
        .unwrap();
    seed_agent_runtime(
        &store,
        "a_alias_owner",
        "stale-target-alias",
        "s_alias_owner",
    )
    .await;
    let thread_id = ThreadId("t_stable_no_alias_hop".into());
    Threads::new(&store)
        .create(&thread_id, "stable-no-alias-hop", PROJECT, "sender")
        .await
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO thread_members (thread_id, session_name, agent_id, joined_at) \
             VALUES (?1, 'sender', 'a_sender', 1), \
                    (?1, 'stale-target-alias', 'a_offline_target', 2)",
            libsql::params![thread_id.0.clone()],
        )
        .await
        .unwrap();
    let realtime = Arc::new(MockRealtime::default());
    let bus = Bus::new(
        store.clone(),
        realtime.clone(),
        Arc::new(RejectResolveIdentity),
        Arc::new(NullSink),
    );

    let ack = bus
        .send(
            &caller_with_agent("sender", "s_sender", "a_sender"),
            SendRequest {
                to: SendTarget::Post {
                    thread: "stable-no-alias-hop".into(),
                },
                summary: None,
                body: "stable edge stays with its immutable owner".into(),
                mention: Vec::new(),
                metadata: None,
                idempotency_key: Some("stable-no-alias-hop".into()),
            },
        )
        .await
        .unwrap();

    assert_eq!(ack.fanout, Some(0));
    assert!(realtime.recipients().is_empty());
    let mut rows = store
        .conn
        .query(
            "SELECT COUNT(*) FROM in_flight WHERE message_id = ?1",
            libsql::params![ack.message_id.0],
        )
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
        0
    );
}

#[tokio::test]
async fn stable_thread_edge_never_falls_back_to_another_agents_alias() {
    assert_stable_thread_edge_never_alias_hops(migrated_store().await).await;
}

#[tokio::test]
async fn split_stable_thread_edge_never_falls_back_to_another_agents_alias() {
    let (_directory, store) = split_store().await;
    assert_stable_thread_edge_never_alias_hops(store).await;
}

#[tokio::test]
async fn split_authority_thread_post_composes_identity_and_transport_edges() {
    let (_directory, store) = split_store().await;
    seed_agent_runtime(&store, "a_split_sender", "split-sender", "s_split_sender").await;
    seed_agent_runtime(&store, "a_split_member", "split-member", "s_split_member").await;

    let threads = Threads::new(&store);
    let thread_id = ThreadId("t_split_thread".into());
    threads
        .create(&thread_id, "split-thread", PROJECT, "split-sender")
        .await
        .unwrap();
    threads
        .add_member(&thread_id, "split-sender")
        .await
        .unwrap();
    threads
        .add_member(&thread_id, "split-member")
        .await
        .unwrap();

    let realtime = Arc::new(MockRealtime::default());
    let bus = Bus::new(
        store,
        realtime.clone(),
        Arc::new(RejectResolveIdentity),
        Arc::new(NullSink),
    );
    let ack = bus
        .send(
            &caller_with_agent("split-sender", "s_split_sender", "a_split_sender"),
            SendRequest {
                to: SendTarget::Post {
                    thread: "split-thread".into(),
                },
                summary: None,
                body: "split stores must still fan out".into(),
                mention: vec![],
                metadata: None,
                idempotency_key: Some("split-thread-1".into()),
            },
        )
        .await
        .unwrap();

    assert_eq!(ack.fanout, Some(1));
    assert_eq!(
        realtime.recipients(),
        vec![SessionId("s_split_member".into())]
    );
}

#[tokio::test]
async fn split_authority_thread_join_writes_brief_without_daemon_fts() {
    let (_directory, store) = split_store().await;
    seed_agent_runtime(&store, "a_joiner", "joiner", "s_joiner").await;
    Threads::new(&store)
        .create(
            &ThreadId("t_join".into()),
            "join-thread",
            PROJECT,
            "operator",
        )
        .await
        .unwrap();

    let realtime = Arc::new(MockRealtime::default());
    let bus = Bus::new(
        store.clone(),
        realtime.clone(),
        Arc::new(RejectResolveIdentity),
        Arc::new(NullSink),
    );
    bus.join_thread(
        &caller_with_agent("joiner", "s_joiner", "a_joiner"),
        JoinThreadRequest {
            name: "join-thread".into(),
        },
    )
    .await
    .unwrap();

    assert!(Threads::new(&store)
        .is_member(&ThreadId("t_join".into()), "joiner")
        .await
        .unwrap());
    assert_eq!(realtime.recipients(), vec![SessionId("s_joiner".into())]);
}

#[tokio::test]
async fn split_authority_topic_publish_composes_identity_and_transport_edges() {
    let (_directory, store) = split_store().await;
    seed_agent_runtime(&store, "a_topic_sender", "topic-sender", "s_topic_sender").await;
    seed_agent_runtime(&store, "a_topic_member", "topic-member", "s_topic_member").await;

    let topics = Topics::new(&store);
    topics.ensure("split-topic", PROJECT).await.unwrap();
    topics
        .subscribe("split-topic", "s_topic_member", None)
        .await
        .unwrap();

    let realtime = Arc::new(MockRealtime::default());
    let bus = Bus::new(
        store,
        realtime.clone(),
        Arc::new(RejectResolveIdentity),
        Arc::new(NullSink),
    );
    let ack = bus
        .send(
            &caller_with_agent("topic-sender", "s_topic_sender", "a_topic_sender"),
            SendRequest {
                to: SendTarget::Publish {
                    topic: "split-topic".into(),
                },
                summary: None,
                body: "split topic fanout".into(),
                mention: vec![],
                metadata: None,
                idempotency_key: Some("split-topic-1".into()),
            },
        )
        .await
        .unwrap();

    assert_eq!(ack.fanout, Some(1));
    assert_eq!(
        realtime.recipients(),
        vec![SessionId("s_topic_member".into())]
    );
}

#[tokio::test]
async fn split_authority_reply_reads_transport_with_identity_resolved_separately() {
    let (_directory, store) = split_store().await;
    seed_agent_runtime(&store, "a_reply_sender", "reply-sender", "s_reply_sender").await;
    seed_agent_runtime(&store, "a_reply_member", "reply-member", "s_reply_member").await;

    let realtime = Arc::new(MockRealtime::default());
    let bus = Bus::new(
        store,
        realtime.clone(),
        Arc::new(RejectResolveIdentity),
        Arc::new(NullSink),
    );
    bus.send(
        &caller_with_agent("reply-sender", "s_reply_sender", "a_reply_sender"),
        SendRequest {
            to: SendTarget::Dm {
                name: None,
                agent_id: Some(AgentId("a_reply_member".into())),
            },
            summary: None,
            body: "first half".into(),
            mention: vec![],
            metadata: None,
            idempotency_key: Some("split-reply-inbound".into()),
        },
    )
    .await
    .unwrap();

    let ack = bus
        .send(
            &caller_with_agent("reply-member", "s_reply_member", "a_reply_member"),
            SendRequest {
                to: SendTarget::Reply,
                summary: None,
                body: "second half".into(),
                mention: vec![],
                metadata: None,
                idempotency_key: Some("split-reply-outbound".into()),
            },
        )
        .await
        .unwrap();

    assert_eq!(ack.fanout, Some(1));
    assert_eq!(
        realtime.recipients(),
        vec![
            SessionId("s_reply_member".into()),
            SessionId("s_reply_sender".into()),
        ]
    );
}

#[tokio::test]
async fn thread_post_fans_out_to_member_whose_runtime_is_stopped() {
    let store = migrated_store().await;
    seed_agent_runtime(&store, "a_sender", "sender", "s_sender").await;

    // "flappy": agents row + STOPPED runtime + session row WITHOUT agent_id stamped —
    // the exact shape the gate caught live (stale-heartbeat reconcile stopped CLI
    // listeners' runtime rows mid-run).
    Agents::new(&store)
        .create(new_agent("a_flappy", "flappy"))
        .await
        .unwrap();
    AgentRuntimes::new(&store)
        .create(new_runtime("s_flappy", "a_flappy"))
        .await
        .unwrap();
    AgentRuntimes::new(&store).stop("s_flappy").await.unwrap();
    Sessions::new(&store)
        .create(new_session("s_flappy", "flappy"))
        .await
        .unwrap();

    {
        let threads = Threads::new(&store);
        let tid = ThreadId("t_backend".into());
        threads
            .create(&tid, "backend", PROJECT, "sender")
            .await
            .unwrap();
        threads.add_member(&tid, "sender").await.unwrap();
        store
            .conn
            .execute(
                "INSERT INTO thread_members (thread_id, session_name, agent_id, joined_at) \
                 VALUES ('t_backend', 'flappy', 'a_flappy', 2)",
                (),
            )
            .await
            .unwrap();
    }

    // The identity mock does NOT know flappy: resolution must survive on store truth alone.
    let identity = Arc::new(MockIdentity {
        project: PROJECT.into(),
        names: HashMap::from([(
            "sender".to_string(),
            (
                SessionId("s_sender".to_string()),
                Some(AgentId("a_sender".to_string())),
            ),
        )]),
    });
    let realtime = Arc::new(MockRealtime::default());
    let bus = Bus::new(
        store.clone(),
        realtime.clone() as Arc<dyn DispatchPort>,
        identity as Arc<dyn IdentityPort>,
        Arc::new(NullSink) as Arc<dyn EventSink>,
    );

    let ack = bus
        .send(
            &caller_with_agent("sender", "s_sender", "a_sender"),
            SendRequest {
                to: SendTarget::Post {
                    thread: "backend".into(),
                },
                summary: None,
                body: "durable membership beats runtime flap".into(),
                mention: vec![],
                metadata: None,
                idempotency_key: None,
            },
        )
        .await
        .unwrap();

    assert_eq!(
        ack.fanout,
        Some(1),
        "a stopped runtime must not drop a durable member from fan-out"
    );
    assert_eq!(realtime.recipients(), vec![SessionId("s_flappy".into())]);
    let mut rows = store
        .conn
        .query(
            "SELECT state FROM in_flight WHERE recipient_session = 's_flappy'",
            (),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().expect("durable in_flight row");
    assert_eq!(row.get::<String>(0).unwrap(), "pending");
}

#[tokio::test]
async fn thread_post_batch_resolves_many_fossil_sessions_without_per_member_identity_calls() {
    let store = migrated_store().await;
    seed_agent_runtime(&store, "a_sender_batch", "sender-batch", "s_sender_batch").await;

    let threads = Threads::new(&store);
    let thread_id = ThreadId("t_batch_resolution".into());
    threads
        .create(&thread_id, "batch-resolution", PROJECT, "sender-batch")
        .await
        .unwrap();
    threads
        .add_member(&thread_id, "sender-batch")
        .await
        .unwrap();

    // Fossil rows deliberately have no durable agent id. They are still fully resolvable from the
    // sessions table and therefore belong in the one batch JOIN result.
    for index in 0..32 {
        let name = format!("fossil-{index}");
        let session_id = format!("s_fossil_{index}");
        Sessions::new(&store)
            .create(new_session(&session_id, &name))
            .await
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO thread_members (thread_id, session_name, agent_id, joined_at) \
                 VALUES (?1, ?2, NULL, ?3)",
                libsql::params![thread_id.0.clone(), name, index + 10],
            )
            .await
            .unwrap();
    }

    let realtime = Arc::new(MockRealtime::default());
    let bus = Bus::new(
        store.clone(),
        realtime.clone(),
        Arc::new(RejectResolveIdentity),
        Arc::new(NullSink),
    );

    let ack = bus
        .send(
            &caller_with_agent("sender-batch", "s_sender_batch", "a_sender_batch"),
            SendRequest {
                to: SendTarget::Post {
                    thread: "batch-resolution".into(),
                },
                summary: None,
                body: "one resolution query regardless of member count".into(),
                mention: vec![],
                metadata: None,
                idempotency_key: Some("batch-resolution-1".into()),
            },
        )
        .await
        .unwrap();

    assert_eq!(ack.fanout, Some(32));
    assert_eq!(realtime.recipients().len(), 32);
}

#[tokio::test]
async fn thread_post_stamps_the_threads_project_not_the_callers() {
    // Gate-caught 2026-07-10 (phase 4): the local operator (project "default") posted into a
    // "load"-project thread via the WS lane; the row was stamped with the CALLER's project, so
    // the fanned-out members — who drain project-scoped — could never receive it and the mail
    // aged to DLQ. The durable row must carry the conversation's project.
    let store = migrated_store().await;
    seed_agent_runtime(&store, "a_sender", "sender", "s_sender").await;
    seed_agent_runtime(&store, "a_member", "member", "s_member").await;

    {
        let threads = Threads::new(&store);
        let tid = ThreadId("t_scoped".into());
        threads
            .create(&tid, "scoped", PROJECT, "sender")
            .await
            .unwrap();
        threads.add_member(&tid, "sender").await.unwrap();
        threads.add_member(&tid, "member").await.unwrap();
    }

    let identity = Arc::new(MockIdentity {
        project: "other-project".into(),
        names: HashMap::new(),
    });
    let realtime = Arc::new(MockRealtime::default());
    let bus = Bus::new(
        store.clone(),
        realtime.clone() as Arc<dyn DispatchPort>,
        identity as Arc<dyn IdentityPort>,
        Arc::new(NullSink) as Arc<dyn EventSink>,
    );

    // Caller claims a DIFFERENT project than the thread's.
    let mut caller = caller_with_agent("sender", "s_sender", "a_sender");
    caller.project = "other-project".into();

    let request = SendRequest {
        to: SendTarget::Post {
            thread: "scoped".into(),
        },
        summary: None,
        body: "project follows the conversation".into(),
        mention: vec![],
        metadata: None,
        idempotency_key: Some("cross-project-thread-1".into()),
    };
    let first = bus.send(&caller, request.clone()).await.unwrap();
    let retry = bus.send(&caller, request).await.unwrap();
    assert_eq!(retry.message_id, first.message_id);

    let keyless = SendRequest {
        to: SendTarget::Post {
            thread: "scoped".into(),
        },
        summary: None,
        body: "keyless duplicate ignores project metadata".into(),
        mention: vec![],
        metadata: None,
        idempotency_key: None,
    };
    let keyless_first = bus.send(&caller, keyless.clone()).await.unwrap();
    let keyless_retry = bus.send(&caller, keyless).await.unwrap();
    assert_eq!(
        keyless_retry.message_id, keyless_first.message_id,
        "the legacy duplicate window must not use project metadata as identity"
    );

    let mut rows = store
        .conn
        .query(
            "SELECT project FROM messages WHERE message_id = ?1",
            libsql::params![first.message_id.0.clone()],
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().expect("message row");
    assert_eq!(
        row.get::<String>(0).unwrap(),
        PROJECT,
        "thread post must carry the THREAD's project so members can drain it"
    );
    let mut rows = store
        .conn
        .query("SELECT COUNT(*) FROM messages", ())
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
        2
    );
}

#[tokio::test]
async fn direct_dm_resolves_global_agent_when_projects_differ() {
    let store = migrated_store().await;
    seed_agent_runtime(
        &store,
        "a_sender_global",
        "sender-global",
        "s_sender_global",
    )
    .await;

    Agents::new(&store)
        .create(NewAgent {
            agent_id: "a_remote".into(),
            project: "descriptive-other-project".into(),
            name: Some("remote".into()),
            default_harness: Some("claude".into()),
            role: None,
            tier: None,
            owner: None,
        })
        .await
        .unwrap();
    AgentRuntimes::new(&store)
        .create(new_runtime("s_remote", "a_remote"))
        .await
        .unwrap();
    let mut remote_session = new_session("s_remote", "remote");
    remote_session.project = "descriptive-other-project".into();
    Sessions::new(&store).create(remote_session).await.unwrap();
    Sessions::new(&store)
        .set_agent_id(&SessionId("s_remote".into()), "a_remote")
        .await
        .unwrap();

    // The caller-scoped identity lookup intentionally knows nobody. Bus routing must fall back to
    // globally unique durable agent identity because project is metadata, not an ACL boundary.
    let identity = Arc::new(MockIdentity {
        project: PROJECT.into(),
        names: HashMap::new(),
    });
    let realtime = Arc::new(MockRealtime::default());
    let bus = Bus::new(
        store.clone(),
        realtime.clone() as Arc<dyn DispatchPort>,
        identity,
        Arc::new(NullSink),
    );

    let ack = bus
        .send(
            &caller_with_agent("sender-global", "s_sender_global", "a_sender_global"),
            SendRequest {
                to: SendTarget::Dm {
                    name: Some("remote".into()),
                    agent_id: None,
                },
                summary: None,
                body: "project is metadata".into(),
                mention: vec![],
                metadata: None,
                idempotency_key: Some("cross-project-dm-1".into()),
            },
        )
        .await
        .unwrap();

    assert_eq!(ack.fanout, Some(1));
    assert_eq!(realtime.recipients(), vec![SessionId("s_remote".into())]);
}

#[tokio::test]
async fn direct_dm_agent_id_reaches_an_unnamed_agent() {
    let store = migrated_store().await;
    seed_agent_runtime(&store, "a_sender_named", "sender-named", "s_sender_named").await;
    Agents::new(&store)
        .create(NewAgent {
            agent_id: "a_unnamed".into(),
            project: "metadata-only".into(),
            name: None,
            default_harness: Some("codex".into()),
            role: None,
            tier: None,
            owner: None,
        })
        .await
        .unwrap();
    AgentRuntimes::new(&store)
        .create(new_runtime("s_unnamed", "a_unnamed"))
        .await
        .unwrap();
    Sessions::new(&store)
        .create(NewSession {
            session_id: SessionId("s_unnamed".into()),
            name: None,
            agent: Some("codex".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: Some("codex-unnamed".into()),
            client_key: Some("ck_unnamed".into()),
            cwd: None,
            project: "metadata-only".into(),
            transport: Some("codex-appserver".into()),
        })
        .await
        .unwrap();
    Sessions::new(&store)
        .set_agent_id(&SessionId("s_unnamed".into()), "a_unnamed")
        .await
        .unwrap();

    let identity = Arc::new(MockIdentity {
        project: PROJECT.into(),
        names: HashMap::new(),
    });
    let realtime = Arc::new(MockRealtime::default());
    let bus = Bus::new(
        store,
        realtime.clone() as Arc<dyn DispatchPort>,
        identity,
        Arc::new(NullSink),
    );

    let ack = bus
        .send(
            &caller_with_agent("sender-named", "s_sender_named", "a_sender_named"),
            SendRequest {
                to: SendTarget::Dm {
                    name: None,
                    agent_id: Some(AgentId("a_unnamed".into())),
                },
                summary: None,
                body: "identity is enough".into(),
                mention: vec![],
                metadata: None,
                idempotency_key: Some("unnamed-agent-dm-1".into()),
            },
        )
        .await
        .unwrap();

    assert_eq!(ack.fanout, Some(1));
    assert_eq!(realtime.recipients(), vec![SessionId("s_unnamed".into())]);
}

#[tokio::test]
async fn direct_dm_agent_id_ignores_mismatched_display_name_for_routing() {
    let store = migrated_store().await;
    seed_agent_runtime(&store, "a_sender_id", "sender-id", "s_sender_id").await;
    seed_agent_runtime(&store, "a_actual", "actual", "s_actual").await;
    seed_agent_runtime(&store, "a_imposter", "imposter", "s_imposter").await;

    let identity = Arc::new(MockIdentity {
        project: PROJECT.into(),
        names: HashMap::from([(
            "imposter".to_string(),
            (
                SessionId("s_imposter".into()),
                Some(AgentId("a_imposter".into())),
            ),
        )]),
    });
    let realtime = Arc::new(MockRealtime::default());
    let bus = Bus::new(
        store,
        realtime.clone() as Arc<dyn DispatchPort>,
        identity,
        Arc::new(NullSink),
    );

    let ack = bus
        .send(
            &caller_with_agent("sender-id", "s_sender_id", "a_sender_id"),
            SendRequest {
                to: SendTarget::Dm {
                    name: Some("imposter".into()),
                    agent_id: Some(AgentId("a_actual".into())),
                },
                summary: None,
                body: "stable identity wins".into(),
                mention: vec![],
                metadata: None,
                idempotency_key: Some("agent-id-wins-1".into()),
            },
        )
        .await
        .unwrap();

    assert_eq!(ack.fanout, Some(1));
    assert_eq!(realtime.recipients(), vec![SessionId("s_actual".into())]);
}

#[tokio::test]
async fn positional_dm_token_prefers_an_exact_agent_id_over_a_colliding_alias() {
    let store = migrated_store().await;
    seed_agent_runtime(
        &store,
        "a_sender_prefix",
        "sender-prefix",
        "s_sender_prefix",
    )
    .await;
    seed_agent_runtime(&store, "a_team", "id-owner", "s_id_owner").await;
    seed_agent_runtime(&store, "a_named_team", "a_team", "s_named_team").await;

    let identity = Arc::new(MockIdentity {
        project: PROJECT.into(),
        names: HashMap::from([(
            "a_team".to_string(),
            (
                SessionId("s_named_team".into()),
                Some(AgentId("a_named_team".into())),
            ),
        )]),
    });
    let realtime = Arc::new(MockRealtime::default());
    let bus = Bus::new(
        store,
        realtime.clone() as Arc<dyn DispatchPort>,
        identity,
        Arc::new(NullSink),
    );

    let ack = bus
        .send(
            &caller_with_agent("sender-prefix", "s_sender_prefix", "a_sender_prefix"),
            SendRequest {
                to: SendTarget::dm_name("a_team"),
                summary: None,
                body: "the stable id wins over its alias collision".into(),
                mention: vec![],
                metadata: None,
                idempotency_key: Some("prefixed-name-dm-1".into()),
            },
        )
        .await
        .unwrap();

    assert_eq!(ack.fanout, Some(1));
    assert_eq!(realtime.recipients(), vec![SessionId("s_id_owner".into())]);
}

#[tokio::test]
async fn direct_dm_without_name_or_agent_id_fails_before_any_write() {
    let store = migrated_store().await;
    seed_agent_runtime(&store, "a_sender_empty", "sender-empty", "s_sender_empty").await;
    let identity = Arc::new(MockIdentity {
        project: PROJECT.into(),
        names: HashMap::new(),
    });
    let realtime = Arc::new(MockRealtime::default());
    let bus = Bus::new(
        store.clone(),
        realtime.clone() as Arc<dyn DispatchPort>,
        identity,
        Arc::new(NullSink),
    );

    let error = bus
        .send(
            &caller_with_agent("sender-empty", "s_sender_empty", "a_sender_empty"),
            SendRequest {
                to: SendTarget::Dm {
                    name: None,
                    agent_id: None,
                },
                summary: None,
                body: "must not persist".into(),
                mention: vec![],
                metadata: None,
                idempotency_key: None,
            },
        )
        .await
        .unwrap_err();

    assert_eq!(error.code, nexus_contracts::codes::INVALID_PARAMS);
    assert_eq!(realtime.count.load(Ordering::SeqCst), 0);
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
