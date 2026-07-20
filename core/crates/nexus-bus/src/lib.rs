//! # nexus-bus
//!
//! The Nexus **bus** (backend spec §3): the one `to` routing contract — **DM | thread | topic** —
//! with pure-lookup resolution, policy gating, single-transaction fan-out, and the [`BusPort`]
//! service. There is **no orchestrator**: [`Router::resolve`] is a pure registry lookup, policy
//! decides allow/deny only, delivery is fan-out, and nothing reorders or inserts into the message
//! path. `--mention` is a stored soft highlight, not a route.
//!
//! - [`Router`] — resolve a [`nexus_contracts::send::SendTarget`] into a delivery set.
//! - [`Bus`] — `impl BusPort`: `send` (resolve → policy check → one `messages` row + N `in_flight`
//!   rows in one transaction → ring bells → emit a [`nexus_contracts::events::WsEvent`]) plus thread
//!   and topic management.
//!
//! Core invariants (asserted in tests, backend §3/§11): a DM resolves to exactly one recipient
//! enqueue and never enters a third agent's context; a thread post writes one message row and fans
//! out one `in_flight` per member atomically; an unknown `to` is an explicit `NotFound` (never a
//! silent drop); a `reply` targets the caller's last inbound scope.
//!
//! [`BusPort`]: nexus_contracts::ports::BusPort

pub mod broadcast_messages;
mod dm;
mod error;
mod policy;
pub mod router;
pub mod service;
mod sql_batch;
mod thread;
mod topic;

pub use router::Router;
pub use service::Bus;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;

    use nexus_contracts::ack::{AckRequest, AckResponse, AckThreadsRequest};
    use nexus_contracts::batch::{ConsumeRequest, NexusBatch};
    use nexus_contracts::enums::Tier;
    use nexus_contracts::events::WsEvent;
    use nexus_contracts::ids::{AgentId, MessageId, SessionId};
    use nexus_contracts::ports::BusPort;
    use nexus_contracts::ports::{
        Caller, ContractError, DispatchPort, EventSink, IdentityPort, PortResult,
    };
    use nexus_contracts::register::{
        HeartbeatResponse, MemberListRequest, MemberListResponse, RegisterRequest,
        RegisterResponse, StatusRequest, StatusResponse, Whoami,
    };
    use nexus_contracts::rpc::codes;
    use nexus_contracts::send::{SendRequest, SendTarget};
    use nexus_store::repos::{
        AgentRuntimes, Agents, Inbox, NewAgent, NewAgentRuntime, NewSession, Sessions, Threads,
        Topics,
    };
    use nexus_store::{DaemonStore, Store};

    use super::*;

    // ---- Mock DispatchPort: counts enqueues, records (recipient, message) pairs. ----
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
            unreachable!("not used by bus tests")
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
        fn enqueue_count(&self) -> usize {
            self.count.load(Ordering::SeqCst)
        }
        fn recipients(&self) -> Vec<SessionId> {
            self.enqueued
                .lock()
                .unwrap()
                .iter()
                .map(|(r, _)| r.clone())
                .collect()
        }
    }

    // ---- Mock IdentityPort: resolves name → SessionId from a fixed map. ----
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
        async fn status(
            &self,
            _caller: &Caller,
            _req: StatusRequest,
        ) -> PortResult<StatusResponse> {
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

    // ---- Null EventSink: drops events (web console broadcast not under test here). ----
    struct NullSink;
    #[async_trait]
    impl EventSink for NullSink {
        async fn emit(&self, _event: WsEvent) {}
    }

    const PROJECT: &str = "p_demo";

    async fn migrated_store() -> Arc<Store> {
        let s = Store::open(":memory:").await.unwrap();
        s.migrate().await.unwrap();
        Arc::new(s)
    }

    async fn split_store() -> Arc<Store> {
        let stores = DaemonStore::open(":memory:").await.unwrap();
        Arc::new(stores.compatibility_store())
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

    async fn seed_legacy_sessions(store: &Store, pairs: &[(&str, &str)]) {
        let sessions = Sessions::new(store);
        for (name, session_id) in pairs {
            sessions
                .create(new_session(session_id, name))
                .await
                .unwrap();
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

    fn identity(pairs: &[(&str, &str)]) -> Arc<MockIdentity> {
        let names = pairs
            .iter()
            .map(|(n, s)| (n.to_string(), (SessionId(s.to_string()), None)))
            .collect();
        Arc::new(MockIdentity {
            project: PROJECT.into(),
            names,
        })
    }

    fn identity_with_agents(pairs: &[(&str, &str, &str)]) -> Arc<MockIdentity> {
        let names = pairs
            .iter()
            .map(|(n, s, a)| {
                (
                    n.to_string(),
                    (SessionId(s.to_string()), Some(AgentId(a.to_string()))),
                )
            })
            .collect();
        Arc::new(MockIdentity {
            project: PROJECT.into(),
            names,
        })
    }

    fn build(store: Arc<Store>, identity: Arc<MockIdentity>) -> (Bus, Arc<MockRealtime>) {
        let realtime = Arc::new(MockRealtime::default());
        let bus = Bus::new(
            store,
            realtime.clone() as Arc<dyn DispatchPort>,
            identity as Arc<dyn IdentityPort>,
            Arc::new(NullSink) as Arc<dyn EventSink>,
        );
        (bus, realtime)
    }

    fn build_with_clock(
        store: Arc<Store>,
        identity: Arc<MockIdentity>,
        now_fn: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> (Bus, Arc<MockRealtime>) {
        let realtime = Arc::new(MockRealtime::default());
        let bus = Bus::new_with_clock(
            store,
            realtime.clone() as Arc<dyn DispatchPort>,
            identity as Arc<dyn IdentityPort>,
            Arc::new(NullSink) as Arc<dyn EventSink>,
            now_fn,
        );
        (bus, realtime)
    }

    #[tokio::test]
    async fn dm_resolves_to_two_party_and_enqueues_one_recipient() {
        let store = migrated_store().await;
        let id = identity(&[("ben", "s_ben")]);
        let (bus, realtime) = build(store, id);

        let ack = bus
            .send(
                &caller("etan", "s_etan"),
                SendRequest {
                    to: SendTarget::dm_name("ben"),
                    summary: None,
                    body: "take the auth refactor?".into(),
                    mention: vec![],
                    metadata: None,
                    idempotency_key: None,
                },
            )
            .await
            .unwrap();

        assert_eq!(ack.fanout, Some(1), "DM fan-out counts the recipient");
        assert_eq!(realtime.enqueue_count(), 1, "exactly one recipient enqueue");
        assert_eq!(realtime.recipients(), vec![SessionId("s_ben".into())]);
    }

    #[tokio::test]
    async fn send_rejects_empty_or_whitespace_body_before_writing() {
        let store = migrated_store().await;
        let id = identity(&[("ben", "s_ben")]);
        let (bus, realtime) = build(store.clone(), id);

        for body in ["", " \n\t "] {
            let err = bus
                .send(
                    &caller("etan", "s_etan"),
                    SendRequest {
                        to: SendTarget::dm_name("ben"),
                        summary: None,
                        body: body.into(),
                        mention: vec![],
                        metadata: None,
                        idempotency_key: None,
                    },
                )
                .await
                .unwrap_err();
            assert_eq!(err.code, codes::INVALID_PARAMS);
            assert!(err.message.contains("body"));
        }

        assert_eq!(message_count(&store).await, 0);
        assert_eq!(realtime.enqueue_count(), 0);
    }

    #[tokio::test]
    async fn dm_idempotency_key_returns_original_message_without_second_insert() {
        let store = migrated_store().await;
        let id = identity(&[("ben", "s_ben")]);
        let fixed_now = Arc::new(|| 1_700_000_000);
        let (bus, realtime) = build_with_clock(store.clone(), id, fixed_now);

        let req = SendRequest {
            to: SendTarget::dm_name("ben"),
            summary: None,
            body: "retry once".into(),
            mention: vec![],
            metadata: None,
            idempotency_key: Some("web:dm:ben:client-1".into()),
        };
        let first = bus
            .send(&caller("etan", "s_etan"), req.clone())
            .await
            .unwrap();
        let retry = bus.send(&caller("etan", "s_etan"), req).await.unwrap();

        assert_eq!(retry, first, "retry returns the original ack");
        assert_eq!(
            realtime.enqueue_count(),
            1,
            "retry must not ring a second inbox bell"
        );
        assert_eq!(message_count(&store).await, 1, "one durable message row");
    }

    #[tokio::test]
    async fn duplicate_dm_body_inside_short_window_returns_original_message() {
        let store = migrated_store().await;
        let id = identity(&[("ben", "s_ben")]);
        let now = Arc::new(std::sync::atomic::AtomicI64::new(1_700_000_000));
        let now_fn = {
            let now = now.clone();
            Arc::new(move || now.load(std::sync::atomic::Ordering::SeqCst))
        };
        let (bus, realtime) = build_with_clock(store.clone(), id, now_fn);

        let req = SendRequest {
            to: SendTarget::dm_name("ben"),
            summary: None,
            body: "same click twice".into(),
            mention: vec![],
            metadata: None,
            idempotency_key: None,
        };
        let first = bus
            .send(&caller("etan", "s_etan"), req.clone())
            .await
            .unwrap();
        now.store(1_700_001_999, std::sync::atomic::Ordering::SeqCst);
        let retry = bus.send(&caller("etan", "s_etan"), req).await.unwrap();

        assert_eq!(retry, first, "legacy duplicate returns the original ack");
        assert_eq!(
            realtime.enqueue_count(),
            1,
            "legacy duplicate must not enqueue a second delivery"
        );
        assert_eq!(message_count(&store).await, 1, "one durable message row");
    }

    #[tokio::test]
    async fn duplicate_dm_body_after_short_window_creates_new_message() {
        let store = migrated_store().await;
        let id = identity(&[("ben", "s_ben")]);
        let now = Arc::new(std::sync::atomic::AtomicI64::new(1_700_000_000));
        let now_fn = {
            let now = now.clone();
            Arc::new(move || now.load(std::sync::atomic::Ordering::SeqCst))
        };
        let (bus, realtime) = build_with_clock(store.clone(), id, now_fn);

        let req = SendRequest {
            to: SendTarget::dm_name("ben"),
            summary: None,
            body: "same click after debounce".into(),
            mention: vec![],
            metadata: None,
            idempotency_key: None,
        };
        let first = bus
            .send(&caller("etan", "s_etan"), req.clone())
            .await
            .unwrap();
        now.store(1_700_002_001, std::sync::atomic::Ordering::SeqCst);
        let second = bus.send(&caller("etan", "s_etan"), req).await.unwrap();

        assert_ne!(
            second.message_id, first.message_id,
            "legacy duplicate window expires under the injected clock"
        );
        assert_eq!(realtime.enqueue_count(), 2);
        assert_eq!(message_count(&store).await, 2);
    }

    #[tokio::test]
    async fn different_idempotency_keys_do_not_collapse_same_body_sends() {
        let store = migrated_store().await;
        let id = identity(&[("ben", "s_ben")]);
        let (bus, realtime) = build(store.clone(), id);

        let first = SendRequest {
            to: SendTarget::dm_name("ben"),
            summary: None,
            body: "same body intentional".into(),
            mention: vec![],
            metadata: None,
            idempotency_key: Some("web:dm:ben:first".into()),
        };
        let mut second = first.clone();
        second.idempotency_key = Some("web:dm:ben:second".into());

        let first_ack = bus.send(&caller("etan", "s_etan"), first).await.unwrap();
        let second_ack = bus.send(&caller("etan", "s_etan"), second).await.unwrap();

        assert_ne!(
            second_ack.message_id, first_ack.message_id,
            "explicitly different producer keys are distinct sends"
        );
        assert_eq!(realtime.enqueue_count(), 2);
        assert_eq!(message_count(&store).await, 2);
    }

    #[tokio::test]
    async fn dm_stores_stable_sender_and_recipient_identity() {
        let store = migrated_store().await;
        seed_agent_runtime(&store, "a_ben", "ben", "s_ben").await;
        let id = identity_with_agents(&[("ben", "s_ben", "a_ben")]);
        let (bus, realtime) = build(store.clone(), id);

        bus.send(
            &caller_with_agent("etan", "s_etan", "a_etan"),
            SendRequest {
                to: SendTarget::dm_name("ben"),
                summary: None,
                body: "stable ids".into(),
                mention: vec![],
                metadata: None,
                idempotency_key: None,
            },
        )
        .await
        .unwrap();

        let mut rows = store
            .conn
            .query(
                "SELECT from_agent_id, to_agent_id, to_name FROM messages WHERE kind = 'dm'",
                (),
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().expect("message row");
        assert_eq!(row.get::<String>(0).unwrap(), "a_etan");
        assert_eq!(row.get::<String>(1).unwrap(), "a_ben");
        assert_eq!(row.get::<String>(2).unwrap(), "ben");

        let mut rows = store
            .conn
            .query(
                "SELECT recipient_session, recipient_agent_id FROM in_flight",
                (),
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().expect("in-flight row");
        assert_eq!(row.get::<String>(0).unwrap(), "s_ben");
        assert_eq!(row.get::<String>(1).unwrap(), "a_ben");
        assert_eq!(realtime.recipients(), vec![SessionId("s_ben".into())]);
    }

    async fn message_count(store: &Store) -> i64 {
        let mut rows = store
            .conn
            .query("SELECT COUNT(*) FROM messages", ())
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        row.get(0).unwrap()
    }

    async fn queued_bodies_for(store: &Store, session: &str) -> Vec<String> {
        let mut rows = store
            .conn
            .query(
                "SELECT m.body FROM in_flight f JOIN messages m ON m.message_id = f.message_id \
                 WHERE f.recipient_session = ?1 ORDER BY m.created_at, m.message_id",
                libsql::params![session],
            )
            .await
            .unwrap();
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.unwrap() {
            out.push(row.get(0).unwrap());
        }
        out
    }

    #[tokio::test]
    async fn thread_post_fans_out_to_members_except_the_sender() {
        let store = migrated_store().await;
        // 3-member thread: ben, dylan, ana.
        {
            let threads = Threads::new(&store);
            let tid = nexus_contracts::ids::ThreadId("t_backend".into());
            threads
                .create(&tid, "backend", PROJECT, "ben")
                .await
                .unwrap();
            for m in ["ben", "dylan", "ana"] {
                threads.add_member(&tid, m).await.unwrap();
            }
        }
        seed_legacy_sessions(
            &store,
            &[("ben", "s_ben"), ("dylan", "s_dylan"), ("ana", "s_ana")],
        )
        .await;
        let id = identity(&[("ben", "s_ben"), ("dylan", "s_dylan"), ("ana", "s_ana")]);
        let (bus, realtime) = build(store.clone(), id);

        let ack = bus
            .send(
                &caller("ben", "s_ben"),
                SendRequest {
                    to: SendTarget::Post {
                        thread: "backend".into(),
                    },
                    summary: Some("plan".into()),
                    body: "step 1 ...".into(),
                    mention: vec!["dylan".into()],
                    metadata: None,
                    idempotency_key: None,
                },
            )
            .await
            .unwrap();

        // ben is the sender AND a member, but a sender is never delivered its own post → {dylan, ana}.
        assert_eq!(
            ack.fanout,
            Some(2),
            "fan-out to the 2 OTHER members, not the sender"
        );
        assert_eq!(
            realtime.enqueue_count(),
            2,
            "2 in_flight enqueues — the sender is excluded"
        );
        let recips = realtime.recipients();
        assert!(
            !recips.contains(&SessionId("s_ben".into())),
            "the sender (ben) must never be a recipient of its own thread post: {recips:?}"
        );
        // Exactly one durable messages row was written.
        let mut rows = store
            .conn
            .query("SELECT COUNT(*) FROM messages", ())
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        let n: i64 = row.get(0).unwrap();
        assert_eq!(n, 1, "one messages row even on fan-out");
    }

    #[tokio::test]
    async fn thread_post_uses_member_agent_id_when_member_name_lookup_is_stale() {
        let store = migrated_store().await;
        seed_agent_runtime(&store, "a_sender", "sender", "s_sender").await;
        seed_agent_runtime(&store, "a_original", "member-current", "s_original").await;
        seed_agent_runtime(&store, "a_imposter", "member-old", "s_imposter").await;

        {
            let threads = Threads::new(&store);
            let tid = nexus_contracts::ids::ThreadId("t_backend".into());
            threads
                .create(&tid, "backend", PROJECT, "sender")
                .await
                .unwrap();
            threads.add_member(&tid, "sender").await.unwrap();
            store
                .conn
                .execute(
                    "INSERT INTO thread_members (thread_id, session_name, agent_id, joined_at) \
                     VALUES ('t_backend', 'member-old', 'a_original', 2)",
                    (),
                )
                .await
                .unwrap();
        }

        let id = identity_with_agents(&[
            ("sender", "s_sender", "a_sender"),
            ("member-old", "s_imposter", "a_imposter"),
        ]);
        let (bus, realtime) = build(store.clone(), id);

        let ack = bus
            .send(
                &caller_with_agent("sender", "s_sender", "a_sender"),
                SendRequest {
                    to: SendTarget::Post {
                        thread: "backend".into(),
                    },
                    summary: None,
                    body: "route by member id".into(),
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
            "stable member identity should fan out even when the name resolver is stale"
        );
        assert_eq!(realtime.recipients(), vec![SessionId("s_original".into())]);

        let mut rows = store
            .conn
            .query(
                "SELECT recipient_agent_id FROM in_flight WHERE recipient_session = 's_original'",
                (),
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        assert_eq!(row.get::<String>(0).unwrap(), "a_original");
    }

    #[tokio::test]
    async fn topic_publish_uses_subscriber_agent_id_after_runtime_replacement() {
        let store = migrated_store().await;
        seed_agent_runtime(&store, "a_sender", "sender", "s_sender").await;
        seed_agent_runtime(&store, "a_subscriber", "subscriber", "s_subscriber_old").await;

        {
            let topics = Topics::new(&store);
            topics.ensure("deploys", PROJECT).await.unwrap();
            topics
                .subscribe("deploys", "s_subscriber_old", None)
                .await
                .unwrap();
        }
        AgentRuntimes::new(&store)
            .stop("s_subscriber_old")
            .await
            .unwrap();
        AgentRuntimes::new(&store)
            .create(new_runtime("s_subscriber_new", "a_subscriber"))
            .await
            .unwrap();
        Sessions::new(&store)
            .rebind_name_to_session(new_session("s_subscriber_new", "subscriber"))
            .await
            .unwrap();

        let id = identity_with_agents(&[("sender", "s_sender", "a_sender")]);
        let (bus, realtime) = build(store.clone(), id);

        let ack = bus
            .send(
                &caller_with_agent("sender", "s_sender", "a_sender"),
                SendRequest {
                    to: SendTarget::Publish {
                        topic: "deploys".into(),
                    },
                    summary: None,
                    body: "ship it".into(),
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
            "stable subscription identity should follow the active replacement runtime"
        );
        assert_eq!(
            realtime.recipients(),
            vec![SessionId("s_subscriber_new".into())]
        );

        let mut rows = store
            .conn
            .query(
                "SELECT recipient_agent_id FROM in_flight \
                 WHERE recipient_session = 's_subscriber_new'",
                (),
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        assert_eq!(row.get::<String>(0).unwrap(), "a_subscriber");
    }

    #[tokio::test]
    async fn topic_publish_persists_subscriber_agent_id_for_offline_subscription() {
        let store = migrated_store().await;
        seed_agent_runtime(&store, "a_sender", "sender", "s_sender").await;
        Agents::new(&store)
            .create(new_agent("a_subscriber", "subscriber"))
            .await
            .unwrap();
        let subscriber_session = SessionId("s_subscriber_legacy".into());
        Sessions::new(&store)
            .create(new_session(&subscriber_session.0, "subscriber"))
            .await
            .unwrap();
        Sessions::new(&store)
            .set_agent_id(&subscriber_session, "a_subscriber")
            .await
            .unwrap();

        {
            let topics = Topics::new(&store);
            topics.ensure("deploys", PROJECT).await.unwrap();
            store
                .conn
                .execute(
                    "INSERT INTO subscriptions (topic, subscriber_session, subscriber_agent_id, \
                     cursor, subscribed_at) VALUES ('deploys', 's_subscriber_legacy', \
                     'a_subscriber', 0, 1)",
                    (),
                )
                .await
                .unwrap();
        }

        let id = identity_with_agents(&[("sender", "s_sender", "a_sender")]);
        let (bus, _realtime) = build(store.clone(), id);

        bus.send(
            &caller_with_agent("sender", "s_sender", "a_sender"),
            SendRequest {
                to: SendTarget::Publish {
                    topic: "deploys".into(),
                },
                summary: None,
                body: "wake when you return".into(),
                mention: vec![],
                metadata: None,
                idempotency_key: None,
            },
        )
        .await
        .unwrap();

        let mut rows = store
            .conn
            .query(
                "SELECT COALESCE(recipient_agent_id, '') FROM in_flight \
                 WHERE recipient_session = 's_subscriber_legacy'",
                (),
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        assert_eq!(row.get::<String>(0).unwrap(), "a_subscriber");
    }

    #[tokio::test]
    async fn thread_join_delivers_one_brief_never_prior_posts() {
        let store = migrated_store().await;
        {
            let threads = Threads::new(&store);
            let tid = nexus_contracts::ids::ThreadId("t_backend".into());
            threads
                .create(&tid, "backend", PROJECT, "ben")
                .await
                .unwrap();
            threads.add_member(&tid, "ben").await.unwrap();
        }
        seed_legacy_sessions(
            &store,
            &[("ben", "s_ben"), ("ana", "s_ana"), ("dylan", "s_dylan")],
        )
        .await;
        let id = identity(&[("ben", "s_ben"), ("ana", "s_ana"), ("dylan", "s_dylan")]);
        let (bus, realtime) = build(store.clone(), id);

        bus.send(
            &caller("ben", "s_ben"),
            SendRequest {
                to: SendTarget::Post {
                    thread: "backend".into(),
                },
                summary: None,
                body: "before anyone else joined".into(),
                mention: vec![],
                metadata: None,
                idempotency_key: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(
            realtime.enqueue_count(),
            0,
            "the original post has no other current members to fan out to"
        );

        bus.join_thread(
            &caller("ana", "s_ana"),
            nexus_contracts::threads::JoinThreadRequest {
                name: "backend".into(),
            },
        )
        .await
        .unwrap();
        bus.add_thread_member(
            &caller("ben", "s_ben"),
            nexus_contracts::threads::ThreadMemberRequest {
                name: "backend".into(),
                member: "dylan".into(),
            },
        )
        .await
        .unwrap();

        // Join delivers one brief (tail + how to pull history). Prior posts are never
        // re-delivered because full-history backfill can head-of-line block live traffic.
        let ana_bodies = queued_bodies_for(&store, "s_ana").await;
        assert_eq!(
            ana_bodies.len(),
            1,
            "exactly one join brief: {ana_bodies:?}"
        );
        assert!(
            ana_bodies[0].contains("You joined thread \"backend\" (1 prior posts)"),
            "brief names the thread and prior-post count: {}",
            ana_bodies[0]
        );
        assert!(
            ana_bodies[0].contains("[ben] before anyone else joined"),
            "brief carries the tail excerpt: {}",
            ana_bodies[0]
        );
        assert!(
            ana_bodies[0].contains("nexus history --thread backend"),
            "brief tells the joiner how to pull history: {}",
            ana_bodies[0]
        );
        assert!(
            ana_bodies[0].contains("nexus search"),
            "brief tells the joiner how to search: {}",
            ana_bodies[0]
        );
        let dylan_bodies = queued_bodies_for(&store, "s_dylan").await;
        assert_eq!(
            dylan_bodies.len(),
            1,
            "operator add-member also gets exactly one brief: {dylan_bodies:?}"
        );
        assert_eq!(
            realtime.recipients(),
            vec![SessionId("s_ana".into()), SessionId("s_dylan".into())],
            "the brief rings exactly the newly added members"
        );

        bus.join_thread(
            &caller("ana", "s_ana"),
            nexus_contracts::threads::JoinThreadRequest {
                name: "backend".into(),
            },
        )
        .await
        .unwrap();
        assert_eq!(
            queued_bodies_for(&store, "s_ana").await.len(),
            1,
            "repeat joins are idempotent — no second brief"
        );
        assert_eq!(
            realtime.recipients(),
            vec![SessionId("s_ana".into()), SessionId("s_dylan".into())],
            "repeat joins do not re-ring"
        );

        let mut rows = store
            .conn
            .query(
                "SELECT m.message_id, m.body FROM messages_fts f \
                 JOIN messages m ON m.rowid = f.rowid \
                 WHERE messages_fts MATCH 'history'",
                (),
            )
            .await
            .unwrap();
        let first = rows.next().await.unwrap().expect("ana join brief fts");
        assert!(
            first
                .get::<String>(1)
                .unwrap()
                .contains("You joined thread \"backend\""),
            "FTS row must map to the join-brief message body"
        );
        let second = rows.next().await.unwrap().expect("dylan join brief fts");
        assert!(
            second
                .get::<String>(1)
                .unwrap()
                .contains("You joined thread \"backend\""),
            "FTS row must map to the join-brief message body"
        );
        assert!(rows.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn generic_to_prefers_thread_over_same_named_agent() {
        let store = migrated_store().await;
        {
            let threads = Threads::new(&store);
            let tid = nexus_contracts::ids::ThreadId("t_backend".into());
            threads
                .create(&tid, "backend", PROJECT, "ben")
                .await
                .unwrap();
            for m in ["ben", "dylan"] {
                threads.add_member(&tid, m).await.unwrap();
            }
        }
        seed_legacy_sessions(&store, &[("ben", "s_ben"), ("dylan", "s_dylan")]).await;
        let id = identity(&[
            ("ben", "s_ben"),
            ("dylan", "s_dylan"),
            ("backend", "s_backend"),
        ]);
        let (bus, realtime) = build(store, id);

        let ack = bus
            .send(
                &caller("ben", "s_ben"),
                SendRequest {
                    to: SendTarget::dm_name("backend"),
                    summary: None,
                    body: "thread wins".into(),
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
            "generic to=backend must route to the thread"
        );
        assert_eq!(realtime.recipients(), vec![SessionId("s_dylan".into())]);
    }

    #[tokio::test]
    async fn dm_never_enters_third_agents_context() {
        let store = migrated_store().await;
        let id = identity(&[("ben", "s_ben"), ("carol", "s_carol")]);
        let (bus, realtime) = build(store, id);

        // a (etan) DMs b (ben); c (carol) must get no in_flight.
        bus.send(
            &caller("etan", "s_etan"),
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

        let recipients = realtime.recipients();
        assert_eq!(recipients, vec![SessionId("s_ben".into())]);
        assert!(
            !recipients.contains(&SessionId("s_carol".into())),
            "DM never enters a third agent's context (context hygiene)"
        );
    }

    #[tokio::test]
    async fn unknown_to_is_explicit_error_not_silent() {
        let store = migrated_store().await;
        let id = identity(&[("ben", "s_ben")]);
        let (bus, realtime) = build(store, id);

        let err = bus
            .send(
                &caller("etan", "s_etan"),
                SendRequest {
                    to: SendTarget::dm_name("ghost"),
                    summary: None,
                    body: "anyone?".into(),
                    mention: vec![],
                    metadata: None,
                    idempotency_key: None,
                },
            )
            .await
            .expect_err("unknown name must error");
        assert_eq!(err.code, nexus_contracts::codes::NOT_FOUND);
        assert_eq!(
            realtime.enqueue_count(),
            0,
            "nothing enqueued on a bad resolve"
        );
    }

    #[tokio::test]
    async fn reply_targets_current_conversation() {
        let store = migrated_store().await;
        let id = identity(&[("ben", "s_ben"), ("etan", "s_etan")]);
        let (bus, realtime) = build(store.clone(), id);

        // etan DMs ben → ben now has an inbound DM from etan.
        bus.send(
            &caller("etan", "s_etan"),
            SendRequest {
                to: SendTarget::dm_name("ben"),
                summary: None,
                body: "take the auth refactor?".into(),
                mention: vec![],
                metadata: None,
                idempotency_key: None,
            },
        )
        .await
        .unwrap();
        let after_first = realtime.enqueue_count();
        assert_eq!(after_first, 1);

        // ben replies (no target) → must route back to etan (the last inbound scope).
        bus.send(
            &caller("ben", "s_ben"),
            SendRequest {
                to: SendTarget::Reply,
                summary: None,
                body: "on it".into(),
                mention: vec![],
                metadata: None,
                idempotency_key: None,
            },
        )
        .await
        .unwrap();

        let recipients = realtime.recipients();
        assert_eq!(recipients.len(), 2);
        assert_eq!(
            recipients[1],
            SessionId("s_etan".into()),
            "reply routes back to the last inbound DM sender"
        );
    }

    #[tokio::test]
    async fn reply_context_survives_settled_transport_payload_discard() {
        let store = split_store().await;
        seed_agent_runtime(&store, "a_ben", "ben", "s_ben").await;
        seed_agent_runtime(&store, "a_etan", "etan", "s_etan").await;
        let id = identity_with_agents(&[("ben", "s_ben", "a_ben"), ("etan", "s_etan", "a_etan")]);
        let (bus, realtime) = build(store.clone(), id);

        let inbound = bus
            .send(
                &caller_with_agent("etan", "s_etan", "a_etan"),
                SendRequest {
                    to: SendTarget::dm_name("ben"),
                    summary: None,
                    body: "reply after projection cleanup".into(),
                    mention: vec![],
                    metadata: None,
                    idempotency_key: None,
                },
            )
            .await
            .unwrap();

        store
            .conn
            .execute(
                "UPDATE in_flight SET state = 'injecting' WHERE message_id = ?1",
                libsql::params![inbound.message_id.0.clone()],
            )
            .await
            .unwrap();
        store
            .conn
            .execute(
                "UPDATE in_flight SET state = 'delivered' WHERE message_id = ?1",
                libsql::params![inbound.message_id.0.clone()],
            )
            .await
            .unwrap();
        assert!(
            Inbox::new(&store)
                .discard_settled_message_if_complete(&inbound.message_id)
                .await
                .unwrap(),
            "split transport must discard the settled rich payload"
        );
        assert_eq!(message_count(&store).await, 0);

        bus.send(
            &caller_with_agent("ben", "s_ben", "a_ben"),
            SendRequest {
                to: SendTarget::Reply,
                summary: None,
                body: "still routes back to etan".into(),
                mention: vec![],
                metadata: None,
                idempotency_key: None,
            },
        )
        .await
        .expect("reply route must outlive disposable message and in-flight rows");

        assert_eq!(
            realtime.recipients(),
            vec![SessionId("s_ben".into()), SessionId("s_etan".into())]
        );
    }

    #[tokio::test]
    async fn reply_uses_stable_agent_identity_after_runtime_replacement() {
        let store = migrated_store().await;
        seed_agent_runtime(&store, "a_ben", "ben", "s_ben_old").await;
        seed_agent_runtime(&store, "a_etan", "etan", "s_etan").await;
        let id =
            identity_with_agents(&[("ben", "s_ben_old", "a_ben"), ("etan", "s_etan", "a_etan")]);
        let (bus, realtime) = build(store.clone(), id);

        bus.send(
            &caller_with_agent("etan", "s_etan", "a_etan"),
            SendRequest {
                to: SendTarget::dm_name("ben"),
                summary: None,
                body: "this lands on the old runtime".into(),
                mention: vec![],
                metadata: None,
                idempotency_key: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(realtime.recipients(), vec![SessionId("s_ben_old".into())]);

        bus.send(
            &caller_with_agent("ben", "s_ben_new", "a_ben"),
            SendRequest {
                to: SendTarget::Reply,
                summary: None,
                body: "reply from revived runtime".into(),
                mention: vec![],
                metadata: None,
                idempotency_key: None,
            },
        )
        .await
        .unwrap();

        let recipients = realtime.recipients();
        assert_eq!(recipients.len(), 2);
        assert_eq!(
            recipients[1],
            SessionId("s_etan".into()),
            "reply finds the inbound row by agent_id, not only by old runtime"
        );
    }

    #[tokio::test]
    async fn reply_dm_uses_sender_agent_id_after_sender_rename_and_name_reuse() {
        let store = migrated_store().await;
        seed_agent_runtime(&store, "a_original", "renamed-sender", "s_original").await;
        seed_agent_runtime(&store, "a_imposter", "old-sender", "s_imposter").await;
        seed_agent_runtime(&store, "a_receiver", "receiver", "s_receiver").await;

        store
            .conn
            .execute(
                "INSERT INTO messages (message_id, from_name, kind, to_name, body, provenance, \
                 project, created_at, from_agent_id, to_agent_id) \
                 VALUES ('m_inbound', 'old-sender', 'dm', 'receiver', 'hello', '{}', ?1, 1, \
                 'a_original', 'a_receiver')",
                libsql::params![PROJECT],
            )
            .await
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO in_flight (in_flight_id, message_id, recipient_session, \
                 recipient_agent_id, state) VALUES ('if_receiver', 'm_inbound', 's_receiver', \
                 'a_receiver', 'pending')",
                (),
            )
            .await
            .unwrap();

        let id = identity_with_agents(&[
            ("old-sender", "s_imposter", "a_imposter"),
            ("renamed-sender", "s_original", "a_original"),
            ("receiver", "s_receiver", "a_receiver"),
        ]);
        let (bus, realtime) = build(store, id);

        bus.send(
            &caller_with_agent("receiver", "s_receiver", "a_receiver"),
            SendRequest {
                to: SendTarget::Reply,
                summary: None,
                body: "reply to original".into(),
                mention: vec![],
                metadata: None,
                idempotency_key: None,
            },
        )
        .await
        .unwrap();

        assert_eq!(realtime.recipients(), vec![SessionId("s_original".into())]);
    }
}
