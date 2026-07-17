//! Task 3 tests: `AppState::push_as_source` — source event fans out to topic subscribers via the
//! real bus (in-memory store), updates `last_fired_at`, and rejects bad inputs.
//!
//! Test setup pattern mirrors nexus-bus topic tests: migrated in-memory store, real `Bus` wired via
//! `AppState::new` with mock non-bus ports. The subscriber agent and source row are created directly
//! in the store before each test.

use std::sync::Arc;

use async_trait::async_trait;

use nexus::daemon::{AppState, WsSink};
use nexus_contracts::{
    codes, AckRequest, AckResponse, AckThreadsRequest, AssignProjectResponse, Caller,
    ChannelRequest, ConsumeRequest, HeartbeatResponse, HistoryRequest, HistoryResponse, MessageId,
    MonitorRequest, NexusBatch, NotifyRequest, NotifyResponse, PortResult, PushRequest,
    RegisterRequest, RegisterResponse, RemoveRequest, RemoveResponse, RouteForwardRequest,
    SearchRequest, SearchResponse, SessionId, SpawnRequest, SpawnResponse, StatusRequest,
    StatusResponse, Tier, Whoami,
};
use nexus_store::{
    repos::{NewSession, Sessions, Sources, Topics},
    Store,
};

// ---- Minimal mock ports (only bus is real) ----

struct MockIdentity;
#[async_trait]
impl nexus_contracts::IdentityPort for MockIdentity {
    async fn register(&self, _req: RegisterRequest) -> PortResult<RegisterResponse> {
        Ok(RegisterResponse {
            agent_id: None,
            session_id: SessionId("s_mock".into()),
            directive: "mock".into(),
        })
    }
    async fn whoami(&self, _caller: &Caller) -> PortResult<Whoami> {
        unimplemented!()
    }
    async fn resolve(&self, _project: &str, name: &str) -> PortResult<Caller> {
        // Return a stub caller so the bus can resolve subscriber sessions for topic sends.
        Ok(Caller {
            agent_id: None,
            session: SessionId(format!("s_{name}")),
            name: name.to_string(),
            project: PROJECT.to_string(),
            tier: Tier::Agent,
        })
    }
    async fn members(
        &self,
        _c: &Caller,
        _r: nexus_contracts::MemberListRequest,
    ) -> PortResult<nexus_contracts::MemberListResponse> {
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
    async fn rename(
        &self,
        _c: &Caller,
        _r: nexus_contracts::RenameRequest,
    ) -> PortResult<nexus_contracts::RenameResponse> {
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
    async fn launch(&self, _req: SpawnRequest) -> PortResult<SpawnResponse> {
        Ok(SpawnResponse {
            session_id: SessionId("s_launched".into()),
        })
    }
    async fn remove(&self, _req: RemoveRequest) -> PortResult<RemoveResponse> {
        unimplemented!()
    }
    async fn prompt(&self, _r: &SessionId, _t: String) -> PortResult<()> {
        Ok(())
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

struct MockAdmin;
#[async_trait]
impl nexus_contracts::AdminPort for MockAdmin {
    async fn spawn(&self, _c: &Caller, _r: SpawnRequest) -> PortResult<SpawnResponse> {
        unimplemented!()
    }
    async fn remove(&self, _c: &Caller, _r: RemoveRequest) -> PortResult<RemoveResponse> {
        unimplemented!()
    }
    async fn assign_role(
        &self,
        _c: &Caller,
        _r: nexus_contracts::AssignRoleRequest,
    ) -> PortResult<nexus_contracts::AssignRoleResponse> {
        unimplemented!()
    }
    async fn channel(&self, _c: &Caller, _r: ChannelRequest) -> PortResult<()> {
        Ok(())
    }
    async fn route(&self, _c: &Caller, _r: RouteForwardRequest) -> PortResult<()> {
        Ok(())
    }
    async fn monitor(&self, _c: &Caller, _r: MonitorRequest) -> PortResult<()> {
        Ok(())
    }
    async fn assign_project(
        &self,
        _c: &Caller,
        _r: nexus_contracts::AssignProjectRequest,
    ) -> PortResult<nexus_contracts::AssignProjectResponse> {
        unimplemented!()
    }
}

const PROJECT: &str = "p_test";
const TOPIC: &str = "t";
const SOURCE_NAME: &str = "my-source";
const TOKEN_HASH: &str = "hash_xyz";

/// Open a migrated in-memory store, wire the real bus in `AppState::new`.
async fn make_state() -> (AppState, Arc<Store>) {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();

    // Wire a real bus so actual message rows get written.
    let realtime: Arc<dyn nexus_contracts::DispatchPort> = Arc::new(MockRealtime);
    let identity: Arc<dyn nexus_contracts::IdentityPort> = Arc::new(MockIdentity);
    let events: Arc<dyn nexus_contracts::EventSink> = Arc::new(WsSink::new(16, None));
    let bus = Arc::new(nexus_bus::Bus::new(
        store.clone(),
        realtime.clone(),
        identity.clone(),
        events.clone(),
    ));

    let state = AppState::new(
        store.clone(),
        WsSink::new(16, None),
        identity,
        Arc::new(MockAgent),
        realtime,
        bus,
        Arc::new(MockSearch),
        Arc::new(MockNotify),
        Arc::new(MockAdmin),
        PROJECT.into(),
    );
    (state, store)
}

/// Register a subscriber session so the bus can enqueue for them, then subscribe to the topic.
async fn setup_subscriber(store: &Store, session_id: &str, name: &str, topic: &str) {
    // Create a session row so the subscriber exists in the identity layer.
    Sessions::new(store)
        .create(NewSession {
            session_id: SessionId(session_id.into()),
            name: Some(name.into()),
            agent: None,
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: None,
            client_key: None,
            cwd: None,
            project: PROJECT.into(),
            transport: None,
        })
        .await
        .unwrap();
    // Ensure topic row exists in project.
    Topics::new(store).ensure(topic, PROJECT).await.unwrap();
    // Subscribe the session.
    Topics::new(store)
        .subscribe(topic, session_id, None)
        .await
        .unwrap();
}

/// Create the source row (enabled by default).
async fn setup_source(store: &Store, name: &str, topic: &str, enabled: bool) {
    Sources::new(store)
        .create(name, TOKEN_HASH, topic, 1000)
        .await
        .unwrap();
    if !enabled {
        Sources::new(store).set_enabled(name, false).await.unwrap();
    }
}

fn operator_caller() -> Caller {
    Caller {
        agent_id: None,
        session: SessionId("s_operator".into()),
        name: "operator".into(),
        project: PROJECT.into(),
        tier: Tier::Admin,
    }
}

// ---- Tests ----

/// Happy path: push with a subscriber → queued receipt + canonical id, inbox row, fired stamp.
#[tokio::test]
async fn push_fans_out_to_subscriber_and_returns_queued_receipt() {
    let (state, store) = make_state().await;
    setup_subscriber(&store, "s_agent1", "agent1", TOPIC).await;
    setup_source(&store, SOURCE_NAME, TOPIC, true).await;

    let resp = state
        .push_as_source(
            &operator_caller(),
            PushRequest {
                source: SOURCE_NAME.into(),
                topic: None,
                summary: None,
                body: "hi".into(),
                meta: None,
            },
        )
        .await
        .unwrap();

    assert_eq!(resp.topic, TOPIC);
    assert!(
        resp.queued_to >= 1,
        "queued_to must be >= 1, got {}",
        resp.queued_to
    );
    assert!(resp.message_id.is_some());

    // Exactly one messages row was written.
    let mut rows = store
        .conn
        .query("SELECT COUNT(*) FROM messages", ())
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    let n: i64 = row.get(0).unwrap();
    assert_eq!(n, 1, "exactly one message row on fan-out");

    let mut rows = store
        .conn
        .query("SELECT provenance FROM messages LIMIT 1", ())
        .await
        .unwrap();
    let provenance: String = rows.next().await.unwrap().unwrap().get(0).unwrap();
    let provenance: serde_json::Value = serde_json::from_str(&provenance).unwrap();
    assert_eq!(
        provenance["kind"], "notification",
        "source transport must arrive as notification provenance"
    );

    // One in_flight row for the subscriber.
    let mut rows = store
        .conn
        .query("SELECT COUNT(*) FROM in_flight", ())
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    let n: i64 = row.get(0).unwrap();
    assert_eq!(n, 1, "one in_flight row for subscriber");

    // touch_fired was called: last_fired_at is now set.
    let src = Sources::new(&store)
        .find(SOURCE_NAME)
        .await
        .unwrap()
        .unwrap();
    assert!(
        src.last_fired_at.is_some(),
        "touch_fired must set last_fired_at"
    );
}

/// A push to a topic NOBODY has subscribed to (so it's absent from the topic registry) must be a
/// successful ZERO-delivery, not an error — a producer can't be coupled to consumer state. (This is
/// the `topic="errors"` case from the live `custom_app_local.py` acceptance run.)
#[tokio::test]
async fn push_to_unsubscribed_topic_returns_zero_delivery_not_error() {
    let (state, store) = make_state().await;
    // Source exists; NO subscriber → "ghost-topic" was never created in the registry.
    setup_source(&store, SOURCE_NAME, "ghost-topic", true).await;

    let resp = state
        .push_as_source(
            &operator_caller(),
            PushRequest {
                source: SOURCE_NAME.into(),
                topic: Some("ghost-topic".into()),
                summary: None,
                body: "nobody home".into(),
                meta: None,
            },
        )
        .await
        .expect("push to an unsubscribed topic must succeed, not error");

    assert_eq!(resp.topic, "ghost-topic");
    assert_eq!(resp.queued_to, 0, "no subscribers → zero queued rows");
    assert_eq!(resp.message_id, None);

    // Nothing was published.
    let mut rows = store
        .conn
        .query("SELECT COUNT(*) FROM messages", ())
        .await
        .unwrap();
    let n: i64 = rows.next().await.unwrap().unwrap().get(0).unwrap();
    assert_eq!(n, 0, "unsubscribed-topic push writes no message row");

    // But the source still recorded the fire.
    let src = Sources::new(&store)
        .find(SOURCE_NAME)
        .await
        .unwrap()
        .unwrap();
    assert!(
        src.last_fired_at.is_some(),
        "touch_fired runs even on zero-delivery"
    );
}

/// Empty body → INVALID_PARAMS.
#[tokio::test]
async fn push_empty_body_returns_invalid_argument() {
    let (state, store) = make_state().await;
    setup_source(&store, SOURCE_NAME, TOPIC, true).await;

    let err = state
        .push_as_source(
            &operator_caller(),
            PushRequest {
                source: SOURCE_NAME.into(),
                topic: None,
                summary: None,
                body: "".into(),
                meta: None,
            },
        )
        .await
        .expect_err("empty body must error");

    assert_eq!(err.code, codes::INVALID_PARAMS);
}

/// Missing source → NOT_FOUND.
#[tokio::test]
async fn push_missing_source_returns_not_found() {
    let (state, _store) = make_state().await;

    let err = state
        .push_as_source(
            &operator_caller(),
            PushRequest {
                source: "ghost-source".into(),
                topic: None,
                summary: None,
                body: "ping".into(),
                meta: None,
            },
        )
        .await
        .expect_err("missing source must error");

    assert_eq!(err.code, codes::NOT_FOUND);
}

/// Disabled source → UNAUTHORIZED (FORBIDDEN).
#[tokio::test]
async fn push_disabled_source_returns_forbidden() {
    let (state, store) = make_state().await;
    setup_source(&store, SOURCE_NAME, TOPIC, false).await;

    let err = state
        .push_as_source(
            &operator_caller(),
            PushRequest {
                source: SOURCE_NAME.into(),
                topic: None,
                summary: None,
                body: "ping".into(),
                meta: None,
            },
        )
        .await
        .expect_err("disabled source must error");

    assert_eq!(err.code, codes::UNAUTHORIZED);
}

/// meta folds into body: delivered message body contains both the body text and the meta block.
#[tokio::test]
async fn push_meta_folds_into_body() {
    let (state, store) = make_state().await;
    setup_subscriber(&store, "s_agent2", "agent2", TOPIC).await;
    setup_source(&store, SOURCE_NAME, TOPIC, true).await;

    let meta = serde_json::json!({ "sha": "abc123", "branch": "main" });
    state
        .push_as_source(
            &operator_caller(),
            PushRequest {
                source: SOURCE_NAME.into(),
                topic: None,
                summary: None,
                body: "build passed".into(),
                meta: Some(meta.clone()),
            },
        )
        .await
        .unwrap();

    // The stored message body must contain both the user body AND the meta JSON block.
    let mut rows = store
        .conn
        .query("SELECT body FROM messages LIMIT 1", ())
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    let body: String = row.get(0).unwrap();
    assert!(
        body.contains("build passed"),
        "body must contain the push body text"
    );
    assert!(body.contains("<meta>"), "body must contain <meta> block");
    assert!(
        body.contains("abc123"),
        "body must contain meta JSON content"
    );
}

/// topic override: passing `topic` in req overrides the source's default topic.
#[tokio::test]
async fn push_topic_override_routes_to_override_topic() {
    let (state, store) = make_state().await;
    let override_topic = "override-topic";
    setup_subscriber(&store, "s_agent3", "agent3", override_topic).await;
    // Source default topic is different.
    setup_source(&store, SOURCE_NAME, "other-topic", true).await;
    // Also ensure override-topic exists in project (ensure handles idempotency).
    Topics::new(&store)
        .ensure(override_topic, PROJECT)
        .await
        .unwrap();

    let resp = state
        .push_as_source(
            &operator_caller(),
            PushRequest {
                source: SOURCE_NAME.into(),
                topic: Some(override_topic.into()),
                summary: None,
                body: "hi override".into(),
                meta: None,
            },
        )
        .await
        .unwrap();

    assert_eq!(
        resp.topic, override_topic,
        "topic must be the overridden value"
    );
    assert!(resp.queued_to >= 1);
    assert!(resp.message_id.is_some());
}
