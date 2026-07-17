use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use libsql::params;
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

async fn insert_session(store: &Store, name: &str, session: &str, kind: &str) {
    store
        .conn
        .execute(
            "INSERT INTO sessions (session_id, name, kind, tier, project, presence, created_at) \
             VALUES (?1, ?2, ?3, 'agent', ?4, 'online', 1)",
            params![session, name, kind, PROJECT],
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn human_thread_post_stores_human_provenance_kind() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();

    let threads = Threads::new(&store);
    let thread_id = ThreadId("t_backend".into());
    threads
        .create(&thread_id, "backend", PROJECT, "etan")
        .await
        .unwrap();
    for member in ["etan", "ben"] {
        threads.add_member(&thread_id, member).await.unwrap();
    }
    insert_session(&store, "etan", "s_etan", "human").await;

    let identity = Arc::new(MockIdentity {
        sessions: HashMap::from([
            ("etan".to_string(), SessionId("s_etan".into())),
            ("ben".to_string(), SessionId("s_ben".into())),
        ]),
    });
    let bus = Bus::new(
        store.clone(),
        Arc::new(MockRealtime),
        identity,
        Arc::new(NoopSink),
    );

    bus.send(
        &caller("etan", "s_etan"),
        SendRequest {
            to: SendTarget::Post {
                thread: "backend".into(),
            },
            summary: None,
            body: "from the web console".into(),
            mention: vec![],
            idempotency_key: None,
        },
    )
    .await
    .unwrap();

    // Session kind is immutable for one SessionId. Once resolved, a second send must not query the
    // compatibility row again; a runtime rebind mints a new SessionId and therefore a new key.
    store
        .conn
        .execute("DELETE FROM sessions WHERE session_id = 's_etan'", ())
        .await
        .unwrap();
    bus.send(
        &caller("etan", "s_etan"),
        SendRequest {
            to: SendTarget::Post {
                thread: "backend".into(),
            },
            summary: None,
            body: "cached human provenance".into(),
            mention: vec![],
            idempotency_key: Some("cached-human-kind".into()),
        },
    )
    .await
    .unwrap();

    let mut rows = store
        .conn
        .query("SELECT provenance FROM messages WHERE kind = 'thread'", ())
        .await
        .unwrap();
    let mut provenances = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        provenances.push(row.get::<String>(0).unwrap());
    }
    assert_eq!(provenances.len(), 2);
    assert!(provenances.iter().all(|provenance| provenance
        .contains(r#""kind":"human""#)),
        "every send from the cached human session should preserve human provenance: {provenances:?}"
    );
}
