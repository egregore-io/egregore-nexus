use std::sync::Arc;

use async_trait::async_trait;
use nexus_contracts::{enums::Scope as MsgScope, message::Provenance};
use nexus_contracts::{
    Caller, HeartbeatResponse, HistoryRequest, IdentityPort, Kind, MemberListRequest,
    MemberListResponse, Message, MessageId, PortResult, ProjectId, RegisterRequest,
    RegisterResponse, SearchPort, SessionId, StatusRequest, StatusResponse, ThreadId, Tier, Whoami,
};
use nexus_search::Search;
use nexus_store::repos::{Messages, Threads};
use nexus_store::Store;

struct NoIdentity;

#[async_trait]
impl IdentityPort for NoIdentity {
    async fn register(&self, _req: RegisterRequest) -> PortResult<RegisterResponse> {
        unimplemented!()
    }
    async fn whoami(&self, _caller: &Caller) -> PortResult<Whoami> {
        unimplemented!()
    }
    async fn resolve(&self, _project: &str, _name: &str) -> PortResult<Caller> {
        unimplemented!()
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

async fn migrated() -> Arc<Store> {
    let store = Store::open(":memory:").await.unwrap();
    store.migrate().await.unwrap();
    Arc::new(store)
}

fn caller(name: &str) -> Caller {
    Caller {
        agent_id: None,
        session: SessionId(format!("s_{name}")),
        name: name.to_string(),
        project: "p_demo".to_string(),
        tier: Tier::Agent,
    }
}

async fn insert_thread_msg(store: &Store, id: &str, thread_id: &str, body: &str) {
    Messages::new(store)
        .insert(&Message {
            id: MessageId(id.to_string()),
            project: ProjectId("p_demo".to_string()),
            from: "ben".to_string(),
            scope: MsgScope::Thread,
            thread: Some(ThreadId(thread_id.to_string())),
            topic: None,
            body: body.to_string(),
            summary: None,
            provenance: Provenance {
                from: "ben".to_string(),
                kind: Kind::Agent,
                thread: Some("backend".to_string()),
                topic: None,
                stamp: None,
            },
            created_at: 1,
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn archived_threads_are_hidden_from_scoped_history() {
    let store = migrated().await;
    let threads = Threads::new(&store);
    let thread_id = ThreadId("t_backend".to_string());
    threads
        .create(&thread_id, "backend", "p_demo", "ben")
        .await
        .unwrap();
    threads.add_member(&thread_id, "ana").await.unwrap();
    insert_thread_msg(&store, "m_archived", "t_backend", "hidden archival note").await;

    let search = Search::new(store.clone(), Arc::new(NoIdentity));
    let before = search
        .history(
            &caller("ana"),
            HistoryRequest {
                thread: Some("backend".to_string()),
                with: None,
                topic: None,
                limit: Some(10),
                before: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(before.entries.len(), 1);

    threads.archive("backend").await.unwrap();

    let after = search
        .history(
            &caller("ana"),
            HistoryRequest {
                thread: Some("backend".to_string()),
                with: None,
                topic: None,
                limit: Some(10),
                before: None,
            },
        )
        .await
        .unwrap();
    assert!(
        after.entries.is_empty(),
        "archived thread history should not be reachable through active scoped reads"
    );
}
