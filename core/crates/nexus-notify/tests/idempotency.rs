use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use nexus_contracts::ports::{BusPort, Caller, ContractError, EventSink, NotifyPort, PortResult};
use nexus_contracts::send::{Ack, SendRequest, SendTarget};
use nexus_contracts::threads::{
    CreateThreadRequest, JoinThreadRequest, LeaveThreadRequest, ThreadListResponse,
    ThreadMemberRequest, ThreadMembersRequest, ThreadMembersResponse,
};
use nexus_contracts::topics::{
    SubscribeRequest, SubscribeResponse, TopicListResponse, UnsubscribeRequest,
};
use nexus_contracts::{MessageId, NotifyRequest, WsEvent};
use nexus_notify::{Notify, RouteRule, RoutingRules};
use nexus_store::repos::Topics;
use nexus_store::Store;

#[derive(Default)]
struct BusCalls {
    targets: Vec<SendTarget>,
    idempotency_keys: Vec<Option<String>>,
}

struct MockBus {
    calls: Mutex<BusCalls>,
    known: Vec<String>,
}

impl MockBus {
    fn new(known: &[&str]) -> Self {
        Self {
            calls: Mutex::new(BusCalls::default()),
            known: known.iter().map(|name| (*name).to_string()).collect(),
        }
    }

    fn targets(&self) -> Vec<SendTarget> {
        self.calls.lock().unwrap().targets.clone()
    }

    fn idempotency_keys(&self) -> Vec<Option<String>> {
        self.calls.lock().unwrap().idempotency_keys.clone()
    }
}

#[async_trait]
impl BusPort for MockBus {
    async fn send(&self, _caller: &Caller, request: SendRequest) -> PortResult<Ack> {
        if let SendTarget::Dm { name, agent_id } = &request.to {
            let recipient = agent_id
                .as_ref()
                .map(|id| id.0.as_str())
                .or(name.as_deref())
                .unwrap_or("");
            if !self.known.iter().any(|known| known == recipient) {
                return Err(ContractError {
                    code: nexus_contracts::codes::NOT_FOUND,
                    message: format!("no such recipient: {recipient}"),
                });
            }
        }

        let mut calls = self.calls.lock().unwrap();
        calls.targets.push(request.to);
        calls.idempotency_keys.push(request.idempotency_key);
        Ok(Ack {
            message_id: MessageId("m_sent".into()),
            fanout: None,
        })
    }

    async fn create_thread(
        &self,
        _caller: &Caller,
        _request: CreateThreadRequest,
    ) -> PortResult<()> {
        Ok(())
    }

    async fn join_thread(&self, _caller: &Caller, _request: JoinThreadRequest) -> PortResult<()> {
        Ok(())
    }

    async fn leave_thread(&self, _caller: &Caller, _request: LeaveThreadRequest) -> PortResult<()> {
        Ok(())
    }

    async fn add_thread_member(
        &self,
        _caller: &Caller,
        _request: ThreadMemberRequest,
    ) -> PortResult<()> {
        Ok(())
    }

    async fn remove_thread_member(
        &self,
        _caller: &Caller,
        _request: ThreadMemberRequest,
    ) -> PortResult<()> {
        Ok(())
    }

    async fn threads(&self, _caller: &Caller) -> PortResult<ThreadListResponse> {
        Ok(ThreadListResponse { threads: vec![] })
    }

    async fn thread_members(
        &self,
        _caller: &Caller,
        _request: ThreadMembersRequest,
    ) -> PortResult<ThreadMembersResponse> {
        Ok(ThreadMembersResponse {
            name: String::new(),
            members: vec![],
        })
    }

    async fn subscribe(
        &self,
        _caller: &Caller,
        _request: SubscribeRequest,
    ) -> PortResult<SubscribeResponse> {
        Ok(SubscribeResponse {
            topic: String::new(),
            cursor: 0,
        })
    }

    async fn unsubscribe(&self, _caller: &Caller, _request: UnsubscribeRequest) -> PortResult<()> {
        Ok(())
    }

    async fn topics(&self, _caller: &Caller) -> PortResult<TopicListResponse> {
        Ok(TopicListResponse { topics: vec![] })
    }
}

struct NoopEvents;

#[async_trait]
impl EventSink for NoopEvents {
    async fn emit(&self, _event: WsEvent) {}
}

async fn migrated_store() -> Arc<Store> {
    let store = Store::open(":memory:").await.unwrap();
    store.migrate().await.unwrap();
    Arc::new(store)
}

#[tokio::test]
async fn verified_ingest_derives_stable_distinct_keys_for_pub_and_each_dm() {
    let store = migrated_store().await;
    let bus = Arc::new(MockBus::new(&["ana", "a_unnamed"]));
    let notify = Notify::new(
        store,
        bus.clone(),
        Arc::new(NoopEvents),
        RoutingRules::new(vec![
            RouteRule {
                source: Some("pager".into()),
                topic: None,
                to: "ana".into(),
            },
            RouteRule {
                source: Some("pager".into()),
                topic: None,
                to: "a_unnamed".into(),
            },
        ]),
    );
    let request = NotifyRequest {
        source: "pager".into(),
        topic: None,
        payload: serde_json::json!({"incident": "INC-1"}),
    };

    notify
        .ingest_verified(request.clone(), "notify:root-command".into())
        .await
        .unwrap();
    notify
        .ingest_verified(request, "notify:root-command".into())
        .await
        .unwrap();

    let keys = bus.idempotency_keys();
    assert_eq!(keys.len(), 6);
    assert_eq!(&keys[..3], &keys[3..], "reclaim must derive the same keys");
    assert!(keys.iter().all(Option::is_some));
    let mut unique = keys[..3]
        .iter()
        .map(|key| key.as_deref().unwrap())
        .collect::<Vec<_>>();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(unique.len(), 3, "pub and each DM require distinct keys");

    let targets = bus.targets();
    assert!(targets.iter().any(|target| matches!(
        target,
        SendTarget::Dm { name, agent_id }
            if name.as_deref() == Some("ana") && agent_id.is_none()
    )));
    assert!(targets.iter().any(|target| matches!(
        target,
        SendTarget::Dm { name, agent_id }
            if name.as_deref() == Some("a_unnamed") && agent_id.is_none()
    )));
}

#[tokio::test]
async fn verified_topic_ingest_separates_pub_and_routed_topic_keys() {
    let store = migrated_store().await;
    Topics::new(&store).ensure("ci", "nexus").await.unwrap();
    Topics::new(&store)
        .subscribe("ci", "ben", None)
        .await
        .unwrap();
    let bus = Arc::new(MockBus::new(&["ben"]));
    let notify = Notify::new(
        store,
        bus.clone(),
        Arc::new(NoopEvents),
        RoutingRules::default(),
    );
    let request = NotifyRequest {
        source: "github".into(),
        topic: Some("ci".into()),
        payload: serde_json::json!({"status": "green"}),
    };

    notify
        .ingest_verified(request.clone(), "notify:topic-root".into())
        .await
        .unwrap();
    notify
        .ingest_verified(request, "notify:topic-root".into())
        .await
        .unwrap();

    let keys = bus.idempotency_keys();
    assert_eq!(keys.len(), 4);
    assert_eq!(&keys[..2], &keys[2..], "reclaim must derive the same keys");
    assert_ne!(keys[0], keys[1], "Pub and routed topic effects must differ");
    assert!(matches!(
        &bus.targets()[..],
        [
            SendTarget::Publish { topic: first },
            SendTarget::Publish { topic: second },
            SendTarget::Publish { topic: third },
            SendTarget::Publish { topic: fourth }
        ] if first == "pub" && second == "ci" && third == "pub" && fourth == "ci"
    ));
}
