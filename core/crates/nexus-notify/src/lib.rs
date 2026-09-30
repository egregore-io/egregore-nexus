//! `nexus-notify` — internal notification ingest, HMAC verify, the Pub monitor feed, and
//! route-by-source/topic dispatch (backend spec §7). Implements
//! [`NotifyPort`](nexus_contracts::ports::NotifyPort).
//!
//! **v4 framing — internal ingest only.** There is **no HTTP `/notify` webhook in the daemon**.
//! External producers (CI, GitHub, cron, …) hit the TS gateway's public API with an HMAC over the
//! timestamp plus exact raw body. The gateway verifies before enqueueing a durable signed envelope;
//! the command worker independently re-verifies it before [`NotifyPort::ingest_verified`]. Direct
//! daemon [`NotifyPort::ingest`] remains unverified and dropped. Invalid public signatures never
//! enqueue and therefore create no daemon notification state.
//!
//! **Pub = monitor, routing = dispatch.** Every verified notification is *always* appended to the
//! `pub` topic for the monitor view; it reaches an *agent* only via a standing route rule
//! (route-by-source/topic, [`RoutingRules`]) or an admin one-shot [`forward`](Notify::forward).
//! Landing in Pub never, by itself, pushes a notification to an agent.

mod error;
mod hmac;
mod ingest;
mod pubfeed;
mod routing;
mod service;

pub use crate::hmac::{verify_hmac, verify_timestamped_hmac};
pub use pubfeed::PUB_TOPIC;
pub use routing::{RouteRule, RoutingRules};
pub use service::{notification_effect_key, Notify};

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;

    use async_trait::async_trait;

    use nexus_contracts::admin::{ChannelOp, ChannelRequest, RouteForwardRequest};
    use nexus_contracts::enums::Tier;
    use nexus_contracts::events::WsEvent;
    use nexus_contracts::ids::{MessageId, SessionId};
    use nexus_contracts::notify::NotifyRequest;
    use nexus_contracts::ports::{
        BusPort, Caller, ContractError, EventSink, NotifyPort, PortResult,
    };
    use nexus_contracts::send::{Ack, SendRequest, SendTarget};
    use nexus_contracts::threads::{
        CreateThreadRequest, JoinThreadRequest, LeaveThreadRequest, ThreadListResponse,
        ThreadMemberRequest, ThreadMembersRequest, ThreadMembersResponse,
    };
    use nexus_contracts::topics::{
        SubscribeRequest, SubscribeResponse, TopicListResponse, UnsubscribeRequest,
    };
    use nexus_store::repos::{Notifications, Topics};
    use nexus_store::Store;

    use crate::{Notify, RoutingRules};

    // ---- A bus mock that records every send target so tests can assert Pub-vs-agent split. ----
    #[derive(Default)]
    struct BusCalls {
        /// `(target_describe, body)` for every `send` call.
        sends: Vec<String>,
        targets: Vec<SendTarget>,
    }

    struct MockBus {
        calls: Mutex<BusCalls>,
        /// Names that resolve as a real DM recipient; others return NotFound.
        known: Vec<String>,
    }
    impl MockBus {
        fn new(known: &[&str]) -> Self {
            MockBus {
                calls: Mutex::new(BusCalls::default()),
                known: known.iter().map(|s| s.to_string()).collect(),
            }
        }
        fn targets(&self) -> Vec<String> {
            self.calls.lock().unwrap().sends.clone()
        }
        fn target_shapes(&self) -> Vec<SendTarget> {
            self.calls.lock().unwrap().targets.clone()
        }
    }

    #[async_trait]
    impl BusPort for MockBus {
        async fn preflight_send(&self, _caller: &Caller, req: &SendRequest) -> PortResult<()> {
            if let SendTarget::Dm { name, agent_id } = &req.to {
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
            Ok(())
        }

        async fn preflight_notify_target(
            &self,
            _caller: &Caller,
            target: &nexus_contracts::NotifyTarget,
        ) -> PortResult<Option<nexus_contracts::AgentId>> {
            Ok(match target {
                nexus_contracts::NotifyTarget::Agent { agent_id } => Some(agent_id.clone()),
                _ => None,
            })
        }

        async fn send(&self, _caller: &Caller, req: SendRequest) -> PortResult<Ack> {
            let desc = match &req.to {
                SendTarget::Publish { topic } => format!("publish:{topic}"),
                SendTarget::Dm { name, agent_id } => format!(
                    "dm:{}",
                    agent_id
                        .as_ref()
                        .map(|id| id.0.as_str())
                        .or(name.as_deref())
                        .unwrap_or("")
                ),
                SendTarget::Post { thread } => format!("post:{thread}"),
                SendTarget::Reply => "reply".to_string(),
            };
            {
                let mut calls = self.calls.lock().unwrap();
                calls.sends.push(desc.clone());
                calls.targets.push(req.to.clone());
            }
            // Unknown DM target → NotFound (so routing audit skips it); publishes always ok.
            if let SendTarget::Dm { name, agent_id } = &req.to {
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
            Ok(Ack {
                message_id: MessageId("m_sent".into()),
                fanout: None,
            })
        }
        async fn create_thread(&self, _c: &Caller, _r: CreateThreadRequest) -> PortResult<()> {
            Ok(())
        }
        async fn join_thread(&self, _c: &Caller, _r: JoinThreadRequest) -> PortResult<()> {
            Ok(())
        }
        async fn leave_thread(&self, _c: &Caller, _r: LeaveThreadRequest) -> PortResult<()> {
            Ok(())
        }
        async fn add_thread_member(&self, _c: &Caller, _r: ThreadMemberRequest) -> PortResult<()> {
            Ok(())
        }
        async fn remove_thread_member(
            &self,
            _c: &Caller,
            _r: ThreadMemberRequest,
        ) -> PortResult<()> {
            Ok(())
        }
        async fn threads(&self, _c: &Caller) -> PortResult<ThreadListResponse> {
            Ok(ThreadListResponse { threads: vec![] })
        }
        async fn thread_members(
            &self,
            _c: &Caller,
            _r: ThreadMembersRequest,
        ) -> PortResult<ThreadMembersResponse> {
            Ok(ThreadMembersResponse {
                name: String::new(),
                members: vec![],
            })
        }
        async fn subscribe(
            &self,
            _c: &Caller,
            _r: SubscribeRequest,
        ) -> PortResult<SubscribeResponse> {
            Ok(SubscribeResponse {
                topic: String::new(),
                cursor: 0,
            })
        }
        async fn unsubscribe(&self, _c: &Caller, _r: UnsubscribeRequest) -> PortResult<()> {
            Ok(())
        }
        async fn topics(&self, _c: &Caller) -> PortResult<TopicListResponse> {
            Ok(TopicListResponse { topics: vec![] })
        }
    }

    // ---- An event sink mock that records emitted WsEvents. ----
    #[derive(Default)]
    struct MockEvents {
        emitted: Mutex<Vec<WsEvent>>,
    }
    #[async_trait]
    impl EventSink for MockEvents {
        async fn emit(&self, event: WsEvent) {
            self.emitted.lock().unwrap().push(event);
        }
    }

    async fn migrated_store() -> Arc<Store> {
        let s = Store::open(":memory:").await.unwrap();
        s.migrate().await.unwrap();
        Arc::new(s)
    }

    fn caller(name: &str, tier: Tier) -> Caller {
        Caller {
            agent_id: None,
            session: SessionId(format!("s_{name}")),
            name: name.into(),
            project: "nexus".into(),
            tier,
        }
    }

    // =====================================================================================
    // Load-bearing tests.
    // =====================================================================================

    #[tokio::test]
    async fn bad_signature_is_dropped_with_hmac_ok_false_and_no_agent_path() {
        let store = migrated_store().await;
        let bus = Arc::new(MockBus::new(&["ben"]));
        let events = Arc::new(MockEvents::default());
        let notify = Notify::new(
            store.clone(),
            bus.clone(),
            events.clone(),
            RoutingRules::default(),
        );

        let req = NotifyRequest {
            source: "ci".into(),
            topic: Some("ci".into()),
            payload: serde_json::json!({ "status": "broken" }),
        };
        // hmac_ok = false → must be recorded but dropped.
        let resp = notify.ingest(req, false).await.unwrap();

        assert!(!resp.hmac_ok);
        assert!(
            resp.routed_to.is_empty(),
            "dropped notif must route to nobody"
        );

        // No agent path at all: the bus was never touched (no publish, no dm).
        assert!(bus.targets().is_empty(), "bad-sig must not hit the bus");
        // No web console event emitted.
        assert!(events.emitted.lock().unwrap().is_empty());

        // Recorded with hmac_ok = false and empty routed_to.
        let row = Notifications::new(&store)
            .get(&resp.notif_id.0)
            .await
            .unwrap()
            .unwrap();
        assert!(!row.hmac_ok);
        assert_eq!(row.routed_to.as_deref(), Some(""));
    }

    #[tokio::test]
    async fn no_topic_lands_in_pub_only_not_to_any_agent() {
        let store = migrated_store().await;
        let bus = Arc::new(MockBus::new(&["ben", "ana"]));
        let events = Arc::new(MockEvents::default());
        let notify = Notify::new(
            store.clone(),
            bus.clone(),
            events.clone(),
            RoutingRules::default(),
        );

        let req = NotifyRequest {
            source: "cron".into(),
            topic: None,
            payload: serde_json::json!({ "tick": 1 }),
        };
        let resp = notify.ingest(req, true).await.unwrap();

        assert!(resp.hmac_ok);
        assert!(resp.routed_to.is_empty(), "no topic → no agent dispatch");

        // The Pub feed got it; no DM (no in_flight) for any agent.
        let targets = bus.targets();
        assert!(
            targets.iter().any(|t| t == "publish:pub"),
            "pub feed must be appended"
        );
        assert!(
            !targets.iter().any(|t| t.starts_with("dm:")),
            "no agent should be dispatched: {targets:?}"
        );

        // Event emitted with empty routed_to.
        let emitted = events.emitted.lock().unwrap();
        assert!(matches!(
            emitted.first(),
            Some(WsEvent::NotificationReceived { routed_to, .. }) if routed_to.is_empty()
        ));
    }

    #[tokio::test]
    async fn explicit_recipient_uses_notification_spine_and_records_audit() {
        let store = migrated_store().await;
        let bus = Arc::new(MockBus::new(&["ana", "a_unnamed"]));
        let events = Arc::new(MockEvents::default());
        let notify = Notify::new(
            store.clone(),
            bus.clone(),
            events.clone(),
            RoutingRules::default(),
        );

        let req = NotifyRequest {
            source: "nexus-thread".into(),
            topic: None,
            payload: serde_json::json!({
                "event": "thread.added",
                "thread": "backend",
                "member": "ana",
            }),
        };
        let resp = notify
            .ingest_for(
                req,
                true,
                "proj".into(),
                vec!["ana".into(), "a_unnamed".into()],
            )
            .await
            .unwrap();

        assert!(resp.hmac_ok);
        assert_eq!(
            resp.routed_to,
            vec!["ana".to_string(), "a_unnamed".to_string()]
        );
        let targets = bus.targets();
        assert!(
            targets.iter().any(|t| t == "publish:pub"),
            "explicit notifications still land in Pub"
        );
        assert!(
            targets.iter().any(|t| t == "dm:ana"),
            "explicit recipient must get a DM notification: {targets:?}"
        );
        assert!(targets.iter().any(|t| t == "dm:a_unnamed"));
        let shapes = bus.target_shapes();
        assert!(shapes.iter().any(|target| matches!(
            target,
            SendTarget::Dm { name, agent_id }
                if name.as_deref() == Some("ana") && agent_id.is_none()
        )));
        assert!(shapes.iter().any(|target| matches!(
            target,
            SendTarget::Dm { name, agent_id }
                if name.as_deref() == Some("a_unnamed") && agent_id.is_none()
        )));

        let row = Notifications::new(&store)
            .get(&resp.notif_id.0)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.source.as_deref(), Some("nexus-thread"));
        assert_eq!(row.routed_to.as_deref(), Some("ana,a_unnamed"));
    }

    #[tokio::test]
    async fn topic_match_routes_to_subscribers() {
        let store = migrated_store().await;
        // Subscribe `ben` to topic `ci` in the real store. `subscribers()` returns the subscriber
        // session value; we use the name as the session here so the audit reads "ben".
        {
            let topics = Topics::new(&store);
            topics.ensure("ci", "nexus").await.unwrap();
            topics.subscribe("ci", "ben", None).await.unwrap();
        }
        let bus = Arc::new(MockBus::new(&["ben"]));
        let events = Arc::new(MockEvents::default());
        let notify = Notify::new(
            store.clone(),
            bus.clone(),
            events.clone(),
            RoutingRules::default(),
        );

        let req = NotifyRequest {
            source: "github".into(),
            topic: Some("ci".into()),
            payload: serde_json::json!({ "status": "green" }),
        };
        let resp = notify.ingest(req, true).await.unwrap();

        assert!(resp.hmac_ok);
        assert_eq!(resp.routed_to, vec!["ben".to_string()]);

        let targets = bus.targets();
        assert!(
            targets.iter().any(|t| t == "publish:pub"),
            "pub still gets it"
        );
        assert!(
            targets.iter().any(|t| t == "publish:ci"),
            "ci subscribers must be dispatched via the topic fan-out: {targets:?}"
        );

        // Recorded in routed_to.
        let row = Notifications::new(&store)
            .get(&resp.notif_id.0)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.routed_to.as_deref(), Some("ben"));

        // Event carries the routed recipient.
        let emitted = events.emitted.lock().unwrap();
        assert!(matches!(
            emitted.first(),
            Some(WsEvent::NotificationReceived { routed_to, .. }) if routed_to == &vec!["ben".to_string()]
        ));
    }

    #[tokio::test]
    async fn admin_forward_requires_admin_tier() {
        let store = migrated_store().await;
        // Record a notification to forward.
        let notif_id = Notifications::new(&store)
            .record(Some("ci"), Some("ci"), true, "{\"ok\":true}", "")
            .await
            .unwrap();

        let bus = Arc::new(MockBus::new(&["ben"]));
        let events = Arc::new(MockEvents::default());
        let notify = Notify::new(
            store.clone(),
            bus.clone(),
            events.clone(),
            RoutingRules::default(),
        );

        let fwd = RouteForwardRequest {
            notif: MessageId(notif_id.clone()),
            to: "ben".into(),
        };

        // Agent caller → Unauthorized.
        let agent = caller("dylan", Tier::Agent);
        let err = notify.forward(&agent, fwd.clone()).await.unwrap_err();
        assert_eq!(err.code, nexus_contracts::codes::UNAUTHORIZED);
        assert!(
            bus.targets().is_empty(),
            "rejected forward must not dispatch"
        );

        // Admin caller → ok, one-shot (a single dm dispatch, not a standing rule).
        let admin = caller("root", Tier::Admin);
        notify.forward(&admin, fwd).await.unwrap();
        let targets = bus.targets();
        assert_eq!(
            targets,
            vec!["dm:ben".to_string()],
            "admin forward is one-shot"
        );
    }

    // Sanity: the channel op is also tier-gated.
    #[tokio::test]
    async fn channel_requires_admin_tier() {
        let store = migrated_store().await;
        let bus = Arc::new(MockBus::new(&[]));
        let events = Arc::new(MockEvents::default());
        let notify = Notify::new(store, bus, events, RoutingRules::default());

        let req = ChannelRequest {
            op: ChannelOp::Create,
            topic: "ops".into(),
            source: None,
        };
        let agent = caller("dylan", Tier::Agent);
        assert_eq!(
            notify.channel(&agent, req.clone()).await.unwrap_err().code,
            nexus_contracts::codes::UNAUTHORIZED
        );
        let admin = caller("root", Tier::Admin);
        notify.channel(&admin, req).await.unwrap();
    }
}
