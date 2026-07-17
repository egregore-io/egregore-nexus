use async_trait::async_trait;
use nexus_admin::Admin;
use nexus_contracts::admin::{
    AssignProjectRequest, AssignProjectResponse, AssignRoleRequest, AssignRoleResponse, ChannelOp,
    ChannelRequest, MonitorRequest, RemoveRequest, RemoveResponse, RouteForwardRequest,
    SpawnRequest, SpawnResponse,
};
use nexus_contracts::codes;
use nexus_contracts::enums::{Harness, Tier};
use nexus_contracts::events::WsEvent;
use nexus_contracts::ids::{MessageId, SessionId};
use nexus_contracts::notify::{NotifyRequest, NotifyResponse};
use nexus_contracts::ports::{
    AdminPort, AgentTurnExecutionPort, Caller, EventSink, IdentityPort, NotifyPort, PortResult,
};
use nexus_contracts::register::{
    HeartbeatResponse, MemberListRequest, MemberListResponse, RegisterRequest, RegisterResponse,
    StatusRequest, StatusResponse, Whoami,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Default)]
struct MockAgent {
    launched: AtomicUsize,
    removed: AtomicUsize,
}

#[async_trait]
impl AgentTurnExecutionPort for MockAgent {
    async fn inject_turn(
        &self,
        _recipient: &SessionId,
        _batch: &nexus_contracts::batch::NexusBatch,
    ) -> PortResult<()> {
        Ok(())
    }

    async fn launch(&self, _req: SpawnRequest) -> PortResult<SpawnResponse> {
        self.launched.fetch_add(1, Ordering::SeqCst);
        Ok(SpawnResponse {
            session_id: SessionId("s_new".into()),
        })
    }

    async fn remove(&self, req: RemoveRequest) -> PortResult<RemoveResponse> {
        self.removed.fetch_add(1, Ordering::SeqCst);
        Ok(RemoveResponse {
            name: Some(req.name),
            status: "removed".into(),
        })
    }
}

#[derive(Default)]
struct MockNotify {
    forwarded: AtomicUsize,
    channelled: AtomicUsize,
}

#[async_trait]
impl NotifyPort for MockNotify {
    async fn ingest(&self, _req: NotifyRequest, _hmac_ok: bool) -> PortResult<NotifyResponse> {
        unreachable!("admin never ingests")
    }

    async fn forward(&self, _caller: &Caller, _req: RouteForwardRequest) -> PortResult<()> {
        self.forwarded.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn channel(&self, _caller: &Caller, _req: ChannelRequest) -> PortResult<()> {
        self.channelled.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Default)]
struct MockIdentity {
    roles_assigned: AtomicUsize,
}

#[async_trait]
impl IdentityPort for MockIdentity {
    async fn register(&self, _req: RegisterRequest) -> PortResult<RegisterResponse> {
        unreachable!()
    }

    async fn whoami(&self, _caller: &Caller) -> PortResult<Whoami> {
        unreachable!()
    }

    async fn resolve(&self, _project: &str, _name: &str) -> PortResult<Caller> {
        unreachable!()
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
        name: &str,
        to_project: &str,
    ) -> PortResult<AssignProjectResponse> {
        Ok(AssignProjectResponse {
            name: Some(name.to_string()),
            project: to_project.to_string(),
        })
    }

    async fn assign_role(&self, name: &str, role: &str) -> PortResult<AssignRoleResponse> {
        self.roles_assigned.fetch_add(1, Ordering::SeqCst);
        Ok(AssignRoleResponse {
            name: Some(name.to_string()),
            role: role.to_string(),
        })
    }
}

#[derive(Default)]
struct MockSink {
    emitted: AtomicUsize,
}

#[async_trait]
impl EventSink for MockSink {
    async fn emit(&self, _event: WsEvent) {
        self.emitted.fetch_add(1, Ordering::SeqCst);
    }
}

fn caller(tier: Tier) -> Caller {
    Caller {
        agent_id: None,
        session: SessionId("s_1".into()),
        name: "x".into(),
        project: "p".into(),
        tier,
    }
}

fn build() -> (Admin, Arc<MockAgent>, Arc<MockNotify>, Arc<MockSink>) {
    let agent = Arc::new(MockAgent::default());
    let notify = Arc::new(MockNotify::default());
    let sink = Arc::new(MockSink::default());
    let admin = Admin::new(
        Arc::new(MockIdentity::default()) as Arc<dyn IdentityPort>,
        agent.clone() as Arc<dyn AgentTurnExecutionPort>,
        notify.clone() as Arc<dyn NotifyPort>,
        sink.clone() as Arc<dyn EventSink>,
    );
    (admin, agent, notify, sink)
}

fn spawn_req() -> SpawnRequest {
    SpawnRequest {
        kind: Harness::Codex,
        name: Some("dylan".into()),
        identity_policy: None,
        cwd: None,
        project: Some("p".into()),
        role: Some("backend".into()),
        initial_prompt: None,
        resume: None,
        harness_args: Vec::new(),
        headless: false,
        backend: None,
    }
}

#[tokio::test]
async fn every_admin_op_rejects_non_admin() {
    let (admin, agent, notify, sink) = build();
    let agent_caller = caller(Tier::Agent);

    let err = admin.spawn(&agent_caller, spawn_req()).await.unwrap_err();
    assert_eq!(err.code, codes::UNAUTHORIZED);
    assert!(admin
        .remove(
            &agent_caller,
            RemoveRequest {
                agent_id: None,
                name: "dylan".into(),
                kill: false
            }
        )
        .await
        .is_err());
    assert!(admin
        .assign_role(
            &agent_caller,
            AssignRoleRequest {
                agent_id: None,
                name: "dylan".into(),
                role: "lead".into()
            }
        )
        .await
        .is_err());
    assert!(admin
        .channel(
            &agent_caller,
            ChannelRequest {
                op: ChannelOp::Create,
                topic: "ci".into(),
                source: None
            }
        )
        .await
        .is_err());
    assert!(admin
        .route(
            &agent_caller,
            RouteForwardRequest {
                notif: MessageId("n_1".into()),
                to: "dylan".into()
            }
        )
        .await
        .is_err());
    assert!(admin
        .monitor(
            &agent_caller,
            MonitorRequest {
                follow: false,
                scope: None
            }
        )
        .await
        .is_err());
    assert!(admin
        .assign_project(
            &agent_caller,
            AssignProjectRequest {
                agent_id: None,
                name: "ben".into(),
                project: "lens".into()
            },
        )
        .await
        .is_err());

    assert_eq!(agent.launched.load(Ordering::SeqCst), 0);
    assert_eq!(agent.removed.load(Ordering::SeqCst), 0);
    assert_eq!(notify.forwarded.load(Ordering::SeqCst), 0);
    assert_eq!(notify.channelled.load(Ordering::SeqCst), 0);
    assert_eq!(sink.emitted.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn admin_spawn_delegates_to_turn_exec_port() {
    let (admin, agent, _notify, _sink) = build();
    let response = admin
        .spawn(&caller(Tier::Admin), spawn_req())
        .await
        .unwrap();

    assert_eq!(response.session_id, SessionId("s_new".into()));
    assert_eq!(
        agent.launched.load(Ordering::SeqCst),
        1,
        "mock port records the launch"
    );
}

#[tokio::test]
async fn admin_remove_delegates_to_turn_exec_port() {
    let (admin, agent, _notify, _sink) = build();
    let response = admin
        .remove(
            &caller(Tier::Admin),
            RemoveRequest {
                agent_id: None,
                name: "dylan".into(),
                kill: false,
            },
        )
        .await
        .unwrap();

    assert_eq!(response.status, "removed");
    assert_eq!(agent.removed.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn admin_route_goes_through_notify_forward_one_shot() {
    let (admin, _agent, notify, _sink) = build();
    admin
        .route(
            &caller(Tier::Admin),
            RouteForwardRequest {
                notif: MessageId("n_1".into()),
                to: "dylan".into(),
            },
        )
        .await
        .unwrap();

    assert_eq!(notify.forwarded.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn admin_channel_delegates_to_notify_port() {
    let (admin, _agent, notify, _sink) = build();
    admin
        .channel(
            &caller(Tier::Admin),
            ChannelRequest {
                op: ChannelOp::SetRoute,
                topic: "ci".into(),
                source: Some("gh".into()),
            },
        )
        .await
        .unwrap();

    assert_eq!(notify.channelled.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn admin_assign_role_is_display_label_only() {
    let identity = Arc::new(MockIdentity::default());
    let agent = Arc::new(MockAgent::default());
    let notify = Arc::new(MockNotify::default());
    let sink = Arc::new(MockSink::default());
    let admin = Admin::new(
        identity.clone(),
        agent.clone(),
        notify.clone(),
        sink.clone(),
    );
    let response = admin
        .assign_role(
            &caller(Tier::Admin),
            AssignRoleRequest {
                agent_id: None,
                name: "dylan".into(),
                role: "lead".into(),
            },
        )
        .await
        .unwrap();

    assert_eq!(response.name.as_deref(), Some("dylan"));
    assert_eq!(response.role, "lead");
    assert_eq!(identity.roles_assigned.load(Ordering::SeqCst), 1);
    assert_eq!(agent.launched.load(Ordering::SeqCst), 0);
    assert_eq!(notify.forwarded.load(Ordering::SeqCst), 0);
    assert_eq!(notify.channelled.load(Ordering::SeqCst), 0);
    assert_eq!(sink.emitted.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn admin_assign_project_rejects_non_admin() {
    let (admin, _agent, _notify, _sink) = build();
    let agent_caller = caller(Tier::Agent);
    let err = admin
        .assign_project(
            &agent_caller,
            AssignProjectRequest {
                agent_id: None,
                name: "dylan".into(),
                project: "nexus".into(),
            },
        )
        .await
        .unwrap_err();

    assert_eq!(err.code, codes::UNAUTHORIZED);
}

#[tokio::test]
async fn admin_assign_project_delegates_to_identity_and_echoes() {
    let (admin, agent, notify, sink) = build();
    let response = admin
        .assign_project(
            &caller(Tier::Admin),
            AssignProjectRequest {
                agent_id: None,
                name: "dylan".into(),
                project: "nexus".into(),
            },
        )
        .await
        .unwrap();

    assert_eq!(response.name.as_deref(), Some("dylan"));
    assert_eq!(response.project, "nexus");
    assert_eq!(agent.launched.load(Ordering::SeqCst), 0);
    assert_eq!(notify.forwarded.load(Ordering::SeqCst), 0);
    assert_eq!(notify.channelled.load(Ordering::SeqCst), 0);
    assert_eq!(sink.emitted.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn admin_cannot_inject_into_message_path() {
    let (admin, _agent, notify, _sink) = build();
    admin
        .route(
            &caller(Tier::Admin),
            RouteForwardRequest {
                notif: MessageId("n_1".into()),
                to: "dylan".into(),
            },
        )
        .await
        .unwrap();
    assert_eq!(notify.forwarded.load(Ordering::SeqCst), 1);

    let _obj: &dyn AdminPort = &admin;
    let _ = <Admin as AdminPort>::spawn;
    let _ = <Admin as AdminPort>::remove;
    let _ = <Admin as AdminPort>::assign_role;
    let _ = <Admin as AdminPort>::assign_project;
    let _ = <Admin as AdminPort>::channel;
    let _ = <Admin as AdminPort>::route;
    let _ = <Admin as AdminPort>::monitor;
}
