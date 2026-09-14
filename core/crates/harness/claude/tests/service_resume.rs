use std::sync::Arc;

use async_trait::async_trait;
use nexus_agent::adapter::engine::HarnessCommand;
use nexus_agent::registry::AdapterRegistry;
use nexus_agent::service::Agent;
use nexus_agent::Adapter;
use nexus_contracts::{
    AgentTurnExecutionPort, AssignProjectResponse, BatchCounts, Caller, ContractError, EventSink,
    HarnessId, HeartbeatResponse, IdentityPort, Kind, MemberListRequest, MemberListResponse,
    NexusBatch, RegisterRequest, RegisterResponse, Scope, SessionId, StatusRequest, StatusResponse,
    Whoami, WsEvent,
};
use nexus_harness_claude::ClaudeAdapter;

const FAKE_HARNESS: &str = env!("CARGO_BIN_EXE_fake_claude_acp_agent");

fn fake_command_failing_load() -> HarnessCommand {
    HarnessCommand {
        program: FAKE_HARNESS.to_string(),
        args: vec![],
        cwd: None,
        env: vec![("FAKE_ACP_FAIL_LOAD".to_string(), "1".to_string())],
    }
}

struct StubIdentity;

#[async_trait]
impl IdentityPort for StubIdentity {
    async fn register(&self, _r: RegisterRequest) -> Result<RegisterResponse, ContractError> {
        unimplemented!()
    }
    async fn whoami(&self, _c: &Caller) -> Result<Whoami, ContractError> {
        unimplemented!()
    }
    async fn resolve(&self, _p: &str, name: &str) -> Result<Caller, ContractError> {
        Err(ContractError {
            code: nexus_contracts::codes::NOT_FOUND,
            message: name.into(),
        })
    }
    async fn members(
        &self,
        _c: &Caller,
        _r: MemberListRequest,
    ) -> Result<MemberListResponse, ContractError> {
        unimplemented!()
    }
    async fn status(
        &self,
        _c: &Caller,
        _r: StatusRequest,
    ) -> Result<StatusResponse, ContractError> {
        unimplemented!()
    }
    async fn heartbeat(&self, _c: &Caller) -> Result<HeartbeatResponse, ContractError> {
        unimplemented!()
    }
    async fn assign_project(
        &self,
        _n: &str,
        _t: &str,
    ) -> Result<AssignProjectResponse, ContractError> {
        unimplemented!()
    }
}

#[derive(Clone, Default)]
struct NoopSink;

#[async_trait]
impl EventSink for NoopSink {
    async fn emit(&self, _e: WsEvent) {}
}

fn failing_load_registry() -> AdapterRegistry {
    let mut reg = AdapterRegistry::new();
    reg.register(
        &HarnessId::new("claude").unwrap(),
        Arc::new(|_ctx| {
            Arc::new(ClaudeAdapter::with_command(fake_command_failing_load())) as Arc<dyn Adapter>
        }),
    );
    reg
}

fn one_dm_batch() -> NexusBatch {
    let id = nexus_common::new_message_id();
    let m = nexus_contracts::BatchMessage {
        id: id.clone(),
        from: "boss".into(),
        kind: Kind::Agent,
        scope: Scope::Dm,
        thread: None,
        topic: None,
        body: "ping after restart".into(),
        truncated: false,
    };
    NexusBatch {
        counts: BatchCounts {
            dms: 1,
            thread: 0,
            total: 1,
        },
        dms: vec![m],
        threads: vec![],
        dm_message_ids: vec![id.clone()],
        thread_message_ids: vec![],
        message_ids: vec![id],
        auto_reply_note: None,
    }
}

#[tokio::test]
async fn open_session_for_resume_fail_leaves_live_injectable_binding() {
    let agent = Agent::new(
        failing_load_registry(),
        Arc::new(StubIdentity),
        Arc::new(NoopSink),
    );
    let session = SessionId("s_respawn_test".into());

    agent
        .open_session_for(
            session.clone(),
            "worker1",
            "default",
            HarnessId::new("claude").unwrap(),
            None,
            vec![],
            Some("dead-resume-key"),
        )
        .await
        .expect("open_session_for must recover via session/new and return Ok");

    assert!(
        agent.is_live(&session),
        "after resume-fail->session/new fallback, the session must be live"
    );
    agent
        .inject_turn(&session, &one_dm_batch())
        .await
        .expect("inject_turn must land on the live fallback session");
}
