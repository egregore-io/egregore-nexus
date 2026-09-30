use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use nexus_agent::{
    Adapter, AdapterProviderLimit, AdapterRegistry, Agent, MockAdapter, StreamEvent,
};
use nexus_contracts::{
    codes, AgentTurnExecutionPort, AgentUpdateKind, AssignProjectResponse, BatchCounts,
    BatchMessage, Caller, ContractError, EventSink, Harness, HeartbeatResponse, IdentityPort,
    InjectError, Kind, MemberListRequest, MemberListResponse, NexusBatch, ProviderLimitReason,
    RegisterRequest, RegisterResponse, RemoveRequest, Scope, SessionId, SpawnRequest,
    StatusRequest, StatusResponse, Whoami, WsEvent,
};

/// A minimal IdentityPort stub; the agent service only needs `resolve` for `remove`.
struct StubIdentity;

#[async_trait]
impl IdentityPort for StubIdentity {
    async fn register(&self, _req: RegisterRequest) -> Result<RegisterResponse, ContractError> {
        unimplemented!()
    }

    async fn whoami(&self, _caller: &Caller) -> Result<Whoami, ContractError> {
        unimplemented!()
    }

    async fn resolve(&self, _project: &str, name: &str) -> Result<Caller, ContractError> {
        Err(ContractError {
            code: codes::NOT_FOUND,
            message: format!("no such name: {name}"),
        })
    }

    async fn members(
        &self,
        _caller: &Caller,
        _req: MemberListRequest,
    ) -> Result<MemberListResponse, ContractError> {
        unimplemented!()
    }

    async fn status(
        &self,
        _caller: &Caller,
        _req: StatusRequest,
    ) -> Result<StatusResponse, ContractError> {
        unimplemented!()
    }

    async fn heartbeat(&self, _caller: &Caller) -> Result<HeartbeatResponse, ContractError> {
        unimplemented!()
    }

    async fn assign_project(
        &self,
        _name: &str,
        _to_project: &str,
    ) -> Result<AssignProjectResponse, ContractError> {
        unimplemented!()
    }
}

#[derive(Clone, Default)]
struct RecordingSink {
    events: Arc<Mutex<Vec<WsEvent>>>,
}

impl RecordingSink {
    fn events(&self) -> Vec<WsEvent> {
        self.events.lock().unwrap().clone()
    }
}

#[async_trait]
impl EventSink for RecordingSink {
    async fn emit(&self, event: WsEvent) {
        self.events.lock().unwrap().push(event);
    }
}

fn dm(from: &str, kind: Kind, body: &str) -> BatchMessage {
    BatchMessage {
        id: nexus_common::new_message_id(),
        from: from.into(),
        kind,
        scope: Scope::Dm,
        thread: None,
        topic: None,
        body: body.into(),
        truncated: false,
    }
}

fn batch(dms: Vec<BatchMessage>, threads: Vec<BatchMessage>) -> NexusBatch {
    let dm_ids: Vec<_> = dms.iter().map(|m| m.id.clone()).collect();
    let th_ids: Vec<_> = threads.iter().map(|m| m.id.clone()).collect();
    let all: Vec<_> = dm_ids.iter().chain(th_ids.iter()).cloned().collect();
    NexusBatch {
        counts: BatchCounts {
            dms: dms.len() as u32,
            thread: threads.len() as u32,
            total: (dms.len() + threads.len()) as u32,
        },
        dms,
        threads,
        dm_message_ids: dm_ids,
        thread_message_ids: th_ids,
        message_ids: all,
    }
}

fn agent_with_bound_mock() -> (Agent, MockAdapter, RecordingSink, SessionId) {
    let registry = AdapterRegistry::with_builtins();
    let sink = RecordingSink::default();
    let agent = Agent::new(registry, Arc::new(StubIdentity), Arc::new(sink.clone()));
    let mock = MockAdapter::new();
    let session = SessionId("s_test".into());
    agent.bind_session(
        session.clone(),
        "dylan",
        "demo",
        Arc::new(mock.clone()),
        false,
    );
    (agent, mock, sink, session)
}

#[tokio::test]
async fn inject_turn_wraps_bus_traffic_in_nexus_batch() {
    let (agent, mock, _sink, session) = agent_with_bound_mock();
    let b = batch(
        vec![
            dm("dylan", Kind::Agent, "rebase before you start"),
            dm("ana", Kind::Agent, "take the auth refactor?"),
        ],
        vec![],
    );

    agent.inject_turn(&session, &b).await.unwrap();

    let prompt = mock.last_prompt().expect("a prompt was injected");
    assert!(
        prompt.contains("<nexus-batch"),
        "must wrap in <nexus-batch>: {prompt}"
    );
    assert!(
        prompt.contains("receiver=\"s_test\""),
        "must name the recipient session on the batch and items: {prompt}"
    );
    assert!(
        prompt.contains("target=\"dm:s_test\""),
        "DM items must name the per-recipient target: {prompt}"
    );
    assert!(
        prompt.contains("<nexus from=\"dylan\""),
        "must carry per-message provenance: {prompt}"
    );
}

#[tokio::test]
async fn core_user_dm_injected_plain_no_wrapper() {
    let (agent, mock, _sink, session) = agent_with_bound_mock();
    let b = batch(
        vec![dm("erin", Kind::Human, "take the auth refactor today?")],
        vec![],
    );

    agent.inject_turn(&session, &b).await.unwrap();

    let prompt = mock.last_prompt().expect("a prompt was injected");
    assert_eq!(prompt, "take the auth refactor today?");
    assert!(
        !prompt.contains("<nexus"),
        "core user DM must be plain: {prompt}"
    );
}

#[tokio::test]
async fn adapter_init_failure_surfaces_errored_status_and_retains_session() {
    let mut registry = AdapterRegistry::with_builtins();
    let sink = RecordingSink::default();
    let failing = MockAdapter::new();
    failing.fail_open_session("acp handshake refused");
    registry.register(
        Harness::Claude,
        Arc::new(move |_cwd| Arc::new(failing.clone()) as Arc<dyn Adapter>),
    );
    let agent = Agent::new(registry, Arc::new(StubIdentity), Arc::new(sink.clone()));

    let req = SpawnRequest {
        kind: Harness::Claude,
        name: Some("ben".into()),
        identity_policy: None,
        cwd: None,
        project: None,
        role: None,
        initial_prompt: None,
        resume: None,
        harness_args: Vec::new(),
        headless: false,
        backend: None,
    };
    let err = agent
        .launch(req)
        .await
        .expect_err("launch must surface the init failure");
    assert_eq!(err.code, codes::INTERNAL_ERROR);

    let events = sink.events();
    let status = events
        .iter()
        .find(|e| matches!(e, WsEvent::AgentStatus { .. }))
        .expect("an agent.status event was emitted");
    if let WsEvent::AgentStatus { session_id, .. } = status {
        assert!(
            agent.has_session(session_id),
            "session must be retained for retry"
        );
    }
}

#[tokio::test]
async fn remove_kill_true_calls_adapter_kill() {
    let (agent, mock, _sink, session) = agent_with_bound_mock();
    assert_eq!(mock.kill_count(), 0, "no kills before remove");

    let req = RemoveRequest {
        agent_id: None,
        name: "dylan".into(),
        kill: true,
    };
    let resp = agent.remove(req).await.expect("remove must succeed");

    assert_eq!(resp.status, "removed");
    assert_eq!(
        mock.kill_count(),
        1,
        "kill=true must call adapter.kill() exactly once"
    );
    assert!(!agent.has_session(&session), "session must be detached");
}

#[tokio::test]
async fn remove_kill_false_does_not_call_adapter_kill() {
    let (agent, mock, _sink, session) = agent_with_bound_mock();

    let req = RemoveRequest {
        agent_id: None,
        name: "dylan".into(),
        kill: false,
    };
    let resp = agent.remove(req).await.expect("remove must succeed");

    assert_eq!(resp.status, "removed");
    assert_eq!(
        mock.kill_count(),
        0,
        "kill=false (evict) must NOT call adapter.kill()"
    );
    assert!(!agent.has_session(&session), "session must be detached");
}

#[tokio::test]
async fn remove_not_found_no_kill() {
    let (agent, mock, _sink, _session) = agent_with_bound_mock();
    let req = RemoveRequest {
        agent_id: None,
        name: "nobody".into(),
        kill: true,
    };

    let resp = agent
        .remove(req)
        .await
        .expect("remove must succeed (not_found is not an error)");

    assert_eq!(resp.status, "not_found");
    assert_eq!(
        mock.kill_count(),
        0,
        "kill must not fire for an unknown session"
    );
}

#[tokio::test]
async fn stream_updates_relay_as_agent_update_events() {
    let (agent, mock, sink, session) = agent_with_bound_mock();
    mock.script_events(vec![
        StreamEvent::thinking("pondering"),
        StreamEvent::text("the answer"),
    ]);
    let b = batch(vec![dm("ana", Kind::Agent, "status?")], vec![]);

    agent.inject_turn(&session, &b).await.unwrap();

    let all = sink.events();
    let user_inputs = all
        .iter()
        .filter(|e| {
            matches!(e, WsEvent::AgentUpdate { session_id, kind, .. }
                if *session_id == session && *kind == AgentUpdateKind::UserInput)
        })
        .count();
    assert_eq!(
        user_inputs, 0,
        "ACP bus inject_turn must not double-emit the realtime loop's user_input projection"
    );

    let updates: Vec<(AgentUpdateKind, serde_json::Value)> = all
        .into_iter()
        .filter_map(|e| match e {
            WsEvent::AgentUpdate {
                session_id,
                kind,
                data,
            } if session_id == session && kind != AgentUpdateKind::TurnEnd => Some((kind, data)),
            _ => None,
        })
        .collect();

    assert_eq!(updates.len(), 2, "both stream content events relayed");
    assert_eq!(updates[0].0, AgentUpdateKind::Thinking);
    assert_eq!(updates[0].1["text"], "pondering");
    assert_eq!(updates[1].0, AgentUpdateKind::Text);
    assert_eq!(updates[1].1["text"], "the answer");
}

#[tokio::test]
async fn observed_bus_inject_emits_user_input_before_reply_stream() {
    let (agent, mock, sink, session) = agent_with_bound_mock();
    mock.script_events(vec![StreamEvent::text("visible reply")]);
    let b = batch(vec![dm("ana", Kind::Agent, "status?")], vec![]);
    let accepted_event = WsEvent::AgentUpdate {
        session_id: session.clone(),
        kind: AgentUpdateKind::UserInput,
        data: serde_json::json!({
            "text": "<nexus-batch>status?</nexus-batch>",
            "source": "bus",
            "clientMessageId": "bus:m_observed",
        }),
    };

    agent
        .inject_turn_observed(&session, &b, Arc::new(sink.clone()), accepted_event)
        .await
        .unwrap();

    let kinds: Vec<AgentUpdateKind> = sink
        .events()
        .into_iter()
        .filter_map(|event| match event {
            WsEvent::AgentUpdate { kind, .. } => Some(kind),
            _ => None,
        })
        .collect();

    assert_eq!(
        kinds,
        vec![
            AgentUpdateKind::UserInput,
            AgentUpdateKind::Text,
            AgentUpdateKind::TurnEnd,
        ],
        "observed bus input must precede assistant output and turn_end"
    );
}

#[tokio::test]
async fn observed_inject_preserves_adapter_provider_limit() {
    let (agent, mock, _sink, session) = agent_with_bound_mock();
    mock.fail_inject_provider_limit(AdapterProviderLimit {
        harness: Harness::Claude,
        reason: ProviderLimitReason::RateLimit,
        reset_hint: None,
        provider: Some("anthropic".into()),
        model: Some("claude-opus".into()),
        source: "test.structured_frame".into(),
    });
    let b = batch(vec![dm("ana", Kind::Agent, "status?")], vec![]);
    let accepted_event = WsEvent::AgentUpdate {
        session_id: session.clone(),
        kind: AgentUpdateKind::UserInput,
        data: serde_json::json!({ "text": "status?", "source": "bus" }),
    };

    let err = agent
        .inject_turn_observed(
            &session,
            &b,
            Arc::new(RecordingSink::default()),
            accepted_event,
        )
        .await
        .expect_err("typed provider limit should reach realtime unchanged");
    let InjectError::ProviderLimit(limit) = err else {
        panic!("expected provider limit, got {err:?}");
    };
    assert_eq!(limit.session, session);
    assert_eq!(limit.harness, Harness::Claude);
    assert_eq!(limit.reason, ProviderLimitReason::RateLimit);
    assert_eq!(limit.source, "test.structured_frame");
}

#[tokio::test]
async fn observed_prompt_emits_initial_prompt_before_reply_stream() {
    let (agent, mock, sink, session) = agent_with_bound_mock();
    mock.script_events(vec![StreamEvent::text("boot acknowledged")]);
    let accepted_event = WsEvent::AgentUpdate {
        session_id: session.clone(),
        kind: AgentUpdateKind::UserInput,
        data: serde_json::json!({
            "text": "You are ada.",
            "source": "initial_prompt",
            "clientMessageId": "initial-prompt:r_ada",
            "runtimeId": "r_ada",
            "harness": "codex",
        }),
    };

    agent
        .prompt_observed(
            &session,
            "You are ada.".to_string(),
            Arc::new(sink.clone()),
            accepted_event,
        )
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let events = sink.events();
    let user_inputs: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            WsEvent::AgentUpdate {
                kind: AgentUpdateKind::UserInput,
                data,
                ..
            } => Some(data.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        user_inputs.len(),
        1,
        "observed prompt must not emit both a direct-drive echo and an accepted event"
    );
    assert_eq!(user_inputs[0]["source"], "initial_prompt");
    assert_eq!(user_inputs[0]["clientMessageId"], "initial-prompt:r_ada");

    let kinds: Vec<AgentUpdateKind> = events
        .into_iter()
        .filter_map(|event| match event {
            WsEvent::AgentUpdate { kind, .. } => Some(kind),
            _ => None,
        })
        .collect();
    assert_eq!(
        kinds,
        vec![
            AgentUpdateKind::UserInput,
            AgentUpdateKind::Text,
            AgentUpdateKind::TurnEnd,
        ],
        "accepted initial prompt must precede assistant output and turn_end"
    );
}

#[tokio::test]
async fn prompt_direct_drive_emits_exactly_one_user_input() {
    let (agent, _mock, sink, session) = agent_with_bound_mock();

    agent
        .prompt(&session, "hello from operator".into())
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let all = sink.events();
    let user_inputs = all
        .iter()
        .filter(|e| {
            matches!(e, WsEvent::AgentUpdate { session_id, kind, .. }
                if *session_id == session && *kind == AgentUpdateKind::UserInput)
        })
        .count();
    assert_eq!(
        user_inputs, 1,
        "direct-drive prompt must emit exactly one user_input"
    );

    let input_event = all.iter().find(
        |e| matches!(e, WsEvent::AgentUpdate { kind, .. } if *kind == AgentUpdateKind::UserInput),
    );
    if let Some(WsEvent::AgentUpdate { data, .. }) = input_event {
        assert_eq!(data["text"], "hello from operator");
    }
}

#[test]
fn is_harness_alive_returns_some_true_for_live_session() {
    let (agent, _mock, _sink, session) = agent_with_bound_mock();

    assert_eq!(
        agent.is_harness_alive(&session),
        Some(true),
        "is_harness_alive must return Some(true) for a live ACP session"
    );
}

#[test]
fn is_harness_alive_returns_some_false_for_unknown_session() {
    let (agent, _mock, _sink, _session) = agent_with_bound_mock();
    let unknown = SessionId("s_unknown_xyz".into());

    assert_eq!(
        agent.is_harness_alive(&unknown),
        Some(false),
        "is_harness_alive must return Some(false) for an unknown/unbound session"
    );
}

#[test]
fn is_harness_alive_returns_some_false_for_errored_session() {
    let registry = AdapterRegistry::with_builtins();
    let sink = RecordingSink::default();
    let agent = Agent::new(registry, Arc::new(StubIdentity), Arc::new(sink));
    let mock = MockAdapter::new();
    let session = SessionId("s_errored".into());

    agent.bind_session(
        session.clone(),
        "errored_agent",
        "demo",
        Arc::new(mock),
        true,
    );

    assert_eq!(
        agent.is_harness_alive(&session),
        Some(false),
        "is_harness_alive must return Some(false) for an errored ACP session"
    );
}
