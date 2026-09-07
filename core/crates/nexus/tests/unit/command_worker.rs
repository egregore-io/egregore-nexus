use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;

use nexus_common::{Config, HookGatewayMode};
use nexus_contracts::{
    AgentTurnExecutionPort, DaemonIpcCall, DaemonIpcCaller, DaemonIpcRequest, InterruptRequest,
    Kind, Locality, NexusBatch, NotifySendRequest, NotifyTarget, PortResult, PromptRequest,
    RegisterRequest, RemoveRequest, RemoveResponse, SessionId, SpawnRequest, SpawnResponse,
    SteerRequest, Tier, DAEMON_IPC_PROTOCOL_VERSION,
};
use nexus_store::command_kinds;
use nexus_store::repos::{
    Agents, CommandIntents, DaemonState, InboxSubscriptions, NewAgent, NewCommandIntent,
    NewInboxSubscription, NewSession, Sessions,
};
use nexus_store::{DaemonStore, Store};

use super::*;

struct FirstPromptHangs {
    calls: AtomicUsize,
}

#[derive(Default)]
struct PromptDispatchBarrier {
    calls: AtomicUsize,
    entered: Notify,
    release: Notify,
}

#[derive(Default)]
struct FireAndForgetPromptBarrier {
    active: AtomicBool,
    session: Mutex<Option<SessionId>>,
    prompt_entered: Notify,
    wait_entered: Notify,
    release: Notify,
}

#[derive(Default)]
struct InterruptSpy {
    calls: AtomicUsize,
}

#[derive(Default)]
struct SteerDispatchSpy {
    calls: AtomicUsize,
}

#[derive(Default)]
struct ExactDispatchSpy {
    calls: Mutex<Vec<(String, SessionId)>>,
    probes: AtomicUsize,
}

#[async_trait::async_trait]
impl AgentTurnExecutionPort for ExactDispatchSpy {
    async fn inject_turn(&self, _: &SessionId, _: &NexusBatch) -> PortResult<()> {
        panic!("no bus delivery")
    }
    async fn launch(&self, _: SpawnRequest) -> PortResult<SpawnResponse> {
        panic!("exact dispatch cannot revive")
    }
    async fn remove(&self, _: RemoveRequest) -> PortResult<RemoveResponse> {
        panic!("no removal")
    }
    fn is_harness_alive(&self, _: &SessionId) -> Option<bool> {
        self.probes.fetch_add(1, Ordering::SeqCst);
        Some(true)
    }
    async fn prompt(&self, recipient: &SessionId, _: String) -> PortResult<()> {
        self.calls
            .lock()
            .unwrap()
            .push(("prompt".into(), recipient.clone()));
        Ok(())
    }
    async fn steer_observed(
        &self,
        recipient: &SessionId,
        _: String,
        _: Arc<dyn nexus_contracts::EventSink>,
        _: nexus_contracts::WsEvent,
    ) -> PortResult<nexus_contracts::SteerResponse> {
        self.calls
            .lock()
            .unwrap()
            .push(("steer".into(), recipient.clone()));
        Ok(
            serde_json::from_value(serde_json::json!({"accepted":true,"delivery":"steered"}))
                .unwrap(),
        )
    }
    async fn interrupt_active_turn(&self, recipient: &SessionId) -> PortResult<()> {
        self.calls
            .lock()
            .unwrap()
            .push(("interrupt".into(), recipient.clone()));
        Ok(())
    }
    async fn compact(&self, recipient: &SessionId) -> PortResult<()> {
        self.calls
            .lock()
            .unwrap()
            .push(("compact".into(), recipient.clone()));
        Ok(())
    }
}

async fn bind_exact_test_runtime(state: &AppState, session: &str) {
    let sessions = Sessions::new(&state.store);
    sessions
        .create(NewSession {
            session_id: SessionId(session.into()),
            name: Some(format!("target {session}")),
            agent: Some("claude".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: None,
            client_key: None,
            cwd: None,
            project: "metadata".into(),
            transport: Some("pty".into()),
        })
        .await
        .unwrap();
    sessions
        .set_agent_id(&SessionId(session.into()), "a_exact")
        .await
        .unwrap();
    nexus_store::repos::AgentRuntimes::new(&state.store)
        .create(nexus_store::repos::NewAgentRuntime {
            runtime_id: session.into(),
            agent_id: "a_exact".into(),
            harness: "claude".into(),
            cwd: None,
            transport: Some("pty".into()),
            presence: Some("online".into()),
            active: true,
        })
        .await
        .unwrap();
}

async fn exact_test_agent(state: &AppState) {
    Agents::new(&state.store)
        .create(NewAgent {
            agent_id: "a_exact".into(),
            project: "metadata".into(),
            name: Some("exact target".into()),
            default_harness: Some("claude".into()),
            role: None,
            tier: Some("agent".into()),
            owner: None,
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn exact_durable_dispatch_rejects_original_session_after_real_same_agent_rebind() {
    for (method, kind) in [
        ("prompt", command_kinds::harness::PROMPT),
        ("steer", command_kinds::harness::STEER),
        ("interrupt", command_kinds::harness::INTERRUPT),
        ("compact", command_kinds::harness::COMPACT),
    ] {
        let exec = Arc::new(ExactDispatchSpy::default());
        let state = test_state_with_turn_exec(exec.clone()).await;
        let operator = state
            .identity
            .register(human_register("operator", "ck_exact"))
            .await
            .unwrap();
        exact_test_agent(&state).await;
        bind_exact_test_runtime(&state, "s_original").await;
        let mut old = prompt_intent("cmd_exact_old", &operator, "operator", "ck_exact", "old", 1);
        old.kind = kind.into();
        old.request_json = serde_json::json!({"agentId":"a_exact", "expectedSessionId":"s_original", "name":"not routing authority", "text":"old", "clientMessageId":"client_exact_old"}).to_string();
        let repo = CommandIntents::new(&state.store);
        repo.insert_pending(old).await.unwrap();
        bind_exact_test_runtime(&state, "s_current").await;
        assert!(process_next(&state).await.unwrap());
        let old = repo.get("cmd_exact_old").await.unwrap().unwrap();
        assert_eq!(old.status, "error", "{method}: {old:?}");
        assert!(
            exec.calls.lock().unwrap().is_empty(),
            "old durable row must not reach the replacement harness"
        );
        assert_eq!(
            exec.probes.load(Ordering::SeqCst),
            0,
            "exact resolution must bypass ensure_alive"
        );

        let mut current = prompt_intent(
            "cmd_exact_current",
            &operator,
            "operator",
            "ck_exact",
            "current",
            2,
        );
        current.kind = kind.into();
        current.request_json = serde_json::json!({"agentId":"a_exact", "expectedSessionId":"s_current", "name":"ignored", "text":"current"}).to_string();
        repo.insert_pending(current).await.unwrap();
        assert!(process_next(&state).await.unwrap());
        let current = repo.get("cmd_exact_current").await.unwrap().unwrap();
        assert_eq!(current.status, "done", "{method}: {current:?}");
        let result: serde_json::Value =
            serde_json::from_str(current.result_json.as_deref().unwrap()).unwrap();
        assert_eq!(result["sessionId"], "s_current");
        assert_eq!(
            *exec.calls.lock().unwrap(),
            vec![(method.into(), SessionId("s_current".into()))]
        );
        assert_eq!(exec.probes.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn exact_redirect_paths_preserve_original_request_and_retry_identity_through_rebind() {
    for path in ["promote", "redirect", "already-steer"] {
        let exec = Arc::new(ExactDispatchSpy::default());
        let state = test_state_with_turn_exec(exec.clone()).await;
        let operator = state
            .identity
            .register(human_register("operator", "ck_exact_redirect"))
            .await
            .unwrap();
        exact_test_agent(&state).await;
        bind_exact_test_runtime(&state, "s_original").await;
        let mut intent = prompt_intent(
            "cmd_exact_redirect",
            &operator,
            "operator",
            "ck_exact_redirect",
            "redirect",
            1,
        );
        intent.request_json = r#"{ "name":"ignored", "agentId":"a_exact", "expectedSessionId":"s_original", "text":"redirect", "clientMessageId":"cm_exact_redirect" }"#.into();
        intent.idempotency_key = Some("cm_exact_redirect".into());
        if path == "already-steer" {
            intent.kind = command_kinds::harness::STEER.into();
        }
        let original = intent.clone();
        let repo = CommandIntents::new(&state.store);
        repo.insert_pending(intent).await.unwrap();
        if path == "promote" {
            assert!(repo
                .promote_pending_prompt_to_steer("cmd_exact_redirect")
                .await
                .unwrap());
        } else if path == "redirect" {
            let request = serde_json::from_value(serde_json::json!({"agentId":"a_exact", "expectedSessionId":"s_original", "action":"redirect_now", "clientMutationId":"mut_exact_redirect", "commandId":"cmd_exact_redirect", "expectedRevision":1})).unwrap();
            assert_eq!(
                nexus_store::repos::CommandQueue::new(&state.store)
                    .mutate_with_active_sessions(
                        "metadata",
                        &request,
                        2,
                        &[SessionId("s_original".into())]
                    )
                    .await
                    .unwrap()
                    .status,
                200
            );
        }
        let converted = repo.get("cmd_exact_redirect").await.unwrap().unwrap();
        assert_eq!(converted.kind, command_kinds::harness::STEER);
        assert_eq!(
            converted.request_json, original.request_json,
            "no JSON normalization during conversion"
        );
        let mut retry = original;
        retry.command_id = "cmd_retry_new_envelope".into();
        assert_eq!(
            repo.insert_pending_idempotent(retry).await.unwrap(),
            "cmd_exact_redirect"
        );
        bind_exact_test_runtime(&state, "s_current").await;
        assert!(process_next(&state).await.unwrap());
        assert_eq!(
            repo.get("cmd_exact_redirect")
                .await
                .unwrap()
                .unwrap()
                .status,
            "error",
            "{path}"
        );
        assert!(exec.calls.lock().unwrap().is_empty());
        assert_eq!(exec.probes.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn exact_dispatch_rejects_absent_and_misowned_runtime_without_revive() {
    for state_kind in ["absent", "no-session", "misowned"] {
        let exec = Arc::new(ExactDispatchSpy::default());
        let state = test_state_with_turn_exec(exec.clone()).await;
        let operator = state
            .identity
            .register(human_register("operator", "ck_exact_absent"))
            .await
            .unwrap();
        exact_test_agent(&state).await;
        if state_kind != "absent" {
            bind_exact_test_runtime(&state, "s_original").await;
            if state_kind == "no-session" {
                state
                    .store
                    .conn
                    .execute("DELETE FROM sessions WHERE session_id = 's_original'", ())
                    .await
                    .unwrap();
            } else {
                state.store.conn.execute("UPDATE sessions SET agent_id = 'a_foreign' WHERE session_id = 's_original'", ()).await.unwrap();
            }
        }
        for (index, kind) in [
            command_kinds::harness::PROMPT,
            command_kinds::harness::STEER,
            command_kinds::harness::INTERRUPT,
        ]
        .into_iter()
        .enumerate()
        {
            let id = format!("cmd_exact_missing_{index}");
            let mut intent = prompt_intent(
                &id,
                &operator,
                "operator",
                "ck_exact_absent",
                "must not revive",
                1,
            );
            intent.kind = kind.into();
            intent.request_json = serde_json::json!({"agentId":"a_exact", "expectedSessionId":"s_original", "name":"exact target", "text":"must not revive"}).to_string();
            CommandIntents::new(&state.store)
                .insert_pending(intent)
                .await
                .unwrap();
            assert!(process_next(&state).await.unwrap());
            assert_eq!(
                CommandIntents::new(&state.store)
                    .get(&id)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                "error"
            );
        }
        assert!(exec.calls.lock().unwrap().is_empty());
        assert_eq!(exec.probes.load(Ordering::SeqCst), 0);
    }
}

#[async_trait::async_trait]
impl AgentTurnExecutionPort for SteerDispatchSpy {
    async fn inject_turn(&self, _recipient: &SessionId, _batch: &NexusBatch) -> PortResult<()> {
        panic!("steer must not fall back to bus injection")
    }

    async fn launch(&self, _req: SpawnRequest) -> PortResult<SpawnResponse> {
        panic!("steer test must not launch a harness")
    }

    async fn remove(&self, _req: RemoveRequest) -> PortResult<RemoveResponse> {
        panic!("steer test must not remove a harness")
    }

    async fn steer_observed(
        &self,
        _recipient: &SessionId,
        _text: String,
        _events: Arc<dyn nexus_contracts::EventSink>,
        _accepted_event: nexus_contracts::WsEvent,
    ) -> PortResult<nexus_contracts::SteerResponse> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(nexus_contracts::SteerResponse {
            session_id: None,
            accepted: true,
            delivery: nexus_contracts::SteerDelivery::Steered,
            turn_id: Some("native-steer-ack".into()),
        })
    }
}

#[async_trait::async_trait]
impl AgentTurnExecutionPort for InterruptSpy {
    async fn inject_turn(&self, _recipient: &SessionId, _batch: &NexusBatch) -> PortResult<()> {
        Ok(())
    }

    async fn launch(&self, _req: SpawnRequest) -> PortResult<SpawnResponse> {
        unimplemented!("interrupt command test does not launch harnesses")
    }

    async fn remove(&self, _req: RemoveRequest) -> PortResult<RemoveResponse> {
        unimplemented!("interrupt command test does not remove harnesses")
    }

    async fn interrupt_active_turn(&self, _recipient: &SessionId) -> PortResult<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Default)]
struct LivenessProbeSpy {
    probes: AtomicUsize,
}

#[async_trait::async_trait]
impl AgentTurnExecutionPort for LivenessProbeSpy {
    async fn inject_turn(&self, _recipient: &SessionId, _batch: &NexusBatch) -> PortResult<()> {
        Ok(())
    }

    async fn launch(&self, _req: SpawnRequest) -> PortResult<SpawnResponse> {
        unimplemented!("notification preflight test does not launch harnesses")
    }

    async fn remove(&self, _req: RemoveRequest) -> PortResult<RemoveResponse> {
        unimplemented!("notification preflight test does not remove harnesses")
    }

    fn is_harness_alive(&self, _recipient: &SessionId) -> Option<bool> {
        self.probes.fetch_add(1, Ordering::SeqCst);
        Some(false)
    }
}

#[async_trait::async_trait]
impl AgentTurnExecutionPort for FirstPromptHangs {
    async fn inject_turn(&self, _recipient: &SessionId, _batch: &NexusBatch) -> PortResult<()> {
        Ok(())
    }

    async fn launch(&self, _req: SpawnRequest) -> PortResult<SpawnResponse> {
        unimplemented!("command-worker timeout tests do not launch harnesses")
    }

    async fn remove(&self, _req: RemoveRequest) -> PortResult<RemoveResponse> {
        unimplemented!("command-worker timeout tests do not remove harnesses")
    }

    async fn prompt(&self, _recipient: &SessionId, _text: String) -> PortResult<()> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl AgentTurnExecutionPort for PromptDispatchBarrier {
    async fn inject_turn(&self, _recipient: &SessionId, _batch: &NexusBatch) -> PortResult<()> {
        Ok(())
    }

    async fn launch(&self, _req: SpawnRequest) -> PortResult<SpawnResponse> {
        unimplemented!("command-worker shutdown tests do not launch harnesses")
    }

    async fn remove(&self, _req: RemoveRequest) -> PortResult<RemoveResponse> {
        unimplemented!("command-worker shutdown tests do not remove harnesses")
    }

    async fn prompt(&self, _recipient: &SessionId, _text: String) -> PortResult<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        self.release.notified().await;
        Ok(())
    }
}

#[async_trait::async_trait]
impl AgentTurnExecutionPort for FireAndForgetPromptBarrier {
    async fn inject_turn(&self, _recipient: &SessionId, _batch: &NexusBatch) -> PortResult<()> {
        Ok(())
    }

    async fn launch(&self, _req: SpawnRequest) -> PortResult<SpawnResponse> {
        unimplemented!("command-worker shutdown tests do not launch harnesses")
    }

    async fn remove(&self, _req: RemoveRequest) -> PortResult<RemoveResponse> {
        unimplemented!("command-worker shutdown tests do not remove harnesses")
    }

    async fn prompt(&self, recipient: &SessionId, _text: String) -> PortResult<()> {
        *self.session.lock().unwrap() = Some(recipient.clone());
        self.active.store(true, Ordering::SeqCst);
        self.prompt_entered.notify_one();
        Ok(())
    }

    fn active_turn_sessions(&self) -> Vec<SessionId> {
        if !self.active.load(Ordering::SeqCst) {
            return Vec::new();
        }
        self.session.lock().unwrap().clone().into_iter().collect()
    }

    async fn wait_for_turn_completion(&self, recipient: &SessionId) -> PortResult<()> {
        assert_eq!(self.session.lock().unwrap().as_ref(), Some(recipient));
        self.wait_entered.notify_one();
        self.release.notified().await;
        self.active.store(false, Ordering::SeqCst);
        Ok(())
    }
}

async fn test_state_with_turn_exec(turn_exec: Arc<dyn AgentTurnExecutionPort>) -> AppState {
    test_state_with_config(Config::default(), turn_exec).await
}

async fn test_state_with_config(
    config: Config,
    turn_exec: Arc<dyn AgentTurnExecutionPort>,
) -> AppState {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    AppState::wire_with_turn_exec(store, &config, turn_exec)
}

async fn test_state() -> AppState {
    test_state_with_turn_exec(Arc::new(FirstPromptHangs {
        calls: AtomicUsize::new(0),
    }))
    .await
}

#[tokio::test]
async fn verified_notification_rejects_an_ambiguous_recipient_before_delivery_side_effects() {
    let state = test_state().await;
    state
        .store
        .conn
        .execute("DROP INDEX idx_sessions_name", ())
        .await
        .unwrap();
    for (session_id, project, created_at) in
        [("s_notify_one", "one", 1_i64), ("s_notify_two", "two", 2)]
    {
        Sessions::new(&state.store)
            .create(NewSession {
                session_id: SessionId(session_id.into()),
                name: Some("ambiguous-notify-target".into()),
                agent: Some("other".into()),
                kind: "agent".into(),
                role: None,
                tier: "agent".into(),
                harness_session_id: Some(format!("hs_{session_id}")),
                client_key: Some(format!("ck_{session_id}")),
                cwd: None,
                project: project.into(),
                transport: Some("pty".into()),
            })
            .await
            .unwrap();
        state
            .store
            .conn
            .execute(
                "UPDATE sessions SET created_at = ?2 WHERE session_id = ?1",
                libsql::params![session_id, created_at],
            )
            .await
            .unwrap();
    }
    InboxSubscriptions::new(&state.store)
        .upsert_active(NewInboxSubscription {
            subscription_id: "sub_notify_one".into(),
            project: "one".into(),
            caller_name: "ambiguous-notify-target".into(),
            caller_session_id: "s_notify_one".into(),
            caller_agent_id: None,
            caller_client_key: Some("ck_s_notify_one".into()),
            timeout_ms: None,
            max: None,
            created_at: 3,
        })
        .await
        .unwrap();

    let error = drive_verified_notification_routes(
        &state,
        &nexus_contracts::NotifyRequest {
            source: "test".into(),
            topic: None,
            payload: serde_json::json!({"ok": true}),
        },
        "notify-ambiguous",
        &["ambiguous-notify-target".into()],
    )
    .await
    .unwrap_err();

    assert_eq!(error.code, nexus_contracts::codes::INVALID_PARAMS);
    assert!(error.message.contains("multiple legacy identities"));
    assert!(InboxSubscriptions::new(&state.store)
        .has_active_for_session("s_notify_one")
        .await
        .unwrap());
}

#[tokio::test]
async fn notification_send_resolves_a_name_before_installing_any_runtime_hold() {
    let agent = Arc::new(LivenessProbeSpy::default());
    let state = test_state_with_turn_exec(agent.clone()).await;
    let caller = state
        .identity
        .register(human_register("operator", "ck_notify_preflight"))
        .await
        .unwrap();
    state
        .store
        .identity_conn()
        .execute("DROP INDEX idx_agents_name_unique", ())
        .await
        .unwrap();
    state
        .store
        .conn
        .execute("DROP INDEX idx_sessions_name", ())
        .await
        .unwrap();

    let sessions = Sessions::new(&state.store);
    for (index, project) in [(1, "one"), (2, "two")] {
        let agent_id = format!("a_ambiguous_notify_{index}");
        let session_id = SessionId(format!("s_ambiguous_notify_{index}"));
        Agents::new(&state.store)
            .create(NewAgent {
                agent_id: agent_id.clone(),
                project: project.into(),
                name: Some("ambiguous-notify-hold".into()),
                default_harness: Some("claude".into()),
                role: None,
                tier: Some("agent".into()),
                owner: None,
            })
            .await
            .unwrap();
        sessions
            .create(NewSession {
                session_id: session_id.clone(),
                name: Some("ambiguous-notify-hold".into()),
                agent: Some("claude".into()),
                kind: "agent".into(),
                role: None,
                tier: "agent".into(),
                harness_session_id: Some(format!("hs_ambiguous_notify_{index}")),
                client_key: Some(format!("ck_ambiguous_notify_{index}")),
                cwd: None,
                project: project.into(),
                transport: Some("pty".into()),
            })
            .await
            .unwrap();
        sessions.set_agent_id(&session_id, &agent_id).await.unwrap();
    }

    let request = NotifySendRequest {
        target: NotifyTarget::Name {
            name: "ambiguous-notify-hold".into(),
        },
        source: Some("operator".into()),
        body: "must fail before hold".into(),
        idempotency_key: Some("notify:preflight-before-hold".into()),
    };
    CommandIntents::new(&state.store)
        .insert_pending(NewCommandIntent {
            command_id: "cmd_notify_preflight_before_hold".into(),
            kind: command_kinds::notification::SEND.into(),
            project: "metadata-only".into(),
            caller_name: "operator".into(),
            caller_session_id: Some(caller.session_id.0.clone()),
            caller_agent_id: None,
            caller_runtime_id: Some(caller.session_id.0.clone()),
            caller_client_key: Some("ck_notify_preflight".into()),
            caller_principal_id: None,
            caller_kind: Some("human".into()),
            caller_tier: Some("admin".into()),
            idempotency_key: request.idempotency_key.clone(),
            request_json: serde_json::to_string(&request).unwrap(),
            created_at: 1,
        })
        .await
        .unwrap();

    assert!(process_next(&state).await.unwrap());
    let command = CommandIntents::new(&state.store)
        .get("cmd_notify_preflight_before_hold")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(command.status, "error");
    assert!(command.error_json.unwrap().contains("ambiguous"));
    assert_eq!(
        agent.probes.load(Ordering::SeqCst),
        0,
        "resolution failure must happen before the runtime-hold liveness boundary"
    );
    let mut counts = state
        .store
        .conn
        .query(
            "SELECT (SELECT COUNT(*) FROM messages), (SELECT COUNT(*) FROM in_flight)",
            (),
        )
        .await
        .unwrap();
    let row = counts.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 0);
    assert_eq!(row.get::<i64>(1).unwrap(), 0);
}

#[tokio::test]
async fn notification_hook_rejection_leaves_the_target_lifecycle_untouched() {
    let agent = Arc::new(LivenessProbeSpy::default());
    let mut config = Config::default();
    config.hook_gateway_mode = HookGatewayMode::Required;
    let state = test_state_with_config(config, agent.clone()).await;
    let caller = state
        .identity
        .register(human_register("operator", "ck_notify_hook_reject"))
        .await
        .unwrap();
    let agent_id = "a_notify_hook_target";
    let session_id = SessionId("s_notify_hook_target".into());
    Agents::new(&state.store)
        .create(NewAgent {
            agent_id: agent_id.into(),
            project: "default".into(),
            name: Some("notify-hook-target".into()),
            default_harness: Some("claude".into()),
            role: None,
            tier: Some("agent".into()),
            owner: None,
        })
        .await
        .unwrap();
    Sessions::new(&state.store)
        .create(NewSession {
            session_id: session_id.clone(),
            name: Some("notify-hook-target".into()),
            agent: Some("claude".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: Some("hs_notify_hook_target".into()),
            client_key: Some("ck_notify_hook_target".into()),
            cwd: None,
            project: "default".into(),
            transport: Some("pty".into()),
        })
        .await
        .unwrap();
    Sessions::new(&state.store)
        .set_agent_id(&session_id, agent_id)
        .await
        .unwrap();

    let request = NotifySendRequest {
        target: NotifyTarget::Agent {
            agent_id: nexus_contracts::AgentId(agent_id.into()),
        },
        source: Some("operator".into()),
        body: "must reject before lifecycle hold".into(),
        idempotency_key: Some("notify:hook-reject-before-hold".into()),
    };
    CommandIntents::new(&state.store)
        .insert_pending(NewCommandIntent {
            command_id: "cmd_notify_hook_reject_before_hold".into(),
            kind: command_kinds::notification::SEND.into(),
            project: "metadata-only".into(),
            caller_name: "operator".into(),
            caller_session_id: Some(caller.session_id.0.clone()),
            caller_agent_id: None,
            caller_runtime_id: Some(caller.session_id.0.clone()),
            caller_client_key: Some("ck_notify_hook_reject".into()),
            caller_principal_id: None,
            caller_kind: Some("human".into()),
            caller_tier: Some("admin".into()),
            idempotency_key: request.idempotency_key.clone(),
            request_json: serde_json::to_string(&request).unwrap(),
            created_at: 1,
        })
        .await
        .unwrap();

    assert!(process_next(&state).await.unwrap());
    let command = CommandIntents::new(&state.store)
        .get("cmd_notify_hook_reject_before_hold")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(command.status, "error");
    assert!(command
        .error_json
        .unwrap()
        .contains("hook-capable Gateway is unavailable"));
    assert_eq!(
        agent.probes.load(Ordering::SeqCst),
        0,
        "hook rejection must happen before the runtime-hold liveness boundary"
    );
    let mut counts = state
        .store
        .conn
        .query(
            "SELECT (SELECT COUNT(*) FROM messages), (SELECT COUNT(*) FROM in_flight)",
            (),
        )
        .await
        .unwrap();
    let row = counts.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 0);
    assert_eq!(row.get::<i64>(1).unwrap(), 0);
}

fn human_register(name: &str, client_key: &str) -> RegisterRequest {
    RegisterRequest {
        agent_id: None,
        name: Some(name.into()),
        harness: hid("other"),
        harness_session_id: format!("hs_{client_key}"),
        project: "default".into(),
        client_key: client_key.into(),
        runtime_credential: None,
        tier: Tier::Admin,
        kind: Some(Kind::Human),
        locality: Default::default(),
        access: None,
        role: None,
        cwd: None,
    }
}

fn prompt_intent(
    id: &str,
    caller: &nexus_contracts::RegisterResponse,
    caller_name: &str,
    caller_client_key: &str,
    text: &str,
    created_at: i64,
) -> NewCommandIntent {
    NewCommandIntent {
        command_id: id.into(),
        kind: command_kinds::harness::PROMPT.into(),
        project: "default".into(),
        caller_name: caller_name.into(),
        caller_session_id: Some(caller.session_id.0.clone()),
        caller_agent_id: caller.agent_id.as_ref().map(|id| id.0.clone()),
        caller_runtime_id: Some(caller.session_id.0.clone()),
        caller_client_key: Some(caller_client_key.into()),
        caller_principal_id: None,
        caller_kind: Some("human".into()),
        caller_tier: Some("admin".into()),
        idempotency_key: None,
        request_json: serde_json::to_string(&PromptRequest {
            expected_session_id: None,
            agent_id: None,
            name: "Target Human".into(),
            text: text.into(),
            client_message_id: None,
        })
        .unwrap(),
        created_at,
    }
}

fn steer_intent(
    id: &str,
    caller: &nexus_contracts::RegisterResponse,
    caller_name: &str,
    caller_client_key: &str,
    created_at: i64,
) -> NewCommandIntent {
    let mut intent = prompt_intent(
        id,
        caller,
        caller_name,
        caller_client_key,
        "steer now",
        created_at,
    );
    intent.kind = command_kinds::harness::STEER.into();
    intent.request_json = serde_json::to_string(&SteerRequest {
        expected_session_id: None,
        agent_id: None,
        name: "Target Human".into(),
        text: "steer now".into(),
        client_message_id: Some("steer:1".into()),
    })
    .unwrap();
    intent
}

#[tokio::test]
async fn harness_prompt_claim_lease_outlives_execution_timeout() {
    let exec = Arc::new(FirstPromptHangs {
        calls: AtomicUsize::new(0),
    });
    let state = test_state_with_turn_exec(exec).await;
    let alex = state
        .identity
        .register(human_register("Alex Morgan", "ck_operator"))
        .await
        .unwrap();
    state
        .identity
        .register(human_register("Target Human", "ck_target"))
        .await
        .unwrap();

    let repo = CommandIntents::new(&state.store);
    repo.insert_pending(prompt_intent(
        "cmd_prompt",
        &alex,
        "Alex Morgan",
        "ck_operator",
        "claim only",
        1,
    ))
    .await
    .unwrap();

    let row = claim_next_for_lane(&state, WorkerLane::HarnessPrompt)
        .await
        .unwrap()
        .unwrap();

    let claimed_at = row.claimed_at.expect("prompt row should be claimed");
    assert_eq!(row.lease_until, Some(claimed_at + HARNESS_PROMPT_LEASE_MS));
    assert!(
        HARNESS_PROMPT_LEASE_MS
            > i64::try_from(HARNESS_PROMPT_EXECUTION_TIMEOUT.as_millis()).unwrap(),
        "prompt claim must not expire before the prompt execution timeout fires"
    );
}

#[tokio::test]
async fn persisted_unsupported_auto_prompt_is_rejected_without_native_delivery() {
    let exec = Arc::new(PromptDispatchBarrier::default());
    let state = test_state_with_turn_exec(exec.clone()).await;
    let operator = state
        .identity
        .register(human_register("operator", "ck_auto_reject"))
        .await
        .unwrap();
    let mut intent = prompt_intent(
        "cmd_auto_reject",
        &operator,
        "operator",
        "ck_auto_reject",
        "hello",
        1,
    );
    let mut params: serde_json::Value = serde_json::from_str(&intent.request_json).unwrap();
    params["delivery"] = serde_json::json!("auto");
    intent.request_json = params.to_string();
    let repo = CommandIntents::new(&state.store);
    repo.insert_pending(intent).await.unwrap();
    assert!(process_next(&state).await.unwrap());
    let row = repo.get("cmd_auto_reject").await.unwrap().unwrap();
    assert_eq!(
        row.status, "error",
        "unsupported durable rows must settle, not remain wedged"
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(row.error_json.as_deref().unwrap()).unwrap()
            ["code"],
        nexus_contracts::codes::INVALID_PARAMS
    );
    assert_eq!(exec.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn redirected_persisted_intent_rejects_unsupported_options_before_native_steer() {
    for exact in [false, true] {
        for path in ["promote", "redirect", "already-steer"] {
            for extra in [
                serde_json::json!({"delivery": "auto"}),
                serde_json::json!({"modelSelection": {"modelId": "target-choice"}}),
                serde_json::json!({}),
                serde_json::json!({"delivery": null, "modelSelection": null}),
            ] {
                let unsupported = extra.as_object().unwrap().values().any(|v| !v.is_null());
                let exec = Arc::new(SteerDispatchSpy::default());
                let state = test_state_with_turn_exec(exec.clone()).await;
                let operator = state
                    .identity
                    .register(human_register("operator", "ck_redirect_operator"))
                    .await
                    .unwrap();
                let target_session = if exact {
                    exact_test_agent(&state).await;
                    bind_exact_test_runtime(&state, "s_exact_options").await;
                    SessionId("s_exact_options".into())
                } else {
                    state
                        .identity
                        .register(human_register("Target Human", "ck_redirect_target"))
                        .await
                        .unwrap()
                        .session_id
                };
                let repo = CommandIntents::new(&state.store);
                let mut intent = prompt_intent(
                    "cmd_redirect",
                    &operator,
                    "operator",
                    "ck_redirect_operator",
                    "hello",
                    1,
                );
                let mut params: serde_json::Value =
                    serde_json::from_str(&intent.request_json).unwrap();
                params
                    .as_object_mut()
                    .unwrap()
                    .extend(extra.as_object().unwrap().clone());
                if exact {
                    params["agentId"] = serde_json::json!("a_exact");
                    params["expectedSessionId"] = serde_json::json!(target_session);
                }
                intent.request_json = params.to_string();
                if path == "already-steer" {
                    intent.kind = command_kinds::harness::STEER.into();
                }
                repo.insert_pending(intent).await.unwrap();
                match path {
                    "promote" => assert!(repo
                        .promote_pending_prompt_to_steer("cmd_redirect")
                        .await
                        .unwrap()),
                    "redirect" => {
                        let mut request_wire = serde_json::json!({
                            "name": "Target Human", "action": "redirect_now", "clientMutationId": "mut_redirect",
                            "commandId": "cmd_redirect", "expectedRevision": 1
                        });
                        if exact {
                            request_wire["agentId"] = serde_json::json!("a_exact");
                            request_wire["expectedSessionId"] = serde_json::json!(target_session);
                        }
                        let request: nexus_contracts::CommandQueueMutationRequest =
                            serde_json::from_value(request_wire).unwrap();
                        let outcome = nexus_store::repos::CommandQueue::new(&state.store)
                            .mutate_with_active_sessions("default", &request, 2, &[target_session])
                            .await
                            .unwrap();
                        assert_eq!(outcome.status, 200, "{outcome:?}");
                    }
                    _ => {}
                }
                let converted = repo.get("cmd_redirect").await.unwrap().unwrap();
                assert_eq!(converted.kind, command_kinds::harness::STEER);
                assert_eq!(
                    converted.request_json,
                    params.to_string(),
                    "conversion must preserve intent"
                );
                assert!(process_next(&state).await.unwrap());
                let terminal = repo.get("cmd_redirect").await.unwrap().unwrap();
                if unsupported {
                    assert_eq!(terminal.status, "error", "{path}: {extra}");
                    let error: serde_json::Value =
                        serde_json::from_str(terminal.error_json.as_deref().unwrap()).unwrap();
                    assert_eq!(error["code"], nexus_contracts::codes::INVALID_PARAMS);
                    assert_eq!(exec.calls.load(Ordering::SeqCst), 0);
                } else {
                    assert_eq!(terminal.status, "done", "{path}: {extra}: {terminal:?}");
                    assert_eq!(
                        exec.calls.load(Ordering::SeqCst),
                        1,
                        "ordinary explicit steer stays supported"
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn worker_settles_expired_armed_auto_as_unknown_without_native_redelivery() {
    let exec = Arc::new(PromptDispatchBarrier::default());
    let state = test_state_with_turn_exec(exec.clone()).await;
    let operator = state
        .identity
        .register(human_register("operator", "ck_auto_expired"))
        .await
        .unwrap();
    let mut intent = prompt_intent(
        "cmd_auto_expired",
        &operator,
        "operator",
        "ck_auto_expired",
        "hello",
        1,
    );
    let mut params: serde_json::Value = serde_json::from_str(&intent.request_json).unwrap();
    params["delivery"] = serde_json::json!("auto");
    intent.request_json = params.to_string();
    let repo = CommandIntents::new(&state.store);
    repo.insert_pending(intent).await.unwrap();
    let old_claim = repo.claim_next(100, 10).await.unwrap().unwrap();
    assert!(repo
        .mark_auto_started_for_claim(&old_claim, 101)
        .await
        .unwrap());
    assert!(!process_next(&state).await.unwrap());
    let row = repo.get("cmd_auto_expired").await.unwrap().unwrap();
    assert_eq!(row.status, "error");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(row.error_json.as_deref().unwrap()).unwrap()
            ["code"],
        nexus_contracts::codes::DELIVERY_UNCERTAIN
    );
    assert_eq!(row.attempts, 1);
    assert_eq!(exec.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn graceful_shutdown_drains_the_active_prompt_and_defers_pre_fence_backlog() {
    let exec = Arc::new(PromptDispatchBarrier::default());
    let state = test_state_with_turn_exec(exec.clone()).await;
    let operator = state
        .identity
        .register(human_register("Alex Morgan", "ck_shutdown_operator"))
        .await
        .unwrap();
    state
        .identity
        .register(human_register("Target Human", "ck_shutdown_target"))
        .await
        .unwrap();
    let repo = CommandIntents::new(&state.store);
    repo.insert_pending(prompt_intent(
        "cmd_shutdown_active",
        &operator,
        "Alex Morgan",
        "ck_shutdown_operator",
        "reach the accepted boundary",
        1,
    ))
    .await
    .unwrap();
    repo.insert_pending(prompt_intent(
        "cmd_shutdown_pre_fence",
        &operator,
        "Alex Morgan",
        "ck_shutdown_operator",
        "also accepted before shutdown",
        2,
    ))
    .await
    .unwrap();

    let worker = spawn(state.clone());
    tokio::time::timeout(Duration::from_secs(1), exec.entered.notified())
        .await
        .expect("the active prompt must enter the transport boundary");

    begin_shutdown(&state).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !worker.is_finished(),
        "shutdown must wait for the active dispatch boundary"
    );
    exec.release.notify_one();
    tokio::time::timeout(Duration::from_secs(1), worker)
        .await
        .expect("the command worker should stop after the active prompt settles")
        .expect("the command worker must not panic during shutdown");

    let active = repo.get("cmd_shutdown_active").await.unwrap().unwrap();
    assert_eq!(active.status, "done");
    let pre_fence = repo.get("cmd_shutdown_pre_fence").await.unwrap().unwrap();
    assert_eq!(pre_fence.status, "pending");
    assert_eq!(pre_fence.claimed_at, None);
    assert_eq!(pre_fence.started_at, None);
    assert_eq!(exec.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn graceful_shutdown_waits_for_a_fire_and_forget_provider_turn() {
    let exec = Arc::new(FireAndForgetPromptBarrier::default());
    let state = test_state_with_turn_exec(exec.clone()).await;
    let operator = state
        .identity
        .register(human_register("Alex Morgan", "ck_shutdown_turn_operator"))
        .await
        .unwrap();
    state
        .identity
        .register(human_register("Target Human", "ck_shutdown_turn_target"))
        .await
        .unwrap();
    CommandIntents::new(&state.store)
        .insert_pending(prompt_intent(
            "cmd_shutdown_fire_and_forget",
            &operator,
            "Alex Morgan",
            "ck_shutdown_turn_operator",
            "finish the provider turn before transport teardown",
            1,
        ))
        .await
        .unwrap();

    let worker = spawn(state.clone());
    tokio::time::timeout(Duration::from_secs(1), exec.prompt_entered.notified())
        .await
        .expect("the prompt must reach the fire-and-forget boundary");
    begin_shutdown(&state).await;
    tokio::time::timeout(Duration::from_secs(1), exec.wait_entered.notified())
        .await
        .expect("shutdown must await the active provider turn");
    assert!(
        !worker.is_finished(),
        "the worker must remain alive until provider completion"
    );

    exec.release.notify_one();
    tokio::time::timeout(Duration::from_secs(1), worker)
        .await
        .expect("the worker should finish after provider completion")
        .expect("the worker must not panic during shutdown");
    assert!(exec.active_turn_sessions().is_empty());
}

#[tokio::test]
async fn prompt_acceptance_permit_linearizes_before_shutdown_fence() {
    let exec = Arc::new(PromptDispatchBarrier::default());
    let state = test_state_with_turn_exec(exec.clone()).await;
    let operator = state
        .identity
        .register(human_register("Alex Morgan", "ck_prompt_fence_operator"))
        .await
        .unwrap();
    state
        .identity
        .register(human_register("Target Human", "ck_prompt_fence_target"))
        .await
        .unwrap();
    CommandIntents::new(&state.store)
        .insert_pending(prompt_intent(
            "cmd_prompt_fence",
            &operator,
            "Alex Morgan",
            "ck_prompt_fence_operator",
            "accept before shutdown",
            1,
        ))
        .await
        .unwrap();

    let worker_state = state.clone();
    let worker = tokio::spawn(async move {
        process_next_for_lane(&worker_state, WorkerLane::HarnessPrompt).await
    });
    tokio::time::timeout(Duration::from_secs(1), exec.entered.notified())
        .await
        .expect("prompt must enter its external acceptance boundary");

    begin_shutdown(&state).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !worker.is_finished(),
        "a prompt permitted before shutdown must retain its accepted-boundary task"
    );

    exec.release.notify_one();
    assert!(tokio::time::timeout(Duration::from_secs(1), worker)
        .await
        .expect("prompt worker should finish")
        .expect("prompt worker task must not panic")
        .unwrap());
}

#[tokio::test]
async fn graceful_shutdown_defers_a_prompt_claimed_before_its_actor_starts() {
    let exec = Arc::new(PromptDispatchBarrier::default());
    let state = test_state_with_turn_exec(exec.clone()).await;
    let operator = state
        .identity
        .register(human_register("Alex Morgan", "ck_shutdown_claim_operator"))
        .await
        .unwrap();
    let target = state
        .identity
        .register(human_register("Target Human", "ck_shutdown_claim_target"))
        .await
        .unwrap();
    let repo = CommandIntents::new(&state.store);
    repo.insert_pending(prompt_intent(
        "cmd_shutdown_claimed",
        &operator,
        "Alex Morgan",
        "ck_shutdown_claim_operator",
        "finish the owned dispatch",
        1,
    ))
    .await
    .unwrap();
    let row = claim_next_for_lane(&state, WorkerLane::HarnessPrompt)
        .await
        .unwrap()
        .expect("the prompt should be owned before shutdown starts");

    begin_shutdown(&state).await;
    run_harness_prompt_session_actor(state.clone(), target.session_id.0, row).await;

    let row = repo.get("cmd_shutdown_claimed").await.unwrap().unwrap();
    assert_eq!(row.status, "pending");
    assert_eq!(row.claimed_at, None);
    assert_eq!(row.started_at, None);
    assert_eq!(exec.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn explicit_steer_lane_bypasses_claimed_prompt_boundary() {
    let state = test_state().await;
    let alex = state
        .identity
        .register(human_register("Alex Morgan", "ck_operator"))
        .await
        .unwrap();
    state
        .identity
        .register(human_register("Target Human", "ck_target"))
        .await
        .unwrap();
    let repo = CommandIntents::new(&state.store);
    repo.insert_pending(prompt_intent(
        "cmd_prompt_boundary",
        &alex,
        "Alex Morgan",
        "ck_operator",
        "queue me",
        1,
    ))
    .await
    .unwrap();
    repo.insert_pending(steer_intent(
        "cmd_steer_now",
        &alex,
        "Alex Morgan",
        "ck_operator",
        2,
    ))
    .await
    .unwrap();

    let prompt = claim_next_for_lane(&state, WorkerLane::HarnessPrompt)
        .await
        .unwrap()
        .expect("prompt claims first");
    assert_eq!(prompt.command_id, "cmd_prompt_boundary");

    let steer = claim_next_for_lane(&state, WorkerLane::HarnessSteer)
        .await
        .unwrap()
        .expect("steer remains independently claimable");
    assert_eq!(steer.command_id, "cmd_steer_now");
}

#[tokio::test]
async fn durable_interrupt_runs_once_on_the_control_lane_with_the_authenticated_caller() {
    let exec = Arc::new(InterruptSpy::default());
    let state = test_state_with_turn_exec(exec.clone()).await;
    let operator = state
        .identity
        .register(human_register("Operator", "ck_interrupt_operator"))
        .await
        .unwrap();
    state
        .identity
        .register(human_register("Target Human", "ck_interrupt_target"))
        .await
        .unwrap();
    let repo = CommandIntents::new(&state.store);
    repo.insert_pending(NewCommandIntent {
        command_id: "cmd_interrupt_control".into(),
        kind: command_kinds::harness::INTERRUPT.into(),
        project: "presentation-only".into(),
        caller_name: "Operator".into(),
        caller_session_id: Some(operator.session_id.0.clone()),
        caller_agent_id: None,
        caller_runtime_id: Some(operator.session_id.0.clone()),
        caller_client_key: Some("ck_interrupt_operator".into()),
        caller_principal_id: None,
        caller_kind: Some("human".into()),
        caller_tier: Some("admin".into()),
        idempotency_key: Some("cm_interrupt_control".into()),
        request_json: serde_json::to_string(&InterruptRequest {
            expected_session_id: None,
            agent_id: None,
            name: "Target Human".into(),
            client_message_id: Some("cm_interrupt_control".into()),
        })
        .unwrap(),
        created_at: 1,
    })
    .await
    .unwrap();

    assert!(process_next_for_lane(&state, WorkerLane::Control)
        .await
        .unwrap());
    assert_eq!(exec.calls.load(Ordering::SeqCst), 1);
    let row = repo.get("cmd_interrupt_control").await.unwrap().unwrap();
    assert_eq!(row.status, "done");
    assert_eq!(row.caller_session_id, Some(operator.session_id.0));
    assert_eq!(row.caller_kind.as_deref(), Some("local.human"));
}

#[tokio::test]
async fn command_claims_wait_for_the_store_write_gate() {
    let state = test_state().await;
    let write_guard = state.store.write_lock().lock_owned().await;
    let claimant_state = state.clone();
    let claimant =
        tokio::spawn(
            async move { claim_next_for_lane(&claimant_state, WorkerLane::Control).await },
        );

    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(
        !claimant.is_finished(),
        "remote Hrana claims must not overlap another durable write section"
    );

    drop(write_guard);
    let claimed = tokio::time::timeout(Duration::from_millis(100), claimant)
        .await
        .expect("claim should continue after the durable write gate opens")
        .expect("claim task should not panic")
        .expect("claim query should succeed");
    assert!(claimed.is_none());

    let write_guard = state.store.write_lock().lock_owned().await;
    let actor_state = state.clone();
    let actor_claim = tokio::spawn(async move {
        claim_next_harness_prompt_for_session(&actor_state, "missing-session").await
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(
        !actor_claim.is_finished(),
        "per-session prompt actors must use the same durable claim gate"
    );
    drop(write_guard);
    let claimed = tokio::time::timeout(Duration::from_millis(100), actor_claim)
        .await
        .expect("actor claim should continue after the durable write gate opens")
        .expect("actor claim task should not panic")
        .expect("actor claim query should succeed");
    assert!(claimed.is_none());
}

#[tokio::test]
async fn idle_worker_wait_wakes_on_command_intent_signal_before_poll_interval() {
    let state = test_state().await;
    let epoch = state.store.command_intents_epoch();
    let waiter_state = state.clone();
    let wait = tokio::spawn(async move {
        wait_for_command_intent_or_poll(&waiter_state, epoch).await;
    });

    tokio::time::sleep(Duration::from_millis(5)).await;
    CommandIntents::new(&state.store)
        .insert_pending(NewCommandIntent {
            command_id: "cmd_signal".into(),
            kind: command_kinds::identity::REGISTER.into(),
            project: "default".into(),
            caller_name: "signal-test".into(),
            caller_session_id: None,
            caller_agent_id: None,
            caller_runtime_id: None,
            caller_client_key: None,
            caller_principal_id: None,
            caller_kind: None,
            caller_tier: None,
            idempotency_key: None,
            request_json: "{}".into(),
            created_at: now(),
        })
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_millis(75), wait)
        .await
        .expect("command-intent signal should wake before the 250ms fallback poll")
        .expect("wait task should not panic");
}

#[tokio::test]
async fn harness_prompt_timeout_marks_error_and_unblocks_next_prompt() {
    let exec = Arc::new(FirstPromptHangs {
        calls: AtomicUsize::new(0),
    });
    let state = test_state_with_turn_exec(exec).await;
    let alex = state
        .identity
        .register(human_register("Alex Morgan", "ck_operator"))
        .await
        .unwrap();
    state
        .identity
        .register(human_register("Target Human", "ck_target"))
        .await
        .unwrap();

    let repo = CommandIntents::new(&state.store);
    repo.insert_pending(prompt_intent(
        "cmd_hung_prompt",
        &alex,
        "Alex Morgan",
        "ck_operator",
        "first hangs",
        1,
    ))
    .await
    .unwrap();
    repo.insert_pending(prompt_intent(
        "cmd_next_prompt",
        &alex,
        "Alex Morgan",
        "ck_operator",
        "second runs",
        2,
    ))
    .await
    .unwrap();

    assert!(process_next_for_lane(&state, WorkerLane::HarnessPrompt)
        .await
        .unwrap());
    let first = repo.get("cmd_hung_prompt").await.unwrap().unwrap();
    assert_eq!(first.status, "error");
    assert!(first
        .error_json
        .as_deref()
        .unwrap()
        .contains("harness.prompt command cmd_hung_prompt timed out"));

    assert!(process_next_for_lane(&state, WorkerLane::HarnessPrompt)
        .await
        .unwrap());
    let second = repo.get("cmd_next_prompt").await.unwrap().unwrap();
    assert_eq!(second.status, "done");
}

#[tokio::test]
async fn registered_agent_id_is_exclusive_and_cannot_alias_hop_during_command_auth() {
    let state = test_state().await;
    state
        .store
        .identity_conn()
        .execute(
            "INSERT INTO agents (agent_id, project, name, tier, created_at) \
             VALUES ('a_real', 'identity-metadata', 'real-agent', 'agent', 1)",
            (),
        )
        .await
        .unwrap();
    let registered = state
        .identity
        .register(human_register("temporary-human", "ck_orphan_agent"))
        .await
        .unwrap();
    state
        .store
        .conn
        .execute(
            "UPDATE sessions SET agent_id = 'a_orphan', name = 'a_real', \
             kind = 'agent', tier = 'agent' WHERE session_id = ?1",
            libsql::params![registered.session_id.0.clone()],
        )
        .await
        .unwrap();
    let repo = CommandIntents::new(&state.store);
    repo.insert_pending(NewCommandIntent {
        command_id: "cmd_orphan_agent".into(),
        kind: command_kinds::message_post::SEND.into(),
        project: "caller-metadata".into(),
        caller_name: "a_real".into(),
        caller_session_id: Some(registered.session_id.0.clone()),
        caller_agent_id: Some("a_orphan".into()),
        caller_runtime_id: Some(registered.session_id.0.clone()),
        caller_client_key: Some("ck_orphan_agent".into()),
        caller_principal_id: None,
        caller_kind: Some("agent".into()),
        caller_tier: Some("agent".into()),
        idempotency_key: None,
        request_json: "{}".into(),
        created_at: 1,
    })
    .await
    .unwrap();
    let orphan = repo.get("cmd_orphan_agent").await.unwrap().unwrap();

    let error = resolve_command_caller(&state, &orphan)
        .await
        .expect_err("an orphan session agent id must not resolve through its mutable name");
    assert_eq!(error.code, codes::UNAUTHORIZED);
    assert!(error.message.contains("not a durable identity"));

    state
        .store
        .conn
        .execute(
            "UPDATE sessions SET agent_id = 'a_real' WHERE session_id = ?1",
            libsql::params![registered.session_id.0.clone()],
        )
        .await
        .unwrap();
    let mut mismatched = orphan;
    mismatched.caller_agent_id = Some("a_other".into());
    let error = resolve_command_caller(&state, &mismatched)
        .await
        .expect_err("legacy command evidence must match the registered stable id");
    assert_eq!(error.code, codes::UNAUTHORIZED);
    assert!(error.message.contains("caller agent does not match"));
}

#[tokio::test]
async fn human_command_auth_never_resolves_its_display_name_as_an_agent() {
    let state = test_state().await;
    Agents::new(&state.store)
        .create(NewAgent {
            agent_id: "a_same_human_label".into(),
            project: "agent-metadata".into(),
            name: Some("browser-user".into()),
            default_harness: Some("claude".into()),
            role: None,
            tier: Some("agent".into()),
            owner: None,
        })
        .await
        .unwrap();
    let agent_session = SessionId("s_same_human_label_agent".into());
    Sessions::new(&state.store)
        .create(NewSession {
            session_id: agent_session.clone(),
            name: Some("agent-runtime-label".into()),
            agent: Some("claude".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: None,
            client_key: Some("ck_same_human_label_agent".into()),
            cwd: None,
            project: "agent-metadata".into(),
            transport: Some("pty".into()),
        })
        .await
        .unwrap();
    Sessions::new(&state.store)
        .set_agent_id(&agent_session, "a_same_human_label")
        .await
        .unwrap();
    let human = state
        .identity
        .register(human_register("browser-user", "ck_browser_user"))
        .await
        .unwrap();
    assert_eq!(human.agent_id, None);
    Sessions::new(&state.store)
        .set_agent_id(&human.session_id, "a_same_human_label")
        .await
        .unwrap();
    let intent = NewCommandIntent {
        command_id: "cmd_human_alias_boundary".into(),
        kind: command_kinds::message_post::SEND.into(),
        project: "caller-metadata".into(),
        caller_name: "browser-user".into(),
        caller_session_id: Some(human.session_id.0.clone()),
        caller_agent_id: Some("a_same_human_label".into()),
        caller_runtime_id: Some(human.session_id.0.clone()),
        caller_client_key: Some("ck_browser_user".into()),
        caller_principal_id: None,
        caller_kind: Some("human".into()),
        caller_tier: Some("admin".into()),
        idempotency_key: None,
        request_json: "{}".into(),
        created_at: 1,
    };
    CommandIntents::new(&state.store)
        .insert_pending(intent)
        .await
        .unwrap();
    let row = CommandIntents::new(&state.store)
        .get("cmd_human_alias_boundary")
        .await
        .unwrap()
        .unwrap();

    let caller = resolve_command_caller(&state, &row).await.unwrap();

    assert_eq!(caller.session, human.session_id);
    assert_eq!(caller.name, "browser-user");
    assert_eq!(caller.agent_id, None);
}

#[tokio::test]
async fn gateway_transport_external_principal_is_a_caller_without_native_session_identity() {
    let state = test_state().await;
    Agents::new(&state.store)
        .create(NewAgent {
            agent_id: "a_outside_alias".into(),
            project: "default".into(),
            name: Some("outside-user".into()),
            default_harness: None,
            role: None,
            tier: Some("agent".into()),
            owner: None,
        })
        .await
        .unwrap();
    let repo = CommandIntents::new(&state.store);
    repo.insert_pending(NewCommandIntent {
        command_id: "cmd_external_transport".into(),
        kind: command_kinds::message_post::SEND.into(),
        project: "default".into(),
        caller_name: "outside-user".into(),
        caller_session_id: Some("transport:telegram".into()),
        caller_agent_id: None,
        caller_runtime_id: Some("transport:telegram".into()),
        caller_client_key: None,
        caller_principal_id: Some("x_outside".into()),
        caller_kind: Some("external.human".into()),
        caller_tier: Some("agent".into()),
        idempotency_key: Some("transport:telegram:update-1".into()),
        request_json: "{}".into(),
        created_at: 1,
    })
    .await
    .unwrap();
    let row = repo
        .get("cmd_external_transport")
        .await
        .unwrap()
        .expect("external transport command");

    let caller = resolve_command_caller(&state, &row)
        .await
        .expect("gateway-authenticated external principal");

    assert_eq!(caller.agent_id, None);
    assert_eq!(caller.session, SessionId("transport:telegram".into()));
    assert_eq!(caller.name, "outside-user");
    assert_eq!(caller.locality, Locality::External);
    assert_eq!(caller.access.as_deref(), Some("guest"));
    assert_eq!(caller.principal_id.as_deref(), Some("x_outside"));
}

#[tokio::test]
async fn no_other_client_keyless_shape_can_enter_the_gateway_transport_principal_lane() {
    let state = test_state().await;
    let repo = CommandIntents::new(&state.store);
    repo.insert_pending(NewCommandIntent {
        command_id: "cmd_external_shape_base".into(),
        kind: command_kinds::message_post::SEND.into(),
        project: "default".into(),
        caller_name: "outside-user".into(),
        caller_session_id: Some("transport:telegram".into()),
        caller_agent_id: None,
        caller_runtime_id: Some("transport:telegram".into()),
        caller_client_key: None,
        caller_principal_id: Some("x_outside".into()),
        caller_kind: Some("external.human".into()),
        caller_tier: Some("agent".into()),
        idempotency_key: None,
        request_json: "{}".into(),
        created_at: 1,
    })
    .await
    .unwrap();
    let base = repo
        .get("cmd_external_shape_base")
        .await
        .unwrap()
        .expect("shape base");

    let mut invalid = Vec::new();
    let mut row = base.clone();
    row.caller_kind = Some("external.agent".into());
    invalid.push(("external agent", row));
    let mut row = base.clone();
    row.caller_kind = Some("local.human".into());
    invalid.push(("local human", row));
    let mut row = base.clone();
    row.caller_kind = Some("trusted.human".into());
    invalid.push(("trusted human", row));
    let mut row = base.clone();
    row.caller_principal_id = Some("h_local".into());
    invalid.push(("non-external principal", row));
    let mut row = base.clone();
    row.caller_session_id = Some("source:telegram".into());
    row.caller_runtime_id = row.caller_session_id.clone();
    invalid.push(("non-transport session", row));
    let mut row = base.clone();
    row.kind = command_kinds::thread::CREATE.into();
    invalid.push(("non-message command", row));

    for (label, row) in invalid {
        let error = resolve_command_caller(&state, &row).await.expect_err(label);
        assert_eq!(error.code, codes::UNAUTHORIZED, "{label}");
        assert!(error.message.contains("verified client key"), "{label}");
    }
}

#[tokio::test]
async fn daemon_accepted_human_command_survives_only_a_real_boot_change() {
    let state = test_state().await;
    DaemonState::new(&state.store)
        .set_boot_epoch("boot_accept", 1)
        .await
        .unwrap();
    let human = state
        .identity
        .register(human_register("restart-human", "ck_restart_human"))
        .await
        .unwrap();
    let response = crate::daemon::daemon_ipc::handle_request(
        &state,
        "boot-token",
        DaemonIpcRequest {
            version: DAEMON_IPC_PROTOCOL_VERSION,
            token: "boot-token".into(),
            request_id: "rpc-restart-human".into(),
            caller: Some(DaemonIpcCaller {
                name: Some("restart-human".into()),
                project: "default".into(),
                session_id: Some(human.session_id.0.clone()),
                agent_id: None,
                runtime_id: Some(human.session_id.0.clone()),
                client_key: Some("ck_restart_human".into()),
                kind: Kind::Human,
                locality: Locality::Local,
                access: None,
                principal_id: None,
                tier: Tier::Admin,
            }),
            call: DaemonIpcCall::Enqueue {
                command_id: "cmd-restart-human".into(),
                kind: command_kinds::thread::CREATE.into(),
                params: serde_json::json!({"name": "restart-boundary", "members": []}),
                idempotency_key: None,
            },
        },
    )
    .await;
    assert!(response.error.is_none(), "{:?}", response.error);
    let row = CommandIntents::new(&state.store)
        .get("cmd-restart-human")
        .await
        .unwrap()
        .expect("daemon-accepted command");

    state
        .store
        .conn
        .execute(
            "DELETE FROM sessions WHERE session_id = ?1",
            libsql::params![human.session_id.0.clone()],
        )
        .await
        .unwrap();
    let same_boot = resolve_command_caller(&state, &row)
        .await
        .expect_err("same-boot session loss must remain unauthorized");
    assert_eq!(same_boot.code, nexus_contracts::codes::UNAUTHORIZED);

    DaemonState::new(&state.store)
        .set_boot_epoch("boot_restarted", 2)
        .await
        .unwrap();
    let rebound = state
        .identity
        .register(human_register("restart-human", "ck_restart_human"))
        .await
        .unwrap();
    assert_ne!(
        rebound.session_id, human.session_id,
        "a fresh daemon registration must exercise the new-session boundary"
    );
    let caller = resolve_command_caller(&state, &row)
        .await
        .expect("daemon-validated human authority must survive a boot change");
    assert_eq!(caller.name, "restart-human");
    assert_eq!(caller.session, human.session_id);
    assert_eq!(caller.agent_id, None);
    assert_eq!(caller.project, "default");
    assert_eq!(caller.tier, Tier::Admin);

    let forged = NewCommandIntent {
        command_id: "cmd-forged-restart-human".into(),
        kind: command_kinds::thread::CREATE.into(),
        project: "default".into(),
        caller_name: "restart-human".into(),
        caller_session_id: Some(human.session_id.0.clone()),
        caller_agent_id: None,
        caller_runtime_id: Some(human.session_id.0.clone()),
        caller_client_key: Some("ck_restart_human".into()),
        caller_principal_id: None,
        caller_kind: Some("human".into()),
        caller_tier: Some("admin".into()),
        idempotency_key: None,
        request_json: serde_json::json!({"name": "forged-restart", "members": []}).to_string(),
        created_at: 2,
    };
    CommandIntents::new(&state.store)
        .insert_pending(forged)
        .await
        .unwrap();
    let forged = CommandIntents::new(&state.store)
        .get("cmd-forged-restart-human")
        .await
        .unwrap()
        .unwrap();
    let error = resolve_command_caller(&state, &forged)
        .await
        .expect_err("an unattested durable row must not gain restart authority");
    assert_eq!(error.code, nexus_contracts::codes::UNAUTHORIZED);
}

#[tokio::test]
async fn split_daemon_restart_executes_the_pre_restart_validated_human_command() {
    let path = std::env::temp_dir().join(format!(
        "nexus-command-caller-restart-{}-{}.db",
        std::process::id(),
        now()
    ));
    let human_session;
    {
        let daemon = DaemonStore::open(path.to_string_lossy().as_ref())
            .await
            .unwrap();
        let state = AppState::wire(Arc::new(daemon.compatibility_store()), &Config::default());
        DaemonState::new(&state.store)
            .set_boot_epoch("boot_split_accept", 1)
            .await
            .unwrap();
        let human = state
            .identity
            .register(human_register(
                "split-restart-human",
                "ck_split_restart_human",
            ))
            .await
            .unwrap();
        human_session = human.session_id.0.clone();
        let response = crate::daemon::daemon_ipc::handle_request(
            &state,
            "boot-token",
            DaemonIpcRequest {
                version: DAEMON_IPC_PROTOCOL_VERSION,
                token: "boot-token".into(),
                request_id: "rpc-split-restart-human".into(),
                caller: Some(DaemonIpcCaller {
                    name: Some("split-restart-human".into()),
                    project: "default".into(),
                    session_id: Some(human_session.clone()),
                    agent_id: None,
                    runtime_id: Some(human_session.clone()),
                    client_key: Some("ck_split_restart_human".into()),
                    kind: Kind::Human,
                    locality: Locality::Local,
                    access: None,
                    principal_id: None,
                    tier: Tier::Admin,
                }),
                call: DaemonIpcCall::Enqueue {
                    command_id: "cmd-split-restart-human".into(),
                    kind: command_kinds::thread::CREATE.into(),
                    params: serde_json::json!({
                        "name": "split-restart-boundary",
                        "members": []
                    }),
                    idempotency_key: None,
                },
            },
        )
        .await;
        assert!(response.error.is_none(), "{:?}", response.error);
        let row = CommandIntents::new(&state.store)
            .get("cmd-split-restart-human")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            row.caller_validated_boot_epoch.as_deref(),
            Some("boot_split_accept")
        );
    }

    {
        let daemon = DaemonStore::open(path.to_string_lossy().as_ref())
            .await
            .unwrap();
        let state = AppState::wire(Arc::new(daemon.compatibility_store()), &Config::default());
        DaemonState::new(&state.store)
            .set_boot_epoch("boot_split_restarted", 2)
            .await
            .unwrap();
        assert!(Sessions::new(&state.store)
            .find_by_session_id(&SessionId(human_session.clone()))
            .await
            .unwrap()
            .is_none());
        assert!(process_next(&state).await.unwrap());
        let row = CommandIntents::new(&state.store)
            .get("cmd-split-restart-human")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.status, "done", "{:?}", row.error_json);
    }

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}

#[tokio::test]
async fn maintenance_reaps_terminal_command_intents_by_retention() {
    let config = Config {
        // Keep a wide wall-clock margin: the full workspace runs this async test alongside many
        // other targets, so a 50 ms "recent" row can legitimately age past a 100 ms cutoff before
        // maintenance executes. The contract under test is relative ordering, not scheduler speed.
        command_intent_retention_ms: 10_000,
        ..Config::default()
    };
    let state = test_state_with_config(
        config,
        Arc::new(FirstPromptHangs {
            calls: AtomicUsize::new(0),
        }),
    )
    .await;
    let repo = CommandIntents::new(&state.store);
    let ts = now();
    let alex = state
        .identity
        .register(human_register("Alex Morgan", "ck_operator"))
        .await
        .unwrap();

    repo.insert_pending(prompt_intent(
        "cmd_old_done",
        &alex,
        "Alex Morgan",
        "ck_operator",
        "old done",
        ts - 1_000,
    ))
    .await
    .unwrap();
    repo.insert_pending(prompt_intent(
        "cmd_recent_error",
        &alex,
        "Alex Morgan",
        "ck_operator",
        "recent error",
        ts - 1_000,
    ))
    .await
    .unwrap();
    repo.insert_pending(prompt_intent(
        "cmd_pending",
        &alex,
        "Alex Morgan",
        "ck_operator",
        "pending",
        ts - 1_000,
    ))
    .await
    .unwrap();
    repo.mark_done("cmd_old_done", r#"{"ok":true}"#, ts - 20_000)
        .await
        .unwrap();
    repo.mark_error("cmd_recent_error", r#"{"message":"recent"}"#, ts - 50)
        .await
        .unwrap();

    let mut retention_state = RetentionPolicyState::default();
    maybe_reap_operational_tables(&state, &mut retention_state)
        .await
        .unwrap();

    assert!(repo.get("cmd_old_done").await.unwrap().is_none());
    assert_eq!(
        repo.get("cmd_recent_error").await.unwrap().unwrap().status,
        "error"
    );
    assert_eq!(
        repo.get("cmd_pending").await.unwrap().unwrap().status,
        "pending"
    );
}

/// A validated [`nexus_contracts::HarnessId`] from a literal (panics on invalid — test-only).
fn hid(s: &str) -> nexus_contracts::HarnessId {
    nexus_contracts::HarnessId::new(s).expect("valid harness id literal")
}
