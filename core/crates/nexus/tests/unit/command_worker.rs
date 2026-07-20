use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use nexus_common::{Config, HookGatewayMode};
use nexus_contracts::{
    AgentTurnExecutionPort, InterruptRequest, Kind, NexusBatch, NotifySendRequest, NotifyTarget,
    PortResult, PromptRequest, RegisterRequest, RemoveRequest, RemoveResponse, SessionId,
    SpawnRequest, SpawnResponse, SteerRequest, Tier,
};
use nexus_store::command_kinds;
use nexus_store::repos::{
    Agents, CommandIntents, InboxSubscriptions, NewAgent, NewCommandIntent, NewInboxSubscription,
    NewSession, Sessions,
};
use nexus_store::Store;

use super::*;

struct FirstPromptHangs {
    calls: AtomicUsize,
}

#[derive(Default)]
struct InterruptSpy {
    calls: AtomicUsize,
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
        caller_kind: Some("human".into()),
        caller_tier: Some("admin".into()),
        idempotency_key: None,
        request_json: serde_json::to_string(&PromptRequest {
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
        caller_kind: Some("human".into()),
        caller_tier: Some("admin".into()),
        idempotency_key: Some("cm_interrupt_control".into()),
        request_json: serde_json::to_string(&InterruptRequest {
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
