use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use nexus::daemon::{command_worker, AppState};
use nexus_common::Config;
use nexus_contracts::{
    AgentTurnExecutionPort, ConsumeRequest, Harness, InboxSubscriptionNextRequest, Kind,
    NexusBatch, PortResult, PromptRequest, RegisterRequest, RemoveRequest, RemoveResponse,
    SendRequest, SendTarget, SessionId, SpawnRequest, SpawnResponse, Tier,
};
use nexus_store::command_kinds;
use nexus_store::repos::{
    caller_subscription_id, CommandIntents, InboxSubscriptions, NewCommandIntent,
    NewInboxSubscription,
};
use nexus_store::Store;

async fn test_state() -> AppState {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    AppState::wire(store, &Config::default())
}

async fn test_state_with_turn_exec(turn_exec: Arc<dyn AgentTurnExecutionPort>) -> AppState {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    AppState::wire_with_turn_exec(store, &Config::default(), turn_exec)
}

struct SlowPromptExec {
    delay: Duration,
}

#[async_trait::async_trait]
impl AgentTurnExecutionPort for SlowPromptExec {
    async fn inject_turn(&self, _recipient: &SessionId, _batch: &NexusBatch) -> PortResult<()> {
        Ok(())
    }

    async fn launch(&self, _req: SpawnRequest) -> PortResult<SpawnResponse> {
        unimplemented!("command-worker starvation tests do not launch harnesses")
    }

    async fn remove(&self, _req: RemoveRequest) -> PortResult<RemoveResponse> {
        unimplemented!("command-worker starvation tests do not remove harnesses")
    }

    async fn prompt(&self, _recipient: &SessionId, _text: String) -> PortResult<()> {
        tokio::time::sleep(self.delay).await;
        Ok(())
    }
}

struct ObservedPromptExec {
    delay: Duration,
    active: AtomicUsize,
    max_active: AtomicUsize,
    starts: Mutex<Vec<(String, String)>>,
}

impl ObservedPromptExec {
    fn new(delay: Duration) -> Self {
        Self {
            delay,
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
            starts: Mutex::new(Vec::new()),
        }
    }

    fn max_active(&self) -> usize {
        self.max_active.load(Ordering::SeqCst)
    }

    fn starts(&self) -> Vec<(String, String)> {
        self.starts.lock().unwrap().clone()
    }

    fn observe_start(&self) {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        let mut current = self.max_active.load(Ordering::SeqCst);
        while active > current {
            match self.max_active.compare_exchange(
                current,
                active,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(next) => current = next,
            }
        }
    }
}

#[async_trait::async_trait]
impl AgentTurnExecutionPort for ObservedPromptExec {
    async fn inject_turn(&self, _recipient: &SessionId, _batch: &NexusBatch) -> PortResult<()> {
        Ok(())
    }

    async fn launch(&self, _req: SpawnRequest) -> PortResult<SpawnResponse> {
        unimplemented!("command-worker starvation tests do not launch harnesses")
    }

    async fn remove(&self, _req: RemoveRequest) -> PortResult<RemoveResponse> {
        unimplemented!("command-worker starvation tests do not remove harnesses")
    }

    async fn prompt(&self, recipient: &SessionId, text: String) -> PortResult<()> {
        self.observe_start();
        self.starts
            .lock()
            .unwrap()
            .push((recipient.0.clone(), text.clone()));
        if text.contains("stuck") {
            tokio::time::sleep(Duration::from_secs(60)).await;
        } else {
            tokio::time::sleep(self.delay).await;
        }
        self.active.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    }
}

fn human_register(name: &str, client_key: &str) -> RegisterRequest {
    RegisterRequest {
        agent_id: None,
        name: Some(name.into()),
        harness: Harness::Other,
        harness_session_id: format!("hs_{client_key}"),
        project: "default".into(),
        client_key: client_key.into(),
        runtime_credential: None,
        tier: Tier::Admin,
        kind: Some(Kind::Human),
        role: None,
        cwd: None,
    }
}

fn command_intent(
    command_id: &str,
    kind: &str,
    caller: &nexus_contracts::RegisterResponse,
    caller_name: &str,
    caller_client_key: &str,
    request_json: String,
    created_at: i64,
) -> NewCommandIntent {
    NewCommandIntent {
        command_id: command_id.into(),
        kind: kind.into(),
        project: "default".into(),
        caller_name: caller_name.into(),
        caller_session_id: Some(caller.session_id.0.clone()),
        caller_agent_id: caller.agent_id.as_ref().map(|id| id.0.clone()),
        caller_runtime_id: Some(caller.session_id.0.clone()),
        caller_client_key: Some(caller_client_key.into()),
        caller_kind: Some("human".into()),
        caller_tier: Some("admin".into()),
        idempotency_key: None,
        request_json,
        created_at,
    }
}

fn prompt_command(
    command_id: &str,
    caller: &nexus_contracts::RegisterResponse,
    caller_name: &str,
    caller_client_key: &str,
    target: &str,
    text: &str,
    created_at: i64,
) -> NewCommandIntent {
    command_intent(
        command_id,
        command_kinds::harness::PROMPT,
        caller,
        caller_name,
        caller_client_key,
        serde_json::to_string(&PromptRequest {
            agent_id: None,
            name: target.into(),
            text: text.into(),
            client_message_id: None,
        })
        .unwrap(),
        created_at,
    )
}

async fn wait_for_status(state: &AppState, command_id: &str, status: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        let row = CommandIntents::new(&state.store)
            .get(command_id)
            .await
            .unwrap()
            .expect("command row exists");
        if row.status == status {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {command_id} to become {status}; last status was {}",
            row.status
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn held_inbox_consume_does_not_starve_message_post_send() {
    let state = test_state().await;
    let alex = state
        .identity
        .register(human_register("Alex Morgan", "ck_operator"))
        .await
        .unwrap();
    state
        .identity
        .register(human_register("Blake Human", "ck_blake"))
        .await
        .unwrap();

    CommandIntents::new(&state.store)
        .insert_pending(command_intent(
            "cmd_long_consume",
            command_kinds::inbox::CONSUME,
            &alex,
            "Alex Morgan",
            "ck_operator",
            serde_json::to_string(&ConsumeRequest {
                timeout_ms: Some(1_500),
                max: None,
            })
            .unwrap(),
            1,
        ))
        .await
        .unwrap();

    let worker = command_worker::spawn(state.clone());
    wait_for_status(
        &state,
        "cmd_long_consume",
        "claimed",
        Duration::from_millis(800),
    )
    .await;

    CommandIntents::new(&state.store)
        .insert_pending(command_intent(
            "cmd_send",
            command_kinds::message_post::SEND,
            &alex,
            "Alex Morgan",
            "ck_operator",
            serde_json::to_string(&SendRequest {
                to: SendTarget::dm_name("Blake Human"),
                summary: None,
                body: "send should not wait for long consume".into(),
                mention: vec![],
                idempotency_key: None,
            })
            .unwrap(),
            2,
        ))
        .await
        .unwrap();

    wait_for_status(&state, "cmd_send", "done", Duration::from_millis(800)).await;
    worker.abort();
}

#[tokio::test]
async fn held_inbox_subscription_next_does_not_starve_message_post_send() {
    let state = test_state().await;
    let alex = state
        .identity
        .register(human_register("Alex Morgan", "ck_operator"))
        .await
        .unwrap();
    state
        .identity
        .register(human_register("Blake Human", "ck_blake"))
        .await
        .unwrap();
    let subscription_id = caller_subscription_id("default", &alex.session_id.0);
    InboxSubscriptions::new(&state.store)
        .upsert_active(NewInboxSubscription {
            subscription_id: subscription_id.clone(),
            project: "default".into(),
            caller_name: "Alex Morgan".into(),
            caller_session_id: alex.session_id.0.clone(),
            caller_agent_id: alex.agent_id.as_ref().map(|id| id.0.clone()),
            caller_client_key: Some("ck_operator".into()),
            timeout_ms: Some(1_500),
            max: None,
            created_at: 1,
        })
        .await
        .unwrap();

    CommandIntents::new(&state.store)
        .insert_pending(command_intent(
            "cmd_long_subscription_next",
            command_kinds::inbox::SUBSCRIPTION_NEXT,
            &alex,
            "Alex Morgan",
            "ck_operator",
            serde_json::to_string(&InboxSubscriptionNextRequest {
                subscription_id,
                timeout_ms: Some(1_500),
            })
            .unwrap(),
            1,
        ))
        .await
        .unwrap();

    let worker = command_worker::spawn(state.clone());
    wait_for_status(
        &state,
        "cmd_long_subscription_next",
        "claimed",
        Duration::from_millis(800),
    )
    .await;

    CommandIntents::new(&state.store)
        .insert_pending(command_intent(
            "cmd_send_while_subscription_waits",
            command_kinds::message_post::SEND,
            &alex,
            "Alex Morgan",
            "ck_operator",
            serde_json::to_string(&SendRequest {
                to: SendTarget::dm_name("Blake Human"),
                summary: None,
                body: "send should not wait for subscription next".into(),
                mention: vec![],
                idempotency_key: None,
            })
            .unwrap(),
            2,
        ))
        .await
        .unwrap();

    wait_for_status(
        &state,
        "cmd_send_while_subscription_waits",
        "done",
        Duration::from_millis(800),
    )
    .await;
    worker.abort();
}

#[tokio::test]
async fn long_inbox_consume_is_bounded_below_command_lease() {
    let state = test_state().await;
    let alex = state
        .identity
        .register(human_register("Alex Morgan", "ck_operator"))
        .await
        .unwrap();

    CommandIntents::new(&state.store)
        .insert_pending(command_intent(
            "cmd_oversized_consume",
            command_kinds::inbox::CONSUME,
            &alex,
            "Alex Morgan",
            "ck_operator",
            serde_json::to_string(&ConsumeRequest {
                timeout_ms: Some(60_000),
                max: None,
            })
            .unwrap(),
            1,
        ))
        .await
        .unwrap();

    let result = tokio::time::timeout(
        Duration::from_millis(1_500),
        command_worker::process_next(&state),
    )
    .await;
    assert!(
        result.is_ok(),
        "a caller-requested long poll must be sliced below the command lease"
    );
    assert!(result.unwrap().unwrap());

    let row = CommandIntents::new(&state.store)
        .get("cmd_oversized_consume")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.status, "done");
}

#[tokio::test]
async fn inbox_lane_expires_dead_keyed_consumer_before_reclaiming() {
    let state = test_state().await;
    state
        .store
        .conn
        .execute(
            "INSERT INTO sessions (session_id, name, kind, tier, project, presence, paused, \
             client_key, last_heartbeat, created_at) VALUES ('s_dead_consumer', 'dead-consumer', \
             'agent', 'agent', 'default', 'online', 0, 'ck_dead_consumer', 1, 1)",
            (),
        )
        .await
        .unwrap();
    let repo = CommandIntents::new(&state.store);
    repo.insert_pending(NewCommandIntent {
        command_id: "cmd_dead_keyed_consume".into(),
        kind: command_kinds::inbox::CONSUME.into(),
        project: "default".into(),
        caller_name: "dead-consumer".into(),
        caller_session_id: None,
        caller_agent_id: None,
        caller_runtime_id: None,
        caller_client_key: Some("ck_dead_consumer".into()),
        caller_kind: Some("agent".into()),
        caller_tier: Some("agent".into()),
        idempotency_key: None,
        request_json: serde_json::to_string(&ConsumeRequest {
            timeout_ms: Some(60_000),
            max: None,
        })
        .unwrap(),
        created_at: 1,
    })
    .await
    .unwrap();
    repo.claim_next_kind(10, 1, command_kinds::inbox::CONSUME)
        .await
        .unwrap()
        .unwrap();

    let worker = command_worker::spawn(state.clone());
    wait_for_status(
        &state,
        "cmd_dead_keyed_consume",
        "error",
        Duration::from_millis(800),
    )
    .await;
    let row = repo.get("cmd_dead_keyed_consume").await.unwrap().unwrap();
    assert!(row
        .error_json
        .as_deref()
        .unwrap()
        .contains("stale inbox consumer"));
    worker.abort();
}

#[tokio::test]
async fn expired_claimed_control_command_does_not_block_later_message_post_send() {
    let state = test_state().await;
    let alex = state
        .identity
        .register(human_register("Alex Morgan", "ck_operator"))
        .await
        .unwrap();
    state
        .identity
        .register(human_register("Blake Human", "ck_blake"))
        .await
        .unwrap();

    let repo = CommandIntents::new(&state.store);
    repo.insert_pending(command_intent(
        "cmd_stale_prompt",
        command_kinds::harness::PROMPT,
        &alex,
        "Alex Morgan",
        "ck_operator",
        serde_json::to_string(&PromptRequest {
            agent_id: None,
            name: "missing-agent".into(),
            text: "this prompt was claimed by a worker that died".into(),
            client_message_id: None,
        })
        .unwrap(),
        1,
    ))
    .await
    .unwrap();
    repo.claim_next(10, 1).await.unwrap().unwrap();

    repo.insert_pending(command_intent(
        "cmd_send_after_stale_claim",
        command_kinds::message_post::SEND,
        &alex,
        "Alex Morgan",
        "ck_operator",
        serde_json::to_string(&SendRequest {
            to: SendTarget::dm_name("Blake Human"),
            summary: None,
            body: "send should not wait behind an expired control claim".into(),
            mention: vec![],
            idempotency_key: None,
        })
        .unwrap(),
        2,
    ))
    .await
    .unwrap();

    let worker = command_worker::spawn(state.clone());
    wait_for_status(
        &state,
        "cmd_stale_prompt",
        "error",
        Duration::from_millis(800),
    )
    .await;
    wait_for_status(
        &state,
        "cmd_send_after_stale_claim",
        "done",
        Duration::from_millis(800),
    )
    .await;
    worker.abort();
}

#[tokio::test]
async fn held_harness_prompt_does_not_starve_message_post_send() {
    let state = test_state_with_turn_exec(Arc::new(SlowPromptExec {
        delay: Duration::from_millis(1_500),
    }))
    .await;
    let alex = state
        .identity
        .register(human_register("Alex Morgan", "ck_operator"))
        .await
        .unwrap();
    state
        .identity
        .register(human_register("Bianca", "ck_bianca"))
        .await
        .unwrap();
    state
        .identity
        .register(human_register("Blake Human", "ck_blake"))
        .await
        .unwrap();

    CommandIntents::new(&state.store)
        .insert_pending(command_intent(
            "cmd_long_prompt",
            command_kinds::harness::PROMPT,
            &alex,
            "Alex Morgan",
            "ck_operator",
            serde_json::to_string(&PromptRequest {
                agent_id: None,
                name: "Bianca".into(),
                text: "long model turn".into(),
                client_message_id: None,
            })
            .unwrap(),
            1,
        ))
        .await
        .unwrap();

    let worker = command_worker::spawn(state.clone());
    wait_for_status(
        &state,
        "cmd_long_prompt",
        "claimed",
        Duration::from_millis(800),
    )
    .await;

    CommandIntents::new(&state.store)
        .insert_pending(command_intent(
            "cmd_send_while_prompt_runs",
            command_kinds::message_post::SEND,
            &alex,
            "Alex Morgan",
            "ck_operator",
            serde_json::to_string(&SendRequest {
                to: SendTarget::dm_name("Blake Human"),
                summary: None,
                body: "send should not wait for a long prompt turn".into(),
                mention: vec![],
                idempotency_key: None,
            })
            .unwrap(),
            2,
        ))
        .await
        .unwrap();

    wait_for_status(
        &state,
        "cmd_send_while_prompt_runs",
        "done",
        Duration::from_millis(800),
    )
    .await;
    worker.abort();
}

#[tokio::test]
async fn harness_prompts_for_different_sessions_overlap() {
    let exec = Arc::new(ObservedPromptExec::new(Duration::from_millis(20)));
    let state = test_state_with_turn_exec(exec.clone()).await;
    let alex = state
        .identity
        .register(human_register("Alex Morgan", "ck_operator"))
        .await
        .unwrap();
    state
        .identity
        .register(human_register("Ada", "ck_ada"))
        .await
        .unwrap();
    state
        .identity
        .register(human_register("Blake", "ck_blake"))
        .await
        .unwrap();

    let repo = CommandIntents::new(&state.store);
    repo.insert_pending(prompt_command(
        "cmd_ada_prompt",
        &alex,
        "Alex Morgan",
        "ck_operator",
        "Ada",
        "first",
        1,
    ))
    .await
    .unwrap();
    repo.insert_pending(prompt_command(
        "cmd_blake_prompt",
        &alex,
        "Alex Morgan",
        "ck_operator",
        "Blake",
        "second",
        2,
    ))
    .await
    .unwrap();

    let worker = command_worker::spawn(state.clone());
    wait_for_status(&state, "cmd_ada_prompt", "done", Duration::from_millis(800)).await;
    wait_for_status(
        &state,
        "cmd_blake_prompt",
        "done",
        Duration::from_millis(800),
    )
    .await;
    worker.abort();

    assert!(
        exec.max_active() >= 2,
        "different target sessions should execute concurrently"
    );
}

#[tokio::test]
async fn harness_prompts_for_same_session_stay_ordered() {
    let exec = Arc::new(ObservedPromptExec::new(Duration::from_millis(15)));
    let state = test_state_with_turn_exec(exec.clone()).await;
    let alex = state
        .identity
        .register(human_register("Alex Morgan", "ck_operator"))
        .await
        .unwrap();
    state
        .identity
        .register(human_register("Ada", "ck_ada"))
        .await
        .unwrap();

    let repo = CommandIntents::new(&state.store);
    repo.insert_pending(prompt_command(
        "cmd_ada_1",
        &alex,
        "Alex Morgan",
        "ck_operator",
        "Ada",
        "first",
        1,
    ))
    .await
    .unwrap();
    repo.insert_pending(prompt_command(
        "cmd_ada_2",
        &alex,
        "Alex Morgan",
        "ck_operator",
        "Ada",
        "second",
        2,
    ))
    .await
    .unwrap();

    let worker = command_worker::spawn(state.clone());
    wait_for_status(&state, "cmd_ada_1", "done", Duration::from_millis(800)).await;
    wait_for_status(&state, "cmd_ada_2", "done", Duration::from_millis(800)).await;
    worker.abort();

    assert_eq!(
        exec.max_active(),
        1,
        "same target session must not have overlapping prompt execution"
    );
    assert_eq!(
        exec.starts()
            .into_iter()
            .map(|(_session, text)| text)
            .collect::<Vec<_>>(),
        ["first", "second"]
    );
}

#[tokio::test]
async fn stuck_harness_prompt_session_does_not_delay_other_sessions() {
    let exec = Arc::new(ObservedPromptExec::new(Duration::from_millis(1)));
    let state = test_state_with_turn_exec(exec).await;
    let alex = state
        .identity
        .register(human_register("Alex Morgan", "ck_operator"))
        .await
        .unwrap();
    state
        .identity
        .register(human_register("Ada", "ck_ada"))
        .await
        .unwrap();
    state
        .identity
        .register(human_register("Blake", "ck_blake"))
        .await
        .unwrap();

    let repo = CommandIntents::new(&state.store);
    repo.insert_pending(prompt_command(
        "cmd_ada_stuck",
        &alex,
        "Alex Morgan",
        "ck_operator",
        "Ada",
        "stuck",
        1,
    ))
    .await
    .unwrap();
    repo.insert_pending(prompt_command(
        "cmd_ada_next",
        &alex,
        "Alex Morgan",
        "ck_operator",
        "Ada",
        "same session waits",
        2,
    ))
    .await
    .unwrap();
    repo.insert_pending(prompt_command(
        "cmd_blake_prompt",
        &alex,
        "Alex Morgan",
        "ck_operator",
        "Blake",
        "other session runs",
        3,
    ))
    .await
    .unwrap();

    let worker = command_worker::spawn(state.clone());
    wait_for_status(
        &state,
        "cmd_ada_stuck",
        "claimed",
        Duration::from_millis(800),
    )
    .await;
    wait_for_status(
        &state,
        "cmd_blake_prompt",
        "done",
        Duration::from_millis(40),
    )
    .await;
    let same_session = repo.get("cmd_ada_next").await.unwrap().unwrap();
    assert_eq!(same_session.status, "pending");
    worker.abort();
}
