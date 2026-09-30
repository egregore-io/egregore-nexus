use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use nexus::daemon::AppState;
use nexus_common::Config;
use nexus_contracts::{
    AgentTurnExecutionPort, CreateThreadRequest, Kind, NexusBatch, PortResult, RegisterRequest,
    RemoveRequest, RemoveResponse, SendRequest, SendTarget, SessionId, SpawnRequest, SpawnResponse,
    Tier, WsEvent,
};
use nexus_store::repos::{AgentRuntimes, InitialPromptDeliveries, Sessions};
use nexus_store::Store;
use tokio::sync::{mpsc, oneshot, Mutex};

const PROJECT: &str = "default";

#[test]
fn headed_claude_observes_completion_before_delivering_its_initial_prompt() {
    let source = include_str!("../src/daemon/app/launch_orchestration.rs");
    let observer = source
        .rfind("initial_prompt_needs_claude_forwarder")
        .expect("headed Claude completion observer preflight");
    let deliver = source[observer..]
        .find("deliver_prepared_initial_prompt")
        .map(|offset| observer + offset)
        .expect("headed initial-prompt delivery after observer preflight");

    assert!(
        observer < deliver,
        "Claude's native forwarder must observe Stop before prompt_observed waits for it"
    );
}

#[derive(Debug)]
enum ExecEvent {
    Prompt(String),
    Accepted(WsEvent),
    Drained(SessionId, NexusBatch),
}

struct InitialPromptExec {
    events: mpsc::UnboundedSender<ExecEvent>,
    prompt_gate: Mutex<Option<oneshot::Receiver<()>>>,
    fail_prompt: bool,
}

impl InitialPromptExec {
    fn new(
        events: mpsc::UnboundedSender<ExecEvent>,
        prompt_gate: Option<oneshot::Receiver<()>>,
        fail_prompt: bool,
    ) -> Self {
        Self {
            events,
            prompt_gate: Mutex::new(prompt_gate),
            fail_prompt,
        }
    }
}

#[async_trait]
impl AgentTurnExecutionPort for InitialPromptExec {
    async fn inject_turn(&self, recipient: &SessionId, batch: &NexusBatch) -> PortResult<()> {
        self.events
            .send(ExecEvent::Drained(recipient.clone(), batch.clone()))
            .expect("test receiver is alive");
        Ok(())
    }

    async fn launch(&self, _req: SpawnRequest) -> PortResult<SpawnResponse> {
        Ok(SpawnResponse {
            session_id: SessionId("s_initial_prompt_launch".into()),
        })
    }

    async fn remove(&self, req: RemoveRequest) -> PortResult<RemoveResponse> {
        Ok(RemoveResponse {
            name: Some(req.name),
            status: "removed".into(),
        })
    }

    async fn prompt_observed(
        &self,
        _recipient: &SessionId,
        text: String,
        events: Arc<dyn nexus_contracts::EventSink>,
        accepted_event: WsEvent,
    ) -> PortResult<()> {
        self.events
            .send(ExecEvent::Prompt(text))
            .expect("test receiver is alive");
        if let Some(rx) = self.prompt_gate.lock().await.take() {
            let _ = rx.await;
        }
        if self.fail_prompt {
            return Err(nexus_contracts::ContractError {
                code: -32004,
                message: "initial prompt rejected by harness".into(),
            });
        }
        events.emit(accepted_event.clone()).await;
        self.events
            .send(ExecEvent::Accepted(accepted_event))
            .expect("test receiver is alive");
        Ok(())
    }
}

async fn state_with_exec(exec: Arc<dyn AgentTurnExecutionPort>) -> AppState {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let state = AppState::wire_with_turn_exec(store, &Config::default(), exec);
    state
        .wait_for_runtime_identity_ready()
        .await
        .expect("runtime identity ready");
    state
}

fn human(name: &str, client_key: &str) -> RegisterRequest {
    RegisterRequest {
        agent_id: None,
        name: Some(name.into()),
        harness: hid("other"),
        harness_session_id: format!("hs_{client_key}"),
        project: PROJECT.into(),
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

fn launch_request(name: &str, initial_prompt: Option<&str>) -> SpawnRequest {
    SpawnRequest {
        kind: hid("codex"),
        name: Some(name.into()),
        identity_policy: None,
        cwd: Some(format!("/tmp/nexus-initial-prompt-{name}")),
        project: Some(PROJECT.into()),
        role: Some("reviewer".into()),
        initial_prompt: initial_prompt.map(str::to_string),
        resume: None,
        harness_args: Vec::new(),
        headless: true,
        backend: None,
    }
}

async fn wait_for_registered(state: &AppState, name: &str) -> SessionId {
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        if let Some(row) = Sessions::new(&state.store)
            .find_by_name(PROJECT, name)
            .await
            .unwrap()
        {
            return row.session_id;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {name} to be registered"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn fresh_launch_initial_prompt_blocks_thread_drain_until_accepted() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let (release_prompt, prompt_gate) = oneshot::channel();
    let exec = Arc::new(InitialPromptExec::new(tx, Some(prompt_gate), false));
    let state = state_with_exec(exec).await;
    state
        .identity
        .register(human("alex", "ck_operator_initial_prompt"))
        .await
        .unwrap();
    let caller = state.identity.resolve(PROJECT, "alex").await.unwrap();

    let launch_state = state.clone();
    let launch = tokio::spawn(async move {
        launch_state
            .launch_agent(
                launch_request(
                    "booted-codex",
                    Some("You are <var.name> as <var.role> in <var.harness>."),
                ),
                PROJECT,
                None,
            )
            .await
    });

    let session = wait_for_registered(&state, "booted-codex").await;
    let event = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("initial prompt was not attempted")
        .expect("event channel open");
    match event {
        ExecEvent::Prompt(text) => {
            assert_eq!(text, "You are booted-codex as reviewer in codex.");
        }
        other => panic!("expected initial prompt first, got {other:?}"),
    }

    state
        .bus
        .create_thread(
            &caller,
            CreateThreadRequest {
                name: "initial-prompt-thread".into(),
                members: vec!["booted-codex".into()],
            },
        )
        .await
        .unwrap();
    state
        .bus
        .send(
            &caller,
            SendRequest {
                to: SendTarget::Post {
                    thread: "initial-prompt-thread".into(),
                },
                summary: None,
                body: "must wait behind boot prompt".into(),
                mention: vec![],
                metadata: None,
                idempotency_key: None,
            },
        )
        .await
        .unwrap();

    assert!(
        tokio::time::timeout(Duration::from_millis(120), rx.recv())
            .await
            .is_err(),
        "thread fanout must not drain while the initial prompt is still pending"
    );

    release_prompt.send(()).unwrap();
    let accepted = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("initial prompt acceptance event missing")
        .expect("event channel open");
    match accepted {
        ExecEvent::Accepted(WsEvent::AgentUpdate {
            session_id,
            kind,
            data,
        }) => {
            assert_eq!(session_id, session);
            assert_eq!(kind, nexus_contracts::AgentUpdateKind::UserInput);
            assert_eq!(data["source"], "initial_prompt");
            assert_eq!(data["clientMessageId"], format!("initial-prompt:{session}"));
            assert_eq!(data["runtimeId"], session.0);
        }
        other => panic!("expected accepted user_input event, got {other:?}"),
    }

    let drained = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("queued thread message did not drain after initial prompt acceptance")
        .expect("event channel open");
    match drained {
        ExecEvent::Drained(recipient, batch) => {
            assert_eq!(recipient, session);
            assert_eq!(batch.counts.thread, 1);
            assert_eq!(batch.threads[0].body, "must wait behind boot prompt");
        }
        other => panic!("expected thread drain after acceptance, got {other:?}"),
    }

    launch.await.unwrap().unwrap();
    let delivery = InitialPromptDeliveries::new(&state.store)
        .get(&session.0)
        .await
        .unwrap()
        .expect("delivery row");
    assert_eq!(delivery.status, "accepted");
}

#[tokio::test]
async fn failed_headless_initial_prompt_tears_down_runtime_and_does_not_wake() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let exec = Arc::new(InitialPromptExec::new(tx, None, true));
    let state = state_with_exec(exec).await;

    let err = state
        .launch_agent(
            launch_request("boot-fails", Some("You are <var.name>.")),
            PROJECT,
            None,
        )
        .await
        .expect_err("initial prompt failure must fail the launch");
    assert!(
        err.message.contains("initial prompt rejected"),
        "unexpected error: {err:?}"
    );

    let event = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("initial prompt should be attempted")
        .expect("event channel open");
    assert!(matches!(event, ExecEvent::Prompt(_)));
    assert!(
        tokio::time::timeout(Duration::from_millis(120), rx.recv())
            .await
            .is_err(),
        "failed initial prompt must not emit accepted input or drain inbox"
    );

    let row = Sessions::new(&state.store)
        .find_by_name(PROJECT, "boot-fails")
        .await
        .unwrap()
        .expect("failed launch leaves auditable session row");
    assert_eq!(row.presence.as_deref(), Some("offline"));
    let runtime = AgentRuntimes::new(&state.store)
        .find_by_runtime_id(&row.session_id.0)
        .await
        .unwrap()
        .expect("failed launch leaves auditable runtime row");
    assert!(
        !runtime.active,
        "failed launch runtime must not remain active"
    );
    assert_eq!(runtime.presence.as_deref(), Some("offline"));
    let delivery = InitialPromptDeliveries::new(&state.store)
        .get(&row.session_id.0)
        .await
        .unwrap()
        .expect("delivery row");
    assert_eq!(delivery.status, "failed");
}

#[tokio::test]
async fn invalid_initial_prompt_template_is_rejected_before_runtime_is_addressable() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let exec = Arc::new(InitialPromptExec::new(tx, None, false));
    let state = state_with_exec(exec).await;

    let err = state
        .launch_agent(
            launch_request("bad-template", Some("Project is <var.project>.")),
            PROJECT,
            None,
        )
        .await
        .expect_err("invalid template must fail launch");
    assert!(
        err.message
            .contains("unknown initial prompt variable <var.project>"),
        "unexpected error: {err:?}"
    );

    assert!(
        Sessions::new(&state.store)
            .find_by_name(PROJECT, "bad-template")
            .await
            .unwrap()
            .is_none(),
        "invalid initial prompt must fail before the runtime becomes addressable"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(120), rx.recv())
            .await
            .is_err(),
        "invalid initial prompt must not reach the harness or emit accepted input"
    );
}

/// A validated [`nexus_contracts::HarnessId`] from a literal (panics on invalid — test-only).
fn hid(s: &str) -> nexus_contracts::HarnessId {
    nexus_contracts::HarnessId::new(s).expect("valid harness id literal")
}
