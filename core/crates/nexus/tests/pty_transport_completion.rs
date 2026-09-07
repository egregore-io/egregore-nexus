use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use nexus::daemon::pty_transport::PtyTransport;
use nexus_contracts::batch::{BatchCounts, NexusBatch};
use nexus_contracts::ports::{AgentTurnExecutionPort, EventSink, InjectError};
use nexus_contracts::{AgentUpdateKind, SessionId, WsEvent};
use nexus_pty::{HarnessInput, TurnCompletionEvidence};

#[derive(Default)]
struct AcceptedOnlyHarness {
    calls: AtomicUsize,
}

#[async_trait]
impl HarnessInput for AcceptedOnlyHarness {
    async fn send_turn(&self, _text: &str) -> Result<(), String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct NoopSink;

#[tokio::test]
async fn legacy_observation_is_unknown_and_never_holds_completion_or_dispatch() {
    let session = SessionId("unknown-terminal".into());
    let transport = PtyTransport::default();
    let harness = Arc::new(AcceptedOnlyHarness::default());
    assert_eq!(
        transport.observe_turn(&session).state,
        nexus_contracts::TurnState::Unavailable
    );
    transport.bind(session.clone(), harness.clone());
    let observation = transport.observe_turn(&session);
    assert_eq!(observation.state, nexus_contracts::TurnState::Unknown);
    assert_eq!(observation.stamp, None);
    assert_eq!(
        observation.steer_capability,
        transport.steer_capability(&session)
    );
    assert!(transport.active_turn_sessions().is_empty());
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        transport.wait_for_turn_completion(&session),
    )
    .await
    .unwrap()
    .unwrap();
    transport.prompt(&session, "legacy".into()).await.unwrap();
    assert_eq!(harness.calls.load(Ordering::SeqCst), 1);
    assert_eq!(transport.observe_turn(&session), observation);
}

#[async_trait]
impl EventSink for NoopSink {
    async fn emit(&self, _event: WsEvent) {}
}

#[derive(Default)]
struct ContextAcceptedHarness {
    calls: AtomicUsize,
}

struct BlockingContextAcceptedHarness {
    started: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
}

impl Default for BlockingContextAcceptedHarness {
    fn default() -> Self {
        Self {
            started: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
        }
    }
}

#[async_trait]
impl HarnessInput for BlockingContextAcceptedHarness {
    async fn send_turn(&self, _text: &str) -> Result<(), String> {
        self.started.add_permits(1);
        self.release
            .acquire()
            .await
            .map_err(|error| error.to_string())?
            .forget();
        Ok(())
    }

    fn turn_completion_evidence(&self) -> TurnCompletionEvidence {
        TurnCompletionEvidence::ContextAccepted
    }
}

#[async_trait]
impl HarnessInput for ContextAcceptedHarness {
    async fn send_turn(&self, _text: &str) -> Result<(), String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn turn_completion_evidence(&self) -> TurnCompletionEvidence {
        TurnCompletionEvidence::ContextAccepted
    }
}

#[tokio::test]
async fn observed_inject_accepts_harness_context_evidence() {
    let session = SessionId("s_context_accepted".into());
    let transport = PtyTransport::default();
    let harness = Arc::new(ContextAcceptedHarness::default());
    transport.bind(session.clone(), harness.clone() as Arc<dyn HarnessInput>);
    let batch = NexusBatch {
        counts: BatchCounts {
            dms: 0,
            thread: 0,
            total: 0,
        },
        dms: vec![],
        threads: vec![],
        dm_message_ids: vec![],
        thread_message_ids: vec![],
        message_ids: vec![],
    };

    transport
        .inject_turn_observed(
            &session,
            &batch,
            Arc::new(NoopSink),
            WsEvent::AgentUpdate {
                session_id: session.clone(),
                kind: AgentUpdateKind::UserInput,
                data: serde_json::json!({ "text": "accepted" }),
            },
        )
        .await
        .expect("context acceptance is sufficient durable transport evidence");

    assert_eq!(harness.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn observed_inject_rejects_input_acceptance_without_context_evidence() {
    let session = SessionId("s_acceptance_only".into());
    let transport = PtyTransport::default();
    let harness = Arc::new(AcceptedOnlyHarness::default());
    transport.bind(session.clone(), harness.clone() as Arc<dyn HarnessInput>);
    let batch = NexusBatch {
        counts: BatchCounts {
            dms: 0,
            thread: 0,
            total: 0,
        },
        dms: vec![],
        threads: vec![],
        dm_message_ids: vec![],
        thread_message_ids: vec![],
        message_ids: vec![],
    };

    let error = transport
        .inject_turn_observed(
            &session,
            &batch,
            Arc::new(NoopSink),
            WsEvent::AgentUpdate {
                session_id: session.clone(),
                kind: AgentUpdateKind::UserInput,
                data: serde_json::json!({ "text": "accepted" }),
            },
        )
        .await
        .expect_err("input acceptance alone cannot settle delivered");

    let InjectError::Contract(error) = error else {
        panic!("expected explicit unsupported Contract error, got {error:?}");
    };
    assert!(
        error.message.contains("cannot observe context acceptance"),
        "unsupported completion boundary must be explicit: {}",
        error.message
    );
    assert_eq!(
        harness.calls.load(Ordering::SeqCst),
        0,
        "unsupported acceptance-only paths must fail before bytes reach the harness"
    );
}

#[tokio::test]
async fn pty_turn_activity_is_observable_until_authoritative_completion() {
    let session = SessionId("s_active_pty".into());
    let transport = PtyTransport::default();
    let harness = Arc::new(BlockingContextAcceptedHarness::default());
    transport.bind(session.clone(), harness.clone() as Arc<dyn HarnessInput>);

    let turn_transport = transport.clone();
    let turn_session = session.clone();
    let turn = tokio::spawn(async move {
        turn_transport
            .prompt(&turn_session, "hold this PTY turn".into())
            .await
    });
    harness
        .started
        .acquire()
        .await
        .expect("blocking harness start semaphore remains open")
        .forget();

    assert_eq!(transport.active_turn_sessions(), vec![session.clone()]);
    let wait_transport = transport.clone();
    let wait_session = session.clone();
    let completion =
        tokio::spawn(async move { wait_transport.wait_for_turn_completion(&wait_session).await });
    tokio::task::yield_now().await;
    assert!(!completion.is_finished());

    harness.release.add_permits(1);
    turn.await.unwrap().unwrap();
    completion.await.unwrap().unwrap();
    assert!(transport.active_turn_sessions().is_empty());
}
