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

#[async_trait]
impl EventSink for NoopSink {
    async fn emit(&self, _event: WsEvent) {}
}

#[derive(Default)]
struct ContextAcceptedHarness {
    calls: AtomicUsize,
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
