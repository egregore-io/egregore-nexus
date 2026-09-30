//! Explicit delivery retry contract. Kept out of daemon production modules so the core test-layout
//! gate can enforce the crate-level integration-test boundary.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use nexus::daemon::{dispatch, AppState};
use nexus_common::{now, Config};
use nexus_contracts::{
    AgentTurnExecutionPort, Caller, ContractError, DlqRequeueRequest, EventSink, Harness,
    InjectError, InjectResult, Message, MessageId, NexusBatch, ProjectId, ProviderLimit,
    ProviderLimitReason, Request, RequestId, Scope, SessionId, SpawnRequest, SpawnResponse, Tier,
    WsEvent,
};
use nexus_store::repos::{Messages, NewSession, Sessions};
use nexus_store::Store;

struct LimitOnceTurnExec {
    attempts: AtomicUsize,
}

#[async_trait]
impl AgentTurnExecutionPort for LimitOnceTurnExec {
    async fn inject_turn(
        &self,
        _recipient: &SessionId,
        _batch: &NexusBatch,
    ) -> Result<(), ContractError> {
        unreachable!("event loop uses the observed boundary")
    }

    async fn inject_turn_observed(
        &self,
        recipient: &SessionId,
        _batch: &NexusBatch,
        events: Arc<dyn EventSink>,
        accepted_event: WsEvent,
    ) -> InjectResult<()> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
        if attempt == 1 {
            return Err(InjectError::ProviderLimit(ProviderLimit {
                harness: Harness::Claude,
                session: recipient.clone(),
                reason: ProviderLimitReason::UsageLimit,
                reset_hint: None,
                provider: Some("test-provider".into()),
                model: Some("test-model".into()),
                source: "delivery-requeue-test".into(),
            }));
        }
        events.emit(accepted_event).await;
        Ok(())
    }

    async fn launch(&self, _req: SpawnRequest) -> Result<SpawnResponse, ContractError> {
        unreachable!("the test runtime is already registered")
    }

    async fn remove(
        &self,
        _req: nexus_contracts::RemoveRequest,
    ) -> Result<nexus_contracts::RemoveResponse, ContractError> {
        unreachable!()
    }
}

fn admin() -> Caller {
    Caller {
        agent_id: None,
        session: SessionId("s_admin".into()),
        name: "operator".into(),
        project: "default".into(),
        tier: Tier::Admin,
    }
}

async fn delivery_row(store: &Store, message_id: &str) -> (String, i64, String, String) {
    let mut rows = store
        .conn
        .query(
            "SELECT in_flight_id, attempt_count, state, COALESCE(error_code, '') \
             FROM in_flight WHERE message_id = ?1",
            [message_id],
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().expect("delivery row");
    (
        row.get(0).unwrap(),
        row.get(1).unwrap(),
        row.get(2).unwrap(),
        row.get(3).unwrap(),
    )
}

#[tokio::test]
async fn terminal_delivery_retries_only_after_explicit_admin_requeue() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let exec = Arc::new(LimitOnceTurnExec {
        attempts: AtomicUsize::new(0),
    });
    let state = AppState::wire_with_turn_exec(store.clone(), &Config::default(), exec.clone());
    let session = SessionId("s_explicit_retry".into());
    Sessions::new(&store)
        .create(NewSession {
            session_id: session.clone(),
            name: Some("retry-agent".into()),
            agent: Some("claude".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: Some("claude-retry-native".into()),
            client_key: Some("ck_retry_agent".into()),
            cwd: Some("/tmp/retry-agent".into()),
            project: "default".into(),
            transport: Some("acp".into()),
        })
        .await
        .unwrap();
    state.ensure_agent_loop("default", "retry-agent").await;

    let message_id = MessageId("m_explicit_retry".into());
    Messages::new(&store)
        .insert(&Message {
            id: message_id.clone(),
            project: ProjectId("default".into()),
            from: "operator".into(),
            scope: Scope::Dm,
            thread: None,
            topic: None,
            body: "retry only when asked".into(),
            summary: None,
            provenance: nexus_contracts::message::Provenance {
                from: "operator".into(),
                kind: nexus_contracts::Kind::Human,
                thread: None,
                topic: None,
                stamp: None,
            },
            created_at: now(),
        })
        .await
        .unwrap();
    state.realtime.enqueue(&session, &message_id).await.unwrap();

    let mut failed = None;
    for _ in 0..200 {
        let row = delivery_row(&store, &message_id.0).await;
        if row.2 == "error" {
            failed = Some(row);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let (in_flight_id, attempts, state_before, code) = failed.expect("provider failure settles");
    assert_eq!(attempts, 1);
    assert_eq!(state_before, "error");
    assert_eq!(code, "provider_limit");

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        exec.attempts.load(Ordering::SeqCst),
        1,
        "terminal failure never retries on a timer"
    );

    let response = dispatch(
        &state,
        Some(admin()),
        Request {
            jsonrpc: nexus_contracts::JSONRPC_VERSION.into(),
            id: Some(RequestId::Num(1)),
            method: "admin.dlq.requeue".into(),
            params: Some(
                serde_json::to_value(DlqRequeueRequest {
                    in_flight_id: Some(in_flight_id),
                    for_target: None,
                    since: None,
                })
                .unwrap(),
            ),
        },
    )
    .await;
    assert!(
        response.error.is_none(),
        "requeue failed: {:?}",
        response.error
    );

    for _ in 0..200 {
        let row = delivery_row(&store, &message_id.0).await;
        if row.2 == "delivered" {
            assert_eq!(
                row.1, 2,
                "explicit retry increments the durable audit count"
            );
            assert_eq!(exec.attempts.load(Ordering::SeqCst), 2);
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("explicitly requeued delivery did not settle delivered");
}
