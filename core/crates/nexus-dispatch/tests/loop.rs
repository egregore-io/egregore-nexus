//! Integration test for the per-agent [`EventLoop`] + [`DispatchService`] service.
//!
//! Uses a **mock turn-exec port** that records every injected batch (and can gate its first turn so
//! the test can enqueue more messages *during* a turn to exercise coalescing), over a real
//! `:memory:` store. Asserts: one wake on an idle agent → one turn with both messages; two more
//! enqueued mid-turn → one coalesced follow-up turn (2 injected turns total, not 3); `ack` clears
//! the in-flight row.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;

use nexus_common::Config;
use nexus_contracts::ack::AckRequest;
use nexus_contracts::admin::{RemoveRequest, RemoveResponse, SpawnRequest, SpawnResponse};
use nexus_contracts::batch::NexusBatch;
use nexus_contracts::enums::{Kind, Scope, Tier};
use nexus_contracts::events::WsEvent;
use nexus_contracts::ids::{MessageId, ProjectId, SessionId};
use nexus_contracts::message::{Message, Provenance};
use nexus_contracts::ports::{
    AgentTurnExecutionPort, Caller, ContractError, DispatchPort, EventSink, InjectError,
    InjectResult,
};
use nexus_contracts::{
    AgentUpdateKind, DeliveryTiming, HarnessId, OperatorAction, ProviderError, ProviderLimit,
    ProviderLimitReason, ResetHint, SteerCapability, SteerDelivery, SteerResponse,
};
use nexus_store::repos::{Inbox, Messages};
use nexus_store::Store;
use serde_json::Value;
use tokio::sync::Notify;

use nexus_dispatch::{
    AgentRegistry, AgentState, Bell, DispatchService, EventLoop, LoopDeps, ServiceDeps,
};

/// Records every injected batch. The first `inject_turn` parks on `gate` (after announcing it has
/// started via `started`) so the test can enqueue more rows mid-turn to exercise coalescing.
struct MockTurnExec {
    injected: Mutex<Vec<NexusBatch>>,
    started: Notify,
    gate: Notify,
}

impl MockTurnExec {
    fn new() -> Self {
        MockTurnExec {
            injected: Mutex::new(Vec::new()),
            started: Notify::new(),
            gate: Notify::new(),
        }
    }
    fn turn_count(&self) -> usize {
        self.injected.lock().unwrap().len()
    }
}

/// Models Codex reporting `turn/completed { status: interrupted }`: the current observed turn
/// remains open until `interrupted` is signalled, then completes normally so the event loop must
/// immediately re-drain anything that arrived during it.
struct InterruptedTurnExec {
    injected: Mutex<Vec<NexusBatch>>,
    first_started: Notify,
    interrupted: Notify,
}

/// Models a Codex app-server turn that remains active while a second durable bus batch arrives.
/// Native steer accepts the second batch into that active context without completing the first.
struct NativeSteerTurnExec {
    session: SessionId,
    injected: Mutex<Vec<NexusBatch>>,
    steered: Mutex<Vec<String>>,
    active: AtomicBool,
    first_started: Notify,
    first_gate: Notify,
    steer_accepted: Notify,
}

/// Models an interrupt-and-send ACP adapter whose replacement prompt is serialized behind the
/// current prompt. The current prompt can observe cancellation only while its future is polled.
/// Awaiting the replacement inline from the event-loop bell branch therefore deadlocks: that
/// branch stops polling the very future that must release the serialization boundary.
struct SerializedInterruptAndSendTurnExec {
    session: SessionId,
    injected: Mutex<Vec<NexusBatch>>,
    active: AtomicBool,
    interrupts: AtomicUsize,
    steer_calls: AtomicUsize,
    first_error: Option<ContractError>,
    interrupt_error: Option<ContractError>,
    first_started: Notify,
    interrupt_requested: Notify,
    terminal_release: Notify,
    first_completed: Notify,
}

struct ActivePromptGuard<'a> {
    active: &'a AtomicBool,
    completed: &'a Notify,
}

impl Drop for ActivePromptGuard<'_> {
    fn drop(&mut self) {
        self.active.store(false, Ordering::SeqCst);
        self.completed.notify_one();
    }
}

impl SerializedInterruptAndSendTurnExec {
    fn new(session: SessionId) -> Self {
        Self {
            session,
            injected: Mutex::new(Vec::new()),
            active: AtomicBool::new(false),
            interrupts: AtomicUsize::new(0),
            steer_calls: AtomicUsize::new(0),
            first_error: None,
            interrupt_error: None,
            first_started: Notify::new(),
            interrupt_requested: Notify::new(),
            terminal_release: Notify::new(),
            first_completed: Notify::new(),
        }
    }

    fn with_first_error(session: SessionId, error: ContractError) -> Self {
        Self {
            first_error: Some(error),
            ..Self::new(session)
        }
    }

    fn with_interrupt_error(session: SessionId, error: ContractError) -> Self {
        Self {
            interrupt_error: Some(error),
            ..Self::new(session)
        }
    }

    fn begin_injection(&self, batch: &NexusBatch) -> bool {
        let first = {
            let mut injected = self.injected.lock().unwrap();
            injected.push(batch.clone());
            injected.len() == 1
        };
        if first {
            self.active.store(true, Ordering::SeqCst);
            self.first_started.notify_one();
        }
        first
    }

    async fn finish_injection(&self, first: bool) -> Result<(), ContractError> {
        if !first {
            return Ok(());
        }
        let _active_prompt = ActivePromptGuard {
            active: &self.active,
            completed: &self.first_completed,
        };
        self.interrupt_requested.notified().await;
        self.terminal_release.notified().await;
        match &self.first_error {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }
}

#[async_trait]
impl AgentTurnExecutionPort for SerializedInterruptAndSendTurnExec {
    async fn inject_turn(
        &self,
        _recipient: &SessionId,
        batch: &NexusBatch,
    ) -> Result<(), ContractError> {
        let first = self.begin_injection(batch);
        self.finish_injection(first).await
    }

    async fn inject_turn_observed(
        &self,
        _recipient: &SessionId,
        batch: &NexusBatch,
        events: Arc<dyn EventSink>,
        accepted_event: WsEvent,
    ) -> InjectResult<()> {
        let first = self.begin_injection(batch);
        events.emit(accepted_event).await;
        self.finish_injection(first)
            .await
            .map_err(InjectError::Contract)
    }

    async fn steer_observed(
        &self,
        _recipient: &SessionId,
        _text: String,
        events: Arc<dyn EventSink>,
        accepted_event: WsEvent,
    ) -> Result<SteerResponse, ContractError> {
        self.steer_calls.fetch_add(1, Ordering::SeqCst);
        self.interrupt_active_turn(_recipient).await?;
        self.first_completed.notified().await;
        events.emit(accepted_event).await;
        Ok(SteerResponse {
            session_id: None,
            accepted: true,
            delivery: SteerDelivery::InterruptedAndStarted,
            turn_id: None,
        })
    }

    fn steer_capability(&self, _recipient: &SessionId) -> SteerCapability {
        SteerCapability::InterruptAndSend
    }

    fn active_turn_sessions(&self) -> Vec<SessionId> {
        self.active
            .load(Ordering::SeqCst)
            .then(|| self.session.clone())
            .into_iter()
            .collect()
    }

    async fn interrupt_active_turn(&self, _recipient: &SessionId) -> Result<(), ContractError> {
        self.interrupts.fetch_add(1, Ordering::SeqCst);
        if let Some(error) = &self.interrupt_error {
            return Err(error.clone());
        }
        self.interrupt_requested.notify_one();
        Ok(())
    }

    async fn launch(&self, _req: SpawnRequest) -> Result<SpawnResponse, ContractError> {
        unreachable!()
    }

    async fn remove(&self, _req: RemoveRequest) -> Result<RemoveResponse, ContractError> {
        unreachable!()
    }
}

/// Models the narrow race where the tracked active turn ends between the daemon's capability
/// check and the native steer RPC. The rejected payload was not admitted and must become the next
/// normal turn after the first completion is observed.
struct NativeSteerRaceTurnExec {
    session: SessionId,
    injected: Mutex<Vec<NexusBatch>>,
    active: AtomicBool,
    first_started: Notify,
    first_gate: Notify,
    steer_rejected: Notify,
    steer_error: ContractError,
}

/// Models a turn that is already active outside the bus loop. Tool-stream activity is deliberately
/// separate from the final completion signal so timing cannot be inferred from visible updates.
struct BoundaryWaitTurnExec {
    session: SessionId,
    active: AtomicBool,
    injected: Mutex<Vec<NexusBatch>>,
    wait_started: Notify,
    final_completion: Notify,
}

impl BoundaryWaitTurnExec {
    fn new(session: SessionId) -> Self {
        Self {
            session,
            active: AtomicBool::new(true),
            injected: Mutex::new(Vec::new()),
            wait_started: Notify::new(),
            final_completion: Notify::new(),
        }
    }
}

#[async_trait]
impl AgentTurnExecutionPort for BoundaryWaitTurnExec {
    async fn inject_turn(
        &self,
        _recipient: &SessionId,
        batch: &NexusBatch,
    ) -> Result<(), ContractError> {
        self.injected.lock().unwrap().push(batch.clone());
        Ok(())
    }

    fn active_turn_sessions(&self) -> Vec<SessionId> {
        self.active
            .load(Ordering::SeqCst)
            .then(|| self.session.clone())
            .into_iter()
            .collect()
    }

    async fn wait_for_turn_completion(&self, _recipient: &SessionId) -> Result<(), ContractError> {
        self.wait_started.notify_one();
        self.final_completion.notified().await;
        self.active.store(false, Ordering::SeqCst);
        Ok(())
    }

    async fn launch(&self, _req: SpawnRequest) -> Result<SpawnResponse, ContractError> {
        unreachable!()
    }

    async fn remove(&self, _req: RemoveRequest) -> Result<RemoveResponse, ContractError> {
        unreachable!()
    }
}

impl NativeSteerRaceTurnExec {
    fn new(session: SessionId) -> Self {
        Self::with_error(
            session,
            ContractError {
                code: nexus_contracts::codes::ACTIVE_TURN_REQUIRED,
                message: "no active turn to steer".into(),
            },
        )
    }

    fn with_error(session: SessionId, steer_error: ContractError) -> Self {
        Self {
            session,
            injected: Mutex::new(Vec::new()),
            active: AtomicBool::new(false),
            first_started: Notify::new(),
            first_gate: Notify::new(),
            steer_rejected: Notify::new(),
            steer_error,
        }
    }
}

#[async_trait]
impl AgentTurnExecutionPort for NativeSteerRaceTurnExec {
    async fn inject_turn(
        &self,
        _recipient: &SessionId,
        batch: &NexusBatch,
    ) -> Result<(), ContractError> {
        let first = {
            let mut injected = self.injected.lock().unwrap();
            injected.push(batch.clone());
            injected.len() == 1
        };
        if first {
            self.active.store(true, Ordering::SeqCst);
            self.first_started.notify_one();
            self.first_gate.notified().await;
            self.active.store(false, Ordering::SeqCst);
        }
        Ok(())
    }

    async fn steer_observed(
        &self,
        _recipient: &SessionId,
        _text: String,
        _events: Arc<dyn EventSink>,
        _accepted_event: WsEvent,
    ) -> Result<SteerResponse, ContractError> {
        self.active.store(false, Ordering::SeqCst);
        self.steer_rejected.notify_one();
        Err(self.steer_error.clone())
    }

    fn steer_capability(&self, _recipient: &SessionId) -> SteerCapability {
        SteerCapability::NativeSteer
    }

    fn active_turn_sessions(&self) -> Vec<SessionId> {
        if self.active.load(Ordering::SeqCst) {
            vec![self.session.clone()]
        } else {
            Vec::new()
        }
    }

    async fn launch(&self, _req: SpawnRequest) -> Result<SpawnResponse, ContractError> {
        unreachable!()
    }

    async fn remove(&self, _req: RemoveRequest) -> Result<RemoveResponse, ContractError> {
        unreachable!()
    }
}

impl NativeSteerTurnExec {
    fn new(session: SessionId) -> Self {
        Self {
            session,
            injected: Mutex::new(Vec::new()),
            steered: Mutex::new(Vec::new()),
            active: AtomicBool::new(false),
            first_started: Notify::new(),
            first_gate: Notify::new(),
            steer_accepted: Notify::new(),
        }
    }
}

#[async_trait]
impl AgentTurnExecutionPort for NativeSteerTurnExec {
    async fn inject_turn(
        &self,
        _recipient: &SessionId,
        batch: &NexusBatch,
    ) -> Result<(), ContractError> {
        self.injected.lock().unwrap().push(batch.clone());
        self.active.store(true, Ordering::SeqCst);
        self.first_started.notify_one();
        self.first_gate.notified().await;
        self.active.store(false, Ordering::SeqCst);
        Ok(())
    }

    async fn steer_observed(
        &self,
        _recipient: &SessionId,
        text: String,
        events: Arc<dyn EventSink>,
        accepted_event: WsEvent,
    ) -> Result<SteerResponse, ContractError> {
        self.steered.lock().unwrap().push(text);
        events.emit(accepted_event).await;
        self.steer_accepted.notify_one();
        Ok(SteerResponse {
            session_id: None,
            accepted: true,
            delivery: SteerDelivery::Steered,
            turn_id: Some("turn_active".into()),
        })
    }

    fn steer_capability(&self, _recipient: &SessionId) -> SteerCapability {
        SteerCapability::NativeSteer
    }

    fn active_turn_sessions(&self) -> Vec<SessionId> {
        if self.active.load(Ordering::SeqCst) {
            vec![self.session.clone()]
        } else {
            Vec::new()
        }
    }

    async fn launch(&self, _req: SpawnRequest) -> Result<SpawnResponse, ContractError> {
        unreachable!()
    }

    async fn remove(&self, _req: RemoveRequest) -> Result<RemoveResponse, ContractError> {
        unreachable!()
    }
}

impl InterruptedTurnExec {
    fn new() -> Self {
        Self {
            injected: Mutex::new(Vec::new()),
            first_started: Notify::new(),
            interrupted: Notify::new(),
        }
    }

    fn turn_count(&self) -> usize {
        self.injected.lock().unwrap().len()
    }
}

#[async_trait]
impl AgentTurnExecutionPort for InterruptedTurnExec {
    async fn inject_turn(
        &self,
        _recipient: &SessionId,
        batch: &NexusBatch,
    ) -> Result<(), ContractError> {
        let is_first = {
            let mut injected = self.injected.lock().unwrap();
            injected.push(batch.clone());
            injected.len() == 1
        };
        if is_first {
            self.first_started.notify_one();
            self.interrupted.notified().await;
        }
        Ok(())
    }

    async fn launch(&self, _req: SpawnRequest) -> Result<SpawnResponse, ContractError> {
        Err(ContractError {
            code: -32603,
            message: "unused".into(),
        })
    }

    async fn remove(&self, _req: RemoveRequest) -> Result<RemoveResponse, ContractError> {
        Err(ContractError {
            code: -32603,
            message: "unused".into(),
        })
    }
}

#[async_trait]
impl AgentTurnExecutionPort for MockTurnExec {
    async fn inject_turn(
        &self,
        _recipient: &SessionId,
        batch: &NexusBatch,
    ) -> Result<(), ContractError> {
        let is_first = {
            let mut g = self.injected.lock().unwrap();
            g.push(batch.clone());
            g.len() == 1
        };
        if is_first {
            // Announce the turn has begun, then hold it open until the test releases the gate.
            self.started.notify_one();
            self.gate.notified().await;
        }
        Ok(())
    }
    async fn launch(&self, _req: SpawnRequest) -> Result<SpawnResponse, ContractError> {
        Err(ContractError {
            code: -32603,
            message: "unused".into(),
        })
    }
    async fn remove(&self, _req: RemoveRequest) -> Result<RemoveResponse, ContractError> {
        Err(ContractError {
            code: -32603,
            message: "unused".into(),
        })
    }
}

struct FailingTurnExec {
    started: Notify,
}

#[async_trait]
impl AgentTurnExecutionPort for FailingTurnExec {
    async fn inject_turn(
        &self,
        _recipient: &SessionId,
        _batch: &NexusBatch,
    ) -> Result<(), ContractError> {
        self.started.notify_one();
        Err(ContractError {
            code: -32004,
            message: "agent session is not receiving turns".into(),
        })
    }
    async fn launch(&self, _req: SpawnRequest) -> Result<SpawnResponse, ContractError> {
        Err(ContractError {
            code: -32603,
            message: "unused".into(),
        })
    }
    async fn remove(&self, _req: RemoveRequest) -> Result<RemoveResponse, ContractError> {
        Err(ContractError {
            code: -32603,
            message: "unused".into(),
        })
    }
}

struct ProviderLimitOnceTurnExec {
    attempts: AtomicUsize,
    started: Notify,
    reset_delay_ms: i64,
}

impl ProviderLimitOnceTurnExec {
    fn attempts(&self) -> usize {
        self.attempts.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl AgentTurnExecutionPort for ProviderLimitOnceTurnExec {
    async fn inject_turn(
        &self,
        _recipient: &SessionId,
        _batch: &NexusBatch,
    ) -> Result<(), ContractError> {
        Ok(())
    }

    async fn inject_turn_observed(
        &self,
        recipient: &SessionId,
        _batch: &NexusBatch,
        events: Arc<dyn EventSink>,
        accepted_event: WsEvent,
    ) -> InjectResult<()> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
        self.started.notify_waiters();
        if attempt == 1 {
            return Err(InjectError::ProviderLimit(ProviderLimit {
                harness: HarnessId::new("codex").unwrap(),
                session: recipient.clone(),
                reason: ProviderLimitReason::UsageLimit,
                reset_hint: Some(ResetHint {
                    unix_ms: now_unix_ms() + self.reset_delay_ms,
                }),
                provider: Some("codex".into()),
                model: Some("test-model".into()),
                source: "test-provider-limit".into(),
            }));
        }
        events.emit(accepted_event).await;
        Ok(())
    }

    async fn launch(&self, _req: SpawnRequest) -> Result<SpawnResponse, ContractError> {
        Err(ContractError {
            code: -32603,
            message: "unused".into(),
        })
    }

    async fn remove(&self, _req: RemoveRequest) -> Result<RemoveResponse, ContractError> {
        Err(ContractError {
            code: -32603,
            message: "unused".into(),
        })
    }
}

/// Injects a turn whose harness-completion notification never arrives: it announces it has started
/// (via `started`) then hangs forever, so the loop's bounded completion await elapses. Models the
/// lost-completion outage that must dead-letter (not falsely deliver) the batch.
struct HangingTurnExec {
    started: Notify,
}

struct OperatorActionTurnExec {
    attempts: AtomicUsize,
}

struct ProviderErrorTurnExec {
    attempts: AtomicUsize,
}

#[async_trait]
impl AgentTurnExecutionPort for ProviderErrorTurnExec {
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
        _events: Arc<dyn EventSink>,
        _accepted_event: WsEvent,
    ) -> InjectResult<()> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        Err(InjectError::ProviderError(ProviderError {
            harness: HarnessId::new("claude").unwrap(),
            session: recipient.clone(),
            reason: "server_error".into(),
            provider: Some("anthropic".into()),
            model: Some("claude-sonnet-5".into()),
            retryable: true,
            source: "claude.acp.prompt_error".into(),
        }))
    }

    async fn launch(&self, _req: SpawnRequest) -> Result<SpawnResponse, ContractError> {
        unreachable!()
    }

    async fn remove(&self, _req: RemoveRequest) -> Result<RemoveResponse, ContractError> {
        unreachable!()
    }
}

#[async_trait]
impl AgentTurnExecutionPort for OperatorActionTurnExec {
    async fn inject_turn(
        &self,
        _recipient: &SessionId,
        _batch: &NexusBatch,
    ) -> Result<(), ContractError> {
        Ok(())
    }

    async fn inject_turn_observed(
        &self,
        recipient: &SessionId,
        _batch: &NexusBatch,
        _events: Arc<dyn EventSink>,
        _accepted_event: WsEvent,
    ) -> InjectResult<()> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        Err(InjectError::OperatorAction(OperatorAction {
            harness: HarnessId::new("codex").unwrap(),
            session: recipient.clone(),
            reason: "sign in required".into(),
            provider: Some("codex".into()),
            model: None,
            source: "test-operator-action".into(),
        }))
    }

    async fn launch(&self, _req: SpawnRequest) -> Result<SpawnResponse, ContractError> {
        unreachable!()
    }

    async fn remove(&self, _req: RemoveRequest) -> Result<RemoveResponse, ContractError> {
        unreachable!()
    }
}

#[async_trait]
impl AgentTurnExecutionPort for HangingTurnExec {
    async fn inject_turn(
        &self,
        _recipient: &SessionId,
        _batch: &NexusBatch,
    ) -> Result<(), ContractError> {
        self.started.notify_one();
        std::future::pending::<()>().await;
        Ok(())
    }
    async fn launch(&self, _req: SpawnRequest) -> Result<SpawnResponse, ContractError> {
        Err(ContractError {
            code: -32603,
            message: "unused".into(),
        })
    }
    async fn remove(&self, _req: RemoveRequest) -> Result<RemoveResponse, ContractError> {
        Err(ContractError {
            code: -32603,
            message: "unused".into(),
        })
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

async fn wait_for_user_input(events: &RecordingSink, session: &SessionId) -> Value {
    for _ in 0..200 {
        if let Some(data) = events.events().iter().find_map(|event| match event {
            WsEvent::AgentUpdate {
                session_id,
                kind,
                data,
            } if session_id == session && *kind == AgentUpdateKind::UserInput => Some(data.clone()),
            _ => None,
        }) {
            return data;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("user_input was not emitted for {session}");
}

/// A no-op event sink.
struct NullSink;
#[async_trait]
impl EventSink for NullSink {
    async fn emit(&self, _event: WsEvent) {}
}

const PROJECT: &str = "p_demo";

fn dm(id: &str, from: &str, body: &str) -> Message {
    message(id, from, body, Kind::Agent)
}

fn human_dm(id: &str, from: &str, body: &str) -> Message {
    message(id, from, body, Kind::Human)
}

fn message(id: &str, from: &str, body: &str, kind: Kind) -> Message {
    Message {
        id: MessageId(id.into()),
        project: ProjectId(PROJECT.into()),
        from: from.into(),
        scope: Scope::Dm,
        thread: None,
        topic: None,
        body: body.into(),
        summary: None,
        provenance: Provenance {
            from: from.into(),
            kind,
            locality: Default::default(),
            access: None,
            thread: None,
            topic: None,
            stamp: None,
        },
        created_at: 0,
    }
}

async fn insert(store: &Store, m: &Message) {
    Messages::new(store).insert(m).await.unwrap();
}

fn caller(session: &SessionId) -> Caller {
    Caller {
        agent_id: None,
        session: session.clone(),
        name: "ana".into(),
        project: PROJECT.into(),
        tier: Tier::Agent,
        locality: Default::default(),
        access: None,
        principal_id: None,
    }
}

fn now_unix_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

async fn wait_for_attempts(exec: &ProviderLimitOnceTurnExec, expected: usize) {
    for _ in 0..200 {
        if exec.attempts() >= expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!(
        "expected at least {expected} attempts, saw {}",
        exec.attempts()
    );
}

async fn wait_for_delivery_state(store: &Store, message_id: &str, expected: &str) {
    for _ in 0..200 {
        let mut rows = store
            .conn
            .query(
                "SELECT state FROM in_flight WHERE message_id = ?1 LIMIT 1",
                [message_id],
            )
            .await
            .unwrap();
        if let Some(row) = rows.next().await.unwrap() {
            if row.get::<String>(0).unwrap() == expected {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("delivery {message_id} did not reach {expected}");
}

#[tokio::test]
async fn idle_agent_drains_one_turn_then_coalesces_a_followup() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let cfg = Config::default();

    let bell = Bell::new();
    let registry = AgentRegistry::new();
    let turn_exec = Arc::new(MockTurnExec::new());
    let session = SessionId("s_ana".into());

    let service = DispatchService::new(ServiceDeps {
        store: store.clone(),
        bell: bell.clone(),
        registry: registry.clone(),
        project: PROJECT.into(),
        drain_limit: cfg.drain_limit,
        preview_chars: cfg.msg_preview_chars,
    });

    let _loop = EventLoop::spawn(
        session.clone(),
        LoopDeps {
            store: store.clone(),
            bell: bell.clone(),
            registry: registry.clone(),
            turn_exec: turn_exec.clone(),
            events: Arc::new(NullSink),
            project: PROJECT.into(),
            drain_limit: cfg.drain_limit,
            preview_chars: cfg.msg_preview_chars,
            completion_timeout: std::time::Duration::from_secs(45),
            provider_limit_default_cooldown: std::time::Duration::from_millis(50),
        },
    );

    // Two messages for an idle agent → enqueued, bell rung.
    insert(&store, &dm("m_01", "ana", "take the auth refactor today?")).await;
    insert(&store, &dm("m_02", "ben", "rebase before you start.")).await;
    service
        .enqueue(&session, &MessageId("m_01".into()))
        .await
        .unwrap();
    service
        .enqueue(&session, &MessageId("m_02".into()))
        .await
        .unwrap();

    // Wait until the loop has started injecting the first turn.
    tokio::time::timeout(Duration::from_secs(2), turn_exec.started.notified())
        .await
        .expect("first turn should start");

    // First turn carries BOTH messages as one batch.
    {
        let injected = turn_exec.injected.lock().unwrap();
        assert_eq!(injected.len(), 1, "exactly one turn so far");
        assert_eq!(
            injected[0].counts.total, 2,
            "both messages in one drained turn"
        );
    }

    // Mid-turn: two MORE arrive. They must coalesce into a single follow-up turn (not 2 turns).
    insert(&store, &dm("m_03", "cleo", "ping")).await;
    insert(&store, &dm("m_04", "dan", "pong")).await;
    service
        .enqueue(&session, &MessageId("m_03".into()))
        .await
        .unwrap();
    service
        .enqueue(&session, &MessageId("m_04".into()))
        .await
        .unwrap();

    // Release the held first turn; the loop's turn-end re-drain should pick up m_03+m_04 as ONE turn.
    turn_exec.gate.notify_one();

    // Wait for the follow-up turn to land.
    let mut tries = 0;
    while turn_exec.turn_count() < 2 && tries < 200 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        tries += 1;
    }

    let injected = turn_exec.injected.lock().unwrap();
    assert_eq!(
        injected.len(),
        2,
        "two messages mid-turn coalesce into ONE follow-up turn (2 total, not 3)"
    );
    assert_eq!(
        injected[1].counts.total, 2,
        "the follow-up turn carries both coalesced messages"
    );
}

#[tokio::test]
async fn after_tool_loop_waits_for_authoritative_final_completion_before_injecting() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let cfg = Config::default();
    let bell = Bell::new();
    let registry = AgentRegistry::new();
    let session = SessionId("s_after_tool_loop".into());
    let turn_exec = Arc::new(BoundaryWaitTurnExec::new(session.clone()));

    insert(
        &store,
        &dm(
            "m_after_tool_loop",
            "casey",
            "deliver only after the final model boundary",
        ),
    )
    .await;
    Inbox::new(&store)
        .enqueue_with_timing(
            &MessageId("m_after_tool_loop".into()),
            &session,
            DeliveryTiming::AfterToolLoop,
        )
        .await
        .unwrap();

    let _loop = EventLoop::spawn(
        session.clone(),
        LoopDeps {
            store: store.clone(),
            bell,
            registry,
            turn_exec: turn_exec.clone(),
            events: Arc::new(NullSink),
            project: PROJECT.into(),
            drain_limit: cfg.drain_limit,
            preview_chars: cfg.msg_preview_chars,
            completion_timeout: Duration::from_secs(45),
            provider_limit_default_cooldown: Duration::from_millis(50),
        },
    );

    tokio::time::timeout(Duration::from_secs(2), turn_exec.wait_started.notified())
        .await
        .expect("the timing policy should wait on the adapter boundary");
    tokio::task::yield_now().await;
    assert!(
        turn_exec.injected.lock().unwrap().is_empty(),
        "visible activity before final completion must not release after_tool_loop"
    );

    turn_exec.final_completion.notify_one();
    wait_for_delivery_state(&store, "m_after_tool_loop", "delivered").await;
    assert_eq!(turn_exec.injected.lock().unwrap().len(), 1);
}

/// An already-active native turn whose completion observer can fail without the
/// queued bus batch ever crossing the harness boundary.
struct UnavailableBoundaryExec {
    session: SessionId,
    active: AtomicBool,
    available: AtomicBool,
    block_wait: bool,
    wait_gate: Notify,
    waits: Notify,
    wait_times: Mutex<Vec<tokio::time::Instant>>,
    injected: Mutex<Vec<NexusBatch>>,
}

#[async_trait]
impl AgentTurnExecutionPort for UnavailableBoundaryExec {
    fn active_turn_sessions(&self) -> Vec<SessionId> {
        self.active
            .load(Ordering::SeqCst)
            .then(|| self.session.clone())
            .into_iter()
            .collect()
    }

    async fn wait_for_turn_completion(&self, _: &SessionId) -> Result<(), ContractError> {
        self.wait_times
            .lock()
            .unwrap()
            .push(tokio::time::Instant::now());
        self.waits.notify_one();
        if self.block_wait {
            self.wait_gate.notified().await;
        }
        if self.available.load(Ordering::SeqCst) {
            self.active.store(false, Ordering::SeqCst);
            Ok(())
        } else {
            Err(ContractError {
                code: -32004,
                message: "original active-turn boundary unavailable".into(),
            })
        }
    }

    async fn inject_turn(&self, _: &SessionId, batch: &NexusBatch) -> Result<(), ContractError> {
        self.injected.lock().unwrap().push(batch.clone());
        Ok(())
    }

    async fn inject_turn_observed(
        &self,
        recipient: &SessionId,
        batch: &NexusBatch,
        events: Arc<dyn EventSink>,
        accepted_event: WsEvent,
    ) -> InjectResult<()> {
        self.inject_turn(recipient, batch)
            .await
            .map_err(InjectError::Contract)?;
        events.emit(accepted_event).await;
        Ok(())
    }

    async fn launch(&self, _: SpawnRequest) -> Result<SpawnResponse, ContractError> {
        unreachable!()
    }
    async fn remove(&self, _: RemoveRequest) -> Result<RemoveResponse, ContractError> {
        unreachable!()
    }
}

struct BoundaryRetryFixture {
    store: Arc<Store>,
    session: SessionId,
    exec: Arc<UnavailableBoundaryExec>,
    events: RecordingSink,
    deps: LoopDeps,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for BoundaryRetryFixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl BoundaryRetryFixture {
    async fn new(timing: DeliveryTiming, block_wait: bool) -> Self {
        let store = Arc::new(Store::open(":memory:").await.unwrap());
        store.migrate().await.unwrap();
        let session = SessionId("s_boundary_retry".into());
        let exec = Arc::new(UnavailableBoundaryExec {
            session: session.clone(),
            active: AtomicBool::new(true),
            available: AtomicBool::new(false),
            block_wait,
            wait_gate: Notify::new(),
            waits: Notify::new(),
            wait_times: Mutex::new(Vec::new()),
            injected: Mutex::new(Vec::new()),
        });
        let events = RecordingSink::default();
        insert(
            &store,
            &dm("m_boundary_retry", "peer", "retain my unsent mail"),
        )
        .await;
        Inbox::new(&store)
            .enqueue_with_timing(&MessageId("m_boundary_retry".into()), &session, timing)
            .await
            .unwrap();
        let cfg = Config::default();
        let deps = LoopDeps {
            store: store.clone(),
            bell: Bell::new(),
            registry: AgentRegistry::new(),
            turn_exec: exec.clone(),
            events: Arc::new(events.clone()),
            project: PROJECT.into(),
            drain_limit: cfg.drain_limit,
            preview_chars: cfg.msg_preview_chars,
            completion_timeout: Duration::from_secs(45),
            provider_limit_default_cooldown: Duration::from_millis(50),
        };
        let task = EventLoop::spawn(session.clone(), deps.clone());
        Self {
            store,
            session,
            exec,
            events,
            deps,
            task,
        }
    }

    async fn wait_for_boundary_calls(&self, count: usize) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while self.exec.wait_times.lock().unwrap().len() < count {
                self.exec.waits.notified().await;
            }
        })
        .await
        .expect("boundary recheck must not be stranded without another bell");
        tokio::task::yield_now().await;
    }

    async fn assert_row(&self, state: &str, attempts: i64) {
        let mut rows = self.store.conn.query(
            "SELECT state, attempt_count, error_code FROM in_flight WHERE message_id = 'm_boundary_retry'", (),
        ).await.unwrap();
        let row = rows.next().await.unwrap().unwrap();
        assert_eq!(
            row.get::<String>(0).unwrap(),
            state,
            "an unavailable original-turn boundary must not settle unsent mail"
        );
        assert_eq!(
            row.get::<i64>(1).unwrap(),
            attempts,
            "waiting is not a delivery attempt"
        );
        assert_eq!(row.get::<Option<String>>(2).unwrap(), None);
    }

    async fn assert_unsubmitted(&self) {
        self.assert_row("notified", 0).await;
        assert!(self.exec.injected.lock().unwrap().is_empty());
        assert!(
            self.events.events().is_empty(),
            "unsent mail must have no canonical input echo or settlement"
        );
    }

    async fn assert_delivered_once(&self) {
        wait_for_delivery_state(&self.store, "m_boundary_retry", "delivered").await;
        self.assert_row("delivered", 1).await;
        assert_eq!(self.exec.injected.lock().unwrap().len(), 1);
        assert_eq!(
            self.events
                .events()
                .iter()
                .filter(|event| matches!(
                    event,
                    WsEvent::AgentUpdate {
                        kind: AgentUpdateKind::UserInput,
                        ..
                    }
                ))
                .count(),
            1
        );
        assert_eq!(
            self.events
                .events()
                .iter()
                .filter(|event| matches!(event, WsEvent::MessageDelivered { .. }))
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn initial_boundary_wait_error_retains_yield_turn_mail_until_idle_without_another_bell() {
    boundary_wait_idle_recovery(DeliveryTiming::YieldTurn).await;
}

#[tokio::test]
async fn initial_boundary_wait_error_retains_after_tool_loop_mail_until_idle_without_another_bell()
{
    boundary_wait_idle_recovery(DeliveryTiming::AfterToolLoop).await;
}

async fn boundary_wait_idle_recovery(timing: DeliveryTiming) {
    let fixture = BoundaryRetryFixture::new(timing, false).await;
    fixture.wait_for_boundary_calls(1).await;
    fixture.assert_unsubmitted().await;
    fixture.exec.active.store(false, Ordering::SeqCst);
    fixture.assert_delivered_once().await;
    fixture.deps.bell.ring(&fixture.session);
    tokio::time::sleep(Duration::from_millis(50)).await;
    fixture.assert_delivered_once().await;
}

#[tokio::test]
async fn initial_boundary_wait_error_recovers_observer_without_replaying_input() {
    let fixture = BoundaryRetryFixture::new(DeliveryTiming::AfterToolLoop, false).await;
    fixture.wait_for_boundary_calls(1).await;
    fixture.assert_unsubmitted().await;
    fixture.exec.available.store(true, Ordering::SeqCst);
    fixture.wait_for_boundary_calls(2).await;
    fixture.assert_delivered_once().await;
}

#[tokio::test]
async fn initial_boundary_wait_errors_are_backed_off_even_when_new_bells_arrive() {
    let fixture = BoundaryRetryFixture::new(DeliveryTiming::YieldTurn, false).await;
    fixture.wait_for_boundary_calls(1).await;
    fixture.assert_unsubmitted().await;
    for _ in 0..20 {
        fixture.deps.bell.ring(&fixture.session);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        fixture.exec.wait_times.lock().unwrap().len(),
        1,
        "bells must not bypass error backoff"
    );
    fixture.wait_for_boundary_calls(3).await;
    fixture.assert_unsubmitted().await;
    let times = fixture.exec.wait_times.lock().unwrap().clone();
    assert!(
        times
            .windows(2)
            .all(|pair| pair[1].duration_since(pair[0]) >= Duration::from_millis(500)),
        "immediate errors must not become a busy loop: {times:?}"
    );
    fixture.exec.active.store(false, Ordering::SeqCst);
    fixture.assert_delivered_once().await;
}

#[tokio::test]
async fn initial_boundary_wait_honors_holds_set_during_failed_wait() {
    boundary_wait_hold_case(false).await;
}

#[tokio::test]
async fn initial_boundary_wait_honors_holds_set_during_successful_wait() {
    boundary_wait_hold_case(true).await;
}

async fn boundary_wait_hold_case(available: bool) {
    for state in [AgentState::Paused, AgentState::Offline] {
        let fixture = BoundaryRetryFixture::new(DeliveryTiming::AfterToolLoop, true).await;
        fixture.wait_for_boundary_calls(1).await;
        fixture.deps.registry.set(&fixture.session, state);
        fixture.exec.available.store(available, Ordering::SeqCst);
        fixture.exec.wait_gate.notify_one();
        tokio::time::sleep(Duration::from_millis(1100)).await;
        fixture.assert_unsubmitted().await;
        assert_eq!(
            fixture.deps.registry.get(&fixture.session),
            state,
            "drain cleanup must preserve external holds"
        );
        assert_eq!(fixture.exec.wait_times.lock().unwrap().len(), 1);
        fixture.exec.active.store(false, Ordering::SeqCst);
        fixture
            .deps
            .registry
            .set(&fixture.session, AgentState::Idle);
        fixture.deps.bell.ring(&fixture.session);
        fixture.assert_delivered_once().await;
    }
}

#[tokio::test]
async fn initial_boundary_wait_abort_cancels_recheck_and_reattach_preserves_unsent_mail() {
    let mut fixture = BoundaryRetryFixture::new(DeliveryTiming::YieldTurn, false).await;
    fixture.wait_for_boundary_calls(1).await;
    fixture.assert_unsubmitted().await;
    fixture.task.abort();
    assert!((&mut fixture.task).await.unwrap_err().is_cancelled());
    fixture.exec.active.store(false, Ordering::SeqCst);
    fixture.deps.bell.ring(&fixture.session);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    fixture.assert_unsubmitted().await;
    assert_eq!(fixture.exec.wait_times.lock().unwrap().len(), 1);
    fixture.task = EventLoop::spawn(fixture.session.clone(), fixture.deps.clone());
    fixture.assert_delivered_once().await;
}

#[tokio::test]
async fn active_turn_interrupt_without_adapter_support_waits_and_delivers_at_the_boundary() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let cfg = Config::default();
    let session = SessionId("s_interrupt_unsupported".into());
    let turn_exec = Arc::new(BoundaryWaitTurnExec::new(session.clone()));

    insert(
        &store,
        &dm(
            "m_interrupt_unsupported",
            "casey",
            "a busy uninterruptible agent must still receive this",
        ),
    )
    .await;
    Inbox::new(&store)
        .enqueue_with_timing(
            &MessageId("m_interrupt_unsupported".into()),
            &session,
            DeliveryTiming::Interrupt,
        )
        .await
        .unwrap();

    let _loop = EventLoop::spawn(
        session.clone(),
        LoopDeps {
            store: store.clone(),
            bell: Bell::new(),
            registry: AgentRegistry::new(),
            turn_exec: turn_exec.clone(),
            events: Arc::new(NullSink),
            project: PROJECT.into(),
            drain_limit: cfg.drain_limit,
            preview_chars: cfg.msg_preview_chars,
            completion_timeout: Duration::from_secs(45),
            provider_limit_default_cooldown: Duration::from_millis(50),
        },
    );

    // The loop must WAIT on the adapter boundary rather than settle the batch as an error.
    tokio::time::timeout(Duration::from_secs(2), turn_exec.wait_started.notified())
        .await
        .expect("a busy uninterruptible target must wait for the turn boundary");
    tokio::task::yield_now().await;
    assert!(
        turn_exec.injected.lock().unwrap().is_empty(),
        "nothing may be injected while the original turn is still active"
    );

    turn_exec.final_completion.notify_one();
    wait_for_delivery_state(&store, "m_interrupt_unsupported", "delivered").await;
    assert_eq!(
        turn_exec.injected.lock().unwrap().len(),
        1,
        "a busy uninterruptible agent must still receive the message at its turn boundary"
    );

    let mut rows = store
        .conn
        .query(
            "SELECT COUNT(*) FROM in_flight WHERE message_id = ?1 AND state = 'error'",
            ["m_interrupt_unsupported"],
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(
        row.get::<i64>(0).unwrap(),
        0,
        "the message must never be dead-lettered"
    );
}

#[tokio::test]
async fn active_native_turn_receives_new_bus_batch_before_current_turn_completes() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let cfg = Config::default();
    let bell = Bell::new();
    let registry = AgentRegistry::new();
    let session = SessionId("s_native_steer".into());
    let turn_exec = Arc::new(NativeSteerTurnExec::new(session.clone()));
    let events = RecordingSink::default();
    let service = DispatchService::new(ServiceDeps {
        store: store.clone(),
        bell: bell.clone(),
        registry: registry.clone(),
        project: PROJECT.into(),
        drain_limit: cfg.drain_limit,
        preview_chars: cfg.msg_preview_chars,
    });
    let _loop = EventLoop::spawn(
        session.clone(),
        LoopDeps {
            store: store.clone(),
            bell,
            registry,
            turn_exec: turn_exec.clone(),
            events: Arc::new(events.clone()),
            project: PROJECT.into(),
            drain_limit: cfg.drain_limit,
            preview_chars: cfg.msg_preview_chars,
            completion_timeout: Duration::from_secs(45),
            provider_limit_default_cooldown: Duration::from_millis(50),
        },
    );

    insert(
        &store,
        &dm("m_active_first", "Alex", "hold the active turn"),
    )
    .await;
    service
        .enqueue(&session, &MessageId("m_active_first".into()))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), turn_exec.first_started.notified())
        .await
        .expect("first turn should be active");

    insert(
        &store,
        &dm("m_active_steer", "casey", "arrive at the next yield"),
    )
    .await;
    service
        .enqueue(&session, &MessageId("m_active_steer".into()))
        .await
        .unwrap();

    tokio::time::timeout(
        Duration::from_millis(500),
        turn_exec.steer_accepted.notified(),
    )
    .await
    .expect("mid-turn DM should be admitted without waiting for completion");
    wait_for_delivery_state(&store, "m_active_steer", "delivered").await;

    assert_eq!(turn_exec.injected.lock().unwrap().len(), 1);
    let steered = turn_exec.steered.lock().unwrap();
    assert_eq!(steered.len(), 1);
    assert!(steered[0].contains("arrive at the next yield"));
    assert_eq!(
        events
            .events()
            .iter()
            .filter(|event| matches!(
                event,
                WsEvent::AgentUpdate {
                    kind: AgentUpdateKind::UserInput,
                    ..
                }
            ))
            .count(),
        1,
        "native steer must emit one accepted input event"
    );

    turn_exec.first_gate.notify_one();
    wait_for_delivery_state(&store, "m_active_first", "delivered").await;
}

#[tokio::test]
async fn interrupt_and_send_redrives_after_cancel_without_deadlocking_the_active_turn() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let cfg = Config::default();
    let bell = Bell::new();
    let registry = AgentRegistry::new();
    let session = SessionId("s_serialized_interrupt".into());
    let turn_exec = Arc::new(SerializedInterruptAndSendTurnExec::new(session.clone()));
    let events = RecordingSink::default();
    let service = DispatchService::new(ServiceDeps {
        store: store.clone(),
        bell: bell.clone(),
        registry: registry.clone(),
        project: PROJECT.into(),
        drain_limit: cfg.drain_limit,
        preview_chars: cfg.msg_preview_chars,
    });
    let _loop = EventLoop::spawn(
        session.clone(),
        LoopDeps {
            store: store.clone(),
            bell,
            registry,
            turn_exec: turn_exec.clone(),
            events: Arc::new(events.clone()),
            project: PROJECT.into(),
            drain_limit: cfg.drain_limit,
            preview_chars: cfg.msg_preview_chars,
            completion_timeout: Duration::from_secs(45),
            provider_limit_default_cooldown: Duration::from_millis(50),
        },
    );

    insert(
        &store,
        &dm("m_serialized_first", "Alex", "current model turn"),
    )
    .await;
    service
        .enqueue(&session, &MessageId("m_serialized_first".into()))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), turn_exec.first_started.notified())
        .await
        .expect("first prompt should be active");

    insert(
        &store,
        &dm(
            "m_serialized_followup",
            "casey",
            "interrupt, then deliver this as the next prompt",
        ),
    )
    .await;
    service
        .enqueue(&session, &MessageId("m_serialized_followup".into()))
        .await
        .unwrap();

    for _ in 0..200 {
        if turn_exec.interrupts.load(Ordering::SeqCst) > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        turn_exec.interrupts.load(Ordering::SeqCst),
        1,
        "one accepted cancellation must not self-ring into a cancellation storm"
    );
    let mut rows = store
        .conn
        .query(
            "SELECT state, attempt_count FROM in_flight WHERE message_id = ?1",
            ["m_serialized_followup"],
        )
        .await
        .unwrap();
    let row = rows
        .next()
        .await
        .unwrap()
        .expect("replacement delivery row");
    assert_eq!(row.get::<String>(0).unwrap(), "notified");
    assert_eq!(
        row.get::<i64>(1).unwrap(),
        0,
        "cancelling the old turn must not claim an unsent replacement prompt"
    );

    turn_exec.terminal_release.notify_one();

    wait_for_delivery_state(&store, "m_serialized_followup", "delivered").await;
    wait_for_delivery_state(&store, "m_serialized_first", "delivered").await;

    let injected = turn_exec.injected.lock().unwrap();
    assert_eq!(
        injected.len(),
        2,
        "the replacement must run as the next turn"
    );
    assert_eq!(
        injected[1].message_ids,
        vec![MessageId("m_serialized_followup".into())]
    );
    assert_eq!(turn_exec.interrupts.load(Ordering::SeqCst), 1);
    assert_eq!(
        turn_exec.steer_calls.load(Ordering::SeqCst),
        0,
        "the event loop must not await a serialized replacement while its active future is paused"
    );
    let accepted_client_ids = events
        .events()
        .into_iter()
        .filter_map(|event| match event {
            WsEvent::AgentUpdate {
                kind: AgentUpdateKind::UserInput,
                data,
                ..
            } => data
                .get("clientMessageId")
                .and_then(Value::as_str)
                .map(str::to_owned),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        accepted_client_ids,
        vec![
            "bus:m_serialized_first".to_string(),
            "bus:m_serialized_followup".to_string(),
        ],
        "the original and replacement accepted boundaries must each be projected exactly once"
    );
}

#[tokio::test]
async fn accepted_interrupt_redrives_once_after_the_active_turn_times_out() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let cfg = Config::default();
    let bell = Bell::new();
    let registry = AgentRegistry::new();
    let session = SessionId("s_interrupt_timeout".into());
    let turn_exec = Arc::new(SerializedInterruptAndSendTurnExec::new(session.clone()));
    let events = RecordingSink::default();
    let service = DispatchService::new(ServiceDeps {
        store: store.clone(),
        bell: bell.clone(),
        registry: registry.clone(),
        project: PROJECT.into(),
        drain_limit: cfg.drain_limit,
        preview_chars: cfg.msg_preview_chars,
    });
    let _loop = EventLoop::spawn(
        session.clone(),
        LoopDeps {
            store: store.clone(),
            bell,
            registry,
            turn_exec: turn_exec.clone(),
            events: Arc::new(events.clone()),
            project: PROJECT.into(),
            drain_limit: cfg.drain_limit,
            preview_chars: cfg.msg_preview_chars,
            completion_timeout: Duration::from_millis(250),
            provider_limit_default_cooldown: Duration::from_millis(50),
        },
    );

    insert(&store, &dm("m_timeout_active", "Alex", "never completes")).await;
    service
        .enqueue(&session, &MessageId("m_timeout_active".into()))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), turn_exec.first_started.notified())
        .await
        .expect("first prompt should be active");

    insert(
        &store,
        &dm("m_timeout_replacement", "casey", "deliver after timeout"),
    )
    .await;
    service
        .enqueue(&session, &MessageId("m_timeout_replacement".into()))
        .await
        .unwrap();
    for _ in 0..200 {
        if turn_exec.interrupts.load(Ordering::SeqCst) == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let mut rows = store
        .conn
        .query(
            "SELECT state, attempt_count FROM in_flight WHERE message_id = ?1",
            ["m_timeout_replacement"],
        )
        .await
        .unwrap();
    let row = rows
        .next()
        .await
        .unwrap()
        .expect("replacement delivery row");
    assert_eq!(row.get::<String>(0).unwrap(), "notified");
    assert_eq!(row.get::<i64>(1).unwrap(), 0);
    assert_eq!(
        events
            .events()
            .into_iter()
            .filter(|event| matches!(
                event,
                WsEvent::AgentUpdate {
                    kind: AgentUpdateKind::UserInput,
                    ..
                }
            ))
            .count(),
        1,
        "the replacement is not accepted before the active turn closes"
    );

    wait_for_delivery_state(&store, "m_timeout_active", "error").await;
    wait_for_delivery_state(&store, "m_timeout_replacement", "delivered").await;
    assert_eq!(turn_exec.interrupts.load(Ordering::SeqCst), 1);
    let injected = turn_exec.injected.lock().unwrap();
    assert_eq!(injected.len(), 2);
    assert_eq!(
        injected[1].message_ids,
        vec![MessageId("m_timeout_replacement".into())]
    );
    let accepted_client_ids = events
        .events()
        .into_iter()
        .filter_map(|event| match event {
            WsEvent::AgentUpdate {
                kind: AgentUpdateKind::UserInput,
                data,
                ..
            } => data
                .get("clientMessageId")
                .and_then(Value::as_str)
                .map(str::to_owned),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        accepted_client_ids,
        vec![
            "bus:m_timeout_active".to_string(),
            "bus:m_timeout_replacement".to_string(),
        ]
    );
}

#[tokio::test]
async fn later_arrivals_coalesce_without_reinterrupting_the_canceled_turn() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let cfg = Config::default();
    let bell = Bell::new();
    let registry = AgentRegistry::new();
    let session = SessionId("s_interrupt_coalesce".into());
    let turn_exec = Arc::new(SerializedInterruptAndSendTurnExec::new(session.clone()));
    let events = RecordingSink::default();
    let service = DispatchService::new(ServiceDeps {
        store: store.clone(),
        bell: bell.clone(),
        registry: registry.clone(),
        project: PROJECT.into(),
        drain_limit: cfg.drain_limit,
        preview_chars: cfg.msg_preview_chars,
    });
    let _loop = EventLoop::spawn(
        session.clone(),
        LoopDeps {
            store: store.clone(),
            bell,
            registry,
            turn_exec: turn_exec.clone(),
            events: Arc::new(events.clone()),
            project: PROJECT.into(),
            drain_limit: cfg.drain_limit,
            preview_chars: cfg.msg_preview_chars,
            completion_timeout: Duration::from_secs(45),
            provider_limit_default_cooldown: Duration::from_millis(50),
        },
    );

    insert(&store, &dm("m_coalesce_active", "Alex", "current turn")).await;
    service
        .enqueue(&session, &MessageId("m_coalesce_active".into()))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), turn_exec.first_started.notified())
        .await
        .expect("first prompt should be active");

    insert(&store, &dm("m_coalesce_one", "casey", "first replacement")).await;
    service
        .enqueue(&session, &MessageId("m_coalesce_one".into()))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while turn_exec.interrupts.load(Ordering::SeqCst) != 1 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the first replacement must trigger cancellation before later arrivals");
    let mut rows = store
        .conn
        .query(
            "SELECT state, attempt_count FROM in_flight WHERE message_id = ?1",
            ["m_coalesce_one"],
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().expect("first replacement row");
    assert_eq!(row.get::<String>(0).unwrap(), "notified");
    assert_eq!(row.get::<i64>(1).unwrap(), 0);

    for message_id in ["m_coalesce_two", "m_coalesce_three"] {
        insert(&store, &dm(message_id, "casey", message_id)).await;
        service
            .enqueue(&session, &MessageId(message_id.into()))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(
        turn_exec.interrupts.load(Ordering::SeqCst),
        1,
        "later bells must coalesce without sending another interrupt"
    );
    for message_id in ["m_coalesce_one", "m_coalesce_two", "m_coalesce_three"] {
        let mut rows = store
            .conn
            .query(
                "SELECT state, attempt_count FROM in_flight WHERE message_id = ?1",
                [message_id],
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().expect("coalesced delivery row");
        assert_eq!(row.get::<String>(0).unwrap(), "notified");
        assert_eq!(row.get::<i64>(1).unwrap(), 0);
    }

    turn_exec.terminal_release.notify_one();
    wait_for_delivery_state(&store, "m_coalesce_active", "delivered").await;
    for message_id in ["m_coalesce_one", "m_coalesce_two", "m_coalesce_three"] {
        wait_for_delivery_state(&store, message_id, "delivered").await;
        let mut rows = store
            .conn
            .query(
                "SELECT attempt_count FROM in_flight WHERE message_id = ?1",
                [message_id],
            )
            .await
            .unwrap();
        assert_eq!(
            rows.next()
                .await
                .unwrap()
                .expect("delivered replacement row")
                .get::<i64>(0)
                .unwrap(),
            1,
            "each coalesced row crosses the harness boundary exactly once"
        );
    }
    assert_eq!(turn_exec.interrupts.load(Ordering::SeqCst), 1);
    let injected = turn_exec.injected.lock().unwrap();
    assert_eq!(injected.len(), 2);
    assert_eq!(
        injected[1].message_ids,
        vec![
            MessageId("m_coalesce_one".into()),
            MessageId("m_coalesce_two".into()),
            MessageId("m_coalesce_three".into()),
        ]
    );
    let accepted_client_ids = events
        .events()
        .into_iter()
        .filter_map(|event| match event {
            WsEvent::AgentUpdate {
                kind: AgentUpdateKind::UserInput,
                data,
                ..
            } => data
                .get("clientMessageId")
                .and_then(Value::as_str)
                .map(str::to_owned),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        accepted_client_ids,
        vec![
            "bus:m_coalesce_active".to_string(),
            "bus:m_coalesce_one,m_coalesce_two,m_coalesce_three".to_string(),
        ],
        "the combined replacement batch is accepted exactly once"
    );
}

#[tokio::test]
async fn interrupted_turn_error_still_redrives_the_unclaimed_replacement_once() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let cfg = Config::default();
    let bell = Bell::new();
    let registry = AgentRegistry::new();
    let session = SessionId("s_interrupted_error".into());
    let turn_exec = Arc::new(SerializedInterruptAndSendTurnExec::with_first_error(
        session.clone(),
        ContractError {
            code: -32000,
            message: "active prompt was canceled".into(),
        },
    ));
    let events = RecordingSink::default();
    let service = DispatchService::new(ServiceDeps {
        store: store.clone(),
        bell: bell.clone(),
        registry: registry.clone(),
        project: PROJECT.into(),
        drain_limit: cfg.drain_limit,
        preview_chars: cfg.msg_preview_chars,
    });
    let _loop = EventLoop::spawn(
        session.clone(),
        LoopDeps {
            store: store.clone(),
            bell,
            registry,
            turn_exec: turn_exec.clone(),
            events: Arc::new(events.clone()),
            project: PROJECT.into(),
            drain_limit: cfg.drain_limit,
            preview_chars: cfg.msg_preview_chars,
            completion_timeout: Duration::from_secs(45),
            provider_limit_default_cooldown: Duration::from_millis(50),
        },
    );

    insert(&store, &dm("m_canceled_error", "Alex", "cancel this turn")).await;
    service
        .enqueue(&session, &MessageId("m_canceled_error".into()))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), turn_exec.first_started.notified())
        .await
        .expect("first prompt should be active");

    insert(
        &store,
        &dm("m_after_canceled_error", "casey", "deliver after cancel"),
    )
    .await;
    service
        .enqueue(&session, &MessageId("m_after_canceled_error".into()))
        .await
        .unwrap();
    for _ in 0..200 {
        if turn_exec.interrupts.load(Ordering::SeqCst) == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    turn_exec.terminal_release.notify_one();

    wait_for_delivery_state(&store, "m_canceled_error", "error").await;
    wait_for_delivery_state(&store, "m_after_canceled_error", "delivered").await;
    assert_eq!(turn_exec.interrupts.load(Ordering::SeqCst), 1);
    assert_eq!(turn_exec.injected.lock().unwrap().len(), 2);
    let accepted_client_ids = events
        .events()
        .into_iter()
        .filter_map(|event| match event {
            WsEvent::AgentUpdate {
                kind: AgentUpdateKind::UserInput,
                data,
                ..
            } => data
                .get("clientMessageId")
                .and_then(Value::as_str)
                .map(str::to_owned),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        accepted_client_ids,
        vec![
            "bus:m_canceled_error".to_string(),
            "bus:m_after_canceled_error".to_string(),
        ]
    );
}

#[tokio::test]
async fn refused_interrupt_keeps_the_replacement_for_turn_boundary_delivery() {
    refused_interrupt_replacement_case(false, false).await;
}

#[tokio::test]
async fn refused_interrupt_redrives_unsubmitted_replacement_after_turn_error() {
    refused_interrupt_replacement_case(true, false).await;
}

#[tokio::test]
async fn refused_interrupt_redrives_unsubmitted_replacement_after_turn_timeout() {
    refused_interrupt_replacement_case(false, true).await;
}

async fn refused_interrupt_replacement_case(first_errors: bool, first_times_out: bool) {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let cfg = Config::default();
    let bell = Bell::new();
    let registry = AgentRegistry::new();
    let session = SessionId("s_interrupt_failure".into());
    let mut turn_exec = SerializedInterruptAndSendTurnExec::with_interrupt_error(
        session.clone(),
        ContractError {
            code: -32001,
            message: "session/cancel failed".into(),
        },
    );
    if first_errors {
        turn_exec.first_error = Some(ContractError {
            code: -32000,
            message: "the original turn failed independently".into(),
        });
    }
    let turn_exec = Arc::new(turn_exec);
    let events = RecordingSink::default();
    let service = DispatchService::new(ServiceDeps {
        store: store.clone(),
        bell: bell.clone(),
        registry: registry.clone(),
        project: PROJECT.into(),
        drain_limit: cfg.drain_limit,
        preview_chars: cfg.msg_preview_chars,
    });
    let _loop = EventLoop::spawn(
        session.clone(),
        LoopDeps {
            store: store.clone(),
            bell,
            registry,
            turn_exec: turn_exec.clone(),
            events: Arc::new(events.clone()),
            project: PROJECT.into(),
            drain_limit: cfg.drain_limit,
            preview_chars: cfg.msg_preview_chars,
            completion_timeout: if first_times_out {
                Duration::from_secs(1)
            } else {
                Duration::from_secs(45)
            },
            provider_limit_default_cooldown: Duration::from_millis(50),
        },
    );

    insert(
        &store,
        &dm("m_interrupt_failure_first", "Alex", "active turn"),
    )
    .await;
    service
        .enqueue(&session, &MessageId("m_interrupt_failure_first".into()))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), turn_exec.first_started.notified())
        .await
        .expect("first prompt should be active");

    insert(
        &store,
        &dm(
            "m_interrupt_failure_replacement",
            "casey",
            "retain this unsubmitted message until the boundary",
        ),
    )
    .await;
    service
        .enqueue(
            &session,
            &MessageId("m_interrupt_failure_replacement".into()),
        )
        .await
        .unwrap();

    // The refused cancel must not invent an attempt, inject, or emit an accepted event — but it
    // must also not discard the message. It stays queued for turn-boundary delivery.
    tokio::time::timeout(Duration::from_secs(2), async {
        while turn_exec.interrupts.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the interrupt should have been attempted");
    assert_eq!(turn_exec.interrupts.load(Ordering::SeqCst), 1);
    assert_eq!(turn_exec.injected.lock().unwrap().len(), 1);
    let mut rows = store
        .conn
        .query(
            "SELECT attempt_count, state FROM in_flight WHERE message_id = ?1",
            ["m_interrupt_failure_replacement"],
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().expect("replacement row");
    assert_eq!(
        row.get::<i64>(0).unwrap(),
        0,
        "a failed cancel must not invent a replacement prompt attempt"
    );
    assert_ne!(
        row.get::<String>(1).unwrap(),
        "error",
        "a refused cancel must not dead-letter the replacement"
    );
    let accepted_client_ids = events
        .events()
        .into_iter()
        .filter_map(|event| match event {
            WsEvent::AgentUpdate {
                kind: AgentUpdateKind::UserInput,
                data,
                ..
            } => data
                .get("clientMessageId")
                .and_then(Value::as_str)
                .map(str::to_owned),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        accepted_client_ids,
        vec!["bus:m_interrupt_failure_first".to_string()]
    );

    // Let the original turn finish. The replacement the refused cancel could not deliver early
    // must now be delivered at the turn boundary rather than stay lost.
    if !first_times_out {
        turn_exec.interrupt_requested.notify_one();
        turn_exec.terminal_release.notify_one();
    }
    wait_for_delivery_state(
        &store,
        "m_interrupt_failure_first",
        if first_errors || first_times_out {
            "error"
        } else {
            "delivered"
        },
    )
    .await;
    wait_for_delivery_state(&store, "m_interrupt_failure_replacement", "delivered").await;
    assert_eq!(
        turn_exec.injected.lock().unwrap().len(),
        2,
        "the replacement must reach the agent once the active turn ends"
    );
    assert_eq!(turn_exec.interrupts.load(Ordering::SeqCst), 1);
    assert_eq!(turn_exec.steer_calls.load(Ordering::SeqCst), 0);
    let mut rows = store
        .conn
        .query(
            "SELECT attempt_count FROM in_flight WHERE message_id = ?1",
            ["m_interrupt_failure_replacement"],
        )
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
        1
    );
    let replacement_accepts = events
        .events()
        .into_iter()
        .filter(|event| {
            matches!(event, WsEvent::AgentUpdate {
            kind: AgentUpdateKind::UserInput, data, ..
        } if data.get("clientMessageId").and_then(Value::as_str)
            == Some("bus:m_interrupt_failure_replacement"))
        })
        .count();
    assert_eq!(
        replacement_accepts, 1,
        "replacement acceptance must not be duplicated"
    );
    _loop.abort();
}

#[tokio::test]
async fn rejected_native_steer_race_delivers_once_after_current_turn_completes() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let cfg = Config::default();
    let bell = Bell::new();
    let registry = AgentRegistry::new();
    let session = SessionId("s_native_steer_race".into());
    let turn_exec = Arc::new(NativeSteerRaceTurnExec::new(session.clone()));
    let service = DispatchService::new(ServiceDeps {
        store: store.clone(),
        bell: bell.clone(),
        registry: registry.clone(),
        project: PROJECT.into(),
        drain_limit: cfg.drain_limit,
        preview_chars: cfg.msg_preview_chars,
    });
    let _loop = EventLoop::spawn(
        session.clone(),
        LoopDeps {
            store: store.clone(),
            bell,
            registry,
            turn_exec: turn_exec.clone(),
            events: Arc::new(NullSink),
            project: PROJECT.into(),
            drain_limit: cfg.drain_limit,
            preview_chars: cfg.msg_preview_chars,
            completion_timeout: Duration::from_secs(45),
            provider_limit_default_cooldown: Duration::from_millis(50),
        },
    );

    insert(&store, &dm("m_race_first", "Alex", "finish this turn")).await;
    service
        .enqueue(&session, &MessageId("m_race_first".into()))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), turn_exec.first_started.notified())
        .await
        .expect("first turn should be active");

    insert(
        &store,
        &dm("m_race_followup", "casey", "deliver once after the race"),
    )
    .await;
    service
        .enqueue(&session, &MessageId("m_race_followup".into()))
        .await
        .unwrap();
    tokio::time::timeout(
        Duration::from_millis(500),
        turn_exec.steer_rejected.notified(),
    )
    .await
    .expect("native steer precondition should reject promptly");
    wait_for_delivery_state(&store, "m_race_followup", "notified").await;

    turn_exec.first_gate.notify_one();
    wait_for_delivery_state(&store, "m_race_followup", "delivered").await;

    let injected = turn_exec.injected.lock().unwrap();
    assert_eq!(injected.len(), 2);
    assert_eq!(
        injected[1].message_ids,
        vec![MessageId("m_race_followup".into())]
    );
}

#[tokio::test]
async fn non_steerable_native_turn_keeps_bus_mail_for_post_turn_delivery() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let cfg = Config::default();
    let bell = Bell::new();
    let registry = AgentRegistry::new();
    let session = SessionId("s_native_review".into());
    let turn_exec = Arc::new(NativeSteerRaceTurnExec::with_error(
        session.clone(),
        ContractError {
            code: -32004,
            message: "cannot steer a review turn".into(),
        },
    ));
    let service = DispatchService::new(ServiceDeps {
        store: store.clone(),
        bell: bell.clone(),
        registry: registry.clone(),
        project: PROJECT.into(),
        drain_limit: cfg.drain_limit,
        preview_chars: cfg.msg_preview_chars,
    });
    let _loop = EventLoop::spawn(
        session.clone(),
        LoopDeps {
            store: store.clone(),
            bell,
            registry,
            turn_exec: turn_exec.clone(),
            events: Arc::new(NullSink),
            project: PROJECT.into(),
            drain_limit: cfg.drain_limit,
            preview_chars: cfg.msg_preview_chars,
            completion_timeout: Duration::from_secs(45),
            provider_limit_default_cooldown: Duration::from_millis(50),
        },
    );

    insert(&store, &dm("m_review_first", "Alex", "review in progress")).await;
    service
        .enqueue(&session, &MessageId("m_review_first".into()))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), turn_exec.first_started.notified())
        .await
        .expect("review turn should be active");

    insert(
        &store,
        &dm("m_review_followup", "casey", "wait without being lost"),
    )
    .await;
    service
        .enqueue(&session, &MessageId("m_review_followup".into()))
        .await
        .unwrap();
    tokio::time::timeout(
        Duration::from_millis(500),
        turn_exec.steer_rejected.notified(),
    )
    .await
    .expect("non-steerable turn should reject promptly");
    wait_for_delivery_state(&store, "m_review_followup", "notified").await;

    turn_exec.first_gate.notify_one();
    wait_for_delivery_state(&store, "m_review_followup", "delivered").await;
    let injected = turn_exec.injected.lock().unwrap();
    assert_eq!(injected.len(), 2);
    assert_eq!(
        injected[1].message_ids,
        vec![MessageId("m_review_followup".into())]
    );
}

#[tokio::test]
async fn interrupted_turn_completion_redrains_queued_followup() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let cfg = Config::default();
    let bell = Bell::new();
    let registry = AgentRegistry::new();
    let turn_exec = Arc::new(InterruptedTurnExec::new());
    let session = SessionId("s_cancel_drain".into());
    let service = DispatchService::new(ServiceDeps {
        store: store.clone(),
        bell: bell.clone(),
        registry: registry.clone(),
        project: PROJECT.into(),
        drain_limit: cfg.drain_limit,
        preview_chars: cfg.msg_preview_chars,
    });
    let _loop = EventLoop::spawn(
        session.clone(),
        LoopDeps {
            store: store.clone(),
            bell,
            registry,
            turn_exec: turn_exec.clone(),
            events: Arc::new(NullSink),
            project: PROJECT.into(),
            drain_limit: cfg.drain_limit,
            preview_chars: cfg.msg_preview_chars,
            completion_timeout: Duration::from_secs(45),
            provider_limit_default_cooldown: Duration::from_millis(50),
        },
    );

    insert(&store, &dm("m_before_cancel", "Alex", "current turn")).await;
    service
        .enqueue(&session, &MessageId("m_before_cancel".into()))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), turn_exec.first_started.notified())
        .await
        .expect("first turn should be active before interruption");

    insert(
        &store,
        &dm("m_after_cancel", "Alex", "run after cancellation"),
    )
    .await;
    service
        .enqueue(&session, &MessageId("m_after_cancel".into()))
        .await
        .unwrap();

    // This is the normalized interrupted terminal boundary. No later message or manual bell is
    // allowed to rescue the queued follow-up.
    turn_exec.interrupted.notify_one();
    for _ in 0..200 {
        if turn_exec.turn_count() == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let injected = turn_exec.injected.lock().unwrap();
    assert_eq!(injected.len(), 2, "the queued follow-up must drain once");
    assert_eq!(
        injected[0].message_ids,
        vec![MessageId("m_before_cancel".into())]
    );
    assert_eq!(
        injected[1].message_ids,
        vec![MessageId("m_after_cancel".into())]
    );
}

#[tokio::test]
async fn drained_bus_batch_is_projected_as_session_visible_user_input() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let cfg = Config::default();

    let bell = Bell::new();
    let registry = AgentRegistry::new();
    let turn_exec = Arc::new(MockTurnExec::new());
    let events = RecordingSink::default();
    let session = SessionId("s_ana".into());

    let service = DispatchService::new(ServiceDeps {
        store: store.clone(),
        bell: bell.clone(),
        registry: registry.clone(),
        project: PROJECT.into(),
        drain_limit: cfg.drain_limit,
        preview_chars: cfg.msg_preview_chars,
    });

    let _loop = EventLoop::spawn(
        session.clone(),
        LoopDeps {
            store: store.clone(),
            bell: bell.clone(),
            registry,
            turn_exec: turn_exec.clone(),
            events: Arc::new(events.clone()),
            project: PROJECT.into(),
            drain_limit: cfg.drain_limit,
            preview_chars: cfg.msg_preview_chars,
            completion_timeout: std::time::Duration::from_secs(45),
            provider_limit_default_cooldown: std::time::Duration::from_millis(50),
        },
    );

    insert(
        &store,
        &dm("m_session_visible", "ben", "visible from steer"),
    )
    .await;
    service
        .enqueue(&session, &MessageId("m_session_visible".into()))
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(2), turn_exec.started.notified())
        .await
        .expect("bus turn should start");

    turn_exec.gate.notify_one();
    let input = wait_for_user_input(&events, &session).await;

    assert_eq!(input["source"], "bus");
    assert_eq!(input["messageIds"].as_array().map(Vec::len), Some(1));
    assert_eq!(input["messageIds"][0], "m_session_visible");
    assert_eq!(input["clientMessageId"], "bus:m_session_visible");
    assert!(
        input["text"]
            .as_str()
            .expect("session input text")
            .contains("visible from steer"),
        "session input should show what was injected into the harness: {input:?}"
    );
}

#[tokio::test]
async fn per_agent_loop_injects_full_message_bodies_not_previews() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let cfg = Config::default();

    let bell = Bell::new();
    let registry = AgentRegistry::new();
    let turn_exec = Arc::new(MockTurnExec::new());
    let events = RecordingSink::default();
    let session = SessionId("s_ana".into());

    let service = DispatchService::new(ServiceDeps {
        store: store.clone(),
        bell: bell.clone(),
        registry: registry.clone(),
        project: PROJECT.into(),
        drain_limit: cfg.drain_limit,
        preview_chars: cfg.msg_preview_chars,
    });

    let _loop = EventLoop::spawn(
        session.clone(),
        LoopDeps {
            store: store.clone(),
            bell,
            registry,
            turn_exec: turn_exec.clone(),
            events: Arc::new(events.clone()),
            project: PROJECT.into(),
            drain_limit: cfg.drain_limit,
            preview_chars: cfg.msg_preview_chars,
            completion_timeout: std::time::Duration::from_secs(45),
            provider_limit_default_cooldown: std::time::Duration::from_millis(50),
        },
    );

    let long_body = format!(
        "{} END_OF_FULL_BODY",
        "x".repeat(cfg.msg_preview_chars as usize + 64)
    );
    insert(&store, &dm("m_long", "ben", &long_body)).await;
    service
        .enqueue(&session, &MessageId("m_long".into()))
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(2), turn_exec.started.notified())
        .await
        .expect("long bus turn should start");

    turn_exec.gate.notify_one();
    let input = wait_for_user_input(&events, &session).await;

    let injected = turn_exec.injected.lock().unwrap();
    assert_eq!(injected.len(), 1);
    assert_eq!(injected[0].dms.len(), 1);
    assert_eq!(
        injected[0].dms[0].body, long_body,
        "daemon-to-agent delivery must not inject preview-truncated context"
    );
    assert!(!injected[0].dms[0].truncated);
    assert!(
        input["text"]
            .as_str()
            .expect("session input text")
            .contains("END_OF_FULL_BODY"),
        "session-visible user_input should show the same full input sent to the harness: {input:?}"
    );
}

#[tokio::test]
async fn single_plain_human_dm_projection_matches_injected_text() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let cfg = Config::default();

    let bell = Bell::new();
    let registry = AgentRegistry::new();
    let turn_exec = Arc::new(MockTurnExec::new());
    let events = RecordingSink::default();
    let session = SessionId("s_ana".into());

    let service = DispatchService::new(ServiceDeps {
        store: store.clone(),
        bell: bell.clone(),
        registry: registry.clone(),
        project: PROJECT.into(),
        drain_limit: cfg.drain_limit,
        preview_chars: cfg.msg_preview_chars,
    });

    let _loop = EventLoop::spawn(
        session.clone(),
        LoopDeps {
            store: store.clone(),
            bell,
            registry,
            turn_exec: turn_exec.clone(),
            events: Arc::new(events.clone()),
            project: PROJECT.into(),
            drain_limit: cfg.drain_limit,
            preview_chars: cfg.msg_preview_chars,
            completion_timeout: std::time::Duration::from_secs(45),
            provider_limit_default_cooldown: std::time::Duration::from_millis(50),
        },
    );

    insert(&store, &human_dm("m_plain", "Alex", "plain human text")).await;
    service
        .enqueue(&session, &MessageId("m_plain".into()))
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(2), turn_exec.started.notified())
        .await
        .expect("human DM turn should start");

    turn_exec.gate.notify_one();
    let input = wait_for_user_input(&events, &session).await;

    assert_eq!(input["text"], "plain human text");
    assert!(
        !input["text"]
            .as_str()
            .unwrap_or_default()
            .contains("<nexus"),
        "plain human DM projection must match the plain harness injection text: {input:?}"
    );
}

#[tokio::test]
async fn ack_moves_row_to_acked_and_clears_pending() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let cfg = Config::default();
    let session = SessionId("s_ben".into());

    let service = DispatchService::new(ServiceDeps {
        store: store.clone(),
        bell: Bell::new(),
        registry: AgentRegistry::new(),
        project: PROJECT.into(),
        drain_limit: cfg.drain_limit,
        preview_chars: cfg.msg_preview_chars,
    });

    insert(&store, &dm("m_10", "ana", "hi")).await;
    service
        .enqueue(&session, &MessageId("m_10".into()))
        .await
        .unwrap();

    // Ack is a consumer settlement after successful harness completion, never a shortcut from
    // queued/notified. Model the observed delivery boundary explicitly.
    let inbox = Inbox::new(&store);
    inbox.mark_notified(&session).await.unwrap();
    assert_eq!(
        inbox
            .mark_injecting(&MessageId("m_10".into()), &session)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        inbox
            .mark_delivered(&MessageId("m_10".into()), &session)
            .await
            .unwrap(),
        1
    );

    let resp = service
        .ack(
            &caller(&session),
            AckRequest {
                message_id: MessageId("m_10".into()),
            },
        )
        .await
        .unwrap();
    assert_eq!(resp.acked, 1);

    // Acked → no longer pending.
    assert!(inbox
        .pending_for(&session, PROJECT, 50)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn failed_injection_is_terminal_and_is_not_retried() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let cfg = Config::default();

    let bell = Bell::new();
    let registry = AgentRegistry::new();
    let turn_exec = Arc::new(FailingTurnExec {
        started: Notify::new(),
    });
    let events = RecordingSink::default();
    let session = SessionId("s_codex_dead".into());

    let service = DispatchService::new(ServiceDeps {
        store: store.clone(),
        bell: bell.clone(),
        registry: registry.clone(),
        project: PROJECT.into(),
        drain_limit: cfg.drain_limit,
        preview_chars: cfg.msg_preview_chars,
    });

    let _loop = EventLoop::spawn(
        session.clone(),
        LoopDeps {
            store: store.clone(),
            bell,
            registry,
            turn_exec: turn_exec.clone(),
            events: Arc::new(events.clone()),
            project: PROJECT.into(),
            drain_limit: cfg.drain_limit,
            preview_chars: cfg.msg_preview_chars,
            completion_timeout: std::time::Duration::from_secs(45),
            provider_limit_default_cooldown: std::time::Duration::from_millis(50),
        },
    );

    insert(
        &store,
        &dm("m_dead_codex", "ada", "settle the failed attempt"),
    )
    .await;
    service
        .enqueue(&session, &MessageId("m_dead_codex".into()))
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(2), turn_exec.started.notified())
        .await
        .expect("failing injection should be attempted");

    wait_for_delivery_state(&store, "m_dead_codex", "error").await;

    let mut rows = store
        .conn
        .query(
            "SELECT state, delivered_at, acked_at, attempt_count, error_code FROM in_flight \
             WHERE message_id = 'm_dead_codex' AND recipient_session = 's_codex_dead'",
            (),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().expect("in_flight row");
    assert_eq!(row.get::<String>(0).unwrap(), "error");
    assert_eq!(row.get::<Option<i64>>(1).unwrap(), None);
    assert_eq!(row.get::<Option<i64>>(2).unwrap(), None);
    assert_eq!(row.get::<i64>(3).unwrap(), 1);
    assert_eq!(row.get::<String>(4).unwrap(), "target_unreachable");

    // Even another bell must not make the terminal row eligible again.
    service.deps().bell.ring(&session);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut attempts = store
        .conn
        .query(
            "SELECT attempt_count FROM in_flight WHERE message_id = 'm_dead_codex'",
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        attempts
            .next()
            .await
            .unwrap()
            .unwrap()
            .get::<i64>(0)
            .unwrap(),
        1
    );

    assert!(
        events.events().iter().all(|event| {
            !matches!(
                event,
                WsEvent::AgentUpdate {
                    kind: AgentUpdateKind::UserInput,
                    ..
                }
            )
        }),
        "failed injection must not create a visible session input row"
    );
}

#[tokio::test]
async fn provider_limit_is_terminal_and_never_schedules_a_retry() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let cfg = Config::default();

    let bell = Bell::new();
    let registry = AgentRegistry::new();
    let turn_exec = Arc::new(ProviderLimitOnceTurnExec {
        attempts: AtomicUsize::new(0),
        started: Notify::new(),
        reset_delay_ms: 250,
    });
    let events = RecordingSink::default();
    let session = SessionId("s_codex_limited".into());

    let service = DispatchService::new(ServiceDeps {
        store: store.clone(),
        bell: bell.clone(),
        registry: registry.clone(),
        project: PROJECT.into(),
        drain_limit: cfg.drain_limit,
        preview_chars: cfg.msg_preview_chars,
    });

    let _loop = EventLoop::spawn(
        session.clone(),
        LoopDeps {
            store: store.clone(),
            bell: bell.clone(),
            registry: registry.clone(),
            turn_exec: turn_exec.clone(),
            events: Arc::new(events.clone()),
            project: PROJECT.into(),
            drain_limit: cfg.drain_limit,
            preview_chars: cfg.msg_preview_chars,
            completion_timeout: Duration::from_secs(45),
            provider_limit_default_cooldown: Duration::from_millis(50),
        },
    );

    insert(&store, &dm("m_limited_1", "ada", "will hit limit")).await;
    service
        .enqueue(&session, &MessageId("m_limited_1".into()))
        .await
        .unwrap();

    wait_for_attempts(&turn_exec, 1).await;
    assert_eq!(registry.get(&session), AgentState::ProviderLimited);

    let inbox = Inbox::new(&store);
    wait_for_delivery_state(&store, "m_limited_1", "error").await;
    let mut failed = store
        .conn
        .query(
            "SELECT attempt_count, error_code, delivered_at FROM in_flight \
             WHERE message_id = 'm_limited_1'",
            (),
        )
        .await
        .unwrap();
    let failed = failed.next().await.unwrap().unwrap();
    assert_eq!(failed.get::<i64>(0).unwrap(), 1);
    assert_eq!(failed.get::<String>(1).unwrap(), "provider_limit");
    assert_eq!(failed.get::<Option<i64>>(2).unwrap(), None);

    insert(&store, &dm("m_limited_2", "ada", "queue while held")).await;
    service
        .enqueue(&session, &MessageId("m_limited_2".into()))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(
        turn_exec.attempts(),
        1,
        "enqueue/ring while held must not re-inject"
    );

    // The reset hint is diagnostic only. Waiting beyond it must not clear the hold or reattempt.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        registry.get(&session),
        AgentState::ProviderLimited,
        "provider limit remains explicit operator state"
    );
    assert_eq!(
        turn_exec.attempts(),
        1,
        "reset hints must not schedule retries"
    );
    assert_eq!(
        inbox
            .pending_for(&session, PROJECT, 50)
            .await
            .unwrap()
            .len(),
        1,
        "a new message may remain queued while the session is provider-limited"
    );

    let delivered = events
        .events()
        .into_iter()
        .filter(|event| {
            matches!(
                event,
                WsEvent::MessageDelivered { recipient, .. } if recipient == &session
            )
        })
        .count();
    assert_eq!(
        delivered, 0,
        "a provider-limited attempt must never emit delivered"
    );
}

#[tokio::test]
async fn provider_error_is_terminal_with_structured_explicit_retry_evidence() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let cfg = Config::default();
    let bell = Bell::new();
    let registry = AgentRegistry::new();
    let turn_exec = Arc::new(ProviderErrorTurnExec {
        attempts: AtomicUsize::new(0),
    });
    let session = SessionId("s_claude_server_error".into());
    let service = DispatchService::new(ServiceDeps {
        store: store.clone(),
        bell: bell.clone(),
        registry: registry.clone(),
        project: PROJECT.into(),
        drain_limit: cfg.drain_limit,
        preview_chars: cfg.msg_preview_chars,
    });
    let _loop = EventLoop::spawn(
        session.clone(),
        LoopDeps {
            store: store.clone(),
            bell: bell.clone(),
            registry,
            turn_exec: turn_exec.clone(),
            events: Arc::new(RecordingSink::default()),
            project: PROJECT.into(),
            drain_limit: cfg.drain_limit,
            preview_chars: cfg.msg_preview_chars,
            completion_timeout: Duration::from_secs(45),
            provider_limit_default_cooldown: Duration::from_millis(10),
        },
    );

    insert(
        &store,
        &dm("m_server_error", "ada", "survive transient provider error"),
    )
    .await;
    service
        .enqueue(&session, &MessageId("m_server_error".into()))
        .await
        .unwrap();
    wait_for_delivery_state(&store, "m_server_error", "error").await;

    let mut rows = store
        .conn
        .query(
            "SELECT attempt_count, error_code, error_reason, error_details_json \
             FROM in_flight WHERE message_id = 'm_server_error'",
            (),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 1);
    assert_eq!(row.get::<String>(1).unwrap(), "provider_error");
    assert_eq!(row.get::<String>(2).unwrap(), "server_error");
    let details: serde_json::Value = serde_json::from_str(&row.get::<String>(3).unwrap()).unwrap();
    assert_eq!(details["retryable"], true);
    assert_eq!(details["source"], "claude.acp.prompt_error");

    bell.ring(&session);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        turn_exec.attempts.load(Ordering::SeqCst),
        1,
        "retryable evidence is advisory; only an explicit requeue may retry"
    );
}

#[tokio::test]
async fn operator_action_is_terminal_and_never_retries() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let cfg = Config::default();
    let bell = Bell::new();
    let registry = AgentRegistry::new();
    let turn_exec = Arc::new(OperatorActionTurnExec {
        attempts: AtomicUsize::new(0),
    });
    let session = SessionId("s_codex_auth_required".into());
    let service = DispatchService::new(ServiceDeps {
        store: store.clone(),
        bell: bell.clone(),
        registry: registry.clone(),
        project: PROJECT.into(),
        drain_limit: cfg.drain_limit,
        preview_chars: cfg.msg_preview_chars,
    });
    let _loop = EventLoop::spawn(
        session.clone(),
        LoopDeps {
            store: store.clone(),
            bell: bell.clone(),
            registry,
            turn_exec: turn_exec.clone(),
            events: Arc::new(RecordingSink::default()),
            project: PROJECT.into(),
            drain_limit: cfg.drain_limit,
            preview_chars: cfg.msg_preview_chars,
            completion_timeout: Duration::from_secs(45),
            provider_limit_default_cooldown: Duration::from_millis(10),
        },
    );

    insert(&store, &dm("m_auth", "ada", "requires provider auth")).await;
    service
        .enqueue(&session, &MessageId("m_auth".into()))
        .await
        .unwrap();
    wait_for_delivery_state(&store, "m_auth", "error").await;

    let mut rows = store
        .conn
        .query(
            "SELECT attempt_count, error_code FROM in_flight WHERE message_id = 'm_auth'",
            (),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 1);
    assert_eq!(row.get::<String>(1).unwrap(), "operator_action");

    bell.ring(&session);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(turn_exec.attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn completion_timeout_dead_letters_not_delivered() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let cfg = Config::default();

    let bell = Bell::new();
    let registry = AgentRegistry::new();
    let turn_exec = Arc::new(HangingTurnExec {
        started: Notify::new(),
    });
    let events = RecordingSink::default();
    let session = SessionId("s_codex_hung".into());
    let later_bell = bell.clone();

    let service = DispatchService::new(ServiceDeps {
        store: store.clone(),
        bell: bell.clone(),
        registry: registry.clone(),
        project: PROJECT.into(),
        drain_limit: cfg.drain_limit,
        preview_chars: cfg.msg_preview_chars,
    });

    let _loop = EventLoop::spawn(
        session.clone(),
        LoopDeps {
            store: store.clone(),
            bell,
            registry,
            turn_exec: turn_exec.clone(),
            events: Arc::new(events.clone()),
            project: PROJECT.into(),
            drain_limit: cfg.drain_limit,
            preview_chars: cfg.msg_preview_chars,
            // Tiny completion budget so the lost-completion timeout fires promptly in-test.
            completion_timeout: Duration::from_millis(50),
            provider_limit_default_cooldown: Duration::from_millis(50),
        },
    );

    insert(&store, &dm("m_hung_codex", "ada", "never completes")).await;
    service
        .enqueue(&session, &MessageId("m_hung_codex".into()))
        .await
        .unwrap();

    // The injection is attempted (and then hangs forever inside the harness).
    tokio::time::timeout(Duration::from_secs(2), turn_exec.started.notified())
        .await
        .expect("hanging injection should be attempted");

    // Give the 50ms completion timeout ample room to fire and dead-letter the batch.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // The row must be dead-lettered: `error` state, never delivered, never acked.
    let mut rows = store
        .conn
        .query(
            "SELECT state, delivered_at, acked_at FROM in_flight \
             WHERE message_id = 'm_hung_codex' AND recipient_session = 's_codex_hung'",
            (),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().expect("in_flight row");
    assert_eq!(
        row.get::<String>(0).unwrap(),
        "error",
        "completion timeout must dead-letter the row, not mark it delivered"
    );
    assert_eq!(
        row.get::<Option<i64>>(1).unwrap(),
        None,
        "dead-lettered row must have no delivered_at"
    );
    assert_eq!(row.get::<Option<i64>>(2).unwrap(), None);

    // And it must NOT have emitted a false MessageDelivered for this session.
    assert!(
        events.events().iter().all(|event| {
            !matches!(
                event,
                WsEvent::MessageDelivered { recipient, .. } if recipient == &session
            )
        }),
        "completion timeout must not emit a false MessageDelivered"
    );

    // A failed pre-submission wait may be rechecked, but a claimed submission
    // with unknown completion must remain terminal even when more bells arrive.
    drop(rows);
    for _ in 0..3 {
        later_bell.ring(&session);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let mut rows = store.conn.query(
        "SELECT state, attempt_count, error_code FROM in_flight WHERE message_id = 'm_hung_codex'", (),
    ).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<String>(0).unwrap(), "error");
    assert_eq!(
        row.get::<i64>(1).unwrap(),
        1,
        "an uncertain submission must never be replayed"
    );
    assert_eq!(row.get::<String>(2).unwrap(), "completion_timeout");
}
