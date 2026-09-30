use super::*;
use nexus_contracts::{
    DispatchPort, Kind, Message, MessageId, ProjectId, Provenance, Scope, WsEvent,
};
use nexus_dispatch::{AgentRegistry, Bell, DispatchService, EventLoop, LoopDeps, ServiceDeps};
use nexus_store::{repos::Messages, Store};
use std::sync::atomic::{AtomicBool, AtomicU64};
use tokio::sync::oneshot;

#[derive(Default)]
struct QueueEvents(Mutex<Vec<WsEvent>>);

#[async_trait]
impl EventSink for QueueEvents {
    async fn emit(&self, event: WsEvent) {
        self.0.lock().unwrap().push(event);
    }
}

struct WriterGate {
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
    returned: oneshot::Sender<()>,
}

struct HeldWriter {
    entered: oneshot::Receiver<()>,
    release: oneshot::Sender<()>,
    returned: oneshot::Receiver<()>,
}

struct NativeQueueInput {
    completion: Arc<ClaudeTurnCompletion>,
    offset: AtomicU64,
    submitted: Mutex<Vec<ClaudeHookRecord>>,
    interrupts: AtomicUsize,
    hold_receipt: AtomicBool,
    reject: AtomicBool,
    writer_gate: Mutex<Option<WriterGate>>,
}

#[async_trait]
impl HarnessInput for NativeQueueInput {
    async fn send_turn(&self, text: &str) -> Result<(), String> {
        if self.reject.load(Ordering::SeqCst) {
            return Err("fixture rejected input before acceptance".into());
        }
        let offset = self.offset.fetch_add(1, Ordering::SeqCst) + 1;
        let submit = parse_hook_record(
            &serde_json::json!({
                "event":"UserPromptSubmit", "session_id":"native", "prompt_id":format!("p{offset}"),
                "prompt":format!("{text}\n")
            }),
            offset,
        );
        self.submitted.lock().unwrap().push(submit.clone());
        if self.hold_receipt.load(Ordering::SeqCst) {
            return Ok(());
        }
        self.completion
            .observe_hooks(&[submit.clone()], Some(offset), true);
        assert!(self.completion.accept_native_user_input(&submit).await);
        assert!(!self.completion.accept_native_user_input(&submit).await);
        let gate = self.writer_gate.lock().unwrap().take();
        if let Some(gate) = gate {
            // The real ClaudeNativeHarness still owns its input mutex until this
            // inner write returns, even though the native receipt already arrived.
            gate.entered.send(()).unwrap();
            gate.release.await.unwrap();
            gate.returned.send(()).unwrap();
        }
        Ok(())
    }

    async fn interrupt_active_turn(&self) -> Result<(), String> {
        self.interrupts.fetch_add(1, Ordering::SeqCst);
        // Native Ctrl-C does not manufacture the missing normal Stop hook.
        Ok(())
    }
}

struct Fixture {
    store: Arc<Store>,
    completion: Arc<ClaudeTurnCompletion>,
    input: Arc<NativeQueueInput>,
    events: Arc<QueueEvents>,
    service: DispatchService,
    session: SessionId,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Fixture {
    async fn new(already_active: bool) -> Self {
        Self::with_timeout(already_active, Duration::from_secs(60)).await
    }

    async fn with_timeout(already_active: bool, completion_timeout: Duration) -> Self {
        let store = Arc::new(Store::open(":memory:").await.unwrap());
        store.migrate().await.unwrap();
        let completion = Arc::new(ClaudeTurnCompletion::new(Some("native".into()), None));
        if already_active {
            completion.observe_hooks(
                &[native_hook("UserPromptSubmit", "existing work", 1)],
                Some(1),
                true,
            );
        }
        let input = Arc::new(NativeQueueInput {
            completion: completion.clone(),
            offset: AtomicU64::new(1),
            submitted: Mutex::new(Vec::new()),
            interrupts: AtomicUsize::new(0),
            hold_receipt: AtomicBool::new(false),
            reject: AtomicBool::new(false),
            writer_gate: Mutex::new(None),
        });
        let transport = Arc::new(PtyTransport::default());
        let session = SessionId("s_native_queue".into());
        transport.bind(
            session.clone(),
            Arc::new(ClaudeNativeHarness {
                input: input.clone(),
                completion: completion.clone(),
            }),
        );
        let events = Arc::new(QueueEvents::default());
        let bell = Bell::new();
        let registry = AgentRegistry::new();
        let service = DispatchService::new(ServiceDeps {
            store: store.clone(),
            bell: bell.clone(),
            registry: registry.clone(),
            project: "queue-test".into(),
            drain_limit: 100,
            preview_chars: 100,
        });
        let task = EventLoop::spawn(
            session.clone(),
            LoopDeps {
                store: store.clone(),
                bell,
                registry,
                turn_exec: transport,
                events: events.clone(),
                project: "queue-test".into(),
                drain_limit: 100,
                preview_chars: 100,
                completion_timeout,
                provider_limit_default_cooldown: Duration::from_secs(60),
            },
        );
        Self {
            store,
            completion,
            input,
            events,
            service,
            session,
            task,
        }
    }

    async fn enqueue(&self, id: &str) {
        Messages::new(&self.store)
            .insert(&Message {
                id: MessageId(id.into()),
                project: ProjectId("queue-test".into()),
                from: "sender".into(),
                scope: Scope::Dm,
                thread: None,
                topic: None,
                body: "same intentional message".into(),
                summary: None,
                provenance: Provenance {
                    from: "sender".into(),
                    kind: Kind::Agent,
                    locality: Default::default(),
                    access: None,
                    thread: None,
                    topic: None,
                    stamp: None,
                },
                created_at: 0,
            })
            .await
            .unwrap();
        self.service
            .enqueue(&self.session, &MessageId(id.into()))
            .await
            .unwrap();
    }

    async fn state(&self, id: &str) -> String {
        let mut rows = self
            .store
            .conn
            .query("SELECT state FROM in_flight WHERE message_id=?1", [id])
            .await
            .unwrap();
        rows.next().await.unwrap().unwrap().get(0).unwrap()
    }

    async fn attempts(&self, id: &str) -> i64 {
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT attempt_count FROM in_flight WHERE message_id=?1",
                [id],
            )
            .await
            .unwrap();
        rows.next().await.unwrap().unwrap().get(0).unwrap()
    }

    fn hold_next_writer(&self) -> HeldWriter {
        let (entered_tx, entered) = oneshot::channel();
        let (release, release_rx) = oneshot::channel();
        let (returned_tx, returned) = oneshot::channel();
        assert!(self
            .input
            .writer_gate
            .lock()
            .unwrap()
            .replace(WriterGate {
                entered: entered_tx,
                release: release_rx,
                returned: returned_tx,
            })
            .is_none());
        HeldWriter {
            entered,
            release,
            returned,
        }
    }

    async fn wait_claimed_once(&self, id: &str) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                // Query only this fixture's in-memory Store, independently of dispatch.
                let mut rows = self
                    .store
                    .conn
                    .query(
                        "SELECT state, attempt_count FROM in_flight WHERE message_id=?1",
                        [id],
                    )
                    .await
                    .unwrap();
                let row = rows.next().await.unwrap().unwrap();
                let state: String = row.get(0).unwrap();
                let attempts: i64 = row.get(1).unwrap();
                assert!(attempts <= 1, "a queued message must not be replayed");
                if (state.as_str(), attempts) == ("injecting", 1) {
                    return;
                }
                drop(rows);
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("mid-turn queue handler must claim followup as injecting/attempt1 before release");
    }

    async fn abort_loop(&mut self) {
        self.task.abort();
        let error = tokio::time::timeout(Duration::from_secs(2), &mut self.task)
            .await
            .expect("test teardown must not hang")
            .expect_err("event loop must stop through explicit test cancellation");
        assert!(error.is_cancelled());
    }

    async fn wait_state(&self, id: &str, state: &str) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while self.state(id).await != state {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("native queue delivery must settle without waiting for Stop");
    }

    async fn wait_submissions(&self, count: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while self.input.submitted.lock().unwrap().len() < count {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("pending mail must reach native input during the original turn");
    }

    fn echoes(&self) -> usize {
        self.events
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|e| {
                matches!(
                    e,
                    WsEvent::AgentUpdate {
                        kind: nexus_contracts::AgentUpdateKind::UserInput,
                        ..
                    }
                )
            })
            .count()
    }

    fn stop(&self, prompt_id: String) {
        let offset = self.input.offset.fetch_add(1, Ordering::SeqCst) + 1;
        let mut stop = native_hook("Stop", "", offset);
        stop.prompt_id = Some(prompt_id);
        self.completion.observe_hooks(&[stop], Some(offset), true);
    }
}

#[tokio::test]
async fn already_active_native_turn_queues_bus_input_without_ctrl_c() {
    let f = Fixture::new(true).await;
    for (index, id) in ["m_one", "m_two"].iter().enumerate() {
        f.enqueue(id).await;
        f.wait_state(id, "delivered").await;
        assert_eq!(
            f.input.interrupts.load(Ordering::SeqCst),
            0,
            "bus delivery must not send Ctrl-C"
        );
        assert_eq!(f.echoes(), index + 1);
        assert!(
            f.completion.has_open_turn(),
            "receipt is not a terminal fact"
        );
    }
    assert_eq!(f.input.submitted.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn bus_owned_turn_accepts_more_mail_without_interrupt_or_completing_original() {
    let f = Fixture::new(false).await;
    f.enqueue("m_original").await;
    f.wait_submissions(1).await;
    f.enqueue("m_followup").await;
    f.wait_state("m_followup", "delivered").await;
    assert_eq!(f.input.interrupts.load(Ordering::SeqCst), 0);
    assert_ne!(f.state("m_original").await, "delivered");
    assert_eq!(f.echoes(), 2);
    assert!(f.completion.has_open_turn());
    let first = f.input.submitted.lock().unwrap()[0]
        .prompt_id
        .clone()
        .unwrap();
    f.stop(first);
    f.wait_state("m_original", "delivered").await;
    assert!(
        f.completion.has_open_turn(),
        "original Stop must not finish the queued turn"
    );
    assert_eq!(f.input.submitted.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn held_bus_owned_writer_resumes_without_followup_and_settles_on_own_stop() {
    let mut f = Fixture::new(false).await;
    let held = f.hold_next_writer();
    f.enqueue("m_original").await;
    tokio::time::timeout(Duration::from_secs(2), held.entered)
        .await
        .expect("original inner writer must enter while the real input mutex is held")
        .unwrap();
    assert_eq!(
        f.echoes(),
        1,
        "native receipt must emit exactly one accepted echo"
    );
    assert_eq!(f.state("m_original").await, "injecting");
    assert!(f.completion.has_open_turn());
    held.release.send(()).unwrap();
    let returned = tokio::time::timeout(Duration::from_secs(2), held.returned).await;
    if returned.is_err() {
        f.abort_loop().await;
    }
    returned
        .expect("released original writer must return when no second mail is queued")
        .unwrap();
    assert_eq!(f.input.interrupts.load(Ordering::SeqCst), 0);
    assert_eq!(f.state("m_original").await, "injecting");
    let original = f.input.submitted.lock().unwrap()[0]
        .prompt_id
        .clone()
        .unwrap();
    f.stop(original);
    f.wait_state("m_original", "delivered").await;
    assert!(!f.completion.has_open_turn());
    assert_eq!(f.echoes(), 1);
    assert_eq!(f.input.submitted.lock().unwrap().len(), 1);
    assert_eq!(f.input.interrupts.load(Ordering::SeqCst), 0);
    f.abort_loop().await;
}

#[tokio::test]
async fn mid_turn_native_queue_keeps_polling_original_writer_holding_input_lock() {
    let mut f = Fixture::new(false).await;
    let held = f.hold_next_writer();
    f.enqueue("m_original").await;
    tokio::time::timeout(Duration::from_secs(2), held.entered)
        .await
        .expect("original inner writer must enter while the real input mutex is held")
        .unwrap();
    f.enqueue("m_followup").await;
    f.wait_claimed_once("m_followup").await;
    assert_eq!(f.state("m_original").await, "injecting");
    assert_eq!(f.input.submitted.lock().unwrap().len(), 1);
    assert_eq!(f.echoes(), 1);
    assert_eq!(f.input.interrupts.load(Ordering::SeqCst), 0);

    held.release.send(()).unwrap();
    let returned = tokio::time::timeout(Duration::from_secs(2), held.returned).await;
    if returned.is_err() {
        f.abort_loop().await;
    }
    returned
        .expect("released original writer must return while the claimed followup waits for its input lock")
        .unwrap();
    f.wait_state("m_followup", "delivered").await;
    assert_eq!(f.input.submitted.lock().unwrap().len(), 2);
    assert_eq!(f.echoes(), 2);
    assert_eq!(f.input.interrupts.load(Ordering::SeqCst), 0);
    assert_eq!(f.state("m_original").await, "injecting");
    let original = f.input.submitted.lock().unwrap()[0]
        .prompt_id
        .clone()
        .unwrap();
    f.stop(original);
    f.wait_state("m_original", "delivered").await;
    assert!(
        f.completion.has_open_turn(),
        "original Stop must not finish followup"
    );
    f.abort_loop().await;
}

#[tokio::test]
async fn original_stop_settles_while_queued_write_remains_owned() {
    let mut f = Fixture::new(false).await;
    let original_write = f.hold_next_writer();
    f.enqueue("m_original").await;
    tokio::time::timeout(Duration::from_secs(2), original_write.entered)
        .await
        .unwrap()
        .unwrap();
    original_write.release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), original_write.returned)
        .await
        .unwrap()
        .unwrap();
    let original = f.input.submitted.lock().unwrap()[0]
        .prompt_id
        .clone()
        .unwrap();

    let followup_write = f.hold_next_writer();
    f.enqueue("m_followup").await;
    tokio::time::timeout(Duration::from_secs(2), followup_write.entered)
        .await
        .unwrap()
        .unwrap();
    f.stop(original);
    f.wait_state("m_original", "delivered").await;
    assert_eq!(f.state("m_followup").await, "injecting");
    followup_write.release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), followup_write.returned)
        .await
        .unwrap()
        .unwrap();
    f.wait_state("m_followup", "delivered").await;
    assert_eq!(f.echoes(), 2);
    assert_eq!(f.input.submitted.lock().unwrap().len(), 2);
    assert_eq!(f.input.interrupts.load(Ordering::SeqCst), 0);
    assert!(f.completion.has_open_turn());
    f.abort_loop().await;
}

#[tokio::test]
async fn queue_wait_does_not_disable_original_timeout_or_fabricate_acceptance() {
    let mut f = Fixture::with_timeout(false, Duration::from_millis(250)).await;
    let held = f.hold_next_writer();
    f.enqueue("m_original").await;
    tokio::time::timeout(Duration::from_secs(2), held.entered)
        .await
        .unwrap()
        .unwrap();
    // The followup may write after the original times out, but has no native receipt.
    f.input.hold_receipt.store(true, Ordering::SeqCst);
    f.enqueue("m_followup").await;
    f.wait_claimed_once("m_followup").await;
    f.wait_state("m_original", "error").await;
    f.wait_submissions(2).await;
    f.wait_state("m_followup", "error").await;
    let mut rows = f
        .store
        .conn
        .query(
            "SELECT error_code FROM in_flight WHERE message_id='m_followup'",
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        rows.next()
            .await
            .unwrap()
            .unwrap()
            .get::<String>(0)
            .unwrap(),
        nexus_store::repos::inbox::COMPLETION_TIMEOUT_ERROR_CODE
    );
    drop(rows);
    assert!(
        held.release.send(()).is_err(),
        "timed-out original writer must be dropped"
    );
    assert!(tokio::time::timeout(Duration::from_secs(2), held.returned)
        .await
        .unwrap()
        .is_err());
    assert_eq!(
        f.echoes(),
        1,
        "followup cannot get an accepted echo without a receipt"
    );
    assert_eq!(f.attempts("m_original").await, 1);
    assert_eq!(f.attempts("m_followup").await, 1);
    f.service.deps().bell.ring(&f.session);
    assert!(
        tokio::time::timeout(Duration::from_millis(30), f.wait_submissions(3))
            .await
            .is_err()
    );
    assert_eq!(f.attempts("m_followup").await, 1);
    assert_eq!(f.input.interrupts.load(Ordering::SeqCst), 0);
    f.abort_loop().await;
}

#[tokio::test]
async fn aborting_loop_drops_held_writer_and_waiting_handoff_without_orphan_submission() {
    let mut f = Fixture::new(false).await;
    let held = f.hold_next_writer();
    f.enqueue("m_original").await;
    tokio::time::timeout(Duration::from_secs(2), held.entered)
        .await
        .unwrap()
        .unwrap();
    f.enqueue("m_followup").await;
    f.wait_claimed_once("m_followup").await;
    f.abort_loop().await;
    assert!(
        held.release.send(()).is_err(),
        "no writer task may survive loop teardown"
    );
    assert!(tokio::time::timeout(Duration::from_secs(2), held.returned)
        .await
        .unwrap()
        .is_err());
    assert_eq!(f.input.submitted.lock().unwrap().len(), 1);
    assert_eq!(f.echoes(), 1);
    assert_eq!(f.state("m_original").await, "injecting");
    assert_eq!(f.state("m_followup").await, "injecting");
    assert_eq!(f.input.interrupts.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn aborting_loop_drops_an_actively_writing_queued_handoff() {
    let mut f = Fixture::new(false).await;
    let original_write = f.hold_next_writer();
    f.enqueue("m_original").await;
    tokio::time::timeout(Duration::from_secs(2), original_write.entered)
        .await
        .unwrap()
        .unwrap();
    original_write.release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), original_write.returned)
        .await
        .unwrap()
        .unwrap();
    let followup_write = f.hold_next_writer();
    f.enqueue("m_followup").await;
    tokio::time::timeout(Duration::from_secs(2), followup_write.entered)
        .await
        .unwrap()
        .unwrap();
    f.abort_loop().await;
    assert!(followup_write.release.send(()).is_err());
    assert!(
        tokio::time::timeout(Duration::from_secs(2), followup_write.returned)
            .await
            .unwrap()
            .is_err()
    );
    assert_eq!(f.input.submitted.lock().unwrap().len(), 2);
    assert_eq!(f.echoes(), 2);
    assert_eq!(f.state("m_original").await, "injecting");
    assert_eq!(f.state("m_followup").await, "injecting");
    assert_eq!(f.input.interrupts.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn native_queue_settlement_requires_receipt_and_failure_never_replays() {
    let f = Fixture::new(true).await;
    f.input.hold_receipt.store(true, Ordering::SeqCst);
    f.enqueue("m_waiting_receipt").await;
    f.wait_submissions(1).await;
    // The fixture write has returned. Only a later native receipt may settle delivery.
    assert!(tokio::time::timeout(
        Duration::from_millis(30),
        f.wait_state("m_waiting_receipt", "delivered")
    )
    .await
    .is_err());
    assert_ne!(f.state("m_waiting_receipt").await, "delivered");
    assert_eq!(f.echoes(), 0);
    assert_eq!(f.input.interrupts.load(Ordering::SeqCst), 0);
    let receipt = f.input.submitted.lock().unwrap()[0].clone();
    f.completion
        .observe_hooks(&[receipt.clone()], Some(receipt.end_offset), true);
    assert!(f.completion.accept_native_user_input(&receipt).await);
    f.wait_state("m_waiting_receipt", "delivered").await;
    assert_eq!(f.echoes(), 1);
    f.input.hold_receipt.store(false, Ordering::SeqCst);
    f.input.reject.store(true, Ordering::SeqCst);
    f.enqueue("m_rejected").await;
    f.wait_state("m_rejected", "error").await;
    assert_eq!(f.echoes(), 1);
    f.input.reject.store(false, Ordering::SeqCst);
    f.enqueue("m_next").await;
    f.wait_state("m_next", "delivered").await;
    assert_eq!(f.state("m_rejected").await, "error");
    assert_eq!(f.input.submitted.lock().unwrap().len(), 2);
    assert_eq!(f.echoes(), 2);
    assert_eq!(f.input.interrupts.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn native_queue_holds_leave_mail_unattempted_until_resume_despite_old_bells() {
    use nexus_dispatch::AgentState;
    for hold in [
        AgentState::Paused,
        AgentState::Offline,
        AgentState::ProviderLimited,
    ] {
        let f = Fixture::new(false).await;
        f.enqueue("m_original").await;
        f.wait_submissions(1).await;
        f.service.deps().registry.set(&f.session, hold);
        f.enqueue("m_held").await;
        f.service.deps().bell.ring(&f.session);
        assert!(
            tokio::time::timeout(Duration::from_millis(30), f.wait_submissions(2))
                .await
                .is_err(),
            "old bell must not bypass hold {hold:?}"
        );
        assert_eq!(f.state("m_held").await, "pending");
        let mut rows = f
            .store
            .conn
            .query(
                "SELECT attempt_count FROM in_flight WHERE message_id='m_held'",
                (),
            )
            .await
            .unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
            0
        );
        drop(rows);
        assert_ne!(f.state("m_original").await, "delivered");
        f.service.deps().registry.set(&f.session, AgentState::Busy);
        f.service.deps().bell.ring(&f.session);
        f.wait_state("m_held", "delivered").await;
        assert_eq!(f.input.interrupts.load(Ordering::SeqCst), 0);
        let first = f.input.submitted.lock().unwrap()[0]
            .prompt_id
            .clone()
            .unwrap();
        f.stop(first);
        f.wait_state("m_original", "delivered").await;
        assert_eq!(f.input.submitted.lock().unwrap().len(), 2);
    }
}
