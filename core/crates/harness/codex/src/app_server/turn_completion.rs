//! Turn-completion rendezvous for headed Codex app-server sessions.
//!
//! `turn/start` returns a `TurnStartResponse`, but headed Codex inbox delivery is not complete
//! until the harness turn reaches `turn/completed` or `error`. Bus batches wait on this rendezvous
//! before the realtime loop marks stored rows delivered. Direct operator prompts return after
//! `turn/start` acceptance; the notification forwarder still owns progress/completion for observe
//! consumers. Direct operator prompts return after `turn/start` acceptance, while native active
//! turn ids keep the daemon's durable prompt boundary queue held until completion. Explicit steer
//! requests use the same runtime ids through a separate control path.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use nexus_contracts::events::WsEvent;
use nexus_contracts::ports::EventSink;
use nexus_contracts::AgentUpdateKind;
use serde_json::Value;
use tokio::sync::oneshot;

const RECENT_COMPLETIONS_LIMIT: usize = 128;
const RECENT_RECEIPTS_LIMIT: usize = 128;
const ACCEPTED_ECHO_LIMIT: usize = 128;
const ACCEPTED_INPUT_RECEIPT_LIMIT: usize = 128;

type TurnKey = (String, String);
type AcceptedInputKey = (String, String, String);

#[derive(Default)]
struct Inner {
    /// Latest native active turn observed for each Codex thread. This is routing authority for
    /// explicit `turn/steer`; UI run ids and daemon prompt rows never populate it.
    active_turn_ids: HashMap<String, String>,
    /// Latest terminal native turn for each thread. Responses can arrive after terminal
    /// notifications, so this tombstone prevents late `turn/start` acceptance from resurrecting
    /// a completed turn as active.
    last_finished_turn_ids: HashMap<String, String>,
    waiters: HashMap<TurnKey, Vec<oneshot::Sender<CodexTurnCompletion>>>,
    completed: HashMap<TurnKey, CodexTurnCompletion>,
    completed_order: VecDeque<TurnKey>,
    receipt_waiters: HashMap<TurnKey, Vec<oneshot::Sender<CodexTurnCompletion>>>,
    receipts: HashMap<TurnKey, CodexTurnCompletion>,
    receipt_order: VecDeque<TurnKey>,
    next_accepted_id: u64,
    accepted_events: HashMap<String, VecDeque<AcceptedEvent>>,
    accepted_user_input_echo_queue: HashMap<String, VecDeque<PendingAcceptedUserInputEcho>>,
    accepted_user_input_echoes: HashMap<String, VecDeque<AcceptedUserInputEcho>>,
    accepted_input_receipt_waiters: HashMap<AcceptedInputKey, Vec<oneshot::Sender<()>>>,
    accepted_input_receipts: HashMap<AcceptedInputKey, ()>,
    accepted_input_receipt_order: VecDeque<AcceptedInputKey>,
}

struct AcceptedEvent {
    id: u64,
    events: Arc<dyn EventSink>,
    event: WsEvent,
}

struct AcceptedUserInputEcho {
    turn_id: String,
    text: String,
}

struct PendingAcceptedUserInputEcho {
    id: u64,
    text: String,
}

/// Shared tracker that lets the app-server transport wait for forwarder-observed turn completion.
#[derive(Clone)]
pub struct CodexTurnTracker {
    inner: Arc<Mutex<Inner>>,
}

impl Default for CodexTurnTracker {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner::default())),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CodexTurnFailure {
    pub thread_id: String,
    pub turn_id: String,
    pub params: Value,
}

#[derive(Debug, Clone, PartialEq)]
enum CodexTurnCompletion {
    Completed,
    Failed(CodexTurnFailure),
}

/// Error returned when a headed Codex turn does not report successful completion.
#[derive(Debug, Clone, PartialEq)]
pub enum CodexTurnWaitError {
    Timeout {
        thread_id: String,
        turn_id: String,
        timeout: Duration,
    },
    Failed(CodexTurnFailure),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueuedAcceptedEvent {
    thread_id: String,
    id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueuedAcceptedUserInputEcho {
    thread_id: String,
    id: u64,
}

impl std::fmt::Display for CodexTurnWaitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Timeout {
                    thread_id,
                    turn_id,
                    timeout,
                } => format!(
                    "timed out waiting for codex turn {thread_id}/{turn_id} to complete after {}s",
                    timeout.as_secs()
                ),
                Self::Failed(failure) => format!(
                    "codex turn {}/{} failed",
                    failure.thread_id, failure.turn_id
                ),
            }
        )
    }
}

impl std::error::Error for CodexTurnWaitError {}

impl CodexTurnTracker {
    /// Native active turn currently observed for `thread_id`.
    pub fn active_turn_id(&self, thread_id: &str) -> Option<String> {
        self.inner
            .lock()
            .unwrap()
            .active_turn_ids
            .get(thread_id)
            .cloned()
    }

    /// Record runtime evidence that `turn_id` is active for `thread_id`.
    ///
    /// `turn/started` is the primary source. Non-terminal item/delta notifications also call this
    /// to recover authority when Nexus attaches after the start notification or loses a race with
    /// a TUI-originated turn.
    pub fn observe_active_turn(&self, thread_id: &str, turn_id: &str) {
        let mut inner = self.inner.lock().unwrap();
        if inner
            .last_finished_turn_ids
            .get(thread_id)
            .map(String::as_str)
            == Some(turn_id)
        {
            return;
        }
        inner
            .active_turn_ids
            .insert(thread_id.to_string(), turn_id.to_string());
    }

    /// Seed an idle tracker from a successful `turn/start` response without overwriting newer
    /// notification evidence. `turn/start` can itself steer, so an existing native id always wins.
    pub fn record_turn_start_acceptance(&self, thread_id: &str, turn_id: &str) {
        let mut inner = self.inner.lock().unwrap();
        if inner
            .last_finished_turn_ids
            .get(thread_id)
            .map(String::as_str)
            == Some(turn_id)
        {
            return;
        }
        inner
            .active_turn_ids
            .entry(thread_id.to_string())
            .or_insert_with(|| turn_id.to_string());
    }

    /// Clear all active-turn authority for a thread after Codex reports there is no active turn.
    pub fn clear_active_turn(&self, thread_id: &str) {
        self.inner.lock().unwrap().active_turn_ids.remove(thread_id);
    }

    /// Queue a session-visible accepted event for the next Codex turn notification on `thread_id`.
    ///
    /// Codex app-server can stream notifications before the `turn/start` response. The transport
    /// queues the event before sending `turn/start`; the forwarder emits it before the first turn
    /// notification if the stream wins the race, otherwise the transport emits it after a
    /// successful response.
    pub fn queue_accepted_event(
        &self,
        thread_id: &str,
        events: Arc<dyn EventSink>,
        event: WsEvent,
    ) -> QueuedAcceptedEvent {
        let mut inner = self.inner.lock().unwrap();
        let id = inner.next_accepted_id;
        inner.next_accepted_id += 1;
        inner
            .accepted_events
            .entry(thread_id.to_string())
            .or_default()
            .push_back(AcceptedEvent { id, events, event });
        QueuedAcceptedEvent {
            thread_id: thread_id.to_string(),
            id,
        }
    }

    /// Queue a direct-prompt accepted input marker for the next Codex turn on `thread_id`.
    ///
    /// Dispatch already emitted the visible `user_input` row before calling transport prompt, so
    /// this queue only exists to suppress Codex's later `item/completed userMessage` echo.
    pub fn queue_accepted_user_input_echo(
        &self,
        thread_id: &str,
        text: String,
    ) -> QueuedAcceptedUserInputEcho {
        let mut inner = self.inner.lock().unwrap();
        let id = inner.next_accepted_id;
        inner.next_accepted_id += 1;
        inner
            .accepted_user_input_echo_queue
            .entry(thread_id.to_string())
            .or_default()
            .push_back(PendingAcceptedUserInputEcho { id, text });
        QueuedAcceptedUserInputEcho {
            thread_id: thread_id.to_string(),
            id,
        }
    }

    /// Emit the queued accepted event matching `queued`, if it has not already been emitted by the
    /// forwarder.
    pub async fn emit_accepted_event(
        &self,
        queued: &QueuedAcceptedEvent,
        turn_id: Option<&str>,
    ) -> bool {
        let Some(accepted) = self.take_accepted_event(&queued.thread_id, Some(queued.id)) else {
            return false;
        };
        let accepted_text = accepted_user_input_text(&accepted.event).map(str::to_string);
        accepted.events.emit(accepted.event).await;
        if let (Some(turn_id), Some(text)) = (turn_id, accepted_text) {
            self.record_accepted_user_input_echo(&queued.thread_id, turn_id, text);
        }
        true
    }

    /// Emit the next accepted event for `thread_id`, used by the notification forwarder before it
    /// forwards Codex assistant output for the same turn.
    pub async fn emit_next_accepted_event_for_thread(
        &self,
        thread_id: &str,
        turn_id: Option<&str>,
    ) -> bool {
        let Some(accepted) = self.take_accepted_event(thread_id, None) else {
            return false;
        };
        let accepted_text = accepted_user_input_text(&accepted.event).map(str::to_string);
        accepted.events.emit(accepted.event).await;
        if let (Some(turn_id), Some(text)) = (turn_id, accepted_text) {
            self.record_accepted_user_input_echo(thread_id, turn_id, text);
        }
        true
    }

    /// Cancel a queued accepted event after `turn/start` fails before any turn notification claimed
    /// the event.
    pub fn cancel_accepted_event(&self, queued: &QueuedAcceptedEvent) -> bool {
        self.take_accepted_event(&queued.thread_id, Some(queued.id))
            .is_some()
    }

    /// Cancel a queued direct-prompt echo marker after `turn/start` fails before notifications
    /// claimed it.
    pub fn cancel_accepted_user_input_echo(&self, queued: &QueuedAcceptedUserInputEcho) -> bool {
        self.take_queued_user_input_echo(&queued.thread_id, Some(queued.id))
            .is_some()
    }

    fn take_accepted_event(&self, thread_id: &str, id: Option<u64>) -> Option<AcceptedEvent> {
        let mut inner = self.inner.lock().unwrap();
        let queue = inner.accepted_events.get_mut(thread_id)?;
        let accepted = match id {
            Some(id) => {
                let pos = queue.iter().position(|event| event.id == id)?;
                queue.remove(pos)
            }
            None => queue.pop_front(),
        };
        if queue.is_empty() {
            inner.accepted_events.remove(thread_id);
        }
        accepted
    }

    fn take_queued_user_input_echo(
        &self,
        thread_id: &str,
        id: Option<u64>,
    ) -> Option<PendingAcceptedUserInputEcho> {
        let mut inner = self.inner.lock().unwrap();
        let queue = inner.accepted_user_input_echo_queue.get_mut(thread_id)?;
        let pending = match id {
            Some(id) => {
                let pos = queue.iter().position(|echo| echo.id == id)?;
                queue.remove(pos)
            }
            None => queue.pop_front(),
        };
        if queue.is_empty() {
            inner.accepted_user_input_echo_queue.remove(thread_id);
        }
        pending
    }

    /// Record the queued direct-prompt echo marker once a `turn/start` response returns a turn id.
    ///
    /// If the notification forwarder won the race and already claimed the marker, this is a no-op.
    pub fn record_accepted_user_input_echo_for_queued(
        &self,
        queued: &QueuedAcceptedUserInputEcho,
        turn_id: &str,
    ) -> bool {
        let Some(pending) = self.take_queued_user_input_echo(&queued.thread_id, Some(queued.id))
        else {
            return false;
        };
        self.record_accepted_user_input_echo(&queued.thread_id, turn_id, pending.text);
        true
    }

    /// Record the next queued direct-prompt echo marker for the first notification of this turn.
    ///
    /// This covers the app-server race where Codex emits turn notifications before the
    /// `turn/start` response reaches the transport.
    pub fn record_next_accepted_user_input_echo_for_thread(
        &self,
        thread_id: &str,
        turn_id: &str,
    ) -> bool {
        let Some(pending) = self.take_queued_user_input_echo(thread_id, None) else {
            return false;
        };
        self.record_accepted_user_input_echo(thread_id, turn_id, pending.text);
        true
    }

    fn record_accepted_user_input_echo(&self, thread_id: &str, turn_id: &str, text: String) {
        let mut inner = self.inner.lock().unwrap();
        let queue = inner
            .accepted_user_input_echoes
            .entry(thread_id.to_string())
            .or_default();
        queue.push_back(AcceptedUserInputEcho {
            turn_id: turn_id.to_string(),
            text,
        });
        while queue.len() > ACCEPTED_ECHO_LIMIT {
            queue.pop_front();
        }
    }

    /// Consume one previously emitted accepted user-input echo for this Codex turn.
    ///
    /// Codex app-server may later report the same input as `item/completed userMessage`. Direct
    /// native TUI input has no matching accepted event, so it still renders normally.
    pub fn take_accepted_user_input_echo(
        &self,
        thread_id: &str,
        turn_id: &str,
        text: &str,
    ) -> bool {
        let mut inner = self.inner.lock().unwrap();
        let Some(queue) = inner.accepted_user_input_echoes.get_mut(thread_id) else {
            return false;
        };
        let Some(pos) = queue
            .iter()
            .position(|echo| echo.turn_id == turn_id && echo.text == text)
        else {
            return false;
        };
        queue.remove(pos);
        if queue.is_empty() {
            inner.accepted_user_input_echoes.remove(thread_id);
        }
        drop(inner);
        self.observe_accepted_user_input_echo(thread_id, turn_id, text);
        true
    }

    /// Record the exact native `item/completed userMessage` echo for one Nexus-accepted input.
    ///
    /// `turn/steer` acceptance alone is not durable context admission: the daemon can restart
    /// before Codex records the steered user message. The forwarder calls this only after matching
    /// the native echo against a Nexus-owned accepted-input marker.
    pub fn observe_accepted_user_input_echo(&self, thread_id: &str, turn_id: &str, text: &str) {
        let key = (thread_id.to_string(), turn_id.to_string(), text.to_string());
        let waiters = {
            let mut inner = self.inner.lock().unwrap();
            if let Some(waiters) = inner.accepted_input_receipt_waiters.remove(&key) {
                waiters
            } else {
                if !inner.accepted_input_receipts.contains_key(&key) {
                    inner.accepted_input_receipt_order.push_back(key.clone());
                    while inner.accepted_input_receipt_order.len() > ACCEPTED_INPUT_RECEIPT_LIMIT {
                        if let Some(old) = inner.accepted_input_receipt_order.pop_front() {
                            inner.accepted_input_receipts.remove(&old);
                        }
                    }
                }
                inner.accepted_input_receipts.insert(key, ());
                return;
            }
        };
        for waiter in waiters {
            let _ = waiter.send(());
        }
    }

    /// Wait for Codex's native transcript to prove that one accepted input reached the turn.
    ///
    /// A bounded recent-receipt buffer covers the race where the app-server notification arrives
    /// before the `turn/steer` response returns to the transport.
    pub async fn wait_for_accepted_user_input_echo(
        &self,
        thread_id: &str,
        turn_id: &str,
        text: &str,
        timeout: Duration,
    ) -> Result<(), CodexTurnWaitError> {
        let key = (thread_id.to_string(), turn_id.to_string(), text.to_string());
        let rx = {
            let mut inner = self.inner.lock().unwrap();
            if inner.accepted_input_receipts.remove(&key).is_some() {
                inner.accepted_input_receipt_order.retain(|row| row != &key);
                return Ok(());
            }
            let (tx, rx) = oneshot::channel();
            inner
                .accepted_input_receipt_waiters
                .entry(key.clone())
                .or_default()
                .push(tx);
            rx
        };

        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(())) | Ok(Err(_)) => Ok(()),
            Err(_elapsed) => {
                let mut inner = self.inner.lock().unwrap();
                if let Some(waiters) = inner.accepted_input_receipt_waiters.get_mut(&key) {
                    waiters.retain(|tx| !tx.is_closed());
                    if waiters.is_empty() {
                        inner.accepted_input_receipt_waiters.remove(&key);
                    }
                }
                Err(CodexTurnWaitError::Timeout {
                    thread_id: thread_id.to_string(),
                    turn_id: turn_id.to_string(),
                    timeout,
                })
            }
        }
    }

    /// Drop stale accepted-input echo markers after Codex reports the turn finished.
    pub fn clear_accepted_user_input_echoes_for_turn(&self, thread_id: &str, turn_id: &str) {
        let mut inner = self.inner.lock().unwrap();
        let Some(queue) = inner.accepted_user_input_echoes.get_mut(thread_id) else {
            return;
        };
        queue.retain(|echo| echo.turn_id != turn_id);
        if queue.is_empty() {
            inner.accepted_user_input_echoes.remove(thread_id);
        }
    }

    /// Wait until the forwarder reports `turn/completed` or `error` for `thread_id`/`turn_id`.
    ///
    /// A small recent-completion buffer covers the race where Codex sends the completion
    /// notification before the `turn/start` response reaches the transport.
    pub async fn wait_for_completion(
        &self,
        thread_id: &str,
        turn_id: &str,
        timeout: Duration,
    ) -> Result<(), CodexTurnWaitError> {
        let key = (thread_id.to_string(), turn_id.to_string());
        let rx = {
            let mut inner = self.inner.lock().unwrap();
            if let Some(completion) = inner.completed.remove(&key) {
                return completion.into_result();
            }
            let (tx, rx) = oneshot::channel();
            inner.waiters.entry(key.clone()).or_default().push(tx);
            rx
        };

        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(completion)) => completion.into_result(),
            Ok(Err(_)) => Ok(()),
            Err(_elapsed) => {
                let mut inner = self.inner.lock().unwrap();
                if let Some(waiters) = inner.waiters.get_mut(&key) {
                    waiters.retain(|tx| !tx.is_closed());
                    if waiters.is_empty() {
                        inner.waiters.remove(&key);
                    }
                }
                Err(CodexTurnWaitError::Timeout {
                    thread_id: thread_id.to_string(),
                    turn_id: turn_id.to_string(),
                    timeout,
                })
            }
        }
    }

    /// Wait until Codex proves the injected input reached model context.
    ///
    /// The first assistant text, reasoning, plan, or tool activity is sufficient evidence even
    /// when later native steering keeps the same turn open. A terminal completion is also a
    /// receipt; a terminal failure before any model progress remains an error.
    pub async fn wait_for_delivery_receipt(
        &self,
        thread_id: &str,
        turn_id: &str,
        timeout: Duration,
    ) -> Result<(), CodexTurnWaitError> {
        let key = (thread_id.to_string(), turn_id.to_string());
        let rx = {
            let mut inner = self.inner.lock().unwrap();
            if let Some(receipt) = inner.receipts.remove(&key) {
                return receipt.into_result();
            }
            let (tx, rx) = oneshot::channel();
            inner
                .receipt_waiters
                .entry(key.clone())
                .or_default()
                .push(tx);
            rx
        };

        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(receipt)) => receipt.into_result(),
            Ok(Err(_)) => Ok(()),
            Err(_elapsed) => {
                let mut inner = self.inner.lock().unwrap();
                if let Some(waiters) = inner.receipt_waiters.get_mut(&key) {
                    waiters.retain(|tx| !tx.is_closed());
                    if waiters.is_empty() {
                        inner.receipt_waiters.remove(&key);
                    }
                }
                Err(CodexTurnWaitError::Timeout {
                    thread_id: thread_id.to_string(),
                    turn_id: turn_id.to_string(),
                    timeout,
                })
            }
        }
    }

    /// Record model progress without clearing native active-turn authority.
    pub fn observe_delivery_receipt(&self, thread_id: &str, turn_id: &str) {
        self.settle_delivery_receipt(thread_id, turn_id, CodexTurnCompletion::Completed);
    }

    /// Mark one Codex turn complete and wake any matching transport waiter.
    pub fn complete(&self, thread_id: &str, turn_id: &str) {
        self.finish(thread_id, turn_id, CodexTurnCompletion::Completed);
    }

    /// Mark one Codex turn failed and wake any matching transport waiter.
    pub fn fail(&self, thread_id: &str, turn_id: &str, params: Value) {
        self.finish(
            thread_id,
            turn_id,
            CodexTurnCompletion::Failed(CodexTurnFailure {
                thread_id: thread_id.to_string(),
                turn_id: turn_id.to_string(),
                params,
            }),
        );
    }

    fn finish(&self, thread_id: &str, turn_id: &str, completion: CodexTurnCompletion) {
        self.settle_delivery_receipt(thread_id, turn_id, completion.clone());
        self.clear_accepted_user_input_echoes_for_turn(thread_id, turn_id);
        let key = (thread_id.to_string(), turn_id.to_string());
        let waiters = {
            let mut inner = self.inner.lock().unwrap();
            if inner.active_turn_ids.get(thread_id).map(String::as_str) == Some(turn_id) {
                inner.active_turn_ids.remove(thread_id);
            }
            inner
                .last_finished_turn_ids
                .insert(thread_id.to_string(), turn_id.to_string());
            if let Some(waiters) = inner.waiters.remove(&key) {
                waiters
            } else {
                if !inner.completed.contains_key(&key) {
                    inner.completed_order.push_back(key.clone());
                    while inner.completed_order.len() > RECENT_COMPLETIONS_LIMIT {
                        if let Some(old) = inner.completed_order.pop_front() {
                            inner.completed.remove(&old);
                        }
                    }
                }
                inner.completed.insert(key, completion);
                return;
            }
        };

        for waiter in waiters {
            let _ = waiter.send(completion.clone());
        }
    }

    fn settle_delivery_receipt(
        &self,
        thread_id: &str,
        turn_id: &str,
        receipt: CodexTurnCompletion,
    ) {
        let key = (thread_id.to_string(), turn_id.to_string());
        let waiters = {
            let mut inner = self.inner.lock().unwrap();
            if let Some(waiters) = inner.receipt_waiters.remove(&key) {
                waiters
            } else {
                if !inner.receipts.contains_key(&key) {
                    inner.receipt_order.push_back(key.clone());
                    while inner.receipt_order.len() > RECENT_RECEIPTS_LIMIT {
                        if let Some(old) = inner.receipt_order.pop_front() {
                            inner.receipts.remove(&old);
                        }
                    }
                }
                inner.receipts.insert(key, receipt);
                return;
            }
        };

        for waiter in waiters {
            let _ = waiter.send(receipt.clone());
        }
    }
}

impl CodexTurnCompletion {
    fn into_result(self) -> Result<(), CodexTurnWaitError> {
        match self {
            Self::Completed => Ok(()),
            Self::Failed(failure) => Err(CodexTurnWaitError::Failed(failure)),
        }
    }
}

fn accepted_user_input_text(event: &WsEvent) -> Option<&str> {
    match event {
        WsEvent::AgentUpdate {
            kind: AgentUpdateKind::UserInput,
            data,
            ..
        } => data.get("text").and_then(serde_json::Value::as_str),
        _ => None,
    }
}
