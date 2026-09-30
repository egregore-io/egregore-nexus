//! Turn-completion rendezvous for headed Codex app-server sessions.
//!
//! `turn/start` returns a `TurnStartResponse`, but headed Codex inbox delivery is not complete
//! until the harness turn reaches `turn/completed` or `error`. Bus batches wait on this rendezvous
//! before the realtime loop marks stored rows delivered. Direct operator prompts return after
//! `turn/start` acceptance; the notification forwarder still owns progress/completion for observe
//! consumers. Direct operator prompts return after `turn/start` acceptance, while native active
//! turn ids keep the daemon's durable prompt boundary queue held until completion. Explicit steer
//! requests use the same runtime ids through a separate control path.

use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;

use nexus_contracts::events::WsEvent;
use nexus_contracts::ports::EventSink;
use nexus_contracts::{AgentUpdateKind, TurnObservation, TurnObservationStamp, TurnState};
use serde_json::Value;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use super::jsonrpc::{CodexRpcError, Notification};
use super::protocol::method;
use super::provider_limit::turn_error_will_retry;

const RECENT_COMPLETIONS_LIMIT: usize = 128;
const RECENT_RECEIPTS_LIMIT: usize = 128;
const ACCEPTED_ECHO_LIMIT: usize = 128;
const ACCEPTED_INPUT_RECEIPT_LIMIT: usize = 128;

type TurnKey = (String, String);
type AcceptedInputKey = (String, String, String);

#[derive(Default)]
struct Inner {
    phase: OwnerPhase,
    telemetry: super::telemetry::State,
    account_telemetry: super::account_telemetry::State,
    native_revision: u64,
    fact_revision: u64,
    disconnected: bool,
    idle_threads: HashSet<String>,
    /// Latest native active turn observed for each Codex thread. This is routing authority for
    /// explicit `turn/steer`; UI run ids and daemon prompt rows never populate it.
    active_turn_ids: HashMap<String, String>,
    /// Bounded recent native terminals. Responses can arrive after more than one terminal;
    /// serial receipt settlement must neither replace these facts nor revive their turns.
    native_terminals: HashSet<TurnKey>,
    native_terminal_order: VecDeque<TurnKey>,
    waiters: HashMap<TurnKey, Vec<oneshot::Sender<CodexTurnCompletion>>>,
    completed: HashMap<TurnKey, CodexTurnCompletion>,
    completed_order: VecDeque<TurnKey>,
    // Projected terminals, after all earlier native input records have been processed. Raw
    // reader terminals are too early: an input receipt can still be behind a blocked sink.
    input_closed_turns: HashSet<TurnKey>,
    input_closed_order: VecDeque<TurnKey>,
    receipt_waiters: HashMap<TurnKey, Vec<oneshot::Sender<CodexTurnCompletion>>>,
    receipts: HashMap<TurnKey, CodexTurnCompletion>,
    receipt_order: VecDeque<TurnKey>,
    next_accepted_id: u64,
    accepted_events: HashMap<String, VecDeque<AcceptedEvent>>,
    accepted_user_input_echo_queue: HashMap<String, VecDeque<PendingAcceptedUserInputEcho>>,
    accepted_user_input_echoes: HashMap<String, VecDeque<AcceptedUserInputEcho>>,
    cancelled_user_input_echo_queue: HashMap<String, VecDeque<String>>,
    cancelled_user_input_echoes: HashMap<String, VecDeque<AcceptedUserInputEcho>>,
    next_accepted_input_receipt_waiter_id: u64,
    accepted_input_receipt_waiters: HashMap<AcceptedInputKey, Vec<AcceptedInputReceiptWaiter>>,
    accepted_input_receipts: HashMap<AcceptedInputKey, ()>,
    accepted_input_receipt_order: VecDeque<AcceptedInputKey>,
}

#[derive(Default)]
enum OwnerPhase {
    #[default]
    Legacy,
    Provisional(Option<String>),
    Published(String),
    Revoked,
}

#[derive(Default)]
struct Registry {
    legacy: Inner,
    next_owner: u64,
    owners: HashMap<u64, Inner>,
}

struct OwnerLease {
    id: u64,
    observation_id: String,
    registry: Weak<Mutex<Registry>>,
    revoked: CancellationToken,
    model_reporting: Option<nexus_agent::adapter::NativeModelReporting>,
}

impl Drop for OwnerLease {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.upgrade() {
            let mut registry = registry.lock().unwrap();
            if let Some(reporting) = &self.model_reporting {
                reporting.sink().revoke();
            }
            registry.owners.remove(&self.id);
        } else if let Some(reporting) = &self.model_reporting {
            reporting.sink().revoke();
        }
    }
}

struct TrackerGuard<'a> {
    registry: MutexGuard<'a, Registry>,
    owner: Option<u64>,
}

impl Deref for TrackerGuard<'_> {
    type Target = Inner;
    fn deref(&self) -> &Inner {
        match self.owner {
            Some(id) => &self.registry.owners[&id],
            None => &self.registry.legacy,
        }
    }
}

impl DerefMut for TrackerGuard<'_> {
    fn deref_mut(&mut self) -> &mut Inner {
        match self.owner {
            Some(id) => self.registry.owners.get_mut(&id).unwrap(),
            None => &mut self.registry.legacy,
        }
    }
}

struct AcceptedEvent {
    id: u64,
    events: Arc<dyn EventSink>,
    event: WsEvent,
    boundary: AcceptedEventBoundary,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AcceptedEventBoundary {
    AnyTurnNotification,
    NativeUserInput,
}

struct AcceptedUserInputEcho {
    turn_id: String,
    text: String,
}

struct PendingAcceptedUserInputEcho {
    id: u64,
    text: String,
}

struct AcceptedInputReceiptWaiter {
    id: u64,
    tx: oneshot::Sender<()>,
}

struct AcceptedInputReceiptRegistration {
    tracker: CodexTurnTracker,
    key: AcceptedInputKey,
    id: u64,
    rx: oneshot::Receiver<()>,
}

impl Drop for AcceptedInputReceiptRegistration {
    fn drop(&mut self) {
        self.tracker
            .remove_accepted_input_receipt_waiter(&self.key, self.id);
    }
}

/// Shared tracker that lets the app-server transport wait for forwarder-observed turn completion.
#[derive(Clone)]
pub struct CodexTurnTracker {
    inner: Arc<Mutex<Registry>>,
    owner: Option<Arc<OwnerLease>>,
}

impl Default for CodexTurnTracker {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Registry::default())),
            owner: None,
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
    InputReceiptUnavailable {
        thread_id: String,
        turn_id: String,
    },
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
                Self::InputReceiptUnavailable { thread_id, turn_id } => format!(
                    "codex input receipt unavailable for {thread_id}/{turn_id}; delivery is unconfirmed"
                ),
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
    fn lock(&self, thread_id: &str) -> TrackerGuard<'_> {
        let registry = self.inner.lock().unwrap();
        let owner = self.owner.as_ref().map(|owner| owner.id).or_else(|| {
            let mut matches = registry.owners.iter().filter_map(|(id, state)| {
                matches!(&state.phase, OwnerPhase::Published(thread) if thread == thread_id)
                    .then_some(*id)
            });
            let first = matches.next()?;
            matches.next().is_none().then_some(first)
        });
        TrackerGuard { registry, owner }
    }

    pub(super) fn new_owner_with_reporting(
        &self,
        thread: Option<String>,
        model_reporting: Option<nexus_agent::adapter::NativeModelReporting>,
    ) -> Self {
        let mut registry = self.inner.lock().unwrap();
        registry.next_owner += 1;
        let id = registry.next_owner;
        registry.owners.insert(
            id,
            Inner {
                phase: OwnerPhase::Provisional(thread),
                ..Inner::default()
            },
        );
        Self {
            inner: self.inner.clone(),
            owner: Some(Arc::new(OwnerLease {
                id,
                observation_id: nexus_common::new_binding_id(),
                registry: Arc::downgrade(&self.inner),
                revoked: CancellationToken::new(),
                model_reporting,
            })),
        }
    }

    pub(super) fn same_owner(&self, other: &Self) -> bool {
        match (&self.owner, &other.owner) {
            (Some(left), Some(right)) => Arc::ptr_eq(left, right),
            _ => false,
        }
    }

    pub(super) fn publish_owner(&self, thread: &str) -> bool {
        let mut inner = self.lock(thread);
        if inner.disconnected {
            return false;
        }
        match &inner.phase {
            OwnerPhase::Provisional(expected)
                if expected.as_deref().is_none_or(|id| id == thread) => {}
            _ => return false,
        }
        if let Some(reporting) = self
            .owner
            .as_ref()
            .and_then(|owner| owner.model_reporting.as_ref())
        {
            if !reporting.sink().bind_native_root(thread) {
                return false;
            }
        }
        inner.active_turn_ids.retain(|id, _| id == thread);
        inner.idle_threads.retain(|id| id == thread);
        inner.native_terminals.retain(|(id, _)| id == thread);
        inner.native_terminal_order.retain(|(id, _)| id == thread);
        inner.phase = OwnerPhase::Published(thread.to_string());
        inner.fact_revision += 1;
        true
    }

    pub(super) fn revoke_owner(&self) {
        if let Some(owner) = &self.owner {
            let mut inner = self.lock("");
            if !matches!(inner.phase, OwnerPhase::Revoked) {
                inner.phase = OwnerPhase::Revoked;
                inner.fact_revision += 1;
            }
            owner.revoked.cancel();
            if let Some(reporting) = &owner.model_reporting {
                reporting.sink().revoke();
            }
        }
    }

    /// Check the captured model/native pair while native revocation remains excluded.
    pub(super) fn with_model_binding<T>(
        &self,
        thread: &str,
        reporting: &nexus_agent::adapter::NativeModelReporting,
        f: impl FnOnce(&str) -> T,
    ) -> Option<T> {
        let owner = self.owner.as_ref()?;
        if !owner.model_reporting.as_ref()?.same_owner(reporting) {
            return None;
        }
        let inner = self.lock(thread);
        if inner.disconnected
            || !matches!(&inner.phase, OwnerPhase::Published(current) if current == thread)
        {
            return None;
        }
        if !reporting
            .sink()
            .accepts_profile(reporting.profile().identity())
        {
            return None;
        }
        Some(f(thread))
    }

    /// A setup response belongs only to the provisional connection that submitted it.
    pub(super) fn observe_setup_model(&self, thread: &str, result: &Value) -> bool {
        let Some(reporting) = self
            .owner
            .as_ref()
            .and_then(|owner| owner.model_reporting.as_ref())
        else {
            return true;
        };
        let inner = self.lock(thread);
        if inner.disconnected
            || !matches!(&inner.phase, OwnerPhase::Provisional(expected) if expected.as_deref().is_none_or(|id| id == thread))
        {
            return false;
        }
        let Some(value) = super::model_reporting::decode_configured(result, thread) else {
            return false;
        };
        reporting.sink().bind_native_root(thread)
            && reporting
                .sink()
                .observe(nexus_contracts::model_report::NativeModelUpdate {
                    native_session_id: thread.into(),
                    field: nexus_contracts::model_report::ModelEvidenceField::Configured,
                    value,
                })
    }

    pub(super) fn is_revoked(&self) -> bool {
        self.owner
            .as_ref()
            .is_some_and(|owner| owner.revoked.is_cancelled())
    }

    pub(super) async fn revoked(&self) {
        match &self.owner {
            Some(owner) => owner.revoked.cancelled().await,
            None => std::future::pending().await,
        }
    }

    pub(super) fn admit<T>(
        &self,
        submit: impl FnOnce() -> Result<T, CodexRpcError>,
    ) -> Result<T, CodexRpcError> {
        let inner = self.lock("");
        if !matches!(inner.phase, OwnerPhase::Published(_)) {
            return Err(CodexRpcError::Closed);
        }
        // start_send is local SplitSink admission, not a wire or native acknowledgement.
        submit()
    }

    pub(super) fn admit_setup<T>(
        &self,
        submit: impl FnOnce() -> Result<T, CodexRpcError>,
    ) -> Result<T, CodexRpcError> {
        let inner = self.lock("");
        if !matches!(inner.phase, OwnerPhase::Provisional(_)) {
            return Err(CodexRpcError::Closed);
        }
        submit()
    }

    pub(super) fn native_revision(&self) -> u64 {
        self.lock("").native_revision
    }

    /// Snapshot this incarnation's native facts, separate from request-admission revisions.
    pub(super) fn observe_turn(&self, thread: &str) -> TurnObservation {
        let Some(owner) = &self.owner else {
            return TurnObservation::default();
        };
        let inner = self.lock(thread);
        TurnObservation {
            state: if inner.disconnected {
                TurnState::Unavailable
            } else {
                match &inner.phase {
                    OwnerPhase::Revoked => TurnState::Unavailable,
                    OwnerPhase::Published(expected) if expected == thread => {
                        if inner.active_turn_ids.contains_key(thread) {
                            TurnState::NativeOpen
                        } else if inner.idle_threads.contains(thread) {
                            TurnState::VerifiedIdle
                        } else {
                            TurnState::Unknown
                        }
                    }
                    _ => TurnState::Unknown,
                }
            },
            stamp: Some(TurnObservationStamp {
                owner: owner.observation_id.clone(),
                revision: inner.fact_revision,
            }),
            ..Default::default()
        }
    }

    pub(super) fn observe_disconnect(&self) {
        let mut inner = self.lock("");
        if !inner.disconnected {
            inner.disconnected = true;
            inner.fact_revision += 1;
        }
        if let Some(reporting) = self
            .owner
            .as_ref()
            .and_then(|owner| owner.model_reporting.as_ref())
        {
            reporting.sink().revoke();
        }
    }

    /// A validated matching resume or successful fresh thread creation can prove idle only
    /// if no newer native notification arrived while that setup request was pending.
    pub(super) fn seed_idle(&self, thread: &str, before: u64) {
        let mut inner = self.lock(thread);
        if !thread.is_empty()
            && matches!(inner.phase, OwnerPhase::Provisional(_))
            && inner.native_revision == before
            && !inner.active_turn_ids.contains_key(thread)
            && inner.idle_threads.insert(thread.to_string())
        {
            inner.fact_revision += 1;
        }
    }

    pub(super) fn apply_steer_response(&self, thread: &str, turn: Option<&str>, before: u64) {
        let mut inner = self.lock(thread);
        if matches!(inner.phase, OwnerPhase::Revoked) || inner.native_revision != before {
            return;
        }
        match turn {
            Some(turn)
                if !inner
                    .native_terminals
                    .contains(&(thread.to_string(), turn.to_string())) =>
            {
                record_active(&mut inner, thread, turn);
            }
            None => {
                clear_authority(&mut inner, thread);
            }
            _ => {}
        }
    }

    pub(super) fn select_thread(&self, thread: &str) -> bool {
        let mut inner = self.lock(thread);
        match &inner.phase {
            OwnerPhase::Provisional(expected)
                if expected.as_deref().is_none_or(|id| id == thread) =>
            {
                inner.phase = OwnerPhase::Provisional(Some(thread.to_string()));
                true
            }
            _ => false,
        }
    }

    pub(super) fn seed_resume(&self, thread: &str, turn: Option<&str>, before: u64) {
        let mut inner = self.lock(thread);
        if matches!(inner.phase, OwnerPhase::Provisional(_)) && inner.native_revision == before {
            if let Some(turn) = turn {
                if !inner
                    .native_terminals
                    .contains(&(thread.to_string(), turn.to_string()))
                {
                    if !inner.active_turn_ids.contains_key(thread) {
                        record_active(&mut inner, thread, turn);
                    }
                }
            }
        }
    }

    pub(super) fn ingest_native(&self, note: &Notification) {
        if note.id.is_some() {
            return;
        }
        if matches!(
            note.method.as_str(),
            "account/rateLimits/updated" | "account/updated"
        ) {
            // These native account-wide notifications have no threadId. They belong to this
            // connection's captured reporter, never to a late/current registry lookup or turn.
            if note.params.get("threadId").is_some() {
                return;
            }
            let mut inner = self.lock("");
            let root = match &inner.phase {
                OwnerPhase::Published(root) if !inner.disconnected => root.clone(),
                _ => return,
            };
            if let Some(reporting) = self
                .owner
                .as_ref()
                .and_then(|owner| owner.model_reporting.as_ref())
            {
                inner.account_telemetry.ingest(note, &root, reporting);
            }
            return;
        }
        let Some(thread) = note.params.get("threadId").and_then(Value::as_str) else {
            return;
        };
        let mut inner = self.lock(thread);
        match &inner.phase {
            OwnerPhase::Revoked => return,
            OwnerPhase::Published(expected) if expected != thread => return,
            OwnerPhase::Provisional(Some(expected)) if expected != thread => return,
            _ => {}
        }
        if matches!(&inner.phase, OwnerPhase::Published(expected) if expected == thread)
            && !inner.disconnected
        {
            if let Some(reporting) = self
                .owner
                .as_ref()
                .and_then(|owner| owner.model_reporting.as_ref())
            {
                inner.telemetry.ingest(note, thread, reporting);
            }
        }
        // Token metadata is not native turn-start/status authority, including legacy owners.
        if note.method == method::TOKEN_USAGE_UPDATED {
            return;
        }
        let Some(turn) = note
            .params
            .get("turnId")
            .and_then(Value::as_str)
            .or_else(|| note.params.get("turn")?.get("id")?.as_str())
        else {
            return;
        };
        // Fresh setup retains compact per-thread summaries until thread/start identifies its owner.
        if !inner.active_turn_ids.contains_key(thread)
            && inner.active_turn_ids.len() >= RECENT_COMPLETIONS_LIMIT
        {
            // A bounded provisional summary cannot turn dropped native-open evidence into idle.
            // Fail this candidate closed; a later attempt may establish its own complete summary.
            inner.phase = OwnerPhase::Revoked;
            inner.fact_revision += 1;
            if let Some(owner) = &self.owner {
                owner.revoked.cancel();
            }
            return;
        }
        inner.native_revision = inner.native_revision.wrapping_add(1);
        if note.method == method::TURN_COMPLETED
            || (note.method == method::TURN_FAILED && !turn_error_will_retry(&note.params))
        {
            record_terminal(&mut inner, thread, turn);
        } else if !inner
            .native_terminals
            .contains(&(thread.to_string(), turn.to_string()))
        {
            record_active(&mut inner, thread, turn);
        }
    }

    /// Native active turn currently observed for `thread_id`.
    pub fn active_turn_id(&self, thread_id: &str) -> Option<String> {
        let inner = self.lock(thread_id);
        if matches!(inner.phase, OwnerPhase::Revoked) || (
            self.owner.is_none() && inner.registry.owners.values().filter(|state|
                matches!(&state.phase, OwnerPhase::Published(thread) if thread == thread_id)
            ).count() > 1
        ) { return None; }
        inner.active_turn_ids.get(thread_id).cloned()
    }

    /// Apply consumed native terminal evidence to routing authority without settling receipts.
    /// Display/storage I/O may still be pending; it must not keep this exact turn busy or allow
    /// a late start response to resurrect it. A newer active turn is never cleared here.
    pub(super) fn observe_terminal_turn(&self, thread_id: &str, turn_id: &str) {
        let mut inner = self.lock(thread_id);
        if !matches!(inner.phase, OwnerPhase::Revoked) {
            record_terminal(&mut inner, thread_id, turn_id);
        }
    }

    /// Record runtime evidence that `turn_id` is active for `thread_id`.
    ///
    /// `turn/started` is the primary source. Non-terminal item/delta notifications also call this
    /// to recover authority when Nexus attaches after the start notification or loses a race with
    /// a TUI-originated turn.
    pub fn observe_active_turn(&self, thread_id: &str, turn_id: &str) {
        let mut inner = self.lock(thread_id);
        if matches!(inner.phase, OwnerPhase::Revoked)
            || inner
                .native_terminals
                .contains(&(thread_id.to_string(), turn_id.to_string()))
        {
            return;
        }
        record_active(&mut inner, thread_id, turn_id);
    }

    /// Seed an idle tracker from a successful `turn/start` response without overwriting newer
    /// notification evidence. `turn/start` can itself steer, so an existing native id always wins.
    pub fn record_turn_start_acceptance(&self, thread_id: &str, turn_id: &str) {
        let mut inner = self.lock(thread_id);
        if matches!(inner.phase, OwnerPhase::Revoked)
            || inner
                .native_terminals
                .contains(&(thread_id.to_string(), turn_id.to_string()))
        {
            return;
        }
        if !inner.active_turn_ids.contains_key(thread_id) {
            record_active(&mut inner, thread_id, turn_id);
        }
    }

    /// Clear all active-turn authority for a thread after Codex reports there is no active turn.
    pub fn clear_active_turn(&self, thread_id: &str) {
        clear_authority(&mut self.lock(thread_id), thread_id);
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
        self.queue_accepted_event_at_boundary(
            thread_id,
            events,
            event,
            AcceptedEventBoundary::AnyTurnNotification,
        )
    }

    /// Queue an accepted event that may publish only when the exact native user-message receipt
    /// arrives. Restarted Codex threads can replay unrelated turn notifications before a new
    /// prompt reaches context; those notifications must not claim the prompt's visible echo.
    pub fn queue_accepted_event_on_native_user_input(
        &self,
        thread_id: &str,
        events: Arc<dyn EventSink>,
        event: WsEvent,
    ) -> QueuedAcceptedEvent {
        self.queue_accepted_event_at_boundary(
            thread_id,
            events,
            event,
            AcceptedEventBoundary::NativeUserInput,
        )
    }

    fn queue_accepted_event_at_boundary(
        &self,
        thread_id: &str,
        events: Arc<dyn EventSink>,
        event: WsEvent,
        boundary: AcceptedEventBoundary,
    ) -> QueuedAcceptedEvent {
        let mut inner = self.lock(thread_id);
        let id = inner.next_accepted_id;
        inner.next_accepted_id += 1;
        inner
            .accepted_events
            .entry(thread_id.to_string())
            .or_default()
            .push_back(AcceptedEvent {
                id,
                events,
                event,
                boundary,
            });
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
        let mut inner = self.lock(thread_id);
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
        let Some(accepted) = self.take_next_accepted_event(
            thread_id,
            AcceptedEventBoundary::AnyTurnNotification,
            None,
        ) else {
            return false;
        };
        let accepted_text = accepted_user_input_text(&accepted.event).map(str::to_string);
        accepted.events.emit(accepted.event).await;
        if let (Some(turn_id), Some(text)) = (turn_id, accepted_text) {
            self.record_accepted_user_input_echo(thread_id, turn_id, text);
        }
        true
    }

    /// Publish one prompt event only at its exact native user-message boundary.
    pub async fn emit_native_user_input_accepted_event(
        &self,
        thread_id: &str,
        _turn_id: &str,
        text: &str,
    ) -> bool {
        let Some(accepted) = self.take_next_accepted_event(
            thread_id,
            AcceptedEventBoundary::NativeUserInput,
            Some(text),
        ) else {
            return false;
        };
        accepted.events.emit(accepted.event).await;
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

    /// Cancel a prompt wait without allowing its late native user-message echo to repaint as
    /// unowned input. The tombstone is consumed by the exact next native receipt and is bounded
    /// by the same ceiling as accepted echo markers.
    pub fn tombstone_cancelled_user_input_echo(
        &self,
        queued: &QueuedAcceptedUserInputEcho,
        turn_id: Option<&str>,
        text: &str,
    ) -> bool {
        let mut inner = self.lock(&queued.thread_id);
        let mut removed = false;

        if let Some(turn_id) = turn_id {
            let mut remove_queue = false;
            if let Some(queue) = inner.accepted_user_input_echoes.get_mut(&queued.thread_id) {
                if let Some(pos) = queue
                    .iter()
                    .position(|echo| echo.turn_id == turn_id && echo.text == text)
                {
                    queue.remove(pos);
                    removed = true;
                }
                remove_queue = queue.is_empty();
            }
            if remove_queue {
                inner.accepted_user_input_echoes.remove(&queued.thread_id);
            }
        }

        if !removed {
            let mut remove_queue = false;
            if let Some(queue) = inner
                .accepted_user_input_echo_queue
                .get_mut(&queued.thread_id)
            {
                if let Some(pos) = queue.iter().position(|echo| echo.id == queued.id) {
                    queue.remove(pos);
                    removed = true;
                }
                remove_queue = queue.is_empty();
            }
            if remove_queue {
                inner
                    .accepted_user_input_echo_queue
                    .remove(&queued.thread_id);
            }
        }

        if !removed {
            return false;
        }
        if let Some(turn_id) = turn_id {
            let queue = inner
                .cancelled_user_input_echoes
                .entry(queued.thread_id.clone())
                .or_default();
            queue.push_back(AcceptedUserInputEcho {
                turn_id: turn_id.to_string(),
                text: text.to_string(),
            });
            while queue.len() > ACCEPTED_ECHO_LIMIT {
                queue.pop_front();
            }
        } else {
            let queue = inner
                .cancelled_user_input_echo_queue
                .entry(queued.thread_id.clone())
                .or_default();
            queue.push_back(text.to_string());
            while queue.len() > ACCEPTED_ECHO_LIMIT {
                queue.pop_front();
            }
        }
        true
    }

    /// Consume an exact cancellation tombstone. A match suppresses the late native echo but does
    /// not publish the cancelled Nexus accepted event or satisfy its dropped waiter.
    pub fn take_cancelled_user_input_echo(
        &self,
        thread_id: &str,
        turn_id: &str,
        text: &str,
    ) -> bool {
        let mut inner = self.lock(thread_id);
        let mut matched = false;
        let mut remove_bound_queue = false;
        if let Some(queue) = inner.cancelled_user_input_echoes.get_mut(thread_id) {
            if let Some(pos) = queue
                .iter()
                .position(|echo| echo.turn_id == turn_id && echo.text == text)
            {
                queue.remove(pos);
                matched = true;
            }
            remove_bound_queue = queue.is_empty();
        }
        if remove_bound_queue {
            inner.cancelled_user_input_echoes.remove(thread_id);
        }
        if matched {
            return true;
        }

        let mut remove_pending_queue = false;
        if let Some(queue) = inner.cancelled_user_input_echo_queue.get_mut(thread_id) {
            if let Some(pos) = queue.iter().position(|pending| pending == text) {
                queue.remove(pos);
                matched = true;
            }
            remove_pending_queue = queue.is_empty();
        }
        if remove_pending_queue {
            inner.cancelled_user_input_echo_queue.remove(thread_id);
        }
        matched
    }

    fn take_accepted_event(&self, thread_id: &str, id: Option<u64>) -> Option<AcceptedEvent> {
        let mut inner = self.lock(thread_id);
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

    fn take_next_accepted_event(
        &self,
        thread_id: &str,
        boundary: AcceptedEventBoundary,
        text: Option<&str>,
    ) -> Option<AcceptedEvent> {
        let mut inner = self.lock(thread_id);
        let queue = inner.accepted_events.get_mut(thread_id)?;
        let pos = queue.iter().position(|accepted| {
            accepted.boundary == boundary
                && text.is_none_or(|expected| {
                    accepted_user_input_text(&accepted.event) == Some(expected)
                })
        })?;
        let accepted = queue.remove(pos)?;
        if queue.is_empty() {
            inner.accepted_events.remove(thread_id);
        }
        Some(accepted)
    }

    fn take_queued_user_input_echo(
        &self,
        thread_id: &str,
        id: Option<u64>,
    ) -> Option<PendingAcceptedUserInputEcho> {
        let mut inner = self.lock(thread_id);
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

    fn record_accepted_user_input_echo(&self, thread_id: &str, turn_id: &str, text: String) {
        let mut inner = self.lock(thread_id);
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

    /// Claim one Nexus-owned user-input echo for this exact native turn and text.
    ///
    /// The turn/start response and native notification may arrive in either order. Match a marker
    /// already bound by the response first, then an exact-text pending marker. Unrelated resumed
    /// turn notifications can therefore never steal a newly queued prompt.
    pub fn take_accepted_user_input_echo(
        &self,
        thread_id: &str,
        turn_id: &str,
        text: &str,
    ) -> bool {
        let mut inner = self.lock(thread_id);
        let mut matched = false;
        let mut remove_bound_queue = false;
        if let Some(queue) = inner.accepted_user_input_echoes.get_mut(thread_id) {
            if let Some(pos) = queue
                .iter()
                .position(|echo| echo.turn_id == turn_id && echo.text == text)
            {
                queue.remove(pos);
                matched = true;
            }
            remove_bound_queue = queue.is_empty();
        }
        if remove_bound_queue {
            inner.accepted_user_input_echoes.remove(thread_id);
        }
        if matched {
            return true;
        }

        let mut remove_pending_queue = false;
        if let Some(queue) = inner.accepted_user_input_echo_queue.get_mut(thread_id) {
            if let Some(pos) = queue.iter().position(|echo| echo.text == text) {
                queue.remove(pos);
                matched = true;
            }
            remove_pending_queue = queue.is_empty();
        }
        if remove_pending_queue {
            inner.accepted_user_input_echo_queue.remove(thread_id);
        }
        matched
    }

    /// Record the exact native `item/completed userMessage` echo for one Nexus-accepted input.
    ///
    /// `turn/steer` acceptance alone is not durable context admission: the daemon can restart
    /// before Codex records the steered user message. The forwarder calls this only after matching
    /// the native echo against a Nexus-owned accepted-input marker.
    pub fn observe_accepted_user_input_echo(&self, thread_id: &str, turn_id: &str, text: &str) {
        let key = (thread_id.to_string(), turn_id.to_string(), text.to_string());
        let waiters = {
            let mut inner = self.lock(thread_id);
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
            let _ = waiter.tx.send(());
        }
    }

    fn remove_accepted_input_receipt_waiter(&self, key: &AcceptedInputKey, id: u64) {
        let mut inner = self.lock(&key.0);
        let remove_key = if let Some(waiters) = inner.accepted_input_receipt_waiters.get_mut(key) {
            waiters.retain(|waiter| waiter.id != id);
            waiters.is_empty()
        } else {
            false
        };
        if remove_key {
            inner.accepted_input_receipt_waiters.remove(key);
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
        let registration = {
            let mut inner = self.lock(thread_id);
            if inner.accepted_input_receipts.remove(&key).is_some() {
                inner.accepted_input_receipt_order.retain(|row| row != &key);
                return Ok(());
            }
            if inner
                .input_closed_turns
                .contains(&(thread_id.to_owned(), turn_id.to_owned()))
            {
                return Err(CodexTurnWaitError::InputReceiptUnavailable {
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                });
            }
            let (tx, rx) = oneshot::channel();
            let id = inner.next_accepted_input_receipt_waiter_id;
            inner.next_accepted_input_receipt_waiter_id =
                inner.next_accepted_input_receipt_waiter_id.wrapping_add(1);
            inner
                .accepted_input_receipt_waiters
                .entry(key.clone())
                .or_default()
                .push(AcceptedInputReceiptWaiter { id, tx });
            AcceptedInputReceiptRegistration {
                tracker: self.clone(),
                key,
                id,
                rx,
            }
        };
        let mut registration = registration;

        match tokio::time::timeout(timeout, &mut registration.rx).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(CodexTurnWaitError::InputReceiptUnavailable {
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.to_owned(),
            }),
            Err(_elapsed) => Err(CodexTurnWaitError::Timeout {
                thread_id: thread_id.to_string(),
                turn_id: turn_id.to_string(),
                timeout,
            }),
        }
    }

    /// Drop stale accepted-input echo markers after Codex reports the turn finished.
    pub fn clear_accepted_user_input_echoes_for_turn(&self, thread_id: &str, turn_id: &str) {
        let mut inner = self.lock(thread_id);
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
            let mut inner = self.lock(thread_id);
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
                let mut inner = self.lock(thread_id);
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
            let mut inner = self.lock(thread_id);
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
                let mut inner = self.lock(thread_id);
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
        self.observe_terminal_turn(thread_id, turn_id);
        self.finish(thread_id, turn_id, CodexTurnCompletion::Completed);
    }

    /// Mark one Codex turn failed and wake any matching transport waiter.
    pub fn fail(&self, thread_id: &str, turn_id: &str, params: Value) {
        self.observe_terminal_turn(thread_id, turn_id);
        self.settle_failure(thread_id, turn_id, params);
    }

    pub(super) fn settle_completion(&self, thread_id: &str, turn_id: &str) {
        self.finish(thread_id, turn_id, CodexTurnCompletion::Completed);
    }

    pub(super) fn settle_failure(&self, thread_id: &str, turn_id: &str, params: Value) {
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
            let mut inner = self.lock(thread_id);
            if inner.input_closed_turns.insert(key.clone()) {
                inner.input_closed_order.push_back(key.clone());
                while inner.input_closed_order.len() > RECENT_COMPLETIONS_LIMIT {
                    if let Some(old) = inner.input_closed_order.pop_front() {
                        inner.input_closed_turns.remove(&old);
                    }
                }
            }
            // Dropping only this turn's unresolved receipt senders wakes them as unavailable,
            // never as accepted. Exact receipts already processed remain successful.
            inner
                .accepted_input_receipt_waiters
                .retain(|(thread, turn, _), _| thread != thread_id || turn != turn_id);
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
            let mut inner = self.lock(thread_id);
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

fn record_terminal(inner: &mut Inner, thread: &str, turn: &str) {
    let cleared = inner.active_turn_ids.get(thread).map(String::as_str) == Some(turn);
    if cleared {
        inner.active_turn_ids.remove(thread);
    }
    let key = (thread.to_string(), turn.to_string());
    if inner.native_terminals.insert(key.clone()) {
        inner.fact_revision += 1;
        if !inner.active_turn_ids.contains_key(thread) {
            inner.idle_threads.insert(thread.to_string());
        }
        inner.native_terminal_order.push_back(key);
        while inner.native_terminal_order.len() > RECENT_COMPLETIONS_LIMIT {
            if let Some(old) = inner.native_terminal_order.pop_front() {
                inner.native_terminals.remove(&old);
                if !inner
                    .native_terminals
                    .iter()
                    .any(|(thread, _)| thread == &old.0)
                {
                    inner.idle_threads.remove(&old.0);
                }
            }
        }
    }
}

fn record_active(inner: &mut Inner, thread: &str, turn: &str) {
    let was_idle = inner.idle_threads.remove(thread);
    let previous = inner
        .active_turn_ids
        .insert(thread.to_string(), turn.to_string());
    if was_idle || previous.as_deref() != Some(turn) {
        inner.fact_revision += 1;
    }
}

fn clear_authority(inner: &mut Inner, thread: &str) {
    let active = inner.active_turn_ids.remove(thread).is_some();
    let idle = inner.idle_threads.remove(thread);
    if active || idle {
        inner.fact_revision += 1;
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

#[cfg(test)]
#[path = "../../tests/unit/app_server_turn_completion.rs"]
mod ownership_tests;
