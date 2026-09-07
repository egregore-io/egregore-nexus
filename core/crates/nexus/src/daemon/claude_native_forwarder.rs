//! Daemon-owned live loop for headed Claude native bridge output.
//!
//! The Claude harness crate owns parsing and cursor persistence through
//! `nexus_harness_claude::native::forwarder::forward_once`. The daemon owns lifecycle: after a
//! headed Claude runtime is launched, revived, or adopted on daemon boot, this module repeatedly
//! forwards newly appended hook JSONL into the shared [`EventSink`]. That keeps `/agent/<name>`
//! observe streams live without scraping the full-screen Claude TUI.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use nexus_common::NexusError;
use nexus_contracts::ids::SessionId;
use nexus_contracts::ports::EventSink;
use nexus_dispatch::Bell;
use nexus_harness_claude::native::bridge::ClaudeNativeBridgePaths;
use nexus_harness_claude::native::forwarder::{
    forward_once_with_observations, ClaudeForwarderStats, ClaudeHookObservationSink,
    ClaudeToolObservationSink,
};
use nexus_harness_claude::native::hooks::{hook_log_path, message_delta_log_path};
use nexus_harness_claude::native::transcript::ClaudeHookRecord;
use nexus_harness_claude::storage::ClaudeRuntimeStateRepo;
use nexus_pty::TurnAcceptanceObserver;
use nexus_store::Store;
use nexus_transcript::ToolCallObservation;

use crate::daemon::gateway_stream_socket::GatewayStreamPublisher;
use crate::daemon::transcript_archive::archive_claude_once;

/// Default polling cadence for launch-local Claude hook JSONL files.
pub const DEFAULT_CLAUDE_NATIVE_FORWARDER_POLL_MS: u64 = 250;

/// Binding-owned native activity and receipt facts, ingested before presentation awaits.
/// Compatibility waiters retain terminal counters; observed inputs instead await their own
/// record-provenance receipt, callback completion, and matching terminal fact.
pub struct ClaudeTurnCompletion {
    owner: uuid::Uuid,
    adopting: bool,
    hook_log: Mutex<Option<PathBuf>>,
    activity: Mutex<ClaudeActivity>,
    generation: AtomicU64,
    notify: tokio::sync::Notify,
    submission_generation: AtomicU64,
    submission_notify: tokio::sync::Notify,
    next_accepted_input_id: AtomicU64,
    accepted_inputs: Mutex<VecDeque<PendingAcceptedInput>>,
}

struct ClaudeActivity {
    valid_owner: bool,
    native_session: Option<String>,
    offset: u64,
    unknown: bool,
    ambiguous: bool,
    open: Option<ClaudeHookRecord>,
}

impl Default for ClaudeTurnCompletion {
    fn default() -> Self {
        Self::new(None, None)
    }
}

struct PendingAcceptedInput {
    id: u64,
    text: String,
    observer: Arc<dyn TurnAcceptanceObserver>,
    accepted: Arc<AtomicBool>,
    owner: uuid::Uuid,
    native_session: Option<String>,
    after_offset: u64,
    receipt: Option<ClaudeHookRecord>,
    accepting: bool,
    terminal: Arc<AtomicBool>,
}

/// RAII ownership for one programmatic input awaiting its native `UserPromptSubmit` fact.
/// Dropping a cancelled/failed send removes only that registration, so a later manual prompt is
/// never mistaken for the abandoned caller-bound input.
pub struct ClaudeAcceptedInputRegistration {
    completion: Arc<ClaudeTurnCompletion>,
    id: u64,
    accepted: Arc<AtomicBool>,
    terminal: Arc<AtomicBool>,
}

impl ClaudeAcceptedInputRegistration {
    pub fn was_accepted(&self) -> bool {
        self.accepted.load(Ordering::SeqCst)
    }

    /// Completion remains bound to this input, including when Stop and another Submit were
    /// ingested together while the caller's acceptance callback was still blocked.
    pub async fn wait(&self, timeout: Duration) -> Result<(), String> {
        tokio::time::timeout(timeout, async {
            loop {
                let changed = self.completion.notify.notified();
                if !self.completion.is_current() {
                    return Err("native binding was replaced".into());
                }
                if self.was_accepted() && self.terminal.load(Ordering::SeqCst) {
                    return Ok(());
                }
                changed.await;
            }
        })
        .await
        .map_err(|_| "native receipt/terminal completion timed out".to_string())?
    }
}

impl Drop for ClaudeAcceptedInputRegistration {
    fn drop(&mut self) {
        self.completion.remove_accepted_input(self.id);
    }
}

impl ClaudeTurnCompletion {
    pub fn new(native_session: Option<String>, hook_log: Option<PathBuf>) -> Self {
        let offset = hook_log
            .as_ref()
            .and_then(|p| std::fs::metadata(p).ok())
            .map_or(0, |m| m.len());
        Self {
            owner: uuid::Uuid::new_v4(),
            adopting: false,
            hook_log: Mutex::new(hook_log),
            activity: Mutex::new(ClaudeActivity {
                valid_owner: true,
                native_session,
                offset,
                unknown: true,
                ambiguous: false,
                open: None,
            }),
            generation: AtomicU64::new(0),
            notify: tokio::sync::Notify::new(),
            submission_generation: AtomicU64::new(0),
            submission_notify: tokio::sync::Notify::new(),
            next_accepted_input_id: AtomicU64::new(0),
            accepted_inputs: Mutex::new(VecDeque::new()),
        }
    }

    pub fn is_current(&self) -> bool {
        self.activity.lock().unwrap().valid_owner
    }
    pub(crate) fn for_adoption(hook_log: Option<PathBuf>) -> Self {
        Self {
            adopting: true,
            ..Self::new(None, hook_log)
        }
    }
    pub fn owner_id(&self) -> uuid::Uuid {
        self.owner
    }
    pub fn has_open_turn(&self) -> bool {
        let state = self.activity.lock().unwrap();
        state.valid_owner && state.open.is_some()
    }
    pub fn is_unknown(&self) -> bool {
        self.activity.lock().unwrap().unknown
    }
    pub fn invalidate(&self) {
        self.activity.lock().unwrap().valid_owner = false;
        self.notify.notify_waiters();
        self.submission_notify.notify_waiters();
    }
    pub(crate) fn attach_resume_identity(&self, native_session: Option<String>) {
        let mut state = self.activity.lock().unwrap();
        if self.adopting && state.valid_owner && state.native_session.is_none() {
            state.native_session = native_session;
        }
    }
    pub(crate) fn attach_hook_source(&self, path: PathBuf) {
        let mut state = self.activity.lock().unwrap();
        if !state.valid_owner {
            return;
        }
        let mut source = self.hook_log.lock().unwrap();
        if source.as_ref() != Some(&path) {
            // Adoption can resolve a non-default sidecar path only after an awaited store read.
            // Historical bytes at that newly attached source cannot establish fresh activity.
            state.offset =
                std::fs::metadata(&path).map_or(state.offset, |m| m.len().max(state.offset));
            state.unknown = true;
            *source = Some(path);
        }
    }
    pub async fn wait_for_idle(&self) {
        loop {
            let changed = self.notify.notified();
            if !self.has_open_turn() {
                return;
            }
            changed.await;
        }
    }
    pub fn snapshot(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    pub fn signal(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    pub async fn wait_after(&self, observed: u64, timeout: Duration) -> Result<(), String> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let notified = self.notify.notified();
            if !self.is_current() {
                return Err("native binding was replaced".into());
            }
            if self.snapshot() > observed {
                return Ok(());
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err("Claude native turn completion timed out".to_string());
            }
            tokio::time::timeout(remaining, notified)
                .await
                .map_err(|_| "Claude native turn completion timed out".to_string())?;
        }
    }

    /// Snapshot the number of structured `UserPromptSubmit` boundaries seen for this runtime.
    /// Claude can keep an accepted prompt visible as a queued-command preview while a tool loop
    /// is active, so terminal geometry alone cannot prove whether Enter was accepted.
    pub fn submission_snapshot(&self) -> u64 {
        self.submission_generation.load(Ordering::SeqCst)
    }

    pub fn signal_submission(&self) {
        self.submission_generation.fetch_add(1, Ordering::SeqCst);
        self.submission_notify.notify_waiters();
    }

    pub async fn wait_for_submission_after(
        &self,
        observed: u64,
        timeout: Duration,
    ) -> Result<(), String> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let notified = self.submission_notify.notified();
            if self.submission_snapshot() > observed {
                return Ok(());
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err("Claude native prompt submission timed out".to_string());
            }
            tokio::time::timeout(remaining, notified)
                .await
                .map_err(|_| "Claude native prompt submission timed out".to_string())?;
        }
    }

    /// Register the exact programmatic input whose native echo must be replaced by the
    /// caller-bound accepted event carried by `observer`.
    /// The pre-write file offset excludes old records. Native session, owner and record identity
    /// survive the asynchronous callback; identical concurrent manual text remains ambiguous
    /// without a native caller token. Dropping registration never retracts a terminal write.
    pub fn register_accepted_input(
        self: &Arc<Self>,
        text: impl Into<String>,
        observer: Arc<dyn TurnAcceptanceObserver>,
    ) -> ClaudeAcceptedInputRegistration {
        let id = self.next_accepted_input_id.fetch_add(1, Ordering::SeqCst);
        let accepted = Arc::new(AtomicBool::new(false));
        let terminal = Arc::new(AtomicBool::new(false));
        let state = self.activity.lock().unwrap();
        let after_offset = self
            .hook_log
            .lock()
            .unwrap()
            .as_ref()
            .and_then(|p| std::fs::metadata(p).ok())
            .map_or(state.offset, |m| m.len().max(state.offset));
        self.accepted_inputs
            .lock()
            .unwrap()
            .push_back(PendingAcceptedInput {
                id,
                text: text.into(),
                observer,
                accepted: accepted.clone(),
                owner: self.owner,
                native_session: state.native_session.clone(),
                after_offset,
                receipt: None,
                accepting: false,
                terminal: terminal.clone(),
            });
        ClaudeAcceptedInputRegistration {
            completion: self.clone(),
            id,
            accepted,
            terminal,
        }
    }

    fn remove_accepted_input(&self, id: u64) {
        self.accepted_inputs
            .lock()
            .unwrap()
            .retain(|pending| pending.id != id);
    }

    pub async fn accept_native_user_input(&self, record: &ClaudeHookRecord) -> bool {
        let (observer, accepted) = {
            let state = self.activity.lock().unwrap();
            if !state.valid_owner {
                return false;
            }
            let mut inputs = self.accepted_inputs.lock().unwrap();
            let Some(pending) = inputs.iter_mut().find(|pending| {
                pending.owner == self.owner
                    && pending.receipt.as_ref() == Some(record)
                    && !pending.accepting
            }) else {
                return false;
            };
            pending.accepting = true;
            (pending.observer.clone(), pending.accepted.clone())
        };
        observer.accepted().await;
        if !self.is_current() {
            return true;
        }
        accepted.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
        true
    }
}

#[async_trait]
impl ClaudeHookObservationSink for ClaudeTurnCompletion {
    fn observe_hooks(&self, records: &[ClaudeHookRecord], file_len: Option<u64>, complete: bool) {
        let mut state = self.activity.lock().unwrap();
        if !state.valid_owner {
            return;
        }
        if file_len.is_none_or(|len| len < state.offset) {
            state.unknown = true;
            state.ambiguous |= state.open.is_some();
            // A truncated source cannot close established work, even after cursor rewind.
            return;
        }
        let mut inputs = self.accepted_inputs.lock().unwrap();
        for record in records {
            if record.end_offset <= state.offset {
                continue;
            }
            state.offset = record.end_offset;
            if !record.valid {
                state.unknown = true;
                state.ambiguous = true;
                continue;
            }
            if state.native_session.is_none() && record.kind.as_deref() == Some("SessionStart") {
                state.native_session = record.session_id.clone();
                for pending in inputs.iter_mut() {
                    if pending.owner == self.owner && pending.native_session.is_none() {
                        pending.native_session = record.session_id.clone();
                    }
                }
            }
            if state.native_session.is_none() || state.native_session != record.session_id {
                state.unknown = true;
                state.ambiguous = true;
                continue;
            }
            match record.kind.as_deref() {
                Some("UserPromptSubmit") => {
                    state.ambiguous |= state.open.is_some();
                    state.open = Some(record.clone());
                    state.unknown = state.ambiguous;
                    self.signal_submission();
                    if let Some(pending) = inputs.iter_mut().find(|p| {
                        p.owner == self.owner
                            && p.native_session == record.session_id
                            && p.after_offset < record.end_offset
                            && p.receipt.is_none()
                            && Some(&p.text) == record.prompt.as_ref()
                    }) {
                        pending.receipt = Some(record.clone());
                    }
                }
                Some("Stop" | "StopFailure") => {
                    let singleton = (!state.ambiguous)
                        .then(|| state.open.as_ref().map(|open| open.end_offset))
                        .flatten();
                    let matches = |submit: &ClaudeHookRecord| {
                        submit.end_offset < record.end_offset
                            && submit.session_id == record.session_id
                            && match (&submit.prompt_id, &record.prompt_id) {
                                (Some(submit_id), Some(stop_id)) => submit_id == stop_id,
                                // Existing hooks may omit either id. Append order can match only
                                // the sole observed, nonconflicting open submit; never an overlap.
                                _ => singleton == Some(submit.end_offset),
                            }
                    };
                    for pending in inputs.iter_mut() {
                        if pending.receipt.as_ref().is_some_and(matches) {
                            pending.terminal.store(true, Ordering::SeqCst);
                        }
                    }
                    if state.open.as_ref().is_some_and(matches) {
                        state.open = None;
                        state.unknown = false;
                        state.ambiguous = false;
                        self.signal();
                    } else {
                        state.unknown = true;
                        state.ambiguous |= state.open.is_some();
                    }
                }
                Some("SessionStart") if state.open.is_some() => {
                    state.unknown = true;
                    state.ambiguous = true;
                }
                _ => {}
            }
        }
        if !complete {
            state.unknown = true;
        }
        self.notify.notify_waiters();
    }
    async fn accept_input(&self, record: &ClaudeHookRecord) -> bool {
        self.accept_native_user_input(record).await
    }
}

/// Resolve the bridge paths for a runtime from Claude-owned sidecar state, falling back to the
/// deterministic `~/.nexus/claude-sessions/<runtime>/bridge` layout used during launch.
pub async fn claude_native_paths_for_runtime(
    store: &Store,
    session: &SessionId,
) -> Result<ClaudeNativeBridgePaths, NexusError> {
    let state = ClaudeRuntimeStateRepo::new(store)
        .find_by_runtime_id(session)
        .await?;
    if let Some(bridge_dir) = state.and_then(|row| row.bridge_dir) {
        return Ok(paths_from_bridge_dir(bridge_dir));
    }
    Ok(ClaudeNativeBridgePaths::new(
        &default_nexus_state_dir(),
        session,
    ))
}

/// Spawn a poll loop that forwards Claude hook records into `events`. `TurnEnd`,
/// `SessionStart`/user-input, and compaction records ring the session bell so pending bus
/// deliveries can be retried after the headed runtime becomes idle or redraws around compaction.
pub fn spawn_claude_native_forwarder(
    store: Arc<Store>,
    session: SessionId,
    paths: ClaudeNativeBridgePaths,
    events: Arc<dyn EventSink>,
    bell: Bell,
    poll_ms: u64,
) -> tokio::task::JoinHandle<()> {
    spawn_claude_native_forwarder_with_tool_events(
        store, session, paths, events, bell, poll_ms, None, None,
    )
}

/// Spawn a Claude forwarder with an optional ephemeral gateway developer-event publisher.
pub fn spawn_claude_native_forwarder_with_tool_events(
    store: Arc<Store>,
    session: SessionId,
    paths: ClaudeNativeBridgePaths,
    events: Arc<dyn EventSink>,
    bell: Bell,
    poll_ms: u64,
    tool_events: Option<Arc<dyn ClaudeToolObservationSink>>,
    completion: Option<Arc<ClaudeTurnCompletion>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let delay = Duration::from_millis(poll_ms.max(1));
        loop {
            if completion.as_ref().is_some_and(|c| !c.is_current()) {
                break;
            }
            match forward_once_with_observations(
                store.clone(),
                session.clone(),
                paths.clone(),
                events.clone(),
                tool_events.clone(),
                completion
                    .clone()
                    .map(|c| c as Arc<dyn ClaudeHookObservationSink>),
            )
            .await
            {
                Ok(stats) => {
                    spawn_archive_for_lifecycle_stats(store.clone(), session.clone(), stats);
                    if should_ring_delivery_for_stats(stats) {
                        bell.ring(&session);
                    }
                    if stats.text_events > 0
                        || stats.turn_end_events > 0
                        || stats.compaction_events > 0
                    {
                        tracing::debug!(
                            target: "nexus::claude_native_forwarder",
                            session = %session,
                            text_events = stats.text_events,
                            turn_end_events = stats.turn_end_events,
                            compaction_events = stats.compaction_events,
                            "forwarded Claude native bridge records"
                        );
                    }
                }
                Err(error) => {
                    tracing::warn!(
                        target: "nexus::claude_native_forwarder",
                        session = %session,
                        error = %error,
                        "Claude native bridge forward pass failed"
                    );
                }
            }
            tokio::time::sleep(delay).await;
        }
    })
}

/// Gateway-backed, observation-only sink for Claude native tool-call phases.
pub(crate) struct GatewayClaudeToolObservationSink {
    publisher: GatewayStreamPublisher,
    agent_name: String,
}

impl GatewayClaudeToolObservationSink {
    pub(crate) fn new(publisher: GatewayStreamPublisher, agent_name: String) -> Self {
        Self {
            publisher,
            agent_name,
        }
    }
}

impl ClaudeToolObservationSink for GatewayClaudeToolObservationSink {
    fn publish_tool_call(&self, session: &SessionId, observation: ToolCallObservation) {
        self.publisher
            .publish_tool_call_observation(session, &self.agent_name, observation);
    }
}

fn spawn_archive_for_lifecycle_stats(
    store: Arc<Store>,
    session: SessionId,
    stats: ClaudeForwarderStats,
) {
    if store.has_split_authority() {
        return;
    }
    let Some(last_event) = archive_event_for_stats(stats) else {
        return;
    };
    tokio::spawn(async move {
        if let Err(error) = archive_claude_once(store, &session, Some(last_event)).await {
            tracing::warn!(
                target: "nexus::transcript_archive",
                session = %session,
                error = %error,
                "Claude transcript archive lifecycle pass failed"
            );
        }
    });
}

fn archive_event_for_stats(stats: ClaudeForwarderStats) -> Option<String> {
    if stats.compaction_events > 0 {
        Some("Compact".to_string())
    } else if stats.turn_end_events > 0 {
        Some("Stop".to_string())
    } else if stats.session_start_events > 0 {
        Some("SessionStart".to_string())
    } else {
        None
    }
}

fn should_ring_delivery_for_stats(stats: ClaudeForwarderStats) -> bool {
    stats.session_start_events > 0
        || stats.turn_end_events > 0
        || stats.compaction_events > 0
        || stats.user_input_events > 0
}

fn paths_from_bridge_dir(bridge_dir: PathBuf) -> ClaudeNativeBridgePaths {
    ClaudeNativeBridgePaths {
        settings_path: bridge_dir.join("settings.json"),
        hook_log_path: hook_log_path(&bridge_dir),
        message_delta_log_path: message_delta_log_path(&bridge_dir),
        identity_path: nexus_harness_claude::native::hooks::identity_path(&bridge_dir),
        bridge_dir,
    }
}

fn default_nexus_state_dir() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
    PathBuf::from(home).join(".nexus")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claude_lifecycle_stats_rearm_delivery() {
        assert!(should_ring_delivery_for_stats(ClaudeForwarderStats {
            session_start_events: 1,
            ..Default::default()
        }));
        assert!(should_ring_delivery_for_stats(ClaudeForwarderStats {
            turn_end_events: 1,
            ..Default::default()
        }));
        assert!(should_ring_delivery_for_stats(ClaudeForwarderStats {
            compaction_events: 1,
            ..Default::default()
        }));
        assert!(should_ring_delivery_for_stats(ClaudeForwarderStats {
            user_input_events: 1,
            ..Default::default()
        }));
        assert!(!should_ring_delivery_for_stats(ClaudeForwarderStats {
            text_events: 1,
            ..Default::default()
        }));
    }

    #[test]
    fn claude_lifecycle_stats_select_archive_event() {
        assert_eq!(
            archive_event_for_stats(ClaudeForwarderStats {
                session_start_events: 1,
                ..Default::default()
            }),
            Some("SessionStart".to_string())
        );
        assert_eq!(
            archive_event_for_stats(ClaudeForwarderStats {
                turn_end_events: 1,
                ..Default::default()
            }),
            Some("Stop".to_string())
        );
        assert_eq!(
            archive_event_for_stats(ClaudeForwarderStats {
                compaction_events: 1,
                turn_end_events: 1,
                ..Default::default()
            }),
            Some("Compact".to_string())
        );
        assert_eq!(
            archive_event_for_stats(ClaudeForwarderStats {
                user_input_events: 1,
                ..Default::default()
            }),
            None
        );
    }
}
