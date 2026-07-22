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
use nexus_contracts::{AgentUpdateKind, GatewayProjectionEffect, WsEvent};
use nexus_dispatch::Bell;
use nexus_harness_claude::native::bridge::ClaudeNativeBridgePaths;
use nexus_harness_claude::native::forwarder::{
    forward_once_with_tool_observations, ClaudeForwarderStats, ClaudeToolObservationSink,
};
use nexus_harness_claude::native::hooks::{hook_log_path, message_delta_log_path};
use nexus_harness_claude::storage::ClaudeRuntimeStateRepo;
use nexus_pty::TurnAcceptanceObserver;
use nexus_store::Store;
use nexus_transcript::ToolCallObservation;

use crate::daemon::gateway_stream_socket::GatewayStreamPublisher;
use crate::daemon::transcript_archive::archive_claude_once;

/// Default polling cadence for launch-local Claude hook JSONL files.
pub const DEFAULT_CLAUDE_NATIVE_FORWARDER_POLL_MS: u64 = 250;

/// Per-runtime terminal-generation signal shared by the Claude native input wrapper and hook
/// forwarder. A waiter snapshots before submitting input and settles only after a later Stop or
/// StopFailure boundary is observed from Claude's native hook stream.
#[derive(Default)]
pub struct ClaudeTurnCompletion {
    generation: AtomicU64,
    notify: tokio::sync::Notify,
    submission_generation: AtomicU64,
    submission_notify: tokio::sync::Notify,
    next_accepted_input_id: AtomicU64,
    accepted_inputs: Mutex<VecDeque<PendingAcceptedInput>>,
}

struct PendingAcceptedInput {
    id: u64,
    text: String,
    observer: Arc<dyn TurnAcceptanceObserver>,
    accepted: Arc<AtomicBool>,
}

/// RAII ownership for one programmatic input awaiting its native `UserPromptSubmit` fact.
/// Dropping a cancelled/failed send removes only that registration, so a later manual prompt is
/// never mistaken for the abandoned caller-bound input.
pub struct ClaudeAcceptedInputRegistration {
    completion: Arc<ClaudeTurnCompletion>,
    id: u64,
    accepted: Arc<AtomicBool>,
}

impl ClaudeAcceptedInputRegistration {
    pub fn was_accepted(&self) -> bool {
        self.accepted.load(Ordering::SeqCst)
    }
}

impl Drop for ClaudeAcceptedInputRegistration {
    fn drop(&mut self) {
        self.completion.remove_accepted_input(self.id);
    }
}

impl ClaudeTurnCompletion {
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
    pub fn register_accepted_input(
        self: &Arc<Self>,
        text: impl Into<String>,
        observer: Arc<dyn TurnAcceptanceObserver>,
    ) -> ClaudeAcceptedInputRegistration {
        let id = self.next_accepted_input_id.fetch_add(1, Ordering::SeqCst);
        let accepted = Arc::new(AtomicBool::new(false));
        self.accepted_inputs
            .lock()
            .unwrap()
            .push_back(PendingAcceptedInput {
                id,
                text: text.into(),
                observer,
                accepted: accepted.clone(),
            });
        ClaudeAcceptedInputRegistration {
            completion: self.clone(),
            id,
            accepted,
        }
    }

    fn remove_accepted_input(&self, id: u64) {
        self.accepted_inputs
            .lock()
            .unwrap()
            .retain(|pending| pending.id != id);
    }

    pub(crate) async fn accept_native_user_input(&self, text: &str) -> bool {
        let pending = {
            let mut inputs = self.accepted_inputs.lock().unwrap();
            let Some(index) = inputs.iter().position(|pending| pending.text == text) else {
                return false;
            };
            inputs.remove(index).expect("matched accepted input exists")
        };
        pending.observer.accepted().await;
        pending.accepted.store(true, Ordering::SeqCst);
        true
    }
}

struct ClaudeAcceptedInputSink {
    upstream: Arc<dyn EventSink>,
    completion: Arc<ClaudeTurnCompletion>,
}

#[async_trait]
impl EventSink for ClaudeAcceptedInputSink {
    async fn emit(&self, event: WsEvent) {
        let native_user_text = match &event {
            WsEvent::AgentUpdate {
                kind: AgentUpdateKind::UserInput,
                data,
                ..
            } => data.get("text").and_then(serde_json::Value::as_str),
            _ => None,
        };
        if let Some(text) = native_user_text {
            if self.completion.accept_native_user_input(text).await {
                return;
            }
        }
        self.upstream.emit(event).await;
    }

    async fn project(&self, effect: GatewayProjectionEffect) {
        self.upstream.project(effect).await;
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
            let forward_events: Arc<dyn EventSink> = match completion.as_ref() {
                Some(completion) => Arc::new(ClaudeAcceptedInputSink {
                    upstream: events.clone(),
                    completion: completion.clone(),
                }),
                None => events.clone(),
            };
            match forward_once_with_tool_observations(
                store.clone(),
                session.clone(),
                paths.clone(),
                forward_events,
                tool_events.clone(),
            )
            .await
            {
                Ok(stats) => {
                    if stats.user_input_events > 0 {
                        if let Some(completion) = completion.as_ref() {
                            completion.signal_submission();
                        }
                    }
                    if stats.turn_end_events > 0 {
                        if let Some(completion) = completion.as_ref() {
                            completion.signal();
                        }
                    }
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
