//! Ephemeral daemon-side developer events for native tool-call observations.
//!
//! This service is deliberately in-memory only. It turns
//! [`nexus_transcript::ToolCallObservation`] values into metadata-only
//! [`DeveloperEventEnvelope`](nexus_contracts::DeveloperEventEnvelope)s on reserved
//! `sys.agent.<name>.tool_call` topics, keeps a bounded per-session ring for reconnect replay, and
//! wakes only WebSocket subscribers. It never writes `developer_events`, inserts command intents,
//! or injects agent turns.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use nexus_common::now;
use nexus_contracts::ids::SessionId;
use nexus_contracts::{DeveloperEventEnvelope, DeveloperEventKind, DeveloperToolCallPhase};
use nexus_transcript::{ToolCallObservation, ToolCallPhase};
use tokio::sync::broadcast;

/// Default number of ephemeral tool-call events retained per session topic.
pub const DEFAULT_TOOL_CALL_EVENT_RING_CAP: usize = 256;
/// Default number of in-flight native tool-call ids retained for dedupe/name carry.
pub const DEFAULT_TOOL_CALL_START_CACHE_CAP: usize = 1024;

/// In-memory publisher for daemon-origin `sys.agent.<name>.tool_call` events.
///
/// The service is cheap to clone and safe to call from native forwarder tasks. It owns no store
/// handle by construction, so publishing these events cannot create durable rows or participate in
/// bus fanout.
#[derive(Clone)]
pub struct ToolCallEventService {
    inner: Arc<Mutex<ToolCallEventInner>>,
    ring_cap: usize,
    start_cache_cap: usize,
}

/// Replay result plus a live receiver for one session's tool-call event topic.
pub struct ToolCallEventSubscription {
    /// Retained events with `seq > after_seq`, oldest first.
    pub replay: ToolCallEventReplay,
    /// Live events published after subscription.
    pub live: broadcast::Receiver<DeveloperEventEnvelope>,
}

/// Bounded replay response for an ephemeral tool-call subscription.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCallEventReplay {
    pub events: Vec<DeveloperEventEnvelope>,
    pub gap: Option<ToolCallEventGap>,
}

/// Indicates the requested ephemeral cursor is older than the retained ring.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCallEventGap {
    pub session_id: SessionId,
    pub after_seq: i64,
    pub first_retained_seq: i64,
}

#[derive(Default)]
struct ToolCallEventInner {
    topics: HashMap<SessionId, SessionTopic>,
}

struct SessionTopic {
    topic: String,
    next_seq: i64,
    ring: VecDeque<DeveloperEventEnvelope>,
    tx: broadcast::Sender<DeveloperEventEnvelope>,
    active_starts: HashMap<String, String>,
    start_order: VecDeque<String>,
    phase_keys: HashSet<(String, &'static str)>,
    phase_order: VecDeque<(String, &'static str)>,
}

impl Default for ToolCallEventService {
    fn default() -> Self {
        Self::new(
            DEFAULT_TOOL_CALL_EVENT_RING_CAP,
            DEFAULT_TOOL_CALL_START_CACHE_CAP,
        )
    }
}

impl ToolCallEventService {
    /// Build a service with explicit caps. Zero caps are clamped to one retained event/start.
    pub fn new(ring_cap: usize, start_cache_cap: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(ToolCallEventInner::default())),
            ring_cap: ring_cap.max(1),
            start_cache_cap: start_cache_cap.max(1),
        }
    }

    /// Publish one native tool-call observation as an ephemeral developer event.
    ///
    /// Returns `None` when a native `(tool_call_id, phase)` duplicate is suppressed. Observations
    /// without a native id are emitted best-effort because they cannot be deduped safely.
    pub fn publish_tool_call(
        &self,
        session_id: &SessionId,
        agent_name: &str,
        observation: ToolCallObservation,
    ) -> Option<DeveloperEventEnvelope> {
        let mut inner = self.inner.lock().unwrap();
        let topic = inner
            .topics
            .entry(session_id.clone())
            .or_insert_with(|| SessionTopic::new(tool_call_topic(agent_name), self.ring_cap));
        topic.topic = tool_call_topic(agent_name);

        let phase = developer_phase(observation.phase);
        let tool_call_id = observation.tool_call_id.as_deref();
        if let Some(tool_call_id) = tool_call_id {
            if !topic.remember_phase(tool_call_id, phase, self.start_cache_cap) {
                return None;
            }
        }

        let tool = match (tool_call_id, phase) {
            (Some(tool_call_id), DeveloperToolCallPhase::Post) if observation.tool.is_empty() => {
                topic
                    .active_starts
                    .get(tool_call_id)
                    .cloned()
                    .unwrap_or_else(|| observation.tool.clone())
            }
            _ => observation.tool.clone(),
        };

        match (tool_call_id, phase) {
            (Some(tool_call_id), DeveloperToolCallPhase::Pre) => {
                topic.remember_start(tool_call_id, &observation.tool, self.start_cache_cap);
            }
            (Some(tool_call_id), DeveloperToolCallPhase::Post) => {
                topic.forget_start(tool_call_id);
            }
            _ => {}
        }

        let event = DeveloperEventEnvelope {
            kind: DeveloperEventKind::ToolCall,
            topic: topic.topic.clone(),
            seq: topic.next_seq,
            ts: now(),
            thread: None,
            dm: None,
            from: None,
            message_id: None,
            agent: Some(agent_name.to_string()),
            session_id: Some(session_id.clone()),
            lifecycle: None,
            current_work: None,
            data: None,
            tool: Some(tool),
            phase: Some(phase),
            ok: Some(observation.ok),
        };
        topic.next_seq += 1;
        topic.ring.push_back(event.clone());
        while topic.ring.len() > self.ring_cap {
            topic.ring.pop_front();
        }
        let _ = topic.tx.send(event.clone());
        Some(event)
    }

    /// Subscribe to retained and live tool-call events for a session.
    pub fn subscribe_tool_calls(
        &self,
        session_id: &SessionId,
        after_seq: i64,
    ) -> ToolCallEventSubscription {
        let mut inner = self.inner.lock().unwrap();
        let topic = inner
            .topics
            .entry(session_id.clone())
            .or_insert_with(|| SessionTopic::new(String::new(), self.ring_cap));
        let first_retained_seq = topic.ring.front().map(|event| event.seq);
        let gap = first_retained_seq.and_then(|first| {
            if after_seq > 0 && after_seq < first - 1 {
                Some(ToolCallEventGap {
                    session_id: session_id.clone(),
                    after_seq,
                    first_retained_seq: first,
                })
            } else {
                None
            }
        });
        let events = topic
            .ring
            .iter()
            .filter(|event| event.seq > after_seq)
            .cloned()
            .collect();
        ToolCallEventSubscription {
            replay: ToolCallEventReplay { events, gap },
            live: topic.tx.subscribe(),
        }
    }

    #[cfg(test)]
    fn active_start_count(&self, session_id: &SessionId) -> usize {
        self.inner
            .lock()
            .unwrap()
            .topics
            .get(session_id)
            .map(|topic| topic.active_starts.len())
            .unwrap_or(0)
    }

    #[cfg(test)]
    fn active_start_ids(&self, session_id: &SessionId) -> Vec<String> {
        self.inner
            .lock()
            .unwrap()
            .topics
            .get(session_id)
            .map(|topic| topic.start_order.iter().cloned().collect())
            .unwrap_or_default()
    }

    #[cfg(test)]
    fn phase_key_count(&self, session_id: &SessionId) -> usize {
        self.inner
            .lock()
            .unwrap()
            .topics
            .get(session_id)
            .map(|topic| topic.phase_keys.len())
            .unwrap_or(0)
    }
}

impl SessionTopic {
    fn new(topic: String, live_cap: usize) -> Self {
        let (tx, _rx) = broadcast::channel(live_cap.max(1));
        Self {
            topic,
            next_seq: 1,
            ring: VecDeque::new(),
            tx,
            active_starts: HashMap::new(),
            start_order: VecDeque::new(),
            phase_keys: HashSet::new(),
            phase_order: VecDeque::new(),
        }
    }

    fn remember_phase(
        &mut self,
        tool_call_id: &str,
        phase: DeveloperToolCallPhase,
        cap: usize,
    ) -> bool {
        let key = (tool_call_id.to_string(), phase_token(phase));
        if self.phase_keys.contains(&key) {
            return false;
        }
        self.phase_keys.insert(key.clone());
        self.phase_order.push_back(key);
        while self.phase_keys.len() > cap {
            let Some(oldest) = self.phase_order.pop_front() else {
                break;
            };
            if self.phase_keys.remove(&oldest)
                && oldest.1 == phase_token(DeveloperToolCallPhase::Pre)
            {
                self.active_starts.remove(&oldest.0);
                self.start_order.retain(|id| id != &oldest.0);
            }
        }
        true
    }

    fn remember_start(&mut self, tool_call_id: &str, tool: &str, cap: usize) {
        if !self.active_starts.contains_key(tool_call_id) {
            self.start_order.push_back(tool_call_id.to_string());
        }
        self.active_starts
            .insert(tool_call_id.to_string(), tool.to_string());
        while self.active_starts.len() > cap {
            let Some(oldest) = self.start_order.pop_front() else {
                break;
            };
            self.active_starts.remove(&oldest);
            self.remove_phase_key(&oldest, DeveloperToolCallPhase::Pre);
        }
    }

    fn forget_start(&mut self, tool_call_id: &str) {
        self.active_starts.remove(tool_call_id);
        self.start_order.retain(|id| id != tool_call_id);
        self.remove_phase_key(tool_call_id, DeveloperToolCallPhase::Pre);
    }

    fn remove_phase_key(&mut self, tool_call_id: &str, phase: DeveloperToolCallPhase) {
        let key = (tool_call_id.to_string(), phase_token(phase));
        self.phase_keys.remove(&key);
        self.phase_order.retain(|candidate| candidate != &key);
    }
}

fn tool_call_topic(agent_name: &str) -> String {
    format!("sys.agent.{agent_name}.tool_call")
}

fn developer_phase(phase: ToolCallPhase) -> DeveloperToolCallPhase {
    match phase {
        ToolCallPhase::Pre => DeveloperToolCallPhase::Pre,
        ToolCallPhase::Post => DeveloperToolCallPhase::Post,
    }
}

fn phase_token(phase: DeveloperToolCallPhase) -> &'static str {
    match phase {
        DeveloperToolCallPhase::Pre => "pre",
        DeveloperToolCallPhase::Post => "post",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session() -> SessionId {
        SessionId("s_tool_events".to_string())
    }

    fn obs(id: Option<&str>, tool: &str, phase: ToolCallPhase, ok: bool) -> ToolCallObservation {
        ToolCallObservation {
            tool_call_id: id.map(str::to_string),
            tool: tool.to_string(),
            phase,
            ok,
        }
    }

    #[test]
    fn publishes_monotonic_sequences_and_replays_after_cursor() {
        let service = ToolCallEventService::new(8, 8);
        let session = session();

        let first = service
            .publish_tool_call(
                &session,
                "otto",
                obs(None, "Read", ToolCallPhase::Pre, true),
            )
            .expect("first event");
        let second = service
            .publish_tool_call(
                &session,
                "otto",
                obs(None, "Read", ToolCallPhase::Post, true),
            )
            .expect("second event");

        assert_eq!(first.seq, 1);
        assert_eq!(second.seq, 2);
        assert_eq!(first.topic, "sys.agent.otto.tool_call");
        assert_eq!(second.phase, Some(DeveloperToolCallPhase::Post));
        assert_eq!(second.ok, Some(true));

        let subscription = service.subscribe_tool_calls(&session, 1);
        assert_eq!(subscription.replay.gap, None);
        assert_eq!(subscription.replay.events, vec![second]);
    }

    #[tokio::test]
    async fn subscription_receives_live_events_without_store_or_bell() {
        let service = ToolCallEventService::new(8, 8);
        let session = session();
        let mut subscription = service.subscribe_tool_calls(&session, 0);

        let event = service
            .publish_tool_call(
                &session,
                "morgan",
                obs(Some("tc_1"), "Bash", ToolCallPhase::Pre, true),
            )
            .expect("event");

        let live = subscription.live.recv().await.expect("live event");
        assert_eq!(live, event);
        assert_eq!(live.topic, "sys.agent.morgan.tool_call");
        assert_eq!(live.session_id, Some(session));
    }

    #[test]
    fn ring_is_bounded_and_reports_gap_for_dropped_cursor() {
        let service = ToolCallEventService::new(2, 8);
        let session = session();
        for index in 0..4 {
            service
                .publish_tool_call(
                    &session,
                    "iris",
                    obs(None, &format!("tool-{index}"), ToolCallPhase::Pre, true),
                )
                .expect("event");
        }

        let fresh = service.subscribe_tool_calls(&session, 0);
        assert_eq!(fresh.replay.gap, None);
        assert_eq!(
            fresh
                .replay
                .events
                .iter()
                .map(|event| event.seq)
                .collect::<Vec<_>>(),
            vec![3, 4]
        );

        let stale = service.subscribe_tool_calls(&session, 1);
        assert_eq!(
            stale.replay.gap,
            Some(ToolCallEventGap {
                session_id: session,
                after_seq: 1,
                first_retained_seq: 3,
            })
        );
        assert_eq!(stale.replay.events.len(), 2);
    }

    #[test]
    fn orphan_start_cache_is_bounded_oldest_first() {
        let service = ToolCallEventService::new(8, 2);
        let session = session();
        for id in ["tc_a", "tc_b", "tc_c"] {
            service
                .publish_tool_call(
                    &session,
                    "rufus",
                    obs(Some(id), "Read", ToolCallPhase::Pre, true),
                )
                .expect("event");
        }

        assert_eq!(service.active_start_count(&session), 2);
        assert_eq!(
            service.active_start_ids(&session),
            vec!["tc_b".to_string(), "tc_c".to_string()]
        );
    }

    #[test]
    fn matching_post_removes_start_cache_entry() {
        let service = ToolCallEventService::new(8, 8);
        let session = session();
        service
            .publish_tool_call(
                &session,
                "dexter",
                obs(Some("tc_1"), "Edit", ToolCallPhase::Pre, true),
            )
            .expect("pre event");
        assert_eq!(service.active_start_ids(&session), vec!["tc_1".to_string()]);

        service
            .publish_tool_call(
                &session,
                "dexter",
                obs(Some("tc_1"), "Edit", ToolCallPhase::Post, false),
            )
            .expect("post event");

        assert_eq!(service.active_start_count(&session), 0);
    }

    #[test]
    fn matching_post_can_carry_tool_name_from_start() {
        let service = ToolCallEventService::new(8, 8);
        let session = session();
        service
            .publish_tool_call(
                &session,
                "dexter",
                obs(Some("tc_1"), "Bash", ToolCallPhase::Pre, true),
            )
            .expect("pre event");

        let post = service
            .publish_tool_call(
                &session,
                "dexter",
                obs(Some("tc_1"), "", ToolCallPhase::Post, true),
            )
            .expect("post event");

        assert_eq!(post.tool.as_deref(), Some("Bash"));
        assert_eq!(service.active_start_count(&session), 0);
    }

    #[test]
    fn duplicate_native_phase_is_suppressed_until_start_is_evicted() {
        let service = ToolCallEventService::new(8, 1);
        let session = session();
        service
            .publish_tool_call(
                &session,
                "otto",
                obs(Some("tc_1"), "Read", ToolCallPhase::Pre, true),
            )
            .expect("first pre");
        assert!(
            service
                .publish_tool_call(
                    &session,
                    "otto",
                    obs(Some("tc_1"), "Read", ToolCallPhase::Pre, true)
                )
                .is_none(),
            "same native id + phase should be suppressed while retained"
        );

        service
            .publish_tool_call(
                &session,
                "otto",
                obs(Some("tc_2"), "Bash", ToolCallPhase::Pre, true),
            )
            .expect("evicting pre");

        assert!(
            service
                .publish_tool_call(
                    &session,
                    "otto",
                    obs(Some("tc_1"), "Read", ToolCallPhase::Pre, true)
                )
                .is_some(),
            "old orphan starts are evicted to keep the map bounded"
        );
    }

    #[test]
    fn phase_dedupe_cache_is_bounded_for_post_only_events() {
        let service = ToolCallEventService::new(8, 2);
        let session = session();
        for id in ["tc_a", "tc_b", "tc_c"] {
            service
                .publish_tool_call(
                    &session,
                    "otto",
                    obs(Some(id), "Bash", ToolCallPhase::Post, true),
                )
                .expect("post event");
        }

        assert_eq!(service.phase_key_count(&session), 2);
        assert!(
            service
                .publish_tool_call(
                    &session,
                    "otto",
                    obs(Some("tc_a"), "Bash", ToolCallPhase::Post, true)
                )
                .is_some(),
            "old post-only phase keys are evicted to keep dedupe bounded"
        );
    }
}
