//! Ephemeral daemon-side developer events for fleet-wide agent status.
//!
//! This service is deliberately in-memory only, mirroring
//! [`tool_call_events`](super::tool_call_events): it turns agent lifecycle/presence
//! changes (`agent.status` / `agent.spawned` / `agent.removed` [`WsEvent`]s) into
//! metadata-only [`DeveloperEventEnvelope`]s on the reserved **`sys.fleet.status`**
//! topic, keeps one bounded ring for reconnect replay, and wakes only gateway push
//! subscribers. Every subscription ends its bounded replay with an ordered `resync` fact; clients
//! then reconcile from the canonical member read before applying later sequence numbers. It never
//! writes `developer_events` rows — the durable
//! `sys.agent.lifecycle` topic remains the store-backed record; this is the
//! realtime push mirror the frontend subscribes to for live presence dots.
//!
//! The gateway subscribes on the push socket with the well-known pseudo session id
//! [`FLEET_SESSION_KEY`] (there is no per-agent scoping: fleet status is
//! discovery-level data, visible to any WS client of the local gateway).

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use nexus_common::now;
use nexus_contracts::ids::SessionId;
use nexus_contracts::{DeveloperEventEnvelope, DeveloperEventKind};

/// Reserved ephemeral topic carrying fleet-wide agent status events.
pub const FLEET_STATUS_TOPIC: &str = "sys.fleet.status";
/// Pseudo session id the gateway push relay subscribes with for the fleet lane.
pub const FLEET_SESSION_KEY: &str = "fleet";
/// Default number of ephemeral fleet-status events retained for reconnect replay.
pub const DEFAULT_FLEET_EVENT_RING_CAP: usize = 512;

/// One fleet-status observation to publish. `lifecycle` is the phase verb the
/// frontend switches on (`status` / `spawned` / `removed` / `resync`); `data` carries the
/// verb-specific payload (for `status`: `{"presence": …, "paused": …}`).
pub struct FleetStatusObservation {
    pub lifecycle: &'static str,
    pub agent: Option<String>,
    pub session_id: SessionId,
    pub current_work: Option<String>,
    pub data: Option<serde_json::Value>,
}

/// Bounded replay response for an ephemeral fleet-status subscription.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FleetEventReplay {
    pub events: Vec<DeveloperEventEnvelope>,
    /// Latest globally published seq while the replay lock was held. A subscriber-local resync
    /// boundary uses this cursor without appending/broadcasting another global event.
    pub current_seq: i64,
    /// `Some` when the requested cursor is older than the retained ring.
    pub gap: Option<FleetEventGap>,
}

/// Indicates the requested ephemeral cursor is older than the retained ring.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FleetEventGap {
    pub after_seq: i64,
    pub first_retained_seq: i64,
}

/// In-memory single-topic publisher for `sys.fleet.status` events.
///
/// Cheap to clone; owns no store handle by construction, so publishing cannot
/// create durable rows or participate in bus fanout.
#[derive(Clone)]
pub struct FleetEventService {
    inner: Arc<Mutex<FleetEventInner>>,
    ring_cap: usize,
}

struct FleetEventInner {
    next_seq: i64,
    ring: VecDeque<DeveloperEventEnvelope>,
}

impl Default for FleetEventService {
    fn default() -> Self {
        Self::new(DEFAULT_FLEET_EVENT_RING_CAP)
    }
}

impl FleetEventService {
    /// Build a service with an explicit ring cap. Zero is clamped to one retained event.
    pub fn new(ring_cap: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(FleetEventInner {
                next_seq: 1,
                ring: VecDeque::new(),
            })),
            ring_cap: ring_cap.max(1),
        }
    }

    /// Publish one fleet-status observation as an ephemeral developer event.
    pub fn publish(&self, observation: FleetStatusObservation) -> DeveloperEventEnvelope {
        let mut inner = self.inner.lock().unwrap();
        let event = DeveloperEventEnvelope {
            kind: DeveloperEventKind::AgentLifecycle,
            topic: FLEET_STATUS_TOPIC.to_string(),
            seq: inner.next_seq,
            ts: now(),
            thread: None,
            dm: None,
            from: None,
            message_id: None,
            agent: observation.agent,
            session_id: Some(observation.session_id),
            lifecycle: Some(observation.lifecycle.to_string()),
            current_work: observation.current_work,
            data: observation.data,
            tool: None,
            phase: None,
            ok: None,
        };
        inner.next_seq += 1;
        inner.ring.push_back(event.clone());
        while inner.ring.len() > self.ring_cap {
            inner.ring.pop_front();
        }
        event
    }

    /// Retained events with `seq > after_seq`, oldest first, plus a gap marker when
    /// the cursor predates the ring (same semantics as the tool-call lane).
    pub fn replay(&self, after_seq: i64) -> FleetEventReplay {
        let inner = self.inner.lock().unwrap();
        let first_retained_seq = inner.ring.front().map(|event| event.seq);
        let gap = first_retained_seq.and_then(|first| {
            if after_seq > 0 && after_seq < first - 1 {
                Some(FleetEventGap {
                    after_seq,
                    first_retained_seq: first,
                })
            } else {
                None
            }
        });
        let events = inner
            .ring
            .iter()
            .filter(|event| event.seq > after_seq)
            .cloned()
            .collect();
        FleetEventReplay {
            events,
            current_seq: inner.next_seq - 1,
            gap,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn observation(lifecycle: &'static str, session: &str) -> FleetStatusObservation {
        FleetStatusObservation {
            lifecycle,
            agent: Some("demoa".to_string()),
            session_id: SessionId(session.to_string()),
            current_work: None,
            data: Some(json!({"presence": "online", "paused": false})),
        }
    }

    #[test]
    fn publishes_monotonic_seq_on_the_fleet_topic() {
        let service = FleetEventService::new(8);
        let a = service.publish(observation("status", "s_a"));
        let b = service.publish(observation("spawned", "s_b"));
        assert_eq!(a.topic, FLEET_STATUS_TOPIC);
        assert_eq!(a.kind, DeveloperEventKind::AgentLifecycle);
        assert_eq!((a.seq, b.seq), (1, 2));
        assert_eq!(b.lifecycle.as_deref(), Some("spawned"));
    }

    #[test]
    fn replay_filters_by_cursor_and_flags_gaps() {
        let service = FleetEventService::new(2);
        for i in 0..4 {
            service.publish(observation("status", &format!("s_{i}")));
        }
        // Ring holds seqs 3..=4. Cursor 3 → only seq 4, no gap.
        let tail = service.replay(3);
        assert_eq!(
            tail.events.iter().map(|e| e.seq).collect::<Vec<_>>(),
            vec![4]
        );
        assert_eq!(tail.current_seq, 4);
        assert!(tail.gap.is_none());
        // Cursor 1 predates the ring → gap with first retained seq.
        let stale = service.replay(1);
        assert_eq!(stale.gap.as_ref().map(|g| g.first_retained_seq), Some(3));
        // Fresh subscriber (cursor 0) replays everything retained without a gap.
        let fresh = service.replay(0);
        assert_eq!(fresh.events.len(), 2);
        assert_eq!(fresh.current_seq, 4);
        assert!(fresh.gap.is_none());
    }
}
