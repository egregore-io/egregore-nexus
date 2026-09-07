//! Bounded, RAM-only daemon-to-Gateway projection delivery within one boot epoch.
//!
//! This module never opens a store or file. Core transport continues if Gateway is absent or slow;
//! buffered mode retains only a bounded observation backlog, while best-effort attempts live
//! delivery once. Canonical product durability belongs to Gateway.

use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use nexus_common::{GatewayProjectionBacklogConfig, GatewayProjectionDeliveryMode};
use nexus_contracts::{
    GatewayProjectionAck, GatewayProjectionEvent, GatewayProjectionKind, GATEWAY_PROJECTION_VERSION,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq)]
pub enum AppendProjection {
    Buffered(GatewayProjectionEvent),
    LiveOnly(GatewayProjectionEvent),
    Duplicate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GatewayProjectionGap {
    pub daemon_epoch: String,
    pub after_seq: i64,
    pub through_seq: i64,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GatewayProjectionReplay {
    pub daemon_epoch: String,
    pub gap: Option<GatewayProjectionGap>,
    pub events: Vec<GatewayProjectionEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayProjectionBacklogStats {
    pub delivery_mode: GatewayProjectionDeliveryMode,
    pub daemon_epoch: String,
    pub events: usize,
    pub bytes: usize,
    pub gaps: u64,
    pub dropped: u64,
    pub coalesced: u64,
    pub acked_through: i64,
    pub next_seq: i64,
    pub resync_required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayProjectionModeTransition {
    pub previous_mode: GatewayProjectionDeliveryMode,
    pub delivery_mode: GatewayProjectionDeliveryMode,
    pub previous_epoch: String,
    pub daemon_epoch: String,
    pub cleared_events: usize,
    pub resync_required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum GatewayProjectionBacklogError {
    #[error("projection ACK is not used in best-effort mode")]
    AckNotRequired,
    #[error("projection ACK epoch {received:?} does not match {expected:?}")]
    EpochMismatch { expected: String, received: String },
    #[error("projection ACK cannot advance beyond a gap before that gap is observed")]
    AckBeyondUnseenGap,
    #[error("projection ACK advances beyond the last sent position {sent_through}")]
    AckBeyondSent { sent_through: i64 },
}

#[derive(Clone)]
pub struct GatewayProjectionBacklog {
    inner: Arc<Mutex<State>>,
}

struct PendingGap {
    gap: GatewayProjectionGap,
    sent: bool,
}

struct State {
    config: GatewayProjectionBacklogConfig,
    daemon_epoch: String,
    next_seq: i64,
    acked_through: i64,
    sent_through: i64,
    events: VecDeque<GatewayProjectionEvent>,
    seen: HashSet<String>,
    bytes: usize,
    pending_gap: Option<PendingGap>,
    gap_count: u64,
    dropped: u64,
    coalesced: u64,
    resync_required: bool,
}

impl GatewayProjectionBacklog {
    pub fn new(config: GatewayProjectionBacklogConfig) -> Self {
        Self::with_epoch(config, fresh_epoch())
    }

    pub fn with_epoch(
        config: GatewayProjectionBacklogConfig,
        daemon_epoch: impl Into<String>,
    ) -> Self {
        let mut config = config;
        config.batch_events = config.batch_events.max(1);
        Self {
            inner: Arc::new(Mutex::new(State {
                config,
                daemon_epoch: daemon_epoch.into(),
                next_seq: 0,
                acked_through: 0,
                sent_through: 0,
                events: VecDeque::new(),
                seen: HashSet::new(),
                bytes: 0,
                pending_gap: None,
                gap_count: 0,
                dropped: 0,
                coalesced: 0,
                resync_required: false,
            })),
        }
    }

    pub fn append(
        &self,
        event_id: impl Into<String>,
        kind: GatewayProjectionKind,
        occurred_at: i64,
        payload: Value,
    ) -> AppendProjection {
        let event_id = event_id.into();
        let mut state = self
            .inner
            .lock()
            .expect("Gateway projection backlog poisoned");
        if state.seen.contains(&event_id) {
            return AppendProjection::Duplicate;
        }
        state.next_seq += 1;
        let event = GatewayProjectionEvent {
            event_id: event_id.clone(),
            daemon_epoch: state.daemon_epoch.clone(),
            seq: state.next_seq,
            occurred_at,
            kind,
            version: GATEWAY_PROJECTION_VERSION,
            payload,
        };
        state.seen.insert(event_id);

        if state.config.delivery_mode == GatewayProjectionDeliveryMode::BestEffort {
            return AppendProjection::LiveOnly(event);
        }

        // Sequence numbers are immutable once assigned. Removing a superseded snapshot here
        // would leave a silent hole: Gateway cannot commit or ACK the later sequence, and the
        // outstanding replay window would never advance. Retain every unacknowledged event;
        // only bounded overflow may evict it, with an explicit loss boundary.
        state.bytes += event_size(&event);
        state.events.push_back(event.clone());
        enforce_limits(&mut state);
        AppendProjection::Buffered(event)
    }

    /// Return at most one configured batch and mark its cursor/gap as observed by the connection.
    pub fn replay_batch(&self) -> GatewayProjectionReplay {
        let mut state = self
            .inner
            .lock()
            .expect("Gateway projection backlog poisoned");
        if state.config.delivery_mode == GatewayProjectionDeliveryMode::BestEffort {
            return GatewayProjectionReplay {
                daemon_epoch: state.daemon_epoch.clone(),
                gap: None,
                events: Vec::new(),
            };
        }
        let gap = state.pending_gap.as_mut().map(|pending| {
            pending.sent = true;
            pending.gap.clone()
        });
        let events = state
            .events
            .iter()
            .take(state.config.batch_events)
            .cloned()
            .collect::<Vec<_>>();
        let sent_through = events
            .last()
            .map(|event| event.seq)
            .or_else(|| gap.as_ref().map(|gap| gap.through_seq))
            .unwrap_or(state.acked_through);
        state.sent_through = state.sent_through.max(sent_through);
        GatewayProjectionReplay {
            daemon_epoch: state.daemon_epoch.clone(),
            gap,
            events,
        }
    }

    pub fn ack(&self, ack: &GatewayProjectionAck) -> Result<usize, GatewayProjectionBacklogError> {
        let mut state = self
            .inner
            .lock()
            .expect("Gateway projection backlog poisoned");
        if state.config.delivery_mode == GatewayProjectionDeliveryMode::BestEffort {
            return Err(GatewayProjectionBacklogError::AckNotRequired);
        }
        if ack.daemon_epoch != state.daemon_epoch {
            return Err(GatewayProjectionBacklogError::EpochMismatch {
                expected: state.daemon_epoch.clone(),
                received: ack.daemon_epoch.clone(),
            });
        }
        if ack.through_seq <= state.acked_through {
            return Ok(0);
        }
        if let Some(pending) = &state.pending_gap {
            if !pending.sent && ack.through_seq > pending.gap.after_seq {
                return Err(GatewayProjectionBacklogError::AckBeyondUnseenGap);
            }
        }
        if ack.through_seq > state.sent_through {
            return Err(GatewayProjectionBacklogError::AckBeyondSent {
                sent_through: state.sent_through,
            });
        }

        let mut trimmed = 0;
        while state
            .events
            .front()
            .is_some_and(|event| event.seq <= ack.through_seq)
        {
            if let Some(event) = state.events.pop_front() {
                state.bytes = state.bytes.saturating_sub(event_size(&event));
                trimmed += 1;
            }
        }
        if state
            .pending_gap
            .as_ref()
            .is_some_and(|pending| pending.sent && pending.gap.through_seq <= ack.through_seq)
        {
            state.pending_gap = None;
        }
        state.acked_through = ack.through_seq;
        Ok(trimmed)
    }

    pub fn set_delivery_mode(
        &self,
        delivery_mode: GatewayProjectionDeliveryMode,
    ) -> GatewayProjectionModeTransition {
        let mut state = self
            .inner
            .lock()
            .expect("Gateway projection backlog poisoned");
        let previous_mode = state.config.delivery_mode;
        let previous_epoch = state.daemon_epoch.clone();
        if previous_mode == delivery_mode {
            return GatewayProjectionModeTransition {
                previous_mode,
                delivery_mode,
                previous_epoch: previous_epoch.clone(),
                daemon_epoch: previous_epoch,
                cleared_events: 0,
                resync_required: state.resync_required,
            };
        }

        let cleared_events = state.events.len();
        if cleared_events > 0 {
            state.gap_count += 1;
            state.dropped += cleared_events as u64;
        }
        state.events.clear();
        state.seen.clear();
        state.bytes = 0;
        state.pending_gap = None;
        state.config.delivery_mode = delivery_mode;
        state.daemon_epoch = fresh_epoch();
        state.next_seq = 0;
        state.acked_through = 0;
        state.sent_through = 0;
        state.resync_required = delivery_mode == GatewayProjectionDeliveryMode::Buffered;
        GatewayProjectionModeTransition {
            previous_mode,
            delivery_mode,
            previous_epoch,
            daemon_epoch: state.daemon_epoch.clone(),
            cleared_events,
            resync_required: state.resync_required,
        }
    }

    pub fn take_resync_required(&self) -> bool {
        let mut state = self
            .inner
            .lock()
            .expect("Gateway projection backlog poisoned");
        std::mem::take(&mut state.resync_required)
    }

    /// Account for a best-effort event that had no connected Gateway receiver.
    pub fn note_live_drop(&self) {
        let mut state = self
            .inner
            .lock()
            .expect("Gateway projection backlog poisoned");
        state.dropped += 1;
    }

    pub fn stats(&self) -> GatewayProjectionBacklogStats {
        let state = self
            .inner
            .lock()
            .expect("Gateway projection backlog poisoned");
        GatewayProjectionBacklogStats {
            delivery_mode: state.config.delivery_mode,
            daemon_epoch: state.daemon_epoch.clone(),
            events: state.events.len(),
            bytes: state.bytes,
            gaps: state.gap_count,
            dropped: state.dropped,
            coalesced: state.coalesced,
            acked_through: state.acked_through,
            next_seq: state.next_seq,
            resync_required: state.resync_required,
        }
    }
}

fn enforce_limits(state: &mut State) {
    while state.events.len() > state.config.max_events || state.bytes > state.config.max_bytes {
        let Some(evicted) = state.events.pop_front() else {
            break;
        };
        state.bytes = state.bytes.saturating_sub(event_size(&evicted));
        state.dropped += 1;
        record_gap(state, evicted.seq, "backlog_overflow");
    }
}

fn record_gap(state: &mut State, through_seq: i64, reason: &str) {
    match &mut state.pending_gap {
        Some(pending) if pending.gap.daemon_epoch == state.daemon_epoch => {
            pending.gap.through_seq = pending.gap.through_seq.max(through_seq);
            pending.sent = false;
        }
        _ => {
            state.gap_count += 1;
            state.pending_gap = Some(PendingGap {
                gap: GatewayProjectionGap {
                    daemon_epoch: state.daemon_epoch.clone(),
                    after_seq: state.acked_through,
                    through_seq,
                    reason: reason.to_string(),
                },
                sent: false,
            });
        }
    }
}

fn event_size(event: &GatewayProjectionEvent) -> usize {
    serde_json::to_vec(event).map_or(0, |wire| wire.len())
}

fn fresh_epoch() -> String {
    format!("gateway_{}", Uuid::new_v4().simple())
}
