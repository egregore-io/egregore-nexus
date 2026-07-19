use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use nexus_contracts::SessionId;
use tokio::sync::watch;

/// Cancellation-safe accounting for harness turns whose protocol completion has not arrived yet.
#[derive(Clone)]
pub struct ActiveTurnTracker {
    inner: Arc<TrackerInner>,
}

struct TrackerInner {
    counts: Mutex<HashMap<SessionId, usize>>,
    generation: watch::Sender<u64>,
}

impl Default for ActiveTurnTracker {
    fn default() -> Self {
        let (generation, _) = watch::channel(0);
        Self {
            inner: Arc::new(TrackerInner {
                counts: Mutex::new(HashMap::new()),
                generation,
            }),
        }
    }
}

impl ActiveTurnTracker {
    /// Mark `session` active until the returned guard is dropped.
    pub fn begin(&self, session: &SessionId) -> ActiveTurnGuard {
        let mut counts = self.inner.counts.lock().unwrap();
        *counts.entry(session.clone()).or_default() += 1;
        drop(counts);
        self.inner
            .generation
            .send_modify(|generation| *generation = generation.wrapping_add(1));
        ActiveTurnGuard {
            inner: self.inner.clone(),
            session: session.clone(),
        }
    }

    /// Stable snapshot of sessions with at least one unfinished harness turn.
    pub fn sessions(&self) -> Vec<SessionId> {
        let mut sessions: Vec<_> = self.inner.counts.lock().unwrap().keys().cloned().collect();
        sessions.sort_by(|left, right| left.0.cmp(&right.0));
        sessions
    }

    /// Await the final guard for `session`. Returns immediately when no turn is active.
    pub async fn wait_for_completion(&self, session: &SessionId) {
        let mut generation = self.inner.generation.subscribe();
        loop {
            if !self.inner.counts.lock().unwrap().contains_key(session) {
                return;
            }
            if generation.changed().await.is_err() {
                return;
            }
        }
    }
}

/// RAII token that keeps one session in the active-turn set.
pub struct ActiveTurnGuard {
    inner: Arc<TrackerInner>,
    session: SessionId,
}

impl Drop for ActiveTurnGuard {
    fn drop(&mut self) {
        let mut counts = self.inner.counts.lock().unwrap();
        if let Some(count) = counts.get_mut(&self.session) {
            *count -= 1;
            if *count == 0 {
                counts.remove(&self.session);
            }
        }
        drop(counts);
        self.inner
            .generation
            .send_modify(|generation| *generation = generation.wrapping_add(1));
    }
}
