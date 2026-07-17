//! The wake bell — a per-session [`tokio::sync::Notify`] (spec §2.2). The bus rings a recipient's
//! bell after committing its in-flight row; the recipient's event loop parks on the same bell and
//! wakes to drain. `Notify` stores **one** permit, so a `ring` that precedes a `wait` is not lost
//! (the next `wait` returns immediately) — this is what makes the crash-safe re-ring on attach work
//! (spec §2.1: a missed bell is re-driven because the row is still `pending`).
//!
//! Bells are created lazily, one per [`SessionId`], behind a `std::sync::Mutex<HashMap>`. We use the
//! std mutex (never held across an `.await`) rather than a third-party concurrent map to keep the
//! dependency surface minimal.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use nexus_contracts::ids::SessionId;
use tokio::sync::Notify;

/// A lazily-populated map of per-session wake [`Notify`]s. Cheap to [`clone`](Bell::clone): the
/// inner map is shared behind an `Arc`, so the bus and every event loop hold the same bells.
#[derive(Clone, Default)]
pub struct Bell {
    bells: Arc<Mutex<HashMap<SessionId, Arc<Notify>>>>,
}

impl Bell {
    /// Create an empty bell registry.
    pub fn new() -> Self {
        Bell {
            bells: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Get (creating if absent) the `Arc<Notify>` for one session.
    fn handle(&self, session: &SessionId) -> Arc<Notify> {
        let mut map = self.bells.lock().expect("bell map poisoned");
        map.entry(session.clone())
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone()
    }

    /// Ring a session's bell — wakes one waiter, or stores a permit for the next [`wait`](Bell::wait)
    /// if none is parked yet (so a ring-before-wait is never lost).
    pub fn ring(&self, session: &SessionId) {
        self.handle(session).notify_one();
    }

    /// Park until this session's bell is rung. Returns as soon as a permit is available (a prior
    /// `ring` makes this return immediately).
    pub async fn wait(&self, session: &SessionId) {
        // Clone the Arc out before awaiting so the map lock is never held across `.await`.
        let notify = self.handle(session);
        notify.notified().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;

    #[tokio::test]
    async fn ring_before_wait_does_not_deadlock() {
        let bell = Bell::new();
        let s = SessionId("s_ana".into());
        // Ring first, then wait: the stored permit must satisfy the wait immediately.
        bell.ring(&s);
        let r = timeout(Duration::from_millis(200), bell.wait(&s)).await;
        assert!(r.is_ok(), "ring-before-wait must not deadlock");
    }

    #[tokio::test]
    async fn wait_wakes_on_ring() {
        let bell = Bell::new();
        let s = SessionId("s_ben".into());
        let waiter = {
            let bell = bell.clone();
            let s = s.clone();
            tokio::spawn(async move { bell.wait(&s).await })
        };
        // Give the waiter a moment to park, then ring.
        tokio::task::yield_now().await;
        bell.ring(&s);
        let r = timeout(Duration::from_millis(200), waiter).await;
        assert!(r.is_ok(), "wait must wake on a subsequent ring");
    }

    #[tokio::test]
    async fn bells_are_per_session() {
        let bell = Bell::new();
        let a = SessionId("s_a".into());
        let b = SessionId("s_b".into());
        // Ringing a does NOT satisfy a wait on b.
        bell.ring(&a);
        let r = timeout(Duration::from_millis(100), bell.wait(&b)).await;
        assert!(r.is_err(), "a ring on one session must not wake another");
    }
}
