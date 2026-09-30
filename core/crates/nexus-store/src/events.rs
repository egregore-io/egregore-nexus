//! In-process store write topics for daemon-local wakeups.
//!
//! These topics are a push hint, not durable state. They remove polling floors for writers that go
//! through this `Store` handle; direct cross-process store writers still rely on their fallback
//! heartbeat until their ingress is routed through the daemon.

use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::Notify;

/// Fixed daemon-local topics fired by store write sites.
pub struct StoreEventBus {
    command_intent_inserted: StoreEventTopic,
    command_intent_completed: StoreEventTopic,
    inbox_enqueued: StoreEventTopic,
    developer_event_appended: StoreEventTopic,
    stream_row_appended: StoreEventTopic,
    turn_end_appended: StoreEventTopic,
    session_lifecycle_changed: StoreEventTopic,
}

impl StoreEventBus {
    /// Create an empty event bus.
    pub fn new() -> Self {
        StoreEventBus {
            command_intent_inserted: StoreEventTopic::new(),
            command_intent_completed: StoreEventTopic::new(),
            inbox_enqueued: StoreEventTopic::new(),
            developer_event_appended: StoreEventTopic::new(),
            stream_row_appended: StoreEventTopic::new(),
            turn_end_appended: StoreEventTopic::new(),
            session_lifecycle_changed: StoreEventTopic::new(),
        }
    }

    /// Rows inserted into `command_intents`.
    pub fn command_intent_inserted(&self) -> &StoreEventTopic {
        &self.command_intent_inserted
    }

    /// Rows that reached `done`, `error`, or `cancelled` and can complete a held daemon IPC call.
    pub fn command_intent_completed(&self) -> &StoreEventTopic {
        &self.command_intent_completed
    }

    /// Rows inserted into `in_flight`.
    pub fn inbox_enqueued(&self) -> &StoreEventTopic {
        &self.inbox_enqueued
    }

    /// Rows inserted into `developer_events`.
    pub fn developer_event_appended(&self) -> &StoreEventTopic {
        &self.developer_event_appended
    }

    /// Rows inserted into `mem.stream_events`.
    pub fn stream_row_appended(&self) -> &StoreEventTopic {
        &self.stream_row_appended
    }

    /// `turn_end` rows inserted into `mem.stream_events`.
    pub fn turn_end_appended(&self) -> &StoreEventTopic {
        &self.turn_end_appended
    }

    /// Session/runtime lifecycle rows changed.
    pub fn session_lifecycle_changed(&self) -> &StoreEventTopic {
        &self.session_lifecycle_changed
    }
}

/// One latched in-process topic.
pub struct StoreEventTopic {
    epoch: AtomicU64,
    notify: Notify,
}

impl StoreEventTopic {
    fn new() -> Self {
        StoreEventTopic {
            epoch: AtomicU64::new(0),
            notify: Notify::new(),
        }
    }

    /// Current topic generation.
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::SeqCst)
    }

    /// Signal this topic.
    pub fn signal(&self) {
        self.epoch.fetch_add(1, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    /// Wait until this topic observes a signal after `epoch`.
    pub async fn wait_after(&self, epoch: u64) {
        loop {
            let notified = self.notify.notified();
            if self.epoch() != epoch {
                return;
            }
            notified.await;
        }
    }
}
