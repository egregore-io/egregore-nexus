//! Per-runtime serialization for harness resurrection.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use nexus_contracts::SessionId;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

/// Serializes resurrection work for one durable runtime without blocking unrelated runtimes.
///
/// Boot adoption and delivery recovery can discover the same dead harness concurrently. Both must
/// share one resurrection attempt: structured runtimes replace their loopback bridge and terminal
/// viewer as a pair, so overlapping launches can kill or bind each other's replacement process.
#[derive(Clone, Default)]
pub(crate) struct RuntimeReviveGate {
    slots: Arc<Mutex<HashMap<SessionId, Weak<AsyncMutex<()>>>>>,
}

impl RuntimeReviveGate {
    pub(crate) async fn acquire(&self, session: &SessionId) -> OwnedMutexGuard<()> {
        let slot = {
            let mut slots = self.slots.lock().unwrap();
            slots.retain(|_, slot| slot.strong_count() > 0);
            match slots.get(session).and_then(Weak::upgrade) {
                Some(slot) => slot,
                None => {
                    let slot = Arc::new(AsyncMutex::new(()));
                    slots.insert(session.clone(), Arc::downgrade(&slot));
                    slot
                }
            }
        };
        slot.lock_owned().await
    }
}

#[cfg(test)]
#[path = "../../tests/unit/runtime_revive_gate.rs"]
mod tests;
