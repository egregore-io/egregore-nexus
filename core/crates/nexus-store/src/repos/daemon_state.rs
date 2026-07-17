//! Small durable daemon state values shared across local facets.
//!
//! The daemon records a fresh boot epoch on each process start. Long-lived consumers such as MCP
//! inbox drains and web observers can compare that value with the one they last saw and reconnect
//! after a daemon restart instead of waiting forever on a severed in-process waiter.

use libsql::params;

use nexus_common::NexusError;

use crate::error::store_err;
use crate::repos::sessions::get_text;
use crate::state::Store;

/// Persistence for process-local daemon state that needs a durable restart marker.
pub struct DaemonState<'a> {
    store: &'a Store,
}

impl<'a> DaemonState<'a> {
    /// Bind a repo to the shared store connection.
    pub fn new(store: &'a Store) -> Self {
        DaemonState { store }
    }

    /// Persist the current daemon boot epoch.
    pub async fn set_boot_epoch(&self, epoch: &str, now: i64) -> Result<(), NexusError> {
        self.store
            .identity_conn()
            .execute(
                "INSERT INTO daemon_state (key, value, updated_at) VALUES ('boot_epoch', ?1, ?2) \
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value, \
                 updated_at = excluded.updated_at",
                params![epoch, now],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Read the last recorded daemon boot epoch, if any.
    pub async fn boot_epoch(&self) -> Result<Option<String>, NexusError> {
        let mut rows = self
            .store
            .identity_conn()
            .query(
                "SELECT value FROM daemon_state WHERE key = 'boot_epoch'",
                (),
            )
            .await
            .map_err(store_err)?;
        match rows.next().await.map_err(store_err)? {
            Some(row) => Ok(Some(get_text(&row, 0)?)),
            None => Ok(None),
        }
    }
}
