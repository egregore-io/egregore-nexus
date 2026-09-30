//! The `notifications` repo — the push-in routing audit (who-gets-what, resolved at ingest).
//! Bad-signature notifications are still recorded (`hmac_ok = 0`) for the web console audit.

use libsql::params;

use nexus_common::{new_message_id, now, NexusError};

use crate::error::store_err;
use crate::repos::sessions::{get_opt_int, get_text};
use crate::state::Store;

/// Persistence for the `notifications` audit table.
pub struct Notifications<'a> {
    store: &'a Store,
}

/// A `notifications` row (audit record).
#[derive(Debug, Clone, PartialEq)]
pub struct NotificationRow {
    pub notif_id: String,
    pub source: Option<String>,
    pub topic: Option<String>,
    pub hmac_ok: bool,
    pub payload: Option<String>,
    pub routed_to: Option<String>,
    pub created_at: i64,
}

impl<'a> Notifications<'a> {
    /// Bind a repo to the shared store connection.
    pub fn new(store: &'a Store) -> Self {
        Notifications { store }
    }

    /// Record an ingested notification (verified or not) and return its generated id. `routed_to`
    /// is the resolved-recipient csv computed at ingest (empty for a dropped bad-signature one).
    pub async fn record(
        &self,
        source: Option<&str>,
        topic: Option<&str>,
        hmac_ok: bool,
        payload: &str,
        routed_to: &str,
    ) -> Result<String, NexusError> {
        let notif_id = format!("n_{}", new_message_id().0);
        self.store
            .conn
            .execute(
                "INSERT INTO notifications \
                 (notif_id, source, topic, hmac_ok, payload, routed_to, created_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    notif_id.clone(),
                    source,
                    topic,
                    hmac_ok as i64,
                    payload,
                    routed_to,
                    now()
                ],
            )
            .await
            .map_err(store_err)?;
        Ok(notif_id)
    }

    /// Record one verified logical ingest under a deterministic notification id.
    ///
    /// Returns the canonical row and whether this call inserted it. A command reclaim therefore
    /// observes the same `notif_id`/audit bytes and does not emit a second accepted audit event.
    pub async fn record_once(
        &self,
        notif_id: &str,
        source: Option<&str>,
        topic: Option<&str>,
        hmac_ok: bool,
        payload: &str,
        routed_to: &str,
    ) -> Result<(NotificationRow, bool), NexusError> {
        let changed = self
            .store
            .conn
            .execute(
                "INSERT OR IGNORE INTO notifications \
                 (notif_id, source, topic, hmac_ok, payload, routed_to, created_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    notif_id,
                    source,
                    topic,
                    hmac_ok as i64,
                    payload,
                    routed_to,
                    now()
                ],
            )
            .await
            .map_err(store_err)?;
        let row = self
            .get(notif_id)
            .await?
            .ok_or_else(|| NexusError::Store("idempotent notification audit disappeared".into()))?;
        Ok((row, changed > 0))
    }

    /// Fetch a recorded notification by id (audit read).
    pub async fn get(&self, notif_id: &str) -> Result<Option<NotificationRow>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT notif_id, source, topic, hmac_ok, payload, routed_to, created_at \
                 FROM notifications WHERE notif_id = ?1",
                params![notif_id],
            )
            .await
            .map_err(store_err)?;
        match rows.next().await.map_err(store_err)? {
            Some(row) => Ok(Some(NotificationRow {
                notif_id: get_text(&row, 0)?,
                source: crate::repos::sessions::get_opt_text(&row, 1)?,
                topic: crate::repos::sessions::get_opt_text(&row, 2)?,
                hmac_ok: get_opt_int(&row, 3)?.unwrap_or(0) != 0,
                payload: crate::repos::sessions::get_opt_text(&row, 4)?,
                routed_to: crate::repos::sessions::get_opt_text(&row, 5)?,
                created_at: get_opt_int(&row, 6)?.unwrap_or(0),
            })),
            None => Ok(None),
        }
    }
}
