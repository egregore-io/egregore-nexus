use libsql::params;

use nexus_common::NexusError;
use nexus_contracts::MessageId;

use crate::error::store_err;
use crate::Store;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewDeliveryObligation {
    pub message_id: String,
    pub recipient_agent_id: String,
    pub recipient_runtime_id: Option<String>,
    pub payload_json: String,
    pub dedupe_key: String,
    pub attempt: i64,
    pub state: String,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryObligationRow {
    pub message_id: String,
    pub recipient_agent_id: String,
    pub recipient_runtime_id: Option<String>,
    pub payload_json: String,
    pub dedupe_key: String,
    pub attempt: i64,
    pub state: String,
    pub created_at: i64,
    pub updated_at: i64,
}

pub struct DeliveryObligations<'a> {
    store: &'a Store,
}

impl<'a> DeliveryObligations<'a> {
    pub fn new(store: &'a Store) -> Self {
        Self { store }
    }

    pub async fn insert(&self, obligation: NewDeliveryObligation) -> Result<(), NexusError> {
        self.store
            .identity_conn()
            .execute(
                "INSERT INTO delivery_obligations
                 (message_id, recipient_agent_id, recipient_runtime_id, payload_json,
                  dedupe_key, attempt, state, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8)
                 ON CONFLICT(message_id, recipient_agent_id) DO UPDATE SET
                   recipient_runtime_id = excluded.recipient_runtime_id,
                   payload_json = excluded.payload_json,
                   dedupe_key = excluded.dedupe_key,
                   attempt = excluded.attempt,
                   state = excluded.state,
                   updated_at = excluded.updated_at",
                params![
                    obligation.message_id,
                    obligation.recipient_agent_id,
                    obligation.recipient_runtime_id,
                    obligation.payload_json,
                    obligation.dedupe_key,
                    obligation.attempt,
                    obligation.state,
                    obligation.created_at
                ],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    pub async fn pending(&self) -> Result<Vec<DeliveryObligationRow>, NexusError> {
        let mut rows = self
            .store
            .identity_conn()
            .query(
                "SELECT message_id, recipient_agent_id, recipient_runtime_id, payload_json,
                        dedupe_key, attempt, state, created_at, updated_at
                 FROM delivery_obligations
                 WHERE state NOT IN ('delivered', 'rejected')
                 ORDER BY created_at, message_id, recipient_agent_id",
                (),
            )
            .await
            .map_err(store_err)?;
        let mut obligations = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            obligations.push(DeliveryObligationRow {
                message_id: row.get(0).map_err(store_err)?,
                recipient_agent_id: row.get(1).map_err(store_err)?,
                recipient_runtime_id: row.get(2).map_err(store_err)?,
                payload_json: row.get(3).map_err(store_err)?,
                dedupe_key: row.get(4).map_err(store_err)?,
                attempt: row.get(5).map_err(store_err)?,
                state: row.get(6).map_err(store_err)?,
                created_at: row.get(7).map_err(store_err)?,
                updated_at: row.get(8).map_err(store_err)?,
            });
        }
        Ok(obligations)
    }

    pub async fn remove(
        &self,
        message_id: &str,
        recipient_agent_id: &str,
    ) -> Result<bool, NexusError> {
        let changed = self
            .store
            .identity_conn()
            .execute(
                "DELETE FROM delivery_obligations
                 WHERE message_id = ?1 AND recipient_agent_id = ?2",
                params![message_id, recipient_agent_id],
            )
            .await
            .map_err(store_err)?;
        Ok(changed > 0)
    }

    pub async fn remove_for_runtime(
        &self,
        message_id: &str,
        recipient_runtime_id: &str,
    ) -> Result<u64, NexusError> {
        self.store
            .identity_conn()
            .execute(
                "DELETE FROM delivery_obligations
                 WHERE message_id = ?1 AND (
                   recipient_runtime_id = ?2 OR recipient_agent_id = (
                     SELECT agent_id FROM agent_runtimes WHERE runtime_id = ?2 LIMIT 1
                   )
                 )",
                params![message_id, recipient_runtime_id],
            )
            .await
            .map_err(store_err)
    }

    /// Durably settle every rendered message in one pull ACK before volatile transport commits.
    ///
    /// The split daemon cannot atomically commit its file-backed continuity store and boot-scoped
    /// transport database. Pull ACK therefore uses the durable side as the acceptance boundary:
    /// after the pending batch has been validated, this one SQLite statement removes the exact
    /// recipient obligations atomically, then the caller commits its volatile batch/in-flight
    /// rows. A crash anywhere after this statement cannot resurrect already-rendered messages on
    /// the next boot; a lost volatile commit remains safely retryable against the pending batch.
    pub async fn settle_pull_batch_for_runtime(
        &self,
        message_ids: &[MessageId],
        recipient_runtime_id: &str,
    ) -> Result<u64, NexusError> {
        if message_ids.is_empty() {
            return Ok(0);
        }
        let ids_json = serde_json::to_string(
            &message_ids
                .iter()
                .map(|message_id| message_id.0.as_str())
                .collect::<Vec<_>>(),
        )
        .map_err(|error| NexusError::Store(error.to_string()))?;
        self.store
            .identity_conn()
            .execute(
                "DELETE FROM delivery_obligations
                 WHERE message_id IN (SELECT value FROM json_each(?1)) AND (
                   recipient_runtime_id = ?2 OR recipient_agent_id = (
                     SELECT agent_id FROM agent_runtimes WHERE runtime_id = ?2 LIMIT 1
                   )
                 )",
                params![ids_json, recipient_runtime_id],
            )
            .await
            .map_err(store_err)
    }
}
