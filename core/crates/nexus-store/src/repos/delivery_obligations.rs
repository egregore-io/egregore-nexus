use libsql::params;

use nexus_common::NexusError;
use nexus_contracts::MessageId;

use crate::error::store_err;
use crate::Store;

// The native transactional-batch API has no bindings. Only encoded JSON data
// enters SQL text; cap its expansion before constructing the hexadecimal literal.
fn json_sql_literal(value: &serde_json::Value) -> Result<String, NexusError> {
    use std::fmt::Write;
    let json = serde_json::to_vec(value).map_err(|error| NexusError::Store(error.to_string()))?;
    if json.len() > 64 * 1024 * 1024 {
        return Err(NexusError::Store(
            "continuity batch exceeds 64 MiB JSON limit".into(),
        ));
    }
    let mut literal = String::with_capacity(json.len() * 2 + 17);
    literal.push_str("CAST(X'");
    for byte in json {
        write!(literal, "{byte:02x}").expect("writing to String");
    }
    literal.push_str("' AS TEXT)");
    Ok(literal)
}

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
        self.restore_requeued(&[obligation]).await
    }

    /// Commit continuity before the caller acquires or commits transport state.
    /// Every writer must own the identity gate, including ordinary admission:
    /// otherwise it could accidentally participate in another owner's rollback.
    pub(crate) async fn restore_requeued(
        &self,
        obligations: &[NewDeliveryObligation],
    ) -> Result<(), NexusError> {
        if obligations.is_empty() {
            return Ok(());
        }
        let data = obligations
            .iter()
            .map(|obligation| {
                serde_json::json!([
                    obligation.message_id,
                    obligation.recipient_agent_id,
                    obligation.recipient_runtime_id,
                    obligation.payload_json,
                    obligation.dedupe_key,
                    obligation.attempt,
                    obligation.state,
                    obligation.created_at
                ])
            })
            .collect::<Vec<_>>();
        let data = json_sql_literal(&serde_json::Value::Array(data))?;
        self.store.execute_identity_write_batch("delivery_obligation_upsert", &format!(
                    "INSERT INTO delivery_obligations
                 (message_id, recipient_agent_id, recipient_runtime_id, payload_json,
                  dedupe_key, attempt, state, created_at, updated_at)
                 SELECT json_extract(value, '$[0]'), json_extract(value, '$[1]'),
                        json_extract(value, '$[2]'), json_extract(value, '$[3]'),
                        json_extract(value, '$[4]'), json_extract(value, '$[5]'),
                        json_extract(value, '$[6]'), json_extract(value, '$[7]'), json_extract(value, '$[7]')
                 FROM json_each({data}) WHERE true
                 ON CONFLICT(message_id, recipient_agent_id) DO UPDATE SET
                   recipient_runtime_id = excluded.recipient_runtime_id,
                   payload_json = excluded.payload_json,
                   dedupe_key = excluded.dedupe_key,
                   attempt = excluded.attempt,
                   state = excluded.state,
                   updated_at = excluded.updated_at"
        )).await
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
        self.store
            .execute_identity_parameterized_write(
                "delivery_obligation_remove",
                "DELETE FROM delivery_obligations
                 WHERE message_id = ?1 AND recipient_agent_id = ?2",
                params![message_id, recipient_agent_id],
            )
            .await
            .map(|changed| changed > 0)
    }

    pub async fn remove_for_runtime(
        &self,
        message_id: &str,
        recipient_runtime_id: &str,
    ) -> Result<u64, NexusError> {
        self.store
            .execute_identity_parameterized_write(
                "delivery_obligation_remove_runtime",
                "DELETE FROM delivery_obligations
                 WHERE message_id = ?1 AND (
                   recipient_runtime_id = ?2 OR recipient_agent_id = (
                     SELECT agent_id FROM agent_runtimes WHERE runtime_id = ?2 LIMIT 1
                   )
                 )",
                params![message_id, recipient_runtime_id],
            )
            .await
    }

    /// Durably settle one TTL batch before its volatile transaction commits.
    ///
    /// Each tuple is (message id, stable recipient agent id, runtime fallback).
    /// Stable identity wins when present; a reused runtime must not retire a
    /// different agent's obligation. Own the identity write gate and commit:
    /// executing on its shared connection alone could join another owner's
    /// transaction and lose the settlement on their rollback. The caller holds
    /// the distinct split-store transport gate; identity-only owners must finish
    /// before acquiring transport. An owned rollback also undoes earlier deletes
    /// if a later recipient raises SQLite FAIL instead of statement-wide ABORT.
    pub(crate) async fn settle_expired_recipients(
        &self,
        recipients: &[(String, Option<String>, Option<String>)],
    ) -> Result<(), NexusError> {
        if recipients.is_empty() {
            return Ok(());
        }
        let recipients_json = json_sql_literal(&serde_json::json!(recipients))?;
        self.store
            .execute_identity_write_batch(
                "inbox_expiry_settlement",
                &format!(
                    "DELETE FROM delivery_obligations WHERE EXISTS (
                   SELECT 1 FROM json_each({recipients_json}) AS recipient
                   WHERE message_id = json_extract(recipient.value, '$[0]') AND (
                     (json_extract(recipient.value, '$[1]') IS NOT NULL AND
                      recipient_agent_id = json_extract(recipient.value, '$[1]')) OR
                     (json_extract(recipient.value, '$[1]') IS NULL AND
                      recipient_runtime_id = json_extract(recipient.value, '$[2]'))
                   )
                 )"
                ),
            )
            .await
    }

    /// Durably settle every rendered message in one pull ACK before volatile transport commits.
    ///
    /// The split daemon cannot atomically commit its file-backed continuity store and boot-scoped
    /// transport database. Pull ACK therefore uses the durable side as the acceptance boundary:
    /// after the pending batch has been validated, one gated statement removes the exact
    /// recipient obligations, then the caller commits its volatile batch/in-flight
    /// rows. A crash anywhere after this durable commit cannot resurrect already-rendered messages on
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
            .execute_identity_parameterized_write(
                "delivery_obligation_pull_settlement",
                "DELETE FROM delivery_obligations
                 WHERE message_id IN (SELECT value FROM json_each(?1)) AND (
                   recipient_runtime_id = ?2 OR recipient_agent_id = (
                     SELECT agent_id FROM agent_runtimes WHERE runtime_id = ?2 LIMIT 1
                   )
                 )",
                params![ids_json, recipient_runtime_id],
            )
            .await
    }
}
