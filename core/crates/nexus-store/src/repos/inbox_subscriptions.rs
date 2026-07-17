//! Durable inbox subscriptions.
//!
//! `inbox.consume` remains the legacy one-shot drain command. This repo backs daemon-owned
//! subscriptions: the daemon drains a caller's `in_flight` rows into durable subscription batches,
//! and clients render/ack those batches without depending on one live long-poll command row.

use libsql::params;

use nexus_common::{new_message_id, now, NexusError};
use nexus_contracts::{NexusBatch, SessionId};

use crate::error::{store_err, store_msg};
use crate::repos::sessions::{get_opt_int, get_opt_text, get_text};
use crate::repos::{AgentRuntimes, DeveloperEvents};
use crate::state::Store;

/// Active daemon-tracked inbox subscription.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboxSubscriptionRow {
    pub subscription_id: String,
    pub project: String,
    pub caller_name: String,
    pub caller_session_id: String,
    pub caller_agent_id: Option<String>,
    pub caller_client_key: Option<String>,
    pub status: String,
    pub timeout_ms: Option<i64>,
    pub max: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
    pub last_drained_at: Option<i64>,
    pub last_error: Option<String>,
}

/// New or refreshed durable subscription.
#[derive(Debug, Clone)]
pub struct NewInboxSubscription {
    pub subscription_id: String,
    pub project: String,
    pub caller_name: String,
    pub caller_session_id: String,
    pub caller_agent_id: Option<String>,
    pub caller_client_key: Option<String>,
    pub timeout_ms: Option<i64>,
    pub max: Option<i64>,
    pub created_at: i64,
}

/// One durable, client-visible batch for a subscription.
#[derive(Debug, Clone, PartialEq)]
pub struct InboxSubscriptionBatchRow {
    pub batch_id: String,
    pub subscription_id: String,
    pub batch_json: String,
    pub message_signature: String,
    pub status: String,
    pub created_at: i64,
    pub consumed_at: Option<i64>,
}

/// Persistence for daemon-tracked inbox subscriptions and durable batches.
pub struct InboxSubscriptions<'a> {
    store: &'a Store,
}

impl<'a> InboxSubscriptions<'a> {
    /// Bind a repo to the shared store connection.
    pub fn new(store: &'a Store) -> Self {
        InboxSubscriptions { store }
    }

    /// Insert or refresh a caller-owned active subscription.
    pub async fn upsert_active(&self, row: NewInboxSubscription) -> Result<(), NexusError> {
        let event = row.clone();
        self.store
            .conn
            .execute(
                "INSERT INTO inbox_subscriptions (subscription_id, project, caller_name, \
                 caller_session_id, caller_agent_id, caller_client_key, status, timeout_ms, max, \
                 created_at, updated_at, last_drained_at, last_error) VALUES \
                 (?1, ?2, ?3, ?4, ?5, ?6, 'active', ?7, ?8, ?9, ?9, NULL, NULL) \
                 ON CONFLICT(subscription_id) DO UPDATE SET \
                   project = excluded.project, caller_name = excluded.caller_name, \
                   caller_session_id = excluded.caller_session_id, \
                   caller_agent_id = excluded.caller_agent_id, \
                   caller_client_key = excluded.caller_client_key, status = 'active', \
                   timeout_ms = excluded.timeout_ms, max = excluded.max, \
                   updated_at = excluded.updated_at, last_error = NULL",
                params![
                    row.subscription_id,
                    row.project,
                    row.caller_name,
                    row.caller_session_id,
                    row.caller_agent_id,
                    row.caller_client_key,
                    row.timeout_ms,
                    row.max,
                    row.created_at
                ],
            )
            .await
            .map_err(store_err)?;
        self.append_subscription_action_best_effort(
            "inbox.subscribe",
            &event.subscription_id,
            &event.caller_name,
            &event.caller_session_id,
            serde_json::json!({
                "subscriptionId": event.subscription_id.clone(),
                "timeoutMs": event.timeout_ms,
                "max": event.max,
            }),
            event.created_at,
        )
        .await;
        Ok(())
    }

    /// Return one subscription by id.
    pub async fn get(
        &self,
        subscription_id: &str,
    ) -> Result<Option<InboxSubscriptionRow>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                &format!("{SELECT_SUBSCRIPTION} WHERE subscription_id = ?1"),
                params![subscription_id],
            )
            .await
            .map_err(store_err)?;
        match rows.next().await.map_err(store_err)? {
            Some(row) => Ok(Some(row_to_subscription(&row)?)),
            None => Ok(None),
        }
    }

    /// Load every active subscription for daemon boot/recovery.
    pub async fn list_active(&self) -> Result<Vec<InboxSubscriptionRow>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                &format!("{SELECT_SUBSCRIPTION} WHERE status = 'active' ORDER BY created_at ASC"),
                (),
            )
            .await
            .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(row_to_subscription(&row)?);
        }
        Ok(out)
    }

    /// Whether `session_id` currently owns delivery through a durable pull subscription.
    ///
    /// This is the single-consumer ownership signal used by daemon push/revive paths: while an
    /// active subscription exists, the subscriber drains and acknowledges its own inbox and a
    /// harness event loop must not race it for the same `in_flight` rows.
    pub async fn has_active_for_session(&self, session_id: &str) -> Result<bool, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT 1 FROM inbox_subscriptions \
                 WHERE caller_session_id = ?1 AND status = 'active' LIMIT 1",
                params![session_id],
            )
            .await
            .map_err(store_err)?;
        Ok(rows.next().await.map_err(store_err)?.is_some())
    }

    /// Mark a subscription inactive.
    pub async fn mark_inactive(
        &self,
        subscription_id: &str,
        updated_at: i64,
    ) -> Result<bool, NexusError> {
        let changed = self
            .store
            .conn
            .execute(
                "UPDATE inbox_subscriptions SET status = 'inactive', updated_at = ?2 \
                 WHERE subscription_id = ?1",
                params![subscription_id, updated_at],
            )
            .await
            .map_err(store_err)?;
        if changed > 0 {
            if let Some(row) = self.get(subscription_id).await? {
                self.append_subscription_action_best_effort(
                    "inbox.unsubscribe",
                    &row.subscription_id,
                    &row.caller_name,
                    &row.caller_session_id,
                    serde_json::json!({
                        "subscriptionId": row.subscription_id.clone(),
                    }),
                    updated_at,
                )
                .await;
            }
        }
        Ok(changed > 0)
    }

    async fn append_subscription_action_best_effort(
        &self,
        action: &str,
        subscription_id: &str,
        caller_name: &str,
        caller_session_id: &str,
        data: serde_json::Value,
        created_at: i64,
    ) {
        if let Err(error) = DeveloperEvents::new(self.store)
            .append_action(
                &format!("sys.inbox.{caller_name}"),
                action,
                Some(caller_name),
                None,
                None,
                Some(caller_session_id),
                data,
                created_at,
            )
            .await
        {
            tracing::warn!(
                target: "nexus_store::inbox_subscriptions",
                action,
                subscription_id,
                error = ?error,
                "failed to append inbox subscription developer event"
            );
        }
    }

    /// Record that the subscription drain ran successfully.
    pub async fn mark_drained(
        &self,
        subscription_id: &str,
        updated_at: i64,
    ) -> Result<(), NexusError> {
        self.store
            .conn
            .execute(
                "UPDATE inbox_subscriptions SET updated_at = ?2, last_drained_at = ?2, \
                 last_error = NULL WHERE subscription_id = ?1",
                params![subscription_id, updated_at],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Persist a drain error without deactivating the subscription.
    pub async fn mark_error(
        &self,
        subscription_id: &str,
        error: &str,
        updated_at: i64,
    ) -> Result<(), NexusError> {
        self.store
            .conn
            .execute(
                "UPDATE inbox_subscriptions SET updated_at = ?2, last_error = ?3 \
                 WHERE subscription_id = ?1",
                params![subscription_id, updated_at, error],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Insert a durable batch if this subscription has no pending copy of the same message set.
    pub async fn insert_pending_batch(
        &self,
        subscription_id: &str,
        batch: &NexusBatch,
        created_at: i64,
    ) -> Result<Option<InboxSubscriptionBatchRow>, NexusError> {
        if batch.counts.total == 0 {
            return Ok(None);
        }
        let batch_id = new_message_id().0;
        let batch_json = serde_json::to_string(batch).map_err(store_msg)?;
        let message_signature = message_signature(batch);
        let inserted = self
            .store
            .conn
            .execute(
                "INSERT OR IGNORE INTO inbox_subscription_batches (batch_id, subscription_id, \
                 batch_json, message_signature, status, created_at, consumed_at) VALUES \
                 (?1, ?2, ?3, ?4, 'pending', ?5, NULL)",
                params![
                    batch_id,
                    subscription_id,
                    batch_json,
                    message_signature,
                    created_at
                ],
            )
            .await
            .map_err(store_err)?;
        if inserted == 0 {
            return Ok(None);
        }
        self.pending_batch(subscription_id).await
    }

    /// Return the oldest pending durable batch for a subscription.
    pub async fn pending_batch(
        &self,
        subscription_id: &str,
    ) -> Result<Option<InboxSubscriptionBatchRow>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                &format!(
                    "{SELECT_BATCH} WHERE subscription_id = ?1 AND status = 'pending' \
                     ORDER BY created_at ASC LIMIT 1"
                ),
                params![subscription_id],
            )
            .await
            .map_err(store_err)?;
        match rows.next().await.map_err(store_err)? {
            Some(row) => Ok(Some(row_to_batch(&row)?)),
            None => Ok(None),
        }
    }

    /// Mark a durable subscription batch consumed after client render and split-ack.
    pub async fn mark_batch_consumed(
        &self,
        subscription_id: &str,
        batch_id: &str,
        consumed_at: i64,
    ) -> Result<bool, NexusError> {
        let changed = self
            .store
            .conn
            .execute(
                "UPDATE inbox_subscription_batches SET status = 'consumed', consumed_at = ?3 \
                 WHERE subscription_id = ?1 AND batch_id = ?2 AND status = 'pending'",
                params![subscription_id, batch_id, consumed_at],
            )
            .await
            .map_err(store_err)?;
        Ok(changed > 0)
    }

    /// Atomically acknowledge a client-rendered pull batch and settle its delivery rows.
    ///
    /// Harness delivery follows `notified -> injecting -> delivered -> acked`, but a durable pull
    /// subscriber is itself the delivery endpoint: its explicit batch acknowledgement is the
    /// terminal receipt. The rows therefore move directly from `pending`/`notified` to `acked`
    /// here. A row already `delivered` (or `acked` by a legacy split-ack) is also accepted, while a
    /// terminal error is never resurrected. The batch and every row commit together so a failed
    /// acknowledgement cannot create a replayed rendered batch or a silently unsettled delivery.
    pub async fn acknowledge_pull_batch(
        &self,
        subscription_id: &str,
        batch_id: &str,
        recipient: &SessionId,
        consumed_at: i64,
    ) -> Result<bool, NexusError> {
        let Some(row) = self.pending_batch_by_id(subscription_id, batch_id).await? else {
            return Ok(false);
        };
        let batch: NexusBatch = serde_json::from_str(&row.batch_json).map_err(store_msg)?;
        let recipient_agent_id = AgentRuntimes::new(self.store)
            .find_by_runtime_id(&recipient.0)
            .await?
            .map(|runtime| runtime.agent_id);
        let txn = self.store.begin_write_txn("inbox_subscription_ack").await?;
        let result = async {
            for message_id in &batch.message_ids {
                let changed = txn
                    .execute(
                        "UPDATE in_flight SET state = 'acked', acked_at = ?3 \
                         WHERE message_id = ?1 AND state IN ('pending','notified','delivered') \
                         AND (recipient_session = ?2 OR \
                           (?4 IS NOT NULL AND recipient_agent_id = ?4))",
                        params![
                            message_id.0.clone(),
                            recipient.0.clone(),
                            consumed_at,
                            recipient_agent_id.clone()
                        ],
                    )
                    .await?;
                if changed == 0
                    && !delivery_is_acked(
                        &txn,
                        &message_id.0,
                        &recipient.0,
                        recipient_agent_id.as_deref(),
                    )
                    .await?
                {
                    return Err(NexusError::Invalid(format!(
                        "pull batch {batch_id} contains an unsettled or terminal delivery {}",
                        message_id.0
                    )));
                }
            }
            let changed = txn
                .execute(
                    "UPDATE inbox_subscription_batches SET status = 'consumed', consumed_at = ?3 \
                     WHERE subscription_id = ?1 AND batch_id = ?2 AND status = 'pending'",
                    params![subscription_id, batch_id, consumed_at],
                )
                .await?;
            if changed != 1 {
                return Err(NexusError::Invalid(format!(
                    "pull batch {batch_id} is no longer pending for {subscription_id}"
                )));
            }
            Ok(())
        }
        .await;
        match result {
            Ok(()) => {
                txn.commit().await?;
                Ok(true)
            }
            Err(error) => {
                txn.rollback(&error).await?;
                Err(error)
            }
        }
    }

    async fn pending_batch_by_id(
        &self,
        subscription_id: &str,
        batch_id: &str,
    ) -> Result<Option<InboxSubscriptionBatchRow>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                &format!(
                    "{SELECT_BATCH} WHERE subscription_id = ?1 AND batch_id = ?2 \
                     AND status = 'pending' LIMIT 1"
                ),
                params![subscription_id, batch_id],
            )
            .await
            .map_err(store_err)?;
        match rows.next().await.map_err(store_err)? {
            Some(row) => Ok(Some(row_to_batch(&row)?)),
            None => Ok(None),
        }
    }
}

async fn delivery_is_acked(
    txn: &crate::state::WriteTxn,
    message_id: &str,
    recipient: &str,
    recipient_agent_id: Option<&str>,
) -> Result<bool, NexusError> {
    let mut rows = txn
        .query(
            "SELECT 1 FROM in_flight WHERE message_id = ?1 AND state = 'acked' \
             AND (recipient_session = ?2 OR \
               (?3 IS NOT NULL AND recipient_agent_id = ?3)) LIMIT 1",
            params![message_id, recipient, recipient_agent_id],
        )
        .await?;
    Ok(rows.next().await.map_err(store_err)?.is_some())
}

fn message_signature(batch: &NexusBatch) -> String {
    batch
        .message_ids
        .iter()
        .map(|id| id.0.as_str())
        .collect::<Vec<_>>()
        .join(",")
}

const SELECT_SUBSCRIPTION: &str = "SELECT subscription_id, project, caller_name, \
 caller_session_id, caller_agent_id, caller_client_key, status, timeout_ms, max, created_at, \
 updated_at, last_drained_at, last_error FROM inbox_subscriptions";

const SELECT_BATCH: &str = "SELECT batch_id, subscription_id, batch_json, message_signature, \
 status, created_at, consumed_at FROM inbox_subscription_batches";

fn row_to_subscription(row: &libsql::Row) -> Result<InboxSubscriptionRow, NexusError> {
    Ok(InboxSubscriptionRow {
        subscription_id: get_text(row, 0)?,
        project: get_text(row, 1)?,
        caller_name: get_text(row, 2)?,
        caller_session_id: get_text(row, 3)?,
        caller_agent_id: get_opt_text(row, 4)?,
        caller_client_key: get_opt_text(row, 5)?,
        status: get_text(row, 6)?,
        timeout_ms: get_opt_int(row, 7)?,
        max: get_opt_int(row, 8)?,
        created_at: get_opt_int(row, 9)?.unwrap_or_default(),
        updated_at: get_opt_int(row, 10)?.unwrap_or_default(),
        last_drained_at: get_opt_int(row, 11)?,
        last_error: get_opt_text(row, 12)?,
    })
}

fn row_to_batch(row: &libsql::Row) -> Result<InboxSubscriptionBatchRow, NexusError> {
    Ok(InboxSubscriptionBatchRow {
        batch_id: get_text(row, 0)?,
        subscription_id: get_text(row, 1)?,
        batch_json: get_text(row, 2)?,
        message_signature: get_text(row, 3)?,
        status: get_text(row, 4)?,
        created_at: get_opt_int(row, 5)?.unwrap_or_default(),
        consumed_at: get_opt_int(row, 6)?,
    })
}

/// Build the deterministic subscription id used by `nexus listen`/MCP for one caller identity.
pub fn caller_subscription_id(project: &str, session_id: &str) -> String {
    format!("sub_inbox_{project}_{session_id}")
}

/// Timestamp helper for callers that do not need a custom clock in tests.
pub fn subscription_now() -> i64 {
    now()
}
