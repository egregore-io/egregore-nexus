//! The `in_flight` repo — the transient per-recipient delivery state machine
//! (`pending → notified → injecting → delivered → acked`, backend §2.1), plus the terminal `error`
//! dead-letter branch. `injecting` is the durable one-attempt claim immediately before crossing the
//! external harness boundary; app-server/ACP acceptance alone does not prove delivery. Terminal
//! errors are never automatically re-drained, and an `injecting` row found after restart becomes
//! `delivery_outcome_unknown` until an operator explicitly requeues it. The durable message copy
//! always stays in `messages`.
//!
//! Rows carry both the concrete runtime session that was rung and, when known, the stable agent id.
//! Drains therefore accept either the exact live session snapshot or the current active runtime for
//! the stable agent. This preserves runtime replacement without hiding freshly addressed DM/thread
//! rows when the compatibility `agent_runtimes` row lags behind the still-live `sessions` row.

use std::collections::HashSet;

use libsql::params;

use nexus_common::{new_message_id, now, NexusError};
use nexus_contracts::ids::{MessageId, SessionId};
use nexus_contracts::message::Message;
use nexus_contracts::{GatewayProjectionEffect, GatewayProjectionKind};

use crate::error::{store_err, store_msg};
use crate::repos::messages::Messages;
use crate::repos::sessions::{get_opt_int, get_opt_text, get_text};
use crate::repos::{AgentRuntimes, Agents, DeliveryObligations, Sessions};
use crate::state::{Store, WriteTxn};

const DEFAULT_DLQ_LIMIT: u32 = 50;
const MAX_DLQ_LIMIT: u32 = 500;
const DLQ_PREVIEW_CHARS: usize = 80;
pub const DELIVERY_TIMEOUT_REASON: &str = "delivery_timeout";
pub const DELIVERY_TIMEOUT_TTL_MS: i64 = 30 * 60 * 1_000;
pub const TARGET_DEAD_ERROR_CODE: &str = "target_dead";
pub const TARGET_UNREACHABLE_ERROR_CODE: &str = "target_unreachable";
pub const PROVIDER_LIMIT_ERROR_CODE: &str = "provider_limit";
pub const OPERATOR_ACTION_ERROR_CODE: &str = "operator_action";
pub const PROVIDER_ERROR_CODE: &str = "provider_error";
pub const CONTRACT_ERROR_CODE: &str = "contract_error";
pub const COMPLETION_TIMEOUT_ERROR_CODE: &str = "completion_timeout";
pub const DELIVERY_OUTCOME_UNKNOWN_ERROR_CODE: &str = "delivery_outcome_unknown";

/// Filters for operator dead-letter scans and batch mutations.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeadLetterFilter {
    pub target: Option<String>,
    pub since: Option<i64>,
    pub limit: u32,
}

/// A single operator-visible dead-letter row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadLetterEntry {
    pub in_flight_id: String,
    pub message_id: MessageId,
    pub sender: String,
    pub recipient_name: Option<String>,
    pub recipient_agent_id: Option<String>,
    pub recipient_session: Option<SessionId>,
    pub created_at: i64,
    pub dead_lettered_at: Option<i64>,
    /// Number of external harness attempts already made for this recipient row.
    pub attempt_count: u32,
    /// Stable machine-readable terminal class such as `provider_error`.
    pub error_code: Option<String>,
    pub error_reason: Option<String>,
    /// Structured terminal evidence retained for explicit operator retry decisions.
    pub error_details: Option<serde_json::Value>,
    pub body_preview: String,
}

/// Dead-letter status summary for daemon status.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeadLetterSummary {
    pub count: u64,
    pub oldest_created_at: Option<i64>,
}

/// Selector for requeue/purge mutations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeadLetterSelector {
    InFlightId(String),
    Filter(DeadLetterFilter),
}

/// Result of a dead-letter mutation.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeadLetterMutation {
    pub count: u64,
    pub in_flight_ids: Vec<String>,
}

/// One unresolved recipient for a committed message. The stable agent id is authoritative when
/// present; the concrete session remains the compatibility/revive lookup for legacy rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageDeliveryTarget {
    pub recipient_session: Option<SessionId>,
    pub recipient_agent_id: Option<String>,
}

/// Persistence for the `in_flight` delivery queue.
pub struct Inbox<'a> {
    store: &'a Store,
}

fn delivery_effect_from_row(row: &libsql::Row) -> Result<GatewayProjectionEffect, NexusError> {
    let in_flight_id = get_text(row, 0)?;
    let message_id = get_text(row, 1)?;
    let state = get_text(row, 4)?;
    let attempt_count = get_opt_int(row, 5)?.unwrap_or(0).max(0);
    let delivered_at = get_opt_int(row, 6)?;
    let acked_at = get_opt_int(row, 7)?;
    let failed_at = get_opt_int(row, 8)?;
    let occurred_at = acked_at.or(delivered_at).or(failed_at).unwrap_or_else(now);
    let error_details = get_opt_text(row, 11)?.map(|raw| {
        serde_json::from_str::<serde_json::Value>(&raw)
            .unwrap_or_else(|_| serde_json::json!({ "unparsed": raw }))
    });
    Ok(GatewayProjectionEffect {
        event_id: format!("delivery:{in_flight_id}:{attempt_count}:{state}"),
        occurred_at,
        kind: GatewayProjectionKind::DeliverySettled,
        payload: serde_json::json!({
            "inFlightId": in_flight_id,
            "messageId": message_id,
            "recipientSessionId": get_opt_text(row, 2)?,
            "recipientAgentId": get_opt_text(row, 3)?,
            "state": state,
            "attempt": attempt_count,
            "deliveredAt": delivered_at,
            "ackedAt": acked_at,
            "failedAt": failed_at,
            "errorCode": get_opt_text(row, 9)?,
            "errorReason": get_opt_text(row, 10)?,
            "errorDetails": error_details,
        }),
    })
}

impl<'a> Inbox<'a> {
    /// Bind a repo to the shared store connection.
    pub fn new(store: &'a Store) -> Self {
        Inbox { store }
    }

    /// Recipients that still need a live drain for one committed message. Terminal and already
    /// injecting rows are excluded so an idempotent command replay cannot create a second attempt.
    pub async fn delivery_targets_for_message(
        &self,
        message: &MessageId,
    ) -> Result<Vec<MessageDeliveryTarget>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT recipient_session, recipient_agent_id FROM in_flight \
                 WHERE message_id = ?1 AND state IN ('pending', 'notified') \
                 ORDER BY in_flight_id",
                params![message.0.clone()],
            )
            .await
            .map_err(store_err)?;
        let mut targets = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            targets.push(MessageDeliveryTarget {
                recipient_session: get_opt_text(&row, 0)?.map(SessionId),
                recipient_agent_id: get_opt_text(&row, 1)?,
            });
        }
        Ok(targets)
    }

    /// Write one `pending` in_flight row for `recipient` for an existing `message`. Idempotent on
    /// `(message_id, recipient)` (the UNIQUE index): a re-enqueue is a no-op. When the runtime is
    /// already bound to a stable agent, the stable recipient column is populated too.
    pub async fn enqueue(
        &self,
        message: &MessageId,
        recipient: &SessionId,
    ) -> Result<(), NexusError> {
        let in_flight_id = new_message_id().0; // a fresh opaque id (prefixed)
        let recipient_agent_id = self.agent_id_for_runtime(recipient).await?;
        self.store
            .conn
            .execute(
                "INSERT OR IGNORE INTO in_flight (in_flight_id, message_id, recipient_session, \
                 recipient_agent_id, state) VALUES (?1, ?2, ?3, ?4, 'pending')",
                params![
                    in_flight_id,
                    message.0.clone(),
                    recipient.0.clone(),
                    recipient_agent_id
                ],
            )
            .await
            .map_err(store_err)?;
        self.store.events().inbox_enqueued().signal();
        Ok(())
    }

    /// Write one `pending` row for a stable recipient. If the agent currently has an active
    /// runtime, the legacy `recipient_session` column is filled for old drainers as well.
    pub async fn enqueue_for_agent(
        &self,
        message: &MessageId,
        recipient_agent_id: &str,
    ) -> Result<(), NexusError> {
        let in_flight_id = new_message_id().0;
        let recipient_session = AgentRuntimes::new(self.store)
            .active_for_agent(recipient_agent_id)
            .await?
            .map(|runtime| runtime.runtime_id);
        self.store
            .conn
            .execute(
                "INSERT OR IGNORE INTO in_flight (in_flight_id, message_id, recipient_session, \
                 recipient_agent_id, state) VALUES (?1, ?2, ?3, ?4, 'pending')",
                params![
                    in_flight_id,
                    message.0.clone(),
                    recipient_session,
                    recipient_agent_id
                ],
            )
            .await
            .map_err(store_err)?;
        self.store.events().inbox_enqueued().signal();
        Ok(())
    }

    /// The recipient's pending+notified queue (not yet delivered/acked), oldest first, capped at
    /// `limit`. Returns each in_flight id paired with the full durable [`Message`].
    pub async fn pending_for(
        &self,
        recipient: &SessionId,
        _project: &str,
        limit: u32,
    ) -> Result<Vec<(String, Message)>, NexusError> {
        self.queued_for(recipient, limit, true).await
    }

    /// The exact rows already included in a recipient's latest notification boundary.
    ///
    /// Agent injection uses this narrower view after [`mark_notified`](Self::mark_notified).
    /// A row committed between that notification statement and this read remains `pending` for
    /// the next boundary instead of entering a batch that cannot be claimed atomically.
    pub async fn notified_for(
        &self,
        recipient: &SessionId,
        _project: &str,
        limit: u32,
    ) -> Result<Vec<(String, Message)>, NexusError> {
        self.queued_for(recipient, limit, false).await
    }

    async fn queued_for(
        &self,
        recipient: &SessionId,
        limit: u32,
        include_pending: bool,
    ) -> Result<Vec<(String, Message)>, NexusError> {
        let agent_id = self.agent_id_for_runtime(recipient).await?;
        if self
            .runtime_is_superseded(recipient, agent_id.as_deref())
            .await?
        {
            return Ok(Vec::new());
        }
        let eligible_states = if include_pending {
            "('pending','notified')"
        } else {
            "('notified')"
        };
        let mut rows = self
            .store
            .conn
            .query(
                &format!(
                    "SELECT f.in_flight_id, m.message_id, m.from_name, m.kind, m.thread_id, m.topic, \
                 m.summary, m.body, m.provenance, m.project, m.created_at \
                 FROM in_flight f JOIN messages m ON m.message_id = f.message_id \
                 WHERE ((f.recipient_session = ?1 AND EXISTS ( \
                   SELECT 1 FROM sessions s WHERE s.session_id = ?1 \
                   AND s.presence <> 'offline' LIMIT 1)) \
                 OR (f.recipient_agent_id IS NULL AND f.recipient_session = ?1) \
                 OR (?3 IS NOT NULL AND f.recipient_agent_id = ?3)) \
                 AND f.state IN {eligible_states} \
                 ORDER BY m.created_at ASC LIMIT ?2"
                ),
                params![recipient.0.clone(), limit as i64, agent_id],
            )
            .await
            .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            let in_flight_id = crate::repos::sessions::get_text(&row, 0)?;
            let msg = Messages::row_to_message_joined(&row)?;
            out.push((in_flight_id, msg));
        }
        Ok(out)
    }

    /// Transition pending rows for a recipient to `notified` (the bell has rung).
    pub async fn mark_notified(&self, recipient: &SessionId) -> Result<(), NexusError> {
        let agent_id = self.agent_id_for_runtime(recipient).await?;
        if self
            .runtime_is_superseded(recipient, agent_id.as_deref())
            .await?
        {
            return Ok(());
        }
        self.store
            .conn
            .execute(
                "UPDATE in_flight SET state = 'notified' \
                 WHERE state = 'pending' AND ((recipient_session = ?1 AND EXISTS ( \
                   SELECT 1 FROM sessions s WHERE s.session_id = ?1 \
                   AND s.presence <> 'offline' LIMIT 1)) \
                 OR (recipient_agent_id IS NULL \
                 AND recipient_session = ?1) OR (?2 IS NOT NULL AND recipient_agent_id = ?2))",
                params![recipient.0.clone(), agent_id],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Durably claim the one permitted automatic injection attempt.
    ///
    /// Only a `notified` row is eligible. The returned affected-row count is the settlement truth:
    /// zero means the row was stale, already attempted, or belongs to another recipient.
    pub async fn mark_injecting(
        &self,
        message: &MessageId,
        recipient: &SessionId,
    ) -> Result<u64, NexusError> {
        let agent_id = self.agent_id_for_runtime(recipient).await?;
        self.store
            .conn
            .execute(
                "UPDATE in_flight SET state = 'injecting', \
                 attempt_count = attempt_count + 1, attempt_started_at = ?3, failed_at = NULL, \
                 error_code = NULL, error_reason = NULL, error_details_json = NULL, \
                 delivered_at = NULL, acked_at = NULL \
                 WHERE message_id = ?1 AND state = 'notified' \
                 AND (recipient_session = ?2 OR (?4 IS NOT NULL AND recipient_agent_id = ?4))",
                params![message.0.clone(), recipient.0.clone(), now(), agent_id],
            )
            .await
            .map_err(store_err)
    }

    /// Atomically claim one complete notification batch before crossing the harness boundary.
    ///
    /// The statement changes either every requested row or none. In particular, a newly enqueued
    /// `pending` row can never cause the earlier `notified` prefix to be partially claimed and
    /// subsequently dead-lettered even though no harness injection occurred.
    pub async fn mark_batch_injecting(
        &self,
        messages: &[MessageId],
        recipient: &SessionId,
    ) -> Result<u64, NexusError> {
        if messages.is_empty() {
            return Ok(0);
        }
        let ids = messages
            .iter()
            .map(|message| message.0.as_str())
            .collect::<HashSet<_>>();
        if ids.len() != messages.len() {
            return Ok(0);
        }
        let ids_json = serde_json::to_string(&ids).map_err(store_msg)?;
        let agent_id = self.agent_id_for_runtime(recipient).await?;
        let expected = messages.len().min(i64::MAX as usize) as i64;
        self.store
            .conn
            .execute(
                "UPDATE in_flight SET state = 'injecting', \
                 attempt_count = attempt_count + 1, attempt_started_at = ?3, failed_at = NULL, \
                 error_code = NULL, error_reason = NULL, error_details_json = NULL, \
                 delivered_at = NULL, acked_at = NULL \
                 WHERE message_id IN (SELECT value FROM json_each(?1)) \
                   AND state = 'notified' \
                   AND (recipient_session = ?2 OR (?4 IS NOT NULL AND recipient_agent_id = ?4)) \
                   AND (SELECT COUNT(*) FROM in_flight candidate \
                        WHERE candidate.message_id IN (SELECT value FROM json_each(?1)) \
                          AND candidate.state = 'notified' \
                          AND (candidate.recipient_session = ?2 OR \
                               (?4 IS NOT NULL AND candidate.recipient_agent_id = ?4))) = ?5",
                params![ids_json, recipient.0.clone(), now(), agent_id, expected],
            )
            .await
            .map_err(store_err)
    }

    /// Restore a native-steer claim after the harness explicitly rejected it because the active
    /// turn ended before context admission.
    ///
    /// This transition is deliberately narrower than a general retry: only the exact recipient's
    /// current `injecting` row is eligible, and callers may use it only for the
    /// `ACTIVE_TURN_REQUIRED` precondition response. That response proves the external harness did
    /// not accept the payload, so returning it to `notified` cannot duplicate model context. The
    /// attempt counter remains as audit evidence that Nexus crossed the RPC boundary.
    pub async fn restore_rejected_steer(
        &self,
        message: &MessageId,
        recipient: &SessionId,
    ) -> Result<u64, NexusError> {
        let agent_id = self.agent_id_for_runtime(recipient).await?;
        self.store
            .conn
            .execute(
                "UPDATE in_flight SET state = 'notified', attempt_started_at = NULL, \
                 delivered_at = NULL, acked_at = NULL, failed_at = NULL, error_code = NULL, \
                 error_reason = NULL, error_details_json = NULL \
                 WHERE message_id = ?1 AND state = 'injecting' \
                 AND (recipient_session = ?2 OR (?3 IS NOT NULL AND recipient_agent_id = ?3))",
                params![message.0.clone(), recipient.0.clone(), agent_id],
            )
            .await
            .map_err(store_err)
    }

    /// Mark a successful model turn delivered after a durable `injecting` claim.
    ///
    /// The affected-row count is authoritative; callers must emit delivery events only for one
    /// changed row. Acceptance at the external harness boundary is not sufficient.
    pub async fn mark_delivered(
        &self,
        message: &MessageId,
        recipient: &SessionId,
    ) -> Result<u64, NexusError> {
        let agent_id = self.agent_id_for_runtime(recipient).await?;
        let changed = self
            .store
            .conn
            .execute(
                "UPDATE in_flight SET state = 'delivered', delivered_at = ?3, acked_at = NULL, \
                 failed_at = NULL, error_code = NULL, error_reason = NULL, \
                 error_details_json = NULL \
                 WHERE message_id = ?1 AND state = 'injecting' \
                 AND (recipient_session = ?2 OR (?4 IS NOT NULL AND recipient_agent_id = ?4))",
                params![message.0.clone(), recipient.0.clone(), now(), agent_id],
            )
            .await
            .map_err(store_err)?;
        if self.store.has_split_authority() {
            DeliveryObligations::new(self.store)
                .remove_for_runtime(&message.0, &recipient.0)
                .await?;
        }
        Ok(changed)
    }

    /// Mark the exact messages rendered by one authenticated human session as delivered.
    ///
    /// Human browser reads do not cross a harness injection boundary, so their eligible states are
    /// `pending` and `notified` rather than `injecting`. The session predicate is the authority
    /// boundary and the JSON id set keeps the whole visible page in one daemon-owned statement.
    pub async fn mark_human_read_delivered(
        &self,
        recipient_session: &str,
        message_ids: &[MessageId],
        delivered_at: i64,
    ) -> Result<u64, NexusError> {
        let session = recipient_session.trim();
        if session.is_empty() {
            return Ok(0);
        }
        let ids = message_ids
            .iter()
            .map(|message| message.0.trim())
            .filter(|message| !message.is_empty())
            .collect::<HashSet<_>>();
        if ids.is_empty() {
            return Ok(0);
        }
        let ids_json = serde_json::to_string(&ids).map_err(store_msg)?;
        self.store
            .conn
            .execute(
                "UPDATE in_flight SET state = 'delivered', \
                 delivered_at = COALESCE(delivered_at, ?3) \
                 WHERE recipient_session = ?1 \
                   AND message_id IN (SELECT value FROM json_each(?2)) \
                   AND state IN ('pending', 'notified')",
                params![session, ids_json, delivered_at],
            )
            .await
            .map_err(store_err)
    }

    /// Dead-letter one row to the terminal `error` state: a turn whose harness completion never
    /// arrived. Mirrors [`mark_delivered`](Self::mark_delivered) but sets `state = 'error'` and
    /// leaves `delivered_at`/`acked_at` untouched (NULL). The `error` row is never re-drained (it is
    /// outside the `pending`/`notified` drain set) and never counted as delivered — so a lost
    /// completion is neither falsely-delivered nor storm-re-injected on every future bell.
    pub async fn mark_delivery_failed(
        &self,
        message: &MessageId,
        recipient: &SessionId,
    ) -> Result<u64, NexusError> {
        self.mark_delivery_failed_with_reason(message, recipient, "turn completion timeout")
            .await
    }

    /// Dead-letter one row with an operator-visible reason.
    pub async fn mark_delivery_failed_with_reason(
        &self,
        message: &MessageId,
        recipient: &SessionId,
        reason: &str,
    ) -> Result<u64, NexusError> {
        self.mark_delivery_error(
            message,
            recipient,
            COMPLETION_TIMEOUT_ERROR_CODE,
            reason,
            None,
        )
        .await
    }

    /// Record a terminal machine-readable delivery failure.
    ///
    /// `pending` is accepted so a known dead or unreachable target can be recorded without
    /// inventing an external attempt. `notified` and `injecting` cover wake and provider failures.
    /// Terminal/delivered rows are immutable until an explicit dead-letter requeue.
    pub async fn mark_delivery_error(
        &self,
        message: &MessageId,
        recipient: &SessionId,
        code: &str,
        reason: &str,
        details_json: Option<&str>,
    ) -> Result<u64, NexusError> {
        let agent_id = self.agent_id_for_runtime(recipient).await?;
        let changed = self
            .store
            .conn
            .execute(
                "UPDATE in_flight SET state = 'error', failed_at = ?3, error_code = ?4, \
                 error_reason = ?5, error_details_json = ?6, delivered_at = NULL, acked_at = NULL \
                 WHERE message_id = ?1 AND state IN ('pending','notified','injecting') \
                 AND (recipient_session = ?2 OR (?7 IS NOT NULL AND recipient_agent_id = ?7))",
                params![
                    message.0.clone(),
                    recipient.0.clone(),
                    now(),
                    code,
                    reason,
                    details_json,
                    agent_id
                ],
            )
            .await
            .map_err(store_err)?;
        if self.store.has_split_authority() {
            DeliveryObligations::new(self.store)
                .remove_for_runtime(&message.0, &recipient.0)
                .await?;
        }
        Ok(changed)
    }

    async fn agent_id_for_runtime(
        &self,
        recipient: &SessionId,
    ) -> Result<Option<String>, NexusError> {
        let mut rows = self
            .store
            .identity_conn()
            .query(
                "SELECT agent_id FROM agent_runtimes WHERE runtime_id = ?1 LIMIT 1",
                params![recipient.0.clone()],
            )
            .await
            .map_err(store_err)?;
        let Some(row) = rows.next().await.map_err(store_err)? else {
            return Ok(None);
        };
        Ok(Some(row.get(0).map_err(store_err)?))
    }

    async fn runtime_is_superseded(
        &self,
        recipient: &SessionId,
        agent_id: Option<&str>,
    ) -> Result<bool, NexusError> {
        let Some(agent_id) = agent_id else {
            return Ok(false);
        };
        Ok(AgentRuntimes::new(self.store)
            .active_for_agent(agent_id)
            .await?
            .is_some_and(|runtime| runtime.runtime_id != recipient.0))
    }

    /// Build the terminal Gateway delivery fact for one already-settled recipient row.
    ///
    /// `in_flight_id` plus the completed attempt number and terminal state form the stable
    /// idempotency key. Re-reading the same effect after reclaim therefore cannot duplicate the
    /// Gateway row, while an explicit operator requeue creates a new attempt revision.
    pub async fn gateway_delivery_effect(
        &self,
        message: &MessageId,
        recipient: &SessionId,
    ) -> Result<Option<GatewayProjectionEffect>, NexusError> {
        let agent_id = self.agent_id_for_runtime(recipient).await?;
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT in_flight_id, message_id, recipient_session, recipient_agent_id, state, \
                 attempt_count, delivered_at, acked_at, failed_at, error_code, error_reason, \
                 error_details_json FROM in_flight WHERE message_id = ?1 \
                 AND (recipient_session = ?2 OR (?3 IS NOT NULL AND recipient_agent_id = ?3)) \
                 AND state IN ('delivered','acked','error') ORDER BY rowid DESC LIMIT 1",
                params![message.0.clone(), recipient.0.clone(), agent_id],
            )
            .await
            .map_err(store_err)?;
        let Some(row) = rows.next().await.map_err(store_err)? else {
            return Ok(None);
        };
        Ok(Some(delivery_effect_from_row(&row)?))
    }

    /// Build all terminal facts for a recipient after a bulk lifecycle settlement.
    pub async fn gateway_delivery_effects_for_recipient(
        &self,
        recipient: &SessionId,
    ) -> Result<Vec<GatewayProjectionEffect>, NexusError> {
        let agent_id = self.agent_id_for_runtime(recipient).await?;
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT in_flight_id, message_id, recipient_session, recipient_agent_id, state, \
                 attempt_count, delivered_at, acked_at, failed_at, error_code, error_reason, \
                 error_details_json FROM in_flight WHERE (recipient_session = ?1 OR \
                 (?2 IS NOT NULL AND recipient_agent_id = ?2)) \
                 AND state IN ('delivered','acked','error') ORDER BY rowid",
                params![recipient.0.clone(), agent_id],
            )
            .await
            .map_err(store_err)?;
        let mut effects = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            effects.push(delivery_effect_from_row(&row)?);
        }
        Ok(effects)
    }

    /// Drop the boot-scoped rich message and recipient attempt rows once every recipient is
    /// terminal and the caller has copied the settlement into the Gateway projection backlog.
    /// Legacy single-store callers retain their historical rows for compatibility.
    pub async fn discard_settled_message_if_complete(
        &self,
        message: &MessageId,
    ) -> Result<bool, NexusError> {
        if !self.store.has_split_authority() {
            return Ok(false);
        }
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT COUNT(*), \
                   SUM(CASE WHEN state IN ('delivered','acked','error') THEN 0 ELSE 1 END) \
                 FROM in_flight WHERE message_id = ?1",
                params![message.0.clone()],
            )
            .await
            .map_err(store_err)?;
        let Some(row) = rows.next().await.map_err(store_err)? else {
            return Ok(false);
        };
        let total = get_opt_int(&row, 0)?.unwrap_or_default();
        let unsettled = get_opt_int(&row, 1)?.unwrap_or_default();
        drop(rows);
        if total == 0 || unsettled != 0 {
            return Ok(false);
        }

        let txn = self
            .store
            .begin_write_txn("discard_settled_message")
            .await?;
        let result = async {
            let changed = txn
                .execute(
                    "DELETE FROM in_flight WHERE message_id = ?1 \
                     AND NOT EXISTS (SELECT 1 FROM in_flight pending \
                       WHERE pending.message_id = ?1 \
                         AND pending.state NOT IN ('delivered','acked','error'))",
                    params![message.0.clone()],
                )
                .await?;
            if changed == 0 {
                return Ok(false);
            }
            txn.execute(
                "DELETE FROM messages WHERE message_id = ?1 \
                 AND NOT EXISTS (SELECT 1 FROM in_flight WHERE message_id = ?1)",
                params![message.0.clone()],
            )
            .await?;
            Ok(true)
        }
        .await;
        match result {
            Ok(discarded) => {
                txn.commit().await?;
                Ok(discarded)
            }
            Err(error) => {
                txn.rollback(&error).await?;
                Err(error)
            }
        }
    }

    /// Settle every still-eligible row for one runtime when the daemon cannot establish a
    /// delivery transport. This is the cold-target counterpart to per-message injection errors:
    /// the target failure is durable and the rows are no longer boot- or bell-retryable.
    pub async fn mark_recipient_pending_error(
        &self,
        recipient: &SessionId,
        code: &str,
        reason: &str,
        details_json: Option<&str>,
    ) -> Result<u64, NexusError> {
        let agent_id = self.agent_id_for_runtime(recipient).await?;
        self.store
            .conn
            .execute(
                "UPDATE in_flight SET state = 'error', failed_at = ?2, error_code = ?3, \
                 error_reason = ?4, error_details_json = ?5, delivered_at = NULL, acked_at = NULL \
                 WHERE state IN ('pending','notified') AND (recipient_session = ?1 \
                 OR (?6 IS NOT NULL AND recipient_agent_id = ?6))",
                params![
                    recipient.0.clone(),
                    now(),
                    code,
                    reason,
                    details_json,
                    agent_id
                ],
            )
            .await
            .map_err(store_err)
    }

    /// Fail closed every attempt currently owned by a harness runtime that has exited.
    ///
    /// Once a row is `injecting`, Nexus cannot prove whether the external harness consumed the
    /// prompt before its process died. Runtime-death recovery therefore records an ambiguous
    /// terminal outcome and never makes the row automatically eligible again.
    pub async fn mark_recipient_injecting_outcome_unknown(
        &self,
        recipient: &SessionId,
        reason: &str,
    ) -> Result<u64, NexusError> {
        let agent_id = self.agent_id_for_runtime(recipient).await?;
        self.store
            .conn
            .execute(
                "UPDATE in_flight SET state = 'error', failed_at = ?2, error_code = ?3, \
                 error_reason = ?4, error_details_json = ?5, delivered_at = NULL, acked_at = NULL \
                 WHERE state = 'injecting' AND (recipient_session = ?1 \
                 OR (?6 IS NOT NULL AND recipient_agent_id = ?6))",
                params![
                    recipient.0.clone(),
                    now(),
                    DELIVERY_OUTCOME_UNKNOWN_ERROR_CODE,
                    reason,
                    "{\"source\":\"harness_exit\"}",
                    agent_id
                ],
            )
            .await
            .map_err(store_err)
    }

    /// Per-message ack (DM split-ack): move one delivered row to `acked` and return affected rows.
    pub async fn ack(&self, message: &MessageId, recipient: &SessionId) -> Result<u64, NexusError> {
        let agent_id = self.agent_id_for_runtime(recipient).await?;
        self.store
            .conn
            .execute(
                "UPDATE in_flight SET state = 'acked', acked_at = ?3 \
                 WHERE message_id = ?1 AND state = 'delivered' \
                 AND (recipient_session = ?2 OR (?4 IS NOT NULL AND recipient_agent_id = ?4))",
                params![message.0.clone(), recipient.0.clone(), now(), agent_id],
            )
            .await
            .map_err(store_err)
    }

    /// Bulk ack (thread split-ack): move many rows to `acked` for one recipient.
    pub async fn ack_many(
        &self,
        messages: &[MessageId],
        recipient: &SessionId,
    ) -> Result<u64, NexusError> {
        let mut affected = 0;
        for m in messages {
            affected += self.ack(m, recipient).await?;
        }
        Ok(affected)
    }

    /// Convert every attempt left `injecting` across daemon restart into an ambiguous terminal
    /// error. The operation is idempotent and never makes a row eligible for automatic delivery.
    pub async fn recover_stale_injecting(&self) -> Result<DeadLetterMutation, NexusError> {
        let mut recovered = Vec::new();
        loop {
            // Probe before opening a write transaction so the common no-op boot path never
            // acquires the daemon's serialized writer gate.
            let candidates = self.stale_injecting_candidates().await?;
            if candidates.is_empty() {
                break;
            }
            let txn = self
                .store
                .begin_write_txn("inbox_recover_injecting")
                .await?;
            let failed_at = now();
            let candidates_json = serde_json::to_string(&candidates)
                .map_err(|error| NexusError::Store(error.to_string()))?;
            let result = async {
                let mut rows = txn
                    .query(
                        "UPDATE in_flight SET state = 'error', failed_at = ?2, \
                         error_code = ?3, error_reason = ?4, error_details_json = ?5, \
                         delivered_at = NULL, acked_at = NULL \
                         WHERE state = 'injecting' AND in_flight_id IN ( \
                           SELECT value FROM json_each(?1) \
                         ) RETURNING in_flight_id",
                        params![
                            candidates_json,
                            failed_at,
                            DELIVERY_OUTCOME_UNKNOWN_ERROR_CODE,
                            "delivery outcome unknown after restart",
                            "{\"source\":\"daemon_restart\"}"
                        ],
                    )
                    .await?;
                let mut updated = HashSet::new();
                while let Some(row) = rows.next().await.map_err(store_err)? {
                    updated.insert(get_text(&row, 0)?);
                }
                Ok(updated)
            }
            .await;
            match result {
                Ok(updated) => {
                    txn.commit().await?;
                    recovered.extend(
                        candidates
                            .into_iter()
                            .filter(|candidate| updated.contains(candidate)),
                    );
                }
                Err(error) => {
                    txn.rollback(&error).await?;
                    return Err(error);
                }
            }
        }
        Ok(DeadLetterMutation {
            count: recovered.len() as u64,
            in_flight_ids: recovered,
        })
    }

    /// Read-path probe for [`Self::recover_stale_injecting`], kept outside its write transaction
    /// so a no-op boot recovery does not acquire the embedded writer gate.
    async fn stale_injecting_candidates(&self) -> Result<Vec<String>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT in_flight_id FROM in_flight WHERE state = 'injecting' \
                 ORDER BY in_flight_id LIMIT ?1",
                params![i64::from(MAX_DLQ_LIMIT)],
            )
            .await
            .map_err(store_err)?;
        let mut candidates = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            candidates.push(get_text(&row, 0)?);
        }
        Ok(candidates)
    }

    /// Distinct recipient sessions that still have an undrained (`pending`/`notified`) row. Used on
    /// daemon boot to re-spawn exactly the agents with leftover mail (spec §6.5).
    ///
    /// Stable-agent rows with a concrete `recipient_session` remain revive candidates even after the
    /// matching runtime was stopped/offlined by liveness convergence. Rows addressed only by stable
    /// id still require an active runtime because there is no concrete session id to respawn.
    pub async fn recipients_with_pending(&self) -> Result<Vec<SessionId>, NexusError> {
        self.recipients_with_pending_cutoff(None).await
    }

    /// Distinct recipients whose undrained message predates a daemon boot cutoff.
    ///
    /// Boot recovery is spawned asynchronously after the composition root returns. Without this
    /// boundary, a newly posted message can enter the queue before recovery takes its snapshot and
    /// be mistaken for pre-boot backlog, racing the normal live delivery/consumer path. The strict
    /// comparison deliberately excludes messages stamped in the same millisecond as boot start.
    pub async fn recipients_with_pending_before(
        &self,
        created_before: i64,
    ) -> Result<Vec<SessionId>, NexusError> {
        self.recipients_with_pending_cutoff(Some(created_before))
            .await
    }

    async fn recipients_with_pending_cutoff(
        &self,
        created_before: Option<i64>,
    ) -> Result<Vec<SessionId>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT DISTINCT f.recipient_session, f.recipient_agent_id FROM in_flight f \
                 WHERE f.state IN ('pending','notified') \
                   AND (?1 IS NULL OR EXISTS (SELECT 1 FROM messages m \
                     WHERE m.message_id = f.message_id AND m.created_at < ?1)) \
                 ORDER BY f.recipient_session, f.recipient_agent_id",
                params![created_before],
            )
            .await
            .map_err(store_err)?;
        let mut targets = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            targets.push((get_opt_text(&row, 0)?, get_opt_text(&row, 1)?));
        }
        drop(rows);

        let sessions = Sessions::new(self.store);
        let runtimes = AgentRuntimes::new(self.store);
        let mut out = HashSet::new();
        for (recipient_session, recipient_agent_id) in targets {
            if let Some(agent_id) = recipient_agent_id.as_deref() {
                if let Some(runtime) = runtimes.active_for_agent(agent_id).await? {
                    out.insert(runtime.runtime_id);
                    continue;
                }
            }
            let Some(session_id) = recipient_session.as_deref() else {
                continue;
            };
            let live = sessions
                .find_by_session_id(&SessionId(session_id.to_string()))
                .await?;
            let runtime_matches = match recipient_agent_id.as_deref() {
                Some(agent_id) => runtimes
                    .find_by_runtime_id(session_id)
                    .await?
                    .is_some_and(|runtime| runtime.agent_id == agent_id),
                None => false,
            };
            if recipient_agent_id.is_none()
                || live
                    .as_ref()
                    .is_some_and(|row| row.presence.as_deref().unwrap_or("offline") != "offline")
                || runtime_matches
            {
                out.insert(session_id.to_string());
            }
        }
        let mut out = out.into_iter().map(SessionId).collect::<Vec<_>>();
        out.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(out)
    }

    /// Delete undrained rows whose recipient has no remaining live delivery path.
    ///
    /// This is a maintenance reaper for post-incident backlog cleanup: it only removes
    /// `pending`/`notified` rows when the concrete `recipient_session` row no longer EXISTS
    /// (`admin delete`d fossil) and the stable `recipient_agent_id` has no active runtime.
    ///
    /// Offline/stale-heartbeat sessions are deliberately NOT reaped: pending rows are durable
    /// mail, and an existing session — however quiet — can still be revived or reconnect. An
    /// earlier offline+stale-heartbeat clause here deleted freshly-registered consumers' pending
    /// rows (no heartbeat yet = "stale") the moment the boot respawn pass ran; any policy for
    /// aging out stale-but-existing recipients needs an explicit operator surface (B9/B18), not
    /// a silent boot reap.
    pub async fn reap_undeliverable_for_dead_recipients(&self) -> Result<u64, NexusError> {
        let dead = self.undeliverable_candidates(None).await?;
        self.delete_candidates(&dead).await
    }

    /// Preserve, rather than delete, queued mail whose target no longer has any delivery path.
    /// This is the boot-time policy: the sender/operator can inspect the terminal failure in DLQ.
    pub async fn dead_letter_undeliverable_for_dead_recipients(&self) -> Result<u64, NexusError> {
        self.dead_letter_undeliverable_for_dead_recipients_cutoff(None)
            .await
    }

    /// Apply boot-time dead-letter settlement only to messages older than the captured boot start.
    pub async fn dead_letter_undeliverable_for_dead_recipients_before(
        &self,
        created_before: i64,
    ) -> Result<u64, NexusError> {
        self.dead_letter_undeliverable_for_dead_recipients_cutoff(Some(created_before))
            .await
    }

    async fn dead_letter_undeliverable_for_dead_recipients_cutoff(
        &self,
        created_before: Option<i64>,
    ) -> Result<u64, NexusError> {
        let dead = self.undeliverable_candidates(created_before).await?;
        if dead.is_empty() {
            return Ok(0);
        }
        let dead_json = serde_json::to_string(&dead).map_err(store_msg)?;
        self.store
            .conn
            .execute(
                "UPDATE in_flight SET state = 'error', failed_at = ?1, error_code = ?2, \
                 error_reason = ?3, error_details_json = ?4, delivered_at = NULL, acked_at = NULL \
                 WHERE state IN ('pending','notified') AND in_flight_id IN ( \
                   SELECT value FROM json_each(?5))",
                params![
                    now(),
                    TARGET_DEAD_ERROR_CODE,
                    "target no longer has a delivery runtime",
                    "{\"source\":\"boot_reconcile\"}",
                    dead_json
                ],
            )
            .await
            .map_err(store_err)
    }

    async fn undeliverable_candidates(
        &self,
        created_before: Option<i64>,
    ) -> Result<Vec<String>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT f.in_flight_id, f.recipient_session, f.recipient_agent_id \
                 FROM in_flight f WHERE f.state IN ('pending','notified') \
                   AND (?1 IS NULL OR EXISTS (SELECT 1 FROM messages m \
                     WHERE m.message_id = f.message_id AND m.created_at < ?1))",
                params![created_before],
            )
            .await
            .map_err(store_err)?;
        let mut candidates = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            candidates.push((
                get_text(&row, 0)?,
                get_opt_text(&row, 1)?,
                get_opt_text(&row, 2)?,
            ));
        }
        drop(rows);

        let sessions = Sessions::new(self.store);
        let runtimes = AgentRuntimes::new(self.store);
        let mut dead = Vec::new();
        for (in_flight_id, recipient_session, recipient_agent_id) in candidates {
            let live_session = match recipient_session.as_deref() {
                Some(session_id) => sessions
                    .find_by_session_id(&SessionId(session_id.to_string()))
                    .await?
                    .is_some(),
                None => false,
            };
            let active_runtime = match recipient_agent_id.as_deref() {
                Some(agent_id) => runtimes.active_for_agent(agent_id).await?.is_some(),
                None => false,
            };
            if !live_session && !active_runtime {
                dead.push(in_flight_id);
            }
        }
        Ok(dead)
    }

    async fn delete_candidates(&self, candidates: &[String]) -> Result<u64, NexusError> {
        if candidates.is_empty() {
            return Ok(0);
        }
        let candidates_json = serde_json::to_string(candidates).map_err(store_msg)?;
        self.store
            .conn
            .execute(
                "DELETE FROM in_flight WHERE state IN ('pending','notified') \
                 AND in_flight_id IN (SELECT value FROM json_each(?1))",
                params![candidates_json],
            )
            .await
            .map_err(store_err)
    }

    /// Move old pending/notified deliveries to the operator-visible dead-letter queue.
    ///
    /// `in_flight` rows do not carry their own creation timestamp; the durable message timestamp is
    /// the creation clock already shown by the DLQ surface.
    ///
    /// The expiry probe runs on the plain read path BEFORE any write transaction is opened. With
    /// no expired rows (the common case, every sweep tick), acquiring the serialized daemon writer
    /// would add needless contention. Only when expired
    /// ids exist does a write transaction open; its UPDATEs re-check `state IN
    /// ('pending','notified')`, so rows that changed state after the probe are skipped, and the
    /// returned mutation counts only rows actually dead-lettered.
    pub async fn dead_letter_expired_deliveries(
        &self,
        now_ms: i64,
        ttl_ms: i64,
    ) -> Result<DeadLetterMutation, NexusError> {
        let cutoff = now_ms.saturating_sub(ttl_ms.max(0));
        let mut dead_lettered = Vec::new();
        loop {
            let candidates = self.expired_delivery_candidates(cutoff).await?;
            if candidates.is_empty() {
                break;
            }
            let txn = self.store.begin_write_txn("inbox_delivery_timeout").await?;
            let candidates_json = serde_json::to_string(&candidates)
                .map_err(|error| NexusError::Store(error.to_string()))?;
            let result = async {
                let mut rows = txn
                    .query(
                        "UPDATE in_flight SET state = 'error', failed_at = ?3, \
                         error_code = ?2, error_reason = ?2, error_details_json = NULL, \
                         delivered_at = NULL, acked_at = NULL \
                         WHERE state IN ('pending','notified') AND in_flight_id IN ( \
                           SELECT value FROM json_each(?1) \
                         ) RETURNING in_flight_id",
                        params![candidates_json, DELIVERY_TIMEOUT_REASON, now_ms],
                    )
                    .await?;
                let mut updated = HashSet::new();
                while let Some(row) = rows.next().await.map_err(store_err)? {
                    updated.insert(get_text(&row, 0)?);
                }
                Ok(updated)
            }
            .await;
            match result {
                Ok(updated) => {
                    txn.commit().await?;
                    dead_lettered.extend(
                        candidates
                            .into_iter()
                            .filter(|candidate| updated.contains(candidate)),
                    );
                }
                Err(error) => {
                    txn.rollback(&error).await?;
                    return Err(error);
                }
            }
        }
        Ok(DeadLetterMutation {
            count: dead_lettered.len() as u64,
            in_flight_ids: dead_lettered,
        })
    }

    /// Read-path probe for [`Self::dead_letter_expired_deliveries`]: expired pending/notified
    /// delivery ids as of `cutoff`. Deliberately OUTSIDE any write transaction so a no-op sweep
    /// never acquires the serialized embedded writer.
    async fn expired_delivery_candidates(&self, cutoff: i64) -> Result<Vec<String>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT f.in_flight_id \
                 FROM in_flight f JOIN messages m ON m.message_id = f.message_id \
                 WHERE f.state IN ('pending','notified') \
                   AND m.created_at IS NOT NULL \
                   AND m.created_at <= ?1 \
                 ORDER BY m.created_at ASC, f.in_flight_id ASC LIMIT ?2",
                params![cutoff, i64::from(MAX_DLQ_LIMIT)],
            )
            .await
            .map_err(store_err)?;
        let mut candidates = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            candidates.push(get_text(&row, 0)?);
        }
        Ok(candidates)
    }

    /// Return terminal `error` rows with enough context for an operator to decide whether to
    /// requeue or purge. Bodies are truncated to a preview so status/list output cannot dump large
    /// message payloads by accident.
    pub async fn dead_letters(
        &self,
        filter: DeadLetterFilter,
    ) -> Result<Vec<DeadLetterEntry>, NexusError> {
        let filter = self.resolve_dead_letter_filter(filter).await?;
        let limit = normalized_limit(filter.limit);
        let mut rows = match (&filter.target, filter.since) {
            (Some(target), Some(since)) => {
                self.store
                    .conn
                    .query(
                        dead_letter_select_sql(true, true),
                        params![target.clone(), since, limit],
                    )
                    .await
            }
            (Some(target), None) => {
                self.store
                    .conn
                    .query(
                        dead_letter_select_sql(true, false),
                        params![target.clone(), limit],
                    )
                    .await
            }
            (None, Some(since)) => {
                self.store
                    .conn
                    .query(dead_letter_select_sql(false, true), params![since, limit])
                    .await
            }
            (None, None) => {
                self.store
                    .conn
                    .query(dead_letter_select_sql(false, false), params![limit])
                    .await
            }
        }
        .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(dead_letter_row(&row)?);
        }
        drop(rows);
        for entry in &mut out {
            if entry.recipient_name.is_none() {
                if let Some(agent_id) = entry.recipient_agent_id.as_deref() {
                    entry.recipient_name = Agents::new(self.store)
                        .find_by_id(agent_id)
                        .await?
                        .and_then(|agent| agent.name);
                }
            }
        }
        Ok(out)
    }

    async fn resolve_dead_letter_filter(
        &self,
        mut filter: DeadLetterFilter,
    ) -> Result<DeadLetterFilter, NexusError> {
        let Some(target) = filter.target.as_deref() else {
            return Ok(filter);
        };
        if target.starts_with("a_") || target.starts_with("s_") {
            return Ok(filter);
        }
        if let Some(agent) = Agents::new(self.store).find_by_name(target).await? {
            filter.target = Some(agent.agent_id);
        } else if let Some(session) = Sessions::new(self.store)
            .find_by_name_any_project(target)
            .await?
        {
            filter.target = Some(session.session_id.0);
        }
        Ok(filter)
    }

    async fn resolve_dead_letter_selector(
        &self,
        selector: DeadLetterSelector,
    ) -> Result<DeadLetterSelector, NexusError> {
        match selector {
            DeadLetterSelector::Filter(filter) => Ok(DeadLetterSelector::Filter(
                self.resolve_dead_letter_filter(filter).await?,
            )),
            exact => Ok(exact),
        }
    }

    /// Count terminal dead letters for status output.
    pub async fn dead_letter_summary(&self) -> Result<DeadLetterSummary, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT COUNT(*), MIN(m.created_at) \
                 FROM in_flight f JOIN messages m ON m.message_id = f.message_id \
                 WHERE f.state = 'error'",
                (),
            )
            .await
            .map_err(store_err)?;
        let Some(row) = rows.next().await.map_err(store_err)? else {
            return Ok(DeadLetterSummary::default());
        };
        Ok(DeadLetterSummary {
            count: get_opt_int(&row, 0)?.unwrap_or_default() as u64,
            oldest_created_at: get_opt_int(&row, 1)?,
        })
    }

    /// Move selected dead-letter rows back to `pending` after explicit operator choice.
    ///
    /// The completed `attempt_count` is retained as the audit counter; transient attempt timestamps,
    /// terminal error evidence, and settlement timestamps are cleared for the new eligible attempt.
    pub async fn requeue_dead_letters(
        &self,
        selector: DeadLetterSelector,
    ) -> Result<DeadLetterMutation, NexusError> {
        let selector = self.resolve_dead_letter_selector(selector).await?;
        let txn = self.store.begin_write_txn("inbox_dlq_requeue").await?;
        let result = async {
            let ids = self.selected_dead_letter_ids(&txn, &selector).await?;
            if ids.is_empty() {
                return Ok(DeadLetterMutation::default());
            }
            for id in &ids {
                txn.execute(
                    "UPDATE in_flight SET state = 'pending', attempt_started_at = NULL, \
                     failed_at = NULL, error_code = NULL, error_reason = NULL, \
                     error_details_json = NULL, delivered_at = NULL, acked_at = NULL \
                     WHERE in_flight_id = ?1 AND state = 'error'",
                    params![id.clone()],
                )
                .await?;
            }
            Ok(DeadLetterMutation {
                count: ids.len() as u64,
                in_flight_ids: ids,
            })
        }
        .await;
        match result {
            Ok(mutation) => {
                txn.commit().await?;
                if mutation.count > 0 {
                    self.store.events().inbox_enqueued().signal();
                }
                Ok(mutation)
            }
            Err(error) => {
                txn.rollback(&error).await?;
                Err(error)
            }
        }
    }

    /// Delete selected dead-letter rows after explicit operator choice.
    pub async fn purge_dead_letters(
        &self,
        selector: DeadLetterSelector,
    ) -> Result<DeadLetterMutation, NexusError> {
        let selector = self.resolve_dead_letter_selector(selector).await?;
        let txn = self.store.begin_write_txn("inbox_dlq_purge").await?;
        let result = async {
            let ids = self.selected_dead_letter_ids(&txn, &selector).await?;
            if ids.is_empty() {
                return Ok(DeadLetterMutation::default());
            }
            for id in &ids {
                txn.execute(
                    "DELETE FROM in_flight WHERE in_flight_id = ?1 AND state = 'error'",
                    params![id.clone()],
                )
                .await?;
            }
            Ok(DeadLetterMutation {
                count: ids.len() as u64,
                in_flight_ids: ids,
            })
        }
        .await;
        match result {
            Ok(mutation) => {
                txn.commit().await?;
                Ok(mutation)
            }
            Err(error) => {
                txn.rollback(&error).await?;
                Err(error)
            }
        }
    }

    async fn selected_dead_letter_ids(
        &self,
        txn: &WriteTxn,
        selector: &DeadLetterSelector,
    ) -> Result<Vec<String>, NexusError> {
        match selector {
            DeadLetterSelector::InFlightId(id) => {
                let mut rows = txn
                    .query(
                        "SELECT in_flight_id FROM in_flight \
                         WHERE state = 'error' AND in_flight_id = ?1",
                        params![id.clone()],
                    )
                    .await?;
                let mut ids = Vec::new();
                while let Some(row) = rows.next().await.map_err(store_err)? {
                    ids.push(get_text(&row, 0)?);
                }
                Ok(ids)
            }
            DeadLetterSelector::Filter(filter) => {
                let mut rows = dead_letter_query(
                    txn,
                    &DeadLetterFilter {
                        limit: if filter.limit == 0 {
                            MAX_DLQ_LIMIT
                        } else {
                            filter.limit
                        },
                        ..filter.clone()
                    },
                )
                .await?;
                let mut ids = Vec::new();
                while let Some(row) = rows.next().await.map_err(store_err)? {
                    ids.push(get_text(&row, 0)?);
                }
                Ok(ids)
            }
        }
    }
}

async fn dead_letter_query(
    txn: &WriteTxn,
    filter: &DeadLetterFilter,
) -> Result<libsql::Rows, NexusError> {
    let limit = normalized_limit(filter.limit);
    match (filter.target.as_ref(), filter.since) {
        (Some(target), Some(since)) => {
            txn.query(
                dead_letter_select_sql(true, true),
                params![target.clone(), since, limit],
            )
            .await
        }
        (Some(target), None) => {
            txn.query(
                dead_letter_select_sql(true, false),
                params![target.clone(), limit],
            )
            .await
        }
        (None, Some(since)) => {
            txn.query(dead_letter_select_sql(false, true), params![since, limit])
                .await
        }
        (None, None) => {
            txn.query(dead_letter_select_sql(false, false), params![limit])
                .await
        }
    }
}

fn normalized_limit(limit: u32) -> i64 {
    u32::clamp(
        if limit == 0 { DEFAULT_DLQ_LIMIT } else { limit },
        1,
        MAX_DLQ_LIMIT,
    ) as i64
}

fn dead_letter_select_sql(with_target: bool, with_since: bool) -> &'static str {
    match (with_target, with_since) {
        (true, true) => {
            "SELECT f.in_flight_id, f.message_id, m.from_name, f.recipient_session, \
             f.recipient_agent_id, s.name, m.created_at, \
             COALESCE(f.failed_at, f.delivered_at), \
             f.attempt_count, f.error_code, f.error_reason, f.error_details_json, m.body \
             FROM in_flight f JOIN messages m ON m.message_id = f.message_id \
             LEFT JOIN sessions s ON s.session_id = f.recipient_session \
             WHERE f.state = 'error' AND m.created_at >= ?2 \
             AND (?1 = f.recipient_session OR ?1 = f.recipient_agent_id OR ?1 = s.name) \
             ORDER BY m.created_at ASC, f.rowid ASC, f.in_flight_id ASC LIMIT ?3"
        }
        (true, false) => {
            "SELECT f.in_flight_id, f.message_id, m.from_name, f.recipient_session, \
             f.recipient_agent_id, s.name, m.created_at, \
             COALESCE(f.failed_at, f.delivered_at), \
             f.attempt_count, f.error_code, f.error_reason, f.error_details_json, m.body \
             FROM in_flight f JOIN messages m ON m.message_id = f.message_id \
             LEFT JOIN sessions s ON s.session_id = f.recipient_session \
             WHERE f.state = 'error' \
             AND (?1 = f.recipient_session OR ?1 = f.recipient_agent_id OR ?1 = s.name) \
             ORDER BY m.created_at ASC, f.rowid ASC, f.in_flight_id ASC LIMIT ?2"
        }
        (false, true) => {
            "SELECT f.in_flight_id, f.message_id, m.from_name, f.recipient_session, \
             f.recipient_agent_id, s.name, m.created_at, \
             COALESCE(f.failed_at, f.delivered_at), \
             f.attempt_count, f.error_code, f.error_reason, f.error_details_json, m.body \
             FROM in_flight f JOIN messages m ON m.message_id = f.message_id \
             LEFT JOIN sessions s ON s.session_id = f.recipient_session \
             WHERE f.state = 'error' AND m.created_at >= ?1 \
             ORDER BY m.created_at ASC, f.rowid ASC, f.in_flight_id ASC LIMIT ?2"
        }
        (false, false) => {
            "SELECT f.in_flight_id, f.message_id, m.from_name, f.recipient_session, \
             f.recipient_agent_id, s.name, m.created_at, \
             COALESCE(f.failed_at, f.delivered_at), \
             f.attempt_count, f.error_code, f.error_reason, f.error_details_json, m.body \
             FROM in_flight f JOIN messages m ON m.message_id = f.message_id \
             LEFT JOIN sessions s ON s.session_id = f.recipient_session \
             WHERE f.state = 'error' \
             ORDER BY m.created_at ASC, f.rowid ASC, f.in_flight_id ASC LIMIT ?1"
        }
    }
}

fn dead_letter_row(row: &libsql::Row) -> Result<DeadLetterEntry, NexusError> {
    let attempt_count = get_opt_int(row, 8)?
        .unwrap_or_default()
        .clamp(0, u32::MAX as i64) as u32;
    let error_details = get_opt_text(row, 11)?.map(|raw| {
        serde_json::from_str::<serde_json::Value>(&raw)
            .unwrap_or_else(|_| serde_json::json!({ "unparsed": raw }))
    });
    Ok(DeadLetterEntry {
        in_flight_id: get_text(row, 0)?,
        message_id: MessageId(get_text(row, 1)?),
        sender: get_text(row, 2)?,
        recipient_session: get_opt_text(row, 3)?.map(SessionId),
        recipient_agent_id: get_opt_text(row, 4)?,
        recipient_name: get_opt_text(row, 5)?,
        created_at: get_opt_int(row, 6)?.unwrap_or_default(),
        dead_lettered_at: get_opt_int(row, 7)?,
        attempt_count,
        error_code: get_opt_text(row, 9)?,
        error_reason: get_opt_text(row, 10)?,
        error_details,
        body_preview: preview_body(&get_opt_text(row, 12)?.unwrap_or_default()),
    })
}

fn preview_body(body: &str) -> String {
    let mut out = String::new();
    for ch in body.chars().take(DLQ_PREVIEW_CHARS) {
        out.push(ch);
    }
    out
}
