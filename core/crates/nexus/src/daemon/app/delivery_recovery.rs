//! Bounded pre-injection recovery for durable recipient rows.
//!
//! Harness injection attempts have separate settlement semantics. This shard only tracks failures
//! that happen while establishing a recipient transport, before user input can cross that boundary.

use super::*;

impl AppState {
    /// Settle one accepted delivery immediately when its durable agent identity is explicitly
    /// dead. This check must run before pull-consumer and harness ownership routing: DEAD is a
    /// terminal identity fact, not a transport-selection hint.
    pub(crate) async fn settle_delivery_if_target_dead(
        &self,
        message_id: &nexus_contracts::MessageId,
        target: &SessionRow,
        reason: &str,
        source: &str,
    ) -> bool {
        let Some(agent_id) = target.agent_id.as_deref() else {
            return false;
        };
        let is_dead = Agents::new(&self.store)
            .lifecycle_for_id(agent_id)
            .await
            .ok()
            .and_then(|(state, _)| state)
            .as_deref()
            == Some("dead");
        if !is_dead {
            return false;
        }

        let details = serde_json::json!({ "source": source }).to_string();
        if let Err(error) = Inbox::new(&self.store)
            .mark_delivery_error(
                message_id,
                &target.session_id,
                nexus_store::repos::inbox::TARGET_DEAD_ERROR_CODE,
                reason,
                Some(&details),
            )
            .await
        {
            tracing::error!(
                target: "nexus::delivery",
                %error,
                message = %message_id,
                session = %target.session_id,
                "failed to persist dead-target delivery error"
            );
        }
        true
    }

    /// Rebuild boot-scoped message and inbox rows from the minimal persistent delivery journal.
    ///
    /// This is deliberately idempotent within one boot: the message row is inserted once and the
    /// inbox has a unique message/recipient edge. The persistent obligation remains authoritative
    /// until the normal delivery state machine settles and removes it.
    pub async fn restore_unsettled_delivery_obligations_once(&self) -> Result<usize, NexusError> {
        // Legacy single-store embeddings keep their pending inbox rows in place across this path;
        // only the split daemon reconstructs volatile transport from the continuity journal.
        if !self.store.has_split_authority() {
            return Ok(0);
        }
        let obligations = DeliveryObligations::new(&self.store).pending().await?;
        let messages = nexus_store::repos::Messages::new(&self.store);
        let inbox = Inbox::new(&self.store);
        let sessions = Sessions::new(&self.store);
        let mut restored = 0;

        for obligation in obligations {
            let Some(runtime_id) = obligation.recipient_runtime_id.as_deref() else {
                tracing::warn!(
                    target: "nexus::delivery",
                    message_id = %obligation.message_id,
                    agent_id = %obligation.recipient_agent_id,
                    "pending continuity row has no runtime descriptor yet; preserving it for a later wake"
                );
                continue;
            };
            let runtime = SessionId(runtime_id.to_string());
            if sessions.find_by_session_id(&runtime).await?.is_none() {
                tracing::warn!(
                    target: "nexus::delivery",
                    message_id = %obligation.message_id,
                    agent_id = %obligation.recipient_agent_id,
                    runtime_id,
                    "pending continuity row has no reconstructed runtime; preserving it for a later wake"
                );
                continue;
            }
            let message: Message = match serde_json::from_str(&obligation.payload_json) {
                Ok(message) => message,
                Err(error) => {
                    tracing::error!(
                        target: "nexus::delivery",
                        message_id = %obligation.message_id,
                        agent_id = %obligation.recipient_agent_id,
                        %error,
                        "pending continuity payload is malformed; preserving it without injection"
                    );
                    continue;
                }
            };
            if messages
                .get(&message.project.0, &message.id)
                .await?
                .is_none()
            {
                messages
                    .insert_with_agents(&message, None, Some(&obligation.recipient_agent_id))
                    .await?;
            }
            inbox.enqueue(&message.id, &runtime).await?;
            restored += 1;
        }
        Ok(restored)
    }

    /// Return an active pending-respawn backoff entry for integration verification.
    #[doc(hidden)]
    pub fn pending_respawn_backoff_for(
        &self,
        session: &SessionId,
        now_ms: i64,
    ) -> Option<PendingRespawnBackoff> {
        self.pending_respawn_backoff
            .lock()
            .expect("pending respawn backoff map poisoned")
            .get(session)
            .copied()
            .filter(|state| state.next_retry_at > now_ms)
    }

    /// Clear process-local pending-respawn backoff state.
    #[doc(hidden)]
    pub fn clear_pending_respawn_backoff(&self, session: &SessionId) {
        self.pending_respawn_backoff
            .lock()
            .expect("pending respawn backoff map poisoned")
            .remove(session);
    }

    /// Record one pending-recipient respawn failure and return its retry state.
    #[doc(hidden)]
    pub fn record_pending_respawn_failure(
        &self,
        session: &SessionId,
        now_ms: i64,
    ) -> PendingRespawnBackoff {
        let mut map = self
            .pending_respawn_backoff
            .lock()
            .expect("pending respawn backoff map poisoned");
        let failures = map
            .get(session)
            .map(|state| state.failures.saturating_add(1))
            .unwrap_or(1);
        let state = PendingRespawnBackoff {
            failures,
            next_retry_at: now_ms + pending_respawn_delay_ms(failures),
        };
        map.insert(session.clone(), state);
        state
    }

    /// Preserve a pre-injection delivery while a non-dead recipient is bootstrapping.
    ///
    /// Failures one and two only advance process-local backoff. Failure three records the durable
    /// terminal outcome. `message_id=None` settles all pending rows for boot recovery; a concrete
    /// message settles only the route that triggered this wake.
    pub(crate) async fn record_revive_failure_or_terminalize(
        &self,
        message_id: Option<&nexus_contracts::MessageId>,
        session: &SessionId,
        reason: &str,
        source: &str,
    ) -> PendingRespawnBackoff {
        let backoff = self.record_pending_respawn_failure(session, now());
        let terminal = backoff.failures >= PENDING_RESPAWN_TOMBSTONE_FAILURES;
        if terminal {
            let details = serde_json::json!({
                "source": source,
                "reviveFailures": backoff.failures,
            })
            .to_string();
            let result = match message_id {
                Some(message_id) => {
                    Inbox::new(&self.store)
                        .mark_delivery_error(
                            message_id,
                            session,
                            TARGET_UNREACHABLE_ERROR_CODE,
                            reason,
                            Some(&details),
                        )
                        .await
                }
                None => {
                    Inbox::new(&self.store)
                        .mark_recipient_pending_error(
                            session,
                            TARGET_UNREACHABLE_ERROR_CODE,
                            reason,
                            Some(&details),
                        )
                        .await
                }
            };
            if let Err(error) = result {
                tracing::error!(
                    target: "nexus::delivery",
                    %error,
                    %session,
                    failures = backoff.failures,
                    "failed to persist exhausted revive delivery outcome"
                );
            }
        }
        tracing::warn!(
            target: "nexus::revive",
            %session,
            failures = backoff.failures,
            next_retry_at = backoff.next_retry_at,
            terminal,
            source,
            reason,
            "pre-injection recipient revive failed"
        );
        backoff
    }
}
