//! Post-commit wake settlement for notification-source topic deliveries.
//!
//! Source ingress owns authentication and the canonical bus write. This module owns the daemon
//! edge after that commit: revive each exact agent recipient, preserving non-dead bootstrap rows
//! for the bounded retry policy while keeping durably dead recipients terminal.

use nexus_contracts::{ContractError, MessageId};
use nexus_store::repos::inbox::{TARGET_DEAD_ERROR_CODE, TARGET_UNREACHABLE_ERROR_CODE};
use nexus_store::repos::{Agents, Inbox, Sessions};

use super::app::AppState;
use super::services::source::SourceDeliveryRef;

impl AppState {
    /// Apply the post-commit wake policy to the exact delivery rows created by a source publish.
    /// Source ingress is accepted only after the bus transaction commits. Each agent is revived
    /// concurrently; a durable-dead target is terminal immediately, while a non-dead bootstrap
    /// failure remains pending for the shared three-attempt recovery policy.
    pub(crate) async fn wake_source_deliveries(
        &self,
        message_id: &MessageId,
        deliveries: Vec<SourceDeliveryRef>,
    ) {
        let mut wakes = tokio::task::JoinSet::new();
        for delivery in deliveries {
            let this = self.clone();
            let message_id = message_id.clone();
            wakes.spawn(async move {
                this.wake_source_delivery(&message_id, delivery).await;
            });
        }
        while let Some(result) = wakes.join_next().await {
            if let Err(error) = result {
                tracing::error!(
                    target: "nexus::source",
                    error = %error,
                    "source subscriber wake task terminated unexpectedly"
                );
            }
        }
    }

    async fn wake_source_delivery(&self, message_id: &MessageId, delivery: SourceDeliveryRef) {
        let sessions = Sessions::new(&self.store);
        let row = match delivery.recipient_agent_id.as_deref() {
            Some(agent_id) => {
                let agents = Agents::new(&self.store);
                match agents.lifecycle_for_id(agent_id).await {
                    Ok((Some(state), reason)) if state == "dead" => {
                        let reason = reason.unwrap_or_else(|| "durable agent is dead".into());
                        self.mark_source_wake_error(
                            message_id,
                            &delivery,
                            TARGET_DEAD_ERROR_CODE,
                            &format!("source subscriber is dead: {reason}"),
                            serde_json::json!({
                                "source": "source_push",
                                "recipientAgentId": agent_id,
                                "recipientSession": delivery.recipient_session.0.clone(),
                                "deadReason": reason,
                            }),
                        )
                        .await;
                        return;
                    }
                    Ok(_) => {}
                    Err(error) => {
                        tracing::warn!(
                            target: "nexus::source",
                            message = %message_id,
                            agent_id,
                            error = %error,
                            "could not read source subscriber lifecycle before revive"
                        );
                    }
                }
                sessions.find_by_agent_id(agent_id).await
            }
            None => {
                sessions
                    .find_by_session_id(&delivery.recipient_session)
                    .await
            }
        };

        let row = match row {
            Ok(Some(row)) if row.is_agent() => row,
            Ok(Some(_)) => return,
            Ok(None) => {
                self.mark_source_wake_error(
                    message_id,
                    &delivery,
                    TARGET_UNREACHABLE_ERROR_CODE,
                    "source subscriber has no resumable runtime",
                    serde_json::json!({
                        "source": "source_push",
                        "recipientAgentId": delivery.recipient_agent_id.clone(),
                        "recipientSession": delivery.recipient_session.0.clone(),
                    }),
                )
                .await;
                return;
            }
            Err(error) => {
                self.mark_source_wake_error(
                    message_id,
                    &delivery,
                    TARGET_UNREACHABLE_ERROR_CODE,
                    "source subscriber runtime lookup failed",
                    serde_json::json!({
                        "source": "source_push",
                        "recipientAgentId": delivery.recipient_agent_id.clone(),
                        "recipientSession": delivery.recipient_session.0.clone(),
                        "storeError": error.to_string(),
                    }),
                )
                .await;
                return;
            }
        };

        // A durable inbox subscription is an explicit consumer claim. The bus commit already rang
        // the shared held-receive bell; do not race that pull consumer with harness revival.
        if self.inbox_subscription_owns_delivery(&row.session_id).await {
            return;
        }

        let revived = match delivery.recipient_agent_id.as_deref() {
            Some(agent_id) => self.ensure_alive_agent(agent_id).await,
            None => match row.name.as_deref() {
                Some(name) => self.ensure_alive(name, &row.project).await,
                None => Err(ContractError {
                    code: nexus_contracts::codes::INVALID_PARAMS,
                    message: format!("source subscriber runtime {} is unnamed", row.session_id.0),
                }),
            },
        };

        if let Err(error) = revived {
            self.record_revive_failure_or_terminalize(
                Some(message_id),
                &delivery.recipient_session,
                &format!("source subscriber revive failed: {}", error.message),
                "source_push",
            )
            .await;
        }
    }

    async fn mark_source_wake_error(
        &self,
        message_id: &MessageId,
        delivery: &SourceDeliveryRef,
        code: &str,
        reason: &str,
        details: serde_json::Value,
    ) {
        let details = details.to_string();
        match Inbox::new(&self.store)
            .mark_delivery_error(
                message_id,
                &delivery.recipient_session,
                code,
                reason,
                Some(&details),
            )
            .await
        {
            Ok(1) => tracing::warn!(
                target: "nexus::source",
                message = %message_id,
                recipient = %delivery.recipient_session,
                error_code = code,
                reason,
                "source subscriber wake failed; delivery is terminal"
            ),
            Ok(affected) => tracing::warn!(
                target: "nexus::source",
                message = %message_id,
                recipient = %delivery.recipient_session,
                error_code = code,
                affected,
                "source wake failure did not transition exactly one delivery row"
            ),
            Err(error) => tracing::error!(
                target: "nexus::source",
                message = %message_id,
                recipient = %delivery.recipient_session,
                error_code = code,
                error = %error,
                "failed to persist source subscriber wake error"
            ),
        }
    }
}
