//! Notification sources: registration, tokens, enable/rotate/delete, and
//! external push-in (`push_as_source`) + source-scoped reads. Extracted from
//! AppState verbatim (R5.1 Task 1); owns only the ports these paths use.

use std::sync::Arc;

use libsql::params;
use nexus_common::{new_source_token, now, NexusError};
use nexus_contracts::{
    BusPort, Caller, ContractError, Kind, Message, MessageId, PushRequest, PushResponse,
    ReadRequest, SendRequest, SendTarget, SessionId, Tier,
};
use nexus_store::repos::Sources;
use nexus_store::Store;

/// One exact recipient row committed by a source publish.
///
/// Source wake uses the committed `in_flight` rows rather than re-reading subscriptions after the
/// publish, so a concurrent subscribe/unsubscribe cannot make Nexus revive a target that did not
/// receive this canonical message (or omit one that did).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SourceDeliveryRef {
    pub recipient_session: SessionId,
    pub recipient_agent_id: Option<String>,
}

/// Internal result of a committed source publish.
///
/// The public response remains the source receipt. The daemon composition root additionally needs
/// the canonical message id and exact durable recipients to apply its post-commit wake policy.
pub(crate) struct SourcePushOutcome {
    pub response: PushResponse,
    pub message_id: Option<MessageId>,
    pub deliveries: Vec<SourceDeliveryRef>,
}

#[derive(Clone)]
pub struct SourceService {
    store: Arc<Store>,
    bus: Arc<dyn BusPort>,
}

impl SourceService {
    pub fn new(store: Arc<Store>, bus: Arc<dyn BusPort>) -> Self {
        Self { store, bus }
    }

    /// The `read` registry method: fetch a message by id, scoped to the caller's project.
    pub async fn read_message(
        &self,
        caller: &Caller,
        req: &ReadRequest,
    ) -> Result<Message, ContractError> {
        let repo = nexus_store::repos::messages::Messages::new(&self.store);
        match repo.get(&caller.project, &req.id).await {
            Ok(Some(m)) => Ok(m),
            Ok(None) => Err(NexusError::NotFound(req.id.0.clone()).to_contract_error()),
            Err(e) => Err(e.to_contract_error()),
        }
    }

    /// Push an event from a named notification source onto the bus, fanning out to the topic's
    /// subscribers.
    ///
    /// # Steps
    /// 1. Reject empty `req.body` → `INVALID_PARAMS`.
    /// 2. Resolve the source row; reject if missing (`NOT_FOUND`) or `!enabled` (`UNAUTHORIZED`).
    /// 3. Resolve the topic: `req.topic` override or the source's default.
    /// 4. Fold `meta` into the body so the agent receives it verbatim (see note below).
    /// 5. Build a derived `Caller` naming the source so provenance says who pushed.
    /// 6. Publish to the topic via `bus.send`, carrying the durable command idempotency key when
    ///    the caller came through retryable command ingress.
    /// 7. Record `touch_fired` on the source row.
    /// 8. Return the public response plus the canonical message id and exact committed recipients
    ///    for the daemon's post-commit wake policy.
    ///
    /// ## Meta passthrough decision
    ///
    /// `SendRequest` has no `meta` field. Adding one would require touching the bus, the store
    /// schema, and the message model — a large diff. The smallest-diff approach is to fold `meta`
    /// into the body text: when `req.meta` is `Some(m)`, the bus body becomes
    /// `"{body}\n\n<meta>{json}</meta>"`. The agent reads only the `body` field rendered inside
    /// `<nexus …>{body}</nexus>`, so both the human-readable body AND the meta block are visible
    /// verbatim. No schema changes needed.
    ///
    /// ## Source-as-sender decision
    ///
    /// We build a derived `Caller` with `name = req.source` so the message's `from_name` (written
    /// by `broadcast_messages`) attributes the event to the source name rather than the operator.
    /// Verification (reading `nexus-bus` router `resolve_topic`): a topic publish does NOT check
    /// whether the caller is a registered member; topic names are global and project is metadata.
    /// Publishing to a topic is open (no membership gate). The derived caller's `project` is the
    /// operator's project (where the subscribed agents live), so topic resolution scopes correctly.
    pub(crate) async fn push_as_source(
        &self,
        caller: &Caller,
        req: PushRequest,
        command_idempotency_key: Option<String>,
    ) -> Result<SourcePushOutcome, ContractError> {
        // 1. Validate body.
        if req.body.is_empty() {
            return Err(ContractError {
                code: nexus_contracts::codes::INVALID_PARAMS,
                message: "body must not be empty".into(),
            });
        }

        // 2. Resolve source row.
        let sources = Sources::new(&self.store);
        let row = sources
            .find(&req.source)
            .await
            .map_err(|e| e.to_contract_error())?
            .ok_or_else(|| ContractError {
                code: nexus_contracts::codes::NOT_FOUND,
                message: format!("source not found: {}", req.source),
            })?;
        if !row.enabled {
            return Err(ContractError {
                code: nexus_contracts::codes::UNAUTHORIZED,
                message: format!("source is disabled: {}", req.source),
            });
        }

        // 3. Resolve topic.
        let topic = req.topic.clone().unwrap_or(row.topic.clone());

        // 3b. A source must not fail because nobody is listening yet. A topic exists in the registry
        // only once it has been subscribed to; publishing to an unsubscribed topic would `NotFound`.
        // For fire-and-forget notifications that is a successful ZERO-delivery, not an error — so if
        // the topic is unknown, record the fire and return `queued_to: 0` without publishing.
        let topic_known = nexus_store::repos::Topics::new(&self.store)
            .exists(&topic)
            .await
            .map_err(|e| e.to_contract_error())?;
        if !topic_known {
            sources
                .touch_fired(&req.source, now())
                .await
                .map_err(|e| e.to_contract_error())?;
            return Ok(SourcePushOutcome {
                response: PushResponse {
                    topic,
                    message_id: None,
                    queued_to: 0,
                },
                message_id: None,
                deliveries: Vec::new(),
            });
        }

        // 4. Fold meta into body.
        let bus_body = match &req.meta {
            Some(m) => format!(
                "{}\n\n<meta>{}</meta>",
                req.body,
                serde_json::to_string(m).unwrap_or_default()
            ),
            None => req.body.clone(),
        };

        // 5. Build source-as-sender caller: source name as the `from` so provenance is attributed
        //    to the source, not the operator. Project = caller's project (agents live there).
        let source_caller = Caller {
            agent_id: None,
            session: caller.session.clone(),
            name: req.source.clone(),
            project: caller.project.clone(),
            tier: caller.tier,
        };

        // 6. Publish to topic via the bus publish fan-out.
        let ack = self
            .bus
            .send_with_kind(
                &source_caller,
                SendRequest {
                    to: SendTarget::Publish {
                        topic: topic.clone(),
                    },
                    summary: req.summary.clone(),
                    body: bus_body,
                    mention: vec![],
                    metadata: None,
                    idempotency_key: command_idempotency_key,
                },
                Kind::Notification,
            )
            .await?;

        // Read the transaction's exact fan-out, not the current subscription registry. The bus
        // has already committed the canonical message and every in_flight row before returning.
        let deliveries = self
            .committed_delivery_refs(&ack.message_id)
            .await
            .map_err(|e| e.to_contract_error())?;

        // 7. Record fire timestamp.
        sources
            .touch_fired(&req.source, now())
            .await
            .map_err(|e| e.to_contract_error())?;

        // 8. Return response.
        Ok(SourcePushOutcome {
            response: PushResponse {
                topic,
                message_id: Some(ack.message_id.clone()),
                queued_to: ack.fanout.unwrap_or(0),
            },
            message_id: Some(ack.message_id),
            deliveries,
        })
    }

    async fn committed_delivery_refs(
        &self,
        message_id: &MessageId,
    ) -> Result<Vec<SourceDeliveryRef>, NexusError> {
        // Reclaimed commands may resolve to an earlier canonical bus write. Only rows still
        // waiting for their first attempt are wakeable; injecting/delivered/acked/error rows are
        // terminal or already owned by the delivery loop and must never trigger another revive.
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT recipient_session, recipient_agent_id FROM in_flight \
                 WHERE message_id = ?1 AND state IN ('pending','notified') \
                 ORDER BY in_flight_id",
                params![message_id.0.clone()],
            )
            .await
            .map_err(|e| NexusError::Store(e.to_string()))?;
        let mut deliveries = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|e| NexusError::Store(e.to_string()))?
        {
            let recipient_session = row
                .get::<String>(0)
                .map_err(|e| NexusError::Store(e.to_string()))?;
            let recipient_agent_id = row
                .get::<Option<String>>(1)
                .map_err(|e| NexusError::Store(e.to_string()))?;
            deliveries.push(SourceDeliveryRef {
                recipient_session: SessionId(recipient_session),
                recipient_agent_id,
            });
        }
        Ok(deliveries)
    }

    /// Map a [`nexus_store::repos::SourceRow`] into the contract [`Source`] type, dropping the
    /// token (the `Source` wire type has no token field — tokens are returned only on register/rotate).
    fn row_to_source_contract(row: nexus_store::repos::SourceRow) -> nexus_contracts::Source {
        nexus_contracts::Source {
            name: row.name,
            topic: row.topic,
            enabled: row.enabled,
            created_at: row.created_at,
            last_fired_at: row.last_fired_at,
        }
    }

    /// Admin-guarded: register a new notification source and return its plaintext token.
    pub async fn register_source(
        &self,
        caller: &Caller,
        req: nexus_contracts::SourceRegisterRequest,
    ) -> Result<nexus_contracts::SourceRegisterResponse, ContractError> {
        nexus_identity::tier_guard(caller, Tier::Admin).map_err(|e| e.to_contract_error())?;
        let token = new_source_token();
        let topic = req.topic.clone().unwrap_or_else(|| req.name.clone());
        let created_at = now();
        Sources::new(&self.store)
            .create(&req.name, &token, &topic, created_at)
            .await
            .map_err(|e| e.to_contract_error())?;
        let source = nexus_contracts::Source {
            name: req.name,
            topic,
            enabled: true,
            created_at,
            last_fired_at: None,
        };
        Ok(nexus_contracts::SourceRegisterResponse { source, token })
    }

    /// List all registered notification sources (token NOT included in the returned `Source` type).
    pub async fn list_sources(&self) -> Result<nexus_contracts::SourceListResponse, ContractError> {
        let rows = Sources::new(&self.store)
            .list()
            .await
            .map_err(|e| e.to_contract_error())?;
        let sources = rows.into_iter().map(Self::row_to_source_contract).collect();
        Ok(nexus_contracts::SourceListResponse { sources })
    }

    /// Fetch a single source by name (NOT_FOUND if absent).
    pub async fn show_source(&self, name: &str) -> Result<nexus_contracts::Source, ContractError> {
        Sources::new(&self.store)
            .find(name)
            .await
            .map_err(|e| e.to_contract_error())?
            .map(Self::row_to_source_contract)
            .ok_or_else(|| ContractError {
                code: nexus_contracts::codes::NOT_FOUND,
                message: format!("source not found: {name}"),
            })
    }

    /// Enable or disable a source; returns the updated `Source` (NOT_FOUND if absent).
    pub async fn set_source_enabled(
        &self,
        name: &str,
        enabled: bool,
    ) -> Result<nexus_contracts::Source, ContractError> {
        let sources = Sources::new(&self.store);
        // Verify it exists before updating (set_enabled silently no-ops on a missing name).
        sources
            .find(name)
            .await
            .map_err(|e| e.to_contract_error())?
            .ok_or_else(|| ContractError {
                code: nexus_contracts::codes::NOT_FOUND,
                message: format!("source not found: {name}"),
            })?;
        sources
            .set_enabled(name, enabled)
            .await
            .map_err(|e| e.to_contract_error())?;
        // Re-fetch to return the post-update state.
        sources
            .find(name)
            .await
            .map_err(|e| e.to_contract_error())?
            .map(Self::row_to_source_contract)
            .ok_or_else(|| ContractError {
                code: nexus_contracts::codes::NOT_FOUND,
                message: format!("source not found after update: {name}"),
            })
    }

    /// Admin-guarded: rotate the token for a source; returns the new plaintext token (NOT_FOUND if absent).
    pub async fn rotate_source(
        &self,
        caller: &Caller,
        name: &str,
    ) -> Result<nexus_contracts::SourceTokenResponse, ContractError> {
        nexus_identity::tier_guard(caller, Tier::Admin).map_err(|e| e.to_contract_error())?;
        let sources = Sources::new(&self.store);
        // Verify it exists before rotating.
        sources
            .find(name)
            .await
            .map_err(|e| e.to_contract_error())?
            .ok_or_else(|| ContractError {
                code: nexus_contracts::codes::NOT_FOUND,
                message: format!("source not found: {name}"),
            })?;
        let token = new_source_token();
        sources
            .set_token(name, &token)
            .await
            .map_err(|e| e.to_contract_error())?;
        Ok(nexus_contracts::SourceTokenResponse {
            name: name.to_string(),
            token,
        })
    }

    /// Delete a source; returns a `SourceRef` echoing the name.
    pub async fn delete_source(
        &self,
        name: &str,
    ) -> Result<nexus_contracts::SourceRef, ContractError> {
        Sources::new(&self.store)
            .delete(name)
            .await
            .map_err(|e| e.to_contract_error())?;
        Ok(nexus_contracts::SourceRef {
            name: name.to_string(),
        })
    }

    /// Admin-guarded: return the plaintext token for a source (for gateway HMAC verification).
    pub async fn source_token(
        &self,
        caller: &Caller,
        name: &str,
    ) -> Result<nexus_contracts::SourceTokenResponse, ContractError> {
        nexus_identity::tier_guard(caller, Tier::Admin).map_err(|e| e.to_contract_error())?;
        let row = Sources::new(&self.store)
            .find(name)
            .await
            .map_err(|e| e.to_contract_error())?
            .ok_or_else(|| ContractError {
                code: nexus_contracts::codes::NOT_FOUND,
                message: format!("source not found: {name}"),
            })?;
        Ok(nexus_contracts::SourceTokenResponse {
            name: row.name,
            token: row.token,
        })
    }
}
