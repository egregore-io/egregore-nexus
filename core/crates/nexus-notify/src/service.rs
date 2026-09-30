//! The [`Notify`] service — `impl NotifyPort` (backend §7). Internal ingest only: there is **no**
//! HTTP `/notify` route in the daemon. The TS gateway verifies external producers and enqueues the
//! matching signed command intent. The daemon worker independently verifies that envelope and calls
//! `ingest_verified` with the durable command idempotency root. Ordinary `ingest` is the legacy
//! unverified seam and never inherits Gateway trust.
//!
//! The whole §7 flow:
//! 1. **Record audit** — always write a `notifications` row with `hmac_ok` (verified or not).
//! 2. **Bad signature → drop** — `routed_to` stays empty, no `notification` message, no Pub feed,
//!    **no agent path**. Returns a successful response (the producer's call is acknowledged).
//! 3. **Ingest message** — store a `notification`-kind message (durable, source/topic set).
//! 4a. **Pub feed (always)** — publish to the `pub` topic so the monitor renders it.
//! 4b. **Route (if any)** — [`RoutingRules::resolve`] → for each recipient, a bus dispatch
//!     (in-flight + bell → §2 delivery). **Pub = monitor, routing = dispatch.**
//! 5. **Emit** `notification.received` with `routed_to` (web console audit).
//!
//! Verified public ingest derives one deterministic standalone message id, notification audit id,
//! and a distinct Message Post idempotency key for Pub, topic, and each DM. Command reclaim can
//! therefore resume partial fan-out without duplicating any canonical fact, delivery row, harness
//! injection, or audit event.
//!
//! `forward` is the admin one-shot ad-hoc forward (tier-gated, **not** a standing rule). `channel`
//! manages the Pub-feed topic / route config.

use std::collections::VecDeque;
use std::sync::Arc;

use nexus_common::{now, NexusError};
use nexus_contracts::admin::{ChannelOp, ChannelRequest, RouteForwardRequest};
use nexus_contracts::enums::{Kind, Scope, Tier};
use nexus_contracts::events::WsEvent;
use nexus_contracts::ids::{MessageId, ProjectId, TopicId};
use nexus_contracts::message::{Message, Provenance};
use nexus_contracts::notify::{NotifyRequest, NotifyResponse};
use nexus_contracts::ports::{BusPort, Caller, EventSink, NotifyPort, PortResult, PreparedBusSend};
use nexus_contracts::send::{SendRequest, SendTarget};
use nexus_store::repos::{Messages, Notifications, Topics};
use nexus_store::Store;
use sha2::{Digest, Sha256};

use crate::error::to_port;
use crate::ingest::{render_body, render_summary, system_caller, NOTIFY_PROJECT};
use crate::pubfeed::pub_topic;
use crate::routing::RoutingRules;

/// The notification service. Owns the shared store (audit + message + subscription reads), the bus
/// port (Pub publish + routed dispatch), an event sink, and the standing routing rules.
pub struct Notify {
    store: Arc<Store>,
    bus: Arc<dyn BusPort>,
    events: Arc<dyn EventSink>,
    rules: RoutingRules,
}

impl Notify {
    /// Build the notify service over the shared store + the bus port + an event sink + the standing
    /// route-by-source/topic rules.
    pub fn new(
        store: Arc<Store>,
        bus: Arc<dyn BusPort>,
        events: Arc<dyn EventSink>,
        rules: RoutingRules,
    ) -> Self {
        Notify {
            store,
            bus,
            events,
            rules,
        }
    }

    /// The full ingest path (steps 1–5 above), in `NexusError` terms.
    async fn ingest_inner(
        &self,
        req: NotifyRequest,
        hmac_ok: bool,
    ) -> Result<NotifyResponse, NexusError> {
        self.ingest_inner_for(req, hmac_ok, None, Vec::new(), None)
            .await
    }

    /// Ingest plus explicit one-shot recipients. This reuses the normal Pub/audit path, then routes
    /// the same notification body to additional named agents via DM.
    async fn ingest_inner_for(
        &self,
        req: NotifyRequest,
        hmac_ok: bool,
        explicit_project: Option<String>,
        explicit_recipients: Vec<String>,
        idempotency_root: Option<&str>,
    ) -> Result<NotifyResponse, NexusError> {
        let payload_json = render_body(&req);

        // ---- Step 1+2: bad signature → record (hmac_ok=false, empty routed_to) and DROP. ----
        if !hmac_ok {
            let notif_id = if self.store.has_split_authority() {
                format!("n_{}", nexus_common::new_message_id().0)
            } else {
                self.store_notifications()
                    .record(
                        Some(&req.source),
                        req.topic.as_deref(),
                        false,
                        &payload_json,
                        "", // dropped: no recipients, no agent path
                    )
                    .await?
            };
            // No message, no Pub feed, no routing, no event — the body never touches an agent.
            return Ok(NotifyResponse {
                notif_id: MessageId(notif_id),
                routed_to: Vec::new(),
                hmac_ok: false,
            });
        }

        let mut caller = system_caller(&req);
        if let Some(project) = explicit_project {
            caller.project = project;
        }
        let topic_for_msg = req.topic.clone().unwrap_or_else(|| pub_topic().to_string());
        let summary = render_summary(&req);
        let subscriptions = self.subscriptions_for(req.topic.as_deref()).await?;
        let recipients = self.rules.resolve(&req, &subscriptions);
        let explicit_recipients = dedupe_explicit_recipients(explicit_recipients, &[]);

        let pub_req = SendRequest {
            to: SendTarget::Publish {
                topic: pub_topic().to_string(),
            },
            summary: Some(summary.clone()),
            body: payload_json.clone(),
            mention: Vec::new(),
            metadata: None,
            idempotency_key: notification_effect_key(idempotency_root, "pub", pub_topic()),
        };
        let routed_topic_req = req.topic.as_deref().and_then(|topic| {
            (!recipients.is_empty()).then(|| SendRequest {
                to: SendTarget::Publish {
                    topic: topic.to_string(),
                },
                summary: Some(summary.clone()),
                body: payload_json.clone(),
                mention: Vec::new(),
                metadata: None,
                idempotency_key: notification_effect_key(idempotency_root, "topic", topic),
            })
        });
        let standing_dm_reqs = if req.topic.is_none() {
            recipients
                .iter()
                .map(|name| {
                    (
                        name.clone(),
                        notification_dm_request(
                            name,
                            &summary,
                            &payload_json,
                            notification_effect_key(idempotency_root, "dm", name),
                        ),
                    )
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let explicit_dm_reqs = explicit_recipients
            .iter()
            .map(|name| {
                (
                    name.clone(),
                    notification_dm_request(
                        name,
                        &summary,
                        &payload_json,
                        notification_effect_key(idempotency_root, "dm", name),
                    ),
                )
            })
            .collect::<Vec<_>>();

        // A verified command is one logical ingest with several bus effects. Prepare every target
        // and `before_send` hook before the standalone notification message, Pub row, delivery
        // row, or wake can commit. The concrete bus freezes each resolved recipient set inside the
        // preparation, so alias changes between this barrier and commit cannot reroute an effect.
        let mut prepared_effects = idempotency_root.is_some().then(VecDeque::new);
        if let Some(prepared) = prepared_effects.as_mut() {
            let requests = std::iter::once(&pub_req)
                .chain(routed_topic_req.iter())
                .chain(
                    standing_dm_reqs
                        .iter()
                        .chain(explicit_dm_reqs.iter())
                        .map(|(_, request)| request),
                );
            for request in requests {
                match self
                    .bus
                    .prepare_send(&caller, request.clone(), Some(Kind::Notification))
                    .await
                {
                    Ok(effect) => prepared.push_back(effect),
                    Err(error) => {
                        discard_prepared_effects(self.bus.as_ref(), &caller, prepared).await;
                        return Err(NexusError::from(error));
                    }
                }
            }
        }

        // ---- Step 3: ingest as a durable `notification`-kind message. ----
        let (msg_id, _) = match self
            .store_notification_message(&caller, &req, &topic_for_msg, &summary, idempotency_root)
            .await
        {
            Ok(message_id) => message_id,
            Err(error) => {
                if let Some(prepared) = prepared_effects.as_mut() {
                    discard_prepared_effects(self.bus.as_ref(), &caller, prepared).await;
                }
                return Err(error);
            }
        };
        for effect in Messages::new(&self.store)
            .gateway_projection_effects(&msg_id)
            .await?
        {
            self.events.project(effect).await;
        }

        // ---- Step 4a: Pub feed — ALWAYS append to the `pub` monitor topic. ----
        // A publish to a topic fans out only to its explicit subscribers (a monitor view); this is
        // never, by itself, a push into an agent's turn path.
        // A Pub publish with no subscribers is a no-op fan-out, not an error.
        if let Err(error) =
            dispatch_notification_effect(self.bus.as_ref(), &caller, pub_req, &mut prepared_effects)
                .await
        {
            if prepared_effects.is_some() {
                // A durable verified ingest may be reclaimed after cancellation or a partial
                // commit. Leave every accepted preparation in the concrete bus ledger so the
                // same idempotency root reuses the frozen targets and hook output.
                return Err(NexusError::from(error));
            }
            tracing::debug!(%error, "pub-feed publish returned a port error (treated as soft)");
        }

        // ---- Step 4b: routed dispatch (in-flight + bell → §2 delivery). ----
        // A topic match dispatches to that topic's subscribers: a single topic publish fans out to
        // exactly those sessions (the bus writes one in-flight row + rings one bell each). Standing
        // route-by-source rules dispatch to their named target via a DM. Either way `routed_to`
        // records the resolved recipient set for the web console audit.
        let mut routed_to: Vec<String> = Vec::new();
        if req.topic.is_some() {
            if let Some(route_req) = routed_topic_req {
                match dispatch_notification_effect(
                    self.bus.as_ref(),
                    &caller,
                    route_req,
                    &mut prepared_effects,
                )
                .await
                {
                    Ok(_) => routed_to.extend(recipients.iter().cloned()),
                    Err(error) => {
                        if prepared_effects.is_some() {
                            return Err(NexusError::from(error));
                        }
                        tracing::warn!(topic = %topic_for_msg, %error, "notification topic dispatch failed");
                    }
                }
            }
        } else {
            // No topic but a standing route-by-source rule matched: one-shot DM per target.
            for (name, dm) in standing_dm_reqs {
                match send_notification_dm(
                    self.bus.as_ref(),
                    &caller,
                    dm,
                    &name,
                    &mut prepared_effects,
                )
                .await
                {
                    Ok(true) => routed_to.push(name),
                    Ok(false) => {}
                    Err(error) => {
                        return Err(error);
                    }
                }
            }
        }

        for (name, dm) in explicit_dm_reqs {
            if routed_to.iter().any(|routed| routed == &name) {
                discard_next_prepared_effect(self.bus.as_ref(), &caller, &mut prepared_effects)
                    .await;
                continue;
            }
            match send_notification_dm(self.bus.as_ref(), &caller, dm, &name, &mut prepared_effects)
                .await
            {
                Ok(true) => routed_to.push(name),
                Ok(false) => {}
                Err(error) => return Err(error),
            }
        }

        if let Some(prepared) = prepared_effects.as_mut() {
            debug_assert!(
                prepared.is_empty(),
                "every prepared effect must be consumed"
            );
            discard_prepared_effects(self.bus.as_ref(), &caller, prepared).await;
        }

        // ---- Record the audit row with the resolved recipients. ----
        let routed_csv = routed_to.join(",");
        // Gateway's canonical notification audit is keyed by this standalone message id. Reuse
        // it for the compatibility response/local audit so every authority sees one logical id.
        let stable_notif_id = idempotency_root.is_some().then(|| msg_id.0.clone());
        let (notif_id, audit_inserted, routed_to) = if self.store.has_split_authority() {
            (
                stable_notif_id
                    .unwrap_or_else(|| format!("n_{}", nexus_common::new_message_id().0)),
                true,
                routed_to,
            )
        } else if let Some(stable_notif_id) = stable_notif_id {
            let (row, inserted) = self
                .store_notifications()
                .record_once(
                    &stable_notif_id,
                    Some(&req.source),
                    req.topic.as_deref(),
                    true,
                    &payload_json,
                    &routed_csv,
                )
                .await?;
            (
                row.notif_id,
                inserted,
                csv_recipients(row.routed_to.as_deref()),
            )
        } else {
            (
                self.store_notifications()
                    .record(
                        Some(&req.source),
                        req.topic.as_deref(),
                        true,
                        &payload_json,
                        &routed_csv,
                    )
                    .await?,
                true,
                routed_to,
            )
        };

        // ---- Step 5: emit `notification.received` with routed_to (web console). ----
        if audit_inserted {
            self.events
                .emit(WsEvent::NotificationReceived {
                    notif_id: MessageId(notif_id.clone()),
                    routed_to: routed_to.clone(),
                })
                .await;
        }

        let _ = msg_id; // the durable message id; the response keys off the audit notif_id.
        Ok(NotifyResponse {
            notif_id: MessageId(notif_id),
            routed_to,
            hmac_ok: true,
        })
    }

    /// Store the durable `notification`-kind message (scope=topic, kind=notification provenance).
    async fn store_notification_message(
        &self,
        caller: &Caller,
        req: &NotifyRequest,
        topic: &str,
        summary: &str,
        idempotency_root: Option<&str>,
    ) -> Result<(MessageId, bool), NexusError> {
        let created_at = now();
        let stable_id = notification_ingest_id(idempotency_root, "message", "m_notify");
        let id = MessageId(
            stable_id.unwrap_or_else(|| format!("m_{}", nexus_common::new_message_id().0)),
        );
        if idempotency_root.is_some()
            && Messages::new(&self.store)
                .get(NOTIFY_PROJECT, &id)
                .await?
                .is_some()
        {
            return Ok((id, false));
        }
        let provenance = Provenance {
            from: caller.name.clone(),
            kind: Kind::Notification,
            locality: Default::default(),
            access: None,
            thread: None,
            topic: Some(topic.to_string()),
            stamp: None,
        };
        let msg = Message {
            id: id.clone(),
            project: ProjectId(NOTIFY_PROJECT.into()),
            from: caller.name.clone(),
            scope: Scope::Topic,
            thread: None,
            topic: Some(TopicId(topic.to_string())),
            body: render_body(req),
            summary: Some(summary.to_string()),
            provenance,
            created_at,
        };
        match Messages::new(&self.store).insert(&msg).await {
            Ok(message_id) => Ok((message_id, true)),
            Err(error) if idempotency_root.is_some() && is_unique_constraint(&error) => {
                match Messages::new(&self.store).get(NOTIFY_PROJECT, &id).await? {
                    Some(_) => Ok((id, false)),
                    None => Err(error),
                }
            }
            Err(error) => Err(error),
        }
    }

    /// Read the `(topic, subscriber)` pairs the resolver needs. For a topic-bearing notification we
    /// only need that topic's subscribers; with no topic there are none (Pub-only).
    async fn subscriptions_for(
        &self,
        topic: Option<&str>,
    ) -> Result<Vec<(String, String)>, NexusError> {
        match topic {
            Some(t) => {
                let subs = Topics::new(&self.store).subscribers(t).await?;
                Ok(subs.into_iter().map(|name| (t.to_string(), name)).collect())
            }
            None => Ok(Vec::new()),
        }
    }

    fn store_notifications(&self) -> Notifications<'_> {
        Notifications::new(&self.store)
    }

    /// The admin ad-hoc forward (§7/§8): a one-shot push of one recorded notification to one
    /// agent/thread. Tier-gated; **not** a standing rule, **not** a message-path insertion.
    async fn forward_inner(&self, caller: &Caller, req: RouteForwardRequest) -> PortResult<()> {
        if caller.tier != Tier::Admin {
            return Err(NexusError::Unauthorized.to_contract_error());
        }
        // Resolve the recorded notification for its payload (audit-backed forward).
        let row = to_port(self.store_notifications().get(&req.notif.0).await)?
            .ok_or_else(|| NexusError::NotFound(req.notif.0.clone()).to_contract_error())?;
        let body = row.payload.unwrap_or_default();
        let summary = match (&row.source, &row.topic) {
            (Some(s), Some(t)) => format!("{}/{}", s, t),
            (Some(s), None) => s.clone(),
            _ => "notification".to_string(),
        };
        // One-shot DM to the target (an agent name or thread name). This is the dispatcher acting;
        // it is a single forward, not a standing route rule.
        let dm = SendRequest {
            to: SendTarget::dm_name(req.to),
            summary: Some(summary),
            body,
            mention: Vec::new(),
            metadata: None,
            idempotency_key: None,
        };
        self.bus
            .send_with_kind(caller, dm, Kind::Notification)
            .await
            .map(|_| ())
    }

    // (channel_inner below stays in NexusError terms — it only touches the store + tier gate.)

    /// Admin channel op (§8): manage the Pub-feed topic / route config. v4 wires create/delete to
    /// the topic registry; `setRoute` is recorded as a standing route rule by the daemon's config
    /// (out of this crate's store surface), so it is accepted as a no-op here.
    async fn channel_inner(&self, caller: &Caller, req: ChannelRequest) -> Result<(), NexusError> {
        if caller.tier != Tier::Admin {
            return Err(NexusError::Unauthorized);
        }
        let topics = Topics::new(&self.store);
        match req.op {
            ChannelOp::Create => topics.ensure(&req.topic, &caller.project).await,
            ChannelOp::Delete => {
                // No hard-delete of a topic in v4's store (subscriptions are cursor rows); ensure
                // it exists so the op is idempotent/observable. A real delete is a schema concern.
                topics.ensure(&req.topic, &caller.project).await
            }
            ChannelOp::SetRoute => Ok(()),
        }
    }
}

#[async_trait::async_trait]
impl NotifyPort for Notify {
    async fn ingest(&self, req: NotifyRequest, hmac_ok: bool) -> PortResult<NotifyResponse> {
        to_port(self.ingest_inner(req, hmac_ok).await)
    }

    async fn ingest_verified(
        &self,
        req: NotifyRequest,
        idempotency_root: String,
    ) -> PortResult<NotifyResponse> {
        to_port(
            self.ingest_inner_for(req, true, None, Vec::new(), Some(&idempotency_root))
                .await,
        )
    }

    async fn ingest_for(
        &self,
        req: NotifyRequest,
        hmac_ok: bool,
        project: String,
        recipients: Vec<String>,
    ) -> PortResult<NotifyResponse> {
        to_port(
            self.ingest_inner_for(req, hmac_ok, Some(project), recipients, None)
                .await,
        )
    }

    async fn forward(&self, caller: &Caller, req: RouteForwardRequest) -> PortResult<()> {
        self.forward_inner(caller, req).await
    }

    async fn channel(&self, caller: &Caller, req: ChannelRequest) -> PortResult<()> {
        to_port(self.channel_inner(caller, req).await)
    }
}

async fn send_notification_dm(
    bus: &dyn BusPort,
    caller: &Caller,
    dm: SendRequest,
    name: &str,
    prepared_effects: &mut Option<VecDeque<PreparedBusSend>>,
) -> Result<bool, NexusError> {
    let verified = prepared_effects.is_some();
    match dispatch_notification_effect(bus, caller, dm, prepared_effects).await {
        Ok(_) => Ok(true),
        Err(error) if verified => Err(NexusError::from(error)),
        Err(error) => {
            tracing::warn!(recipient = %name, %error, "notification route target failed");
            Ok(false)
        }
    }
}

async fn dispatch_notification_effect(
    bus: &dyn BusPort,
    caller: &Caller,
    request: SendRequest,
    prepared_effects: &mut Option<VecDeque<PreparedBusSend>>,
) -> PortResult<nexus_contracts::Ack> {
    match prepared_effects {
        Some(prepared) => {
            let effect = prepared
                .pop_front()
                .ok_or_else(|| nexus_contracts::ContractError {
                    code: nexus_contracts::codes::INTERNAL_ERROR,
                    message: "verified notification lost a prepared bus effect".into(),
                })?;
            bus.commit_prepared(caller, effect).await
        }
        None => {
            bus.send_with_kind(caller, request, Kind::Notification)
                .await
        }
    }
}

async fn discard_next_prepared_effect(
    bus: &dyn BusPort,
    caller: &Caller,
    prepared_effects: &mut Option<VecDeque<PreparedBusSend>>,
) {
    let Some(effect) = prepared_effects.as_mut().and_then(VecDeque::pop_front) else {
        return;
    };
    if let Err(error) = bus.discard_prepared(caller, effect).await {
        tracing::warn!(%error, "failed to discard skipped notification preparation");
    }
}

async fn discard_prepared_effects(
    bus: &dyn BusPort,
    caller: &Caller,
    prepared: &mut VecDeque<PreparedBusSend>,
) {
    while let Some(effect) = prepared.pop_front() {
        if let Err(error) = bus.discard_prepared(caller, effect).await {
            tracing::warn!(%error, "failed to discard rejected notification preparation");
        }
    }
}

fn notification_dm_request(
    name: &str,
    summary: &str,
    body: &str,
    idempotency_key: Option<String>,
) -> SendRequest {
    SendRequest {
        to: SendTarget::dm_name(name.to_string()),
        summary: Some(summary.to_string()),
        body: body.to_string(),
        mention: Vec::new(),
        metadata: None,
        idempotency_key,
    }
}

/// Derive one fixed-width Message Post key per durable notification effect. Length-prefixing every
/// component prevents ambiguous concatenations; hashing keeps producer keys and target labels out
/// of the stored key while remaining deterministic across command reclaim.
/// Deterministic Message Post idempotency key for one verified notification side effect.
///
/// The daemon command worker also uses this to locate the exact routed delivery after commit when
/// it must persist a target revival failure. Keeping one derivation prevents broad recipient-level
/// error updates from touching unrelated pending mail.
pub fn notification_effect_key(root: Option<&str>, effect: &str, target: &str) -> Option<String> {
    let root = root.map(str::trim).filter(|root| !root.is_empty())?;
    let mut hash = Sha256::new();
    for component in [root, effect, target] {
        hash.update((component.len() as u64).to_be_bytes());
        hash.update(component.as_bytes());
    }
    let digest = hash.finalize();
    let hex = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    Some(format!("notify-effect:{hex}"))
}

fn notification_ingest_id(root: Option<&str>, effect: &str, prefix: &str) -> Option<String> {
    let root = root.map(str::trim).filter(|root| !root.is_empty())?;
    let mut hash = Sha256::new();
    for component in [root, effect] {
        hash.update((component.len() as u64).to_be_bytes());
        hash.update(component.as_bytes());
    }
    Some(format!("{prefix}_{:x}", hash.finalize()))
}

fn csv_recipients(csv: Option<&str>) -> Vec<String> {
    csv.unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|recipient| !recipient.is_empty())
        .map(str::to_string)
        .collect()
}

fn is_unique_constraint(error: &NexusError) -> bool {
    matches!(error, NexusError::Store(message) if message.contains("UNIQUE constraint failed"))
}

fn dedupe_explicit_recipients(recipients: Vec<String>, already_routed: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    for name in recipients {
        let name = name.trim().to_string();
        if name.is_empty() {
            continue;
        }
        if already_routed.iter().any(|r| r == &name) || out.iter().any(|r| r == &name) {
            continue;
        }
        out.push(name);
    }
    out
}
