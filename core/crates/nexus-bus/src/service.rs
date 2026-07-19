//! The [`Bus`] service — `impl BusPort` (backend §3). The one `to` routing contract end to end:
//! [`Router::resolve`] (pure lookup) → message policy check → [`broadcast_messages`] (one
//! `messages` row + N `in_flight` rows in one transaction) → ring bells via [`DispatchPort`] → emit
//! a committed bus event. Plus the thread (create/join/leave/members/list) and topic
//! (subscribe/unsubscribe/list) management ops.
//!
//! No orchestrator: nothing here reorders or inserts into the message path. Resolution is a lookup;
//! policy is allow/deny only; delivery is fan-out; `--mention` is stored as a soft highlight, not a
//! routing change.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use libsql::params;
use nexus_common::{new_thread_id, now, NexusError};
use nexus_contracts::enums::{Kind, Scope, Tier};
use nexus_contracts::events::WsEvent;
use nexus_contracts::hooks::{
    DeliveryTiming, HookAction, HookBeforeSendRequest, HookMessage, HookSender,
};
use nexus_contracts::ids::{MessageId, SessionId};
use nexus_contracts::notify::{NotifySendRequest, NotifyTarget};
use nexus_contracts::ports::{
    BusPort, Caller, DispatchPort, EventSink, IdentityPort, MessageHookPort, PortResult,
};
use nexus_contracts::send::{
    validate_send_body, validate_send_request, Ack, SendRequest, SendTarget,
};
use nexus_contracts::threads::{
    ArchiveThreadRequest, CreateThreadRequest, DeleteThreadRequest, JoinThreadRequest,
    LeaveThreadRequest, RenameThreadRequest, ThreadListResponse, ThreadMemberRequest,
    ThreadMembersRequest, ThreadMembersResponse, ThreadSummary,
};
use nexus_contracts::topics::{
    SubscribeRequest, SubscribeResponse, TopicListResponse, TopicSummary, UnsubscribeRequest,
};
use nexus_store::repos::{DeveloperEvents, Messages, Threads, Topics};
use nexus_store::Store;
use sha2::{Digest, Sha256};

use crate::broadcast_messages::{broadcast_messages, WriteSpec};
use crate::error::to_port;
use crate::policy::MessagePolicy;
use crate::router::{Recipient, Resolved, Router};
use crate::sql_batch::{literal, opt_literal, push_stmt};
use crate::{dm, thread, topic};

const LEGACY_DUPLICATE_WINDOW_MS: i64 = 2_000;

struct EvaluatedSend {
    request: SendRequest,
    timing: DeliveryTiming,
}

/// The bus service. Owns the shared store (sole writer), the realtime port (enqueue + bell), the
/// identity port (name resolution), and an event sink (web console WS broadcast).
pub struct Bus {
    store: Arc<Store>,
    realtime: Arc<dyn DispatchPort>,
    events: Arc<dyn EventSink>,
    router: Router,
    message_hooks: Option<Arc<dyn MessageHookPort>>,
    now: Arc<dyn Fn() -> i64 + Send + Sync>,
    /// Session kind is immutable for one runtime id. A rebind mints a new SessionId, naturally
    /// invalidating this cache without coupling Bus to identity registration callbacks.
    sender_kinds: Mutex<HashMap<SessionId, Kind>>,
}

impl Bus {
    /// Build a bus over the shared store + the realtime/identity ports + an event sink.
    pub fn new(
        store: Arc<Store>,
        realtime: Arc<dyn DispatchPort>,
        identity: Arc<dyn IdentityPort>,
        events: Arc<dyn EventSink>,
    ) -> Self {
        Self::build(store, realtime, identity, events, None, Arc::new(now))
    }

    /// Build a bus whose single canonical send boundary delegates to a Gateway-owned hook port.
    pub fn new_with_message_hooks(
        store: Arc<Store>,
        realtime: Arc<dyn DispatchPort>,
        identity: Arc<dyn IdentityPort>,
        events: Arc<dyn EventSink>,
        message_hooks: Arc<dyn MessageHookPort>,
    ) -> Self {
        Self::build(
            store,
            realtime,
            identity,
            events,
            Some(message_hooks),
            Arc::new(now),
        )
    }

    fn build(
        store: Arc<Store>,
        realtime: Arc<dyn DispatchPort>,
        identity: Arc<dyn IdentityPort>,
        events: Arc<dyn EventSink>,
        message_hooks: Option<Arc<dyn MessageHookPort>>,
        now: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> Self {
        let router = Router::new(store.clone(), identity);
        Bus {
            store,
            realtime,
            events,
            router,
            message_hooks,
            now,
            sender_kinds: Mutex::new(HashMap::new()),
        }
    }

    /// Build a bus with an injected clock for tests that exercise short timing windows.
    #[cfg(test)]
    pub fn new_with_clock(
        store: Arc<Store>,
        realtime: Arc<dyn DispatchPort>,
        identity: Arc<dyn IdentityPort>,
        events: Arc<dyn EventSink>,
        now_fn: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> Self {
        Self::build(store, realtime, identity, events, None, now_fn)
    }

    /// The whole send path: resolve `to` → write + fan-out atomically → emit the event → ack.
    /// `Ack.fanout` is the number of recipient inbox rows created for every Message Post scope:
    /// DM, thread, and topic. Producer idempotency keys and the short legacy duplicate window both
    /// return the original ack before touching the durable message log.
    async fn send_inner(
        &self,
        caller: &Caller,
        req: SendRequest,
        sender_kind: Option<Kind>,
    ) -> Result<Ack, NexusError> {
        validate_send_request(&req).map_err(|e| NexusError::Invalid(e.message))?;
        let resolved = self.router.resolve(caller, &req.to).await?;
        self.send_resolved(caller, req, sender_kind, resolved).await
    }

    async fn notify_inner(
        &self,
        caller: &Caller,
        req: NotifySendRequest,
    ) -> Result<Ack, NexusError> {
        validate_send_body(&req.body).map_err(|e| NexusError::Invalid(e.message))?;
        if req
            .source
            .as_deref()
            .is_some_and(|source| source.trim().is_empty())
        {
            return Err(NexusError::Invalid(
                "notification source must not be empty".into(),
            ));
        }
        let resolved = self.router.resolve_notify(caller, &req.target).await?;
        let source = req
            .source
            .as_deref()
            .map(str::trim)
            .filter(|source| !source.is_empty())
            .unwrap_or(&caller.name)
            .to_string();
        let mut source_caller = caller.clone();
        source_caller.name = source.clone();
        // Local IPC and the authenticated gateway are the trust boundaries. The notification
        // transport does not add a second policy system after the target was accepted there.
        source_caller.tier = Tier::Admin;
        let hook_target = notify_hook_target(&req.target, &resolved);
        self.send_resolved(
            &source_caller,
            SendRequest {
                // Routing is already resolved, but hooks receive the equivalent immutable target.
                to: hook_target,
                summary: Some(source),
                body: req.body,
                mention: Vec::new(),
                metadata: None,
                idempotency_key: req.idempotency_key,
            },
            Some(Kind::Notification),
            resolved,
        )
        .await
    }

    async fn send_resolved(
        &self,
        caller: &Caller,
        mut req: SendRequest,
        sender_kind: Option<Kind>,
        resolved: Resolved,
    ) -> Result<Ack, NexusError> {
        let idempotency_key =
            normalized_idempotency_key(req.idempotency_key.as_deref()).map(str::to_string);
        if let Some(key) = idempotency_key.as_deref() {
            if let Some(ack) = self.ack_for_idempotency_key(caller, key).await? {
                return Ok(ack);
            }
        }
        let use_legacy_duplicate_window = idempotency_key.is_none();

        MessagePolicy::new(&self.store)
            .check(caller, &resolved)
            .await?;
        let evaluated = self.evaluate_before_send(caller, req).await?;
        req = evaluated.request;
        let timing = evaluated.timing;
        let created_at = (self.now)();

        let (message_id, fanout, event): (MessageId, Option<u32>, WsEvent) = match resolved {
            Resolved::Dm(recipient) => {
                let to_name = recipient
                    .name
                    .clone()
                    .unwrap_or_else(|| recipient.session.0.clone());
                if use_legacy_duplicate_window {
                    if let Some(ack) = self
                        .ack_for_legacy_duplicate(
                            caller,
                            Scope::Dm,
                            &to_name,
                            &req.body,
                            created_at,
                        )
                        .await?
                    {
                        return Ok(ack);
                    }
                }
                let recipients = [recipient];
                let (id, delivered) = self
                    .broadcast_idempotent(
                        caller,
                        dm::spec(&recipients),
                        req.summary.clone(),
                        req.body.clone(),
                        req.metadata.clone(),
                        req.mention.clone(),
                        timing,
                        created_at,
                        idempotency_key.as_deref(),
                        sender_kind,
                    )
                    .await?;
                (
                    id.clone(),
                    Some(delivered),
                    WsEvent::MessageCreated { message_id: id },
                )
            }
            Resolved::LocalOperatorDm(operator_name) => {
                if use_legacy_duplicate_window {
                    if let Some(ack) = self
                        .ack_for_legacy_duplicate(
                            caller,
                            Scope::Dm,
                            &operator_name,
                            &req.body,
                            created_at,
                        )
                        .await?
                    {
                        return Ok(ack);
                    }
                }
                let recipients: [Recipient; 0] = [];
                let (id, delivered) = self
                    .broadcast_idempotent(
                        caller,
                        dm::local_operator_spec(operator_name, &recipients),
                        req.summary.clone(),
                        req.body.clone(),
                        req.metadata.clone(),
                        req.mention.clone(),
                        timing,
                        created_at,
                        idempotency_key.as_deref(),
                        sender_kind,
                    )
                    .await?;
                (
                    id.clone(),
                    Some(delivered),
                    WsEvent::MessageCreated { message_id: id },
                )
            }
            Resolved::Thread(thread_id, members) => {
                // The thread name for the in-band provenance tag.
                let (thread_name, thread_project) =
                    self.thread_name_for(caller, &thread_id).await?;
                if use_legacy_duplicate_window {
                    if let Some(ack) = self
                        .ack_for_legacy_duplicate(
                            caller,
                            Scope::Thread,
                            &thread_name,
                            &req.body,
                            created_at,
                        )
                        .await?
                    {
                        return Ok(ack);
                    }
                }
                let (id, delivered) = self
                    .broadcast_idempotent(
                        caller,
                        thread::spec(&thread_id, thread_name, thread_project, &members),
                        req.summary.clone(),
                        req.body.clone(),
                        req.metadata.clone(),
                        req.mention.clone(),
                        timing,
                        created_at,
                        idempotency_key.as_deref(),
                        sender_kind,
                    )
                    .await?;
                (
                    id.clone(),
                    Some(delivered),
                    WsEvent::MessageCreated { message_id: id },
                )
            }
            Resolved::Topic(topic_id, subscribers) => {
                if use_legacy_duplicate_window {
                    if let Some(ack) = self
                        .ack_for_legacy_duplicate(
                            caller,
                            Scope::Topic,
                            &topic_id.0,
                            &req.body,
                            created_at,
                        )
                        .await?
                    {
                        return Ok(ack);
                    }
                }
                let (id, delivered) = self
                    .broadcast_idempotent(
                        caller,
                        topic::spec(&topic_id, &subscribers),
                        req.summary.clone(),
                        req.body.clone(),
                        req.metadata.clone(),
                        req.mention.clone(),
                        timing,
                        created_at,
                        idempotency_key.as_deref(),
                        sender_kind,
                    )
                    .await?;
                (
                    id.clone(),
                    Some(delivered),
                    WsEvent::TopicPublished {
                        topic: topic_id.0.clone(),
                        message_id: id,
                    },
                )
            }
            Resolved::Group(group, members) => {
                if use_legacy_duplicate_window {
                    if let Some(ack) = self
                        .ack_for_legacy_duplicate(caller, Scope::Dm, &group, &req.body, created_at)
                        .await?
                    {
                        return Ok(ack);
                    }
                }
                let (id, delivered) = self
                    .broadcast_idempotent(
                        caller,
                        dm::group_spec(group, &members),
                        req.summary.clone(),
                        req.body.clone(),
                        req.metadata.clone(),
                        req.mention.clone(),
                        timing,
                        created_at,
                        idempotency_key.as_deref(),
                        sender_kind,
                    )
                    .await?;
                (
                    id.clone(),
                    Some(delivered),
                    WsEvent::MessageCreated { message_id: id },
                )
            }
        };

        // The broadcast transaction has committed at this point. In buffered mode enqueue the
        // canonical facts before returning the local acceptance receipt, so a Gateway disconnect
        // cannot create a same-boot hole. Idempotent replays return above and do not create a
        // second effect.
        for effect in Messages::new(&self.store)
            .gateway_projection_effects(&message_id)
            .await?
        {
            self.events.project(effect).await;
        }
        self.events.emit(event).await;
        Ok(Ack { message_id, fanout })
    }

    /// Resolve a thread's name from its id for the provenance tag.
    /// Thread display name + the thread's OWN project (the durable row must carry the
    /// conversation's project, not the caller's — recipients drain project-scoped).
    async fn thread_name_for(
        &self,
        _caller: &Caller,
        thread_id: &nexus_contracts::ids::ThreadId,
    ) -> Result<(String, String), NexusError> {
        let threads = Threads::new(&self.store);
        for row in threads.list().await? {
            if &row.thread_id == thread_id {
                return Ok((row.name, row.project));
            }
        }
        Err(NexusError::NotFound(format!("thread:{}", thread_id.0)))
    }

    async fn broadcast_idempotent(
        &self,
        caller: &Caller,
        spec: WriteSpec<'_>,
        summary: Option<String>,
        body: String,
        metadata: Option<serde_json::Map<String, serde_json::Value>>,
        mention: Vec<String>,
        timing: DeliveryTiming,
        created_at: i64,
        idempotency_key: Option<&str>,
        sender_kind: Option<Kind>,
    ) -> Result<(MessageId, u32), NexusError> {
        let sender_kind = match sender_kind {
            Some(kind) => kind,
            None => self.sender_kind(caller).await?,
        };
        let result = broadcast_messages(
            &self.store,
            &self.realtime,
            caller,
            spec,
            summary,
            body,
            metadata,
            mention,
            timing,
            created_at,
            idempotency_key,
            sender_kind,
        )
        .await;
        match (result, idempotency_key) {
            (Ok(done), _) => Ok(done),
            (Err(err), Some(key)) if is_unique_constraint(&err) => {
                let ack = self
                    .ack_for_idempotency_key(caller, key)
                    .await?
                    .ok_or(err)?;
                Ok((ack.message_id, ack.fanout.unwrap_or(0)))
            }
            (Err(err), _) => Err(err),
        }
    }

    async fn evaluate_before_send(
        &self,
        caller: &Caller,
        req: SendRequest,
    ) -> Result<EvaluatedSend, NexusError> {
        if req
            .metadata
            .as_ref()
            .is_some_and(|metadata| metadata.contains_key("_nexus"))
        {
            return Err(NexusError::Invalid(
                "metadata._nexus is reserved for Nexus provenance".into(),
            ));
        }
        let Some(hooks) = &self.message_hooks else {
            return Ok(EvaluatedSend {
                request: req,
                timing: DeliveryTiming::default(),
            });
        };

        let evaluation_id = hook_evaluation_id(caller, req.idempotency_key.as_deref());
        let original_sender = HookSender {
            agent_id: caller.agent_id.clone(),
            name: caller.name.clone(),
        };
        let original_target = req.to.clone();
        let result = hooks
            .before_send(HookBeforeSendRequest {
                evaluation_id: evaluation_id.clone(),
                message: HookMessage {
                    sender: original_sender.clone(),
                    target: original_target.clone(),
                    body: req.body,
                    summary: req.summary,
                    mention: req.mention,
                    metadata: req.metadata.unwrap_or_default(),
                },
            })
            .await
            .map_err(NexusError::from)?;

        if result.evaluation_id != evaluation_id {
            return Err(NexusError::Invalid(
                "hook result evaluationId does not match the request".into(),
            ));
        }
        if result.message.sender != original_sender || result.message.target != original_target {
            return Err(NexusError::Invalid(
                "before_send hooks cannot change sender or target".into(),
            ));
        }
        if result.message.metadata.contains_key("_nexus") {
            return Err(NexusError::Invalid(
                "hook result cannot write reserved metadata._nexus".into(),
            ));
        }
        if result.action == HookAction::Reject {
            return Err(NexusError::HookRejected);
        }
        validate_send_body(&result.message.body)
            .map_err(|error| NexusError::Invalid(error.message))?;

        let mut metadata = result.message.metadata;
        if !result.executed_by.is_empty() {
            metadata.insert(
                "_nexus".into(),
                serde_json::json!({
                    "hooks": {
                        "executedBy": result.executed_by,
                    }
                }),
            );
        }
        Ok(EvaluatedSend {
            request: SendRequest {
                to: original_target,
                summary: result.message.summary,
                body: result.message.body,
                mention: result.message.mention,
                metadata: (!metadata.is_empty()).then_some(metadata),
                idempotency_key: req.idempotency_key,
            },
            timing: result.timing.unwrap_or_default(),
        })
    }

    async fn sender_kind(&self, caller: &Caller) -> Result<Kind, NexusError> {
        if let Some(kind) = self
            .sender_kinds
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&caller.session)
            .copied()
        {
            return Ok(kind);
        }
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT kind FROM sessions \
                 WHERE session_id = ?1 OR (name = ?2 \
                   AND NOT EXISTS (SELECT 1 FROM sessions WHERE session_id = ?1)) \
                 ORDER BY CASE WHEN session_id = ?1 THEN 0 ELSE 1 END, rowid DESC LIMIT 1",
                params![caller.session.0.clone(), caller.name.clone()],
            )
            .await
            .map_err(|error| NexusError::Store(error.to_string()))?;
        let raw = match rows
            .next()
            .await
            .map_err(|error| NexusError::Store(error.to_string()))?
        {
            Some(row) => row
                .get::<String>(0)
                .map_err(|error| NexusError::Store(error.to_string()))?,
            None => "agent".to_string(),
        };
        let kind = match raw.as_str() {
            "human" => Kind::Human,
            "notification" => Kind::Notification,
            "app" => Kind::App,
            _ => Kind::Agent,
        };
        self.sender_kinds
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(caller.session.clone(), kind);
        Ok(kind)
    }

    async fn ack_for_idempotency_key(
        &self,
        caller: &Caller,
        key: &str,
    ) -> Result<Option<Ack>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT message_id FROM messages \
                 WHERE sender_session_id = ?1 AND idempotency_key = ?2 \
                 ORDER BY created_at DESC LIMIT 1",
                params![caller.session.0.clone(), key],
            )
            .await
            .map_err(|e| NexusError::Store(e.to_string()))?;
        match rows
            .next()
            .await
            .map_err(|e| NexusError::Store(e.to_string()))?
        {
            Some(row) => {
                let id = MessageId(row.get::<String>(0).map_err(store_row_err)?);
                Ok(Some(self.ack_for_message_id(id).await?))
            }
            None => Ok(None),
        }
    }

    async fn ack_for_legacy_duplicate(
        &self,
        caller: &Caller,
        scope: Scope,
        to_name: &str,
        body: &str,
        created_at: i64,
    ) -> Result<Option<Ack>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT message_id FROM messages \
                 WHERE sender_session_id = ?1 AND kind = ?2 AND to_name = ?3 \
                   AND body = ?4 AND created_at >= ?5 \
                 ORDER BY created_at DESC LIMIT 1",
                params![
                    caller.session.0.clone(),
                    scope_token(scope),
                    to_name,
                    body,
                    created_at - LEGACY_DUPLICATE_WINDOW_MS
                ],
            )
            .await
            .map_err(|e| NexusError::Store(e.to_string()))?;
        match rows
            .next()
            .await
            .map_err(|e| NexusError::Store(e.to_string()))?
        {
            Some(row) => {
                let id = MessageId(row.get::<String>(0).map_err(store_row_err)?);
                Ok(Some(self.ack_for_message_id(id).await?))
            }
            None => Ok(None),
        }
    }

    async fn ack_for_message_id(&self, message_id: MessageId) -> Result<Ack, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT COUNT(*) FROM in_flight WHERE message_id = ?1",
                params![message_id.0.clone()],
            )
            .await
            .map_err(|e| NexusError::Store(e.to_string()))?;
        let fanout = match rows
            .next()
            .await
            .map_err(|e| NexusError::Store(e.to_string()))?
        {
            Some(row) => row.get::<i64>(0).map_err(store_row_err)?,
            None => 0,
        };
        Ok(Ack {
            message_id,
            fanout: Some(fanout.max(0) as u32),
        })
    }

    /// On thread join, deliver one notification to the new member: the recent tail plus how to
    /// pull/search the full history. History itself is never re-delivered because a full-history
    /// backfill can flood the injection queue and head-of-line block live traffic.
    async fn send_thread_join_brief(
        &self,
        caller: &Caller,
        thread_id: &nexus_contracts::ids::ThreadId,
        thread_name: &str,
        recipient: &Recipient,
    ) -> Result<(), NexusError> {
        const TAIL: i64 = 5;
        const EXCERPT: usize = 160;

        let mut rows = self
            .store
            .conn
            .query(
                "SELECT from_name, body,                         (SELECT COUNT(*) FROM messages                          WHERE project = ?1 AND kind = 'thread' AND thread_id = ?2) AS total                  FROM messages                  WHERE project = ?1 AND kind = 'thread' AND thread_id = ?2                  ORDER BY created_at DESC, message_id DESC LIMIT ?3",
                params![caller.project.clone(), thread_id.0.clone(), TAIL],
            )
            .await
            .map_err(store_row_err)?;
        let mut tail: Vec<(String, String)> = Vec::new();
        let mut total: i64 = 0;
        while let Some(row) = rows.next().await.map_err(store_row_err)? {
            let from: String = row.get(0).map_err(store_row_err)?;
            let body: String = row.get(1).map_err(store_row_err)?;
            total = row.get(2).map_err(store_row_err)?;
            tail.push((from, body));
        }
        drop(rows);
        tail.reverse(); // oldest → newest

        let mut body =
            format!("You joined thread \"{thread_name}\" ({total} prior posts). Recent tail:\n");
        if tail.is_empty() {
            body.push_str("(no prior posts)\n");
        }
        for (from, text) in &tail {
            let one_line = text.replace('\n', " ");
            let excerpt: String = one_line.chars().take(EXCERPT).collect();
            let ellipsis = if one_line.chars().count() > EXCERPT {
                "…"
            } else {
                ""
            };
            body.push_str(&format!("[{from}] {excerpt}{ellipsis}\n"));
        }
        body.push_str(&format!(
            "Prior history is not re-delivered. Pull it on demand: `nexus history --thread {thread_name} --limit N` · `nexus search \"<terms>\" --thread {thread_name}`."
        ));

        let message_id = nexus_common::new_message_id();
        let created_at = nexus_common::now();
        // The drain deserializes messages.provenance as JSON — it must never be NULL.
        let provenance = nexus_contracts::Provenance {
            from: "nexus".to_string(),
            kind: nexus_contracts::Kind::Notification,
            thread: Some(thread_name.to_string()),
            topic: None,
            stamp: None,
        };
        let provenance_json =
            serde_json::to_string(&provenance).map_err(|e| NexusError::Store(e.to_string()))?;
        let summary = format!("joined thread {thread_name}");
        let mut batch = String::new();
        push_stmt(
            &mut batch,
            format!(
                "INSERT INTO messages (message_id, from_name, kind, to_name, summary, body, \
                 provenance, project, created_at, to_agent_id) \
                 VALUES ({}, 'nexus', 'notification', {}, {}, {}, {}, {}, {}, {})",
                literal(&message_id.0),
                opt_literal(recipient.name.as_deref()),
                literal(&summary),
                literal(&body),
                literal(&provenance_json),
                literal(&caller.project),
                created_at,
                opt_literal(recipient.agent_id.as_ref().map(|id| id.0.as_str()))
            ),
        );
        if !self.store.has_split_authority() {
            push_stmt(
                &mut batch,
                format!(
                    "INSERT INTO messages_fts (rowid, summary, body) \
                     VALUES ((SELECT rowid FROM messages WHERE message_id = {}), {}, {})",
                    literal(&message_id.0),
                    literal(&summary),
                    literal(&body)
                ),
            );
        }
        push_stmt(
            &mut batch,
            format!(
                "INSERT OR IGNORE INTO in_flight (in_flight_id, message_id, recipient_session, \
                 recipient_agent_id, state) VALUES ({}, {}, {}, {}, 'pending')",
                literal(&nexus_common::new_message_id().0),
                literal(&message_id.0),
                literal(&recipient.session.0),
                opt_literal(recipient.agent_id.as_ref().map(|id| id.0.as_str()))
            ),
        );
        self.store
            .execute_write_batch("thread_join_brief", &batch)
            .await?;
        for effect in Messages::new(&self.store)
            .gateway_projection_effects(&message_id)
            .await?
        {
            self.events.project(effect).await;
        }
        self.realtime
            .enqueue(&recipient.session, &message_id)
            .await
            .map_err(NexusError::from)?;
        Ok(())
    }

    /// Append a metadata-only action developer event without letting telemetry failures fail the
    /// command handler. These rows are observational and never wake or inject agent turns.
    async fn append_action_best_effort(
        &self,
        topic: &str,
        action: &str,
        caller: &Caller,
        thread_name: Option<&str>,
        session_id: Option<&str>,
        data: serde_json::Value,
    ) {
        if let Err(error) = DeveloperEvents::new(&self.store)
            .append_action(
                topic,
                action,
                Some(&caller.name),
                thread_name,
                caller.agent_id.as_ref().map(|id| id.0.as_str()),
                session_id,
                data,
                (self.now)(),
            )
            .await
        {
            tracing::warn!(
                target: "nexus_bus::developer_events",
                action,
                topic,
                error = ?error,
                "failed to append action developer event"
            );
        }
    }

    async fn project_thread_state(&self, name: &str) -> Result<(), NexusError> {
        let threads = Threads::new(&self.store);
        let row = threads
            .find_any_by_name(name)
            .await?
            .ok_or_else(|| NexusError::NotFound(format!("thread:{name}")))?;
        let members = threads.members(&row.thread_id).await?;
        let declared = serde_json::json!({
            "threadId": row.thread_id.0,
            "name": row.name,
            "project": row.project,
            "topic": row.topic,
            "description": row.description,
            "createdBy": row.created_by,
            "createdAt": row.created_at,
            "archivedAt": row.archived_at,
        });
        self.events
            .project(projection_effect(
                "thread",
                nexus_contracts::GatewayProjectionKind::ThreadDeclared,
                declared,
            ))
            .await;
        let membership = serde_json::json!({
            "threadId": row.thread_id.0,
            "name": name,
            "members": members,
            "project": row.project,
        });
        self.events
            .project(projection_effect(
                "thread-membership",
                nexus_contracts::GatewayProjectionKind::ThreadMembershipChanged,
                membership,
            ))
            .await;
        Ok(())
    }

    async fn project_deleted_thread(&self, thread_id: &str, name: &str, project: &str) {
        let payload = serde_json::json!({
            "threadId": thread_id,
            "name": name,
            "project": project,
            "deleted": true,
        });
        self.events
            .project(projection_effect(
                "thread",
                nexus_contracts::GatewayProjectionKind::ThreadDeclared,
                payload,
            ))
            .await;
    }

    async fn project_topic_state(&self, topic: &str) -> Result<(), NexusError> {
        let mut topic_rows = self
            .store
            .conn
            .query(
                "SELECT topic, project, created_at FROM topics WHERE topic = ?1 LIMIT 1",
                params![topic],
            )
            .await
            .map_err(store_row_err)?;
        let Some(row) = topic_rows.next().await.map_err(store_row_err)? else {
            return Err(NexusError::NotFound(format!("topic:{topic}")));
        };
        let topic_name = row.get::<String>(0).map_err(store_row_err)?;
        let project = row.get::<String>(1).map_err(store_row_err)?;
        let created_at = row.get::<i64>(2).map_err(store_row_err)?;
        drop(topic_rows);
        self.events
            .project(projection_effect(
                "topic",
                nexus_contracts::GatewayProjectionKind::TopicDeclared,
                serde_json::json!({
                    "topic": topic_name,
                    "project": project,
                    "createdAt": created_at,
                }),
            ))
            .await;

        let mut rows = self
            .store
            .conn
            .query(
                "SELECT subscriber_session, subscriber_agent_id, sub_group, cursor, subscribed_at \
                 FROM subscriptions WHERE topic = ?1 \
                 ORDER BY COALESCE(subscriber_agent_id, subscriber_session), subscribed_at",
                params![topic],
            )
            .await
            .map_err(store_row_err)?;
        let mut subscribers = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_row_err)? {
            subscribers.push(serde_json::json!({
                "sessionId": row.get::<String>(0).map_err(store_row_err)?,
                "agentId": row.get::<Option<String>>(1).map_err(store_row_err)?,
                "group": row.get::<Option<String>>(2).map_err(store_row_err)?,
                "cursor": row.get::<i64>(3).map_err(store_row_err)?,
                "subscribedAt": row.get::<i64>(4).map_err(store_row_err)?,
            }));
        }
        self.events
            .project(projection_effect(
                "topic-subscription",
                nexus_contracts::GatewayProjectionKind::TopicSubscriptionChanged,
                serde_json::json!({
                    "topic": topic,
                    "project": project,
                    "subscribers": subscribers,
                }),
            ))
            .await;
        Ok(())
    }
}

fn projection_effect(
    prefix: &str,
    kind: nexus_contracts::GatewayProjectionKind,
    payload: serde_json::Value,
) -> nexus_contracts::GatewayProjectionEffect {
    let canonical = serde_json::to_vec(&payload).unwrap_or_default();
    let digest = Sha256::digest(&canonical);
    nexus_contracts::GatewayProjectionEffect {
        event_id: format!("{prefix}:{digest:x}"),
        occurred_at: now(),
        kind,
        payload,
    }
}

fn normalized_idempotency_key(key: Option<&str>) -> Option<&str> {
    key.map(str::trim).filter(|key| !key.is_empty())
}

fn hook_evaluation_id(caller: &Caller, idempotency_key: Option<&str>) -> String {
    match normalized_idempotency_key(idempotency_key) {
        Some(key) => {
            let mut digest = Sha256::new();
            digest.update(b"nexus.hooks.before_send.v1\0");
            digest.update(
                caller
                    .agent_id
                    .as_ref()
                    .map(|agent_id| agent_id.0.as_str())
                    .unwrap_or(&caller.session.0)
                    .as_bytes(),
            );
            digest.update(b"\0");
            digest.update(key.as_bytes());
            format!("he_{:x}", digest.finalize())
        }
        None => nexus_common::new_message_id().0.replacen("m_", "he_", 1),
    }
}

fn notify_hook_target(target: &NotifyTarget, resolved: &Resolved) -> SendTarget {
    match target {
        NotifyTarget::Agent { agent_id } => SendTarget::dm_agent(agent_id.clone(), None),
        NotifyTarget::Name { name } | NotifyTarget::Group { group: name } => {
            SendTarget::dm_name(name)
        }
        NotifyTarget::Thread { thread } => SendTarget::Post {
            thread: thread.clone(),
        },
        NotifyTarget::Auto { value } => match resolved {
            Resolved::Thread(_, _) => SendTarget::Post {
                thread: value.clone(),
            },
            _ => SendTarget::dm_name(value),
        },
    }
}

fn scope_token(scope: Scope) -> &'static str {
    match scope {
        Scope::Dm => "dm",
        Scope::Thread => "thread",
        Scope::Topic => "topic",
    }
}

fn store_row_err(err: libsql::Error) -> NexusError {
    NexusError::Store(err.to_string())
}

fn is_unique_constraint(err: &NexusError) -> bool {
    match err {
        NexusError::Store(msg) => {
            msg.contains("UNIQUE constraint failed") || msg.contains("constraint failed")
        }
        _ => false,
    }
}

#[async_trait::async_trait]
impl BusPort for Bus {
    async fn send(&self, caller: &Caller, req: SendRequest) -> PortResult<Ack> {
        to_port(self.send_inner(caller, req, None).await)
    }

    async fn send_with_kind(
        &self,
        caller: &Caller,
        req: SendRequest,
        kind: Kind,
    ) -> PortResult<Ack> {
        to_port(self.send_inner(caller, req, Some(kind)).await)
    }

    async fn notify(&self, caller: &Caller, req: NotifySendRequest) -> PortResult<Ack> {
        to_port(self.notify_inner(caller, req).await)
    }

    async fn create_thread(&self, caller: &Caller, req: CreateThreadRequest) -> PortResult<()> {
        let result: Result<(), NexusError> = async {
            let threads = Threads::new(&self.store);
            if threads.find_any_by_name(&req.name).await?.is_some() {
                return Err(NexusError::DuplicateName(req.name.clone()));
            }
            let thread_id = new_thread_id();
            threads
                .create(&thread_id, &req.name, &caller.project, &caller.name)
                .await?;
            // The creator is always a member; plus any initial members.
            threads.add_member(&thread_id, &caller.name).await?;
            for m in &req.members {
                threads.add_member(&thread_id, m).await?;
            }
            let members = threads.members(&thread_id).await?;
            self.append_action_best_effort(
                &format!("sys.thread.{}", req.name),
                "thread.create",
                caller,
                Some(&req.name),
                None,
                serde_json::json!({
                    "thread": req.name.clone(),
                    "members": members.clone(),
                }),
            )
            .await;
            self.project_thread_state(&req.name).await?;
            self.events
                .emit(WsEvent::ThreadCreated {
                    thread: req.name.clone(),
                    members,
                })
                .await;
            Ok(())
        }
        .await;
        to_port(result)
    }

    async fn join_thread(&self, caller: &Caller, req: JoinThreadRequest) -> PortResult<()> {
        to_port(self.member_change(caller, &req.name, true).await)
    }

    async fn leave_thread(&self, caller: &Caller, req: LeaveThreadRequest) -> PortResult<()> {
        to_port(self.member_change(caller, &req.name, false).await)
    }

    async fn archive_thread(&self, caller: &Caller, req: ArchiveThreadRequest) -> PortResult<()> {
        let result: Result<(), NexusError> = async {
            Threads::new(&self.store).archive(&req.name).await?;
            self.append_action_best_effort(
                &format!("sys.thread.{}", req.name),
                "thread.archive",
                caller,
                Some(&req.name),
                None,
                serde_json::json!({ "thread": req.name.clone() }),
            )
            .await;
            self.project_thread_state(&req.name).await?;
            self.events
                .emit(WsEvent::ThreadMemberChanged {
                    thread: req.name,
                    members: Vec::new(),
                })
                .await;
            Ok(())
        }
        .await;
        to_port(result)
    }

    async fn delete_thread(&self, caller: &Caller, req: DeleteThreadRequest) -> PortResult<()> {
        let result: Result<(), NexusError> = async {
            let threads = Threads::new(&self.store);
            let deleted = threads
                .find_any_by_name(&req.name)
                .await?
                .ok_or_else(|| NexusError::NotFound(format!("thread:{}", req.name)))?;
            threads.delete(&req.name).await?;
            self.append_action_best_effort(
                &format!("sys.thread.{}", req.name),
                "thread.delete",
                caller,
                Some(&req.name),
                None,
                serde_json::json!({ "thread": req.name.clone() }),
            )
            .await;
            self.project_deleted_thread(&deleted.thread_id.0, &deleted.name, &deleted.project)
                .await;
            self.events
                .emit(WsEvent::ThreadMemberChanged {
                    thread: req.name,
                    members: Vec::new(),
                })
                .await;
            Ok(())
        }
        .await;
        to_port(result)
    }

    /// Rename a thread by global name. Admin-only and collision-safe; the thread id and
    /// memberships stay intact, only the display name changes.
    async fn rename_thread(&self, caller: &Caller, req: RenameThreadRequest) -> PortResult<()> {
        let result: Result<(), NexusError> = async {
            if caller.tier != nexus_contracts::Tier::Admin {
                return Err(NexusError::Unauthorized);
            }

            if req.name == req.new_name {
                return Ok(());
            }

            let threads = Threads::new(&self.store);
            let current = threads
                .find_any_by_name(&req.name)
                .await?
                .ok_or_else(|| NexusError::NotFound(format!("thread:{}", req.name)))?;
            if let Some(existing) = threads.find_any_by_name(&req.new_name).await? {
                if existing.thread_id != current.thread_id {
                    return Err(NexusError::DuplicateName(req.new_name.clone()));
                }
            }
            threads.rename(&req.name, &req.new_name).await?;
            self.append_action_best_effort(
                &format!("sys.thread.{}", req.new_name),
                "thread.rename",
                caller,
                Some(&req.new_name),
                None,
                serde_json::json!({
                    "oldName": req.name.clone(),
                    "newName": req.new_name.clone(),
                }),
            )
            .await;
            self.project_thread_state(&req.new_name).await?;
            Ok(())
        }
        .await;
        to_port(result)
    }

    async fn add_thread_member(&self, caller: &Caller, req: ThreadMemberRequest) -> PortResult<()> {
        to_port(
            self.member_change_named(caller, &req.name, &req.member, true, None)
                .await,
        )
    }

    async fn remove_thread_member(
        &self,
        caller: &Caller,
        req: ThreadMemberRequest,
    ) -> PortResult<()> {
        to_port(
            self.member_change_named(caller, &req.name, &req.member, false, None)
                .await,
        )
    }

    async fn threads(&self, _caller: &Caller) -> PortResult<ThreadListResponse> {
        let result: Result<ThreadListResponse, NexusError> = async {
            let threads = Threads::new(&self.store);
            let developer_events = DeveloperEvents::new(&self.store);
            let mut out = Vec::new();
            for row in threads.list().await? {
                let members = threads.members(&row.thread_id).await?;
                let latest_seq = developer_events
                    .latest_seq(&format!("sys.message.thread.{}", row.name))
                    .await?;
                out.push(ThreadSummary {
                    name: row.name,
                    topic: row.topic,
                    description: row.description,
                    members,
                    last_at: None,
                    latest_seq: Some(latest_seq),
                });
            }
            Ok(ThreadListResponse { threads: out })
        }
        .await;
        to_port(result)
    }

    async fn thread_members(
        &self,
        _caller: &Caller,
        req: ThreadMembersRequest,
    ) -> PortResult<ThreadMembersResponse> {
        let result: Result<ThreadMembersResponse, NexusError> = async {
            let threads = Threads::new(&self.store);
            let row = threads
                .find_active_any_by_name(&req.name)
                .await?
                .ok_or_else(|| NexusError::NotFound(format!("thread:{}", req.name)))?;
            let members = threads.members(&row.thread_id).await?;
            Ok(ThreadMembersResponse {
                name: req.name,
                members,
            })
        }
        .await;
        to_port(result)
    }

    async fn subscribe(
        &self,
        caller: &Caller,
        req: SubscribeRequest,
    ) -> PortResult<SubscribeResponse> {
        let result: Result<SubscribeResponse, NexusError> = async {
            let topics = Topics::new(&self.store);
            topics.ensure(&req.topic, &caller.project).await?;
            let cursor = topics
                .subscribe(&req.topic, &caller.session.0, req.group.as_deref())
                .await?;
            self.append_action_best_effort(
                &format!("sys.topic.{}", req.topic),
                "topic.subscribe",
                caller,
                None,
                Some(&caller.session.0),
                serde_json::json!({
                    "topic": req.topic.clone(),
                    "group": req.group.clone(),
                }),
            )
            .await;
            self.project_topic_state(&req.topic).await?;
            Ok(SubscribeResponse {
                topic: req.topic,
                cursor,
            })
        }
        .await;
        to_port(result)
    }

    async fn unsubscribe(&self, caller: &Caller, req: UnsubscribeRequest) -> PortResult<()> {
        let result: Result<(), NexusError> = async {
            Topics::new(&self.store)
                .unsubscribe(&req.topic, &caller.session.0)
                .await?;
            self.append_action_best_effort(
                &format!("sys.topic.{}", req.topic),
                "topic.unsubscribe",
                caller,
                None,
                Some(&caller.session.0),
                serde_json::json!({ "topic": req.topic.clone() }),
            )
            .await;
            self.project_topic_state(&req.topic).await?;
            Ok(())
        }
        .await;
        to_port(result)
    }

    async fn topics(&self, caller: &Caller) -> PortResult<TopicListResponse> {
        let result: Result<TopicListResponse, NexusError> = async {
            let topics = Topics::new(&self.store);
            let mut out = Vec::new();
            for t in topics.list(&caller.project).await? {
                let subscribers = topics.subscribers(&t).await?.len() as u32;
                out.push(TopicSummary {
                    topic: t,
                    subscribers,
                });
            }
            Ok(TopicListResponse { topics: out })
        }
        .await;
        to_port(result)
    }
}

impl Bus {
    /// Join (`add = true`) or leave (`add = false`) a thread as the caller, emitting
    /// `thread.member.changed`.
    async fn member_change(
        &self,
        caller: &Caller,
        name: &str,
        add: bool,
    ) -> Result<(), NexusError> {
        let member = caller.name.clone();
        let action = if add { "thread.join" } else { "thread.leave" };
        self.member_change_named(caller, name, &member, add, Some(action))
            .await
    }

    /// Add (`add = true`) or remove (`add = false`) a SPECIFIC `member` to/from a thread, resolving
    /// the thread by global name and emitting `thread.member.changed`. `member_change`
    /// (self join/leave) is the `member == caller.name` case.
    async fn member_change_named(
        &self,
        caller: &Caller,
        name: &str,
        member: &str,
        add: bool,
        action: Option<&str>,
    ) -> Result<(), NexusError> {
        let threads = Threads::new(&self.store);
        let row = threads
            .find_active_any_by_name(name)
            .await?
            .ok_or_else(|| NexusError::NotFound(format!("thread:{name}")))?;
        if add {
            let already = threads.is_member(&row.thread_id, member).await?;
            threads.add_member(&row.thread_id, member).await?;
            if !already {
                if let Some(recipient) = self.router.member_recipient(caller, member).await? {
                    self.send_thread_join_brief(caller, &row.thread_id, name, &recipient)
                        .await?;
                }
            }
        } else {
            threads.remove_member(&row.thread_id, member).await?;
        }
        let members = threads.members(&row.thread_id).await?;
        if let Some(action) = action {
            self.append_action_best_effort(
                &format!("sys.thread.{name}"),
                action,
                caller,
                Some(name),
                None,
                serde_json::json!({
                    "thread": name,
                    "member": member,
                }),
            )
            .await;
        }
        self.events
            .project(projection_effect(
                "thread-membership",
                nexus_contracts::GatewayProjectionKind::ThreadMembershipChanged,
                serde_json::json!({
                    "threadId": row.thread_id.0,
                    "name": name,
                    "members": members.clone(),
                    "project": row.project,
                }),
            ))
            .await;
        self.events
            .emit(WsEvent::ThreadMemberChanged {
                thread: name.to_string(),
                members,
            })
            .await;
        Ok(())
    }
}
