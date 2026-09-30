//! The router — the **one `to` contract** (backend §3): resolve a [`SendTarget`] into a concrete
//! delivery set. Resolution is a **pure lookup**: it reads the identity, thread, and topic
//! registries and returns who-gets-what. It never reorders, gates, or inserts into the path
//! (no orchestrator); the only decision it makes is name → scope → recipients.
//!
//! - **DM target** (`SendTarget::Dm{name, agent_id}`) → route by authoritative stable id when
//!   present. A positional target accepts an exact stable id first and resolves a display alias
//!   only when no durable id owns that token;
//!   if `name` is the local web-console `operator`, write a local-human DM row;
//!   otherwise resolve it to exactly one runtime session.
//! - **Thread** (`SendTarget::Post{thread}`) → the thread's member set. A `to` that names a thread
//!   is a thread send regardless of any same-named agent — one `to` contract.
//! - **Topic** (`SendTarget::Publish{topic}`) → the topic's current subscribers (cursor-based).
//! - **Reply** (`SendTarget::Reply`) → the scope of the caller's most recent inbound message.
//!
//! Unknown `to` → [`NexusError::NotFound`] (explicit, not silent). An ambiguous name →
//! [`NexusError::Ambiguous`].

use std::sync::Arc;

use libsql::params;

use nexus_common::NexusError;
use nexus_contracts::codes;
use nexus_contracts::enums::Scope;
use nexus_contracts::ids::{AgentId, SessionId, ThreadId, TopicId};
use nexus_contracts::notify::NotifyTarget;
use nexus_contracts::ports::{Caller, IdentityPort};
use nexus_contracts::send::SendTarget;
use nexus_store::repos::{AgentGroups, AgentRuntimes, Agents, Sessions, Threads, Topics};
use nexus_store::Store;

/// A resolved recipient: stable agent identity when available plus the runtime session to wake now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Recipient {
    pub agent_id: Option<AgentId>,
    pub session: SessionId,
    pub name: Option<String>,
}

impl Recipient {
    fn from_caller(caller: Caller) -> Self {
        Self {
            agent_id: caller.agent_id,
            session: caller.session,
            name: Some(caller.name),
        }
    }
}

/// A resolved delivery set — the output of the pure `to` lookup. Carries the durable scope plus
/// the concrete recipient sessions the fan-out will enqueue against.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Resolved {
    /// A private 2-party DM to exactly one recipient session (the sender is never a recipient).
    Dm(Recipient),
    /// A private DM to the local web-console operator. It writes a durable DM row that the operator
    /// pane can observe, but has no runtime session to enqueue.
    LocalOperatorDm(String),
    /// A thread fan-out: the thread id (hidden from agents) + every member session.
    Thread(ThreadId, Vec<Recipient>),
    /// A topic publish: the topic id + every current subscriber session.
    Topic(TopicId, Vec<Recipient>),
    /// A one-shot group notification. The label is display metadata; recipients are resolved once.
    Group(String, Vec<Recipient>),
}

/// The pure-lookup resolver. Holds the shared store (for thread/topic registries) and the identity
/// port (for name → session resolution + caller scope). No mutation, no message-path side effects.
pub struct Router {
    store: Arc<Store>,
    identity: Arc<dyn IdentityPort>,
}

impl Router {
    /// Build a router over the shared store + identity port.
    pub fn new(store: Arc<Store>, identity: Arc<dyn IdentityPort>) -> Self {
        Router { store, identity }
    }

    /// Resolve a [`SendTarget`] for `caller` into a [`Resolved`] delivery set (backend §3).
    ///
    /// This is the whole routing contract: a pure lookup with no gating. Unknown names/threads/
    /// topics return [`NexusError::NotFound`]; a name bound to multiple live sessions returns
    /// [`NexusError::Ambiguous`].
    pub(crate) async fn resolve(
        &self,
        caller: &Caller,
        target: &SendTarget,
    ) -> Result<Resolved, NexusError> {
        match target {
            SendTarget::Dm { name, agent_id } => match agent_id {
                Some(agent_id) => self.resolve_dm_agent_id(caller, agent_id.clone()).await,
                None => {
                    let name = name.as_deref().ok_or_else(|| {
                        NexusError::Invalid("dm target requires name or agentId".into())
                    })?;
                    self.resolve_name_target(caller, name).await
                }
            },
            SendTarget::Post { thread } => self.resolve_thread(caller, thread).await,
            SendTarget::Publish { topic } => self.resolve_topic(caller, topic).await,
            SendTarget::Reply => self.resolve_reply(caller).await,
        }
    }

    /// Resolve the explicit one-shot notification target. Bare values are accepted only when they
    /// identify one target class; `group:`, `thread:`, and `agent:` CLI prefixes remove ambiguity.
    pub(crate) async fn resolve_notify(
        &self,
        caller: &Caller,
        target: &NotifyTarget,
    ) -> Result<Resolved, NexusError> {
        match target {
            NotifyTarget::Agent { agent_id } => {
                self.resolve_dm_agent_id(caller, agent_id.clone()).await
            }
            NotifyTarget::Name { name } => self.resolve_dm(caller, name).await,
            NotifyTarget::Group { group } => self.resolve_group(caller, group).await,
            NotifyTarget::Thread { thread } => self.resolve_thread(caller, thread).await,
            NotifyTarget::Auto { value } if value.starts_with("a_") => {
                self.resolve_dm_agent_id(caller, AgentId(value.clone()))
                    .await
            }
            NotifyTarget::Auto { value } => self.resolve_notify_auto(caller, value).await,
        }
    }

    async fn resolve_notify_auto(
        &self,
        caller: &Caller,
        value: &str,
    ) -> Result<Resolved, NexusError> {
        let groups = AgentGroups::new(&self.store);
        let group_exists = groups.exists_any_project(value).await?;
        let thread_exists = Threads::new(&self.store)
            .find_active_any_by_name(value)
            .await?
            .is_some();
        let agent_exists = Agents::new(&self.store)
            .find_by_name(value)
            .await?
            .is_some();
        let matches = [group_exists, thread_exists, agent_exists]
            .into_iter()
            .filter(|matched| *matched)
            .count();
        if matches > 1 {
            return Err(NexusError::Ambiguous(format!(
                "notify target {value:?}; use group:{value}, thread:{value}, or agent:{value}"
            )));
        }
        if group_exists {
            return self.resolve_group(caller, value).await;
        }
        if thread_exists {
            return self.resolve_thread(caller, value).await;
        }
        self.resolve_dm(caller, value).await
    }

    async fn resolve_group(&self, caller: &Caller, group: &str) -> Result<Resolved, NexusError> {
        let members = AgentGroups::new(&self.store)
            .members_any_project(group)
            .await?;
        if members.is_empty() {
            return Err(NexusError::NotFound(format!(
                "group:{group} has no members"
            )));
        }
        let mut recipients = Vec::with_capacity(members.len());
        for (agent_id, stored_name) in members {
            let mut recipient = self.recipient_for_agent_id(caller, agent_id).await?;
            if recipient.name.is_none() {
                recipient.name = stored_name;
            }
            recipients.push(recipient);
        }
        Ok(Resolved::Group(group.to_string(), recipients))
    }

    /// Generic positional target: thread names win so `nexus send --to <thread>` uses the same
    /// one-`to` contract as `post`. An exact durable agent id wins over an equal-looking display
    /// alias; aliases remain a fallback. Every agent result then routes
    /// through the stable-id path.
    async fn resolve_name_target(
        &self,
        caller: &Caller,
        name: &str,
    ) -> Result<Resolved, NexusError> {
        let threads = Threads::new(&self.store);
        if let Some(row) = threads.find_active_any_by_name(name).await? {
            let sessions = self
                .thread_member_recipients(caller, &row.thread_id)
                .await?;
            return Ok(Resolved::Thread(row.thread_id, sessions));
        }
        let agents = Agents::new(&self.store);
        if agents.find_by_id(name).await?.is_some() {
            return self
                .resolve_dm_agent_id(caller, AgentId(name.to_string()))
                .await;
        }
        self.resolve_dm(caller, name).await
    }

    /// DM fallback: resolve the name to a single recipient session. A name that does not resolve is
    /// an explicit `NotFound` (never a silent drop), except the local-human `operator` sink used by
    /// the zero-auth local web console when no registered member owns that name.
    async fn resolve_dm(&self, caller: &Caller, name: &str) -> Result<Resolved, NexusError> {
        match self.identity.resolve(&caller.project, name).await {
            Ok(recipient) => match recipient.agent_id {
                Some(agent_id) => self.resolve_dm_agent_id(caller, agent_id).await,
                None => Ok(Resolved::Dm(Recipient::from_caller(recipient))),
            },
            Err(err)
                if err.code == codes::NOT_FOUND || err.code == codes::PROJECT_SCOPE_VIOLATION =>
            {
                if let Some(recipient) = self.member_recipient(caller, name).await? {
                    return Ok(Resolved::Dm(recipient));
                }
                if is_local_operator_name(name) {
                    return Ok(Resolved::LocalOperatorDm(name.to_string()));
                }
                Err(NexusError::from(err))
            }
            Err(err) => Err(NexusError::from(err)),
        }
    }

    /// Thread: resolve the thread's member sessions. The fan-out itself (and the rule that a sender
    /// is never delivered its own post — the same shared spine a DM uses) lives in
    /// [`broadcast_messages`]; this only names the candidate recipients.
    async fn resolve_thread(&self, caller: &Caller, thread: &str) -> Result<Resolved, NexusError> {
        let threads = Threads::new(&self.store);
        let row = threads
            .find_active_any_by_name(thread)
            .await?
            .ok_or_else(|| NexusError::NotFound(format!("thread:{thread}")))?;
        let sessions = self
            .thread_member_recipients(caller, &row.thread_id)
            .await?;
        Ok(Resolved::Thread(row.thread_id, sessions))
    }

    /// Topic: fan-out to the current subscribers. Stable subscriptions resolve to the active
    /// runtime when one exists and otherwise keep their `subscriber_agent_id` on the delivery row
    /// so pending mail can follow a replacement runtime; legacy session-only subscriptions are
    /// used as-is.
    async fn resolve_topic(&self, caller: &Caller, topic: &str) -> Result<Resolved, NexusError> {
        let sessions = self.topic_subscriber_recipients(caller, topic).await?;
        Ok(Resolved::Topic(TopicId(topic.to_string()), sessions))
    }

    /// Reply: resolve into the scope of the caller's most recent inbound message, then route as
    /// that scope would (backend §3 reply-in-context). The reply targets the conversation the
    /// caller last received from — a DM reply goes back to that sender; a thread reply re-fans to
    /// that thread.
    async fn resolve_reply(&self, caller: &Caller) -> Result<Resolved, NexusError> {
        let (scope, from_name, from_agent_id, thread_id) = self.last_inbound(caller).await?;
        match scope {
            Scope::Dm => match from_agent_id {
                Some(agent_id) => self.resolve_dm_agent_id(caller, agent_id).await,
                None => self.resolve_dm(caller, &from_name).await,
            },
            Scope::Thread => {
                let tid = thread_id.ok_or_else(|| {
                    NexusError::Invalid("reply: thread message has no thread".into())
                })?;
                let sessions = self.thread_member_recipients(caller, &tid).await?;
                Ok(Resolved::Thread(tid, sessions))
            }
            Scope::Topic => Err(NexusError::Invalid(
                "reply: cannot reply into a topic publish".into(),
            )),
        }
    }

    /// Find the caller's most recent injected conversation from the bounded reply cursor. Legacy
    /// stores fall back to their newest rich `in_flight`/`messages` row. Returns
    /// `(scope, from_name, from_agent_id, thread_id)`.
    async fn last_inbound(
        &self,
        caller: &Caller,
    ) -> Result<(Scope, String, Option<AgentId>, Option<ThreadId>), NexusError> {
        let recipient_key = caller
            .agent_id
            .as_ref()
            .map(|agent_id| agent_id.0.as_str())
            .unwrap_or(caller.session.0.as_str());
        let mut contexts = self
            .store
            .conn
            .query(
                "SELECT scope, from_name, from_agent_id, thread_id FROM reply_contexts \
                 WHERE recipient_key = ?1 OR recipient_key = ?2 \
                 ORDER BY CASE WHEN recipient_key = ?1 THEN 0 ELSE 1 END LIMIT 1",
                params![recipient_key, caller.session.0.clone()],
            )
            .await
            .map_err(|e| NexusError::Store(e.to_string()))?;
        if let Some(row) = contexts
            .next()
            .await
            .map_err(|e| NexusError::Store(e.to_string()))?
        {
            let kind: String = row.get(0).map_err(|e| NexusError::Store(e.to_string()))?;
            let from_name: String = row.get(1).map_err(|e| NexusError::Store(e.to_string()))?;
            let from_agent_id: Option<String> =
                row.get(2).map_err(|e| NexusError::Store(e.to_string()))?;
            let thread_id: Option<String> =
                row.get(3).map_err(|e| NexusError::Store(e.to_string()))?;
            return Ok((
                scope_from_kind(&kind),
                from_name,
                from_agent_id.map(AgentId),
                thread_id.map(ThreadId),
            ));
        }
        drop(contexts);

        // Resolve stable identity through its own authority before querying the boot-scoped
        // transport ledger. A split daemon deliberately has no `agent_runtimes` table on the
        // transport connection, so cross-authority SQL is both invalid and unnecessary.
        let recipient_agent_id = match caller.agent_id.as_ref() {
            Some(agent_id) => Some(agent_id.0.clone()),
            None => match AgentRuntimes::new(&self.store)
                .find_by_runtime_id(&caller.session.0)
                .await?
            {
                Some(runtime) => Some(runtime.agent_id),
                None => Sessions::new(&self.store)
                    .find_by_session_id(&caller.session)
                    .await?
                    .and_then(|session| session.agent_id),
            },
        };
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT m.kind, m.from_name, m.from_agent_id, m.thread_id FROM in_flight f \
                 JOIN messages m ON m.message_id = f.message_id \
                 WHERE ((f.recipient_agent_id IS NULL AND f.recipient_session = ?1) \
                   OR (?2 IS NOT NULL AND f.recipient_agent_id = ?2)) \
                 ORDER BY m.created_at DESC LIMIT 1",
                params![caller.session.0.clone(), recipient_agent_id],
            )
            .await
            .map_err(|e| NexusError::Store(e.to_string()))?;
        let row = rows
            .next()
            .await
            .map_err(|e| NexusError::Store(e.to_string()))?
            .ok_or_else(|| NexusError::NotFound("reply: no inbound message to reply to".into()))?;
        let kind: String = row.get(0).map_err(|e| NexusError::Store(e.to_string()))?;
        let from_name: String = row.get(1).map_err(|e| NexusError::Store(e.to_string()))?;
        let from_agent_id: Option<String> =
            row.get(2).map_err(|e| NexusError::Store(e.to_string()))?;
        let thread_id: Option<String> = row.get(3).map_err(|e| NexusError::Store(e.to_string()))?;
        let scope = scope_from_kind(&kind);
        Ok((
            scope,
            from_name,
            from_agent_id.map(AgentId),
            thread_id.map(ThreadId),
        ))
    }

    async fn resolve_dm_agent_id(
        &self,
        caller: &Caller,
        agent_id: AgentId,
    ) -> Result<Resolved, NexusError> {
        Ok(Resolved::Dm(
            self.recipient_for_agent_id(caller, agent_id).await?,
        ))
    }

    async fn recipient_for_agent_id(
        &self,
        _caller: &Caller,
        agent_id: AgentId,
    ) -> Result<Recipient, NexusError> {
        let sessions = Sessions::new(&self.store);
        let row = match sessions
            .active_runtime_session_for_agent(&agent_id.0)
            .await?
        {
            Some(row) => row,
            None => sessions
                .find_by_agent_id(&agent_id.0)
                .await?
                .ok_or_else(|| NexusError::NotFound(format!("agent id {}", agent_id.0)))?,
        };
        Ok(Recipient {
            agent_id: Some(agent_id),
            session: row.session_id,
            name: row.name,
        })
    }

    /// Resolve stored thread member refs into sessions, preferring the stable `agent_id` recorded
    /// on the membership edge and falling back to the legacy display name for fossil rows.
    async fn thread_member_recipients(
        &self,
        caller: &Caller,
        thread_id: &ThreadId,
    ) -> Result<Vec<Recipient>, NexusError> {
        let members = Threads::new(&self.store).member_refs(thread_id).await?;
        let mut recipients = Vec::with_capacity(members.len());
        for member in members {
            if let Some(agent_id) = member.agent_id.as_deref() {
                if let Some(recipient) = self
                    .stable_edge_recipient(agent_id, None, Some(&member.session_name))
                    .await?
                {
                    recipients.push(recipient);
                }
                // A stable edge is exclusive. If its runtime is absent, the member is offline;
                // never reinterpret its mutable display alias as a different identity.
                continue;
            }
            if let Some(recipient) = self.member_recipient(caller, &member.session_name).await? {
                recipients.push(recipient);
            }
        }
        Ok(recipients)
    }

    async fn topic_subscriber_recipients(
        &self,
        _caller: &Caller,
        topic: &str,
    ) -> Result<Vec<Recipient>, NexusError> {
        if self.store.has_split_authority() {
            let topics = Topics::new(&self.store);
            if !topics.exists(topic).await? {
                return Err(NexusError::NotFound(format!("topic:{topic}")));
            }
            let subscribers = topics.subscriber_refs(topic).await?;
            let mut recipients = Vec::with_capacity(subscribers.len());
            for subscriber in subscribers {
                if let Some(agent_id) = subscriber.subscriber_agent_id.as_deref() {
                    if let Some(recipient) = self
                        .stable_edge_recipient(agent_id, Some(&subscriber.subscriber_session), None)
                        .await?
                    {
                        recipients.push(recipient);
                        continue;
                    }
                }
                let fallback = Sessions::new(&self.store)
                    .find_by_session_id(&SessionId(subscriber.subscriber_session.clone()))
                    .await?;
                recipients.push(Recipient {
                    agent_id: fallback
                        .as_ref()
                        .and_then(|session| session.agent_id.clone())
                        .map(AgentId),
                    session: SessionId(subscriber.subscriber_session),
                    name: fallback.and_then(|session| session.name),
                });
            }
            return Ok(recipients);
        }

        // The LEFT JOIN makes topic existence and every subscriber resolution one query. A topic
        // with zero subscribers still yields one marker row; an unknown topic yields no rows.
        let mut rows = self
            .store
            .conn
            .query(
                "WITH edges AS ( \
                   SELECT t.topic, s.subscriber_session, s.subscriber_agent_id, s.subscribed_at \
                   FROM topics t LEFT JOIN subscriptions s ON s.topic = t.topic \
                   WHERE t.topic = ?1 \
                 ) \
                 SELECT subscriber_agent_id, \
                   COALESCE( \
                     (SELECT r.runtime_id FROM agent_runtimes r \
                      WHERE r.agent_id = edges.subscriber_agent_id \
                        AND r.active = 1 AND r.stopped_at IS NULL \
                      ORDER BY r.rowid DESC LIMIT 1), \
                     subscriber_session \
                   ) AS recipient_session, \
                   COALESCE( \
                     (SELECT a.name FROM agents a \
                      WHERE a.agent_id = edges.subscriber_agent_id LIMIT 1), \
                     (SELECT s.name FROM sessions s \
                      WHERE s.agent_id = edges.subscriber_agent_id AND s.name IS NOT NULL \
                      ORDER BY s.rowid DESC LIMIT 1), \
                     (SELECT s.name FROM sessions s \
                      WHERE s.session_id = edges.subscriber_session LIMIT 1) \
                   ) AS recipient_name \
                 FROM edges ORDER BY subscribed_at",
                params![topic],
            )
            .await
            .map_err(|e| NexusError::Store(e.to_string()))?;
        let mut topic_exists = false;
        let mut recipients = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|e| NexusError::Store(e.to_string()))?
        {
            topic_exists = true;
            let session: Option<String> =
                row.get(1).map_err(|e| NexusError::Store(e.to_string()))?;
            let Some(session) = session else {
                continue;
            };
            let agent_id: Option<String> =
                row.get(0).map_err(|e| NexusError::Store(e.to_string()))?;
            let name: Option<String> = row.get(2).map_err(|e| NexusError::Store(e.to_string()))?;
            recipients.push(Recipient {
                agent_id: agent_id.map(AgentId),
                session: SessionId(session),
                name,
            });
        }
        if !topic_exists {
            return Err(NexusError::NotFound(format!("topic:{topic}")));
        }
        Ok(recipients)
    }

    /// Compose one stable identity edge across the persistent identity authority and the
    /// boot-scoped transport directory. SQL must never join tables owned by different databases.
    async fn stable_edge_recipient(
        &self,
        agent_id: &str,
        fallback_session: Option<&str>,
        fallback_name: Option<&str>,
    ) -> Result<Option<Recipient>, NexusError> {
        let agent_name = Agents::new(&self.store)
            .find_by_id(agent_id)
            .await?
            .and_then(|agent| agent.name);
        let sessions = Sessions::new(&self.store);
        if let Some(runtime) = sessions.active_runtime_session_for_agent(agent_id).await? {
            return Ok(Some(Recipient {
                agent_id: Some(AgentId(agent_id.to_string())),
                session: runtime.session_id,
                name: agent_name.or_else(|| fallback_name.map(str::to_string)),
            }));
        }

        let mut session = sessions.find_by_agent_id(agent_id).await?;
        if session.is_none() {
            // A stopped runtime remains exact stable-ID evidence. Its compatibility session may
            // predate the sessions.agent_id stamp, but runtime ownership is immutable evidence;
            // no mutable alias participates in this recovery path.
            for runtime in AgentRuntimes::new(&self.store)
                .list_for_agent(agent_id, true)
                .await?
            {
                let Some(candidate) = sessions
                    .find_by_session_id(&SessionId(runtime.runtime_id))
                    .await?
                else {
                    continue;
                };
                if candidate
                    .agent_id
                    .as_deref()
                    .is_some_and(|owner| owner != agent_id)
                {
                    return Err(NexusError::Invalid(format!(
                        "stable edge for agent {agent_id} points at session {} owned by {}",
                        candidate.session_id,
                        candidate.agent_id.as_deref().unwrap_or("<unbound>")
                    )));
                }
                session = Some(candidate);
                break;
            }
        }
        if session.is_none() {
            if let Some(session_id) = fallback_session {
                if let Some(candidate) = sessions
                    .find_by_session_id(&SessionId(session_id.to_string()))
                    .await?
                {
                    if candidate
                        .agent_id
                        .as_deref()
                        .is_some_and(|owner| owner != agent_id)
                    {
                        return Err(NexusError::Invalid(format!(
                            "stable edge for agent {agent_id} points at session {} owned by {}",
                            candidate.session_id,
                            candidate.agent_id.as_deref().unwrap_or("<unbound>")
                        )));
                    }
                    session = Some(candidate);
                }
            }
        }
        let session_id = session
            .as_ref()
            .map(|session| session.session_id.clone())
            .or_else(|| fallback_session.map(|session| SessionId(session.to_string())));
        Ok(session_id.map(|session_id| Recipient {
            agent_id: Some(AgentId(agent_id.to_string())),
            session: session_id,
            name: agent_name
                .or_else(|| session.and_then(|session| session.name))
                .or_else(|| fallback_name.map(str::to_string)),
        }))
    }

    /// Resolve one thread member name into its current recipient session. A member that no longer
    /// resolves stays in the roster but is absent from delivery/backfill fan-out.
    pub(crate) async fn member_recipient(
        &self,
        caller: &Caller,
        name: &str,
    ) -> Result<Option<Recipient>, NexusError> {
        let mut agents = Agents::new(&self.store).find_all_by_name(name).await?;
        match agents.len() {
            0 => {}
            1 => {
                return self
                    .recipient_for_agent_id(caller, AgentId(agents.remove(0).agent_id))
                    .await
                    .map(Some)
            }
            count => {
                return Err(NexusError::Ambiguous(format!(
                    "thread member name {name:?} matches {count} durable agents; address membership by stable agent id"
                )))
            }
        }
        let row = Sessions::new(&self.store)
            .find_unique_by_name_any_project(name)
            .await?;
        Ok(row.map(|row| Recipient {
            agent_id: row.agent_id.map(AgentId),
            session: row.session_id,
            name: row.name,
        }))
    }
}

fn is_local_operator_name(name: &str) -> bool {
    name == "operator"
}

fn scope_from_kind(kind: &str) -> Scope {
    match kind {
        "thread" => Scope::Thread,
        "topic" => Scope::Topic,
        _ => Scope::Dm,
    }
}
