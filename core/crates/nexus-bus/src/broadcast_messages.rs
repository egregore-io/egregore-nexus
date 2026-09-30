//! Broadcast a message to its recipients + atomicity (backend §3, §11). One send = **one** durable
//! `messages` row, then **N** `in_flight` enqueues (one per recipient), then a bell per recipient.
//! The message row + every `in_flight` row are written in **one explicit atomic store
//! unit** so a partial broadcast is impossible (§11), even when a caller already owns the outer
//! transaction; the bells are rung after the commit via [`DispatchPort::enqueue`] (the
//! realtime layer owns the bell — but the durable rows have already been written atomically, making
//! each enqueue an idempotent no-op on the already-written row). Producer idempotency keys are
//! stored on the message row and uniquely scoped to the sender session.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use futures::future::join_all;
use libsql::params;

use nexus_common::{new_message_id, NexusError};
use nexus_contracts::enums::{Kind, Scope};
use nexus_contracts::ids::{MessageId, ProjectId, ThreadId, TopicId};
use nexus_contracts::message::{Message, Provenance};
use nexus_contracts::ports::{Caller, DispatchPort};
use nexus_contracts::DeliveryTiming;
use nexus_store::repos::{DeliveryObligations, NewDeliveryObligation};
use nexus_store::Store;

use crate::router::Recipient;
use crate::sql_batch::BROADCAST_INGRESS_SQL;

/// The durable scope-specific fields of a single send, resolved from the `to` contract.
pub(crate) struct WriteSpec<'a> {
    pub scope: Scope,
    pub thread: Option<ThreadId>,
    pub topic: Option<TopicId>,
    /// DM display recipient. Normal runtime DMs derive this from the resolved recipient; local
    /// operator DMs have no runtime recipient, so they set it explicitly for read views.
    pub dm_name: Option<String>,
    /// Thread NAME (for the in-band provenance tag); `None` for DMs/topics.
    pub thread_name: Option<String>,
    /// Topic name (for provenance); `None` otherwise.
    pub topic_name: Option<String>,
    /// The CONVERSATION's project, when it differs from the caller's. Thread posts must stamp
    /// the thread's project on the durable row: recipients drain project-scoped, so a row
    /// stamped with a cross-project CALLER's project is undeliverable by construction and ages
    /// to DLQ (operator WS posts into a load-project thread).
    pub project: Option<String>,
    pub recipients: &'a [Recipient],
}

/// Write the canonical message row plus every recipient's `in_flight` row in one transaction,
/// then ring each recipient's bell. Returns the new message id and the number of recipients
/// actually delivered to.
///
/// The fan-out spine is shared by DM, thread, and topic: a sender is **never** delivered its own
/// message (the durable `messages` row still records it as the sender). The caller's own session is
/// filtered out of the recipient set here — one rule, every scope — so a DM, a thread post, and a
/// publish all behave identically (a DM already names only the other party, so this is a no-op there;
/// a thread/topic drops the sender if it is a member/subscriber).
///
/// Atomicity (§11): the `messages` row and all `in_flight` rows release together — a crash or error
/// mid-write leaves either nothing or the whole broadcast, never a partial set. The write goes
/// through [`Store::begin_write_txn`] — a real `BEGIN IMMEDIATE` serialized on the process-wide
/// write lock (no caller opens an outer transaction; the sole caller is `service.rs`). The bell
/// ring (`enqueue`) runs after commit; because the `in_flight` row already exists,
/// `DispatchPort::enqueue`'s `INSERT OR IGNORE` is a no-op and only the bell fires.
pub(crate) async fn broadcast_messages(
    store: &Store,
    realtime: &Arc<dyn DispatchPort>,
    caller: &Caller,
    spec: WriteSpec<'_>,
    summary: Option<String>,
    body: String,
    metadata: Option<serde_json::Map<String, serde_json::Value>>,
    mention: Vec<String>,
    delivery_timing: DeliveryTiming,
    created_at: i64,
    idempotency_key: Option<&str>,
    sender_kind: Kind,
) -> Result<(MessageId, u32), NexusError> {
    let message_id = new_message_id();
    let provenance = Provenance {
        from: caller.name.clone(),
        kind: sender_kind,
        thread: spec.thread_name.clone(),
        topic: spec.topic_name.clone(),
        stamp: None,
    };
    let msg = Message {
        id: message_id.clone(),
        project: ProjectId(
            spec.project
                .clone()
                .unwrap_or_else(|| caller.project.clone()),
        ),
        from: caller.name.clone(),
        scope: spec.scope,
        thread: spec.thread.clone(),
        topic: spec.topic.clone(),
        body,
        summary,
        provenance,
        created_at,
    };

    // ---- Single atomic unit: messages row + all in_flight rows (broadcast atomicity, §11). ----
    let provenance_json =
        serde_json::to_string(&msg.provenance).map_err(|e| NexusError::Store(e.to_string()))?;
    let metadata_json = metadata
        .map(|value| serde_json::to_string(&value))
        .transpose()
        .map_err(|error| NexusError::Store(error.to_string()))?;
    let mention_json = (!mention.is_empty())
        .then(|| serde_json::to_string(&mention))
        .transpose()
        .map_err(|error| NexusError::Store(error.to_string()))?;
    // `messages.to_name` is the display target used by UI/read models: DM rows carry the recipient
    // name when known; thread/topic rows carry the named conversation.
    let to_agent_id = match msg.scope {
        Scope::Dm if spec.recipients.len() == 1 => spec
            .recipients
            .first()
            .and_then(|r| r.agent_id.as_ref().map(|id| id.0.clone())),
        Scope::Dm => None,
        Scope::Thread | Scope::Topic => None,
    };
    let to_name = match msg.scope {
        Scope::Thread => msg.provenance.thread.clone(),
        Scope::Topic => msg.provenance.topic.clone(),
        Scope::Dm => spec
            .dm_name
            .clone()
            .or_else(|| spec.recipients.first().and_then(|r| r.name.clone())),
    };
    // `to_name` is optional display metadata. ID-addressed messages to unnamed agents must not
    // substitute the sender as the recipient; developer-event topic labels can use the stable id
    // without changing the canonical message row.
    let to_display = to_name
        .as_deref()
        .or(to_agent_id.as_deref())
        .unwrap_or("unnamed");
    // A sender is never delivered its own message — drop the caller's session from the recipient set
    // (one rule, shared by DM/thread/topic). This is what `Ack.fanout` counts.
    let delivered: Vec<&Recipient> = spec
        .recipients
        .iter()
        .filter(|r| {
            r.session != caller.session
                && match (&r.agent_id, &caller.agent_id) {
                    (Some(a), Some(c)) => a != c,
                    _ => true,
                }
        })
        .collect();

    // Persist the minimum restart capsule before exposing acceptance. This is intentionally
    // separate from the boot-scoped rich message row: after a daemon restart only this payload,
    // stable recipient and idempotency identity are needed to resume unsettled delivery.
    let continuity_payload = if store.has_split_authority() {
        Some(
            serde_json::to_string(&serde_json::json!({
                "message": msg,
                "deliveryTiming": delivery_timing,
            }))
            .map_err(|error| NexusError::Store(error.to_string()))?,
        )
    } else {
        None
    };
    let obligations = store
        .has_split_authority()
        .then(|| DeliveryObligations::new(store));
    let mut inserted_obligations: Vec<(String, String)> = Vec::new();
    if let (Some(obligations), Some(continuity_payload)) = (&obligations, continuity_payload) {
        for recipient in &delivered {
            let recipient_agent_id = recipient
                .agent_id
                .as_ref()
                .map(|id| id.0.clone())
                .unwrap_or_else(|| recipient.session.0.clone());
            if let Err(error) = obligations
                .insert(NewDeliveryObligation {
                    message_id: msg.id.0.clone(),
                    recipient_agent_id: recipient_agent_id.clone(),
                    recipient_runtime_id: Some(recipient.session.0.clone()),
                    payload_json: continuity_payload.clone(),
                    dedupe_key: format!("delivery:{}:{recipient_agent_id}", msg.id.0),
                    attempt: 0,
                    state: "pending".into(),
                    created_at,
                })
                .await
            {
                for (agent_id, _) in &inserted_obligations {
                    let _ = obligations.remove(&msg.id.0, agent_id).await;
                }
                return Err(error);
            }
            inserted_obligations.push((recipient_agent_id, recipient.session.0.clone()));
        }
    }

    // libsql's public transactional-batch API is string-only. The schema-owned write-only view
    // expands these two internal JSON arrays in one INSTEAD OF trigger, so message + FTS + every
    // in_flight row + developer events remain one request/implicit transaction while the 64 KiB
    // class body is bound exactly once and no caller-controlled value is interpolated into SQL.
    let recipients_json = serde_json::to_string(
        &delivered
            .iter()
            .map(|recipient| {
                serde_json::json!({
                    "inFlightId": new_message_id().0,
                    "session": recipient.session.0,
                    "agentId": recipient.agent_id.as_ref().map(|id| id.0.clone()),
                })
            })
            .collect::<Vec<_>>(),
    )
    .map_err(|error| NexusError::Store(error.to_string()))?;
    let events_json = serde_json::to_string(&developer_message_events(
        caller,
        msg.scope,
        to_display,
        msg.provenance.thread.as_deref(),
        spec.recipients,
        &delivered,
    ))
    .map_err(|error| NexusError::Store(error.to_string()))?;
    if let Err(error) = store
        .execute_parameterized_write(
            "broadcast",
            BROADCAST_INGRESS_SQL,
            params![
                msg.id.0.clone(),
                msg.from.clone(),
                scope_token(msg.scope),
                to_name.clone(),
                msg.thread.as_ref().map(|thread| thread.0.clone()),
                msg.topic.as_ref().map(|topic| topic.0.clone()),
                msg.summary.clone(),
                msg.body.clone(),
                provenance_json,
                msg.project.0.clone(),
                msg.created_at,
                caller.agent_id.as_ref().map(|id| id.0.clone()),
                to_agent_id,
                caller.session.0.clone(),
                idempotency_key.map(str::to_string),
                metadata_json,
                mention_json,
                delivery_timing.as_str(),
                recipients_json,
                events_json,
            ],
        )
        .await
    {
        if let Some(obligations) = &obligations {
            for (agent_id, _) in &inserted_obligations {
                let _ = obligations.remove(&msg.id.0, agent_id).await;
            }
        }
        return Err(error);
    }
    store.events().developer_event_appended().signal();

    // ---- Post-release: ring each recipient's bell (durable rows already exist). ----
    let bell_results = join_all(delivered.iter().map(|recipient| {
        let realtime = realtime.clone();
        let session = recipient.session.clone();
        let message = msg.id.clone();
        async move {
            let result = realtime.enqueue(&session, &message).await;
            (session, result)
        }
    }))
    .await;
    for (session, result) in bell_results {
        if let Err(error) = result {
            tracing::warn!(
                target: "nexus_bus::broadcast",
                recipient = %session,
                message = %msg.id,
                error = %error,
                "recipient bell failed after durable broadcast; continuing"
            );
        }
    }

    Ok((message_id, delivered.len() as u32))
}

fn scope_token(s: Scope) -> &'static str {
    match s {
        Scope::Dm => "dm",
        Scope::Thread => "thread",
        Scope::Topic => "topic",
    }
}

fn developer_message_events(
    caller: &Caller,
    scope: Scope,
    to_name: &str,
    thread_name: Option<&str>,
    recipients: &[Recipient],
    delivered: &[&Recipient],
) -> Vec<serde_json::Value> {
    let mut events: BTreeMap<String, (Option<String>, Option<String>)> = BTreeMap::new();
    match scope {
        Scope::Thread => {
            if let Some(thread) = thread_name {
                events.insert(
                    format!("sys.message.thread.{thread}"),
                    (Some(thread.to_string()), None),
                );
            }
            for name in visible_thread_names(&caller.name, recipients) {
                events.insert(
                    format!("sys.inbox.{name}"),
                    (thread_name.map(str::to_string), None),
                );
            }
        }
        Scope::Dm => {
            events.insert(
                format!("sys.dm.{}", caller.name),
                (None, Some(to_name.to_string())),
            );
            events.insert(
                format!("sys.dm.{to_name}"),
                (None, Some(caller.name.clone())),
            );
            for name in visible_dm_names(&caller.name, to_name, delivered) {
                let counterparty = if name == caller.name {
                    to_name
                } else {
                    &caller.name
                };
                events.insert(
                    format!("sys.inbox.{name}"),
                    (None, Some(counterparty.to_string())),
                );
            }
        }
        Scope::Topic => {
            for recipient in delivered {
                if let Some(name) = recipient.name.as_deref() {
                    events.insert(format!("sys.inbox.{name}"), (None, None));
                }
            }
        }
    }
    events
        .into_iter()
        .map(|(topic, (thread_name, dm_name))| {
            serde_json::json!({
                "topic": topic,
                "threadName": thread_name,
                "dmName": dm_name,
            })
        })
        .collect()
}

fn visible_thread_names(caller_name: &str, recipients: &[Recipient]) -> BTreeSet<String> {
    let mut names = BTreeSet::from([caller_name.to_string()]);
    for recipient in recipients {
        if let Some(name) = recipient.name.as_deref() {
            names.insert(name.to_string());
        }
    }
    names
}

fn visible_dm_names(
    caller_name: &str,
    to_name: &str,
    delivered: &[&Recipient],
) -> BTreeSet<String> {
    let mut names = BTreeSet::from([caller_name.to_string(), to_name.to_string()]);
    for recipient in delivered {
        if let Some(name) = recipient.name.as_deref() {
            names.insert(name.to_string());
        }
    }
    names
}
