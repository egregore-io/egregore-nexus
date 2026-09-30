//! Daemon-owned atomic mutations for the durable session prompt queue.

use std::collections::{HashMap, HashSet};

use libsql::params;
use nexus_common::NexusError;
use nexus_contracts::{
    AgentId, CommandQueueAction, CommandQueueEntry, CommandQueueMutationRequest,
    CommandQueueSnapshot, CommandQueueState, CommandQueueTransition, DaemonIpcCaller, Kind,
    SessionId, SteerCapability, Tier,
};
use serde_json::{json, Map, Value};

use crate::error::{store_err, store_msg};
use crate::repos::sessions::{get_opt_int, get_opt_text, get_text};
use crate::repos::Sessions;
use crate::state::{Store, WriteTxn};

const PROMPT: &str = crate::command_kinds::harness::PROMPT;
const STEER: &str = crate::command_kinds::harness::STEER;

/// HTTP-neutral payload plus the compatibility status used by the local gateway route.
#[derive(Debug, Clone, PartialEq)]
pub struct CommandQueueMutationOutcome {
    pub status: u16,
    pub body: Value,
}

/// Private call evidence, never deserialized from a mutation or included in retry identity.
#[derive(Debug, Clone)]
pub struct CapturedQueueCapability {
    pub session_id: SessionId,
    pub steer_capability: SteerCapability,
}

/// Bounded monotonic queue transitions returned to the Gateway reconnect poller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandQueueEventsPage {
    pub events: Vec<CommandQueueTransition>,
    pub next_seq: i64,
    pub latest_seq: i64,
    pub gap: bool,
}

/// Atomic pending-queue mutation service. The daemon is its only production caller.
pub struct CommandQueue<'a> {
    store: &'a Store,
}

#[derive(Debug, Clone, Copy)]
struct QueueTarget<'a> {
    name: Option<&'a str>,
    agent_id: Option<&'a str>,
    expected_session_id: Option<&'a str>,
}

impl<'a> QueueTarget<'a> {
    fn from_request(request: &'a CommandQueueMutationRequest) -> Self {
        Self {
            name: request
                .name
                .as_deref()
                .map(str::trim)
                .filter(|name| !name.is_empty()),
            agent_id: request.agent_id.as_ref().map(|id| id.0.as_str()),
            expected_session_id: request.expected_session_id.as_ref().map(|id| id.0.as_str()),
        }
    }

    fn display_name(&self, runtime: &TargetRuntime) -> String {
        runtime
            .name
            .as_deref()
            .or(self.name)
            .or(self.agent_id)
            .unwrap_or_default()
            .to_string()
    }
}

#[derive(Debug, Clone)]
struct TargetRuntime {
    session_id: Option<String>,
    name: Option<String>,
    steer_capability: SteerCapability,
    turn_active: bool,
}

impl TargetRuntime {
    fn steer_capability(&self) -> SteerCapability {
        self.steer_capability
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueueEffect {
    None,
    WakeRedirect,
    CompleteCancelled,
}

impl<'a> CommandQueue<'a> {
    pub fn new(store: &'a Store) -> Self {
        Self { store }
    }

    /// Read original submission evidence, without enqueueing or claiming work.
    pub async fn snapshot_for_submission(
        &self,
        agent_id: &AgentId,
        session_id: &str,
        client_message_id: &str,
        requester: &DaemonIpcCaller,
    ) -> Result<CommandQueueSnapshot, NexusError> {
        if client_message_id.trim().is_empty()
            || client_message_id.len() > 256
            || client_message_id.chars().any(char::is_control)
        {
            return Err(NexusError::Invalid(
                "invalid original client message id".into(),
            ));
        }
        self.snapshot_selected(
            None,
            Some(agent_id),
            Some(session_id),
            &[],
            Some(requester),
            Some(client_message_id),
        )
        .await
    }

    /// Hydrate one daemon-owned session queue without exposing the raw store over IPC.
    pub async fn snapshot_with_active_sessions(
        &self,
        name: Option<&str>,
        agent_id: Option<&AgentId>,
        active_sessions: &[SessionId],
    ) -> Result<CommandQueueSnapshot, NexusError> {
        self.snapshot_for_requester(name, agent_id, None, active_sessions, None)
            .await
    }

    /// Exact reads may inspect retained, inactive runtimes. This never grants execution authority.
    pub async fn snapshot_for_requester(
        &self,
        name: Option<&str>,
        agent_id: Option<&AgentId>,
        expected_session_id: Option<&str>,
        active_sessions: &[SessionId],
        requester: Option<&DaemonIpcCaller>,
    ) -> Result<CommandQueueSnapshot, NexusError> {
        self.snapshot_selected(
            name,
            agent_id,
            expected_session_id,
            active_sessions,
            requester,
            None,
        )
        .await
    }

    async fn snapshot_selected(
        &self,
        name: Option<&str>,
        agent_id: Option<&AgentId>,
        expected_session_id: Option<&str>,
        active_sessions: &[SessionId],
        requester: Option<&DaemonIpcCaller>,
        client_message_id: Option<&str>,
    ) -> Result<CommandQueueSnapshot, NexusError> {
        let target = QueueTarget {
            name: name.map(str::trim).filter(|name| !name.is_empty()),
            agent_id: agent_id.map(|id| id.0.as_str()),
            expected_session_id,
        };
        let runtime = if let Some(expected) = expected_session_id {
            let Some(agent) = target.agent_id.filter(|id| !id.trim().is_empty()) else {
                return Err(NexusError::Invalid(
                    "expectedSessionId requires stable agentId".into(),
                ));
            };
            if expected.trim().is_empty()
                || !retained_runtime_owned(self.store, agent, expected).await?
            {
                return Err(NexusError::Invalid(
                    "exact session is not retained by this agent".into(),
                ));
            }
            let descriptive = Sessions::new(self.store)
                .find_by_session_id(&SessionId(expected.to_string()))
                .await?;
            let mut runtime =
                runtime_from_session(self.store, descriptive, active_sessions).await?;
            runtime.session_id = Some(expected.to_string());
            if runtime.name.is_none() {
                runtime.name = crate::repos::Agents::new(self.store)
                    .find_by_id(agent)
                    .await?
                    .and_then(|row| row.name);
            }
            runtime
        } else {
            target_runtime(self.store, target, active_sessions).await?
        };
        let seq = latest_queue_seq_store(self.store).await?;
        let mut commands = queue_entries(
            self.store,
            target,
            runtime.session_id.as_deref(),
            client_message_id,
            requester,
        )
        .await?;
        for command in &mut commands {
            command.correlation_owned =
                Some(correlation_owned(self.store, &command.command_id, requester).await?);
        }
        let steer_capability = runtime.steer_capability();
        Ok(CommandQueueSnapshot {
            target: target.display_name(&runtime),
            session_id: runtime.session_id,
            turn_active: runtime.turn_active,
            steer_capability,
            seq,
            revision: seq,
            commands,
            observation: None,
        })
    }

    /// Read one bounded global transition page after a reconnect cursor.
    pub async fn events_after(&self, after_seq: i64) -> Result<CommandQueueEventsPage, NexusError> {
        queue_events_after(self.store, after_seq).await
    }

    /// Shared authenticated global polling retains a scanned cursor even when rows are filtered.
    pub async fn events_for_requester(
        &self,
        after_seq: i64,
        requester: Option<&DaemonIpcCaller>,
    ) -> Result<CommandQueueEventsPage, NexusError> {
        let mut page = queue_events_after(self.store, after_seq).await?;
        for event in &mut page.events {
            event.correlation_owned =
                Some(correlation_owned(self.store, &event.command_id, requester).await?);
        }
        Ok(page)
    }

    /// Apply one idempotent compare-and-set mutation under the store's single write transaction.
    /// `project` is retained only as audit metadata; command identity and queue cursors are global.
    pub async fn mutate(
        &self,
        project: &str,
        request: &CommandQueueMutationRequest,
        now: i64,
    ) -> Result<CommandQueueMutationOutcome, NexusError> {
        self.mutate_with_active_sessions(project, request, now, &[])
            .await
    }

    /// Apply a queue mutation with the daemon's current adapter-owned active-turn snapshot.
    pub async fn mutate_with_active_sessions(
        &self,
        project: &str,
        request: &CommandQueueMutationRequest,
        now: i64,
        active_sessions: &[SessionId],
    ) -> Result<CommandQueueMutationOutcome, NexusError> {
        self.mutate_with_capability(project, request, now, active_sessions, None)
            .await
    }

    pub async fn mutate_with_capability(
        &self,
        project: &str,
        request: &CommandQueueMutationRequest,
        now: i64,
        active_sessions: &[SessionId],
        capability: Option<&CapturedQueueCapability>,
    ) -> Result<CommandQueueMutationOutcome, NexusError> {
        if request.expected_session_id.as_ref().is_some_and(|id| {
            id.0.trim().is_empty()
                || request
                    .agent_id
                    .as_ref()
                    .is_none_or(|agent| agent.0.trim().is_empty())
        }) {
            return Ok(error_outcome(
                request,
                "expectedSessionId requires a nonempty session id and stable agentId",
                400,
            ));
        }
        if request
            .name
            .as_deref()
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .is_none()
            && request.agent_id.is_none()
        {
            return Ok(error_outcome(request, "name or agentId is required", 400));
        }
        if request.client_mutation_id.trim().is_empty() {
            return Ok(error_outcome(request, "clientMutationId is required", 400));
        }

        let canonical = canonical_request(request)?;
        let canonical_json = serde_json::to_string(&canonical).map_err(store_msg)?;
        let target = QueueTarget::from_request(request);
        let tx = self
            .store
            .begin_identity_write_txn("command_queue_mutation")
            .await?;

        if let Some(previous) = replay(&tx, request, &canonical).await? {
            tx.commit().await?;
            return Ok(previous);
        }

        // Resolve NEW work under the same identity write gate as apply. A
        // committed receipt replays without re-resolving a later runtime.
        let mut runtime = mutation_target_runtime(self.store, &tx, target, active_sessions).await?;
        runtime.steer_capability = capability
            .filter(|captured| {
                runtime.session_id.as_deref() == Some(captured.session_id.0.as_str())
            })
            .map(|captured| captured.steer_capability)
            .unwrap_or(SteerCapability::None);

        tx.execute(
            "INSERT INTO command_queue_mutations \
             (project, client_mutation_id, request_json, response_status, response_json, created_at) \
             VALUES (?1, ?2, ?3, NULL, NULL, ?4)",
            params![project, request.client_mutation_id.as_str(), canonical_json, now],
        )
        .await?;

        let (outcome, effect) = apply_mutation(&tx, request, target, now, &runtime).await?;
        let response_json = serde_json::to_string(&outcome.body).map_err(store_msg)?;
        tx.execute(
            "UPDATE command_queue_mutations SET response_status = ?1, response_json = ?2 \
             WHERE project = ?3 AND client_mutation_id = ?4",
            params![
                i64::from(outcome.status),
                response_json,
                project,
                request.client_mutation_id.as_str()
            ],
        )
        .await?;
        tx.commit().await?;

        match effect {
            QueueEffect::WakeRedirect => self.store.notify_command_intent_inserted(),
            QueueEffect::CompleteCancelled => {
                self.store.events().command_intent_completed().signal()
            }
            QueueEffect::None => {}
        }
        Ok(outcome)
    }
}

async fn replay(
    tx: &WriteTxn,
    request: &CommandQueueMutationRequest,
    canonical: &Value,
) -> Result<Option<CommandQueueMutationOutcome>, NexusError> {
    let mut rows = tx
        .query(
            "SELECT request_json, response_status, response_json FROM command_queue_mutations \
             WHERE client_mutation_id = ?1 ORDER BY created_at LIMIT 1",
            params![request.client_mutation_id.as_str()],
        )
        .await?;
    let Some(row) = rows.next().await.map_err(store_err)? else {
        return Ok(None);
    };
    let previous_json = get_text(&row, 0)?;
    let previous = serde_json::from_str::<Value>(&previous_json).map_err(store_msg)?;
    if previous != *canonical {
        return Ok(Some(error_outcome(
            request,
            "clientMutationId was already used for a different mutation",
            409,
        )));
    }
    let status = get_opt_int(&row, 1)?;
    let body = get_opt_text(&row, 2)?;
    match (status, body) {
        (Some(status), Some(body)) => Ok(Some(CommandQueueMutationOutcome {
            status: u16::try_from(status).map_err(store_msg)?,
            body: serde_json::from_str(&body).map_err(store_msg)?,
        })),
        _ => Ok(Some(error_outcome(
            request,
            "queue mutation is still being committed",
            409,
        ))),
    }
}

async fn target_runtime(
    store: &Store,
    target: QueueTarget<'_>,
    active_sessions: &[SessionId],
) -> Result<TargetRuntime, NexusError> {
    let sessions = Sessions::new(store);
    let session = match target.agent_id {
        Some(agent_id) => match sessions.active_runtime_session_for_agent(agent_id).await? {
            Some(session) => Some(session),
            None => sessions.find_by_agent_id(agent_id).await?,
        },
        None => match target.name {
            Some(name) => sessions.find_unique_by_name_any_project(name).await?,
            None => None,
        },
    };
    runtime_from_session(store, session, active_sessions).await
}

async fn mutation_target_runtime(
    store: &Store,
    tx: &WriteTxn,
    target: QueueTarget<'_>,
    active_sessions: &[SessionId],
) -> Result<TargetRuntime, NexusError> {
    let sessions = Sessions::new(store);
    let session = if let Some(agent_id) = target.agent_id {
        // agent_runtimes belongs to identity authority. Transport session rows
        // describe this binding but cannot choose a replacement for exact mode.
        let mut rows = tx.query(
            "SELECT runtime_id FROM agent_runtimes WHERE agent_id = ?1 AND active = 1 AND stopped_at IS NULL ORDER BY started_at DESC LIMIT 1",
            params![agent_id],
        ).await?;
        if let Some(row) = rows.next().await.map_err(store_err)? {
            let runtime_id = get_text(&row, 0)?;
            let session = sessions
                .find_by_session_id(&SessionId(runtime_id.clone()))
                .await?;
            if session
                .as_ref()
                .is_some_and(|row| row.agent_id.as_deref() != Some(agent_id))
            {
                return Err(NexusError::Invalid(format!(
                    "active runtime {runtime_id} is not owned by agent {agent_id}"
                )));
            }
            if session.is_none() && target.expected_session_id.is_none() {
                sessions.find_by_agent_id(agent_id).await?
            } else {
                session
            }
        } else if target.expected_session_id.is_none() {
            sessions.find_by_agent_id(agent_id).await?
        } else {
            None
        }
    } else if let Some(name) = target.name {
        sessions.find_unique_by_name_any_project(name).await?
    } else {
        None
    };
    runtime_from_session(store, session, active_sessions).await
}

async fn runtime_from_session(
    store: &Store,
    session: Option<crate::types::SessionRow>,
    active_sessions: &[SessionId],
) -> Result<TargetRuntime, NexusError> {
    let Some(session) = session else {
        return Ok(TargetRuntime {
            session_id: None,
            name: None,
            steer_capability: SteerCapability::None,
            turn_active: false,
        });
    };
    let mut turn_active = active_sessions
        .iter()
        .any(|active| active == &session.session_id);
    if !turn_active && !store.has_split_authority() {
        let mut rows = store
            .conn
            .query(
                "SELECT 1 FROM agent_session_turns WHERE session_id = ?1 \
                 AND status = 'streaming' AND finalized_at IS NULL LIMIT 1",
                params![session.session_id.0.clone()],
            )
            .await
            .map_err(store_err)?;
        turn_active = rows.next().await.map_err(store_err)?.is_some();
    }
    Ok(TargetRuntime {
        session_id: Some(session.session_id.0),
        name: session.name,
        steer_capability: SteerCapability::None,
        turn_active,
    })
}

async fn queue_entries(
    store: &Store,
    target: QueueTarget<'_>,
    session_id: Option<&str>,
    client_message_id: Option<&str>,
    requester: Option<&DaemonIpcCaller>,
) -> Result<Vec<CommandQueueEntry>, NexusError> {
    let has_agent_id = i64::from(target.agent_id.is_some());
    let has_session_id = i64::from(session_id.is_some());
    let identity = store.identity_conn();
    let mut rows = identity
        .query(
            "SELECT command_id, kind, status, request_json, error_json, revision, created_at, \
             claimed_at, started_at, completed_at, COALESCE((SELECT MAX(seq) \
             FROM command_intent_events e WHERE e.command_id = command_intents.command_id), 0), \
             (SELECT session_id FROM command_intent_events e WHERE e.command_id = command_intents.command_id ORDER BY seq LIMIT 1) \
             FROM command_intents WHERE kind IN (?1, ?2) \
             AND (?8 IS NULL OR json_extract(request_json, '$.clientMessageId') = ?8) AND ( \
               (?3 = 1 AND json_extract(request_json, '$.agentId') = ?4) OR \
               (?3 = 0 AND json_extract(request_json, '$.agentId') IS NULL \
                  AND json_extract(request_json, '$.name') = ?5) OR \
               (?6 = 1 AND command_id IN (SELECT command_id FROM command_intent_events \
                  WHERE session_id = ?7)) \
             ) ORDER BY CASE WHEN status IN ('pending', 'claimed') THEN 0 ELSE 1 END, \
             created_at ASC",
            params![
                PROMPT,
                STEER,
                has_agent_id,
                target.agent_id.unwrap_or(""),
                target.name.unwrap_or(""),
                has_session_id,
                session_id.unwrap_or(""),
                client_message_id
            ],
        )
        .await
        .map_err(store_err)?;
    let mut entries = Vec::new();
    while let Some(row) = rows.next().await.map_err(store_err)? {
        let request_json = get_text(&row, 3)?;
        let request = serde_json::from_str::<Value>(&request_json).unwrap_or(Value::Null);
        let binding =
            command_session_binding(store, &request, get_opt_text(&row, 11)?.as_deref()).await?;
        if binding.as_deref() != session_id || binding.is_none() {
            continue;
        }
        if client_message_id.is_some()
            && !correlation_owned(store, &get_text(&row, 0)?, requester).await?
        {
            continue;
        }
        if client_message_id.is_some() && !entries.is_empty() {
            return Err(NexusError::Invalid(
                "original client message id has conflicting durable commands".into(),
            ));
        }
        let status = get_text(&row, 2)?;
        let started_at = get_opt_int(&row, 8)?;
        entries.push(CommandQueueEntry {
            command_id: get_text(&row, 0)?,
            client_message_id: request
                .get("clientMessageId")
                .and_then(Value::as_str)
                .map(str::to_string),
            session_id: session_id.map(str::to_string),
            text: request
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            state: command_queue_state(&status, started_at),
            mode: if get_text(&row, 1)? == STEER {
                "redirect".into()
            } else {
                "queue".into()
            },
            revision: get_opt_int(&row, 5)?.unwrap_or(1),
            seq: get_opt_int(&row, 10)?.unwrap_or_default(),
            created_at: get_opt_int(&row, 6)?.unwrap_or_default(),
            claimed_at: get_opt_int(&row, 7)?,
            started_at,
            completed_at: get_opt_int(&row, 9)?,
            error: get_opt_text(&row, 4)?
                .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
                .and_then(|value| {
                    value
                        .get("message")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                }),
            error_code: get_opt_text(&row, 4)?
                .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
                .and_then(|value| value.get("code").and_then(Value::as_i64))
                .and_then(|code| i32::try_from(code).ok()),
            correlation_owned: None,
        });
        // Validate ownership before applying the bounded viewport; another lane cannot starve it.
        if entries.len() == 100 {
            break;
        }
    }
    Ok(entries)
}

/// Read the original request binding. Event session ids alone are not target authority.
pub(crate) async fn command_session_binding(
    store: &Store,
    request: &Value,
    original_session: Option<&str>,
) -> Result<Option<String>, NexusError> {
    let agent = request
        .get("agentId")
        .and_then(Value::as_str)
        .filter(|id| !id.trim().is_empty());
    if request.get("expectedSessionId").is_some() {
        let Some(session) = request
            .get("expectedSessionId")
            .and_then(Value::as_str)
            .filter(|id| !id.trim().is_empty())
        else {
            return Ok(None);
        };
        let Some(agent) = agent else {
            return Ok(None);
        };
        return Ok(retained_runtime_owned(store, agent, session)
            .await?
            .then(|| session.to_string()));
    }
    let Some(session) = original_session.filter(|id| !id.is_empty()) else {
        return Ok(None);
    };
    let row = Sessions::new(store)
        .find_by_session_id(&SessionId(session.to_string()))
        .await?;
    let runtime = crate::repos::AgentRuntimes::new(store)
        .find_by_runtime_id(session)
        .await?;
    if let Some(agent) = agent {
        if runtime.as_ref().is_some_and(|row| row.agent_id != agent)
            || row
                .as_ref()
                .is_some_and(|row| row.agent_id.as_deref() != Some(agent))
        {
            return Ok(None);
        }
        if runtime.is_some() || row.is_some() {
            return Ok(Some(session.to_string()));
        }
    } else if let Some(row) = row {
        if row
            .agent_id
            .as_deref()
            .is_none_or(|id| id.trim().is_empty())
        {
            return Ok(None);
        }
        // Without caller-supplied sessionId, the first journal row captured the
        // target at enqueue. Its retained owner survives descriptive renames.
        if request
            .get("name")
            .and_then(Value::as_str)
            .is_some_and(|name| {
                !name.trim().is_empty()
                    && (Some(name) == row.name.as_deref() || request.get("sessionId").is_none())
            })
            && runtime
                .as_ref()
                .is_none_or(|runtime| row.agent_id.as_deref() == Some(runtime.agent_id.as_str()))
        {
            return Ok(Some(session.to_string()));
        }
    }
    Ok(None)
}

async fn retained_runtime_owned(
    store: &Store,
    agent: &str,
    session: &str,
) -> Result<bool, NexusError> {
    let runtime = crate::repos::AgentRuntimes::new(store)
        .find_by_runtime_id(session)
        .await?;
    if runtime.as_ref().is_none_or(|row| row.agent_id != agent) {
        return Ok(false);
    }
    let descriptive = Sessions::new(store)
        .find_by_session_id(&SessionId(session.to_string()))
        .await?;
    Ok(descriptive
        .as_ref()
        .is_none_or(|row| row.agent_id.as_deref() == Some(agent)))
}

async fn correlation_owned(
    store: &Store,
    command_id: &str,
    requester: Option<&DaemonIpcCaller>,
) -> Result<bool, NexusError> {
    let Some(requester) = requester else {
        return Ok(false);
    };
    let Some(caller) = crate::repos::CommandIntents::new(store)
        .get(command_id)
        .await?
    else {
        return Ok(false);
    };
    if caller.project != requester.project
        || requester.kind != Kind::Human
        || caller
            .caller_kind
            .as_deref()
            .and_then(nexus_contracts::entity_kind::parse)
            != Some((requester.locality, requester.kind))
        || caller.caller_tier.as_deref()
            != Some(match requester.tier {
                Tier::Admin => "admin",
                Tier::Agent => "agent",
            })
    {
        return Ok(false);
    }
    if let Some(key) = requester
        .client_key
        .as_deref()
        .filter(|key| !key.is_empty())
    {
        return Ok(caller.caller_client_key.as_deref() == Some(key)
            && requester
                .principal_id
                .as_deref()
                .is_some_and(|id| !id.is_empty())
            && caller.caller_principal_id == requester.principal_id);
    }
    Ok(requester.tier == Tier::Admin
        && requester.session_id.as_deref() == Some("local-operator")
        && matches!(
            requester.runtime_id.as_deref(),
            None | Some("local-operator")
        )
        && caller.caller_session_id.as_deref() == Some("local-operator")
        && matches!(
            caller.caller_runtime_id.as_deref(),
            None | Some("local-operator")
        )
        && caller.caller_client_key.is_none())
}

async fn latest_queue_seq_store(store: &Store) -> Result<i64, NexusError> {
    let identity = store.identity_conn();
    let mut rows = identity
        .query(
            "SELECT COALESCE(MAX(seq), 0) FROM command_intent_events",
            (),
        )
        .await
        .map_err(store_err)?;
    match rows.next().await.map_err(store_err)? {
        Some(row) => Ok(get_opt_int(&row, 0)?.unwrap_or_default()),
        None => Ok(0),
    }
}

async fn queue_events_after(
    store: &Store,
    after_seq: i64,
) -> Result<CommandQueueEventsPage, NexusError> {
    let identity = store.identity_conn();
    let mut bounds = identity
        .query(
            "SELECT COALESCE(MIN(CASE WHEN seq > ?1 THEN seq END), 0), \
             COALESCE(MAX(seq), 0) FROM command_intent_events",
            params![after_seq],
        )
        .await
        .map_err(store_err)?;
    let (next_seq, latest_seq) = match bounds.next().await.map_err(store_err)? {
        Some(row) => (
            get_opt_int(&row, 0)?.unwrap_or_default(),
            get_opt_int(&row, 1)?.unwrap_or_default(),
        ),
        None => (0, 0),
    };
    let gap = after_seq > 0 && next_seq > after_seq.saturating_add(1);
    if gap {
        return Ok(CommandQueueEventsPage {
            events: Vec::new(),
            next_seq,
            latest_seq,
            gap,
        });
    }
    let mut rows = identity
        .query(
            "SELECT e.seq, e.session_id, e.command_id, e.client_message_id, \
                    c.kind, c.caller_name, c.caller_session_id, c.caller_agent_id, \
                    c.caller_principal_id, c.caller_kind, e.state, e.mode, e.revision, c.request_json, \
                    (SELECT session_id FROM command_intent_events first WHERE first.command_id = c.command_id ORDER BY seq LIMIT 1) \
             FROM command_intent_events e \
             LEFT JOIN command_intents c ON c.command_id = e.command_id \
             WHERE e.seq > ?1 ORDER BY e.seq LIMIT 500",
            params![after_seq],
        )
        .await
        .map_err(store_err)?;
    let mut events = Vec::new();
    let mut scanned_seq = after_seq;
    while let Some(row) = rows.next().await.map_err(store_err)? {
        scanned_seq = get_opt_int(&row, 0)?.unwrap_or(scanned_seq);
        let request = get_opt_text(&row, 13)?
            .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
            .unwrap_or(Value::Null);
        let session_id =
            command_session_binding(store, &request, get_opt_text(&row, 14)?.as_deref()).await?;
        if session_id.is_none() {
            continue;
        }
        events.push(CommandQueueTransition {
            seq: get_opt_int(&row, 0)?.unwrap_or_default(),
            session_id,
            command_id: get_text(&row, 2)?,
            client_message_id: get_opt_text(&row, 3)?,
            command_kind: get_text(&row, 4)?,
            caller_name: get_text(&row, 5)?,
            caller_session_id: get_opt_text(&row, 6)?,
            caller_agent_id: get_opt_text(&row, 7)?.map(AgentId),
            caller_principal_id: get_opt_text(&row, 8)?,
            caller_kind: get_opt_text(&row, 9)?,
            state: command_queue_event_state(&get_text(&row, 10)?),
            mode: get_text(&row, 11)?,
            revision: get_opt_int(&row, 12)?.unwrap_or_default(),
            correlation_owned: None,
        });
    }
    Ok(CommandQueueEventsPage {
        events,
        next_seq: scanned_seq,
        latest_seq,
        gap,
    })
}

fn command_queue_state(status: &str, started_at: Option<i64>) -> CommandQueueState {
    match status {
        "pending" => CommandQueueState::Queued,
        "claimed" if started_at.is_some() => CommandQueueState::Started,
        "claimed" => CommandQueueState::Claimed,
        "done" => CommandQueueState::Completed,
        "error" => CommandQueueState::Failed,
        "cancelled" => CommandQueueState::Cancelled,
        _ => CommandQueueState::Failed,
    }
}

fn command_queue_event_state(state: &str) -> CommandQueueState {
    match state {
        "queued" => CommandQueueState::Queued,
        "claimed" => CommandQueueState::Claimed,
        "started" => CommandQueueState::Started,
        "completed" => CommandQueueState::Completed,
        "failed" => CommandQueueState::Failed,
        "cancelled" => CommandQueueState::Cancelled,
        _ => CommandQueueState::Failed,
    }
}

async fn apply_mutation(
    tx: &WriteTxn,
    request: &CommandQueueMutationRequest,
    target: QueueTarget<'_>,
    now: i64,
    runtime: &TargetRuntime,
) -> Result<(CommandQueueMutationOutcome, QueueEffect), NexusError> {
    if target
        .expected_session_id
        .is_some_and(|expected| runtime.session_id.as_deref() != Some(expected))
    {
        return Ok((
            error_outcome(
                request,
                "expected session does not match the agent's existing active runtime",
                409,
            ),
            QueueEffect::None,
        ));
    }
    match request.action {
        CommandQueueAction::RedirectNow => {
            let Some((command_id, expected_revision)) = command_and_revision(request) else {
                return Ok((
                    error_outcome(request, "commandId and expectedRevision are required", 400),
                    QueueEffect::None,
                ));
            };
            if runtime.steer_capability() == SteerCapability::None {
                return Ok((
                    error_outcome(
                        request,
                        "target harness cannot redirect an active turn",
                        409,
                    ),
                    QueueEffect::None,
                ));
            }
            if !runtime.turn_active {
                return Ok((
                    error_outcome(request, "target has no active turn to redirect", 409),
                    QueueEffect::None,
                ));
            }
            if !command_matches_target(tx, command_id, target, runtime, true).await? {
                return Ok((cas_conflict(request), QueueEffect::None));
            }
            let changed = tx
                .execute(
                    "UPDATE command_intents SET kind = ?1, revision = revision + 1 \
                     WHERE command_id = ?2 AND kind = ?3 \
                       AND status = 'pending' AND revision = ?4",
                    params![STEER, command_id, PROMPT, expected_revision],
                )
                .await?;
            if changed != 1 {
                return Ok((cas_conflict(request), QueueEffect::None));
            }
            let revision = command_revision(tx, command_id).await?;
            let seq = latest_queue_seq(tx).await?;
            Ok((
                success_outcome(request, runtime, CommandQueueState::Queued, seq, revision),
                QueueEffect::WakeRedirect,
            ))
        }
        CommandQueueAction::Cancel => {
            let Some((command_id, expected_revision)) = command_and_revision(request) else {
                return Ok((
                    error_outcome(request, "commandId and expectedRevision are required", 400),
                    QueueEffect::None,
                ));
            };
            if !command_matches_target(tx, command_id, target, runtime, false).await? {
                return Ok((cas_conflict(request), QueueEffect::None));
            }
            let changed = tx
                .execute(
                    "UPDATE command_intents SET status = 'cancelled', revision = revision + 1, \
                     completed_at = ?1, lease_until = NULL WHERE command_id = ?2 \
                       AND kind = ?3 AND status = 'pending' AND revision = ?4",
                    params![now, command_id, PROMPT, expected_revision],
                )
                .await?;
            if changed != 1 {
                return Ok((cas_conflict(request), QueueEffect::None));
            }
            let revision = command_revision(tx, command_id).await?;
            let seq = latest_queue_seq(tx).await?;
            Ok((
                success_outcome(
                    request,
                    runtime,
                    CommandQueueState::Cancelled,
                    seq,
                    revision,
                ),
                QueueEffect::CompleteCancelled,
            ))
        }
        CommandQueueAction::Edit => {
            let Some((command_id, expected_revision)) = command_and_revision(request) else {
                return Ok((
                    error_outcome(
                        request,
                        "commandId, expectedRevision, and text are required",
                        400,
                    ),
                    QueueEffect::None,
                ));
            };
            let Some(text) = request
                .text
                .as_deref()
                .map(str::trim)
                .filter(|text| !text.is_empty())
            else {
                return Ok((
                    error_outcome(
                        request,
                        "commandId, expectedRevision, and text are required",
                        400,
                    ),
                    QueueEffect::None,
                ));
            };
            if !command_matches_target(tx, command_id, target, runtime, false).await? {
                return Ok((cas_conflict(request), QueueEffect::None));
            }
            let changed = tx
                .execute(
                    "UPDATE command_intents SET request_json = json_set(request_json, '$.text', ?1), \
                     revision = revision + 1 WHERE command_id = ?2 AND kind = ?3 \
                       AND status = 'pending' AND revision = ?4",
                    params![text, command_id, PROMPT, expected_revision],
                )
                .await?;
            if changed != 1 {
                return Ok((cas_conflict(request), QueueEffect::None));
            }
            let revision = command_revision(tx, command_id).await?;
            let seq = latest_queue_seq(tx).await?;
            Ok((
                success_outcome(request, runtime, CommandQueueState::Queued, seq, revision),
                QueueEffect::None,
            ))
        }
        CommandQueueAction::Reorder => reorder(tx, request, target, runtime).await,
    }
}

async fn reorder(
    tx: &WriteTxn,
    request: &CommandQueueMutationRequest,
    target: QueueTarget<'_>,
    runtime: &TargetRuntime,
) -> Result<(CommandQueueMutationOutcome, QueueEffect), NexusError> {
    let unique = request.command_ids.iter().collect::<HashSet<_>>();
    if request.command_ids.is_empty() || unique.len() != request.command_ids.len() {
        return Ok((
            error_outcome(request, "commandIds must be a non-empty unique list", 400),
            QueueEffect::None,
        ));
    }
    let expected = request
        .expected_revisions
        .iter()
        .map(|entry| (entry.command_id.as_str(), entry.revision))
        .collect::<HashMap<_, _>>();
    if expected.len() != request.command_ids.len()
        || request
            .command_ids
            .iter()
            .any(|command_id| !expected.contains_key(command_id.as_str()))
    {
        return Ok((
            error_outcome(request, "expectedRevisions must cover every commandId", 400),
            QueueEffect::None,
        ));
    }

    let mut created_at = Vec::with_capacity(request.command_ids.len());
    for command_id in &request.command_ids {
        if !command_matches_target(tx, command_id, target, runtime, false).await? {
            return Ok((reorder_conflict(request), QueueEffect::None));
        }
        let mut rows = tx
            .query(
                "SELECT created_at, revision FROM command_intents WHERE command_id = ?1 \
                 AND kind = ?2 AND status = 'pending' LIMIT 1",
                params![command_id.as_str(), PROMPT],
            )
            .await?;
        let Some(row) = rows.next().await.map_err(store_err)? else {
            return Ok((reorder_conflict(request), QueueEffect::None));
        };
        if get_opt_int(&row, 1)?.unwrap_or_default()
            != expected
                .get(command_id.as_str())
                .copied()
                .unwrap_or_default()
        {
            return Ok((reorder_revision_conflict(request), QueueEffect::None));
        }
        created_at.push(get_opt_int(&row, 0)?.unwrap_or_default());
    }
    let base = created_at.into_iter().min().unwrap_or_default();
    for (index, command_id) in request.command_ids.iter().enumerate() {
        let changed = tx
            .execute(
                "UPDATE command_intents SET created_at = ?1, revision = revision + 1 \
                 WHERE command_id = ?2 AND kind = ?3 \
                   AND status = 'pending' AND revision = ?4",
                params![
                    base + i64::try_from(index).map_err(store_msg)?,
                    command_id.as_str(),
                    PROMPT,
                    expected[command_id.as_str()]
                ],
            )
            .await?;
        if changed != 1 {
            return Ok((reorder_revision_conflict(request), QueueEffect::None));
        }
    }
    let seq = latest_queue_seq(tx).await?;
    Ok((
        success_outcome(request, runtime, CommandQueueState::Queued, seq, None),
        QueueEffect::None,
    ))
}

async fn command_matches_target(
    tx: &WriteTxn,
    command_id: &str,
    target: QueueTarget<'_>,
    runtime: &TargetRuntime,
    require_original_selector: bool,
) -> Result<bool, NexusError> {
    let mut rows = tx
        .query(
            "SELECT request_json FROM command_intents WHERE command_id = ?1 LIMIT 1",
            params![command_id],
        )
        .await?;
    let Some(row) = rows.next().await.map_err(store_err)? else {
        return Ok(false);
    };
    let command_request = serde_json::from_str::<Value>(&get_text(&row, 0)?).map_err(store_msg)?;
    if let Some(expected) = target.expected_session_id {
        let original = command_request.get("expectedSessionId");
        // Redirect changes execution intent; an unbound legacy row cannot
        // acquire a new selector by conversion. Existing selectors always match.
        if original.is_some_and(|original| original.as_str() != Some(expected))
            || (require_original_selector && original.is_none())
        {
            return Ok(false);
        }
    }
    let direct = match target.agent_id {
        Some(agent_id) => command_request.get("agentId").and_then(Value::as_str) == Some(agent_id),
        None => {
            command_request.get("agentId").is_none_or(Value::is_null)
                && command_request.get("name").and_then(Value::as_str) == target.name
        }
    };
    if direct {
        return Ok(true);
    }
    let Some(session_id) = runtime.session_id.as_deref() else {
        return Ok(false);
    };
    let mut rows = tx
        .query(
            "SELECT EXISTS(SELECT 1 FROM command_intent_events \
             WHERE command_id = ?1 AND session_id = ?2)",
            params![command_id, session_id],
        )
        .await?;
    Ok(rows
        .next()
        .await
        .map_err(store_err)?
        .map(|row| get_opt_int(&row, 0).map(|value| value.unwrap_or(0) == 1))
        .transpose()?
        .unwrap_or(false))
}

async fn command_revision(tx: &WriteTxn, command_id: &str) -> Result<Option<i64>, NexusError> {
    let mut rows = tx
        .query(
            "SELECT revision FROM command_intents WHERE command_id = ?1",
            params![command_id],
        )
        .await?;
    match rows.next().await.map_err(store_err)? {
        Some(row) => get_opt_int(&row, 0),
        None => Ok(None),
    }
}

async fn latest_queue_seq(tx: &WriteTxn) -> Result<i64, NexusError> {
    let mut rows = tx
        .query(
            "SELECT COALESCE(MAX(seq), 0) FROM command_intent_events",
            (),
        )
        .await?;
    match rows.next().await.map_err(store_err)? {
        Some(row) => Ok(get_opt_int(&row, 0)?.unwrap_or_default()),
        None => Ok(0),
    }
}

fn command_and_revision(request: &CommandQueueMutationRequest) -> Option<(&str, i64)> {
    Some((
        request.command_id.as_deref()?.trim(),
        request.expected_revision?,
    ))
    .filter(|(command_id, _)| !command_id.is_empty())
}

fn canonical_request(request: &CommandQueueMutationRequest) -> Result<Value, NexusError> {
    let mut expected = request.expected_revisions.clone();
    expected.sort_by(|left, right| left.command_id.cmp(&right.command_id));
    let mut canonical = json!({
        "name": request.name.as_deref().map(str::trim),
        "agentId": request.agent_id,
        "action": serde_json::to_value(request.action).map_err(store_msg)?,
        "commandId": request.command_id,
        "expectedRevision": request.expected_revision,
        "text": request.text,
        "commandIds": request.command_ids,
        "expectedRevisions": expected,
    });
    if let Some(expected) = &request.expected_session_id {
        canonical["expectedSessionId"] = json!(expected);
    }
    Ok(canonical)
}

fn success_outcome(
    request: &CommandQueueMutationRequest,
    runtime: &TargetRuntime,
    state: CommandQueueState,
    seq: i64,
    revision: Option<i64>,
) -> CommandQueueMutationOutcome {
    let mut body = Map::new();
    body.insert(
        "clientMutationId".into(),
        Value::String(request.client_mutation_id.clone()),
    );
    if let Some(command_id) = request.command_id.as_ref() {
        body.insert("commandId".into(), Value::String(command_id.clone()));
    }
    if let Some(session_id) = runtime.session_id.as_ref() {
        body.insert("sessionId".into(), Value::String(session_id.clone()));
    }
    body.insert(
        "state".into(),
        serde_json::to_value(state).expect("queue state serializes"),
    );
    body.insert(
        "steerCapability".into(),
        serde_json::to_value(runtime.steer_capability()).expect("steer capability serializes"),
    );
    if let Some(revision) = revision {
        body.insert("revision".into(), Value::Number(revision.into()));
    }
    body.insert("seq".into(), Value::Number(seq.into()));
    CommandQueueMutationOutcome {
        status: 200,
        body: Value::Object(body),
    }
}

fn error_outcome(
    request: &CommandQueueMutationRequest,
    error: &str,
    status: u16,
) -> CommandQueueMutationOutcome {
    let mut body = Map::new();
    if !request.client_mutation_id.is_empty() {
        body.insert(
            "clientMutationId".into(),
            Value::String(request.client_mutation_id.clone()),
        );
    }
    if let Some(command_id) = request.command_id.as_ref() {
        body.insert("commandId".into(), Value::String(command_id.clone()));
    }
    body.insert("error".into(), Value::String(error.into()));
    CommandQueueMutationOutcome {
        status,
        body: Value::Object(body),
    }
}

fn cas_conflict(request: &CommandQueueMutationRequest) -> CommandQueueMutationOutcome {
    error_outcome(
        request,
        "command revision changed or command is no longer queued",
        409,
    )
}

fn reorder_conflict(request: &CommandQueueMutationRequest) -> CommandQueueMutationOutcome {
    error_outcome(request, "one or more commands are no longer queued", 409)
}

fn reorder_revision_conflict(request: &CommandQueueMutationRequest) -> CommandQueueMutationOutcome {
    error_outcome(request, "one or more command revisions changed", 409)
}
