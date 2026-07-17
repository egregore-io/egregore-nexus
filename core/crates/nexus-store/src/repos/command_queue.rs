//! Daemon-owned atomic mutations for the durable session prompt queue.

use std::collections::{HashMap, HashSet};

use libsql::params;
use nexus_common::NexusError;
use nexus_contracts::{
    CommandQueueAction, CommandQueueMutationRequest, CommandQueueState, SessionId, SteerCapability,
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

/// Atomic pending-queue mutation service. The daemon is its only production caller.
pub struct CommandQueue<'a> {
    store: &'a Store,
}

#[derive(Debug, Clone)]
struct TargetRuntime {
    session_id: Option<String>,
    harness: Option<String>,
    transport: Option<String>,
    turn_active: bool,
}

impl TargetRuntime {
    fn steer_capability(&self) -> SteerCapability {
        if self.transport.as_deref() == Some("codex-appserver") {
            SteerCapability::NativeSteer
        } else if self.harness.is_some() {
            SteerCapability::InterruptAndSend
        } else {
            SteerCapability::None
        }
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

    /// Apply one idempotent compare-and-set mutation under the store's single write transaction.
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
        if request.name.trim().is_empty() {
            return Ok(error_outcome(request, "name is required", 400));
        }
        if request.client_mutation_id.trim().is_empty() {
            return Ok(error_outcome(request, "clientMutationId is required", 400));
        }

        let canonical = canonical_request(request)?;
        let canonical_json = serde_json::to_string(&canonical).map_err(store_msg)?;
        let runtime = target_runtime(self.store, project, request, active_sessions).await?;
        let tx = self
            .store
            .begin_identity_write_txn("command_queue_mutation")
            .await?;

        if let Some(previous) = replay(&tx, project, request, &canonical).await? {
            tx.commit().await?;
            return Ok(previous);
        }

        tx.execute(
            "INSERT INTO command_queue_mutations \
             (project, client_mutation_id, request_json, response_status, response_json, created_at) \
             VALUES (?1, ?2, ?3, NULL, NULL, ?4)",
            params![project, request.client_mutation_id.as_str(), canonical_json, now],
        )
        .await?;

        let (outcome, effect) = apply_mutation(&tx, project, request, now, &runtime).await?;
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
    project: &str,
    request: &CommandQueueMutationRequest,
    canonical: &Value,
) -> Result<Option<CommandQueueMutationOutcome>, NexusError> {
    let mut rows = tx
        .query(
            "SELECT request_json, response_status, response_json FROM command_queue_mutations \
             WHERE project = ?1 AND client_mutation_id = ?2 LIMIT 1",
            params![project, request.client_mutation_id.as_str()],
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
    project: &str,
    request: &CommandQueueMutationRequest,
    active_sessions: &[SessionId],
) -> Result<TargetRuntime, NexusError> {
    let agent_id = request.agent_id.as_ref().map(|id| id.0.as_str());
    let sessions = Sessions::new(store);
    let session = match agent_id {
        Some(agent_id) => sessions.find_by_agent_id(agent_id).await?,
        None => sessions.find_by_name(project, request.name.trim()).await?,
    };
    let Some(session) = session else {
        return Ok(TargetRuntime {
            session_id: None,
            harness: None,
            transport: None,
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
        harness: session.agent,
        transport: session.transport,
        turn_active,
    })
}

async fn apply_mutation(
    tx: &WriteTxn,
    project: &str,
    request: &CommandQueueMutationRequest,
    now: i64,
    runtime: &TargetRuntime,
) -> Result<(CommandQueueMutationOutcome, QueueEffect), NexusError> {
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
            if !command_matches_target(tx, project, command_id, request, runtime).await? {
                return Ok((cas_conflict(request), QueueEffect::None));
            }
            let changed = tx
                .execute(
                    "UPDATE command_intents SET kind = ?1, revision = revision + 1 \
                     WHERE command_id = ?2 AND project = ?3 AND kind = ?4 \
                       AND status = 'pending' AND revision = ?5",
                    params![STEER, command_id, project, PROMPT, expected_revision],
                )
                .await?;
            if changed != 1 {
                return Ok((cas_conflict(request), QueueEffect::None));
            }
            let revision = command_revision(tx, project, command_id).await?;
            let seq = latest_queue_seq(tx, project).await?;
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
            if !command_matches_target(tx, project, command_id, request, runtime).await? {
                return Ok((cas_conflict(request), QueueEffect::None));
            }
            let changed = tx
                .execute(
                    "UPDATE command_intents SET status = 'cancelled', revision = revision + 1, \
                     completed_at = ?1, lease_until = NULL WHERE command_id = ?2 AND project = ?3 \
                       AND kind = ?4 AND status = 'pending' AND revision = ?5",
                    params![now, command_id, project, PROMPT, expected_revision],
                )
                .await?;
            if changed != 1 {
                return Ok((cas_conflict(request), QueueEffect::None));
            }
            let revision = command_revision(tx, project, command_id).await?;
            let seq = latest_queue_seq(tx, project).await?;
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
            if !command_matches_target(tx, project, command_id, request, runtime).await? {
                return Ok((cas_conflict(request), QueueEffect::None));
            }
            let changed = tx
                .execute(
                    "UPDATE command_intents SET request_json = json_set(request_json, '$.text', ?1), \
                     revision = revision + 1 WHERE command_id = ?2 AND project = ?3 AND kind = ?4 \
                       AND status = 'pending' AND revision = ?5",
                    params![text, command_id, project, PROMPT, expected_revision],
                )
                .await?;
            if changed != 1 {
                return Ok((cas_conflict(request), QueueEffect::None));
            }
            let revision = command_revision(tx, project, command_id).await?;
            let seq = latest_queue_seq(tx, project).await?;
            Ok((
                success_outcome(request, runtime, CommandQueueState::Queued, seq, revision),
                QueueEffect::None,
            ))
        }
        CommandQueueAction::Reorder => reorder(tx, project, request, runtime).await,
    }
}

async fn reorder(
    tx: &WriteTxn,
    project: &str,
    request: &CommandQueueMutationRequest,
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
        if !command_matches_target(tx, project, command_id, request, runtime).await? {
            return Ok((reorder_conflict(request), QueueEffect::None));
        }
        let mut rows = tx
            .query(
                "SELECT created_at, revision FROM command_intents WHERE command_id = ?1 \
                 AND project = ?2 AND kind = ?3 AND status = 'pending' LIMIT 1",
                params![command_id.as_str(), project, PROMPT],
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
                 WHERE command_id = ?2 AND project = ?3 AND kind = ?4 \
                   AND status = 'pending' AND revision = ?5",
                params![
                    base + i64::try_from(index).map_err(store_msg)?,
                    command_id.as_str(),
                    project,
                    PROMPT,
                    expected[command_id.as_str()]
                ],
            )
            .await?;
        if changed != 1 {
            return Ok((reorder_revision_conflict(request), QueueEffect::None));
        }
    }
    let seq = latest_queue_seq(tx, project).await?;
    Ok((
        success_outcome(request, runtime, CommandQueueState::Queued, seq, None),
        QueueEffect::None,
    ))
}

async fn command_matches_target(
    tx: &WriteTxn,
    project: &str,
    command_id: &str,
    request: &CommandQueueMutationRequest,
    runtime: &TargetRuntime,
) -> Result<bool, NexusError> {
    let mut rows = tx
        .query(
            "SELECT request_json FROM command_intents WHERE command_id = ?1 AND project = ?2 LIMIT 1",
            params![command_id, project],
        )
        .await?;
    let Some(row) = rows.next().await.map_err(store_err)? else {
        return Ok(false);
    };
    let command_request = serde_json::from_str::<Value>(&get_text(&row, 0)?).map_err(store_msg)?;
    let direct = match request.agent_id.as_ref() {
        Some(agent_id) => {
            command_request.get("agentId").and_then(Value::as_str) == Some(agent_id.0.as_str())
        }
        None => {
            command_request.get("agentId").is_none_or(Value::is_null)
                && command_request.get("name").and_then(Value::as_str) == Some(request.name.trim())
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

async fn command_revision(
    tx: &WriteTxn,
    project: &str,
    command_id: &str,
) -> Result<Option<i64>, NexusError> {
    let mut rows = tx
        .query(
            "SELECT revision FROM command_intents WHERE project = ?1 AND command_id = ?2",
            params![project, command_id],
        )
        .await?;
    match rows.next().await.map_err(store_err)? {
        Some(row) => get_opt_int(&row, 0),
        None => Ok(None),
    }
}

async fn latest_queue_seq(tx: &WriteTxn, project: &str) -> Result<i64, NexusError> {
    let mut rows = tx
        .query(
            "SELECT COALESCE(MAX(seq), 0) FROM command_intent_events WHERE project = ?1",
            params![project],
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
    Ok(json!({
        "name": request.name.trim(),
        "agentId": request.agent_id,
        "action": serde_json::to_value(request.action).map_err(store_msg)?,
        "commandId": request.command_id,
        "expectedRevision": request.expected_revision,
        "text": request.text,
        "commandIds": request.command_ids,
        "expectedRevisions": expected,
    }))
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
