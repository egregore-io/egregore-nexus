//! Store-backed command ingress.
//!
//! Producers write `command_intents` rows; the daemon claims rows from this table and executes the
//! real domain operation through existing services. This is an ingress queue, not a history table:
//! durable Message Post history remains in `messages`, and delivery remains in `in_flight`.

use libsql::params;

use nexus_common::NexusError;
use nexus_contracts::entity_kind;

use crate::error::{store_err, store_msg};
use crate::repos::sessions::{get_opt_int, get_opt_text, get_text};
use crate::repos::{AgentRef, Agents, Sessions};
use crate::state::Store;
use crate::types::SessionRow;

/// One queued daemon command row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandIntentRow {
    pub command_id: String,
    pub kind: String,
    pub status: String,
    pub project: String,
    pub caller_name: String,
    pub caller_session_id: Option<String>,
    pub caller_agent_id: Option<String>,
    pub caller_runtime_id: Option<String>,
    pub caller_client_key: Option<String>,
    pub caller_principal_id: Option<String>,
    /// Boot epoch in which the daemon resolved this caller against a live registered session.
    /// `None` is intentionally fail-closed across restart; direct repository producers cannot
    /// manufacture the authority granted by daemon IPC acceptance.
    pub caller_validated_boot_epoch: Option<String>,
    pub caller_kind: Option<String>,
    pub caller_tier: Option<String>,
    pub idempotency_key: Option<String>,
    pub request_json: String,
    pub result_json: Option<String>,
    pub error_json: Option<String>,
    pub attempts: i64,
    pub revision: i64,
    pub created_at: i64,
    pub claimed_at: Option<i64>,
    /// When the daemon began executing the current claim. Kept separate from `claimed_at` so
    /// clients can render the durable `claimed -> started` transition without inventing state.
    pub started_at: Option<i64>,
    pub lease_until: Option<i64>,
    pub completed_at: Option<i64>,
}

/// Pending/claimed depth for one command kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandIntentDepth {
    pub kind: String,
    pub pending: i64,
    pub claimed: i64,
}

/// Durable acceptance receipt for queue-shaped producer surfaces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandIntentReceipt {
    pub command_id: String,
    pub status: String,
    pub created_at: i64,
    pub revision: i64,
    pub session_id: Option<String>,
    pub seq: i64,
}

/// Fields needed to enqueue a pending command.
#[derive(Debug, Clone)]
pub struct NewCommandIntent {
    pub command_id: String,
    pub kind: String,
    pub project: String,
    pub caller_name: String,
    pub caller_session_id: Option<String>,
    pub caller_agent_id: Option<String>,
    pub caller_runtime_id: Option<String>,
    pub caller_client_key: Option<String>,
    pub caller_principal_id: Option<String>,
    pub caller_kind: Option<String>,
    pub caller_tier: Option<String>,
    pub idempotency_key: Option<String>,
    pub request_json: String,
    pub created_at: i64,
}

/// Persistence for the `command_intents` ingress table.
pub struct CommandIntents<'a> {
    store: &'a Store,
}

/// Settlement of a prompt or steer admission attempt. `Rejected` requires evidence that no native
/// delivery occurred; transport loss and response/receipt timeouts are always `Uncertain`.
pub enum PromptCommandOutcome<'a> {
    Completed(&'a str),
    Rejected(&'a str),
    Uncertain,
}

impl<'a> CommandIntents<'a> {
    /// Arm only the exact, unexpired claim before crossing native admission.
    pub async fn mark_prompt_started_for_claim(
        &self,
        claim: &CommandIntentRow,
        now: i64,
    ) -> Result<bool, NexusError> {
        let updated = self
            .store
            .identity_conn()
            .execute(
                "UPDATE command_intents SET started_at = ?4, revision = revision + 1 \
             WHERE command_id = ?1 AND claimed_at = ?2 AND attempts = ?3 \
               AND status = 'claimed' AND started_at IS NULL AND lease_until > ?4 \
               AND kind IN ('harness.prompt', 'harness.steer') AND lease_until = ?5",
                params![
                    claim.command_id.as_str(),
                    claim.claimed_at,
                    claim.attempts,
                    now,
                    claim.lease_until
                ],
            )
            .await
            .map_err(store_err)?;
        Ok(updated == 1)
    }

    /// Release the current prompt attempt only after typed native nonacceptance. This must never
    /// be called on a timeout, lost response, or cancelled future. Queue order remains unchanged.
    pub async fn defer_prompt_claim_known_not_accepted(
        &self,
        claim: &CommandIntentRow,
        now: i64,
    ) -> Result<bool, NexusError> {
        let updated = self.store.identity_conn().execute(
            "UPDATE command_intents SET status = 'pending', claimed_at = NULL, started_at = NULL, \
             lease_until = NULL, revision = revision + 1 \
             WHERE command_id = ?1 AND claimed_at = ?2 AND attempts = ?3 \
               AND status = 'claimed' AND started_at IS NOT NULL \
               AND kind IN ('harness.prompt', 'harness.steer') AND lease_until = ?4 AND lease_until > ?5",
            params![claim.command_id.as_str(), claim.claimed_at, claim.attempts, claim.lease_until, now],
        ).await.map_err(store_err)?;
        // Deliberately do not ring ingress here. Native boundary readiness, not this release,
        // must make a deferred row eligible again; otherwise unsupported busy input spins.
        Ok(updated == 1)
    }

    /// Settle one attempt without permitting a late callback to overwrite a newer/terminal row.
    pub async fn settle_prompt_claim(
        &self,
        claim: &CommandIntentRow,
        outcome: PromptCommandOutcome<'_>,
        now: i64,
    ) -> Result<bool, NexusError> {
        let uncertain = prompt_uncertain_error_json();
        let (status, result, error) = match outcome {
            PromptCommandOutcome::Completed(result) => ("done", Some(result), None),
            PromptCommandOutcome::Rejected(error) => ("error", None, Some(error)),
            PromptCommandOutcome::Uncertain => ("error", None, Some(uncertain.as_str())),
        };
        let updated = self
            .store
            .identity_conn()
            .execute(
                "UPDATE command_intents SET status = ?4, result_json = ?5, error_json = ?6, \
             completed_at = ?7, lease_until = NULL, revision = revision + 1 \
             WHERE command_id = ?1 AND claimed_at = ?2 AND attempts = ?3 \
               AND status = 'claimed' AND started_at IS NOT NULL \
               AND kind IN ('harness.prompt', 'harness.steer') AND lease_until = ?8 AND lease_until > ?7",
                params![
                    claim.command_id.as_str(),
                    claim.claimed_at,
                    claim.attempts,
                    status,
                    result,
                    error,
                    now,
                    claim.lease_until
                ],
            )
            .await
            .map_err(store_err)?;
        if updated > 0 {
            self.store.events().command_intent_completed().signal();
        }
        Ok(updated == 1)
    }

    /// Expired armed prompt deliveries are unknown, not reclaimable. Their detached native
    /// work may still finish; this terminal explicitly does not assert that delivery stopped.
    pub async fn expire_started_prompt_claims(&self, now: i64) -> Result<u64, NexusError> {
        let updated = self
            .store
            .identity_conn()
            .execute(
                &format!(
                    "UPDATE command_intents SET status = 'error', error_json = ?2, \
             completed_at = ?1, lease_until = NULL, revision = revision + 1 \
             WHERE status = 'claimed' AND lease_until <= ?1 AND NOT ({PROMPT_RECLAIM_SAFE})"
                ),
                params![now, prompt_uncertain_error_json()],
            )
            .await
            .map_err(store_err)?;
        if updated > 0 {
            self.store.events().command_intent_completed().signal();
        }
        Ok(updated)
    }
    /// Bind a repo to the shared store connection.
    pub fn new(store: &'a Store) -> Self {
        CommandIntents { store }
    }

    /// Insert a new pending command intent. The command id is caller-provided so submitters can
    /// poll the same row for completion.
    pub async fn insert_pending(&self, row: NewCommandIntent) -> Result<(), NexusError> {
        self.insert_pending_with_caller_validation(row, None).await
    }

    /// Insert a new pending command whose caller was resolved by the daemon in `boot_epoch`.
    /// The marker is written in the same SQLite statement as the command acceptance boundary.
    pub async fn insert_pending_with_validated_caller(
        &self,
        row: NewCommandIntent,
        boot_epoch: &str,
    ) -> Result<(), NexusError> {
        self.insert_pending_with_caller_validation(row, Some(boot_epoch))
            .await
    }

    async fn insert_pending_with_caller_validation(
        &self,
        mut row: NewCommandIntent,
        caller_validated_boot_epoch: Option<&str>,
    ) -> Result<(), NexusError> {
        normalize_caller_kind(&mut row)?;
        self
            .store
            .identity_conn()
            .execute(
                 "INSERT INTO command_intents (command_id, kind, status, project, caller_name, \
                 caller_session_id, caller_agent_id, caller_runtime_id, caller_client_key, \
                 caller_principal_id, caller_validated_boot_epoch, caller_kind, caller_tier, \
                 idempotency_key, request_json, result_json, \
                 error_json, attempts, created_at, claimed_at, started_at, lease_until, completed_at) VALUES \
                 (?1, ?2, 'pending', ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, \
                 NULL, NULL, 0, ?15, NULL, NULL, NULL, NULL)",
                params![
                    row.command_id,
                    row.kind,
                    row.project,
                    row.caller_name,
                    row.caller_session_id,
                    row.caller_agent_id,
                    row.caller_runtime_id,
                    row.caller_client_key,
                    row.caller_principal_id,
                    caller_validated_boot_epoch,
                    row.caller_kind,
                    row.caller_tier,
                    row.idempotency_key,
                    row.request_json,
                    row.created_at
                ],
            )
            .await
            .map_err(store_err)?;
        self.store.events().command_intent_inserted().signal();
        Ok(())
    }

    /// Insert a command, or resume the existing durable row when a transport reconnect repeats
    /// the exact same caller-provided command id. Reusing an id for a different operation is an
    /// invalid request rather than an implicit dedupe: otherwise a reconnect could observe the
    /// result of a command it did not submit.
    pub async fn insert_pending_or_resume(
        &self,
        row: NewCommandIntent,
    ) -> Result<String, NexusError> {
        self.insert_pending_or_resume_with_caller_validation(row, None)
            .await
    }

    /// Insert or resume a command accepted after daemon-side caller validation.
    pub async fn insert_pending_or_resume_with_validated_caller(
        &self,
        row: NewCommandIntent,
        boot_epoch: &str,
    ) -> Result<String, NexusError> {
        self.insert_pending_or_resume_with_caller_validation(row, Some(boot_epoch))
            .await
    }

    async fn insert_pending_or_resume_with_caller_validation(
        &self,
        mut row: NewCommandIntent,
        caller_validated_boot_epoch: Option<&str>,
    ) -> Result<String, NexusError> {
        normalize_caller_kind(&mut row)?;
        let command_id = row.command_id.clone();
        if let Some(existing) = self.get(&command_id).await? {
            return matching_existing_command_with_validation(
                &existing,
                &row,
                "command id",
                caller_validated_boot_epoch.is_some(),
            );
        }
        if row.idempotency_key.is_some() {
            return self
                .insert_pending_idempotent_with_caller_validation(row, caller_validated_boot_epoch)
                .await;
        }

        match self
            .insert_pending_with_caller_validation(row.clone(), caller_validated_boot_epoch)
            .await
        {
            Ok(()) => Ok(command_id),
            Err(err) if is_unique_constraint(&err) => match self.get(&command_id).await? {
                Some(existing) => matching_existing_command_with_validation(
                    &existing,
                    &row,
                    "command id",
                    caller_validated_boot_epoch.is_some(),
                ),
                None => Err(err),
            },
            Err(err) => Err(err),
        }
    }

    /// Insert one session prompt under a caller-held ingress serialization boundary.
    /// Existing command/idempotency retries remain resumable even when the target is full;
    /// only a genuinely new pending row consumes capacity.
    pub async fn insert_pending_or_resume_bounded_session_prompt(
        &self,
        row: NewCommandIntent,
        max_pending: i64,
    ) -> Result<String, NexusError> {
        self.insert_pending_or_resume_bounded_session_prompt_with_caller_validation(
            row,
            max_pending,
            None,
        )
        .await
    }

    /// Bounded prompt insertion after daemon-side caller validation.
    pub async fn insert_pending_or_resume_bounded_session_prompt_with_validated_caller(
        &self,
        row: NewCommandIntent,
        max_pending: i64,
        boot_epoch: &str,
    ) -> Result<String, NexusError> {
        self.insert_pending_or_resume_bounded_session_prompt_with_caller_validation(
            row,
            max_pending,
            Some(boot_epoch),
        )
        .await
    }

    async fn insert_pending_or_resume_bounded_session_prompt_with_caller_validation(
        &self,
        mut row: NewCommandIntent,
        max_pending: i64,
        caller_validated_boot_epoch: Option<&str>,
    ) -> Result<String, NexusError> {
        normalize_caller_kind(&mut row)?;
        if max_pending <= 0 {
            return Err(NexusError::Invalid(
                "session command queue capacity must be positive".into(),
            ));
        }
        if row.kind != crate::command_kinds::harness::PROMPT {
            return self
                .insert_pending_or_resume_with_caller_validation(row, caller_validated_boot_epoch)
                .await;
        }
        if let Some(existing) = self.get(&row.command_id).await? {
            return matching_existing_command_with_validation(
                &existing,
                &row,
                "command id",
                caller_validated_boot_epoch.is_some(),
            );
        }
        if let Some(idempotency_key) = row.idempotency_key.as_deref() {
            if let Some(existing_id) = self
                .find_idempotent_command(
                    &row.project,
                    &row.kind,
                    &row.caller_name,
                    row.caller_session_id.as_deref(),
                    row.caller_client_key.as_deref(),
                    idempotency_key,
                )
                .await?
            {
                let existing = self.get(&existing_id).await?.ok_or_else(|| {
                    store_msg(format!("idempotent command disappeared: {existing_id}"))
                })?;
                return matching_existing_command_with_validation(
                    &existing,
                    &row,
                    "idempotency key",
                    caller_validated_boot_epoch.is_some(),
                );
            }
        }
        if self.pending_session_prompt_count(&row).await? >= max_pending {
            return Err(NexusError::CommandQueueFull);
        }
        self.insert_pending_or_resume_with_caller_validation(row, caller_validated_boot_epoch)
            .await
    }

    async fn pending_session_prompt_count(
        &self,
        row: &NewCommandIntent,
    ) -> Result<i64, NexusError> {
        let request: serde_json::Value =
            serde_json::from_str(&row.request_json).map_err(store_msg)?;
        let agent_id = request.get("agentId").and_then(serde_json::Value::as_str);
        let name = request.get("name").and_then(serde_json::Value::as_str);
        if agent_id.is_none() && name.is_none() {
            return Err(NexusError::Invalid(
                "session prompt target requires name or agentId".into(),
            ));
        }
        let mut rows = self
            .store
            .identity_conn()
            .query(
                "SELECT COUNT(*) FROM command_intents WHERE kind = ?1 AND status = 'pending' \
                 AND ((?2 IS NOT NULL AND json_extract(request_json, '$.agentId') = ?2) \
                 OR (?2 IS NULL AND \
                 json_extract(request_json, '$.agentId') IS NULL AND \
                 json_extract(request_json, '$.name') = ?3))",
                params![crate::command_kinds::harness::PROMPT, agent_id, name],
            )
            .await
            .map_err(store_err)?;
        match rows.next().await.map_err(store_err)? {
            Some(count) => Ok(get_opt_int(&count, 0)?.unwrap_or_default()),
            None => Ok(0),
        }
    }

    /// Insert a command row, or return the existing command id for the same caller-provided
    /// idempotency key. This is used by retry-prone producer surfaces such as MCP tool calls: a
    /// retried call polls the first row instead of executing a duplicate write.
    pub async fn insert_pending_idempotent(
        &self,
        row: NewCommandIntent,
    ) -> Result<String, NexusError> {
        self.insert_pending_idempotent_with_caller_validation(row, None)
            .await
    }

    async fn insert_pending_idempotent_with_caller_validation(
        &self,
        mut row: NewCommandIntent,
        caller_validated_boot_epoch: Option<&str>,
    ) -> Result<String, NexusError> {
        normalize_caller_kind(&mut row)?;
        let command_id = row.command_id.clone();
        let project = row.project.clone();
        let kind = row.kind.clone();
        let caller_name = row.caller_name.clone();
        let caller_session_id = row.caller_session_id.clone();
        let caller_client_key = row.caller_client_key.clone();
        let idempotency_key = row.idempotency_key.clone();
        if let Some(idempotency_key) = idempotency_key.as_deref() {
            if let Some(existing_id) = self
                .find_idempotent_command(
                    &project,
                    &kind,
                    &caller_name,
                    caller_session_id.as_deref(),
                    caller_client_key.as_deref(),
                    idempotency_key,
                )
                .await?
            {
                let existing = self.get(&existing_id).await?.ok_or_else(|| {
                    store_msg(format!("idempotent command disappeared: {existing_id}"))
                })?;
                return matching_existing_command_with_validation(
                    &existing,
                    &row,
                    "idempotency key",
                    caller_validated_boot_epoch.is_some(),
                );
            }
        }
        match self
            .insert_pending_with_caller_validation(row.clone(), caller_validated_boot_epoch)
            .await
        {
            Ok(()) => return Ok(command_id),
            Err(err) if idempotency_key.is_some() && is_unique_constraint(&err) => {}
            Err(err) => return Err(err),
        }

        let existing_id = self
            .find_idempotent_command(
                &project,
                &kind,
                &caller_name,
                caller_session_id.as_deref(),
                caller_client_key.as_deref(),
                idempotency_key.as_deref().expect("checked above"),
            )
            .await?
            .unwrap_or(command_id);
        let existing = self
            .get(&existing_id)
            .await?
            .ok_or_else(|| store_msg(format!("idempotent command disappeared: {existing_id}")))?;
        matching_existing_command_with_validation(
            &existing,
            &row,
            "idempotency key",
            caller_validated_boot_epoch.is_some(),
        )
    }

    async fn find_idempotent_command(
        &self,
        project: &str,
        kind: &str,
        caller_name: &str,
        caller_session_id: Option<&str>,
        caller_client_key: Option<&str>,
        idempotency_key: &str,
    ) -> Result<Option<String>, NexusError> {
        let session_input = kind == crate::command_kinds::harness::PROMPT
            || kind == crate::command_kinds::harness::STEER;
        let query = if session_input {
            self.store
                .identity_conn()
                .query(
                    "SELECT command_id FROM command_intents WHERE project = ?1 \
                     AND kind IN (?2, ?3) \
                     AND COALESCE(caller_client_key, caller_session_id, caller_name) = \
                       COALESCE(?4, ?5, ?6) AND idempotency_key = ?7 LIMIT 1",
                    params![
                        project,
                        crate::command_kinds::harness::PROMPT,
                        crate::command_kinds::harness::STEER,
                        caller_client_key,
                        caller_session_id,
                        caller_name,
                        idempotency_key
                    ],
                )
                .await
        } else {
            self.store
                .identity_conn()
                .query(
                    "SELECT command_id FROM command_intents WHERE project = ?1 AND kind = ?2 \
                     AND COALESCE(caller_client_key, caller_session_id, caller_name) = \
                       COALESCE(?3, ?4, ?5) AND idempotency_key = ?6 LIMIT 1",
                    params![
                        project,
                        kind,
                        caller_client_key,
                        caller_session_id,
                        caller_name,
                        idempotency_key
                    ],
                )
                .await
        };
        let mut rows = query.map_err(store_err)?;
        match rows.next().await.map_err(store_err)? {
            Some(row) => Ok(Some(get_text(&row, 0)?)),
            None => Ok(None),
        }
    }

    /// Claim the oldest pending or expired command row. Returns `None` when no work is available.
    ///
    /// The single `UPDATE ... RETURNING` statement makes accidental duplicate workers converge:
    /// only the worker that still sees the row as pending/expired transitions it to `claimed`.
    pub async fn claim_next(
        &self,
        now: i64,
        lease_ms: i64,
    ) -> Result<Option<CommandIntentRow>, NexusError> {
        self.claim_next_matching(now, lease_ms, ClaimFilter::Any)
            .await
    }

    /// Claim the oldest pending or expired command row with the requested kind.
    pub async fn claim_next_kind(
        &self,
        now: i64,
        lease_ms: i64,
        kind: &str,
    ) -> Result<Option<CommandIntentRow>, NexusError> {
        self.claim_next_matching(now, lease_ms, ClaimFilter::Kind(kind))
            .await
    }

    /// Claim the oldest ready `harness.prompt` row whose target session is not already busy.
    ///
    /// Prompt commands are the only command-intent kind that may run in parallel while still
    /// requiring per-session ordering. This query therefore skips:
    ///
    /// - target sessions that already have a non-expired claimed `harness.prompt` row, and
    /// - target sessions that the harness turn tracker reports as active after command-row
    ///   completion.
    ///
    /// The target name lives in `request_json.name`, so the query resolves it through `sessions`
    /// at claim time. Missing targets are deliberately not skipped; dispatch owns the resulting
    /// validation error.
    pub async fn claim_next_ready_harness_prompt(
        &self,
        now: i64,
        lease_ms: i64,
        active_sessions: &[String],
    ) -> Result<Option<CommandIntentRow>, NexusError> {
        self.claim_next_ready_harness_prompt_with_native_queues(now, lease_ms, active_sessions, &[])
            .await
    }

    /// Native operator queues may accept input during an older bus turn. The daemon supplies
    /// exact currently opted-in runtime IDs; startup and command-claim exclusions still apply.
    pub async fn claim_next_ready_harness_prompt_with_native_queues(
        &self,
        now: i64,
        lease_ms: i64,
        active_sessions: &[String],
        native_queue_sessions: &[String],
    ) -> Result<Option<CommandIntentRow>, NexusError> {
        for candidate in self
            .claim_candidates(now, crate::command_kinds::harness::PROMPT)
            .await?
        {
            let target = self.resolve_target_session(&candidate).await?;
            if target
                .as_ref()
                .is_some_and(|row| active_sessions.contains(&row.session_id.0))
                || self
                    .has_pending_initial_prompt(&candidate, target.as_ref())
                    .await?
                || (!target
                    .as_ref()
                    .is_some_and(|row| native_queue_sessions.contains(&row.session_id.0))
                    && self
                        .has_older_unsettled_delivery(&candidate, target.as_ref())
                        .await?)
                || self
                    .has_claimed_prompt_for_target(now, &candidate, target.as_ref())
                    .await?
            {
                continue;
            }
            if let Some(claimed) = self
                .claim_command_if_available(&candidate.command_id, now, lease_ms)
                .await?
            {
                return Ok(Some(claimed));
            }
        }
        Ok(None)
    }

    /// Claim the oldest ready `harness.prompt` row for one resolved target session.
    ///
    /// The daemon's per-session prompt actors use this after they own a session lane. The initial
    /// global claim finds work for any idle session; subsequent claims stay keyed to the same
    /// session so one actor cannot steal another session's queue.
    pub async fn claim_next_ready_harness_prompt_for_session(
        &self,
        now: i64,
        lease_ms: i64,
        target_session_id: &str,
    ) -> Result<Option<CommandIntentRow>, NexusError> {
        self.claim_next_ready_harness_prompt_for_session_with_native_queue(
            now,
            lease_ms,
            target_session_id,
            false,
        )
        .await
    }

    /// Session-actor counterpart of the exact native-queue scheduling exception above.
    pub async fn claim_next_ready_harness_prompt_for_session_with_native_queue(
        &self,
        now: i64,
        lease_ms: i64,
        target_session_id: &str,
        native_queue: bool,
    ) -> Result<Option<CommandIntentRow>, NexusError> {
        for candidate in self
            .claim_candidates(now, crate::command_kinds::harness::PROMPT)
            .await?
        {
            let target = self.resolve_target_session(&candidate).await?;
            if target.as_ref().map(|row| row.session_id.0.as_str()) != Some(target_session_id)
                || self
                    .has_pending_initial_prompt(&candidate, target.as_ref())
                    .await?
                || (!native_queue
                    && self
                        .has_older_unsettled_delivery(&candidate, target.as_ref())
                        .await?)
                || self
                    .has_claimed_prompt_for_target(now, &candidate, target.as_ref())
                    .await?
            {
                continue;
            }
            if let Some(claimed) = self
                .claim_command_if_available(&candidate.command_id, now, lease_ms)
                .await?
            {
                return Ok(Some(claimed));
            }
        }
        Ok(None)
    }

    /// Claim the oldest ready harness command row whose target session is not boot-pending.
    ///
    /// Used by `harness.warm` and `harness.compact`; they must not run against a session whose
    /// launch initial prompt has not yet been accepted.
    pub async fn claim_next_ready_harness_command(
        &self,
        now: i64,
        lease_ms: i64,
        kind: &str,
    ) -> Result<Option<CommandIntentRow>, NexusError> {
        for candidate in self.claim_candidates(now, kind).await? {
            let target = self.resolve_target_session(&candidate).await?;
            if self
                .has_pending_initial_prompt(&candidate, target.as_ref())
                .await?
            {
                continue;
            }
            if let Some(claimed) = self
                .claim_command_if_available(&candidate.command_id, now, lease_ms)
                .await?
            {
                return Ok(Some(claimed));
            }
        }
        Ok(None)
    }

    async fn claim_candidates(
        &self,
        now: i64,
        kind: &str,
    ) -> Result<Vec<CommandIntentRow>, NexusError> {
        let mut rows = self
            .store
            .identity_conn()
            .query(
                &format!(
                    "SELECT {COLUMNS} FROM command_intents WHERE kind = ?1 AND \
                     (status = 'pending' OR (status = 'claimed' AND lease_until <= ?2)) \
                     AND {PROMPT_RECLAIM_SAFE} \
                     ORDER BY created_at ASC"
                ),
                params![kind, now],
            )
            .await
            .map_err(store_err)?;
        let mut candidates = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            candidates.push(row_to_command_intent(&row)?);
        }
        Ok(candidates)
    }

    async fn claim_command_if_available(
        &self,
        command_id: &str,
        now: i64,
        lease_ms: i64,
    ) -> Result<Option<CommandIntentRow>, NexusError> {
        // The daemon owns the transport gate while claiming. On split stores,
        // also exclude identity batches until this write cursor is finalized.
        // This introduces no explicit transaction or rollback owner.
        let _identity_guard = self.store.lock_split_identity_write().await;
        let mut rows = self
            .store
            .identity_conn()
            .query(
                &format!(
                    "UPDATE command_intents SET status = 'claimed', attempts = attempts + 1, \
                     revision = revision + 1, claimed_at = ?1, started_at = NULL, lease_until = ?2 \
                     WHERE command_id = ?3 AND \
                     (status = 'pending' OR (status = 'claimed' AND lease_until <= ?1)) \
                     AND {PROMPT_RECLAIM_SAFE} \
                     RETURNING {COLUMNS}"
                ),
                params![now, now + lease_ms, command_id],
            )
            .await
            .map_err(store_err)?;
        let claimed = match rows.next().await.map_err(store_err)? {
            Some(row) => Ok(Some(row_to_command_intent(&row)?)),
            None => Ok(None),
        };
        drop(rows);
        claimed
    }

    async fn resolve_target_session(
        &self,
        command: &CommandIntentRow,
    ) -> Result<Option<SessionRow>, NexusError> {
        let request: serde_json::Value =
            serde_json::from_str(&command.request_json).map_err(store_msg)?;
        let sessions = Sessions::new(self.store);
        if let Some(agent_id) = request.get("agentId").and_then(serde_json::Value::as_str) {
            if let Some(session) = sessions.active_runtime_session_for_agent(agent_id).await? {
                return Ok(Some(session));
            }
            return sessions.find_by_agent_id(agent_id).await;
        }
        let Some(name) = request.get("name").and_then(serde_json::Value::as_str) else {
            return Ok(None);
        };
        let agents = Agents::new(self.store);
        let parsed = AgentRef::parse(name);
        let resolved = match agents.resolve_ref("", &parsed, true).await {
            Err(NexusError::NotFound(_)) if matches!(parsed, AgentRef::Id(_)) => {
                agents
                    .resolve_ref("", &AgentRef::Name(name.to_string()), true)
                    .await
            }
            result => result,
        };
        match resolved {
            Ok(agent) => match sessions
                .active_runtime_session_for_agent(&agent.agent_id)
                .await?
            {
                Some(session) => Ok(Some(session)),
                None => sessions.find_by_agent_id(&agent.agent_id).await,
            },
            Err(NexusError::NotFound(_)) => sessions.find_unique_by_name_any_project(name).await,
            Err(error) => Err(error),
        }
    }

    async fn has_pending_initial_prompt(
        &self,
        command: &CommandIntentRow,
        target: Option<&SessionRow>,
    ) -> Result<bool, NexusError> {
        let request: serde_json::Value =
            serde_json::from_str(&command.request_json).map_err(store_msg)?;
        let agent_id = request
            .get("agentId")
            .and_then(serde_json::Value::as_str)
            .or_else(|| target.and_then(|row| row.agent_id.as_deref()));
        let session_id = target.map(|row| row.session_id.0.as_str());
        let mut rows = self
            .store
            .identity_conn()
            .query(
                "SELECT 1 FROM initial_prompt_deliveries WHERE status = 'pending' AND \
                 ((?1 IS NOT NULL AND agent_id = ?1) OR (?2 IS NOT NULL AND session_id = ?2)) \
                 LIMIT 1",
                params![agent_id, session_id],
            )
            .await
            .map_err(store_err)?;
        Ok(rows.next().await.map_err(store_err)?.is_some())
    }

    /// Preserve accepted ordering across daemon restart.
    ///
    /// The split daemon restores durable delivery obligations into its boot-scoped inbox after
    /// command ingress becomes available. A prompt accepted after one of those messages must stay
    /// queued until the older recipient obligation settles; otherwise the prompt worker can start
    /// a native turn first and the restored bus delivery is appended to that same native turn.
    async fn has_older_unsettled_delivery(
        &self,
        command: &CommandIntentRow,
        target: Option<&SessionRow>,
    ) -> Result<bool, NexusError> {
        if !self.store.has_split_authority() {
            return Ok(false);
        }
        let request: serde_json::Value =
            serde_json::from_str(&command.request_json).map_err(store_msg)?;
        let agent_id = request
            .get("agentId")
            .and_then(serde_json::Value::as_str)
            .or_else(|| target.and_then(|row| row.agent_id.as_deref()));
        let runtime_id = target.map(|row| row.session_id.0.as_str());
        if agent_id.is_none() && runtime_id.is_none() {
            return Ok(false);
        }
        let mut rows = self
            .store
            .identity_conn()
            .query(
                "SELECT 1 FROM delivery_obligations
                 WHERE state NOT IN ('delivered', 'rejected')
                   AND created_at <= ?1
                   AND ((?2 IS NOT NULL AND recipient_agent_id = ?2)
                     OR (?3 IS NOT NULL AND recipient_runtime_id = ?3))
                 LIMIT 1",
                params![command.created_at, agent_id, runtime_id],
            )
            .await
            .map_err(store_err)?;
        Ok(rows.next().await.map_err(store_err)?.is_some())
    }

    async fn has_claimed_prompt_for_target(
        &self,
        now: i64,
        candidate: &CommandIntentRow,
        target: Option<&SessionRow>,
    ) -> Result<bool, NexusError> {
        let Some(target_session_id) = target.map(|row| row.session_id.0.as_str()) else {
            return Ok(false);
        };
        let mut rows = self
            .store
            .identity_conn()
            .query(
                &format!(
                    "SELECT {COLUMNS} FROM command_intents WHERE kind = ?1 AND status = 'claimed' \
                     AND (lease_until > ?2 OR NOT ({PROMPT_RECLAIM_SAFE})) AND command_id != ?3"
                ),
                params![
                    crate::command_kinds::harness::PROMPT,
                    now,
                    candidate.command_id.as_str()
                ],
            )
            .await
            .map_err(store_err)?;
        let mut active = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            active.push(row_to_command_intent(&row)?);
        }
        for claimed in active {
            if self
                .resolve_target_session(&claimed)
                .await?
                .as_ref()
                .is_some_and(|row| row.session_id.0 == target_session_id)
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Claim the oldest pending or expired command row except the requested kind.
    pub async fn claim_next_except_kind(
        &self,
        now: i64,
        lease_ms: i64,
        kind: &str,
    ) -> Result<Option<CommandIntentRow>, NexusError> {
        self.claim_next_matching(now, lease_ms, ClaimFilter::ExceptKind(kind))
            .await
    }

    /// Claim the oldest pending or expired command row except two requested kinds.
    pub async fn claim_next_except_two_kinds(
        &self,
        now: i64,
        lease_ms: i64,
        first: &str,
        second: &str,
    ) -> Result<Option<CommandIntentRow>, NexusError> {
        self.claim_next_matching(now, lease_ms, ClaimFilter::ExceptTwoKinds(first, second))
            .await
    }

    /// Claim the oldest pending or expired command row except four requested kinds.
    pub async fn claim_next_except_four_kinds(
        &self,
        now: i64,
        lease_ms: i64,
        first: &str,
        second: &str,
        third: &str,
        fourth: &str,
    ) -> Result<Option<CommandIntentRow>, NexusError> {
        self.claim_next_matching(
            now,
            lease_ms,
            ClaimFilter::ExceptFourKinds(first, second, third, fourth),
        )
        .await
    }

    /// Claim the oldest pending or expired command row except five requested kinds.
    pub async fn claim_next_except_five_kinds(
        &self,
        now: i64,
        lease_ms: i64,
        first: &str,
        second: &str,
        third: &str,
        fourth: &str,
        fifth: &str,
    ) -> Result<Option<CommandIntentRow>, NexusError> {
        self.claim_next_matching(
            now,
            lease_ms,
            ClaimFilter::ExceptFiveKinds(first, second, third, fourth, fifth),
        )
        .await
    }

    /// Claim the oldest pending or expired command row matching any requested kind.
    pub async fn claim_next_matching_kind_set(
        &self,
        now: i64,
        lease_ms: i64,
        kinds: &[&str],
    ) -> Result<Option<CommandIntentRow>, NexusError> {
        let kinds_json = serde_json::to_string(kinds).map_err(store_msg)?;
        self.claim_next_matching(now, lease_ms, ClaimFilter::KindSet(&kinds_json))
            .await
    }

    /// Claim the oldest pending or expired command row excluding all requested kinds.
    pub async fn claim_next_excluding_kind_set(
        &self,
        now: i64,
        lease_ms: i64,
        kinds: &[&str],
    ) -> Result<Option<CommandIntentRow>, NexusError> {
        let kinds_json = serde_json::to_string(kinds).map_err(store_msg)?;
        self.claim_next_matching(now, lease_ms, ClaimFilter::ExceptKindSet(&kinds_json))
            .await
    }

    async fn claim_next_matching(
        &self,
        now: i64,
        lease_ms: i64,
        filter: ClaimFilter<'_>,
    ) -> Result<Option<CommandIntentRow>, NexusError> {
        // Keep the implicit UPDATE ... RETURNING write entirely inside the
        // split identity gate, including row decoding and statement finalization.
        let _identity_guard = self.store.lock_split_identity_write().await;
        let update = format!("UPDATE command_intents \
             SET status = 'claimed', attempts = attempts + 1, revision = revision + 1, claimed_at = ?1, started_at = NULL, lease_until = ?2 \
             WHERE command_id = ( \
                 SELECT command_id FROM command_intents \
                 WHERE (status = 'pending' OR (status = 'claimed' AND lease_until <= ?1)) \
                 AND {PROMPT_RECLAIM_SAFE}");
        let suffix = format!(
            ") \
             AND (status = 'pending' OR (status = 'claimed' AND lease_until <= ?1)) \
             AND {PROMPT_RECLAIM_SAFE} RETURNING "
        );
        let sql = match filter {
            ClaimFilter::Any => {
                format!("{update} ORDER BY created_at ASC LIMIT 1 {suffix}{COLUMNS}")
            }
            ClaimFilter::Kind(_) => {
                format!("{update} AND kind = ?3 ORDER BY created_at ASC LIMIT 1 {suffix}{COLUMNS}")
            }
            ClaimFilter::ExceptKind(_) => {
                format!("{update} AND kind != ?3 ORDER BY created_at ASC LIMIT 1 {suffix}{COLUMNS}")
            }
            ClaimFilter::ExceptTwoKinds(_, _) => format!(
                "{update} AND kind NOT IN (?3, ?4) ORDER BY created_at ASC LIMIT 1 {suffix}{COLUMNS}"
            ),
            ClaimFilter::ExceptFourKinds(_, _, _, _) => format!(
                "{update} AND kind NOT IN (?3, ?4, ?5, ?6) ORDER BY created_at ASC LIMIT 1 {suffix}{COLUMNS}"
            ),
            ClaimFilter::ExceptFiveKinds(_, _, _, _, _) => format!(
                "{update} AND kind NOT IN (?3, ?4, ?5, ?6, ?7) ORDER BY created_at ASC LIMIT 1 {suffix}{COLUMNS}"
            ),
            ClaimFilter::KindSet(_) => format!(
                "{update} AND kind IN (SELECT value FROM json_each(?3)) ORDER BY created_at ASC LIMIT 1 {suffix}{COLUMNS}"
            ),
            ClaimFilter::ExceptKindSet(_) => format!(
                "{update} AND kind NOT IN (SELECT value FROM json_each(?3)) ORDER BY created_at ASC LIMIT 1 {suffix}{COLUMNS}"
            ),
        };

        let mut rows = match filter {
            ClaimFilter::Any => {
                self.store
                    .identity_conn()
                    .query(&sql, params![now, now + lease_ms])
                    .await
            }
            ClaimFilter::Kind(kind) | ClaimFilter::ExceptKind(kind) => {
                self.store
                    .identity_conn()
                    .query(&sql, params![now, now + lease_ms, kind])
                    .await
            }
            ClaimFilter::ExceptTwoKinds(first, second) => {
                self.store
                    .identity_conn()
                    .query(&sql, params![now, now + lease_ms, first, second])
                    .await
            }
            ClaimFilter::ExceptFourKinds(first, second, third, fourth) => {
                self.store
                    .identity_conn()
                    .query(
                        &sql,
                        params![now, now + lease_ms, first, second, third, fourth],
                    )
                    .await
            }
            ClaimFilter::ExceptFiveKinds(first, second, third, fourth, fifth) => {
                self.store
                    .identity_conn()
                    .query(
                        &sql,
                        params![now, now + lease_ms, first, second, third, fourth, fifth],
                    )
                    .await
            }
            ClaimFilter::KindSet(kinds_json) | ClaimFilter::ExceptKindSet(kinds_json) => {
                self.store
                    .identity_conn()
                    .query(&sql, params![now, now + lease_ms, kinds_json])
                    .await
            }
        }
        .map_err(store_err)?;
        let claimed = match rows.next().await.map_err(store_err)? {
            Some(row) => Ok(Some(row_to_command_intent(&row)?)),
            None => Ok(None),
        };
        drop(rows);
        claimed
    }

    /// Mark a command as successfully executed and persist the serialized result.
    pub async fn mark_done(
        &self,
        command_id: &str,
        result_json: &str,
        now: i64,
    ) -> Result<(), NexusError> {
        let updated = self
            .store
            .identity_conn()
            .execute(
                &format!("UPDATE command_intents SET status = 'done', revision = revision + 1, result_json = ?2, error_json = NULL, \
                 completed_at = ?3, lease_until = NULL WHERE command_id = ?1 AND {GENERIC_SETTLEMENT_SAFE}"),
                params![command_id, result_json, now],
            )
            .await
            .map_err(store_err)?;
        if updated > 0 {
            self.store.events().command_intent_completed().signal();
        }
        Ok(())
    }

    /// Record the durable transition for other claims. Prompts and steers must use
    /// [`Self::mark_prompt_started_for_claim`] with the complete captured attempt.
    pub async fn mark_started_for_claim(
        &self,
        command_id: &str,
        claimed_at: i64,
        started_at: i64,
    ) -> Result<bool, NexusError> {
        let updated = self
            .store
            .identity_conn()
            .execute(
                "UPDATE command_intents SET started_at = ?3, revision = revision + 1 WHERE command_id = ?1 \
                 AND status = 'claimed' AND claimed_at = ?2 AND started_at IS NULL \
                 AND kind NOT IN ('harness.prompt', 'harness.steer')",
                params![command_id, claimed_at, started_at],
            )
            .await
            .map_err(store_err)?;
        Ok(updated > 0)
    }

    /// Return one current, unfinished claim to the durable pending queue when daemon shutdown
    /// wins before the command crosses its external acceptance boundary.
    ///
    /// Armed prompts and steers are excluded; typed nonacceptance requires the full claim.
    /// For these legacy claims the timestamp is the ownership token: a stale worker cannot release a row that a
    /// newer daemon has already reclaimed. Keep `attempts` unchanged so the durable row records
    /// that this daemon owned the claim even though it never reached the external provider.
    pub async fn release_claim_for_shutdown_retry(
        &self,
        command_id: &str,
        claimed_at: i64,
    ) -> Result<bool, NexusError> {
        let updated = self
            .store
            .identity_conn()
            .execute(
                &format!(
                    "UPDATE command_intents SET status = 'pending', revision = revision + 1, \
                 claimed_at = NULL, started_at = NULL, lease_until = NULL \
                 WHERE command_id = ?1 AND status = 'claimed' AND claimed_at = ?2 \
                 AND {PROMPT_RECLAIM_SAFE}"
                ),
                params![command_id, claimed_at],
            )
            .await
            .map_err(store_err)?;
        if updated > 0 {
            self.store.events().command_intent_inserted().signal();
        }
        Ok(updated > 0)
    }

    /// Atomically promote one still-pending boundary prompt into the redirect lane.
    ///
    /// The same row, command id, client idempotency key, and request JSON survive. A concurrent
    /// worker claim wins by changing `status` first; this update then returns `false` without
    /// cancelling or inserting anything, so cancel-plus-send duplication is impossible.
    pub async fn promote_pending_prompt_to_steer(
        &self,
        command_id: &str,
    ) -> Result<bool, NexusError> {
        let updated = self
            .store
            .identity_conn()
            .execute(
                "UPDATE command_intents SET kind = ?2, revision = revision + 1 WHERE command_id = ?1 \
                 AND kind = ?3 AND status = 'pending'",
                params![
                    command_id,
                    crate::command_kinds::harness::STEER,
                    crate::command_kinds::harness::PROMPT
                ],
            )
            .await
            .map_err(store_err)?;
        if updated > 0 {
            self.store.events().command_intent_inserted().signal();
        }
        Ok(updated > 0)
    }

    /// Cancel one pending prompt without racing a worker claim.
    pub async fn cancel_pending_prompt(
        &self,
        command_id: &str,
        now: i64,
    ) -> Result<bool, NexusError> {
        let updated = self
            .store
            .identity_conn()
            .execute(
                "UPDATE command_intents SET status = 'cancelled', revision = revision + 1, completed_at = ?2, \
                 lease_until = NULL WHERE command_id = ?1 AND kind = ?3 AND status = 'pending'",
                params![command_id, now, crate::command_kinds::harness::PROMPT],
            )
            .await
            .map_err(store_err)?;
        if updated > 0 {
            self.store.events().command_intent_completed().signal();
        }
        Ok(updated > 0)
    }

    /// Mark a command done only if the same claim is still current.
    ///
    /// Command leases make daemon restarts safe by allowing another worker to reclaim expired rows.
    /// A slow worker from the old claim can still finish later; this guarded update prevents that
    /// stale completion from overwriting the result owned by the newer claim.
    pub async fn mark_done_for_claim(
        &self,
        command_id: &str,
        claimed_at: i64,
        result_json: &str,
        now: i64,
    ) -> Result<bool, NexusError> {
        let updated = self
            .store
            .identity_conn()
            .execute(
                &format!("UPDATE command_intents SET status = 'done', revision = revision + 1, result_json = ?3, error_json = NULL, \
                 completed_at = ?4, lease_until = NULL WHERE command_id = ?1 \
                 AND status = 'claimed' AND claimed_at = ?2 AND {GENERIC_SETTLEMENT_SAFE}"),
                params![command_id, claimed_at, result_json, now],
            )
            .await
            .map_err(store_err)?;
        if updated > 0 {
            self.store.events().command_intent_completed().signal();
        }
        Ok(updated > 0)
    }

    /// Mark a command as failed and persist a structured error JSON string.
    pub async fn mark_error(
        &self,
        command_id: &str,
        error_json: &str,
        now: i64,
    ) -> Result<(), NexusError> {
        let updated = self
            .store
            .identity_conn()
            .execute(
                &format!("UPDATE command_intents SET status = 'error', revision = revision + 1, error_json = ?2, \
                 completed_at = ?3, lease_until = NULL WHERE command_id = ?1 AND {GENERIC_SETTLEMENT_SAFE}"),
                params![command_id, error_json, now],
            )
            .await
            .map_err(store_err)?;
        if updated > 0 {
            self.store.events().command_intent_completed().signal();
        }
        Ok(())
    }

    /// Mark a command failed only if the same claim is still current.
    ///
    /// This is the error-side twin of [`Self::mark_done_for_claim`].
    pub async fn mark_error_for_claim(
        &self,
        command_id: &str,
        claimed_at: i64,
        error_json: &str,
        now: i64,
    ) -> Result<bool, NexusError> {
        let updated = self
            .store
            .identity_conn()
            .execute(
                &format!("UPDATE command_intents SET status = 'error', revision = revision + 1, error_json = ?3, \
                 completed_at = ?4, lease_until = NULL WHERE command_id = ?1 \
                 AND status = 'claimed' AND claimed_at = ?2 AND {GENERIC_SETTLEMENT_SAFE}"),
                params![command_id, claimed_at, error_json, now],
            )
            .await
            .map_err(store_err)?;
        if updated > 0 {
            self.store.events().command_intent_completed().signal();
        }
        Ok(updated > 0)
    }

    /// Expire claimed `inbox.consume` rows owned by callers whose heartbeat is stale or offline.
    ///
    /// Long-poll consumes are the only command kind that intentionally waits for a wake signal. When
    /// the owning MCP/CLI session died during a daemon restart, re-running those stale rows would
    /// spend the inbox worker's time on consumers that can no longer receive the result. The daemon
    /// calls this before claiming inbox work so live consumers keep the lane.
    pub async fn expire_stale_inbox_consumes(
        &self,
        now: i64,
        heartbeat_ttl_ms: i64,
    ) -> Result<u64, NexusError> {
        let error_json =
            r#"{"message":"stale inbox consumer expired after heartbeat timeout"}"#.to_string();
        let mut rows = self
            .store
            .identity_conn()
            .query(
                &format!(
                    "SELECT {COLUMNS} FROM command_intents WHERE kind = ?1 AND status = 'claimed'"
                ),
                params![crate::command_kinds::inbox::CONSUME],
            )
            .await
            .map_err(store_err)?;
        let mut candidates = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            candidates.push(row_to_command_intent(&row)?);
        }

        let sessions = Sessions::new(self.store);
        let mut updated = 0;
        for candidate in candidates {
            let session = if let Some(session_id) = candidate.caller_session_id.as_deref() {
                if session_id == "local-operator" {
                    continue;
                }
                sessions
                    .find_by_session_id(&nexus_contracts::SessionId(session_id.to_string()))
                    .await?
            } else if let Some(client_key) = candidate.caller_client_key.as_deref() {
                sessions.find_by_client_key_any_project(client_key).await?
            } else {
                None
            };
            let stale = session.as_ref().is_none_or(|session| {
                session.presence.as_deref().unwrap_or("offline") == "offline"
                    || session
                        .last_heartbeat
                        .is_none_or(|heartbeat| now.saturating_sub(heartbeat) > heartbeat_ttl_ms)
            });
            if !stale {
                continue;
            }
            updated += self
                .store
                .identity_conn()
                .execute(
                    "UPDATE command_intents SET status = 'error', revision = revision + 1, \
                     error_json = ?2, completed_at = ?3, lease_until = NULL \
                     WHERE command_id = ?1 AND status = 'claimed'",
                    params![candidate.command_id, error_json.as_str(), now],
                )
                .await
                .map_err(store_err)?;
        }
        if updated > 0 {
            self.store.events().command_intent_completed().signal();
        }
        Ok(updated)
    }

    /// Load one command intent by id.
    pub async fn get(&self, command_id: &str) -> Result<Option<CommandIntentRow>, NexusError> {
        let mut rows = self
            .store
            .identity_conn()
            .query(
                &format!("{SELECT} WHERE command_id = ?1"),
                params![command_id],
            )
            .await
            .map_err(store_err)?;
        match rows.next().await.map_err(store_err)? {
            Some(row) => Ok(Some(row_to_command_intent(&row)?)),
            None => Ok(None),
        }
    }

    /// Load the current durable command row plus its latest append-only queue cursor.
    pub async fn receipt(
        &self,
        command_id: &str,
    ) -> Result<Option<CommandIntentReceipt>, NexusError> {
        let Some(command) = self.get(command_id).await? else {
            return Ok(None);
        };
        let mut rows = self
            .store
            .identity_conn()
            .query(
                "SELECT session_id, (SELECT MAX(seq) FROM command_intent_events WHERE command_id = ?1) FROM command_intent_events \
                 WHERE command_id = ?1 ORDER BY seq LIMIT 1",
                params![command_id],
            )
            .await
            .map_err(store_err)?;
        let event = rows.next().await.map_err(store_err)?;
        let request =
            serde_json::from_str(&command.request_json).unwrap_or(serde_json::Value::Null);
        let original_session = event
            .as_ref()
            .map(|row| get_opt_text(row, 0))
            .transpose()?
            .flatten();
        let session_id = super::command_queue::command_session_binding(
            self.store,
            &request,
            original_session.as_deref(),
        )
        .await?;
        Ok(Some(CommandIntentReceipt {
            command_id: command.command_id,
            status: command.status,
            created_at: command.created_at,
            revision: command.revision,
            session_id,
            seq: match event.as_ref() {
                Some(row) => get_opt_int(row, 1)?.unwrap_or(0),
                None => 0,
            },
        }))
    }

    /// Return scriptable lane depths grouped by command kind.
    pub async fn lane_depths(&self) -> Result<Vec<CommandIntentDepth>, NexusError> {
        let mut rows = self
            .store
            .identity_conn()
            .query(
                "SELECT kind, \
                 SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END), \
                 SUM(CASE WHEN status = 'claimed' THEN 1 ELSE 0 END) \
                 FROM command_intents WHERE status IN ('pending', 'claimed') \
                 GROUP BY kind ORDER BY kind",
                (),
            )
            .await
            .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(CommandIntentDepth {
                kind: get_text(&row, 0)?,
                pending: get_opt_int(&row, 1)?.unwrap_or(0),
                claimed: get_opt_int(&row, 2)?.unwrap_or(0),
            });
        }
        Ok(out)
    }

    /// Count claimed rows whose lease has expired.
    pub async fn expired_claimed_count(&self, now: i64) -> Result<i64, NexusError> {
        let mut rows = self
            .store
            .identity_conn()
            .query(
                "SELECT COUNT(*) FROM command_intents \
                 WHERE status = 'claimed' AND lease_until IS NOT NULL AND lease_until <= ?1",
                params![now],
            )
            .await
            .map_err(store_err)?;
        match rows.next().await.map_err(store_err)? {
            Some(row) => Ok(get_opt_int(&row, 0)?.unwrap_or(0)),
            None => Ok(0),
        }
    }

    /// Reap claimed rows whose dispatch boundary did not finish within the daemon's bounded
    /// graceful drain. These rows are externally ambiguous and must not be replayed automatically.
    pub async fn reap_claimed_for_shutdown(&self, now: i64) -> Result<u64, NexusError> {
        let error_json = r#"{"message":"daemon shutdown"}"#;
        let uncertain = prompt_uncertain_error_json();
        let updated = self
            .store
            .identity_conn()
            .execute(
                &format!(
                    "UPDATE command_intents SET status = 'error', revision = revision + 1, \
                 error_json = CASE WHEN {PROMPT_RECLAIM_SAFE} THEN ?2 ELSE ?3 END, \
                 completed_at = ?1, lease_until = NULL WHERE status = 'claimed'"
                ),
                params![now, error_json, uncertain],
            )
            .await
            .map_err(store_err)?;
        if updated > 0 {
            self.store.events().command_intent_completed().signal();
        }
        Ok(updated)
    }

    /// Delete terminal command-intent rows older than the caller's retention cutoff.
    ///
    /// `command_intents` is an ingress queue, not durable history. Completed results are only kept
    /// long enough for submitters to observe them; durable domain records live in their domain
    /// tables. Non-terminal rows are never deleted by this maintenance pass.
    pub async fn reap_terminal_older_than(&self, cutoff_timestamp: i64) -> Result<u64, NexusError> {
        let tx = self
            .store
            .begin_identity_write_txn("command_intents_reap_terminal")
            .await?;
        tx.execute(
            "DELETE FROM command_queue_mutations \
             WHERE response_json IS NOT NULL AND created_at <= ?1",
            params![cutoff_timestamp],
        )
        .await?;
        tx.execute(
            &format!(
                "DELETE FROM command_intent_events WHERE command_id IN ( \
               SELECT command_id FROM command_intents \
               WHERE status IN ('done', 'error', 'cancelled') \
                 AND completed_at IS NOT NULL AND completed_at <= ?1 \
                 AND {PROMPT_TERMINAL_REAP_SAFE} \
             )"
            ),
            params![cutoff_timestamp],
        )
        .await?;
        let reaped = tx
            .execute(
                &format!(
                    "DELETE FROM command_intents \
                 WHERE status IN ('done', 'error', 'cancelled') \
                   AND completed_at IS NOT NULL \
                   AND completed_at <= ?1 AND {PROMPT_TERMINAL_REAP_SAFE}"
                ),
                params![cutoff_timestamp],
            )
            .await?;
        tx.commit().await?;
        Ok(reaped)
    }
}

enum ClaimFilter<'a> {
    Any,
    Kind(&'a str),
    ExceptKind(&'a str),
    ExceptTwoKinds(&'a str, &'a str),
    ExceptFourKinds(&'a str, &'a str, &'a str, &'a str),
    ExceptFiveKinds(&'a str, &'a str, &'a str, &'a str, &'a str),
    KindSet(&'a str),
    ExceptKindSet(&'a str),
}

// For every prompt or steer delivery, `started_at` is a conservative may-have-attempted fence,
// NOT proof of native acceptance. Persist it before invoking admission. A lost response or
// expired lease cannot prove zero effect, so no claim path may replay an armed row. Other
// commands retain their existing reclaim behavior. This lives in the identity store and thus
// survives replacement of the daemon's boot-scoped transport database.
const PROMPT_RECLAIM_SAFE: &str = "NOT (kind IN ('harness.prompt', 'harness.steer') \
    AND started_at IS NOT NULL)";

const GENERIC_SETTLEMENT_SAFE: &str = "NOT (kind IN ('harness.prompt', 'harness.steer') \
    AND (started_at IS NOT NULL OR COALESCE(json_extract(CASE WHEN json_valid(request_json) THEN request_json ELSE '{}' END, '$.delivery') = 'auto', 0)))";

// Unknown delivery remains an unresolved obligation. Removing its receipt would allow a
// reconnect with the same id to create a new send. Known outcomes retain normal retention.
const PROMPT_TERMINAL_REAP_SAFE: &str = "NOT (kind IN ('harness.prompt', 'harness.steer') \
    AND COALESCE(json_extract(CASE WHEN json_valid(error_json) THEN error_json ELSE '{}' END, '$.code') = -32011, 0))";

fn prompt_uncertain_error_json() -> String {
    serde_json::json!({
        "code": nexus_contracts::codes::DELIVERY_UNCERTAIN,
        "message": "Delivery outcome is unknown; the agent may have received this message. Do not automatically retry."
    }).to_string()
}

const COLUMNS: &str = "command_id, kind, status, project, caller_name, caller_session_id, \
     caller_agent_id, caller_runtime_id, caller_client_key, caller_principal_id, \
     caller_validated_boot_epoch, caller_kind, caller_tier, \
     idempotency_key, request_json, result_json, error_json, attempts, revision, created_at, claimed_at, started_at, lease_until, \
     completed_at";

const SELECT: &str = "SELECT command_id, kind, status, project, caller_name, caller_session_id, \
     caller_agent_id, caller_runtime_id, caller_client_key, caller_principal_id, \
     caller_validated_boot_epoch, caller_kind, caller_tier, \
     idempotency_key, request_json, result_json, error_json, attempts, revision, created_at, claimed_at, started_at, lease_until, \
     completed_at FROM command_intents";

fn row_to_command_intent(row: &libsql::Row) -> Result<CommandIntentRow, NexusError> {
    let caller_kind = canonical_caller_kind(get_opt_text(row, 11)?)?;
    Ok(CommandIntentRow {
        command_id: get_text(row, 0)?,
        kind: get_text(row, 1)?,
        status: get_text(row, 2)?,
        project: get_text(row, 3)?,
        caller_name: get_text(row, 4)?,
        caller_session_id: get_opt_text(row, 5)?,
        caller_agent_id: get_opt_text(row, 6)?,
        caller_runtime_id: get_opt_text(row, 7)?,
        caller_client_key: get_opt_text(row, 8)?,
        caller_principal_id: get_opt_text(row, 9)?,
        caller_validated_boot_epoch: get_opt_text(row, 10)?,
        caller_kind,
        caller_tier: get_opt_text(row, 12)?,
        idempotency_key: get_opt_text(row, 13)?,
        request_json: get_text(row, 14)?,
        result_json: get_opt_text(row, 15)?,
        error_json: get_opt_text(row, 16)?,
        attempts: get_opt_int(row, 17)?.unwrap_or(0),
        revision: get_opt_int(row, 18)?.unwrap_or(1),
        created_at: get_opt_int(row, 19)?.unwrap_or(0),
        claimed_at: get_opt_int(row, 20)?,
        started_at: get_opt_int(row, 21)?,
        lease_until: get_opt_int(row, 22)?,
        completed_at: get_opt_int(row, 23)?,
    })
}

fn normalize_caller_kind(row: &mut NewCommandIntent) -> Result<(), NexusError> {
    row.caller_kind = canonical_caller_kind(row.caller_kind.take())?;
    Ok(())
}

fn canonical_caller_kind(value: Option<String>) -> Result<Option<String>, NexusError> {
    value
        .map(|value| {
            entity_kind::parse(&value)
                .map(|(locality, kind)| entity_kind::dotted(locality, kind))
                .ok_or_else(|| NexusError::Invalid(format!("unknown stored caller kind {value:?}")))
        })
        .transpose()
}

fn is_unique_constraint(err: &NexusError) -> bool {
    err.to_string().contains("UNIQUE constraint failed")
}

fn matching_existing_command(
    existing: &CommandIntentRow,
    requested: &NewCommandIntent,
    identity: &str,
) -> Result<String, NexusError> {
    let matches = matching_idempotent_kind(&existing.kind, &requested.kind)
        && existing.project == requested.project
        && existing.caller_name == requested.caller_name
        && existing.caller_session_id == requested.caller_session_id
        && existing.caller_agent_id == requested.caller_agent_id
        && existing.caller_runtime_id == requested.caller_runtime_id
        && existing.caller_client_key == requested.caller_client_key
        && existing.caller_principal_id == requested.caller_principal_id
        && existing.caller_kind == requested.caller_kind
        && existing.caller_tier == requested.caller_tier
        && existing.idempotency_key == requested.idempotency_key
        && existing.request_json == requested.request_json;
    if matches {
        Ok(existing.command_id.clone())
    } else {
        Err(NexusError::Invalid(format!(
            "{identity} already belongs to a different command request"
        )))
    }
}

fn matching_existing_command_with_validation(
    existing: &CommandIntentRow,
    requested: &NewCommandIntent,
    identity: &str,
    require_validated_caller: bool,
) -> Result<String, NexusError> {
    let command_id = matching_existing_command(existing, requested, identity)?;
    if require_validated_caller && existing.caller_validated_boot_epoch.is_none() {
        return Err(NexusError::Invalid(format!(
            "{identity} belongs to a command without daemon-validated caller authority"
        )));
    }
    Ok(command_id)
}

fn matching_idempotent_kind(existing: &str, requested: &str) -> bool {
    if existing == requested {
        return true;
    }
    let is_session_input = |kind: &str| {
        kind == crate::command_kinds::harness::PROMPT
            || kind == crate::command_kinds::harness::STEER
    };
    is_session_input(existing) && is_session_input(requested)
}
