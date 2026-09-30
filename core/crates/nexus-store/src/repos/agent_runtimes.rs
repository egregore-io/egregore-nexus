//! Disposable agent runtime repo.
//!
//! `agent_runtimes` records the current process/session representing a durable agent. The generic
//! row holds harness label, cwd, transport, presence, heartbeat, generic OS process ids, and bounded
//! observation-only model reports. Harness-native resume/control state belongs in harness storage.

use libsql::params;

use nexus_common::presence::presence_token;
use nexus_common::{now, NexusError, RuntimeProcessIds};
use nexus_contracts::enums::Presence;
use nexus_contracts::ids::SessionId;
use nexus_contracts::model_report::{
    ModelEvidenceCapability, ModelEvidenceSlot, ModelInvalidReason, ModelReportBackend,
    RuntimeModelReport, MAX_MODEL_REPORT_REVISION,
};

use crate::error::store_err;
use crate::repos::sessions::{get_opt_int, get_opt_text, get_text};
use crate::repos::{DeveloperEvents, Sessions};
use crate::state::{Store, WriteTxn};
use crate::types::AgentRuntimeRow;

// Keep ordinary runtime-retention eligibility independent of model JSON decoding. The managed
// caller must separately exclude every retained local observer slot before selecting deletion.
const RETENTION_ELIGIBLE: &str =
    "active=0 AND model_report_revision=0 AND stopped_at IS NOT NULL AND stopped_at<=?1";

/// Fields needed to create a runtime row.
#[derive(Debug, Clone)]
pub struct NewAgentRuntime {
    pub runtime_id: String,
    pub agent_id: String,
    pub harness: String,
    pub cwd: Option<String>,
    pub transport: Option<String>,
    pub presence: Option<String>,
    pub active: bool,
}

/// Captured durable runtime binding required by an exact stop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectedRuntimeBinding<'a> {
    Missing,
    Agent(&'a str),
}

/// Durable outcome of an exact runtime stop attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExactRuntimeStop {
    BindingChanged,
    Missing,
    Stopped,
}

/// Confirmed identity-transaction outcome for one captured disposable runtime binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectedRuntimeResidueCleanup {
    BindingChanged,
    AlreadyAbsent,
    Removed,
}

/// What this activation call can confirm about its identity transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeActivationCommitState {
    /// No activation writes were attempted, or an explicit rollback was confirmed.
    NotCommitted,
    /// The identity commit succeeded; a later lifecycle append may still have failed.
    Committed,
    /// Commit, rollback, or transaction-state loss prevents confirmation. Cancellation does not
    /// return a typed receipt; settlement belongs to the caller's coordinator.
    Unknown,
}

/// Captured durable target and stopped sibling bindings, not frozen lifecycle event names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeActivationChanges {
    target_pair: (String, String),
    stopped_sibling_pairs: Vec<(String, String)>,
}

impl RuntimeActivationChanges {
    /// The activated `(runtime_id, agent_id)` binding captured inside the transaction.
    pub fn target_pair(&self) -> &(String, String) {
        &self.target_pair
    }

    /// The complete stopped sibling bindings, sorted and deduplicated.
    pub fn stopped_sibling_pairs(&self) -> &[(String, String)] {
        &self.stopped_sibling_pairs
    }
}

/// A stale selection makes no changes; an applied selection carries the committed bindings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectedRuntimeActivation {
    SelectionChanged,
    Applied(RuntimeActivationChanges),
}

/// Store-constructed activation failure with an explicit durable disposition.
#[derive(Debug)]
pub struct RuntimeActivationFailure {
    cause: NexusError,
    commit_state: RuntimeActivationCommitState,
    confirmed_changes: Option<RuntimeActivationChanges>,
}

impl RuntimeActivationFailure {
    /// The original operation error, including any explicit rollback failure context.
    pub fn cause(&self) -> &NexusError {
        &self.cause
    }

    /// Durable phase knowledge, never inferred from the error text.
    pub fn commit_state(&self) -> RuntimeActivationCommitState {
        self.commit_state
    }

    /// Present only after a confirmed identity commit followed by a lifecycle append failure.
    pub fn confirmed_changes(&self) -> Option<&RuntimeActivationChanges> {
        self.confirmed_changes.as_ref()
    }

    fn new(cause: NexusError, commit_state: RuntimeActivationCommitState) -> Self {
        Self {
            cause,
            commit_state,
            confirmed_changes: None,
        }
    }
}

impl std::fmt::Display for RuntimeActivationFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.cause.fmt(f)
    }
}

impl std::error::Error for RuntimeActivationFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}

/// Persistence for the `agent_runtimes` table.
pub struct AgentRuntimes<'a> {
    store: &'a Store,
}

impl<'a> AgentRuntimes<'a> {
    /// Bind a repo to the shared store connection.
    pub fn new(store: &'a Store) -> Self {
        AgentRuntimes { store }
    }

    /// Capture active sibling bindings without decoding model metadata. Active rows with an
    /// existing `stopped_at` remain selected, matching legacy activation semantics.
    /// This snapshot is only a selector: activation revalidates it under the identity writer.
    pub async fn active_sibling_runtime_pairs(
        &self,
        agent_id: &str,
        runtime_id: &str,
    ) -> Result<Vec<(String, String)>, NexusError> {
        let rows = self
            .store
            .identity_conn()
            .query(ACTIVE_SIBLING_PAIRS, params![agent_id, runtime_id])
            .await
            .map_err(store_err)?;
        runtime_pairs(rows).await
    }

    /// Create an active runtime only if its exact ID is still missing and the entire captured
    /// active sibling set still matches. Invalid selectors fail before any writes.
    /// Legacy `create` and its callers retain their independent behavior.
    pub async fn create_active_selected(
        &self,
        runtime: NewAgentRuntime,
        siblings: &[(String, String)],
    ) -> Result<SelectedRuntimeActivation, RuntimeActivationFailure> {
        if !runtime.active {
            return Err(RuntimeActivationFailure::new(
                NexusError::Invalid("selected runtime creation requires active=true".into()),
                RuntimeActivationCommitState::NotCommitted,
            ));
        }
        let runtime_id = runtime.runtime_id.clone();
        let agent_id = runtime.agent_id.clone();
        self.activate_selection(&runtime_id, &agent_id, Some(runtime), siblings)
            .await
    }

    /// Activate only the exact agent binding and entire captured active sibling set. Checks
    /// precede all metadata decoding and writes under one identity transaction. The target's
    /// own observer/model/process/heartbeat fields are preserved, as in legacy `set_active`.
    pub async fn activate_selected(
        &self,
        runtime_id: &str,
        agent_id: &str,
        siblings: &[(String, String)],
    ) -> Result<SelectedRuntimeActivation, RuntimeActivationFailure> {
        self.activate_selection(runtime_id, agent_id, None, siblings)
            .await
    }

    async fn activate_selection(
        &self,
        runtime_id: &str,
        agent_id: &str,
        new_runtime: Option<NewAgentRuntime>,
        siblings: &[(String, String)],
    ) -> Result<SelectedRuntimeActivation, RuntimeActivationFailure> {
        if siblings
            .iter()
            .any(|(id, agent)| id == runtime_id || agent != agent_id)
        {
            return Err(RuntimeActivationFailure::new(
                NexusError::Invalid("selected siblings must belong to the target agent and exclude the target runtime".into()),
                RuntimeActivationCommitState::NotCommitted,
            ));
        }
        let mut selected = siblings.to_vec();
        selected.sort();
        selected.dedup();
        let tx = self
            .store
            .begin_identity_write_txn("agent_runtime_activate_selected")
            .await
            .map_err(|cause| {
                RuntimeActivationFailure::new(cause, RuntimeActivationCommitState::NotCommitted)
            })?;
        let ts = now();
        let result = async {
            // Read only identity columns until both target and complete sibling selection match.
            // In particular, a rebound row's malformed private model metadata cannot mask a race.
            let mut target_rows = tx.query(
                "SELECT agent_id FROM agent_runtimes WHERE runtime_id=?1",
                params![runtime_id],
            ).await?;
            let target_agent = target_rows.next().await.map_err(store_err)?
                .map(|row| get_text(&row, 0)).transpose()?;
            drop(target_rows);
            let target_matches = if new_runtime.is_some() {
                target_agent.is_none()
            } else {
                target_agent.as_deref() == Some(agent_id)
            };
            let actual = runtime_pairs(tx.query(ACTIVE_SIBLING_PAIRS, params![agent_id, runtime_id]).await?).await?;
            if !target_matches || actual != selected {
                return Ok(SelectedRuntimeActivation::SelectionChanged);
            }
            if new_runtime.is_some() {
                let mut retired = tx.query(
                    "SELECT 1 FROM retired_model_runtime_ids WHERE runtime_id=?1",
                    params![runtime_id],
                ).await?;
                if retired.next().await.map_err(store_err)?.is_some() {
                    return Err(NexusError::Invalid("retired runtime identity cannot be reused".into()));
                }
            }
            for (id, agent) in &actual {
                invalidate_model_owner(&tx, id).await?;
                let changed = tx.execute(
                    "UPDATE agent_runtimes SET active=0, stopped_at=COALESCE(stopped_at,?3),
                     os_pid=NULL, os_pgid=NULL WHERE runtime_id=?1 AND agent_id=?2 AND active=1",
                    params![id.as_str(), agent.as_str(), ts],
                ).await?;
                if changed != 1 {
                    return Err(NexusError::Store("selected sibling activation lost authority".into()));
                }
            }
            let changed = if let Some(runtime) = new_runtime {
                tx.execute(
                    "INSERT INTO agent_runtimes (runtime_id,agent_id,harness,cwd,transport,
                     presence,active,started_at,stopped_at,last_heartbeat)
                     VALUES (?1,?2,?3,?4,?5,?6,1,?7,NULL,NULL)",
                    params![runtime.runtime_id,runtime.agent_id,runtime.harness,runtime.cwd,
                        runtime.transport,runtime.presence,ts],
                ).await?
            } else {
                tx.execute(
                    "UPDATE agent_runtimes SET active=1,stopped_at=NULL WHERE runtime_id=?1 AND agent_id=?2",
                    params![runtime_id, agent_id],
                ).await?
            };
            if changed != 1 {
                return Err(NexusError::Store("selected target activation lost authority".into()));
            }
            Ok(SelectedRuntimeActivation::Applied(RuntimeActivationChanges {
                target_pair: (runtime_id.to_owned(), agent_id.to_owned()),
                stopped_sibling_pairs: actual,
            }))
        }.await;
        // Keep durable phase information here; legacy finish_runtime_tx erases it. A failed
        // commit stays Unknown even if the owned Drop subsequently succeeds in cleanup.
        let outcome = match result {
            Ok(outcome) => {
                tx.commit().await.map_err(|cause| {
                    RuntimeActivationFailure::new(cause, RuntimeActivationCommitState::Unknown)
                })?;
                outcome
            }
            Err(cause) => {
                return Err(match tx.rollback_confirmed(&cause).await {
                    Ok(true) => RuntimeActivationFailure::new(
                        cause,
                        RuntimeActivationCommitState::NotCommitted,
                    ),
                    Ok(false) => {
                        RuntimeActivationFailure::new(cause, RuntimeActivationCommitState::Unknown)
                    }
                    Err(cleanup_cause) => RuntimeActivationFailure::new(
                        cleanup_cause,
                        RuntimeActivationCommitState::Unknown,
                    ),
                });
            }
        };
        if let SelectedRuntimeActivation::Applied(changes) = &outcome {
            for (id, _) in changes.stopped_sibling_pairs() {
                // Preserve late name attribution and the original append error. These captured
                // bindings describe committed runtime changes, not an event-name snapshot.
                if let Err(cause) = self.append_stopped_lifecycle(id, ts).await {
                    return Err(RuntimeActivationFailure {
                        cause,
                        commit_state: RuntimeActivationCommitState::Committed,
                        confirmed_changes: Some(changes.clone()),
                    });
                }
            }
            self.store.events().session_lifecycle_changed().signal();
        }
        Ok(outcome)
    }

    /// Replace only the captured predecessor (including NULL), never a late current-owner lookup.
    /// Snapshot revisions are provisional wire values; only the transaction allocates authority.
    /// NULL matches the current unowned state, not an initial reservation epoch. The daemon's
    /// serialized reservation registry must reject obsolete reservations across a NULL ABA cycle.
    /// Inactive/stopped runtimes admit only inactive, non-Observed staging reports. Claiming never
    /// restores runtime liveness; applying evidence still requires an active, non-stopped runtime.
    pub async fn claim_model_observer(
        &self,
        runtime_id: &str,
        agent_id: &str,
        expected_token: Option<&str>,
        new_token: &str,
        initial: &RuntimeModelReport,
    ) -> Result<bool, NexusError> {
        validate_token(new_token)?;
        validate_snapshot(initial)?;
        let tx = self
            .store
            .begin_identity_write_txn("model_observer_claim")
            .await?;
        let result = async {
            let Some(state) = model_state(&tx, runtime_id).await? else {
                return Ok(false);
            };
            if state.agent_id != agent_id
                || state.token.as_deref() != expected_token
                || state.token.as_deref() == Some(new_token)
            {
                return Ok(false);
            }
            if (!state.active || state.stopped)
                && (initial.observer_active
                    || [
                        &initial.configured,
                        &initial.turn_selected,
                        &initial.response_reported,
                    ]
                    .iter()
                    .any(|slot| matches!(slot, ModelEvidenceSlot::Observed { .. }))
                    || initial
                        .telemetry
                        .as_ref()
                        .is_some_and(|report| report.has_observations()))
            {
                return Ok(false);
            }
            state.require_trusted_report()?;
            let mut report = initial.clone();
            report.report_revision = state.next_revision()?;
            write_report(&tx, runtime_id, &state, Some(new_token), 0, &report).await
        }
        .await;
        finish_model_tx(tx, result).await
    }

    /// Accept a strictly newer sequence from the exact captured active owner.
    pub async fn apply_model_report(
        &self,
        runtime_id: &str,
        agent_id: &str,
        token: &str,
        observer_sequence: i64,
        snapshot: &RuntimeModelReport,
    ) -> Result<bool, NexusError> {
        validate_token(token)?;
        validate_snapshot(snapshot)?;
        if observer_sequence <= 0 {
            return Err(NexusError::Invalid(
                "model observer sequence must be positive".into(),
            ));
        }
        let tx = self
            .store
            .begin_identity_write_txn("model_report_apply")
            .await?;
        let result = async {
            let Some(state) = model_state(&tx, runtime_id).await? else {
                return Ok(false);
            };
            if state.agent_id != agent_id
                || !state.active
                || state.stopped
                || state.token.as_deref() != Some(token)
                || observer_sequence <= state.sequence
            {
                return Ok(false);
            }
            state.require_trusted_report()?;
            if state
                .report
                .as_ref()
                .is_some_and(|previous| previous.backend != snapshot.backend)
            {
                return Err(NexusError::Invalid(
                    "model backend change requires a new observer claim".into(),
                ));
            }
            let mut report = snapshot.clone();
            report.report_revision = state.next_revision()?;
            write_report(
                &tx,
                runtime_id,
                &state,
                Some(token),
                observer_sequence,
                &report,
            )
            .await
        }
        .await;
        finish_model_tx(tx, result).await
    }

    /// Await durable invalidation of exactly this owner, preserving known historical evidence.
    pub async fn revoke_model_observer(
        &self,
        runtime_id: &str,
        agent_id: &str,
        token: &str,
    ) -> Result<bool, NexusError> {
        validate_token(token)?;
        let tx = self
            .store
            .begin_identity_write_txn("model_observer_revoke")
            .await?;
        let result = async {
            let Some(state) = model_state(&tx, runtime_id).await? else {
                return Ok(false);
            };
            if state.agent_id != agent_id || state.token.as_deref() != Some(token) {
                return Ok(false);
            }
            let report = state.inactive_report()?;
            write_report(&tx, runtime_id, &state, None, 0, &report).await
        }
        .await;
        finish_model_tx(tx, result).await
    }
    /// Insert a runtime. Creating an active runtime deactivates any previous active runtime for
    /// the same stable agent.
    pub async fn create(&self, runtime: NewAgentRuntime) -> Result<String, NexusError> {
        let runtime_id = runtime.runtime_id.clone();
        let stopped_siblings = if runtime.active {
            self.active_sibling_runtime_ids(&runtime.agent_id, &runtime_id)
                .await?
        } else {
            Vec::new()
        };
        let ts = now();
        let tx = self
            .store
            .begin_identity_write_txn("agent_runtime_create")
            .await?;
        let result = async {
            let mut retired = tx
                .query(
                    "SELECT 1 FROM retired_model_runtime_ids WHERE runtime_id=?1",
                    params![runtime_id.as_str()],
                )
                .await?;
            if retired.next().await.map_err(store_err)?.is_some() {
                return Err(NexusError::Invalid(
                    "retired runtime identity cannot be reused".into(),
                ));
            }
            drop(retired);
            if runtime.active {
                invalidate_selected(
                    &tx,
                    "agent_id = ?1 AND active = 1",
                    params![runtime.agent_id.as_str()],
                )
                .await?;
                tx.execute(
                    "UPDATE agent_runtimes SET active = 0, stopped_at = COALESCE(stopped_at, ?2), \
                 os_pid = NULL, os_pgid = NULL \
                 WHERE agent_id = ?1 AND active = 1",
                    params![runtime.agent_id.clone(), ts],
                )
                .await?;
            }
            tx.execute(
                "INSERT INTO agent_runtimes (runtime_id, agent_id, harness, cwd, transport, \
             presence, active, started_at, stopped_at, last_heartbeat) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, NULL, NULL)",
                params![
                    runtime.runtime_id,
                    runtime.agent_id,
                    runtime.harness,
                    runtime.cwd,
                    runtime.transport,
                    runtime.presence,
                    runtime.active as i64,
                    ts
                ],
            )
            .await?;
            Ok(())
        }
        .await;
        finish_runtime_tx(tx, result).await?;
        for stopped_runtime_id in stopped_siblings {
            self.append_stopped_lifecycle(&stopped_runtime_id, ts)
                .await?;
        }
        self.store.events().session_lifecycle_changed().signal();
        Ok(runtime_id)
    }

    /// Return the active runtime for a stable agent, if one exists.
    pub async fn active_for_agent(
        &self,
        agent_id: &str,
    ) -> Result<Option<AgentRuntimeRow>, NexusError> {
        self.query_one(
            "WHERE agent_id = ?1 AND active = 1 AND stopped_at IS NULL \
             ORDER BY started_at DESC LIMIT 1",
            params![agent_id],
        )
        .await
    }

    /// Legacy standalone retention, without daemon-local observer exclusion. Managed callers
    /// must use selected retention through their coordinator instead. No lifecycle or signal.
    pub async fn reap_retention_unmanaged(&self, cutoff: i64) -> Result<u64, NexusError> {
        self.store
            .identity_conn()
            .execute(
                "DELETE FROM agent_runtimes
             WHERE active = 0
               AND model_report_revision = 0
               AND stopped_at IS NOT NULL
               AND stopped_at <= ?1",
                params![cutoff],
            )
            .await
            .map_err(store_err)
    }

    /// Snapshot the existing retention predicate, without certifying local observer absence.
    pub async fn retention_candidate_pairs(
        &self,
        cutoff: i64,
    ) -> Result<Vec<(String, String)>, NexusError> {
        let mut rows = self.store.identity_conn().query(
            &format!("SELECT runtime_id,agent_id FROM agent_runtimes WHERE {RETENTION_ELIGIBLE} ORDER BY runtime_id,agent_id"),
            params![cutoff],
        ).await.map_err(store_err)?;
        let mut pairs = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            pairs.push((get_text(&row, 0)?, get_text(&row, 1)?));
        }
        Ok(pairs)
    }

    /// Delete only captured pairs that still satisfy ordinary retention inside the identity TX.
    ///
    /// The caller must exclude local observers and hold admission against new reservations.
    /// Exact duplicates are processed once; missing, rebound or ineligible rows are skipped.
    /// Returned sorted pairs crossed checked deletion, final absence checks and confirmed commit.
    /// Errors do not certify rollback. Like legacy retention, this emits no lifecycle or signal.
    /// Pair equality is not same-value delete/reinsert incarnation protection.
    pub async fn reap_retention_selected(
        &self,
        cutoff: i64,
        selected: &[(String, String)],
    ) -> Result<Vec<(String, String)>, NexusError> {
        let selected: std::collections::BTreeSet<_> = selected.iter().cloned().collect();
        if selected.is_empty() {
            return Ok(Vec::new());
        }
        let tx = self
            .store
            .begin_identity_write_txn("agent_runtime_selected_retention")
            .await?;
        let mut changed = Vec::new();
        let result = async {
            for (runtime, agent) in selected {
                let mut rows = tx.query(
                    &format!("SELECT 1 FROM agent_runtimes WHERE {RETENTION_ELIGIBLE} AND runtime_id=?2 AND agent_id=?3"),
                    params![cutoff,runtime.as_str(),agent.as_str()],
                ).await?;
                let eligible = rows.next().await.map_err(store_err)?.is_some();
                drop(rows);
                if !eligible { continue; }
                let count = tx.execute(
                    &format!("DELETE FROM agent_runtimes WHERE {RETENTION_ELIGIBLE} AND runtime_id=?2 AND agent_id=?3"),
                    params![cutoff,runtime.as_str(),agent.as_str()],
                ).await?;
                if count != 1 { return Err(NexusError::Store("selected runtime retention delete did not affect exactly one row".into())); }
                changed.push((runtime,agent));
            }
            // Check after ALL writes: a later DELETE trigger can recreate an earlier target.
            for (runtime, _) in &changed {
                let mut rows = tx.query("SELECT 1 FROM agent_runtimes WHERE runtime_id=?1",params![runtime.as_str()]).await?;
                let remains = rows.next().await.map_err(store_err)?.is_some();
                drop(rows);
                if remains { return Err(NexusError::Store("selected runtime retention target remains after deletion".into())); }
            }
            Ok(())
        }.await;
        finish_runtime_tx(tx, result).await?;
        Ok(changed)
    }

    /// Find a runtime by its disposable runtime id.
    pub async fn find_by_runtime_id(
        &self,
        runtime_id: &str,
    ) -> Result<Option<AgentRuntimeRow>, NexusError> {
        self.query_one("WHERE runtime_id = ?1", params![runtime_id])
            .await
    }

    /// Remove an impossible runtime row owned by a non-agent compatibility session.
    ///
    /// This is intentionally keyed only by the disposable runtime id: callers must first prove
    /// that the authoritative `sessions` row has a non-agent kind. The durable agent identity and
    /// every sibling runtime remain untouched. Versioned model history cannot be discarded here.
    pub async fn remove_non_agent_residue(&self, runtime_id: &str) -> Result<bool, NexusError> {
        let tx = self
            .store
            .begin_identity_write_txn("agent_runtime_remove_residue")
            .await?;
        let result = async {
            require_unversioned_runtime(&tx, runtime_id).await?;
            tx.execute(
                "DELETE FROM agent_runtimes WHERE runtime_id = ?1",
                params![runtime_id],
            )
            .await
            .map(|changed| changed > 0)
        }
        .await;
        let changed = finish_model_tx(tx, result).await?;
        if changed {
            self.store.events().session_lifecycle_changed().signal();
        }
        Ok(changed)
    }

    /// Remove only the exact captured, unversioned non-agent runtime residue.
    ///
    /// The caller must prove the authoritative Session is non-agent and coordinate local
    /// unclaimed observers. This transaction checks the runtime binding, not cross-store
    /// exclusion or same-value delete/reinsert incarnation. Missing is never a wildcard.
    /// Every successful outcome crosses a confirmed commit; an error does not certify no
    /// effects. Removed also checks post-delete absence inside the transaction, including
    /// effects of DELETE triggers. Durable agents and sibling runtimes remain untouched.
    pub async fn remove_non_agent_residue_selected(
        &self,
        runtime_id: &str,
        expected: ExpectedRuntimeBinding<'_>,
    ) -> Result<SelectedRuntimeResidueCleanup, NexusError> {
        let tx = self
            .store
            .begin_identity_write_txn("agent_runtime_selected_residue")
            .await?;
        let result = async {
            // Compare binding before parsing potentially corrupt model metadata on foreign rows.
            let mut rows = tx
                .query(
                    "SELECT agent_id FROM agent_runtimes WHERE runtime_id=?1",
                    params![runtime_id],
                )
                .await?;
            let current = rows
                .next()
                .await
                .map_err(store_err)?
                .map(|row| get_text(&row, 0))
                .transpose()?;
            drop(rows);
            let Some(agent_id) = current else {
                return Ok(match expected {
                    ExpectedRuntimeBinding::Missing => SelectedRuntimeResidueCleanup::AlreadyAbsent,
                    ExpectedRuntimeBinding::Agent(_) => {
                        SelectedRuntimeResidueCleanup::BindingChanged
                    }
                });
            };
            if expected != ExpectedRuntimeBinding::Agent(&agent_id) {
                return Ok(SelectedRuntimeResidueCleanup::BindingChanged);
            }
            require_unversioned_runtime(&tx, runtime_id).await?;
            let changed = tx
                .execute(
                    "DELETE FROM agent_runtimes WHERE runtime_id=?1 AND agent_id=?2",
                    params![runtime_id, agent_id],
                )
                .await?;
            if changed != 1 {
                return Err(NexusError::Store(
                    "selected runtime residue delete did not affect exactly one row".into(),
                ));
            }
            let mut rows = tx
                .query(
                    "SELECT 1 FROM agent_runtimes WHERE runtime_id=?1",
                    params![runtime_id],
                )
                .await?;
            let remains = rows.next().await.map_err(store_err)?.is_some();
            drop(rows);
            if remains {
                return Err(NexusError::Store(
                    "selected runtime residue remains after deletion".into(),
                ));
            }
            Ok(SelectedRuntimeResidueCleanup::Removed)
        }
        .await;
        match result {
            Ok(outcome) => {
                tx.commit().await?;
                if outcome == SelectedRuntimeResidueCleanup::Removed {
                    self.store.events().session_lifecycle_changed().signal();
                }
                Ok(outcome)
            }
            Err(error) => {
                tx.rollback(&error).await?;
                Err(error)
            }
        }
    }

    /// Roll back identity-side rows for a registration that never became externally visible.
    ///
    /// `runtime_id` is the freshly generated compatibility session id, so deleting that exact row
    /// cannot affect an older runtime. `generated_agent_id` is supplied only for the legacy
    /// name-only path, whose durable id is derived from that fresh session id. Existing explicit
    /// stable identities are never removed by this cleanup. Versioned model reports carry durable
    /// report history, so those rows are not eligible for staged-registration cleanup.
    pub async fn remove_staged_registration(
        &self,
        runtime_id: &str,
        generated_agent_id: Option<&str>,
    ) -> Result<(), NexusError> {
        let tx = self
            .store
            .begin_identity_write_txn("agent_registration_rollback")
            .await?;
        let result = async {
            require_unversioned_runtime(&tx, runtime_id).await?;
            tx.execute(
                "DELETE FROM agent_runtimes WHERE runtime_id = ?1",
                params![runtime_id],
            )
            .await?;
            if let Some(agent_id) = generated_agent_id {
                tx.execute(
                    "DELETE FROM agents WHERE agent_id = ?1
                     AND NOT EXISTS (
                       SELECT 1 FROM agent_runtimes WHERE agent_id = ?1
                     )
                     AND NOT EXISTS (
                       SELECT 1 FROM agent_credentials WHERE agent_id = ?1
                     )
                     AND NOT EXISTS (
                       SELECT 1 FROM native_thread_bindings WHERE agent_id = ?1
                     )",
                    params![agent_id],
                )
                .await?;
            }
            Ok(())
        }
        .await;
        match result {
            Ok(()) => {
                tx.commit().await?;
                self.store.events().session_lifecycle_changed().signal();
                Ok(())
            }
            Err(error) => {
                tx.rollback(&error).await?;
                Err(error)
            }
        }
    }

    /// Update runtime presence and heartbeat. Takes the [`Presence`] enum and serializes it to the
    /// stored lowercase token via the canonical [`nexus_common::presence::presence_token`].
    pub async fn set_presence(
        &self,
        runtime_id: &str,
        presence: Presence,
    ) -> Result<(), NexusError> {
        self.store
            .identity_conn()
            .execute(
                "UPDATE agent_runtimes SET presence = ?2, last_heartbeat = ?3 \
                 WHERE runtime_id = ?1",
                params![runtime_id, presence_token(presence), now()],
            )
            .await
            .map_err(store_err)?;
        self.store.events().session_lifecycle_changed().signal();
        Ok(())
    }

    /// Refresh an existing runtime as live, online, and freshly heartbeated.
    ///
    /// This is the durable-runtime twin of refreshing the compatibility `sessions` row for a live
    /// daemon-owned harness. It can recover this exact row when no sibling runtime is active, but it
    /// never steals `activeRuntime` away from a newer active sibling.
    pub async fn mark_live(&self, runtime_id: &str) -> Result<(), NexusError> {
        let runtime = self
            .find_by_runtime_id(runtime_id)
            .await?
            .ok_or_else(|| NexusError::NotFound(format!("runtime:{runtime_id}")))?;
        self.store
            .identity_conn()
            .execute(
                "UPDATE agent_runtimes SET active = 1, presence = 'online', stopped_at = NULL, \
                 last_heartbeat = ?2 WHERE runtime_id = ?1 AND NOT EXISTS ( \
                   SELECT 1 FROM agent_runtimes sibling \
                   WHERE sibling.agent_id = ?3 \
                     AND sibling.runtime_id <> ?1 \
                     AND sibling.active = 1 \
                     AND sibling.stopped_at IS NULL \
                 )",
                params![runtime_id, now(), runtime.agent_id],
            )
            .await
            .map_err(store_err)?;
        self.store.events().session_lifecycle_changed().signal();
        Ok(())
    }

    /// Toggle runtime active state. Activating one runtime deactivates any sibling runtime.
    pub async fn set_active(&self, runtime_id: &str, active: bool) -> Result<(), NexusError> {
        let runtime = self
            .find_by_runtime_id(runtime_id)
            .await?
            .ok_or_else(|| NexusError::NotFound(format!("runtime:{runtime_id}")))?;
        let ts = now();
        let stopped_siblings = if active {
            self.active_sibling_runtime_ids(&runtime.agent_id, runtime_id)
                .await?
        } else {
            Vec::new()
        };
        let tx = self
            .store
            .begin_identity_write_txn("agent_runtime_set_active")
            .await?;
        let result = async {
            if active {
                invalidate_selected(
                    &tx,
                    "agent_id = ?1 AND runtime_id <> ?2 AND active = 1",
                    params![runtime.agent_id.as_str(), runtime_id],
                )
                .await?;
                tx.execute(
                    "UPDATE agent_runtimes SET active = 0, stopped_at = COALESCE(stopped_at, ?2), \
                 os_pid = NULL, os_pgid = NULL \
                 WHERE agent_id = ?1 AND runtime_id <> ?3 AND active = 1",
                    params![runtime.agent_id, ts, runtime_id],
                )
                .await?;
                tx.execute(
                    "UPDATE agent_runtimes SET active = 1, stopped_at = NULL WHERE runtime_id = ?1",
                    params![runtime_id],
                )
                .await?;
            } else {
                invalidate_model_owner(&tx, runtime_id).await?;
                tx.execute(
                    "UPDATE agent_runtimes SET active = 0, stopped_at = COALESCE(stopped_at, ?2), \
                 os_pid = NULL, os_pgid = NULL \
                 WHERE runtime_id = ?1",
                    params![runtime_id, ts],
                )
                .await?;
            }
            Ok(())
        }
        .await;
        finish_runtime_tx(tx, result).await?;
        for stopped_runtime_id in stopped_siblings {
            self.append_stopped_lifecycle(&stopped_runtime_id, ts)
                .await?;
        }
        if !active && runtime.active && runtime.stopped_at.is_none() {
            self.append_stopped_lifecycle(runtime_id, ts).await?;
        }
        self.store.events().session_lifecycle_changed().signal();
        Ok(())
    }

    /// Stop a runtime and mark it offline.
    pub async fn stop(&self, runtime_id: &str) -> Result<(), NexusError> {
        let before = self.find_by_runtime_id(runtime_id).await?;
        let ts = now();
        let tx = self
            .store
            .begin_identity_write_txn("agent_runtime_stop")
            .await?;
        let result = async {
            invalidate_model_owner(&tx, runtime_id).await?;
            tx.execute(
                "UPDATE agent_runtimes SET active = 0, presence = 'offline', \
                 stopped_at = COALESCE(stopped_at, ?2), \
                 os_pid = NULL, os_pgid = NULL \
                 WHERE runtime_id = ?1",
                params![runtime_id, ts],
            )
            .await?;
            Ok(())
        }
        .await;
        finish_runtime_tx(tx, result).await?;
        if before
            .as_ref()
            .map(|row| row.active && row.stopped_at.is_none())
            .unwrap_or(false)
        {
            self.append_stopped_lifecycle(runtime_id, ts).await?;
        }
        self.store.events().session_lifecycle_changed().signal();
        Ok(())
    }

    /// Stop only the exact captured durable runtime binding.
    ///
    /// [`ExpectedRuntimeBinding::Missing`] asserts that the row is absent; it is never a wildcard.
    /// A matching row is durably stopped and has its generic process ledger cleared, but this does
    /// not claim that any native process was killed.
    pub async fn stop_if_binding_matches(
        &self,
        runtime_id: &str,
        expected: ExpectedRuntimeBinding<'_>,
    ) -> Result<ExactRuntimeStop, NexusError> {
        let tx = self
            .store
            .begin_identity_write_txn("agent_runtime_exact_stop")
            .await?;
        let ts = now();
        let result = async {
            // Read only the binding and lifecycle fields first. A foreign binding must be rejected
            // before corrupt model metadata can affect the outcome.
            let mut rows = tx
                .query(
                    "SELECT agent_id, active, stopped_at FROM agent_runtimes \
                     WHERE runtime_id = ?1",
                    params![runtime_id],
                )
                .await?;
            let current = rows
                .next()
                .await
                .map_err(store_err)?
                .map(|row| -> Result<_, NexusError> {
                    Ok((
                        get_text(&row, 0)?,
                        get_opt_int(&row, 1)? == Some(1),
                        get_opt_int(&row, 2)?.is_some(),
                    ))
                })
                .transpose()?;
            drop(rows);

            let Some((agent_id, was_active, was_stopped)) = current else {
                return Ok((
                    match expected {
                        ExpectedRuntimeBinding::Missing => ExactRuntimeStop::Missing,
                        ExpectedRuntimeBinding::Agent(_) => ExactRuntimeStop::BindingChanged,
                    },
                    false,
                ));
            };
            match expected {
                ExpectedRuntimeBinding::Missing => {
                    return Ok((ExactRuntimeStop::BindingChanged, false));
                }
                ExpectedRuntimeBinding::Agent(expected_agent_id)
                    if agent_id != expected_agent_id =>
                {
                    return Ok((ExactRuntimeStop::BindingChanged, false));
                }
                ExpectedRuntimeBinding::Agent(_) => {}
            }

            invalidate_model_owner(&tx, runtime_id).await?;
            let changed = tx
                .execute(
                    "UPDATE agent_runtimes SET active = 0, presence = 'offline', \
                     stopped_at = COALESCE(stopped_at, ?2), \
                     os_pid = NULL, os_pgid = NULL \
                     WHERE runtime_id = ?1 AND agent_id = ?3",
                    params![runtime_id, ts, agent_id],
                )
                .await?;
            if changed != 1 {
                return Err(NexusError::Store(
                    "exact runtime stop lost its durable binding".into(),
                ));
            }
            Ok((ExactRuntimeStop::Stopped, was_active && !was_stopped))
        }
        .await;
        match result {
            Ok((outcome, append_lifecycle)) => {
                tx.commit().await?;
                if append_lifecycle {
                    self.append_stopped_lifecycle(runtime_id, ts).await?;
                }
                if outcome == ExactRuntimeStop::Stopped {
                    self.store.events().session_lifecycle_changed().signal();
                }
                Ok(outcome)
            }
            Err(error) => {
                tx.rollback(&error).await?;
                Err(error)
            }
        }
    }

    /// Stop active runtime rows whose effective heartbeat is missing or older than `ttl_ms`.
    ///
    /// Runtime rows can be heartbeated directly, but daemon-owned headed runtimes often rely on the
    /// compatibility `sessions.last_heartbeat` row. The reconcile rule therefore uses the freshest
    /// non-null heartbeat across both rows for the same `runtime_id`, or its start time.
    /// Candidates are revalidated inside the identity transaction; this does not exclude a direct
    /// transport-store write after the final session read.
    pub async fn stop_stale(&self, now_ms: i64, ttl_ms: i64) -> Result<(), NexusError> {
        let stale = self.stale_active_runtime_pairs(now_ms, ttl_ms).await?;
        self.stop_stale_selected(&stale, now_ms, ttl_ms).await?;
        Ok(())
    }

    /// Revalidate and stop only the exact captured `(runtime_id, agent_id)` pairs.
    ///
    /// Exact duplicate pairs are processed once. Missing, rebound, inactive, stopped, or newly
    /// fresh rows are skipped without writes. Lifecycle facts are appended after the identity
    /// transaction commits, so an append failure can be returned after the runtime changes are
    /// durable.
    pub async fn stop_stale_selected(
        &self,
        candidates: &[(String, String)],
        now_ms: i64,
        ttl_ms: i64,
    ) -> Result<Vec<(String, String)>, NexusError> {
        let mut seen = std::collections::HashSet::new();
        let mut selected = Vec::new();
        for candidate in candidates {
            if seen.insert(candidate.clone()) {
                selected.push(candidate.clone());
            }
        }
        if selected.is_empty() {
            return Ok(Vec::new());
        }
        let tx = self
            .store
            .begin_identity_write_txn("agent_runtime_stop_stale")
            .await?;
        let mut stopped = Vec::new();
        let result = async {
            let sessions = Sessions::new(self.store);
            for (runtime_id, agent_id) in selected {
                let mut rows = tx
                    .query(
                        &format!(
                            "{SELECT} WHERE runtime_id = ?1 AND agent_id = ?2 \
                             AND active = 1 AND stopped_at IS NULL"
                        ),
                        params![runtime_id.as_str(), agent_id.as_str()],
                    )
                    .await?;
                let runtime = rows
                    .next()
                    .await
                    .map_err(store_err)?
                    .map(|row| row_to_runtime(&row))
                    .transpose()?;
                drop(rows);
                let Some(runtime) = runtime else { continue };
                let live = sessions
                    .find_by_session_id(&SessionId(runtime_id.clone()))
                    .await?;
                if !runtime_is_stale(&runtime, live.as_ref(), now_ms, ttl_ms) {
                    continue;
                }
                invalidate_model_owner(&tx, &runtime_id).await?;
                let changed = tx
                    .execute(
                        "UPDATE agent_runtimes \
                     SET active = 0, presence = 'offline', stopped_at = ?1, \
                         os_pid = NULL, os_pgid = NULL \
                     WHERE runtime_id = ?2 AND agent_id = ?3 AND active = 1 AND stopped_at IS NULL",
                        params![now_ms, runtime_id.as_str(), agent_id.clone()],
                    )
                    .await?;
                if changed > 0 {
                    stopped.push((runtime_id, agent_id));
                }
            }
            Ok(())
        }
        .await;
        finish_runtime_tx(tx, result).await?;
        if !stopped.is_empty() {
            for (runtime_id, _) in &stopped {
                self.append_stopped_lifecycle(&runtime_id, now_ms).await?;
            }
            self.store.events().session_lifecycle_changed().signal();
        }
        Ok(stopped)
    }

    /// Persist the OS process ids currently owned by this runtime.
    ///
    /// The tuple is generic process identity, not harness-private state. Daemon boot uses it to
    /// reap known orphaned process groups left behind by daemon crashes or forced exits.
    pub async fn set_process_ids(
        &self,
        runtime_id: &str,
        entry: RuntimeProcessIds,
    ) -> Result<(), NexusError> {
        let changed = self
            .store
            .identity_conn()
            .execute(
                "UPDATE agent_runtimes SET os_pid = ?2, os_pgid = ?3 \
                 WHERE runtime_id = ?1",
                params![
                    runtime_id,
                    i64::from(entry.os_pid),
                    i64::from(entry.os_pgid)
                ],
            )
            .await
            .map_err(store_err)?;
        if changed == 0 {
            return Err(NexusError::NotFound(format!("runtime:{runtime_id}")));
        }
        Ok(())
    }

    /// Clear the runtime's OS process ids after the process group has been verified dead.
    pub async fn clear_process_ids(&self, runtime_id: &str) -> Result<(), NexusError> {
        self.store
            .identity_conn()
            .execute(
                "UPDATE agent_runtimes SET os_pid = NULL, os_pgid = NULL WHERE runtime_id = ?1",
                params![runtime_id],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// List rows whose durable process ids should be considered for daemon boot reaping.
    ///
    /// Active ACP rows are daemon-owned stdio processes and cannot be adopted after daemon death,
    /// so a matching ledger tuple is an orphan. Stopped/offline rows are also candidates. Active
    /// headed/app-server rows remain adoptable and are deliberately excluded.
    pub async fn list_boot_process_reap_candidates(
        &self,
    ) -> Result<Vec<AgentRuntimeRow>, NexusError> {
        let mut rows = self
            .store
            .identity_conn()
            .query(
                &format!(
                    "{SELECT} WHERE os_pid IS NOT NULL \
                     AND os_pgid IS NOT NULL \
                     AND (transport = 'acp' OR active = 0 OR stopped_at IS NOT NULL \
                          OR COALESCE(presence, 'offline') = 'offline') \
                     ORDER BY started_at DESC"
                ),
                (),
            )
            .await
            .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(row_to_runtime(&row)?);
        }
        Ok(out)
    }

    /// List runtimes for one stable agent. By default only the active runtime is returned; set
    /// `include_stopped` to include historical and inactive rows.
    pub async fn list_for_agent(
        &self,
        agent_id: &str,
        include_stopped: bool,
    ) -> Result<Vec<AgentRuntimeRow>, NexusError> {
        let mut rows = if include_stopped {
            self.store
                .identity_conn()
                .query(
                    &format!("{SELECT} WHERE agent_id = ?1 ORDER BY started_at DESC"),
                    params![agent_id],
                )
                .await
        } else {
            self.store
                .identity_conn()
                .query(
                    &format!(
                        "{SELECT} WHERE agent_id = ?1 AND active = 1 AND stopped_at IS NULL \
                         ORDER BY started_at DESC"
                    ),
                    params![agent_id],
                )
                .await
        }
        .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(row_to_runtime(&row)?);
        }
        Ok(out)
    }

    /// List active runtime rows matching a harness and transport. This is used by daemon boot
    /// adoption for harness-owned sidecars, where the generic runtime row identifies which
    /// disposable sessions should have auxiliary observers reattached without relaunching them.
    pub async fn list_active_by_harness_transport(
        &self,
        harness: &str,
        transport: &str,
    ) -> Result<Vec<AgentRuntimeRow>, NexusError> {
        let mut rows = self
            .store
            .identity_conn()
            .query(
                &format!(
                    "{SELECT} WHERE harness = ?1 AND transport = ?2 AND active = 1 \
                     AND stopped_at IS NULL ORDER BY started_at DESC"
                ),
                params![harness, transport],
            )
            .await
            .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(row_to_runtime(&row)?);
        }
        Ok(out)
    }

    /// List active runtime rows matching a transport. Daemon boot adoption uses this to reattach
    /// loop/observer tasks for headed runtimes that survived the daemon process restart.
    pub async fn list_active_by_transport(
        &self,
        transport: &str,
    ) -> Result<Vec<AgentRuntimeRow>, NexusError> {
        let mut rows = self
            .store
            .identity_conn()
            .query(
                &format!(
                    "{SELECT} WHERE transport = ?1 AND active = 1 AND stopped_at IS NULL \
                     ORDER BY started_at DESC"
                ),
                params![transport],
            )
            .await
            .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(row_to_runtime(&row)?);
        }
        Ok(out)
    }

    async fn query_one(
        &self,
        where_clause: &str,
        p: impl libsql::params::IntoParams,
    ) -> Result<Option<AgentRuntimeRow>, NexusError> {
        let mut rows = self
            .store
            .identity_conn()
            .query(&format!("{SELECT} {where_clause}"), p)
            .await
            .map_err(store_err)?;
        match rows.next().await.map_err(store_err)? {
            Some(row) => Ok(Some(row_to_runtime(&row)?)),
            None => Ok(None),
        }
    }

    /// Capture active runtime bindings that are currently stale under the reconciliation policy.
    pub async fn stale_active_runtime_pairs(
        &self,
        now_ms: i64,
        ttl_ms: i64,
    ) -> Result<Vec<(String, String)>, NexusError> {
        let mut rows = self
            .store
            .identity_conn()
            .query(
                &format!("{SELECT} WHERE active = 1 AND stopped_at IS NULL ORDER BY started_at"),
                (),
            )
            .await
            .map_err(store_err)?;
        let mut runtimes = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            runtimes.push(row_to_runtime(&row)?);
        }
        drop(rows);

        let sessions = Sessions::new(self.store);
        let mut out = Vec::new();
        for runtime in runtimes {
            let live = sessions
                .find_by_session_id(&SessionId(runtime.runtime_id.clone()))
                .await?;
            if runtime_is_stale(&runtime, live.as_ref(), now_ms, ttl_ms) {
                out.push((runtime.runtime_id, runtime.agent_id));
            }
        }
        Ok(out)
    }

    async fn active_sibling_runtime_ids(
        &self,
        agent_id: &str,
        runtime_id: &str,
    ) -> Result<Vec<String>, NexusError> {
        let mut rows = self
            .store
            .identity_conn()
            .query(
                "SELECT runtime_id FROM agent_runtimes \
                 WHERE agent_id = ?1 AND runtime_id <> ?2 AND active = 1",
                params![agent_id, runtime_id],
            )
            .await
            .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(get_text(&row, 0)?);
        }
        Ok(out)
    }

    async fn append_stopped_lifecycle(
        &self,
        runtime_id: &str,
        created_at: i64,
    ) -> Result<(), NexusError> {
        let Some(agent_name) = self.agent_name_for_runtime(runtime_id).await? else {
            return Ok(());
        };
        DeveloperEvents::new(self.store)
            .append_agent_lifecycle(
                &agent_name,
                &SessionId(runtime_id.to_string()),
                "stopped",
                None,
                created_at,
            )
            .await?;
        Ok(())
    }

    async fn agent_name_for_runtime(&self, runtime_id: &str) -> Result<Option<String>, NexusError> {
        let mut rows = self
            .store
            .identity_conn()
            .query(
                "SELECT a.name FROM agent_runtimes ar \
                 LEFT JOIN agents a ON a.agent_id = ar.agent_id \
                 WHERE ar.runtime_id = ?1 LIMIT 1",
                params![runtime_id],
            )
            .await
            .map_err(store_err)?;
        let durable_name = match rows.next().await.map_err(store_err)? {
            Some(row) => get_opt_text(&row, 0)?,
            None => None,
        };
        if durable_name.is_some() {
            return Ok(durable_name);
        }
        Ok(Sessions::new(self.store)
            .find_by_session_id(&SessionId(runtime_id.to_string()))
            .await?
            .and_then(|row| row.name))
    }
}

fn runtime_is_stale(
    runtime: &AgentRuntimeRow,
    live: Option<&crate::types::SessionRow>,
    now_ms: i64,
    ttl_ms: i64,
) -> bool {
    let effective_heartbeat = runtime
        .last_heartbeat
        .into_iter()
        .chain(live.and_then(|row| row.last_heartbeat))
        .chain(std::iter::once(runtime.started_at))
        .max()
        .unwrap_or(runtime.started_at);
    let explicitly_offline =
        live.is_some_and(|row| row.presence.as_deref().unwrap_or("offline") == "offline");
    explicitly_offline || now_ms.saturating_sub(effective_heartbeat) > ttl_ms
}

const SELECT: &str = "SELECT runtime_id, agent_id, harness, cwd, transport, presence, active, \
     started_at, stopped_at, last_heartbeat, os_pid, os_pgid, model_observer_token, \
     model_observer_sequence, model_report_revision, model_report_json FROM agent_runtimes";

pub(crate) fn row_to_runtime(row: &libsql::Row) -> Result<AgentRuntimeRow, NexusError> {
    let (token, sequence, revision) = decode_authority(row, 12)?;
    let report = decode_report(row, 15, revision, token.is_some());
    Ok(AgentRuntimeRow {
        runtime_id: get_text(row, 0)?,
        agent_id: get_text(row, 1)?,
        harness: get_text(row, 2)?,
        cwd: get_opt_text(row, 3)?,
        transport: get_opt_text(row, 4)?,
        presence: get_opt_text(row, 5)?,
        active: get_opt_int(row, 6)?.unwrap_or(0) != 0,
        started_at: get_opt_int(row, 7)?.unwrap_or(0),
        stopped_at: get_opt_int(row, 8)?,
        last_heartbeat: get_opt_int(row, 9)?,
        os_pid: get_opt_int(row, 10)?,
        os_pgid: get_opt_int(row, 11)?,
        model_observer_token: token,
        model_observer_sequence: sequence,
        model_report_revision: revision,
        model_report: report,
    })
}

fn validate_token(token: &str) -> Result<(), NexusError> {
    if token.trim().is_empty() || token.len() > 128 || token.chars().any(char::is_control) {
        return Err(NexusError::Invalid("invalid model observer token".into()));
    }
    Ok(())
}

fn validate_snapshot(report: &RuntimeModelReport) -> Result<(), NexusError> {
    report
        .validate()
        .map_err(|e| NexusError::Invalid(e.to_string()))?;
    if report.backend.is_unknown() {
        return Err(NexusError::Invalid(
            "unknown model backend is reserved for durable invalidation".into(),
        ));
    }
    Ok(())
}

fn corrupt_authority() -> NexusError {
    NexusError::Store("corrupt stored model report authority".into())
}

fn decode_authority(
    row: &libsql::Row,
    offset: i32,
) -> Result<(Option<String>, i64, i64), NexusError> {
    let token = match row.get_value(offset).map_err(|_| corrupt_authority())? {
        libsql::Value::Null => None,
        libsql::Value::Text(token) => {
            validate_token(&token).map_err(|_| corrupt_authority())?;
            Some(token)
        }
        _ => return Err(corrupt_authority()),
    };
    let integer = |index| match row.get_value(index).map_err(|_| corrupt_authority())? {
        libsql::Value::Integer(n) if n >= 0 => Ok(n),
        _ => Err(corrupt_authority()),
    };
    let sequence = integer(offset + 1)?;
    let revision = integer(offset + 2)?;
    if revision as u64 > MAX_MODEL_REPORT_REVISION
        || (token.is_none() && sequence != 0)
        || (token.is_some() && revision == 0)
    {
        return Err(corrupt_authority());
    }
    Ok((token, sequence, revision))
}

/// Only the report is omitted on malformed JSON; authority is separately validated and retained.
/// Never include private tokens or raw metadata in diagnostics.
fn decode_report(
    row: &libsql::Row,
    index: i32,
    revision: i64,
    owned: bool,
) -> Option<RuntimeModelReport> {
    match row.get_value(index) {
        Ok(libsql::Value::Null) if revision == 0 => None,
        Ok(libsql::Value::Text(json)) => {
            if let Ok(report) = serde_json::from_str::<RuntimeModelReport>(&json) {
                if report.report_revision == revision as u64 && (owned || !report.observer_active) {
                    return Some(report);
                }
            }
            tracing::warn!("untrusted stored model report omitted");
            None
        }
        _ => {
            tracing::warn!("untrusted stored model report omitted");
            None
        }
    }
}

struct ModelState {
    agent_id: String,
    active: bool,
    stopped: bool,
    token: Option<String>,
    sequence: i64,
    revision: i64,
    report: Option<RuntimeModelReport>,
    has_json: bool,
}
impl ModelState {
    fn require_trusted_report(&self) -> Result<(), NexusError> {
        if self.report.is_none() && (self.revision != 0 || self.has_json) {
            return Err(NexusError::Store(
                "corrupt stored model report; explicit invalidation required".into(),
            ));
        }
        Ok(())
    }
    fn next_revision(&self) -> Result<u64, NexusError> {
        if self.revision as u64 >= MAX_MODEL_REPORT_REVISION {
            return Err(NexusError::Store("model report revision exhausted".into()));
        }
        Ok(self.revision as u64 + 1)
    }
    fn inactive_report(&self) -> Result<RuntimeModelReport, NexusError> {
        let revision = self.next_revision()?;
        if let Some(mut report) = self.report.clone() {
            report.observer_active = false;
            report.report_revision = revision;
            return Ok(report);
        }
        let invalid = ModelEvidenceSlot::Invalid {
            capability: ModelEvidenceCapability::Unverified,
            reason: ModelInvalidReason::CorruptStoredMetadata,
        };
        Ok(RuntimeModelReport {
            backend: ModelReportBackend::new("unknown").map_err(|_| corrupt_authority())?,
            observer_active: false,
            report_revision: revision,
            configured: invalid.clone(),
            turn_selected: invalid.clone(),
            response_reported: invalid,
            telemetry: None,
        })
    }
}
async fn model_state(tx: &WriteTxn, id: &str) -> Result<Option<ModelState>, NexusError> {
    let mut rows = tx
        .query(
            "SELECT agent_id, active, stopped_at, model_observer_token, model_observer_sequence, \
         model_report_revision, model_report_json FROM agent_runtimes WHERE runtime_id = ?1",
            params![id],
        )
        .await?;
    let Some(row) = rows.next().await.map_err(store_err)? else {
        return Ok(None);
    };
    let (token, sequence, revision) = decode_authority(&row, 3)?;
    let report = decode_report(&row, 6, revision, token.is_some());
    let has_json = !matches!(row.get_value(6).map_err(store_err)?, libsql::Value::Null);
    Ok(Some(ModelState {
        agent_id: get_text(&row, 0)?,
        active: get_opt_int(&row, 1)? == Some(1),
        stopped: get_opt_int(&row, 2)?.is_some(),
        token,
        sequence,
        revision,
        report,
        has_json,
    }))
}
async fn write_report(
    tx: &WriteTxn,
    id: &str,
    state: &ModelState,
    token: Option<&str>,
    sequence: i64,
    report: &RuntimeModelReport,
) -> Result<bool, NexusError> {
    let json = serde_json::to_string(report)
        .map_err(|_| NexusError::Store("invalid model report snapshot".into()))?;
    let changed = tx
        .execute(
            "UPDATE agent_runtimes SET model_observer_token = ?1, model_observer_sequence = ?2, \
         model_report_revision = ?3, model_report_json = ?4 \
         WHERE runtime_id = ?5 AND agent_id = ?6 AND model_observer_token IS ?7 \
         AND model_observer_sequence = ?8 AND model_report_revision = ?9",
            params![
                token,
                sequence,
                report.report_revision as i64,
                json,
                id,
                state.agent_id.as_str(),
                state.token.as_deref(),
                state.sequence,
                state.revision
            ],
        )
        .await?;
    Ok(changed > 0)
}
async fn finish_model_tx(
    tx: WriteTxn,
    result: Result<bool, NexusError>,
) -> Result<bool, NexusError> {
    match result {
        Ok(changed) => {
            tx.commit().await?;
            Ok(changed)
        }
        Err(error) => {
            tx.rollback(&error).await?;
            Err(error)
        }
    }
}

/// Called inside the same identity transaction as the liveness transition. Missing reports on
/// never-observed rows stay absent. Corrupt JSON with sound revision authority is recoverable.
async fn invalidate_model_owner(tx: &WriteTxn, id: &str) -> Result<(), NexusError> {
    if let Some(state) = model_state(tx, id).await? {
        if state.token.is_some()
            || (state.report.is_none() && (state.revision > 0 || state.has_json))
        {
            let report = state.inactive_report()?;
            if !write_report(tx, id, &state, None, 0, &report).await? {
                return Err(NexusError::Store(
                    "model invalidation lost authority".into(),
                ));
            }
        }
    }
    Ok(())
}

async fn invalidate_selected(
    tx: &WriteTxn,
    predicate: &str,
    p: impl libsql::params::IntoParams,
) -> Result<(), NexusError> {
    let mut rows = tx
        .query(
            &format!("SELECT runtime_id FROM agent_runtimes WHERE {predicate}"),
            p,
        )
        .await?;
    let mut ids = Vec::new();
    while let Some(row) = rows.next().await.map_err(store_err)? {
        ids.push(get_text(&row, 0)?);
    }
    drop(rows);
    for id in ids {
        invalidate_model_owner(tx, &id).await?;
    }
    Ok(())
}

const ACTIVE_SIBLING_PAIRS: &str = "SELECT runtime_id,agent_id FROM agent_runtimes
    WHERE agent_id=?1 AND runtime_id<>?2 AND active=1 ORDER BY runtime_id,agent_id";

async fn runtime_pairs(mut rows: libsql::Rows) -> Result<Vec<(String, String)>, NexusError> {
    let mut pairs = Vec::new();
    while let Some(row) = rows.next().await.map_err(store_err)? {
        pairs.push((get_text(&row, 0)?, get_text(&row, 1)?));
    }
    Ok(pairs)
}

async fn finish_runtime_tx(tx: WriteTxn, result: Result<(), NexusError>) -> Result<(), NexusError> {
    match result {
        Ok(()) => tx.commit().await,
        Err(error) => {
            tx.rollback(&error).await?;
            Err(error)
        }
    }
}

async fn require_unversioned_runtime(tx: &WriteTxn, id: &str) -> Result<(), NexusError> {
    if let Some(state) = model_state(tx, id).await? {
        if state.revision != 0 || state.token.is_some() || state.has_json {
            return Err(NexusError::Store(
                "cannot discard versioned model runtime history during registration cleanup".into(),
            ));
        }
    }
    Ok(())
}
