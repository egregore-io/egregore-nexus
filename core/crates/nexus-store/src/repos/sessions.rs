//! The `sessions` repo — register-once identity rows, project-scoped reads. Higher-level
//! resume-vs-reject identity logic lives in `nexus-identity`; this repo is pure persistence.

use libsql::params;

use nexus_common::presence::presence_token;
use nexus_common::{now, NexusError};
use nexus_contracts::entity_kind;
use nexus_contracts::enums::{Kind, Presence};
use nexus_contracts::ids::SessionId;

use crate::error::store_err;
use crate::repos::{AgentRuntimes, Agents, DeveloperEvents};
use crate::state::{Store, WriteTxn};
use crate::types::SessionRow;

/// Fields needed to create a session row. Named rows are globally addressable; `None` means this
/// is a staged identity that must be explicitly named later.
#[derive(Debug, Clone)]
pub struct NewSession {
    pub session_id: SessionId,
    pub name: Option<String>,
    pub agent: Option<String>,
    pub kind: String,
    pub role: Option<String>,
    pub tier: String,
    pub harness_session_id: Option<String>,
    pub client_key: Option<String>,
    pub cwd: Option<String>,
    pub project: String,
    /// Transport mode: `'pty'` | `'acp'` | `'codex-appserver'`. `None` → NULL (legacy row).
    pub transport: Option<String>,
}

/// Opaque authority over the exact persisted image of one staged registration.
///
/// Use only with the same `Store` that issued it. The normalized projection is convenience,
/// not authority. This is not a cross-store, dual-store atomicity, or same-value delete/reinsert
/// incarnation guarantee, and does not exclude arbitrary direct writers bypassing the gate.
/// Deliberately has no public constructor, serialization, or raw/client-key debug output.
pub struct CapturedStagedSession {
    raw: [libsql::Value; 20],
    row: SessionRow,
}

impl CapturedStagedSession {
    /// Read-only normalized projection; never use projection equality to authorize a write.
    pub fn row(&self) -> &SessionRow {
        &self.row
    }
}

/// Result after selecting and, if still exact, committing only the agent binding update.
pub enum SelectedStagedSessionStamp {
    SelectionChanged,
    Updated(CapturedStagedSession),
}

/// Result of a selected staged-row cleanup; no lifecycle fact is appended.
pub enum SelectedStagedSessionCleanup {
    AlreadyAbsent,
    SelectionChanged,
    Removed,
}

/// Identity-store phase only; a failed commit never supplies a deletion receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityPurgeCommitState {
    /// Submission did not begin, or an explicit rollback completed successfully.
    NotCommitted,
    /// Commit/rollback failure or transaction-state loss prevents confirmation.
    Unknown,
}

/// Exact identity deletion association, not transport cleanup or authentication authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityPurgeReceipt {
    session_id: SessionId,
    agent_id: Option<String>,
    project: String,
    name: Option<String>,
    runtime_pairs: Vec<(String, String)>,
}

impl IdentityPurgeReceipt {
    /// Original selection label, binding downstream name-based cleanup to this receipt.
    pub fn project(&self) -> &str {
        &self.project
    }
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }
    /// Target association captured by the identity transaction, not a Session ownership proof.
    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }
    /// Exact supplied stable identity; None never authorizes name-based agent deletion.
    pub fn agent_id(&self) -> Option<&str> {
        self.agent_id.as_deref()
    }
    /// Sorted bindings whose IDs were verified absent before the confirmed identity commit.
    pub fn runtime_pairs(&self) -> &[(String, String)] {
        &self.runtime_pairs
    }
}

/// Successful identity transaction outcome; no transport deletion or lifecycle publication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectedIdentityPurge {
    SelectionChanged,
    Purged(IdentityPurgeReceipt),
}

/// Transport-only outcome after confirmed commit. Purged confirms Session absence with
/// best-effort child cleanup, not all-history erasure or rollback of the identity phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectedTransportPurge {
    SelectionChanged,
    Purged,
}

#[derive(Debug)]
pub struct IdentityPurgeFailure {
    cause: NexusError,
    commit_state: IdentityPurgeCommitState,
}

impl IdentityPurgeFailure {
    /// Original cause, retaining any explicit rollback failure context.
    pub fn cause(&self) -> &NexusError {
        &self.cause
    }
    /// Durable disposition obtained from transaction operations, never error-text inference.
    pub fn commit_state(&self) -> IdentityPurgeCommitState {
        self.commit_state
    }
}

impl std::fmt::Display for IdentityPurgeFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.cause.fmt(f)
    }
}
impl std::error::Error for IdentityPurgeFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}

const CAPTURED_SESSION_COLUMNS: [&str; 20] = [
    "session_id",
    "name",
    "agent",
    "kind",
    "role",
    "tier",
    "harness_session_id",
    "client_key",
    "cwd",
    "project",
    "current_work",
    "presence",
    "paused",
    "paused_by",
    "callback_url",
    "last_heartbeat",
    "created_at",
    "transport",
    "metadata_json",
    "agent_id",
];

fn staged_raw_image(row: &libsql::Row) -> Result<[libsql::Value; 20], NexusError> {
    let mut raw = std::array::from_fn(|_| libsql::Value::Null);
    for (index, value) in raw.iter_mut().enumerate() {
        *value = row.get_value(index as i32).map_err(store_err)?;
    }
    Ok(raw)
}

fn capture_staged_row(row: &libsql::Row) -> Result<CapturedStagedSession, NexusError> {
    Ok(CapturedStagedSession {
        raw: staged_raw_image(row)?,
        row: row_to_session(row)?,
    })
}

fn same_staged_image(left: &[libsql::Value; 20], right: &[libsql::Value; 20]) -> bool {
    left.iter()
        .zip(right)
        .all(|(left, right)| match (left, right) {
            (libsql::Value::Real(left), libsql::Value::Real(right)) => {
                left.to_bits() == right.to_bits()
            }
            _ => left == right,
        })
}

async fn select_staged_row(
    txn: &WriteTxn,
    id: &libsql::Value,
) -> Result<Option<libsql::Row>, NexusError> {
    let mut rows = txn
        .query(
            &format!(
                "SELECT {} FROM sessions WHERE session_id=?1",
                CAPTURED_SESSION_COLUMNS.join(", ")
            ),
            params![id.clone()],
        )
        .await?;
    rows.next().await.map_err(store_err)
}

async fn selected_transport_session_matches(
    txn: &WriteTxn,
    selected: &SessionRow,
) -> Result<bool, NexusError> {
    let row = select_staged_row(txn, &libsql::Value::Text(selected.session_id.0.clone())).await?;
    match row {
        Some(row) => Ok(row_to_session(&row)? == *selected),
        None => Ok(false),
    }
}

async fn best_effort_transport_delete(
    txn: &WriteTxn,
    sql: &str,
    params: impl libsql::params::IntoParams,
) -> Result<(), NexusError> {
    if let Err(cause) = txn.execute(sql, params).await {
        // Ordinary child failures retain legacy best effort. A trigger may instead have ended
        // the transaction: never execute the remaining cleanup outside its pinned transaction.
        let check = txn.query("SELECT 1", ()).await.map_err(|state| {
            NexusError::Store(format!(
                "{cause}; selected transport purge cannot continue child cleanup: {state}"
            ))
        })?;
        drop(check);
    }
    Ok(())
}

async fn commit_staged_result<T>(
    txn: WriteTxn,
    result: Result<T, NexusError>,
) -> Result<T, NexusError> {
    match result {
        Ok(value) => {
            // A commit error is uncertain: never return successful/advanced authority.
            txn.commit().await?;
            Ok(value)
        }
        Err(error) => {
            txn.rollback(&error).await?;
            Err(error)
        }
    }
}

/// Persistence for the `sessions` table.
pub struct Sessions<'a> {
    store: &'a Store,
}

enum PresenceLifecyclePolicy {
    Strict,
    BestEffort,
}

impl<'a> Sessions<'a> {
    /// Bind a repo to the shared store connection.
    pub fn new(store: &'a Store) -> Self {
        Sessions { store }
    }

    /// Stage and capture the actual persisted row in one transport write transaction.
    /// Projection decoding and raw capture precede commit; no `started` fact or signal is emitted.
    /// A receipt is returned only on confirmed commit. An error at commit may be ambiguous.
    pub async fn create_staged_registration_captured(
        &self,
        s: NewSession,
        metadata_json: Option<String>,
    ) -> Result<CapturedStagedSession, NexusError> {
        let txn = self
            .store
            .begin_write_txn("session_create_staged_captured")
            .await?;
        let result = async {
            let ts = now();
            let id = libsql::Value::Text(s.session_id.0.clone());
            let kind = canonical_kind(&s.kind)?;
            let changed = txn
                .execute(
                    "INSERT INTO sessions (session_id, name, agent, kind, role, tier, \
                 harness_session_id, client_key, cwd, project, presence, paused, created_at, \
                 transport, metadata_json) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 'online', 0, ?11, ?12, ?13)",
                    params![
                        s.session_id.0,
                        s.name,
                        s.agent,
                        kind,
                        s.role,
                        s.tier,
                        s.harness_session_id,
                        s.client_key,
                        s.cwd,
                        s.project,
                        ts,
                        s.transport,
                        metadata_json
                    ],
                )
                .await?;
            if changed != 1 {
                return Err(NexusError::Store(
                    "staged insertion did not insert exactly one row".into(),
                ));
            }
            let row = select_staged_row(&txn, &id)
                .await?
                .ok_or_else(|| NexusError::Store("staged insertion row missing".into()))?;
            capture_staged_row(&row)
        }
        .await;
        commit_staged_result(txn, result).await
    }

    /// Stamp only an exact same-store selection and return its advanced image after commit.
    /// Only `agent_id` may change. Precommit failure rolls back; commit errors are uncertain and
    /// must not be interpreted as permission to clean up by ID or to fabricate a new receipt.
    /// If the binding value does not change, the original image still matches; this is not an
    /// incarnation counter or an ABA guard.
    pub async fn set_agent_id_selected(
        &self,
        selected: &CapturedStagedSession,
        agent_id: &str,
    ) -> Result<SelectedStagedSessionStamp, NexusError> {
        let txn = self
            .store
            .begin_write_txn("session_set_agent_id_selected")
            .await?;
        let result = async {
            let Some(row) = select_staged_row(&txn, &selected.raw[0]).await? else {
                return Ok(SelectedStagedSessionStamp::SelectionChanged);
            };
            if !same_staged_image(&staged_raw_image(&row)?, &selected.raw) {
                return Ok(SelectedStagedSessionStamp::SelectionChanged);
            }
            let changed = txn
                .execute(
                    "UPDATE sessions SET agent_id=?2 WHERE session_id=?1",
                    params![selected.raw[0].clone(), agent_id],
                )
                .await?;
            if changed != 1 {
                return Err(NexusError::Store(
                    "selected staged stamp did not update exactly one row".into(),
                ));
            }
            let row = select_staged_row(&txn, &selected.raw[0])
                .await?
                .ok_or_else(|| {
                    NexusError::Store("selected staged stamp row missing after update".into())
                })?;
            let mut expected = selected.raw.clone();
            expected[19] = libsql::Value::Text(agent_id.into());
            if !same_staged_image(&staged_raw_image(&row)?, &expected) {
                return Err(NexusError::Store(
                    "selected staged stamp changed unexpected fields".into(),
                ));
            }
            Ok(SelectedStagedSessionStamp::Updated(capture_staged_row(
                &row,
            )?))
        }
        .await;
        commit_staged_result(txn, result).await
    }

    /// Remove only an exact same-store selection, signaling a change only after confirmed commit.
    /// No lifecycle fact is appended. Commit failure returns uncertainty, not successful cleanup;
    /// this API does not authorize generated-agent cleanup or mutations in another authority.
    pub async fn remove_staged_registration_selected(
        &self,
        selected: &CapturedStagedSession,
    ) -> Result<SelectedStagedSessionCleanup, NexusError> {
        let txn = self
            .store
            .begin_write_txn("session_remove_staged_selected")
            .await?;
        let result = async {
            let Some(row) = select_staged_row(&txn, &selected.raw[0]).await? else {
                return Ok(SelectedStagedSessionCleanup::AlreadyAbsent);
            };
            if !same_staged_image(&staged_raw_image(&row)?, &selected.raw) {
                return Ok(SelectedStagedSessionCleanup::SelectionChanged);
            }
            let changed = txn
                .execute(
                    "DELETE FROM sessions WHERE session_id=?1",
                    params![selected.raw[0].clone()],
                )
                .await?;
            if changed != 1 {
                return Err(NexusError::Store(
                    "selected staged cleanup did not delete exactly one row".into(),
                ));
            }
            Ok(SelectedStagedSessionCleanup::Removed)
        }
        .await;
        let outcome = commit_staged_result(txn, result).await?;
        if matches!(outcome, SelectedStagedSessionCleanup::Removed) {
            self.store.events().session_lifecycle_changed().signal();
        }
        Ok(outcome)
    }

    /// Insert a new session row (presence `online`, not paused, `created_at = now()`).
    pub async fn create(&self, s: NewSession) -> Result<SessionId, NexusError> {
        let is_agent = is_agent_kind(&s.kind)?;
        let session_id = self.insert(s.clone(), None).await?;
        let ts = now();
        if is_agent {
            if let Some(name) = s.name.as_deref() {
                self.append_lifecycle(name, &session_id, "started", None, ts)
                    .await?;
            }
        }
        self.store.events().session_lifecycle_changed().signal();
        Ok(session_id)
    }

    /// Insert the compatibility row for a registration without publishing lifecycle truth yet.
    ///
    /// Identity registration spans the transport and identity authorities in split-store mode.
    /// The service stages this row, binds the stable runtime, and only then calls
    /// [`finalize_staged_registration`](Self::finalize_staged_registration). If binding fails it
    /// removes the invisible row with
    /// [`remove_staged_registration`](Self::remove_staged_registration), so readers never receive
    /// a `started` fact for an identity that did not finish registering.
    pub async fn create_staged_registration(&self, s: NewSession) -> Result<SessionId, NexusError> {
        self.insert(s, None).await
    }

    /// Insert a staged registration with the caller's parallel identity facets already durable.
    pub async fn create_staged_registration_with_metadata(
        &self,
        s: NewSession,
        metadata_json: Option<String>,
    ) -> Result<SessionId, NexusError> {
        self.insert(s, metadata_json).await
    }

    /// Publish the compatibility lifecycle fact for a fully-bound staged registration.
    pub async fn finalize_staged_registration(&self, row: &SessionRow) -> Result<(), NexusError> {
        let result = if row.is_agent() {
            match row.name.as_deref() {
                Some(name) => {
                    self.append_lifecycle(name, &row.session_id, "started", None, now())
                        .await
                }
                None => Ok(()),
            }
        } else {
            Ok(())
        };
        self.store.events().session_lifecycle_changed().signal();
        result
    }

    /// Remove a registration row that never crossed the stable identity/runtime boundary.
    pub async fn remove_staged_registration(&self, session: &SessionId) -> Result<(), NexusError> {
        self.store
            .conn
            .execute(
                "DELETE FROM sessions WHERE session_id = ?1",
                params![session.0.clone()],
            )
            .await
            .map_err(store_err)?;
        self.store.events().session_lifecycle_changed().signal();
        Ok(())
    }

    async fn insert(
        &self,
        s: NewSession,
        metadata_json: Option<String>,
    ) -> Result<SessionId, NexusError> {
        let ts = now();
        let session_id = s.session_id.clone();
        let kind = canonical_kind(&s.kind)?;
        self.store
            .conn
            .execute(
                "INSERT INTO sessions (session_id, name, agent, kind, role, tier, \
                 harness_session_id, client_key, cwd, project, presence, paused, created_at, \
                 transport, metadata_json) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 'online', 0, ?11, ?12, ?13)",
                params![
                    s.session_id.0.clone(),
                    s.name,
                    s.agent,
                    kind,
                    s.role,
                    s.tier,
                    s.harness_session_id,
                    s.client_key,
                    s.cwd,
                    s.project,
                    ts,
                    s.transport,
                    metadata_json
                ],
            )
            .await
            .map_err(store_err)?;
        Ok(session_id)
    }

    /// Find a session by its idempotency `client_key` (the resume path).
    pub async fn find_by_client_key(
        &self,
        project: &str,
        client_key: &str,
    ) -> Result<Option<SessionRow>, NexusError> {
        self.query_one(
            "WHERE project = ?1 AND client_key = ?2",
            params![project, client_key],
        )
        .await
    }

    /// Find a session by its globally unique client key. Project is descriptive metadata and must
    /// not participate in caller authentication or transport routing.
    pub async fn find_by_client_key_any_project(
        &self,
        client_key: &str,
    ) -> Result<Option<SessionRow>, NexusError> {
        self.query_one("WHERE client_key = ?1", params![client_key])
            .await
    }

    /// Find a session by its unique `name`, scoped to the project.
    pub async fn find_by_name(
        &self,
        project: &str,
        name: &str,
    ) -> Result<Option<SessionRow>, NexusError> {
        self.query_one("WHERE project = ?1 AND name = ?2", params![project, name])
            .await
    }

    /// Update a session's presence. Takes the [`Presence`] enum and serializes it to the stored
    /// lowercase token (`online`|`busy`|`offline`) via the canonical
    /// [`nexus_common::presence::presence_token`].
    pub async fn set_presence(
        &self,
        session: &SessionId,
        presence: Presence,
    ) -> Result<(), NexusError> {
        self.set_presence_with_lifecycle_policy(session, presence, PresenceLifecyclePolicy::Strict)
            .await
    }

    /// Update required presence state while treating only its lifecycle append as best-effort.
    ///
    /// Lookup and UPDATE errors remain load-bearing. After a successful UPDATE, an append error
    /// is warned and the lifecycle-change signal is still sent. This does not add a transaction,
    /// retry, or an exactly-once guarantee; callers choose whether this telemetry policy fits.
    pub async fn set_presence_with_best_effort_lifecycle(
        &self,
        session: &SessionId,
        presence: Presence,
    ) -> Result<(), NexusError> {
        self.set_presence_with_lifecycle_policy(
            session,
            presence,
            PresenceLifecyclePolicy::BestEffort,
        )
        .await
    }

    async fn set_presence_with_lifecycle_policy(
        &self,
        session: &SessionId,
        presence: Presence,
        lifecycle_policy: PresenceLifecyclePolicy,
    ) -> Result<(), NexusError> {
        let before = self.find_by_session_id(session).await?;
        self.store
            .conn
            .execute(
                "UPDATE sessions SET presence = ?2 WHERE session_id = ?1",
                params![session.0.clone(), presence_token(presence)],
            )
            .await
            .map_err(store_err)?;
        if let Some(row) = before.as_ref() {
            if let Some(lifecycle) = lifecycle_for_presence_transition(
                row.presence.as_deref().unwrap_or("offline"),
                presence_token(presence),
            ) {
                if let Err(error) = self
                    .append_lifecycle(&row.display_name(), session, lifecycle, None, now())
                    .await
                {
                    match lifecycle_policy {
                        PresenceLifecyclePolicy::Strict => return Err(error),
                        PresenceLifecyclePolicy::BestEffort => tracing::warn!(
                            target: "nexus::presence",
                            session = %session,
                            error = %error,
                            "failed best-effort Session presence lifecycle append"
                        ),
                    }
                }
            }
        }
        self.store.events().session_lifecycle_changed().signal();
        Ok(())
    }

    /// Mark one previously selected compatibility session offline only if its transport-local
    /// identity and staleness still match.
    ///
    /// Selection and the conditional mutation share one transaction on the transport store;
    /// `expected_agent_id = None` matches SQL `NULL` exactly and is not a wildcard. Only presence
    /// changes. The transaction commits before the lifecycle append because that append opens its
    /// own transport transaction. Consequently an append failure is returned after the offline
    /// row is already durable and must not be interpreted as a retry-safe no-effect result.
    /// Attribution comes from the row captured in the transaction, without a post-commit reread.
    /// This boundary does not claim protection from same-session/same-agent ABA before entry or
    /// coordinate a later transition in another store authority.
    pub async fn set_offline_if_stale_selected(
        &self,
        session: &SessionId,
        expected_agent_id: Option<&str>,
        now_ms: i64,
        ttl_ms: i64,
    ) -> Result<bool, NexusError> {
        let txn = self
            .store
            .begin_write_txn("session_set_offline_if_stale_selected")
            .await?;
        let result = async {
            let identity_matches = "(agent_id = ?2 OR (agent_id IS NULL AND ?2 IS NULL))";
            let stale = "COALESCE(presence, 'offline') <> 'offline' \
                         AND ?3 - COALESCE(last_heartbeat, created_at) > ?4";
            let mut rows = txn
                .query(
                    &format!("{SELECT} WHERE session_id = ?1 AND {identity_matches} AND {stale}"),
                    params![session.0.clone(), expected_agent_id, now_ms, ttl_ms],
                )
                .await?;
            let selected = match rows.next().await.map_err(store_err)? {
                Some(row) => Some(row_to_session(&row)?),
                None => None,
            };
            drop(rows);
            let Some(selected) = selected else {
                return Ok(None);
            };
            let changed = txn
                .execute(
                    &format!(
                        "UPDATE sessions SET presence = 'offline' \
                         WHERE session_id = ?1 AND {identity_matches} AND {stale}"
                    ),
                    params![session.0.clone(), expected_agent_id, now_ms, ttl_ms],
                )
                .await?;
            Ok((changed == 1).then_some(selected))
        }
        .await;

        let selected = match result {
            Ok(selected) => {
                txn.commit().await?;
                selected
            }
            Err(error) => {
                txn.rollback(&error).await?;
                return Err(error);
            }
        };
        let Some(selected) = selected else {
            return Ok(false);
        };
        self.append_lifecycle(
            &selected.display_name(),
            &selected.session_id,
            "offline",
            None,
            now(),
        )
        .await?;
        self.store.events().session_lifecycle_changed().signal();
        Ok(true)
    }

    /// Update the operator-visible work-state field and emit a metadata-only lifecycle event.
    ///
    /// `current_work` is the developer event counterpart to `nexus status --work`: tooling can
    /// watch `sys.agent.lifecycle` instead of polling roster rows, while no agent turn is woken.
    pub async fn set_current_work(
        &self,
        session: &SessionId,
        work: Option<&str>,
    ) -> Result<(), NexusError> {
        let before = self
            .find_by_session_id(session)
            .await?
            .ok_or_else(|| NexusError::NotFound(session.0.clone()))?;
        if before.current_work.as_deref() == work {
            return Ok(());
        }
        self.store
            .conn
            .execute(
                "UPDATE sessions SET current_work = ?2 WHERE session_id = ?1",
                params![session.0.clone(), work],
            )
            .await
            .map_err(store_err)?;
        self.append_lifecycle(&before.display_name(), session, "current_work", work, now())
            .await?;
        self.store.events().session_lifecycle_changed().signal();
        Ok(())
    }

    /// Update a session's durable transport label (`'pty'` | `'acp'`).
    pub async fn set_transport(
        &self,
        session: &SessionId,
        transport: &str,
    ) -> Result<(), NexusError> {
        self.store
            .conn
            .execute(
                "UPDATE sessions SET transport = ?2 WHERE session_id = ?1",
                params![session.0.clone(), transport],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Stamp the stable durable identity backing this compatibility/live session row.
    ///
    /// Launch/revive paths resolve an `agent_id` at the edge, then persist that id on `sessions`
    /// before the row can be used for later attach/revive reads. Fossil rows are intentionally left
    /// `NULL` until a path has resolved the identity explicitly.
    pub async fn set_agent_id(
        &self,
        session: &SessionId,
        agent_id: &str,
    ) -> Result<(), NexusError> {
        self.store
            .conn
            .execute(
                "UPDATE sessions SET agent_id = ?2 WHERE session_id = ?1",
                params![session.0.clone(), agent_id],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Reclaim an existing offline identity row for a daemon-owned launch that minted a new
    /// `session_id`. This preserves the display name and pending inbox while making `members` and
    /// `resolve(name)` point at the newly bound transport session.
    pub async fn rebind_name_to_session(&self, s: NewSession) -> Result<SessionId, NexusError> {
        let new_session_id = s.session_id.0.clone();
        let event_session_id = SessionId(new_session_id.clone());
        let is_agent = is_agent_kind(&s.kind)?;
        let kind = canonical_kind(&s.kind)?;
        let name = s
            .name
            .clone()
            .ok_or_else(|| NexusError::Invalid("cannot rebind an unnamed session".into()))?;
        let existing = self
            .find_by_name(&s.project, &name)
            .await?
            .ok_or_else(|| NexusError::NotFound(name.clone()))?;
        let ts = now();
        self.store
            .conn
            .execute(
                "UPDATE sessions SET session_id = ?1, agent = ?2, kind = ?3, role = ?4, \
                 tier = ?5, harness_session_id = ?6, client_key = ?7, cwd = ?8, project = ?9, \
                 presence = 'online', paused = 0, paused_by = NULL, last_heartbeat = ?10, \
                 transport = ?11 WHERE session_id = ?12",
                params![
                    new_session_id.clone(),
                    s.agent,
                    kind,
                    s.role,
                    s.tier,
                    s.harness_session_id,
                    s.client_key,
                    s.cwd,
                    s.project,
                    ts,
                    s.transport,
                    existing.session_id.0.clone()
                ],
            )
            .await
            .map_err(store_err)?;
        self.store
            .conn
            .execute(
                "UPDATE in_flight SET recipient_session = ?1 WHERE recipient_session = ?2",
                params![new_session_id, existing.session_id.0.clone()],
            )
            .await
            .map_err(store_err)?;
        if is_agent {
            self.append_lifecycle(&name, &event_session_id, "started", None, ts)
                .await?;
        }
        self.store.events().session_lifecycle_changed().signal();
        Ok(existing.session_id)
    }

    /// Persist the harness-native resume key. For ACP this is the ACP `session/load` id; for headed
    /// Codex app-server sessions this is the Codex thread id discovered from rollout metadata.
    pub async fn set_harness_session_id(
        &self,
        session: &SessionId,
        harness_session_id: &str,
    ) -> Result<(), NexusError> {
        self.store
            .conn
            .execute(
                "UPDATE sessions SET harness_session_id = ?2 WHERE session_id = ?1",
                params![session.0.clone(), harness_session_id],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Clear the harness-native resume key. `admin.remove` uses this to explicitly release native
    /// resume ownership (for example a headed Codex thread id) while retaining the compatibility
    /// session row and message history.
    pub async fn clear_harness_session_id(&self, session: &SessionId) -> Result<(), NexusError> {
        self.store
            .conn
            .execute(
                "UPDATE sessions SET harness_session_id = NULL WHERE session_id = ?1",
                params![session.0.clone()],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Clear a session's client key. Credential revocation uses this to make an already-running
    /// runtime fail its next authenticated command without deleting its history.
    pub async fn clear_client_key(&self, session: &SessionId) -> Result<(), NexusError> {
        self.store
            .conn
            .execute(
                "UPDATE sessions SET client_key = NULL WHERE session_id = ?1",
                params![session.0.clone()],
            )
            .await
            .map_err(store_err)?;
        if self.store.has_split_authority() {
            self.store
                .identity_conn()
                .execute(
                    "UPDATE identity_sessions SET client_key = NULL WHERE runtime_id = ?1",
                    params![session.0.clone()],
                )
                .await
                .map_err(store_err)?;
        }
        Ok(())
    }

    /// Rebind a session's display name (the `rename` op). The daemon is the sole writer; the caller
    /// guards name uniqueness in the project before calling.
    pub async fn set_name(&self, session: &SessionId, name: &str) -> Result<(), NexusError> {
        self.store
            .conn
            .execute(
                "UPDATE sessions SET name = ?2 WHERE session_id = ?1",
                params![session.0.clone(), name],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Set the operator-facing role label on the compatibility session row.
    ///
    /// Durable identity reads use `agents.role`; member/whoami compatibility views use
    /// `sessions.role`, so admin role assignment keeps both columns aligned.
    pub async fn set_role(&self, session: &SessionId, role: &str) -> Result<(), NexusError> {
        self.set_role_value(session, Some(role)).await
    }

    /// Set or clear the compatibility display role.
    ///
    /// The nullable form lets cross-database callers compensate to the exact prior state.
    pub async fn set_role_value(
        &self,
        session: &SessionId,
        role: Option<&str>,
    ) -> Result<(), NexusError> {
        self.store
            .conn
            .execute(
                "UPDATE sessions SET role = ?2 WHERE session_id = ?1",
                params![session.0.clone(), role],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Move one exact compatibility/live session row to another project.
    ///
    /// Id-aware admin assignment paths use the selected `session_id` instead of re-reading by name,
    /// which keeps rename/name-reuse cases from moving the wrong row.
    pub async fn set_project(&self, session: &SessionId, project: &str) -> Result<(), NexusError> {
        self.store
            .conn
            .execute(
                "UPDATE sessions SET project = ?2 WHERE session_id = ?1",
                params![session.0.clone(), project],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Set the authorization tier on a compatibility/live session row.
    ///
    /// Durable grants update `agents.tier` and this row so existing runtimes resolve the new
    /// privilege ceiling on their next command without requiring a re-register.
    pub async fn set_tier(&self, session: &SessionId, tier: &str) -> Result<(), NexusError> {
        self.store
            .conn
            .execute(
                "UPDATE sessions SET tier = ?2 WHERE session_id = ?1",
                params![session.0.clone(), tier],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// PURGE — erase a durable agent identity entirely (the `delete` op): the compatibility
    /// session row, stable runtime/credential rows, in-flight rows, thread memberships, ACL grant
    /// edges, and every message it sent or received. Caller kills the live process first.
    /// Identity retirement, ACL, runtime, credential, and agent deletion commit atomically;
    /// invalid model authority or retirement failure aborts that identity transaction. Transport
    /// cleanup is separate and best-effort. A missing row is not an error.
    ///
    /// Legacy callers may only have `session_id` and the requested name, so this wrapper derives the
    /// stable id from the selected session/runtime before falling back to a name lookup.
    pub async fn purge(&self, session: &SessionId, name: &str) -> Result<(), NexusError> {
        let session_row = self.find_by_session_id(session).await?;
        let runtime = AgentRuntimes::new(self.store)
            .find_by_runtime_id(&session.0)
            .await?;
        let agent = Agents::new(self.store).find_by_name(name).await?;
        let agent_id = session_row
            .as_ref()
            .and_then(|row| row.agent_id.clone())
            .or_else(|| runtime.as_ref().map(|row| row.agent_id.clone()))
            .or_else(|| agent.as_ref().map(|row| row.agent_id.clone()));
        let project = session_row
            .as_ref()
            .map(|row| row.project.clone())
            .or_else(|| agent.as_ref().map(|row| row.project.clone()));
        self.purge_exact(
            session,
            project.as_deref().unwrap_or_default(),
            Some(name),
            agent_id.as_deref(),
        )
        .await
    }

    /// Purge using an already-selected session row.
    ///
    /// Id-aware admin delete paths call this after resolving the target row once. The row's own
    /// `agent_id`, project, name, and session id remain the deletion keys, so a stale request name
    /// cannot cause the purge to delete a different durable identity that now owns that name.
    pub async fn purge_selected(&self, row: &SessionRow) -> Result<(), NexusError> {
        let agent_id = match row.agent_id.as_deref() {
            Some(agent_id) => Some(agent_id.to_string()),
            None => match row.name.as_deref() {
                Some(name) => {
                    self.agent_id_for_selected_session(&row.session_id, &row.project, name)
                        .await?
                }
                None => None,
            },
        };
        self.purge_exact(
            &row.session_id,
            &row.project,
            row.name.as_deref(),
            agent_id.as_deref(),
        )
        .await
    }

    async fn agent_id_for_selected_session(
        &self,
        session: &SessionId,
        project: &str,
        name: &str,
    ) -> Result<Option<String>, NexusError> {
        if let Some(runtime) = AgentRuntimes::new(self.store)
            .find_by_runtime_id(&session.0)
            .await?
        {
            return Ok(Some(runtime.agent_id));
        }
        Ok(Agents::new(self.store)
            .find_by_project_name(project, name)
            .await?
            .map(|agent| agent.agent_id))
    }

    /// Capture the full identity purge predicate; this read alone authorizes no deletion.
    pub async fn runtime_pairs_for_purge(
        &self,
        session: &SessionId,
        agent_id: Option<&str>,
    ) -> Result<Vec<(String, String)>, NexusError> {
        let mut rows = self.store.identity_conn().query(
            "SELECT runtime_id,agent_id FROM agent_runtimes WHERE (?1 IS NOT NULL AND agent_id=?1) OR runtime_id=?2 ORDER BY runtime_id,agent_id",
            params![agent_id, session.0.as_str()],
        ).await.map_err(store_err)?;
        let mut pairs = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            pairs.push((get_text(&row, 0)?, get_text(&row, 1)?));
        }
        Ok(pairs)
    }

    /// Delete only the selected identity-store predicate after matching its entire runtime set.
    ///
    /// The caller must authorize this row's session, stable agent and name-based ACL predicates
    /// under the same Store's managed lifecycle exclusion. This method provides neither Session
    /// binding validation, native teardown, transport cleanup nor same-value incarnation proof.
    /// Empty selection is exact, not a wildcard. A confirmed receipt must be retained before any
    /// subsequent fallible transport work; do not call legacy purge as its tail.
    pub async fn purge_identity_selected(
        &self,
        row: &SessionRow,
        captured_agent_id: Option<&str>,
        expected_pairs: &[(String, String)],
    ) -> Result<SelectedIdentityPurge, IdentityPurgeFailure> {
        let mut pairs = expected_pairs.to_vec();
        pairs.sort();
        pairs.dedup();
        self.purge_identity(
            &row.session_id,
            &row.project,
            row.name.as_deref(),
            captured_agent_id,
            Some(&pairs),
        )
        .await
    }

    async fn purge_identity(
        &self,
        session: &SessionId,
        project: &str,
        name: Option<&str>,
        agent_id: Option<&str>,
        expected_pairs: Option<&[(String, String)]>,
    ) -> Result<SelectedIdentityPurge, IdentityPurgeFailure> {
        let tx = self
            .store
            .begin_identity_write_txn("session_purge_identity")
            .await
            .map_err(|cause| IdentityPurgeFailure {
                cause,
                commit_state: IdentityPurgeCommitState::NotCommitted,
            })?;
        let result = async {
            let mut selection = tx.query(
                "SELECT runtime_id,agent_id FROM agent_runtimes WHERE (?1 IS NOT NULL AND agent_id=?1) OR runtime_id=?2 ORDER BY runtime_id,agent_id",
                params![agent_id, session.0.as_str()],
            ).await?;
            let mut actual = Vec::new();
            while let Some(row) = selection.next().await.map_err(store_err)? {
                actual.push((get_text(&row, 0)?, get_text(&row, 1)?));
            }
            drop(selection);
            if expected_pairs.is_some_and(|expected| expected != actual) {
                return Ok(SelectedIdentityPurge::SelectionChanged);
            }
            // Use the deletion predicate inside the writer transaction: stopped siblings and the
            // exact-runtime fallback must retire alongside the selected stable identity. Reuse
            // the runtime decoder so malformed authority fails closed, but malformed JSON alone
            // never prevents erasing the observation payload.
            let mut rows = tx
                .query(
                    "SELECT runtime_id, agent_id, harness, cwd, transport, presence, active, \
                 started_at, stopped_at, last_heartbeat, os_pid, os_pgid, model_observer_token, \
                 model_observer_sequence, model_report_revision, model_report_json \
                 FROM agent_runtimes WHERE (?1 IS NOT NULL AND agent_id = ?1) OR runtime_id = ?2",
                    params![agent_id, session.0.as_str()],
                )
                .await?;
            let mut retired_ids = Vec::new();
            while let Some(row) = rows.next().await.map_err(store_err)? {
                let runtime = crate::repos::agent_runtimes::row_to_runtime(&row)?;
                if runtime.model_report_revision > 0 {
                    retired_ids.push(runtime.runtime_id);
                }
            }
            drop(rows);
            for runtime_id in retired_ids {
                tx.execute(
                    "INSERT INTO retired_model_runtime_ids(runtime_id) VALUES (?1) \
                     ON CONFLICT(runtime_id) DO NOTHING",
                    params![runtime_id.as_str()],
                )
                .await?;
                // A silently ignored insert is not retirement. Existing guards are idempotent.
                let mut guard = tx
                    .query(
                        "SELECT 1 FROM retired_model_runtime_ids WHERE runtime_id = ?1",
                        params![runtime_id.as_str()],
                    )
                    .await?;
                if guard.next().await.map_err(store_err)?.is_none() {
                    return Err(NexusError::Store(
                        "runtime identity retirement did not persist".into(),
                    ));
                }
            }
            // Keep AgentAccessGrants::purge_deleted_agent's exact actor/target predicate, but
            // execute it here so a failed retirement cannot destructively change the ACL graph.
            tx.execute(
                "DELETE FROM agent_acl_grants \
                 WHERE (?1 IS NOT NULL AND agent_id = ?1) \
                    OR (?3 IS NOT NULL AND principal_project = ?2 AND principal_name = ?3) \
                    OR (?3 IS NOT NULL AND granted_by_project = ?2 AND granted_by_name = ?3) \
                    OR (?1 IS NOT NULL AND principal_agent_id = ?1)",
                params![agent_id, project, name],
            )
            .await?;
            tx.execute(
                "DELETE FROM agent_credentials WHERE ?1 IS NOT NULL AND agent_id = ?1",
                params![agent_id],
            )
            .await?;
            let deleted = tx.execute(
                "DELETE FROM agent_runtimes WHERE (?1 IS NOT NULL AND agent_id = ?1) \
                 OR runtime_id = ?2",
                params![agent_id, session.0.as_str()],
            )
            .await?;
            if expected_pairs.is_some() && deleted != actual.len() as u64 {
                return Err(NexusError::Store("selected purge runtime deletion count mismatch".into()));
            }
            tx.execute(
                "DELETE FROM agents WHERE (?1 IS NOT NULL AND agent_id = ?1) \
                 OR (?4 AND ?1 IS NULL AND ?3 IS NOT NULL AND project = ?2 AND name = ?3)",
                params![agent_id, project, name, expected_pairs.is_none()],
            )
            .await?;
            // Verify the final transaction image after all identity writes and their triggers.
            if expected_pairs.is_some() {
                let mut remaining = tx.query(
                    "SELECT 1 FROM agent_runtimes WHERE (?1 IS NOT NULL AND agent_id=?1) OR runtime_id=?2 LIMIT 1",
                    params![agent_id, session.0.as_str()],
                ).await?;
                if remaining.next().await.map_err(store_err)?.is_some() {
                    return Err(NexusError::Store("selected purge runtime deletion left rows".into()));
                }
                drop(remaining);
                // A trigger could recreate a sibling ID under another agent, outside the original
                // predicate. The receipt must not certify that captured ID as absent either.
                for (runtime_id, _) in &actual {
                    let mut remaining = tx.query(
                        "SELECT 1 FROM agent_runtimes WHERE runtime_id=?1",
                        params![runtime_id.as_str()],
                    ).await?;
                    if remaining.next().await.map_err(store_err)?.is_some() {
                        return Err(NexusError::Store("selected purge runtime deletion left rows".into()));
                    }
                }
                let mut remaining = tx.query(
                    "SELECT 1 FROM agents WHERE ?1 IS NOT NULL AND agent_id=?1
                     UNION ALL SELECT 1 FROM agent_credentials WHERE ?1 IS NOT NULL AND agent_id=?1
                     UNION ALL SELECT 1 FROM agent_acl_grants WHERE
                       (?1 IS NOT NULL AND agent_id=?1)
                       OR (?3 IS NOT NULL AND principal_project=?2 AND principal_name=?3)
                       OR (?3 IS NOT NULL AND granted_by_project=?2 AND granted_by_name=?3)
                       OR (?1 IS NOT NULL AND principal_agent_id=?1) LIMIT 1",
                    params![agent_id, project, name],
                ).await?;
                if remaining.next().await.map_err(store_err)?.is_some() {
                    return Err(NexusError::Store("selected purge identity deletion left rows".into()));
                }
            }
            Ok(SelectedIdentityPurge::Purged(IdentityPurgeReceipt {
                session_id: session.clone(), agent_id: agent_id.map(str::to_owned), runtime_pairs: actual,
                project: project.to_owned(), name: name.map(str::to_owned),
            }))
        }
        .await;
        match result {
            Ok(outcome) => {
                tx.commit().await.map_err(|cause| IdentityPurgeFailure {
                    cause,
                    commit_state: IdentityPurgeCommitState::Unknown,
                })?;
                Ok(outcome)
            }
            Err(cause) => Err(match tx.rollback_confirmed(&cause).await {
                Ok(true) => IdentityPurgeFailure {
                    cause,
                    commit_state: IdentityPurgeCommitState::NotCommitted,
                },
                Ok(false) => IdentityPurgeFailure {
                    cause,
                    commit_state: IdentityPurgeCommitState::Unknown,
                },
                Err(cause) => IdentityPurgeFailure {
                    cause,
                    commit_state: IdentityPurgeCommitState::Unknown,
                },
            }),
        }
    }

    /// Continue a confirmed identity purge without repeating any identity-store effects.
    ///
    /// Caller retains the same Store's managed exclusion and the original normalized Session
    /// selection. This is not authentication, raw-image equality, or incarnation authority.
    /// Every error/veto follows an already committed identity phase; lifecycle append may also
    /// fail after transport commit. Preserve the identity receipt when reporting that partial
    /// outcome. Missing Session is a veto, not permission to delete by a possibly reused name.
    pub async fn purge_transport_selected(
        &self,
        selected: &SessionRow,
        receipt: &IdentityPurgeReceipt,
    ) -> Result<SelectedTransportPurge, NexusError> {
        let txn = self
            .store
            .begin_write_txn("session_purge_transport_selected")
            .await?;
        let result = async {
            if receipt.session_id() != &selected.session_id || receipt.project() != selected.project
                || receipt.name() != selected.name.as_deref()
                || !selected_transport_session_matches(&txn, selected).await? {
                return Ok(SelectedTransportPurge::SelectionChanged);
            }
            // The authorized runtime fallback agent is distinct from a nullable Session stamp.
            best_effort_transport_delete(&txn,
                "DELETE FROM in_flight WHERE recipient_session=?1 OR (?2 IS NOT NULL AND recipient_agent_id=?2)",
                params![receipt.session_id().0.as_str(), receipt.agent_id()]).await?;
            best_effort_transport_delete(&txn,
                "DELETE FROM thread_members WHERE (?1 IS NOT NULL AND session_name=?1) OR (?2 IS NOT NULL AND agent_id=?2)",
                params![receipt.name(), receipt.agent_id()]).await?;
            best_effort_transport_delete(&txn,
                "DELETE FROM messages WHERE (?1 IS NOT NULL AND (from_name=?1 OR to_name=?1)) OR (?2 IS NOT NULL AND (from_agent_id=?2 OR to_agent_id=?2))",
                params![receipt.name(), receipt.agent_id()]).await?;
            if !selected_transport_session_matches(&txn, selected).await? {
                return Err(NexusError::Store("selected transport purge Session changed during child cleanup".into()));
            }
            let changed = txn.execute("DELETE FROM sessions WHERE session_id=?1", params![selected.session_id.0.as_str()]).await?;
            if changed != 1 {
                return Err(NexusError::Store("selected transport purge did not delete exactly one Session".into()));
            }
            if select_staged_row(&txn, &libsql::Value::Text(selected.session_id.0.clone())).await?.is_some() {
                return Err(NexusError::Store("selected transport purge left a Session row".into()));
            }
            Ok(SelectedTransportPurge::Purged)
        }.await;
        let outcome = commit_staged_result(txn, result).await?;
        if outcome == SelectedTransportPurge::Purged {
            if let Some(name) = receipt.name() {
                self.append_lifecycle(name, receipt.session_id(), "stopped", None, now())
                    .await?;
            }
            self.store.events().session_lifecycle_changed().signal();
        }
        Ok(outcome)
    }

    async fn purge_exact(
        &self,
        session: &SessionId,
        project: &str,
        name: Option<&str>,
        agent_id: Option<&str>,
    ) -> Result<(), NexusError> {
        self.purge_identity(session, project, name, agent_id, None)
            .await
            .map_err(|failure| failure.cause)?;
        let conn = &self.store.conn;
        // Transport has a separate authority in split stores; do not imply cross-DB rollback.
        let _ = conn
            .execute(
                "DELETE FROM in_flight WHERE recipient_session = ?1 \
                 OR (?2 IS NOT NULL AND recipient_agent_id = ?2)",
                params![session.0.clone(), agent_id],
            )
            .await;
        let _ = conn
            .execute(
                "DELETE FROM thread_members WHERE (?1 IS NOT NULL AND session_name = ?1) \
                 OR (?2 IS NOT NULL AND agent_id = ?2)",
                params![name, agent_id],
            )
            .await;
        let _ = conn
            .execute(
                "DELETE FROM messages WHERE (?1 IS NOT NULL AND (from_name = ?1 OR to_name = ?1)) \
                 OR (?2 IS NOT NULL AND (from_agent_id = ?2 OR to_agent_id = ?2))",
                params![name, agent_id],
            )
            .await;
        conn.execute(
            "DELETE FROM sessions WHERE session_id = ?1",
            params![session.0.clone()],
        )
        .await
        .map_err(store_err)?;
        if let Some(name) = name {
            self.append_lifecycle(name, session, "stopped", None, now())
                .await?;
        }
        self.store.events().session_lifecycle_changed().signal();
        Ok(())
    }

    /// Set (or clear) the paused hold + its source (self vs admin).
    pub async fn set_paused(
        &self,
        session: &SessionId,
        paused: bool,
        paused_by: Option<&str>,
    ) -> Result<(), NexusError> {
        self.store
            .conn
            .execute(
                "UPDATE sessions SET paused = ?2, paused_by = ?3 WHERE session_id = ?1",
                params![session.0.clone(), paused as i64, paused_by],
            )
            .await
            .map_err(store_err)?;
        self.store.events().session_lifecycle_changed().signal();
        Ok(())
    }

    /// Refresh `last_heartbeat` to now.
    pub async fn touch_heartbeat(&self, session: &SessionId) -> Result<(), NexusError> {
        self.store
            .conn
            .execute(
                "UPDATE sessions SET last_heartbeat = ?2 WHERE session_id = ?1",
                params![session.0.clone(), now()],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Flip an `offline` (non-paused) session back to `online` on proven activity.
    ///
    /// The boot presume-dead reconcile marks every non-transport-backed session offline;
    /// this is the promised RETURN path — an authenticated command from the session proves it
    /// alive again. Without it the offline flip is sticky for CLI/MCP peers.
    pub async fn restore_online_on_activity(
        &self,
        session: &SessionId,
    ) -> Result<bool, NexusError> {
        let changed = self
            .store
            .conn
            .execute(
                "UPDATE sessions SET presence = 'online' \
                 WHERE session_id = ?1 AND COALESCE(presence, 'offline') = 'offline' \
                   AND paused = 0",
                params![session.0.clone()],
            )
            .await
            .map_err(store_err)?;
        if changed > 0 {
            self.store.events().session_lifecycle_changed().signal();
        }
        Ok(changed > 0)
    }

    /// Find a session by its unique `name` across **all** projects (LIMIT 1, insertion-order).
    /// Used by `assign_project` to locate the row before mutating its `project` column.
    pub async fn find_by_name_any_project(
        &self,
        name: &str,
    ) -> Result<Option<SessionRow>, NexusError> {
        self.query_one("WHERE name = ?1 LIMIT 1", params![name])
            .await
    }

    /// Resolve a legacy session-only name globally without guessing across duplicate metadata.
    /// Canonical durable-agent paths resolve through [`Agents`] first; this fallback exists only
    /// for fossils and non-agent principals that have no durable agent row.
    pub async fn find_unique_by_name_any_project(
        &self,
        name: &str,
    ) -> Result<Option<SessionRow>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                &format!("{SELECT} WHERE name = ?1 ORDER BY created_at LIMIT 2"),
                params![name],
            )
            .await
            .map_err(store_err)?;
        let Some(first) = rows.next().await.map_err(store_err)? else {
            return Ok(None);
        };
        let first = row_to_session(&first)?;
        if rows.next().await.map_err(store_err)?.is_some() {
            return Err(NexusError::Ambiguous(format!(
                "session name {name:?} matches multiple legacy identities; address a durable agent by a_* id"
            )));
        }
        Ok(Some(first))
    }

    /// Find a session by its `session_id` (any project). Used on boot re-spawn to resolve a
    /// pending recipient back to its `name`/`kind`/`project`.
    pub async fn find_by_session_id(
        &self,
        session: &SessionId,
    ) -> Result<Option<SessionRow>, NexusError> {
        self.query_one("WHERE session_id = ?1", params![session.0.clone()])
            .await
    }

    /// Find a session by its harness-native resume key across all projects.
    ///
    /// This is used by harnesses whose native session ids must be single-owner in Nexus, such as a
    /// headed Codex thread id. It deliberately returns insertion order so legacy duplicates can be
    /// detected and cleaned up by higher-level management tooling without changing this read.
    pub async fn find_by_harness_session_id(
        &self,
        project: &str,
        harness_session_id: &str,
    ) -> Result<Option<SessionRow>, NexusError> {
        self.query_one(
            "WHERE project = ?1 AND harness_session_id = ?2",
            params![project, harness_session_id],
        )
        .await
    }

    /// Find a session by its harness-native resume key across all projects.
    ///
    /// This is used by harnesses whose native session ids must be single-owner in Nexus, such as a
    /// headed Codex thread id. It deliberately returns insertion order so legacy duplicates can be
    /// detected and cleaned up by higher-level management tooling without changing this read.
    pub async fn find_by_harness_session_id_any_project(
        &self,
        harness_session_id: &str,
    ) -> Result<Vec<SessionRow>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                &format!("{SELECT} WHERE harness_session_id = ?1 ORDER BY created_at"),
                params![harness_session_id],
            )
            .await
            .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(row_to_session(&row)?);
        }
        Ok(out)
    }

    /// Find the newest compatibility session row bound to a durable identity.
    ///
    /// Identity-by-id slice 1: daemon paths that already hold an `agent_id` read the live/compat
    /// directory row without re-resolving the mutable name. Rows with NULL `agent_id` are fossils
    /// and are intentionally invisible to this read.
    pub async fn find_by_agent_id(&self, agent_id: &str) -> Result<Option<SessionRow>, NexusError> {
        self.query_one(
            "WHERE agent_id = ?1 ORDER BY created_at DESC LIMIT 1",
            params![agent_id],
        )
        .await
    }

    /// Resolve the session row backing an identity's ACTIVE runtime (`agent_runtimes.active = 1`).
    ///
    /// This is the revive/attach spine: runtime selection is keyed by `agent_id`, then the exact
    /// `runtime_id` picks the session row (`sessions.session_id == agent_runtimes.runtime_id`).
    /// Returns `None` when the identity has no active runtime or the runtime has no session row.
    pub async fn active_runtime_session_for_agent(
        &self,
        agent_id: &str,
    ) -> Result<Option<SessionRow>, NexusError> {
        let Some(runtime) = AgentRuntimes::new(self.store)
            .active_for_agent(agent_id)
            .await?
        else {
            return Ok(None);
        };
        let runtime_id = runtime.runtime_id;
        let Some(session) = self
            .find_by_session_id(&SessionId(runtime_id.clone()))
            .await?
        else {
            return Ok(None);
        };
        if session.agent_id.as_deref() != Some(agent_id) {
            return Err(NexusError::Invalid(format!(
                "active runtime {runtime_id} belongs to agent {agent_id}, but its session row belongs to {}",
                session.agent_id.as_deref().unwrap_or("<unbound>")
            )));
        }
        Ok(Some(session))
    }

    /// Check whether a session named `name` already exists in `project` (collision guard).
    pub async fn name_exists_in(&self, project: &str, name: &str) -> Result<bool, NexusError> {
        Ok(self.find_by_name(project, name).await?.is_some())
    }

    /// List every session in a project (the directory / presence source).
    pub async fn list(&self, project: &str) -> Result<Vec<SessionRow>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                &format!("{} WHERE project = ?1 ORDER BY created_at", SELECT),
                params![project],
            )
            .await
            .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(row_to_session(&row)?);
        }
        Ok(out)
    }

    /// List every session row across projects. Daemon lifecycle/status paths use this because they
    /// operate on process-owned transports, not a caller's project scope.
    pub async fn list_all(&self) -> Result<Vec<SessionRow>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(&format!("{SELECT} ORDER BY created_at"), ())
            .await
            .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(row_to_session(&row)?);
        }
        Ok(out)
    }

    async fn query_one(
        &self,
        where_clause: &str,
        p: impl libsql::params::IntoParams,
    ) -> Result<Option<SessionRow>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(&format!("{SELECT} {where_clause}"), p)
            .await
            .map_err(store_err)?;
        match rows.next().await.map_err(store_err)? {
            Some(row) => Ok(Some(row_to_session(&row)?)),
            None => Ok(None),
        }
    }

    /// Return raw non-offline rows whose heartbeat/birth timestamp is older than the TTL.
    /// Daemon reconciliation uses this list to route every transition through its complete
    /// presence writer, preserving ordered status emission as well as store convergence.
    pub async fn stale_online_rows(
        &self,
        now_ms: i64,
        ttl_ms: i64,
    ) -> Result<Vec<SessionRow>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                &format!(
                    "{SELECT} WHERE COALESCE(presence, 'offline') <> 'offline' \
                     AND ?1 - COALESCE(last_heartbeat, created_at) > ?2"
                ),
                params![now_ms, ttl_ms],
            )
            .await
            .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(row_to_session(&row)?);
        }
        Ok(out)
    }

    async fn append_lifecycle(
        &self,
        name: &str,
        session: &SessionId,
        lifecycle: &str,
        current_work: Option<&str>,
        created_at: i64,
    ) -> Result<(), NexusError> {
        DeveloperEvents::new(self.store)
            .append_agent_lifecycle(name, session, lifecycle, current_work, created_at)
            .await?;
        Ok(())
    }
}

const SELECT: &str = "SELECT session_id, name, agent, kind, role, tier, harness_session_id, \
     client_key, cwd, project, current_work, presence, paused, paused_by, callback_url, \
     last_heartbeat, created_at, transport, metadata_json, agent_id FROM sessions";

fn row_to_session(row: &libsql::Row) -> Result<SessionRow, NexusError> {
    let kind = canonical_kind(&get_text(row, 3)?)?;
    Ok(SessionRow {
        session_id: SessionId(get_text(row, 0)?),
        name: get_opt_text(row, 1)?,
        agent: get_opt_text(row, 2)?,
        kind,
        role: get_opt_text(row, 4)?,
        tier: get_text(row, 5)?,
        harness_session_id: get_opt_text(row, 6)?,
        client_key: get_opt_text(row, 7)?,
        cwd: get_opt_text(row, 8)?,
        project: get_text(row, 9)?,
        current_work: get_opt_text(row, 10)?,
        presence: get_opt_text(row, 11)?,
        paused: get_opt_int(row, 12)?.unwrap_or(0) != 0,
        paused_by: get_opt_text(row, 13)?,
        callback_url: get_opt_text(row, 14)?,
        last_heartbeat: get_opt_int(row, 15)?,
        created_at: get_opt_int(row, 16)?.unwrap_or(0),
        transport: get_opt_text(row, 17)?,
        metadata_json: get_opt_text(row, 18)?,
        agent_id: get_opt_text(row, 19)?,
    })
}

fn canonical_kind(value: &str) -> Result<String, NexusError> {
    entity_kind::parse(value)
        .map(|(locality, kind)| entity_kind::dotted(locality, kind))
        .ok_or_else(|| NexusError::Invalid(format!("unknown stored session kind {value:?}")))
}

fn is_agent_kind(value: &str) -> Result<bool, NexusError> {
    entity_kind::parse(value)
        .map(|(_, kind)| kind == Kind::Agent)
        .ok_or_else(|| NexusError::Invalid(format!("unknown session kind {value:?}")))
}

fn lifecycle_for_presence_transition(previous: &str, next: &str) -> Option<&'static str> {
    if previous == next {
        return None;
    }
    match next {
        "offline" => Some("offline"),
        "online" if previous == "offline" => Some("started"),
        _ => None,
    }
}

pub(crate) fn get_text(row: &libsql::Row, idx: i32) -> Result<String, NexusError> {
    Ok(get_opt_text(row, idx)?.unwrap_or_default())
}
pub(crate) fn get_opt_text(row: &libsql::Row, idx: i32) -> Result<Option<String>, NexusError> {
    match row.get_value(idx).map_err(store_err)? {
        libsql::Value::Text(s) => Ok(Some(s)),
        libsql::Value::Null => Ok(None),
        other => Ok(Some(format!("{other:?}"))),
    }
}
pub(crate) fn get_opt_int(row: &libsql::Row, idx: i32) -> Result<Option<i64>, NexusError> {
    match row.get_value(idx).map_err(store_err)? {
        libsql::Value::Integer(i) => Ok(Some(i)),
        libsql::Value::Null => Ok(None),
        _ => Ok(None),
    }
}
