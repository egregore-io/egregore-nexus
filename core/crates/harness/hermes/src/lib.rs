//! Hermes-owned child-session pass: native descendant discovery through
//! `sessions.parent_session_id`, byte-bounded message-row decoding and durable per-child
//! cursors. The daemon owns the parent forward loop, its runtime state, root discovery and the
//! parent row decoder, and merges the generic [`ChildPassStats`] this crate returns; the child
//! pass's concrete Hermes names, queries and locators live here.

use std::path::Path;

use libsql::params;
use nexus_agent::adapter::hermes::native::{
    HermesForwardState, HermesMessageRow, HermesNativeAdapter,
};
use nexus_common::{now, NexusError};
use nexus_contracts::ids::SessionId;
use nexus_contracts::ports::EventSink;
use nexus_contracts::{ChildResolution, ChildStream, WsEvent};
use nexus_store::repos::{ChildStreamEvents, DaemonState, LaneMutation};
use nexus_store::Store;
use serde_json::json;

/// The harness id Hermes runtimes and their children are attributed under.
pub const HARNESS_ID: &str = "hermes";

/// What one child pass did, merged by the daemon into its forwarder statistics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChildPassStats {
    /// `child_agent.update` events emitted for descendant sessions. Never parent activity.
    pub child_events: usize,
    /// Child passes or child sessions that failed; the parent pass is unaffected.
    pub child_errors: usize,
    /// Discovery cuts that may hide unregistered descendants right now: a parent's child page
    /// that was full (resumed by keyset next pass), the per-pass registration cap, and known
    /// sessions at the depth limit whose children are never explored. Non-zero means this pass
    /// was not a complete enumeration.
    pub child_discovery_truncated: usize,
    /// Known sessions at some depth that were not explored this pass because more of them exist
    /// than the per-level frontier bound; they rotate in on later passes. Non-zero means
    /// exploration below them is spread over passes, not skipped.
    pub child_discovery_deferred: usize,
}

/// Child sessions one pass will serve, least recently served first.
const MAX_HERMES_CHILDREN_PER_PASS: usize = 32;
/// Message rows one pass will forward from one child session.
const MAX_HERMES_CHILD_ROWS_PER_PASS: i64 = 256;
/// Payload bytes (content plus tool calls) one pass will materialize from one child session.
const MAX_HERMES_CHILD_BYTES_PER_PASS: i64 = 1024 * 1024;
/// Payload bytes of a single message row above which the row is never materialized.
const MAX_HERMES_CHILD_RECORD_BYTES: i64 = 256 * 1024;
/// Descendant sessions one pass will register in total.
const MAX_HERMES_CHILD_DISCOVERY: usize = 256;
/// Children one discovery query returns for one parent (keyset continues next pass).
const MAX_HERMES_CHILDREN_PER_QUERY: usize = 64;
/// Frontier parents explored per level and pass, chosen globally across all known sessions at
/// that depth, least recently explored first.
const MAX_HERMES_FRONTIER_PER_LEVEL: usize = 32;
/// Parent hops followed from the root before a descendant is left unregistered.
const MAX_HERMES_CHILD_DEPTH: u32 = 8;

/// Run one child pass for the runtime's current root and report what it did. Never fails: a
/// pass-level error is counted and logged, and the pass retries next time; discovery cuts and
/// deferrals are logged so an operator sees incomplete coverage.
pub async fn run_child_pass(
    store: &Store,
    session: &SessionId,
    events: &dyn EventSink,
    db_path: &Path,
    root: &str,
) -> ChildPassStats {
    let mut stats = ChildPassStats::default();
    if let Err(error) =
        forward_child_sessions(store, session, events, db_path, root, &mut stats).await
    {
        stats.child_errors += 1;
        tracing::warn!(
            target: "nexus::hermes_child_streams",
            session = %session,
            error = %error,
            "Hermes child pass failed; parent pass unaffected"
        );
    }
    if stats.child_discovery_truncated > 0 {
        tracing::info!(
            target: "nexus::hermes_child_streams",
            session = %session,
            truncated = stats.child_discovery_truncated,
            deferred = stats.child_discovery_deferred,
            "Hermes child discovery was cut this pass; descendants may still be unregistered"
        );
    } else if stats.child_discovery_deferred > 0 {
        tracing::debug!(
            target: "nexus::hermes_child_streams",
            session = %session,
            deferred = stats.child_discovery_deferred,
            "Hermes child discovery deferred part of the frontier to later passes"
        );
    }
    if stats.child_errors > 0 {
        tracing::warn!(
            target: "nexus::hermes_child_streams",
            session = %session,
            child_errors = stats.child_errors,
            "Hermes child sessions failed this pass; retried next pass"
        );
    }
    stats
}

/// Durable cursor of one Hermes descendant session, scoped to the root it was verified under
/// and to a daemon epoch. The root is part of the identity: a runtime whose bound root changes
/// keeps serving earlier roots' children under those roots, never relabelled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HermesChildCursor {
    pub runtime_id: SessionId,
    /// The captured root this child was verified as a descendant of.
    pub root: String,
    pub child_session_id: String,
    pub parent_session_id: String,
    pub depth: u32,
    /// Daemon boot epoch the cursor was last advanced under.
    pub epoch: String,
    /// Source generation: the session row's `started_at`, fixed for the session's life.
    pub generation: String,
    /// Last message row id forwarded.
    pub cursor: i64,
    pub halted: bool,
    pub halt_reason: String,
    pub served_at: i64,
    /// When this child was last used as a discovery frontier parent; discovery rotates through
    /// known children least recently explored first.
    pub explored_at: i64,
    pub updated_at: i64,
}

/// Repo for the Hermes child cursors, durable next to `hermes_runtime_state`.
pub struct HermesChildStreamsRepo<'a> {
    store: &'a Store,
}

impl<'a> HermesChildStreamsRepo<'a> {
    pub fn new(store: &'a Store) -> Self {
        Self { store }
    }

    pub async fn ensure_schema(&self) -> Result<(), NexusError> {
        self.store
            .conn
            .execute(
                "CREATE TABLE IF NOT EXISTS hermes_child_streams (
                    runtime_id TEXT NOT NULL,
                    root TEXT NOT NULL,
                    child_session_id TEXT NOT NULL,
                    parent_session_id TEXT NOT NULL,
                    depth INTEGER NOT NULL,
                    epoch TEXT NOT NULL,
                    generation TEXT NOT NULL DEFAULT '',
                    cursor INTEGER NOT NULL DEFAULT 0,
                    halted INTEGER NOT NULL DEFAULT 0,
                    halt_reason TEXT NOT NULL DEFAULT '',
                    served_at INTEGER NOT NULL DEFAULT 0,
                    explored_at INTEGER NOT NULL DEFAULT 0,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY (runtime_id, root, child_session_id)
                )",
                (),
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    const COLUMNS: &'static str = "runtime_id, root, child_session_id, parent_session_id, depth, epoch, generation, cursor, halted, halt_reason, served_at, explored_at, updated_at";

    pub async fn find(
        &self,
        runtime_id: &SessionId,
        root: &str,
        child_session_id: &str,
    ) -> Result<Option<HermesChildCursor>, NexusError> {
        self.ensure_schema().await?;
        let mut rows = self
            .store
            .conn
            .query(
                &format!(
                    "SELECT {} FROM hermes_child_streams
                     WHERE runtime_id = ?1 AND root = ?2 AND child_session_id = ?3",
                    Self::COLUMNS
                ),
                params![
                    runtime_id.0.clone(),
                    root.to_string(),
                    child_session_id.to_string()
                ],
            )
            .await
            .map_err(store_err)?;
        match rows.next().await.map_err(store_err)? {
            Some(row) => Ok(Some(child_cursor_from_row(&row)?)),
            None => Ok(None),
        }
    }

    /// Every child cursor of a runtime, in root then child session id order (tests and
    /// inspection).
    pub async fn list(&self, runtime_id: &SessionId) -> Result<Vec<HermesChildCursor>, NexusError> {
        self.ensure_schema().await?;
        let mut rows = self
            .store
            .conn
            .query(
                &format!(
                    "SELECT {} FROM hermes_child_streams WHERE runtime_id = ?1
                     ORDER BY root ASC, child_session_id ASC",
                    Self::COLUMNS
                ),
                params![runtime_id.0.clone()],
            )
            .await
            .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(child_cursor_from_row(&row)?);
        }
        Ok(out)
    }

    /// The newest known child of `parent` under `root` by (started_at, id): the keyset a
    /// discovery query continues after, so every child is registered exactly once in
    /// (started_at, id) order across passes without any unbounded exclusion list.
    pub async fn newest_known_child(
        &self,
        runtime_id: &SessionId,
        root: &str,
        parent: &str,
    ) -> Result<Option<(f64, String)>, NexusError> {
        self.ensure_schema().await?;
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT generation, child_session_id FROM hermes_child_streams
                 WHERE runtime_id = ?1 AND root = ?2 AND parent_session_id = ?3
                 ORDER BY CAST(generation AS REAL) DESC, child_session_id DESC LIMIT 1",
                params![runtime_id.0.clone(), root.to_string(), parent.to_string()],
            )
            .await
            .map_err(store_err)?;
        match rows.next().await.map_err(store_err)? {
            Some(row) => {
                let generation: String = row.get(0).map_err(store_err)?;
                let id: String = row.get(1).map_err(store_err)?;
                Ok(Some((generation.parse::<f64>().unwrap_or(0.0), id)))
            }
            None => Ok(None),
        }
    }

    /// Known sessions at `depth` under `root`, least recently explored first, at most `limit`
    /// plus one so the caller can tell whether more were deferred. Chosen globally across every
    /// parent at that depth, so exploration rotates fairly between parent groups.
    pub async fn frontier_at_depth(
        &self,
        runtime_id: &SessionId,
        root: &str,
        depth: u32,
        limit: usize,
    ) -> Result<Vec<String>, NexusError> {
        self.ensure_schema().await?;
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT child_session_id FROM hermes_child_streams
                 WHERE runtime_id = ?1 AND root = ?2 AND depth = ?3
                 ORDER BY explored_at ASC, child_session_id ASC LIMIT ?4",
                params![
                    runtime_id.0.clone(),
                    root.to_string(),
                    i64::from(depth),
                    i64::try_from(limit + 1).unwrap_or(i64::MAX)
                ],
            )
            .await
            .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(row.get::<String>(0).map_err(store_err)?);
        }
        Ok(out)
    }

    /// How many known sessions sit at `depth` under `root` (bounded by the discovery caps).
    pub async fn count_at_depth(
        &self,
        runtime_id: &SessionId,
        root: &str,
        depth: u32,
    ) -> Result<usize, NexusError> {
        self.ensure_schema().await?;
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT COUNT(*) FROM hermes_child_streams
                 WHERE runtime_id = ?1 AND root = ?2 AND depth = ?3",
                params![runtime_id.0.clone(), root.to_string(), i64::from(depth)],
            )
            .await
            .map_err(store_err)?;
        match rows.next().await.map_err(store_err)? {
            Some(row) => Ok(usize::try_from(row.get::<i64>(0).map_err(store_err)?).unwrap_or(0)),
            None => Ok(0),
        }
    }

    pub async fn mark_explored(
        &self,
        runtime_id: &SessionId,
        root: &str,
        child_session_id: &str,
        explored_at: i64,
    ) -> Result<(), NexusError> {
        self.ensure_schema().await?;
        self.store
            .conn
            .execute(
                "UPDATE hermes_child_streams SET explored_at = ?4
                 WHERE runtime_id = ?1 AND root = ?2 AND child_session_id = ?3",
                params![
                    runtime_id.0.clone(),
                    root.to_string(),
                    child_session_id.to_string(),
                    explored_at
                ],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Register a discovered descendant with an untouched cursor unless a row exists.
    pub async fn insert_if_absent(&self, cursor: &HermesChildCursor) -> Result<(), NexusError> {
        self.ensure_schema().await?;
        let ts = now();
        self.store
            .conn
            .execute(
                "INSERT OR IGNORE INTO hermes_child_streams
                    (runtime_id, root, child_session_id, parent_session_id, depth, epoch, generation,
                     cursor, halted, halt_reason, served_at, explored_at, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?13)",
                params![
                    cursor.runtime_id.0.clone(),
                    cursor.root.clone(),
                    cursor.child_session_id.clone(),
                    cursor.parent_session_id.clone(),
                    i64::from(cursor.depth),
                    cursor.epoch.clone(),
                    cursor.generation.clone(),
                    cursor.cursor,
                    i64::from(cursor.halted),
                    cursor.halt_reason.clone(),
                    cursor.served_at,
                    cursor.explored_at,
                    ts
                ],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// One SQL page of the cursors served least recently, across every root of the runtime.
    pub async fn least_recently_served(
        &self,
        runtime_id: &SessionId,
        limit: usize,
    ) -> Result<Vec<HermesChildCursor>, NexusError> {
        self.ensure_schema().await?;
        let mut rows = self
            .store
            .conn
            .query(
                &format!(
                    "SELECT {} FROM hermes_child_streams WHERE runtime_id = ?1
                     ORDER BY served_at ASC, root ASC, child_session_id ASC LIMIT ?2",
                    Self::COLUMNS
                ),
                params![
                    runtime_id.0.clone(),
                    i64::try_from(limit).unwrap_or(i64::MAX)
                ],
            )
            .await
            .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(child_cursor_from_row(&row)?);
        }
        Ok(out)
    }

    /// Record that a pass is about to attempt this child, independent of the outcome.
    pub async fn mark_attempt(
        &self,
        runtime_id: &SessionId,
        root: &str,
        child_session_id: &str,
        served_at: i64,
    ) -> Result<(), NexusError> {
        self.ensure_schema().await?;
        self.store
            .conn
            .execute(
                "UPDATE hermes_child_streams SET served_at = ?4, updated_at = ?4
                 WHERE runtime_id = ?1 AND root = ?2 AND child_session_id = ?3",
                params![
                    runtime_id.0.clone(),
                    root.to_string(),
                    child_session_id.to_string(),
                    served_at
                ],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    pub async fn upsert(&self, cursor: &HermesChildCursor) -> Result<(), NexusError> {
        self.ensure_schema().await?;
        let ts = now();
        self.store
            .conn
            .execute(
                "INSERT INTO hermes_child_streams
                    (runtime_id, root, child_session_id, parent_session_id, depth, epoch, generation,
                     cursor, halted, halt_reason, served_at, explored_at, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?13)
                 ON CONFLICT(runtime_id, root, child_session_id) DO UPDATE SET
                    parent_session_id = excluded.parent_session_id,
                    depth = excluded.depth,
                    epoch = excluded.epoch,
                    generation = excluded.generation,
                    cursor = excluded.cursor,
                    halted = excluded.halted,
                    halt_reason = excluded.halt_reason,
                    served_at = excluded.served_at,
                    explored_at = excluded.explored_at,
                    updated_at = excluded.updated_at",
                params![
                    cursor.runtime_id.0.clone(),
                    cursor.root.clone(),
                    cursor.child_session_id.clone(),
                    cursor.parent_session_id.clone(),
                    i64::from(cursor.depth),
                    cursor.epoch.clone(),
                    cursor.generation.clone(),
                    cursor.cursor,
                    i64::from(cursor.halted),
                    cursor.halt_reason.clone(),
                    cursor.served_at,
                    cursor.explored_at,
                    ts
                ],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }
}

fn child_cursor_from_row(row: &libsql::Row) -> Result<HermesChildCursor, NexusError> {
    Ok(HermesChildCursor {
        runtime_id: SessionId(row.get::<String>(0).map_err(store_err)?),
        root: row.get::<String>(1).map_err(store_err)?,
        child_session_id: row.get::<String>(2).map_err(store_err)?,
        parent_session_id: row.get::<String>(3).map_err(store_err)?,
        depth: u32::try_from(row.get::<i64>(4).map_err(store_err)?).unwrap_or(u32::MAX),
        epoch: row.get::<String>(5).map_err(store_err)?,
        generation: row.get::<String>(6).map_err(store_err)?,
        cursor: row.get::<i64>(7).map_err(store_err)?,
        halted: row.get::<i64>(8).map_err(store_err)? != 0,
        halt_reason: row.get::<String>(9).map_err(store_err)?,
        served_at: row.get::<i64>(10).map_err(store_err)?,
        explored_at: row.get::<i64>(11).map_err(store_err)?,
        updated_at: row.get::<i64>(12).map_err(store_err)?,
    })
}

pub fn is_missing_column(error: &libsql::Error) -> bool {
    error.to_string().contains("no such column")
}

/// Children of `parent` in the native store after the keyset `after` (started_at, id), at most
/// `limit` plus one row so the caller can tell a full page from an exhausted one.
async fn children_after(
    db: &Store,
    parent: &str,
    after: Option<&(f64, String)>,
    limit: usize,
) -> Result<Option<Vec<(String, f64)>>, NexusError> {
    let (after_started, after_id) = match after {
        Some((started, id)) => (*started, id.clone()),
        None => (f64::MIN, String::new()),
    };
    let rows = db
        .conn
        .query(
            "SELECT id, started_at FROM sessions
             WHERE parent_session_id = ?1
               AND (started_at > ?2 OR (started_at = ?2 AND id > ?3))
             ORDER BY started_at ASC, id ASC LIMIT ?4",
            params![
                parent.to_string(),
                after_started,
                after_id,
                i64::try_from(limit + 1).unwrap_or(i64::MAX)
            ],
        )
        .await;
    let mut rows = match rows {
        Ok(rows) => rows,
        Err(error) if is_missing_column(&error) || is_missing_table(&error) => return Ok(None),
        Err(error) => return Err(store_err(error)),
    };
    let mut out = Vec::new();
    while let Some(row) = rows.next().await.map_err(store_err)? {
        let id: String = row.get(0).map_err(store_err)?;
        let started: f64 = row.get::<Option<f64>>(1).map_err(store_err)?.unwrap_or(0.0);
        out.push((id, started));
    }
    Ok(Some(out))
}

/// Walk native `parent_session_id` links from the current root and register descendants.
///
/// Resumable and fair without any unbounded state. Per explored parent the query continues
/// after the newest child already registered for it (keyset on `started_at`, `id`), so
/// children are registered once in that order across passes and a full page never hides the
/// rest. The frontier at each depth is chosen globally across all known sessions at that depth,
/// least recently explored first, so exploration rotates between parent groups instead of
/// starving the later ones; a parent is marked explored only when it was actually queried.
/// Every bound is counted: a full page, the registration cap and sessions at the depth limit in
/// `stats.child_discovery_truncated` (may hide unregistered descendants now); frontier
/// candidates beyond the per-level bound in `stats.child_discovery_deferred` (explored on later
/// passes). A pass is never claimed to be a complete enumeration.
///
/// Assumption: a child newly visible in the store sorts after the newest registered child of its
/// parent by (`started_at`, `id`). Hermes sets `started_at` at session creation; a backdated or
/// imported child that sorts before the keyset is not discovered.
async fn discover_descendants(
    db: &Store,
    repo: &HermesChildStreamsRepo<'_>,
    session: &SessionId,
    root: &str,
    epoch: &str,
    stats: &mut ChildPassStats,
) -> Result<(), NexusError> {
    let explored_at = now();
    let mut registered = 0usize;
    for depth in 0..MAX_HERMES_CHILD_DEPTH {
        let frontier: Vec<String> = if depth == 0 {
            vec![root.to_string()]
        } else {
            let mut found = repo
                .frontier_at_depth(session, root, depth, MAX_HERMES_FRONTIER_PER_LEVEL)
                .await?;
            if found.len() > MAX_HERMES_FRONTIER_PER_LEVEL {
                found.truncate(MAX_HERMES_FRONTIER_PER_LEVEL);
                stats.child_discovery_deferred += 1;
            }
            found
        };
        if frontier.is_empty() {
            break;
        }
        for parent in &frontier {
            let remaining = MAX_HERMES_CHILD_DISCOVERY.saturating_sub(registered);
            if remaining == 0 {
                stats.child_discovery_truncated += 1;
                return Ok(());
            }
            let limit = remaining.min(MAX_HERMES_CHILDREN_PER_QUERY);
            let after = repo.newest_known_child(session, root, parent).await?;
            let Some(mut page) = children_after(db, parent, after.as_ref(), limit).await? else {
                return Ok(());
            };
            if page.len() > limit {
                page.truncate(limit);
                stats.child_discovery_truncated += 1;
            }
            for (id, started) in page {
                if id == root {
                    continue;
                }
                repo.insert_if_absent(&HermesChildCursor {
                    runtime_id: session.clone(),
                    root: root.to_string(),
                    child_session_id: id,
                    parent_session_id: parent.clone(),
                    depth: depth + 1,
                    epoch: epoch.to_string(),
                    generation: format!("{started}"),
                    cursor: 0,
                    halted: false,
                    halt_reason: String::new(),
                    served_at: 0,
                    explored_at: 0,
                    updated_at: 0,
                })
                .await?;
                registered += 1;
            }
            if depth > 0 {
                repo.mark_explored(session, root, parent, explored_at)
                    .await?;
            }
        }
    }
    // Sessions at the depth limit have children this walk never explores.
    let at_limit = repo
        .count_at_depth(session, root, MAX_HERMES_CHILD_DEPTH)
        .await?;
    if at_limit > 0 {
        stats.child_discovery_truncated += at_limit;
    }
    Ok(())
}

fn hermes_child_stream(cursor: &HermesChildCursor) -> ChildStream {
    ChildStream {
        harness: "hermes".into(),
        root: cursor.root.clone(),
        id: Some(cursor.child_session_id.clone()),
        locator: format!("hermes:sessions/{}", cursor.child_session_id),
        parent: Some(cursor.parent_session_id.clone()),
        parent_ref: None,
        depth: Some(cursor.depth),
        resolution: ChildResolution::LineageVerified,
        evidence: Some("sessions.parent_session_id chain to root".into()),
    }
}

/// Declare bounded unknown coverage for a child session under the current epoch, on the lane
/// of the root the cursor was verified under.
async fn declare_hermes_coverage(
    store: &Store,
    session: &SessionId,
    epoch: &str,
    cursor: &HermesChildCursor,
    reason: &str,
    extra: serde_json::Value,
) -> Result<LaneMutation, NexusError> {
    let mut coverage = json!({
        "generation": cursor.generation,
        "from": cursor.cursor,
        "unknown_before": true,
        "reason": reason,
    });
    if let (Some(target), Some(source)) = (coverage.as_object_mut(), extra.as_object()) {
        for (key, value) in source {
            target.insert(key.clone(), value.clone());
        }
    }
    ChildStreamEvents::new(store)
        .set_coverage(
            session,
            epoch,
            &hermes_child_stream(cursor),
            &coverage,
            &store.child_stream_bounds(),
        )
        .await
}

/// Tail the root's descendant sessions into the child lane behind durable epoch-scoped
/// cursors keyed by the root they were verified under. Discovery (see
/// [`discover_descendants`]) runs for the current root; scheduling serves one SQL page of the
/// least recently served cursors of every root the runtime ever owned, each marked as
/// attempted before any work, and each child forwards a byte- and row-bounded page of message
/// rows under its own root. A consumed cursor from another epoch, a session whose `started_at`
/// changed or that vanished, or a record over the byte budget, declares bounded unknown
/// coverage with its reason and halts; no replay or skip policy is adopted.
async fn forward_child_sessions(
    store: &Store,
    session: &SessionId,
    events: &dyn EventSink,
    db_path: &Path,
    root: &str,
    stats: &mut ChildPassStats,
) -> Result<(), NexusError> {
    let epoch = match DaemonState::new(store).boot_epoch().await? {
        Some(epoch) if !epoch.is_empty() => epoch,
        _ => return Ok(()),
    };
    if !db_path.exists() {
        return Ok(());
    }
    let db = Store::open(&path_to_text(db_path)).await?;
    let repo = HermesChildStreamsRepo::new(store);

    discover_descendants(&db, &repo, session, root, &epoch, stats).await?;

    // Scheduling: one page across every root, attempts marked before any work.
    let served_at = now();
    let batch = repo
        .least_recently_served(session, MAX_HERMES_CHILDREN_PER_PASS)
        .await?;
    for cursor in &batch {
        repo.mark_attempt(session, &cursor.root, &cursor.child_session_id, served_at)
            .await?;
    }
    for mut cursor in batch {
        cursor.served_at = served_at;
        let child_id = cursor.child_session_id.clone();
        if let Err(error) =
            serve_child_session(store, &db, &repo, session, events, &epoch, cursor, stats).await
        {
            stats.child_errors += 1;
            tracing::warn!(
                target: "nexus::hermes_child_streams",
                session = %session, child = %child_id, error = %error,
                "Hermes child session pass failed; retried next pass"
            );
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn serve_child_session(
    store: &Store,
    db: &Store,
    repo: &HermesChildStreamsRepo<'_>,
    session: &SessionId,
    events: &dyn EventSink,
    epoch: &str,
    mut cursor: HermesChildCursor,
    stats: &mut ChildPassStats,
) -> Result<(), NexusError> {
    // A halted cursor re-declares its coverage under every later epoch; it never advances.
    if cursor.halted {
        if cursor.epoch != epoch {
            let reason = cursor.halt_reason.clone();
            if declare_hermes_coverage(store, session, epoch, &cursor, &reason, json!({})).await?
                == LaneMutation::Applied
            {
                cursor.epoch = epoch.to_string();
            }
        }
        repo.upsert(&cursor).await?;
        return Ok(());
    }
    let consumed = cursor.cursor > 0;
    if !consumed && cursor.epoch != epoch {
        cursor.epoch = epoch.to_string();
    }

    // The session row is the source: its `started_at` is the generation.
    let mut rows = db
        .conn
        .query(
            "SELECT started_at FROM sessions WHERE id = ?1",
            params![cursor.child_session_id.clone()],
        )
        .await
        .map_err(store_err)?;
    let current_generation = rows
        .next()
        .await
        .map_err(store_err)?
        .map(|row| {
            row.get::<Option<f64>>(0)
                .map(|started| format!("{}", started.unwrap_or(0.0)))
        })
        .transpose()
        .map_err(store_err)?;
    let halt: Option<(&str, serde_json::Value)> = if consumed && cursor.epoch != epoch {
        Some(("daemon_restart_policy_undecided", json!({})))
    } else {
        match current_generation {
            None => Some(("source_missing", json!({}))),
            Some(current) if current != cursor.generation => Some((
                "source_replaced",
                json!({ "previous_generation": cursor.generation, "current_generation": current }),
            )),
            Some(_) => None,
        }
    };
    if let Some((reason, extra)) = halt {
        if declare_hermes_coverage(store, session, epoch, &cursor, reason, extra).await?
            == LaneMutation::Applied
        {
            cursor.epoch = epoch.to_string();
            cursor.halted = true;
            cursor.halt_reason = reason.to_string();
        } else {
            tracing::warn!(
                target: "nexus::hermes_child_streams",
                session = %session, child = %cursor.child_session_id, reason,
                "child coverage declaration refused by the lane bounds; retried next pass"
            );
        }
        repo.upsert(&cursor).await?;
        return Ok(());
    }

    let read = read_child_message_rows(db, &cursor.child_session_id, cursor.cursor).await?;
    let child = hermes_child_stream(&cursor);
    let mut translate =
        HermesForwardState::from_cursor(Some(cursor.child_session_id.clone()), cursor.cursor);
    let adapter = HermesNativeAdapter;
    for row in &read.rows {
        // Row provenance: every event decoded from one native row shares this reference. It
        // names the row within the session's generation, not a unique event.
        let source_ref = format!(
            "hermes:{}@{}#{}",
            cursor.child_session_id, cursor.generation, row.id
        );
        for event in adapter.decode(row, &mut translate) {
            events
                .emit(WsEvent::ChildAgentUpdate {
                    session_id: session.clone(),
                    child: child.clone(),
                    kind: event.kind,
                    source_ref: source_ref.clone(),
                    data: event.data,
                })
                .await;
            stats.child_events += 1;
        }
        cursor.cursor = cursor.cursor.max(row.id);
    }
    if let Some((row_id, bytes)) = read.oversized {
        // A single record over the byte budget can never be forwarded; the rows before it
        // were, and this is declared rather than left as a silent stall.
        let reason = "record_exceeds_byte_budget";
        if declare_hermes_coverage(
            store,
            session,
            epoch,
            &cursor,
            reason,
            json!({ "row": row_id, "bytes": bytes, "budget": MAX_HERMES_CHILD_RECORD_BYTES }),
        )
        .await?
            == LaneMutation::Applied
        {
            cursor.halted = true;
            cursor.halt_reason = reason.to_string();
        }
    }
    repo.upsert(&cursor).await?;
    Ok(())
}

/// One byte- and row-bounded read of a child session's message rows.
struct ChildRows {
    rows: Vec<HermesMessageRow>,
    /// The first record whose content or tool calls exceed the per-record byte budget: its row
    /// id and size. Reading stops before it; nothing of it is materialized or parsed.
    oversized: Option<(i64, i64)>,
}

/// Active message rows of `child_session_id` after `after_message_id`, at most
/// [`MAX_HERMES_CHILD_ROWS_PER_PASS`] rows and about [`MAX_HERMES_CHILD_BYTES_PER_PASS`] bytes
/// of payload. Payload columns are only returned when within the per-record budget: the store
/// reports their exact byte lengths (`length(CAST(... AS BLOB))`, which counts every byte of a
/// multibyte text and does not stop at an embedded NUL as `length()` on TEXT would) and
/// withholds oversized ones, so nothing larger than the budget is ever materialized or
/// JSON-parsed on the daemon's forward path. Admitted payloads are fetched as BLOB so an
/// embedded NUL survives materialization.
async fn read_child_message_rows(
    db: &Store,
    child_session_id: &str,
    after_message_id: i64,
) -> Result<ChildRows, NexusError> {
    let rows = db
        .conn
        .query(
            "SELECT id, session_id, role,
                    CASE WHEN COALESCE(length(CAST(content AS BLOB)), 0) <= ?4 THEN CAST(content AS BLOB) ELSE NULL END,
                    tool_call_id,
                    CASE WHEN COALESCE(length(CAST(tool_calls AS BLOB)), 0) <= ?4 THEN CAST(tool_calls AS BLOB) ELSE NULL END,
                    tool_name, timestamp, finish_reason, active,
                    COALESCE(length(CAST(content AS BLOB)), 0) + COALESCE(length(CAST(tool_calls AS BLOB)), 0)
             FROM messages
             WHERE session_id = ?1 AND id > ?2 AND active = 1
             ORDER BY id ASC LIMIT ?3",
            params![
                child_session_id.to_string(),
                after_message_id.max(0),
                MAX_HERMES_CHILD_ROWS_PER_PASS,
                MAX_HERMES_CHILD_RECORD_BYTES
            ],
        )
        .await;
    let mut rows = match rows {
        Ok(rows) => rows,
        Err(error) if is_missing_table(&error) => {
            return Ok(ChildRows {
                rows: Vec::new(),
                oversized: None,
            })
        }
        Err(error) => return Err(store_err(error)),
    };
    let mut out = Vec::new();
    let mut budget = MAX_HERMES_CHILD_BYTES_PER_PASS;
    while let Some(row) = rows.next().await.map_err(store_err)? {
        let bytes: i64 = row.get(10).map_err(store_err)?;
        if bytes > MAX_HERMES_CHILD_RECORD_BYTES {
            let id: i64 = row.get(0).map_err(store_err)?;
            return Ok(ChildRows {
                rows: out,
                oversized: Some((id, bytes)),
            });
        }
        if !out.is_empty() && bytes > budget {
            // The pass budget is spent; the rest waits for the next pass.
            break;
        }
        budget = budget.saturating_sub(bytes);
        out.push(child_message_row(&row)?);
    }
    Ok(ChildRows {
        rows: out,
        oversized: None,
    })
}

/// A payload column fetched as BLOB (so an embedded NUL survives, where a TEXT read stops at
/// it), decoded as strict UTF-8. The parent path keeps its TEXT read in the daemon's parent
/// decoder (`message_row` in the daemon forwarder).
fn blob_text(row: &libsql::Row, idx: i32) -> Result<Option<String>, NexusError> {
    match row.get_value(idx).map_err(store_err)? {
        libsql::Value::Null => Ok(None),
        libsql::Value::Blob(bytes) => String::from_utf8(bytes).map(Some).map_err(|error| {
            NexusError::Store(format!(
                "invalid UTF-8 in Hermes child payload column {idx}: {}",
                error.utf8_error()
            ))
        }),
        libsql::Value::Text(text) => Ok(Some(text)),
        other => Err(NexusError::Store(format!(
            "unexpected Hermes payload column type: {other:?}"
        ))),
    }
}

/// The child-query counterpart of the daemon's parent decoder (`message_row`), whose payload
/// columns arrive as BLOB.
fn child_message_row(row: &libsql::Row) -> Result<HermesMessageRow, NexusError> {
    let tool_calls = blob_text(row, 5)?
        .filter(|raw| !raw.trim().is_empty())
        .map(|raw| {
            serde_json::from_str(&raw)
                .map_err(|e| NexusError::Store(format!("invalid Hermes tool_calls JSON: {e}")))
        })
        .transpose()?;
    Ok(HermesMessageRow {
        id: row.get(0).map_err(store_err)?,
        session_id: row.get(1).map_err(store_err)?,
        role: row.get(2).map_err(store_err)?,
        content: blob_text(row, 3)?,
        tool_call_id: row.get(4).map_err(store_err)?,
        tool_calls,
        tool_name: row.get(6).map_err(store_err)?,
        timestamp: row.get(7).map_err(store_err)?,
        finish_reason: row.get(8).map_err(store_err)?,
        active: row.get::<Option<i64>>(9).map_err(store_err)?.unwrap_or(1) != 0,
    })
}

fn path_to_text(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn store_err(e: libsql::Error) -> NexusError {
    NexusError::Store(e.to_string())
}

fn is_missing_table(error: &libsql::Error) -> bool {
    error.to_string().contains("no such table")
}
