//! The `child_stream_events` repo — the bounded volatile lane for native child streams.
//!
//! Subagents, delegated sessions and sub-threads share a Nexus owner session with their captured
//! root. The harness managing code attributes each of their updates with a [`ChildStream`] and
//! the sink appends them here, never to `mem.stream_events`, the gateway projection or `/agent`
//! history. The lane is bounded per owner session; every eviction, refusal and compaction is
//! accounted so a reader sees loss instead of assuming continuity. Volatile, like
//! `stream_events`; no durability claim.
//!
//! # Mutation discipline
//!
//! Every mutation and every read holds the store's child-lane gate, so admission counts, bound
//! enforcement, loss accounting and reads never interleave across tasks. The gate is a task
//! mutex, not a SQL transaction: unrelated writers on the shared connection are never captured.
//! Within a mutation the statements are ordered so that any failure leaves a bounded, truthfully
//! accounted state. This is statement-level safety, not a whole-append transaction: a statement
//! that failed changed nothing, and everything the earlier statements did (the lane record, any
//! eviction batch and its accounting) stays committed and accounted:
//!
//! - the lane record is written before its first event, so no event can exist without a lane;
//! - room is made before an event is inserted, never after, so a failing eviction never grows
//!   retained rows or bytes;
//! - loss bookkeeping runs inside SQLite `AFTER DELETE` triggers (see `migrate.rs`), so it is
//!   exactly as atomic as the delete: a row that is not deleted is never counted, a deleted row
//!   is counted once;
//! - inserted ids come back from `RETURNING`, never from the connection's shared counter.
//!
//! # Metadata bounds
//!
//! Lane records outlive their rows as tombstones. Their size is bounded by construction: each
//! record's identity JSON, key, source reference and coverage are capped per record
//! ([`MAX_CHILD_JSON_BYTES`], [`MAX_KEY_BYTES`], [`MAX_SOURCE_REF_BYTES`],
//! [`MAX_COVERAGE_BYTES`]) and the number of records per owner session is capped by the lane
//! budget, with compaction into a single session loss row. An event larger than the whole byte
//! budget is refused, never stored.

use libsql::params;
use nexus_common::{now, NexusError};
use nexus_contracts::events::{ChildResolution, ChildStream};
use nexus_contracts::ids::SessionId;
use serde_json::Value;

use crate::error::store_err;
use crate::state::Store;

/// Lane key of the per-session sentinel that counts refused observations.
pub const REFUSED_SENTINEL_KEY: &str = "s:refused";
/// Per-record cap on the serialized [`ChildStream`] identity.
pub const MAX_CHILD_JSON_BYTES: usize = 4096;
/// Per-record cap on the namespaced lane key.
pub const MAX_KEY_BYTES: usize = 512;
/// Per-record cap on a child event's `source_ref`.
pub const MAX_SOURCE_REF_BYTES: usize = 1024;
/// Per-lane cap on the serialized coverage declaration.
pub const MAX_COVERAGE_BYTES: usize = 2048;
const SENTINEL_HARNESS: &str = "*";
const SENTINEL_ROOT: &str = "*";
const EVICTION_BATCH: i64 = 32;

/// Per-owner-session bounds for the child lane. Configured by the daemon; defaults match
/// `nexus-common::Config`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChildStreamBounds {
    pub max_rows_per_session: u32,
    pub max_bytes_per_session: u64,
    pub max_lanes_per_session: u32,
    pub max_unresolved_lanes_per_session: u32,
}

impl Default for ChildStreamBounds {
    fn default() -> Self {
        ChildStreamBounds {
            max_rows_per_session: 2048,
            max_bytes_per_session: 4 * 1024 * 1024,
            max_lanes_per_session: 256,
            max_unresolved_lanes_per_session: 64,
        }
    }
}

impl From<&nexus_common::Config> for ChildStreamBounds {
    fn from(config: &nexus_common::Config) -> Self {
        ChildStreamBounds {
            max_rows_per_session: config.child_stream_max_rows_per_session,
            max_bytes_per_session: config.child_stream_max_bytes_per_session,
            max_lanes_per_session: config.child_stream_max_lanes_per_session,
            max_unresolved_lanes_per_session: config.child_stream_max_unresolved_lanes_per_session,
        }
    }
}

/// Namespaced lane key: `n:<native id>` when the source carries one, else `l:<locator>`.
/// Sentinels use `s:<name>`. The namespaces never collide.
pub fn child_key(child: &ChildStream) -> String {
    match child.id.as_deref().filter(|id| !id.is_empty()) {
        Some(id) => format!("n:{id}"),
        None => format!("l:{}", child.locator),
    }
}

/// Which bound refused an observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefusedBy {
    /// The unresolved-lane cardinality bound.
    UnresolvedLanes,
    /// The total lane budget, with no tombstone left to compact.
    Lanes,
    /// A per-record metadata cap, or an event larger than the whole byte budget.
    Oversized,
}

/// Result of one append.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendOutcome {
    Stored(i64),
    Refused(RefusedBy),
}

/// Result of a metadata-only lane mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LaneMutation {
    Applied,
    Refused(RefusedBy),
}

/// One child lane row as read back.
#[derive(Debug, Clone, PartialEq)]
pub struct ChildStreamEventRow {
    pub id: i64,
    pub session_id: String,
    pub harness: String,
    pub root: String,
    pub child_key: String,
    pub epoch: String,
    pub child: ChildStream,
    pub kind: String,
    pub source_ref: String,
    pub data: String,
    pub bytes: i64,
    pub created_at: i64,
}

/// Optional narrowing for [`ChildStreamEvents::page`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LaneFilter {
    pub harness: Option<String>,
    pub root: Option<String>,
    pub child_key: Option<String>,
}

/// One SQL-limited page of lane rows.
#[derive(Debug, Clone, PartialEq)]
pub struct Page {
    pub rows: Vec<ChildStreamEventRow>,
    /// Pass back as `after_id` for the next page; `None` when the page was the last.
    pub next_after_id: Option<i64>,
}

/// Composite position in the lane enumeration: the full ordering key, so lanes that share a
/// child key under different roots or harnesses are never skipped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaneCursor {
    pub child_key: String,
    pub harness: String,
    pub root: String,
}

/// One lane's identity plus its live and lost accounting.
#[derive(Debug, Clone, PartialEq)]
pub struct LaneSummary {
    pub child: ChildStream,
    pub child_key: String,
    pub epoch: String,
    pub first_seen_id: i64,
    pub live_first_id: Option<i64>,
    pub live_last_id: Option<i64>,
    pub live_rows: i64,
    pub live_bytes: i64,
    pub evicted_through: i64,
    pub evicted_rows: i64,
    pub evicted_bytes: i64,
    pub refused_rows: i64,
    /// What part of the native source the harness pass declared this lane covers, when any.
    pub coverage: Option<Value>,
}

impl LaneSummary {
    /// The cursor that continues the enumeration after this lane.
    pub fn cursor(&self) -> LaneCursor {
        LaneCursor {
            child_key: self.child_key.clone(),
            harness: self.child.harness.clone(),
            root: self.child.root.clone(),
        }
    }
}

/// One SQL-limited page of lanes, in full key order.
#[derive(Debug, Clone, PartialEq)]
pub struct LanePage {
    pub lanes: Vec<LaneSummary>,
    pub next_after: Option<LaneCursor>,
}

/// Owner-level loss summary: what tombstone compaction removed from the lane table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionLoss {
    pub epoch: String,
    pub compacted_lanes: i64,
    pub evicted_rows: i64,
    pub evicted_bytes: i64,
    pub refused_rows: i64,
    pub updated_at: i64,
}

/// Accessor for the attached volatile child lane tables.
pub struct ChildStreamEvents<'a> {
    store: &'a Store,
}

impl<'a> ChildStreamEvents<'a> {
    pub fn new(store: &'a Store) -> Self {
        ChildStreamEvents { store }
    }

    /// Append one child update under `epoch`, applying every bound in the same gated call.
    ///
    /// Refused observations are counted on the session's `s:refused` sentinel lane: an
    /// oversized identity, key or source reference, an event larger than the whole byte budget,
    /// a new (or newly unresolved) lane beyond the unresolved bound, or a new lane beyond the lane
    /// budget when no tombstone can be compacted. Then the lane record is written, room is made
    /// by evicting oldest-first across the session (accounted by the delete trigger), and only
    /// then is the event inserted.
    #[allow(clippy::too_many_arguments)]
    pub async fn append(
        &self,
        session_id: &SessionId,
        epoch: &str,
        child: &ChildStream,
        kind: &str,
        source_ref: &str,
        data: &str,
        bounds: &ChildStreamBounds,
    ) -> Result<AppendOutcome, NexusError> {
        let _gate = self.store.child_lane_gate().lock_owned().await;
        let key = child_key(child);
        let child_json = serialize_child(child)?;
        let unresolved = child.resolution == ChildResolution::Unresolved;
        let ts = now();
        let bytes = (data.len() + child_json.len() + source_ref.len() + key.len()) as i64;
        let max_bytes = i64::try_from(bounds.max_bytes_per_session).unwrap_or(i64::MAX);
        if child_json.len() > MAX_CHILD_JSON_BYTES
            || key.len() > MAX_KEY_BYTES
            || source_ref.len() > MAX_SOURCE_REF_BYTES
            || bytes > max_bytes
        {
            self.count_refusal(session_id, epoch, ts).await?;
            return Ok(AppendOutcome::Refused(RefusedBy::Oversized));
        }
        if let Some(refused) = self
            .admit(session_id, epoch, child, &key, unresolved, bounds, ts)
            .await?
        {
            return Ok(AppendOutcome::Refused(refused));
        }

        // Lane before event: no event can ever exist without its lane record.
        self.upsert_lane_identity(session_id, epoch, child, &key, &child_json, unresolved, ts)
            .await?;
        // Room before insert: a failing eviction inserts nothing, so retained state only ever
        // shrinks, and every shrink was accounted by the delete trigger.
        self.make_room(session_id, bytes, bounds).await?;

        let conn = self.store.stream_conn();
        let mut inserted = conn
            .query(
                "INSERT INTO mem.child_stream_events \
                 (session_id, harness, root, child_key, epoch, child, kind, source_ref, data, bytes, created_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11) RETURNING id",
                params![
                    session_id.0.clone(),
                    child.harness.clone(),
                    child.root.clone(),
                    key.clone(),
                    epoch.to_string(),
                    child_json,
                    kind.to_string(),
                    source_ref.to_string(),
                    data.to_string(),
                    bytes,
                    ts
                ],
            )
            .await
            .map_err(store_err)?;
        let id: i64 = inserted
            .next()
            .await
            .map_err(store_err)?
            .ok_or_else(|| NexusError::Store("child lane insert returned no id".into()))?
            .get(0)
            .map_err(store_err)?;
        drop(inserted);
        // Record the first row id on the lane; a lane that never gets this update is still
        // compactable once evictions are accounted on it.
        conn.execute(
            "UPDATE mem.child_stream_lanes SET first_seen_id = ?5, updated_at = ?6 \
             WHERE session_id = ?1 AND harness = ?2 AND root = ?3 AND child_key = ?4 \
               AND first_seen_id = 0",
            params![
                session_id.0.clone(),
                child.harness.clone(),
                child.root.clone(),
                key,
                id,
                ts
            ],
        )
        .await
        .map_err(store_err)?;
        Ok(AppendOutcome::Stored(id))
    }

    /// Rows of the session's child lane with `id > after_id`, narrowed by `filter`, at most
    /// `limit` rows. Read under the gate, so it never observes a half-applied mutation.
    pub async fn page(
        &self,
        session_id: &SessionId,
        filter: &LaneFilter,
        after_id: i64,
        limit: u32,
    ) -> Result<Page, NexusError> {
        let _gate = self.store.child_lane_gate().lock_owned().await;
        let limit = i64::from(limit.max(1));
        let harness = filter.harness.clone().unwrap_or_default();
        let root = filter.root.clone().unwrap_or_default();
        let key = filter.child_key.clone().unwrap_or_default();
        let mut rows = self
            .store
            .stream_conn()
            .query(
                "SELECT id, session_id, harness, root, child_key, epoch, child, kind, source_ref, data, bytes, created_at \
                 FROM mem.child_stream_events \
                 WHERE session_id = ?1 AND id > ?2 \
                   AND (?3 = '' OR harness = ?3) AND (?4 = '' OR root = ?4) AND (?5 = '' OR child_key = ?5) \
                 ORDER BY id ASC LIMIT ?6",
                params![session_id.0.clone(), after_id, harness, root, key, limit + 1],
            )
            .await
            .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            let child_json: String = row.get(6).map_err(store_err)?;
            out.push(ChildStreamEventRow {
                id: row.get(0).map_err(store_err)?,
                session_id: row.get(1).map_err(store_err)?,
                harness: row.get(2).map_err(store_err)?,
                root: row.get(3).map_err(store_err)?,
                child_key: row.get(4).map_err(store_err)?,
                epoch: row.get(5).map_err(store_err)?,
                child: parse_child(&child_json)?,
                kind: row.get(7).map_err(store_err)?,
                source_ref: row.get(8).map_err(store_err)?,
                data: row.get(9).map_err(store_err)?,
                bytes: row.get(10).map_err(store_err)?,
                created_at: row.get(11).map_err(store_err)?,
            });
        }
        let next_after_id = if out.len() as i64 > limit {
            out.truncate(limit as usize);
            out.last().map(|row| row.id)
        } else {
            None
        };
        Ok(Page {
            rows: out,
            next_after_id,
        })
    }

    /// Lanes of the session in full key order (child key, harness, root), continuing after
    /// `after`, at most `limit` lanes. Tombstones, coverage-only lanes and the refusal sentinel
    /// are included. Read under the gate.
    pub async fn lanes(
        &self,
        session_id: &SessionId,
        after: Option<&LaneCursor>,
        limit: u32,
    ) -> Result<LanePage, NexusError> {
        let _gate = self.store.child_lane_gate().lock_owned().await;
        let limit = i64::from(limit.max(1));
        let (after_key, after_harness, after_root) = match after {
            Some(cursor) => (
                cursor.child_key.clone(),
                cursor.harness.clone(),
                cursor.root.clone(),
            ),
            None => (String::new(), String::new(), String::new()),
        };
        let mut rows = self
            .store
            .stream_conn()
            .query(
                "SELECT l.harness, l.root, l.child_key, l.epoch, l.child, l.first_seen_id, \
                        l.evicted_through, l.evicted_rows, l.evicted_bytes, l.refused_rows, l.coverage, \
                        (SELECT MIN(e.id) FROM mem.child_stream_events e WHERE e.session_id = l.session_id AND e.harness = l.harness AND e.root = l.root AND e.child_key = l.child_key), \
                        (SELECT MAX(e.id) FROM mem.child_stream_events e WHERE e.session_id = l.session_id AND e.harness = l.harness AND e.root = l.root AND e.child_key = l.child_key), \
                        (SELECT COUNT(*) FROM mem.child_stream_events e WHERE e.session_id = l.session_id AND e.harness = l.harness AND e.root = l.root AND e.child_key = l.child_key), \
                        (SELECT COALESCE(SUM(e.bytes), 0) FROM mem.child_stream_events e WHERE e.session_id = l.session_id AND e.harness = l.harness AND e.root = l.root AND e.child_key = l.child_key) \
                 FROM mem.child_stream_lanes l \
                 WHERE l.session_id = ?1 AND (l.child_key, l.harness, l.root) > (?2, ?3, ?4) \
                 ORDER BY l.child_key ASC, l.harness ASC, l.root ASC LIMIT ?5",
                params![
                    session_id.0.clone(),
                    after_key,
                    after_harness,
                    after_root,
                    limit + 1
                ],
            )
            .await
            .map_err(store_err)?;
        let mut lanes = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            let child_json: String = row.get(4).map_err(store_err)?;
            let coverage: String = row.get(10).map_err(store_err)?;
            let live_rows: i64 = row.get(13).map_err(store_err)?;
            let (live_first_id, live_last_id) = if live_rows > 0 {
                (
                    Some(row.get::<i64>(11).map_err(store_err)?),
                    Some(row.get::<i64>(12).map_err(store_err)?),
                )
            } else {
                (None, None)
            };
            lanes.push(LaneSummary {
                child: parse_child(&child_json)?,
                child_key: row.get(2).map_err(store_err)?,
                epoch: row.get(3).map_err(store_err)?,
                first_seen_id: row.get(5).map_err(store_err)?,
                live_first_id,
                live_last_id,
                live_rows,
                live_bytes: row.get(14).map_err(store_err)?,
                evicted_through: row.get(6).map_err(store_err)?,
                evicted_rows: row.get(7).map_err(store_err)?,
                evicted_bytes: row.get(8).map_err(store_err)?,
                refused_rows: row.get(9).map_err(store_err)?,
                coverage: if coverage.is_empty() {
                    None
                } else {
                    Some(serde_json::from_str(&coverage).map_err(|e| {
                        NexusError::Store(format!("invalid child lane coverage: {e}"))
                    })?)
                },
            });
        }
        let next_after = if lanes.len() as i64 > limit {
            lanes.truncate(limit as usize);
            lanes.last().map(LaneSummary::cursor)
        } else {
            None
        };
        Ok(LanePage { lanes, next_after })
    }

    /// The owner-level loss summary, when compaction has ever run for the session.
    pub async fn session_loss(
        &self,
        session_id: &SessionId,
    ) -> Result<Option<SessionLoss>, NexusError> {
        let _gate = self.store.child_lane_gate().lock_owned().await;
        let mut rows = self
            .store
            .stream_conn()
            .query(
                "SELECT epoch, compacted_lanes, evicted_rows, evicted_bytes, refused_rows, updated_at \
                 FROM mem.child_stream_session_loss WHERE session_id = ?1",
                params![session_id.0.clone()],
            )
            .await
            .map_err(store_err)?;
        let Some(row) = rows.next().await.map_err(store_err)? else {
            return Ok(None);
        };
        Ok(Some(SessionLoss {
            epoch: row.get(0).map_err(store_err)?,
            compacted_lanes: row.get(1).map_err(store_err)?,
            evicted_rows: row.get(2).map_err(store_err)?,
            evicted_bytes: row.get(3).map_err(store_err)?,
            refused_rows: row.get(4).map_err(store_err)?,
            updated_at: row.get(5).map_err(store_err)?,
        }))
    }

    /// Declare what part of the native source a lane covers. Called by a harness pass. Creating
    /// or updating the lane record goes through the same admission bounds as an append (lane
    /// budget, unresolved bound, identity caps) and updates the lane's identity, status and
    /// epoch consistently with the declaration; the coverage value itself is capped.
    pub async fn set_coverage(
        &self,
        session_id: &SessionId,
        epoch: &str,
        child: &ChildStream,
        coverage: &Value,
        bounds: &ChildStreamBounds,
    ) -> Result<LaneMutation, NexusError> {
        let _gate = self.store.child_lane_gate().lock_owned().await;
        let key = child_key(child);
        let child_json = serialize_child(child)?;
        let coverage_json = coverage.to_string();
        let unresolved = child.resolution == ChildResolution::Unresolved;
        let ts = now();
        if child_json.len() > MAX_CHILD_JSON_BYTES
            || key.len() > MAX_KEY_BYTES
            || coverage_json.len() > MAX_COVERAGE_BYTES
        {
            self.count_refusal(session_id, epoch, ts).await?;
            return Ok(LaneMutation::Refused(RefusedBy::Oversized));
        }
        if let Some(refused) = self
            .admit(session_id, epoch, child, &key, unresolved, bounds, ts)
            .await?
        {
            return Ok(LaneMutation::Refused(refused));
        }
        self.store
            .stream_conn()
            .execute(
                "INSERT INTO mem.child_stream_lanes \
                 (session_id, harness, root, child_key, epoch, child, unresolved, first_seen_id, coverage, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0, ?8, ?9) \
                 ON CONFLICT(session_id, harness, root, child_key) DO UPDATE SET \
                   coverage = excluded.coverage, child = excluded.child, unresolved = excluded.unresolved, \
                   epoch = excluded.epoch, updated_at = excluded.updated_at",
                params![
                    session_id.0.clone(),
                    child.harness.clone(),
                    child.root.clone(),
                    key,
                    epoch.to_string(),
                    child_json,
                    i64::from(unresolved),
                    coverage_json,
                    ts
                ],
            )
            .await
            .map_err(store_err)?;
        Ok(LaneMutation::Applied)
    }

    /// Drop every child lane row, lane record and loss summary of the session.
    pub async fn evict_session(&self, session_id: &SessionId) -> Result<u64, NexusError> {
        let _gate = self.store.child_lane_gate().lock_owned().await;
        let conn = self.store.stream_conn();
        let rows = conn
            .execute(
                "DELETE FROM mem.child_stream_events WHERE session_id = ?1",
                params![session_id.0.clone()],
            )
            .await
            .map_err(store_err)?;
        conn.execute(
            "DELETE FROM mem.child_stream_lanes WHERE session_id = ?1",
            params![session_id.0.clone()],
        )
        .await
        .map_err(store_err)?;
        conn.execute(
            "DELETE FROM mem.child_stream_session_loss WHERE session_id = ?1",
            params![session_id.0.clone()],
        )
        .await
        .map_err(store_err)?;
        Ok(rows)
    }

    /// Admission under the gate: `None` admits, `Some(reason)` refuses (already counted).
    /// A lane that exists as verified and now arrives unresolved is admitted through the
    /// unresolved bound like a new unresolved lane.
    #[allow(clippy::too_many_arguments)]
    async fn admit(
        &self,
        session_id: &SessionId,
        epoch: &str,
        child: &ChildStream,
        key: &str,
        unresolved: bool,
        bounds: &ChildStreamBounds,
        ts: i64,
    ) -> Result<Option<RefusedBy>, NexusError> {
        let existing = self
            .lane_state(session_id, &child.harness, &child.root, key)
            .await?;
        let becomes_unresolved = match existing {
            Some(existing_unresolved) => unresolved && !existing_unresolved,
            None => unresolved,
        };
        if becomes_unresolved
            && self.count_lanes(session_id, true).await?
                >= i64::from(bounds.max_unresolved_lanes_per_session)
        {
            self.count_refusal(session_id, epoch, ts).await?;
            return Ok(Some(RefusedBy::UnresolvedLanes));
        }
        if existing.is_none()
            && self.count_lanes(session_id, false).await? >= i64::from(bounds.max_lanes_per_session)
            && !self.compact_one_tombstone(session_id).await?
        {
            self.count_refusal(session_id, epoch, ts).await?;
            return Ok(Some(RefusedBy::Lanes));
        }
        Ok(None)
    }

    /// Write the lane's identity, status and epoch (creating the record with `first_seen_id`
    /// 0); coverage and accounting are untouched.
    #[allow(clippy::too_many_arguments)]
    async fn upsert_lane_identity(
        &self,
        session_id: &SessionId,
        epoch: &str,
        child: &ChildStream,
        key: &str,
        child_json: &str,
        unresolved: bool,
        ts: i64,
    ) -> Result<(), NexusError> {
        self.store
            .stream_conn()
            .execute(
                "INSERT INTO mem.child_stream_lanes \
                 (session_id, harness, root, child_key, epoch, child, unresolved, first_seen_id, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0, ?8) \
                 ON CONFLICT(session_id, harness, root, child_key) DO UPDATE SET \
                   epoch = excluded.epoch, child = excluded.child, unresolved = excluded.unresolved, \
                   updated_at = excluded.updated_at",
                params![
                    session_id.0.clone(),
                    child.harness.clone(),
                    child.root.clone(),
                    key.to_string(),
                    epoch.to_string(),
                    child_json.to_string(),
                    i64::from(unresolved),
                    ts
                ],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// `Some(unresolved flag)` when the lane record exists.
    async fn lane_state(
        &self,
        session_id: &SessionId,
        harness: &str,
        root: &str,
        key: &str,
    ) -> Result<Option<bool>, NexusError> {
        let mut rows = self
            .store
            .stream_conn()
            .query(
                "SELECT unresolved FROM mem.child_stream_lanes \
                 WHERE session_id = ?1 AND harness = ?2 AND root = ?3 AND child_key = ?4",
                params![
                    session_id.0.clone(),
                    harness.to_string(),
                    root.to_string(),
                    key.to_string()
                ],
            )
            .await
            .map_err(store_err)?;
        match rows.next().await.map_err(store_err)? {
            Some(row) => Ok(Some(row.get::<i64>(0).map_err(store_err)? != 0)),
            None => Ok(None),
        }
    }

    /// Lanes of the session excluding sentinels; `unresolved_only` narrows to unresolved lanes.
    async fn count_lanes(
        &self,
        session_id: &SessionId,
        unresolved_only: bool,
    ) -> Result<i64, NexusError> {
        let mut rows = self
            .store
            .stream_conn()
            .query(
                "SELECT COUNT(*) FROM mem.child_stream_lanes \
                 WHERE session_id = ?1 AND child_key NOT LIKE 's:%' AND (?2 = 0 OR unresolved = 1)",
                params![session_id.0.clone(), i64::from(unresolved_only)],
            )
            .await
            .map_err(store_err)?;
        let row = rows
            .next()
            .await
            .map_err(store_err)?
            .ok_or_else(|| NexusError::Store("count child lanes".into()))?;
        row.get(0).map_err(store_err)
    }

    async fn count_refusal(
        &self,
        session_id: &SessionId,
        epoch: &str,
        ts: i64,
    ) -> Result<(), NexusError> {
        let sentinel = ChildStream {
            harness: SENTINEL_HARNESS.into(),
            root: SENTINEL_ROOT.into(),
            id: None,
            locator: REFUSED_SENTINEL_KEY.into(),
            parent: None,
            parent_ref: None,
            depth: None,
            resolution: ChildResolution::Unresolved,
            evidence: None,
        };
        let child_json = serialize_child(&sentinel)?;
        self.store
            .stream_conn()
            .execute(
                "INSERT INTO mem.child_stream_lanes \
                 (session_id, harness, root, child_key, epoch, child, unresolved, first_seen_id, refused_rows, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1, 0, 1, ?7) \
                 ON CONFLICT(session_id, harness, root, child_key) DO UPDATE SET \
                   refused_rows = refused_rows + 1, epoch = excluded.epoch, updated_at = excluded.updated_at",
                params![
                    session_id.0.clone(),
                    SENTINEL_HARNESS.to_string(),
                    SENTINEL_ROOT.to_string(),
                    REFUSED_SENTINEL_KEY.to_string(),
                    epoch.to_string(),
                    child_json,
                    ts
                ],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Delete the oldest tombstone; the lane delete trigger folds its counters into the session
    /// loss row inside the same statement. A tombstone is a non-sentinel lane that once held rows
    /// (or had evictions) and holds none now; a lane that only declared coverage is not one.
    /// Returns `false` when nothing is compactable.
    async fn compact_one_tombstone(&self, session_id: &SessionId) -> Result<bool, NexusError> {
        let conn = self.store.stream_conn();
        let mut rows = conn
            .query(
                "SELECT harness, root, child_key \
                 FROM mem.child_stream_lanes l \
                 WHERE l.session_id = ?1 AND l.child_key NOT LIKE 's:%' \
                   AND (l.first_seen_id > 0 OR l.evicted_rows > 0) \
                   AND NOT EXISTS ( \
                     SELECT 1 FROM mem.child_stream_events e \
                     WHERE e.session_id = l.session_id AND e.harness = l.harness AND e.root = l.root AND e.child_key = l.child_key) \
                 ORDER BY l.first_seen_id ASC LIMIT 1",
                params![session_id.0.clone()],
            )
            .await
            .map_err(store_err)?;
        let Some(row) = rows.next().await.map_err(store_err)? else {
            return Ok(false);
        };
        let harness: String = row.get(0).map_err(store_err)?;
        let root: String = row.get(1).map_err(store_err)?;
        let key: String = row.get(2).map_err(store_err)?;
        drop(rows);
        conn.execute(
            "DELETE FROM mem.child_stream_lanes \
             WHERE session_id = ?1 AND harness = ?2 AND root = ?3 AND child_key = ?4",
            params![session_id.0.clone(), harness, root, key],
        )
        .await
        .map_err(store_err)?;
        Ok(true)
    }

    async fn session_totals(&self, session_id: &SessionId) -> Result<(i64, i64), NexusError> {
        let mut rows = self
            .store
            .stream_conn()
            .query(
                "SELECT COUNT(*), COALESCE(SUM(bytes), 0) FROM mem.child_stream_events WHERE session_id = ?1",
                params![session_id.0.clone()],
            )
            .await
            .map_err(store_err)?;
        let row = rows
            .next()
            .await
            .map_err(store_err)?
            .ok_or_else(|| NexusError::Store("child lane totals".into()))?;
        Ok((
            row.get(0).map_err(store_err)?,
            row.get(1).map_err(store_err)?,
        ))
    }

    /// Evict oldest-first, one statement per batch, until an event of `incoming_bytes` fits
    /// within the session's row and byte bounds. Each delete statement carries its own
    /// accounting through the delete trigger. A failing batch changed nothing; earlier batches
    /// stay committed and accounted.
    async fn make_room(
        &self,
        session_id: &SessionId,
        incoming_bytes: i64,
        bounds: &ChildStreamBounds,
    ) -> Result<(), NexusError> {
        let max_rows = i64::from(bounds.max_rows_per_session.max(1));
        let max_bytes = i64::try_from(bounds.max_bytes_per_session).unwrap_or(i64::MAX);
        loop {
            let (rows_total, bytes_total) = self.session_totals(session_id).await?;
            let over_rows = rows_total + 1 > max_rows;
            let over_bytes = bytes_total + incoming_bytes > max_bytes;
            if (!over_rows && !over_bytes) || rows_total == 0 {
                return Ok(());
            }
            let wanted = if over_rows {
                rows_total + 1 - max_rows
            } else {
                1
            };
            let batch = wanted.clamp(1, EVICTION_BATCH);
            self.store
                .stream_conn()
                .execute(
                    "DELETE FROM mem.child_stream_events WHERE id IN ( \
                       SELECT id FROM mem.child_stream_events WHERE session_id = ?1 \
                       ORDER BY id ASC LIMIT ?2)",
                    params![session_id.0.clone(), batch],
                )
                .await
                .map_err(store_err)?;
        }
    }
}

fn serialize_child(child: &ChildStream) -> Result<String, NexusError> {
    serde_json::to_string(child)
        .map_err(|e| NexusError::Store(format!("serialize child stream: {e}")))
}

fn parse_child(json: &str) -> Result<ChildStream, NexusError> {
    serde_json::from_str(json).map_err(|e| NexusError::Store(format!("invalid child stream: {e}")))
}
