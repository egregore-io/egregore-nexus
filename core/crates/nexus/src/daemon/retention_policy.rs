//! Operational retention policy for daemon-owned queue/cache tables.
//!
//! The durable conversation/search substrate (`messages`, `threads`) is product history and is
//! intentionally exempt. This module reaps bounded operational residue — terminal command rows,
//! consumed subscription batches, acked/error delivery rows, old notification audit rows,
//! developer-event telemetry rows, stopped-runtime sidecars, and the materialized `/agent`
//! transcript tail: finalized `agent_session_messages`/
//! `agent_session_turns` rows older than the retention window (48h default,
//! `NEXUS_MATERIALIZER_RETENTION_MS`, `0` disables), deleted in bounded chunks.

use libsql::params;

use nexus_common::{now, NexusError};
use nexus_contracts::EventSink;
use nexus_store::{
    repos::{
        inbox::{DeadLetterMutation, Inbox, DELIVERY_TIMEOUT_REASON, DELIVERY_TIMEOUT_TTL_MS},
        AgentRuntimes, CommandIntents, DeveloperEvents, NewDeveloperEvent, AGENT_LIFECYCLE_TOPIC,
    },
    Store,
};

use crate::daemon::app::AppState;

const DEFAULT_REAP_INTERVAL_MS: i64 = 24 * 60 * 60 * 1_000;
const DELIVERY_TIMEOUT_SWEEP_INTERVAL_MS: i64 = 60 * 1_000;
const MB: i64 = 1_024 * 1_024;
/// Default materialized `/agent` transcript retention: 48 hours. An unbounded backlog can grow
/// rapidly, and the resulting
/// store size and write churn can stall the write lane.
pub const DEFAULT_MATERIALIZER_RETENTION_MS: i64 = 48 * 60 * 60 * 1_000;
/// Materializer reap deletes run in bounded batches so the first sweep over a large backlog
/// never issues one giant write transaction (large writes can stall the durable lane).
const MATERIALIZER_REAP_CHUNK: i64 = 5_000;

/// Mutable state for deciding when the next operational retention sweep should run.
#[derive(Debug, Default)]
pub(crate) struct RetentionPolicyState {
    last_reap_at_ms: i64,
    last_delivery_timeout_at_ms: i64,
    was_over_size_threshold: bool,
}

/// Static retention settings loaded from daemon config.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperationalRetentionPolicy {
    pub retention_ms: i64,
    /// Materialized `/agent` history retention (`NEXUS_MATERIALIZER_RETENTION_MS`).
    /// Defaults to [`DEFAULT_MATERIALIZER_RETENTION_MS`] (48h); an explicit `<= 0`
    /// disables the materializer reap. Reaped rows are finalized turn transcripts —
    /// `/agent` scrollback older than the window is discarded; `messages`/`threads`
    /// product history is never touched.
    pub materializer_retention_ms: i64,
    pub interval_ms: i64,
    pub size_threshold_mb: i64,
    /// Delivery TTL before an undelivered `in_flight` row is dead-lettered
    /// (`NEXUS_DELIVERY_TTL_MS`). Defaults to [`DELIVERY_TIMEOUT_TTL_MS`] (30 min);
    /// values `<= 0` or unparsable keep the default. Override exists for the
    /// load-regression gate, which needs the TTL→DLQ path observable in minutes.
    pub delivery_timeout_ttl_ms: i64,
    /// Cadence of the delivery-timeout sweep (`NEXUS_DELIVERY_SWEEP_INTERVAL_MS`).
    /// Defaults to 60s; same fallback rules as the TTL. Gate-only knob.
    pub delivery_sweep_interval_ms: i64,
}

/// Parse an env-provided millisecond knob: positive integers win, anything else
/// (unset, junk, zero, negative) falls back to the production default.
fn env_ms_or(raw: Option<&str>, default: i64) -> i64 {
    match raw.and_then(|v| v.parse::<i64>().ok()) {
        Some(ms) if ms > 0 => ms,
        _ => default,
    }
}

/// Resolve `NEXUS_MATERIALIZER_RETENTION_MS`: unset/junk → the 48h default; an explicit
/// integer wins verbatim, so `0` (or negative) is the documented way to disable the reap.
pub fn materializer_retention_from_env(raw: Option<&str>) -> i64 {
    match raw.map(str::trim).and_then(|v| v.parse::<i64>().ok()) {
        Some(ms) => ms,
        None => DEFAULT_MATERIALIZER_RETENTION_MS,
    }
}

/// Counts from one operational retention sweep.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RetentionSweep {
    pub command_intents: u64,
    pub delivery_timeouts: u64,
    pub materialized_turns: u64,
    pub materialized_messages: u64,
    pub initial_prompt_deliveries: u64,
    pub inbox_subscription_batches: u64,
    pub inbox_subscriptions: u64,
    pub in_flight: u64,
    pub notifications: u64,
    pub developer_events: u64,
    pub runtime_sidecars: u64,
    pub producer_identities: u64,
    pub agent_runtimes: u64,
}

impl RetentionSweep {
    fn total(self) -> u64 {
        self.command_intents
            + self.delivery_timeouts
            + self.materialized_messages
            + self.materialized_turns
            + self.initial_prompt_deliveries
            + self.inbox_subscription_batches
            + self.inbox_subscriptions
            + self.in_flight
            + self.notifications
            + self.developer_events
            + self.runtime_sidecars
            + self.producer_identities
            + self.agent_runtimes
    }

    fn add(&mut self, other: RetentionSweep) {
        self.command_intents += other.command_intents;
        self.delivery_timeouts += other.delivery_timeouts;
        self.materialized_messages += other.materialized_messages;
        self.materialized_turns += other.materialized_turns;
        self.initial_prompt_deliveries += other.initial_prompt_deliveries;
        self.inbox_subscription_batches += other.inbox_subscription_batches;
        self.inbox_subscriptions += other.inbox_subscriptions;
        self.in_flight += other.in_flight;
        self.notifications += other.notifications;
        self.developer_events += other.developer_events;
        self.runtime_sidecars += other.runtime_sidecars;
        self.producer_identities += other.producer_identities;
        self.agent_runtimes += other.agent_runtimes;
    }
}

impl OperationalRetentionPolicy {
    fn from_state(state: &AppState) -> Self {
        Self {
            retention_ms: state.command_intent_retention_ms(),
            materializer_retention_ms: materializer_retention_from_env(
                std::env::var("NEXUS_MATERIALIZER_RETENTION_MS")
                    .ok()
                    .as_deref(),
            ),
            interval_ms: state.reap_interval_ms(),
            size_threshold_mb: state.reap_size_threshold_mb(),
            delivery_timeout_ttl_ms: env_ms_or(
                std::env::var("NEXUS_DELIVERY_TTL_MS").ok().as_deref(),
                DELIVERY_TIMEOUT_TTL_MS,
            ),
            delivery_sweep_interval_ms: env_ms_or(
                std::env::var("NEXUS_DELIVERY_SWEEP_INTERVAL_MS")
                    .ok()
                    .as_deref(),
                DELIVERY_TIMEOUT_SWEEP_INTERVAL_MS,
            ),
        }
    }

    fn cutoff(self, ts: i64) -> Option<i64> {
        if self.retention_ms < 0 {
            None
        } else {
            Some(ts.saturating_sub(self.retention_ms))
        }
    }

    fn threshold_bytes(self) -> Option<i64> {
        if self.size_threshold_mb <= 0 {
            None
        } else {
            Some(self.size_threshold_mb.saturating_mul(MB))
        }
    }
}

impl RetentionPolicyState {
    fn should_run(
        &mut self,
        policy: OperationalRetentionPolicy,
        ts: i64,
        db_size_bytes: Option<i64>,
    ) -> bool {
        let over_size_threshold = match (policy.threshold_bytes(), db_size_bytes) {
            (Some(threshold), Some(size)) => size >= threshold,
            _ => false,
        };
        let crossed_size_threshold = over_size_threshold && !self.was_over_size_threshold;
        self.was_over_size_threshold = over_size_threshold;

        let interval_due = self.last_reap_at_ms == 0
            || ts.saturating_sub(self.last_reap_at_ms) >= policy.interval_ms.max(1);
        if crossed_size_threshold || interval_due {
            self.last_reap_at_ms = ts;
            return true;
        }
        false
    }

    fn should_run_delivery_timeouts(
        &mut self,
        policy: OperationalRetentionPolicy,
        ts: i64,
    ) -> bool {
        let due = self.last_delivery_timeout_at_ms == 0
            || ts.saturating_sub(self.last_delivery_timeout_at_ms)
                >= policy.delivery_sweep_interval_ms;
        if due {
            self.last_delivery_timeout_at_ms = ts;
        }
        due
    }
}

/// Parse `NEXUS_REAP_INTERVAL`. Bare numbers are milliseconds; suffixes `ms`, `s`, `m`, `h`,
/// `d`, and `w` are accepted, along with `daily` and `weekly`.
pub fn parse_reap_interval_ms(raw: &str) -> i64 {
    let trimmed = raw.trim().to_ascii_lowercase();
    if trimmed.is_empty() || trimmed == "daily" {
        return DEFAULT_REAP_INTERVAL_MS;
    }
    if trimmed == "weekly" {
        return 7 * DEFAULT_REAP_INTERVAL_MS;
    }

    let (number, multiplier) = if let Some(n) = trimmed.strip_suffix("ms") {
        (n, 1)
    } else if let Some(n) = trimmed.strip_suffix('s') {
        (n, 1_000)
    } else if let Some(n) = trimmed.strip_suffix('m') {
        (n, 60 * 1_000)
    } else if let Some(n) = trimmed.strip_suffix('h') {
        (n, 60 * 60 * 1_000)
    } else if let Some(n) = trimmed.strip_suffix('d') {
        (n, DEFAULT_REAP_INTERVAL_MS)
    } else if let Some(n) = trimmed.strip_suffix('w') {
        (n, 7 * DEFAULT_REAP_INTERVAL_MS)
    } else {
        (trimmed.as_str(), 1)
    };

    number
        .trim()
        .parse::<i64>()
        .ok()
        .and_then(|n| n.checked_mul(multiplier))
        .filter(|n| *n > 0)
        .unwrap_or(DEFAULT_REAP_INTERVAL_MS)
}

/// Run operational retention when the configured interval is due or the store crosses the size
/// threshold. Size probing is best-effort so remote/libSQL deployments still get interval cleanup.
pub(crate) async fn maybe_reap_operational_tables(
    state: &AppState,
    run_state: &mut RetentionPolicyState,
) -> Result<Option<RetentionSweep>, NexusError> {
    let policy = OperationalRetentionPolicy::from_state(state);
    let ts = now();
    let mut sweep = RetentionSweep::default();
    if run_state.should_run_delivery_timeouts(policy, ts) {
        let mutation = sweep_delivery_timeouts(state, ts, policy.delivery_timeout_ttl_ms).await?;
        sweep.delivery_timeouts = mutation.count;
    }
    let db_size_bytes = match main_db_size_bytes(&state.store).await {
        Ok(size) => size,
        Err(err) => {
            tracing::debug!(error = %err, "could not read store size for retention threshold");
            None
        }
    };
    if !run_state.should_run(policy, ts, db_size_bytes) {
        return if sweep.total() > 0 {
            log_retention_sweep(sweep, policy);
            Ok(Some(sweep))
        } else {
            Ok(None)
        };
    }

    sweep.add(reap_operational_tables_with_presence(&state.store, policy, ts, Some(state)).await?);
    if sweep.total() > 0 {
        log_retention_sweep(sweep, policy);
    }
    Ok(Some(sweep))
}

pub async fn reap_operational_tables(
    store: &Store,
    policy: OperationalRetentionPolicy,
    ts: i64,
) -> Result<RetentionSweep, NexusError> {
    reap_operational_tables_with_presence(store, policy, ts, None).await
}

async fn reap_operational_tables_with_presence(
    store: &Store,
    policy: OperationalRetentionPolicy,
    ts: i64,
    state: Option<&AppState>,
) -> Result<RetentionSweep, NexusError> {
    let Some(cutoff) = policy.cutoff(ts) else {
        return Ok(RetentionSweep::default());
    };

    let mut sweep = RetentionSweep {
        command_intents: CommandIntents::new(store)
            .reap_terminal_older_than(cutoff)
            .await?,
        ..RetentionSweep::default()
    };

    let delivery_timeouts = match state {
        Some(state) => sweep_delivery_timeouts(state, ts, policy.delivery_timeout_ttl_ms).await?,
        None => dead_letter_delivery_timeouts(store, ts, policy.delivery_timeout_ttl_ms).await?,
    };
    sweep.delivery_timeouts = delivery_timeouts.count;

    sweep.initial_prompt_deliveries = store
        .identity_conn()
        .execute(
            "DELETE FROM initial_prompt_deliveries
             WHERE status IN ('accepted', 'failed')
               AND COALESCE(accepted_at_ms, failed_at_ms, created_at_ms) <= ?1
               AND NOT EXISTS (
                 SELECT 1 FROM agent_runtimes r
                 WHERE r.runtime_id = initial_prompt_deliveries.runtime_id
                   AND r.active = 1
                   AND r.stopped_at IS NULL
               )",
            params![cutoff],
        )
        .await
        .map_err(sql_err)?;

    sweep.inbox_subscription_batches = store
        .conn
        .execute(
            "DELETE FROM inbox_subscription_batches
             WHERE status = 'consumed'
               AND consumed_at IS NOT NULL
               AND consumed_at <= ?1",
            params![cutoff],
        )
        .await
        .map_err(sql_err)?;

    sweep.inbox_subscriptions = store
        .conn
        .execute(
            "DELETE FROM inbox_subscriptions
             WHERE status = 'inactive'
               AND updated_at <= ?1
               AND NOT EXISTS (
                 SELECT 1 FROM inbox_subscription_batches b
                 WHERE b.subscription_id = inbox_subscriptions.subscription_id
               )",
            params![cutoff],
        )
        .await
        .map_err(sql_err)?;

    sweep.in_flight = store
        .conn
        .execute(
            "DELETE FROM in_flight
             WHERE (state = 'acked' AND acked_at IS NOT NULL AND acked_at <= ?1)
                OR (
                  state = 'error'
                  AND EXISTS (
                    SELECT 1 FROM messages m
                    WHERE m.message_id = in_flight.message_id
                      AND m.created_at IS NOT NULL
                      AND m.created_at <= ?1
                  )
                )",
            params![cutoff],
        )
        .await
        .map_err(sql_err)?;

    if !store.has_split_authority() && policy.materializer_retention_ms > 0 {
        let m_cutoff = ts.saturating_sub(policy.materializer_retention_ms);
        // Projections only, and never a live turn: terminal turns + their messages older
        // than the window. Order matters (messages first — turn_id FK-by-convention).
        // Chunked so a large backlog reaps as many small autocommit writes, never one
        // giant transaction.
        sweep.materialized_messages = delete_chunked(
            store,
            "DELETE FROM agent_session_messages
             WHERE id IN (
               SELECT id FROM agent_session_messages
               WHERE COALESCE(finalized_at, updated_at, created_at) <= ?1
                 AND status <> 'streaming'
               LIMIT ?2
             )",
            m_cutoff,
        )
        .await?;
        sweep.materialized_turns = delete_chunked(
            store,
            "DELETE FROM agent_session_turns
             WHERE id IN (
               SELECT id FROM agent_session_turns
               WHERE status <> 'streaming'
                 AND COALESCE(finalized_at, updated_at, started_at) <= ?1
               LIMIT ?2
             )",
            m_cutoff,
        )
        .await?;
    }

    if !store.has_split_authority() {
        sweep.notifications = store
            .conn
            .execute(
                "DELETE FROM notifications
                 WHERE created_at IS NOT NULL
                   AND created_at <= ?1",
                params![cutoff],
            )
            .await
            .map_err(sql_err)?;
    }

    sweep.developer_events = store
        .conn
        .execute(
            "DELETE FROM developer_events
             WHERE created_at IS NOT NULL
               AND created_at <= ?1",
            params![cutoff],
        )
        .await
        .map_err(sql_err)?;

    if !store.has_split_authority() {
        sweep.runtime_sidecars +=
            delete_sidecar_for_stopped_runtimes(store, "claude_runtime_state", cutoff).await?;
        sweep.runtime_sidecars +=
            delete_sidecar_for_stopped_runtimes(store, "codex_runtime_state", cutoff).await?;
        sweep.runtime_sidecars +=
            delete_sidecar_for_stopped_runtimes(store, "opencode_runtime_state", cutoff).await?;
        sweep.runtime_sidecars +=
            delete_sidecar_for_stopped_runtimes(store, "hermes_runtime_state", cutoff).await?;
    }

    sweep.producer_identities = store
        .identity_conn()
        .execute(
            "DELETE FROM producer_identities
             WHERE EXISTS (
               SELECT 1 FROM agent_runtimes r
               WHERE r.runtime_id = producer_identities.runtime_id
                 AND r.active = 0
                 AND r.stopped_at IS NOT NULL
                 AND r.stopped_at <= ?1
             )",
            params![cutoff],
        )
        .await
        .map_err(sql_err)?;

    sweep.agent_runtimes = match state.map(|state| &state.presence) {
        Some(presence) => presence
            .reap_runtime_retention(cutoff)
            .await
            .map_err(|cause| {
                NexusError::Store(format!(
                    "{cause}; earlier operational cleanup may already have committed"
                ))
            })?,
        None => {
            AgentRuntimes::new(store)
                .reap_retention_unmanaged(cutoff)
                .await?
        }
    };

    Ok(sweep)
}

/// Run a `DELETE ... WHERE id IN (SELECT ... LIMIT ?2)` statement repeatedly until it stops
/// making progress, so no single write transaction exceeds [`MATERIALIZER_REAP_CHUNK`] rows.
async fn delete_chunked(store: &Store, sql: &str, cutoff: i64) -> Result<u64, NexusError> {
    let mut total = 0u64;
    loop {
        let affected = store
            .conn
            .execute(sql, params![cutoff, MATERIALIZER_REAP_CHUNK])
            .await
            .map_err(sql_err)?;
        total += affected;
        if (affected as i64) < MATERIALIZER_REAP_CHUNK {
            return Ok(total);
        }
    }
}

async fn delete_sidecar_for_stopped_runtimes(
    store: &Store,
    table: &str,
    cutoff: i64,
) -> Result<u64, NexusError> {
    if !table_exists(store, table).await? {
        return Ok(0);
    }
    let sql = format!(
        "DELETE FROM {table}
         WHERE EXISTS (
           SELECT 1 FROM agent_runtimes r
           WHERE r.runtime_id = {table}.runtime_id
             AND r.active = 0
             AND r.stopped_at IS NOT NULL
             AND r.stopped_at <= ?1
         )"
    );
    store
        .conn
        .execute(&sql, params![cutoff])
        .await
        .map_err(sql_err)
}

async fn table_exists(store: &Store, table: &str) -> Result<bool, NexusError> {
    let mut rows = store
        .conn
        .query(
            "SELECT 1 FROM sqlite_master
             WHERE type = 'table' AND name = ?1
             LIMIT 1",
            params![table],
        )
        .await
        .map_err(sql_err)?;
    Ok(rows.next().await.map_err(sql_err)?.is_some())
}

async fn main_db_size_bytes(store: &Store) -> Result<Option<i64>, NexusError> {
    let Some(page_count) = pragma_i64(store, "PRAGMA page_count").await? else {
        return Ok(None);
    };
    let Some(page_size) = pragma_i64(store, "PRAGMA page_size").await? else {
        return Ok(None);
    };
    Ok(Some(page_count.saturating_mul(page_size)))
}

async fn pragma_i64(store: &Store, sql: &str) -> Result<Option<i64>, NexusError> {
    let mut rows = store.conn.query(sql, ()).await.map_err(sql_err)?;
    match rows.next().await.map_err(sql_err)? {
        Some(row) => row.get::<i64>(0).map(Some).map_err(sql_err),
        None => Ok(None),
    }
}

fn sql_err(error: libsql::Error) -> NexusError {
    NexusError::Store(error.to_string())
}

fn log_retention_sweep(sweep: RetentionSweep, policy: OperationalRetentionPolicy) {
    tracing::info!(
        command_intents = sweep.command_intents,
        delivery_timeouts = sweep.delivery_timeouts,
        materialized_messages = sweep.materialized_messages,
        materialized_turns = sweep.materialized_turns,
        initial_prompt_deliveries = sweep.initial_prompt_deliveries,
        inbox_subscription_batches = sweep.inbox_subscription_batches,
        inbox_subscriptions = sweep.inbox_subscriptions,
        in_flight = sweep.in_flight,
        notifications = sweep.notifications,
        developer_events = sweep.developer_events,
        runtime_sidecars = sweep.runtime_sidecars,
        producer_identities = sweep.producer_identities,
        agent_runtimes = sweep.agent_runtimes,
        retention_ms = policy.retention_ms,
        size_threshold_mb = policy.size_threshold_mb,
        "reaped operational rows"
    );
}

/// Run the daemon's timeout settlement path at the supplied maintenance timestamp.
#[doc(hidden)]
pub async fn sweep_delivery_timeouts(
    state: &AppState,
    ts: i64,
    ttl_ms: i64,
) -> Result<DeadLetterMutation, NexusError> {
    let mutation = dead_letter_delivery_timeouts(&state.store, ts, ttl_ms).await?;
    // Settlement has committed before projection. A projection/read failure must never reverse
    // it or re-admit the input. As with other RAM-backed effects, a process crash in this gap
    // can lose the projection; there is no durable cross-authority outbox here. These are current
    // terminal facts: an intervening explicit operator requeue/purge can remove a fact before
    // this read. Never project a newly pending revision as the old timeout transition.
    for effect in Inbox::new(&state.store)
        .gateway_delivery_effects_for_ids(&mutation.in_flight_ids)
        .await?
    {
        state.ws.project(effect).await;
    }
    Ok(mutation)
}

async fn dead_letter_delivery_timeouts(
    store: &Store,
    ts: i64,
    ttl_ms: i64,
) -> Result<DeadLetterMutation, NexusError> {
    let mutation = Inbox::new(store)
        .dead_letter_expired_deliveries(ts, ttl_ms)
        .await?;
    append_delivery_timeout_event(store, &mutation, ts).await;
    Ok(mutation)
}

async fn append_delivery_timeout_event(store: &Store, mutation: &DeadLetterMutation, ts: i64) {
    if mutation.count == 0 {
        return;
    }
    let data = serde_json::json!({
        "count": mutation.count,
        "inFlightIds": mutation.in_flight_ids,
        "errorReason": DELIVERY_TIMEOUT_REASON,
    })
    .to_string();
    if let Err(error) = DeveloperEvents::new(store)
        .append(NewDeveloperEvent {
            topic: AGENT_LIFECYCLE_TOPIC.to_string(),
            kind: "agent_lifecycle".to_string(),
            message_id: None,
            thread_name: None,
            dm_name: None,
            from_name: None,
            agent_name: None,
            session_id: None,
            lifecycle: Some("dlq.timeout".to_string()),
            current_work: None,
            data_json: Some(data),
            created_at: ts,
        })
        .await
    {
        tracing::warn!(
            error = %error,
            lifecycle = "dlq.timeout",
            "dlq delivery-timeout developer-event telemetry append failed"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delivery_knob_parsing_defaults_and_overrides() {
        assert_eq!(
            env_ms_or(None, DELIVERY_TIMEOUT_TTL_MS),
            DELIVERY_TIMEOUT_TTL_MS
        );
        assert_eq!(env_ms_or(Some("10000"), DELIVERY_TIMEOUT_TTL_MS), 10_000);
        // Zero, negative, and junk all fall back to the production default.
        assert_eq!(
            env_ms_or(Some("0"), DELIVERY_TIMEOUT_TTL_MS),
            DELIVERY_TIMEOUT_TTL_MS
        );
        assert_eq!(
            env_ms_or(Some("-5"), DELIVERY_TIMEOUT_TTL_MS),
            DELIVERY_TIMEOUT_TTL_MS
        );
        assert_eq!(
            env_ms_or(Some("junk"), DELIVERY_TIMEOUT_TTL_MS),
            DELIVERY_TIMEOUT_TTL_MS
        );
    }

    #[test]
    fn delivery_sweep_interval_is_policy_driven() {
        let policy = OperationalRetentionPolicy {
            retention_ms: 100,
            interval_ms: 60_000,
            size_threshold_mb: 0,
            materializer_retention_ms: 0,
            delivery_timeout_ttl_ms: DELIVERY_TIMEOUT_TTL_MS,
            delivery_sweep_interval_ms: 5_000,
        };
        let mut state = RetentionPolicyState::default();
        assert!(
            state.should_run_delivery_timeouts(policy, 1_000),
            "first tick always due"
        );
        assert!(
            !state.should_run_delivery_timeouts(policy, 5_999),
            "inside the window"
        );
        assert!(
            state.should_run_delivery_timeouts(policy, 6_000),
            "window elapsed"
        );
    }

    #[tokio::test]
    async fn materializer_reap_disabled_at_zero_and_windowed_when_on() {
        let store = Store::open(":memory:").await.unwrap();
        store.migrate().await.unwrap();
        store.conn.execute(
            "INSERT INTO agent_session_turns (id, session_id, status, first_stream_event_id, last_stream_event_id, started_at, updated_at, finalized_at) \
             VALUES ('t_old','s_x','completed', 1, 1, 1, 1, 1), ('t_new','s_x','completed', 1, 1, ?1, ?1, ?1), \
                    ('t_live','s_x','streaming', 1, 1, 1, 1, NULL)",
            params![i64::MAX / 2],
        ).await.unwrap();
        store.conn.execute(
            "INSERT INTO agent_session_messages (id, session_id, turn_id, ordinal, role, author, content_json, status, first_stream_event_id, last_stream_event_id, created_at, updated_at) \
             VALUES ('m_old','s_x','t_old',0,'assistant','a','{}','completed', 1, 1, 1, 1)",
            (),
        ).await.unwrap();

        // Explicitly disabled (materializer_retention_ms = 0): nothing materialized is touched.
        let off = OperationalRetentionPolicy {
            retention_ms: 10,
            interval_ms: 0,
            size_threshold_mb: 0,
            materializer_retention_ms: 0,
            delivery_timeout_ttl_ms: DELIVERY_TIMEOUT_TTL_MS,
            delivery_sweep_interval_ms: DELIVERY_TIMEOUT_SWEEP_INTERVAL_MS,
        };
        let sweep = reap_operational_tables(&store, off, 1_000_000)
            .await
            .unwrap();
        assert_eq!(sweep.materialized_turns, 0);
        assert_eq!(sweep.materialized_messages, 0);

        // Enabled: old terminal rows go, streaming + in-window rows stay.
        let on = OperationalRetentionPolicy {
            retention_ms: 10,
            interval_ms: 0,
            size_threshold_mb: 0,
            materializer_retention_ms: 100,
            delivery_timeout_ttl_ms: DELIVERY_TIMEOUT_TTL_MS,
            delivery_sweep_interval_ms: DELIVERY_TIMEOUT_SWEEP_INTERVAL_MS,
        };
        let sweep = reap_operational_tables(&store, on, 1_000_000)
            .await
            .unwrap();
        assert_eq!(sweep.materialized_messages, 1, "old message reaped");
        assert_eq!(sweep.materialized_turns, 1, "old terminal turn reaped");
        let mut rows = store
            .conn
            .query("SELECT id FROM agent_session_turns ORDER BY id", ())
            .await
            .unwrap();
        let mut left = Vec::new();
        while let Some(row) = rows.next().await.unwrap() {
            left.push(row.get::<String>(0).unwrap());
        }
        assert_eq!(left, vec!["t_live".to_string(), "t_new".to_string()]);
    }
    use std::sync::Arc;

    use nexus_common::Config;

    async fn migrated() -> Store {
        let store = Store::open(":memory:").await.unwrap();
        store.migrate().await.unwrap();
        store
    }

    async fn count_where(store: &Store, table: &str, where_sql: &str) -> i64 {
        let sql = format!("SELECT COUNT(*) FROM {table} WHERE {where_sql}");
        let mut rows = store.conn.query(&sql, ()).await.unwrap();
        let row = rows.next().await.unwrap().unwrap();
        row.get::<i64>(0).unwrap()
    }

    async fn insert_command_intent(
        store: &Store,
        command_id: &str,
        status: &str,
        completed_at: Option<i64>,
    ) {
        store
            .conn
            .execute(
                "INSERT INTO command_intents (
                   command_id, kind, status, project, caller_name, request_json,
                   attempts, created_at, completed_at
                 ) VALUES (?1, 'test.intent', ?2, 'p', 'tester', '{}', 0, 100, ?3)",
                params![command_id, status, completed_at],
            )
            .await
            .unwrap();
    }

    async fn create_sidecar_tables(store: &Store) {
        for table in [
            "claude_runtime_state",
            "codex_runtime_state",
            "opencode_runtime_state",
            "hermes_runtime_state",
        ] {
            store
                .conn
                .execute(
                    &format!("CREATE TABLE {table} (runtime_id TEXT PRIMARY KEY)"),
                    (),
                )
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn parses_reap_intervals() {
        assert_eq!(parse_reap_interval_ms("daily"), DEFAULT_REAP_INTERVAL_MS);
        assert_eq!(
            parse_reap_interval_ms("weekly"),
            7 * DEFAULT_REAP_INTERVAL_MS
        );
        assert_eq!(parse_reap_interval_ms("15m"), 15 * 60 * 1_000);
        assert_eq!(parse_reap_interval_ms("2h"), 2 * 60 * 60 * 1_000);
        assert_eq!(parse_reap_interval_ms("250ms"), 250);
        assert_eq!(parse_reap_interval_ms("bad"), DEFAULT_REAP_INTERVAL_MS);
    }

    #[tokio::test]
    async fn size_threshold_runs_on_crossing_without_busy_looping() {
        let policy = OperationalRetentionPolicy {
            retention_ms: 100,
            interval_ms: 60_000,
            size_threshold_mb: 1,
            materializer_retention_ms: 0,
            delivery_timeout_ttl_ms: DELIVERY_TIMEOUT_TTL_MS,
            delivery_sweep_interval_ms: DELIVERY_TIMEOUT_SWEEP_INTERVAL_MS,
        };
        let mut state = RetentionPolicyState {
            last_reap_at_ms: 1_000,
            last_delivery_timeout_at_ms: 0,
            was_over_size_threshold: false,
        };

        assert!(state.should_run(policy, 1_100, Some(2 * MB)));
        assert!(!state.should_run(policy, 1_200, Some(2 * MB)));
        assert!(!state.should_run(policy, 1_300, Some(MB / 2)));
        assert!(state.should_run(policy, 1_400, Some(2 * MB)));
    }

    #[tokio::test]
    async fn reaps_operational_rows_without_deleting_product_history() {
        let store = migrated().await;
        create_sidecar_tables(&store).await;

        insert_command_intent(&store, "cmd_old_done", "done", Some(800)).await;
        insert_command_intent(&store, "cmd_recent_error", "error", Some(950)).await;
        insert_command_intent(&store, "cmd_pending", "pending", None).await;

        store
            .conn
            .execute(
                "INSERT INTO messages (message_id, from_name, kind, body, project, created_at)
                 VALUES ('m_old', 'ada', 'dm', 'keep product history', 'p', 100),
                        ('m_recent', 'ada', 'dm', 'recent', 'p', 950)",
                (),
            )
            .await
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO in_flight (
                   in_flight_id, message_id, recipient_session, state, acked_at
                 ) VALUES ('if_old_acked', 'm_old', 's_old', 'acked', 800),
                          ('if_recent_acked', 'm_recent', 's_recent', 'acked', 950),
                          ('if_pending', 'm_old', 's_pending', 'pending', NULL),
                          ('if_old_error', 'm_old', 's_error', 'error', NULL)",
                (),
            )
            .await
            .unwrap();

        store
            .conn
            .execute(
                "INSERT INTO notifications (notif_id, source, topic, hmac_ok, payload, routed_to, created_at)
                 VALUES ('n_old', 'test', 't', 1, '{}', '[]', 800),
                        ('n_recent', 'test', 't', 1, '{}', '[]', 950)",
                (),
            )
            .await
            .unwrap();

        store
            .conn
            .execute(
                "INSERT INTO developer_event_topics (topic, latest_seq, updated_at)
                 VALUES ('sys.agent.lifecycle', 2, 950)",
                (),
            )
            .await
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO developer_events (
                   topic, seq, kind, agent_name, session_id, lifecycle, created_at
                 ) VALUES
                   ('sys.agent.lifecycle', 1, 'agent_lifecycle', 'ada', 's_ada', 'attach', 800),
                   ('sys.agent.lifecycle', 2, 'agent_lifecycle', 'ada', 's_ada', 'resume', 950)",
                (),
            )
            .await
            .unwrap();

        store
            .conn
            .execute(
                "INSERT INTO agent_runtimes (runtime_id, agent_id, harness, active, started_at, stopped_at)
                 VALUES ('rt_old', 'a_old', 'codex', 0, 100, 800),
                        ('rt_active', 'a_active', 'codex', 1, 100, NULL)",
                (),
            )
            .await
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO producer_identities (runtime_id, producer_id, created_at, updated_at)
                 VALUES ('rt_old', 'p_old', 100, 100),
                        ('rt_active', 'p_active', 100, 100)",
                (),
            )
            .await
            .unwrap();
        for table in [
            "claude_runtime_state",
            "codex_runtime_state",
            "opencode_runtime_state",
            "hermes_runtime_state",
        ] {
            store
                .conn
                .execute(
                    &format!("INSERT INTO {table} (runtime_id) VALUES ('rt_old'), ('rt_active')"),
                    (),
                )
                .await
                .unwrap();
        }

        store
            .conn
            .execute(
                "INSERT INTO initial_prompt_deliveries (
                   runtime_id, agent_id, session_id, harness, template, rendered_prompt,
                   client_message_id, status, created_at_ms, accepted_at_ms, failed_at_ms
                 ) VALUES
                   ('rt_old', 'a_old', 'rt_old', 'codex', 't', 'r', 'c1', 'accepted', 100, 800, NULL),
                   ('rt_active', 'a_active', 'rt_active', 'codex', 't', 'r', 'c2', 'accepted', 100, 800, NULL),
                   ('rt_pending', 'a_pending', 'rt_pending', 'codex', 't', 'r', 'c3', 'pending', 100, NULL, NULL)",
                (),
            )
            .await
            .unwrap();

        store
            .conn
            .execute(
                "INSERT INTO inbox_subscriptions (
                   subscription_id, project, caller_name, caller_session_id, status, created_at, updated_at
                 ) VALUES
                   ('sub_old', 'p', 'ada', 's_ada', 'inactive', 100, 800),
                   ('sub_active', 'p', 'ada', 's_ada', 'active', 100, 800),
                   ('sub_has_batch', 'p', 'ada', 's_ada', 'inactive', 100, 800)",
                (),
            )
            .await
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO inbox_subscription_batches (
                   batch_id, subscription_id, batch_json, message_signature, status, created_at, consumed_at
                 ) VALUES
                   ('b_old', 'sub_has_batch', '{}', 'm1', 'consumed', 100, 800),
                   ('b_pending', 'sub_has_batch', '{}', 'm2', 'pending', 100, NULL)",
                (),
            )
            .await
            .unwrap();

        let policy = OperationalRetentionPolicy {
            retention_ms: 100,
            interval_ms: DEFAULT_REAP_INTERVAL_MS,
            size_threshold_mb: 1_024,
            materializer_retention_ms: 0,
            delivery_timeout_ttl_ms: DELIVERY_TIMEOUT_TTL_MS,
            delivery_sweep_interval_ms: DELIVERY_TIMEOUT_SWEEP_INTERVAL_MS,
        };
        let sweep = reap_operational_tables(&store, policy, 1_000)
            .await
            .unwrap();

        assert_eq!(sweep.command_intents, 1);
        assert_eq!(sweep.in_flight, 2);
        assert_eq!(sweep.notifications, 1);
        assert_eq!(sweep.developer_events, 1);
        assert_eq!(sweep.runtime_sidecars, 4);
        assert_eq!(sweep.producer_identities, 1);
        assert_eq!(sweep.agent_runtimes, 1);
        assert_eq!(sweep.initial_prompt_deliveries, 1);
        assert_eq!(sweep.inbox_subscription_batches, 1);
        assert_eq!(sweep.inbox_subscriptions, 1);

        assert_eq!(count_where(&store, "messages", "1=1").await, 2);
        assert_eq!(
            count_where(&store, "command_intents", "command_id = 'cmd_pending'").await,
            1
        );
        assert_eq!(
            count_where(&store, "command_intents", "command_id = 'cmd_recent_error'").await,
            1
        );
        assert_eq!(
            count_where(&store, "in_flight", "in_flight_id = 'if_pending'").await,
            1
        );
        assert_eq!(
            count_where(&store, "in_flight", "in_flight_id = 'if_recent_acked'").await,
            1
        );
        assert_eq!(
            count_where(&store, "notifications", "notif_id = 'n_recent'").await,
            1
        );
        assert_eq!(
            count_where(&store, "developer_events", "lifecycle = 'resume'").await,
            1
        );
        assert_eq!(
            count_where(&store, "agent_runtimes", "runtime_id = 'rt_active'").await,
            1
        );
        assert_eq!(
            count_where(&store, "producer_identities", "runtime_id = 'rt_active'").await,
            1
        );
        assert_eq!(
            count_where(
                &store,
                "initial_prompt_deliveries",
                "runtime_id = 'rt_active'"
            )
            .await,
            1
        );
        assert_eq!(
            count_where(
                &store,
                "initial_prompt_deliveries",
                "runtime_id = 'rt_pending'"
            )
            .await,
            1
        );
        assert_eq!(
            count_where(
                &store,
                "inbox_subscription_batches",
                "batch_id = 'b_pending'"
            )
            .await,
            1
        );
        assert_eq!(
            count_where(
                &store,
                "inbox_subscriptions",
                "subscription_id = 'sub_active'"
            )
            .await,
            1
        );
        assert_eq!(
            count_where(
                &store,
                "inbox_subscriptions",
                "subscription_id = 'sub_has_batch'"
            )
            .await,
            1
        );
    }

    #[tokio::test]
    async fn retention_sweep_dead_letters_delivery_timeouts_and_emits_event() {
        let store = migrated().await;
        let ts = 1_800_000_000_i64;
        let old = ts - nexus_store::repos::inbox::DELIVERY_TIMEOUT_TTL_MS - 1;
        let recent = ts - nexus_store::repos::inbox::DELIVERY_TIMEOUT_TTL_MS + 1;
        store
            .conn
            .execute(
                "INSERT INTO messages (message_id, from_name, kind, body, project, created_at)
                 VALUES ('m_old_pending', 'ada', 'dm', 'old pending', 'p', ?1),
                        ('m_old_notified', 'ada', 'dm', 'old notified', 'p', ?1),
                        ('m_recent', 'ada', 'dm', 'recent', 'p', ?2),
                        ('m_delivered', 'ada', 'dm', 'delivered', 'p', ?1)",
                params![old, recent],
            )
            .await
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO in_flight (
                   in_flight_id, message_id, recipient_session, state
                 ) VALUES ('if_old_pending', 'm_old_pending', 's_old', 'pending'),
                          ('if_old_notified', 'm_old_notified', 's_old', 'notified'),
                          ('if_recent', 'm_recent', 's_recent', 'pending'),
                          ('if_delivered', 'm_delivered', 's_old', 'delivered')",
                (),
            )
            .await
            .unwrap();

        let policy = OperationalRetentionPolicy {
            retention_ms: 24 * 60 * 60 * 1_000,
            interval_ms: DEFAULT_REAP_INTERVAL_MS,
            size_threshold_mb: 1_024,
            materializer_retention_ms: 0,
            delivery_timeout_ttl_ms: DELIVERY_TIMEOUT_TTL_MS,
            delivery_sweep_interval_ms: DELIVERY_TIMEOUT_SWEEP_INTERVAL_MS,
        };
        let sweep = reap_operational_tables(&store, policy, ts).await.unwrap();

        assert_eq!(sweep.delivery_timeouts, 2);
        assert_eq!(sweep.in_flight, 0);
        assert_eq!(
            count_where(
                &store,
                "in_flight",
                "state = 'error' AND error_reason = 'delivery_timeout'"
            )
            .await,
            2
        );
        assert_eq!(
            count_where(
                &store,
                "in_flight",
                "in_flight_id = 'if_recent' AND state = 'pending'"
            )
            .await,
            1
        );
        assert_eq!(
            count_where(
                &store,
                "in_flight",
                "in_flight_id = 'if_delivered' AND state = 'delivered'"
            )
            .await,
            1
        );

        let mut rows = store
            .conn
            .query(
                "SELECT lifecycle, data_json FROM developer_events \
                 WHERE topic = 'sys.agent.lifecycle' ORDER BY seq",
                (),
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().expect("dlq timeout event");
        assert_eq!(row.get::<String>(0).unwrap(), "dlq.timeout");
        let data: serde_json::Value = serde_json::from_str(&row.get::<String>(1).unwrap()).unwrap();
        assert_eq!(data["count"], 2);
        assert_eq!(data["errorReason"], "delivery_timeout");
        let ids = data["inFlightIds"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(ids, vec!["if_old_notified", "if_old_pending"]);
        assert!(rows.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delivery_timeout_sweep_runs_between_operational_retention_intervals() {
        let store = Arc::new(migrated().await);
        let ts = now();
        let old = ts - DELIVERY_TIMEOUT_TTL_MS - 1;
        store
            .conn
            .execute(
                "INSERT INTO messages (message_id, from_name, kind, body, project, created_at)
                 VALUES ('m_old_pending', 'ada', 'dm', 'old pending', 'p', ?1)",
                params![old],
            )
            .await
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO in_flight (
                   in_flight_id, message_id, recipient_session, state
                 ) VALUES ('if_old_pending', 'm_old_pending', 's_old', 'pending')",
                (),
            )
            .await
            .unwrap();
        let state = AppState::wire(store.clone(), &Config::default());
        let mut retention_state = RetentionPolicyState {
            last_reap_at_ms: ts,
            last_delivery_timeout_at_ms: 0,
            was_over_size_threshold: false,
        };

        let sweep = maybe_reap_operational_tables(&state, &mut retention_state)
            .await
            .unwrap()
            .expect("delivery timeout sweep should report work");

        assert_eq!(sweep.delivery_timeouts, 1);
        assert_eq!(sweep.command_intents, 0);
        assert_eq!(sweep.in_flight, 0);
        assert_eq!(
            count_where(
                &store,
                "in_flight",
                "in_flight_id = 'if_old_pending' \
                 AND state = 'error' \
                 AND error_reason = 'delivery_timeout'"
            )
            .await,
            1
        );
    }
}
