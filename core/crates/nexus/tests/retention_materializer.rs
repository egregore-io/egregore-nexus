//! Materializer retention: default-on window resolution + chunked backlog reap (N24).
//!
//! An observed write-lane wedge was driven by an unbounded
//! `agent_session_messages` backlog. Delivered transcript tails get deleted. These tests pin
//! the two required behaviors:
//! the env knob now DEFAULTS the reap on (48h; explicit `0` disables), and a large
//! backlog is deleted in bounded chunks, never one giant write transaction.

use nexus_store::repos::inbox::DELIVERY_TIMEOUT_TTL_MS;
use nexus_store::Store;

use nexus::daemon::retention_policy::{
    materializer_retention_from_env, reap_operational_tables, OperationalRetentionPolicy,
    DEFAULT_MATERIALIZER_RETENTION_MS,
};

#[test]
fn materializer_retention_defaults_on_and_zero_disables() {
    // Unset and junk both resolve to the 48h default (reap ON).
    assert_eq!(
        materializer_retention_from_env(None),
        DEFAULT_MATERIALIZER_RETENTION_MS
    );
    assert_eq!(
        materializer_retention_from_env(Some("junk")),
        DEFAULT_MATERIALIZER_RETENTION_MS
    );
    assert_eq!(
        materializer_retention_from_env(Some("")),
        DEFAULT_MATERIALIZER_RETENTION_MS
    );
    // Explicit values win verbatim: positive sets the window, 0/negative disable.
    assert_eq!(materializer_retention_from_env(Some("3600000")), 3_600_000);
    assert_eq!(materializer_retention_from_env(Some("0")), 0);
    assert_eq!(materializer_retention_from_env(Some("-1")), -1);
    assert_eq!(materializer_retention_from_env(Some(" 60000 ")), 60_000);
}

fn policy_with_materializer_window(window_ms: i64) -> OperationalRetentionPolicy {
    OperationalRetentionPolicy {
        retention_ms: 10,
        interval_ms: 0,
        size_threshold_mb: 0,
        materializer_retention_ms: window_ms,
        delivery_timeout_ttl_ms: DELIVERY_TIMEOUT_TTL_MS,
        delivery_sweep_interval_ms: 60_000,
    }
}

/// A backlog larger than one reap chunk (5000 rows) is fully deleted — the chunked
/// delete loops until the tail is gone instead of stopping after the first batch.
#[tokio::test]
async fn materializer_reap_drains_backlog_larger_than_one_chunk() {
    let store = Store::open(":memory:").await.unwrap();
    store.migrate().await.unwrap();

    store
        .conn
        .execute(
            "INSERT INTO agent_session_turns (id, session_id, status, first_stream_event_id, \
             last_stream_event_id, started_at, updated_at, finalized_at) \
             VALUES ('t_old', 's_x', 'completed', 1, 1, 1, 1, 1)",
            (),
        )
        .await
        .unwrap();
    // 5001 old finalized messages: one more than MATERIALIZER_REAP_CHUNK.
    store
        .conn
        .execute(
            "WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 5001) \
             INSERT INTO agent_session_messages (id, session_id, turn_id, ordinal, role, author, \
             content_json, status, first_stream_event_id, last_stream_event_id, created_at, \
             updated_at, finalized_at) \
             SELECT 'm_' || n, 's_x', 't_old', n, 'assistant', 'a', '{}', 'completed', 1, 1, 1, 1, 1 \
             FROM seq",
            (),
        )
        .await
        .unwrap();
    // One live streaming row that must survive the sweep.
    store
        .conn
        .execute(
            "INSERT INTO agent_session_messages (id, session_id, turn_id, ordinal, role, author, \
             content_json, status, first_stream_event_id, last_stream_event_id, created_at, \
             updated_at) \
             VALUES ('m_live', 's_x', 't_old', 6000, 'assistant', 'a', '{}', 'streaming', 1, 1, 1, 1)",
            (),
        )
        .await
        .unwrap();

    let sweep = reap_operational_tables(&store, policy_with_materializer_window(100), 1_000_000)
        .await
        .unwrap();

    assert_eq!(sweep.materialized_messages, 5001, "whole backlog drained");
    assert_eq!(sweep.materialized_turns, 1);

    let mut rows = store
        .conn
        .query("SELECT id FROM agent_session_messages", ())
        .await
        .unwrap();
    let mut left = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        left.push(row.get::<String>(0).unwrap());
    }
    assert_eq!(left, vec!["m_live".to_string()], "streaming row survives");
}
