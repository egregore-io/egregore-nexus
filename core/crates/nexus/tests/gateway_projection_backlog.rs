use nexus::daemon::gateway_projection_backlog::{
    AppendProjection, GatewayProjectionBacklog, GatewayProjectionBacklogError,
};
use nexus_common::{Config, GatewayProjectionBacklogConfig, GatewayProjectionDeliveryMode};
use nexus_contracts::{GatewayProjectionAck, GatewayProjectionKind};
use serde_json::json;

fn config(
    max_events: usize,
    max_bytes: usize,
    batch_events: usize,
) -> GatewayProjectionBacklogConfig {
    GatewayProjectionBacklogConfig {
        delivery_mode: GatewayProjectionDeliveryMode::Buffered,
        max_events,
        max_bytes,
        batch_events,
    }
}

fn append(
    backlog: &GatewayProjectionBacklog,
    event_id: &str,
    kind: GatewayProjectionKind,
    body: &str,
) -> AppendProjection {
    backlog.append(
        event_id,
        kind,
        100,
        json!({"messageId": event_id, "body": body}),
    )
}

#[test]
fn buffered_is_the_default_even_without_a_gateway() {
    let loaded = Config::default();
    assert_eq!(
        loaded.gateway_projection.delivery_mode,
        GatewayProjectionDeliveryMode::Buffered
    );
    assert_eq!(loaded.gateway_projection.max_events, 50_000);
    assert_eq!(loaded.gateway_projection.max_bytes, 67_108_864);
    assert_eq!(loaded.gateway_projection.batch_events, 1_000);

    let backlog = GatewayProjectionBacklog::with_epoch(loaded.gateway_projection, "boot-a");
    assert!(matches!(
        append(
            &backlog,
            "message:m1",
            GatewayProjectionKind::MessageAccepted,
            "hello"
        ),
        AppendProjection::Buffered(_)
    ));
    assert_eq!(backlog.stats().events, 1);
}

#[test]
fn append_replay_idempotency_and_batches_are_ordered() {
    let backlog = GatewayProjectionBacklog::with_epoch(config(10, 100_000, 2), "boot-a");
    for id in ["m1", "m2", "m3"] {
        assert!(matches!(
            append(&backlog, id, GatewayProjectionKind::MessageAccepted, id),
            AppendProjection::Buffered(_)
        ));
    }
    assert_eq!(
        append(
            &backlog,
            "m2",
            GatewayProjectionKind::MessageAccepted,
            "duplicate"
        ),
        AppendProjection::Duplicate
    );

    let first = backlog.replay_batch();
    assert!(first.gap.is_none());
    assert_eq!(
        first
            .events
            .iter()
            .map(|event| event.event_id.as_str())
            .collect::<Vec<_>>(),
        ["m1", "m2"]
    );
    assert_eq!(
        first
            .events
            .iter()
            .map(|event| event.seq)
            .collect::<Vec<_>>(),
        [1, 2]
    );
    backlog
        .ack(&GatewayProjectionAck {
            daemon_epoch: "boot-a".into(),
            through_seq: 2,
        })
        .unwrap();
    let second = backlog.replay_batch();
    assert_eq!(second.events.len(), 1);
    assert_eq!(second.events[0].event_id, "m3");
    assert_eq!(second.events[0].seq, 3);
}

#[test]
fn ack_trims_only_sent_contiguous_rows_and_cannot_jump_an_unseen_gap() {
    let backlog = GatewayProjectionBacklog::with_epoch(config(2, 100_000, 2), "boot-a");
    for id in ["m1", "m2", "m3"] {
        append(&backlog, id, GatewayProjectionKind::MessageAccepted, id);
    }
    assert_eq!(backlog.stats().events, 2);
    assert_eq!(backlog.stats().gaps, 1);
    assert_eq!(
        backlog.ack(&GatewayProjectionAck {
            daemon_epoch: "boot-a".into(),
            through_seq: 3,
        }),
        Err(GatewayProjectionBacklogError::AckBeyondUnseenGap)
    );

    let replay = backlog.replay_batch();
    assert_eq!(replay.gap.as_ref().map(|gap| gap.through_seq), Some(1));
    assert_eq!(
        replay
            .events
            .iter()
            .map(|event| event.seq)
            .collect::<Vec<_>>(),
        [2, 3]
    );
    assert_eq!(
        backlog.ack(&GatewayProjectionAck {
            daemon_epoch: "boot-a".into(),
            through_seq: 4,
        }),
        Err(GatewayProjectionBacklogError::AckBeyondSent { sent_through: 3 })
    );
    assert_eq!(
        backlog
            .ack(&GatewayProjectionAck {
                daemon_epoch: "boot-a".into(),
                through_seq: 2,
            })
            .unwrap(),
        1
    );
    assert_eq!(backlog.stats().events, 1);
}

#[test]
fn event_and_byte_limits_evict_only_behind_an_explicit_gap() {
    let by_event = GatewayProjectionBacklog::with_epoch(config(1, 100_000, 5), "boot-events");
    append(
        &by_event,
        "m1",
        GatewayProjectionKind::MessageAccepted,
        "one",
    );
    append(
        &by_event,
        "m2",
        GatewayProjectionKind::MessageAccepted,
        "two",
    );
    let replay = by_event.replay_batch();
    assert_eq!(
        replay.gap.as_ref().map(|gap| gap.reason.as_str()),
        Some("backlog_overflow")
    );
    assert_eq!(replay.events[0].event_id, "m2");

    let probe = GatewayProjectionBacklog::with_epoch(config(10, 100_000, 5), "probe");
    let first = append(
        &probe,
        "m1",
        GatewayProjectionKind::MessageAccepted,
        "a moderately sized event",
    );
    let first_bytes = match first {
        AppendProjection::Buffered(event) => serde_json::to_vec(&event).unwrap().len(),
        other => panic!("expected buffered event, got {other:?}"),
    };
    let by_bytes =
        GatewayProjectionBacklog::with_epoch(config(10, first_bytes + 8, 5), "boot-bytes");
    append(
        &by_bytes,
        "m1",
        GatewayProjectionKind::MessageAccepted,
        "a moderately sized event",
    );
    append(
        &by_bytes,
        "m2",
        GatewayProjectionKind::MessageAccepted,
        "another moderately sized event",
    );
    assert!(by_bytes.stats().bytes <= first_bytes + 8);
    assert!(by_bytes.stats().gaps >= 1);
}

#[test]
fn replaceable_snapshots_coalesce_before_canonical_history_is_evicted() {
    let backlog = GatewayProjectionBacklog::with_epoch(config(2, 100_000, 5), "boot-a");
    backlog.append(
        "presence:a_ada:1",
        GatewayProjectionKind::PresenceChanged,
        1,
        json!({"agentId": "a_ada", "presence": "online"}),
    );
    backlog.append(
        "message:m1",
        GatewayProjectionKind::MessageAccepted,
        2,
        json!({"messageId": "m1"}),
    );
    backlog.append(
        "presence:a_ada:2",
        GatewayProjectionKind::PresenceChanged,
        3,
        json!({"agentId": "a_ada", "presence": "busy"}),
    );

    let replay = backlog.replay_batch();
    assert!(
        replay.gap.is_none(),
        "snapshot coalescing is not a history gap"
    );
    assert_eq!(
        replay
            .events
            .iter()
            .map(|event| event.event_id.as_str())
            .collect::<Vec<_>>(),
        ["message:m1", "presence:a_ada:2"]
    );
    assert_eq!(backlog.stats().coalesced, 1);
}

#[test]
fn agent_session_frames_never_consume_projection_backlog_capacity() {
    let backlog = GatewayProjectionBacklog::with_epoch(config(1, 256, 1), "boot-a");
    assert_eq!(backlog.stats().events, 0);
    // The only append API accepts a closed GatewayProjectionKind. Agent updates and terminal bytes
    // have no representation here, so stream pressure cannot evict canonical history.
    assert!(serde_json::from_value::<GatewayProjectionKind>(json!("agent.update")).is_err());
}

#[test]
fn best_effort_never_retains_or_requires_ack() {
    let mut cfg = config(10, 100_000, 5);
    cfg.delivery_mode = GatewayProjectionDeliveryMode::BestEffort;
    let backlog = GatewayProjectionBacklog::with_epoch(cfg, "boot-a");
    assert!(matches!(
        append(
            &backlog,
            "m1",
            GatewayProjectionKind::MessageAccepted,
            "hello"
        ),
        AppendProjection::LiveOnly(_)
    ));
    assert_eq!(backlog.stats().events, 0);
    assert!(backlog.replay_batch().events.is_empty());
    assert_eq!(
        backlog.ack(&GatewayProjectionAck {
            daemon_epoch: "boot-a".into(),
            through_seq: 1,
        }),
        Err(GatewayProjectionBacklogError::AckNotRequired)
    );
}

#[test]
fn explicit_mode_switch_clears_backlog_advances_epoch_and_requests_resync() {
    let backlog = GatewayProjectionBacklog::with_epoch(config(10, 100_000, 5), "boot-a");
    append(
        &backlog,
        "m1",
        GatewayProjectionKind::MessageAccepted,
        "hello",
    );
    let to_best = backlog.set_delivery_mode(GatewayProjectionDeliveryMode::BestEffort);
    assert_eq!(to_best.previous_epoch, "boot-a");
    assert_ne!(to_best.daemon_epoch, "boot-a");
    assert_eq!(backlog.stats().events, 0);
    assert_eq!(backlog.stats().gaps, 1);
    assert!(!backlog.take_resync_required());

    let best_epoch = backlog.stats().daemon_epoch;
    let to_buffered = backlog.set_delivery_mode(GatewayProjectionDeliveryMode::Buffered);
    assert_ne!(to_buffered.daemon_epoch, best_epoch);
    assert!(backlog.take_resync_required());
    assert!(
        !backlog.take_resync_required(),
        "resync scheduling is edge-triggered"
    );
}

#[test]
fn new_backlog_represents_a_restart_with_a_new_empty_epoch_and_no_persistence() {
    let first = GatewayProjectionBacklog::new(config(10, 100_000, 5));
    append(
        &first,
        "m1",
        GatewayProjectionKind::MessageAccepted,
        "hello",
    );
    let second = GatewayProjectionBacklog::new(config(10, 100_000, 5));
    assert_ne!(first.stats().daemon_epoch, second.stats().daemon_epoch);
    assert_eq!(second.stats().events, 0);
    assert!(second.replay_batch().events.is_empty());

    let source = include_str!("../src/daemon/gateway_projection_backlog.rs");
    for forbidden in ["nexus_store", "CREATE TABLE", "INSERT INTO", "db_path"] {
        assert!(
            !source.contains(forbidden),
            "projection backlog must stay RAM-only: {forbidden}"
        );
    }
}
