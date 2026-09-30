use std::sync::Arc;

use nexus::daemon::gateway_stream_socket::{
    gateway_stream_endpoint_manifest_path, read_gateway_stream_endpoint_manifest,
    serve_gateway_stream_connection, spawn_gateway_stream_socket, write_gateway_client_frame,
    GatewayStreamClientFrame, GatewayStreamFrame, GatewayStreamLane, GatewayStreamPublisher,
    GatewayStreamSubscription,
};
use nexus::daemon::services::fleet_events::{FleetStatusObservation, FLEET_SESSION_KEY};
use nexus::daemon::WsSink;
use nexus_common::{GatewayProjectionBacklogConfig, GatewayProjectionDeliveryMode};
use nexus_contracts::{
    AgentUpdateKind, DeveloperEventKind, DeveloperToolCallPhase, EventSink, GatewayProjectionAck,
    GatewayProjectionEvent, GatewayProjectionKind, Presence, SessionId, WsEvent,
    GATEWAY_PROJECTION_VERSION,
};
use nexus_store::{
    repos::{AgentRuntimes, Agents, NewAgent, NewAgentRuntime, NewSession, Sessions, StreamEvents},
    Store,
};
use nexus_transcript::{ToolCallObservation, ToolCallPhase};
use serde_json::json;
use tokio::io::{duplex, AsyncReadExt};
use tokio::time::{timeout, Duration};

async fn read_gateway_stream_frame<R>(reader: &mut R) -> GatewayStreamFrame
where
    R: tokio::io::AsyncRead + Unpin,
{
    let len = reader.read_u32().await.unwrap() as usize;
    let mut payload = vec![0_u8; len];
    reader.read_exact(&mut payload).await.unwrap();
    serde_json::from_slice(&payload).unwrap()
}

async fn read_gateway_stream_frame_named<R>(reader: &mut R, name: &str) -> GatewayStreamFrame
where
    R: tokio::io::AsyncRead + Unpin,
{
    timeout(Duration::from_secs(2), read_gateway_stream_frame(reader))
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {name}"))
}

#[test]
fn gateway_projection_and_ack_frames_have_explicit_wire_tags() {
    let event = GatewayProjectionEvent {
        event_id: "message:m1".to_string(),
        daemon_epoch: "boot_test".to_string(),
        seq: 1,
        occurred_at: 10,
        kind: GatewayProjectionKind::MessageAccepted,
        version: GATEWAY_PROJECTION_VERSION,
        payload: json!({"messageId": "m1"}),
    };
    assert_eq!(
        serde_json::to_value(GatewayStreamFrame::Projection {
            event: event.clone()
        })
        .unwrap(),
        json!({"t": "projection", "event": serde_json::to_value(event).unwrap()})
    );
    assert_eq!(
        serde_json::to_value(GatewayStreamClientFrame::ProjectionAck {
            ack: GatewayProjectionAck {
                daemon_epoch: "boot_test".to_string(),
                through_seq: 1,
            }
        })
        .unwrap(),
        json!({
            "t": "projection.ack",
            "ack": {"daemonEpoch": "boot_test", "throughSeq": 1}
        })
    );
}

#[tokio::test]
async fn gateway_stream_accepts_projection_ack_and_keeps_connection_live() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let publisher = GatewayStreamPublisher::new(16);
    let projection_epoch = publisher.projection_stats().daemon_epoch;
    let (client, server) = duplex(4096);
    let (mut client_read, mut client_write) = tokio::io::split(client);
    let server_task = tokio::spawn(serve_gateway_stream_connection(
        server,
        store,
        publisher,
        "expected-token".to_string(),
        "boot_test".to_string(),
    ));

    write_gateway_client_frame(
        &mut client_write,
        &GatewayStreamClientFrame::Hello {
            version: 1,
            token: "expected-token".to_string(),
            subscriptions: vec![],
            hooks: None,
        },
    )
    .await
    .unwrap();
    let _ = read_gateway_stream_frame_named(&mut client_read, "ready").await;

    write_gateway_client_frame(
        &mut client_write,
        &GatewayStreamClientFrame::ProjectionAck {
            ack: GatewayProjectionAck {
                daemon_epoch: projection_epoch,
                through_seq: 0,
            },
        },
    )
    .await
    .unwrap();
    write_gateway_client_frame(
        &mut client_write,
        &GatewayStreamClientFrame::Ping {
            id: Some("after-ack".to_string()),
        },
    )
    .await
    .unwrap();
    assert_eq!(
        read_gateway_stream_frame_named(&mut client_read, "pong after projection ack").await,
        GatewayStreamFrame::Pong {
            id: Some("after-ack".to_string())
        }
    );

    drop(client_write);
    drop(client_read);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn buffered_projection_replays_in_acknowledged_batches() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let publisher = GatewayStreamPublisher::new_with_projection_config(
        16,
        GatewayProjectionBacklogConfig {
            delivery_mode: GatewayProjectionDeliveryMode::Buffered,
            max_events: 10,
            max_bytes: 100_000,
            batch_events: 1,
        },
        "boot_projection",
    );
    publisher.publish_projection(
        "message:m1",
        GatewayProjectionKind::MessageAccepted,
        1,
        json!({"messageId": "m1"}),
    );
    publisher.publish_projection(
        "message:m2",
        GatewayProjectionKind::MessageAccepted,
        2,
        json!({"messageId": "m2"}),
    );

    let (client, server) = duplex(8192);
    let (mut client_read, mut client_write) = tokio::io::split(client);
    let server_task = tokio::spawn(serve_gateway_stream_connection(
        server,
        store,
        publisher.clone(),
        "expected-token".to_string(),
        "boot_projection".to_string(),
    ));
    write_gateway_client_frame(
        &mut client_write,
        &GatewayStreamClientFrame::Hello {
            version: 1,
            token: "expected-token".to_string(),
            subscriptions: vec![],
            hooks: None,
        },
    )
    .await
    .unwrap();
    let _ = read_gateway_stream_frame_named(&mut client_read, "ready").await;
    let first = read_gateway_stream_frame_named(&mut client_read, "first projection batch").await;
    let first_event = match first {
        GatewayStreamFrame::Projection { event } => event,
        other => panic!("expected first projection, got {other:?}"),
    };
    assert_eq!(first_event.event_id, "message:m1");
    assert_eq!(first_event.seq, 1);

    write_gateway_client_frame(
        &mut client_write,
        &GatewayStreamClientFrame::ProjectionAck {
            ack: GatewayProjectionAck {
                daemon_epoch: "boot_projection".into(),
                through_seq: 1,
            },
        },
    )
    .await
    .unwrap();
    let second = read_gateway_stream_frame_named(&mut client_read, "second projection batch").await;
    let second_event = match second {
        GatewayStreamFrame::Projection { event } => event,
        other => panic!("expected second projection, got {other:?}"),
    };
    assert_eq!(second_event.event_id, "message:m2");
    assert_eq!(second_event.seq, 2);

    write_gateway_client_frame(
        &mut client_write,
        &GatewayStreamClientFrame::ProjectionAck {
            ack: GatewayProjectionAck {
                daemon_epoch: "boot_projection".into(),
                through_seq: 2,
            },
        },
    )
    .await
    .unwrap();
    write_gateway_client_frame(
        &mut client_write,
        &GatewayStreamClientFrame::Ping {
            id: Some("drained".into()),
        },
    )
    .await
    .unwrap();
    assert_eq!(
        read_gateway_stream_frame_named(&mut client_read, "pong after drain").await,
        GatewayStreamFrame::Pong {
            id: Some("drained".into())
        }
    );
    assert_eq!(publisher.projection_stats().events, 0);

    drop(client_write);
    drop(client_read);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn partial_projection_ack_does_not_replay_the_unsettled_batch_tail() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let publisher = GatewayStreamPublisher::new_with_projection_config(
        16,
        GatewayProjectionBacklogConfig {
            delivery_mode: GatewayProjectionDeliveryMode::Buffered,
            max_events: 10,
            max_bytes: 100_000,
            batch_events: 2,
        },
        "boot_projection_partial_ack",
    );
    for index in 1..=3 {
        publisher.publish_projection(
            format!("message:m{index}"),
            GatewayProjectionKind::MessageAccepted,
            index,
            json!({"messageId": format!("m{index}")}),
        );
    }

    let (client, server) = duplex(8192);
    let (mut client_read, mut client_write) = tokio::io::split(client);
    let server_task = tokio::spawn(serve_gateway_stream_connection(
        server,
        store,
        publisher,
        "expected-token".to_string(),
        "boot_projection_partial_ack".to_string(),
    ));
    write_gateway_client_frame(
        &mut client_write,
        &GatewayStreamClientFrame::Hello {
            version: 1,
            token: "expected-token".to_string(),
            subscriptions: vec![],
            hooks: None,
        },
    )
    .await
    .unwrap();
    let _ = read_gateway_stream_frame_named(&mut client_read, "ready").await;
    for expected in ["message:m1", "message:m2"] {
        let frame =
            read_gateway_stream_frame_named(&mut client_read, "initial projection batch").await;
        let GatewayStreamFrame::Projection { event } = frame else {
            panic!("expected projection event");
        };
        assert_eq!(event.event_id, expected);
    }

    write_gateway_client_frame(
        &mut client_write,
        &GatewayStreamClientFrame::ProjectionAck {
            ack: GatewayProjectionAck {
                daemon_epoch: "boot_projection_partial_ack".into(),
                through_seq: 1,
            },
        },
    )
    .await
    .unwrap();
    write_gateway_client_frame(
        &mut client_write,
        &GatewayStreamClientFrame::Ping {
            id: Some("partial-ack".into()),
        },
    )
    .await
    .unwrap();
    assert_eq!(
        read_gateway_stream_frame_named(&mut client_read, "pong after partial ack").await,
        GatewayStreamFrame::Pong {
            id: Some("partial-ack".into())
        },
        "a partial ACK must not replay the still in-flight batch tail"
    );

    write_gateway_client_frame(
        &mut client_write,
        &GatewayStreamClientFrame::ProjectionAck {
            ack: GatewayProjectionAck {
                daemon_epoch: "boot_projection_partial_ack".into(),
                through_seq: 2,
            },
        },
    )
    .await
    .unwrap();
    let frame = read_gateway_stream_frame_named(&mut client_read, "next projection batch").await;
    let GatewayStreamFrame::Projection { event } = frame else {
        panic!("expected projection event");
    };
    assert_eq!(event.event_id, "message:m3");

    drop(client_write);
    drop(client_read);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn best_effort_projection_drops_disconnected_history_and_sends_live_once() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let publisher = GatewayStreamPublisher::new_with_projection_config(
        16,
        GatewayProjectionBacklogConfig {
            delivery_mode: GatewayProjectionDeliveryMode::BestEffort,
            max_events: 10,
            max_bytes: 100_000,
            batch_events: 2,
        },
        "boot_best",
    );
    publisher.publish_projection(
        "message:dropped",
        GatewayProjectionKind::MessageAccepted,
        1,
        json!({"messageId": "dropped"}),
    );
    assert_eq!(publisher.projection_stats().dropped, 1);
    assert_eq!(publisher.projection_stats().events, 0);

    let (client, server) = duplex(8192);
    let (mut client_read, mut client_write) = tokio::io::split(client);
    let server_task = tokio::spawn(serve_gateway_stream_connection(
        server,
        store,
        publisher.clone(),
        "expected-token".to_string(),
        "boot_best".to_string(),
    ));
    write_gateway_client_frame(
        &mut client_write,
        &GatewayStreamClientFrame::Hello {
            version: 1,
            token: "expected-token".to_string(),
            subscriptions: vec![],
            hooks: None,
        },
    )
    .await
    .unwrap();
    let _ = read_gateway_stream_frame_named(&mut client_read, "ready").await;

    publisher.publish_projection(
        "message:live",
        GatewayProjectionKind::MessageAccepted,
        2,
        json!({"messageId": "live"}),
    );
    let live =
        read_gateway_stream_frame_named(&mut client_read, "best effort live projection").await;
    match live {
        GatewayStreamFrame::Projection { event } => assert_eq!(event.event_id, "message:live"),
        other => panic!("expected live best-effort projection, got {other:?}"),
    }
    assert_eq!(publisher.projection_stats().events, 0);

    drop(client_write);
    drop(client_read);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn gateway_stream_rejects_bad_token_before_subscription() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let publisher = GatewayStreamPublisher::new(16);
    let (client, server) = duplex(4096);
    let (mut client_read, mut client_write) = tokio::io::split(client);

    let server_task = tokio::spawn(serve_gateway_stream_connection(
        server,
        store,
        publisher,
        "expected-token".to_string(),
        "boot_test".to_string(),
    ));

    write_gateway_client_frame(
        &mut client_write,
        &GatewayStreamClientFrame::Hello {
            version: 1,
            token: "wrong-token".to_string(),
            subscriptions: vec![GatewayStreamSubscription {
                lane: GatewayStreamLane::Agent,
                session_id: "s_ada".to_string(),
                after_id: 0,
            }],
            hooks: None,
        },
    )
    .await
    .unwrap();

    assert_eq!(
        read_gateway_stream_frame_named(&mut client_read, "bad-token error").await,
        GatewayStreamFrame::Error {
            code: "unauthorized".to_string(),
            message: "bad token".to_string(),
        }
    );
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn gateway_stream_drains_catchup_then_live_agent_update() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let session = SessionId("s_ada".to_string());
    let publisher = GatewayStreamPublisher::new(16);

    let first_id = StreamEvents::new(&store)
        .append(
            &session,
            "text",
            &json!({"text": "catchup one"}).to_string(),
        )
        .await
        .unwrap();
    let second_id = StreamEvents::new(&store)
        .append(&session, "turn_end", &json!({}).to_string())
        .await
        .unwrap();

    let (client, server) = duplex(8192);
    let (mut client_read, mut client_write) = tokio::io::split(client);
    let server_task = tokio::spawn(serve_gateway_stream_connection(
        server,
        store.clone(),
        publisher.clone(),
        "expected-token".to_string(),
        "boot_test".to_string(),
    ));

    write_gateway_client_frame(
        &mut client_write,
        &GatewayStreamClientFrame::Hello {
            version: 1,
            token: "expected-token".to_string(),
            subscriptions: vec![GatewayStreamSubscription {
                lane: GatewayStreamLane::Agent,
                session_id: session.0.clone(),
                after_id: 0,
            }],
            hooks: None,
        },
    )
    .await
    .unwrap();

    assert_eq!(
        read_gateway_stream_frame_named(&mut client_read, "ready").await,
        GatewayStreamFrame::Ready {
            version: 1,
            daemon_boot_id: "boot_test".to_string(),
            resume: "store".to_string(),
        }
    );
    assert_eq!(
        read_gateway_stream_frame_named(&mut client_read, "first catch-up").await,
        GatewayStreamFrame::AgentUpdate {
            session_id: session.0.clone(),
            stream_event_id: first_id,
            kind: AgentUpdateKind::Text,
            data: json!({"text": "catchup one", "streamEventId": first_id}),
        }
    );
    assert_eq!(
        read_gateway_stream_frame_named(&mut client_read, "second catch-up").await,
        GatewayStreamFrame::AgentUpdate {
            session_id: session.0.clone(),
            stream_event_id: second_id,
            kind: AgentUpdateKind::TurnEnd,
            data: json!({"streamEventId": second_id}),
        }
    );

    let live_id = StreamEvents::new(&store)
        .append(&session, "text", &json!({"text": "live"}).to_string())
        .await
        .unwrap();
    publisher.publish_agent_update(
        &session,
        live_id,
        AgentUpdateKind::Text,
        json!({"text": "live"}),
    );

    assert_eq!(
        read_gateway_stream_frame_named(&mut client_read, "live update").await,
        GatewayStreamFrame::AgentUpdate {
            session_id: session.0.clone(),
            stream_event_id: live_id,
            kind: AgentUpdateKind::Text,
            data: json!({"text": "live", "streamEventId": live_id}),
        }
    );

    drop(client_write);
    drop(client_read);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn gateway_stream_raw_lane_is_reserved_with_store_gap() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let publisher = GatewayStreamPublisher::new(16);
    let (client, server) = duplex(4096);
    let (mut client_read, mut client_write) = tokio::io::split(client);

    let server_task = tokio::spawn(serve_gateway_stream_connection(
        server,
        store,
        publisher,
        "expected-token".to_string(),
        "boot_test".to_string(),
    ));

    write_gateway_client_frame(
        &mut client_write,
        &GatewayStreamClientFrame::Hello {
            version: 1,
            token: "expected-token".to_string(),
            subscriptions: vec![GatewayStreamSubscription {
                lane: GatewayStreamLane::Raw,
                session_id: "s_ada".to_string(),
                after_id: 7,
            }],
            hooks: None,
        },
    )
    .await
    .unwrap();

    assert_eq!(
        read_gateway_stream_frame_named(&mut client_read, "ready").await,
        GatewayStreamFrame::Ready {
            version: 1,
            daemon_boot_id: "boot_test".to_string(),
            resume: "store".to_string(),
        }
    );
    assert_eq!(
        read_gateway_stream_frame_named(&mut client_read, "raw gap").await,
        GatewayStreamFrame::Gap {
            lane: GatewayStreamLane::Raw,
            session_id: "s_ada".to_string(),
            after_id: 7,
            retry: "store".to_string(),
        }
    );

    drop(client_write);
    drop(client_read);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn gateway_stream_drains_catchup_then_live_developer_tool_call_event() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let session = SessionId("s_ada".to_string());
    let publisher = GatewayStreamPublisher::new(16);

    let replay = publisher
        .publish_tool_call_observation(
            &session,
            "ada",
            ToolCallObservation {
                tool_call_id: Some("tc_replay".to_string()),
                tool: "Read".to_string(),
                phase: ToolCallPhase::Pre,
                ok: true,
            },
        )
        .expect("replay event");

    let (client, server) = duplex(8192);
    let (mut client_read, mut client_write) = tokio::io::split(client);
    let server_task = tokio::spawn(serve_gateway_stream_connection(
        server,
        store.clone(),
        publisher.clone(),
        "expected-token".to_string(),
        "boot_test".to_string(),
    ));

    write_gateway_client_frame(
        &mut client_write,
        &GatewayStreamClientFrame::Hello {
            version: 1,
            token: "expected-token".to_string(),
            subscriptions: vec![GatewayStreamSubscription {
                lane: GatewayStreamLane::DeveloperEvent,
                session_id: session.0.clone(),
                after_id: 0,
            }],
            hooks: None,
        },
    )
    .await
    .unwrap();

    assert_eq!(
        read_gateway_stream_frame_named(&mut client_read, "ready").await,
        GatewayStreamFrame::Ready {
            version: 1,
            daemon_boot_id: "boot_test".to_string(),
            resume: "store".to_string(),
        }
    );
    assert_eq!(
        read_gateway_stream_frame_named(&mut client_read, "developer-event replay").await,
        GatewayStreamFrame::DeveloperEvent {
            session_id: session.0.clone(),
            event: replay,
        }
    );

    let live = publisher
        .publish_tool_call_observation(
            &session,
            "ada",
            ToolCallObservation {
                tool_call_id: Some("tc_replay".to_string()),
                tool: String::new(),
                phase: ToolCallPhase::Post,
                ok: false,
            },
        )
        .expect("live event");

    assert_eq!(live.kind, DeveloperEventKind::ToolCall);
    assert_eq!(live.phase, Some(DeveloperToolCallPhase::Post));
    assert_eq!(live.tool.as_deref(), Some("Read"));
    assert_eq!(
        read_gateway_stream_frame_named(&mut client_read, "developer-event live").await,
        GatewayStreamFrame::DeveloperEvent {
            session_id: session.0.clone(),
            event: live,
        }
    );

    drop(client_write);
    drop(client_read);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn fleet_subscribe_resets_an_ahead_cursor_with_ordered_resync_then_live_status() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let publisher = GatewayStreamPublisher::new(16);
    publisher.publish_fleet_status(FleetStatusObservation {
        lifecycle: "status",
        agent: Some("old".to_string()),
        session_id: SessionId("s_old".to_string()),
        current_work: None,
        data: Some(json!({"presence": "offline", "paused": false})),
    });

    let (client, server) = duplex(8192);
    let (mut client_read, mut client_write) = tokio::io::split(client);
    let server_task = tokio::spawn(serve_gateway_stream_connection(
        server,
        store.clone(),
        publisher.clone(),
        "expected-token".to_string(),
        "boot_new".to_string(),
    ));

    write_gateway_client_frame(
        &mut client_write,
        &GatewayStreamClientFrame::Hello {
            version: 1,
            token: "expected-token".to_string(),
            subscriptions: vec![GatewayStreamSubscription {
                lane: GatewayStreamLane::DeveloperEvent,
                session_id: FLEET_SESSION_KEY.to_string(),
                // Simulate a cursor retained from an older daemon boot.
                after_id: 999,
            }],
            hooks: None,
        },
    )
    .await
    .unwrap();

    assert!(matches!(
        read_gateway_stream_frame_named(&mut client_read, "ready").await,
        GatewayStreamFrame::Ready { .. }
    ));
    let resync_seq = match read_gateway_stream_frame_named(&mut client_read, "fleet resync").await {
        GatewayStreamFrame::DeveloperEvent { session_id, event } => {
            assert_eq!(session_id, FLEET_SESSION_KEY);
            assert_eq!(event.lifecycle.as_deref(), Some("resync"));
            assert_eq!(
                event.data,
                Some(json!({"reason": "subscribe", "source": "members"}))
            );
            event.seq
        }
        other => panic!("expected fleet resync, got {other:?}"),
    };

    let live = publisher.publish_fleet_status(FleetStatusObservation {
        lifecycle: "status",
        agent: Some("demoa".to_string()),
        session_id: SessionId("s_demoa".to_string()),
        current_work: Some("shipping".to_string()),
        data: Some(json!({"presence": "online", "paused": false})),
    });
    assert!(live.seq > resync_seq);
    assert_eq!(
        read_gateway_stream_frame_named(&mut client_read, "fleet live status").await,
        GatewayStreamFrame::DeveloperEvent {
            session_id: FLEET_SESSION_KEY.to_string(),
            event: live,
        }
    );

    drop(client_write);
    drop(client_read);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn fleet_resync_is_connection_local_and_does_not_refresh_existing_subscribers() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let publisher = GatewayStreamPublisher::new(16);

    let (client_a, server_a) = duplex(8192);
    let (mut read_a, mut write_a) = tokio::io::split(client_a);
    let task_a = tokio::spawn(serve_gateway_stream_connection(
        server_a,
        store.clone(),
        publisher.clone(),
        "token-a".to_string(),
        "boot_test".to_string(),
    ));
    write_gateway_client_frame(
        &mut write_a,
        &GatewayStreamClientFrame::Hello {
            version: 1,
            token: "token-a".to_string(),
            subscriptions: vec![GatewayStreamSubscription {
                lane: GatewayStreamLane::DeveloperEvent,
                session_id: FLEET_SESSION_KEY.to_string(),
                after_id: 0,
            }],
            hooks: None,
        },
    )
    .await
    .unwrap();
    let _ = read_gateway_stream_frame_named(&mut read_a, "A ready").await;
    let a_resync = read_gateway_stream_frame_named(&mut read_a, "A resync").await;
    assert!(matches!(
        a_resync,
        GatewayStreamFrame::DeveloperEvent { event, .. }
            if event.lifecycle.as_deref() == Some("resync")
    ));

    let (client_b, server_b) = duplex(8192);
    let (mut read_b, mut write_b) = tokio::io::split(client_b);
    let task_b = tokio::spawn(serve_gateway_stream_connection(
        server_b,
        store,
        publisher.clone(),
        "token-b".to_string(),
        "boot_test".to_string(),
    ));
    write_gateway_client_frame(
        &mut write_b,
        &GatewayStreamClientFrame::Hello {
            version: 1,
            token: "token-b".to_string(),
            subscriptions: vec![GatewayStreamSubscription {
                lane: GatewayStreamLane::DeveloperEvent,
                session_id: FLEET_SESSION_KEY.to_string(),
                after_id: 0,
            }],
            hooks: None,
        },
    )
    .await
    .unwrap();
    let _ = read_gateway_stream_frame_named(&mut read_b, "B ready").await;
    let b_resync = read_gateway_stream_frame_named(&mut read_b, "B resync").await;
    assert!(matches!(
        b_resync,
        GatewayStreamFrame::DeveloperEvent { event, .. }
            if event.lifecycle.as_deref() == Some("resync")
    ));

    let live = publisher.publish_fleet_status(FleetStatusObservation {
        lifecycle: "status",
        agent: Some("demoa".to_string()),
        session_id: SessionId("s_demoa".to_string()),
        current_work: None,
        data: Some(json!({"presence": "online", "paused": false})),
    });
    let expected = GatewayStreamFrame::DeveloperEvent {
        session_id: FLEET_SESSION_KEY.to_string(),
        event: live,
    };
    assert_eq!(
        read_gateway_stream_frame_named(&mut read_a, "A live status after B subscribe").await,
        expected.clone()
    );
    assert_eq!(
        read_gateway_stream_frame_named(&mut read_b, "B live status").await,
        expected
    );

    drop(write_a);
    drop(read_a);
    drop(write_b);
    drop(read_b);
    task_a.await.unwrap().unwrap();
    task_b.await.unwrap().unwrap();
}

#[tokio::test]
async fn ws_sink_enriches_ordered_fleet_status_with_authoritative_activity() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let session = SessionId("s_demoa".to_string());
    let agent_id = "a_demoa";
    Agents::new(&store)
        .create(NewAgent {
            agent_id: agent_id.to_string(),
            project: "default".to_string(),
            name: Some("demoa".to_string()),
            default_harness: Some("codex".to_string()),
            role: None,
            tier: Some("agent".to_string()),
            owner: None,
        })
        .await
        .unwrap();
    Sessions::new(&store)
        .create(NewSession {
            session_id: session.clone(),
            name: Some("demoa".to_string()),
            agent: Some("codex".to_string()),
            kind: "agent".to_string(),
            role: None,
            tier: "agent".to_string(),
            harness_session_id: None,
            client_key: Some("ck_demoa".to_string()),
            cwd: None,
            project: "default".to_string(),
            transport: Some("codex-appserver".to_string()),
        })
        .await
        .unwrap();
    Sessions::new(&store)
        .set_agent_id(&session, agent_id)
        .await
        .unwrap();
    AgentRuntimes::new(&store)
        .create(NewAgentRuntime {
            runtime_id: session.0.clone(),
            agent_id: agent_id.to_string(),
            harness: "codex".to_string(),
            cwd: None,
            transport: Some("codex-appserver".to_string()),
            presence: Some("busy".to_string()),
            active: true,
        })
        .await
        .unwrap();
    Sessions::new(&store)
        .set_current_work(&session, Some("shipping presence"))
        .await
        .unwrap();

    let publisher = GatewayStreamPublisher::new(16);
    let mut frames = publisher.subscribe();
    let sink = WsSink::new(16, Some(store)).with_gateway_stream(publisher);
    sink.emit(WsEvent::AgentStatus {
        session_id: session.clone(),
        presence: Presence::Busy,
        paused: false,
    })
    .await;

    let frame = timeout(Duration::from_secs(1), frames.recv())
        .await
        .expect("fleet status timeout")
        .expect("fleet status channel");
    match frame {
        GatewayStreamFrame::DeveloperEvent { session_id, event } => {
            assert_eq!(session_id, FLEET_SESSION_KEY);
            assert_eq!(event.agent.as_deref(), Some("demoa"));
            assert_eq!(event.current_work.as_deref(), Some("shipping presence"));
            assert_eq!(event.lifecycle.as_deref(), Some("status"));
            assert_eq!(
                event.data,
                Some(json!({"presence": "busy", "paused": false}))
            );
        }
        other => panic!("expected fleet status, got {other:?}"),
    }

    let frame = timeout(Duration::from_secs(1), frames.recv())
        .await
        .expect("presence projection timeout")
        .expect("presence projection channel");
    match frame {
        GatewayStreamFrame::Projection { event } => {
            assert_eq!(
                event.kind,
                nexus_contracts::GatewayProjectionKind::PresenceChanged
            );
            assert_eq!(event.payload["runtimeId"], session.0);
            assert_eq!(event.payload["presence"], "busy");
        }
        other => panic!("expected presence projection after fleet status, got {other:?}"),
    }
}

#[tokio::test]
async fn gateway_stream_reports_ephemeral_gap_for_dropped_developer_event_cursor() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let session = SessionId("s_ada".to_string());
    let publisher = GatewayStreamPublisher::new_with_tool_call_event_caps(16, 2, 8);

    for index in 0..4 {
        publisher
            .publish_tool_call_observation(
                &session,
                "ada",
                ToolCallObservation {
                    tool_call_id: None,
                    tool: format!("tool-{index}"),
                    phase: ToolCallPhase::Pre,
                    ok: true,
                },
            )
            .expect("event");
    }

    let (client, server) = duplex(8192);
    let (mut client_read, mut client_write) = tokio::io::split(client);
    let server_task = tokio::spawn(serve_gateway_stream_connection(
        server,
        store.clone(),
        publisher.clone(),
        "expected-token".to_string(),
        "boot_test".to_string(),
    ));

    write_gateway_client_frame(
        &mut client_write,
        &GatewayStreamClientFrame::Hello {
            version: 1,
            token: "expected-token".to_string(),
            subscriptions: vec![GatewayStreamSubscription {
                lane: GatewayStreamLane::DeveloperEvent,
                session_id: session.0.clone(),
                after_id: 1,
            }],
            hooks: None,
        },
    )
    .await
    .unwrap();

    assert_eq!(
        read_gateway_stream_frame_named(&mut client_read, "ready").await,
        GatewayStreamFrame::Ready {
            version: 1,
            daemon_boot_id: "boot_test".to_string(),
            resume: "store".to_string(),
        }
    );
    assert_eq!(
        read_gateway_stream_frame_named(&mut client_read, "developer-event gap").await,
        GatewayStreamFrame::Gap {
            lane: GatewayStreamLane::DeveloperEvent,
            session_id: session.0.clone(),
            after_id: 1,
            retry: "ephemeral".to_string(),
        }
    );
    match read_gateway_stream_frame_named(&mut client_read, "developer-event replay after gap")
        .await
    {
        GatewayStreamFrame::DeveloperEvent { event, .. } => {
            assert_eq!(event.seq, 3);
            assert_eq!(event.tool.as_deref(), Some("tool-2"));
        }
        other => panic!("expected developer event after gap, got {other:?}"),
    }

    drop(client_write);
    drop(client_read);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn gateway_stream_socket_writes_boot_scoped_manifest_next_to_gateway_json() {
    let home = tempfile::tempdir().unwrap();
    #[cfg(unix)]
    let runtime = tempfile::tempdir().unwrap();
    let home_s = home.path().display().to_string();
    #[cfg(unix)]
    let runtime_s = runtime.path().display().to_string();
    #[cfg(unix)]
    let _env = nexus::cli::ambient::TestEnvGuard::new(&[
        ("NEXUS_HOME", Some(home_s.as_str())),
        ("XDG_RUNTIME_DIR", Some(runtime_s.as_str())),
    ]);
    #[cfg(windows)]
    let _env = nexus::cli::ambient::TestEnvGuard::new(&[("NEXUS_HOME", Some(home_s.as_str()))]);

    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let publisher = GatewayStreamPublisher::new(16);

    {
        let handle =
            spawn_gateway_stream_socket(store, publisher, "boot_manifest_test".to_string())
                .unwrap();
        let manifest = read_gateway_stream_endpoint_manifest().unwrap();
        assert_eq!(manifest.version, 1);
        assert_eq!(manifest.daemon_boot_id, "boot_manifest_test");
        assert_eq!(manifest.path, handle.endpoint().path);
        #[cfg(unix)]
        assert!(manifest.path.starts_with(runtime.path()));
        #[cfg(windows)]
        assert!(manifest
            .path
            .to_string_lossy()
            .starts_with(r"\\.\pipe\nexus-gateway-stream-"));
        assert!(!manifest.token.is_empty());
        assert_eq!(
            gateway_stream_endpoint_manifest_path(),
            home.path().join("gateway-stream-endpoint.json")
        );
    }

    assert!(
        read_gateway_stream_endpoint_manifest().is_none(),
        "dropping the socket handle must remove the stale endpoint manifest"
    );
}

#[cfg(windows)]
#[tokio::test]
async fn gateway_stream_named_pipe_listener_accepts_client() {
    use tokio::net::windows::named_pipe::ClientOptions;

    let home = tempfile::tempdir().unwrap();
    let home_s = home.path().display().to_string();
    let _env = nexus::cli::ambient::TestEnvGuard::new(&[("NEXUS_HOME", Some(home_s.as_str()))]);
    std::fs::write(gateway_stream_endpoint_manifest_path(), b"stale endpoint").unwrap();
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let publisher = GatewayStreamPublisher::new(16);
    let handle = spawn_gateway_stream_socket(
        store,
        publisher,
        format!("windows-pipe-{}", std::process::id()),
    )
    .unwrap();
    let endpoint = handle.endpoint().clone();
    let mut client = ClientOptions::new().open(&endpoint.path).unwrap();

    write_gateway_client_frame(
        &mut client,
        &GatewayStreamClientFrame::Hello {
            version: 1,
            token: endpoint.token,
            subscriptions: vec![],
            hooks: None,
        },
    )
    .await
    .unwrap();

    assert!(matches!(
        read_gateway_stream_frame_named(&mut client, "Windows named-pipe ready frame").await,
        GatewayStreamFrame::Ready { .. }
    ));
}
