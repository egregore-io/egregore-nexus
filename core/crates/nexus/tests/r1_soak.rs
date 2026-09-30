use std::sync::Arc;
use std::time::Instant;

use nexus::daemon::gateway_stream_socket::{
    serve_gateway_stream_connection, write_gateway_client_frame, GatewayStreamClientFrame,
    GatewayStreamFrame, GatewayStreamLane, GatewayStreamPublisher, GatewayStreamSubscription,
};
use nexus_contracts::{AgentUpdateKind, SessionId};
use nexus_store::{
    command_kinds,
    repos::{CommandIntents, NewCommandIntent, StreamEvents},
    Store,
};
use serde_json::json;
use tokio::io::{duplex, AsyncReadExt};
use tokio::time::{timeout, Duration};

async fn read_frame<R>(reader: &mut R) -> GatewayStreamFrame
where
    R: tokio::io::AsyncRead + Unpin,
{
    let len = reader.read_u32().await.unwrap() as usize;
    let mut payload = vec![0_u8; len];
    reader.read_exact(&mut payload).await.unwrap();
    serde_json::from_slice(&payload).unwrap()
}

async fn read_frame_named<R>(reader: &mut R, name: &str) -> GatewayStreamFrame
where
    R: tokio::io::AsyncRead + Unpin,
{
    timeout(Duration::from_secs(2), read_frame(reader))
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {name}"))
}

#[tokio::test]
async fn command_intent_wake_signal_arrives_before_poll_floor() {
    let store = Store::open(":memory:").await.unwrap();
    store.migrate().await.unwrap();
    let epoch = store.command_intents_epoch();
    let wait = store.wait_for_command_intent_after(epoch);
    let started = Instant::now();

    CommandIntents::new(&store)
        .insert_pending(NewCommandIntent {
            command_id: "cmd_r1_1".to_string(),
            kind: command_kinds::harness::WARM.to_string(),
            project: "default".to_string(),
            caller_name: "soak".to_string(),
            caller_session_id: None,
            caller_agent_id: None,
            caller_runtime_id: None,
            caller_client_key: None,
            caller_kind: Some("agent".to_string()),
            caller_tier: Some("admin".to_string()),
            idempotency_key: None,
            request_json: json!({"name":"soak-r1"}).to_string(),
            created_at: 1,
        })
        .await
        .unwrap();

    timeout(Duration::from_millis(50), wait)
        .await
        .expect("command intent wake must not wait for 250ms poll fallback");
    assert!(
        started.elapsed() < Duration::from_millis(100),
        "wake path took {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn gateway_stream_soak_observes_catchup_then_live_without_missing_ids() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let session = SessionId("s_soak_r1".to_string());
    let publisher = GatewayStreamPublisher::new(16);
    let streams = StreamEvents::new(&store);

    let first = streams
        .append(&session, "text", &json!({"text":"catchup-1"}).to_string())
        .await
        .unwrap();
    let second = streams
        .append(&session, "text", &json!({"text":"catchup-2"}).to_string())
        .await
        .unwrap();

    let (client, server) = duplex(8192);
    let (mut client_read, mut client_write) = tokio::io::split(client);
    let server_task = tokio::spawn(serve_gateway_stream_connection(
        server,
        store.clone(),
        publisher.clone(),
        "token".to_string(),
        "boot_r1".to_string(),
    ));

    write_gateway_client_frame(
        &mut client_write,
        &GatewayStreamClientFrame::Hello {
            version: 1,
            token: "token".to_string(),
            subscriptions: vec![GatewayStreamSubscription {
                lane: GatewayStreamLane::Agent,
                session_id: session.0.clone(),
                after_id: 0,
            }],
        },
    )
    .await
    .unwrap();

    assert!(matches!(
        read_frame_named(&mut client_read, "ready").await,
        GatewayStreamFrame::Ready { .. }
    ));

    let mut seen = Vec::new();
    for name in ["catchup first", "catchup second"] {
        match read_frame_named(&mut client_read, name).await {
            GatewayStreamFrame::AgentUpdate {
                stream_event_id, ..
            } => seen.push(stream_event_id),
            other => panic!("expected agent update, got {other:?}"),
        }
    }

    let live = streams
        .append(&session, "text", &json!({"text":"live-1"}).to_string())
        .await
        .unwrap();
    publisher.publish_agent_update(
        &session,
        live,
        AgentUpdateKind::Text,
        json!({"text":"live-1"}),
    );

    match read_frame_named(&mut client_read, "live").await {
        GatewayStreamFrame::AgentUpdate {
            stream_event_id, ..
        } => seen.push(stream_event_id),
        other => panic!("expected live agent update, got {other:?}"),
    }

    assert_eq!(seen, vec![first, second, live]);
    drop(client_write);
    drop(client_read);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn gateway_stream_raw_lane_reports_store_gap_for_fallback_repair() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let publisher = GatewayStreamPublisher::new(16);
    let (client, server) = duplex(4096);
    let (mut client_read, mut client_write) = tokio::io::split(client);

    let server_task = tokio::spawn(serve_gateway_stream_connection(
        server,
        store,
        publisher,
        "token".to_string(),
        "boot_r1".to_string(),
    ));

    write_gateway_client_frame(
        &mut client_write,
        &GatewayStreamClientFrame::Hello {
            version: 1,
            token: "token".to_string(),
            subscriptions: vec![GatewayStreamSubscription {
                lane: GatewayStreamLane::Raw,
                session_id: "s_soak_raw".to_string(),
                after_id: 7,
            }],
        },
    )
    .await
    .unwrap();

    assert!(matches!(
        read_frame_named(&mut client_read, "ready").await,
        GatewayStreamFrame::Ready { .. }
    ));
    assert_eq!(
        read_frame_named(&mut client_read, "raw gap").await,
        GatewayStreamFrame::Gap {
            lane: GatewayStreamLane::Raw,
            session_id: "s_soak_raw".to_string(),
            after_id: 7,
            retry: "store".to_string(),
        }
    );

    drop(client_write);
    drop(client_read);
    server_task.await.unwrap().unwrap();
}
