use std::sync::Arc;
use std::time::Duration;

use nexus::daemon::gateway_hook_bridge::{GatewayHookBridge, GatewayHookBridgeError};
use nexus::daemon::gateway_stream_socket::{
    serve_gateway_stream_connection, write_gateway_client_frame, GatewayStreamClientFrame,
    GatewayStreamFrame, GatewayStreamPublisher,
};
use nexus::daemon::services::loop_wiring::GatewayMessageHookPort;
use nexus_common::HookGatewayMode;
use nexus_contracts::{
    GatewayHookCapabilities, HookBeforeSendRequest, HookEvaluationRequest, HookEvaluationResponse,
    HookMessage, HookSender, MessageHookPort, SendTarget,
};
use nexus_store::Store;
use tokio::io::{duplex, AsyncRead, AsyncReadExt};

fn capabilities(generation: &str) -> GatewayHookCapabilities {
    GatewayHookCapabilities {
        protocol_version: 1,
        generation: generation.to_string(),
        events: vec!["before_send".to_string()],
    }
}

fn request(evaluation_id: &str) -> HookEvaluationRequest {
    HookEvaluationRequest::BeforeSend(HookBeforeSendRequest {
        evaluation_id: evaluation_id.to_string(),
        message: HookMessage {
            sender: HookSender {
                agent_id: None,
                name: "fixture-sender".to_string(),
            },
            target: SendTarget::Post {
                thread: "release".to_string(),
            },
            body: evaluation_id.to_string(),
            summary: None,
            mention: Vec::new(),
            metadata: Default::default(),
        },
    })
}

fn response(request: &HookEvaluationRequest) -> HookEvaluationResponse {
    match request {
        HookEvaluationRequest::BeforeSend(request) => {
            HookEvaluationResponse::BeforeSend(nexus_contracts::HookBeforeSendResult {
                evaluation_id: request.evaluation_id.clone(),
                action: nexus_contracts::HookAction::Continue,
                message: request.message.clone(),
                timing: None,
                executed_by: Vec::new(),
            })
        }
        HookEvaluationRequest::AfterReceipt(_) => unreachable!(),
    }
}

#[tokio::test]
async fn concurrent_evaluations_complete_by_correlation_not_arrival_order() {
    let bridge = GatewayHookBridge::new(8);
    let (provider, mut incoming) = bridge.connect(capabilities("generation-a"));
    let first_bridge = bridge.clone();
    let first = tokio::spawn(async move {
        first_bridge
            .evaluate(request("he_first"), Duration::from_secs(1))
            .await
    });
    let second_bridge = bridge.clone();
    let second = tokio::spawn(async move {
        second_bridge
            .evaluate(request("he_second"), Duration::from_secs(1))
            .await
    });

    let one = incoming.recv().await.expect("first request");
    let two = incoming.recv().await.expect("second request");
    provider.complete(two.correlation_id.clone(), Ok(response(&two.request)));
    provider.complete(one.correlation_id.clone(), Ok(response(&one.request)));

    let first = first.await.expect("first task").expect("first result");
    let second = second.await.expect("second task").expect("second result");
    let ids = [first, second].map(|result| match result {
        HookEvaluationResponse::BeforeSend(result) => result.evaluation_id,
        HookEvaluationResponse::AfterReceipt(_) => unreachable!(),
    });
    assert_eq!(ids, ["he_first", "he_second"]);
    assert_eq!(bridge.pending_count(), 0);
}

#[tokio::test]
async fn unavailable_unsupported_timeout_and_disconnect_are_typed_and_bounded() {
    let bridge = GatewayHookBridge::new(1);
    assert!(matches!(
        bridge
            .evaluate(request("he_absent"), Duration::from_millis(10))
            .await,
        Err(GatewayHookBridgeError::Unavailable)
    ));

    let (provider, mut incoming) = bridge.connect(GatewayHookCapabilities {
        protocol_version: 1,
        generation: "generation-a".to_string(),
        events: vec!["after_receipt".to_string()],
    });
    assert!(matches!(
        bridge
            .evaluate(request("he_unsupported"), Duration::from_millis(10))
            .await,
        Err(GatewayHookBridgeError::UnsupportedEvent(_))
    ));
    drop(provider);

    let (provider, mut replacement) = bridge.connect(capabilities("generation-b"));
    let timed_bridge = bridge.clone();
    let timed = tokio::spawn(async move {
        timed_bridge
            .evaluate(request("he_timeout"), Duration::from_millis(20))
            .await
    });
    replacement.recv().await.expect("timed request");
    assert!(matches!(
        timed.await.expect("timed task"),
        Err(GatewayHookBridgeError::TimedOut)
    ));
    assert_eq!(bridge.pending_count(), 0);

    let disconnected_bridge = bridge.clone();
    let disconnected = tokio::spawn(async move {
        disconnected_bridge
            .evaluate(request("he_disconnect"), Duration::from_secs(1))
            .await
    });
    replacement.recv().await.expect("disconnect request");
    drop(provider);
    assert!(matches!(
        disconnected.await.expect("disconnect task"),
        Err(GatewayHookBridgeError::Disconnected)
    ));
    assert_eq!(bridge.pending_count(), 0);

    // The unused receiver from the unsupported provider is intentionally drained by replacement.
    assert!(incoming.recv().await.is_none());
}

#[tokio::test]
async fn stream_capability_negotiation_carries_one_correlated_terminal_result() {
    let store = Arc::new(Store::open(":memory:").await.expect("store"));
    store.migrate().await.expect("migrate");
    let publisher = GatewayStreamPublisher::new(16);
    let bridge = publisher.hook_bridge();
    let (client, server) = duplex(16 * 1024);
    let (mut reader, mut writer) = tokio::io::split(client);
    let server_task = tokio::spawn(serve_gateway_stream_connection(
        server,
        store,
        publisher,
        "token".to_string(),
        "boot".to_string(),
    ));
    write_gateway_client_frame(
        &mut writer,
        &GatewayStreamClientFrame::Hello {
            version: 1,
            token: "token".to_string(),
            subscriptions: Vec::new(),
            hooks: Some(capabilities("generation-stream")),
        },
    )
    .await
    .expect("hello");
    assert!(matches!(
        read_frame(&mut reader).await,
        GatewayStreamFrame::Ready { .. }
    ));

    let evaluation = bridge.evaluate(request("he_stream"), Duration::from_secs(1));
    tokio::pin!(evaluation);
    assert!(
        futures::poll!(evaluation.as_mut()).is_pending(),
        "a ready frame must mean the hook provider is already installed"
    );
    let GatewayStreamFrame::HookEvaluate { evaluation: frame } = read_frame(&mut reader).await
    else {
        panic!("expected hook evaluation frame")
    };
    write_gateway_client_frame(
        &mut writer,
        &GatewayStreamClientFrame::HookResult {
            correlation_id: frame.correlation_id,
            result: Some(response(&frame.request)),
            error: None,
        },
    )
    .await
    .expect("hook result");

    assert!(matches!(
        evaluation.await,
        Ok(HookEvaluationResponse::BeforeSend(result)) if result.evaluation_id == "he_stream"
    ));
    drop(reader);
    drop(writer);
    server_task
        .await
        .expect("server task")
        .expect("server result");
}

#[tokio::test]
async fn optional_gateway_mode_bypasses_absence_but_required_mode_fails_typed() {
    let optional =
        GatewayMessageHookPort::new(None, HookGatewayMode::Optional, Duration::from_millis(20));
    let request = match request("he_optional") {
        HookEvaluationRequest::BeforeSend(request) => request,
        HookEvaluationRequest::AfterReceipt(_) => unreachable!(),
    };
    let result = optional.before_send(request.clone()).await.unwrap();
    assert_eq!(result.evaluation_id, "he_optional");
    assert_eq!(result.message, request.message);

    let required =
        GatewayMessageHookPort::new(None, HookGatewayMode::Required, Duration::from_millis(20));
    let error = required.before_send(request).await.unwrap_err();
    assert_eq!(error.code, nexus_contracts::codes::HOOK_GATEWAY_UNAVAILABLE);
}

async fn read_frame<R>(reader: &mut R) -> GatewayStreamFrame
where
    R: AsyncRead + Unpin,
{
    let length = reader.read_u32().await.expect("frame length") as usize;
    let mut payload = vec![0; length];
    reader
        .read_exact(&mut payload)
        .await
        .expect("frame payload");
    serde_json::from_slice(&payload).expect("frame JSON")
}
