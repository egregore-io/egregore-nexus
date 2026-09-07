use super::*;

#[tokio::test]
async fn owner_is_checked_after_blocked_writer_readiness_not_before_it() {
    let (client, server) = tokio::io::duplex(8192);
    let release = Arc::new(tokio::sync::Notify::new());
    let server_release = release.clone();
    let (seen, mut received) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let mut socket = tokio_tungstenite::accept_async(server).await.unwrap();
        server_release.notified().await;
        while let Some(Ok(Message::Text(text))) = socket.next().await {
            if seen
                .send(serde_json::from_str::<Value>(&text).unwrap())
                .is_err()
            {
                break;
            }
        }
    });
    let rpc = JsonRpc::connect_stream(UDS_WEBSOCKET_HANDSHAKE_URL, Box::new(client), None)
        .await
        .unwrap();
    {
        let mut writer = rpc.writer.lock().await;
        std::pin::Pin::new(&mut *writer)
            .start_send(Message::Text(
                serde_json::json!({"method": "previous", "padding": "x".repeat(1 << 20)})
                    .to_string()
                    .into(),
            ))
            .unwrap();
        futures::future::poll_fn(|cx| std::pin::Pin::new(&mut *writer).poll_ready(cx))
            .await
            .unwrap();
        std::pin::Pin::new(&mut *writer)
            .start_send(Message::Text(
                serde_json::json!({"method": "queued-previous"})
                    .to_string()
                    .into(),
            ))
            .unwrap();
    }
    let owner = super::super::turn_completion::CodexTurnTracker::default().new_owner(None);
    assert!(owner.publish_owner("thread"));
    let checked = std::sync::atomic::AtomicBool::new(false);
    let mut request = Box::pin(
        rpc.request_with_admission("revoked", Value::Null, |submit| {
            checked.store(true, Ordering::SeqCst);
            owner.admit(submit)
        }),
    );
    assert!(futures::poll!(&mut request).is_pending());
    assert!(
        !checked.load(Ordering::SeqCst),
        "ownership was checked before writer became ready"
    );
    owner.revoke_owner();
    release.notify_one();
    assert!(matches!(request.await, Err(CodexRpcError::Closed)));
    assert!(checked.load(Ordering::SeqCst));
    assert!(rpc.pending.lock().unwrap().is_empty());
    rpc.notify("control", Value::Null).await.unwrap();
    assert_eq!(received.recv().await.unwrap()["method"], "previous");
    assert_eq!(received.recv().await.unwrap()["method"], "queued-previous");
    assert_eq!(received.recv().await.unwrap()["method"], "control");
    assert!(received.try_recv().is_err());
}

#[tokio::test]
async fn revoked_owner_rejects_every_parked_mutation_before_local_admission() {
    for method in [
        "turn/start",
        "turn/steer",
        "thread/compact/start",
        "turn/interrupt",
    ] {
        let (rpc, mut seen) = connected().await;
        let owner = super::super::turn_completion::CodexTurnTracker::default().new_owner(None);
        assert!(owner.publish_owner("thread"));
        let writer = rpc.writer.lock().await;
        let mut request =
            Box::pin(rpc.request_with_admission(method, Value::Null, |submit| owner.admit(submit)));
        assert!(futures::poll!(&mut request).is_pending());
        owner.revoke_owner();
        drop(writer);
        assert!(matches!(request.await, Err(CodexRpcError::Closed)));
        assert!(rpc.pending.lock().unwrap().is_empty());
        rpc.notify("control", Value::Null).await.unwrap();
        assert_eq!(seen.recv().await.unwrap()["method"], "control");
        assert!(seen.try_recv().is_err());
    }
}

#[tokio::test]
async fn cancellation_during_flush_keeps_one_admitted_frame_without_pending_leak() {
    let (client, server) = tokio::io::duplex(8192);
    let release = Arc::new(tokio::sync::Notify::new());
    let server_release = release.clone();
    let (seen, mut received) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let mut socket = tokio_tungstenite::accept_async(server).await.unwrap();
        server_release.notified().await;
        while let Some(Ok(Message::Text(text))) = socket.next().await {
            if seen
                .send(serde_json::from_str::<Value>(&text).unwrap())
                .is_err()
            {
                break;
            }
        }
    });
    let rpc = JsonRpc::connect_stream(UDS_WEBSOCKET_HANDSHAKE_URL, Box::new(client), None)
        .await
        .unwrap();
    let owner = super::super::turn_completion::CodexTurnTracker::default().new_owner(None);
    assert!(owner.publish_owner("thread"));
    let admitted = std::sync::atomic::AtomicBool::new(false);
    let mut request = Box::pin(rpc.request_with_admission(
        "original",
        Value::String("x".repeat(1 << 20)),
        |submit| {
            owner.admit(|| {
                submit()?;
                admitted.store(true, Ordering::SeqCst);
                Ok(())
            })
        },
    ));
    assert!(futures::poll!(&mut request).is_pending());
    assert!(
        admitted.load(Ordering::SeqCst),
        "test must cross start_send before cancellation"
    );
    assert!(
        rpc.writer.try_lock().is_err(),
        "writer must remain held during blocked flush"
    );
    owner.revoke_owner();
    drop(request);
    assert!(rpc.pending.lock().unwrap().is_empty());
    release.notify_one();
    rpc.notify("control", Value::Null).await.unwrap();
    assert_eq!(received.recv().await.unwrap()["method"], "original");
    assert_eq!(received.recv().await.unwrap()["method"], "control");
    assert!(
        received.try_recv().is_err(),
        "post-admission cancellation must not resend"
    );
}

async fn connected() -> (JsonRpc, mpsc::UnboundedReceiver<Value>) {
    let (client, server) = tokio::io::duplex(8192);
    let (seen, received) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let mut socket = tokio_tungstenite::accept_async(server).await.unwrap();
        while let Some(Ok(Message::Text(text))) = socket.next().await {
            seen.send(serde_json::from_str(&text).unwrap()).unwrap();
        }
    });
    (
        JsonRpc::connect_stream(UDS_WEBSOCKET_HANDSHAKE_URL, Box::new(client), None)
            .await
            .unwrap(),
        received,
    )
}

#[tokio::test]
async fn cancelling_before_writer_acquisition_removes_only_original_pending() {
    let (rpc, mut seen) = connected().await;
    let writer = rpc.writer.lock().await;
    let mut request = Box::pin(rpc.request("old", Value::Null));
    assert!(futures::poll!(&mut request).is_pending());
    assert_eq!(rpc.pending.lock().unwrap().len(), 1);
    let mut other = Box::pin(rpc.request("other", Value::Null));
    assert!(futures::poll!(&mut other).is_pending());
    assert_eq!(rpc.pending.lock().unwrap().len(), 2);
    drop(request);
    assert_eq!(
        rpc.pending.lock().unwrap().len(),
        1,
        "cancellation removed another request's correlation"
    );
    drop(other);
    assert!(
        rpc.pending.lock().unwrap().is_empty(),
        "cancelled request correlation leaked"
    );
    drop(writer);
    rpc.notify("control", Value::Null).await.unwrap();
    assert_eq!(seen.recv().await.unwrap()["method"], "control");
    assert!(
        seen.try_recv().is_err(),
        "pre-admission cancellation emitted a frame"
    );
}

#[tokio::test]
async fn cancelling_response_wait_does_not_retract_or_repeat_admitted_request() {
    let (rpc, mut seen) = connected().await;
    let mut request = Box::pin(rpc.request("original", Value::Null));
    tokio::select! {
        _ = &mut request => panic!("server deliberately withholds response"),
        frame = seen.recv() => assert_eq!(frame.unwrap()["method"], "original"),
    }
    drop(request);
    assert!(
        rpc.pending.lock().unwrap().is_empty(),
        "cancelled response waiter leaked"
    );
    rpc.notify("control", Value::Null).await.unwrap();
    assert_eq!(seen.recv().await.unwrap()["method"], "control");
    assert!(seen.try_recv().is_err(), "admitted request was duplicated");
}
