use nexus_contracts::{
    Ack, DeliveryTiming, HookAction, HookAfterReceiptRequest, HookAfterReceiptResult,
    HookBeforeSendRequest, HookBeforeSendResult, HookExecutedBy, HookMessage, HookSender,
    MessageId, MessageMetadataMergeRequest, SendRequest, SendTarget,
};
use serde_json::json;

fn hook_message() -> HookMessage {
    HookMessage {
        sender: HookSender {
            agent_id: Some("a_fixture_sender".into()),
            name: "fixture-sender".into(),
        },
        target: SendTarget::Post {
            thread: "release".into(),
        },
        body: "ship it".into(),
        summary: None,
        mention: vec!["fable".into()],
        metadata: serde_json::Map::from_iter([("nested".into(), json!({"owner": "gateway"}))]),
    }
}

#[test]
fn send_request_metadata_is_optional_and_omitted_when_absent() {
    let compact: SendRequest = serde_json::from_value(json!({
        "to": {"verb": "reply"},
        "body": "hello",
        "mention": []
    }))
    .unwrap();
    assert!(compact.metadata.is_none());
    assert_eq!(serde_json::to_value(compact).unwrap().get("metadata"), None);

    let with_metadata: SendRequest = serde_json::from_value(json!({
        "to": {"verb": "reply"},
        "body": "hello",
        "mention": [],
        "metadata": {"risk": {"score": 3}}
    }))
    .unwrap();
    assert_eq!(with_metadata.metadata.unwrap()["risk"]["score"], 3);
}

#[test]
fn delivery_timing_uses_the_public_policy_names() {
    for (wire, value) in [
        ("interrupt", DeliveryTiming::Interrupt),
        ("yield_turn", DeliveryTiming::YieldTurn),
        ("after_tool_loop", DeliveryTiming::AfterToolLoop),
    ] {
        assert_eq!(serde_json::to_value(&value).unwrap(), wire);
        assert_eq!(
            serde_json::from_value::<DeliveryTiming>(json!(wire)).unwrap(),
            value
        );
    }
}

#[test]
fn before_send_round_trips_mutation_rejection_and_provenance() {
    let request = HookBeforeSendRequest {
        evaluation_id: "he_01".into(),
        message: hook_message(),
    };
    let request_json = serde_json::to_value(&request).unwrap();
    assert_eq!(request_json["evaluationId"], "he_01");
    assert_eq!(request_json["message"]["target"]["verb"], "post");
    assert_eq!(
        serde_json::from_value::<HookBeforeSendRequest>(request_json).unwrap(),
        request
    );

    let result = HookBeforeSendResult {
        evaluation_id: "he_01".into(),
        action: HookAction::Continue,
        message: hook_message(),
        timing: Some(DeliveryTiming::YieldTurn),
        executed_by: vec![HookExecutedBy {
            hook_id: "redact-secrets".into(),
            entrypoint: "main".into(),
            runtime: "python3".into(),
            artifact_digest: "sha256:abc".into(),
            invocation_id: "hi_01".into(),
            outcome: "success".into(),
            attestation: json!({
                "algorithm": "ed25519",
                "keyId": "gwk_1",
                "signature": "c2ln"
            }),
        }],
    };
    let result_json = serde_json::to_value(&result).unwrap();
    assert_eq!(result_json["timing"], "yield_turn");
    assert_eq!(result_json["executedBy"][0]["hookId"], "redact-secrets");
    assert_eq!(
        serde_json::from_value::<HookBeforeSendResult>(result_json).unwrap(),
        result
    );

    let rejected: HookBeforeSendResult = serde_json::from_value(json!({
        "evaluationId": "he_02",
        "action": "reject",
        "message": serde_json::to_value(hook_message()).unwrap(),
        "executedBy": []
    }))
    .unwrap();
    assert_eq!(rejected.action, HookAction::Reject);
}

#[test]
fn after_receipt_round_trips_message_receipt_and_metadata_patch() {
    let request = HookAfterReceiptRequest {
        invocation_id: "hi_receipt_01".into(),
        message: hook_message(),
        receipt: Ack {
            message_id: MessageId("m_01".into()),
            fanout: None,
        },
        executed_by: vec![],
    };
    let request_json = serde_json::to_value(&request).unwrap();
    assert_eq!(request_json["receipt"]["messageId"], "m_01");
    assert_eq!(
        serde_json::from_value::<HookAfterReceiptRequest>(request_json).unwrap(),
        request
    );

    let result = HookAfterReceiptResult {
        invocation_id: "hi_receipt_01".into(),
        metadata: Some(serde_json::Map::from_iter([(
            "indexed".into(),
            json!(true),
        )])),
        executed_by: vec![],
    };
    let result_json = serde_json::to_value(&result).unwrap();
    assert_eq!(result_json["metadata"]["indexed"], true);
    assert_eq!(
        serde_json::from_value::<HookAfterReceiptResult>(result_json).unwrap(),
        result
    );
}

#[test]
fn metadata_merge_carries_a_stable_invocation_id() {
    let request = MessageMetadataMergeRequest {
        message_id: MessageId("m_01".into()),
        invocation_id: "hi_receipt_01".into(),
        metadata: serde_json::Map::from_iter([("indexed".into(), json!(true))]),
    };
    let wire = serde_json::to_value(&request).unwrap();
    assert_eq!(wire["messageId"], "m_01");
    assert_eq!(wire["invocationId"], "hi_receipt_01");
    assert_eq!(wire["metadata"]["indexed"], true);
    assert_eq!(
        serde_json::from_value::<MessageMetadataMergeRequest>(wire).unwrap(),
        request
    );
}
