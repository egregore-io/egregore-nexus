use nexus_contracts::{
    GatewayProjectionAck, GatewayProjectionEvent, GatewayProjectionKind, GATEWAY_PROJECTION_VERSION,
};
use serde_json::json;

fn event(kind: GatewayProjectionKind) -> GatewayProjectionEvent {
    GatewayProjectionEvent {
        event_id: "message:m_01".to_string(),
        daemon_epoch: "boot-01".to_string(),
        seq: 7,
        occurred_at: 1_783_980_000_000,
        kind,
        version: GATEWAY_PROJECTION_VERSION,
        payload: json!({"messageId": "m_01"}),
    }
}

#[test]
fn projection_event_has_stable_camel_case_wire_shape() {
    let wire = serde_json::to_value(event(GatewayProjectionKind::MessageAccepted)).unwrap();
    assert_eq!(
        wire,
        json!({
            "eventId": "message:m_01",
            "daemonEpoch": "boot-01",
            "seq": 7,
            "occurredAt": 1_783_980_000_000_i64,
            "kind": "message.accepted",
            "version": 1,
            "payload": {"messageId": "m_01"}
        })
    );

    let decoded: GatewayProjectionEvent = serde_json::from_value(wire).unwrap();
    assert_eq!(decoded.event_id, "message:m_01");
    assert_eq!(decoded, event(GatewayProjectionKind::MessageAccepted));
}

#[test]
fn all_initial_projection_kinds_use_closed_dotted_tokens() {
    let cases = [
        (GatewayProjectionKind::IdentityUpserted, "identity.upserted"),
        (GatewayProjectionKind::IdentityRemoved, "identity.removed"),
        (GatewayProjectionKind::RuntimeUpserted, "runtime.upserted"),
        (GatewayProjectionKind::RuntimeStopped, "runtime.stopped"),
        (GatewayProjectionKind::ThreadDeclared, "thread.declared"),
        (
            GatewayProjectionKind::ThreadMembershipChanged,
            "thread.membership.changed",
        ),
        (GatewayProjectionKind::TopicDeclared, "topic.declared"),
        (
            GatewayProjectionKind::TopicSubscriptionChanged,
            "topic.subscription.changed",
        ),
        (GatewayProjectionKind::MessageAccepted, "message.accepted"),
        (GatewayProjectionKind::DeliverySettled, "delivery.settled"),
        (
            GatewayProjectionKind::NotificationEmitted,
            "notification.emitted",
        ),
        (GatewayProjectionKind::PresenceChanged, "presence.changed"),
    ];

    for (kind, token) in cases {
        assert_eq!(serde_json::to_value(kind).unwrap(), json!(token));
    }
}

#[test]
fn projection_event_rejects_unknown_version_and_invalid_position() {
    let mut unknown = serde_json::to_value(event(GatewayProjectionKind::MessageAccepted)).unwrap();
    unknown["version"] = json!(2);
    assert!(serde_json::from_value::<GatewayProjectionEvent>(unknown).is_err());

    let mut negative = serde_json::to_value(event(GatewayProjectionKind::MessageAccepted)).unwrap();
    negative["seq"] = json!(-1);
    assert!(serde_json::from_value::<GatewayProjectionEvent>(negative).is_err());
}

#[test]
fn projection_ack_validates_epoch_and_monotonic_position() {
    let ack = GatewayProjectionAck {
        daemon_epoch: "boot-01".to_string(),
        through_seq: 7,
    };
    let wire = serde_json::to_value(&ack).unwrap();
    assert_eq!(wire, json!({"daemonEpoch": "boot-01", "throughSeq": 7}));
    assert_eq!(
        serde_json::from_value::<GatewayProjectionAck>(wire).unwrap(),
        ack
    );
    assert!(serde_json::from_value::<GatewayProjectionAck>(
        json!({"daemonEpoch": "", "throughSeq": 7})
    )
    .is_err());
    assert!(serde_json::from_value::<GatewayProjectionAck>(
        json!({"daemonEpoch": "boot-01", "throughSeq": -1})
    )
    .is_err());
}

#[test]
fn agent_updates_are_not_canonical_projection_kinds() {
    assert!(serde_json::from_value::<GatewayProjectionKind>(json!("agent.update")).is_err());
    assert!(serde_json::from_value::<GatewayProjectionKind>(json!("terminal.raw")).is_err());
}
