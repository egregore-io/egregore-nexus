use nexus_contracts::{AgentId, NotifySendRequest, NotifyTarget};
use serde_json::json;

#[test]
fn explicit_notify_targets_round_trip_without_fanout_controls() {
    let cases = [
        (
            NotifyTarget::Auto {
                value: "reviewers".into(),
            },
            json!({"kind": "auto", "value": "reviewers"}),
        ),
        (
            NotifyTarget::Agent {
                agent_id: AgentId("a_stable".into()),
            },
            json!({"kind": "agent", "agentId": "a_stable"}),
        ),
        (
            NotifyTarget::Name {
                name: "alice".into(),
            },
            json!({"kind": "name", "name": "alice"}),
        ),
        (
            NotifyTarget::Group {
                group: "reviewers".into(),
            },
            json!({"kind": "group", "group": "reviewers"}),
        ),
        (
            NotifyTarget::Thread {
                thread: "release".into(),
            },
            json!({"kind": "thread", "thread": "release"}),
        ),
    ];

    for (target, expected) in cases {
        assert_eq!(serde_json::to_value(&target).unwrap(), expected);
        let decoded: NotifyTarget = serde_json::from_value(expected).unwrap();
        assert_eq!(decoded, target);
    }

    let request = NotifySendRequest {
        target: NotifyTarget::Auto {
            value: "release".into(),
        },
        source: Some("watchdog".into()),
        body: "checkpoint failed".into(),
        idempotency_key: Some("gate:42".into()),
    };
    let json = serde_json::to_value(&request).unwrap();
    assert_eq!(json["source"], "watchdog");
    assert!(json.get("fanout").is_none());
    assert_eq!(
        serde_json::from_value::<NotifySendRequest>(json).unwrap(),
        request
    );
}
