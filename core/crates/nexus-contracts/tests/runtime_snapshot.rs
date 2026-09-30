use nexus_contracts::{
    RuntimeSnapshotFrame, RuntimeSubscribeFrame, RuntimeUnavailableFrame, RuntimeUnsubscribeFrame,
};
use serde_json::{json, Value};

#[test]
fn runtime_snapshot_wire_goldens_and_nested_model_evidence() {
    let fixtures: Vec<Value> =
        serde_json::from_str(include_str!("../fixtures/runtime.snapshot.json")).unwrap();
    for frame in fixtures {
        let back = match frame["t"].as_str().unwrap() {
            "runtime.subscribe" => serde_json::to_value(
                serde_json::from_value::<RuntimeSubscribeFrame>(frame.clone()).unwrap(),
            )
            .unwrap(),
            "runtime.unsubscribe" => serde_json::to_value(
                serde_json::from_value::<RuntimeUnsubscribeFrame>(frame.clone()).unwrap(),
            )
            .unwrap(),
            "runtime.snapshot" => serde_json::to_value(
                serde_json::from_value::<RuntimeSnapshotFrame>(frame.clone()).unwrap(),
            )
            .unwrap(),
            "runtime.unavailable" => serde_json::to_value(
                serde_json::from_value::<RuntimeUnavailableFrame>(frame.clone()).unwrap(),
            )
            .unwrap(),
            _ => panic!("unknown fixture"),
        };
        assert_eq!(back, frame);
    }
    let runtime: Value =
        serde_json::from_str(include_str!("../fixtures/runtime.telemetry.json")).unwrap();
    let frame = json!({"t":"runtime.snapshot","subscriptionId":"fresh-connection/1","agentId":runtime["agentId"],"sequence":1,"runtimes":[runtime]});
    assert_eq!(
        serde_json::to_value(
            serde_json::from_value::<RuntimeSnapshotFrame>(frame.clone()).unwrap()
        )
        .unwrap(),
        frame
    );
}

#[test]
fn runtime_snapshot_rejects_wrong_tags_unsafe_sequence_and_unbounded_ids() {
    let original = json!({"t":"runtime.snapshot","subscriptionId":"sub","agentId":"a","sequence":1,"runtimes":[]});
    for (field, value) in [
        ("t", json!("runtime.unavailable")),
        ("sequence", json!(0)),
        ("sequence", json!(9_007_199_254_740_992_u64)),
        ("subscriptionId", json!("x".repeat(129))),
        ("subscriptionId", json!("bad\n")),
    ] {
        let mut bad = original.clone();
        bad[field] = value;
        assert!(
            serde_json::from_value::<RuntimeSnapshotFrame>(bad).is_err(),
            "{field}"
        );
    }
}
