use nexus_contracts::telemetry::{
    NativeTelemetryUpdate, NativeTelemetryValue, TelemetryProvenance,
};
use nexus_harness_claude::native::statusline::decode;
use serde_json::{json, Value};

fn sample() -> Value {
    json!({"session_id":"root-a","prompt_id":"prompt-a",
        "context_window":{"context_window_size":200000,
            "used_percentage":21.5,"remaining_percentage":78.5,
            "total_input_tokens":43000,"total_output_tokens":999,
            "current_usage":{"input_tokens":1000,"output_tokens":999,
                "cache_creation_input_tokens":2000,"cache_read_input_tokens":40000}},
        "rate_limits":{"five_hour":{"used_percentage":0,"resets_at":1800000000},
            "seven_day":{"used_percentage":42.5,"resets_at":1800600000}}})
}

#[test]
fn copies_native_percent_and_derives_input_only_occupancy_not_cumulative_usage() {
    let result = decode(&sample(), "root-a", 123).unwrap();
    assert_eq!(result.prompt_id.as_deref(), Some("prompt-a"));
    assert_eq!(result.updates.len(), 2);
    let NativeTelemetryUpdate::Context {
        value: NativeTelemetryValue::Observed(context),
        ..
    } = &result.updates[0]
    else {
        panic!("context absent")
    };
    assert_eq!(context.metadata.observed_at.get(), 123);
    assert_eq!(context.used_tokens.as_ref().unwrap().value.get(), 43000);
    assert_eq!(
        context.used_tokens.as_ref().unwrap().provenance,
        TelemetryProvenance::Derived
    );
    assert_eq!(
        context.remaining_tokens.as_ref().unwrap().value.get(),
        157000
    );
    assert_eq!(
        context.remaining_percent.as_ref().unwrap().value.get(),
        78.5
    );
    assert_eq!(
        context.remaining_percent.as_ref().unwrap().provenance,
        TelemetryProvenance::Native
    );
    assert!(context.model.is_none());
    assert!(result
        .updates
        .iter()
        .all(|u| !matches!(u, NativeTelemetryUpdate::Usage { .. })));
}

#[test]
fn null_usage_after_compaction_clears_occupancy_without_inventing_zero() {
    let mut raw = sample();
    raw["context_window"]["current_usage"] = Value::Null;
    raw["context_window"]["used_percentage"] = Value::Null;
    raw["context_window"]["remaining_percentage"] = Value::Null;
    let result = decode(&raw, "root-a", 123).unwrap();
    let NativeTelemetryUpdate::Context {
        value: NativeTelemetryValue::Observed(context),
        ..
    } = &result.updates[0]
    else {
        panic!("capacity absent")
    };
    assert_eq!(
        context
            .effective_capacity_tokens
            .as_ref()
            .unwrap()
            .value
            .get(),
        200000
    );
    assert!(context.used_tokens.is_none());
    assert!(context.remaining_tokens.is_none());
    assert!(context.remaining_percent.is_none());
}

#[test]
fn quota_keeps_native_zero_fraction_reset_units_and_unknown_account() {
    let result = decode(&sample(), "root-a", 123).unwrap();
    let NativeTelemetryUpdate::Quota {
        value: NativeTelemetryValue::Observed(quota),
        ..
    } = &result.updates[1]
    else {
        panic!("quota absent")
    };
    assert!(quota.account_id.is_none());
    assert_eq!(quota.provider_id.as_str(), "claude");
    assert_eq!(quota.windows[0].used_percent.unwrap().get(), 0.0);
    assert_eq!(quota.windows[0].resets_at.unwrap().get(), 1800000000000);
    assert_eq!(quota.windows[0].window_seconds.unwrap().get(), 18000);
    assert_eq!(quota.windows[1].used_percent.unwrap().get(), 42.5);
    assert!(quota.windows.iter().all(|w| w.remaining_percent.is_none()));
}

#[test]
fn missing_categories_are_explicit_unknown_and_foreign_identity_is_rejected() {
    let raw = json!({"session_id":"root-a"});
    let result = decode(&raw, "root-a", 0).unwrap();
    assert!(matches!(
        result.updates[0],
        NativeTelemetryUpdate::Context {
            value: NativeTelemetryValue::Unknown,
            ..
        }
    ));
    assert!(matches!(
        result.updates[1],
        NativeTelemetryUpdate::Quota {
            value: NativeTelemetryValue::Unknown,
            ..
        }
    ));
    assert!(decode(&sample(), "root-b", 123).is_none());
    assert!(decode(&json!({"session_id":""}), "", 123).is_none());
    assert!(decode(&sample(), "root-a", i64::MAX).is_none());
    let mut raw = sample();
    raw["prompt_id"] = json!(42);
    assert!(decode(&raw, "root-a", 123).is_none());
}

#[test]
fn overage_keeps_raw_occupancy_and_native_percent_without_negative_remaining() {
    let mut raw = sample();
    raw["context_window"]["current_usage"]["input_tokens"] = json!(210000);
    raw["context_window"]["used_percentage"] = json!(126);
    raw["context_window"]["remaining_percentage"] = json!(0);
    let result = decode(&raw, "root-a", 123).unwrap();
    let NativeTelemetryUpdate::Context {
        value: NativeTelemetryValue::Observed(context),
        ..
    } = &result.updates[0]
    else {
        panic!("overage absent")
    };
    assert_eq!(context.used_tokens.as_ref().unwrap().value.get(), 252000);
    assert_eq!(context.used_percent.as_ref().unwrap().value.get(), 126.0);
    assert!(context.remaining_tokens.is_none());
}

#[test]
fn invalid_numbers_invalidate_only_the_affected_category() {
    for bad in [json!(-1), json!("100"), json!(9007199254740992u64)] {
        let mut raw = sample();
        raw["context_window"]["current_usage"]["input_tokens"] = bad;
        let result = decode(&raw, "root-a", 123).unwrap();
        assert!(matches!(
            result.updates[0],
            NativeTelemetryUpdate::Context {
                value: NativeTelemetryValue::Invalid,
                ..
            }
        ));
        assert!(matches!(
            result.updates[1],
            NativeTelemetryUpdate::Quota {
                value: NativeTelemetryValue::Observed(_),
                ..
            }
        ));
    }
    let mut raw = sample();
    raw["rate_limits"]["five_hour"]["resets_at"] = json!(i64::MAX);
    let result = decode(&raw, "root-a", 123).unwrap();
    assert!(matches!(
        result.updates[0],
        NativeTelemetryUpdate::Context {
            value: NativeTelemetryValue::Observed(_),
            ..
        }
    ));
    assert!(matches!(
        result.updates[1],
        NativeTelemetryUpdate::Quota {
            value: NativeTelemetryValue::Invalid,
            ..
        }
    ));
}

#[test]
fn capture_is_bounded_allowlisted_and_preserves_native_capture_time() {
    use nexus_harness_claude::native::statusline_capture::{
        capture, read_snapshot, MAX_STATUSLINE_BYTES,
    };
    let dir = temp_dir("capture");
    let path = dir.join("snapshot.json");
    assert!(read_snapshot(&path).unwrap().is_none());
    let mut raw = sample();
    raw["conversation"] = json!("must not be persisted");
    raw["context_window"]["unexpected_secret"] = json!("must not be persisted either");
    raw["rate_limits"]["five_hour"]["extra"] = json!("private");
    capture(serde_json::to_vec(&raw).unwrap().as_slice(), &path, 123).unwrap();
    let snapshot = read_snapshot(&path).unwrap().unwrap();
    assert_eq!(snapshot.observed_at, 123);
    assert!(snapshot.payload.get("conversation").is_none());
    assert!(snapshot.payload["context_window"]
        .get("unexpected_secret")
        .is_none());
    assert!(snapshot.payload["rate_limits"]["five_hour"]
        .get("extra")
        .is_none());
    assert_eq!(
        decode(&snapshot.payload, "root-a", snapshot.observed_at),
        decode(&raw, "root-a", 123)
    );
    let prior = std::fs::read(&path).unwrap();
    assert!(capture(vec![b' '; MAX_STATUSLINE_BYTES + 1].as_slice(), &path, 124).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), prior);
    assert!(capture(b"{broken".as_slice(), &path, 124).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), prior);
    assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 1);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn snapshot_reader_rejects_partial_or_oversized_data_without_freshening() {
    use nexus_harness_claude::native::statusline_capture::{read_snapshot, MAX_STATUSLINE_BYTES};
    let dir = temp_dir("read");
    let path = dir.join("snapshot.json");
    for raw in [
        b"{".to_vec(),
        vec![b' '; MAX_STATUSLINE_BYTES + 1],
        serde_json::to_vec(&json!({"observedAt":i64::MAX,"payload":sample()})).unwrap(),
    ] {
        std::fs::write(&path, raw).unwrap();
        assert!(read_snapshot(&path).is_err());
    }
    std::fs::remove_dir_all(dir).unwrap();
}

fn temp_dir(label: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "nexus-statusline-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}
