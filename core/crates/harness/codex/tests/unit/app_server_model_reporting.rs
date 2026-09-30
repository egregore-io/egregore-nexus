use crate::app_server::model_reporting::{decode_configured, profile};
use nexus_contracts::model_report::ModelEvidenceValue;
use nexus_contracts::{ModelEvidenceCapability, ModelInvalidReason};
use serde_json::{json, Value};

fn native_payload(kind: &str) -> Value {
    let raw: Value = serde_json::from_str(include_str!(
        "../../../../nexus/tests/fixtures/model_reporting/native.json"
    ))
    .unwrap();
    raw["rows"]["codex.headed"]["events"]
        .as_array()
        .unwrap()
        .iter()
        .find(|event| event["kind"] == kind)
        .unwrap()["payload"]
        .clone()
}

#[test]
fn captured_native_start_and_resume_report_only_configured_model() {
    for kind in ["thread/start", "thread/resume"] {
        let payload = native_payload(kind);
        let root = payload["thread"]["id"].as_str().unwrap();
        let Some(ModelEvidenceValue::Observed(model)) = decode_configured(&payload, root) else {
            panic!("actual pinned {kind} metadata must be observed")
        };
        assert_eq!(model.model_id, "gpt-astra");
        assert_eq!(model.provider_id.as_deref(), Some("fixture"));
        assert_eq!(model.native_session_id.as_deref(), Some(root));
        assert_eq!(model.source.as_str(), "codex.appserver.thread");
        assert!(model.native_reported_at.is_none());
        assert!(model.native_turn_id.is_none());
        assert!(model.native_message_id.is_none());
    }
    let profile = profile();
    assert_eq!(profile.backend().as_str(), "codex.appserver");
    assert_eq!(profile.configured(), ModelEvidenceCapability::Supported);
    assert_eq!(profile.turn_selected(), ModelEvidenceCapability::Unverified);
    assert_eq!(
        profile.response_reported(),
        ModelEvidenceCapability::Unverified
    );
    let telemetry = profile.telemetry().expect("native usage/context profile");
    assert_eq!(
        telemetry.usage().capability(),
        ModelEvidenceCapability::Supported
    );
    assert_eq!(
        telemetry.context().capability(),
        ModelEvidenceCapability::Supported
    );
    assert_eq!(
        telemetry.quota().capability(),
        ModelEvidenceCapability::Supported
    );
}

use crate::app_server::{method, CodexTurnTracker, Notification};
use nexus_contracts::model_report::{ModelObservationSink, ModelProfileIdentity};
use nexus_contracts::telemetry::{NativeTelemetryUpdate, NativeTelemetryValue, TokenUsageScope};
use std::sync::{Arc, Mutex};

struct TelemetrySink {
    identity: ModelProfileIdentity,
    updates: Mutex<Vec<NativeTelemetryUpdate>>,
}
impl ModelObservationSink for TelemetrySink {
    fn accepts_profile(&self, identity: &ModelProfileIdentity) -> bool {
        self.identity.matches(identity)
    }
    fn bind_native_root(&self, root: &str) -> bool {
        root == "usage-root"
    }
    fn observe(&self, _: nexus_contracts::model_report::NativeModelUpdate) -> bool {
        true
    }
    fn revoke(&self) {}
    fn observe_telemetry(&self, update: NativeTelemetryUpdate) -> bool {
        self.updates.lock().unwrap().push(update);
        true
    }
}

fn usage_owner() -> (CodexTurnTracker, Arc<TelemetrySink>) {
    let (owner, sink) = pending_usage_owner();
    assert!(owner.publish_owner("usage-root"));
    (owner, sink)
}

fn pending_usage_owner() -> (CodexTurnTracker, Arc<TelemetrySink>) {
    let profile = profile();
    let sink = Arc::new(TelemetrySink {
        identity: profile.identity().clone(),
        updates: Mutex::new(vec![]),
    });
    let owner = CodexTurnTracker::default().new_owner_with_reporting(
        Some("usage-root".into()),
        Some(profile.capture(sink.clone()).unwrap()),
    );
    (owner, sink)
}
fn usage_note() -> Notification {
    Notification {
        method: method::TOKEN_USAGE_UPDATED.into(),
        id: None,
        params: serde_json::from_str(include_str!("../fixtures/codex-token-usage.json")).unwrap(),
    }
}
fn baseline_note() -> Notification {
    Notification {
        method: method::TOKEN_USAGE_UPDATED.into(),
        id: None,
        params: serde_json::from_str(include_str!("../fixtures/codex-token-usage-baseline.json"))
            .unwrap(),
    }
}

fn account_note() -> Notification {
    Notification {
        method: "account/rateLimits/updated".into(),
        id: None,
        params: serde_json::from_str(include_str!("../fixtures/codex-account-rate-limits.json"))
            .unwrap(),
    }
}

fn observed_quota(sink: &TelemetrySink) -> Value {
    let updates = sink.updates.lock().unwrap();
    let Some(NativeTelemetryUpdate::Quota {
        native_session_id,
        value: NativeTelemetryValue::Observed(value),
    }) = updates.last()
    else {
        panic!("native account allowance must be observed")
    };
    assert_eq!(native_session_id, "usage-root");
    serde_json::to_value(value).unwrap()
}

#[test]
fn native_account_windows_preserve_units_scope_and_native_reset_without_cost_inference() {
    let (owner, sink) = usage_owner();
    owner.ingest_native(&account_note());
    let quota = observed_quota(&sink);
    assert_eq!(quota["providerId"], "openai");
    assert!(quota.get("accountId").is_none());
    assert_eq!(quota["metadata"]["nativeSessionId"], "usage-root");
    assert_eq!(
        quota["metadata"]["source"],
        "codex.appserver.accountRateLimits"
    );
    assert_eq!(
        quota["windows"],
        json!([
            {"windowId":"codex/primary","units":"percent","usedPercent":42.0,
             "windowSeconds":18000,"resetsAt":1800000000000_i64},
            {"windowId":"codex/secondary","units":"percent","usedPercent":7.0,
             "windowSeconds":604800,"resetsAt":1800600000000_i64}
        ])
    );
    assert!(owner.active_turn_id("usage-root").is_none());
}

#[test]
fn native_account_sparse_windows_replace_without_adding_and_account_change_clears() {
    let (owner, sink) = usage_owner();
    owner.ingest_native(&account_note());
    owner.ingest_native(&account_note());
    assert_eq!(
        observed_quota(&sink)["windows"].as_array().unwrap().len(),
        2
    );
    let mut note = account_note();
    note.params["rateLimits"] = json!({"limitId":"codex","primary":null,
        "secondary":{"usedPercent":0,"windowDurationMins":10080,"resetsAt":1801200000}});
    owner.ingest_native(&note);
    let quota = observed_quota(&sink);
    assert_eq!(quota["windows"][0]["usedPercent"], 42.0);
    assert_eq!(quota["windows"][1]["usedPercent"], 0.0);
    assert_eq!(quota["windows"][1]["resetsAt"], 1801200000000_i64);
    let count = sink.updates.lock().unwrap().len();
    owner.ingest_native(&Notification {
        method: "account/rateLimits/updated".into(),
        id: None,
        params: json!({"rateLimits":{"limitId":"codex","planType":"pro","primary":null}}),
    });
    assert_eq!(
        sink.updates.lock().unwrap().len(),
        count,
        "metadata-only update cannot refresh window age"
    );
    owner.ingest_native(&Notification {
        method: "account/updated".into(),
        id: None,
        params: json!({"authMode":"chatgpt","planType":"plus"}),
    });
    assert!(matches!(
        sink.updates.lock().unwrap().last(),
        Some(NativeTelemetryUpdate::Quota {
            value: NativeTelemetryValue::Unknown,
            ..
        })
    ));
    owner.ingest_native(&note);
    assert_eq!(
        observed_quota(&sink)["windows"].as_array().unwrap().len(),
        1
    );
}

#[test]
fn native_account_sparse_freshness_retains_oldest_same_bucket_sample() {
    let mut state = crate::app_server::account_telemetry::State::default();
    let initial = account_note().params["rateLimits"].clone();
    assert_eq!(
        quota_at(&mut state, initial, 1000)["metadata"]["observedAt"],
        1000
    );
    let secondary = json!({"limitId":"codex","secondary":{"usedPercent":0}});
    let quota = quota_at(&mut state, secondary, 2000);
    assert_eq!(quota["windows"][0]["usedPercent"], 42.0);
    assert_eq!(quota["windows"][1]["usedPercent"], 0.0);
    assert_eq!(
        quota["metadata"]["observedAt"], 1000,
        "a fresh secondary must not refresh the retained primary"
    );
    let primary = json!({"limitId":"codex","primary":{"usedPercent":43}});
    assert_eq!(
        quota_at(&mut state, primary, 3000)["metadata"]["observedAt"],
        2000
    );
    assert_eq!(
        quota_at(
            &mut state,
            account_note().params["rateLimits"].clone(),
            4000
        )["metadata"]["observedAt"],
        4000
    );
}

#[test]
fn native_account_sparse_freshness_retains_oldest_other_bucket_sample() {
    let mut state = crate::app_server::account_telemetry::State::default();
    let first = json!({"limitId":"first","primary":{"usedPercent":42}});
    let second = json!({"limitId":"second","secondary":{"usedPercent":0}});
    quota_at(&mut state, first.clone(), 1000);
    let quota = quota_at(&mut state, second.clone(), 2000);
    assert_eq!(quota["windows"].as_array().unwrap().len(), 2);
    assert_eq!(
        quota["metadata"]["observedAt"], 1000,
        "a fresh bucket must not refresh another retained bucket"
    );
    assert_eq!(
        quota_at(&mut state, first, 3000)["metadata"]["observedAt"],
        2000
    );
    assert_eq!(
        quota_at(&mut state, second, 4000)["metadata"]["observedAt"],
        3000
    );
}

fn quota_at(
    state: &mut crate::app_server::account_telemetry::State,
    raw: Value,
    observed_at: i64,
) -> Value {
    let Some(NativeTelemetryValue::Observed(value)) =
        state.update_at(&raw, "usage-root", observed_at).unwrap()
    else {
        panic!("expected an observed account sample")
    };
    serde_json::to_value(value).unwrap()
}

#[test]
fn native_account_callbacks_require_published_live_connection_and_no_forged_root() {
    let (owner, sink) = pending_usage_owner();
    owner.ingest_native(&account_note());
    assert!(sink.updates.lock().unwrap().is_empty());
    assert!(owner.publish_owner("usage-root"));
    let mut request = account_note();
    request.id = Some(json!(1));
    owner.ingest_native(&request);
    let mut foreign = account_note();
    foreign.params["threadId"] = json!("child-root");
    owner.ingest_native(&foreign);
    assert!(sink.updates.lock().unwrap().is_empty());
    owner.ingest_native(&account_note());
    observed_quota(&sink);
    owner.observe_disconnect();
    let count = sink.updates.lock().unwrap().len();
    owner.ingest_native(&account_note());
    assert_eq!(sink.updates.lock().unwrap().len(), count);
    let (owner, sink) = usage_owner();
    owner.revoke_owner();
    owner.ingest_native(&account_note());
    assert!(sink.updates.lock().unwrap().is_empty());
}

#[test]
fn native_account_invalid_windows_clear_cache_and_native_overage_is_not_clamped() {
    for bad in [
        json!({"usedPercent":-1}),
        json!({"usedPercent":2.5}),
        json!({"usedPercent":2,"windowDurationMins":0}),
        json!({"usedPercent":2,"resetsAt":9223372036854775807_i64}),
    ] {
        let (owner, sink) = usage_owner();
        owner.ingest_native(&account_note());
        let mut note = account_note();
        note.params["rateLimits"]["primary"] = bad;
        owner.ingest_native(&note);
        assert!(matches!(
            sink.updates.lock().unwrap().last(),
            Some(NativeTelemetryUpdate::Quota {
                value: NativeTelemetryValue::Invalid,
                ..
            })
        ));
        note.params["rateLimits"] = json!({"limitId":"codex","secondary":{"usedPercent":140}});
        owner.ingest_native(&note);
        let quota = observed_quota(&sink);
        assert_eq!(
            quota["windows"],
            json!([{"windowId":"codex/secondary","units":"percent","usedPercent":140.0}])
        );
    }
}

#[test]
fn native_account_bucket_state_is_bounded_and_empty_is_not_zero() {
    let (owner, sink) = usage_owner();
    let mut note = account_note();
    note.params = json!({"rateLimits":{}});
    owner.ingest_native(&note);
    assert!(matches!(
        sink.updates.lock().unwrap().last(),
        Some(NativeTelemetryUpdate::Quota {
            value: NativeTelemetryValue::Unknown,
            ..
        })
    ));
    for index in 0..8 {
        note.params = json!({"rateLimits":{"limitId":format!("bucket-{index}"),"primary":{"usedPercent":index}}});
        owner.ingest_native(&note);
    }
    assert_eq!(
        observed_quota(&sink)["windows"].as_array().unwrap().len(),
        8
    );
    note.params["rateLimits"]["limitId"] = json!("bucket-overflow");
    owner.ingest_native(&note);
    assert!(matches!(
        sink.updates.lock().unwrap().last(),
        Some(NativeTelemetryUpdate::Quota {
            value: NativeTelemetryValue::Invalid,
            ..
        })
    ));
}

fn start_usage_turn(owner: &CodexTurnTracker, turn: &str) {
    owner.ingest_native(&Notification {
        method: method::TURN_STARTED.into(),
        id: None,
        params: json!({"threadId":"usage-root", "turn":{"id":turn}}),
    });
}

#[test]
fn native_usage_replaces_cumulative_values_and_estimates_only_latest_context() {
    let (owner, sink) = usage_owner();
    owner.ingest_native(&baseline_note());
    start_usage_turn(&owner, "usage-turn");
    owner.ingest_native(&Notification {
        method: method::TURN_COMPLETED.into(),
        id: None,
        params: json!({"threadId":"usage-root", "turn":{"id":"usage-turn"}}),
    });
    sink.updates.lock().unwrap().clear();
    let note = usage_note();
    owner.ingest_native(&note);
    owner.ingest_native(&note);
    let updates = sink.updates.lock().unwrap();
    assert_eq!(updates.len(), 4);
    let NativeTelemetryUpdate::Usage {
        value: NativeTelemetryValue::Observed(usage),
        ..
    } = &updates[0]
    else {
        panic!("usage")
    };
    assert_eq!(usage.scope, TokenUsageScope::SessionCumulative);
    assert_eq!(usage.total_tokens.unwrap().get(), 930000);
    assert_eq!(usage.cache_write_tokens.unwrap().get(), 300);
    assert_eq!(usage.counter_id.as_str(), "codex.thread.tokenUsage.total");
    assert!(usage.reset_id.is_none());
    assert!(usage.model.is_none());
    assert_eq!(
        usage.native_turn_id.as_ref().unwrap().as_str(),
        "usage-turn"
    );
    let NativeTelemetryUpdate::Usage {
        value: NativeTelemetryValue::Observed(repeat),
        ..
    } = &updates[2]
    else {
        panic!("repeat")
    };
    assert_eq!(repeat.total_tokens, usage.total_tokens);
    assert_eq!(repeat.counter_id, usage.counter_id);
    assert_eq!(repeat.reset_id, usage.reset_id);
    let NativeTelemetryUpdate::Context {
        value: NativeTelemetryValue::Observed(context),
        ..
    } = &updates[1]
    else {
        panic!("context")
    };
    assert_eq!(context.used_tokens.as_ref().unwrap().value.get(), 42000);
    assert_eq!(
        context.remaining_tokens.as_ref().unwrap().value.get(),
        158000
    );
    assert_eq!(
        context
            .effective_capacity_tokens
            .as_ref()
            .unwrap()
            .value
            .get(),
        200000
    );
    assert_eq!(
        context.used_tokens.as_ref().unwrap().provenance,
        nexus_contracts::telemetry::TelemetryProvenance::Estimated
    );
    assert!(context.used_tokens.as_ref().unwrap().basis.is_some());
    assert!(context.reset_id.is_none());
    assert!(context.output_reserve_tokens.is_none());
    assert!(
        owner.active_turn_id("usage-root").is_none(),
        "token metadata must not open a turn"
    );
}

#[test]
fn native_context_requires_turn_anchor_and_cannot_be_restored_by_old_turn_or_compaction() {
    let (owner, sink) = usage_owner();
    owner.ingest_native(&Notification {
        method: method::THREAD_COMPACTED.into(),
        id: None,
        params: json!({"threadId":"usage-root"}),
    });
    owner.ingest_native(&baseline_note());
    let capacity_only = |updates: &[NativeTelemetryUpdate]| {
        let Some(NativeTelemetryUpdate::Context {
            value: NativeTelemetryValue::Observed(context),
            ..
        }) = updates.last()
        else {
            panic!("capacity")
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
    };
    capacity_only(&sink.updates.lock().unwrap());
    start_usage_turn(&owner, "usage-turn");
    owner.ingest_native(&usage_note());
    for method_name in [
        method::ITEM_STARTED,
        method::ITEM_COMPLETED,
        method::THREAD_COMPACTED,
    ] {
        owner.ingest_native(&Notification { method: method_name.into(), id: None,
            params: json!({"threadId":"usage-root","turnId":"usage-turn","item":{"type":"contextCompaction","id":"compact"}}) });
        assert!(matches!(
            sink.updates.lock().unwrap().last(),
            Some(NativeTelemetryUpdate::Context {
                value: NativeTelemetryValue::Unknown,
                ..
            })
        ));
        owner.ingest_native(&usage_note());
        capacity_only(&sink.updates.lock().unwrap());
        start_usage_turn(&owner, "usage-turn"); // duplicate start cannot erase compaction barrier
        owner.ingest_native(&usage_note());
        capacity_only(&sink.updates.lock().unwrap());
    }
    start_usage_turn(&owner, "new-turn");
    let before = sink.updates.lock().unwrap().len();
    owner.ingest_native(&usage_note());
    assert_eq!(
        sink.updates.lock().unwrap().len(),
        before,
        "old-turn notification cannot regress usage or restore context"
    );
    let mut next = usage_note();
    next.params["turnId"] = json!("new-turn");
    owner.ingest_native(&next);
    // The native UI displays the last reported sample; it does not require a cumulative append.
    next.params["tokenUsage"]["total"]["inputTokens"] = json!(940000);
    next.params["tokenUsage"]["total"]["outputTokens"] = json!(32000);
    next.params["tokenUsage"]["total"]["totalTokens"] = json!(972000);
    next.params["tokenUsage"]["total"]["cachedInputTokens"] = json!(730000);
    next.params["tokenUsage"]["total"]["reasoningOutputTokens"] = json!(9500);
    owner.ingest_native(&next);
    let updates = sink.updates.lock().unwrap();
    let Some(NativeTelemetryUpdate::Context {
        value: NativeTelemetryValue::Observed(context),
        ..
    }) = updates.last()
    else {
        panic!("new context")
    };
    assert_eq!(context.used_tokens.as_ref().unwrap().value.get(), 42000);
}

#[test]
fn native_context_matches_codex_last_reported_percentage_without_invented_append_requirement() {
    let (owner, sink) = usage_owner();
    owner.ingest_native(&usage_note());
    start_usage_turn(&owner, "new-turn");
    let mut repeated = usage_note();
    repeated.params["turnId"] = json!("new-turn");
    owner.ingest_native(&repeated);
    let updates = sink.updates.lock().unwrap();
    let Some(NativeTelemetryUpdate::Context {
        value: NativeTelemetryValue::Observed(context),
        ..
    }) = updates.last()
    else {
        panic!("capacity")
    };
    assert_eq!(
        context.remaining_percent.as_ref().unwrap().value.get(),
        84.0
    );
    assert_eq!(context.used_percent.as_ref().unwrap().value.get(), 16.0);
    assert_eq!(
        context.remaining_percent.as_ref().unwrap().provenance,
        nexus_contracts::telemetry::TelemetryProvenance::Estimated
    );
    assert!(context
        .remaining_percent
        .as_ref()
        .unwrap()
        .basis
        .as_ref()
        .unwrap()
        .as_str()
        .contains("baseline12000"));
}

#[test]
fn native_context_percentage_matches_fixed_baseline_edges_and_keeps_zero_observations() {
    for (used, capacity, remaining_percent, remaining_tokens) in [
        (0, 200000, 100.0, Some(200000)),
        (8000, 200000, 100.0, Some(192000)),
        (42000, 200000, 84.0, Some(158000)),
        (200000, 200000, 0.0, Some(0)),
        (210000, 200000, 0.0, None),
        (8000, 12000, 0.0, Some(4000)),
    ] {
        let (owner, sink) = usage_owner();
        let mut note = usage_note();
        note.params["tokenUsage"]["modelContextWindow"] = json!(capacity);
        note.params["tokenUsage"]["last"] = json!({"inputTokens":used,"outputTokens":0,
            "cachedInputTokens":0,"reasoningOutputTokens":0,"totalTokens":used});
        owner.ingest_native(&note);
        let updates = sink.updates.lock().unwrap();
        let Some(NativeTelemetryUpdate::Context {
            value: NativeTelemetryValue::Observed(context),
            ..
        }) = updates.last()
        else {
            panic!("context")
        };
        assert_eq!(context.used_tokens.as_ref().unwrap().value.get(), used);
        assert_eq!(
            context.remaining_percent.as_ref().unwrap().value.get(),
            remaining_percent
        );
        assert_eq!(
            context.remaining_tokens.as_ref().map(|v| v.value.get()),
            remaining_tokens
        );
    }
}

#[test]
fn native_context_window_error_and_synthetic_totals_never_claim_occupancy() {
    let (owner, sink) = usage_owner();
    start_usage_turn(&owner, "usage-turn");
    owner.ingest_native(&usage_note());
    owner.ingest_native(&Notification {
        method: method::TURN_FAILED.into(),
        id: None,
        params: json!({"threadId":"usage-root","turnId":"usage-turn","willRetry":false,
            "error":{"codexErrorInfo":"contextWindowExceeded","message":"full"}}),
    });
    owner.ingest_native(&usage_note()); // even internally consistent old data remains blocked
    let updates = sink.updates.lock().unwrap();
    let Some(NativeTelemetryUpdate::Context {
        value: NativeTelemetryValue::Observed(context),
        ..
    }) = updates.last()
    else {
        panic!("capacity")
    };
    assert!(context.used_tokens.is_none());
    drop(updates);
    let (owner, sink) = usage_owner();
    start_usage_turn(&owner, "usage-turn");
    let mut synthetic = usage_note();
    for section in ["last", "total"] {
        synthetic.params["tokenUsage"][section] = json!({"inputTokens":0,"outputTokens":0,"cachedInputTokens":0,
            "reasoningOutputTokens":0,"totalTokens":200000});
    }
    owner.ingest_native(&synthetic);
    let updates = sink.updates.lock().unwrap();
    assert!(matches!(
        &updates[updates.len() - 2],
        NativeTelemetryUpdate::Usage {
            value: NativeTelemetryValue::Invalid,
            ..
        }
    ));
    let Some(NativeTelemetryUpdate::Context {
        value: NativeTelemetryValue::Observed(context),
        ..
    }) = updates.last()
    else {
        panic!("capacity")
    };
    assert!(context.used_tokens.is_none());
    assert!(context.remaining_percent.is_none());
}

#[test]
fn native_usage_requires_exact_published_owner_and_notification() {
    let (owner, sink) = usage_owner();
    let mut foreign = usage_note();
    foreign.params["threadId"] = json!("child-root");
    owner.ingest_native(&foreign);
    let mut request = usage_note();
    request.id = Some(json!(1));
    owner.ingest_native(&request);
    assert!(sink.updates.lock().unwrap().is_empty());
    owner.ingest_native(&usage_note());
    assert_eq!(sink.updates.lock().unwrap().len(), 2);
    owner.revoke_owner();
    owner.ingest_native(&usage_note());
    assert_eq!(sink.updates.lock().unwrap().len(), 2);
}

#[test]
fn native_usage_invalidates_bad_values_and_compaction_without_invented_reset() {
    let (owner, sink) = usage_owner();
    owner.ingest_native(&usage_note());
    let mut bad = usage_note();
    bad.params["tokenUsage"]["total"]["inputTokens"] = json!(-1);
    owner.ingest_native(&bad);
    assert!(matches!(
        sink.updates.lock().unwrap().get(2),
        Some(NativeTelemetryUpdate::Usage {
            value: NativeTelemetryValue::Invalid,
            ..
        })
    ));
    owner.ingest_native(&Notification {
        method: method::THREAD_COMPACTED.into(),
        id: None,
        params: json!({"threadId":"usage-root"}),
    });
    assert!(matches!(
        sink.updates.lock().unwrap().last(),
        Some(NativeTelemetryUpdate::Context {
            value: NativeTelemetryValue::Unknown,
            ..
        })
    ));
    let mut lower = usage_note();
    lower.params["tokenUsage"]["total"] = lower.params["tokenUsage"]["last"].clone();
    owner.ingest_native(&lower);
    let updates = sink.updates.lock().unwrap();
    let NativeTelemetryUpdate::Usage {
        value: NativeTelemetryValue::Observed(usage),
        ..
    } = &updates[updates.len() - 2]
    else {
        panic!("lower snapshot")
    };
    assert_eq!(usage.total_tokens.unwrap().get(), 42000);
    assert!(
        usage.reset_id.is_none(),
        "decrease is not proof of a native reset identity"
    );
}

#[test]
fn native_configured_model_never_borrows_foreign_root_or_thread_model_fallback() {
    let mut payload = native_payload("thread/resume");
    assert!(decode_configured(&payload, "other-root").is_none());
    let root = payload["thread"]["id"].as_str().unwrap().to_owned();
    payload.as_object_mut().unwrap().remove("model");
    assert!(matches!(
        decode_configured(&payload, &root),
        Some(ModelEvidenceValue::Unknown(_))
    ));
    // The nested thread model still exists; only the verified response-level field is used.
    assert_eq!(payload["thread"]["model"], "gpt-astra");
    assert!(matches!(
        decode_configured(&json!({}), &root),
        Some(ModelEvidenceValue::Unknown(_))
    ));
    payload["thread"]["id"] = json!(17);
    assert!(decode_configured(&payload, &root).is_none());
}

#[test]
fn native_configured_model_preserves_opaque_values_and_invalidates_malformed_fields() {
    let mut payload = native_payload("thread/start");
    let root = payload["thread"]["id"].as_str().unwrap().to_owned();
    payload["model"] = json!(" opaque/alias:unchanged ");
    payload.as_object_mut().unwrap().remove("modelProvider");
    let Some(ModelEvidenceValue::Observed(model)) = decode_configured(&payload, &root) else {
        panic!("opaque model")
    };
    assert_eq!(model.model_id, " opaque/alias:unchanged ");
    assert_eq!(model.provider_id, None);
    for invalid in [json!(" "), json!(3), json!({}), json!([]), Value::Null] {
        payload["model"] = invalid;
        assert!(matches!(
            decode_configured(&payload, &root),
            Some(ModelEvidenceValue::Invalid(
                ModelInvalidReason::MalformedNativeMetadata
            ))
        ));
    }
    payload["model"] = json!("opaque");
    payload["modelProvider"] = json!(false);
    assert!(matches!(
        decode_configured(&payload, &root),
        Some(ModelEvidenceValue::Invalid(_))
    ));
}
