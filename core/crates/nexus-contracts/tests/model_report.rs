//! Wire-only model evidence: these examples are not native decoder provenance.
use nexus_contracts::AgentRuntimeSummary;
use serde_json::{json, Value};

fn runtime() -> Value {
    json!({"runtimeId":"s_wire", "agentId":"a_wire", "harness":"codex",
        "presence":"online", "active":true, "startedAt":10})
}

fn observation() -> Value {
    json!({"modelId":"gpt-astra", "source":"codex.config", "observedAt":10})
}

#[test]
fn shared_raw_model_observation_validation() {
    let fixtures: Value =
        serde_json::from_str(include_str!("../fixtures/model-report-validation.json")).unwrap();
    for case in fixtures["observations"].as_array().unwrap() {
        let parsed = serde_json::from_str::<nexus_contracts::ModelObservation>(
            case["raw"].as_str().unwrap(),
        );
        assert_eq!(
            parsed.is_ok(),
            case["valid"].as_bool().unwrap(),
            "{}: {parsed:?}",
            case["name"]
        );
        if let Ok(value) = parsed {
            value.validate().unwrap();
            let expected = case
                .get("canonical")
                .cloned()
                .unwrap_or_else(|| serde_json::from_str(case["raw"].as_str().unwrap()).unwrap());
            assert_eq!(
                serde_json::to_value(value).unwrap(),
                expected,
                "{}",
                case["name"]
            );
        }
    }
}

#[test]
fn constructed_model_observation_rejects_unsafe_timestamps() {
    for value in [-9_007_199_254_740_992_i64, 9_007_199_254_740_992_i64] {
        let mut observation: nexus_contracts::ModelObservation =
            serde_json::from_value(observation()).unwrap();
        observation.observed_at = value;
        assert!(observation.validate().is_err());
        assert!(serde_json::to_value(&observation).is_err());
        observation.observed_at = 0;
        observation.native_reported_at = Some(value);
        assert!(observation.validate().is_err());
        assert!(serde_json::to_value(&observation).is_err());
    }
}

#[test]
fn shared_raw_report_validation() {
    let fixtures: Value =
        serde_json::from_str(include_str!("../fixtures/model-report-validation.json")).unwrap();
    for case in fixtures["reports"].as_array().unwrap() {
        let parsed = serde_json::from_str::<nexus_contracts::RuntimeModelReport>(
            case["raw"].as_str().unwrap(),
        );
        assert_eq!(
            parsed.is_ok(),
            case["valid"].as_bool().unwrap(),
            "{}: {parsed:?}",
            case["name"]
        );
        if let Ok(value) = parsed {
            value.validate().unwrap();
            let expected: Value = serde_json::from_str(case["raw"].as_str().unwrap()).unwrap();
            assert_eq!(serde_json::to_value(value).unwrap(), expected);
        }
    }
}

fn report() -> Value {
    json!({"backend":"codexAppServer", "observerActive":true, "reportRevision":1,
        "configured":{"capability":"supported", "status":"observed", "observation":observation()},
        "turnSelected":{"capability":"unverified", "status":"unknown"},
        "responseReported":{"capability":"supported", "status":"unknown", "reason":"awaitingNativeMetadata"}})
}

fn with_report(report: Value) -> Value {
    let mut value = runtime();
    value["modelReport"] = report;
    value
}

fn telemetry() -> Value {
    let metadata = json!({"nativeSessionId":"native/root", "source":"fixture/native", "observedAt":-10, "nativeReportedAt":-11});
    json!({
        "usage":{"status":"observed","capability":"supported","observation":{
            "metadata":metadata,"scope":"turn","counterId":"turn/counter","nativeTurnId":"turn/1",
            "inputTokens":0,"outputTokens":20,"cacheReadTokens":10,"reasoningTokens":12,"totalTokens":20}},
        "context":{"status":"observed","capability":"supported","observation":{
            "metadata":metadata,"model":{"modelId":"opaque/context-model"},
            "effectiveCapacityTokens":{"value":1000,"provenance":"native"},
            "usedTokens":{"value":1100,"provenance":"estimated","basis":"native-history-estimator"},
            "usedPercent":{"value":110.5,"provenance":"estimated","basis":"native-history-estimator"},
            "remainingPercent":{"value":0.0,"provenance":"native"},"compactionCount":2,"resetId":"context/epoch2"}},
        "quota":{"status":"observed","capability":"supported","observation":{
            "metadata":metadata,"providerId":"opaque/provider","windows":[
                {"windowId":"five-hour","units":"requests","used":15.5,"limit":10.0,"usedPercent":155.0,"resetsAt":50,"windowSeconds":18000},
                {"windowId":"week","units":"percent","remainingPercent":17.25}
            ]}}
    })
}

fn with_telemetry(value: Value) -> Value {
    let mut report = report();
    report["telemetry"] = value;
    with_report(report)
}

#[test]
fn telemetry_preserves_native_zero_overage_independent_scopes_and_provenance() {
    let original = with_telemetry(telemetry());
    assert_eq!(roundtrip(original.clone()), original);
    let back = roundtrip(original);
    assert!(
        back["modelReport"]["telemetry"]["usage"]["observation"]
            .get("model")
            .is_none(),
        "do not inherit configured model"
    );
    assert!(
        back["modelReport"]["telemetry"]["quota"]["observation"]
            .get("accountId")
            .is_none(),
        "no guessed account identity"
    );
}

#[test]
fn telemetry_unknown_and_unsupported_are_not_observed_zero() {
    let unknown = json!({
        "usage":{"status":"unknown","capability":"unverified"},
        "context":{"status":"unknown","capability":"unsupported"},
        "quota":{"status":"invalid","capability":"supported"}
    });
    let original = with_telemetry(unknown);
    assert_eq!(roundtrip(original.clone()), original);
    for category in ["usage", "context", "quota"] {
        for (field, value) in [
            ("status", json!("unknown")),
            ("status", json!("invalid")),
            ("capability", json!("unsupported")),
            ("capability", json!("unverified")),
        ] {
            let mut bad = telemetry();
            bad[category][field] = value;
            assert!(
                serde_json::from_value::<AgentRuntimeSummary>(with_telemetry(bad)).is_err(),
                "{category}/{field}"
            );
        }
    }
}

#[test]
fn telemetry_rejects_unsafe_numbers_and_preserves_signed_timestamp_boundaries() {
    for value in [
        json!(-9_007_199_254_740_991_i64),
        json!(9_007_199_254_740_991_i64),
    ] {
        let mut valid = telemetry();
        valid["usage"]["observation"]["metadata"]["observedAt"] = value;
        let original = with_telemetry(valid);
        assert_eq!(roundtrip(original.clone()), original);
    }
    for (path, value) in [
        ("/usage/observation/inputTokens", json!(-1)),
        ("/usage/observation/inputTokens", json!(1.5)),
        (
            "/usage/observation/inputTokens",
            json!(9_007_199_254_740_992_u64),
        ),
        (
            "/usage/observation/metadata/observedAt",
            json!(9_007_199_254_740_992_i64),
        ),
        (
            "/usage/observation/metadata/nativeReportedAt",
            json!(-9_007_199_254_740_992_i64),
        ),
        ("/context/observation/remainingPercent/value", json!(100.01)),
        (
            "/context/observation/effectiveCapacityTokens/value",
            json!(0),
        ),
        ("/quota/observation/windows/0/windowSeconds", json!(0)),
        ("/quota/observation/windows/0/used", json!(-1.0)),
    ] {
        let mut bad = telemetry();
        *bad.pointer_mut(path).unwrap() = value;
        assert!(
            serde_json::from_value::<AgentRuntimeSummary>(with_telemetry(bad)).is_err(),
            "{path}"
        );
    }
}

#[test]
fn telemetry_requires_scope_basis_metrics_and_bounded_unique_windows() {
    let mut cases = Vec::new();
    let mut missing_turn = telemetry();
    missing_turn["usage"]["observation"]
        .as_object_mut()
        .unwrap()
        .remove("nativeTurnId");
    cases.push(missing_turn);
    let mut missing_basis = telemetry();
    missing_basis["context"]["observation"]["usedTokens"]
        .as_object_mut()
        .unwrap()
        .remove("basis");
    cases.push(missing_basis);
    let mut no_usage = telemetry();
    for key in [
        "inputTokens",
        "outputTokens",
        "cacheReadTokens",
        "reasoningTokens",
        "totalTokens",
    ] {
        no_usage["usage"]["observation"]
            .as_object_mut()
            .unwrap()
            .remove(key);
    }
    cases.push(no_usage);
    for windows in [
        json!([]),
        json!([{"windowId":"same","units":"percent","used":0.0},{"windowId":"same","units":"percent","used":1.0}]),
        json!((0..17)
            .map(|index| json!({"windowId":format!("window/{index}"),"units":"percent","used":0.0}))
            .collect::<Vec<_>>()),
        json!([{"windowId":"only-reset","units":"percent","resetsAt":10}]),
    ] {
        let mut bad = telemetry();
        bad["quota"]["observation"]["windows"] = windows;
        cases.push(bad);
    }
    for (index, bad) in cases.into_iter().enumerate() {
        assert!(
            serde_json::from_value::<AgentRuntimeSummary>(with_telemetry(bad)).is_err(),
            "case {index}"
        );
    }
}

#[test]
fn telemetry_rejects_blank_control_or_oversized_native_identity() {
    for id in [" ".into(), "native\u{0085}id".into(), "x".repeat(1025)] {
        let mut bad = telemetry();
        bad["usage"]["observation"]["metadata"]["nativeSessionId"] = json!(id);
        assert!(serde_json::from_value::<AgentRuntimeSummary>(with_telemetry(bad)).is_err());
    }
}

#[test]
fn telemetry_keeps_runtime_report_stack_footprint_bounded() {
    // Runtime reports are embedded in rows captured by nested async operations. New optional
    // telemetry must not reserve its full payload inline even when no collector supplied it.
    let size = std::mem::size_of::<nexus_contracts::RuntimeModelReport>();
    assert!(
        size <= 1024,
        "runtime report occupies {size} bytes on stack"
    );
}

#[test]
fn telemetry_legacy_sink_explicitly_declines_internal_handoff() {
    use nexus_contracts::telemetry::{NativeTelemetryUpdate, NativeTelemetryValue};
    use nexus_contracts::{ModelObservationSink, NativeModelUpdate};
    struct Legacy;
    impl ModelObservationSink for Legacy {
        fn bind_native_root(&self, _: &str) -> bool {
            true
        }
        fn observe(&self, _: NativeModelUpdate) -> bool {
            true
        }
        fn revoke(&self) {}
    }
    let sink: &dyn ModelObservationSink = &Legacy;
    assert!(!sink.observe_telemetry(NativeTelemetryUpdate::Usage {
        native_session_id: "native/root".into(),
        value: NativeTelemetryValue::Unknown,
    }));
}

#[test]
fn telemetry_unicode_boundaries_and_extra_field_canonicalization() {
    use nexus_contracts::telemetry::TelemetryId;
    for id in ["🦀".repeat(256), "\u{feff}".into(), " native/root ".into()] {
        let parsed: TelemetryId = serde_json::from_value(json!(id)).unwrap();
        assert_eq!(serde_json::to_value(parsed).unwrap(), json!(id));
    }
    assert!(TelemetryId::new("🦀".repeat(257)).is_err());
    for raw in [r#""\uD800""#, r#""\uDC00""#] {
        assert!(serde_json::from_str::<TelemetryId>(raw).is_err());
    }
    let paired: TelemetryId = serde_json::from_str(r#""\uD83E\uDD80""#).unwrap();
    assert_eq!(paired.as_str(), "🦀");

    let mut extended = telemetry();
    extended["quota"]["observation"]["windows"] = json!((0..16)
        .map(|index| json!({"windowId":format!("window/{index}"),"units":"requests","used":0.0}))
        .collect::<Vec<_>>());
    let original = with_telemetry(extended.clone());
    assert_eq!(roundtrip(original.clone()), original);
    extended["usage"]["observation"]["futureIgnoredField"] = json!("not an observation");
    assert_eq!(roundtrip(with_telemetry(extended)), original);
}

#[test]
fn telemetry_direct_validation_and_serialization_reject_cross_field_mutation() {
    use nexus_contracts::telemetry::*;
    let valid: RuntimeTelemetryReport = serde_json::from_value(telemetry()).unwrap();
    let mut unsupported = valid.clone();
    unsupported.usage.capability = nexus_contracts::ModelEvidenceCapability::Unsupported;
    let mut missing = valid.clone();
    missing.context.observation = None;
    let mut invalid = valid.clone();
    invalid.quota.status = TelemetryAvailability::Invalid;
    let mut basis = valid.clone();
    basis
        .context
        .observation
        .as_mut()
        .unwrap()
        .used_tokens
        .as_mut()
        .unwrap()
        .basis = None;
    let mut duplicate = valid.clone();
    let observation = duplicate.quota.observation.as_mut().unwrap();
    observation.windows.push(observation.windows[0].clone());
    for value in [unsupported, missing, invalid, basis, duplicate] {
        assert!(value.validate().is_err());
        assert!(serde_json::to_value(value).is_err());
    }
    for value in [
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
        -1.0,
        9_007_199_254_740_992.0,
    ] {
        assert!(TelemetryQuantity::new(value).is_err());
    }
    assert!(TelemetryCounter::new(u64::MAX).is_err());
    assert!(TelemetryTimestamp::new(i64::MIN).is_err());
    assert!(TelemetryTimestamp::new(i64::MAX).is_err());
}

#[test]
fn telemetry_zero_only_counter_and_cumulative_scope_need_no_guessed_turn_or_model() {
    let mut value = telemetry();
    value["usage"]["observation"] = json!({
        "metadata":{"nativeSessionId":"native/root", "source":"fixture/native", "observedAt":0},
        "scope":"sessionCumulative", "counterId":"cumulative", "inputTokens":0
    });
    let original = with_telemetry(value);
    assert_eq!(roundtrip(original.clone()), original);
}

#[test]
fn telemetry_is_forbidden_on_corrupt_unknown_backend_tombstones() {
    for slots in [
        telemetry(),
        json!({"usage":{"status":"unknown","capability":"unverified"},
        "context":{"status":"unknown","capability":"unverified"},"quota":{"status":"unknown","capability":"unverified"}}),
    ] {
        let mut wire = corrupt_storage_tombstone();
        wire["telemetry"] = slots.clone();
        assert!(serde_json::from_value::<nexus_contracts::RuntimeModelReport>(wire).is_err());
        let mut direct: nexus_contracts::RuntimeModelReport =
            serde_json::from_value(corrupt_storage_tombstone()).unwrap();
        direct.telemetry = Some(serde_json::from_value(slots).unwrap());
        assert!(direct.validate().is_err());
        assert!(serde_json::to_value(direct).is_err());
    }
}

fn roundtrip(value: Value) -> Value {
    serde_json::to_value(serde_json::from_value::<AgentRuntimeSummary>(value).unwrap()).unwrap()
}

#[test]
fn opaque_model_id_roundtrips_without_catalog() {
    let value = with_report(report());
    let back = roundtrip(value.clone());
    assert_eq!(
        back["modelReport"]["configured"]["observation"]["modelId"],
        "gpt-astra"
    );
    assert!(back["modelReport"]["configured"]["observation"]
        .get("providerId")
        .is_none());
    assert_eq!(back, value);
}

#[test]
fn old_runtime_payload_omits_model_report() {
    let back = roundtrip(runtime());
    assert!(back.get("modelReport").is_none());
    assert_eq!(back, runtime());
}

#[test]
fn configured_does_not_imply_response_reported() {
    let back = roundtrip(with_report(report()));
    assert_eq!(back["modelReport"]["configured"]["status"], "observed");
    assert_eq!(back["modelReport"]["responseReported"]["status"], "unknown");
    assert!(back["modelReport"]["responseReported"]
        .get("observation")
        .is_none());
}

#[test]
fn report_revision_rejects_nonpositive_or_unsafe_integer() {
    for revision in [json!(0), json!(-1), json!(9007199254740992_u64), json!(1.5)] {
        let mut report = report();
        report["reportRevision"] = revision.clone();
        assert!(
            serde_json::from_value::<AgentRuntimeSummary>(with_report(report)).is_err(),
            "accepted {revision}"
        );
    }
    let mut report = report();
    report["reportRevision"] = json!(9007199254740991_u64);
    assert_eq!(
        roundtrip(with_report(report.clone()))["modelReport"],
        report
    );
}

#[test]
fn malformed_slot_combinations_are_rejected() {
    let observation = observation();
    for slot in [
        json!({"capability":"unsupported","status":"observed","observation":observation}),
        json!({"capability":"unverified","status":"observed","observation":observation}),
        json!({"capability":"supported","status":"observed"}),
        json!({"capability":"supported","status":"unknown","observation":observation}),
        json!({"capability":"supported","status":"invalid","observation":observation,"reason":"malformedNativeMetadata"}),
        json!({"capability":"supported","status":"invalid"}),
        json!({"capability":"supported","status":"unknown","reason":"malformedNativeMetadata"}),
        json!({"capability":"supported","status":"invalid","reason":"awaitingNativeMetadata"}),
        json!({"capability":"supported","status":"observed","observation":observation,"reason":"awaitingNativeMetadata"}),
    ] {
        let mut report = report();
        report["configured"] = slot.clone();
        assert!(
            serde_json::from_value::<AgentRuntimeSummary>(with_report(report)).is_err(),
            "accepted {slot}"
        );
    }
}

#[test]
fn blank_model_provider_and_correlation_ids_are_rejected() {
    for field in [
        "modelId",
        "providerId",
        "nativeSessionId",
        "nativeTurnId",
        "nativeMessageId",
    ] {
        for blank in ["", " \t\n"] {
            let mut report = report();
            report["configured"]["observation"][field] = json!(blank);
            assert!(
                serde_json::from_value::<AgentRuntimeSummary>(with_report(report)).is_err(),
                "accepted blank {field}"
            );
        }
    }
}

#[test]
fn populated_runtime_roundtrips_independent_evidence() {
    let mut report = report();
    report["configured"]["observation"] = json!({
        "modelId":"provider/model:alias", "providerId":"explicit-provider",
        "source":"codex.config", "observedAt":10, "nativeSessionId":"native-s",
        "nativeTurnId":"native-t", "nativeMessageId":"native-m", "nativeReportedAt":9
    });
    report["responseReported"] = json!({"capability":"supported", "status":"observed",
        "observation":{"modelId":"separate-response-model", "source":"opencode.native.message", "observedAt":11}});
    let value = with_report(report);
    assert_eq!(roundtrip(value.clone()), value);
}

#[test]
fn every_valid_slot_variant_and_bounded_reason_roundtrips() {
    for capability in ["supported", "unsupported", "unverified"] {
        for reason in [
            None,
            Some("awaitingNativeMetadata"),
            Some("notInCurrentConfiguration"),
            Some("observerReplaced"),
        ] {
            let mut slot = json!({"capability":capability,"status":"unknown"});
            if let Some(reason) = reason {
                slot["reason"] = json!(reason);
            }
            let parsed: nexus_contracts::ModelEvidenceSlot =
                serde_json::from_value(slot.clone()).unwrap();
            assert!(parsed.validate().is_ok());
            assert_eq!(serde_json::to_value(parsed).unwrap(), slot);
        }
        for reason in [
            "malformedNativeMetadata",
            "ambiguousModelSelection",
            "conflictingNativeMetadata",
        ] {
            let slot = json!({"capability":capability,"status":"invalid","reason":reason});
            let parsed: nexus_contracts::ModelEvidenceSlot =
                serde_json::from_value(slot.clone()).unwrap();
            assert!(parsed.validate().is_ok());
            assert_eq!(serde_json::to_value(parsed).unwrap(), slot);
        }
    }
}

#[test]
fn existing_backend_and_decoder_source_ids_are_preserved() {
    for backend in [
        "acp",
        "codexAppServer",
        "claudeNative",
        "opencodePlugin",
        "hermesGateway",
    ] {
        let mut report = report();
        report["backend"] = json!(backend);
        assert_eq!(
            roundtrip(with_report(report.clone()))["modelReport"],
            report
        );
    }
    for source in [
        "acp.config_options",
        "hermes.acp.models",
        "codex.thread",
        "codex.config",
        "codex.reroute",
        "claude.transcript",
        "claude.hook",
        "opencode.native.message",
        "hermes.native.session",
    ] {
        let mut report = report();
        report["configured"]["observation"]["source"] = json!(source);
        assert_eq!(
            roundtrip(with_report(report.clone()))["modelReport"],
            report
        );
    }
    let mut report = report();
    report["backend"] = json!("");
    assert!(serde_json::from_value::<AgentRuntimeSummary>(with_report(report)).is_err());
}

#[test]
fn observation_requires_typed_source_and_integer_timestamps() {
    for (field, bad) in [
        ("source", json!(42)),
        ("observedAt", json!(1.5)),
        ("nativeReportedAt", json!("yesterday")),
    ] {
        let mut report = report();
        report["configured"]["observation"][field] = bad;
        assert!(serde_json::from_value::<AgentRuntimeSummary>(with_report(report)).is_err());
    }
}

#[test]
fn direct_rust_values_validate_before_store_or_serialization() {
    use nexus_contracts::{ModelEvidenceCapability, ModelEvidenceSlot, RuntimeModelReport};
    let valid: RuntimeModelReport = serde_json::from_value(report()).unwrap();
    assert!(valid.validate().is_ok());
    for revision in [0, 9_007_199_254_740_992] {
        let mut invalid = valid.clone();
        invalid.report_revision = revision;
        assert!(invalid.validate().is_err());
        assert!(serde_json::to_value(invalid).is_err());
    }
    for field in [
        "model",
        "provider",
        "session",
        "turn",
        "message",
        "capability",
    ] {
        let mut invalid = valid.clone();
        let ModelEvidenceSlot::Observed {
            capability,
            observation,
        } = &mut invalid.configured
        else {
            panic!("expected observation")
        };
        match field {
            "model" => observation.model_id = " ".into(),
            "provider" => observation.provider_id = Some(" ".into()),
            "session" => observation.native_session_id = Some(" ".into()),
            "turn" => observation.native_turn_id = Some(" ".into()),
            "message" => observation.native_message_id = Some(" ".into()),
            "capability" => *capability = ModelEvidenceCapability::Unverified,
            _ => unreachable!(),
        }
        assert!(invalid.validate().is_err(), "accepted invalid {field}");
        assert!(
            serde_json::to_value(invalid).is_err(),
            "serialized invalid {field}"
        );
    }
}

#[test]
fn future_unrelated_runtime_fields_remain_compatible() {
    let mut value = with_report(report());
    value["futureRuntimeField"] = json!(true);
    let back = roundtrip(value);
    assert_eq!(back["modelReport"], report());
}

#[test]
fn observation_handoff_is_revision_free_and_object_safe() {
    use nexus_contracts::{
        ModelEvidenceField, ModelEvidenceValue, ModelObservationSink, NativeModelUpdate,
    };
    let update = NativeModelUpdate {
        native_session_id: "native-s".into(),
        field: ModelEvidenceField::Configured,
        value: ModelEvidenceValue::Observed(serde_json::from_value(observation()).unwrap()),
    };
    assert_eq!(update.native_session_id, "native-s");
    // Compile-time surface checks only: this slice intentionally has no sink implementation.
    fn accepts_object_safe_send_sync(_: Option<&dyn ModelObservationSink>) {}
    accepts_object_safe_send_sync(None);
    for field in [
        ModelEvidenceField::TurnSelected,
        ModelEvidenceField::ResponseReported,
    ] {
        assert_ne!(field, update.field);
    }
    let _ = ModelEvidenceValue::Unknown(nexus_contracts::ModelUnknownReason::ObserverReplaced);
    let _ =
        ModelEvidenceValue::Invalid(nexus_contracts::ModelInvalidReason::MalformedNativeMetadata);
}

#[test]
fn direct_serde_entrypoints_cannot_bypass_validation() {
    use nexus_contracts::{ModelEvidenceSlot, ModelObservation, RuntimeModelReport};
    use serde::{Deserialize, Serialize};
    let mut invalid = report();
    invalid["reportRevision"] = json!(0);
    assert!(RuntimeModelReport::deserialize(invalid).is_err());
    let mut invalid = observation();
    invalid["modelId"] = json!(" ");
    assert!(ModelObservation::deserialize(invalid).is_err());
    let invalid =
        json!({"status":"observed", "capability":"unsupported", "observation":observation()});
    assert!(ModelEvidenceSlot::deserialize(invalid).is_err());
    let mut invalid: RuntimeModelReport = serde_json::from_value(report()).unwrap();
    invalid.report_revision = 0;
    assert!(RuntimeModelReport::serialize(&invalid, serde_json::value::Serializer).is_err());
}

fn corrupt_storage_tombstone() -> Value {
    let slot =
        json!({"status":"invalid", "capability":"unverified", "reason":"corruptStoredMetadata"});
    json!({"backend":"unknown", "observerActive":false, "reportRevision":2,
        "configured":slot, "turnSelected":slot, "responseReported":slot})
}

#[test]
fn corrupt_storage_tombstone_roundtrips_both_serde_entrypoints() {
    use nexus_contracts::RuntimeModelReport;
    use serde::{Deserialize, Serialize};
    let value = corrupt_storage_tombstone();
    let parsed: RuntimeModelReport = serde_json::from_value(value.clone()).unwrap();
    assert!(parsed.validate().is_ok());
    assert_eq!(serde_json::to_value(&parsed).unwrap(), value);
    assert_eq!(
        RuntimeModelReport::deserialize(value.clone()).unwrap(),
        parsed
    );
    assert_eq!(
        RuntimeModelReport::serialize(&parsed, serde_json::value::Serializer).unwrap(),
        value
    );
    assert_eq!(roundtrip(with_report(value.clone()))["modelReport"], value);
}

#[test]
fn unknown_backend_rejects_non_tombstone_metadata() {
    use nexus_contracts::RuntimeModelReport;
    use serde::Deserialize;
    let mut active = corrupt_storage_tombstone();
    active["observerActive"] = json!(true);
    let mut bad_values = vec![active];
    for field in ["configured", "turnSelected", "responseReported"] {
        for slot in [
            json!({"status":"observed", "capability":"supported", "observation":observation()}),
            json!({"status":"invalid", "capability":"supported", "reason":"corruptStoredMetadata"}),
            json!({"status":"invalid", "capability":"unsupported", "reason":"corruptStoredMetadata"}),
            json!({"status":"invalid", "capability":"unverified", "reason":"malformedNativeMetadata"}),
            json!({"status":"unknown", "capability":"unverified"}),
        ] {
            let mut value = corrupt_storage_tombstone();
            value[field] = slot;
            bad_values.push(value);
        }
    }
    for value in bad_values {
        assert!(
            serde_json::from_value::<RuntimeModelReport>(value.clone()).is_err(),
            "accepted {value}"
        );
        assert!(
            RuntimeModelReport::deserialize(value.clone()).is_err(),
            "direct serde accepted {value}"
        );
        assert!(serde_json::from_value::<AgentRuntimeSummary>(with_report(value)).is_err());
    }
}

#[test]
fn unknown_backend_invalid_rust_values_cannot_validate_or_serialize() {
    use nexus_contracts::{
        ModelEvidenceCapability, ModelEvidenceSlot, ModelInvalidReason, RuntimeModelReport,
    };
    use serde::Serialize;
    let tombstone: RuntimeModelReport =
        serde_json::from_value(corrupt_storage_tombstone()).unwrap();
    let mut active = tombstone.clone();
    active.observer_active = true;
    let mut invalid_reports = vec![active];
    for index in 0..3 {
        for slot in [
            ModelEvidenceSlot::Observed {
                capability: ModelEvidenceCapability::Supported,
                observation: serde_json::from_value(observation()).unwrap(),
            },
            ModelEvidenceSlot::Invalid {
                capability: ModelEvidenceCapability::Supported,
                reason: ModelInvalidReason::CorruptStoredMetadata,
            },
            ModelEvidenceSlot::Invalid {
                capability: ModelEvidenceCapability::Unsupported,
                reason: ModelInvalidReason::CorruptStoredMetadata,
            },
            ModelEvidenceSlot::Invalid {
                capability: ModelEvidenceCapability::Unverified,
                reason: ModelInvalidReason::MalformedNativeMetadata,
            },
            ModelEvidenceSlot::Unknown {
                capability: ModelEvidenceCapability::Unverified,
                reason: None,
            },
        ] {
            let mut value = tombstone.clone();
            match index {
                0 => value.configured = slot,
                1 => value.turn_selected = slot,
                _ => value.response_reported = slot,
            }
            invalid_reports.push(value);
        }
    }
    for value in invalid_reports {
        assert!(value.validate().is_err());
        assert!(serde_json::to_value(&value).is_err());
        assert!(RuntimeModelReport::serialize(&value, serde_json::value::Serializer).is_err());
    }
}

#[test]
fn unfamiliar_backend_and_source_roundtrip_without_a_catalog() {
    let mut report = report();
    report["backend"] = json!("future.transport");
    report["configured"]["observation"]["source"] = json!("future.metadata");
    assert_eq!(
        roundtrip(with_report(report.clone()))["modelReport"],
        report
    );
}

#[test]
fn backend_and_source_ids_preserve_valid_values_up_to_128_utf8_bytes() {
    use nexus_contracts::{ModelObservationSource, ModelReportBackend};
    use serde::Deserialize;
    for id in [
        " future.Mixed-Case ".to_string(),
        "x".repeat(128),
        "é".repeat(64),
        "🦀".repeat(32),
    ] {
        let backend: ModelReportBackend = serde_json::from_value(json!(id)).unwrap();
        let source: ModelObservationSource = serde_json::from_value(json!(id)).unwrap();
        assert_eq!(serde_json::to_value(&backend).unwrap(), json!(id));
        assert_eq!(serde_json::to_value(&source).unwrap(), json!(id));
        assert_eq!(ModelReportBackend::deserialize(json!(id)).unwrap(), backend);
        assert_eq!(
            ModelObservationSource::deserialize(json!(id)).unwrap(),
            source
        );
        assert_eq!(ModelReportBackend::new(&id).unwrap(), backend);
        assert_eq!(ModelObservationSource::new(&id).unwrap(), source);
        assert_eq!(backend.as_str(), id);
        assert_eq!(source.as_str(), id);
        assert!(backend.validate().is_ok());
        assert!(source.validate().is_ok());
    }
}

#[test]
fn backend_and_source_ids_reject_blank_control_and_overlong_values() {
    use nexus_contracts::{ModelObservationSource, ModelReportBackend};
    use serde::Deserialize;
    for id in [
        "".to_string(),
        "   ".into(),
        "\t".into(),
        "x\ny".into(),
        "x\0y".into(),
        "x\u{7f}y".into(),
        "x\u{85}y".into(),
        "x".repeat(129),
        "é".repeat(65),
        "🦀".repeat(33),
    ] {
        assert!(
            serde_json::from_value::<ModelReportBackend>(json!(id)).is_err(),
            "accepted backend {id:?}"
        );
        assert!(
            serde_json::from_value::<ModelObservationSource>(json!(id)).is_err(),
            "accepted source {id:?}"
        );
        assert!(ModelReportBackend::deserialize(json!(id)).is_err());
        assert!(ModelObservationSource::deserialize(json!(id)).is_err());
        assert!(ModelReportBackend::new(&id).is_err());
        assert!(ModelObservationSource::new(&id).is_err());
    }
}

#[test]
fn reserved_unknown_backend_is_an_exact_match_only() {
    use nexus_contracts::ModelReportBackend;
    assert!(ModelReportBackend::new("unknown").unwrap().is_unknown());
    for id in ["Unknown", " unknown ", "future.unknown"] {
        assert!(!ModelReportBackend::new(id).unwrap().is_unknown());
        let mut report = report();
        report["backend"] = json!(id);
        assert_eq!(
            roundtrip(with_report(report.clone()))["modelReport"],
            report
        );
    }
}
