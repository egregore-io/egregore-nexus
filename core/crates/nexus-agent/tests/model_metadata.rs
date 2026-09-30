//! Raw ACP replacements, before the SDK's tolerant field decoding. No native process.
use nexus_agent::adapter::{AcpModelMetadataDialect, AdapterModelReporting};
use nexus_contracts::model_report::ModelEvidenceValue;
use nexus_contracts::{ModelInvalidReason, ModelObservationSource, ModelUnknownReason};
use serde_json::{json, Value};
#[path = "../src/adapter/model_metadata.rs"]
mod model_metadata;
use model_metadata::{decode_configured, SnapshotKind};

fn dialect(legacy: bool) -> AcpModelMetadataDialect {
    let source = ModelObservationSource::new("test-config").unwrap();
    if legacy {
        AcpModelMetadataDialect::ConfigOptionsAndLegacyModels {
            config_options_source: source,
            legacy_models_source: ModelObservationSource::new("test-legacy").unwrap(),
        }
    } else {
        AcpModelMetadataDialect::ConfigOptions { source }
    }
}
fn option(model: Value) -> Value {
    json!({"id":"chosen","name":"Model","category":"model","type":"select","currentValue":model,"options":[]})
}
fn decode(payload: Value) -> ModelEvidenceValue {
    decode_configured(
        &dialect(false),
        &payload,
        "root",
        17,
        SnapshotKind::SessionResult,
    )
}
#[test]
fn opaque_selected_value_does_not_require_a_catalog_or_infer_provider() {
    let value = decode(json!({"configOptions":[option(json!("  vendor/unlisted 模型  "))]}));
    let ModelEvidenceValue::Observed(value) = value else {
        panic!("{value:?}")
    };
    assert_eq!(value.model_id, "  vendor/unlisted 模型  ");
    assert_eq!(value.provider_id, None);
    assert_eq!(value.native_session_id.as_deref(), Some("root"));
    assert_eq!(value.observed_at, 17);
    assert_eq!(value.source.as_str(), "test-config");
}
#[test]
fn full_replacement_missing_empty_and_nonmodel_are_not_observations() {
    assert_eq!(
        decode(json!({})),
        ModelEvidenceValue::Unknown(ModelUnknownReason::AwaitingNativeMetadata)
    );
    assert_eq!(
        decode(json!({"configOptions":[]})),
        ModelEvidenceValue::Unknown(ModelUnknownReason::NotInCurrentConfiguration)
    );
    let mut other = option(json!("not-a-model"));
    other["category"] = json!("thought_level");
    assert_eq!(
        decode(json!({"configOptions":[other]})),
        ModelEvidenceValue::Unknown(ModelUnknownReason::NotInCurrentConfiguration)
    );
    assert_eq!(
        decode_configured(
            &dialect(false),
            &json!({}),
            "root",
            17,
            SnapshotKind::ConfigReplacement
        ),
        ModelEvidenceValue::Invalid(ModelInvalidReason::MalformedNativeMetadata)
    );
}
#[test]
fn raw_malformed_entries_cannot_be_dropped_to_publish_a_partial_selection() {
    for malformed in [
        json!(null),
        json!({}),
        option(json!(null)),
        option(json!(" ")),
        json!({"id":"x","name":"Broken","category":7,"type":"select","currentValue":"x","options":[]}),
        json!({"id":"x","name":"Broken","category":"mode","type":"select","currentValue":"x","options":[{}]}),
    ] {
        assert_eq!(
            decode(json!({"configOptions":[option(json!("apparently-valid")), malformed]})),
            ModelEvidenceValue::Invalid(ModelInvalidReason::MalformedNativeMetadata)
        );
    }
    for bad in [json!(null), json!({}), json!("text")] {
        assert_eq!(
            decode(json!({"configOptions":bad})),
            ModelEvidenceValue::Invalid(ModelInvalidReason::MalformedNativeMetadata)
        );
    }
}
#[test]
fn raw_category_validation_precedes_sdk_default_on_error() {
    let mut malformed = option(json!("hidden"));
    malformed["category"] = json!(7);
    let typed: agent_client_protocol::schema::v1::SessionConfigOption =
        serde_json::from_value(malformed.clone()).unwrap();
    assert!(
        typed.category.is_none(),
        "pinned SDK discards malformed category"
    );
    assert_eq!(
        decode(json!({"configOptions":[malformed]})),
        ModelEvidenceValue::Invalid(ModelInvalidReason::MalformedNativeMetadata)
    );
}
#[test]
fn multiple_model_selectors_are_ambiguous_even_if_values_match() {
    for second in ["first", "different"] {
        assert_eq!(
            decode(json!({"configOptions":[option(json!("first")),option(json!(second))]})),
            ModelEvidenceValue::Invalid(ModelInvalidReason::AmbiguousModelSelection)
        );
    }
}
#[test]
fn legacy_metadata_requires_captured_dialect_and_conflict_is_not_resolved_by_guessing() {
    let payload = json!({"models":{"currentModelId":"legacy/opaque"}});
    assert_eq!(
        decode(payload.clone()),
        ModelEvidenceValue::Unknown(ModelUnknownReason::AwaitingNativeMetadata)
    );
    let value = decode_configured(
        &dialect(true),
        &payload,
        "root",
        17,
        SnapshotKind::SessionResult,
    );
    let ModelEvidenceValue::Observed(value) = value else {
        panic!("{value:?}")
    };
    assert_eq!(value.model_id, "legacy/opaque");
    assert_eq!(value.source.as_str(), "test-legacy");
    let both = json!({"configOptions":[option(json!("new"))],"models":{"currentModelId":"old"}});
    assert_eq!(
        decode_configured(
            &dialect(true),
            &both,
            "root",
            17,
            SnapshotKind::SessionResult
        ),
        ModelEvidenceValue::Invalid(ModelInvalidReason::ConflictingNativeMetadata)
    );
    let matching =
        json!({"configOptions":[option(json!("same"))],"models":{"currentModelId":"same"}});
    assert!(
        matches!(decode_configured(&dialect(true), &matching, "root", 17, SnapshotKind::SessionResult),
        ModelEvidenceValue::Observed(value) if value.source.as_str() == "test-config")
    );
    let malformed =
        json!({"configOptions":[option(json!("same"))],"models":{"currentModelId":null}});
    assert_eq!(
        decode_configured(
            &dialect(true),
            &malformed,
            "root",
            17,
            SnapshotKind::SessionResult
        ),
        ModelEvidenceValue::Invalid(ModelInvalidReason::MalformedNativeMetadata)
    );
}
#[test]
fn verified_native_new_and_load_payloads_keep_their_opaque_model_ids() {
    let fixtures: Value = serde_json::from_str(include_str!(
        "../../nexus/tests/fixtures/model_reporting/native.json"
    ))
    .unwrap();
    for harness in ["codex", "claude", "opencode", "hermes"] {
        let row = &fixtures["rows"][format!("{harness}.acp")];
        let events = row["events"].as_array().unwrap();
        let mut seen = 0;
        for event in events {
            if !matches!(event["kind"].as_str(), Some("session/new" | "session/load")) {
                continue;
            }
            let value = decode_configured(
                &dialect(harness == "hermes"),
                &event["payload"],
                "fixture-root",
                17,
                SnapshotKind::SessionResult,
            );
            assert!(
                matches!(value, ModelEvidenceValue::Observed(_)),
                "{harness}: {value:?}"
            );
            seen += 1;
        }
        assert_eq!(seen, 2, "{harness}");
    }
}
