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
    assert!(profile.telemetry().is_none());
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
