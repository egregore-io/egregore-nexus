//! Configured-model evidence from the connected app-server's setup response. This is not a
//! response-model decoder and never consults requested argv, a catalog, or another thread.
use nexus_agent::adapter::{
    AdapterTelemetryCapability, AdapterTelemetryReportingProfile, NativeModelReportingProfile,
};
use nexus_contracts::model_report::ModelEvidenceValue;
use nexus_contracts::{
    ModelEvidenceCapability, ModelInvalidReason, ModelObservation, ModelObservationSource,
    ModelReportBackend, ModelUnknownReason,
};
use serde_json::Value;

/// Pure capability construction: no native process or configuration side effects.
pub fn profile() -> NativeModelReportingProfile {
    NativeModelReportingProfile::new(
        ModelReportBackend::new("codex.appserver").unwrap(),
        ModelEvidenceCapability::Supported,
        ModelEvidenceCapability::Unverified,
        ModelEvidenceCapability::Unverified,
    )
    .unwrap()
    .with_telemetry(AdapterTelemetryReportingProfile::new(
        AdapterTelemetryCapability::new(
            ModelEvidenceCapability::Supported,
            Some(ModelObservationSource::new(super::telemetry::SOURCE).unwrap()),
        )
        .unwrap(),
        AdapterTelemetryCapability::new(
            ModelEvidenceCapability::Supported,
            Some(ModelObservationSource::new(super::telemetry::SOURCE).unwrap()),
        )
        .unwrap(),
        AdapterTelemetryCapability::new(
            ModelEvidenceCapability::Supported,
            Some(ModelObservationSource::new(super::account_telemetry::SOURCE).unwrap()),
        )
        .unwrap(),
    ))
}

pub(super) fn decode_configured(result: &Value, root: &str) -> Option<ModelEvidenceValue> {
    if root.trim().is_empty() || !result.is_object() {
        return None;
    }
    let unknown = || ModelEvidenceValue::Unknown(ModelUnknownReason::AwaitingNativeMetadata);
    let invalid = || ModelEvidenceValue::Invalid(ModelInvalidReason::MalformedNativeMetadata);
    match result.get("thread") {
        Some(thread) if thread.get("id").and_then(Value::as_str) != Some(root) => return None,
        // Legacy empty replies remain usable by transport, but cannot attribute model evidence.
        None => return Some(unknown()),
        _ => {}
    }
    let Some(raw_model) = result.get("model") else {
        return Some(unknown());
    };
    let Some(model) = raw_model.as_str() else {
        return Some(invalid());
    };
    let provider = match result.get("modelProvider") {
        None | Some(Value::Null) => None,
        Some(Value::String(provider)) => Some(provider.clone()),
        Some(_) => return Some(invalid()),
    };
    let observation = ModelObservation {
        model_id: model.into(),
        provider_id: provider,
        source: ModelObservationSource::new("codex.appserver.thread").unwrap(),
        observed_at: nexus_common::now(),
        native_session_id: Some(root.into()),
        native_turn_id: None,
        native_message_id: None,
        native_reported_at: None,
    };
    Some(if observation.validate().is_ok() {
        ModelEvidenceValue::Observed(observation)
    } else {
        invalid()
    })
}
