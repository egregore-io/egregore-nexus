//! Raw ACP metadata validation before the SDK's tolerant configuration decoder.
use super::AcpModelMetadataDialect;
use agent_client_protocol::schema::v1::{
    SessionConfigKind, SessionConfigOption, SessionConfigOptionCategory,
};
use nexus_contracts::model_report::ModelEvidenceValue;
use nexus_contracts::{
    ModelInvalidReason, ModelObservation, ModelObservationSource, ModelUnknownReason,
};
use serde_json::Value;

use super::AdapterModelReporting;
use agent_client_protocol::Dispatch;
use nexus_contracts::model_report::{ModelEvidenceField, NativeModelUpdate};
use nexus_contracts::ModelEvidenceCapability;
use std::sync::{Arc, Mutex};

pub(crate) fn decode_context(
    raw: &Value,
    root: &str,
    observed_at: i64,
    source: &ModelObservationSource,
) -> nexus_contracts::telemetry::NativeTelemetryValue<nexus_contracts::telemetry::ContextObservation>
{
    use nexus_contracts::telemetry::*;
    #[derive(serde::Deserialize)]
    struct Usage {
        used: TelemetryCounter,
        size: TelemetryCounter,
    }
    let decode = || -> Option<ContextObservation> {
        let usage: Usage = serde_json::from_value(raw.clone()).ok()?;
        if usage.size.get() == 0 {
            return None;
        }
        let basis = TelemetryId::new("acp.usage_update:last-reported-context-ratio").ok()?;
        let token = |value, estimated| ContextTokenValue {
            value,
            provenance: if estimated {
                TelemetryProvenance::Estimated
            } else {
                TelemetryProvenance::Native
            },
            basis: estimated.then(|| basis.clone()),
        };
        let remaining = usage.size.get().checked_sub(usage.used.get());
        let value = ContextObservation {
            metadata: TelemetryMetadata {
                native_session_id: TelemetryId::new(root).ok()?,
                source: source.clone(),
                observed_at: TelemetryTimestamp::new(observed_at).ok()?,
                native_reported_at: None,
            },
            model: None,
            effective_capacity_tokens: Some(token(usage.size, false)),
            used_tokens: Some(token(usage.used, true)),
            remaining_tokens: remaining
                .map(|value| token(TelemetryCounter::new(value).unwrap(), true)),
            used_percent: None,
            // Structured ACP occupancy uses the reported capacity, without a UI baseline offset.
            // Preserve overfull native used tokens without inventing negative/zero availability.
            remaining_percent: remaining.map(|remaining| ContextPercentageValue {
                value: TelemetryQuantity::new(
                    (remaining as f64 / usage.size.get() as f64 * 100.0).round(),
                )
                .unwrap(),
                provenance: TelemetryProvenance::Estimated,
                basis: Some(basis),
            }),
            output_reserve_tokens: None,
            compaction_count: None,
            reset_id: None,
        };
        value.validate().ok()?;
        Some(value)
    };
    decode().map_or(
        NativeTelemetryValue::Invalid,
        NativeTelemetryValue::Observed,
    )
}

#[derive(Clone)]
pub(crate) enum OpenRequest {
    New,
    Load(String),
}
impl OpenRequest {
    fn method(&self) -> &'static str {
        match self {
            Self::New => "session/new",
            Self::Load(_) => "session/load",
        }
    }
}

/// One engine connection and at most one session-open request: the engine's existing async
/// connection lock serializes opens. This mutex is shared only for synchronous send/registration
/// and raw dispatch; never acquire the engine connection mutex from a callback.
pub(crate) struct ModelMetadataCollector {
    reporting: Option<AdapterModelReporting>,
    connection: Option<String>,
    pending: Option<(Value, OpenRequest)>,
    root: Option<String>,
}
impl ModelMetadataCollector {
    pub(crate) fn new(reporting: Option<AdapterModelReporting>) -> Self {
        Self {
            reporting,
            connection: None,
            pending: None,
            root: None,
        }
    }
    pub(crate) fn begin(&mut self, owner: &str) {
        self.connection = Some(owner.into());
        self.pending = None;
        self.root = None;
    }
    pub(crate) fn register(&mut self, owner: &str, id: Value, request: OpenRequest) {
        if self.connection.as_deref() == Some(owner) {
            self.pending = Some((id, request));
        }
    }
    fn forget(&mut self, owner: &str, id: &Value) {
        if self.connection.as_deref() == Some(owner)
            && self
                .pending
                .as_ref()
                .is_some_and(|(pending, _)| pending == id)
        {
            self.pending = None;
        }
    }
    pub(crate) fn close(&mut self, owner: &str) {
        if self.connection.as_deref() == Some(owner) {
            self.close_current();
        }
    }
    pub(crate) fn close_current(&mut self) {
        self.connection = None;
        self.pending = None;
        self.root = None;
        if let Some(reporting) = &self.reporting {
            reporting.sink().revoke();
        }
    }
    pub(crate) fn dispatch(&mut self, owner: &str, dispatch: &Dispatch) {
        if self.connection.as_deref() != Some(owner) || self.reporting.is_none() {
            return;
        }
        match dispatch {
            Dispatch::Response(result, router) => {
                let Some((id, request)) = self.pending.as_ref() else {
                    return;
                };
                if *id != router.id() || request.method() != router.method() {
                    return;
                }
                let (_, request) = self.pending.take().expect("matching pending request");
                let Ok(payload) = result else {
                    return;
                };
                let root = match request {
                    OpenRequest::New => {
                        let Some(root) = payload
                            .get("sessionId")
                            .and_then(Value::as_str)
                            .filter(|root| !root.trim().is_empty())
                        else {
                            return;
                        };
                        root.to_owned()
                    }
                    OpenRequest::Load(root) => {
                        if root.trim().is_empty()
                            || payload
                                .get("sessionId")
                                .is_some_and(|id| id.as_str() != Some(&root))
                        {
                            return;
                        }
                        root
                    }
                };
                if !payload.is_object() {
                    return;
                }
                if !self
                    .reporting
                    .as_ref()
                    .unwrap()
                    .sink()
                    .bind_native_root(&root)
                {
                    return;
                }
                self.root = Some(root.clone());
                self.publish(payload, &root, SnapshotKind::SessionResult);
            }
            Dispatch::Notification(notification) if notification.method == "session/update" => {
                let Some(root) = &self.root else {
                    return;
                };
                if notification.params.get("sessionId").and_then(Value::as_str) != Some(root) {
                    return;
                }
                let Some(update) = notification.params.get("update") else {
                    return;
                };
                if update.get("sessionUpdate").and_then(Value::as_str)
                    == Some("config_option_update")
                {
                    self.publish(update, root, SnapshotKind::ConfigReplacement);
                } else if update.get("sessionUpdate").and_then(Value::as_str)
                    == Some("usage_update")
                {
                    let reporting = self.reporting.as_ref().unwrap();
                    let Some(capability) =
                        reporting.profile().telemetry().map(|value| value.context())
                    else {
                        return;
                    };
                    if capability.capability() != ModelEvidenceCapability::Supported {
                        return;
                    }
                    let Some(source) = capability.source() else {
                        return;
                    };
                    reporting.sink().observe_telemetry(
                        nexus_contracts::telemetry::NativeTelemetryUpdate::Context {
                            native_session_id: root.clone(),
                            value: decode_context(update, root, nexus_common::now(), source),
                        },
                    );
                }
            }
            _ => {}
        }
    }
    fn publish(&self, payload: &Value, root: &str, kind: SnapshotKind) {
        let Some(reporting) = &self.reporting else {
            return;
        };
        if reporting.profile().configured() != ModelEvidenceCapability::Supported {
            return;
        }
        let elapsed = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH);
        let milliseconds = match elapsed {
            Ok(elapsed) => i64::try_from(elapsed.as_millis()).ok(),
            Err(error) => i64::try_from(error.duration().as_millis())
                .ok()
                .and_then(i64::checked_neg),
        };
        let Some(now) = milliseconds else {
            return;
        };
        reporting.sink().observe(NativeModelUpdate {
            native_session_id: root.into(),
            field: ModelEvidenceField::Configured,
            value: decode_configured(reporting.profile().dialect(), payload, root, now, kind),
        });
    }
}

/// Dropping a session-open waiter cannot leave its request id eligible for a late reply.
pub(crate) struct PendingMetadataRequest {
    collector: Arc<Mutex<ModelMetadataCollector>>,
    owner: String,
    id: Value,
}
impl PendingMetadataRequest {
    pub(crate) fn new(
        collector: Arc<Mutex<ModelMetadataCollector>>,
        owner: String,
        id: Value,
    ) -> Self {
        Self {
            collector,
            owner,
            id,
        }
    }
}
impl Drop for PendingMetadataRequest {
    fn drop(&mut self) {
        self.collector.lock().unwrap().forget(&self.owner, &self.id);
    }
}

pub(crate) struct MetadataConnectionGuard(pub Arc<Mutex<ModelMetadataCollector>>, pub String);
impl Drop for MetadataConnectionGuard {
    fn drop(&mut self) {
        self.0.lock().unwrap().close(&self.1);
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum SnapshotKind {
    SessionResult,
    ConfigReplacement,
}

pub(crate) fn decode_configured(
    dialect: &AcpModelMetadataDialect,
    payload: &Value,
    root: &str,
    observed_at: i64,
    kind: SnapshotKind,
) -> ModelEvidenceValue {
    match decode_selection(dialect, payload, kind) {
        Err(reason) => ModelEvidenceValue::Invalid(reason),
        Ok(None) => ModelEvidenceValue::Unknown(if payload.get("configOptions").is_some() {
            ModelUnknownReason::NotInCurrentConfiguration
        } else {
            ModelUnknownReason::AwaitingNativeMetadata
        }),
        Ok(Some((model_id, source))) => {
            let observation = ModelObservation {
                model_id,
                provider_id: None,
                source,
                observed_at,
                native_session_id: Some(root.into()),
                native_turn_id: None,
                native_message_id: None,
                native_reported_at: None,
            };
            if observation.validate().is_err() {
                ModelEvidenceValue::Invalid(ModelInvalidReason::MalformedNativeMetadata)
            } else {
                ModelEvidenceValue::Observed(observation)
            }
        }
    }
}

fn decode_selection(
    dialect: &AcpModelMetadataDialect,
    payload: &Value,
    kind: SnapshotKind,
) -> Result<Option<(String, ModelObservationSource)>, ModelInvalidReason> {
    use ModelInvalidReason::{ConflictingNativeMetadata, MalformedNativeMetadata};
    if !payload.is_object() {
        return Err(MalformedNativeMetadata);
    }
    let configured = match payload.get("configOptions") {
        Some(options) => config_selection(options)?,
        None if kind == SnapshotKind::ConfigReplacement => return Err(MalformedNativeMetadata),
        None => None,
    };
    let (config_source, legacy_source) = match dialect {
        AcpModelMetadataDialect::ConfigOptions { source } => (source, None),
        AcpModelMetadataDialect::ConfigOptionsAndLegacyModels {
            config_options_source,
            legacy_models_source,
        } => (config_options_source, Some(legacy_models_source)),
    };
    // The verified legacy dialect is a new/load response surface, not an invented notification.
    let legacy = if kind == SnapshotKind::SessionResult && legacy_source.is_some() {
        match payload.get("models") {
            None => None,
            Some(models) => Some(
                models
                    .get("currentModelId")
                    .and_then(Value::as_str)
                    .filter(|id| !id.trim().is_empty())
                    .ok_or(MalformedNativeMetadata)?
                    .to_owned(),
            ),
        }
    } else {
        None
    };
    match (configured, legacy) {
        (Some(configured), Some(legacy)) if configured != legacy => Err(ConflictingNativeMetadata),
        (Some(configured), _) => Ok(Some((configured, config_source.clone()))),
        (None, Some(legacy)) => Ok(Some((
            legacy,
            legacy_source.expect("legacy dialect checked").clone(),
        ))),
        (None, None) => Ok(None),
    }
}

fn config_selection(options: &Value) -> Result<Option<String>, ModelInvalidReason> {
    use ModelInvalidReason::{AmbiguousModelSelection, MalformedNativeMetadata};
    let options = options.as_array().ok_or(MalformedNativeMetadata)?;
    let mut models = Vec::new();
    for raw in options {
        // SessionConfigOption.category uses DefaultOnError. Check the original field before
        // serde can silently erase a malformed category; never use the tolerant VecSkipError list.
        if let Some(category) = raw.get("category") {
            if !category.is_null() && !category.is_string() {
                return Err(MalformedNativeMetadata);
            }
        }
        let option: SessionConfigOption =
            serde_json::from_value(raw.clone()).map_err(|_| MalformedNativeMetadata)?;
        if option.category == Some(SessionConfigOptionCategory::Model) {
            let SessionConfigKind::Select(selected) = option.kind else {
                return Err(MalformedNativeMetadata);
            };
            let value = selected.current_value.to_string();
            if value.trim().is_empty() {
                return Err(MalformedNativeMetadata);
            }
            models.push(value);
        }
    }
    if models.len() > 1 {
        return Err(AmbiguousModelSelection);
    }
    Ok(models.pop())
}
