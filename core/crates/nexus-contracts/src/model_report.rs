//! Observation-only model evidence. No model catalog, selection controls, merging, or collector
//! capability enablement lives here. Configured aliases are not response evidence.

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use typeshare::typeshare;

/// Largest public revision that every JavaScript consumer can represent exactly.
pub const MAX_MODEL_REPORT_REVISION: u64 = 9_007_199_254_740_991;

/// Opaque actual-runtime backend identifier, supplied by its collector rather than a shared
/// catalog. Nonblank, no control characters, at most 128 UTF-8 bytes; preserved exactly.
/// The exact reserved identifier `unknown` is only valid on a corrupt-storage tombstone report.
#[typeshare]
#[derive(Serialize, Debug, Clone, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub struct ModelReportBackend(String);

impl ModelReportBackend {
    pub fn new(value: impl Into<String>) -> Result<Self, ModelReportValidationError> {
        let value = value.into();
        validate_metadata_id(&value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Whether this is the exact reserved corrupt-storage tombstone backend.
    pub fn is_unknown(&self) -> bool {
        self.0 == "unknown"
    }

    pub fn validate(&self) -> Result<(), ModelReportValidationError> {
        validate_metadata_id(&self.0)
    }
}

impl<'de> Deserialize<'de> for ModelReportBackend {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// Whether this backend's decoder has verified support for this evidence field.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ModelEvidenceCapability {
    Supported,
    Unsupported,
    Unverified,
}

/// Opaque decoder provenance, not a model identifier or a claim that a collector is enabled.
/// Nonblank, no control characters, at most 128 UTF-8 bytes; preserved exactly.
/// Collectors decide which evidence field a verified native envelope can establish; in particular
/// a reroute event does not by itself establish the model that generated a response.
#[typeshare]
#[derive(Serialize, Debug, Clone, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub struct ModelObservationSource(String);

impl ModelObservationSource {
    pub fn new(value: impl Into<String>) -> Result<Self, ModelReportValidationError> {
        let value = value.into();
        validate_metadata_id(&value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn validate(&self) -> Result<(), ModelReportValidationError> {
        validate_metadata_id(&self.0)
    }
}

impl<'de> Deserialize<'de> for ModelObservationSource {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ModelUnknownReason {
    AwaitingNativeMetadata,
    NotInCurrentConfiguration,
    ObserverReplaced,
}

#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ModelInvalidReason {
    /// Persisted report JSON could not be trusted; no native decoder evidence is inferred.
    CorruptStoredMetadata,
    MalformedNativeMetadata,
    AmbiguousModelSelection,
    ConflictingNativeMetadata,
}

/// Native evidence with opaque, nonblank identifiers. Provider is supplied explicitly, never
/// inferred by splitting a model id. Timestamps are signed JavaScript-safe Unix milliseconds;
/// native time is optional. Negative values are preserved, never clamped or rounded.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ModelObservation {
    #[serde(with = "nonblank_string")]
    pub model_id: String,
    #[serde(
        default,
        with = "optional_nonblank_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub provider_id: Option<String>,
    pub source: ModelObservationSource,
    #[typeshare(serialized_as = "number")]
    #[serde(with = "model_timestamp")]
    pub observed_at: i64,
    #[serde(
        default,
        with = "optional_nonblank_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub native_session_id: Option<String>,
    #[serde(
        default,
        with = "optional_nonblank_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub native_turn_id: Option<String>,
    #[serde(
        default,
        with = "optional_nonblank_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub native_message_id: Option<String>,
    #[typeshare(serialized_as = "Option<number>")]
    #[serde(
        default,
        with = "optional_model_timestamp",
        skip_serializing_if = "Option::is_none"
    )]
    pub native_reported_at: Option<i64>,
}

/// One independent evidence slot. Only observed evidence carries an observation. The status-tagged
/// wire union is mirrored in gen/postamble.ts because typeshare cannot emit internal tags.
#[typeshare(serialized_as = "ModelEvidenceSlotWire")]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "camelCase", deny_unknown_fields)]
pub enum ModelEvidenceSlot {
    Observed {
        #[serde(with = "supported_capability")]
        capability: ModelEvidenceCapability,
        observation: ModelObservation,
    },
    Unknown {
        capability: ModelEvidenceCapability,
        #[serde(skip_serializing_if = "Option::is_none")]
        reason: Option<ModelUnknownReason>,
    },
    Invalid {
        capability: ModelEvidenceCapability,
        reason: ModelInvalidReason,
    },
}

/// Public persisted snapshot. The store assigns the positive revision before publication; private
/// uncommitted snapshots must not use this wire type with revision zero. Slots never imply one
/// another, and observer activity is independent from the last recorded evidence.
/// The unknown backend is reserved for an inactive corrupt-storage tombstone, not native evidence.
#[typeshare]
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase", try_from = "RuntimeModelReportWire")]
pub struct RuntimeModelReport {
    pub backend: ModelReportBackend,
    pub observer_active: bool,
    #[typeshare(serialized_as = "number")]
    pub report_revision: u64,
    pub configured: ModelEvidenceSlot,
    pub turn_selected: ModelEvidenceSlot,
    pub response_reported: ModelEvidenceSlot,
    /// Optional native telemetry shares this snapshot's exact ownership and durable revision.
    /// Keep its payload out of line: reports are embedded in rows held by nested async operations.
    pub telemetry: Option<Box<crate::telemetry::RuntimeTelemetryReport>>,
}

// This mirror is private: all public serde entrypoints must pass the cross-field validation.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeModelReportWire {
    backend: ModelReportBackend,
    observer_active: bool,
    #[serde(with = "report_revision")]
    report_revision: u64,
    configured: ModelEvidenceSlot,
    turn_selected: ModelEvidenceSlot,
    response_reported: ModelEvidenceSlot,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    telemetry: Option<Box<crate::telemetry::RuntimeTelemetryReport>>,
}

impl TryFrom<RuntimeModelReportWire> for RuntimeModelReport {
    type Error = ModelReportValidationError;

    fn try_from(wire: RuntimeModelReportWire) -> Result<Self, Self::Error> {
        let report = Self {
            backend: wire.backend,
            observer_active: wire.observer_active,
            report_revision: wire.report_revision,
            configured: wire.configured,
            turn_selected: wire.turn_selected,
            response_reported: wire.response_reported,
            telemetry: wire.telemetry,
        };
        report.validate()?;
        Ok(report)
    }
}

impl Serialize for RuntimeModelReport {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(serde::ser::Error::custom)?;
        RuntimeModelReportWire {
            backend: self.backend.clone(),
            observer_active: self.observer_active,
            report_revision: self.report_revision,
            configured: self.configured.clone(),
            turn_selected: self.turn_selected.clone(),
            response_reported: self.response_reported.clone(),
            telemetry: self.telemetry.clone(),
        }
        .serialize(serializer)
    }
}

/// Bounded validation error; does not echo native metadata into error messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModelReportValidationError(pub &'static str);

impl std::fmt::Display for ModelReportValidationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for ModelReportValidationError {}

fn validate_metadata_id(value: &str) -> Result<(), ModelReportValidationError> {
    if value.trim().is_empty() || value.chars().any(char::is_control) || value.len() > 128 {
        return Err(ModelReportValidationError(
            "backend/source identifiers must be nonblank, contain no control characters, and be at most 128 UTF-8 bytes",
        ));
    }
    Ok(())
}

fn nonblank(value: &str) -> Result<(), ModelReportValidationError> {
    if value.trim().is_empty() {
        Err(ModelReportValidationError(
            "model evidence identifiers must not be blank",
        ))
    } else {
        Ok(())
    }
}

impl ModelObservation {
    /// Revalidate values constructed in Rust before a future store accepts them.
    pub fn validate(&self) -> Result<(), ModelReportValidationError> {
        nonblank(&self.model_id)?;
        self.source.validate()?;
        model_timestamp::validate(self.observed_at)?;
        if let Some(value) = self.native_reported_at {
            model_timestamp::validate(value)?;
        }
        for value in [
            &self.provider_id,
            &self.native_session_id,
            &self.native_turn_id,
            &self.native_message_id,
        ]
        .into_iter()
        .flatten()
        {
            nonblank(value)?;
        }
        Ok(())
    }
}

impl ModelEvidenceSlot {
    pub fn validate(&self) -> Result<(), ModelReportValidationError> {
        if let Self::Observed {
            capability,
            observation,
        } = self
        {
            supported_capability::validate(*capability)?;
            observation.validate()?;
        }
        Ok(())
    }
}

impl RuntimeModelReport {
    /// Validate the public wire/store boundary, including directly constructed Rust values.
    pub fn validate(&self) -> Result<(), ModelReportValidationError> {
        self.backend.validate()?;
        report_revision::validate(self.report_revision)?;
        self.configured.validate()?;
        self.turn_selected.validate()?;
        self.response_reported.validate()?;
        if let Some(telemetry) = &self.telemetry {
            telemetry.validate()?;
        }
        if self.backend.is_unknown() {
            let is_corrupt_storage_tombstone = !self.observer_active
                && self.telemetry.is_none()
                && [
                    &self.configured,
                    &self.turn_selected,
                    &self.response_reported,
                ]
                .into_iter()
                .all(|slot| {
                    matches!(
                        slot,
                        ModelEvidenceSlot::Invalid {
                            capability: ModelEvidenceCapability::Unverified,
                            reason: ModelInvalidReason::CorruptStoredMetadata,
                        }
                    )
                });
            if !is_corrupt_storage_tombstone {
                return Err(ModelReportValidationError(
                    "unknown backend requires an inactive, unverified corrupt-storage tombstone in every slot",
                ));
            }
        }
        Ok(())
    }
}

// Private field helpers keep ordinary serde entrypoints validated in both directions, without
// exposing unchecked inherent serialize/deserialize methods on the public report types.
mod nonblank_string {
    use super::*;

    pub fn serialize<S: Serializer>(value: &str, serializer: S) -> Result<S::Ok, S::Error> {
        nonblank(value).map_err(serde::ser::Error::custom)?;
        value.serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
        let value = String::deserialize(deserializer)?;
        nonblank(&value).map_err(serde::de::Error::custom)?;
        Ok(value)
    }
}

mod optional_nonblank_string {
    use super::*;

    pub fn serialize<S: Serializer>(
        value: &Option<String>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        if let Some(value) = value {
            nonblank(value).map_err(serde::ser::Error::custom)?;
        }
        value.serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<String>, D::Error> {
        let value = Option::<String>::deserialize(deserializer)?;
        if let Some(value) = &value {
            nonblank(value).map_err(serde::de::Error::custom)?;
        }
        Ok(value)
    }
}

mod supported_capability {
    use super::*;

    pub fn validate(value: ModelEvidenceCapability) -> Result<(), ModelReportValidationError> {
        if value != ModelEvidenceCapability::Supported {
            return Err(ModelReportValidationError(
                "observed evidence requires supported capability",
            ));
        }
        Ok(())
    }

    pub fn serialize<S: Serializer>(
        value: &ModelEvidenceCapability,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        validate(*value).map_err(serde::ser::Error::custom)?;
        value.serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<ModelEvidenceCapability, D::Error> {
        let value = ModelEvidenceCapability::deserialize(deserializer)?;
        validate(value).map_err(serde::de::Error::custom)?;
        Ok(value)
    }
}

mod model_timestamp {
    use super::*;

    pub fn validate(value: i64) -> Result<(), ModelReportValidationError> {
        if value.unsigned_abs() > MAX_MODEL_REPORT_REVISION {
            return Err(ModelReportValidationError(
                "model observation timestamp must be JavaScript-safe",
            ));
        }
        Ok(())
    }

    pub fn serialize<S: Serializer>(value: &i64, serializer: S) -> Result<S::Ok, S::Error> {
        validate(*value).map_err(serde::ser::Error::custom)?;
        value.serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<i64, D::Error> {
        let value = i64::deserialize(deserializer)?;
        validate(value).map_err(serde::de::Error::custom)?;
        Ok(value)
    }
}

mod optional_model_timestamp {
    use super::*;

    pub fn serialize<S: Serializer>(value: &Option<i64>, serializer: S) -> Result<S::Ok, S::Error> {
        if let Some(value) = value {
            model_timestamp::validate(*value).map_err(serde::ser::Error::custom)?;
        }
        value.serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<i64>, D::Error> {
        let value = Option::<i64>::deserialize(deserializer)?;
        if let Some(value) = value {
            model_timestamp::validate(value).map_err(serde::de::Error::custom)?;
        }
        Ok(value)
    }
}

mod report_revision {
    use super::*;

    pub fn validate(value: u64) -> Result<(), ModelReportValidationError> {
        if !(1..=MAX_MODEL_REPORT_REVISION).contains(&value) {
            return Err(ModelReportValidationError(
                "reportRevision must be a positive JavaScript-safe integer",
            ));
        }
        Ok(())
    }

    pub fn serialize<S: Serializer>(value: &u64, serializer: S) -> Result<S::Ok, S::Error> {
        validate(*value).map_err(serde::ser::Error::custom)?;
        value.serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
        let value = u64::deserialize(deserializer)?;
        validate(value).map_err(serde::de::Error::custom)?;
        Ok(value)
    }
}

/// Observation-only internal handoff; no report revision or store ownership token is supplied by
/// a native decoder. These types intentionally do not appear in the TypeScript wire surface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeModelUpdate {
    pub native_session_id: String,
    pub field: ModelEvidenceField,
    pub value: ModelEvidenceValue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelEvidenceField {
    Configured,
    TurnSelected,
    ResponseReported,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelEvidenceValue {
    Observed(ModelObservation),
    Unknown(ModelUnknownReason),
    Invalid(ModelInvalidReason),
}

/// Opaque in-process immutable profile allocation. Clones preserve correspondence; independently
/// constructed equal profiles do not. Not serialized and not a runtime/launch ownership token.
#[derive(Clone, Default)]
pub struct ModelProfileIdentity(std::sync::Arc<()>);

impl ModelProfileIdentity {
    pub fn matches(&self, other: &Self) -> bool {
        std::sync::Arc::ptr_eq(&self.0, &other.0)
    }
}

impl std::fmt::Debug for ModelProfileIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ModelProfileIdentity { .. }")
    }
}

/// Synchronous captured observer. Native handoff must remain nonblocking; acceptance is not a
/// durable commit. Profile correspondence alone does not authorize a runtime/native launch.
pub trait ModelObservationSink: Send + Sync {
    fn accepts_profile(&self, _profile: &ModelProfileIdentity) -> bool {
        false
    }
    fn bind_native_root(&self, native_session_id: &str) -> bool;
    fn observe(&self, update: NativeModelUpdate) -> bool;
    /// Legacy model-only sinks explicitly decline telemetry. Acceptance is not a durable write.
    fn observe_telemetry(&self, _update: crate::telemetry::NativeTelemetryUpdate) -> bool {
        false
    }
    fn revoke(&self);
}
