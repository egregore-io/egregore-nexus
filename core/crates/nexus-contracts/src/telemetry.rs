//! Bounded observation-only usage, context and account allowance snapshots. These are not
//! billing estimates, model catalogs, or proof that a native collector is enabled.

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use typeshare::typeshare;

use crate::model_report::{
    ModelEvidenceCapability, ModelObservationSource, ModelReportValidationError,
    MAX_MODEL_REPORT_REVISION,
};

type Error = ModelReportValidationError;

/// Internal observation handoff, not a wire event or durable ownership receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativeTelemetryValue<T> {
    Observed(T),
    Unknown,
    Invalid,
}

/// Each update replaces one captured owner's category; it never carries a capability or revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativeTelemetryUpdate {
    Usage {
        native_session_id: String,
        value: NativeTelemetryValue<TokenUsageObservation>,
    },
    Context {
        native_session_id: String,
        value: NativeTelemetryValue<ContextObservation>,
    },
    Quota {
        native_session_id: String,
        value: NativeTelemetryValue<AccountQuotaObservation>,
    },
}

impl NativeTelemetryUpdate {
    pub fn native_session_id(&self) -> &str {
        match self {
            Self::Usage {
                native_session_id, ..
            }
            | Self::Context {
                native_session_id, ..
            }
            | Self::Quota {
                native_session_id, ..
            } => native_session_id,
        }
    }
}

fn invalid(message: &'static str) -> Error {
    ModelReportValidationError(message)
}

/// Opaque non-secret native reporting identity, at most 1024 UTF-8 bytes; preserved exactly.
#[typeshare]
#[derive(Serialize, Debug, Clone, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub struct TelemetryId(String);
impl TelemetryId {
    pub fn new(value: impl Into<String>) -> Result<Self, Error> {
        let value = value.into();
        if value.trim().is_empty() || value.len() > 1024 || value.chars().any(char::is_control) {
            return Err(invalid(
                "telemetry identity must be nonblank, control-free and at most 1024 UTF-8 bytes",
            ));
        }
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl<'de> Deserialize<'de> for TelemetryId {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Self::new(String::deserialize(d)?).map_err(serde::de::Error::custom)
    }
}

/// An exact nonnegative JavaScript-safe native token/count value. Zero is an observation.
#[typeshare(serialized_as = "number")]
#[derive(Serialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(transparent)]
pub struct TelemetryCounter(u64);
impl TelemetryCounter {
    pub fn new(value: u64) -> Result<Self, Error> {
        if value > MAX_MODEL_REPORT_REVISION {
            return Err(invalid("telemetry count must be JavaScript-safe"));
        }
        Ok(Self(value))
    }
    pub fn get(self) -> u64 {
        self.0
    }
}
impl<'de> Deserialize<'de> for TelemetryCounter {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Self::new(u64::deserialize(d)?).map_err(serde::de::Error::custom)
    }
}

/// Signed Unix milliseconds, including negative timestamps, within JavaScript's exact range.
#[typeshare(serialized_as = "number")]
#[derive(Serialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(transparent)]
pub struct TelemetryTimestamp(i64);
impl TelemetryTimestamp {
    pub fn new(value: i64) -> Result<Self, Error> {
        if value.unsigned_abs() > MAX_MODEL_REPORT_REVISION {
            return Err(invalid("telemetry timestamp must be JavaScript-safe"));
        }
        Ok(Self(value))
    }
    pub fn get(self) -> i64 {
        self.0
    }
}
impl<'de> Deserialize<'de> for TelemetryTimestamp {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Self::new(i64::deserialize(d)?).map_err(serde::de::Error::custom)
    }
}

/// Native finite nonnegative quantity, bounded by MAX_SAFE_INTEGER. Fractional native units and
/// over-limit used percentages are preserved, not clamped or converted to inferred costs.
#[typeshare(serialized_as = "number")]
#[derive(Serialize, Debug, Clone, Copy, PartialEq)]
#[serde(transparent)]
pub struct TelemetryQuantity(f64);
// The private field can only be constructed through finite validation, excluding NaN.
impl Eq for TelemetryQuantity {}
impl TelemetryQuantity {
    pub fn new(value: f64) -> Result<Self, Error> {
        if !value.is_finite() || value < 0.0 || value > MAX_MODEL_REPORT_REVISION as f64 {
            return Err(invalid(
                "telemetry quantity must be finite, nonnegative and JavaScript-safe",
            ));
        }
        Ok(Self(value))
    }
    pub fn get(self) -> f64 {
        self.0
    }
}
impl<'de> Deserialize<'de> for TelemetryQuantity {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Self::new(f64::deserialize(d)?).map_err(serde::de::Error::custom)
    }
}

// Private DTO mirrors make ordinary serde and direct construction share validation. Public
// declarations stay explicit for typeshare; no public unchecked serialization helper is exposed.
macro_rules! checked_wire {
    ($name:ident, $wire:ident, {$($field:ident: $ty:ty),* $(,)?}, {$($opt:ident: $oty:ty),* $(,)?}) => {
        #[derive(Serialize, Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct $wire {
            $($field: $ty,)*
            $(#[serde(default, skip_serializing_if = "Option::is_none")] $opt: Option<$oty>,)*
        }
        impl TryFrom<$wire> for $name {
            type Error = Error;
            fn try_from(wire: $wire) -> Result<Self, Error> {
                let value = Self { $($field: wire.$field,)* $($opt: wire.$opt,)* };
                value.validate()?;
                Ok(value)
            }
        }
        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                self.validate().map_err(serde::ser::Error::custom)?;
                $wire { $($field: self.$field.clone(),)* $($opt: self.$opt.clone(),)* }.serialize(s)
            }
        }
    };
}

#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum TelemetryAvailability {
    Observed,
    Unknown,
    Invalid,
}

#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum TelemetryProvenance {
    Native,
    Derived,
    Estimated,
}

#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum TokenUsageScope {
    Turn,
    SessionCumulative,
}

#[typeshare]
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase", try_from = "TelemetryMetadataWire")]
pub struct TelemetryMetadata {
    pub native_session_id: TelemetryId,
    pub source: ModelObservationSource,
    pub observed_at: TelemetryTimestamp,
    pub native_reported_at: Option<TelemetryTimestamp>,
}
checked_wire!(TelemetryMetadata, TelemetryMetadataWire,
    {native_session_id: TelemetryId, source: ModelObservationSource, observed_at: TelemetryTimestamp},
    {native_reported_at: TelemetryTimestamp});
impl TelemetryMetadata {
    pub fn validate(&self) -> Result<(), Error> {
        self.source.validate()
    }
}

#[typeshare]
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase", try_from = "TelemetryModelIdentityWire")]
pub struct TelemetryModelIdentity {
    pub model_id: TelemetryId,
    pub provider_id: Option<TelemetryId>,
}
checked_wire!(TelemetryModelIdentity, TelemetryModelIdentityWire, {model_id: TelemetryId}, {provider_id: TelemetryId});
impl TelemetryModelIdentity {
    pub fn validate(&self) -> Result<(), Error> {
        Ok(())
    }
}

/// Snapshot, never an increment. Counter/reset/scope identity determines whether two observations
/// refer to the same native counter; overlapping breakdowns must not be summed into a total.
#[typeshare]
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase", try_from = "TokenUsageObservationWire")]
pub struct TokenUsageObservation {
    pub metadata: TelemetryMetadata,
    pub scope: TokenUsageScope,
    pub counter_id: TelemetryId,
    pub reset_id: Option<TelemetryId>,
    pub native_turn_id: Option<TelemetryId>,
    pub model: Option<TelemetryModelIdentity>,
    pub input_tokens: Option<TelemetryCounter>,
    pub output_tokens: Option<TelemetryCounter>,
    pub cache_read_tokens: Option<TelemetryCounter>,
    pub cache_write_tokens: Option<TelemetryCounter>,
    pub reasoning_tokens: Option<TelemetryCounter>,
    pub total_tokens: Option<TelemetryCounter>,
}
checked_wire!(TokenUsageObservation, TokenUsageObservationWire,
    {metadata: TelemetryMetadata, scope: TokenUsageScope, counter_id: TelemetryId},
    {reset_id: TelemetryId, native_turn_id: TelemetryId, model: TelemetryModelIdentity,
     input_tokens: TelemetryCounter, output_tokens: TelemetryCounter, cache_read_tokens: TelemetryCounter,
     cache_write_tokens: TelemetryCounter, reasoning_tokens: TelemetryCounter, total_tokens: TelemetryCounter});
impl TokenUsageObservation {
    pub fn validate(&self) -> Result<(), Error> {
        self.metadata.validate()?;
        if self.scope == TokenUsageScope::Turn && self.native_turn_id.is_none() {
            return Err(invalid("turn usage requires native turn identity"));
        }
        if [
            self.input_tokens,
            self.output_tokens,
            self.cache_read_tokens,
            self.cache_write_tokens,
            self.reasoning_tokens,
            self.total_tokens,
        ]
        .iter()
        .all(Option::is_none)
        {
            return Err(invalid(
                "observed token usage requires at least one native counter",
            ));
        }
        Ok(())
    }
}

fn validate_basis(
    provenance: TelemetryProvenance,
    basis: &Option<TelemetryId>,
) -> Result<(), Error> {
    if provenance != TelemetryProvenance::Native && basis.is_none() {
        Err(invalid("derived or estimated context requires a basis"))
    } else {
        Ok(())
    }
}

#[typeshare]
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase", try_from = "ContextTokenValueWire")]
pub struct ContextTokenValue {
    pub value: TelemetryCounter,
    pub provenance: TelemetryProvenance,
    pub basis: Option<TelemetryId>,
}
checked_wire!(ContextTokenValue, ContextTokenValueWire,
    {value: TelemetryCounter, provenance: TelemetryProvenance}, {basis: TelemetryId});
impl ContextTokenValue {
    pub fn validate(&self) -> Result<(), Error> {
        validate_basis(self.provenance, &self.basis)
    }
}

#[typeshare]
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase", try_from = "ContextPercentageValueWire")]
pub struct ContextPercentageValue {
    pub value: TelemetryQuantity,
    pub provenance: TelemetryProvenance,
    pub basis: Option<TelemetryId>,
}
checked_wire!(ContextPercentageValue, ContextPercentageValueWire,
    {value: TelemetryQuantity, provenance: TelemetryProvenance}, {basis: TelemetryId});
impl ContextPercentageValue {
    pub fn validate(&self) -> Result<(), Error> {
        validate_basis(self.provenance, &self.basis)
    }
}

/// Current effective context only, never lifetime token usage or a guessed model maximum.
/// Each value has its own provenance because native capacity and estimated occupancy can coexist.
#[typeshare]
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase", try_from = "ContextObservationWire")]
pub struct ContextObservation {
    pub metadata: TelemetryMetadata,
    pub model: Option<TelemetryModelIdentity>,
    pub effective_capacity_tokens: Option<ContextTokenValue>,
    pub used_tokens: Option<ContextTokenValue>,
    pub remaining_tokens: Option<ContextTokenValue>,
    pub used_percent: Option<ContextPercentageValue>,
    pub remaining_percent: Option<ContextPercentageValue>,
    pub output_reserve_tokens: Option<ContextTokenValue>,
    pub compaction_count: Option<TelemetryCounter>,
    pub reset_id: Option<TelemetryId>,
}
checked_wire!(ContextObservation, ContextObservationWire, {metadata: TelemetryMetadata},
    {model: TelemetryModelIdentity, effective_capacity_tokens: ContextTokenValue, used_tokens: ContextTokenValue,
     remaining_tokens: ContextTokenValue, used_percent: ContextPercentageValue, remaining_percent: ContextPercentageValue,
     output_reserve_tokens: ContextTokenValue, compaction_count: TelemetryCounter, reset_id: TelemetryId});
impl ContextObservation {
    pub fn validate(&self) -> Result<(), Error> {
        self.metadata.validate()?;
        if self.effective_capacity_tokens.is_none()
            && self.used_tokens.is_none()
            && self.remaining_tokens.is_none()
            && self.used_percent.is_none()
            && self.remaining_percent.is_none()
        {
            return Err(invalid(
                "observed context requires a current context metric",
            ));
        }
        for value in [
            &self.effective_capacity_tokens,
            &self.used_tokens,
            &self.remaining_tokens,
            &self.output_reserve_tokens,
        ]
        .into_iter()
        .flatten()
        {
            value.validate()?;
        }
        for value in [&self.used_percent, &self.remaining_percent]
            .into_iter()
            .flatten()
        {
            value.validate()?;
        }
        if self
            .effective_capacity_tokens
            .as_ref()
            .is_some_and(|v| v.value.get() == 0)
        {
            return Err(invalid("effective context capacity must be positive"));
        }
        if self
            .remaining_percent
            .as_ref()
            .is_some_and(|v| v.value.get() > 100.0)
        {
            return Err(invalid("remaining context percentage cannot exceed 100"));
        }
        Ok(())
    }
}

#[typeshare]
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase", try_from = "QuotaWindowWire")]
pub struct QuotaWindow {
    pub window_id: TelemetryId,
    pub units: TelemetryId,
    pub used: Option<TelemetryQuantity>,
    pub remaining: Option<TelemetryQuantity>,
    pub limit: Option<TelemetryQuantity>,
    pub used_percent: Option<TelemetryQuantity>,
    pub remaining_percent: Option<TelemetryQuantity>,
    pub resets_at: Option<TelemetryTimestamp>,
    pub window_seconds: Option<TelemetryCounter>,
}
checked_wire!(QuotaWindow, QuotaWindowWire, {window_id: TelemetryId, units: TelemetryId},
    {used: TelemetryQuantity, remaining: TelemetryQuantity, limit: TelemetryQuantity,
     used_percent: TelemetryQuantity, remaining_percent: TelemetryQuantity, resets_at: TelemetryTimestamp, window_seconds: TelemetryCounter});
impl QuotaWindow {
    pub fn validate(&self) -> Result<(), Error> {
        if [
            self.used,
            self.remaining,
            self.limit,
            self.used_percent,
            self.remaining_percent,
        ]
        .iter()
        .all(Option::is_none)
        {
            return Err(invalid(
                "observed quota window requires a native allowance metric",
            ));
        }
        if self.remaining_percent.is_some_and(|v| v.get() > 100.0) {
            return Err(invalid("remaining quota percentage cannot exceed 100"));
        }
        if self.window_seconds.is_some_and(|v| v.get() == 0) {
            return Err(invalid("quota window duration must be positive"));
        }
        Ok(())
    }
}

/// Account/provider scoped native allowance, merely observed through a runtime. Missing account
/// identity is not authority to combine snapshots across runtimes. No model or price is inferred.
#[typeshare]
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase", try_from = "AccountQuotaObservationWire")]
pub struct AccountQuotaObservation {
    pub metadata: TelemetryMetadata,
    pub provider_id: TelemetryId,
    pub account_id: Option<TelemetryId>,
    pub windows: Vec<QuotaWindow>,
}
checked_wire!(AccountQuotaObservation, AccountQuotaObservationWire,
    {metadata: TelemetryMetadata, provider_id: TelemetryId, windows: Vec<QuotaWindow>}, {account_id: TelemetryId});
impl AccountQuotaObservation {
    pub fn validate(&self) -> Result<(), Error> {
        self.metadata.validate()?;
        if self.windows.is_empty() || self.windows.len() > 16 {
            return Err(invalid("quota requires between one and sixteen windows"));
        }
        let mut ids = std::collections::HashSet::new();
        for window in &self.windows {
            window.validate()?;
            if !ids.insert(window.window_id.as_str()) {
                return Err(invalid("quota window identities must be unique"));
            }
        }
        Ok(())
    }
}

fn validate_slot(
    status: TelemetryAvailability,
    capability: ModelEvidenceCapability,
    has_value: bool,
) -> Result<(), Error> {
    match status {
        TelemetryAvailability::Observed
            if capability == ModelEvidenceCapability::Supported && has_value =>
        {
            Ok(())
        }
        TelemetryAvailability::Unknown | TelemetryAvailability::Invalid if !has_value => Ok(()),
        _ => Err(invalid(
            "telemetry availability, capability and payload disagree",
        )),
    }
}

#[typeshare]
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase", try_from = "TokenUsageSlotWire")]
pub struct TokenUsageSlot {
    pub status: TelemetryAvailability,
    pub capability: ModelEvidenceCapability,
    pub observation: Option<TokenUsageObservation>,
}
checked_wire!(TokenUsageSlot, TokenUsageSlotWire,
    {status: TelemetryAvailability, capability: ModelEvidenceCapability}, {observation: TokenUsageObservation});
impl TokenUsageSlot {
    pub fn validate(&self) -> Result<(), Error> {
        validate_slot(self.status, self.capability, self.observation.is_some())?;
        if let Some(value) = &self.observation {
            value.validate()?;
        }
        Ok(())
    }
}

#[typeshare]
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase", try_from = "ContextSlotWire")]
pub struct ContextSlot {
    pub status: TelemetryAvailability,
    pub capability: ModelEvidenceCapability,
    pub observation: Option<ContextObservation>,
}
checked_wire!(ContextSlot, ContextSlotWire,
    {status: TelemetryAvailability, capability: ModelEvidenceCapability}, {observation: ContextObservation});
impl ContextSlot {
    pub fn validate(&self) -> Result<(), Error> {
        validate_slot(self.status, self.capability, self.observation.is_some())?;
        if let Some(value) = &self.observation {
            value.validate()?;
        }
        Ok(())
    }
}

#[typeshare]
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase", try_from = "AccountQuotaSlotWire")]
pub struct AccountQuotaSlot {
    pub status: TelemetryAvailability,
    pub capability: ModelEvidenceCapability,
    pub observation: Option<AccountQuotaObservation>,
}
checked_wire!(AccountQuotaSlot, AccountQuotaSlotWire,
    {status: TelemetryAvailability, capability: ModelEvidenceCapability}, {observation: AccountQuotaObservation});
impl AccountQuotaSlot {
    pub fn validate(&self) -> Result<(), Error> {
        validate_slot(self.status, self.capability, self.observation.is_some())?;
        if let Some(value) = &self.observation {
            value.validate()?;
        }
        Ok(())
    }
}

#[typeshare]
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase", try_from = "RuntimeTelemetryReportWire")]
pub struct RuntimeTelemetryReport {
    pub usage: TokenUsageSlot,
    pub context: ContextSlot,
    pub quota: AccountQuotaSlot,
}
checked_wire!(RuntimeTelemetryReport, RuntimeTelemetryReportWire,
    {usage: TokenUsageSlot, context: ContextSlot, quota: AccountQuotaSlot}, {});
impl RuntimeTelemetryReport {
    pub fn validate(&self) -> Result<(), Error> {
        self.usage.validate()?;
        self.context.validate()?;
        self.quota.validate()
    }
    pub fn has_observations(&self) -> bool {
        [self.usage.status, self.context.status, self.quota.status]
            .contains(&TelemetryAvailability::Observed)
    }
}
