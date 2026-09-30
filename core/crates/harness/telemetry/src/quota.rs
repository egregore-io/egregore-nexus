//! Pinned Claude ACP selected rate-limit snapshot, not a full account-window inventory.
use nexus_contracts::telemetry::{
    AccountQuotaObservation, NativeTelemetryValue, QuotaWindow, TelemetryCounter, TelemetryId,
    TelemetryMetadata, TelemetryQuantity, TelemetryTimestamp,
};
use serde_json::Value;

#[derive(Default)]
pub struct AcpQuotaSnapshot {
    last: Option<AccountQuotaObservation>,
}

impl AcpQuotaSnapshot {
    pub fn observe(
        &mut self,
        dialect: crate::AcpQuotaDialect,
        raw: &Value,
        root: &str,
        at: i64,
    ) -> NativeTelemetryValue<AccountQuotaObservation> {
        let mut decoded = match dialect {
            crate::AcpQuotaDialect::ClaudeSelectedRateLimit => decode(raw, root, at),
        }
        .unwrap_or(NativeTelemetryValue::Invalid);
        if let NativeTelemetryValue::Observed(value) = &mut decoded {
            if let Some(old) = &self.last {
                if old.metadata.native_session_id == value.metadata.native_session_id
                    && old.windows == value.windows
                {
                    // The native rejected-without-headers path can replay earlier numbers.
                    // A status-only notification does not prove a new numeric measurement.
                    value.metadata.observed_at = old.metadata.observed_at;
                }
            }
            self.last = Some(value.clone());
        }
        // Unknown/Invalid clears the public slot, not this bounded private fingerprint. An
        // identical tuple later still does not prove a new measurement. Lifecycle reset owns
        // clearing the fingerprint; it never restores an observation by itself.
        decoded
    }
}

fn decode(
    raw: &Value,
    root: &str,
    at: i64,
) -> Option<NativeTelemetryValue<AccountQuotaObservation>> {
    if raw.is_null() {
        return Some(NativeTelemetryValue::Unknown);
    }
    if !matches!(
        raw.get("status").and_then(Value::as_str),
        Some("allowed" | "allowed_warning" | "rejected")
    ) {
        return None;
    }
    let utilization = match raw.get("utilization") {
        None | Some(Value::Null) => return Some(NativeTelemetryValue::Unknown),
        Some(value) => value.as_f64()?,
    };
    let kind = raw.get("rateLimitType")?.as_str()?;
    let seconds = match kind {
        "five_hour" => 18000,
        "seven_day" | "seven_day_opus" | "seven_day_sonnet" | "seven_day_overage_included" => {
            604800
        }
        _ => return None,
    };
    let resets_at = match raw.get("resetsAt") {
        None | Some(Value::Null) => None,
        Some(value) => Some(TelemetryTimestamp::new(value.as_i64()?.checked_mul(1000)?).ok()?),
    };
    let observation = AccountQuotaObservation {
        metadata: TelemetryMetadata {
            native_session_id: TelemetryId::new(root).ok()?,
            source: nexus_contracts::ModelObservationSource::new("claude.acp.rate_limit.selected")
                .ok()?,
            observed_at: TelemetryTimestamp::new(at).ok()?,
            native_reported_at: None,
        },
        provider_id: TelemetryId::new("anthropic").ok()?,
        account_id: None,
        windows: vec![QuotaWindow {
            window_id: TelemetryId::new(format!("claude/{kind}")).ok()?,
            units: TelemetryId::new("percent").ok()?,
            used: None,
            remaining: None,
            limit: None,
            used_percent: Some(TelemetryQuantity::new(utilization * 100.0).ok()?),
            remaining_percent: None,
            resets_at,
            window_seconds: Some(TelemetryCounter::new(seconds).ok()?),
        }],
    };
    observation.validate().ok()?;
    Some(NativeTelemetryValue::Observed(observation))
}
