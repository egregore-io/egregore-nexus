use serde_json::Value;

/// Persisted native session-row counters, sampled at the captured gateway hook. These differ
/// from the ACP resident-agent counters. Repeated/decreasing rows replace; no reset is inferred.
pub fn session_usage(
    row: &Value,
    root: &str,
    observed_at: i64,
) -> Option<nexus_contracts::telemetry::NativeTelemetryUpdate> {
    use nexus_contracts::telemetry::{
        NativeTelemetryUpdate, NativeTelemetryValue, TelemetryCounter, TelemetryId,
        TelemetryMetadata, TelemetryTimestamp, TokenUsageObservation, TokenUsageScope,
    };
    if row.get("ended_at").is_some_and(|value| !value.is_null()) {
        return None;
    }
    let value = match row.get("usage") {
        None | Some(Value::Null) => NativeTelemetryValue::Unknown,
        Some(raw) => {
            let decoded = (|| {
                let count = |key| {
                    raw.get(key)
                        .and_then(Value::as_u64)
                        .and_then(|value| TelemetryCounter::new(value).ok())
                };
                let calls = count("api_call_count")?;
                if calls.get() == 0 {
                    // Native fresh rows default to zeros, before any provider call.
                    return Some(NativeTelemetryValue::Unknown);
                }
                let usage = TokenUsageObservation {
                    metadata: TelemetryMetadata {
                        native_session_id: TelemetryId::new(root).ok()?,
                        source: nexus_contracts::ModelObservationSource::new(
                            crate::HERMES_SESSION_USAGE_SOURCE,
                        )
                        .ok()?,
                        observed_at: TelemetryTimestamp::new(observed_at).ok()?,
                        native_reported_at: None,
                    },
                    scope: TokenUsageScope::SessionCumulative,
                    counter_id: TelemetryId::new(crate::HERMES_SESSION_USAGE_SOURCE).ok()?,
                    reset_id: None,
                    native_turn_id: None,
                    model: None,
                    input_tokens: Some(count("input_tokens")?),
                    output_tokens: Some(count("output_tokens")?),
                    cache_read_tokens: Some(count("cache_read_tokens")?),
                    cache_write_tokens: Some(count("cache_write_tokens")?),
                    reasoning_tokens: Some(count("reasoning_tokens")?),
                    // No native total column: do not sum overlapping breakdowns or invent one.
                    total_tokens: None,
                };
                usage.validate().ok()?;
                Some(NativeTelemetryValue::Observed(usage))
            })();
            decoded.unwrap_or(NativeTelemetryValue::Invalid)
        }
    };
    Some(NativeTelemetryUpdate::Usage {
        native_session_id: root.into(),
        value,
    })
}
