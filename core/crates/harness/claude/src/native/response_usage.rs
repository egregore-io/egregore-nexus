//! Native processed response snapshots, not cumulative context or provider billing totals.
use nexus_contracts::telemetry::*;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeResponseUsage {
    counters: Option<[u64; 4]>,
    reasoning: Option<u64>,
}

pub fn parse(outer: &Value, message: &Value) -> Option<ClaudeResponseUsage> {
    // This is native processed usage, not a promise that the provider completed successfully.
    // Explicit abort/error rows and unfinished rows do not replace the last measured response.
    for flag in ["isAbortedMidStream", "isApiErrorMessage"] {
        if outer.get(flag).is_some_and(|v| v != &Value::Bool(false)) {
            return None;
        }
    }
    message
        .get("stop_reason")?
        .as_str()
        .filter(|v| !v.is_empty())?;
    let raw = message.get("usage").filter(|v| !v.is_null())?;
    let count = |field: &str| {
        raw.get(field)
            .and_then(Value::as_u64)
            .filter(|v| TelemetryCounter::new(*v).is_ok())
    };
    let counters = (|| {
        Some([
            count("input_tokens")?,
            count("output_tokens")?,
            count("cache_read_input_tokens")?,
            count("cache_creation_input_tokens")?,
        ])
    })();
    // Compaction can clone a response ID and zero these fields while retaining nested thinking.
    // Without a native discriminator, all-zero defaults are not measured zero-response evidence.
    if counters == Some([0; 4]) {
        return None;
    }
    let reasoning = match raw.get("output_tokens_details") {
        None | Some(Value::Null) => Some(None),
        Some(Value::Object(details)) => match details.get("thinking_tokens") {
            None => Some(None),
            Some(value) => value
                .as_u64()
                .filter(|v| TelemetryCounter::new(*v).is_ok())
                .map(Some),
        },
        Some(_) => None,
    };
    Some(ClaudeResponseUsage {
        counters: counters.filter(|_| reasoning.is_some()),
        reasoning: reasoning.flatten(),
    })
}

impl ClaudeResponseUsage {
    pub fn update(&self, root: &str, at: i64) -> NativeTelemetryUpdate {
        let decode = || {
            let [input, output, read, write] = self.counters?;
            let value = TokenUsageObservation {
                metadata: TelemetryMetadata {
                    native_session_id: TelemetryId::new(root).ok()?,
                    observed_at: TelemetryTimestamp::new(at).ok()?,
                    native_reported_at: None,
                    source: nexus_contracts::ModelObservationSource::new(
                        "claude.transcript.assistant.usage",
                    )
                    .ok()?,
                },
                scope: TokenUsageScope::LastResponse,
                counter_id: TelemetryId::new("claude.transcript.assistant.usage").ok()?,
                native_turn_id: None,
                reset_id: None,
                model: None,
                total_tokens: None,
                input_tokens: Some(TelemetryCounter::new(input).ok()?),
                output_tokens: Some(TelemetryCounter::new(output).ok()?),
                cache_read_tokens: Some(TelemetryCounter::new(read).ok()?),
                cache_write_tokens: Some(TelemetryCounter::new(write).ok()?),
                reasoning_tokens: self.reasoning.map(TelemetryCounter::new).transpose().ok()?,
            };
            value.validate().ok()?;
            Some(value)
        };
        NativeTelemetryUpdate::Usage {
            native_session_id: root.to_owned(),
            value: decode().map_or(
                NativeTelemetryValue::Invalid,
                NativeTelemetryValue::Observed,
            ),
        }
    }
}
