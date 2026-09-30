//! Observation-only Claude statusline decoding; not input or completion authority.
use nexus_contracts::telemetry::*;
use nexus_contracts::ModelObservationSource;
use serde_json::Value;

pub const SOURCE: &str = "claude.statusline";

/// A complete native statusline sample. Prompt identity is an admission fence, not a turn receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeStatuslineTelemetry {
    pub prompt_id: Option<String>,
    pub updates: Vec<NativeTelemetryUpdate>,
}

/// Decode a captured snapshot for one exact native root. Each category replaces its old value;
/// omitted windows are not retained with a newer timestamp. No cumulative usage is inferred from
/// Claude's latest-response context counters, nor account identity from the runtime's identity.
pub fn decode(raw: &Value, root: &str, observed_at: i64) -> Option<ClaudeStatuslineTelemetry> {
    if raw.get("session_id").and_then(Value::as_str) != Some(root) {
        return None;
    }
    let metadata = TelemetryMetadata {
        native_session_id: TelemetryId::new(root).ok()?,
        source: ModelObservationSource::new(SOURCE).ok()?,
        observed_at: TelemetryTimestamp::new(observed_at).ok()?,
        native_reported_at: None,
    };
    let prompt_id = match raw.get("prompt_id") {
        None | Some(Value::Null) => None,
        Some(value) => Some(TelemetryId::new(value.as_str()?).ok()?.as_str().to_owned()),
    };
    Some(ClaudeStatuslineTelemetry {
        prompt_id,
        updates: vec![
            NativeTelemetryUpdate::Context {
                native_session_id: root.into(),
                value: category(context(raw, metadata.clone())),
            },
            NativeTelemetryUpdate::Quota {
                native_session_id: root.into(),
                value: category(quota(raw, metadata)),
            },
        ],
    })
}

fn category<T>(value: Result<Option<T>, ()>) -> NativeTelemetryValue<T> {
    match value {
        Ok(Some(value)) => NativeTelemetryValue::Observed(value),
        Ok(None) => NativeTelemetryValue::Unknown,
        Err(()) => NativeTelemetryValue::Invalid,
    }
}

fn field<'a>(raw: &'a Value, key: &str) -> Option<&'a Value> {
    raw.get(key).filter(|value| !value.is_null())
}

fn count(raw: &Value, key: &str) -> Result<Option<u64>, ()> {
    field(raw, key)
        .map(|v| {
            TelemetryCounter::new(v.as_u64().ok_or(())?)
                .map(|v| v.get())
                .map_err(|_| ())
        })
        .transpose()
}

fn quantity(raw: &Value, key: &str) -> Result<Option<TelemetryQuantity>, ()> {
    field(raw, key)
        .map(|v| TelemetryQuantity::new(v.as_f64().ok_or(())?).map_err(|_| ()))
        .transpose()
}

fn tokens(value: u64, basis: Option<&str>) -> Result<ContextTokenValue, ()> {
    Ok(ContextTokenValue {
        value: TelemetryCounter::new(value).map_err(|_| ())?,
        provenance: if basis.is_some() {
            TelemetryProvenance::Derived
        } else {
            TelemetryProvenance::Native
        },
        basis: basis
            .map(|v| TelemetryId::new(v).map_err(|_| ()))
            .transpose()?,
    })
}

fn percentage(value: TelemetryQuantity) -> ContextPercentageValue {
    ContextPercentageValue {
        value,
        provenance: TelemetryProvenance::Native,
        basis: None,
    }
}

fn context(raw: &Value, metadata: TelemetryMetadata) -> Result<Option<ContextObservation>, ()> {
    let Some(raw) = field(raw, "context_window") else {
        return Ok(None);
    };
    if !raw.is_object() {
        return Err(());
    }
    let capacity = count(raw, "context_window_size")?;
    // current_usage null after compaction must not fall back to older total_* fields.
    let used = if let Some(usage) = field(raw, "current_usage") {
        if !usage.is_object() {
            return Err(());
        }
        match (
            count(usage, "input_tokens")?,
            count(usage, "cache_creation_input_tokens")?,
            count(usage, "cache_read_input_tokens")?,
        ) {
            (Some(input), Some(write), Some(read)) => Some(
                input
                    .checked_add(write)
                    .and_then(|v| v.checked_add(read))
                    .ok_or(())?,
            ),
            _ => None,
        }
    } else {
        None
    };
    let used_percent = quantity(raw, "used_percentage")?.map(percentage);
    let remaining_percent = quantity(raw, "remaining_percentage")?.map(percentage);
    if capacity.is_none() && used.is_none() && used_percent.is_none() && remaining_percent.is_none()
    {
        return Ok(None);
    }
    let value = ContextObservation {
        metadata,
        model: None,
        effective_capacity_tokens: capacity.map(|v| tokens(v, None)).transpose()?,
        used_tokens: used
            .map(|v| tokens(v, Some("claude.statusline.input-plus-cache")))
            .transpose()?,
        remaining_tokens: capacity
            .zip(used)
            .and_then(|(capacity, used)| capacity.checked_sub(used))
            .map(|v| tokens(v, Some("claude.statusline.capacity-minus-input-and-cache")))
            .transpose()?,
        used_percent,
        remaining_percent,
        output_reserve_tokens: None,
        compaction_count: None,
        reset_id: None,
    };
    value.validate().map_err(|_| ())?;
    Ok(Some(value))
}

fn quota(raw: &Value, metadata: TelemetryMetadata) -> Result<Option<AccountQuotaObservation>, ()> {
    let Some(raw) = field(raw, "rate_limits") else {
        return Ok(None);
    };
    if !raw.is_object() {
        return Err(());
    }
    let mut windows = Vec::new();
    for (id, seconds) in [
        ("five_hour", Some(18000)),
        ("seven_day", Some(604800)),
        ("spend_limit", None),
    ] {
        let Some(window) = field(raw, id) else {
            continue;
        };
        if !window.is_object() {
            return Err(());
        }
        let used_percent = quantity(window, "used_percentage")?;
        let resets_at = field(window, "resets_at")
            .map(|v| {
                TelemetryTimestamp::new(v.as_i64().ok_or(())?.checked_mul(1000).ok_or(())?)
                    .map_err(|_| ())
            })
            .transpose()?;
        if used_percent.is_none() {
            continue;
        }
        windows.push(QuotaWindow {
            window_id: TelemetryId::new(id).map_err(|_| ())?,
            units: TelemetryId::new("percent").map_err(|_| ())?,
            used: None,
            remaining: None,
            limit: None,
            used_percent,
            remaining_percent: None,
            resets_at,
            window_seconds: seconds
                .map(TelemetryCounter::new)
                .transpose()
                .map_err(|_| ())?,
        });
    }
    if windows.is_empty() {
        return Ok(None);
    }
    let value = AccountQuotaObservation {
        // Identify the native reporting surface, not an inferred billing account/backend.
        metadata,
        provider_id: TelemetryId::new("claude").map_err(|_| ())?,
        account_id: None,
        windows,
    };
    value.validate().map_err(|_| ())?;
    Ok(Some(value))
}
