//! Native live-message telemetry, independent of the model-selection dedupe.
use nexus_contracts::telemetry::*;
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Default)]
pub struct OpenCodeTelemetry {
    root: Option<String>,
    // Native live reducer orders IDs and retains its last 100 messages, including user rows.
    // This is not a reconstruction of the differently ordered REST hydration path.
    messages: BTreeMap<String, Sample>,
}
#[derive(Default)]
struct Sample {
    usage: Option<NativeTelemetryValue<TokenUsageObservation>>,
    context: Option<NativeTelemetryValue<ContextObservation>>,
}
impl OpenCodeTelemetry {
    pub fn observe(
        &mut self,
        info: &Value,
        capacity: &Value,
        root: &str,
        at: i64,
    ) -> Vec<NativeTelemetryUpdate> {
        if info.get("sessionID").and_then(Value::as_str) != Some(root)
            || self
                .root
                .as_deref()
                .is_some_and(|captured| captured != root)
        {
            return Vec::new();
        }
        let Some(id) = info
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| TelemetryId::new(*id).is_ok())
        else {
            return Vec::new();
        };
        let before = self.selected();
        let sample = if info.get("role").and_then(Value::as_str) == Some("assistant") {
            decode(info, capacity, root, at)
        } else if info.get("role").and_then(Value::as_str) == Some("user") {
            Sample::default()
        } else {
            return Vec::new();
        };
        self.root.get_or_insert_with(|| root.to_owned());
        self.messages.insert(id.into(), sample);
        while self.messages.len() > 100 {
            self.messages.pop_first();
        }
        self.updates(before, id, root)
    }
    pub fn remove(&mut self, id: &str, root: &str) -> Vec<NativeTelemetryUpdate> {
        if self.root.as_deref() != Some(root) {
            return Vec::new();
        }
        let before = self.selected();
        self.messages.remove(id);
        self.updates(before, id, root)
    }
    fn selected(&self) -> (Option<String>, Option<String>) {
        (
            self.messages
                .iter()
                .rev()
                .find(|(_, s)| s.usage.is_some())
                .map(|(id, _)| id.clone()),
            self.messages
                .iter()
                .rev()
                .find(|(_, s)| s.context.is_some())
                .map(|(id, _)| id.clone()),
        )
    }
    fn updates(
        &self,
        before: (Option<String>, Option<String>),
        changed: &str,
        root: &str,
    ) -> Vec<NativeTelemetryUpdate> {
        let after = self.selected();
        let mut updates = Vec::new();
        if before.0 != after.0 || after.0.as_deref() == Some(changed) {
            updates.push(NativeTelemetryUpdate::Usage {
                native_session_id: root.into(),
                value: after
                    .0
                    .as_ref()
                    .and_then(|id| self.messages[id].usage.clone())
                    .unwrap_or(NativeTelemetryValue::Unknown),
            });
        }
        if before.1 != after.1 || after.1.as_deref() == Some(changed) {
            updates.push(NativeTelemetryUpdate::Context {
                native_session_id: root.into(),
                value: after
                    .1
                    .as_ref()
                    .and_then(|id| self.messages[id].context.clone())
                    .unwrap_or(NativeTelemetryValue::Unknown),
            });
        }
        updates
    }
}

fn metadata(root: &str, at: i64, source: &str) -> Option<TelemetryMetadata> {
    Some(TelemetryMetadata {
        native_session_id: TelemetryId::new(root).ok()?,
        observed_at: TelemetryTimestamp::new(at).ok()?,
        native_reported_at: None,
        source: nexus_contracts::ModelObservationSource::new(source).ok()?,
    })
}

fn decode(info: &Value, capacity: &Value, root: &str, at: i64) -> Sample {
    let count = |path: &str| {
        info.pointer(path)
            .and_then(Value::as_u64)
            .and_then(|v| TelemetryCounter::new(v).ok())
    };
    let fields = (|| {
        Some((
            count("/tokens/input")?,
            count("/tokens/output")?,
            count("/tokens/reasoning")?,
            count("/tokens/cache/read")?,
            count("/tokens/cache/write")?,
        ))
    })();
    let usage = info
        .get("finish")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(|_| {
            let decode = || {
                let (input, output, reasoning, read, write) = fields?;
                let value = TokenUsageObservation {
                    metadata: metadata(root, at, crate::OPENCODE_MESSAGE_USAGE_SOURCE)?,
                    scope: TokenUsageScope::LastResponse,
                    counter_id: TelemetryId::new(crate::OPENCODE_MESSAGE_USAGE_SOURCE).ok()?,
                    native_turn_id: None,
                    reset_id: None,
                    model: None,
                    input_tokens: Some(input),
                    output_tokens: Some(output),
                    reasoning_tokens: Some(reasoning),
                    cache_read_tokens: Some(read),
                    cache_write_tokens: Some(write),
                    total_tokens: None,
                };
                value.validate().ok()?;
                Some(value)
            };
            decode().map_or(
                NativeTelemetryValue::Invalid,
                NativeTelemetryValue::Observed,
            )
        });
    let context = count("/tokens/output").filter(|n| n.get() > 0).map(|_| {
        let decode = || {
            let (input, output, reasoning, read, write) = fields?;
            let used = [input, output, reasoning, read, write]
                .into_iter()
                .try_fold(0_u64, |sum, n| sum.checked_add(n.get()))?;
            let basis =
                Some(TelemetryId::new("opencode.1.17.17:live-tui-last-assistant-token-sum").ok()?);
            let token = |n| {
                TelemetryCounter::new(n)
                    .ok()
                    .map(|value| ContextTokenValue {
                        value,
                        provenance: TelemetryProvenance::Estimated,
                        basis: basis.clone(),
                    })
            };
            let percent = |n| {
                TelemetryQuantity::new(n)
                    .ok()
                    .map(|value| ContextPercentageValue {
                        value,
                        provenance: TelemetryProvenance::Estimated,
                        basis: basis.clone(),
                    })
            };
            let capacity = if capacity.is_null() {
                None
            } else {
                Some(TelemetryCounter::new(capacity.as_u64()?).ok()?.get())
            };
            if capacity == Some(0) {
                return None;
            }
            let used_percent = capacity.map(|cap| (used as f64 / cap as f64 * 100.0).round());
            let remaining = capacity.and_then(|cap| cap.checked_sub(used));
            let value = ContextObservation {
                metadata: metadata(root, at, crate::OPENCODE_MESSAGE_CONTEXT_SOURCE)?,
                model: None,
                effective_capacity_tokens: capacity.map(|cap| ContextTokenValue {
                    value: TelemetryCounter::new(cap).unwrap(),
                    provenance: TelemetryProvenance::Native,
                    basis: None,
                }),
                used_tokens: Some(token(used)?),
                remaining_tokens: remaining.and_then(token),
                used_percent: used_percent.and_then(percent),
                remaining_percent: used_percent
                    .filter(|_| remaining.is_some())
                    .and_then(|used| percent(100.0 - used)),
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
    });
    Sample { usage, context }
}
