//! Codex 0.154 app-server token snapshots. Native turn IDs fence context anchors; they are
//! not invented counter epochs. No catalog, account allowance, or lifetime subtraction.
use nexus_agent::adapter::NativeModelReporting;
use nexus_contracts::telemetry::*;
use nexus_contracts::ModelObservationSource;
use serde::Deserialize;
use serde_json::Value;

use super::{method, Notification};

pub(super) const SOURCE: &str = "codex.appserver.tokenUsage";

#[derive(Default)]
pub(super) struct State {
    turn: Option<String>,
    context_blocked: bool,
}

impl State {
    /// Called only for the published exact root while holding its owner registry exclusion.
    pub(super) fn ingest(
        &mut self,
        note: &Notification,
        root: &str,
        reporting: &NativeModelReporting,
    ) {
        let turn = note
            .params
            .get("turnId")
            .and_then(Value::as_str)
            .or_else(|| note.params.get("turn")?.get("id")?.as_str());
        let clear = || {
            reporting
                .sink()
                .observe_telemetry(NativeTelemetryUpdate::Context {
                    native_session_id: root.into(),
                    value: NativeTelemetryValue::Unknown,
                })
        };
        if note.method == method::TURN_STARTED {
            if let Some(turn) = turn.filter(|turn| TelemetryId::new(*turn).is_ok()) {
                if self.turn.as_deref() != Some(turn) {
                    self.turn = Some(turn.into());
                    self.context_blocked = false;
                    clear();
                }
            }
            return;
        }
        let compaction = note.method == method::THREAD_COMPACTED
            || ((note.method == method::ITEM_STARTED || note.method == method::ITEM_COMPLETED)
                && note.params.pointer("/item/type").and_then(Value::as_str)
                    == Some("contextCompaction"));
        let full = note.method == method::TURN_FAILED
            && note
                .params
                .pointer("/error/codexErrorInfo")
                .and_then(Value::as_str)
                == Some("contextWindowExceeded");
        if compaction || full {
            // A compacted notification may omit turnId. Never let an explicitly older turn
            // invalidate the new turn; absence still conservatively clears its anchor.
            if turn.is_none() || self.turn.is_none() || turn == self.turn.as_deref() {
                self.context_blocked = true;
                clear();
            }
            return;
        }
        if note.method != method::TOKEN_USAGE_UPDATED {
            return;
        }
        let Some(turn) = turn.filter(|turn| TelemetryId::new(*turn).is_ok()) else {
            return;
        };
        if self.turn.as_deref().is_some_and(|current| current != turn) {
            return;
        }
        // Attachment may miss turn/started. The first exact-root native sample supplies a
        // last-reported turn identity, not routing authority or a newly minted reset epoch.
        if self.turn.is_none() {
            self.turn = Some(turn.into());
        }
        let Some(metadata) = metadata(root) else {
            return;
        };
        let raw = &note.params["tokenUsage"];
        let usage = decode_usage(&raw["total"], metadata.clone(), turn);
        let context = decode_context(raw, metadata, !self.context_blocked);
        reporting
            .sink()
            .observe_telemetry(NativeTelemetryUpdate::Usage {
                native_session_id: root.into(),
                value: usage,
            });
        reporting
            .sink()
            .observe_telemetry(NativeTelemetryUpdate::Context {
                native_session_id: root.into(),
                value: context,
            });
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Breakdown {
    input_tokens: TelemetryCounter,
    output_tokens: TelemetryCounter,
    cached_input_tokens: TelemetryCounter,
    reasoning_output_tokens: TelemetryCounter,
    total_tokens: TelemetryCounter,
    // Omission is preserved, not replaced by an observed zero.
    cache_write_input_tokens: Option<TelemetryCounter>,
}
impl Breakdown {
    fn consistent(&self) -> bool {
        self.input_tokens
            .get()
            .checked_add(self.output_tokens.get())
            == Some(self.total_tokens.get())
    }
}

fn metadata(root: &str) -> Option<TelemetryMetadata> {
    Some(TelemetryMetadata {
        native_session_id: TelemetryId::new(root).ok()?,
        source: ModelObservationSource::new(SOURCE).ok()?,
        observed_at: TelemetryTimestamp::new(nexus_common::now()).ok()?,
        native_reported_at: None,
    })
}

fn decode_usage(
    raw: &Value,
    metadata: TelemetryMetadata,
    turn: &str,
) -> NativeTelemetryValue<TokenUsageObservation> {
    let Ok(value) = serde_json::from_value::<Breakdown>(raw.clone()) else {
        return NativeTelemetryValue::Invalid;
    };
    // fill_to_context_window can manufacture a total-only value after a window error.
    // Consistency is only a veto, not proof of provenance or freshness.
    if !value.consistent() {
        return NativeTelemetryValue::Invalid;
    }
    NativeTelemetryValue::Observed(TokenUsageObservation {
        metadata,
        scope: TokenUsageScope::SessionCumulative,
        counter_id: TelemetryId::new("codex.thread.tokenUsage.total").unwrap(),
        reset_id: None,
        native_turn_id: Some(TelemetryId::new(turn).unwrap()),
        model: None,
        input_tokens: Some(value.input_tokens),
        output_tokens: Some(value.output_tokens),
        cache_read_tokens: Some(value.cached_input_tokens),
        cache_write_tokens: value.cache_write_input_tokens,
        reasoning_tokens: Some(value.reasoning_output_tokens),
        total_tokens: Some(value.total_tokens),
    })
}

fn token(value: TelemetryCounter, basis: Option<&str>) -> ContextTokenValue {
    ContextTokenValue {
        value,
        provenance: if basis.is_some() {
            TelemetryProvenance::Estimated
        } else {
            TelemetryProvenance::Native
        },
        basis: basis.map(|basis| TelemetryId::new(basis).unwrap()),
    }
}
fn percent(value: f64, basis: &str) -> Option<ContextPercentageValue> {
    Some(ContextPercentageValue {
        value: TelemetryQuantity::new(value).ok()?,
        provenance: TelemetryProvenance::Estimated,
        basis: Some(TelemetryId::new(basis).unwrap()),
    })
}

fn decode_context(
    raw: &Value,
    metadata: TelemetryMetadata,
    anchored: bool,
) -> NativeTelemetryValue<ContextObservation> {
    let capacity = match raw.get("modelContextWindow") {
        None | Some(Value::Null) => None,
        Some(value) => match serde_json::from_value::<TelemetryCounter>(value.clone()) {
            Ok(value) if value.get() > 0 => Some(value),
            _ => return NativeTelemetryValue::Invalid,
        },
    };
    let last = serde_json::from_value::<Breakdown>(raw["last"].clone()).ok();
    // The last reported model sample excludes subsequent local prompt/tool additions. It is
    // not made fresh by a notification (rate-limit events can repeat it under the current turn).
    let synthetic_total = raw.pointer("/total/inputTokens").and_then(Value::as_u64) == Some(0)
        && raw.pointer("/total/outputTokens").and_then(Value::as_u64) == Some(0)
        && raw
            .pointer("/total/totalTokens")
            .and_then(Value::as_u64)
            .is_some_and(|total| total > 0);
    let used = last
        .filter(|last| anchored && last.consistent() && !synthetic_total)
        .map(|last| last.total_tokens);
    if capacity.is_none() && used.is_none() {
        return NativeTelemetryValue::Unknown;
    }
    let mut context = ContextObservation {
        metadata,
        model: None,
        effective_capacity_tokens: capacity.map(|v| token(v, None)),
        used_tokens: used
            .map(|v| token(v, Some("codex.last.totalTokens:last-reported-model-sample"))),
        remaining_tokens: None,
        used_percent: None,
        remaining_percent: None,
        output_reserve_tokens: None,
        compaction_count: None,
        reset_id: None,
    };
    if let (Some(capacity), Some(used)) = (capacity, used) {
        // Match Codex 0.154 TokenUsage::percent_of_context_window_remaining and its TUI
        // caller over last_token_usage. This fixed UI baseline is not an output reserve or
        // a native percent field. Keep the raw token values independent of this percentage.
        let remaining_percent = if capacity.get() <= 12000 {
            0.0
        } else {
            let effective = capacity.get() - 12000;
            let display_used = used.get().saturating_sub(12000);
            (effective.saturating_sub(display_used) as f64 / effective as f64 * 100.0)
                .clamp(0.0, 100.0)
                .round()
        };
        let basis = "codex.0.154.display:baseline12000:last-reported-estimate";
        context.remaining_percent = percent(remaining_percent, basis);
        context.used_percent = percent(100.0 - remaining_percent, basis);
        // Do not clamp an over-window sample into a fabricated zero remaining observation.
        if let Some(remaining) = capacity.get().checked_sub(used.get()) {
            context.remaining_tokens = Some(token(
                TelemetryCounter::new(remaining).unwrap(),
                Some("codex.modelContextWindow-last.totalTokens:last-reported-estimate"),
            ));
        }
    }
    NativeTelemetryValue::Observed(context)
}
