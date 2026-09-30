//! Codex 0.154 account-wide rolling windows, observed through one captured connection.
//! The native notification has no thread/account identifier. Do not manufacture account identity,
//! interpret credits as money, or add windows to session usage. Native account/read requires the
//! Codex/ChatGPT backend; observing its existing notifications adds no provider request.
use std::collections::BTreeMap;

use nexus_agent::adapter::NativeModelReporting;
use nexus_contracts::telemetry::*;
use nexus_contracts::ModelObservationSource;
use serde::Deserialize;

use super::Notification;

pub(super) const SOURCE: &str = "codex.appserver.accountRateLimits";
const MAX_BUCKETS: usize = 8;

#[derive(Default)]
pub(super) struct State {
    // At most primary + secondary for each native bucket; no growing notification history.
    buckets: BTreeMap<Option<String>, Bucket>,
}

#[derive(Default)]
struct Bucket {
    primary: Option<Sample>,
    secondary: Option<Sample>,
}

struct Sample {
    window: QuotaWindow,
    observed_at: TelemetryTimestamp,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Snapshot {
    limit_id: Option<TelemetryId>,
    primary: Option<Window>,
    secondary: Option<Window>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Window {
    used_percent: i32,
    window_duration_mins: Option<u64>,
    resets_at: Option<i64>,
}

impl Window {
    fn decode(self, bucket: Option<&str>, slot: &str) -> Result<QuotaWindow, ()> {
        let id = bucket.map_or_else(|| slot.to_owned(), |id| format!("{id}/{slot}"));
        let window = QuotaWindow {
            window_id: TelemetryId::new(id).map_err(|_| ())?,
            units: TelemetryId::new("percent").unwrap(),
            used: None,
            remaining: None,
            limit: None,
            used_percent: Some(
                TelemetryQuantity::new(f64::from(self.used_percent)).map_err(|_| ())?,
            ),
            // There is no native remaining-percent field in this notification. Preserve the
            // actual used value, including native overage, without a guessed allowance amount.
            remaining_percent: None,
            resets_at: self
                .resets_at
                .map(|seconds| {
                    TelemetryTimestamp::new(seconds.checked_mul(1000).ok_or(())?).map_err(|_| ())
                })
                .transpose()?,
            window_seconds: self
                .window_duration_mins
                .map(|minutes| {
                    TelemetryCounter::new(minutes.checked_mul(60).ok_or(())?).map_err(|_| ())
                })
                .transpose()?,
        };
        window.validate().map_err(|_| ())?;
        Ok(window)
    }
}

impl State {
    /// Caller holds the published, non-disconnected owner registry exclusion. Global account
    /// metadata never becomes thread/turn status authority, even though displayed by a runtime.
    pub(super) fn ingest(
        &mut self,
        note: &Notification,
        root: &str,
        reporting: &NativeModelReporting,
    ) {
        let value = if note.method == "account/updated" {
            self.buckets.clear();
            Some(NativeTelemetryValue::Unknown)
        } else {
            match self.update_at(&note.params["rateLimits"], root, nexus_common::now()) {
                Ok(value) => value,
                Err(()) => {
                    self.buckets.clear();
                    Some(NativeTelemetryValue::Invalid)
                }
            }
        };
        if let Some(value) = value {
            reporting
                .sink()
                .observe_telemetry(NativeTelemetryUpdate::Quota {
                    native_session_id: root.into(),
                    value,
                });
        }
    }

    pub(super) fn update_at(
        &mut self,
        raw: &serde_json::Value,
        root: &str,
        observed_at: i64,
    ) -> Result<Option<NativeTelemetryValue<AccountQuotaObservation>>, ()> {
        let observed_at = TelemetryTimestamp::new(observed_at).map_err(|_| ())?;
        let snapshot: Snapshot = serde_json::from_value(raw.clone()).map_err(|_| ())?;
        if snapshot.primary.is_none() && snapshot.secondary.is_none() {
            // Credits/plan-only sparse metadata does not refresh the age of cached windows.
            return Ok(self
                .buckets
                .is_empty()
                .then_some(NativeTelemetryValue::Unknown));
        }
        let key = snapshot.limit_id.map(|id| id.as_str().to_owned());
        let primary = snapshot
            .primary
            .map(|window| window.decode(key.as_deref(), "primary"))
            .transpose()?;
        let secondary = snapshot
            .secondary
            .map(|window| window.decode(key.as_deref(), "secondary"))
            .transpose()?;
        if !self.buckets.contains_key(&key) && self.buckets.len() >= MAX_BUCKETS {
            return Err(());
        }
        let bucket = self.buckets.entry(key).or_default();
        // Native rolling updates are sparse: absent/null windows do not erase prior available
        // values. Account changes and invalid snapshots clear the entire captured cache instead.
        if let Some(window) = primary {
            bucket.primary = Some(Sample {
                window,
                observed_at,
            });
        }
        if let Some(window) = secondary {
            bucket.secondary = Some(Sample {
                window,
                observed_at,
            });
        }
        let samples: Vec<_> = self
            .buckets
            .values()
            .flat_map(|bucket| [&bucket.primary, &bucket.secondary].into_iter().flatten())
            .collect();
        // The wire has one timestamp for the complete snapshot. A sparse update must not
        // relabel untouched windows (including other buckets) as newly observed.
        let oldest = samples
            .iter()
            .map(|sample| sample.observed_at)
            .min_by_key(|timestamp| timestamp.get())
            .ok_or(())?;
        let value = AccountQuotaObservation {
            metadata: TelemetryMetadata {
                native_session_id: TelemetryId::new(root).map_err(|_| ())?,
                source: ModelObservationSource::new(SOURCE).unwrap(),
                observed_at: oldest,
                native_reported_at: None,
            },
            provider_id: TelemetryId::new("openai").unwrap(),
            // account/rateLimits/updated never supplies an account ID. In particular limitId,
            // planType, native root and configured model/provider are not account identity.
            account_id: None,
            windows: samples.iter().map(|sample| sample.window.clone()).collect(),
        };
        value.validate().map_err(|_| ())?;
        Ok(Some(NativeTelemetryValue::Observed(value)))
    }
}
