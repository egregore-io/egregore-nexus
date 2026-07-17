//! Structured provider-limit classification shared by ACP adapters.
//!
//! The realtime breaker only accepts typed failures produced by harness-owned structured frames.
//! This module deliberately ignores human-visible `message`/`content` strings so visible assistant
//! text can never trip a provider hold.

use std::time::{SystemTime, UNIX_EPOCH};

use agent_client_protocol::Error;
use nexus_contracts::{Harness, ProviderLimitReason, ResetHint};
use serde_json::Value;

use super::{
    AdapterInjectError, AdapterOperatorAction, AdapterProviderError, AdapterProviderLimit,
};

/// Classify an ACP prompt error before the adapter collapses it to a string.
///
/// Only structured `error.data` (plus ACP's structured auth code) participates. The human-facing
/// error message is intentionally ignored.
pub fn classify_acp_prompt_error(
    harness: Harness,
    error: &Error,
    source: &'static str,
) -> Option<AdapterInjectError> {
    if let Some(data) = error.data.as_ref() {
        if let Some(classified) = classify_structured_payload(harness, data, source) {
            return Some(classified);
        }
        if let Some(reason) = retryable_provider_reason(data) {
            return Some(AdapterInjectError::ProviderError(AdapterProviderError {
                harness,
                reason,
                provider: find_string_field(data, &["provider", "provider_name", "providerName"]),
                model: find_string_field(data, &["model", "model_name", "modelName"]),
                retryable: true,
                source: source.to_string(),
            }));
        }
    }

    let code: i32 = error.code.into();
    match code {
        -32000 | 401 | 402 | 403 => {
            Some(AdapterInjectError::OperatorAction(AdapterOperatorAction {
                harness,
                reason: format!("acp error code {code} requires operator action"),
                provider: None,
                model: None,
                source: source.to_string(),
            }))
        }
        429 => Some(AdapterInjectError::ProviderLimit(AdapterProviderLimit {
            harness,
            reason: ProviderLimitReason::RateLimit,
            reset_hint: None,
            provider: None,
            model: None,
            source: source.to_string(),
        })),
        503 => Some(AdapterInjectError::ProviderLimit(AdapterProviderLimit {
            harness,
            reason: ProviderLimitReason::Overloaded,
            reset_hint: None,
            provider: None,
            model: None,
            source: source.to_string(),
        })),
        _ => None,
    }
}

fn retryable_provider_reason(value: &Value) -> Option<String> {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                let normalized_key = normalize_key(key);
                if matches!(
                    normalized_key.as_str(),
                    "errorkind" | "error_kind" | "failurekind" | "failure_kind"
                ) {
                    if let Some(token) = child.as_str() {
                        let token = normalize_token(token);
                        if token == "server_error" {
                            return Some(token);
                        }
                    }
                }
                if let Some(reason) = retryable_provider_reason(child) {
                    return Some(reason);
                }
            }
            None
        }
        Value::Array(items) => items.iter().find_map(retryable_provider_reason),
        _ => None,
    }
}

/// Classify an adapter-owned structured provider payload from a non-ACP surface.
///
/// This is used by headed bridge paths that already preserved a machine-readable provider error.
/// Human-visible message text must stay outside the payload passed here.
pub fn classify_structured_provider_payload(
    harness: Harness,
    value: &Value,
    source: &'static str,
) -> Option<AdapterInjectError> {
    classify_structured_payload(harness, value, source)
}

/// Classify Claude's native `StopFailure` hook fields without looking at visible transcript text.
pub fn classify_claude_stop_failure(
    error: Option<&str>,
    error_details: Option<&str>,
) -> Option<AdapterInjectError> {
    let mut data = serde_json::Map::new();
    if let Some(error) = error {
        data.insert("error".to_string(), Value::String(error.to_string()));
    }
    if let Some(details) = error_details {
        let parsed = serde_json::from_str::<Value>(details)
            .unwrap_or_else(|_| Value::String(details.to_string()));
        data.insert("error_details".to_string(), parsed);
    }
    classify_structured_payload(
        Harness::Claude,
        &Value::Object(data),
        "claude.native.stop_failure",
    )
}

fn classify_structured_payload(
    harness: Harness,
    value: &Value,
    source: &'static str,
) -> Option<AdapterInjectError> {
    let mut observed = Observed::default();
    collect_observed(None, value, &mut observed);

    let provider = find_string_field(value, &["provider", "provider_name", "providerName"]);
    let model = find_string_field(value, &["model", "model_name", "modelName"]);
    let reset_hint = find_reset_hint(value);

    if observed.operator_action {
        let reason = observed
            .operator_token
            .unwrap_or_else(|| "operator_action".to_string());
        return Some(AdapterInjectError::OperatorAction(AdapterOperatorAction {
            harness,
            reason,
            provider,
            model,
            source: source.to_string(),
        }));
    }

    observed.reason.map(|reason| {
        AdapterInjectError::ProviderLimit(AdapterProviderLimit {
            harness,
            reason,
            reset_hint,
            provider,
            model,
            source: source.to_string(),
        })
    })
}

#[derive(Default)]
struct Observed {
    reason: Option<ProviderLimitReason>,
    operator_action: bool,
    operator_token: Option<String>,
}

fn collect_observed(key: Option<&str>, value: &Value, observed: &mut Observed) {
    match value {
        Value::Object(map) => {
            for (child_key, child) in map {
                collect_observed(Some(child_key), child, observed);
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_observed(key, item, observed);
            }
        }
        Value::String(token) => {
            if classification_key(key) {
                observe_token(token, observed);
            }
        }
        Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                observe_status(key, value, observed);
            }
        }
        _ => {}
    }
}

fn classification_key(key: Option<&str>) -> bool {
    let Some(key) = key else {
        return false;
    };
    matches!(
        normalize_key(key).as_str(),
        "error"
            | "code"
            | "type"
            | "reason"
            | "kind"
            | "category"
            | "failure_reason"
            | "failurereason"
            | "failure"
            | "error_code"
            | "errorcode"
            | "error_type"
            | "errortype"
            | "provider_error_code"
            | "providererrorcode"
            | "status"
            | "status_code"
            | "statuscode"
            | "http_status"
            | "httpstatus"
            | "http_status_code"
            | "httpstatuscode"
    )
}

fn observe_token(token: &str, observed: &mut Observed) {
    let token = normalize_token(token);
    let reason = match token.as_str() {
        "rate_limit"
        | "ratelimit"
        | "rate_limit_exceeded"
        | "ratelimitexceeded"
        | "rate_limit_error"
        | "ratelimiterror"
        | "too_many_requests"
        | "toomanyrequests"
        | "429" => Some(ProviderLimitReason::RateLimit),
        "usage_limit"
        | "usagelimit"
        | "usage_limit_reached"
        | "usagelimitreached"
        | "usage_limit_exceeded"
        | "usagelimitexceeded" => Some(ProviderLimitReason::UsageLimit),
        "quota" | "quota_exhausted" | "quotaexhausted" | "quota_exceeded" | "quotaexceeded"
        | "insufficient_quota" | "insufficientquota" => Some(ProviderLimitReason::QuotaExhausted),
        "overloaded"
        | "server_overloaded"
        | "serveroverloaded"
        | "provider_overloaded"
        | "provideroverloaded"
        | "service_unavailable"
        | "serviceunavailable"
        | "503" => Some(ProviderLimitReason::Overloaded),
        "auth_required"
        | "authrequired"
        | "unauthorized"
        | "invalid_api_key"
        | "invalidapikey"
        | "billing_required"
        | "billingrequired"
        | "payment_required"
        | "paymentrequired"
        | "account_disabled"
        | "accountdisabled"
        | "insufficient_balance"
        | "insufficientbalance"
        | "401"
        | "402"
        | "403" => {
            observed.operator_action = true;
            observed.operator_token = Some(token);
            None
        }
        _ => None,
    };
    if observed.reason.is_none() {
        observed.reason = reason;
    }
}

fn observe_status(key: Option<&str>, value: i64, observed: &mut Observed) {
    if !classification_key(key) {
        return;
    }
    match value {
        429 => {
            if observed.reason.is_none() {
                observed.reason = Some(ProviderLimitReason::RateLimit);
            }
        }
        503 | 529 => {
            if observed.reason.is_none() {
                observed.reason = Some(ProviderLimitReason::Overloaded);
            }
        }
        401 | 402 | 403 => {
            observed.operator_action = true;
            observed.operator_token = Some(value.to_string());
        }
        _ => {}
    }
}

fn find_string_field(value: &Value, names: &[&str]) -> Option<String> {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                if names
                    .iter()
                    .any(|name| normalize_key(name) == normalize_key(key))
                {
                    if let Some(value) = child.as_str().filter(|s| !s.is_empty()) {
                        return Some(value.to_string());
                    }
                }
                if let Some(found) = find_string_field(child, names) {
                    return Some(found);
                }
            }
            None
        }
        Value::Array(items) => items.iter().find_map(|item| find_string_field(item, names)),
        _ => None,
    }
}

fn find_reset_hint(value: &Value) -> Option<ResetHint> {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                let key = normalize_key(key);
                if let Some(ms) = parse_number_or_string(child) {
                    let unix_ms = match key.as_str() {
                        "retry_after_ms" | "retryafterms" => now_ms().saturating_add(ms.max(1)),
                        "retry_after_seconds" | "retryafterseconds" => {
                            now_ms().saturating_add(ms.saturating_mul(1000).max(1))
                        }
                        "retry_after" | "retry-after" | "retryafter" => {
                            now_ms().saturating_add(ms.saturating_mul(1000).max(1))
                        }
                        "resets_at" | "resetsat" | "reset_at" | "resetat" | "reset_time"
                        | "resettime" => absolute_ms(ms),
                        _ => {
                            if let Some(found) = find_reset_hint(child) {
                                return Some(found);
                            }
                            continue;
                        }
                    };
                    return Some(ResetHint { unix_ms });
                }
                if let Some(found) = find_reset_hint(child) {
                    return Some(found);
                }
            }
            None
        }
        Value::Array(items) => items.iter().find_map(find_reset_hint),
        _ => None,
    }
}

fn parse_number_or_string(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|v| i64::try_from(v).ok()))
        .or_else(|| value.as_f64().map(|v| v as i64))
        .or_else(|| value.as_str().and_then(|v| v.parse::<i64>().ok()))
}

fn absolute_ms(value: i64) -> i64 {
    if value > 1_000_000_000_000 {
        value
    } else {
        value.saturating_mul(1000)
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

fn normalize_key(value: &str) -> String {
    value
        .chars()
        .filter(|ch| !matches!(ch, '-' | '.'))
        .flat_map(char::to_lowercase)
        .collect()
}

fn normalize_token(value: &str) -> String {
    value
        .trim()
        .chars()
        .map(|ch| match ch {
            '-' | ' ' | '.' => '_',
            other => other.to_ascii_lowercase(),
        })
        .collect()
}
