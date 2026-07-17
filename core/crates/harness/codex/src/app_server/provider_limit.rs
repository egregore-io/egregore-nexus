use nexus_contracts::enums::Harness;
use nexus_contracts::ids::SessionId;
use nexus_contracts::ports::{
    InjectError, OperatorAction, ProviderLimit, ProviderLimitReason, ResetHint,
};
use serde_json::Value;

use super::jsonrpc::CodexRpcError;

const RPC_SOURCE: &str = "codex.app_server.rpc_error";
const TURN_SOURCE: &str = "codex.app_server.turn_error";

pub(crate) fn classify_rpc_error(
    session: &SessionId,
    error: &CodexRpcError,
) -> Option<InjectError> {
    let CodexRpcError::Rpc {
        data: Some(data), ..
    } = error
    else {
        return None;
    };
    classify_structured_payload(session, data, RPC_SOURCE)
}

pub(crate) fn classify_turn_error(session: &SessionId, params: &Value) -> Option<InjectError> {
    if params
        .get("willRetry")
        .or_else(|| params.get("will_retry"))
        .and_then(Value::as_bool)
        == Some(true)
    {
        return None;
    }
    classify_structured_payload(session, params, TURN_SOURCE)
}

pub(crate) fn turn_error_is_structured_hold(params: &Value) -> bool {
    let probe = SessionId("__codex_provider_limit_probe__".into());
    classify_turn_error(&probe, params).is_some()
}

pub(crate) fn turn_error_will_retry(params: &Value) -> bool {
    params
        .get("willRetry")
        .or_else(|| params.get("will_retry"))
        .and_then(Value::as_bool)
        == Some(true)
}

fn classify_structured_payload(
    session: &SessionId,
    payload: &Value,
    source: &str,
) -> Option<InjectError> {
    let (info, context) = find_codex_error_info(payload)?;
    let code = codex_error_code(info)?;
    let provider = string_field(context, &["provider"])
        .or_else(|| string_field(info, &["provider"]))
        .or_else(|| string_field(payload, &["provider"]));
    let model = string_field(context, &["model"])
        .or_else(|| string_field(info, &["model"]))
        .or_else(|| string_field(payload, &["model"]));
    let reset_hint = reset_hint(context)
        .or_else(|| reset_hint(payload))
        .or_else(|| reset_hint(info));

    if let Some(reason) = provider_reason(&code) {
        return Some(InjectError::ProviderLimit(ProviderLimit {
            harness: Harness::Codex,
            session: session.clone(),
            reason,
            reset_hint,
            provider,
            model,
            source: source.to_string(),
        }));
    }

    operator_reason(&code).map(|reason| {
        InjectError::OperatorAction(OperatorAction {
            harness: Harness::Codex,
            session: session.clone(),
            reason,
            provider,
            model,
            source: source.to_string(),
        })
    })
}

fn find_codex_error_info(value: &Value) -> Option<(&Value, &Value)> {
    if let Some(info) = value
        .get("codexErrorInfo")
        .or_else(|| value.get("codex_error_info"))
    {
        return Some((info, value));
    }
    for key in ["error", "data", "details"] {
        if let Some(info) = value.get(key).and_then(find_codex_error_info) {
            return Some(info);
        }
    }
    None
}

fn codex_error_code(info: &Value) -> Option<String> {
    if let Some(code) = info.as_str() {
        return Some(normalize_code(code));
    }
    for key in ["type", "kind", "code", "reason"] {
        if let Some(code) = info.get(key).and_then(Value::as_str) {
            return Some(normalize_code(code));
        }
    }
    info.as_object()
        .and_then(|object| object.keys().next())
        .map(|key| normalize_code(key))
}

fn normalize_code(raw: &str) -> String {
    let mut normalized = String::new();
    for (idx, ch) in raw.chars().enumerate() {
        if ch == '-' || ch == ' ' {
            normalized.push('_');
        } else if ch.is_uppercase() {
            if idx > 0 {
                normalized.push('_');
            }
            normalized.extend(ch.to_lowercase());
        } else {
            normalized.push(ch);
        }
    }
    normalized
}

fn provider_reason(code: &str) -> Option<ProviderLimitReason> {
    match code {
        "usage_limit_exceeded" | "usage_limit" | "usage_limit_reached" => {
            Some(ProviderLimitReason::UsageLimit)
        }
        "rate_limit" | "rate_limit_exceeded" | "rate_limit_reached" => {
            Some(ProviderLimitReason::RateLimit)
        }
        "quota_exceeded" | "quota_exhausted" | "credits_depleted" => {
            Some(ProviderLimitReason::QuotaExhausted)
        }
        "server_overloaded" | "overloaded" => Some(ProviderLimitReason::Overloaded),
        _ => None,
    }
}

fn operator_reason(code: &str) -> Option<String> {
    match code {
        "unauthorized"
        | "authentication_required"
        | "invalid_api_key"
        | "billing_required"
        | "payment_required"
        | "account_disabled" => Some(code.to_string()),
        _ => None,
    }
}

fn string_field(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_str))
        .map(str::to_string)
}

fn reset_hint(value: &Value) -> Option<ResetHint> {
    let unix_ms = absolute_millis(value, &["reset_at", "resetAt", "resets_at", "resetsAt"])
        .or_else(|| relative_millis(value, &["retry_after_ms", "retryAfterMs"], 1))
        .or_else(|| relative_millis(value, &["retry_after", "retryAfter", "retry-after"], 1000))?;
    Some(ResetHint { unix_ms })
}

fn absolute_millis(value: &Value, keys: &[&str]) -> Option<i64> {
    let raw = number_field(value, keys)?;
    Some(if raw > 1_000_000_000_000 {
        raw
    } else {
        raw.saturating_mul(1000)
    })
}

fn relative_millis(value: &Value, keys: &[&str], multiplier: i64) -> Option<i64> {
    let raw = number_field(value, keys)?;
    now_ms().map(|now| now.saturating_add(raw.saturating_mul(multiplier)))
}

fn number_field(value: &Value, keys: &[&str]) -> Option<i64> {
    keys.iter().find_map(|key| {
        let value = value.get(*key)?;
        value
            .as_i64()
            .or_else(|| value.as_u64().and_then(|n| i64::try_from(n).ok()))
            .or_else(|| value.as_str().and_then(|raw| raw.parse::<i64>().ok()))
    })
}

fn now_ms() -> Option<i64> {
    Some(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()?
            .as_millis() as i64,
    )
}

#[cfg(test)]
mod tests {
    use nexus_contracts::ports::{InjectError, ProviderLimitReason};
    use serde_json::json;

    use super::*;

    #[test]
    fn usage_limit_info_classifies_from_structured_turn_error() {
        let session = SessionId("s_codex".into());
        let error = classify_turn_error(
            &session,
            &json!({
                "threadId": "t",
                "turnId": "u",
                "willRetry": false,
                "error": {
                    "message": "usage limit reached",
                    "codexErrorInfo": "usageLimitExceeded",
                    "provider": "openai",
                    "model": "gpt-5",
                    "retry_after_ms": 2500
                }
            }),
        )
        .expect("structured usage limit should classify");

        match error {
            InjectError::ProviderLimit(limit) => {
                assert_eq!(limit.session, session);
                assert_eq!(limit.reason, ProviderLimitReason::UsageLimit);
                assert_eq!(limit.provider.as_deref(), Some("openai"));
                assert_eq!(limit.model.as_deref(), Some("gpt-5"));
                assert!(limit.reset_hint.is_some());
            }
            other => panic!("expected provider limit, got {other:?}"),
        }
    }

    #[test]
    fn will_retry_true_does_not_classify() {
        let session = SessionId("s_codex".into());
        assert!(classify_turn_error(
            &session,
            &json!({
                "willRetry": true,
                "error": {"codexErrorInfo": "usageLimitExceeded"}
            }),
        )
        .is_none());
    }

    #[test]
    fn visible_message_text_alone_does_not_classify() {
        let session = SessionId("s_codex".into());
        assert!(classify_turn_error(
            &session,
            &json!({
                "willRetry": false,
                "error": {"message": "usage_limit_exceeded"}
            }),
        )
        .is_none());
    }
}
