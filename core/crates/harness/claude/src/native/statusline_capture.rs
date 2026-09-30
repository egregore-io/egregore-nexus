//! Bounded launch-local statusline snapshots. No store, provider, or global settings access.
use nexus_contracts::telemetry::{TelemetryId, TelemetryTimestamp};
use serde_json::{json, Value};
use std::io::{self, Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

pub const MAX_STATUSLINE_BYTES: usize = 65536;

/// The timestamp belongs to native capture, never to subsequent forwarder polls.
#[derive(Debug, Clone, PartialEq)]
pub struct StatuslineSnapshot {
    pub observed_at: i64,
    pub payload: Value,
}

pub fn capture(input: impl Read, path: &Path, observed_at: i64) -> io::Result<()> {
    TelemetryTimestamp::new(observed_at).map_err(bad)?;
    let raw = read_json(input)?;
    let root = raw
        .get("session_id")
        .and_then(Value::as_str)
        .ok_or_else(|| bad("missing native root"))?;
    TelemetryId::new(root).map_err(bad)?;
    let mut payload = json!({"session_id":root});
    if let Some(prompt) = raw.get("prompt_id") {
        if !prompt.is_null() {
            TelemetryId::new(
                prompt
                    .as_str()
                    .ok_or_else(|| bad("invalid native prompt id"))?,
            )
            .map_err(bad)?;
        }
        payload["prompt_id"] = prompt.clone();
    }
    if let Some(context) = raw.get("context_window") {
        let mut selected = select_numbers(
            context,
            &[
                "context_window_size",
                "used_percentage",
                "remaining_percentage",
            ],
        );
        if let Some(usage) = context.get("current_usage") {
            selected["current_usage"] = select_numbers(
                usage,
                &[
                    "input_tokens",
                    "cache_creation_input_tokens",
                    "cache_read_input_tokens",
                ],
            );
        }
        payload["context_window"] = selected;
    }
    if let Some(limits) = raw.get("rate_limits") {
        let mut selected = if limits.is_object() {
            json!({})
        } else {
            safe_scalar(limits)
        };
        for id in ["five_hour", "seven_day", "spend_limit"] {
            if let Some(window) = limits.get(id) {
                selected[id] = select_numbers(window, &["used_percentage", "resets_at"]);
            }
        }
        payload["rate_limits"] = selected;
    }
    let bytes = serde_json::to_vec(&json!({"observedAt":observed_at,"payload":payload}))?;
    if bytes.len() > MAX_STATUSLINE_BYTES {
        return Err(bad("statusline snapshot exceeds limit"));
    }
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let parent = path
        .parent()
        .ok_or_else(|| bad("snapshot needs a parent directory"))?;
    let temporary = parent.join(format!(
        ".statusline-{}-{}.tmp",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    // Only clean up a temporary file this invocation actually created.
    let mut file = options.open(&temporary)?;
    let result = (|| {
        file.write_all(&bytes)?;
        drop(file);
        // Ephemeral replacement, not a durable commit receipt. Readers see the prior complete
        // snapshot or this complete snapshot, never a truncated in-place write.
        std::fs::rename(&temporary, path)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

pub fn read_snapshot(path: &Path) -> io::Result<Option<StatuslineSnapshot>> {
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let raw = read_json(file)?;
    let observed_at = raw
        .get("observedAt")
        .and_then(Value::as_i64)
        .ok_or_else(|| bad("missing capture timestamp"))?;
    TelemetryTimestamp::new(observed_at).map_err(bad)?;
    let payload = raw
        .get("payload")
        .filter(|v| v.is_object())
        .ok_or_else(|| bad("invalid snapshot payload"))?
        .clone();
    Ok(Some(StatuslineSnapshot {
        observed_at,
        payload,
    }))
}

fn read_json(input: impl Read) -> io::Result<Value> {
    let mut bytes = Vec::new();
    input
        .take(MAX_STATUSLINE_BYTES as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_STATUSLINE_BYTES {
        return Err(bad("statusline input exceeds limit"));
    }
    serde_json::from_slice(&bytes).map_err(bad)
}

fn select_numbers(raw: &Value, keys: &[&str]) -> Value {
    if !raw.is_object() {
        return safe_scalar(raw);
    }
    let mut selected = json!({});
    for key in keys {
        if let Some(value) = raw.get(*key) {
            selected[*key] = safe_scalar(value);
        }
    }
    selected
}

fn safe_scalar(value: &Value) -> Value {
    match value {
        Value::Number(_) | Value::Null => value.clone(),
        // Preserve an invalid-type signal without persisting arbitrary strings/objects.
        _ => Value::Bool(false),
    }
}

fn bad(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}
