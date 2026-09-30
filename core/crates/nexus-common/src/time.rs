//! The wire clock.

/// Unix epoch milliseconds (the wire clock for every `*_at`/`when` field).
pub fn now() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}
