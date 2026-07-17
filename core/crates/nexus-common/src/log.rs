//! Tracing initialization.

/// Initialize tracing once (env-filter from `RUST_LOG`).
pub fn init_tracing() {
    use tracing_subscriber::{fmt, EnvFilter};
    let _ = fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .try_init();
}
