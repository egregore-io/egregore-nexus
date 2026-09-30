//! Shared poll loop for daemon-owned native session forwarders.
//!
//! Headed harnesses still own their native row discovery, cursor persistence, and translation.
//! This helper owns the common async loop around those passes so harness-specific forwarders do not
//! duplicate poll cadence, error logging, and post-pass wake policy plumbing.

use std::future::Future;
use std::time::Duration;

use nexus_common::NexusError;
use nexus_contracts::ids::SessionId;

/// Static metadata for a native forwarder poll loop.
#[derive(Debug, Clone)]
pub struct NativeForwarderLoopConfig {
    /// Nexus session whose native sidecar is being forwarded.
    pub session: SessionId,
    /// Poll cadence in milliseconds. Values below one millisecond are clamped to one.
    pub poll_ms: u64,
}

/// Spawn the shared poll loop for one daemon-owned native forwarder.
///
/// `run_pass` performs exactly one harness-specific forward pass. `on_success` applies harness
/// policy after a successful pass, such as ringing the realtime bell after a turn-end event and
/// emitting debug stats. Errors are logged and the loop continues after the configured delay.
pub fn spawn_native_forwarder<Stats, RunPass, PassFuture, OnSuccess, OnError>(
    config: NativeForwarderLoopConfig,
    run_pass: RunPass,
    on_success: OnSuccess,
    on_error: OnError,
) -> tokio::task::JoinHandle<()>
where
    Stats: Send + 'static,
    RunPass: Fn() -> PassFuture + Send + Sync + 'static,
    PassFuture: Future<Output = Result<Stats, NexusError>> + Send + 'static,
    OnSuccess: Fn(&SessionId, Stats) + Send + Sync + 'static,
    OnError: Fn(&SessionId, NexusError) + Send + Sync + 'static,
{
    tokio::spawn(async move {
        let delay = Duration::from_millis(config.poll_ms.max(1));
        loop {
            run_native_forward_pass(&config.session, &run_pass, &on_success, &on_error).await;
            tokio::time::sleep(delay).await;
        }
    })
}

async fn run_native_forward_pass<Stats, RunPass, PassFuture, OnSuccess, OnError>(
    session: &SessionId,
    run_pass: &RunPass,
    on_success: &OnSuccess,
    on_error: &OnError,
) where
    RunPass: Fn() -> PassFuture,
    PassFuture: Future<Output = Result<Stats, NexusError>>,
    OnSuccess: Fn(&SessionId, Stats),
    OnError: Fn(&SessionId, NexusError),
{
    match run_pass().await {
        Ok(stats) => on_success(session, stats),
        Err(error) => on_error(session, error),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[tokio::test]
    async fn successful_pass_invokes_success_policy_once() {
        let calls = AtomicUsize::new(0);
        let session = SessionId("s_native".to_string());

        run_native_forward_pass(
            &session,
            &|| async { Ok::<_, NexusError>(7usize) },
            &|observed, stats| {
                assert_eq!(observed, &session);
                assert_eq!(stats, 7);
                calls.fetch_add(1, Ordering::SeqCst);
            },
            &|_, _| panic!("error handler should not run"),
        )
        .await;

        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn failed_pass_does_not_invoke_success_policy() {
        let calls = AtomicUsize::new(0);
        let session = SessionId("s_native".to_string());

        run_native_forward_pass::<usize, _, _, _, _>(
            &session,
            &|| async { Err(NexusError::Internal("boom".to_string())) },
            &|_, _| {
                calls.fetch_add(1, Ordering::SeqCst);
            },
            &|observed, error| {
                assert_eq!(observed, &session);
                assert_eq!(error.to_string(), "internal: boom");
            },
        )
        .await;

        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}
