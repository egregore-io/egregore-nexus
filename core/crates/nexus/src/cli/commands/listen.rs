//! Held-receive command: `nexus listen` (the agent's drain loop).
//!
//! `listen` submits daemon-managed command intents for the drain-once held-receive window, prints
//! each drop, then **split-acks** what it drained: DMs per-message via `inbox.ack`, threads in one
//! bulk `inbox.ack_threads` (backend §2 step 7). This is how an agent or human CLI drains its inbox
//! without opening a daemon ingress transport.

use std::process::ExitCode;

use clap::Args;
use nexus_contracts::{
    AckRequest, AckResponse, AckThreadsRequest, BatchCounts, ConsumeRequest, ContractError,
    InboxSubscribeRequest, InboxSubscribeResponse, InboxSubscriptionAckRequest,
    InboxSubscriptionBatch, InboxSubscriptionNextRequest, InboxSubscriptionNextResponse,
    InboxSubscriptionStatusResponse, InboxUnsubscribeRequest, NexusBatch,
};

use crate::cli::render::{print_human, print_json};
use crate::cli::store_client::StoreClient;

const MAX_CONSECUTIVE_FAILURES: u32 = 10;
const IDLE_POLL_FLOOR: std::time::Duration = std::time::Duration::from_millis(500);
const CLEANUP_WAIT: std::time::Duration = std::time::Duration::from_millis(50);

/// `nexus listen [--timeout-ms N] [--max N] [--no-ack] [--once]`.
#[derive(Args, Debug)]
pub struct ListenArgs {
    /// Held-receive window per drain, in ms (None = daemon default).
    #[arg(long = "timeout-ms")]
    pub timeout_ms: Option<u32>,
    /// Cap on messages drained per drop (None = daemon DRAIN_LIMIT).
    #[arg(long)]
    pub max: Option<u32>,
    /// Do not ack drained messages (peek-only; they remain in-flight).
    #[arg(long = "no-ack")]
    pub no_ack: bool,
    /// Drain exactly once and exit (default loops until interrupted).
    #[arg(long)]
    pub once: bool,
}

/// The drain-once + split-ack loop. Each iteration fetches one bounded daemon-side subscription
/// slice, then (unless `--no-ack`) per-message `ack`s DMs and bulk `ackThreads`s threads. With
/// `--once --timeout-ms N`, bounded internal slices retain one subscription until a batch arrives
/// or the caller's whole `N`-millisecond window expires. Without an explicit timeout, `--once`
/// preserves the historical one-slice behavior. Empty polls are floored client-side: if the
/// daemon-side window returns instantly with no batch, the long-running loop sleeps out the
/// remainder of the floor instead of busy-polling.
pub async fn listen(client: &StoreClient, a: ListenArgs, json: bool) -> ExitCode {
    if a.no_ack {
        return listen_legacy(client, a, json).await;
    }

    let mut subscription = match subscribe_with_retry(client, a.timeout_ms, a.max).await {
        Ok(subscription) => subscription.subscription_id,
        Err(e) => {
            eprintln!("error: {}", e.message);
            return ExitCode::from(1);
        }
    };

    // Transient-error resilience: a long-running listen loop
    // must not stop permanently after a transient command error. Retry with backoff and a fresh
    // subscription; exit only for auth failures (the caller's identity is actually bad) or
    // sustained failure (the daemon is actually gone).
    // Idle floor: the daemon-side held-receive window is the intended
    // pacing, but when it collapses instantly (empty inbox, degraded window) an unfloored
    // loop busy-polls and can saturate a core against an idle inbox. An empty
    // poll never repeats faster than this.
    let mut consecutive_failures: u32 = 0;
    let once_deadline = if a.once {
        a.timeout_ms
            .filter(|timeout_ms| *timeout_ms > 0)
            .map(|timeout_ms| {
                std::time::Instant::now() + std::time::Duration::from_millis(u64::from(timeout_ms))
            })
    } else {
        None
    };

    loop {
        let step_started = std::time::Instant::now();
        let next_timeout_ms = once_deadline
            .map(remaining_timeout_ms)
            .unwrap_or(a.timeout_ms);
        let step: Result<(bool, NexusBatch), ContractError> = async {
            let durable = next_batch(client, &subscription, next_timeout_ms)
                .await?
                .batch;
            let batch = durable
                .as_ref()
                .map(|durable| durable.batch.clone())
                .unwrap_or_else(empty_batch);
            if let Some(durable) = durable {
                split_ack(client, &batch).await?;
                ack_subscription_batch(client, &durable).await?;
                Ok((true, batch))
            } else {
                Ok((false, batch))
            }
        }
        .await;

        match step {
            Ok((had_batch, batch)) => {
                consecutive_failures = 0;
                if a.once {
                    let window_expired = once_deadline
                        .map(|deadline| std::time::Instant::now() >= deadline)
                        .unwrap_or(true);
                    if had_batch || window_expired {
                        render_batch(&batch, json);
                        // Give the daemon a short opportunity to confirm cleanup. The command
                        // intent becomes durable before result polling, so a delayed completion
                        // must not turn a successful one-shot receive into a failure or strand the
                        // caller waiting on cleanup.
                        match tokio::time::timeout(CLEANUP_WAIT, unsubscribe(client, &subscription))
                            .await
                        {
                            Ok(Ok(_)) => {}
                            Ok(Err(error)) => eprintln!(
                                "warn: listener cleanup is still pending: {}",
                                error.message
                            ),
                            Err(_) => eprintln!("warn: listener cleanup is still pending"),
                        }
                        return ExitCode::SUCCESS;
                    }
                    continue;
                }
                render_batch(&batch, json);
                if !had_batch {
                    let elapsed = step_started.elapsed();
                    if elapsed < IDLE_POLL_FLOOR {
                        tokio::time::sleep(IDLE_POLL_FLOOR - elapsed).await;
                    }
                }
            }
            Err(e) => {
                let auth_failure = is_auth_failure(&e);
                consecutive_failures += 1;
                if auth_failure || consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                    eprintln!("error: {}", e.message);
                    return ExitCode::from(1);
                }
                // Cap at 8s: a drain loop must come back faster than callers' patience windows.
                let backoff_ms = 500u64.saturating_mul(1 << consecutive_failures.min(4));
                eprintln!(
                    "warn: listen step failed ({}); retrying in {}ms (attempt {}/{})",
                    e.message, backoff_ms, consecutive_failures, MAX_CONSECUTIVE_FAILURES
                );
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                // Re-subscribe: the durable subscription survives daemon-side, but refreshing
                // it also heals the cases where the failure WAS the subscription.
                if let Ok(fresh) = subscribe(client, a.timeout_ms, a.max).await {
                    subscription = fresh.subscription_id;
                }
            }
        }
    }
}

fn remaining_timeout_ms(deadline: std::time::Instant) -> Option<u32> {
    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
    if remaining.is_zero() {
        return Some(0);
    }
    let millis = remaining.as_millis().clamp(1, u128::from(u32::MAX));
    Some(millis as u32)
}

/// Establish the durable listener subscription with the same transient-failure resilience as the
/// steady-state receive loop. Subscription ids are deterministic per caller and subscribe is an
/// upsert, so retrying a command whose outcome is unknown cannot create a second delivery owner.
async fn subscribe_with_retry(
    client: &StoreClient,
    timeout_ms: Option<u32>,
    max: Option<u32>,
) -> Result<InboxSubscribeResponse, ContractError> {
    let mut failures = 0u32;
    loop {
        match subscribe(client, timeout_ms, max).await {
            Ok(subscription) => return Ok(subscription),
            Err(error) => {
                failures += 1;
                if is_auth_failure(&error) || failures >= MAX_CONSECUTIVE_FAILURES {
                    return Err(error);
                }
                let backoff_ms = 500u64.saturating_mul(1 << failures.min(4));
                eprintln!(
                    "warn: initial listen subscribe failed ({}); retrying in {}ms (attempt {}/{})",
                    error.message, backoff_ms, failures, MAX_CONSECUTIVE_FAILURES
                );
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
            }
        }
    }
}

fn is_auth_failure(error: &ContractError) -> bool {
    let message = error.message.to_lowercase();
    message.contains("unauthorized")
        || message.contains("authentication")
        || message.contains("key-rejected")
        || message.contains("key rejected")
}

async fn listen_legacy(client: &StoreClient, a: ListenArgs, json: bool) -> ExitCode {
    loop {
        let batch = match drain_once(client, a.timeout_ms, a.max).await {
            Ok(b) => b,
            Err(e) => {
                eprintln!("error: {}", e.message);
                return ExitCode::from(1);
            }
        };

        render_batch(&batch, json);

        if a.once {
            return ExitCode::SUCCESS;
        }
    }
}

/// One held-receive drain (`consume`).
pub async fn drain_once(
    client: &StoreClient,
    timeout_ms: Option<u32>,
    max: Option<u32>,
) -> Result<NexusBatch, ContractError> {
    client
        .command(
            nexus_store::command_kinds::inbox::CONSUME,
            &ConsumeRequest { timeout_ms, max },
        )
        .await
}

/// Register or refresh the caller's durable inbox subscription.
pub async fn subscribe(
    client: &StoreClient,
    timeout_ms: Option<u32>,
    max: Option<u32>,
) -> Result<InboxSubscribeResponse, ContractError> {
    client
        .command(
            nexus_store::command_kinds::inbox::SUBSCRIBE,
            &InboxSubscribeRequest { timeout_ms, max },
        )
        .await
}

/// Fetch one durable subscription batch, if ready.
pub async fn next_batch(
    client: &StoreClient,
    subscription_id: &str,
    timeout_ms: Option<u32>,
) -> Result<InboxSubscriptionNextResponse, ContractError> {
    client
        .command(
            nexus_store::command_kinds::inbox::SUBSCRIPTION_NEXT,
            &InboxSubscriptionNextRequest {
                subscription_id: subscription_id.to_string(),
                timeout_ms,
            },
        )
        .await
}

/// Release a durable inbox subscription after a bounded one-shot receive completes.
pub async fn unsubscribe(
    client: &StoreClient,
    subscription_id: &str,
) -> Result<InboxSubscriptionStatusResponse, ContractError> {
    client
        .command(
            nexus_store::command_kinds::inbox::UNSUBSCRIBE,
            &InboxUnsubscribeRequest {
                subscription_id: subscription_id.to_string(),
            },
        )
        .await
}

/// Split-ack a drained drop: per-message `ack` for DMs, one bulk `ackThreads` for threads.
///
/// Each store-backed `consume` command and split-ack command (`ack`/`ackThreads`) is authenticated,
/// so the daemon command worker refreshes `last_heartbeat`; a long-running listen loop therefore
/// keeps presence fresh without a separate heartbeat side channel.
pub async fn split_ack(client: &StoreClient, batch: &NexusBatch) -> Result<(), ContractError> {
    for id in &batch.dm_message_ids {
        let _: AckResponse = client
            .command(
                nexus_store::command_kinds::inbox::ACK,
                &AckRequest {
                    message_id: id.clone(),
                },
            )
            .await?;
    }
    if !batch.thread_message_ids.is_empty() {
        let _: AckResponse = client
            .command(
                nexus_store::command_kinds::inbox::ACK_THREADS,
                &AckThreadsRequest {
                    message_ids: batch.thread_message_ids.clone(),
                },
            )
            .await?;
    }
    Ok(())
}

/// Mark the durable subscription batch consumed after the normal message split-ack succeeds.
pub async fn ack_subscription_batch(
    client: &StoreClient,
    durable: &InboxSubscriptionBatch,
) -> Result<(), ContractError> {
    let _: InboxSubscriptionStatusResponse = client
        .command(
            nexus_store::command_kinds::inbox::SUBSCRIPTION_ACK,
            &InboxSubscriptionAckRequest {
                subscription_id: durable.subscription_id.clone(),
                batch_id: durable.batch_id.clone(),
            },
        )
        .await?;
    Ok(())
}

fn empty_batch() -> NexusBatch {
    NexusBatch {
        counts: BatchCounts {
            dms: 0,
            thread: 0,
            total: 0,
        },
        dms: Vec::new(),
        threads: Vec::new(),
        dm_message_ids: Vec::new(),
        thread_message_ids: Vec::new(),
        message_ids: Vec::new(),
    }
}

/// Render one drop: JSON (the whole `NexusBatch`) or a human summary + per-message lines.
fn render_batch(batch: &NexusBatch, json: bool) {
    if json {
        print_json(batch);
        return;
    }
    if batch.counts.total == 0 {
        print_human("(no messages)");
        return;
    }
    let mut s = format!(
        "dms={} thread={} total={}",
        batch.counts.dms, batch.counts.thread, batch.counts.total
    );
    for m in batch.dms.iter().chain(batch.threads.iter()) {
        let scope = m.thread.as_deref().or(m.topic.as_deref()).unwrap_or("dm");
        s.push_str(&format!(
            "\n[{}] {} ({}): {}",
            m.id.0, m.from, scope, m.body
        ));
    }
    print_human(&s);
}
