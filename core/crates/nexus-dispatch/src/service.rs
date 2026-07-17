//! The [`DispatchService`] service — `impl DispatchPort` (spec §2). It is the bus-facing delivery API:
//! `enqueue` (write the in-flight row, then ring per the [`WakePolicy`]), `consume` (drain-once
//! with a held-receive window), and the split-ack (`ack` per-message, `ack_threads` bulk).
//!
//! **Write-before-ring (crash-safety, §2.1).** `enqueue` commits the `pending` row *before*
//! ringing the bell, so a bell lost between commit and delivery is re-driven by the loop on its
//! next park (and re-rung on [`EventLoop::spawn`](crate::EventLoop::spawn)).

use async_trait::async_trait;

use nexus_contracts::ack::{AckRequest, AckResponse, AckThreadsRequest};
use nexus_contracts::batch::{ConsumeRequest, NexusBatch};
use nexus_contracts::ids::{MessageId, SessionId};
use nexus_contracts::ports::{Caller, DispatchPort, PortResult};
use nexus_store::repos::inbox::Inbox;

use crate::drain::InboxDrainer;
use crate::state::ServiceDeps;
use crate::wake_policy::{WakeDecision, WakePolicy};

/// Default held-receive window for [`consume`](DispatchService::consume) when the request omits one.
const DEFAULT_CONSUME_TIMEOUT_MS: u32 = 0;

/// The dispatch service over the durable store + the wake bell. Cheap to clone (all deps are
/// shared handles); wired into the daemon's `AppState` as `Arc<dyn DispatchPort>`.
#[derive(Clone)]
pub struct DispatchService {
    deps: ServiceDeps,
}

impl DispatchService {
    /// Build the service from its dependency bundle.
    pub fn new(deps: ServiceDeps) -> Self {
        DispatchService { deps }
    }

    /// The shared deps (used by the daemon to spawn per-agent loops with matching state).
    pub fn deps(&self) -> &ServiceDeps {
        &self.deps
    }
}

#[async_trait]
impl DispatchPort for DispatchService {
    /// Write a `pending` in-flight row for `recipient`, then ring its bell unless the wake policy
    /// holds (paused/offline). The row is always written first (crash-safety).
    async fn enqueue(&self, recipient: &SessionId, message: &MessageId) -> PortResult<()> {
        let inbox = Inbox::new(&self.deps.store);
        // 1. WRITE the durable delivery marker first.
        inbox
            .enqueue(message, recipient)
            .await
            .map_err(into_contract)?;

        // 2. RING per the wake policy. Hold (paused/offline) stays queued, re-driven on attach.
        let state = self.deps.registry.get(recipient);
        let decision = WakePolicy::should_wake(state, nexus_contracts::Kind::Agent);
        let rang = decision != WakeDecision::Hold;
        if rang {
            self.deps.bell.ring(recipient);
        }
        // The bus-side hop: in-flight row committed, then the bell rung (unless held). If a live run
        // shows this `rang=true` but the loop's "bell wake received" never fires for the same
        // recipient, the bus and the loop are ringing/waiting on different `Bell` instances.
        tracing::debug!(
            target: "nexus_dispatch::enqueue",
            recipient = %recipient,
            message = %message,
            ?state,
            rang,
            "in-flight written; bell rung per wake policy"
        );
        Ok(())
    }

    /// Drain-once with a held-receive window: drain now; if empty and a timeout was requested,
    /// park on the bell up to `timeout_ms` and drain once more.
    async fn consume(&self, caller: &Caller, req: ConsumeRequest) -> PortResult<NexusBatch> {
        let limit = req.max.unwrap_or(self.deps.drain_limit);
        let inbox = Inbox::new(&self.deps.store);

        let first = InboxDrainer::drain_once(
            &inbox,
            &caller.session,
            &caller.project,
            limit,
            self.deps.preview_chars,
        )
        .await
        .map_err(into_contract)?;
        if first.counts.total > 0 {
            return Ok(first);
        }

        let timeout_ms = req.timeout_ms.unwrap_or(DEFAULT_CONSUME_TIMEOUT_MS);
        if timeout_ms == 0 {
            return Ok(first);
        }

        // Held-receive: wait for a bell (bounded), then drain once more.
        let _ = tokio::time::timeout(
            std::time::Duration::from_millis(timeout_ms as u64),
            self.deps.bell.wait(&caller.session),
        )
        .await;

        InboxDrainer::drain_once(
            &inbox,
            &caller.session,
            &caller.project,
            limit,
            self.deps.preview_chars,
        )
        .await
        .map_err(into_contract)
    }

    /// Per-message ack (DM split-ack): move one in-flight row to `acked`.
    async fn ack(&self, caller: &Caller, req: AckRequest) -> PortResult<AckResponse> {
        let inbox = Inbox::new(&self.deps.store);
        let acked = inbox
            .ack(&req.message_id, &caller.session)
            .await
            .map_err(into_contract)?;
        Ok(AckResponse {
            acked: acked.min(u32::MAX as u64) as u32,
        })
    }

    /// Bulk ack (thread split-ack): move every listed in-flight row to `acked`.
    async fn ack_threads(
        &self,
        caller: &Caller,
        req: AckThreadsRequest,
    ) -> PortResult<AckResponse> {
        let inbox = Inbox::new(&self.deps.store);
        let acked = inbox
            .ack_many(&req.message_ids, &caller.session)
            .await
            .map_err(into_contract)?;
        Ok(AckResponse {
            acked: acked.min(u32::MAX as u64) as u32,
        })
    }
}

/// Map the workspace error to the wire-facing contract error at the port boundary.
fn into_contract(e: nexus_common::NexusError) -> nexus_contracts::ContractError {
    e.to_contract_error()
}
