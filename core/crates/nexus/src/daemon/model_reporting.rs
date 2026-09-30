// Private observation coordinator; boot invalidation, exact offline, and captured compatibility
// and runtime-only stale sweeps participate in shared lifecycle authority. Identity's offline
// operation shares tracked admission/stop but retains its own preparation and status semantics.
// Selected purge owns captured lanes and an agent-wide admission fence through its optional
// transport phase. Other lifecycle mutations must later participate; this
// does not authorize native bindings.
//
// Each reservation owns one tracked worker and one latest watch snapshot. Store ownership is
// serialized separately from synchronous native handoff. Shutdown never aborts uncertain writes.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use futures::FutureExt;
use nexus_agent::adapter::{AdapterTelemetryCapability, AdapterTelemetryReportingProfile};
use nexus_common::NexusError;
use nexus_contracts::model_report::{
    ModelEvidenceCapability, ModelEvidenceField, ModelEvidenceSlot, ModelEvidenceValue,
    ModelObservationSink, ModelReportBackend, ModelUnknownReason, NativeModelUpdate,
    RuntimeModelReport,
};
use nexus_contracts::telemetry::{
    AccountQuotaSlot, ContextSlot, NativeTelemetryUpdate, NativeTelemetryValue,
    RuntimeTelemetryReport, TelemetryAvailability, TelemetryMetadata, TokenUsageSlot,
};
use nexus_contracts::{ports::EventSink, AgentId, SessionId};
use nexus_identity::{
    IdentityOfflineOperation, NonAgentResumeOperation, PreparedIdentityOffline, RuntimeActivation,
    RuntimeActivationError, RuntimeActivationRequest,
};
use nexus_store::repos::agent_runtimes::{
    RuntimeActivationChanges, RuntimeActivationCommitState, SelectedRuntimeActivation,
    SelectedRuntimeResidueCleanup,
};
use nexus_store::repos::sessions::{
    CapturedStagedSession, IdentityPurgeCommitState, IdentityPurgeFailure, SelectedIdentityPurge,
    SelectedStagedSessionCleanup, SelectedTransportPurge,
};
use nexus_store::{
    repos::{
        agent_runtimes::{ExactRuntimeStop, ExpectedRuntimeBinding},
        AgentRuntimes, Sessions,
    },
    types::SessionRow,
    Store,
};
use tokio::sync::{oneshot, watch, Mutex as AsyncMutex, Notify, OwnedMutexGuard};

use crate::daemon::services::presence::{PresenceWriter, TransportRegistry, TransportSnapshot};

type Outcome = Result<bool, String>;

enum OfflineOperation {
    Presence {
        writer: PresenceWriter,
        session: SessionId,
    },
    Identity(IdentityOfflineOperation),
}

impl OfflineOperation {
    fn session_id(&self) -> &SessionId {
        match self {
            Self::Presence { session, .. } => session,
            Self::Identity(operation) => operation.session_id(),
        }
    }

    async fn prepare(self) -> Result<PreparedOffline, NexusError> {
        match self {
            Self::Presence { writer, session } => {
                let changed = writer.prepare_offline(&session).await?;
                Ok(PreparedOffline::Presence {
                    writer,
                    session,
                    changed,
                })
            }
            Self::Identity(operation) => Ok(PreparedOffline::Identity(operation.prepare().await?)),
        }
    }
}

enum PreparedOffline {
    Presence {
        writer: PresenceWriter,
        session: SessionId,
        changed: bool,
    },
    Identity(PreparedIdentityOffline),
}

impl PreparedOffline {
    async fn finish(self) -> Result<(), NexusError> {
        match self {
            Self::Presence {
                writer,
                session,
                changed,
            } => writer.finish_offline(&session, changed).await,
            Self::Identity(prepared) => {
                prepared.finish().await;
                Ok(())
            }
        }
    }
}

#[async_trait::async_trait]
impl RuntimeActivation for ModelReporting {
    async fn cleanup_non_agent_residue(
        &self,
        operation: NonAgentResumeOperation,
        transition: Arc<OwnedMutexGuard<()>>,
    ) -> Result<SelectedRuntimeResidueCleanup, NexusError> {
        let runtime = operation.session_id().clone();
        validate_id(&runtime.0)?;
        {
            let registry = self.inner.registry.lock().unwrap();
            if !registry.ready || registry.shutdown {
                return Err(invalid("model reporting is not admitting residue cleanup"));
            }
        }
        if !operation.matches_current_session().await? {
            return Ok(SelectedRuntimeResidueCleanup::BindingChanged);
        }
        // A failed lookup is not Missing. Retain this exact binding through all later awaits.
        let expected_agent = AgentRuntimes::new(&self.inner.store)
            .find_by_runtime_id(&runtime.0)
            .await?
            .map(|row| row.agent_id);
        if let Some(agent) = expected_agent.as_deref() {
            validate_id(agent)?;
        }
        let receiver = {
            let mut registry = self.inner.registry.lock().unwrap();
            if !registry.ready || registry.shutdown {
                return Err(invalid("model reporting is not admitting residue cleanup"));
            }
            let slot = registry.slots.get(&runtime).cloned().unwrap_or_default();
            if let Some(error) = slot.error.lock().unwrap().as_ref() {
                return Err(internal(error.clone()));
            }
            if *slot.offline_pending.lock().unwrap() {
                return Err(invalid("model observer lane is pending"));
            }
            let cell = slot.current.lock().unwrap().upgrade();
            require_residue_cell(&operation, expected_agent.as_deref(), cell.as_deref())?;
            if let Ok(lane) = slot.lane.try_lock() {
                if lane.uncertain || lane.confirmed.is_some() {
                    return Err(invalid("residue cleanup requires an unconfirmed lane"));
                }
            }
            registry
                .slots
                .entry(runtime)
                .or_insert_with(|| slot.clone());
            *slot.offline_pending.lock().unwrap() = true;
            registry.tasks += 1;
            let (sender, receiver) = oneshot::channel();
            let inner = self.inner.clone();
            tokio::spawn(async move {
                let _transition = transition;
                let result = inner
                    .run_residue_cleanup(operation, expected_agent, &slot, cell.as_deref())
                    .await;
                {
                    let mut registry = inner.registry.lock().unwrap();
                    *slot.offline_pending.lock().unwrap() = false;
                    drop(cell);
                    drop(slot);
                    registry.reclaim_settled_slots();
                    let _ = sender.send(result);
                    registry.tasks -= 1;
                }
                inner.settled.notify_waiters();
            });
            receiver
        };
        receiver
            .await
            .map_err(|_| internal("residue cleanup settlement channel closed without an outcome"))?
    }

    async fn set_identity_offline(
        &self,
        operation: IdentityOfflineOperation,
        transition: Arc<OwnedMutexGuard<()>>,
    ) -> Result<(), NexusError> {
        self.offline(OfflineOperation::Identity(operation), transition)
            .await
    }

    async fn cleanup_unbound_registration(
        &self,
        staged: CapturedStagedSession,
        transition: Arc<OwnedMutexGuard<()>>,
    ) -> Result<SelectedStagedSessionCleanup, NexusError> {
        let runtime = staged.row().session_id.clone();
        validate_id(&runtime.0)?;
        if let Some(agent) = &staged.row().agent_id {
            validate_id(agent)?;
        }
        let receiver = {
            let mut registry = self.inner.registry.lock().unwrap();
            if !registry.ready || registry.shutdown {
                return Err(invalid("model reporting is not admitting unbound cleanup"));
            }
            // Validate before insertion/pending/task mutation. A failed admission must not
            // replace the stable slot or discard a retained owner/failure through GC.
            let slot = registry.slots.get(&runtime).cloned().unwrap_or_default();
            if let Some(error) = slot.error.lock().unwrap().as_ref() {
                return Err(internal(error.clone()));
            }
            if *slot.offline_pending.lock().unwrap() {
                return Err(invalid("model observer lane is pending"));
            }
            let cell = slot.current.lock().unwrap().upgrade();
            if cell.as_ref().is_some_and(|cell| {
                cell.key.runtime_id != runtime
                    || staged.row().agent_id.as_deref() != Some(cell.key.agent_id.0.as_str())
            }) {
                return Err(invalid(
                    "unbound cleanup binding does not match model observer",
                ));
            }
            require_uncommitted_cell(cell.as_deref())?;
            // Closed alone is eligible; claim_requested alone is not committed authority.
            // Contention is not rejection: the owner rechecks under its actual lane later.
            if let Ok(lane) = slot.lane.try_lock() {
                if lane.uncertain || lane.confirmed.is_some() {
                    return Err(invalid("unbound cleanup requires an unconfirmed lane"));
                }
            }
            registry
                .slots
                .entry(runtime)
                .or_insert_with(|| slot.clone());
            *slot.offline_pending.lock().unwrap() = true;
            let (sender, receiver) = oneshot::channel();
            registry.tasks += 1;
            let inner = self.inner.clone();
            tokio::spawn(async move {
                // The real receipt, captured owner/slot and SAME-store caller guard survive
                // loss of the receiver. Provenance/disposition are not inferred from row().
                let _transition = transition;
                let result = inner
                    .run_unbound_cleanup(staged, &slot, cell.as_deref())
                    .await;
                {
                    let mut registry = inner.registry.lock().unwrap();
                    *slot.offline_pending.lock().unwrap() = false;
                    drop(cell);
                    drop(slot);
                    registry.reclaim_settled_slots();
                    let _ = sender.send(result);
                    registry.tasks -= 1;
                }
                inner.settled.notify_waiters();
            });
            receiver
        };
        receiver
            .await
            .map_err(|_| internal("unbound cleanup settlement channel closed without a receipt"))?
    }

    async fn activate_runtime(
        &self,
        request: RuntimeActivationRequest,
        transition: Arc<OwnedMutexGuard<()>>,
    ) -> Result<SelectedRuntimeActivation, RuntimeActivationError> {
        let rejected = |cause| RuntimeActivationError::RejectedBeforeStore { cause };
        let (target, agent) = match &request {
            RuntimeActivationRequest::CreateActive(runtime) => {
                if !runtime.active {
                    return Err(rejected(invalid(
                        "selected runtime creation requires active=true",
                    )));
                }
                (runtime.runtime_id.clone(), runtime.agent_id.clone())
            }
            RuntimeActivationRequest::ActivateExisting {
                runtime_id,
                agent_id,
            } => (runtime_id.0.clone(), agent_id.0.clone()),
        };
        validate_id(&target).map_err(rejected)?;
        validate_id(&agent).map_err(rejected)?;
        {
            let registry = self.inner.registry.lock().unwrap();
            if !registry.ready || registry.shutdown {
                return Err(rejected(invalid(
                    "model reporting is not admitting activation",
                )));
            }
        }
        // Presence is already owned by the caller, not reacquired here. These are selectors;
        // the identity transaction independently revalidates target and the COMPLETE sibling set.
        let selected_target = AgentRuntimes::new(&self.inner.store)
            .find_by_runtime_id(&target)
            .await
            .map_err(rejected)?;
        let target_matches = match &request {
            RuntimeActivationRequest::CreateActive(_) => selected_target.is_none(),
            RuntimeActivationRequest::ActivateExisting { .. } => selected_target
                .as_ref()
                .is_some_and(|row| row.agent_id == agent),
        };
        if !target_matches {
            return Err(rejected(invalid(
                "activation target does not match initial selection",
            )));
        }
        let siblings = AgentRuntimes::new(&self.inner.store)
            .active_sibling_runtime_pairs(&agent, &target)
            .await
            .map_err(rejected)?;
        let receiver = {
            let mut registry = self.inner.registry.lock().unwrap();
            if !registry.ready || registry.shutdown {
                return Err(rejected(invalid(
                    "model reporting is not admitting activation",
                )));
            }
            let mut pairs = siblings.clone();
            pairs.push((target.clone(), agent.clone()));
            pairs.sort();
            let mut captured = Vec::with_capacity(pairs.len());
            // No GC, entry insertion, pending flag or local closure until EVERY participant passes.
            for (runtime, agent) in pairs {
                validate_id(&runtime).map_err(rejected)?;
                validate_id(&agent).map_err(rejected)?;
                let runtime = SessionId(runtime);
                let agent = AgentId(agent);
                let slot = registry.slots.get(&runtime).cloned().unwrap_or_default();
                if let Some(cause) = slot.error.lock().unwrap().as_ref() {
                    return Err(rejected(internal(cause.clone())));
                }
                if *slot.offline_pending.lock().unwrap() {
                    return Err(rejected(invalid("model observer lane is pending")));
                }
                // Failure to acquire is ordinary contention, not proof of retained uncertainty.
                if slot.lane.try_lock().is_ok_and(|lane| lane.uncertain) {
                    return Err(rejected(invalid("model observer lane is uncertain")));
                }
                let cell = slot.current.lock().unwrap().upgrade();
                if cell.as_ref().is_some_and(|cell| {
                    cell.key.runtime_id != runtime || cell.key.agent_id != agent
                }) {
                    return Err(rejected(invalid(
                        "activation binding does not match model observer",
                    )));
                }
                // Matching normally closed predecessors are captured, never reopened.
                captured.push(CapturedRuntime {
                    runtime,
                    agent,
                    slot,
                    cell,
                });
            }
            for capture in &captured {
                registry
                    .slots
                    .entry(capture.runtime.clone())
                    .or_insert_with(|| capture.slot.clone());
                *capture.slot.offline_pending.lock().unwrap() = true;
            }
            let (sender, receiver) = oneshot::channel();
            registry.tasks += 1;
            let inner = self.inner.clone();
            tokio::spawn(async move {
                let _transition = transition;
                let result = inner
                    .run_activation(request, &target, &agent, &siblings, &captured)
                    .await;
                {
                    let mut registry = inner.registry.lock().unwrap();
                    // Only our own admission is cleared; retained error/uncertainty still blocks
                    // failed lanes. Receiver ownership is irrelevant to settlement and reclamation.
                    for capture in &captured {
                        *capture.slot.offline_pending.lock().unwrap() = false;
                    }
                    drop(captured);
                    registry.reclaim_settled_slots();
                    let _ = sender.send(result);
                    registry.tasks -= 1;
                }
                inner.settled.notify_waiters();
            });
            receiver
        };
        receiver
            .await
            .map_err(|_| RuntimeActivationError::OutcomeUnavailable {
                cause: internal("activation settlement channel closed without a receipt"),
            })?
    }
}

pub(crate) struct ModelCapabilityProfile {
    pub configured: ModelEvidenceCapability,
    pub turn_selected: ModelEvidenceCapability,
    pub response_reported: ModelEvidenceCapability,
}

pub(crate) struct ModelObserverKey {
    pub agent_id: AgentId,
    pub runtime_id: SessionId,
    pub token: String,
}

pub(crate) struct ModelReporting {
    inner: Arc<Inner>,
}

#[derive(Debug)]
pub(crate) enum ModelPurgeError {
    BeforeStore {
        cause: NexusError,
    },
    Store {
        failure: IdentityPurgeFailure,
        settlement_error: Option<NexusError>,
    },
    AfterStore {
        receipt: SelectedIdentityPurge,
        cause: NexusError,
    },
    OutcomeUnavailable {
        cause: NexusError,
    },
}

struct Inner {
    store: Arc<Store>,
    events: Arc<dyn EventSink>,
    registry: Mutex<Registry>,
    settled: Notify,
}

#[derive(Default)]
struct Registry {
    ready: bool,
    shutdown: bool,
    initializing: Option<watch::Receiver<Option<Outcome>>>,
    initialization_error: Option<String>,
    tasks: usize,
    slots: HashMap<SessionId, Arc<RuntimeSlot>>,
    // Registry-serialized admission, independently owned until tracked purge settlement. A
    // retained failure is deliberately not reclaimed merely because it has no runtime rows.
    purging_agents: HashMap<AgentId, Arc<PurgeFence>>,
}

#[derive(Default)]
struct PurgeFence {
    error: Mutex<Option<String>>,
}

struct PurgeParticipant {
    runtime: SessionId,
    slot: Arc<RuntimeSlot>,
    cell: Option<Arc<OwnerCell>>,
}

#[derive(Clone, Copy)]
enum PurgeMode {
    IdentityOnly,
    Full,
}

#[derive(Default)]
struct RuntimeSlot {
    lane: AsyncMutex<Lane>,
    current: Mutex<Weak<OwnerCell>>,
    // Admission and completion both hold the registry lock, just like reserve and activate.
    offline_pending: Mutex<bool>,
    // Separate from the async lane so panic handling can retain failure without awaiting a lock.
    error: Mutex<Option<String>>,
    // Read/write only while error is locked: the FIRST failure chooses closure scope. Captured
    // compatibility settlement must not let secondary OLD cleanup resolve a foreign current cell.
    captured_failure_closure: AtomicBool,
}

#[derive(Default)]
struct Lane {
    confirmed: Option<String>,
    // Set before each store await and cleared only by a confirmed result. Dropping a future or
    // unwinding never converts a possibly committed write back into a known NULL predecessor.
    uncertain: bool,
}

struct CapturedRuntime {
    runtime: SessionId,
    agent: AgentId,
    slot: Arc<RuntimeSlot>,
    cell: Option<Arc<OwnerCell>>,
}

impl RuntimeSlot {
    /// Preserve the FIRST failure's cause and closure policy under the same error lock. Later
    /// worker failure may close its owned cell, but must not resolve a new/foreign current owner.
    fn retain_captured_failure(&self, cell: Option<&OwnerCell>, error: &str) {
        let mut failure = self.error.lock().unwrap();
        if failure.is_none() {
            *failure = Some(error.to_owned());
            self.captured_failure_closure.store(true, Ordering::Relaxed);
        }
        if let Some(cell) = cell {
            cell.close();
        }
    }
}

impl CapturedRuntime {
    // This is safe under a lane: never consult registry/current or close a later owner.
    fn retain_failure(&self, error: &str) {
        self.slot
            .retain_captured_failure(self.cell.as_deref(), error);
    }
}

impl Registry {
    fn reclaim_settled_slots(&mut self) {
        // Only the registry may own a reclaimable slot. Captured handles, workers, and in-flight
        // offline tasks preserve its stable identity; failures preserve it even without owners.
        self.slots.retain(|_, slot| {
            Arc::strong_count(slot) > 1
                || *slot.offline_pending.lock().unwrap()
                || slot.error.lock().unwrap().is_some()
                || slot
                    .lane
                    .try_lock()
                    .map_or(true, |lane| lane.uncertain || lane.confirmed.is_some())
        });
    }
}

struct OwnerCell {
    key: ModelObserverKey,
    slot: Arc<RuntimeSlot>,
    coordinator: Weak<Inner>,
    // Captured synchronously at reservation, weak to prevent an unbounded ancestry chain.
    _predecessor: Weak<OwnerCell>,
    initial: RuntimeModelReport,
    telemetry_profile: Option<AdapterTelemetryReportingProfile>,
    profile_identity: Option<nexus_contracts::model_report::ModelProfileIdentity>,
    state: Mutex<CellState>,
    latest: watch::Sender<Snapshot>,
    claimed: watch::Sender<Option<Outcome>>,
    completed: watch::Sender<Option<Outcome>>,
}

struct CellState {
    closed: bool,
    claim_requested: bool,
    committed: bool,
    activated: bool,
    native_root: Option<String>,
    sequence: i64,
    report: RuntimeModelReport,
}

#[derive(Clone)]
struct Snapshot {
    closed: bool,
    claim_requested: bool,
    activated: bool,
    sequence: i64,
    report: RuntimeModelReport,
}

/// External ownership is deliberately not Clone. Sharing this handle through Arc is supported;
/// internal workers hold only the cell, so they cannot keep an abandoned claimant alive.
pub(crate) struct ModelObserverHandle {
    cell: Arc<OwnerCell>,
}

impl Drop for ModelObserverHandle {
    fn drop(&mut self) {
        self.cell.close();
    }
}

impl CellState {
    fn snapshot(&self) -> Snapshot {
        Snapshot {
            closed: self.closed,
            claim_requested: self.claim_requested,
            activated: self.activated,
            sequence: self.sequence,
            report: self.report.clone(),
        }
    }
}

impl OwnerCell {
    fn close(&self) {
        let mut state = self.state.lock().unwrap();
        if !state.closed {
            state.closed = true;
            self.latest.send_replace(state.snapshot());
        }
    }

    fn current(&self) -> bool {
        self.slot
            .current
            .lock()
            .unwrap()
            .upgrade()
            .is_some_and(|cell| {
                cell.key.token == self.key.token && !cell.state.lock().unwrap().closed
            })
    }
}

impl ModelObservationSink for ModelObserverHandle {
    fn accepts_profile(
        &self,
        profile: &nexus_contracts::model_report::ModelProfileIdentity,
    ) -> bool {
        let state = self.cell.state.lock().unwrap();
        !state.closed
            && self
                .cell
                .profile_identity
                .as_ref()
                .is_some_and(|captured| captured.matches(profile))
    }

    fn bind_native_root(&self, root: &str) -> bool {
        let mut state = self.cell.state.lock().unwrap();
        if state.closed || root.trim().is_empty() {
            return false;
        }
        match &state.native_root {
            Some(bound) => bound == root,
            None => {
                state.native_root = Some(root.into());
                true
            }
        }
    }

    fn observe(&self, update: NativeModelUpdate) -> bool {
        let mut state = self.cell.state.lock().unwrap();
        if state.closed || state.native_root.as_deref() != Some(&update.native_session_id) {
            return false;
        }
        let slot = match update.field {
            ModelEvidenceField::Configured => &mut state.report.configured,
            ModelEvidenceField::TurnSelected => &mut state.report.turn_selected,
            ModelEvidenceField::ResponseReported => &mut state.report.response_reported,
        };
        let capability = match slot {
            ModelEvidenceSlot::Observed { capability, .. }
            | ModelEvidenceSlot::Unknown { capability, .. }
            | ModelEvidenceSlot::Invalid { capability, .. } => *capability,
        };
        let replacement = match update.value {
            ModelEvidenceValue::Observed(observation) => {
                if capability != ModelEvidenceCapability::Supported
                    || observation.validate().is_err()
                    || observation
                        .native_session_id
                        .as_deref()
                        .is_some_and(|id| id != update.native_session_id)
                {
                    return false;
                }
                ModelEvidenceSlot::Observed {
                    capability,
                    observation,
                }
            }
            ModelEvidenceValue::Unknown(reason) => ModelEvidenceSlot::Unknown {
                capability,
                reason: Some(reason),
            },
            ModelEvidenceValue::Invalid(reason) => {
                ModelEvidenceSlot::Invalid { capability, reason }
            }
        };
        // Exhaustion closes the ticket instead of wrapping private ordering authority.
        let Some(sequence) = state.sequence.checked_add(1) else {
            state.closed = true;
            self.cell.latest.send_replace(state.snapshot());
            return false;
        };
        match update.field {
            ModelEvidenceField::Configured => state.report.configured = replacement,
            ModelEvidenceField::TurnSelected => state.report.turn_selected = replacement,
            ModelEvidenceField::ResponseReported => state.report.response_reported = replacement,
        }
        state.sequence = sequence;
        self.cell.latest.send_replace(state.snapshot());
        true
    }

    fn observe_telemetry(&self, update: NativeTelemetryUpdate) -> bool {
        let mut state = self.cell.state.lock().unwrap();
        if state.closed || state.native_root.as_deref() != Some(update.native_session_id()) {
            return false;
        }
        let Some(profile) = &self.cell.telemetry_profile else {
            return false;
        };
        let Some(current) = state.report.telemetry.as_deref() else {
            return false;
        };
        let mut replacement = current.clone();
        match update {
            NativeTelemetryUpdate::Usage {
                native_session_id,
                value,
            } => {
                let Some((status, observation)) =
                    captured_telemetry_value(value, profile.usage(), &native_session_id, |value| {
                        &value.metadata
                    })
                else {
                    return false;
                };
                replacement.usage = TokenUsageSlot {
                    status,
                    capability: profile.usage().capability(),
                    observation,
                };
            }
            NativeTelemetryUpdate::Context {
                native_session_id,
                value,
            } => {
                let Some((status, observation)) = captured_telemetry_value(
                    value,
                    profile.context(),
                    &native_session_id,
                    |value| &value.metadata,
                ) else {
                    return false;
                };
                replacement.context = ContextSlot {
                    status,
                    capability: profile.context().capability(),
                    observation,
                };
            }
            NativeTelemetryUpdate::Quota {
                native_session_id,
                value,
            } => {
                let Some((status, observation)) =
                    captured_telemetry_value(value, profile.quota(), &native_session_id, |value| {
                        &value.metadata
                    })
                else {
                    return false;
                };
                replacement.quota = AccountQuotaSlot {
                    status,
                    capability: profile.quota().capability(),
                    observation,
                };
            }
        }
        if replacement.validate().is_err() {
            return false;
        }
        let Some(sequence) = state.sequence.checked_add(1) else {
            state.closed = true;
            self.cell.latest.send_replace(state.snapshot());
            return false;
        };
        state.report.telemetry = Some(Box::new(replacement));
        state.sequence = sequence;
        self.cell.latest.send_replace(state.snapshot());
        true
    }

    fn revoke(&self) {
        self.cell.close();
    }
}

// Validate captured provenance before the caller merges/validates the typed category payload.
// No lookup, storage, capability promotion, or counter arithmetic occurs on the native read path.
fn captured_telemetry_value<T>(
    value: NativeTelemetryValue<T>,
    capability: &AdapterTelemetryCapability,
    root: &str,
    metadata: fn(&T) -> &TelemetryMetadata,
) -> Option<(TelemetryAvailability, Option<T>)> {
    match value {
        NativeTelemetryValue::Observed(value) => {
            let observed = metadata(&value);
            if capability.capability() != ModelEvidenceCapability::Supported
                || capability.source() != Some(&observed.source)
                || observed.native_session_id.as_str() != root
            {
                return None;
            }
            Some((TelemetryAvailability::Observed, Some(value)))
        }
        NativeTelemetryValue::Unknown => Some((TelemetryAvailability::Unknown, None)),
        NativeTelemetryValue::Invalid => Some((TelemetryAvailability::Invalid, None)),
    }
}

fn initial_telemetry(profile: &AdapterTelemetryReportingProfile) -> RuntimeTelemetryReport {
    RuntimeTelemetryReport {
        usage: TokenUsageSlot {
            status: TelemetryAvailability::Unknown,
            capability: profile.usage().capability(),
            observation: None,
        },
        context: ContextSlot {
            status: TelemetryAvailability::Unknown,
            capability: profile.context().capability(),
            observation: None,
        },
        quota: AccountQuotaSlot {
            status: TelemetryAvailability::Unknown,
            capability: profile.quota().capability(),
            observation: None,
        },
    }
}

impl ModelReporting {
    pub(crate) async fn purge(
        &self,
        selected: SessionRow,
        agent: Option<String>,
        transition: Arc<OwnedMutexGuard<()>>,
    ) -> Result<SelectedIdentityPurge, ModelPurgeError> {
        self.purge_mode(selected, agent, transition, PurgeMode::Full)
            .await
    }

    pub(crate) async fn purge_identity(
        &self,
        selected: SessionRow,
        agent: Option<String>,
        transition: Arc<OwnedMutexGuard<()>>,
    ) -> Result<SelectedIdentityPurge, ModelPurgeError> {
        self.purge_mode(selected, agent, transition, PurgeMode::IdentityOnly)
            .await
    }

    async fn purge_mode(
        &self,
        selected: SessionRow,
        agent: Option<String>,
        transition: Arc<OwnedMutexGuard<()>>,
        mode: PurgeMode,
    ) -> Result<SelectedIdentityPurge, ModelPurgeError> {
        // Same-Store guard and authorized Session/agent association are caller preconditions.
        // Full mode keeps admission ownership through the independently committed transport tail.
        let rejected = |cause| ModelPurgeError::BeforeStore { cause };
        validate_id(&selected.session_id.0).map_err(rejected)?;
        if let Some(agent) = &agent {
            validate_id(agent).map_err(rejected)?;
        }
        {
            let registry = self.inner.registry.lock().unwrap();
            if !registry.ready || registry.shutdown {
                return Err(rejected(invalid("model reporting is not admitting purge")));
            }
        }
        let sessions = Sessions::new(&self.inner.store);
        let pairs = sessions
            .runtime_pairs_for_purge(&selected.session_id, agent.as_deref())
            .await
            .map_err(rejected)?;
        let receiver = {
            let mut registry = self.inner.registry.lock().unwrap();
            if !registry.ready || registry.shutdown {
                return Err(rejected(invalid("model reporting is not admitting purge")));
            }
            if agent.as_ref().is_some_and(|agent| {
                registry
                    .purging_agents
                    .contains_key(&AgentId(agent.clone()))
            }) {
                return Err(rejected(invalid(
                    "identity purge is already pending or failed",
                )));
            }
            let mut participants: BTreeMap<String, Option<String>> = pairs
                .iter()
                .map(|(runtime, agent)| (runtime.clone(), Some(agent.clone())))
                .collect();
            participants
                .entry(selected.session_id.0.clone())
                .or_insert_with(|| agent.clone());
            // Include unclaimed local origins even when they have no durable runtime yet.
            for (runtime, slot) in &registry.slots {
                if let Some(cell) = slot.current.lock().unwrap().upgrade() {
                    if agent.as_deref() == Some(cell.key.agent_id.0.as_str()) {
                        participants
                            .entry(runtime.0.clone())
                            .or_insert_with(|| Some(cell.key.agent_id.0.clone()));
                    }
                }
            }
            let mut captured = Vec::with_capacity(participants.len());
            // ALL checks precede slot insertion, pending flags and agent fence installation.
            for (runtime, expected_agent) in participants {
                validate_id(&runtime).map_err(rejected)?;
                if let Some(agent) = &expected_agent {
                    validate_id(agent).map_err(rejected)?;
                }
                let runtime = SessionId(runtime);
                let slot = registry.slots.get(&runtime).cloned().unwrap_or_default();
                if let Some(error) = slot.error.lock().unwrap().as_ref() {
                    return Err(rejected(internal(error.clone())));
                }
                if *slot.offline_pending.lock().unwrap()
                    || slot.lane.try_lock().is_ok_and(|lane| lane.uncertain)
                {
                    return Err(rejected(invalid(
                        "purge participant is pending or uncertain",
                    )));
                }
                let cell = slot.current.lock().unwrap().upgrade();
                if cell.as_ref().is_some_and(|cell| {
                    cell.key.runtime_id != runtime
                        || expected_agent.as_deref() != Some(cell.key.agent_id.0.as_str())
                }) {
                    return Err(rejected(invalid(
                        "purge binding does not match model observer",
                    )));
                }
                captured.push(PurgeParticipant {
                    runtime,
                    slot,
                    cell,
                });
            }
            let fence = Arc::new(PurgeFence::default());
            if let Some(agent) = &agent {
                registry
                    .purging_agents
                    .insert(AgentId(agent.clone()), fence.clone());
            }
            for capture in &captured {
                registry
                    .slots
                    .entry(capture.runtime.clone())
                    .or_insert_with(|| capture.slot.clone());
                *capture.slot.offline_pending.lock().unwrap() = true;
            }
            registry.tasks += 1;
            let (sender, receiver) = oneshot::channel();
            let inner = self.inner.clone();
            tokio::spawn(async move {
                let _transition = transition;
                let result = inner
                    .run_identity_purge(&selected, agent.as_deref(), &pairs, &captured, &fence)
                    .await;
                let result = match (mode, result) {
                    (PurgeMode::Full, Ok(receipt @ SelectedIdentityPurge::Purged(_))) => {
                        inner
                            .run_transport_purge(&selected, receipt, &captured, &fence)
                            .await
                    }
                    (_, result) => result,
                };
                {
                    let mut registry = inner.registry.lock().unwrap();
                    for capture in &captured {
                        *capture.slot.offline_pending.lock().unwrap() = false;
                    }
                    if fence.error.lock().unwrap().is_none() {
                        if let Some(agent) = &agent {
                            let key = AgentId(agent.clone());
                            if registry
                                .purging_agents
                                .get(&key)
                                .is_some_and(|current| Arc::ptr_eq(current, &fence))
                            {
                                registry.purging_agents.remove(&key);
                            }
                        }
                    }
                    drop(captured);
                    registry.reclaim_settled_slots();
                    let _ = sender.send(result);
                    registry.tasks -= 1;
                }
                inner.settled.notify_waiters();
            });
            receiver
        };
        receiver
            .await
            .map_err(|_| ModelPurgeError::OutcomeUnavailable {
                cause: internal("identity purge settlement channel closed without an outcome"),
            })?
    }

    pub(crate) fn new(store: Arc<Store>, events: Arc<dyn EventSink>) -> Self {
        Self {
            inner: Arc::new(Inner {
                store,
                events,
                registry: Mutex::new(Registry::default()),
                settled: Notify::new(),
            }),
        }
    }

    pub(crate) async fn initialize(&self) -> Result<(), NexusError> {
        let receiver = {
            let mut registry = self.inner.registry.lock().unwrap();
            if registry.shutdown {
                return Err(invalid("model reporting is shut down"));
            }
            if registry.ready {
                return Ok(());
            }
            if let Some(receiver) = &registry.initializing {
                receiver.clone()
            } else {
                let (sender, receiver) = watch::channel(None);
                registry.initializing = Some(receiver.clone());
                registry.tasks += 1;
                let inner = self.inner.clone();
                tokio::spawn(async move {
                    let result = guarded(inner.initialize_boot()).await;
                    {
                        let mut registry = inner.registry.lock().unwrap();
                        registry.ready = result.is_ok() && !registry.shutdown;
                        registry.initialization_error = result.as_ref().err().cloned();
                        registry.initializing = None;
                        sender.send_replace(Some(result));
                        registry.tasks -= 1;
                    }
                    inner.settled.notify_waiters();
                });
                receiver
            }
        };
        wait_outcome(receiver).await.map(|_| ())
    }

    pub(crate) fn reserve(
        &self,
        agent: AgentId,
        runtime: SessionId,
        backend: ModelReportBackend,
        capabilities: ModelCapabilityProfile,
    ) -> Result<ModelObserverHandle, NexusError> {
        self.reserve_with_telemetry(agent, runtime, backend, capabilities, None)
    }

    pub(crate) fn reserve_adapter(
        &self,
        agent: AgentId,
        runtime: SessionId,
        profile: &nexus_agent::adapter::AdapterModelReportingProfile,
    ) -> Result<ModelObserverHandle, NexusError> {
        self.reserve_profile(
            agent,
            runtime,
            profile.backend().clone(),
            ModelCapabilityProfile {
                configured: profile.configured(),
                turn_selected: profile.turn_selected(),
                response_reported: profile.response_reported(),
            },
            profile.telemetry().cloned(),
            Some(profile.identity().clone()),
        )
    }

    pub(crate) fn reserve_with_telemetry(
        &self,
        agent: AgentId,
        runtime: SessionId,
        backend: ModelReportBackend,
        capabilities: ModelCapabilityProfile,
        telemetry: Option<AdapterTelemetryReportingProfile>,
    ) -> Result<ModelObserverHandle, NexusError> {
        self.reserve_profile(agent, runtime, backend, capabilities, telemetry, None)
    }

    pub(crate) fn reserve_native(
        &self,
        agent: AgentId,
        runtime: SessionId,
        profile: &nexus_agent::adapter::NativeModelReportingProfile,
    ) -> Result<ModelObserverHandle, NexusError> {
        self.reserve_profile(
            agent,
            runtime,
            profile.backend().clone(),
            ModelCapabilityProfile {
                configured: profile.configured(),
                turn_selected: profile.turn_selected(),
                response_reported: profile.response_reported(),
            },
            profile.telemetry().cloned(),
            Some(profile.identity().clone()),
        )
    }

    fn reserve_profile(
        &self,
        agent: AgentId,
        runtime: SessionId,
        backend: ModelReportBackend,
        capabilities: ModelCapabilityProfile,
        telemetry: Option<AdapterTelemetryReportingProfile>,
        profile_identity: Option<nexus_contracts::model_report::ModelProfileIdentity>,
    ) -> Result<ModelObserverHandle, NexusError> {
        validate_id(&agent.0)?;
        validate_id(&runtime.0)?;
        if backend.is_unknown() || backend.validate().is_err() {
            return Err(invalid("invalid model backend"));
        }
        let executor = tokio::runtime::Handle::try_current()
            .map_err(|_| invalid("model reporting requires an async executor"))?;
        let mut registry = self.inner.registry.lock().unwrap();
        if !registry.ready || registry.shutdown {
            return Err(invalid("model reporting is not admitting observers"));
        }
        if registry.purging_agents.contains_key(&agent) {
            return Err(invalid(
                "model observer agent has a pending or failed identity purge",
            ));
        }
        registry.reclaim_settled_slots();
        let slot = registry.slots.entry(runtime.clone()).or_default().clone();
        if slot.error.lock().unwrap().is_some() || *slot.offline_pending.lock().unwrap() {
            return Err(invalid("model observer lane is closed"));
        }
        let mut current = slot.current.lock().unwrap();
        let predecessor = current.clone();
        if let Some(old) = predecessor.upgrade() {
            old.close();
        }
        let unknown = |capability| ModelEvidenceSlot::Unknown {
            capability,
            reason: Some(ModelUnknownReason::AwaitingNativeMetadata),
        };
        let initial = RuntimeModelReport {
            backend,
            observer_active: false,
            report_revision: 1,
            configured: unknown(capabilities.configured),
            turn_selected: unknown(capabilities.turn_selected),
            response_reported: unknown(capabilities.response_reported),
            telemetry: telemetry
                .as_ref()
                .map(|profile| Box::new(initial_telemetry(profile))),
        };
        let state = CellState {
            closed: false,
            claim_requested: false,
            committed: false,
            activated: false,
            native_root: None,
            sequence: 0,
            report: initial.clone(),
        };
        let (latest, receiver) = watch::channel(state.snapshot());
        let cell = Arc::new(OwnerCell {
            key: ModelObserverKey {
                agent_id: agent,
                runtime_id: runtime,
                token: uuid::Uuid::new_v4().to_string(),
            },
            slot: slot.clone(),
            coordinator: Arc::downgrade(&self.inner),
            _predecessor: predecessor,
            initial,
            telemetry_profile: telemetry,
            profile_identity,
            state: Mutex::new(state),
            latest,
            claimed: watch::channel(None).0,
            completed: watch::channel(None).0,
        });
        *current = Arc::downgrade(&cell);
        drop(current);
        registry.tasks += 1;
        let inner = self.inner.clone();
        let owned = cell.clone();
        executor.spawn(async move {
            let result = guarded(inner.run_owner(&owned, receiver)).await;
            if let Err(error) = &result {
                inner.fail_owner(&owned, error);
            }
            if owned.claimed.borrow().is_none() {
                owned
                    .claimed
                    .send_replace(Some(result.clone().map(|_| false)));
            }
            owned.completed.send_replace(Some(result));
            inner.registry.lock().unwrap().tasks -= 1;
            inner.settled.notify_waiters();
        });
        Ok(ModelObserverHandle { cell })
    }

    pub(crate) async fn commit_claim(
        &self,
        handle: &ModelObserverHandle,
    ) -> Result<bool, NexusError> {
        self.check_handle(handle)?;
        let mut cancellation = ClaimWait {
            cell: &handle.cell,
            completed: false,
        };
        {
            let failure = handle.cell.slot.error.lock().unwrap();
            if let Some(error) = failure.as_ref() {
                cancellation.completed = true;
                return Err(internal(error.clone()));
            }
            let mut state = handle.cell.state.lock().unwrap();
            if state.closed {
                cancellation.completed = true;
                return Ok(false);
            }
            state.claim_requested = true;
            handle.cell.latest.send_replace(state.snapshot());
        }
        let result = wait_outcome(handle.cell.claimed.subscribe()).await;
        cancellation.completed = true;
        result
    }

    pub(crate) fn activate(&self, handle: &ModelObserverHandle, root: &str) -> bool {
        if self.check_handle(handle).is_err() {
            return false;
        }
        let registry = self.inner.registry.lock().unwrap();
        if registry.shutdown || !registry.ready || !handle.cell.current() {
            return false;
        }
        let mut state = handle.cell.state.lock().unwrap();
        if state.closed || !state.committed || state.native_root.as_deref() != Some(root) {
            return false;
        }
        if state.activated {
            return true;
        }
        let Some(sequence) = state.sequence.checked_add(1) else {
            return false;
        };
        state.activated = true;
        state.sequence = sequence;
        state.report.observer_active = true;
        handle.cell.latest.send_replace(state.snapshot());
        true
    }

    pub(crate) async fn revoke_committed(
        &self,
        handle: &ModelObserverHandle,
    ) -> Result<bool, NexusError> {
        self.check_handle(handle)?;
        handle.cell.close();
        wait_outcome(handle.cell.completed.subscribe()).await
    }

    /// Admit one exact-runtime offline transition. The task, not its caller, owns the presence
    /// guard through compatibility writes, lane-serialized stop, and post-commit status emission.
    /// Cancellation before admission writes nothing; cancellation afterward only abandons waiting.
    pub(crate) async fn materialize_offline(
        &self,
        writer: PresenceWriter,
        session: SessionId,
        transition: OwnedMutexGuard<()>,
    ) -> Result<(), NexusError> {
        self.offline(
            OfflineOperation::Presence { writer, session },
            Arc::new(transition),
        )
        .await
    }

    // Same-store operation/guard provenance is supplied by internal wiring, not certified here.
    // Capture the target before consuming preparation; never resolve a later current owner.
    async fn offline(
        &self,
        operation: OfflineOperation,
        transition: Arc<OwnedMutexGuard<()>>,
    ) -> Result<(), NexusError> {
        let session = operation.session_id().clone();
        validate_id(&session.0)?;
        let receiver = {
            let mut registry = self.inner.registry.lock().unwrap();
            if !registry.ready || registry.shutdown {
                return Err(invalid(
                    "model reporting is not admitting offline transitions",
                ));
            }
            let slot = registry.slots.entry(session.clone()).or_default().clone();
            if slot.error.lock().unwrap().is_some() || *slot.offline_pending.lock().unwrap() {
                return Err(invalid("model observer lane is closed"));
            }
            *slot.offline_pending.lock().unwrap() = true;
            if let Some(current) = slot.current.lock().unwrap().upgrade() {
                current.close();
            }
            let (sender, receiver) = watch::channel(None);
            registry.tasks += 1;
            let inner = self.inner.clone();
            tokio::spawn(async move {
                let _transition = transition;
                let result = guarded(async {
                    let prepared = operation.prepare().await?;
                    let projection_agent = {
                        let mut lane = slot.lane.lock().await;
                        if lane.uncertain || slot.error.lock().unwrap().is_some() {
                            return Err(internal(
                                "model observer lane is closed after uncertain settlement",
                            ));
                        }
                        lane.uncertain = true;
                        let mut captured_agent = None;
                        let stopped = guarded(async {
                            let repo = AgentRuntimes::new(&inner.store);
                            captured_agent = repo
                                .find_by_runtime_id(&session.0)
                                .await?
                                .map(|row| AgentId(row.agent_id));
                            repo.stop(&session.0).await?;
                            Ok(true)
                        })
                        .await;
                        if let Err(error) = stopped {
                            // Retain the originating store error (or unwind) before releasing the
                            // lane. Queued OLD cleanup can then fail only after this cause exists.
                            // Do not acquire registry here: lock order never reverses to lane ->
                            // registry, and outer task completion still handles preparation/status.
                            let mut failure = slot.error.lock().unwrap();
                            if failure.is_none() {
                                *failure = Some(error.clone());
                            }
                            return Err(internal(error));
                        }
                        // stop atomically invalidated durable authority. Only its confirmed success
                        // establishes the NULL predecessor; an error or unwind must not infer it.
                        lane.uncertain = false;
                        lane.confirmed = None;
                        captured_agent
                    };
                    // Native close may already have published an observer-inactive but still
                    // online runtime. Publish confirmed durable stop through the same canonical
                    // exact-agent builder, outside the lane and inside owned guard settlement.
                    if let Some(agent) = projection_agent {
                        inner.events.project_runtime_binding(&session, &agent).await;
                    }
                    prepared.finish().await?;
                    Ok(true)
                })
                .await;
                {
                    let mut registry = inner.registry.lock().unwrap();
                    if let Err(error) = &result {
                        let mut failure = slot.error.lock().unwrap();
                        if failure.is_none() {
                            *failure = Some(error.clone());
                        }
                    } else {
                        *slot.offline_pending.lock().unwrap() = false;
                    }
                    // Release this task's ownership before testing strong counts. In production
                    // offline-only traffic has no later reserve call to perform this collection.
                    drop(slot);
                    registry.reclaim_settled_slots();
                    sender.send_replace(Some(result));
                    registry.tasks -= 1;
                }
                inner.settled.notify_waiters();
            });
            receiver
        };
        wait_outcome(receiver).await.map(|_| ())
    }

    /// Compatibility stale settlement owns its captured cell and guard through caller cancellation.
    /// The stores are independent: after session CAS starts, failure is possibly partial, never a
    /// rollback/no-effect promise. Only this captured OLD cell may be failed closed.
    pub(crate) async fn stop_stale_session(
        &self,
        writer: PresenceWriter,
        selected: SessionRow,
        expected_runtime: Option<String>,
        snapshot: TransportSnapshot,
        now_ms: i64,
        ttl_ms: i64,
        transition: OwnedMutexGuard<()>,
    ) -> Result<(), NexusError> {
        validate_id(&selected.session_id.0)?;
        if let Some(agent) = &expected_runtime {
            validate_id(agent)?;
        }
        let receiver = {
            let mut registry = self.inner.registry.lock().unwrap();
            if !registry.ready || registry.shutdown {
                return Err(invalid(
                    "model reporting is not admitting stale transitions",
                ));
            }
            let slot = registry
                .slots
                .get(&selected.session_id)
                .cloned()
                .unwrap_or_default();
            if slot.error.lock().unwrap().is_some() || *slot.offline_pending.lock().unwrap() {
                return Err(invalid("model observer lane is closed"));
            }
            if slot.lane.try_lock().is_ok_and(|lane| lane.uncertain) {
                return Err(invalid("model observer lane is uncertain"));
            }
            let cell = slot.current.lock().unwrap().upgrade();
            if cell.as_ref().is_some_and(|cell| {
                cell.key.runtime_id != selected.session_id
                    || expected_runtime.as_deref() != Some(cell.key.agent_id.0.as_str())
                    || selected
                        .agent_id
                        .as_deref()
                        .is_some_and(|agent| agent != cell.key.agent_id.0)
            }) {
                return Err(invalid(
                    "stale session binding does not match model observer",
                ));
            }
            registry
                .slots
                .entry(selected.session_id.clone())
                .or_insert_with(|| slot.clone());
            *slot.offline_pending.lock().unwrap() = true;
            let (sender, receiver) = watch::channel(None);
            registry.tasks += 1;
            let inner = self.inner.clone();
            tokio::spawn(async move {
                let _transition = transition;
                let retain_failure = |error: &str| {
                    slot.retain_captured_failure(cell.as_deref(), error);
                };
                let result = guarded(async {
                    let mut lane = slot.lane.lock().await;
                    if let Some(error) = slot.error.lock().unwrap().as_ref() {
                        return Err(internal(error.clone()));
                    }
                    if lane.uncertain {
                        return Err(internal("model observer lane is closed after uncertain settlement"));
                    }
                    if !writer.stale_selection_matches(
                        &selected, expected_runtime.as_deref(), now_ms, ttl_ms,
                    ).await? {
                        return Ok(false);
                    }
                    let mut changed = false;
                    let mut stopped = ExactRuntimeStop::Missing;
                    let settlement = guarded(async {
                        lane.uncertain = true;
                        changed = Sessions::new(&inner.store)
                            .set_offline_if_stale_selected(
                                &selected.session_id, selected.agent_id.as_deref(), now_ms, ttl_ms,
                            ).await?;
                        if !changed {
                            lane.uncertain = false;
                            return Ok(false);
                        }
                        let expected = expected_runtime.as_deref().map_or(
                            ExpectedRuntimeBinding::Missing, ExpectedRuntimeBinding::Agent,
                        );
                        stopped = AgentRuntimes::new(&inner.store)
                            .stop_if_binding_matches(&selected.session_id.0, expected).await?;
                        if stopped == ExactRuntimeStop::BindingChanged {
                            return Err(internal("runtime binding changed after session committed"));
                        }
                        if let Some(cell) = &cell {
                            cell.close();
                        }
                        lane.confirmed = None;
                        lane.uncertain = false;
                        Ok(true)
                    }).await;
                    if let Err(cause) = settlement {
                        let error = format!("partial stale transition: session write may have committed; {cause}");
                        retain_failure(&error);
                        return Err(internal(error));
                    }
                    drop(lane);
                    if changed {
                        writer.registry().detach_captured(&selected.session_id, &snapshot);
                        let publication = guarded(async {
                            writer.finish_offline(&selected.session_id, true).await?;
                            if stopped == ExactRuntimeStop::Stopped {
                                let agent = AgentId(expected_runtime.clone().expect("stopped captured runtime"));
                                inner.events.project_runtime_binding(&selected.session_id, &agent).await;
                            }
                            Ok(true)
                        }).await;
                        if let Err(cause) = publication {
                            let error = format!("partial stale transition: persistence committed; publication failed: {cause}");
                            // Persistence is confirmed, but publication settlement is uncertain.
                            let mut lane = slot.lane.lock().await;
                            lane.uncertain = true;
                            // Publish the first cause and its captured-only closure policy before
                            // OLD cleanup can acquire this newly uncertain lane.
                            retain_failure(&error);
                            drop(lane);
                            return Err(internal(error));
                        }
                    }
                    Ok(changed)
                }).await;
                {
                    let mut registry = inner.registry.lock().unwrap();
                    if let Err(error) = &result {
                        retain_failure(error);
                    } else {
                        *slot.offline_pending.lock().unwrap() = false;
                    }
                    drop(cell);
                    drop(slot);
                    registry.reclaim_settled_slots();
                    sender.send_replace(Some(result));
                    registry.tasks -= 1;
                }
                inner.settled.notify_waiters();
            });
            receiver
        };
        wait_outcome(receiver).await.map(|_| ())
    }

    /// Capture runtime-only candidates once under presence, then transfer that guard to tracked
    /// settlement. Selection alone does not close local observers: identity revalidation may skip
    /// a newly fresh row. No compatibility presence, attachment or native state is changed here.
    pub(crate) async fn stop_stale_runtimes(
        &self,
        now_ms: i64,
        ttl_ms: i64,
        transition: OwnedMutexGuard<()>,
    ) -> Result<(), NexusError> {
        self.stop_stale_runtimes_captured(now_ms, ttl_ms, transition, None)
            .await
    }

    pub(crate) async fn stop_stale_runtimes_captured(
        &self,
        now_ms: i64,
        ttl_ms: i64,
        transition: OwnedMutexGuard<()>,
        capture: Option<(TransportRegistry, TransportSnapshot, HashSet<SessionId>)>,
    ) -> Result<(), NexusError> {
        let mut pairs = AgentRuntimes::new(&self.inner.store)
            .stale_active_runtime_pairs(now_ms, ttl_ms)
            .await?;
        if let Some((registry, snapshot, selected)) = capture {
            pairs.retain(|(runtime, _)| {
                let session = SessionId(runtime.clone());
                !selected.contains(&session) && registry.matches_captured(&session, &snapshot)
            });
        }
        let receiver = {
            let mut registry = self.inner.registry.lock().unwrap();
            if !registry.ready || registry.shutdown {
                return Err(invalid(
                    "model reporting is not admitting stale transitions",
                ));
            }
            // Validate the WHOLE batch before installing any slot or setting any pending flag.
            // Even GC here would violate zero registry changes on rejection.
            let mut captured = Vec::with_capacity(pairs.len());
            for (runtime, agent) in &pairs {
                validate_id(runtime)?;
                validate_id(agent)?;
                let runtime = SessionId(runtime.clone());
                let agent = AgentId(agent.clone());
                let slot = registry.slots.get(&runtime).cloned().unwrap_or_default();
                if slot.error.lock().unwrap().is_some() || *slot.offline_pending.lock().unwrap() {
                    return Err(invalid("model observer lane is closed"));
                }
                let cell = slot.current.lock().unwrap().upgrade();
                if cell.as_ref().is_some_and(|cell| {
                    cell.key.runtime_id != runtime || cell.key.agent_id != agent
                }) {
                    return Err(invalid(
                        "stale runtime binding does not match model observer",
                    ));
                }
                // A normally revoked, exact matching predecessor may still need runtime stop.
                // Its cleanup is serialized by this same lane; it is never reopened.
                captured.push(CapturedRuntime {
                    runtime,
                    agent,
                    slot,
                    cell,
                });
            }
            captured.sort_by(|left, right| left.runtime.0.cmp(&right.runtime.0));
            captured.dedup_by(|left, right| left.runtime == right.runtime);
            for capture in &captured {
                registry
                    .slots
                    .entry(capture.runtime.clone())
                    .or_insert_with(|| capture.slot.clone());
                *capture.slot.offline_pending.lock().unwrap() = true;
            }
            let (sender, receiver) = watch::channel(None);
            registry.tasks += 1;
            let inner = self.inner.clone();
            tokio::spawn(async move {
                let _transition = transition;
                let result = guarded(async {
                    let mut lanes = Vec::with_capacity(captured.len());
                    for capture in &captured {
                        lanes.push(capture.slot.lane.lock().await);
                    }
                    let mut changed = Vec::new();
                    let stopped = guarded(async {
                        // Contention is not failure. Inspect all lanes only after owning them,
                        // and before marking uncertainty or entering the identity transaction.
                        for (capture, lane) in captured.iter().zip(&lanes) {
                            if let Some(error) = capture.slot.error.lock().unwrap().as_ref() {
                                return Err(internal(error.clone()));
                            }
                            if lane.uncertain {
                                return Err(internal(
                                    "model observer lane is closed after uncertain settlement",
                                ));
                            }
                        }
                        for lane in &mut lanes {
                            lane.uncertain = true;
                        }
                        changed = AgentRuntimes::new(&inner.store)
                            .stop_stale_selected(&pairs, now_ms, ttl_ms)
                            .await?;
                        let changed_set: HashSet<_> = changed
                            .iter()
                            .map(|(runtime, agent)| (runtime.as_str(), agent.as_str()))
                            .collect();
                        for (capture, lane) in captured.iter().zip(&mut lanes) {
                            if changed_set
                                .contains(&(capture.runtime.0.as_str(), capture.agent.0.as_str()))
                            {
                                if let Some(cell) = &capture.cell {
                                    cell.close();
                                }
                                lane.confirmed = None;
                            }
                            lane.uncertain = false;
                        }
                        Ok(true)
                    })
                    .await;
                    if let Err(error) = stopped {
                        // Store can fail AFTER commit while appending lifecycle facts. Retain
                        // old cache/uncertainty, original cause and captured closure BEFORE lane
                        // release; never infer NULL or ask a later current owner for authority.
                        for capture in &captured {
                            capture.retain_failure(&error);
                        }
                        return Err(internal(error));
                    }
                    drop(lanes);
                    // Local close is after confirmed result, not at the instant of DB commit.
                    // Lane exclusion prevents stale applies during the await. Publication is a
                    // canonical reread outside lanes, never an assumed stopped snapshot.
                    for (runtime, agent) in changed {
                        inner
                            .events
                            .project_runtime_binding(&runtime.into(), &agent.into())
                            .await;
                    }
                    Ok(true)
                })
                .await;
                {
                    let mut registry = inner.registry.lock().unwrap();
                    for capture in &captured {
                        if let Err(error) = &result {
                            capture.retain_failure(error);
                        } else {
                            *capture.slot.offline_pending.lock().unwrap() = false;
                        }
                    }
                    // Captured strong refs must not make successful orphan-only slots immortal.
                    drop(captured);
                    registry.reclaim_settled_slots();
                    sender.send_replace(Some(result));
                    registry.tasks -= 1;
                }
                inner.settled.notify_waiters();
            });
            receiver
        };
        wait_outcome(receiver).await.map(|_| ())
    }

    /// Retention never retires a local origin: every existing slot and agent purge fence vetoes
    /// selection. Only newly installed empty slots participate, excluding reserve until this
    /// tracked task settles. The presence guard comes from this coordinator's own Store.
    pub(crate) async fn reap_retention(&self, cutoff: i64) -> Result<u64, NexusError> {
        let transition = self.inner.store.lock_presence_transition().await;
        let candidates = AgentRuntimes::new(&self.inner.store)
            .retention_candidate_pairs(cutoff)
            .await?;
        let receiver = {
            let mut registry = self.inner.registry.lock().unwrap();
            if !registry.ready || registry.shutdown {
                return Err(invalid(
                    "model reporting is not admitting runtime retention",
                ));
            }
            // Do not GC here: even an apparently idle existing slot is outside this operation's
            // deletion authority. Select the entire set before installing any pending flags.
            let selected: Vec<_> = candidates
                .into_iter()
                .filter(|(runtime, agent)| {
                    !registry.slots.contains_key(&SessionId(runtime.clone()))
                        && !registry
                            .purging_agents
                            .contains_key(&AgentId(agent.clone()))
                })
                .collect();
            if selected.is_empty() {
                return Ok(0);
            }
            let slots: Vec<_> = selected
                .iter()
                .map(|(runtime, _)| {
                    let slot = Arc::new(RuntimeSlot::default());
                    *slot.offline_pending.lock().unwrap() = true;
                    registry
                        .slots
                        .insert(SessionId(runtime.clone()), slot.clone());
                    slot
                })
                .collect();
            let (sender, receiver) = tokio::sync::oneshot::channel();
            registry.tasks += 1;
            let inner = self.inner.clone();
            tokio::spawn(async move {
                let _transition = transition;
                let mut count = 0;
                let result = guarded(async {
                    // Store candidate order is sorted by runtime, matching all multi-lane writers.
                    let mut lanes = Vec::with_capacity(slots.len());
                    for slot in &slots {
                        lanes.push(slot.lane.lock().await);
                    }
                    for (slot, lane) in slots.iter().zip(&lanes) {
                        if lane.uncertain
                            || lane.confirmed.is_some()
                            || slot.error.lock().unwrap().is_some()
                        {
                            return Err(internal("runtime retention lane lost empty ownership"));
                        }
                    }
                    for lane in &mut lanes {
                        lane.uncertain = true;
                    }
                    let write = guarded(async {
                        let changed = AgentRuntimes::new(&inner.store)
                            .reap_retention_selected(cutoff, &selected)
                            .await?;
                        // A returned count is authority only for this captured subset.
                        let unique: std::collections::BTreeSet<_> = changed.iter().collect();
                        if unique.len() != changed.len()
                            || changed
                                .iter()
                                .any(|pair| selected.binary_search(pair).is_err())
                        {
                            return Err(internal(
                                "runtime retention returned a foreign deletion receipt",
                            ));
                        }
                        count = changed.len() as u64;
                        Ok(true)
                    })
                    .await;
                    match &write {
                        Ok(_) => {
                            for lane in &mut lanes {
                                lane.uncertain = false;
                            }
                        }
                        Err(error) => {
                            for slot in &slots {
                                slot.retain_captured_failure(None, error);
                            }
                        }
                    }
                    write.map_err(internal)
                })
                .await;
                {
                    let mut registry = inner.registry.lock().unwrap();
                    for slot in &slots {
                        if let Err(error) = &result {
                            slot.retain_captured_failure(None, error);
                        } else {
                            *slot.offline_pending.lock().unwrap() = false;
                        }
                    }
                    drop(slots);
                    registry.reclaim_settled_slots();
                    registry.tasks -= 1;
                    let _ = sender.send(result.map(|_| count));
                }
                inner.settled.notify_waiters();
            });
            receiver
        };
        receiver
            .await
            .map_err(|_| internal("runtime retention settlement outcome unavailable"))?
            .map_err(internal)
    }

    /// Synchronous local fence; tracked writes retain ownership until async shutdown drains them.
    pub(crate) fn close_admission(&self) {
        {
            let mut registry = self.inner.registry.lock().unwrap();
            registry.shutdown = true;
            registry.ready = false;
            for slot in registry.slots.values() {
                if let Some(cell) = slot.current.lock().unwrap().upgrade() {
                    cell.close();
                }
            }
        }
    }

    pub(crate) async fn shutdown(&self, bounded_wait: Duration) -> Result<(), NexusError> {
        self.close_admission();
        tokio::time::timeout(bounded_wait, async {
            loop {
                let notified = self.inner.settled.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                {
                    let registry = self.inner.registry.lock().unwrap();
                    if registry.tasks == 0 {
                        if let Some(error) = &registry.initialization_error {
                            return Err(internal(error.clone()));
                        }
                        for fence in registry.purging_agents.values() {
                            if let Some(error) = &*fence.error.lock().unwrap() {
                                return Err(internal(error.clone()));
                            }
                        }
                        for slot in registry.slots.values() {
                            if let Some(error) = &*slot.error.lock().unwrap() {
                                return Err(internal(error.clone()));
                            }
                        }
                        return Ok(());
                    }
                }
                notified.await;
            }
        })
        .await
        .map_err(|_| {
            internal("model reporting shutdown incomplete; owned settlement is still running")
        })?
    }

    fn check_handle(&self, handle: &ModelObserverHandle) -> Result<(), NexusError> {
        if handle.cell.coordinator.ptr_eq(&Arc::downgrade(&self.inner)) {
            Ok(())
        } else {
            Err(invalid("foreign model observer handle"))
        }
    }
}

struct ClaimWait<'a> {
    cell: &'a OwnerCell,
    completed: bool,
}

impl Drop for ClaimWait<'_> {
    fn drop(&mut self) {
        if !self.completed {
            let mut state = self.cell.state.lock().unwrap();
            if !state.activated && !state.closed {
                state.closed = true;
                self.cell.latest.send_replace(state.snapshot());
            }
        }
    }
}

impl Inner {
    async fn run_residue_cleanup(
        &self,
        operation: NonAgentResumeOperation,
        expected_agent: Option<String>,
        slot: &RuntimeSlot,
        cell: Option<&OwnerCell>,
    ) -> Result<SelectedRuntimeResidueCleanup, NexusError> {
        let mut lane = slot.lane.lock().await;
        if let Some(error) = slot.error.lock().unwrap().as_ref() {
            return Err(internal(error.clone()));
        }
        require_residue_cell(&operation, expected_agent.as_deref(), cell)?;
        if lane.uncertain || lane.confirmed.is_some() {
            return Err(invalid("residue cleanup requires an unconfirmed lane"));
        }
        lane.uncertain = true;
        let result = AssertUnwindSafe(async {
            if !operation.matches_current_session().await? {
                return Ok(SelectedRuntimeResidueCleanup::BindingChanged);
            }
            let expected = expected_agent
                .as_deref()
                .map(ExpectedRuntimeBinding::Agent)
                .unwrap_or(ExpectedRuntimeBinding::Missing);
            AgentRuntimes::new(&self.store)
                .remove_non_agent_residue_selected(&operation.session_id().0, expected)
                .await
        })
        .catch_unwind()
        .await;
        let result = match result {
            Ok(result) => result,
            Err(payload) => {
                let cause = payload
                    .downcast_ref::<String>()
                    .map(String::as_str)
                    .or_else(|| payload.downcast_ref::<&str>().copied())
                    .unwrap_or("non-string panic payload");
                Err(internal(format!(
                    "residue cleanup outcome unavailable after panic: {cause}"
                )))
            }
        };
        match &result {
            Ok(outcome) => {
                if matches!(
                    outcome,
                    SelectedRuntimeResidueCleanup::Removed
                        | SelectedRuntimeResidueCleanup::AlreadyAbsent
                ) {
                    if let Some(cell) = cell {
                        cell.close();
                    }
                }
                // Clean selection veto preserves the captured reservation and cached authority.
                lane.uncertain = false;
            }
            Err(error) => slot.retain_captured_failure(cell, &error.to_string()),
        }
        result
    }

    async fn run_unbound_cleanup(
        &self,
        staged: CapturedStagedSession,
        slot: &RuntimeSlot,
        cell: Option<&OwnerCell>,
    ) -> Result<SelectedStagedSessionCleanup, NexusError> {
        let mut lane = slot.lane.lock().await;
        if let Some(error) = slot.error.lock().unwrap().as_ref() {
            return Err(internal(error.clone()));
        }
        require_uncommitted_cell(cell)?;
        if lane.uncertain || lane.confirmed.is_some() {
            return Err(invalid("unbound cleanup requires an unconfirmed lane"));
        }
        lane.uncertain = true;
        // Only the transaction-selected Missing receipt authorizes transport CAS. Never
        // late-read an Agent expectation. These independent stores are not one transaction
        // and do not exclude arbitrary direct writers bypassing managed presence authority.
        let result = AssertUnwindSafe(async {
            match AgentRuntimes::new(&self.store)
                .stop_if_binding_matches(
                    &staged.row().session_id.0,
                    ExpectedRuntimeBinding::Missing,
                )
                .await?
            {
                ExactRuntimeStop::Missing => {
                    Sessions::new(&self.store)
                        .remove_staged_registration_selected(&staged)
                        .await
                }
                ExactRuntimeStop::BindingChanged => {
                    Ok(SelectedStagedSessionCleanup::SelectionChanged)
                }
                ExactRuntimeStop::Stopped => {
                    Err(internal("unbound cleanup did not confirm runtime absence"))
                }
            }
        })
        .catch_unwind()
        .await;
        let result = match result {
            Ok(result) => result,
            Err(payload) => {
                let cause = payload
                    .downcast_ref::<String>()
                    .map(String::as_str)
                    .or_else(|| payload.downcast_ref::<&str>().copied())
                    .unwrap_or("non-string panic payload");
                Err(internal(format!(
                    "unbound cleanup store outcome unavailable after panic: {cause}"
                )))
            }
        };
        match &result {
            Ok(outcome) => {
                if matches!(
                    outcome,
                    SelectedStagedSessionCleanup::Removed
                        | SelectedStagedSessionCleanup::AlreadyAbsent
                ) {
                    if let Some(cell) = cell {
                        cell.close();
                    }
                }
                // A clean veto leaves the captured cell and cache untouched and usable.
                lane.uncertain = false;
            }
            Err(error) => {
                // Install the FIRST cause and captured-only closure under exclusion, before
                // a queued OLD worker can observe uncertainty. No rollback certainty implied.
                slot.retain_captured_failure(cell, &error.to_string());
            }
        }
        result
    }

    async fn run_identity_purge(
        &self,
        selected: &SessionRow,
        agent: Option<&str>,
        pairs: &[(String, String)],
        captured: &[PurgeParticipant],
        fence: &PurgeFence,
    ) -> Result<SelectedIdentityPurge, ModelPurgeError> {
        let mut lanes = Vec::with_capacity(captured.len());
        for capture in captured {
            lanes.push(capture.slot.lane.lock().await);
        }
        for (capture, lane) in captured.iter().zip(&lanes) {
            let error = capture.slot.error.lock().unwrap().clone().or_else(|| {
                lane.uncertain
                    .then(|| "purge lane has retained uncertain settlement".to_owned())
            });
            if let Some(error) = error {
                // A prior failure is not a clean no-effect recovery of this agent fence.
                *fence.error.lock().unwrap() = Some(error.clone());
                return Err(ModelPurgeError::BeforeStore {
                    cause: internal(error),
                });
            }
        }
        let session =
            AssertUnwindSafe(Sessions::new(&self.store).find_by_session_id(&selected.session_id))
                .catch_unwind()
                .await;
        match session {
            Ok(Ok(Some(current))) if current == *selected => {}
            Ok(Ok(_)) => return Ok(SelectedIdentityPurge::SelectionChanged),
            Ok(Err(cause)) => return Err(ModelPurgeError::BeforeStore { cause }),
            Err(payload) => {
                return Err(ModelPurgeError::BeforeStore {
                    cause: purge_panic(payload),
                })
            }
        }
        for lane in &mut lanes {
            lane.uncertain = true;
        }
        // Own the actual store result before attempting any local closure. A later panic cannot
        // erase a confirmed receipt or relabel an actual store failure as pre-submission.
        let sessions = Sessions::new(&self.store);
        let outcome =
            match AssertUnwindSafe(sessions.purge_identity_selected(selected, agent, pairs))
                .catch_unwind()
                .await
            {
                Ok(outcome) => outcome,
                Err(payload) => {
                    let cause = purge_panic(payload);
                    retain_purge_failure(captured, fence, &cause.to_string());
                    return Err(ModelPurgeError::OutcomeUnavailable { cause });
                }
            };
        let settled = std::panic::catch_unwind(AssertUnwindSafe(|| -> Result<(), NexusError> {
            match &outcome {
                Ok(SelectedIdentityPurge::Purged(receipt)) => {
                    if receipt.session_id() != &selected.session_id
                        || receipt.agent_id() != agent
                        || receipt.runtime_pairs() != pairs
                        || receipt.project() != selected.project
                        || receipt.name() != selected.name.as_deref()
                    {
                        return Err(internal(
                            "identity purge receipt does not match captured selection",
                        ));
                    }
                    for (capture, lane) in captured.iter().zip(&mut lanes) {
                        if let Some(cell) = &capture.cell {
                            cell.close();
                        }
                        lane.confirmed = None;
                        lane.uncertain = false;
                    }
                }
                Ok(SelectedIdentityPurge::SelectionChanged) => {
                    for lane in &mut lanes {
                        lane.uncertain = false;
                    }
                }
                Err(failure)
                    if failure.commit_state() == IdentityPurgeCommitState::NotCommitted =>
                {
                    for lane in &mut lanes {
                        lane.uncertain = false;
                    }
                }
                Err(_) => {}
            }
            Ok(())
        }));
        let settlement_error = match settled {
            Ok(result) => result.err(),
            Err(payload) => Some(purge_panic(payload)),
        };
        let original = match &outcome {
            Err(failure) if failure.commit_state() == IdentityPurgeCommitState::Unknown => {
                Some(failure.cause().to_string())
            }
            _ => settlement_error.as_ref().map(ToString::to_string),
        };
        if let Some(original) = original {
            for lane in &mut lanes {
                lane.uncertain = true;
            }
            // FIRST cause and captured-only policy are installed before releasing any lane.
            retain_purge_failure(captured, fence, &original);
        }
        match outcome {
            Ok(receipt) => match settlement_error {
                Some(cause) => Err(ModelPurgeError::AfterStore { receipt, cause }),
                None => Ok(receipt),
            },
            Err(failure) => Err(ModelPurgeError::Store {
                failure,
                settlement_error,
            }),
        }
    }

    async fn run_transport_purge(
        &self,
        selected: &SessionRow,
        receipt: SelectedIdentityPurge,
        captured: &[PurgeParticipant],
        fence: &PurgeFence,
    ) -> Result<SelectedIdentityPurge, ModelPurgeError> {
        let SelectedIdentityPurge::Purged(identity) = &receipt else {
            return Ok(receipt);
        };
        // Identity lanes were released, but the tracked caller still owns presence, every
        // pending flag and the whole-agent fence. Never hand the tail to an abandoned waiter.
        let tail = AssertUnwindSafe(
            Sessions::new(&self.store).purge_transport_selected(selected, identity),
        )
        .catch_unwind()
        .await;
        let mut cause = match tail {
            Ok(Ok(SelectedTransportPurge::Purged)) => None,
            Ok(Ok(SelectedTransportPurge::SelectionChanged)) => Some(invalid(
                "transport purge selection changed after committed identity deletion",
            )),
            Ok(Err(cause)) => Some(cause),
            Err(payload) => Some(purge_panic(payload)),
        };
        let mut lanes = Vec::with_capacity(captured.len());
        for capture in captured {
            lanes.push(capture.slot.lane.lock().await);
        }
        // A worker may have retained an earlier failure while the transport await was pending.
        // Preserve that FIRST cause for admission/shutdown, even if the tail also failed.
        let prior = captured.iter().zip(&lanes).find_map(|(capture, lane)| {
            capture.slot.error.lock().unwrap().clone().or_else(|| {
                lane.uncertain
                    .then(|| "purge lane has retained uncertain settlement".to_owned())
            })
        });
        if cause.is_none() {
            cause = prior.as_ref().map(|error| internal(error.clone()));
        }
        if let Some(cause) = cause {
            for lane in &mut lanes {
                lane.uncertain = true;
            }
            retain_purge_failure(captured, fence, &prior.unwrap_or_else(|| cause.to_string()));
            // This receipt remains actual identity-store evidence even when transport may have
            // committed before its lifecycle append failed. No cross-store rollback inference.
            Err(ModelPurgeError::AfterStore { receipt, cause })
        } else {
            Ok(receipt)
        }
    }

    async fn run_activation(
        &self,
        request: RuntimeActivationRequest,
        target: &str,
        agent: &str,
        siblings: &[(String, String)],
        captured: &[CapturedRuntime],
    ) -> Result<SelectedRuntimeActivation, RuntimeActivationError> {
        let mut lanes = Vec::with_capacity(captured.len());
        for capture in captured {
            lanes.push(capture.slot.lane.lock().await);
        }
        // Recheck AFTER the wait: an older claim/apply can have failed while we queued.
        for (capture, lane) in captured.iter().zip(&lanes) {
            if let Some(cause) = capture.slot.error.lock().unwrap().as_ref() {
                return Err(RuntimeActivationError::RejectedBeforeStore {
                    cause: internal(cause.clone()),
                });
            }
            if lane.uncertain {
                return Err(RuntimeActivationError::RejectedBeforeStore {
                    cause: internal("model observer lane is closed after uncertain settlement"),
                });
            }
        }
        for lane in &mut lanes {
            lane.uncertain = true;
        }
        // This catch returns and RETAINS the actual store Result before any later await. The
        // string-erasing watch helper is not an authority channel for activation disposition.
        let store_result = AssertUnwindSafe(async {
            let repo = AgentRuntimes::new(&self.store);
            match request {
                RuntimeActivationRequest::CreateActive(runtime) => {
                    repo.create_active_selected(runtime, siblings).await
                }
                RuntimeActivationRequest::ActivateExisting {
                    runtime_id,
                    agent_id,
                } => {
                    repo.activate_selected(&runtime_id.0, &agent_id.0, siblings)
                        .await
                }
            }
        })
        .catch_unwind()
        .await;
        let outcome = match store_result {
            Ok(result) => result,
            Err(payload) => {
                let cause = activation_panic(payload);
                for capture in captured {
                    capture.retain_failure(&cause.to_string());
                }
                return Err(RuntimeActivationError::OutcomeUnavailable { cause });
            }
        };
        let changes = match &outcome {
            Ok(SelectedRuntimeActivation::Applied(changes)) => Some(changes),
            Err(failure) if failure.commit_state() == RuntimeActivationCommitState::Committed => {
                failure.confirmed_changes()
            }
            _ => None,
        };
        let confirmed = changes
            .is_some_and(|changes| activation_receipt_matches(changes, target, agent, siblings));
        let mut settlement_error = None;
        if changes.is_some() && !confirmed
            || matches!(&outcome, Err(failure) if failure.commit_state() == RuntimeActivationCommitState::Committed && changes.is_none())
        {
            settlement_error = Some(internal(
                "activation receipt does not match full captured selection",
            ));
        }
        let safe_no_change = matches!(&outcome, Ok(SelectedRuntimeActivation::SelectionChanged))
            || matches!(&outcome, Err(failure) if failure.commit_state() == RuntimeActivationCommitState::NotCommitted);
        if confirmed || safe_no_change {
            for (capture, lane) in captured.iter().zip(&mut lanes) {
                if confirmed && capture.runtime.0 != target {
                    if let Some(cell) = &capture.cell {
                        cell.close();
                    }
                    lane.confirmed = None;
                }
                lane.uncertain = false;
            }
        }
        // Install FIRST cause while excluded, before any OLD worker may classify a failure.
        // Confirmed committed effects are settled even when lifecycle append subsequently failed.
        if let Err(failure) = &outcome {
            if failure.commit_state() != RuntimeActivationCommitState::NotCommitted {
                for capture in captured {
                    capture.retain_failure(&failure.cause().to_string());
                }
            }
        } else if let Some(cause) = &settlement_error {
            for capture in captured {
                capture.retain_failure(&cause.to_string());
            }
        }
        drop(lanes);
        if confirmed {
            // Canonical rereads are outside runtime lanes, with pending admission and presence
            // still held. Do not copy the receipt into an assumed projection snapshot.
            let publication = guarded(async {
                for capture in captured {
                    self.events
                        .project_runtime_binding(&capture.runtime, &capture.agent)
                        .await;
                }
                Ok(true)
            })
            .await;
            if let Err(error) = publication {
                let cause = internal(error);
                let mut lanes = Vec::with_capacity(captured.len());
                for capture in captured {
                    lanes.push(capture.slot.lane.lock().await);
                }
                // Same named exclusion guard as the original-cause boundary: secondary OLD
                // cleanup can only observe uncertainty AFTER captured-only failure is installed.
                let original = match &outcome {
                    Err(failure) => failure.cause().to_string(),
                    Ok(_) => cause.to_string(),
                };
                for (capture, lane) in captured.iter().zip(&mut lanes) {
                    lane.uncertain = true;
                    capture.retain_failure(&original);
                }
                drop(lanes);
                settlement_error = Some(cause);
            }
        }
        match outcome {
            Ok(receipt) => match settlement_error {
                Some(cause) => Err(RuntimeActivationError::AfterStore { receipt, cause }),
                None => Ok(receipt),
            },
            Err(failure) => Err(RuntimeActivationError::Store {
                failure,
                settlement_error,
            }),
        }
    }

    fn fail_owner(&self, owned: &OwnerCell, error: &str) {
        // Serialize terminal failure with reserve and activate: a reservation either precedes
        // this closure and is captured here, or follows the retained error and is rejected.
        // Lock order is registry -> error -> current -> cell; none crosses an await. Holding the
        // error lock until closure also prevents a store lane from admitting work in between.
        let _registry = self.registry.lock().unwrap();
        let mut failure = owned.slot.error.lock().unwrap();
        // A prior captured settlement has already chosen exactly which OLD admission to close.
        // Secondary worker cleanup must not reinterpret that failure as authority over NEW.
        if !owned.slot.captured_failure_closure.load(Ordering::Relaxed) {
            let current = owned.slot.current.lock().unwrap();
            if let Some(current) = current.upgrade() {
                current.close();
            }
        }
        owned.close();
        // Secondary cleanup failures must not erase the initiating ownership/stop failure.
        if failure.is_none() {
            *failure = Some(error.to_owned());
        }
        // Only local admission is closed here. Never derive durable revoke authority from this
        // current-ticket lookup; each tracked worker retains its own captured exact token.
    }

    async fn initialize_boot(&self) -> Result<bool, NexusError> {
        // Sole-coordinator boot inventory only. Stream IDs, retaining one captured row at a time,
        // including orphan runtimes; no unbounded in-memory list or normal-operation owner lookup.
        let mut rows = self.store.identity_conn().query(
            "SELECT runtime_id FROM agent_runtimes WHERE model_observer_token IS NOT NULL ORDER BY runtime_id", ()
        ).await.map_err(|e| internal(e.to_string()))?;
        while let Some(row) = rows.next().await.map_err(|e| internal(e.to_string()))? {
            let runtime: String = row.get(0).map_err(|e| internal(e.to_string()))?;
            let captured = AgentRuntimes::new(&self.store)
                .find_by_runtime_id(&runtime)
                .await?;
            let Some(captured) = captured else {
                continue;
            };
            let Some(token) = captured.model_observer_token else {
                continue;
            };
            validate_id(&runtime)?;
            validate_id(&captured.agent_id)?;
            let key = ModelObserverKey {
                runtime_id: runtime.into(),
                agent_id: captured.agent_id.into(),
                token,
            };
            if AgentRuntimes::new(&self.store)
                .revoke_model_observer(&key.runtime_id.0, &key.agent_id.0, &key.token)
                .await?
            {
                self.events
                    .project_runtime_binding(&key.runtime_id, &key.agent_id)
                    .await;
            } else {
                return Err(internal(
                    "boot observer authority changed during exact invalidation",
                ));
            }
        }
        Ok(true)
    }

    async fn run_owner(
        &self,
        cell: &OwnerCell,
        mut updates: watch::Receiver<Snapshot>,
    ) -> Result<bool, NexusError> {
        loop {
            let snapshot = updates.borrow_and_update().clone();
            if snapshot.closed {
                return Ok(false);
            }
            if snapshot.claim_requested {
                break;
            }
            if updates.changed().await.is_err() {
                return Ok(false);
            }
        }
        let admitted = {
            let mut lane = cell.slot.lane.lock().await;
            require_lane(cell, &lane)?;
            if !cell.current() {
                return Ok(false);
            }
            lane.uncertain = true;
            let changed = AgentRuntimes::new(&self.store)
                .claim_model_observer(
                    &cell.key.runtime_id.0,
                    &cell.key.agent_id.0,
                    lane.confirmed.as_deref(),
                    &cell.key.token,
                    &cell.initial,
                )
                .await?;
            lane.uncertain = false;
            if !changed {
                return Ok(false);
            }
            lane.confirmed = Some(cell.key.token.clone());
            if !cell.current() {
                self.cleanup(cell, &mut lane).await?;
                false
            } else {
                cell.state.lock().unwrap().committed = true;
                true
            }
        };
        // Publication is never under the runtime lane. Older frames may reorder; the later
        // Gateway revision guard is required before this foundation is wired into production.
        self.events
            .project_runtime_binding(&cell.key.runtime_id, &cell.key.agent_id)
            .await;
        if !admitted {
            cell.claimed.send_replace(Some(Ok(false)));
            return Ok(false);
        }
        // A cancelled caller or superseding reserve may have closed us during publication.
        if !cell.current() {
            let changed = {
                let mut lane = cell.slot.lane.lock().await;
                require_lane(cell, &lane)?;
                self.cleanup(cell, &mut lane).await?
            };
            if changed {
                self.events
                    .project_runtime_binding(&cell.key.runtime_id, &cell.key.agent_id)
                    .await;
            }
            cell.claimed.send_replace(Some(Ok(false)));
            return Ok(false);
        }
        cell.claimed.send_replace(Some(Ok(true)));
        let mut persisted_sequence = 0;
        loop {
            let snapshot = updates.borrow_and_update().clone();
            if snapshot.closed {
                let changed = {
                    let mut lane = cell.slot.lane.lock().await;
                    require_lane(cell, &lane)?;
                    self.cleanup(cell, &mut lane).await?
                };
                if changed {
                    self.events
                        .project_runtime_binding(&cell.key.runtime_id, &cell.key.agent_id)
                        .await;
                }
                return Ok(changed);
            }
            if snapshot.activated && snapshot.sequence > persisted_sequence {
                let Some(changed) = self.apply_snapshot(cell, &snapshot).await? else {
                    continue;
                };
                persisted_sequence = snapshot.sequence;
                if changed {
                    self.events
                        .project_runtime_binding(&cell.key.runtime_id, &cell.key.agent_id)
                        .await;
                } else {
                    cell.close();
                }
                continue;
            }
            if updates.changed().await.is_err() {
                cell.close();
            }
        }
    }

    /// Apply one captured snapshot only while its exact local owner is still current. `None`
    /// means local authority closed before lane admission, not a failed durable CAS attempt.
    async fn apply_snapshot(
        &self,
        cell: &OwnerCell,
        snapshot: &Snapshot,
    ) -> Result<Option<bool>, NexusError> {
        let mut lane = cell.slot.lane.lock().await;
        require_lane(cell, &lane)?;
        if !cell.current() {
            return Ok(None);
        }
        lane.uncertain = true;
        let changed = AgentRuntimes::new(&self.store)
            .apply_model_report(
                &cell.key.runtime_id.0,
                &cell.key.agent_id.0,
                &cell.key.token,
                snapshot.sequence,
                &snapshot.report,
            )
            .await?;
        lane.uncertain = false;
        Ok(Some(changed))
    }

    async fn cleanup(&self, cell: &OwnerCell, lane: &mut Lane) -> Result<bool, NexusError> {
        lane.uncertain = true;
        let changed = AgentRuntimes::new(&self.store)
            .revoke_model_observer(
                &cell.key.runtime_id.0,
                &cell.key.agent_id.0,
                &cell.key.token,
            )
            .await?;
        lane.uncertain = false;
        if changed && lane.confirmed.as_deref() == Some(&cell.key.token) {
            lane.confirmed = None;
        }
        Ok(changed)
    }
}

fn require_residue_cell(
    operation: &NonAgentResumeOperation,
    expected_agent: Option<&str>,
    cell: Option<&OwnerCell>,
) -> Result<(), NexusError> {
    if let Some(cell) = cell {
        // Present runtime binding is authority; absence alone supplies none. Only a positive
        // captured Session stamp can match an unclaimed local origin for a missing runtime.
        let agent = expected_agent.or_else(|| operation.stale_agent_id());
        if cell.key.runtime_id != *operation.session_id()
            || agent != Some(cell.key.agent_id.0.as_str())
        {
            return Err(invalid(
                "residue cleanup binding does not match model observer",
            ));
        }
    }
    require_uncommitted_cell(cell)
}

fn require_uncommitted_cell(cell: Option<&OwnerCell>) -> Result<(), NexusError> {
    if cell.is_some_and(|cell| {
        let state = cell.state.lock().unwrap();
        state.committed || state.activated
    }) {
        Err(invalid(
            "unbound cleanup cannot remove a committed model observer registration",
        ))
    } else {
        Ok(())
    }
}

fn activation_receipt_matches(
    changes: &RuntimeActivationChanges,
    target: &str,
    agent: &str,
    siblings: &[(String, String)],
) -> bool {
    changes.target_pair().0 == target
        && changes.target_pair().1 == agent
        && changes.stopped_sibling_pairs() == siblings
}

fn retain_purge_failure(captured: &[PurgeParticipant], fence: &PurgeFence, cause: &str) {
    let mut failure = fence.error.lock().unwrap();
    if failure.is_none() {
        *failure = Some(cause.to_owned());
    }
    drop(failure);
    for capture in captured {
        // Retain admission failure even when closing a poisoned local cell itself panics.
        // No registry/current lookup, and the caller still owns every runtime lane.
        let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
            capture
                .slot
                .retain_captured_failure(capture.cell.as_deref(), cause);
        }));
    }
}

fn purge_panic(payload: Box<dyn std::any::Any + Send>) -> NexusError {
    let cause = payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied())
        .unwrap_or("non-string panic payload");
    internal(format!("identity purge settlement panicked: {cause}"))
}

fn activation_panic(payload: Box<dyn std::any::Any + Send>) -> NexusError {
    let cause = payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied())
        .unwrap_or("non-string panic payload");
    internal(format!(
        "activation store outcome unavailable after panic: {cause}"
    ))
}

fn require_lane(cell: &OwnerCell, lane: &Lane) -> Result<(), NexusError> {
    if lane.uncertain || cell.slot.error.lock().unwrap().is_some() {
        Err(internal(
            "model observer lane is closed after uncertain settlement",
        ))
    } else {
        Ok(())
    }
}

async fn guarded(future: impl Future<Output = Result<bool, NexusError>>) -> Outcome {
    match AssertUnwindSafe(future).catch_unwind().await {
        Ok(result) => result.map_err(|e| e.to_string()),
        Err(payload) => {
            let cause = payload
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| payload.downcast_ref::<&str>().copied())
                .unwrap_or("non-string panic payload");
            Err(format!(
                "model observer settlement panicked; lane remains closed: {cause}"
            ))
        }
    }
}

async fn wait_outcome(mut receiver: watch::Receiver<Option<Outcome>>) -> Result<bool, NexusError> {
    loop {
        if let Some(result) = receiver.borrow_and_update().clone() {
            return result.map_err(internal);
        }
        receiver
            .changed()
            .await
            .map_err(|_| internal("model observer settlement channel closed"))?;
    }
}

fn validate_id(id: &str) -> Result<(), NexusError> {
    if id.trim().is_empty() || id.chars().any(char::is_control) {
        Err(invalid("invalid model observer identity"))
    } else {
        Ok(())
    }
}

fn invalid(message: impl Into<String>) -> NexusError {
    NexusError::Invalid(message.into())
}
fn internal(message: impl Into<String>) -> NexusError {
    NexusError::Internal(message.into())
}
