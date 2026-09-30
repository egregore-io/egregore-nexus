//! Connection-owned presence support.
//!
//! Presence truth for daemon-owned harnesses is the live transport registry. The durable
//! `sessions.presence` and `agent_runtimes.presence` columns are materialized projections written
//! through [`PresenceWriter`]. Every effective online/offline transition emits one `agent.status`
//! fact after the store converges; [`WsSink`](crate::daemon::app::WsSink) orders those facts on the
//! ephemeral `sys.fleet.status` lane. CLI/MCP peers remain heartbeat-derived because they have no
//! daemon transport handle.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use nexus_common::presence::presence_token;
use nexus_common::{now, NexusError};
use nexus_contracts::{EventSink, Presence, SessionId, WsEvent};
use nexus_identity::{RuntimeActivation, RuntimeActivationError, RuntimeActivationRequest};
use nexus_store::repos::{
    agent_runtimes::{ExactRuntimeStop, ExpectedRuntimeBinding, SelectedRuntimeActivation},
    AgentRuntimes, Inbox, Sessions,
};
use nexus_store::types::SessionRow;
use nexus_store::Store;

use crate::daemon::model_reporting::{ModelPurgeError, ModelReporting};
use nexus_store::repos::sessions::SelectedIdentityPurge;

pub(crate) enum PresencePurgeOutcome {
    Legacy,
    Managed(SelectedIdentityPurge),
}

pub(crate) enum PresencePurgeError {
    Legacy(NexusError),
    Managed(ModelPurgeError),
}

impl PresencePurgeError {
    pub(crate) fn into_nexus_error(self) -> NexusError {
        let error = match self {
            Self::Legacy(cause) => return cause,
            Self::Managed(error) => error,
        };
        // Keep the actual receipt/failure until this existing public-error boundary. Native
        // release may already have happened even when coordinator submission was rejected.
        let context = match error {
            ModelPurgeError::BeforeStore { cause } => format!("{cause}; purge rejected before identity store"),
            ModelPurgeError::Store { failure, settlement_error } => format!(
                "{}; identity purge commit state {:?}; settlement error: {:?}",
                failure.cause(), failure.commit_state(), settlement_error),
            ModelPurgeError::AfterStore { receipt, cause } => match receipt {
                SelectedIdentityPurge::Purged(receipt) => format!(
                    "{cause}; identity purge committed for {}; transport/settlement incomplete or uncertain",
                    receipt.session_id().0),
                SelectedIdentityPurge::SelectionChanged => format!(
                    "{cause}; identity purge selection changed; no deletion receipt"),
            },
            ModelPurgeError::OutcomeUnavailable { cause } => format!(
                "{cause}; identity purge outcome unavailable; deletion may have committed"),
        };
        NexusError::Store(context)
    }
}

/// In-memory transport dimensions that can make a daemon-owned session present.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum TransportHandle {
    EventLoop,
    NativeForwarder(String),
    RawStream,
}

/// Daemon-local registry of live transport handles per session.
#[derive(Clone, Default)]
pub(crate) struct TransportRegistry {
    inner: Arc<Mutex<TransportInstances>>,
}

// Allocation identity distinguishes attachments even when their handle values are equal.
type TransportInstances = HashMap<SessionId, BTreeMap<TransportHandle, Arc<()>>>;

/// Immutable capture of attachment instances, shared cheaply by clones.
///
/// Captures keep their markers alive until dropped; the registry retains only current instances,
/// with no retired marker history. This is transport identity, not native-process ownership.
#[derive(Clone)]
pub(crate) struct TransportSnapshot {
    instances: Arc<TransportInstances>,
}

impl TransportRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Every attach creates a fresh instance, including replacement of an equal handle.
    pub(crate) fn attach(&self, session: &SessionId, handle: TransportHandle) {
        self.inner
            .lock()
            .expect("transport registry poisoned")
            .entry(session.clone())
            .or_default()
            .insert(handle, Arc::new(()));
    }

    /// Capture all current instances under the registry lock, before selecting stale candidates.
    pub(crate) fn snapshot(&self) -> TransportSnapshot {
        let inner = self.inner.lock().expect("transport registry poisoned");
        TransportSnapshot {
            instances: Arc::new(inner.clone()),
        }
    }

    /// Exact set equality, including captured absence and equal-handle replacement identity.
    pub(crate) fn matches_captured(
        &self,
        session: &SessionId,
        snapshot: &TransportSnapshot,
    ) -> bool {
        let inner = self.inner.lock().expect("transport registry poisoned");
        match (inner.get(session), snapshot.instances.get(session)) {
            (None, None) => true,
            (Some(current), Some(old)) => {
                current.len() == old.len()
                    && current.iter().all(|(key, value)| {
                        old.get(key)
                            .is_some_and(|captured| Arc::ptr_eq(value, captured))
                    })
            }
            _ => false,
        }
    }

    /// Remove only this session's instances that still match the capture by allocation identity.
    ///
    /// Returns true if any captured instance was removed, not whether the session became absent.
    /// Attachments added or replaced after capture survive, including equal-valued handles.
    pub(crate) fn detach_captured(
        &self,
        session: &SessionId,
        snapshot: &TransportSnapshot,
    ) -> bool {
        let Some(captured) = snapshot.instances.get(session) else {
            return false;
        };
        let mut inner = self.inner.lock().expect("transport registry poisoned");
        let Some(handles) = inner.get_mut(session) else {
            return false;
        };
        let before = handles.len();
        handles.retain(|handle, instance| {
            !captured
                .get(handle)
                .is_some_and(|old| Arc::ptr_eq(old, instance))
        });
        let removed = handles.len() != before;
        if handles.is_empty() {
            inner.remove(session);
        }
        removed
    }

    pub(crate) fn detach(&self, session: &SessionId, handle: &TransportHandle) {
        let mut inner = self.inner.lock().expect("transport registry poisoned");
        if let Some(handles) = inner.get_mut(session) {
            handles.remove(handle);
            if handles.is_empty() {
                inner.remove(session);
            }
        }
    }

    pub(crate) fn detach_all(&self, session: &SessionId) {
        self.inner
            .lock()
            .expect("transport registry poisoned")
            .remove(session);
    }

    pub(crate) fn is_present(&self, session: &SessionId) -> bool {
        self.inner
            .lock()
            .expect("transport registry poisoned")
            .get(session)
            .map(|handles| !handles.is_empty())
            .unwrap_or(false)
    }
}

/// Sole daemon-side writer for materialized harness presence columns.
#[derive(Clone)]
pub(crate) struct PresenceWriter {
    store: Arc<Store>,
    events: Arc<dyn EventSink>,
    registry: TransportRegistry,
    model_reporting: Option<Arc<ModelReporting>>,
}

/// Short-lived admission for managed identity binding and its online projection.
///
/// Private fields tie the guard to the writer that acquired it: callers cannot supply another
/// Store's guard or recursively acquire presence when materializing the admitted binding.
pub(crate) struct PresenceBindingTransition<'a> {
    writer: &'a PresenceWriter,
    transition: Arc<tokio::sync::OwnedMutexGuard<()>>,
}

enum OnlineLifecyclePolicy {
    Strict,
    ResumeBestEffort,
}

impl PresenceBindingTransition<'_> {
    pub(crate) async fn materialize_online(&self, session: &SessionId) -> Result<(), NexusError> {
        self.writer
            .materialize_online_under_transition(
                session,
                &self.transition,
                OnlineLifecyclePolicy::Strict,
            )
            .await
    }
}

impl PresenceWriter {
    /// Immutable configured ownership decides the route, never readiness or an error fallback.
    /// Caller retains this same Store's presence guard from original authorized selection.
    pub(crate) async fn purge_selected(
        &self,
        selected: SessionRow,
        agent: Option<String>,
        transition: Arc<tokio::sync::OwnedMutexGuard<()>>,
    ) -> Result<PresencePurgeOutcome, PresencePurgeError> {
        match &self.model_reporting {
            Some(reporting) => reporting
                .purge(selected, agent, transition)
                .await
                .map(PresencePurgeOutcome::Managed)
                .map_err(PresencePurgeError::Managed),
            None => {
                // Standalone compatibility keeps legacy fallback identity selection, not the
                // managed captured-agent contract. No receipt is fabricated for this route.
                let _transition = transition;
                Sessions::new(&self.store)
                    .purge_selected(&selected)
                    .await
                    .map(|()| PresencePurgeOutcome::Legacy)
                    .map_err(PresencePurgeError::Legacy)
            }
        }
    }

    /// The immutable managed/unmanaged choice is not a readiness fallback. Coordinator retention
    /// acquires its own same-store presence guard and tracks only the runtime deletion stage.
    pub(crate) async fn reap_runtime_retention(&self, cutoff: i64) -> Result<u64, NexusError> {
        match &self.model_reporting {
            Some(reporting) => reporting.reap_retention(cutoff).await,
            None => {
                AgentRuntimes::new(&self.store)
                    .reap_retention_unmanaged(cutoff)
                    .await
            }
        }
    }

    pub(crate) fn new(
        store: Arc<Store>,
        events: Arc<dyn EventSink>,
        registry: TransportRegistry,
    ) -> Self {
        Self {
            store,
            events,
            registry,
            model_reporting: None,
        }
    }

    /// Production wiring shares the boot-owned coordinator. The unmanaged constructor does not
    /// initialize observers or silently acquire background lifecycle ownership.
    pub(crate) fn with_model_reporting(mut self, reporting: Arc<ModelReporting>) -> Self {
        self.model_reporting = Some(reporting);
        self
    }

    pub(crate) fn registry(&self) -> TransportRegistry {
        self.registry.clone()
    }

    pub(crate) async fn binding_transition(&self) -> PresenceBindingTransition<'_> {
        PresenceBindingTransition {
            writer: self,
            transition: Arc::new(self.store.lock_presence_transition().await),
        }
    }

    pub(crate) async fn mark_transport_present(
        &self,
        session: &SessionId,
        handle: TransportHandle,
    ) -> Result<(), NexusError> {
        let transition = Arc::new(self.store.lock_presence_transition().await);
        self.registry.attach(session, handle);
        self.materialize_online_under_transition(
            session,
            &transition,
            OnlineLifecyclePolicy::Strict,
        )
        .await
    }

    pub(crate) async fn materialize_online(&self, session: &SessionId) -> Result<(), NexusError> {
        let transition = Arc::new(self.store.lock_presence_transition().await);
        self.materialize_online_under_transition(
            session,
            &transition,
            OnlineLifecyclePolicy::Strict,
        )
        .await
    }

    /// Resume alone permits Session lifecycle telemetry failure after its required UPDATE.
    /// Activation, runtime presence, and status-read errors still stop the caller's wakeable tail.
    pub(crate) async fn materialize_online_for_resume(
        &self,
        session: &SessionId,
    ) -> Result<(), NexusError> {
        let transition = Arc::new(self.store.lock_presence_transition().await);
        self.materialize_online_under_transition(
            session,
            &transition,
            OnlineLifecyclePolicy::ResumeBestEffort,
        )
        .await
    }

    // Every caller acquires this writer's Store guard before attachment/binding/materialization.
    // Neither the Arc nor the guard type proves Store provenance; these private callers do.
    async fn materialize_online_under_transition(
        &self,
        session: &SessionId,
        transition: &Arc<tokio::sync::OwnedMutexGuard<()>>,
        lifecycle_policy: OnlineLifecyclePolicy,
    ) -> Result<(), NexusError> {
        let sessions = Sessions::new(&self.store);
        let before = sessions.find_by_session_id(session).await?;
        let presence = if before
            .as_ref()
            .is_some_and(|row| row.presence.as_deref() == Some("busy"))
        {
            Presence::Busy
        } else {
            Presence::Online
        };
        sessions.touch_heartbeat(session).await?;
        if before
            .as_ref()
            .is_some_and(|row| row.presence.as_deref() != Some(presence_token(presence)))
        {
            match lifecycle_policy {
                OnlineLifecyclePolicy::Strict => sessions.set_presence(session, presence).await?,
                OnlineLifecyclePolicy::ResumeBestEffort => {
                    sessions
                        .set_presence_with_best_effort_lifecycle(session, presence)
                        .await?
                }
            }
        }
        let runtimes = AgentRuntimes::new(&self.store);
        if let Some(runtime) = runtimes.find_by_runtime_id(&session.0).await? {
            self.activate_existing_runtime(session, runtime.agent_id.into(), transition)
                .await?;
            runtimes.set_presence(&session.0, presence).await?;
        }
        if before
            .as_ref()
            .is_some_and(|row| row.presence.as_deref() != Some(presence_token(presence)))
        {
            self.emit_status(session, presence).await?;
        }
        Ok(())
    }

    /// Refresh a verified caller and restore an offline, non-paused row to online.
    ///
    /// Unlike daemon-owned transport attachment, authenticated CLI/MCP activity must respect an
    /// explicit pause. Runtime liveness is refreshed only for a non-paused caller, while a fleet
    /// fact is emitted only when the compatibility session actually transitions back online.
    pub(crate) async fn restore_online_on_activity(
        &self,
        session: &SessionId,
    ) -> Result<(), NexusError> {
        let transition = Arc::new(self.store.lock_presence_transition().await);
        let sessions = Sessions::new(&self.store);
        sessions.touch_heartbeat(session).await?;
        let paused = sessions
            .find_by_session_id(session)
            .await?
            .is_some_and(|row| row.paused);
        let changed = sessions.restore_online_on_activity(session).await?;
        let runtimes = AgentRuntimes::new(&self.store);
        if !paused {
            if let Some(runtime) = runtimes.find_by_runtime_id(&session.0).await? {
                self.activate_existing_runtime(session, runtime.agent_id.into(), &transition)
                    .await?;
                runtimes.mark_live(&session.0).await?;
            }
        }
        if changed {
            self.emit_status(session, Presence::Online).await?;
        }
        Ok(())
    }

    // Called only with this writer's lease and the exact agent captured by its runtime read.
    // Delegation retains the lease through cancellation, not the preceding Session writes or
    // this caller's post-activation tail. No veto is permission to repair/roll back those writes.
    async fn activate_existing_runtime(
        &self,
        session: &SessionId,
        agent_id: nexus_contracts::AgentId,
        transition: &Arc<tokio::sync::OwnedMutexGuard<()>>,
    ) -> Result<(), NexusError> {
        let Some(reporting) = &self.model_reporting else {
            return AgentRuntimes::new(&self.store)
                .set_active(&session.0, true)
                .await;
        };
        let result = reporting
            .activate_runtime(
                RuntimeActivationRequest::ActivateExisting {
                    runtime_id: session.clone(),
                    agent_id,
                },
                transition.clone(),
            )
            .await;
        let context = match result {
            Ok(SelectedRuntimeActivation::Applied(_)) => return Ok(()),
            Ok(SelectedRuntimeActivation::SelectionChanged) => {
                "runtime activation selection changed".into()
            }
            Err(RuntimeActivationError::RejectedBeforeStore { cause }) => {
                format!("{cause}; activation rejected before store")
            }
            Err(RuntimeActivationError::Store {
                failure,
                settlement_error,
            }) => {
                let mut context = format!(
                    "{failure}; activation store outcome {:?}",
                    failure.commit_state()
                );
                if let Some(error) = settlement_error {
                    context.push_str(&format!("; activation settlement failed: {error}"));
                }
                context
            }
            Err(RuntimeActivationError::AfterStore { receipt, cause }) => {
                format!("{cause}; activation store receipt {receipt:?}")
            }
            Err(RuntimeActivationError::OutcomeUnavailable { cause }) => {
                format!("{cause}; activation store outcome unavailable")
            }
        };
        Err(NexusError::Store(format!("{context}; partial presence transition: Session heartbeat/conditional presence writes may already have committed")))
    }

    pub(crate) async fn mark_transport_offline(
        &self,
        session: &SessionId,
    ) -> Result<(), NexusError> {
        self.registry.detach_all(session);
        self.materialize_offline(session).await
    }

    /// Materialize a definitely dead harness and fail closed any turn it owned at process exit.
    pub(crate) async fn mark_dead_harness_offline(
        &self,
        session: &SessionId,
    ) -> Result<(), NexusError> {
        Inbox::new(&self.store)
            .mark_recipient_injecting_outcome_unknown(
                session,
                "delivery outcome unknown because the harness exited during injection",
            )
            .await?;
        self.mark_transport_offline(session).await
    }

    pub(crate) async fn materialize_offline(&self, session: &SessionId) -> Result<(), NexusError> {
        let _transition = self.store.lock_presence_transition().await;
        if let Some(reporting) = &self.model_reporting {
            return reporting
                .materialize_offline(self.clone(), session.clone(), _transition)
                .await;
        }
        let changed = self.prepare_offline(session).await?;
        AgentRuntimes::new(&self.store).stop(&session.0).await?;
        self.finish_offline(session, changed).await
    }

    /// Runtime-only convergence shares the attachment/offline guard. Managed settlement owns
    /// that guard even if this caller is cancelled; unmanaged writers retain the repo fallback.
    pub(crate) async fn stop_stale_runtimes(
        &self,
        now_ms: i64,
        ttl_ms: i64,
    ) -> Result<(), NexusError> {
        let transition = self.store.lock_presence_transition().await;
        if let Some(reporting) = &self.model_reporting {
            return reporting
                .stop_stale_runtimes(now_ms, ttl_ms, transition)
                .await;
        }
        AgentRuntimes::new(&self.store)
            .stop_stale(now_ms, ttl_ms)
            .await
    }

    /// One immutable attachment capture covers compatibility selection and runtime-only cleanup.
    /// Managed writers share presence admission; direct repository writers still require the two
    /// independent transaction-local checks and can cause an explicitly partial transition.
    pub(crate) async fn reconcile_stale_presence(
        &self,
        now_ms: i64,
        ttl_ms: i64,
    ) -> Result<(), NexusError> {
        let snapshot = self.registry.snapshot();
        let sessions = Sessions::new(&self.store);
        let runtimes = AgentRuntimes::new(&self.store);
        let mut selected = Vec::new();
        for row in sessions.stale_online_rows(now_ms, ttl_ms).await? {
            let expected_runtime = runtimes
                .find_by_runtime_id(&row.session_id.0)
                .await?
                .map(|row| row.agent_id);
            selected.push((row, expected_runtime));
        }
        // The trailing phase is runtime-only: never retry a compatibility candidate that skipped
        // due to refresh/rebinding or confirmed false CAS using a newly selected runtime owner.
        let compatibility_ids: HashSet<_> = selected
            .iter()
            .map(|(row, _)| row.session_id.clone())
            .collect();
        for (row, expected_runtime) in selected {
            let transition = self.store.lock_presence_transition().await;
            if !self.registry.matches_captured(&row.session_id, &snapshot)
                || !self
                    .stale_selection_matches(&row, expected_runtime.as_deref(), now_ms, ttl_ms)
                    .await?
            {
                continue;
            }
            if let Some(reporting) = &self.model_reporting {
                reporting
                    .stop_stale_session(
                        self.clone(),
                        row,
                        expected_runtime,
                        snapshot.clone(),
                        now_ms,
                        ttl_ms,
                        transition,
                    )
                    .await?;
            } else {
                // Unmanaged compatibility writers have no observer task ownership.
                if sessions
                    .set_offline_if_stale_selected(
                        &row.session_id,
                        row.agent_id.as_deref(),
                        now_ms,
                        ttl_ms,
                    )
                    .await
                    .map_err(|error| {
                        NexusError::Internal(format!(
                            "partial stale transition: session write may have committed: {error}"
                        ))
                    })?
                {
                    let expected = expected_runtime.as_deref().map_or(
                        ExpectedRuntimeBinding::Missing,
                        ExpectedRuntimeBinding::Agent,
                    );
                    let stopped = runtimes.stop_if_binding_matches(&row.session_id.0, expected).await
                        .map_err(|error| NexusError::Internal(format!("partial stale transition: session committed; runtime stop failed: {error}")))?;
                    if stopped == ExactRuntimeStop::BindingChanged {
                        return Err(NexusError::Internal(
                            "partial stale transition: session committed; runtime binding changed"
                                .into(),
                        ));
                    }
                    self.registry.detach_captured(&row.session_id, &snapshot);
                    self.finish_offline(&row.session_id, true).await?;
                }
            }
        }
        let transition = self.store.lock_presence_transition().await;
        if let Some(reporting) = &self.model_reporting {
            reporting
                .stop_stale_runtimes_captured(
                    now_ms,
                    ttl_ms,
                    transition,
                    Some((self.registry.clone(), snapshot, compatibility_ids)),
                )
                .await
        } else {
            let pairs = runtimes
                .stale_active_runtime_pairs(now_ms, ttl_ms)
                .await?
                .into_iter()
                .filter(|(runtime, _)| {
                    !compatibility_ids.contains(&SessionId(runtime.clone()))
                        && self
                            .registry
                            .matches_captured(&SessionId(runtime.clone()), &snapshot)
                })
                .collect::<Vec<_>>();
            runtimes
                .stop_stale_selected(&pairs, now_ms, ttl_ms)
                .await
                .map(|_| ())
        }
    }

    /// Re-read before local admission and once more after lane acquisition. NULL is exact, not a
    /// wildcard. This mirrors the existing compatibility stale predicate without changing it.
    pub(crate) async fn stale_selection_matches(
        &self,
        selected: &SessionRow,
        runtime_agent: Option<&str>,
        now_ms: i64,
        ttl_ms: i64,
    ) -> Result<bool, NexusError> {
        let Some(current) = Sessions::new(&self.store)
            .find_by_session_id(&selected.session_id)
            .await?
        else {
            return Ok(false);
        };
        if current.agent_id != selected.agent_id
            || current.presence.as_deref().unwrap_or("offline") == "offline"
            || now_ms.saturating_sub(current.last_heartbeat.unwrap_or(current.created_at)) <= ttl_ms
        {
            return Ok(false);
        }
        let runtime = AgentRuntimes::new(&self.store)
            .find_by_runtime_id(&selected.session_id.0)
            .await?;
        Ok(runtime.as_ref().map(|row| row.agent_id.as_str()) == runtime_agent)
    }

    /// Called with the owned presence transition guard retained through `finish_offline`.
    pub(crate) async fn prepare_offline(&self, session: &SessionId) -> Result<bool, NexusError> {
        let sessions = Sessions::new(&self.store);
        let before = sessions.find_by_session_id(session).await?;
        sessions.set_presence(session, Presence::Offline).await?;
        Ok(before
            .as_ref()
            .is_some_and(|row| row.presence.as_deref() != Some("offline")))
    }

    /// Runtime stop has committed and its model lane is released before post-commit publication.
    pub(crate) async fn finish_offline(
        &self,
        session: &SessionId,
        changed: bool,
    ) -> Result<(), NexusError> {
        if !self.store.has_split_authority() {
            crate::daemon::agent_session_materializer::abort_open_turns_for_offline_sessions(
                &self.store,
                now(),
            )
            .await?;
        }
        if changed {
            self.emit_status(session, Presence::Offline).await?;
        }
        Ok(())
    }

    /// Emit the post-commit status projection for one agent session. The event carries pause state;
    /// the daemon sink enriches the ordered fleet fact with the row's name and `current_work`.
    async fn emit_status(&self, session: &SessionId, presence: Presence) -> Result<(), NexusError> {
        let Some(row) = Sessions::new(&self.store)
            .find_by_session_id(session)
            .await?
        else {
            return Ok(());
        };
        if !row.is_agent() {
            return Ok(());
        }
        self.events
            .emit(WsEvent::AgentStatus {
                session_id: session.clone(),
                presence,
                paused: row.paused,
            })
            .await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use nexus_store::repos::{Agents, NewAgent, NewAgentRuntime, NewSession};

    #[derive(Default)]
    struct RecordingSink {
        events: Mutex<Vec<WsEvent>>,
    }

    #[async_trait]
    impl EventSink for RecordingSink {
        async fn emit(&self, event: WsEvent) {
            self.events.lock().unwrap().push(event);
        }
    }

    #[test]
    fn transport_registry_tracks_multiple_handles_per_session() {
        let registry = TransportRegistry::new();
        let session = SessionId("s_transport".into());

        registry.attach(&session, TransportHandle::EventLoop);
        registry.attach(&session, TransportHandle::NativeForwarder("claude".into()));
        assert!(registry.is_present(&session));

        registry.detach(&session, &TransportHandle::EventLoop);
        assert!(registry.is_present(&session));

        registry.detach(&session, &TransportHandle::NativeForwarder("claude".into()));
        assert!(!registry.is_present(&session));
    }

    #[tokio::test]
    async fn presence_writer_materializes_offline_projection() {
        let store = Arc::new(Store::open(":memory:").await.unwrap());
        store.migrate().await.unwrap();
        let registry = TransportRegistry::new();
        let sink = Arc::new(RecordingSink::default());
        let writer = PresenceWriter::new(store.clone(), sink.clone(), registry.clone());
        let session = SessionId("s_present".into());

        Agents::new(&store)
            .create(NewAgent {
                agent_id: "a_present".into(),
                project: "default".into(),
                name: Some("present".into()),
                default_harness: Some("codex".into()),
                role: None,
                tier: Some("agent".into()),
                owner: None,
            })
            .await
            .unwrap();
        Sessions::new(&store)
            .create(NewSession {
                session_id: session.clone(),
                name: Some("present".into()),
                agent: Some("codex".into()),
                kind: "agent".into(),
                role: None,
                tier: "agent".into(),
                harness_session_id: None,
                client_key: Some("ck_present".into()),
                cwd: None,
                project: "default".into(),
                transport: Some("pty".into()),
            })
            .await
            .unwrap();
        AgentRuntimes::new(&store)
            .create(NewAgentRuntime {
                runtime_id: session.0.clone(),
                agent_id: "a_present".into(),
                harness: "codex".into(),
                cwd: None,
                transport: Some("pty".into()),
                presence: Some("online".into()),
                active: true,
            })
            .await
            .unwrap();

        writer.mark_transport_offline(&session).await.unwrap();

        assert!(!registry.is_present(&session));
        let row = Sessions::new(&store)
            .find_by_session_id(&session)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.presence.as_deref(), Some("offline"));
        let runtime = AgentRuntimes::new(&store)
            .find_by_runtime_id(&session.0)
            .await
            .unwrap()
            .unwrap();
        assert!(!runtime.active);
        assert_eq!(runtime.presence.as_deref(), Some("offline"));
        assert!(runtime.stopped_at.is_some());
        assert_eq!(
            sink.events.lock().unwrap().as_slice(),
            &[WsEvent::AgentStatus {
                session_id: session.clone(),
                presence: Presence::Offline,
                paused: false,
            }]
        );

        // Idempotent writes are not transitions and must not flood the ordered fleet lane.
        writer.mark_transport_offline(&session).await.unwrap();
        assert_eq!(sink.events.lock().unwrap().len(), 1);

        writer.materialize_online(&session).await.unwrap();
        assert_eq!(
            sink.events.lock().unwrap().last(),
            Some(&WsEvent::AgentStatus {
                session_id: session.clone(),
                presence: Presence::Online,
                paused: false,
            })
        );

        // Transport liveness is orthogonal to operator-authored activity state. The periodic
        // harness keeper must refresh heartbeat/active without erasing an explicit busy status.
        Sessions::new(&store)
            .set_presence(&session, Presence::Busy)
            .await
            .unwrap();
        AgentRuntimes::new(&store)
            .set_presence(&session.0, Presence::Busy)
            .await
            .unwrap();
        let events_before_busy_refresh = sink.events.lock().unwrap().len();
        writer.materialize_online(&session).await.unwrap();
        let busy = Sessions::new(&store)
            .find_by_session_id(&session)
            .await
            .unwrap()
            .unwrap();
        let runtime = AgentRuntimes::new(&store)
            .find_by_runtime_id(&session.0)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(busy.presence.as_deref(), Some("busy"));
        assert_eq!(runtime.presence.as_deref(), Some("busy"));
        assert!(runtime.active);
        assert_eq!(
            sink.events.lock().unwrap().len(),
            events_before_busy_refresh
        );

        Sessions::new(&store)
            .set_paused(&session, true, Some("self"))
            .await
            .unwrap();
        writer.materialize_offline(&session).await.unwrap();
        let events_before_paused_activity = sink.events.lock().unwrap().len();
        writer.restore_online_on_activity(&session).await.unwrap();
        let paused = Sessions::new(&store)
            .find_by_session_id(&session)
            .await
            .unwrap()
            .unwrap();
        let runtime = AgentRuntimes::new(&store)
            .find_by_runtime_id(&session.0)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(paused.presence.as_deref(), Some("offline"));
        assert!(!runtime.active);
        assert_eq!(runtime.presence.as_deref(), Some("offline"));
        assert_eq!(
            sink.events.lock().unwrap().len(),
            events_before_paused_activity
        );
    }
}
