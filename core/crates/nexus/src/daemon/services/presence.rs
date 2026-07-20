//! Connection-owned presence support.
//!
//! Presence truth for daemon-owned harnesses is the live transport registry. The durable
//! `sessions.presence` and `agent_runtimes.presence` columns are materialized projections written
//! through [`PresenceWriter`]. Every effective online/offline transition emits one `agent.status`
//! fact after the store converges; [`WsSink`](crate::daemon::app::WsSink) orders those facts on the
//! ephemeral `sys.fleet.status` lane. CLI/MCP peers remain heartbeat-derived because they have no
//! daemon transport handle.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use nexus_common::presence::presence_token;
use nexus_common::{now, NexusError};
use nexus_contracts::{EventSink, Presence, SessionId, WsEvent};
use nexus_store::repos::{AgentRuntimes, Inbox, Sessions};
use nexus_store::Store;

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
    inner: Arc<Mutex<HashMap<SessionId, BTreeSet<TransportHandle>>>>,
}

impl TransportRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn attach(&self, session: &SessionId, handle: TransportHandle) {
        self.inner
            .lock()
            .expect("transport registry poisoned")
            .entry(session.clone())
            .or_default()
            .insert(handle);
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
}

impl PresenceWriter {
    pub(crate) fn new(
        store: Arc<Store>,
        events: Arc<dyn EventSink>,
        registry: TransportRegistry,
    ) -> Self {
        Self {
            store,
            events,
            registry,
        }
    }

    pub(crate) fn registry(&self) -> TransportRegistry {
        self.registry.clone()
    }

    pub(crate) async fn mark_transport_present(
        &self,
        session: &SessionId,
        handle: TransportHandle,
    ) -> Result<(), NexusError> {
        self.registry.attach(session, handle);
        self.materialize_online(session).await
    }

    pub(crate) async fn materialize_online(&self, session: &SessionId) -> Result<(), NexusError> {
        let _transition = self.store.lock_presence_transition().await;
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
            sessions.set_presence(session, presence).await?;
        }
        let runtimes = AgentRuntimes::new(&self.store);
        if runtimes.find_by_runtime_id(&session.0).await?.is_some() {
            runtimes.set_active(&session.0, true).await?;
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
        let _transition = self.store.lock_presence_transition().await;
        let sessions = Sessions::new(&self.store);
        sessions.touch_heartbeat(session).await?;
        let paused = sessions
            .find_by_session_id(session)
            .await?
            .is_some_and(|row| row.paused);
        let changed = sessions.restore_online_on_activity(session).await?;
        let runtimes = AgentRuntimes::new(&self.store);
        if !paused && runtimes.find_by_runtime_id(&session.0).await?.is_some() {
            runtimes.set_active(&session.0, true).await?;
            runtimes.mark_live(&session.0).await?;
        }
        if changed {
            self.emit_status(session, Presence::Online).await?;
        }
        Ok(())
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
        let sessions = Sessions::new(&self.store);
        let before = sessions.find_by_session_id(session).await?;
        sessions.set_presence(session, Presence::Offline).await?;
        AgentRuntimes::new(&self.store).stop(&session.0).await?;
        if !self.store.has_split_authority() {
            crate::daemon::agent_session_materializer::abort_open_turns_for_offline_sessions(
                &self.store,
                now(),
            )
            .await?;
        }
        if before
            .as_ref()
            .is_some_and(|row| row.presence.as_deref() != Some("offline"))
        {
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
