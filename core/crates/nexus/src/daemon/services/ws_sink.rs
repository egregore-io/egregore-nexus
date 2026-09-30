//! Daemon event-sink projection and gateway publication.

use std::sync::Arc;

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use tokio::sync::broadcast;
use uuid::Uuid;

use nexus_contracts::{EventSink, GatewayProjectionEffect, Notification, WsEvent, JSONRPC_VERSION};
use nexus_store::repos::{
    AgentAccessGrants, AgentRuntimes, Agents, AppendOutcome, ChildStreamBounds, ChildStreamEvents,
    DaemonState, IdentitySessions, Sessions,
};
use nexus_store::Store;

use crate::daemon::transcript_archive::archive_codex_once;

/// The daemon's [`EventSink`]. Every service emits `WsEvent`s through this; the sink buffers live
/// session activity in the volatile store, materializes finalized `/agent` history rows at turn
/// end, and publishes in-process notifications for local observers. Cheap to clone (the channel
/// sender is shared).
#[derive(Clone)]
pub struct WsSink {
    tx: broadcast::Sender<Notification>,
    gateway_stream: Option<crate::daemon::gateway_stream_socket::GatewayStreamPublisher>,
    /// Store for the projected ACP stream. The sink appends every `agent.update` to volatile
    /// `mem.stream_events`, then materializes compact durable `/agent` history rows on `turn_end`.
    /// `None` on the mock/test seam (no DB) means broadcast-only, no persistence.
    store: Option<Arc<Store>>,
    /// Per-owner-session bounds of the volatile child lane (`mem.child_stream_events`).
    child_stream_bounds: ChildStreamBounds,
    /// The daemon boot epoch, captured once from the store's daemon state. Child lane rows are
    /// recorded only under this captured authority, never under a placeholder.
    boot_epoch: Arc<std::sync::OnceLock<String>>,
}

impl WsSink {
    /// New sink with a bounded broadcast buffer and an optional durable store for stream capture.
    pub fn new(capacity: usize, store: Option<Arc<Store>>) -> Self {
        let (tx, _rx) = broadcast::channel(capacity);
        WsSink {
            tx,
            gateway_stream: None,
            store,
            child_stream_bounds: ChildStreamBounds::default(),
            boot_epoch: Arc::new(std::sync::OnceLock::new()),
        }
    }

    /// Replace the default child lane bounds with the daemon's configured values.
    pub fn with_child_stream_bounds(mut self, bounds: ChildStreamBounds) -> Self {
        self.child_stream_bounds = bounds;
        self
    }

    /// The boot epoch this daemon recorded, captured once. `None` until the daemon state holds
    /// one (or when the read fails): the child lane then records nothing rather than something
    /// under a reusable placeholder.
    async fn captured_boot_epoch(&self, store: &Store) -> Option<String> {
        if let Some(epoch) = self.boot_epoch.get() {
            return Some(epoch.clone());
        }
        match DaemonState::new(store).boot_epoch().await {
            Ok(Some(epoch)) if !epoch.is_empty() => {
                let _ = self.boot_epoch.set(epoch.clone());
                Some(epoch)
            }
            Ok(_) => None,
            Err(e) => {
                tracing::warn!(
                    target: "nexus::child_streams",
                    error = %e,
                    "boot epoch read failed; child stream update not recorded"
                );
                None
            }
        }
    }

    /// Attach the daemon-to-gateway push publisher. The stream store remains authoritative; this
    /// publisher only emits frames after a row id exists.
    pub fn with_gateway_stream(
        mut self,
        gateway_stream: crate::daemon::gateway_stream_socket::GatewayStreamPublisher,
    ) -> Self {
        self.gateway_stream = Some(gateway_stream);
        self
    }

    /// Return the optional daemon-to-gateway publisher used for side-channel developer events.
    pub(crate) fn gateway_stream_publisher(
        &self,
    ) -> Option<crate::daemon::gateway_stream_socket::GatewayStreamPublisher> {
        self.gateway_stream.clone()
    }

    /// Subscribe a local observer to the in-process notification stream.
    pub fn subscribe(&self) -> broadcast::Receiver<Notification> {
        self.tx.subscribe()
    }

    /// Wrap a [`WsEvent`] into a JSON-RPC [`Notification`] (dotted `type` → `method`).
    fn to_notification(event: &WsEvent) -> Notification {
        let value = serde_json::to_value(event).unwrap_or(serde_json::Value::Null);
        let method = value
            .get("type")
            .and_then(|t| t.as_str())
            .unwrap_or("unknown")
            .to_string();
        Notification {
            jsonrpc: JSONRPC_VERSION.to_string(),
            method,
            params: Some(value),
        }
    }

    /// Project one authoritative agent lifecycle event onto the ordered fleet-status lane.
    ///
    /// Status events intentionally carry only the stable wire fields. When the daemon store is
    /// available, enrich the fleet fact after the mutation commits so subscribers receive the
    /// current display name and activity (`current_work`) from the same authoritative row.
    async fn fleet_status_observation(
        &self,
        event: &WsEvent,
    ) -> Option<crate::daemon::services::fleet_events::FleetStatusObservation> {
        use crate::daemon::services::fleet_events::FleetStatusObservation;

        match event {
            WsEvent::AgentStatus {
                session_id,
                presence,
                paused,
            } => {
                let row = match &self.store {
                    Some(store) => {
                        match Sessions::new(store).find_by_session_id(session_id).await {
                            Ok(Some(row)) if row.is_agent() => Some(row),
                            Ok(Some(_)) | Ok(None) | Err(_) => return None,
                        }
                    }
                    None => None,
                };
                Some(FleetStatusObservation {
                    lifecycle: "status",
                    agent: row.as_ref().and_then(|row| row.name.clone()),
                    session_id: session_id.clone(),
                    current_work: row.as_ref().and_then(|row| row.current_work.clone()),
                    data: Some(serde_json::json!({
                        "presence": presence,
                        "paused": paused,
                    })),
                })
            }
            WsEvent::AgentSpawned {
                session_id,
                name,
                agent_id,
            } => {
                let row = match &self.store {
                    Some(store) => {
                        match Sessions::new(store).find_by_session_id(session_id).await {
                            Ok(Some(row)) if row.is_agent() => Some(row),
                            Ok(Some(_)) | Ok(None) | Err(_) => return None,
                        }
                    }
                    None => None,
                };
                Some(FleetStatusObservation {
                    lifecycle: "spawned",
                    agent: name
                        .clone()
                        .or_else(|| row.as_ref().and_then(|row| row.name.clone())),
                    session_id: session_id.clone(),
                    current_work: row.as_ref().and_then(|row| row.current_work.clone()),
                    data: Some(serde_json::json!({
                        "agentId": agent_id,
                        "presence": row.as_ref().and_then(|row| row.presence.clone()),
                        "paused": row.as_ref().map(|row| row.paused).unwrap_or(false),
                    })),
                })
            }
            WsEvent::AgentRemoved { session_id, name } => Some(FleetStatusObservation {
                lifecycle: "removed",
                agent: name.clone(),
                session_id: session_id.clone(),
                current_work: None,
                data: None,
            }),
            _ => None,
        }
    }

    async fn lifecycle_projection_effects(&self, event: &WsEvent) -> Vec<GatewayProjectionEffect> {
        let Some(store) = &self.store else {
            return Vec::new();
        };
        match event {
            WsEvent::AgentSpawned {
                session_id,
                name: _,
                agent_id,
            } => {
                let session = match Sessions::new(store).find_by_session_id(session_id).await {
                    Ok(Some(row)) if row.is_agent() => row,
                    Ok(Some(_)) | Ok(None) => return Vec::new(),
                    Err(error) => {
                        tracing::warn!(
                            target: "nexus::gateway_projection",
                            session_id = %session_id,
                            %error,
                            "failed to load lifecycle projection session kind"
                        );
                        return Vec::new();
                    }
                };
                let Some(agent_id) = agent_id.as_deref() else {
                    return Vec::new();
                };
                if session.agent_id.as_deref() != Some(agent_id) {
                    return Vec::new();
                }
                let agent = match Agents::new(store).find_by_id(agent_id).await {
                    Ok(Some(row)) => row,
                    Ok(None) => return Vec::new(),
                    Err(error) => {
                        tracing::warn!(
                            target: "nexus::gateway_projection",
                            agent_id,
                            %error,
                            "failed to load identity projection payload"
                        );
                        return Vec::new();
                    }
                };
                let access_grants =
                    match AgentAccessGrants::new(store).list_for_agent(agent_id).await {
                        Ok(rows) => rows
                            .into_iter()
                            .map(|row| {
                                serde_json::json!({
                                    "principalProject": row.principal_project,
                                    "principalName": row.principal_name,
                                    "principalSessionId": row.principal_session_id,
                                    "principalAgentId": row.principal_agent_id,
                                    "role": row.role,
                                })
                            })
                            .collect::<Vec<_>>(),
                        Err(error) => {
                            tracing::warn!(
                                target: "nexus::gateway_projection",
                                agent_id,
                                %error,
                                "failed to load identity access grants"
                            );
                            return Vec::new();
                        }
                    };
                // Native resurrection metadata enriches the descriptor when available, but it is
                // not part of the canonical agent/runtime ownership proof (compatibility stores
                // predating split authority may not expose this table at all).
                let capsule = IdentitySessions::new(store)
                    .find(&session_id.0)
                    .await
                    .ok()
                    .flatten();
                let runtime = match AgentRuntimes::new(store)
                    .find_by_runtime_id(&session_id.0)
                    .await
                {
                    Ok(Some(row)) if row.agent_id == agent_id => row,
                    Ok(Some(_)) | Ok(None) => return Vec::new(),
                    Err(error) => {
                        tracing::warn!(
                            target: "nexus::gateway_projection",
                            runtime_id = %session_id,
                            %error,
                            "failed to load runtime projection payload"
                        );
                        return Vec::new();
                    }
                };
                let canonical_name = agent.name.clone();
                let identity_payload = serde_json::json!({
                    "agentId": agent.agent_id,
                    "name": canonical_name,
                    "kind": session.kind,
                    "project": agent.project,
                    "defaultHarness": agent.default_harness,
                    "role": agent.role,
                    "tier": agent.tier,
                    "owner": agent.owner_name,
                    "ownerProject": agent.owner_project,
                    "ownerSessionId": agent.owner_session_id,
                    "ownerAgentId": agent.owner_agent_id,
                    "accessGrants": access_grants,
                    "disabledAt": agent.disabled_at,
                    "createdAt": agent.created_at,
                });
                let mut runtime_payload = serde_json::json!({
                    "runtimeId": runtime.runtime_id,
                    "sessionId": session_id.0,
                    "agentId": runtime.agent_id,
                    "kind": session.kind,
                    "harness": runtime.harness,
                    "mode": capsule.as_ref().map(|row| row.mode.as_str()),
                    "backend": capsule.as_ref().and_then(|row| row.backend.as_deref()),
                    "cwd": runtime.cwd,
                    "transport": runtime.transport,
                    "presence": runtime.presence,
                    "active": runtime.active,
                    "startedAt": runtime.started_at,
                    "stoppedAt": runtime.stopped_at,
                    "nativeResumeKey": capsule
                        .as_ref()
                        .and_then(|row| row.native_resume_key.as_deref()),
                    "name": canonical_name,
                });
                // The report and its durable revision come from the same exact-owner runtime
                // read above. Omit legacy absence; never expose the private observer token.
                if let Some(report) = runtime.model_report {
                    runtime_payload["modelReport"] = serde_json::json!(report);
                }
                vec![
                    lifecycle_effect(
                        "identity",
                        nexus_contracts::GatewayProjectionKind::IdentityUpserted,
                        identity_payload,
                    ),
                    lifecycle_effect(
                        "runtime",
                        nexus_contracts::GatewayProjectionKind::RuntimeUpserted,
                        runtime_payload,
                    ),
                ]
            }
            WsEvent::AgentStatus {
                session_id,
                presence,
                paused,
            } => {
                let session = match Sessions::new(store).find_by_session_id(session_id).await {
                    Ok(Some(row)) if row.is_agent() && row.agent_id.is_some() => row,
                    Ok(Some(_)) | Ok(None) | Err(_) => return Vec::new(),
                };
                let runtime = match AgentRuntimes::new(store)
                    .find_by_runtime_id(&session_id.0)
                    .await
                {
                    Ok(Some(row)) if Some(row.agent_id.as_str()) == session.agent_id.as_deref() => {
                        row
                    }
                    Ok(Some(_)) | Ok(None) | Err(_) => return Vec::new(),
                };
                vec![lifecycle_effect(
                    "presence",
                    nexus_contracts::GatewayProjectionKind::PresenceChanged,
                    serde_json::json!({
                        "runtimeId": runtime.runtime_id,
                        "presence": presence,
                        "paused": paused,
                    }),
                )]
            }
            WsEvent::AgentRemoved { session_id, name } => vec![lifecycle_effect(
                "runtime",
                nexus_contracts::GatewayProjectionKind::RuntimeStopped,
                serde_json::json!({
                    "runtimeId": session_id.0,
                    "name": name,
                    "stopped": true,
                }),
            )],
            _ => Vec::new(),
        }
    }
}

fn lifecycle_effect(
    prefix: &str,
    kind: nexus_contracts::GatewayProjectionKind,
    payload: serde_json::Value,
) -> GatewayProjectionEffect {
    let canonical = serde_json::to_vec(&payload).unwrap_or_default();
    let digest = Sha256::digest(canonical);
    GatewayProjectionEffect {
        // Repeating a previously-seen state is still a new committed lifecycle fact. A payload
        // digest alone would suppress transitions such as grant -> revoke back to an empty ACL.
        event_id: format!("{prefix}:{}:{digest:x}", Uuid::new_v4().simple()),
        occurred_at: nexus_common::now(),
        kind,
        payload,
    }
}

#[async_trait]
impl EventSink for WsSink {
    async fn project_runtime_binding(
        &self,
        session: &nexus_contracts::SessionId,
        agent: &nexus_contracts::AgentId,
    ) {
        // Reuse the exact canonical builder, but do not emit a duplicate spawn notification
        // or ephemeral fleet event. Missing/mismatched store authority yields no projection.
        let binding = WsEvent::AgentSpawned {
            session_id: session.clone(),
            name: None,
            agent_id: Some(agent.0.clone()),
        };
        for effect in self.lifecycle_projection_effects(&binding).await {
            self.project(effect).await;
        }
    }

    async fn project(&self, effect: GatewayProjectionEffect) {
        if let Some(gateway_stream) = &self.gateway_stream {
            gateway_stream.publish_projection(
                effect.event_id,
                effect.kind,
                effect.occurred_at,
                effect.payload,
            );
        }
    }

    async fn emit(&self, event: WsEvent) {
        // CHILD STREAMS → BOUNDED LANE: a harness-attributed native child update never touches
        // the parent's stream lane, the gateway projection, the materializer or the archive. It
        // is appended to the volatile child lane under the current boot epoch, then broadcast
        // like any other event. Refusals and store failures are logged, never redirected.
        if let (
            Some(store),
            WsEvent::ChildAgentUpdate {
                session_id,
                child,
                kind,
                source_ref,
                data,
            },
        ) = (&self.store, &event)
        {
            let kind_str = serde_json::to_value(kind)
                .ok()
                .and_then(|v| v.as_str().map(str::to_string))
                .unwrap_or_default();
            match self.captured_boot_epoch(store).await {
                None => {
                    tracing::warn!(
                        target: "nexus::child_streams",
                        session = %session_id, harness = %child.harness, root = %child.root,
                        "child stream update not recorded: boot epoch unavailable (live broadcast still sent)"
                    );
                }
                Some(epoch) => match ChildStreamEvents::new(store)
                    .append(
                        session_id,
                        &epoch,
                        child,
                        &kind_str,
                        source_ref,
                        &data.to_string(),
                        &self.child_stream_bounds,
                    )
                    .await
                {
                    Ok(AppendOutcome::Stored(_)) => {}
                    Ok(AppendOutcome::Refused(by)) => {
                        tracing::warn!(
                            target: "nexus::child_streams",
                            session = %session_id, harness = %child.harness, root = %child.root,
                            locator = %child.locator, refused_by = ?by,
                            "child stream update refused by a lane bound (live broadcast still sent)"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            target: "nexus::child_streams",
                            session = %session_id, error = %e,
                            "failed to buffer child_agent.update in mem.child_stream_events (live broadcast still sent)"
                        );
                    }
                },
            }
        }
        // STREAM → STORE: append every `agent.update` to the volatile stream lane, then fold a
        // finalized turn into `/agent` history at `turn_end`. Failures are logged but do not block
        // live delivery.
        if let (
            Some(store),
            WsEvent::AgentUpdate {
                session_id,
                kind,
                data,
            },
        ) = (&self.store, &event)
        {
            let kind_str = serde_json::to_value(kind)
                .ok()
                .and_then(|v| v.as_str().map(str::to_string))
                .unwrap_or_default();
            match nexus_store::repos::StreamEvents::new(store)
                .append(session_id, &kind_str, &data.to_string())
                .await
            {
                Ok(stream_event_id) => {
                    if let Some(gateway_stream) = &self.gateway_stream {
                        gateway_stream.publish_agent_update(
                            session_id,
                            stream_event_id,
                            *kind,
                            data.clone(),
                        );
                    }
                    if !store.has_split_authority() {
                        if let Err(e) =
                            crate::daemon::agent_session_materializer::materialize_agent_update(
                                store,
                                session_id,
                                stream_event_id,
                                *kind,
                                data,
                            )
                            .await
                        {
                            tracing::warn!(
                                target: "nexus::agent_session_materializer",
                                session = %session_id, stream_event_id, error = %e,
                                "failed to materialize agent.update (live broadcast still sent)"
                            );
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        target: "nexus::stream_events",
                        session = %session_id, error = %e,
                        "failed to buffer agent.update in mem.stream_events (live broadcast still sent)"
                    );
                }
            }
            if *kind == nexus_contracts::AgentUpdateKind::TurnEnd && !store.has_split_authority() {
                let store = store.clone();
                let session_id = session_id.clone();
                tokio::spawn(async move {
                    if let Err(error) =
                        archive_codex_once(store, &session_id, Some("Stop".to_string())).await
                    {
                        tracing::warn!(
                            target: "nexus::transcript_archive",
                            session = %session_id,
                            error = %error,
                            "Codex rollout archive lifecycle pass failed"
                        );
                    }
                });
            }
        }
        let lifecycle_projection_effects = self.lifecycle_projection_effects(&event).await;
        let lifecycle_projection_ready = !matches!(
            event,
            WsEvent::AgentSpawned { .. }
                | WsEvent::AgentStatus { .. }
                | WsEvent::AgentRemoved { .. }
        ) || !lifecycle_projection_effects.is_empty();
        // FLEET STATUS → GATEWAY PUSH: presence/spawn/remove changes also ride the ephemeral
        // `sys.fleet.status` push topic so the web console gets realtime agent status without
        // polling (frontend spec §3.4 — WS-driven presence). In-memory only; the durable
        // `sys.agent.lifecycle` rows remain the record.
        if lifecycle_projection_ready {
            if let Some(gateway_stream) = &self.gateway_stream {
                if let Some(observation) = self.fleet_status_observation(&event).await {
                    gateway_stream.publish_fleet_status(observation);
                }
            }
        }
        // Keep the established fleet-status frame first for existing realtime consumers, then
        // append the durable Gateway projection derived from the same committed lifecycle fact.
        for effect in lifecycle_projection_effects {
            self.project(effect).await;
        }
        // Broadcast is best-effort: a send with zero live receivers is not an error.
        let _ = self.tx.send(WsSink::to_notification(&event));
    }
}
