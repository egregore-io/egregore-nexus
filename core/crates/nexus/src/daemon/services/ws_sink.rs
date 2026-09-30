//! Daemon event-sink projection and gateway publication.

use std::sync::Arc;

use async_trait::async_trait;
use libsql::params;
use sha2::{Digest, Sha256};
use tokio::sync::broadcast;

use nexus_contracts::{EventSink, GatewayProjectionEffect, Notification, WsEvent, JSONRPC_VERSION};
use nexus_store::repos::Sessions;
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
}

impl WsSink {
    /// New sink with a bounded broadcast buffer and an optional durable store for stream capture.
    pub fn new(capacity: usize, store: Option<Arc<Store>>) -> Self {
        let (tx, _rx) = broadcast::channel(capacity);
        WsSink {
            tx,
            gateway_stream: None,
            store,
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
                    Some(store) => Sessions::new(store)
                        .find_by_session_id(session_id)
                        .await
                        .ok()
                        .flatten(),
                    None => None,
                };
                if row.as_ref().is_some_and(|row| row.kind != "agent") {
                    return None;
                }
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
                    Some(store) => Sessions::new(store)
                        .find_by_session_id(session_id)
                        .await
                        .ok()
                        .flatten(),
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
                name,
                agent_id,
            } => {
                let mut effects = Vec::new();
                if let Some(agent_id) = agent_id.as_deref() {
                    let payload = match store
                        .conn
                        .query(
                            "SELECT agent_id, name, project, default_harness, role, tier, owner, \
                             created_at, updated_at FROM agents WHERE agent_id = ?1 LIMIT 1",
                            params![agent_id],
                        )
                        .await
                    {
                        Ok(mut rows) => match rows.next().await {
                            Ok(Some(row)) => serde_json::json!({
                                "agentId": row.get::<String>(0).ok(),
                                "name": row.get::<Option<String>>(1).ok().flatten(),
                                "project": row.get::<String>(2).ok(),
                                "defaultHarness": row.get::<Option<String>>(3).ok().flatten(),
                                "role": row.get::<Option<String>>(4).ok().flatten(),
                                "tier": row.get::<Option<String>>(5).ok().flatten(),
                                "owner": row.get::<Option<String>>(6).ok().flatten(),
                                "createdAt": row.get::<i64>(7).ok(),
                                "updatedAt": row.get::<i64>(8).ok(),
                            }),
                            _ => serde_json::json!({
                                "agentId": agent_id,
                                "name": name,
                            }),
                        },
                        Err(_) => serde_json::json!({
                            "agentId": agent_id,
                            "name": name,
                        }),
                    };
                    effects.push(lifecycle_effect(
                        "identity",
                        nexus_contracts::GatewayProjectionKind::IdentityUpserted,
                        payload,
                    ));
                }
                let runtime_payload = match store
                    .conn
                    .query(
                        "SELECT runtime_id, agent_id, harness, cwd, transport, presence, active, \
                         started_at, stopped_at, native_thread_id FROM agent_runtimes \
                         WHERE runtime_id = ?1 LIMIT 1",
                        params![session_id.0.clone()],
                    )
                    .await
                {
                    Ok(mut rows) => match rows.next().await {
                        Ok(Some(row)) => serde_json::json!({
                            "runtimeId": row.get::<String>(0).ok(),
                            "agentId": row.get::<String>(1).ok(),
                            "harness": row.get::<String>(2).ok(),
                            "cwd": row.get::<Option<String>>(3).ok().flatten(),
                            "transport": row.get::<Option<String>>(4).ok().flatten(),
                            "presence": row.get::<Option<String>>(5).ok().flatten(),
                            "active": row.get::<i64>(6).ok().map(|v| v != 0),
                            "startedAt": row.get::<i64>(7).ok(),
                            "stoppedAt": row.get::<Option<i64>>(8).ok().flatten(),
                            "nativeResumeKey": row.get::<Option<String>>(9).ok().flatten(),
                            "name": name,
                        }),
                        _ => serde_json::json!({
                            "runtimeId": session_id.0,
                            "agentId": agent_id,
                            "name": name,
                        }),
                    },
                    Err(_) => serde_json::json!({
                        "runtimeId": session_id.0,
                        "agentId": agent_id,
                        "name": name,
                    }),
                };
                effects.push(lifecycle_effect(
                    "runtime",
                    nexus_contracts::GatewayProjectionKind::RuntimeUpserted,
                    runtime_payload,
                ));
                effects
            }
            WsEvent::AgentStatus {
                session_id,
                presence,
                paused,
            } => vec![lifecycle_effect(
                "presence",
                nexus_contracts::GatewayProjectionKind::PresenceChanged,
                serde_json::json!({
                    "runtimeId": session_id.0,
                    "presence": presence,
                    "paused": paused,
                }),
            )],
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
        event_id: format!("{prefix}:{digest:x}"),
        occurred_at: nexus_common::now(),
        kind,
        payload,
    }
}

#[async_trait]
impl EventSink for WsSink {
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
        // FLEET STATUS → GATEWAY PUSH: presence/spawn/remove changes also ride the ephemeral
        // `sys.fleet.status` push topic so the web console gets realtime agent status without
        // polling (frontend spec §3.4 — WS-driven presence). In-memory only; the durable
        // `sys.agent.lifecycle` rows remain the record.
        if let Some(gateway_stream) = &self.gateway_stream {
            if let Some(observation) = self.fleet_status_observation(&event).await {
                gateway_stream.publish_fleet_status(observation);
            }
        }
        // Keep the established fleet-status frame first for existing realtime consumers, then
        // append the durable Gateway projection derived from the same committed lifecycle fact.
        for effect in self.lifecycle_projection_effects(&event).await {
            self.project(effect).await;
        }
        // Broadcast is best-effort: a send with zero live receivers is not an error.
        let _ = self.tx.send(WsSink::to_notification(&event));
    }
}
