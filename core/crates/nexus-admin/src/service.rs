//! The [`Admin`] service — `impl AdminPort` (backend spec §8). Every op is **guard + delegate**:
//! it first calls [`tier_guard`](nexus_identity::tier_guard) for [`Tier::Admin`], then hands off to
//! the relevant capability/domain port. Admin holds only `Arc<dyn …Port>` seams — it owns no store,
//! no message-path surface, and exposes no `send`/`enqueue`. Admin = **extra commands only, never
//! insertion into the message path** (the non-negotiable §8 guardrail; anchor §5).

use std::sync::Arc;

use async_trait::async_trait;
use nexus_contracts::admin::{
    AssignProjectRequest, AssignProjectResponse, AssignRoleRequest, AssignRoleResponse,
    ChannelRequest, MonitorRequest, RemoveRequest, RemoveResponse, RouteForwardRequest,
    SpawnRequest, SpawnResponse,
};
use nexus_contracts::ports::{
    AdminPort, AgentTurnExecutionPort, Caller, EventSink, IdentityPort, NotifyPort, PortResult,
};

use crate::{channel, monitor, project, remove, roles, route, spawn};

/// The admin command service. Constructed by the `nexus` binary's `AppState` with the live port
/// handles; consumed as `Arc<dyn AdminPort>`.
///
/// Delegation map (each op tier-guards first):
/// - `spawn`  → [`AgentTurnExecutionPort::launch`]
/// - `remove` → [`AgentTurnExecutionPort::remove`]
/// - `assign_role` → display-label echo (roles are cosmetic; no functional routing)
/// - `channel` → [`NotifyPort::channel`]
/// - `route`   → [`NotifyPort::forward`] (one-shot; the only path-adjacent op, still a guardrail)
/// - `monitor` → observe-only oversight over the [`EventSink`] feed (writes nothing)
pub struct Admin {
    identity: Arc<dyn IdentityPort>,
    agent: Arc<dyn AgentTurnExecutionPort>,
    notify: Arc<dyn NotifyPort>,
    events: Arc<dyn EventSink>,
}

impl Admin {
    /// Wire the admin service from its port seams. No store, no message-path handle by construction.
    pub fn new(
        identity: Arc<dyn IdentityPort>,
        agent: Arc<dyn AgentTurnExecutionPort>,
        notify: Arc<dyn NotifyPort>,
        events: Arc<dyn EventSink>,
    ) -> Self {
        Self {
            identity,
            agent,
            notify,
            events,
        }
    }
}

#[async_trait]
impl AdminPort for Admin {
    async fn spawn(&self, caller: &Caller, req: SpawnRequest) -> PortResult<SpawnResponse> {
        spawn::spawn(&self.agent, caller, req).await
    }

    async fn remove(&self, caller: &Caller, req: RemoveRequest) -> PortResult<RemoveResponse> {
        remove::remove(&self.agent, caller, req).await
    }

    async fn assign_role(
        &self,
        caller: &Caller,
        req: AssignRoleRequest,
    ) -> PortResult<AssignRoleResponse> {
        roles::assign_role(&self.identity, caller, req).await
    }

    async fn assign_project(
        &self,
        caller: &Caller,
        req: AssignProjectRequest,
    ) -> PortResult<AssignProjectResponse> {
        project::assign_project(&self.identity, caller, req).await
    }

    async fn channel(&self, caller: &Caller, req: ChannelRequest) -> PortResult<()> {
        channel::channel(&self.notify, caller, req).await
    }

    async fn route(&self, caller: &Caller, req: RouteForwardRequest) -> PortResult<()> {
        route::route(&self.notify, caller, req).await
    }

    async fn monitor(&self, caller: &Caller, req: MonitorRequest) -> PortResult<()> {
        monitor::monitor(&self.events, caller, req).await
    }
}
