//! Store-backed command intent kind names.
//!
//! Producers write these dotted kind strings into `command_intents.kind`; the daemon worker claims
//! rows and dispatches by the same constants. Keeping the names in one crate prevents CLI, MCP,
//! gateway, and daemon workers from growing string-literal drift now that command intents are the
//! shared control handoff.

/// Message Post command kinds.
pub mod message_post {
    /// Submit a [`nexus_contracts::SendRequest`] and receive a [`nexus_contracts::Ack`].
    pub const SEND: &str = "message.post.send";
}

/// Freeform core-entity metadata command kinds.
pub mod metadata {
    /// Replace a core entity's opaque metadata JSON bag.
    pub const SET: &str = "metadata.set";
}

/// Notification ingress command kinds.
pub mod notification {
    /// Submit a [`nexus_contracts::NotifyRequest`] through the daemon notification registry.
    pub const NOTIFY: &str = "notification.notify";
    /// Submit one explicitly targeted [`nexus_contracts::NotifySendRequest`].
    pub const SEND: &str = "notification.send";
}

/// Inbox delivery command kinds.
pub mod inbox {
    /// Drain pending inbox rows into a [`nexus_contracts::NexusBatch`].
    pub const CONSUME: &str = "inbox.consume";
    /// Register a daemon-tracked durable inbox subscription.
    pub const SUBSCRIBE: &str = "inbox.subscribe";
    /// Fetch the next durable inbox subscription batch.
    pub const SUBSCRIPTION_NEXT: &str = "inbox.subscription_next";
    /// Mark one durable inbox subscription batch consumed.
    pub const SUBSCRIPTION_ACK: &str = "inbox.subscription_ack";
    /// Disable a daemon-tracked durable inbox subscription.
    pub const UNSUBSCRIBE: &str = "inbox.unsubscribe";
    /// Ack one direct-message delivery via [`nexus_contracts::AckRequest`].
    pub const ACK: &str = "inbox.ack";
    /// Ack thread deliveries via [`nexus_contracts::AckThreadsRequest`].
    pub const ACK_THREADS: &str = "inbox.ack_threads";
}

/// Identity command kinds.
pub mod identity {
    /// Register or resume an identity from [`nexus_contracts::RegisterRequest`].
    pub const REGISTER: &str = "identity.register";
    /// Record a metadata-only terminal attach lifecycle event for an agent session.
    pub const ATTACH: &str = "identity.attach";
    /// Rename the caller from [`nexus_contracts::RenameRequest`].
    pub const RENAME: &str = "identity.rename";
}

/// Presence command kinds.
pub mod presence {
    /// Update caller presence/current work.
    pub const STATUS: &str = "presence.status";
    /// Record a caller heartbeat.
    pub const HEARTBEAT: &str = "presence.heartbeat";
}

/// Topic subscription command kinds.
pub mod topic {
    /// Subscribe the caller to a topic.
    pub const SUBSCRIBE: &str = "topic.subscribe";
    /// Unsubscribe the caller from a topic.
    pub const UNSUBSCRIBE: &str = "topic.unsubscribe";
}

/// Thread membership command kinds.
pub mod thread {
    /// Create a named thread.
    pub const CREATE: &str = "thread.create";
    /// Join an existing named thread.
    pub const JOIN: &str = "thread.join";
    /// Leave an existing named thread.
    pub const LEAVE: &str = "thread.leave";
    /// Archive a named thread.
    pub const ARCHIVE: &str = "thread.archive";
    /// Delete a named thread registry and memberships.
    pub const DELETE: &str = "thread.delete";
    /// Rename a named thread.
    pub const RENAME: &str = "thread.rename";
    /// Add one member to a named thread.
    pub const ADD_MEMBER: &str = "thread.add_member";
    /// Remove one member from a named thread.
    pub const REMOVE_MEMBER: &str = "thread.remove_member";
}

/// Harness lifecycle command kinds.
pub mod harness {
    /// Launch a harness runtime.
    pub const LAUNCH: &str = "harness.launch";
    /// Remove a harness runtime.
    pub const REMOVE: &str = "harness.remove";
    /// Revive or resume a harness runtime.
    pub const REVIVE: &str = "harness.revive";
    /// Inject one operator prompt into an existing or revived harness runtime.
    pub const PROMPT: &str = "harness.prompt";
    /// Explicitly steer a native Codex turn without entering the normal prompt boundary queue.
    pub const STEER: &str = "harness.steer";
    /// Interrupt the active turn without injecting replacement input.
    pub const INTERRUPT: &str = "harness.interrupt";
    /// Pre-warm a harness runtime without injecting a prompt.
    pub const WARM: &str = "harness.warm";
    /// Trigger native context compaction on a harness session.
    pub const COMPACT: &str = "harness.compact";
}

/// Admin command kinds.
pub mod admin {
    /// Spawn an agent runtime.
    pub const SPAWN: &str = "admin.spawn";
    /// Remove an agent runtime.
    pub const REMOVE: &str = "admin.remove";
    /// Evict an agent from routing/thread membership.
    pub const EVICT: &str = "admin.evict";
    /// Delete an agent identity and runtime state.
    pub const DELETE: &str = "admin.delete";
    /// Rename or first-name an agent identity.
    pub const RENAME: &str = "admin.rename";
    /// `nexus admin assign <id> <name>` — staged identity assumes a dead owner's name.
    pub const ASSIGN: &str = "admin.assign";
    /// Assign an agent role.
    pub const ASSIGN_ROLE: &str = "admin.assign_role";
    /// Assign an agent project.
    pub const ASSIGN_PROJECT: &str = "admin.assign_project";
    /// Grant a durable agent privilege tier.
    pub const GRANT_TIER: &str = "admin.grant_tier";
    /// Assign a durable agent to a policy group.
    pub const GROUP_ASSIGN: &str = "admin.group.assign";
    /// Create or update a channel.
    pub const CHANNEL: &str = "admin.channel";
    /// Route a message/admin operation.
    pub const ROUTE: &str = "admin.route";
    /// Monitor daemon activity.
    pub const MONITOR: &str = "admin.monitor";
    /// List dead-lettered delivery rows.
    pub const DLQ_LIST: &str = "admin.dlq.list";
    /// Requeue dead-lettered delivery rows.
    pub const DLQ_REQUEUE: &str = "admin.dlq.requeue";
    /// Purge dead-lettered delivery rows.
    pub const DLQ_PURGE: &str = "admin.dlq.purge";
}

/// Durable agent identity command kinds.
pub mod agent {
    /// Create a durable agent identity.
    pub const CREATE: &str = "agent.create";
    /// Grant delegated access to a managed agent session.
    pub const GRANT_ACCESS: &str = "agent.grant_access";
    /// Revoke delegated access to a managed agent session.
    pub const REVOKE_ACCESS: &str = "agent.revoke_access";
    /// Transfer managed agent ownership.
    pub const TRANSFER_OWNER: &str = "agent.transfer_owner";

    /// Agent credential command kinds.
    pub mod credential {
        /// Create a runtime credential.
        pub const CREATE: &str = "agent.credential.create";
        /// Revoke a runtime credential.
        pub const REVOKE: &str = "agent.credential.revoke";
    }
}

/// Notification source command kinds.
pub mod source {
    /// Register a notification source.
    pub const REGISTER: &str = "source.register";
    /// Enable a notification source.
    pub const ENABLE: &str = "source.enable";
    /// Disable a notification source.
    pub const DISABLE: &str = "source.disable";
    /// Rotate a notification source token.
    pub const ROTATE: &str = "source.rotate";
    /// Remove a notification source.
    pub const REMOVE: &str = "source.remove";
    /// Push a signed notification source payload.
    pub const PUSH: &str = "source.push";
}
