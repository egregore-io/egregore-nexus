//! Project-scoped repositories over the §4 schema. Each repo borrows the shared
//! [`Store`](crate::Store) connection; every read is filtered by `project` (the scoping
//! invariant — the daemon never resolves names/threads across projects).
//!
//! `agents`, `agent_credentials`, and `agent_runtimes` are the generic identity/runtime repos.
//! Harness-specific runtime state belongs in the harness crates, not in this store layer.

pub mod agent_access_grants;
pub mod agent_credentials;
pub mod agent_groups;
pub mod agent_runtimes;
pub mod agent_session_messages;
pub mod agents;
pub mod command_intents;
pub mod command_queue;
pub mod daemon_state;
pub mod delivery_obligations;
pub mod developer_events;
pub mod identity_sessions;
pub mod inbox;
pub mod inbox_subscriptions;
pub mod initial_prompts;
pub mod live_sessions;
pub mod messages;
pub mod metadata;
pub mod native_thread_bindings;
pub mod notifications;
pub mod producer_identities;
pub mod routing_threads;
pub mod sessions;
pub mod sources;
pub mod stream_events;
pub mod stream_raw;
pub mod threads;
pub mod topics;
pub mod transcript_archive;

pub use agent_access_grants::{AgentAccessGrants, NewAgentAccessGrant, ROLE_CO_OWNER, ROLE_VIEWER};
pub use agent_credentials::{AgentCredentials, NewAgentCredential};
pub use agent_groups::AgentGroups;
pub use agent_runtimes::{AgentRuntimes, NewAgentRuntime};
pub use agent_session_messages::{
    AgentSessionMessageRow, AgentSessionMessages, AgentSessionTurnRow, NewAgentSessionMessage,
};
pub use agents::{AgentOwner, AgentRef, Agents, NewAgent};
pub use command_intents::{
    AutoCommandOutcome, CommandIntentDepth, CommandIntentReceipt, CommandIntentRow, CommandIntents,
    NewCommandIntent,
};
pub use command_queue::{CommandQueue, CommandQueueEventsPage, CommandQueueMutationOutcome};
pub use daemon_state::DaemonState;
pub use delivery_obligations::{DeliveryObligationRow, DeliveryObligations, NewDeliveryObligation};
pub use developer_events::{
    DeveloperEventRow, DeveloperEvents, NewDeveloperEvent, AGENT_LIFECYCLE_TOPIC,
};
pub use identity_sessions::{IdentitySessionRow, IdentitySessions, NewIdentitySession};
pub use inbox::{
    DeadLetterEntry, DeadLetterFilter, DeadLetterMutation, DeadLetterSelector, DeadLetterSummary,
    Inbox, MessageDeliveryTarget,
};
pub use inbox_subscriptions::{
    caller_subscription_id, subscription_now, InboxSubscriptionBatchRow, InboxSubscriptionRow,
    InboxSubscriptions, NewInboxSubscription,
};
pub use initial_prompts::{
    InitialPromptDeliveries, InitialPromptInsert, InitialPromptInsertOutcome, InitialPromptRow,
};
pub use live_sessions::{LiveSessionRow, LiveSessions, NewLiveSession};
pub use messages::Messages;
pub use metadata::{EntityMetadataRow, Metadata, MetadataEntity};
pub use native_thread_bindings::{NativeThreadBindings, NewNativeThreadBinding};
pub use notifications::Notifications;
pub use producer_identities::{
    ProducerIdentities, ProducerIdentityRow, DEFAULT_PENDING_PRODUCER_IDENTITIES_PER_RUNTIME,
};
pub use routing_threads::{RoutingThreadMemberRow, RoutingThreadRow, RoutingThreads};
pub use sessions::{NewSession, Sessions};
pub use sources::{SourceRow, Sources};
pub use stream_events::{StreamEventRow, StreamEvents};
pub use stream_raw::{StreamRaw, StreamRawCaps, StreamRawRow};
pub use threads::Threads;
pub use topics::Topics;
pub use transcript_archive::{
    NewTranscriptArchive, TranscriptArchive, TranscriptArchiveProgress, TranscriptArchiveRow,
};
