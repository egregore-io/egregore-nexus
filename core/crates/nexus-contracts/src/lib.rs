//! # nexus-contracts
//!
//! The **single source of truth** for every Nexus wire type — the serde-(de)serializable Rust
//! structs and enums that every request, response, and event is built from. The daemon (`core/`)
//! and the `nexus` CLI depend on this crate directly (in-workspace, zero drift); the gateway
//! consumes a generated TypeScript mirror.
//!
//! ## Command envelope
//!
//! Store-backed CLI/MCP/gateway writes use command-intent rows. Those rows carry an internal
//! RPC-shaped envelope, defined in [`rpc`]: [`Request`]/[`Response`]/[`Notification`]/[`RpcError`]
//! with the standard error [`codes`]. The envelope carries the same `*Request`/`*Response` DTOs as
//! typed params/results, so the contract crate remains the fan-out boundary for every producer.
//!
//! ## Method registry
//!
//! [`rpc`] also pins the Nexus **method registry** — each method name mapped to its
//! concrete `params`/`result` type — so a method and its payload types can never drift apart.
//!
//! ## camelCase JSON
//!
//! Every struct/enum is `#[serde(rename_all = "camelCase")]` (enums use their lowercase/dotted
//! tokens) so the wire JSON — and the generated TypeScript — is idiomatic.
//!
//! ## TypeScript mirror (typeshare)
//!
//! Every public type is annotated `#[typeshare]`. The `typeshare` CLI reads this Rust source and
//! emits the TS mirror at `gateway/src/shared/types/contracts.gen.ts`. The Rust is the source;
//! the TS is purely derived and **never hand-edited**.
//!
//! ## Golden fixtures = the cross-language lock
//!
//! Canonical JSON wire fixtures (`fixtures/*.json`) are consumed by **both** the Rust round-trip
//! tests here and the gateway's generated-TS tests. If backend, CLI, or gateway drift, the
//! fixtures fail on whichever side drifted — that is the cross-language no-drift guarantee.
//!
//! A contract change is therefore a **deliberate, versioned event**: additive changes (new optional
//! field, new enum variant) are safe; renames/removals require a crate version bump and a
//! regenerated `contracts.gen.ts`.
//!
//! ## Module map
//!
//! - [`ids`] — string-backed id newtypes (`SessionId`, `MessageId`, `ThreadId`, `TopicId`, `ProjectId`).
//! - [`enums`] — shared closed enums (`Kind`, `Scope`, `Tier`, `Presence`, `Harness`, `DeliveryState`).
//! - [`agents`] — durable agent identities, runtime credentials, and runtime summaries.
//! - [`project`] — `Project` + register-project request/response.
//! - [`message`] — `Message` + `Provenance` (in-band tag attrs + stored crypto stamp).
//! - [`register`] — register handshake, `Whoami`, status/heartbeat (presence), members directory.
//! - [`send`] — `SendRequest` (the one `to` contract: dm/post/publish/reply) + `Ack`.
//! - [`batch`] — `NexusBatch` drain-once delivery envelope (the `<nexus-batch>` JSON form).
//! - [`daemon_ipc`] — versioned local producer↔daemon request/response envelopes.
//! - [`ack`] — split-ack request/response types (DMs per-message, threads bulk).
//! - [`threads`] — thread create/join/leave/members + list.
//! - [`topics`] — topic subscribe/unsubscribe + list.
//! - [`search`] — `SearchRequest`/`SearchHit` + history.
//! - [`notify`] — `NotifyRequest`, `RouteRule`, the HMAC header contract.
//! - [`admin`] — spawn/remove/assign-role/channel/monitor/route-forward.
//! - [`events`] — `WsEvent`, the daemon event taxonomy (legacy tagged-union name).
//! - [`rpc`] — the internal JSON-RPC-shaped envelope + error codes used by the command worker.
//!
//! See `docs/architecture.md` for the public contract boundary.

pub mod ack;
pub mod admin;
pub mod agents;
pub mod batch;
pub mod daemon_ipc;
pub mod enums;
pub mod events;
pub mod gateway_projection;
pub mod ids;
pub mod message;
pub mod metadata;
pub mod notify;
pub mod ports;
pub mod project;
pub mod prompt;
pub mod register;
pub mod rpc;
pub mod search;
pub mod send;
pub mod source;
pub mod threads;
pub mod topics;

// Flat re-export surface: downstream writes `nexus_contracts::SendRequest`.
pub use ack::{AckRequest, AckResponse, AckThreadsRequest};
pub use admin::{
    AdminAssignRequest, AdminAssignResponse, AdminGroupAssignRequest, AdminGroupAssignResponse,
    AdminRenameRequest, AdminRenameResponse, AssignProjectRequest, AssignProjectResponse,
    AssignRoleRequest, AssignRoleResponse, ChannelOp, ChannelRequest, DlqEntry, DlqListRequest,
    DlqListResponse, DlqMutationResponse, DlqPurgeRequest, DlqRequeueRequest, GrantTierRequest,
    GrantTierResponse, MonitorRequest, RemoveRequest, RemoveResponse, RouteForwardRequest,
    SpawnIdentityPolicy, SpawnRequest, SpawnResponse,
};
pub use agents::{
    AgentAccessGrantRequest, AgentAccessGrantResponse, AgentAccessRevokeRequest,
    AgentAccessRevokeResponse, AgentCreateRequest, AgentCreateResponse,
    AgentCredentialCreateRequest, AgentCredentialCreateResponse, AgentCredentialRevokeRequest,
    AgentCredentialRevokeResponse, AgentListRequest, AgentListResponse, AgentOwnerTransferRequest,
    AgentOwnerTransferResponse, AgentRuntimeListRequest, AgentRuntimeListResponse,
    AgentRuntimeSummary, AgentShowRequest, AgentShowResponse, AgentSummary,
};
pub use batch::{
    BatchCounts, BatchMessage, ConsumeRequest, InboxSubscribeRequest, InboxSubscribeResponse,
    InboxSubscriptionAckRequest, InboxSubscriptionBatch, InboxSubscriptionNextRequest,
    InboxSubscriptionNextResponse, InboxSubscriptionStatusResponse, InboxUnsubscribeRequest,
    NexusBatch,
};
pub use daemon_ipc::{
    DaemonIpcCall, DaemonIpcCaller, DaemonIpcRequest, DaemonIpcResponse,
    DAEMON_IPC_PROTOCOL_VERSION,
};
pub use enums::{AgentAccessRole, DeliveryState, Harness, Kind, Presence, Scope, Tier};
pub use events::{
    AgentUpdateKind, DeveloperEventEnvelope, DeveloperEventKind, DeveloperToolCallPhase,
    ToolCallData, WsEvent,
};
pub use gateway_projection::{
    GatewayProjectionAck, GatewayProjectionEffect, GatewayProjectionEvent, GatewayProjectionKind,
    GATEWAY_PROJECTION_VERSION,
};
pub use ids::{AgentId, CredentialId, MessageId, ProjectId, SessionId, ThreadId, TopicId};
pub use message::{Message, Provenance, ProvenanceStamp, ReadRequest};
pub use metadata::{MetadataEntityKind, MetadataGetRequest, MetadataResponse, MetadataSetRequest};
pub use notify::{
    NotifyCommandRequest, NotifyRequest, NotifyResponse, NotifySendRequest, NotifyTarget,
    RouteRule, NOTIFY_SIGNATURE_HEADER, NOTIFY_TIMESTAMP_HEADER,
};
pub use ports::{
    AdminPort, AgentTurnExecutionPort, BusPort, Caller, ContractError, DispatchPort, EventSink,
    IdentityPort, InjectError, InjectResult, NotifyPort, OperatorAction, PortResult, ProviderError,
    ProviderLimit, ProviderLimitReason, RealtimePort, ResetHint, SearchPort,
};
pub use project::{Project, RegisterProjectRequest, RegisterProjectResponse};
pub use prompt::{
    CommandExpectedRevision, CommandQueueAction, CommandQueueEntry, CommandQueueMutationRequest,
    CommandQueueMutationResponse, CommandQueueReceipt, CommandQueueSnapshot, CommandQueueState,
    CommandQueueTransition, CompactRequest, CompactResponse, PromptRequest, PromptResponse,
    SteerCapability, SteerDelivery, SteerRequest, SteerResponse, WarmRequest, WarmResponse,
};
pub use register::{
    HeartbeatRequest, HeartbeatResponse, MemberListRequest, MemberListResponse, MemberSummary,
    RegisterRequest, RegisterResponse, RenameRequest, RenameResponse, StatusRequest,
    StatusResponse, StatusState, Whoami,
};
pub use rpc::{codes, Notification, Request, RequestId, Response, RpcError, JSONRPC_VERSION};
pub use search::{
    HistoryEntry, HistoryRequest, HistoryResponse, SearchHit, SearchMode, SearchRequest,
    SearchResponse,
};
pub use send::{validate_send_body, validate_send_request, Ack, SendRequest, SendTarget};
pub use source::{
    PushRequest, PushResponse, Source, SourceListResponse, SourceRef, SourceRegisterRequest,
    SourceRegisterResponse, SourceTokenResponse, SOURCE_SIGNATURE_HEADER, SOURCE_TIMESTAMP_HEADER,
};
pub use threads::{
    ArchiveThreadRequest, CreateThreadRequest, DeleteThreadRequest, JoinThreadRequest,
    LeaveThreadRequest, RenameThreadRequest, ThreadHeaderMember, ThreadHeaderResponse,
    ThreadListResponse, ThreadMemberRequest, ThreadMembersRequest, ThreadMembersResponse,
    ThreadSummary,
};
pub use topics::{
    SubscribeRequest, SubscribeResponse, TopicListResponse, TopicSummary, UnsubscribeRequest,
};
