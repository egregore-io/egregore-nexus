// --- Hand-authored wire unions (appended by gen-ts.sh / the ts_gen test) ---
//
// typeshare 1.13 cannot emit internally-tagged (`#[serde(tag = ...)]`) algebraic enums or infer
// untagged-enum unions, so the three serde unions below are authored to match the frozen Rust wire
// shape exactly (the same JSON the crate's round-trip tests + golden fixtures assert). The Rust
// types reference these via `#[typeshare(serialized_as = "...")]`. Keep in lockstep with
// `src/send.rs` (SendTarget), `src/notify.rs` (NotifyTarget), `src/daemon_ipc.rs`
// (DaemonIpcCall), `src/events.rs` (WsEvent), and `src/rpc.rs` (RequestId).

// String-backed id newtypes (src/ids.rs). They are `#[serde(transparent)]` and defined via a
// macro, so typeshare's parser does not see the definitions (it only sees the usages); we emit the
// `= string` aliases here. The wire shape is a bare JSON string.
export type SessionId = string;
export type AgentId = string;
export type CredentialId = string;
export type MessageId = string;
export type ThreadId = string;
export type TopicId = string;
export type ProjectId = string;

/** JSON-RPC request id — a bare number or string ([`RequestId`], `#[serde(untagged)]`). */
export type RequestIdWire = number | string;

/** The send target/verb — internally tagged by `verb` ([`SendTarget`]). */
export type SendTargetWire =
	| { verb: "dm"; name?: string; agentId?: AgentId }
	| { verb: "post"; thread: string }
	| { verb: "publish"; topic: string }
	| { verb: "reply" };

/** Explicit one-shot notification target — internally tagged by `kind` ([`NotifyTarget`]). */
export type NotifyTargetWire =
	| { kind: "auto"; value: string }
	| { kind: "agent"; agentId: AgentId }
	| { kind: "name"; name: string }
	| { kind: "group"; group: string }
	| { kind: "thread"; thread: string };

/** One daemon IPC operation — internally tagged by `mode` ([`DaemonIpcCall`]). */
export type DaemonIpcCallWire =
	| { mode: "command"; commandId: string; kind: string; params: unknown; idempotencyKey?: string }
	| { mode: "enqueue"; commandId: string; kind: string; params: unknown; idempotencyKey?: string }
	| { mode: "query"; method: string; params: unknown };

/** The daemon event stream — internally tagged by `type` ([`WsEvent`]). */
export type WsEventWire =
	| { type: "message.created"; messageId: MessageId }
	| { type: "message.delivered"; messageId: MessageId; recipient: SessionId }
	| { type: "agent.update"; sessionId: SessionId; kind: "text" | "thinking" | "tool_call" | "plan" | "commands" | "turn_end" | "user_input"; data: unknown }
	| { type: "agent.status"; sessionId: SessionId; presence: Presence; paused: boolean }
	| { type: "agent.spawned"; sessionId: SessionId; name: string; agentId?: string | null }
	| { type: "agent.removed"; sessionId: SessionId; name: string }
	| { type: "thread.created"; thread: string; members: string[] }
	| { type: "thread.member.changed"; thread: string; members: string[] }
	| { type: "topic.published"; topic: string; messageId: MessageId }
	| { type: "notification.received"; notifId: MessageId; routedTo: string[] }
	| { type: "developer.event"; event: DeveloperEventEnvelope };
