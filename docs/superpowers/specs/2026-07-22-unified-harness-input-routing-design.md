# Unified Harness Input Routing

**Status:** approved design
**Date:** 2026-07-22
**Scope:** Nexus daemon, Gateway session API, harness adapters, and the Lens Nexus client

## Problem

The browser-facing session composer currently chooses between `harness.prompt` and
`harness.steer` from a pushed `turnActive` projection. That projection is useful for display but
cannot authorize delivery: the active turn may end after the projection is rendered and before the
command reaches the harness.

The resulting failure is a time-of-check/time-of-use race:

```text
Lens sees active
  -> Lens sends explicit steer
  -> turn finishes
  -> daemon rejects ACTIVE_TURN_REQUIRED
  -> the user's ordinary Enter action is not delivered
```

This is the wrong ownership boundary. Nexus already has one harness-neutral execution port,
per-harness delivery implementations, declared steering capabilities, a durable command queue, and
race recovery in Message Post dispatch. Direct agent-session input bypasses that scheduler.

## Decision

Normal clients invoke one high-level operation: **session input**. They never select prompt,
steer, interrupt-and-send, or queue.

Nexus persists one `harness.input` command and resolves it through the currently bound harness
adapter at execution time. Every harness implements the same input contract while retaining its
own native behavior.

Explicit `harness.prompt`, `harness.steer`, and interrupt operations remain low-level operations
for compatibility, diagnostics, and SDK callers that intentionally require those exact semantics.
Lens does not use them for ordinary Enter submission.

## Public contract

The Gateway exposes the same request shape over REST, WebSocket, and the future Python SDK:

```jsonc
{
  "agentId": "a_bob",
  "name": "Bob",
  "text": "check this next",
  "clientMessageId": "lens:nexus:Bob:123"
}
```

- REST: `POST /api/conversation/input`
- agent-session WebSocket: `{ "t": "input", ...request }`
- Python SDK: `await nexus.sessions.input(...)`

`agentId` is authoritative when present. `name` is presentation and legacy fallback only.
`clientMessageId` is required for browser and SDK clients and scopes idempotency to the authenticated
caller plus `harness.input`.

Durable acceptance returns one receipt:

```jsonc
{
  "commandId": "cmd_...",
  "clientMessageId": "lens:nexus:Bob:123",
  "sessionId": "s_...",
  "state": "queued",
  "revision": 1,
  "seq": 42
}
```

The acknowledgement proves durable acceptance, not execution. Ordered command transitions later
report `started`, `completed`, `failed`, or `cancelled`. A started/completed transition may report
the adapter-selected disposition:

```text
started | steered | interrupted_and_started | queued
```

Clients may display the disposition but must not choose it.

## Adapter contract

`AgentTurnExecutionPort` remains the single harness boundary. It gains one high-level method with
the same shape for every implementation:

```rust
async fn deliver_input_observed(
    &self,
    recipient: &SessionId,
    text: String,
    events: Arc<dyn EventSink>,
    accepted_event: WsEvent,
) -> PortResult<InputDisposition>;
```

Implementations:

- **Codex app-server:** native steer when its exact native turn remains active; otherwise start a
  fresh prompt under the same invocation.
- **Claude:** use its proven active-turn adapter; otherwise start a prompt.
- **OpenCode, Hermes, and ACP adapters:** use their own native or interrupt-and-send behavior.
- **No safe active redirect capability:** return a queue disposition without losing or failing the
  durable command. The scheduler retains it until the turn boundary.

The adapter resolves native authority immediately before crossing the harness boundary. A
turn-completion race is absorbed inside this invocation and cannot become an ordinary user-facing
failure.

## Durable scheduling

`harness.input` is one durable command from acceptance through terminal settlement.

```text
accepted
  -> resolve stable runtime + adapter
  -> adapter sees current native turn authority
       idle                         -> prompt
       active + native steer        -> steer
       active + interrupt-and-send  -> interrupt-and-send
       active + no safe redirect    -> remain queued
  -> accepted native receipt
  -> started/completed transition with disposition
```

The scheduler must:

1. preserve per-session ordering;
2. resolve the adapter at execution time, never from a browser snapshot;
3. publish the canonical user-input event exactly once at the adapter's accepted boundary;
4. retain the same `commandId` and `clientMessageId` across a turn-boundary fallback;
5. never compose cancel plus prompt in Lens or Gateway;
6. never emit `ACTIVE_TURN_REQUIRED` for `harness.input`;
7. leave an unsupported active delivery durably queued rather than rejected.

## Lens behavior

Lens always sends the WebSocket `input` frame for an ordinary session composer submission.
`turnActive`, `redirectMode`, and queue projections remain presentation facts used for Stop, busy
state, and queue visualization only.

Lens removes its Prompt-versus-Steer branch. It applies the one durable receipt and subsequent
transitions by `clientMessageId`. Exactly one layer owns user-facing failure presentation; the same
terminal error cannot independently create socket, plugin, and composer toasts.

Explicit queue-row actions remain explicit. An operator choosing **Steer now** on a particular
queued row is not an ordinary Enter submission and may retain strict redirect semantics.

## Adjacent race seams

| Seam | Production behavior |
|---|---|
| Active turn ends during ordinary input | Adapter starts a prompt under the same command. |
| Turn starts while an idle input is being claimed | Adapter/scheduler serializes or queues; never duplicates. |
| Runtime session rolls over before execution | Re-resolve by stable agent id; reject mismatched authority before harness mutation. |
| WebSocket disconnects before acknowledgement | Retry the same `clientMessageId`; return the same durable command. |
| Native receipt and queue transition arrive in either order | Reconcile by command/client identity; render once. |
| Stop races natural completion | Treat the requested terminal outcome as satisfied (`already_terminal`), provided stable session identity still matches. |
| `redirect_now` races natural completion | Keep the queued command authoritative and return its current state; do not discard or duplicate it. |
| Explicit low-level steer races completion | Retain typed `ACTIVE_TURN_REQUIRED`; this is intentionally steer-or-fail. |
| Compact races active execution | The adapter serializes at its native legal boundary or returns one typed unsupported result; the client does not infer legality. |

## Compatibility

- Existing low-level REST and daemon commands remain available.
- The existing WebSocket `input` frame keeps its wire name and changes only its internal command
  target from `harness.prompt` to `harness.input`.
- `SteerDelivery::FallbackStarted` remains decode-compatible for old peers but is not the new
  contract. `InputDisposition::Started` is the high-level result.
- Gateway and daemon versions that do not advertise `harness.input` must fail capability
  negotiation before enabling the session composer. Lens must not fall back to client-side route
  selection.

## Verification

Tests must prove:

1. every harness accepts the same `harness.input` request shape;
2. idle input starts once;
3. active native input steers once;
4. active interrupt-and-send runs once;
5. unsupported active input remains queued;
6. a deterministic barrier ending the turn between route selection and native admission still
   delivers once as a prompt with the original ids;
7. a turn starting during prompt admission neither duplicates nor loses the command;
8. reconnect/retry returns the original receipt;
9. runtime rollover cannot deliver to the prior session;
10. accepted input produces one canonical transcript row;
11. Lens emits only `input`, regardless of `turnActive`;
12. one failed command produces one user-visible error;
13. explicit low-level steer continues to reject a missing active turn;
14. Stop, redirect-now, and compact race outcomes match the adjacent-seam table.

## Rejected alternatives

### Gateway fallback composition

Calling `steer`, catching `ACTIVE_TURN_REQUIRED`, and then calling `prompt` creates two commands and
two idempotency domains. A crash between them cannot prove exactly-once delivery.

### Lens fallback composition

This repeats the same flaw at an even less authoritative layer and requires every client to
reimplement harness policy.

### Reinterpreting `harness.prompt`

Changing an established low-level command from prompt-only to automatic routing would silently
alter existing SDK and diagnostic callers. A distinct `harness.input` command makes the higher-level
contract explicit.

## Non-goals

- Changing bus Message Post routing.
- Removing explicit steer, interrupt, compact, or queue mutation APIs.
- Moving queue authority into Gateway or Lens.
- Making `turnActive` less visible; it remains useful presentation telemetry.
