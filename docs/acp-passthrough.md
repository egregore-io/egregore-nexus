# Harness session streams

[← Nexus docs](README.md) · [AG-UI](agui.md) · [Architecture](architecture.md)

Nexus exposes structured harness activity without making a browser understand each harness's
native protocol. Headless ACP adapters and headed native bridges both normalize activity into the
same `agent.update` contract before it reaches the Gateway.

This is a live transport path. It is separate from durable DM, thread, topic, and notification
history.

## Data path

```text
harness
  ├─ headless ACP session/update
  └─ headed native bridge
          │
          ▼
daemon boot-scoped session stream
          │ one upstream per observed session
          ▼
Gateway bounded fanout
  ├─ view=nexus    normalized Nexus events
  ├─ view=agui     converted AG-UI events
  └─ view=terminal raw terminal bytes
```

The first two views come from the same structured model activity. The Gateway converts one source
frame rather than opening a second harness connection for AG-UI consumers. The terminal view is a
different lane and is never canonical conversation text.

## Normalized Nexus view

The `nexus` view emits `agent.update` events with a session ID, update kind, and structured data.
Common update kinds include:

- accepted user input;
- assistant text and reasoning deltas;
- tool-call lifecycle updates;
- usage and metadata updates;
- turn completion.

Harness adapters may receive different native event shapes, but the public Nexus event contract is
harness-neutral. Tool calls must follow [C-TOOL v1](tool-call-contract.md).

## AG-UI view

The `agui` view converts the normalized events into AG-UI run, message, reasoning, tool, and
lifecycle frames. Conversion happens in the Gateway. It does not change daemon routing or create a
second durable transcript.

## Terminal view

The `terminal` view carries base64-encoded PTY or tmux bytes for attach-style presentation. Terminal
bytes may include prompts, redraws, progress displays, or control sequences, so they must not be
treated as model-authored message history.

## Cursors and gaps

Session cursors include the stream epoch and frame position. The Gateway keeps only a bounded
process-local replay ring for each actively observed session. A stale cursor, daemon epoch change,
or overwritten replay window produces an explicit resync/gap boundary.

Clients must:

1. retain the last accepted cursor;
2. reconnect with `after=<cursor>`;
3. treat a resync/gap as a presentation discontinuity;
4. refresh durable message history from Gateway REST when relevant;
5. never infer delivery settlement from a rendered token.

## Delivery and persistence boundaries

- The daemon persists identity, resurrection descriptors, and unsettled-delivery continuity.
- The daemon's message, presence, and session-stream working sets are boot-scoped.
- The Gateway persists product history delivered through its ordered projection contract.
- The live session fanout is bounded and ephemeral in v0.1.0.
- The WebUI connects only to the Gateway.

These boundaries let the daemon remain a lightweight transport while the Gateway owns browser and
history concerns.

## Endpoint

Use the Gateway session-events endpoint:

```text
GET /api/v1/agent-sessions/<session-id>/events?view=nexus
GET /api/v1/agent-sessions/<session-id>/events?view=agui
GET /api/v1/agent-sessions/<session-id>/events?view=terminal
```

All three are Server-Sent Event streams. Add `after=<cursor>` to resume within the bounded replay
window.
