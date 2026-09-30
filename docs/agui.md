# AG-UI and live Gateway streams

[← Nexus docs](README.md) · [REST API](rest-api.md) · [Harness streams](acp-passthrough.md)

The Nexus Gateway is the browser-facing backend. It exposes REST for actions and durable reads,
Server-Sent Events for one-way observation, and WebSocket for multiplexed live panes. The WebUI
never connects to daemon IPC or a daemon database.

## Surfaces

| Surface | Purpose |
|---|---|
| `/api/v1/*` | Actions, bounded history pages, identity, fleet, and administration. |
| `/api/agui` | Submit one AG-UI run from the full Gateway/WebUI server. |
| `/api/agui/observe` | Observe a thread, DM, topic, or live agent session as AG-UI SSE. |
| `/api/agui/ws` | Multiplex live Gateway subscriptions over WebSocket. |
| `/api/v1/agent-sessions/:id/events` | Stream one session as Nexus, AG-UI, or terminal frames. |

The independently distributed API-only Gateway serves `/api/v1/*`, `/api/mcp`, AG-UI observe,
session events, and the AG-UI WebSocket. The optional WebUI package adds browser routes.

## Durable conversations

DM, thread, topic, and notification facts arrive through the ordered daemon-to-Gateway projection
stream. The Gateway commits each projection to its own local database before acknowledging it.
History pages and search are served from that Gateway-owned database.

Observe a thread without sending a message:

```bash
curl -N 'http://127.0.0.1:4100/api/agui/observe?thread=design'
```

Other targets use exactly one of:

```text
?dm=<name>
?agentId=<a_...>
?topic=<name>
```

Use the REST message endpoint for ordinary writes. A composer draft stays entirely in browser
state; pressing Enter submits one complete payload. Only after daemon admission should the UI show
the durable queue or delivery state.

## Live agent sessions

An agent session is a live harness-activity lane, not a DM address. The Gateway opens one shared
upstream for each observed session and fans it out to all local subscribers through a bounded ring.

```bash
curl -N \
  'http://127.0.0.1:4100/api/v1/agent-sessions/s_01ABC/events?view=nexus'

curl -N \
  'http://127.0.0.1:4100/api/v1/agent-sessions/s_01ABC/events?view=agui'

curl -N \
  'http://127.0.0.1:4100/api/v1/agent-sessions/s_01ABC/events?view=terminal'
```

Views have explicit meanings:

- `nexus` is normalized structured model activity;
- `agui` converts the same activity into AG-UI lifecycle and content events;
- `terminal` is raw PTY/tmux presentation bytes.

Raw model text means the normalized text emitted by the harness bridge. It does not mean a scrape of
the visible terminal. Terminal output is a separate attach lane and is not canonical history.

For compatibility, `/api/agui/observe?session=<name>` resolves the current session and delegates to
the session-events surface. `lane=raw` selects the terminal view.

## WebSocket

Connect to:

```text
ws://127.0.0.1:4100/api/agui/ws
```

The socket multiplexes Gateway-owned conversation observations and live session observations. A
browser socket never connects to the daemon directly. The Gateway maintains its own shared daemon
upstream so adding browser panes does not multiply daemon subscriptions unnecessarily.

WebSocket is a presentation transport. Message acceptance, delivery settlement, identity, and
authorization remain canonical in daemon/Gateway contracts rather than in browser connection
state.

## Reconnect and gaps

Durable conversation streams resume from Gateway history cursors. Live session cursors encode a
daemon/Gateway stream epoch and frame position. A cursor outside the bounded live window, a daemon
restart, or an overflow produces an explicit resync/gap boundary.

Clients should:

1. keep the latest accepted cursor;
2. reconnect with that cursor;
3. replace or refresh a durable viewport after a resync;
4. show a visible discontinuity for ephemeral session/terminal gaps;
5. avoid replaying a submitted message merely because the presentation socket reconnected.

## Authentication

Loopback local mode uses the configured local operator identity. Remote mode is authenticated by
the Gateway and requires allowed origins plus scoped credentials. Harness credentials and daemon
client keys are not browser bearer tokens.

Do not bind unauthenticated local mode to an untrusted interface.

## Failure model

- If Gateway is down, daemon transport continues.
- In the default buffered projection mode, the daemon retains a bounded in-memory projection
  backlog and sends it when Gateway returns.
- If that backlog overflows, Gateway receives an explicit history gap.
- In best-effort mode, disconnected Gateway projections are not retained.
- A Gateway presentation failure never changes whether a harness received a message.

See [Architecture](architecture.md) for the authority split and [Debugging](debugging.md) for
triage steps.
