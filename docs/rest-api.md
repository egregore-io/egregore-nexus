# REST API

[← Nexus docs](README.md) · see [getting-started.md](getting-started.md) to run the gateway

The gateway exposes the bus over a REST API under `/api/v1/*`. This is the front door for
humans and external HTTP clients. Under the hood each endpoint is a thin translator: reads and
writes cross the daemon's boot-scoped local IPC endpoint. The daemon owns transport/runtime
authority; canonical product reads use the Gateway's own projected store, not the daemon database.

CLI/MCP `dm`, `post`, `reply`, `publish`, and generic `send` use the same Message Post
command-intent path.

All examples assume the gateway on `http://localhost:4100`. If that port is occupied, the gateway
falls back through `4101`-`4110`; `NEXUS_GATEWAY_PORT` requires an exact port. Check `pnpm dev`
output or `~/.nexus/gateway.json` for the active URL. JSON on the wire is camelCase.
Production daemon boots supervise the gateway process and restart it after an unexpected exit, so
`gateway.json` is normally rewritten by a live daemon-owned child. Set `NEXUS_GATEWAY_SUPERVISE=0`
only when you intentionally run the gateway yourself. Detached/service daemon starts pass
`NEXUS_GATEWAY_DIR` to the foreground daemon when the lifecycle command can resolve the local
gateway package; manual foreground runs can set the same variable explicitly.

For headless deployments that need only the API/protocol gateway, run:

```bash
pnpm --dir gateway/gateway start:api
```

That mode uses the same port discovery and serves `/api/v1/*` plus network MCP at `/api/mcp`.
Webconsole routes intentionally return `404`, so the API can run without requiring the UI route
tree. The umbrella `pnpm --dir gateway start:api` script delegates to the gateway package for
compatibility; webconsole development/build commands live under `gateway/webconsole`. API-only
mode defaults to `--discovery=none` so local smoke tests cannot overwrite the shared
`~/.nexus/gateway.json`; pass `--discovery=write` only for a production API-only gateway instance.
Discovery writers remove their own record on clean exit, and discovery consumers should verify the
recorded PID plus `/api/v1/health` before using the URL. The package split does not change the
gateway URL contract used by Lens or other local clients.

## Canonical runtime WebSocket snapshots

Use the existing `/api/agui/ws` socket without an observe target. Send:

```json
{"t":"runtime.subscribe","subscriptionId":"connection1/brief1","agentId":"a_exact"}
```

The first successful response is a complete canonical snapshot, including stopped history:

```json
{"t":"runtime.snapshot","subscriptionId":"connection1/brief1","agentId":"a_exact","sequence":1,"runtimes":[]}
```

Rows have the same runtime fields as `/api/v1/agents/:agentId/runtimes?includeStopped=true`,
including optional validated `modelReport`. An empty list is authoritative absence, not a read
failure. Native reporting capabilities/collectors are still being integrated; no observation means
unavailable, not zero usage or an inferred quota/context value.

Every read uses the socket's credentials through that authenticated REST route. Post-commit Gateway
change signals trigger serialized rereads; a signal during an awaited read invalidates that read.
Read/authentication failure sends `runtime.unavailable` with the same IDs, the next `sequence`, and
one bounded `reason`: `unauthorized`, `notFound`, `unavailable`, or `invalidSnapshot`. A stalled
read becomes unavailable after five seconds; a replacement read is not started until the original
settles. No internal error detail or raw obsolete projection payload is sent.

Sequence starts at one and advances for snapshots and unavailable frames within one subscription;
it is **not** a durable replay cursor. No `afterSeq` is accepted. On reconnect use a fresh
subscription ID and wait for a fresh complete snapshot. Key delivery by socket generation and
subscription ID, then exact agent/runtime and durable `modelReport.reportRevision`. Invalidate
live availability on disconnect/unavailable/replacement; stopped or inactive-observer evidence
is historical. Never sum repeated cumulative snapshots or derive remaining context from lifetime
token totals.

Send `{"t":"runtime.unsubscribe","subscriptionId":"connection1/brief1"}` to stop a reader.
Subscription IDs are nonblank, control-free, at most 128 UTF-8 bytes, and cannot be reused on the
same socket (even after unsubscribe). Reuse is rejected without replacing the existing reader.
There are at most 16 active and 128 lifetime subscriptions per socket. Invalid controls receive
the existing `input.err` frame. Backpressure closes the socket with code 1013; reconnect rehydrates
instead of pretending a dropped snapshot was delivered. There is no second fleet/telemetry bus.

## Auth

Auth lives **only** at the gateway (the daemon behind it has no browser login state).

- Local mode is zero-login. By default, loopback gateway runs stamp the local operator caller on
  web-console/API writes, so local development does not require a browser login.
- Remote mode requires a human login. If the gateway is server-configured as remote, public, or
  allow-remote, human web/API routes require a valid `nexus_human` cookie. The mode is never inferred
  from request headers such as `Host` or `X-Forwarded-For`.
- Programmatic clients use scoped REST bearer tokens issued by the gateway. Tokens resolve to the
  same Principal shape as a browser login (`actor`, `project`, `tier`, `scopes`, credential facet),
  are stored hashed, expire, refresh through a rotating refresh family, and can be revoked.
- Authorization checks enforce both tier and scope. A token can only exercise capabilities within
  its actor tier; `admin:*` scope on an agent-tier Principal still cannot call Admin-tier routes.
  Web terminal attach uses the dedicated `agent:attach` scope so it can be granted separately from
  agent reads, launch, or admin operations.
- Entity metadata routes are intentionally ungated beyond authentication: any authenticated
  Principal may read or replace the opaque JSON bag on a project-scoped message, session, thread,
  or agent.
- Signed source pushes are their own source-HMAC facet. After HMAC verification, the gateway stamps
  a source Principal on `source.push`; it does not fall back to the local operator outside local
  zero-login mode.
- Remote cookie-backed mutations require the `x-nexus-csrf` header to match the `nexus_csrf` cookie.
  Bearer-token requests and signed source pushes do not use CSRF because they are not browser-cookie
  credentials.

REST bearer clients send:

```
Authorization: Bearer <your key>
```

A missing, expired, revoked, or under-scoped Principal returns `401`/`403` before command ingress.

```bash
# local mode (default)
curl -s http://localhost:4100/api/v1/members

# with a scoped REST bearer
curl -s http://localhost:4100/api/v1/members \
  -H "authorization: Bearer $NEXUS_REST_TOKEN"
```

## Gateway write path and MCP

The gateway also exposes a network MCP endpoint at `POST /api/mcp`. It uses the same gateway
Principal spine as REST bearer requests: a missing, invalid, expired, revoked, or under-scoped
bearer is rejected before MCP dispatch. The first network MCP slice exposes the Message Post tools
`dm`, `post`, `reply`, and `publish`; each requires `message:send` and writes through the same
command-intent path as `/api/v1/messages`.

```bash
curl -s http://localhost:4100/api/mcp \
  -H "authorization: Bearer $NEXUS_REST_TOKEN" \
  -H "content-type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
```

Use `NEXUS_GATEWAY_URL=http://host:port pnpm --dir gateway smoke:mcp-reject` against a running
gateway to live-prove that an unauthenticated remote/network MCP connect is rejected with `401`.
When `NEXUS_GATEWAY_URL` is omitted, the smoke reads `~/.nexus/gateway.json` only after verifying
the recorded PID is alive and `/api/v1/health` is healthy.

The deployed HTTP write path for bus messages is `POST /api/v1/messages` with
`{ "to": { "verb": ... }, "body": ... }`. External HTTP clients, Lens bridges, and browser code
should use that route for Message Post writes; do not hardcode `/api/mcp` as the bus write path.

Local stdio MCP remains separate: `nexus mcp` is still the local trust-path for launched agents and
does not require a bearer token or grow bearer-token tool arguments. Any future network MCP gateway
surface must resolve the same Principal spine and project onto the same command-intent path rather
than becoming a second canonical write API.

## Errors

Errors come back as JSON with a non-2xx status. Validation failures (malformed body) are `400`.
Daemon command errors are mapped to a status and their user-facing message is passed through; a
stack is never leaked. Common mappings: invalid params -> `400`, unknown method -> `404`,
unauthorized -> `401`, forbidden -> `403`, not found -> `404`.

## Agent Session operations

Agent Session commands sit outside `/api/v1` because they act on one live harness session rather
than the Message Post bus. They require the same authenticated human Principal as the Agent Session
observe surface; local zero-login mode supplies the local operator, while remote mode requires a
valid `nexus_human` login cookie.

### `POST /api/conversation/steer`

**Exact-session compatibility boundary:** daemon and Gateway HTTP requests accept
`expectedSessionId` plus stable `agentId` for prompt, steer, interrupt, compact, and queue
mutations. That opt-in rejects stale/absent bindings without revive; explicit
null/empty/partial selectors are invalid before enqueue/mutation. Existing HTTP/IPC/CLI requests that
omit it keep legacy behavior. Successful exact responses and receipts
must carry the matching actual `sessionId`; missing/foreign results return an
unconfirmed-outcome error, never the request selector echoed as evidence.
Agent-session WebSocket commands require this pair on every mutation frame and only
accept it against canonical `session.bound` metadata. The retained browser producer
waits for that binding and does not fall back to name-only HTTP while unbound.
Binding identifies transport/routing, not native admission or lifecycle exclusion.
This contract does not establish Lens carrier integration. See [composer delivery](composer-delivery.md).

Explicitly sends additional input to an active turn. What happens depends on the backend: native
Codex steer adds the text to the running turn; a backend that supports interrupt-and-send interrupts
the turn and starts a new one with the text; a backend with neither rejects the request. This route
enqueues `harness.steer`, waits for the daemon-written final result, and does not reuse the normal
`harness.prompt` boundary queue. `clientMessageId` is both carried to the runtime and used as the
command-ingress idempotency key.

```jsonc
{
  "name": "otto",                  // required unless agentId is present
  "agentId": "a_otto",            // optional stable target; preferred when known
  "text": "check the migration first",
  "clientMessageId": "you:3"       // optional, recommended for retry-safe clients
}
```

A successful response is status `201`:

```jsonc
{
  "ok": true,
  "result": {
    "accepted": true,
    "delivery": "steered",         // or "interrupted_and_started"
    "turnId": "turn_7"             // optional native Codex turn id
  }
}
```

`steered` means Codex accepted the text into the active turn. `interrupted_and_started` means the
backend interrupted the running turn and started a new one carrying the text. If the active Codex
turn is absent or ends before the steer lands, the route returns `409` (`ACTIVE_TURN_REQUIRED`) and a
new turn needs an explicit prompt; nothing falls back automatically (`fallback_started` is decode-only
for old clients and is never emitted). Native receipt of the text is not proof the model consumed it.
A command that is only
claimed is not acknowledged as accepted; the route waits for `done` or returns the command error or
timeout.

## Endpoints

`METHOD path` is relative to `/api/v1`. "Read" endpoints query the display view. "Write/op"
endpoints enqueue daemon command intents unless explicitly marked as not implemented.

### Health

| Method | Path | Auth | What it does |
|---|---|---|---|
| GET | `/health` | none | Liveness probe. Returns `{ "status": "ok" }`. |

### Reads

| Method | Path | What it does |
|---|---|---|
| GET | `/threads` | List threads. |
| GET | `/threads/:name/header` | Compact channel-open header: thread `topic`/`description`, last activity, members with presence, and active-runtime count resolved through durable agent identity. |
| GET | `/threads/:name/history` | A thread's message history. Query: `limit`, backward `before`, or exact forward `after` + `afterRowid` cursor (ints). For authenticated human browser sessions, returned message ids are marked delivered for that exact session. |
| GET | `/threads/:name/members` | A thread's members. |
| GET | `/dms/:name/history` | The caller's DM history with one member. Query: `limit`, backward `before`, or exact forward `after` + `afterRowid` cursor (ints). For authenticated human browser sessions, returned message ids are marked delivered for that exact session. |
| GET | `/members` | The member directory. Query: `includeOffline` (`true`/`1`). |
| GET | `/search` | Search messages in your scope. Query: `q` (or `query`), `mode`, `thread`, `topic`, `with`, `since`, `limit`. |
| GET | `/topics` | List topics. |
| GET | `/notifications` | List notification audit rows plus delivered source pushes for the Pub feed. Query: `limit` (default 100, maximum 500), `before` (epoch milliseconds). |
| GET | `/routing-rules` | List standing routing rules. |
| GET | `/projects` | List project scope labels derived from registered sessions. Project is not a durable primitive. |
| GET | `/whoami` | Resolve a session by name. Query: `name`. `404` if no such session. |
| GET | `/messages/:id` | Read a single message by id. For authenticated human browser sessions, the returned message id is marked delivered for that exact session. |
| GET | `/messages/:id/metadata` | Read a message's opaque metadata JSON bag. Missing metadata returns `{}`. |
| GET | `/sessions/:id/metadata` | Read a session's opaque metadata JSON bag. Missing metadata returns `{}`. |
| GET | `/threads/:name/metadata` | Read a thread's opaque metadata JSON bag. Missing metadata returns `{}`. |
| GET | `/agents/:id` | Read one durable agent identity and its runtime rows. `:id` may be the stable `agentId` or current name. |
| GET | `/agents/:id/metadata` | Read an agent's opaque metadata JSON bag. `:id` may be the stable `agentId` or current name. |
| GET | `/agents/:id/runtimes` | List runtimes for one durable agent. Query: `includeStopped`. |
| GET | `/runtimes` | Top-level runtime list for one durable agent. Query: `agent` or `name` or `agentId`, plus `includeStopped`. |
| GET | `/hooks` | Read the active message-hook generation, handlers, and manifest errors. Local paths are redacted unless the caller is Admin-tier. |
| GET | `/hooks/public-key` | Read the Gateway Ed25519 public key used to verify hook execution provenance. |
| GET | `/hooks/audit` | Read recent hook executions and pending receipts. Query: `limit` (default 100). Results and local details are redacted unless the caller is Admin-tier. |
| GET | `/capabilities` | Machine-readable REST capability list generated from the registered route table, including read routes, command-backed routes, unauthenticated producer routes, and explicit `501` gaps. |
| GET | `/openapi` | OpenAPI 3.x discovery document generated from the same route registry as `/capabilities`. |
| GET | `/sources` | List notification sources without tokens. |
| GET | `/sources/:name` | Read one notification source without its token. |

### Writes / ops

| Method | Path | What it does | Backend path |
|---|---|---|---|
| POST | `/notify` | Ingest an external notification. | command intent: `notification.notify` |
| POST | `/auth/operator-token` | Local-mode first-boot bootstrap for a named human operator bearer. Requires `{ "name": "...", "scopes": [...] }`; remote deployments use login + `/auth/tokens` instead. | gateway credential store |
| POST | `/auth/tokens` | Issue a scoped REST bearer + refresh token for the authenticated Principal. Remote cookie callers must include CSRF. | gateway credential store |
| POST | `/auth/tokens/refresh` | Rotate a refresh token and return a fresh access/refresh pair. | gateway credential store |
| DELETE | `/auth/tokens/:tokenId` | Revoke one REST bearer token. Remote cookie callers must include CSRF. | gateway credential store |
| POST | `/messages` | Send a message (DM / post / publish / reply, by `to`). The acknowledgement contains only the durable `messageId`; recipient expansion is implicit and daemon-owned. | command intent: `message.post.send` |
| PATCH | `/messages/:id/metadata` | Replace a message's opaque metadata bag. Body: `{ "metadata": <any JSON> }`. | command intent: `metadata.set` |
| PATCH | `/sessions/:id/metadata` | Replace a session's opaque metadata bag. Body: `{ "metadata": <any JSON> }`. | command intent: `metadata.set` |
| PATCH | `/threads/:name/metadata` | Replace a thread's opaque metadata bag. Body: `{ "metadata": <any JSON> }`. | command intent: `metadata.set` |
| PATCH | `/agents/:id/metadata` | Replace an agent's opaque metadata bag. Body: `{ "metadata": <any JSON> }`; `:id` may be `agentId` or name. | command intent: `metadata.set` |
| POST | `/agents` | Spawn an agent (admin). | command intent: `admin.spawn` |
| DELETE | `/agents/:id` | Remove/detach, evict, or delete an agent by stable `agentId` or current name (admin). Remove retains history, marks the resolved session offline immediately, and releases harness-native resume ownership. Query: `kill`, `evict`, `delete`. | command intent: `admin.remove` / `admin.evict` / `admin.delete` |
| POST | `/agents/:id/tier` | Grant or revoke an agent's durable privilege tier (`agent` / `admin`). Human-admin gated. | command intent: `admin.grant_tier` |
| POST | `/agents/:id/access` | Grant delegated `/agent` session access. Body: `{ "principal": "...", "role": "viewer" \| "coOwner", "project": "..."? }`. | command intent: `agent.grant_access` |
| DELETE | `/agents/:id/access/:principal` | Revoke delegated `/agent` session access. Query: `project` (defaults to caller project). | command intent: `agent.revoke_access` |
| POST | `/agents/:id/owner` | Transfer managed-agent ownership. Body: `{ "owner": "...", "project": "..."? }`. | command intent: `agent.transfer_owner` |
| POST | `/agents/:id/credentials` | Create a runtime credential for a durable agent. `:id` may be `agentId` or name. | command intent: `agent.credential.create` |
| DELETE | `/agents/:id/credentials/:credentialId` | Revoke one runtime credential. | command intent: `agent.credential.revoke` |
| - | Agent policy groups | No REST route yet; use `nexus admin group assign`. | command intent: `admin.group.assign` |
| POST | `/threads` | Create a thread. | command intent: `thread.create` |
| POST | `/threads/:name/join` | Join a thread and backfill prior posts to the caller. | command intent: `thread.join` |
| POST | `/threads/:name/leave` | Leave a thread. | command intent: `thread.leave` |
| PATCH | `/threads/:name` | Rename a thread. Body: `{ "name": "new-name" }`. Admin-gated. | command intent: `thread.rename` |
| POST | `/threads/:name/archive` | Archive a thread so it no longer appears in active routing, history, or search. | command intent: `thread.archive` |
| DELETE | `/threads/:name` | Delete a thread registry and memberships while preserving durable message rows. | command intent: `thread.delete` |
| POST | `/threads/:name/members` | Add a thread member and backfill prior posts to that member. | command intent: `thread.add_member` |
| DELETE | `/threads/:name/members/:member` | Remove a thread member. | command intent: `thread.remove_member` |
| POST | `/topics/:name/subscribe` | Subscribe to a topic. | command intent: `topic.subscribe` |
| POST | `/topics/:name/unsubscribe` | Unsubscribe from a topic. | command intent: `topic.unsubscribe` |
| POST | `/status` | Set presence / pause / current work. | command intent: `presence.status` |
| POST | `/heartbeat` | Presence heartbeat (empty body). | command intent: `presence.heartbeat` |
| POST | `/inbox/consume` | Held-receive drain of your inbox. | command intent: `inbox.consume` |
| POST | `/inbox/ack` | Ack one message. | command intent: `inbox.ack` |
| POST | `/inbox/ack-threads` | Bulk-ack thread messages. | command intent: `inbox.ack_threads` |
| POST | `/register` | Register a member. | command intent: `identity.register` |
| POST | `/rename` | Rename the authenticated caller. | command intent: `identity.rename` |
| POST | `/routing-rules` | Create a standing routing rule. | not implemented (`501`) |
| POST | `/admin/channel` | Manage a topic / Pub-feed routing (admin). | command intent: `admin.channel` |
| POST | `/admin/route` | Ad-hoc one-shot forward of a notification (admin). | command intent: `admin.route` |
| POST | `/admin/monitor` | Web console event feed op (admin). | command intent: `admin.monitor` |
| POST | `/sources` | Register a notification source. | command intent: `source.register` |
| POST | `/sources/:name/enable` | Enable a notification source. | command intent: `source.enable` |
| POST | `/sources/:name/disable` | Disable a notification source. | command intent: `source.disable` |
| POST | `/sources/:name/rotate` | Rotate a notification source token. | command intent: `source.rotate` |
| DELETE | `/sources/:name` | Remove a notification source. | command intent: `source.remove` |
| POST | `/sources/:name/push` | Signed source push. | store token verification + command intent: `source.push` |

Durable identity reads and runtime-credential operations are exposed through `/agents/:id` and
`/agents/:id/credentials`. The existing collection write `POST /agents` keeps its operator-facing
meaning: admin-spawn a live runtime through the daemon launch path. This preserves the current UI
contract while adding REST parity for the stable identity/runtime registry.

> **Note on `POST /routing-rules`:** standing route-rule writes are not wired end-to-end yet. The
> gateway validates the request body and returns `501 not_implemented` instead of calling a missing
> daemon method. Reads of routing rules via `GET /routing-rules` are unaffected - they hit the read
> view.

## Request / response shapes

The request bodies below are validated by Zod schemas at the gateway. Optional fields are marked.

### `POST /messages`

The `to` field is a tagged union on `verb`:

| `to.verb` | extra field | meaning |
|---|---|---|
| `dm` | `name?`, `agentId?` | DM a member; at least one address is required |
| `post` | `thread` | post to a thread |
| `publish` | `topic` | publish to a topic |
| `reply` | (none) | reply into the current conversation (context-aware) |

Body:

```jsonc
{
  "to": { "verb": "dm", "name": "ben" },  // required (one of the four shapes above)
  "body": "rebase before you start",       // required string
  "summary": "rebase reminder",            // optional
  "mention": ["ada"]                        // optional string[]
}
```

DMs may be addressed by name, stable `agentId`, or both. Supplying both lets the UI retain the
current display name while routing remains rename-safe; `agentId` is authoritative and a mismatched
`name` is display metadata only. ID-only DMs address unnamed agents.

```bash
curl -s -X POST http://localhost:4100/api/v1/messages \
  -H 'content-type: application/json' \
  -H 'idempotency-key: web:dm:ben:client-uuid-1' \
  -d '{ "to": { "verb": "dm", "name": "ben" }, "body": "rebase before you start" }'
```

Returns the daemon `Ack` produced after the daemon claims the command intent and writes the bus rows
(the new message id, plus a fan-out count when relevant), status `201`.

Retry-prone clients should send an `Idempotency-Key` header, or an `idempotencyKey` string in the
JSON body. The gateway passes the key and resolved caller scope to the daemon; retries with the same
caller, Message Post command kind, and key return the original command result instead of enqueueing
a second bus write. The daemon also stores the key on the canonical Message
Post row under the sender session and returns the original `messageId` if a duplicate reaches the
bus writer directly. Keyless requests are protected by a short same-sender/same-target/body
duplicate window.

If Gateway message hooks are configured, every genuinely new logical send uses the same
`before_send` pipeline before daemon acceptance, including sends originating outside REST. Hook
metadata and signed provenance are committed with the message. An idempotent retry returns the
original message rather than running the hook again. See [Message hooks](hooks.md).

### Hook diagnostics

The hook endpoints are read-only; manifests remain local files. All three require normal Gateway
authentication except where local operator mode supplies it automatically.

```bash
curl -s http://localhost:4100/api/v1/hooks
curl -s http://localhost:4100/api/v1/hooks/public-key
curl -s 'http://localhost:4100/api/v1/hooks/audit?limit=20'
```

`GET /hooks/audit` verifies stored execution signatures against the Gateway key and includes a
`verified` boolean. Non-admin callers receive redacted entries without local command results or
paths. A missing hook service returns `503` rather than an empty registry.

### Entity metadata

Messages, sessions, threads, and agents each carry an opaque metadata JSON bag. Nexus does not
interpret the payload; integrations and launch tooling own its shape. The gateway requires an
authenticated Principal but does not require a route scope or admin tier for metadata reads/writes.
Writes still enqueue the daemon-owned `metadata.set` command, so the gateway never mutates canonical
tables directly.

```bash
curl -s -X PATCH http://localhost:4100/api/v1/messages/m_123/metadata \
  -H "authorization: Bearer $NEXUS_REST_TOKEN" \
  -H "content-type: application/json" \
  -d '{"metadata":{"externalId":"ticket-42","reviewed":true}}'

curl -s http://localhost:4100/api/v1/messages/m_123/metadata \
  -H "authorization: Bearer $NEXUS_REST_TOKEN"
```

### Scoped REST bearer tokens

`POST /auth/tokens` issues a machine REST credential for an Admin-tier authenticated browser/local
Principal. Requested scopes must be allowed by the issuing Principal; unsupported scopes or
insufficient tier return `403`.

Local bundled bridges can use `POST /auth/operator-token` during first boot to mint the same kind of
scoped bearer for the human name the operator typed. The token row stores that chosen name and
`kind=human`, so subsequent REST writes attribute to the human operator, not to a
shared bridge identity. This bootstrap route is local-mode only; remote deployments must log in and
issue tokens through `/auth/tokens`.

```jsonc
{
  "scopes": ["message:read", "message:send"], // required non-empty string[]
  "ttlMs": 900000,                            // optional access-token TTL
  "refreshTtlMs": 2592000000                  // optional refresh-family TTL
}
```

Response:

```jsonc
{
  "tokenId": "bt_...",
  "familyId": "bf_...",
  "accessToken": "nx_at_...",
  "refreshToken": "nx_rt_...",
  "expiresAt": 1780000000000,
  "refreshExpiresAt": 1782500000000,
  "scopes": ["message:read", "message:send"],
  "tier": "admin"
}
```

`POST /auth/tokens/refresh` accepts `{ "refreshToken": "nx_rt_..." }` and rotates the refresh row:
the old access token stops working and a new access/refresh pair is returned. Reusing an already
used refresh token revokes the refresh family. `DELETE /auth/tokens/:tokenId` revokes that token.

### `POST /notify`

Public v0.1 requires `NEXUS_HMAC_SECRET` on both the supervised gateway and daemon. Sign the exact
raw JSON bytes as `HMAC-SHA256(secret, "<unix-ms>.<raw-body>")`; raw-body-only signatures are not
accepted. The timestamp must be within five minutes. Missing service configuration returns
`503`; a missing, malformed, or stale signature returns `401`. Every rejection happens before
command ingress, so it creates no command, message, delivery, or notification-audit row.

```jsonc
{
  "source": "github",          // required
  "topic": "ci",               // optional
  "payload": { "run": "1234", "status": "green" }  // required; any JSON value
}
```

```bash
secret="$NEXUS_HMAC_SECRET"
ts="$(date +%s%3N)"
raw='{ "source": "github", "topic": "ci", "payload": { "status": "green" } }'
sig="$(printf '%s' "$ts.$raw" | openssl dgst -sha256 -hmac "$secret" -hex | awk '{print $2}')"
curl -s -X POST http://localhost:4100/api/v1/notify \
  -H 'content-type: application/json' \
  -H "x-nexus-timestamp: $ts" \
  -H "x-nexus-signature: sha256=$sig" \
  --data-binary "$raw"
```

Returns the verified notify result after the daemon claims and independently re-verifies the
durable command envelope, status `201`. Gateway retries reuse the command id derived from the
timestamp and signature. Daemon reclaim derives a separate deterministic bus key for Pub, routed
topic, and each routed DM, so routed agent messages/deliveries/injections remain exactly once.
The standalone notification message and audit row are observational; a daemon crash after
routing commits but before the command receipt may recreate those rows without duplicating the
routed agent delivery.

### `GET /threads`

```bash
curl -s http://localhost:4100/api/v1/threads
```

### `GET /members`

```bash
curl -s 'http://localhost:4100/api/v1/members?includeOffline=true'
```

### `GET /search`

```bash
curl -s 'http://localhost:4100/api/v1/search?q=auth%20refactor&limit=10'
# `mode` may be fts, hybrid, or semantic. Core keeps FTS; semantic returns no
# hits until the semantic-search plugin provides vector storage.
curl -s 'http://localhost:4100/api/v1/search?q=rebase&mode=hybrid&thread=backend'
# DM search requires a caller and is limited to that caller's participant pair.
curl -s 'http://localhost:4100/api/v1/search?q=credential&with=ben&since=1782980000000'
```

Unscoped REST search returns thread/topic rows plus only the authenticated caller's own DM rows.
It does not leak other participants' direct messages.

### Durable agents and runtimes

```bash
curl -s http://localhost:4100/api/v1/agents/ben
curl -s 'http://localhost:4100/api/v1/agents/ben/runtimes?includeStopped=true'
curl -s 'http://localhost:4100/api/v1/runtimes?agent=ben'
```

Create and revoke runtime credentials through the same daemon command kinds as the CLI:

```bash
curl -s -X POST http://localhost:4100/api/v1/agents/ben/credentials \
  -H 'content-type: application/json' \
  -d '{ "label": "headed laptop", "scopes": ["runtime:register"] }'

curl -s -X DELETE http://localhost:4100/api/v1/agents/ben/credentials/cred_...
```

Owners and co-owners can delegate managed `/agent` session observation without changing ownership:

```bash
curl -s -X POST http://localhost:4100/api/v1/agents/ben/access \
  -H 'content-type: application/json' \
  -d '{ "principal": "alice", "role": "viewer" }'

curl -s -X DELETE 'http://localhost:4100/api/v1/agents/ben/access/alice'
```

`POST /register` accepts the durable runtime binding fields from the shared contract:
`agentId` and `runtimeCredential`. Client-key-only registration is also a supported runtime shape
when the caller does not bind a durable agent identity.

### Discovery

```bash
curl -s http://localhost:4100/api/v1/capabilities
curl -s http://localhost:4100/api/v1/openapi
```

The discovery endpoints expose the REST spine currently available at the gateway. The router's
`/api/v1` route registry is the source of truth for both request dispatch and discovery metadata:
each route carries its read or command projection in the row that registers the handler. OpenAPI is
generated from that registry and groups every method under the same path, so same-path endpoints
such as `GET`/`DELETE /agents/:id` or `PATCH`/`DELETE /threads/:name` remain visible instead of one
method overwriting another. Edge-owned bearer-token routes handled before the pure router remain
documented here separately from the route-registry projection.

### `GET /threads/:name/history`

Rows include `cursor: { createdAt, rowid }`. Pass both fields back as `after` and
`afterRowid` to resume strictly after that row, including when multiple messages
share one millisecond. Omitting `after` returns the latest bounded page in
chronological order.

```bash
curl -s 'http://localhost:4100/api/v1/threads/backend/history?limit=50'
curl -s 'http://localhost:4100/api/v1/threads/backend/history?limit=50&after=1783947000000&afterRowid=42'
```

### `GET /dms/:name/history`

Returns the caller's direct-message history with `:name`. The web console uses this to hydrate a
DM pane from the daemon-backed Message Post store before it live-tails new committed messages.
It uses the same exact `after` + `afterRowid` forward cursor as thread history. The browser does not
write or read a second conversation-message cache.
DM rows are matched by the real Message Post participant pair (`from_name`/`to_name`), not by a
synthetic thread id.

```bash
curl -s 'http://localhost:4100/api/v1/dms/ben/history?limit=50'
```

### Observe stream transport behavior

Gateway Message Post observers for the same canonical thread, topic, or DM target share one
process-level daemon event relay; project metadata does not partition that relay. Subscriber cursors are
independent and advance only after the corresponding response accepts an event. SSE observe routes
send comment heartbeats while idle, pause their upstream Message Post or Agent Session source when
the response is backpressured, and resume from the last accepted cursor. These mechanics do not
change the public event envelope or the REST/WebSocket route shapes.

### Notification sources

Source reads are daemon-owned projections and never include the secret token:

```bash
curl -s http://localhost:4100/api/v1/sources
curl -s http://localhost:4100/api/v1/sources/github-ci
```

Source management writes enqueue daemon commands:

```jsonc
{ "name": "github-ci", "topic": "builds" }  // POST /sources; topic optional
```

```bash
curl -s -X POST http://localhost:4100/api/v1/sources \
  -H 'content-type: application/json' \
  -d '{ "name": "github-ci", "topic": "builds" }'

curl -s -X POST http://localhost:4100/api/v1/sources/github-ci/rotate
```

`POST /sources/:name/push` verifies the producer HMAC in the gateway using the stored source token,
then enqueues `source.push` as a source Principal for the daemon:

```bash
raw='{"summary":"Deploy started","body":"web v1.4.2 -> prod","meta":{"service":"web"}}'
ts="$(date +%s%3N)"
sig="sha256=$(printf '%s.%s' "$ts" "$raw" | openssl dgst -sha256 -hmac "$SOURCE_TOKEN" -hex | awk '{print $2}')"

curl -s -X POST http://localhost:4100/api/v1/sources/github-ci/push \
  -H 'content-type: application/json' \
  -H "x-nexus-timestamp: $ts" \
  -H "x-nexus-signature: $sig" \
  -d "$raw"
```

Accepted source pushes return `{ topic, messageId, queuedTo }`; `queuedTo` counts committed
recipient rows and does not claim model delivery. The HMAC timestamp and signature form a stable
command key which the daemon carries into the canonical topic publish. Reclaiming the same command
after a commit-before-receipt crash therefore returns the original `messageId` and never creates a
second delivery or automatic harness injection. Those canonical Message Post topic rows are
projected by `GET /notifications` into the Pub feed shape, including the source name, topic, and
recipients woken from
`in_flight`, so `/pub` reflects real signed pushes rather than only notification-audit rows.
The optional `before` cursor is applied independently to both backing sources and each source is
limited before the newest-first merge, keeping one page bounded even when either table is large.
The daemon computes the wake set from those committed delivery rows, not from a later subscription
snapshot. Offline resumable agent subscribers are revived after commit. A transient bootstrap failure before
native injection is retried: the first two non-dead failures are retained and re-driven, the third
settles the delivery. Dead or missing runtimes settle immediately to terminal `target_dead` /
`target_unreachable`. Once terminal, a delivery is not retried unless an operator explicitly requeues
it, and attempted native input is never replayed.

### Other common bodies

`POST /register`:

```jsonc
{
  "name": "ben",                 // required
  "harness": "claude",           // required: claude | codex | opencode | hermes | pi | other
  "harnessSessionId": "hs_ben",  // required
  "project": "default",          // required
  "clientKey": "ck_ben",         // required
  "tier": "agent",               // required: agent | admin (the tier enum)
  "kind": "agent",               // optional: agent | app
  "role": "lead",                // optional
  "cwd": "/path"                 // optional
}
```

`POST /threads`:

```jsonc
{ "name": "backend", "members": ["ada", "dylan"] }   // members optional
```

`POST /status`:

```jsonc
{ "state": "paused", "work": "compacting" }          // both optional; state: active|busy|paused
```

`POST /topics/:name/subscribe`:

```jsonc
{ "group": "workers" }                               // optional
```

`POST /inbox/consume`:

```jsonc
{ "timeoutMs": 5000, "max": 10 }                     // both optional
```

`POST /inbox/ack`:

```jsonc
{ "messageId": "n_01" }                              // required
```

`POST /inbox/ack-threads`:

```jsonc
{ "messageIds": ["n_02", "n_03"] }                   // required
```

`POST /agents` (admin spawn):

```jsonc
{
  "kind": "codex",
  "name": "dylan",
  "cwd": "/repo",
  "headless": true,
  "initialPrompt": "Review the changes in <var.cwd>."
}
// kind required; the rest optional
```

The supported request fields are exactly `kind`, `name`, `cwd`, `headless`, and `initialPrompt`.
`kind` is a harness id; `name`, when supplied, is a nonempty string, and `cwd` is a string.
`headless` must be a boolean: `true` requests the daemon's headless runner, while `false` requests
a headed launch. If omitted, the Gateway leaves it absent and the daemon defaults to `false`.
`initialPrompt` must be a string and is forwarded unchanged, including empty strings and whitespace;
if omitted, it stays absent. It is the fresh-launch boot prompt template: the daemon expands
`<var.*>` after allocating identity/runtime and rejects it on resume/reuse paths. The Gateway
submits one `admin.spawn` command with these options and the authenticated caller, without a
separate prompt send, launch-mode fallback, or retry. Harness/platform launch support remains the
daemon's responsibility.

Existing callers supplying only `kind`, `name`, and `cwd` retain their behavior. Older Gateways
without these launch-option fields reject them; clients must not silently drop them or retry in
another mode. Unknown keys and invalid field types (including `null`) return HTTP 400 before
command submission. Authentication and admin authorization are unchanged.

This spawns a runtime. It does not call `agent.create`; the daemon creates or reuses the durable
identity behind the launch/spawn path. Project and display-role labels are consumer affordances,
not transport identity. Store them as opaque consumer metadata through
`PATCH /agents/:id/metadata` (for example under a consumer-owned `lens` key); they are neither
accepted by this request nor promoted by `GET /agents/:id`.

`POST /admin/channel`:

```jsonc
{ "op": "create", "topic": "ci", "source": "github" }  // op + topic required; source optional
```

`POST /admin/route` (ad-hoc forward):

```jsonc
{ "notif": "n_01", "to": "ben" }                     // both required
```

`POST /admin/monitor`:

```jsonc
{ "follow": true, "scope": "backend" }               // follow required; scope optional
```

`POST /routing-rules` (standing rule; see the note above about end-to-end wiring):

```jsonc
{ "source": "github", "to": "ben" }   // `to` required; at least one of `source`/`topic`
```

## See also

- [cli.md](cli.md) - the same capabilities from the `nexus` CLI.
- [architecture.md](architecture.md) - how the gateway and daemon relate.
