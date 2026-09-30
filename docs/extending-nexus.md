# Extending Nexus

Nexus is a neutral realtime bus: it routes and wakes, nothing more. There is no orchestrator
to plug into — **extensions live outside the daemon and ride its stable surfaces**. Gateway-owned
message hooks are local extension programs, never daemon plug-ins.

The invariants your extension must respect:

- **No orchestrator.** The bus never coordinates; your controller does, as a peer.
- **Context hygiene.** An agent's context holds only what is addressed to it. Extensions that
  gate delivery *strengthen* this invariant; never build one that leaks unaddressed traffic in.
- **Metadata is yours.** Nexus stores metadata on agents, sessions, messages, and threads and
  **never interprets it**. It is the sanctioned place for extension state and policy flags.
- **Identity is possession of a credential.** Your controller is just another registered
  principal — an agent identity (client key + optional runtime credential) or an operator.

> **Discover, do not guess.** `GET /api/v1/capabilities` reports each protocol surface
> independently. Hook events come from the live hook registry, and
> `protocol.surfaces.transports.providers` comes from the running Gateway transport host (`[]`
> means no host is active; `disabled` is reported rather than hidden).

## Extension surfaces (all shipped)

| Surface | Direction | What it gives you |
|---|---|---|
| **Metadata bags** | read/write | Free-form JSON on agents (`nexus launch --meta '<json>'`, `PATCH /api/v1/agents/{id}/metadata`), sessions, messages, threads. Daemon-ignored by design. |
| **Events lane** (WS) | read | Durable `sys.*` developer-event topics over `/api/agui/ws`: `{"t":"subscribe","topic":"sys.agent.lifecycle","afterSeq":N}`. Ring replay on reconnect — never poll. Topics: `sys.agent.lifecycle`, `sys.fleet.status`, `sys.thread.<name>`. |
| **Observe / AG-UI** (WS) | read/write | Per-session streams (`?session=<name>&afterId=<cursor>`) and input frames (`{"t":"input",...}` acked by `input.ack` / rejected by `input.err`, correlated on `clientMessageId`). |
| **REST projection** | read/write | Everything the daemon exposes, discoverable at `GET /api/v1/capabilities`. |
| **Sources** (inbound webhooks) | write | Named ingress with HMAC over the raw body: `POST /api/v1/sources/{name}/push`. Verified payloads land on the Pub monitor feed; they reach an agent only via a standing route rule (`admin route`) or a one-shot forward. |
| **Message hooks** | read/write | Local shell, JavaScript, Python, or native programs at `before_send` and `after_receipt`. They can transform message fields, add metadata, select named delivery timing, reject before acceptance, and run receipt side effects. See [Message hooks](hooks.md). |
| **CLI as automation** | read/write | Register your controller as an agent; `nexus listen --json` is the same drain loop real agents use. |
| **Terminal socket** | read/write | Per-session UDS with a binary frame protocol (`terminal_socket.rs`); manifests under `~/.nexus/terminal-endpoints/`. Host headed terminals in any emulator. |

## Delivery-shaping recipes

Delivery into an agent's context is gated by exactly one thing: **fan-out follows thread
membership**, and injection follows the **wake policy**. Both are external levers.

### Recipe 1 — membership proxy (recommended)

Goal: an agent receives thread traffic only when mentioned (or any rule you encode in
metadata).

1. Keep the target agent **out** of the noisy thread. No membership → no fan-out → nothing
   enters its context. Ever.
2. Register your controller as its own agent and join it to the thread. It drains normally
   (`nexus listen --json`) or watches read-only via the events lane.
3. When a message matches your rule — the `--mention` field, or a convention in message
   metadata — **DM the target**. DMs always deliver, and the target's context only ever
   contains addressed mail.

Properties: no TTL concerns (mail is only ever addressed when wanted), no special tiers, and
the coordination lives in an agent — the shape the no-orchestrator design wants.

### Recipe 2 — pause-gate (hold and release)

The wake policy is absolute about pause: **`Paused → Hold`, unconditionally** — even a
human's message holds ("a human resumes, they do not punch through"). Held mail stays
durably `pending` and re-drives on resume; nothing is lost.

1. The target sits `paused` by default (`nexus status paused` — pause/resume is the
   self-status surface, so your controller acts with the agent's own credential, which you
   hold as its operator).
2. Your controller watches the stream; on a mention it flips the agent `active`, the queue
   drains, then it re-pauses.

Caveats: held mail ages against `NEXUS_DELIVERY_TTL_MS` toward the dead-letter queue — tune
the TTL or requeue on resume (`admin dlq` surfaces exist). Prefer Recipe 1 when the gate is
long-lived.

### Recipe 3 — external notifier (inbound)

CI / GitHub / cron → `POST /api/v1/sources/{name}/push` with the HMAC header → Pub monitor
feed → `admin route` rule delivers to the subscribed agent. Bad signatures are recorded and
dropped; landing in Pub never by itself pushes to an agent.

### Recipe 4 — custom monitor / dashboard

Subscribe the events lane with `afterSeq` cursors per topic. Reconnects replay the gap from
the server-side ring — your consumer never misses events and never polls.

### Recipe 5 — canonical message policy

Put a `before_send` manifest in `$NEXUS_HOME/gateway/hooks.d` when a rule must cover every
canonical source: CLI, REST, network MCP, notification, or agent-originated traffic. The program
can rewrite body/summary/mentions, merge developer metadata, choose `interrupt`, `yield_turn`, or
`after_tool_loop`, or reject before the daemon commits the message. Use `after_receipt` for
message-ID-aware local automation. Hooks do not see token streams or agent-session events.

## Seams deliberately left open (not yet features)

- **Native per-source wake gating**: `WakePolicy::should_wake(state, source)` carries the
  message-source `Kind` specifically so per-source routing (e.g. subscription gating, native
  mention-only delivery keyed off agent metadata) can land **without a contract change**.
  Today the decision ignores `source`; `--mention` is a soft highlight, never routing.
- **Outbound webhooks**: `sessions.callback_url` is a dormant schema column; there is no
  push-to-URL hook runner. The events lane is the outbound story. If your extension needs HTTP
  push, run a small events-lane consumer that forwards. A future callback runner can reuse the
  message-hook invocation protocol, but it is not shipped in v0.1.5.
- **Routing rules over REST**: `POST /api/v1/routing-rules` is a deliberate `501`; rules are
  created via `admin route`. The read side (`GET /api/v1/routing-rules`) works.

## Rules for extenders

1. **Subscribe, don't poll.** The events lane and observe sockets exist so nothing has to.
2. **Handle `input.err`.** Sends are correlated by `clientMessageId`; a rejection is a frame,
   not an exception. Dropping it makes failures invisible.
3. **HMAC everything inbound.** Sources reject unsigned payloads by design; so should you.
4. **Never impersonate.** One identity per controller; act on other agents only through
   surfaces that permit it (DMs, admin tier where granted, or credentials you legitimately
   hold as their operator).
5. **Policy state goes in metadata**, not in side files the fleet can't see.
6. **Make receipt side effects idempotent.** `after_receipt` is at-least-once; use its stable
   `invocationId` as the external dedupe key.
