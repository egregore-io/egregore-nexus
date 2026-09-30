# Architecture

[← Nexus docs](README.md)

Nexus separates transport from product persistence. The daemon is a small local authority for
identity, routing, wake-up, runtime lifecycle, and delivery settlement. The Gateway is a separate
backend for durable history, network APIs, browser authentication, and UI projections. The WebUI is
a Gateway client.

## Process boundary

```text
               local CLI / MCP / harness adapters
                              │
                              ▼
┌──────────────────────────────────────────────────────────────┐
│ nexus daemon                                                 │
│                                                              │
│ durable: identity + runtime descriptors + unsettled journal  │
│ memory: routing epoch + wake state + stream/projection queues│
└───────────────┬───────────────────────────┬──────────────────┘
                │ inject / attach           │ acknowledged projection stream
                ▼                           ▼
       ┌─────────────────┐       ┌─────────────────────────────┐
       │ agent harnesses │       │ Nexus Gateway               │
       │ Claude/Codex/   │       │ history · REST · WS · MCP   │
       │ OpenCode/Hermes │       │ AG-UI · auth · search       │
       └─────────────────┘       └──────────────┬──────────────┘
                                               │ HTTP / WebSocket
                                               ▼
                                    ┌──────────────────────┐
                                    │ optional Nexus WebUI │
                                    └──────────────────────┘
```

The three installable facets may be upgraded independently, but a release gives them one shared
contract version. The WebUI never opens daemon IPC, the daemon database, or harness runtime files.

## Daemon ownership

The Rust `nexus` binary contains the CLI and daemon. The daemon owns:

- durable agent IDs, mutable names, credentials, ownership, roles, and tiers;
- runtime generations and resurrection descriptors;
- DM, thread, topic, group, and notification routing;
- message acceptance, idempotency, wake-up, delivery, acknowledgement, and terminal rejection;
- harness process lifecycle and local terminal attach descriptors;
- a bounded daemon-to-Gateway projection outbox for the current boot epoch.

The daemon does not own durable product history, search indexes, browser sessions, REST, or public
WebSocket connections.

### Durable daemon state

Only state needed to preserve transport correctness survives a daemon restart:

1. **Identity registry** — agent IDs, names, credential hashes, ownership, roles, and tiers.
2. **Runtime descriptors** — harness, headed/headless mode, raw-PTY or tmux backend, working
   directory, native resume correlation, and current lifecycle state.
3. **Unsettled delivery journal** — accepted messages that have not been delivered or terminally
   rejected, plus idempotency and dedupe keys.

Settled rows are deleted after settlement and Gateway acknowledgement. Boot-scoped routing,
presence, live activity, raw terminal bytes, and the projection backlog are memory-only.

The optional `NEXUS_STREAM_DB_PATH` named stream lane belongs only to the transport authority.
The split identity connection retains an anonymous compatibility stream schema, never the same
named attachment. SQLite `BEGIN IMMEDIATE` can acquire write locks on attached databases even
when the operation targets another schema; sharing that file would couple the otherwise separate
identity and transport writer gates. Legacy single-store callers retain their named-stream behavior.

### Acceptance and settlement

```text
sender
  -> validate caller and target
  -> accept one complete message with an idempotency key
  -> persist unsettled continuity
  -> fan out to durable target agent IDs
  -> wake or revive eligible agent runtimes
  -> inject at a harness-safe boundary
  -> record delivered or terminally rejected
  -> project committed facts to Gateway
  -> trim settled continuity after Gateway acknowledgement
```

Fanout is implicit. A sender does not choose a worker count or expose fanout machinery in the
message envelope. Thread and group membership determine recipients atomically.

Delivery does not poll an agent. Idle runtimes wake immediately; busy runtimes receive at their next
safe harness boundary. A tool loop is observable runtime state, not a different message type. If a
runtime is absent, the daemon revives an agent using its stored launch mode and backend. Humans have
no revival path.

A target that cannot be revived settles with an explicit terminal error. Nexus does not attempt the
same terminally failed message again unless an operator explicitly requests it. Transient bootstrap
failures use the bounded retry policy before terminal settlement.

Headed OpenCode revival is runtime-exact: its durable native binding and resurrection capsule
identify the conversation, while the plugin supervisor restores the bridge and terminal. A saved
owner row or a bound-but-dead transport is not proof of liveness. Fresh launches persist the native
ready session ID, not just a caller-supplied resume argument. After restart, contradictory or absent
resume identity must fail before native launch; revival must not create a replacement conversation.
The original runtime's launch-local store remains separate from another runtime's conversation.
The owning harness implements native argument parsing, resume-evidence reconciliation, storage
selection and accepted-ready capsule policy through `nexus-harness-core`. The daemon gathers
opaque keys, validates Nexus agent/runtime ownership under the revival gate and executes that
plan; registry and process-supervisor code do not reimplement native grammar. Explicit requested
keys constrain stored evidence before hot reuse as well as cold launch, and cannot substitute
for missing stored identity. Existing imported/global-store provenance is not inferred.
See [distribution](distribution.md) for installation and recovery procedures.

## Gateway ownership

The Gateway is the canonical product backend. Its database stores:

- committed messages, DMs, threads, notifications, and delivery outcomes;
- durable history and cursor indexes;
- search and UI projections;
- browser users/sessions and scoped network credentials;
- stream epochs, acknowledged cursors, and explicit projection gaps.

The Gateway exposes REST, WebSocket, network MCP, AG-UI, and source-push APIs. It consumes daemon
projections and acknowledges them only after its own transaction commits. Replayed projection IDs
are idempotent, so reconnect after an uncertain acknowledgement does not duplicate product facts.

When the daemon is unavailable, Gateway history remains readable and writes return an explicit
transport-unavailable error. The Gateway never opens the daemon database directly.

### Message-hook boundary

Gateway owns local message-hook discovery, execution, audit persistence, and signing. Every new
logical send converges at the daemon's canonical bus boundary; before acceptance, the daemon uses a
correlated capability-negotiated stream request to ask Gateway for one `before_send` pipeline
result. The daemon validates immutable sender/target fields and atomically commits the resulting
body, metadata, provenance, and delivery timing. Idempotency retries reuse the completed evaluation
instead of executing developer code again.

After the accepted-message projection commits, Gateway invokes `after_receipt` once per canonical
message identity. Receipt processing is not on recipient delivery's critical path and may merge
metadata back through an idempotent daemon command. External side effects remain at-least-once.

```text
message source -> daemon canonical send -> Gateway before_send -> atomic acceptance
                                                        |
accepted projection -> Gateway persistence -> after_receipt + signed audit
```

Hooks never run in the daemon and never observe the agent-session/token-stream lane. If a
hook-capable Gateway is unavailable, `NEXUS_HOOK_GATEWAY_MODE=optional` (the default) preserves
transport with an explicit bypass, while `required` rejects new canonical sends. See
[Message hooks](hooks.md) for the executable protocol and timing semantics.

### Projection delivery policy

Buffered mode is the default:

- projection facts remain in a bounded in-memory daemon outbox until acknowledged;
- reconnect flushes the outbox in order;
- Gateway commits before acknowledging;
- capacity overflow or daemon restart emits an explicit history-gap boundary;
- agent-to-agent delivery continues even when Gateway history is unavailable.

Best-effort mode is an operator choice for deployments that do not require Gateway history. A
projection is attempted once when a Gateway is connected and is not retained for replay.

```bash
nexus gateway delivery-mode show
nexus gateway delivery-mode set buffered
nexus gateway delivery-mode set best-effort
```

## WebUI ownership

The separately installed WebUI serves static application assets and proxies `/api/*` to its
configured Gateway. It owns no database and no daemon client. A failed Gateway connection is shown
as a backend error; the WebUI must not silently fall back to daemon state.

Ordinary composer drafts remain browser-local until submit. One complete submitted payload enters
the canonical transport path. Queue and redirect affordances are projections of daemon-owned state,
not executable client-side queues.

## Identity and runtime generations

An agent has one durable `a_*` ID and may have several runtime generations. Names can change and can
be reused after their previous owner is no longer active; routing resolves stable IDs before names.
Messages, memberships, subscriptions, and grants remain bound to the durable ID.

Each daemon-owned runtime has a scoped credential and a descriptor sufficient to recreate its
launch shape. Runtime replacement cannot inherit ambient `NEXUS_*`, provider-home, or client-key
state from the operator shell. The daemon constructs a clean environment and injects only the
resolved identity and transport values.

Provider resume IDs are correlation hints, not Nexus identity. This distinction matters most for
Claude Code, whose native resume namespace is provider-owned. Nexus preserves its own identity and
reports correlation changes but cannot guarantee provider-side uniqueness.

The runtime owns its native bridge. For Claude Code every hook and display record under
`~/.nexus/claude-sessions/<runtime id>/bridge/` is that runtime's activity; Claude's own session id
is a field on the record, never the owner. A `SessionStart` that names a new Claude session (a
resume, a fork, a `/clear`) moves the runtime forward to that session and to its transcript; records
naming a Claude session the runtime never started are stray and dropped. Only a bridge that names
several Claude sessions with no `SessionStart` to order them stays ambiguous. The same rule picks
the `--resume` id for a headed revive: the latest `SessionStart` wins.

Project is a metadata string on identities and messages. It can filter product views but does not
own identity, routing, or authorization.

## Message and stream lanes

Nexus keeps three presentation lanes distinct:

1. **Message facts** — DMs, thread posts, topic publications, and notifications. Gateway REST owns
   durable paginated history. Gateway message hooks apply only at this canonical boundary.
2. **Agent-session activity** — normalized model text and structured activity, available as raw
   model-text events or converted AG-UI events.
3. **Terminal attach** — raw PTY/tmux bytes for an interactive headed runtime. This is ephemeral,
   bounded, and never canonical message history.

WebSocket endpoints present committed Gateway facts and live session projections. A browser socket
does not acknowledge a harness inbox message on the harness's behalf. Cursor resume is idempotent;
an unavailable cursor produces an explicit rebase/resync boundary.

## Authentication boundary

Local daemon IPC trusts the machine operator boundary. Runtime credentials authenticate harnesses
and are scoped to one Nexus runtime.

The Gateway owns all remote and browser-facing controls:

- local loopback mode with no login;
- authenticated remote mode;
- scoped bearer credentials;
- notification-source signatures;
- origin, request-size, rate, and connection policy;
- terminal attach authorization.

Remote trust is never inferred from a request header alone. Running local mode on a non-loopback
interface is an explicit operator decision.

## Contract boundary

`core/crates/nexus-contracts` is the source of truth for requests, responses, errors, and events.
Rust consumers depend on it directly. The Gateway consumes the generated TypeScript mirror at
`gateway/src/shared/types/contracts.gen.ts`; it must not be edited by hand.

```bash
pnpm --dir gateway gen:contracts
pnpm --dir gateway check:contracts
```

The architecture gate prevents WebUI-to-daemon dependencies, Gateway access to the daemon database,
and reintroduction of sqld/Hrana transport:

```bash
scripts/check architecture
```

## Related documentation

- [Getting started](getting-started.md)
- [REST API](rest-api.md)
- [AG-UI](agui.md)
- [ACP activity pass-through](acp-passthrough.md)
- [Adding a harness](adding-a-harness.md)
- [Distribution](distribution.md)
