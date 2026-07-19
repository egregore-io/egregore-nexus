# Gateway Message Hooks Design

**Target release:** Nexus v0.1.5

**Status:** Approved direction, recorded for implementation planning

**Date:** 2026-07-19

## Summary

Nexus v0.1.5 adds a language-neutral hook spine owned by Gateway. A hook is an ordinary local
command—typically a shell script, but equally a JavaScript, Python, or native executable—that
receives one versioned JSON document on stdin and returns one JSON document on stdout.

The initial release exposes two public message-boundary events:

- `before_send`: may transform the message, add metadata, reject it, or select a delivery timing
  policy before canonical acceptance.
- `after_receipt`: runs once after Nexus has assigned the canonical message ID and returned the
  public send receipt. It may add metadata and perform side effects, but it cannot change content
  that has already been accepted.

The hook engine, runner, and event adapter boundaries are generic. Future discrete events and
future HTTP callback delivery use the same protocol and execution machinery. v0.1.5 does not ship
language SDKs, callback delivery, programmable timing policies, or token-stream hooks.

## Goals

1. Let developers alter a message immediately before it is sent.
2. Let developers run local automation and add metadata after the sender receives a canonical
   message receipt.
3. Let `before_send` choose one of three explicit delivery policies: `interrupt`, `yield_turn`, or
   `after_tool_loop`.
4. Cover every canonical message source, including Gateway REST/WS, CLI, MCP, and messages created
   by agents, without moving developer-code execution into the daemon.
5. Support shell, JavaScript, Python, and arbitrary executable programs through the same JSON
   contract.
6. Record which exact declared hook artifact ran and attach Gateway-signed execution provenance.
7. Establish an event-neutral spine that can expose additional non-token events without replacing
   the engine or runner.

## Non-goals

- Hooking token deltas, raw model streams, or other agent-session-lane traffic.
- Python or JavaScript SDK packages. Those can later wrap the JSON protocol.
- HTTP callbacks, hosted workers, or remote hook execution.
- A mutating hook-management CLI or REST registry.
- A programmable `custom` delivery policy.
- Arbitrary rerouting, sender impersonation, target mutation, or thread-membership mutation.
- Exactly-once guarantees for arbitrary external side effects.
- Sandboxing code from a developer who already controls the local Gateway account.

## Architectural Boundary

Hooks are a Gateway feature. Gateway owns hook discovery, validation, ordering, command execution,
audit persistence, signing keys, and provenance. The daemon remains the lightweight transport
authority and never loads or executes developer code.

All message sources converge at the daemon's canonical send boundary. Before the daemon accepts a
new message, it asks the connected hook-capable Gateway to evaluate `before_send`. This extends the
existing authenticated, full-duplex local daemon–Gateway stream with correlated request/result
frames. A Gateway-originated send uses the same round trip as CLI-, MCP-, and agent-originated
sends; there is no privileged bypass.

After canonical acceptance, the daemon emits the existing durable message projection with the
public receipt. Gateway uses that projection to invoke `after_receipt` idempotently. This phase is
not on the recipient delivery path and never delays or retracts an accepted message.

The system is divided into three focused units:

```text
Hook Event Adapter -> Hook Engine -> Hook Runner
```

- A **Hook Event Adapter** defines the event payload, allowed result fields, whether execution is
  blocking, and how a valid result changes product state.
- The **Hook Engine** discovers matching hooks, applies deterministic ordering and composition,
  enforces timeouts and failure policies, persists audit records, and creates signed provenance.
- A **Hook Runner** invokes a handler using a transport-neutral `HookInvocation`/`HookResult`
  contract. v0.1.5 ships only `LocalCommandRunner`; a future `HttpCallbackRunner` implements the
  same interface.

## Developer Surface

Gateway discovers hook manifests under:

```text
$NEXUS_HOME/gateway/hooks.d/
```

Scripts may live beside their manifests. A typical directory is:

```text
hooks.d/
|-- 010-redact-secrets.toml
|-- redact-secrets.py
|-- 020-route-timing.toml
`-- route-timing.js
```

A manifest is declarative configuration, not a plugin package:

```toml
version = 1
id = "redact-secrets"
event = "before_send"
order = 10
timeout_ms = 500
on_failure = "reject"
enabled = true

[handler]
kind = "local"
entry = "./redact-secrets.py"
entrypoint = "main"
run = ["python3", "{entry}"]
pass_env = []
```

Rules:

- `id` is unique across the active hook directory.
- `event` is an extensible string resolved through the Gateway event-adapter registry.
- `order` sorts numerically; equal values sort by hook ID.
- `timeout_ms` defaults to 1,000 milliseconds and must be between 1 and 30,000 milliseconds.
- `on_failure` is `continue` or `reject`. `reject` is valid only for blocking events such as
  `before_send`.
- `entry` resolves relative to the manifest and is the artifact Gateway hashes.
- `entrypoint` is the developer-declared logical function or command entrypoint recorded in
  provenance.
- `run` is an argv array. `{entry}` expands to the resolved entry path. Gateway performs no shell
  interpolation.
- `pass_env` explicitly names additional environment variables the command may inherit. It defaults
  to an empty list.
- A directly executable shell script can use `run = ["{entry}"]`; an explicit shell can use
  `run = ["bash", "{entry}"]`.

Gateway watches the directory and debounces filesystem notifications. Reload is atomic across the
complete manifest set: a malformed replacement does not partially update the live registry or
silently remove the last valid security hook. Gateway keeps the last valid snapshot and exposes
the new validation error through logs and inspection.

There are no `add`, `enable`, `disable`, or `remove` commands. Developers edit, add, or remove
files. The only optional CLI affordance is read-only:

```text
nexus gateway hooks list
```

It reports the active generation, hook IDs, events, order, handler paths, artifact availability,
and manifest validation errors. It never changes configuration.

## Versioned Invocation Protocol

The runner contract is stable JSON rather than a language-specific ABI. Event names remain
extensible strings; the engine does not use a closed event enum.

A `before_send` invocation has this logical shape:

```json
{
  "protocol": "nexus.hooks/v1",
  "invocationId": "hi_01...",
  "event": "before_send",
  "handler": {
    "hookId": "redact-secrets",
    "entrypoint": "main",
    "runtime": "python3",
    "artifactDigest": "sha256:...",
    "attestation": {
      "algorithm": "ed25519",
      "keyId": "gwk_...",
      "signature": "base64..."
    }
  },
  "message": {
    "sender": { "agentId": "a_...", "name": "paul" },
    "target": { "verb": "post", "thread": "release" },
    "body": "message text",
    "summary": null,
    "mention": [],
    "metadata": {}
  },
  "executedBy": []
}
```

The handler descriptor and attestation are created by Gateway, not accepted from hook output. The
attestation binds the protocol version, invocation ID, hook ID, event, entrypoint, runtime, and
artifact digest. It is an execution-provenance attestation, not remote attestation or a claim about
transitive dependencies loaded by the script.

A successful `before_send` result may contain:

```json
{
  "action": "continue",
  "message": {
    "body": "modified text",
    "summary": "optional summary",
    "mention": ["fable"]
  },
  "metadata": {
    "redacted": true
  },
  "timing": "yield_turn"
}
```

All fields are optional except `action`, which defaults to `continue` when omitted. `message` is a
patch over the mutable message fields. `metadata` is an object patch; later hooks win on key
collisions. A hook may intentionally stop the send with:

```json
{
  "action": "reject",
  "metadata": {
    "policy": "secret_detected"
  }
}
```

There is no hard-coded `reason` field. Arbitrary diagnostics belong in metadata and the Gateway
audit record.

An `after_receipt` invocation contains the final accepted message plus the public receipt:

```json
{
  "protocol": "nexus.hooks/v1",
  "invocationId": "hi_01...",
  "event": "after_receipt",
  "handler": {},
  "message": {},
  "receipt": {
    "messageId": "m_..."
  },
  "executedBy": []
}
```

Its result may contain only an optional metadata patch. The executable may perform arbitrary local
side effects before returning. Message body, summary, mentions, target, and timing are immutable
after receipt.

Hook stdin contains exactly one JSON document. Successful stdout contains exactly one JSON
document. Logs belong on stderr. Extra stdout, malformed JSON, an unknown result field, or an
event-forbidden mutation is a hook failure.

## Message Mutation Boundary

`before_send` always has message-mutation authority. It requires no additional capability flag.
It may change:

- `body`
- `summary`
- `mention`
- developer-owned metadata
- requested delivery timing

It may not change:

- authenticated sender identity
- target or routing verb
- message ID or idempotency identity
- thread membership or recipient fanout
- daemon or Gateway ownership fields
- the reserved `_nexus` metadata namespace

Sender and target are visible to hooks so a hook can make a contextual decision, but they remain
part of the immutable routing envelope. A future programmable routing feature requires a separate
design rather than smuggling rerouting through message mutation.

`SendRequest` gains an optional metadata object. Gateway-provided `before_send` metadata and signed
provenance are applied before the daemon's message transaction so the body and metadata become one
canonical accepted fact. Existing callers that omit metadata retain their current wire shape and
behavior.

For a request carrying an idempotency key, the daemon resolves an already accepted message before
requesting hook evaluation. A new logical send gets a deterministic evaluation ID scoped to the
authenticated sender and idempotency key; requests without a key get a fresh ID. Gateway persists
the completed pipeline result before returning it and reuses that result when the same evaluation
ID is retried. A transport retry therefore neither repeats hook side effects nor applies a newer
hook generation to an older logical send.

## Composition and Provenance

Matching hooks run sequentially by `(order, id)`. Each hook sees the message produced by the prior
hook and the ordered provenance records for prior executions. Later body/summary/mention patches
replace earlier values. Metadata uses recursive object merge; later scalar or array values replace
earlier values, and `null` is stored as an explicit value rather than a deletion instruction.

After each successful or continued execution, Gateway appends a compact provenance entry:

```json
{
  "hookId": "redact-secrets",
  "entrypoint": "main",
  "runtime": "python3",
  "artifactDigest": "sha256:...",
  "invocationId": "hi_01...",
  "outcome": "success",
  "attestation": {
    "algorithm": "ed25519",
    "keyId": "gwk_...",
    "signature": "base64..."
  }
}
```

The final ordered list is stored under `message.metadata._nexus.hooks.executedBy`. Hooks cannot
write or replace `_nexus`; Gateway adds it after validating hook output. The full original message,
intermediate transformations, handler stderr, timing, and failure details remain in Gateway's audit
store and are linked by `invocationId`. Input and output digests are deliberately omitted because
Gateway already records the original and transformed documents.

A hook continued after failure uses `outcome: "continued_failure"`; detailed diagnostics remain in
the audit row rather than expanding the public provenance object. A rejected send has no accepted
message metadata, so its complete signed execution record remains in the audit store.

Gateway generates one Ed25519 signing key on first hook use, stores the private key as an
owner-protected file in its local persistent data directory, and derives
`keyId` from the public key. Gateway exposes public verification keys through a read-only REST
endpoint. Key rotation is outside v0.1.5; the schema permits multiple key IDs so rotation can be
added without changing provenance records.

For a generic command Gateway can attest only the declared entry artifact and entrypoint. A script
that loads other files or invokes additional programs remains responsible for its own dependency
provenance.

## Delivery Timing Semantics

`before_send` may return exactly one of these policies:

### `interrupt`

Attempt delivery immediately. A native-steer adapter adds the message to the active turn. An
interrupt-and-send adapter atomically cancels the active turn and begins this accepted message as
the next turn. If the recipient exposes neither capability, Nexus returns a capability error rather
than silently downgrading the requested policy.

### `yield_turn`

Deliver at the earliest trustworthy LLM yield boundary. On adapters that expose an injectable tool
yield, this may be the next tool-call boundary. Otherwise it resolves to terminal turn completion.
It never guesses from UI presence or elapsed time.

### `after_tool_loop`

Wait through the complete model/tool sequence and deliver only after terminal completion of the
final LLM turn. It never injects between consecutive tool calls.

The daemon already owns interruption, active-turn state, completion waiters, and tool-call
observations. v0.1.5 adds a small normalized delivery-boundary coordinator that maps those signals
to these three policy names. Harness adapters advertise capabilities; unsupported intermediate
yield behavior falls back only within `yield_turn` to the later terminal boundary. `interrupt`
never silently changes meaning.

If no hook selects timing, the existing default Nexus message delivery behavior remains unchanged.

## Receipt Semantics

`after_receipt` means after the public canonical send receipt, not after every recipient settles.
It runs once per accepted message ID regardless of DM, thread fanout, topic publication, delivery
retry, or recipient count. Recipient delivery internals remain invisible to this public hook event.

Gateway drives `after_receipt` from its durable accepted-message projection. The invocation key is
deterministic from `(messageId, event, hookId, hookGeneration)`. Projection replay therefore does
not create a new logical invocation.

External side effects are at-least-once. A process crash after a script commits an external effect
but before Gateway records completion can cause a retry. Every invocation carries the stable
`invocationId`; hook authors use it as their idempotency key when the side effect requires dedupe.

Metadata returned by `after_receipt` becomes canonical in Gateway immediately and is mirrored back
to the daemon through an idempotent atomic metadata-merge command keyed by invocation ID. The
daemon applies the same recursive object-merge rules in one transaction and projects the result
back to Gateway. That convergence does not re-run message hooks or replace unrelated concurrent
metadata.

## Daemon–Gateway Protocol Extension

The existing stream gains additive, capability-negotiated frames:

- Gateway hello advertises `message_hooks_v1`.
- Gateway sends an active hook generation and whether it has any `before_send` hooks.
- Daemon sends `hook.invoke` with a correlation ID and bounded deadline.
- Gateway returns `hook.result` with the transformed message, provenance, requested timing, or a
  typed rejection/failure.
- Disconnecting or timing out removes the pending correlation entry; late results cannot mutate a
  later message.

The daemon sends hook frames only after capability advertisement, so an older Gateway continues to
work and messages retain pre-v0.1.5 behavior. Gateway validates the daemon boot ID and local stream
token exactly as it does for existing projection traffic.

The bridge is a request/response service separate from the current broadcast publisher. It owns one
active hook-capable Gateway connection, a bounded pending-request map, deadlines, and cancellation.
It does not turn the projection broadcast channel into an RPC mechanism.

## Gateway Availability and Failure Policy

Individual blocking-hook failures follow the hook's manifest:

- `continue`: discard that hook's invalid output, record the failure, append failure provenance to
  the audit trail, and continue from the last valid message state.
- `reject`: reject the send before canonical acceptance.

Timeout, spawn failure, nonzero exit, oversized output, malformed JSON, and forbidden mutation all
count as hook failures. Intentional `action: "reject"` always rejects regardless of `on_failure`.

Gateway availability is a daemon transport setting rather than a per-message field:

- `optional` (default): if no hook-capable Gateway is connected, transport continues without hook
  evaluation and emits a structured hook-bypass diagnostic for later projection.
- `required`: canonical sends reject while hook evaluation is unavailable. This is the explicit
  affordance for developers using hooks as a mandatory policy boundary.

This setting does not move the hook registry into the daemon. It only decides whether transport may
proceed without the Gateway-owned feature. A disconnect during an active evaluation uses the same
setting. When Gateway is present but an individual hook fails, the manifest's `on_failure` remains
authoritative.

The concrete daemon configuration is `NEXUS_HOOK_GATEWAY_MODE=optional|required`; absence means
`optional`. It is read at daemon startup and is intentionally not a hook-management CLI surface.

`after_receipt` never blocks transport. Accepted-message projections remain in the existing bounded
projection backlog and invoke the hook when Gateway returns. If the backlog reports an explicit
projection gap, Gateway records an explicit hook-audit gap rather than inventing executions.

## Gateway Persistence

Gateway stores originals once per pipeline rather than once per hook:

- `hook_runs`: run ID, event, hook generation, original document, final document, message ID when
  available, requested/resolved timing, status, and timestamps.
- `hook_invocations`: invocation ID, run ID, hook ID, order, handler descriptor, artifact digest,
  result, bounded stderr, outcome, and signed provenance.
- `hook_signing_keys`: key ID, public key, owner-protected private-key path,
  creation timestamp, and active state.

Unique constraints on invocation ID make projection replay idempotent. The original message and
final message are not copied into every invocation row. Hook audit data follows Gateway persistence,
backup, and deletion policy; it never enters the daemon's bounded unsettled-delivery journal.

## Local Runner Safety and Resource Bounds

Local hooks are trusted local programs and run with the Gateway operating-system account. Nexus
does not claim to sandbox them. It still prevents accidental process and data hazards:

- argv arrays only; no implicit shell or interpolation beyond the exact `{entry}` placeholder
- resolved manifest-relative entry paths
- a minimal inherited environment containing platform basics and hook context, with explicit
  manifest opt-in for additional environment names
- no automatic forwarding of daemon credentials, harness OAuth data, provider API keys, or Gateway
  signing private keys
- default 1-second and maximum 30-second execution deadline
- 4 MiB stdout cap and 64 KiB retained stderr cap
- bounded concurrent processes and pending invocations
- process-tree termination on timeout or Gateway shutdown
- exact one-document stdout validation
- secret values excluded from normal diagnostic logs

Resource-limit failures use the same event and manifest failure rules as other hook failures.

## Event-Spine Extensibility

The engine stores event names as validated strings and resolves them through registered adapters.
Adding a future event requires a new adapter that defines:

1. its stable public event name;
2. the input payload builder;
3. blocking or observational execution mode;
4. valid result schema and mutation permissions;
5. state-application and idempotency behavior.

It does not require changes to manifest discovery, ordering, the local runner, signing, audit
persistence, or future callback transport. Unknown event names fail manifest validation instead of
being silently ignored.

Python and JavaScript SDKs can later expose typed functions over `nexus.hooks/v1`. They must remain
optional conveniences: a conforming executable using stdin/stdout JSON is always sufficient.

## Testing Strategy

### Contract tests

- Rust and generated TypeScript round trips for optional send metadata, hook frames, timing policy,
  invocation, result, receipt, handler descriptor, and signed provenance.
- Golden tests proving existing metadata-free `SendRequest` JSON is unchanged.
- Rejection tests for target/sender mutation, `_nexus` writes, unknown timing, and unknown fields.

### Gateway unit tests

- Manifest parsing, relative path resolution, duplicate IDs, event validation, deterministic order,
  default values, and atomic hot reload.
- Sequential composition and later-wins metadata merge.
- Local-command success for shell, JavaScript, and Python fixtures.
- Timeout, nonzero exit, malformed/extra stdout, stdout/stderr caps, forbidden mutation, and process
  cleanup.
- Per-hook continue/reject behavior.
- Original stored once, intermediate results linked, deterministic invocation IDs, and idempotent
  projection replay.
- Ed25519 key creation, stable key ID, verification endpoint, artifact digest, ordered provenance,
  and rejection of hook attempts to forge `_nexus`.

### Daemon unit and integration tests

- Capability negotiation with old and hook-capable Gateways.
- Correlated concurrent hook requests, timeout cleanup, disconnect cleanup, and rejection of late
  responses.
- `optional` bypass and `required` rejection when Gateway is absent.
- Every canonical source—direct daemon RPC, CLI command intent, Gateway REST/MCP, notify, and agent
  message—passes through one `before_send` evaluation.
- Idempotency retries reuse the canonical message and do not create duplicate hook runs.
- Exactly one `after_receipt` event per accepted message regardless of fanout.

### Timing tests

- `interrupt` maps to native steer and interrupt-and-send capabilities and errors on unsupported
  adapters.
- `yield_turn` uses an intermediate tool yield when advertised and otherwise waits for terminal
  completion.
- `after_tool_loop` ignores intermediate tool starts/results and releases only at terminal turn
  completion.
- Timing behavior is verified across the current Codex, Claude, OpenCode, and Hermes adapter
  variants using deterministic adapter fixtures before practical harness testing.

### Practical release gate

- One local Gateway with shell, Python, and JavaScript hooks.
- Messages originating from CLI, Gateway REST, MCP, and an agent.
- Body transformation, metadata addition, each timing policy, intentional rejection, continued
  handler failure, required-Gateway failure, and signed provenance verification.
- A bounded multi-harness thread exercise confirming hooks do not touch raw/token session streams
  and do not create duplicate fanout or receipts.
- Full Rust workspace, contract drift, Gateway typecheck/tests/build, package smoke, and platform
  command gates remain mandatory.

## Documentation and Release Surface

v0.1.5 documentation adds:

- a concise hook authoring guide with shell, JavaScript, and Python examples;
- the `nexus.hooks/v1` input/output contract;
- manifest reference and failure semantics;
- timing policy and adapter-capability behavior;
- provenance verification and at-least-once side-effect guidance;
- Gateway optional/required availability configuration;
- an extension guide showing how future event adapters and runners fit the spine.

The public README receives only a short promotional example and link to the hook guide. Internal
architecture details remain in the architecture and design documentation rather than expanding the
README.

## Acceptance Criteria

The v0.1.5 hook feature is complete when:

1. A developer can drop a shell, JavaScript, Python, or native executable plus manifest into
   `hooks.d` and have Gateway load it without a registration command.
2. Every canonical message source invokes the same ordered `before_send` pipeline exactly once per
   logical send attempt.
3. `before_send` can mutate all permitted message fields, add metadata, reject, and select all three
   timing policies without changing routing identity.
4. `after_receipt` runs once per canonical message ID, can perform side effects and add metadata,
   and is replay-idempotent within Gateway.
5. Signed handler provenance identifies the declared hook entry artifact without input/output
   digests, and the public key can verify the attestation.
6. Gateway stores the original pipeline document once and retains complete per-invocation audit
   history without copying it into daemon transport persistence.
7. Gateway absence follows the explicit optional/required setting and can never silently violate a
   configured required boundary.
8. Hook failures, timeouts, invalid mutations, projection gaps, and external-side-effect semantics
   are observable and documented.
9. No hook runs on agent-session/token-stream traffic.
10. All focused, integration, practical harness, full repository, and packaging gates are green.
