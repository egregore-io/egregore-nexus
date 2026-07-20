# Message hooks

[← Nexus docs](README.md)

Nexus Gateway can run local programs at the canonical message boundary. A hook receives one
versioned JSON document on standard input and must return one JSON object on standard output. The
same protocol works with shell scripts, JavaScript, Python, or a native executable.

v0.1.5 exposes two events:

- `before_send` runs before Nexus accepts a new logical message. It may change message text,
  summary, mentions, developer metadata, or delivery timing, and it may reject the send.
- `after_receipt` runs after Nexus has accepted the message and assigned its canonical message ID.
  It may add metadata or perform a local side effect, but it cannot change the accepted message.

> **Capability truth:** read `GET /api/v1/capabilities` and inspect
> `protocol.surfaces.hooks`. Its `events` list is generated from the same registry that validates
> hook manifests, so clients should not infer support from documentation or version strings.

Hooks are a Gateway feature. The daemon never loads developer code, and token deltas, model-text
streams, tool events, and terminal bytes do not enter this hook pipeline.

## Quick start

Hook manifests and programs live under `$NEXUS_HOME/gateway/hooks.d`, which defaults to
`~/.nexus/gateway/hooks.d`. This shell example adds metadata to every new message:

```bash
HOOK_DIR="${NEXUS_HOME:-$HOME/.nexus}/gateway/hooks.d"
mkdir -p "$HOOK_DIR"

cat >"$HOOK_DIR/add-origin.sh" <<'SH'
#!/usr/bin/env sh
set -eu
cat >/dev/null
printf '%s\n' '{"metadata":{"processedBy":"shell"}}'
SH
chmod 700 "$HOOK_DIR/add-origin.sh"

cat >"$HOOK_DIR/010-add-origin.toml" <<'TOML'
version = 1
id = "add-origin"
event = "before_send"
order = 10
timeout_ms = 1000
on_failure = "continue"
enabled = true

[handler]
kind = "local"
entry = "add-origin.sh"
entrypoint = "main"
run = ["sh", "{entry}"]
pass_env = []
TOML

nexus gateway start
nexus gateway hooks list
```

Gateway watches the directory and atomically activates a complete valid snapshot. A bad manifest
does not partially replace the last good generation; inspect the retained generation and errors
with `nexus gateway hooks list --json`.

## JavaScript example

This `before_send` program prefixes the body and chooses `yield_turn` delivery:

```javascript
#!/usr/bin/env node

let input = "";
for await (const chunk of process.stdin) input += chunk;
const invocation = JSON.parse(input);

process.stdout.write(JSON.stringify({
  message: { body: `[reviewed] ${invocation.message.body}` },
  metadata: { review: { hook: invocation.handler.hookId } },
  timing: "yield_turn"
}));
```

Save it as `review.mjs`, then use this handler table in a `before_send` manifest:

```toml
[handler]
kind = "local"
entry = "review.mjs"
entrypoint = "main"
run = ["node", "{entry}"]
pass_env = []
```

## Python example

This `after_receipt` program records the canonical message ID in metadata:

```python
#!/usr/bin/env python3

import json
import sys

invocation = json.load(sys.stdin)
json.dump(
    {"metadata": {"receiptObserved": invocation["receipt"]["messageId"]}},
    sys.stdout,
    separators=(",", ":"),
)
```

Save it as `receipt.py` and register it with:

```toml
version = 1
id = "record-receipt"
event = "after_receipt"
order = 20
timeout_ms = 1000
on_failure = "continue"

[handler]
kind = "local"
entry = "receipt.py"
entrypoint = "main"
run = ["python3", "{entry}"]
pass_env = []
```

## Manifest reference

| Field | Required | Meaning |
|---|---:|---|
| `version` | yes | Manifest version. v0.1.5 accepts `1`. |
| `id` | yes | Unique ID matching `[a-z0-9][a-z0-9._-]{0,63}`. |
| `event` | yes | `before_send` or `after_receipt`. |
| `order` | no | Numeric pipeline order, default `0`. Equal values sort by hook ID. |
| `timeout_ms` | no | Wall-clock deadline, default `1000`; allowed range `1..30000`. |
| `on_failure` | no | `continue` or `reject`, default `continue`. `reject` is valid only for `before_send`. |
| `enabled` | no | Whether the hook participates, default `true`. |
| `handler.kind` | yes | `local` in v0.1.5. |
| `handler.entry` | yes | Regular, non-symlink file resolved relative to the manifest. |
| `handler.entrypoint` | no | Logical function/command name recorded in provenance, default `main`. |
| `handler.run` | yes | Non-empty argv array. Every `{entry}` token expands to the resolved entry path. |
| `handler.pass_env` | no | Explicit additional environment names to inherit, default `[]`. |

`run` is executed directly with no implicit shell. Use `run = ["sh", "{entry}"]` when shell
interpretation is intentional. The child receives a minimal platform environment plus
`NEXUS_HOOK_PROTOCOL`, `NEXUS_HOOK_ID`, `NEXUS_HOOK_EVENT`, `NEXUS_HOOK_INVOCATION_ID`,
`NEXUS_HOOK_ENTRYPOINT`, and `NEXUS_HOOK_MANIFEST`.

## JSON protocol

Every invocation uses `protocol: "nexus.hooks/v1"`. A representative `before_send` input is:

```json
{
  "protocol": "nexus.hooks/v1",
  "invocationId": "hi_...",
  "event": "before_send",
  "handler": {
    "hookId": "review",
    "entrypoint": "main",
    "runtime": "node"
  },
  "executedBy": [],
  "evaluationId": "he_...",
  "messageId": "m_...",
  "message": {
    "sender": { "agentId": "a_...", "name": "fixture-sender" },
    "target": { "verb": "post", "thread": "release" },
    "body": "ship it",
    "mention": [],
    "metadata": {}
  }
}
```

The optional `messageId` is the daemon's preallocated canonical ID for this evaluation. A
`before_send` result accepts only these fields:

```json
{
  "action": "continue",
  "message": {
    "body": "modified text",
    "summary": "optional summary",
    "mention": ["fable"]
  },
  "metadata": { "reviewed": true },
  "timing": "yield_turn"
}
```

Every field is optional; omitted `action` means `continue`. `message` is a patch and may contain
only `body`, `summary`, and `mention`. A `null` summary removes it. Metadata recursively merges,
later hooks win on collisions, and `_nexus` is reserved for Gateway provenance. Sender, target,
routing verb, message ID, and idempotency identity are immutable. To reject before acceptance,
return `{ "action": "reject", "metadata": { ... } }`.

An `after_receipt` invocation also contains:

```json
{
  "receipt": { "messageId": "m_..." }
}
```

Its result may contain only `{ "metadata": { ... } }`. Logs belong on standard error. Empty,
extra, or malformed standard output, unknown fields, forbidden mutations, non-zero exit, timeout,
and resource-limit violations are hook failures.

## Delivery timing

A `before_send` hook may choose one policy:

- `interrupt` — deliver immediately. Nexus uses native steer when supported, otherwise an atomic
  interrupt-and-send capability. It returns a typed error rather than silently changing meaning
  when neither is available.
- `yield_turn` — deliver at the earliest authoritative yield exposed by the harness. If there is no
  unambiguous intermediate yield, Nexus waits for terminal turn completion.
- `after_tool_loop` — wait through the model/tool sequence and deliver only after the final model
  turn completes.

An idle target starts normally for all three policies. Nexus relies on normalized harness
capabilities and completion events, never UI presence, elapsed time, or inferred tool counts. If no
hook selects a timing, the normal Nexus default remains in effect.

## Failure and availability behavior

`on_failure = "continue"` records the failed execution and continues from the last valid message
state. `on_failure = "reject"` rejects a `before_send` before canonical acceptance. An intentional
`action: "reject"` always rejects regardless of the manifest failure policy.

The daemon's Gateway availability mode is configured at startup:

```bash
# Default: sends continue without hook evaluation while Gateway is unavailable.
NEXUS_HOOK_GATEWAY_MODE=optional nexus daemon start

# Mandatory policy boundary: new sends fail while a hook-capable Gateway is unavailable.
NEXUS_HOOK_GATEWAY_MODE=required nexus daemon start
```

This is a transport setting, not a per-message field. `after_receipt` never blocks recipient
delivery. When Gateway returns, accepted-message projection replay resumes receipt processing.

## Side effects and idempotency

Gateway deduplicates hook evaluation by stable invocation identity, but arbitrary external side
effects are at-least-once. A crash after the program performs an effect but before Gateway records
completion can cause a retry. Use `invocationId` as the idempotency key in the external system.

`after_receipt` means after canonical message acceptance, not after every recipient delivery. One
thread post therefore has one receipt-hook identity regardless of fanout size.

## Trust and resource limits

Local hooks are trusted programs running as the Gateway operating-system account. Nexus does not
sandbox them. Install only code you trust and protect `$NEXUS_HOME/gateway/hooks.d` accordingly.

Gateway still bounds accidental failure: argv execution uses `shell: false`, only declared
environment values are forwarded, concurrent child processes are bounded, stdout is capped at
4 MiB, retained stderr at 64 KiB, and timed-out process trees are terminated. Daemon credentials,
harness OAuth state, provider API keys, and the Gateway signing private key are not forwarded
automatically.

## Audit and provenance

Gateway stores hook runs and invocations in its own persistent database. Each execution records the
declared entrypoint, runtime, entry artifact SHA-256, stable invocation ID, outcome, and an Ed25519
signature. Compact ordered provenance is merged under
`message.metadata._nexus.hooks.executedBy`; the original body is not duplicated there.

```bash
curl -s http://localhost:4100/api/v1/hooks
curl -s http://localhost:4100/api/v1/hooks/public-key
curl -s 'http://localhost:4100/api/v1/hooks/audit?limit=20'
```

The audit response marks executions `verified` after checking them against the active Gateway
public key. Administrative callers see local paths and stored results; other authenticated callers
receive a redacted view. The signature attests to the declared entry artifact and execution tuple,
not to transitive files or programs loaded by that artifact.

## Extension seams

The event adapter, engine, and runner are deliberately separate. Future non-token events, an HTTP
callback runner, language SDKs, and a programmable custom timing policy can reuse this spine. They
are not shipped in v0.1.5: local executables, the two message-boundary events, and the three named
timing policies are the complete public surface.

## See also

- [Architecture](architecture.md)
- [Extending Nexus](extending-nexus.md)
- [REST API](rest-api.md)
- [CLI reference](cli.md)
