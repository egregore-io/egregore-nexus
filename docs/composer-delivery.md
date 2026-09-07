# Session composer delivery

An ordinary session Send uses `harness.prompt`: the daemon queues it until the
target can accept a new turn. A client must not choose strict steer merely because
its activity display says the agent is busy. That display can be stale by the time
the request arrives. Explicit steer remains a separate active-turn operation.

This candidate does **not** advertise native automatic delivery. Gateway prompt
ingress and daemon prompt ingress reject non-null `delivery` and `modelSelection`
options rather than silently discard them and send an ordinary prompt. Clients
must preserve configured intent or report unsupported operation. Omitted options
retain the existing boundary-prompt behavior; no WebSocket activity is required
to decide how ordinary Send is delivered.

Steer dispatch applies the same validation before reviving or calling a harness.
Queue redirection preserves the original request JSON, so converted pending rows
(including previously persisted redirects) cannot bypass rejection by changing
command kind. Ordinary explicit steer and redirects with omitted/null options
retain their existing behavior.

## Opt-in exact daemon session dispatch

Daemon `prompt`, `steer`, `interrupt`, and queue-mutation requests accept optional
`expectedSessionId` together with a nonempty stable `agentId`. Explicit null, empty,
non-string, or partial selectors are invalid before durable enqueue or mutation
effects. Omission preserves existing name/id resolution and revival, including
slash-command handling. Gateway/Lens carrier integration is separate; this daemon
slice alone does not make an existing HTTP `sessionId` an exact selector.

Exact dispatch resolves the authoritative active agent runtime and verifies that
its transport session is owned by that agent and matches the selector. It never
calls `ensure_alive` or substitutes a newer runtime. Successful routed responses
include the actual resolved `sessionId`; this is routing evidence, not proof of
native admission, lifecycle exclusion, or completion. A durable S1 row that reaches
dispatch after a same-agent S2 rebind fails without reaching S2. Existing scheduling
can still leave that old row waiting behind a busy lane; no immediate rejection is
promised while it has not reached dispatch.

New queue mutations resolve identity inside the same identity write transaction as
their application. Split-store transport rows supply only a matching owned session
description; they are not queried from the identity transaction. Committed mutation
receipts replay before new target resolution, even after rebind. Reusing their client
id with another selector conflicts; omitted legacy canonical JSON gains no null key.

Prompt-to-steer conversion preserves the original request bytes, selector, and retry
identity. Exact redirect rejects a legacy row lacking the selector or a conflicting
selector; it cannot retrofit exact authority into an unbound command. Omitted legacy
redirect remains compatible. Unsupported delivery/model intent is still rejected at
dispatch, including converted rows, without a native operation.

## Codex completion and display writes

Once the Codex forwarder consumes a terminal native notification, it clears that
exact turn's routing activity before awaiting display/storage I/O. Late start
acceptance cannot resurrect that terminal turn, and a newer active turn is not
cleared. Accepted events, ordered text/terminal output, and receipt/completion
waiters retain their existing ordering; retriable native errors remain active.

This closes the consumed-terminal stale-activity window only. A terminal still
unread behind earlier blocked output, an actor waiting on acceptance, or a held
native admission lock can still delay a queued prompt. It is not proof that every
queue stall is fixed, nor proof of the cause of any particular historical stall.

## Automatic-delivery safety foundation

The durable `command_intents` identity-store journal now protects automatic rows
against replay, independently of the boot-scoped transport database. These guards
are a foundation, not an enabled automatic-delivery capability:

- `claimed` without `started_at` means the worker has not armed admission. An
  expired claim may be reclaimed.
- For `delivery:auto` only, `started_at` means **may have attempted native delivery**.
  It is persisted before any native admission call, under command id, claim
  timestamp, attempt number, and an unexpired lease. It is not an acceptance receipt.
- Once armed, no generic or session-prompt claim path may reclaim the row. Generic
  shutdown retry and legacy settlement helpers cannot clear or overwrite its fence.
- Only exact-claim, known native nonacceptance may defer the same row. Its original
  queue position and id are preserved. The future admission scheduler must make
  this boundary-driven; the defer operation does not ring the ingress wakeup.
- Expiry or shutdown of an unresolved armed row reports `DELIVERY_UNCERTAIN`
  (`-32011`). The agent may still receive or finish the message. This is not a
  known rejection and must not offer automatic retry. Late callbacks cannot
  overwrite this terminal or trigger another send.
- Uncertainty receipts remain as unresolved delivery obligations and are excluded
  from ordinary terminal-result retention. Known outcomes retain normal bounded
  retention. This does not promise deduplication after a known receipt expires.

A crash between arming and the native call can therefore report uncertainty even
when nothing was sent. This conservative tradeoff prevents automatic duplicate
delivery; it is not an exactly-once external-effects guarantee. Restart tests
reopen the actual split daemon identity database, not an in-memory mock.

Native auto admission, lifecycle exclusion, per-session capability advertisement,
and active-eligible scheduling remain disabled until their combined tests pass.
Pre-existing unstarted automatic rows are explicitly rejected without native
delivery; already-armed rows remain uncertain rather than being re-executed.
