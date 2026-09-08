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

## Exact session dispatch and Gateway carriers

Daemon and Gateway HTTP `prompt`, `steer`, `interrupt`, `compact`, and queue-mutation requests accept optional
`expectedSessionId` together with a nonempty stable `agentId`. Explicit null, empty,
non-string, or partial selectors are invalid before durable enqueue or mutation
effects. Omission preserves existing name/id resolution and revival, including
slash-command handling. The selector is `expectedSessionId`, not an unrelated HTTP `sessionId` field. Lens carrier integration remains a separate slice.

Agent-session WebSocket input, steer, interrupt, queue mutation, and structured
command frames require nonempty `agentId` and `expectedSessionId` matching the
connection's canonical observe-response binding. A generic or bus socket is not a
name-only escape for session commands. Read-only command catalogs and bus-mode
DM/post/publish behavior are unchanged. `session.bound` is emitted before queue or
AG-UI pumping and means transport binding only, not a run event or native readiness.
The retained browser source captures that pair per connection, checks a session-path
URL against it, ignores closed-connection callbacks, and refuses unbound/disconnected
sends without falling back to name-only HTTP. Explicit partial/conflicting caller
identity is rejected, never overwritten. Rejections propagate to preserve the draft.

Exact HTTP/WS success receipts must name the actual matching session. Missing or
foreign identities report an unconfirmed outcome, not a fabricated request echo.
Structured `command.ack` retains its pre-dispatch validation meaning; `command.done`
cannot report success for a mismatched result. Structured compact still uses the
compact endpoint and its existing scheduling/timeout, not the normal prompt queue.

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

## Historical queue reads and reconnect binding

Queue reads accept the captured stable `agentId` and `expectedSessionId`. A
retained inactive session may still be inspected after its agent moves to another
session; this grants no dispatch or mutation authority. Exact observe connections
use the same retained ownership and do not warm or revive an agent. Missing,
partial or conflicting ownership fails closed instead of selecting its replacement.

Queue entries, transitions and receipts keep the original validated command
binding. Filtering happens before the bounded queue viewport, and filtered global
events still advance the scanned cursor. This is queue/receipt recovery, not PACT
transcript isolation. On a non-gap queue-event page, `nextSeq` is the last scanned
cursor; on a gap page it remains the first surviving sequence for rehydration.
Optional `errorCode` preserves structured terminal errors;
`correlationOwned` is computed for the authenticated reader, not supplied by a
client. A matching client ID alone does not prove that a shared queue row belongs
to that reader. Cookie identity is resolved without registering or rebinding it
during a read; absent or ambiguous identity cannot authorize correlation.

Failed transitions schedule the existing coalesced snapshot refresh, as queued
transitions do. A sparse failure event or failed refresh does not establish that
native submission had no effect. Clients must retain the original pending input
identity after an unconfirmed acknowledgement, including a later HTTP conflict
or authentication error. Reconnect may replay that identity under the existing
journal fence; it must not manufacture a fresh-ID retry.

Validated exact terminal evidence can retire the original transport operation.
An unknown command ID additionally requires current authenticated caller ownership;
an already known command association must match. A failed terminal is not native
nonacceptance proof, and viewport absence is not a terminal fact. Consumers keep
their pending/draft projection separate from queue state and native model truth.
These are client evidence requirements, not a new daemon status or native ACK.

## Adapter observation on queue snapshots

The optional `observation` on an exact queue snapshot carries the adapter's own
turn state, session, capability and, when available, opaque binding owner and
evidence revision. Queue position and activity have separate versions: an idle
or busy observation can change without a queue transition. Unknown or unavailable
evidence is not idle, and an observation does not reserve native admission.

Subscribed lanes reuse the existing shared snapshot refresh at a bounded cadence,
including when transition pages are empty. This bounds refresh requests, not
network latency. Historical sessions receive their own evidence, never the active
replacement's state. Consumers compare revisions only within the same owner and
discard observations from superseded reads without discarding independent queue
facts.

New redirect mutations use capability captured from the selected adapter, checked
against the transaction-resolved session after committed replay. Stored harness
names and caller-supplied fields cannot grant redirect support. This check is not
an atomic native reservation; actual dispatch still validates its admission.

## Codex native activity and request ownership

The existing JSON-RPC reader updates native turn authority before forwarding a
notification to display processing. An earlier blocked display write therefore
does not prevent a subsequently read terminal notification from clearing its
turn's activity. Accepted events, ordered output and receipt/completion settlement
remain on the serial forwarder; native activity ingestion is not a receipt.
Retryable native errors retain activity. Bounded terminal facts prevent delayed
acceptance responses for recently completed turns from reopening them, and delayed
projection settlement cannot overwrite a newer turn's native state.

Each binding attempt captures a private owner before asynchronous setup. Its
provisional observations cannot replace a published binding; publication validates
the captured attempt, and teardown revokes it. Resume-response seeding cannot
overwrite newer native observations received during the request. Captured receipt
and projection work stays with its original owner after replacement.
Provisional thread summaries are bounded; exceeding that bound revokes the setup
attempt rather than turning discarded native-open evidence into an idle binding.

Setup requests use provisional-owner admission checks too. A private per-session
setup/persistence gate orders sidecar writes, publication and the existing deferred
thread-registration callback. An old write already in progress finishes before
the replacement persists and publishes; an old callback starting afterward fails
its captured-owner check. Registration remains deferred until after launch. Its
existing bounded retry can delay replacement setup, but holds no native request
admission or receipt lock. This is ordering across existing stores, not a new
cross-database transaction or an exactly-once external-effects guarantee.

Construction and retirement also coordinate use of the deterministic session
endpoint. Replacement setup waits for previous owned-child cleanup before
starting or adopting at that path. Revocation remains immediate; a stalled setup
can be cancelled without leaving its child for the replacement to adopt and then
lose. Normal daemon-restart adoption remains available.
The same-path process-exit regressions exercise Unix. Non-Unix abnormal-drop
cleanup retains its existing direct-child kill behavior; these tests do not
certify a synchronous Windows process-exit boundary.

Prompt, steer (including its expected-turn retry), compact and interrupt capture
that owner before waiting. Writer readiness precedes a short shared ownership
check and local `SplitSink::start_send` admission. If replacement wins first, the
old request cannot enter that slot. This is local admission, not proof that bytes
were written or that Codex accepted them. The ownership guard is released before
flush and response waits; a backpressured writer can still delay a control request.

Cancellation removes only the original pending response correlation. Before local
admission it leaves no request frame; afterward it does not retract an admitted
frame or establish retry-safe rejection. A later flush may deliver that original
frame. The durable journal applies the separate ordinary-prompt attempt policy
described below; local native admission is not a durable receipt.

This reuses the existing reader, tracker and serial notification channel. It does
not bound that channel during an indefinitely stalled sink, guarantee progress
when native input itself is unavailable, or prove the cause of a historical
stall. No new automatic-delivery mode is enabled.

## ACP observed prompts

The strict `prompt_observed` path waits for the correlated ACP `session/prompt`
response instead of reporting success when a background relay is spawned. In this
protocol that response is completion-bound evidence, not an early acceptance ACK.
Request construction, input echo, quiescence and local cancellation do not supply
that success boundary. Unsupported adapters fail explicitly.
Hermes's recognized queue-only acknowledgement is not completion evidence, even
when unrelated model output has arrived; strict delivery reports uncertainty
rather than promoting that acknowledgement into success.

This path takes the existing turn's buffered output before releasing its turn
serialization, awaits the caller's original accepted-input event sink, then emits
the retained output in order and its completion marker. Sink completion establishes
local ordering, not remote delivery or durable persistence. Strict output cannot
leak into a legacy relay waiting for the same engine; legacy prompt/bus streaming
and explicit steering retain their separate behavior.
After successful protocol completion, this call no longer counts as active merely
because its presentation sink is blocked. The observed call itself still waits for
ordered presentation; failure or cancellation does not establish verified idle.

Strict output is consequently delayed until the correlated response. The existing
event buffer remains unbounded; this change does not establish streaming parity or
fix that memory limitation. Post-submission errors and missing responses do not
prove that nothing happened. The ordinary durable attempt fence below preserves
that uncertainty rather than permitting an automatic duplicate prompt.

## Headed Claude hook observation and receipts

The existing forwarder records ordered hook facts with their native session,
optional prompt identity and record offset before awaiting display output. A
validated UserPromptSubmit opens native activity, including manually entered
input. Tool output, transcript stop reasons and presentation counters cannot
close it. Matching terminal hook evidence closes its own turn without clearing
a newer turn. Complete hook records remain usable before a partially appended
suffix; missing, truncated or conflicting evidence does not fabricate idle.

Launch, revival and adoption capture fresh observation owners. Old forwarder
attachments, cleanup and replayed offsets cannot replace a current owner's
facts. A fresh unresumed launch establishes native session identity from a valid
new SessionStart, not an old sidecar value. SessionStart alone is not idle proof.

Programmatic input registrations retain their owner, native session and eligible
hook offset. The same record provenance flows into asynchronous accepted-input
matching. A fresh owner's valid SessionStart can establish its pending identity;
foreign or historical same-text records cannot consume that registration.
The observed prompt still requires its matching acceptance callback to finish
and its required terminal fact. Submit and Stop in one poll pass cannot return
success while acceptance presentation is blocked. Terminal-only output does not
substitute for the input receipt, and timeout cleanup does not retract a write.

These facts are observed, not an atomic reservation against direct human input.
A human may start a turn after the latest observation but before a terminal
write. Without a native caller token, concurrent identical manual text cannot
always be distinguished from programmatic input. Missing optional prompt IDs
also limit ambiguous overlapping-turn correlation. File/store reads may stall
before ingestion; this does not promise bounded freshness or credentialed
real-binary acceptance. The tests use controlled hooks and disposable raw/tmux
process seams.

## Internal adapter turn observations

The existing execution adapter exposes `verified_idle`, `native_open`, `unknown`
and `unavailable` independently of queue cursors and transport presence. A bound
observation carries an opaque owner and a revision of its evidence; reading the
same facts does not advance that revision. Owner identities are equality scopes,
not sortable clocks. Unsupported observation defaults to unknown without a made-up
owner, rather than declaring every unobserved session busy or idle.

Codex idle requires positive native completion or validated thread initialization
or resume evidence; an empty compatibility response is not such evidence. Claude
uses the ordered hook facts described above. Generic ACP supplies completion-bound
responses, not native turn-start notifications: enqueueing a request is unknown,
and a sole positively completed request can establish idle. A deferred response,
error, dropped caller or unresolved overlapping request cannot prove idle. Socket
loss is unavailable and cannot alter a replacement owner's observation.

This is an internal observation method, not a new activity service or public
readiness guarantee. Existing scheduler activity, adapter capability and native
admission checks retain their responsibilities. Unknown observations do not add
permanent scheduler blockers; established open work is not cleared by missing
evidence. No snapshot reserves input against a later native or direct-human turn.

## Durable prompt attempts and uncertain outcomes

The existing `command_intents` identity-store journal protects ordinary prompt
attempts and retained automatic rows against replay, independently of the
boot-scoped transport database. This does not enable automatic delivery:

- `claimed` without `started_at` means the worker has not armed admission. An
  expired claim may be reclaimed.
- For `harness.prompt`, `started_at` means **may have attempted native delivery**.
  It is persisted before execution, under command id, claim timestamp, attempt
  number, the captured lease value, and an unexpired current lease. It is not an
  acceptance receipt. Prompt slash-compaction uses the same attempt protection.
- Once armed, no generic or session-prompt claim path may reclaim the row. Generic
  shutdown retry and legacy settlement helpers cannot clear or overwrite its fence.
- Only exact-claim, proven pre-entry nonacceptance may defer the same row. Its
  original queue position and id are preserved. No general error or timeout is
  reinterpreted by message text as retry permission.
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

Within a running worker, one private attempt permit orders a reporting deadline
against adapter entry after asynchronous preflight. If the deadline closes that
permit first, the detached preflight cannot invoke the adapter later. If adapter
entry wins, a timeout or general error is potentially accepted and is reported as
delivery-uncertain. Execution remains detached on a reporting timeout so a store
statement is not cancelled midway; late completion cannot overwrite the terminal
uncertainty. A typed shutdown-before-entry result may release its exact claim.
The short shutdown exclusion is released after the adapter's first poll, not held
through its later I/O or receipt wait.

This retains the original command's replay protection, not an indefinite block on
all subsequent commands for that session. Native activity and existing admission
rules still govern later work. Explicit steer, interrupt and standalone compact
retain their existing delivery policies; this slice fences `harness.prompt` only.

The combined native-auto admission/lifecycle path, its capability advertisement,
and active-eligible scheduling remain disabled until their combined tests pass.
Pre-existing unstarted automatic rows are explicitly rejected without native
delivery; already-armed rows remain uncertain rather than being re-executed.
