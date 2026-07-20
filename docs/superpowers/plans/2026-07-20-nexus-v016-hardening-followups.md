# Nexus v0.1.6 Hardening Follow-ups

**Status:** Deferred follow-up ledger. These items do **not** block the
v0.1.6 transport-shape implementation.

**Decision (Earl, 2026-07-20):** v0.1.6 should establish the working shape;
the issues below can be patched upstream. The implementation may proceed once
the Gateway-owned host, durable bindings, chat-addressed delivery, attribution,
receipt handling, restart behavior, migration continuity, and truthful
capability reporting work end to end.

**Immediate safety invariant:** migration/continuity tooling must never print
or persist raw cookies, client keys, passwords, bearer tokens, or transport
secrets. Domain-separated fingerprints are sufficient for comparison.

## Follow-up ledger

### H1 — Reconcile transport canon

`internal/ROADMAP.md`, the SDK capabilities spec, and the v0.1.6 plan still
describe different ownership and routing models in places: member-level versus
chat-level fan-out, old versus namespaced TAP frames, daemon versus Gateway
delivery ownership, and immediate versus deferred `kind='im'` ownership rows.

**Closure:** make the Gateway-owned, chat-addressed model canonical in all
three documents and add semantic canon checks for those exact decisions.

### H2 — Authorize lane binding as membership

Under the v0.1.6 model, a bound external chat is external membership. A bridge
request to bind a chat to a thread or DM is therefore an authorization change,
not merely provider metadata.

**Closure:** enforce host-side lane policy and lane existence; constrain
automatic DM binding to the resolved external principal; add authenticated,
audited bind/rebind/unbind administration; prove an arbitrary bridge binding is
zero-write; define what happens to pending obligations when a binding moves or
is revoked.

### H3 — Enforce immutable human/session linkage in storage

The migration shape needs an explicit `human_session.principal_id` column and
backfill. Nullable stable-ID columns and a unique index alone still permit new
identity-less rows.

**Closure:** add and backfill the session principal link, then enforce non-null
stable linkage for new human and session rows using constraints, a table
rebuild, or equivalent libsql-safe enforcement. Add raw-SQL negative tests.

### H4 — Pin daemon principal-attribution authority

`command_intents` is caller truth. `command_intent_events` supplies transition
cursors and receipts join back to the command row. The earlier plan text
incorrectly described the `inbox_subscriptions` region as a receipt/audit
table.

**Closure:** enumerate the exact authoritative tables and projection joins.
Add `caller_principal_id` to subscriptions only if that path actually accepts
and projects a principal-bearing caller, with a separate contract test.

### H5 — Define authenticated secrets-CLI mutation

The current Rust Gateway read client is GET-only and only optionally forwards
`NEXUS_REST_TOKEN`; existing `GatewayCmd` arms do not provide a mutation
pattern.

**Closure:** define the JSON POST/DELETE client, discovery and admin credential
source, `admin:*` scope, typed 401/403 behavior, and successful operation under
remote-human mode. Keep secret values out of argv, logs, stdout, and stderr.

### H6 — Remove the production migration-fault backdoor

`#[doc(hidden)] pub` is still a public shipped API. A downstream caller could
select a deliberate migration failure against an arbitrary identity path.

**Closure:** keep the normal production migration incapable of selecting a
fault. Expose fault injection only through a non-default test feature or a
private executor with a feature-gated integration-test shim.

### H7 — Freeze the complete TAP state machine

The draft says the bridge's `transport/hello` is its first frame while also
listing host `transport/init` first. Bind and bind-lane operations have typed
store conflicts but no bridge-visible acknowledgement/refusal contract.

**Closure:** specify spawn -> hello -> version validation -> init -> ready,
including deadlines and failure states. Define bind acknowledgement/refusal
and ensure ingress cannot race a rejected or uncommitted binding. Test missing
or reversed hello, ingress-before-ready, and rejected-bind zero-ingress.

### H8 — Harden identity-continuity evidence

The first continuity-script shape is useful but is not yet durable evidence:
its inventory omits old columns and legacy binding rows, a self-contained hash
can be rewritten with the file, live reads across multiple stores are not a
coherent snapshot, and the second-boot stable-ID comparison lacks a defined
first-verify receipt.

**Closure:** fingerprint every pre-schema row/column, including legacy native
bindings; quiesce/checkpoint stores or use supported coherent snapshots; bind
the pre-snapshot digest into an external root-owned upgrade receipt; publish
artifacts atomically with no-follow/single-link checks; emit and seal an
explicit first-verify stable-ID mapping for the second-boot comparison.

### H9 — Put continuity tooling before its live consumer

The sequential plan currently invokes the identity-continuity script from the
Telegram live gate before the later task implements it.

**Closure:** move continuity tooling before the live transport task, or remove
the sequential dependency by making the gate consume an already-landed tool.

### H10 — Strengthen outbox collision defense

The proposed obligation ID truncates SHA-256 to 24 hexadecimal characters and
relies on `INSERT OR IGNORE`. A collision would silently suppress a legitimate
delivery.

**Closure:** use the full digest and add a unique composite constraint over
`(message_id, provider, external_chat_id)` so a collision becomes a typed
integrity failure rather than message loss.

### H11 — Eliminate Gateway full-suite transport fixture flakiness

The parallel Gateway suite intermittently misses transport fixture state files
or times out while another worker is under load. Immediate reruns pass without
source changes, so a single green run is not reliable merge evidence.

**Closure:** isolate every transport fixture path and lifecycle across Vitest
workers, diagnose the remaining timing/shared-state race, and require three
consecutive clean full Gateway runs before the final rebase/merge gate.

## v0.1.6 shape gate

The deferred items above should not expand the initial implementation gate.
For v0.1.6, prove only:

1. The Gateway launches and negotiates one bridge without a restart loop.
2. External principal/subject and chat/lane bindings survive restart.
3. Telegram ingress retains external-human attribution.
4. Outbound delivery creates one durable obligation per bound chat and settles
   it by receipt.
5. One crash/restart path redelivers without a duplicate provider send.
6. Existing humans, sessions, agents, and legacy native bindings survive the
   migration; minted IDs remain stable after a second boot.
7. Capabilities report real running and disabled transport states.
