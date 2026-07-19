# Gateway Message Hooks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Ship v0.1.5 with a Gateway-owned, language-neutral message-hook system that can transform canonical messages at `before_send`, observe them at `after_receipt`, attach signed provenance, and select `interrupt`, `yield_turn`, or `after_tool_loop` delivery timing.

**Architecture:** Keep one canonical daemon message boundary and one generic Gateway hook engine. The daemon asks the connected Gateway to evaluate `before_send` before it commits a new message; the Gateway later evaluates `after_receipt` from the durable accepted-message projection. Local executable hooks use versioned JSON over stdin/stdout, while a correlated capability-negotiated daemon–Gateway frame pair provides the only new transport seam. Gateway owns manifests, execution, audit history, signing, and the original-message snapshot; the daemon remains transport-focused and stores only the accepted message metadata and unsettled delivery state.

**Tech Stack:** Rust 2021, Tokio, serde, libsql; Node.js 20+, TypeScript 5, Vitest, `@libsql/client`; generated TypeScript contracts; local Unix socket / Windows named-pipe Gateway stream.

---

## Non-negotiable boundaries

- Tests stay outside production source files.
- The browser never talks to the daemon directly.
- Hook programs never receive daemon credentials or direct database access.
- Projects remain metadata and do not participate in hook identity, routing, or authorization.
- Hook code cannot alter sender identity, target identity, message ID, routing, idempotency key, or the reserved `_nexus` metadata namespace.
- `after_receipt` runs once per canonical message ID, not once per recipient.
- Token-stream and agent-session frames are outside the hook pipeline.
- The v0.1.4 tag and release candidate remain immutable while this v0.1.5 work proceeds on `main`.

## Task 1: Add the public hook and message metadata contracts

**Files:**

- Create: `core/crates/nexus-contracts/src/hooks.rs`
- Modify: `core/crates/nexus-contracts/src/send.rs`
- Modify: `core/crates/nexus-contracts/src/metadata.rs`
- Modify: `core/crates/nexus-contracts/src/lib.rs`
- Modify: `core/crates/nexus-contracts/tests/roundtrip.rs`
- Modify: `core/crates/nexus-contracts/tests/optional_wire_fields.rs`
- Modify: `core/crates/nexus-contracts/tests/module_roundtrips.rs`
- Modify: `core/crates/nexus-contracts/tests/ts_gen.rs`
- Modify generated: `gateway/src/shared/types/contracts.gen.ts`

- [ ] **Step 1: Write failing Rust contract tests.**

  Cover an omitted `SendRequest.metadata`, a nested metadata object, all three timing values, a `before_send` request/result round trip, an `after_receipt` request/result round trip, and metadata-merge idempotency.

  ```rust
  let req = HookBeforeSendRequest {
      evaluation_id: "hook-eval-1".into(),
      message: HookMessage { /* immutable route plus mutable content */ },
  };
  assert_eq!(serde_json::from_value::<HookBeforeSendRequest>(serde_json::to_value(&req)?)?, req);
  ```

- [ ] **Step 2: Run the focused tests and confirm RED.**

  ```bash
  CARGO_BUILD_JOBS=2 cargo test -p nexus-contracts --test roundtrip --test optional_wire_fields --test module_roundtrips
  ```

  Expected: compile failures for missing hook DTOs and `SendRequest.metadata`.

- [ ] **Step 3: Implement the smallest stable wire types.**

  Add:

  ```rust
  pub enum DeliveryTiming { Interrupt, YieldTurn, AfterToolLoop }
  pub struct HookBeforeSendRequest { pub evaluation_id: String, pub message: HookMessage }
  pub struct HookBeforeSendResult { pub evaluation_id: String, pub decision: HookDecision, ... }
  pub struct HookAfterReceiptRequest { pub invocation_id: String, pub message: HookMessage, pub receipt: Ack }
  pub struct HookAfterReceiptResult { pub invocation_id: String, pub metadata: Option<Value>, ... }
  ```

  Use additive optional fields, camel-case JSON, and typed validation. Add `metadata: Option<serde_json::Map<String, Value>>` to `SendRequest`. Define a merge request that carries `message_id`, `invocation_id`, and the metadata patch.

- [ ] **Step 4: Regenerate and verify TypeScript.**

  ```bash
  pnpm --dir gateway gen:contracts
  pnpm --dir gateway check:contracts
  ```

- [ ] **Step 5: Run focused GREEN gates and commit.**

  ```bash
  CARGO_BUILD_JOBS=2 cargo test -p nexus-contracts
  git add core/crates/nexus-contracts gateway/src/shared/types/contracts.gen.ts
  git commit -m "feat(hooks): define message hook contracts"
  ```

## Task 2: Discover and validate local hook manifests

**Files:**

- Create: `gateway/src/server/hooks/types.ts`
- Create: `gateway/src/server/hooks/manifest.ts`
- Create: `gateway/src/server/hooks/registry.ts`
- Create: `gateway/src/server/hooks/manifest.test.ts`
- Create: `gateway/src/server/hooks/registry.test.ts`
- Modify: `gateway/package.json`
- Modify: `gateway/pnpm-lock.yaml`

- [ ] **Step 1: Write failing manifest tests.**

  Test `$NEXUS_HOME/gateway/hooks.d/*.toml`, stable `(order, id)` sorting, duplicate IDs, unknown events, invalid commands, unsupported versions, atomic generation snapshots, and retention of the last valid snapshot after a bad hot reload.

  ```toml
  version = 1
  id = "annotate-risk"
  event = "before_send"
  order = 20
  command = ["python3", "/opt/hooks/annotate.py"]
  timeout_ms = 2000
  ```

- [ ] **Step 2: Confirm RED.**

  ```bash
  pnpm --dir gateway exec vitest run src/server/hooks/manifest.test.ts src/server/hooks/registry.test.ts
  ```

  Expected: modules do not exist.

- [ ] **Step 3: Implement strict parsing and immutable snapshots.**

  Add one small TOML dependency. Validate the complete directory before swapping the active snapshot. Reject symlinks and non-regular manifest files. Watch with a bounded debounce; expose explicit `start()` and `close()` lifecycle methods.

- [ ] **Step 4: Run GREEN and commit.**

  ```bash
  pnpm --dir gateway exec vitest run src/server/hooks/manifest.test.ts src/server/hooks/registry.test.ts
  pnpm --dir gateway typecheck
  git add gateway/package.json gateway/pnpm-lock.yaml gateway/src/server/hooks
  git commit -m "feat(gateway): discover local message hooks"
  ```

## Task 3: Build the event-neutral hook engine

**Files:**

- Create: `gateway/src/server/hooks/eventAdapter.ts`
- Create: `gateway/src/server/hooks/merge.ts`
- Create: `gateway/src/server/hooks/engine.ts`
- Create: `gateway/src/server/hooks/merge.test.ts`
- Create: `gateway/src/server/hooks/engine.test.ts`

- [ ] **Step 1: Write failing composition tests.**

  Prove sequential execution, later-result precedence, recursive metadata merge, preserved explicit `null`, immutable routing fields, reserved `_nexus` rejection, rejection short-circuiting, no-hook pass-through, and stable result ordering.

- [ ] **Step 2: Confirm RED.**

  ```bash
  pnpm --dir gateway exec vitest run src/server/hooks/merge.test.ts src/server/hooks/engine.test.ts
  ```

- [ ] **Step 3: Implement one generic spine.**

  Keep event-specific behavior behind:

  ```ts
  export interface HookEventAdapter<Input, Output> {
    readonly event: HookEventName;
    invocation(input: Input, context: HookContext): HookInvocation;
    apply(input: Input, result: HookProgramResult): Output;
  }
  ```

  `HookEngine` selects handlers from one registry snapshot and invokes one `HookRunner` sequentially. It knows nothing about shell, Python, JavaScript, HTTP callbacks, or message routing.

- [ ] **Step 4: Run GREEN and commit.**

  ```bash
  pnpm --dir gateway exec vitest run src/server/hooks/merge.test.ts src/server/hooks/engine.test.ts
  git add gateway/src/server/hooks
  git commit -m "feat(gateway): add generic hook execution spine"
  ```

## Task 4: Execute local shell, JavaScript, Python, and native commands safely

**Files:**

- Create: `gateway/src/server/hooks/localCommandRunner.ts`
- Create: `gateway/src/server/hooks/localCommandRunner.test.ts`
- Create: `gateway/src/server/hooks/test-fixtures/echo-hook.mjs`
- Create: `gateway/src/server/hooks/test-fixtures/echo-hook.py`
- Create: `gateway/src/server/hooks/test-fixtures/echo-hook.sh`

- [ ] **Step 1: Write failing runner tests.**

  Verify exact JSON stdin/stdout, no shell interpolation, bounded stdout/stderr, timeout and process-tree termination, non-zero exit reporting, malformed JSON, excessive output, environment allowlisting, JS/Python/shell parity, and deterministic invocation IDs.

- [ ] **Step 2: Confirm RED.**

  ```bash
  pnpm --dir gateway exec vitest run src/server/hooks/localCommandRunner.test.ts
  ```

- [ ] **Step 3: Implement the command runner.**

  Spawn `command[0]` directly with `shell: false`. Pass one versioned JSON object on stdin. Accept exactly one JSON object on stdout. Bound wall time, bytes, and concurrent children; kill the process group on timeout. Pass only `PATH`, locale, `HOME`, `NEXUS_HOOK_*` identifiers, and manifest-declared environment values.

- [ ] **Step 4: Run GREEN and commit.**

  ```bash
  pnpm --dir gateway exec vitest run src/server/hooks/localCommandRunner.test.ts
  git add gateway/src/server/hooks
  git commit -m "feat(gateway): run local hooks with bounded resources"
  ```

## Task 5: Add Gateway audit persistence and signing

**Files:**

- Modify: `gateway/src/server/store/migrations.ts`
- Modify: `gateway/src/server/store/schema.ts`
- Modify: `gateway/src/server/store/migrations.test.ts`
- Create: `gateway/src/server/hooks/store.ts`
- Create: `gateway/src/server/hooks/store.test.ts`
- Create: `gateway/src/server/hooks/signing.ts`
- Create: `gateway/src/server/hooks/signing.test.ts`

- [ ] **Step 1: Write failing migration, audit, and signature tests.**

  Require an additive schema migration with tables for pipeline evaluations, handler executions, and receipt completion. Assert the original message is stored once per evaluation, later retries reuse it, invocation IDs are unique, artifact digests change with executable bytes, signatures verify, and private key material never enters message metadata.

- [ ] **Step 2: Confirm RED.**

  ```bash
  pnpm --dir gateway exec vitest run src/server/store/migrations.test.ts src/server/hooks/store.test.ts src/server/hooks/signing.test.ts
  ```

- [ ] **Step 3: Implement persistence and attestation.**

  Store a Gateway-local Ed25519 key under `$NEXUS_HOME/gateway/keys/` with owner-only permissions. Sign the canonical tuple `(invocationId, hookId, event, artifactDigest, startedAt, completedAt, outcome)`. Write compact ordered entries under `metadata._nexus.hooks.executedBy`; do not write input/output digests or duplicate the original body.

- [ ] **Step 4: Run GREEN and commit.**

  ```bash
  pnpm --dir gateway exec vitest run src/server/store/migrations.test.ts src/server/hooks/store.test.ts src/server/hooks/signing.test.ts
  git add gateway/src/server/store gateway/src/server/hooks
  git commit -m "feat(gateway): persist and attest hook executions"
  ```

## Task 6: Add correlated hook RPC to the daemon–Gateway stream

**Files:**

- Modify: `core/crates/nexus-contracts/src/hooks.rs`
- Create: `core/crates/nexus/src/daemon/gateway_hook_bridge.rs`
- Modify: `core/crates/nexus/src/daemon/mod.rs`
- Modify: `core/crates/nexus/src/daemon/gateway_stream_socket.rs`
- Create: `core/crates/nexus/tests/gateway_hook_bridge.rs`
- Modify: `gateway/src/server/agui/daemonPushRelay.mjs`
- Modify: `gateway/src/server/agui/daemonPushRelay.d.mts`
- Modify: `gateway/src/server/agui/daemonPushRelay.test.ts`
- Create: `gateway/src/server/hooks/bridge.ts`
- Create: `gateway/src/server/hooks/bridge.test.ts`

- [ ] **Step 1: Write failing Rust and TypeScript protocol tests.**

  Cover capability advertisement, generation IDs, request/response correlation, concurrent evaluations completing out of order, timeout cleanup, Gateway reconnect, unknown frame compatibility, and required/optional mode.

- [ ] **Step 2: Confirm RED.**

  ```bash
  CARGO_BUILD_JOBS=2 cargo test -p nexus gateway_hook_bridge
  pnpm --dir gateway exec vitest run src/server/agui/daemonPushRelay.test.ts src/server/hooks/bridge.test.ts
  ```

- [ ] **Step 3: Implement an additive protocol.**

  Extend `hello` with optional hook capabilities. Add `hook.evaluate` daemon frames and `hook.result` Gateway frames with correlation IDs. Keep pending requests in a bounded map with cancellation and timeouts. Do not route correlated RPC through the broadcast publisher.

- [ ] **Step 4: Implement Gateway bridge lifecycle.**

  The Gateway registers its hook engine on the shared daemon stream connection, advertises its active generation, handles `hook.evaluate`, and returns one terminal `hook.result` per correlation ID.

- [ ] **Step 5: Run GREEN and commit.**

  ```bash
  CARGO_BUILD_JOBS=2 cargo test -p nexus gateway_hook_bridge
  pnpm --dir gateway exec vitest run src/server/agui/daemonPushRelay.test.ts src/server/hooks/bridge.test.ts
  git add core/crates/nexus-contracts core/crates/nexus/src/daemon core/crates/nexus/tests gateway/src/server/agui gateway/src/server/hooks
  git commit -m "feat(hooks): bridge daemon messages through Gateway"
  ```

## Task 7: Put `before_send` on the single canonical message boundary

**Files:**

- Create: `core/crates/nexus-contracts/src/hook_ports.rs`
- Modify: `core/crates/nexus-contracts/src/lib.rs`
- Modify: `core/crates/nexus-bus/src/service.rs`
- Modify: `core/crates/nexus-bus/src/broadcast_messages.rs`
- Create: `core/crates/nexus-bus/tests/hooks.rs`
- Modify: `core/crates/nexus-store/src/repos/messages.rs`
- Modify: `core/crates/nexus-store/tests/repo_roundtrips.rs`
- Modify: `core/crates/nexus/src/daemon/services/loop_wiring.rs`

- [ ] **Step 1: Write failing bus and store tests.**

  Prove every source that reaches `BusPort::send` uses one hook evaluation, duplicate idempotency keys do not rerun hooks, mutable fields are committed atomically with metadata, rejections create no message, and optional mode passes through when Gateway is absent while required mode returns a typed error.

- [ ] **Step 2: Confirm RED.**

  ```bash
  CARGO_BUILD_JOBS=2 cargo test -p nexus-bus --test hooks
  CARGO_BUILD_JOBS=2 cargo test -p nexus-store --test repo_roundtrips
  ```

- [ ] **Step 3: Add a harness-neutral port and preflight idempotency.**

  Add `MessageHookPort` to contracts. Inject it into `Bus`. Before evaluation, normalize the producer key and return an existing acknowledgement if present. Evaluate only genuinely new messages, validate the returned immutable route fields, then resolve policy and commit the transformed message and metadata together.

- [ ] **Step 4: Run GREEN and commit.**

  ```bash
  CARGO_BUILD_JOBS=2 cargo test -p nexus-bus --test hooks
  CARGO_BUILD_JOBS=2 cargo test -p nexus-store --test repo_roundtrips
  git add core/crates/nexus-contracts core/crates/nexus-bus core/crates/nexus-store core/crates/nexus/src/daemon/services
  git commit -m "feat(hooks): evaluate before canonical message commit"
  ```

## Task 8: Enforce delivery timing in the dispatch lane

**Files:**

- Create: `core/crates/nexus-dispatch/src/delivery_timing.rs`
- Modify: `core/crates/nexus-dispatch/src/lib.rs`
- Modify: `core/crates/nexus-dispatch/src/event_loop.rs`
- Create: `core/crates/nexus-dispatch/tests/delivery_timing.rs`
- Modify: `core/crates/nexus-store/src/repos/inbox.rs`
- Create: `core/crates/nexus-store/tests/delivery_timing.rs`
- Modify: `core/crates/nexus-contracts/src/ports.rs`

- [ ] **Step 1: Write failing timing-state tests.**

  Cover:

  - `interrupt`: native steer when supported, otherwise atomic interrupt-and-send, typed unsupported error when neither exists.
  - `yield_turn`: wait for the current turn to complete; use an advertised intermediate yield only when it is unambiguous.
  - `after_tool_loop`: ignore tool starts/results and release only after the adapter reports the final model turn completion.
  - idle targets: all policies start normally without artificial delay.
  - restart: persisted unsettled timing survives daemon replacement without duplicate injection.

- [ ] **Step 2: Confirm RED.**

  ```bash
  CARGO_BUILD_JOBS=2 cargo test -p nexus-dispatch --test delivery_timing
  CARGO_BUILD_JOBS=2 cargo test -p nexus-store --test delivery_timing
  ```

- [ ] **Step 3: Implement explicit timing state.**

  Persist timing on unsettled recipient rows. Keep scheduling harness-neutral through `AgentTurnExecutionPort` capabilities. Treat adapter completion as the authoritative `after_tool_loop` boundary; visible text and inferred tool-call counts are never timing signals.

- [ ] **Step 4: Run GREEN and commit.**

  ```bash
  CARGO_BUILD_JOBS=2 cargo test -p nexus-dispatch --test delivery_timing
  CARGO_BUILD_JOBS=2 cargo test -p nexus-store --test delivery_timing
  git add core/crates/nexus-contracts core/crates/nexus-dispatch core/crates/nexus-store
  git commit -m "feat(dispatch): enforce message delivery timing"
  ```

## Task 9: Run `after_receipt` exactly once and merge metadata idempotently

**Files:**

- Modify: `gateway/src/server/projection/apply.ts`
- Modify: `gateway/src/server/projection/service.ts`
- Modify: `gateway/src/server/projection/apply.test.ts`
- Modify: `gateway/src/server/projection/service.test.ts`
- Create: `gateway/src/server/hooks/afterReceipt.ts`
- Create: `gateway/src/server/hooks/afterReceipt.test.ts`
- Modify: `core/crates/nexus-store/src/repos/metadata.rs`
- Modify: `core/crates/nexus-store/tests/metadata.rs`
- Modify: `core/crates/nexus/src/daemon/routing.rs`
- Create: `core/crates/nexus/tests/hook_metadata_merge.rs`

- [ ] **Step 1: Write failing projection and merge tests.**

  Assert one invocation per `message.accepted` event ID across replay/reconnect, no invocation per delivery outcome, idempotent side-effect identity, immediate Gateway-canonical metadata, eventual daemon metadata merge, retry after daemon outage, and no duplicate provenance entry after acknowledgement loss.

- [ ] **Step 2: Confirm RED.**

  ```bash
  pnpm --dir gateway exec vitest run src/server/hooks/afterReceipt.test.ts src/server/projection/apply.test.ts src/server/projection/service.test.ts
  CARGO_BUILD_JOBS=2 cargo test -p nexus-store --test metadata
  CARGO_BUILD_JOBS=2 cargo test -p nexus hook_metadata_merge
  ```

- [ ] **Step 3: Implement durable receipt processing.**

  After a projection transaction commits, claim the invocation by canonical message ID, run the event through the same engine/runner, update Gateway canonical metadata in the same completion transaction, then submit the invocation-keyed merge to the daemon. A failed side effect remains retryable and visible in audit state.

- [ ] **Step 4: Run GREEN and commit.**

  ```bash
  pnpm --dir gateway exec vitest run src/server/hooks/afterReceipt.test.ts src/server/projection/apply.test.ts src/server/projection/service.test.ts
  CARGO_BUILD_JOBS=2 cargo test -p nexus-store --test metadata
  CARGO_BUILD_JOBS=2 cargo test -p nexus hook_metadata_merge
  git add gateway/src/server/hooks gateway/src/server/projection core/crates/nexus-store core/crates/nexus
  git commit -m "feat(hooks): process accepted message receipts once"
  ```

## Task 10: Wire Gateway startup and expose read-only diagnostics

**Files:**

- Modify: `gateway/src/server/gateway/headless.ts`
- Modify: `gateway/src/server/gateway/headless.test.ts`
- Create: `gateway/src/server/hooks/service.ts`
- Create: `gateway/src/server/hooks/service.test.ts`
- Modify: `gateway/src/server/api/router.ts`
- Modify: `gateway/src/server/api/handlers.ts`
- Modify: `gateway/src/server/api/api.test.ts`
- Modify: `core/crates/nexus/src/cli/commands/gateway.rs`
- Modify: `core/crates/nexus/tests/cli_help.rs`

- [ ] **Step 1: Write failing lifecycle and read-surface tests.**

  Test startup before network listen, clean shutdown, bad manifests retaining the prior generation, `GET /api/v1/hooks`, public-key/audit verification reads, redaction of commands and local paths for non-admin callers, and `nexus gateway hooks list --json` delegating to Gateway.

- [ ] **Step 2: Confirm RED.**

  ```bash
  pnpm --dir gateway exec vitest run src/server/hooks/service.test.ts src/server/gateway/headless.test.ts src/server/api/api.test.ts
  CARGO_BUILD_JOBS=2 cargo test -p nexus cli_help
  ```

- [ ] **Step 3: Wire service lifecycle and diagnostics.**

  Start the registry, signer, store, runner, engine, and bridge as one Gateway-owned service. Keep CLI operations read-only; manifest files remain the mutation surface.

- [ ] **Step 4: Run GREEN and commit.**

  ```bash
  pnpm --dir gateway exec vitest run src/server/hooks/service.test.ts src/server/gateway/headless.test.ts src/server/api/api.test.ts
  CARGO_BUILD_JOBS=2 cargo test -p nexus cli_help
  git add gateway/src/server core/crates/nexus/src/cli core/crates/nexus/tests
  git commit -m "feat(gateway): operate and inspect message hooks"
  ```

## Task 11: Add cross-source and failure-mode practical gates

**Files:**

- Create: `scripts/nexus-hooks-practical-gate`
- Create: `scripts/test-nexus-hooks-practical-gate`
- Modify: `scripts/check`
- Create: `core/tests/fixtures/hooks/before-send.py`
- Create: `core/tests/fixtures/hooks/after-receipt.mjs`

- [ ] **Step 1: Write the script contract test first.**

  Require useful `--help` with no side effects. Use a disposable home and clamped Docker runtime. Exercise direct daemon RPC, CLI command intent, Gateway REST, network MCP, notification, and agent-origin sends. Verify all three timing modes, transformed body/metadata, one signed receipt hook, idempotent retries, Gateway optional/required outage behavior, restart recovery, and no token-stream interception.

- [ ] **Step 2: Confirm RED, implement, and run GREEN.**

  ```bash
  scripts/test-nexus-hooks-practical-gate
  scripts/nexus-hooks-practical-gate --help
  scripts/nexus-hooks-practical-gate
  ```

- [ ] **Step 3: Commit.**

  ```bash
  git add scripts core/tests/fixtures/hooks
  git commit -m "test(hooks): cover canonical message sources"
  ```

## Task 12: Document the public contract and v0.1.5 release

**Files:**

- Create: `docs/hooks.md`
- Modify: `docs/extending-nexus.md`
- Modify: `docs/architecture.md`
- Modify: `docs/cli.md`
- Modify: `docs/rest-api.md`
- Modify: `docs/coding-standards.md`
- Modify: `docs/README.md`
- Modify: `CHANGELOG.md`
- Modify: `gateway/package.json`
- Modify: `gateway/webconsole/package.json`
- Modify: `packages/nexus-cli/package.json`
- Modify: `packages/nexus/package.json`
- Modify: lockfiles and release-identity fixtures selected by `scripts/check release-identity`

- [ ] **Step 1: Write documentation and limitations.**

  Include copy-pasteable shell, JS, and Python examples; manifest fields; JSON schemas; timing semantics; local trust model; audit verification; optional/required Gateway behavior; no token-stream hooks; at-least-once `after_receipt` side effects; and the callback/SDK/custom-policy extension seams without claiming they ship.

- [ ] **Step 2: Bump npm facets to 0.1.5 and make release identity RED then GREEN.**

  ```bash
  scripts/check release-identity
  ```

  Expected RED before the version edits and GREEN after all three package facets and dependency edges agree.

- [ ] **Step 3: Commit.**

  ```bash
  git add docs CHANGELOG.md gateway/package.json gateway/webconsole/package.json gateway/pnpm-lock.yaml packages scripts
  git commit -m "docs(hooks): publish the v0.1.5 contract"
  ```

## Task 13: Run the capped release gate

- [ ] **Step 1: Check layout, generated contracts, and architecture.**

  ```bash
  scripts/check architecture
  scripts/check boundaries
  scripts/check core-test-layout
  pnpm --dir gateway check:contracts
  ```

- [ ] **Step 2: Run Rust in the clamped validator.**

  ```bash
  CARGO_BUILD_JOBS=2 scripts/check rust-workspace
  ```

- [ ] **Step 3: Run Gateway gates.**

  ```bash
  pnpm --dir gateway typecheck
  pnpm --dir gateway test
  pnpm --dir gateway build
  ```

- [ ] **Step 4: Run hook and package practical gates.**

  ```bash
  scripts/nexus-hooks-practical-gate
  scripts/test-nexus-npm-publication-contract
  scripts/test-nexus-npm-launcher
  scripts/check release-identity
  ```

- [ ] **Step 5: Review tracked output and commit only intentional changes.**

  ```bash
  git status --short
  git diff --check
  git log --format='%h %an <%ae> %s' -15
  ```

  Do not commit databases, keys, manifests, hook executables from operator homes, logs, screenshots, OAuth state, or generated runtime data.

## Completion criteria

- All new messages traverse exactly one `before_send` evaluation before canonical commit.
- Hook retries do not duplicate a message or an execution record.
- All three timing values have deterministic adapter-neutral behavior and restart coverage.
- Every accepted message produces at most one canonical `after_receipt` invocation identity.
- Shell, JavaScript, Python, and native executables use the same JSON protocol.
- Gateway audit records preserve the original once and signed execution provenance without body duplication.
- Gateway absence follows the configured optional/required mode without wedging daemon delivery.
- Public docs describe the shipped surface and its limitations accurately.
- Architecture, boundaries, layout, contracts, Rust, Gateway, package, and practical gates are green on the exact v0.1.5 candidate revision.
