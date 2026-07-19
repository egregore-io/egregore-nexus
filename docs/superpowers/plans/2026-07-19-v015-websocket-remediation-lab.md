# v0.1.5 WebSocket Remediation and Nexus–Lens Validation Lab Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended for isolated tasks) or `superpowers:executing-plans` to execute this plan task by task. Use `superpowers:test-driven-development` for every behavior change and `superpowers:verification-before-completion` before any completion claim.

**Goal:** Make Nexus v0.1.5 usable end to end from the real Lens Electron UI, with stable event and agent-session WebSockets, restart-safe identity/projection behavior, packed-package correctness, and a repeatable isolated evidence gate that never touches the host runtime.

**Architecture:** The daemon remains the lightweight identity/runtime/transport authority. Gateway remains the canonical browser backend and durable product-history authority. Fleet and ephemeral tool-call topics ride daemon push; thread and DM topics wake from Gateway's post-commit change bus and read canonical `bus_messages`. Lens hydrates once over REST and receives live invalidations/agent-session frames over Gateway WebSockets. All restart and ocular work runs inside the existing single Docker validator at six CPUs.

**Tech Stack:** Rust/Tokio/libSQL, TypeScript/Node/ESM, Vitest, WebSocket (`ws`), Bash, Docker, Electron, Xvfb, Chrome DevTools Protocol, Python/Pytest, PACTBIN2.

**Design contract:** `docs/superpowers/specs/2026-07-19-v015-websocket-remediation-lab-design.md`

---

## Execution rules

- Do not install, restart, attach to, signal, or reconfigure the host Nexus daemon, Gateway, or Webconsole.
- Do not mount host `~/.nexus` into the validator. Source worktrees may be mounted; runtime state stays under `/tmp/nexus-lens-home` in the container.
- Reuse the one validator container. Never run two Nexus validation containers concurrently.
- Preserve unrelated dirty files. Stage and commit only the exact files named by the completed task.
- Keep Rust tests outside production modules and keep Lens ocular tests outside UI implementation files.
- Every release script must return useful `--help` without Docker, Cargo, browsers, package managers, process cleanup, or runtime mutation.
- Tests drive the real authority boundary. Do not add a compatibility read from daemon storage to make a Gateway test pass.
- No `Co-Authored-By` or agent/tool attribution in commits.

## Ownership and non-overlap

- **Paul:** Tasks 1–8 and 11–14; final integration and release evidence.
- **Bob:** Task 9 packaged-Gateway closure and read-only review of Tasks 2–8 and 11–14. Bob sends a tested diff before commit; Paul integrates it.
- **Fable:** Task 10 complete human-path Electron affordance suite and PACTBIN2 ocular fixtures. Fable works only in `egregore-lens`/Pactree worktrees and sends a tested diff before commit.

## Task 1: Freeze the baseline and host-isolation contract

**Files:**

- Create: `scripts/nexus-v015-host-isolation-snapshot`
- Create: `scripts/test-nexus-v015-host-isolation-snapshot`
- Modify: `docs/release-regression.md`

**Step 1: Write the failing script contract test**

The test must prove that `--help` has no side effects and that a fixture snapshot records only stable host-runtime identity—not live database content:

```bash
scripts/nexus-v015-host-isolation-snapshot --help
scripts/test-nexus-v015-host-isolation-snapshot
```

Expected initial result: FAIL because the script is absent.

**Step 2: Implement the read-only snapshot**

Emit JSON with:

```json
{
  "daemon": { "pid": 0, "bootId": "", "executable": "", "sha256": "" },
  "gateway": { "pid": 0, "listener": "", "executable": "", "sha256": "" },
  "webconsole": { "pid": 0, "listener": "", "executable": "", "sha256": "" },
  "ipc": { "path": "", "inode": 0 },
  "capturedAt": 0
}
```

Read discovery files and `/proc` only. Do not hash `nexus.db`, `gateway.db`, WALs, or logs because legitimate host bus traffic changes them during the lab.

**Step 3: Verify deterministic comparison**

Add a `compare BEFORE AFTER` mode that ignores `capturedAt` and fails on PID, boot ID, executable hash, listener owner, or socket inode change.

**Step 4: Run gates**

```bash
scripts/test-nexus-v015-host-isolation-snapshot
scripts/test-nexus-release-entrypoint-help
```

**Step 5: Commit**

```bash
git add scripts/nexus-v015-host-isolation-snapshot \
  scripts/test-nexus-v015-host-isolation-snapshot docs/release-regression.md
git commit -m "test: define v0.1.5 host isolation evidence"
```

## Task 2: Finish canonical restored-runtime replay

**Files:**

- Modify: `core/crates/nexus/src/daemon/services/ws_sink.rs`
- Modify: `core/crates/nexus/src/daemon/app/runtime_registration.rs`
- Modify: `core/crates/nexus/tests/lightweight_daemon.rs`
- Modify: `gateway/src/server/store/repos/events.ts`
- Modify: `gateway/src/server/projection/apply.ts`
- Modify: `gateway/src/server/projection/apply.test.ts`

**Step 1: Keep the external regressions red before implementation**

The Rust test must seed identity-owned agent/runtime/session rows while the transport store lacks them, restore the runtime, and assert the complete emitted payload:

```rust
assert_eq!(payload["agentId"], "a_bob");
assert_eq!(payload["name"], "Bob");
assert_eq!(payload["sessionId"], "s_bob");
assert_eq!(payload["harness"], "codex");
assert_eq!(payload["transport"], "headless");
assert_eq!(payload["presence"], "online");
```

The Gateway test must apply one old epoch with active Earl, then a new epoch replay for Bob/Paul/Fable and assert that Earl is no longer active while the three current runtimes are fully materialized.

**Step 2: Use the identity authority for descriptors**

In `ws_sink.rs`, query `Agents`, `AgentRuntimes`, and `IdentitySessions` through the identity store. The in-memory transport store remains the stream-event source only.

**Step 3: Reconcile on the first event of a new epoch**

In `apply.ts`, before materializing a prior-epoch replay:

```ts
if (context.epochChanged) {
  await retireActiveRuntimes(db, event.occurredAt);
}
```

Retirement changes current presence/runtime state; it does not delete identity audit rows.

**Step 4: Run focused gates**

```bash
cargo test --manifest-path core/Cargo.toml -p egregore-nexus \
  --test lightweight_daemon restored_runtime
pnpm --dir gateway vitest run src/server/projection/apply.test.ts
cargo fmt --all --manifest-path core/Cargo.toml --check
```

**Step 5: Commit only this boundary**

```bash
git add core/crates/nexus/src/daemon/app/runtime_registration.rs \
  core/crates/nexus/src/daemon/services/ws_sink.rs \
  core/crates/nexus/tests/lightweight_daemon.rs \
  gateway/src/server/store/repos/events.ts \
  gateway/src/server/projection/apply.ts \
  gateway/src/server/projection/apply.test.ts
git commit -m "fix: reconcile restored runtimes after daemon boot"
```

## Task 3: Rebind persistent human principals before acceptance

**Files:**

- Modify: `gateway/src/server/daemon/ipc.ts`
- Modify: `gateway/src/server/command/ingress.ts`
- Modify: `gateway/src/server/command/ingress.test.ts`
- Modify: `gateway/src/routes/api/v1/$.test.ts`
- Modify: `gateway/src/server/identity/human.test.ts`

**Step 1: Cover the cross-boot failure externally**

Create a persistent cookie/client key in boot A, report boot B from the daemon seam, submit the first thread post, and assert registration occurs before enqueue. Cover:

- sequential first write;
- concurrent first writes (singleflight);
- Gateway restart in the same daemon boot;
- a second daemon restart;
- unknown cookie (`401`);
- boot/registration unavailable (`503`, zero accepted intents).

**Step 2: Read the daemon boot ID with a typed options object**

The production call must remain:

```ts
await readDaemonBootId({ nexusHome: opts.nexusHome });
```

Never pass a raw path string to a `DaemonIpcCallOptions` function.

**Step 3: Implement boot-keyed singleflight**

Cache successful bindings by `(nexusHome, clientKey, daemonBootId)`. The pending promise is shared; failure removes it. Re-registration uses the stable human key and updates the returned boot-local session metadata before enqueue.

**Step 4: Verify**

```bash
pnpm --dir gateway vitest run \
  src/server/command/ingress.test.ts \
  'src/routes/api/v1/$.test.ts' \
  src/server/identity/human.test.ts
pnpm --dir gateway typecheck
```

**Step 5: Commit**

```bash
git add gateway/src/server/daemon/ipc.ts \
  gateway/src/server/command/ingress.ts \
  gateway/src/server/command/ingress.test.ts \
  'gateway/src/routes/api/v1/$.test.ts' \
  gateway/src/server/identity/human.test.ts
git commit -m "fix: rebind browser principals after daemon restart"
```

## Task 4: Replace thread/DM timer polling with Gateway post-commit subscriptions

**Files:**

- Create: `gateway/src/server/agui/gatewayDeveloperEvents.ts`
- Create: `gateway/src/server/agui/gatewayDeveloperEvents.test.ts`
- Modify: `gateway/src/server/agui/ws.mjs`
- Modify: `gateway/src/server/agui/ws.d.mts`
- Modify: `gateway/src/server/agui/ws.test.ts`
- Modify: `gateway/src/server/messagePost/readView.ts`
- Modify: `gateway/src/server/messagePost/readView.test.ts`
- Modify: `gateway/src/server/gateway/headless.ts`
- Delete: `gateway/src/server/agui/developerEventsSource.mjs`
- Delete: `gateway/src/server/agui/developerEventsSource.d.mts`
- Delete: `gateway/src/server/agui/developerEventsSource.test.ts`
- Update: `docs/agui.md`

**Step 1: Write red source and socket tests**

Tests must assert:

1. `sys.message.thread.design` performs one initial canonical read, then sleeps without a timer.
2. Publishing `thread-name:design` wakes one bounded read and emits one event.
3. `thread-name:other` does not wake the subscription.
4. Duplicate `message_id` projection does not emit twice.
5. Two subscribers share one process reader but maintain independent cursors/backpressure.
6. `sys.dm.earl` uses `dm-name:earl` and cannot leak another DM lane.
7. Unknown topic shapes emit typed `subscribe.err`.
8. No production path invokes `local.store.read` or `developerEventsSource.since`.

Run and capture the initial failure:

```bash
pnpm --dir gateway vitest run \
  src/server/agui/gatewayDeveloperEvents.test.ts \
  src/server/agui/ws.test.ts \
  src/server/messagePost/readView.test.ts
```

**Step 2: Add a canonical rowid cursor reader**

Add a bounded query over Gateway `bus_messages`:

```ts
SELECT rowid, * FROM bus_messages
WHERE <exact target clause> AND rowid > ?
ORDER BY rowid ASC LIMIT ?
```

Map `rowid` to the browser event `seq`. Rowid is a Gateway-global cursor; gaps between events for one topic are legal, but it is monotonic for every exact topic. Body stays out of the developer-event envelope.

**Step 3: Add a process registry driven by `gatewayChangeBus`**

Subscribe to the exact change key before the first read. A wake schedules one microtask drain; it never creates an interval. Durable rows are always read before waiting, so a missed wake is harmless.

The source contract exposed to `ws.mjs` is:

```ts
interface GatewayDeveloperEventSource {
  subscribe(topic: string, afterSeq: number, handlers: {
    onEvent(event: DeveloperEventEnvelope): boolean | void;
    onGap?(cursor: number): void;
    onError?(error: unknown): void;
  }): { ready: Promise<void>; pause(): void; resume(): void; close(): void };
}
```

**Step 4: Remove the timer path from `ws.mjs`**

Delete `DEFAULT_DEVELOPER_EVENT_POLL_MS`, `#pollMs`, and `#poll`. Thread/DM subscriptions require the injected Gateway canonical source. Fleet/tool calls retain `createDaemonPushDeveloperEventSource`.

**Step 5: Wire the bundled headless Gateway**

`headless.ts` creates the canonical source and passes it to `attachAguiWsUpgrade`. Source-tree and packed modes must use the same injection; no dynamic import of the old daemon-read module remains.

**Step 6: Verify focused and source-boundary gates**

```bash
pnpm --dir gateway vitest run \
  src/server/agui/gatewayDeveloperEvents.test.ts \
  src/server/agui/ws.test.ts \
  src/server/messagePost/readView.test.ts \
  src/server/projection/service.test.ts
! rg -n 'DEFAULT_DEVELOPER_EVENT_POLL_MS|developerEventsSource|local\.store\.read' \
  gateway/src/server/agui/ws.mjs gateway/src/server/agui/gatewayDeveloperEvents.ts
pnpm --dir gateway typecheck
```

**Step 7: Commit**

```bash
git add gateway/src/server/agui gateway/src/server/messagePost/readView.ts \
  gateway/src/server/messagePost/readView.test.ts \
  gateway/src/server/gateway/headless.ts docs/agui.md
git commit -m "fix: push canonical thread events without polling"
```

## Task 5: Surface subscription errors and gaps in Lens

**Files (Lens repository):**

- Modify: `plugins/lens.nexus/lens_nexus/observe.py`
- Modify: `plugins/lens.nexus/lens_nexus/test_observe_stream.py`
- Modify: `plugins/lens.nexus/lens_nexus/backend.py`
- Modify: `plugins/lens.nexus/lens_nexus/test_backend_routes.py`
- Modify: `plugins/lens.nexus/ui/store.ts`
- Modify: `plugins/lens.nexus/ui/store.test.ts`
- Modify: `plugins/lens.nexus/ui/components/StatusBanner.tsx`
- Modify: the external component test selected by Fable after inventory

**Step 1: Add red protocol tests**

`TopicEventReader` must parse and emit typed state for `subscribe.err` and `subscribe.gap`. It must not leave the topic marked live after either frame.

**Step 2: Implement observable recovery**

- `subscribe.err`: set a visible failed/offline topic state and schedule bounded reconnect.
- `subscribe.gap`: perform one bounded REST rehydrate, reset the cursor from the returned snapshot, and resubscribe.
- successful `subscribe.ack`: clear the stale error.

**Step 3: Verify**

```bash
cd /home/earldennison/Projects/egregore-lens
python -m pytest \
  plugins/lens.nexus/lens_nexus/test_observe_stream.py \
  plugins/lens.nexus/lens_nexus/test_backend_routes.py
pnpm vitest run plugins/lens.nexus/ui/store.test.ts
```

**Step 4: Hand off for Paul review before commit**

Fable sends the exact diff and results. Paul verifies the change does not create browser polling or a second runtime authority.

## Task 6: Harden browser-authenticated WebSocket mutations

**Files:**

- Modify: `gateway/src/server/agui/ws.mjs`
- Modify: `gateway/src/server/agui/ws.d.mts`
- Modify: `gateway/src/server/agui/ws.test.ts`
- Modify: `gateway/src/server/agui/wsVerification.test.ts`
- Modify: `gateway/src/server/gateway/headless.test.ts`
- Modify: `gateway/src/app/gatewayClient.test.ts`

**Step 1: Write red auth matrix tests**

Cover `input`, `steer`, `compact`, command, and queue-control frames:

- valid browser cookie plus server-held CSRF succeeds;
- missing/mismatched CSRF rejects before enqueue;
- frame payload cannot self-assert a principal or CSRF token;
- bearer/non-cookie mode follows its explicit existing contract;
- remote browser mutation never loses credentials while being converted to an internal `Request`.

**Step 2: Centralize mutation request construction**

Derive the human principal and CSRF authority from the authenticated upgrade request/session. The internal request builder copies only server-resolved headers. Do not accept `frame.cookie`, `frame.authorization`, or `frame.csrf`.

**Step 3: Keep presentation and acceptance boundaries separate**

An `input.ack`/command receipt is sent only after the underlying REST/command ingress returns its durable acknowledgement. Authentication failure uses a typed frame and creates zero command intents.

**Step 4: Verify**

```bash
pnpm --dir gateway vitest run \
  src/server/agui/ws.test.ts \
  src/server/agui/wsVerification.test.ts \
  src/server/gateway/headless.test.ts \
  src/app/gatewayClient.test.ts
pnpm --dir gateway typecheck
```

**Step 5: Commit**

```bash
git add gateway/src/server/agui/ws.mjs gateway/src/server/agui/ws.d.mts \
  gateway/src/server/agui/ws.test.ts gateway/src/server/agui/wsVerification.test.ts \
  gateway/src/server/gateway/headless.test.ts gateway/src/app/gatewayClient.test.ts
git commit -m "fix: authenticate browser websocket mutations"
```

## Task 7: Prove agent-session materialization and reconnect semantics

**Files:**

- Modify: `gateway/src/routes/api/agui.observe.ts`
- Modify: `gateway/src/routes/api/agui.observe.test.ts`
- Modify: `gateway/src/server/agui/agentSessionProjection.ts`
- Modify: `gateway/src/server/agui/agentSessionProjection.test.ts`
- Modify: `gateway/src/server/agui/daemonPushRelay.mjs`
- Modify: `gateway/src/server/agui/daemonPushRelay.test.ts`
- Modify: `gateway/src/server/agui/ws.test.ts`

**Step 1: Add red restored-lane tests**

Seed a replayed runtime and verify name, agent ID, and session ID all resolve to the same canonical lane. A stale name paired with a valid agent ID must use the ID. Two lanes may not cross-route.

**Step 2: Add reconnect/epoch tests**

Cover:

- initial snapshot with non-null canonical session ID;
- reconnect after accepted `afterId` with no duplicate frame;
- daemon boot replacement invalidating a stale stream epoch;
- typed resync/gap followed by canonical re-observe;
- bounded source pause/resume and `1013` slow-reader close carrying last accepted cursor.

**Step 3: Implement only the missing behavior**

Reuse the existing shared daemon connector and stream projection. Do not add per-browser daemon connections or fall back to Gateway DB polling for live agent activity.

**Step 4: Verify**

```bash
pnpm --dir gateway vitest run \
  src/routes/api/agui.observe.test.ts \
  src/server/agui/agentSessionProjection.test.ts \
  src/server/agui/daemonPushRelay.test.ts \
  src/server/agui/ws.test.ts
```

**Step 5: Commit**

```bash
git add gateway/src/routes/api/agui.observe.ts \
  gateway/src/routes/api/agui.observe.test.ts \
  gateway/src/server/agui/agentSessionProjection.ts \
  gateway/src/server/agui/agentSessionProjection.test.ts \
  gateway/src/server/agui/daemonPushRelay.mjs \
  gateway/src/server/agui/daemonPushRelay.test.ts \
  gateway/src/server/agui/ws.test.ts
git commit -m "fix: resume canonical agent session streams"
```

## Task 8: Repair Webconsole orphan discovery without crash loops

**Files:**

- Modify: `core/crates/nexus/src/webconsole_lifecycle.rs`
- Modify: `core/crates/nexus/tests/webconsole_lifecycle.rs`
- Modify: `core/crates/nexus/tests/unit/webconsole_lifecycle.rs`
- Modify: `docs/cli.md`

**Step 1: Write the external process regression**

Start a fixture Webconsole, delete its discovery file while it still serves `/health`, then invoke start and launch. Assert one of two deterministic contracts:

1. the process is identified as the installed Webconsole, adopted, and discovery is atomically recreated; or
2. the command fails immediately with typed `WEBCONSOLE_PORT_OCCUPIED`.

It must never spawn a second child or wait for an `EADDRINUSE` timeout.

**Step 2: Implement safe probe/adoption**

On missing discovery, probe the configured loopback port. Adoption requires both a healthy Nexus Webconsole marker response and a matching process executable. A merely occupied port returns the typed error.

**Step 3: Verify**

```bash
cargo test --manifest-path core/Cargo.toml -p egregore-nexus \
  --test webconsole_lifecycle
cargo test --manifest-path core/Cargo.toml -p egregore-nexus \
  --test app_unit webconsole_lifecycle
```

**Step 4: Commit**

```bash
git add core/crates/nexus/src/webconsole_lifecycle.rs \
  core/crates/nexus/tests/webconsole_lifecycle.rs \
  core/crates/nexus/tests/unit/webconsole_lifecycle.rs docs/cli.md
git commit -m "fix: recover live webconsole discovery"
```

## Task 9: Close the packed-Gateway WebSocket module graph

**Owner:** Bob, reviewed/integrated by Paul.

**Files:**

- Modify: `gateway/package.json`
- Modify: `gateway/scripts/gateway-serve-impl.mjs`
- Modify: `gateway/src/server/gateway/headless.ts`
- Modify: `gateway/src/server/gateway/gatewayPackageContract.test.ts`
- Modify: `gateway/src/server/agui/plainNodeServe.test.ts`
- Create: `gateway/src/server/gateway/packedWsSmoke.test.ts`

**Step 1: Write a red clean-tarball test**

Pack `@egregore/nexus-gateway`, install it into a temporary empty prefix, change cwd outside the repo, start the API-only Gateway, and exercise:

- `/api/v1/health`;
- a real event-lane upgrade and `ping`/`pong`;
- a thread-topic `subscribe.ack` using the bundled Gateway canonical source;
- an agent-session upgrade against a fixture materializer; and
- import of every dynamic module reachable from the WS entrypoint.

**Step 2: Make the bundle own WS attachment**

Export one bundled helper from `dist-gateway/headless.mjs` that attaches the WebSocket with the canonical developer source. `gateway-serve-impl.mjs` imports that helper from the bundle. It must not import `../src/server/agui/ws.mjs`.

**Step 3: Verify package contents and clean-cwd execution**

```bash
pnpm --dir gateway vitest run \
  src/server/gateway/gatewayPackageContract.test.ts \
  src/server/agui/plainNodeServe.test.ts \
  src/server/gateway/packedWsSmoke.test.ts
pnpm --dir gateway build:gateway-package
pnpm --dir gateway pack --pack-destination /tmp/nexus-v015-pack
```

**Step 4: Send tested diff to Paul**

Paul checks for source fallbacks, version drift, extra package surfaces, and overlap before commit.

## Task 10: Build the human-path Lens affordance and ocular suite

**Owner:** Fable, reviewed/integrated by Paul.

**Files (Lens repository; final names may extend this list but tests remain external):**

- Create: `docs/testing/nexus-electron-affordance-matrix.md`
- Modify: `tests/ocular/run.mjs`
- Modify: `tests/ocular/cdp.mjs`
- Modify: `tests/ocular/helpers.mjs`
- Modify: `tests/ocular/helpers.test.mjs`
- Modify: `vitest.config.ts`
- Add PACTBIN2 fixtures beneath: `tests/ocular/fixtures/nexus-v015/`

**Step 1: Inventory every agent/WS-fed surface**

The matrix must include dashboard/roster, agents, thread/DM lists, conversation history, composer, progressive output, tool/status panes, queue controls, steer, interrupt, compact, attach/session view, notifications, errors/offline/reconnect, settings that affect these surfaces, navigation, reload, and persistence.

Each row includes: stable ID, preconditions, visible Electron path, human input, expected visible output, network/WS evidence, recovery path, artifact list, and release severity.

**Step 2: Enforce human interaction**

The driver in `cdp.mjs` exposes only CDP mouse, keyboard, focus, typing, scroll, visible selector, navigation, screenshot, and reload operations. It must not call React stores, plugin methods, bridge commands, or hidden app functions. Static tests reject `window.__*`, store imports, direct bridge invocation, and renderer `evaluate` that mutates application state.

**Step 3: Implement O1–O16 plus mechanism checks**

O3a/O4a capture timestamped browser WS frames and correlate them to the external Nexus action. Backend evidence must show an exact change-bus wake and zero timer-driven developer-event reads.

All tests collect pre/post screenshots, DOM snapshot, allowlisted console report, WS frames, health, process logs, and PACT files touched. Failure collection is automatic.

**Step 4: Add every remaining affordance case**

No agent/WS-fed surface may remain unlisted. A missing automation is emitted as a blocking `coverage_gap`, not silently skipped.

**Step 5: Verify runner contracts without launching the host app**

```bash
cd /home/earldennison/Projects/egregore-lens
pnpm vitest run tests/ocular/helpers.test.mjs
node tests/ocular/run.mjs --help
```

**Step 6: Send tested diff to Paul**

The actual Electron run waits for Task 11's isolated lab.

## Task 11: Extend the sole Docker validator into the Lens lab

**Files:**

- Modify: `tools/docker/nexus-test.Dockerfile`
- Modify: `scripts/nexus-docker-test-env`
- Create: `scripts/test-nexus-docker-test-env`
- Create: `scripts/nexus-v015-lab-manifest`
- Create: `scripts/test-nexus-v015-lab-manifest`

**Step 1: Write failing lifecycle contract tests**

Test `--help`, command routing, default resource values, mount rejection, and no-side-effect parsing. Required commands:

```text
lab-up
lab-status
lab-gate
lab-evidence
lab-down
```

Expected defaults: six CPUs, 8 GiB RAM, 1,024 PIDs, Gateway host port 4551, CDP host port 4553.

**Step 2: Add container-native Electron dependencies**

Install Xvfb and the Debian libraries required by Electron/Chromium. Keep node modules, Python venv, Electron binary, Pactree native modules, and pactree-fs inside container-local volumes/directories.

**Step 3: Add source mounts without host runtime state**

Mount Nexus, Lens, Egregore, Pactree, Pactree Python, and pactree-fs worktrees. Reject any mount whose source resolves to host `~/.nexus` or a host Lens data directory.

**Step 4: Start the process topology**

Use:

```text
NEXUS_HOME=/tmp/nexus-lens-home/nexus
LENS_DATA_HOME=/tmp/nexus-lens-home/lens-data
LENS_NEXUS_GATEWAY_URL=http://127.0.0.1:4100
DISPLAY=:99
Xvfb :99 -screen 0 1920x1080x24
Electron --no-sandbox with software GL
socat TCP-LISTEN:9223,fork,reuseaddr TCP:127.0.0.1:9222
```

Burn-down mode uses Lens Vite on 1422. Release mode uses the built renderer and packed Nexus artifacts.

**Step 5: Record manifest and evidence roots**

`nexus-v015-lab-manifest` records image/package/source SHAs, dirty path hashes, toolchains, redacted env, limits, and port map under `/tmp/nexus-lens-home/evidence/<run-id>/manifest.json`.

**Step 6: Verify lifecycle contracts**

```bash
scripts/test-nexus-docker-test-env
scripts/test-nexus-v015-lab-manifest
scripts/nexus-docker-test-env --help
```

**Step 7: Recreate the existing sole validator once**

Capture the host-isolation `before` snapshot, then run `lab-up`. Do not create a second named container. Assert:

```bash
docker ps --filter label=egregore-nexus-test=true --format '{{.Names}}' | wc -l
# expected: 1
```

**Step 8: Commit**

```bash
git add tools/docker/nexus-test.Dockerfile scripts/nexus-docker-test-env \
  scripts/test-nexus-docker-test-env scripts/nexus-v015-lab-manifest \
  scripts/test-nexus-v015-lab-manifest
git commit -m "test: add isolated Nexus Lens validation lab"
```

## Task 12: Add the real-network dual-WebSocket acceptance gate

**Files:**

- Create: `scripts/nexus-v015-ws-acceptance`
- Create: `scripts/test-nexus-v015-ws-acceptance`
- Create: `scripts/fixtures/v015-ws/README.md`
- Modify: `scripts/nexus-docker-test-env`
- Modify: `docs/release-regression.md`

**Step 1: Write failing CLI contract tests**

The script supports `--help`, `--run-id`, `--mode source|packed`, and `--evidence-dir`. `--help` is side-effect-free.

**Step 2: Implement real-process phases**

Run, in order:

1. packed install/import/route smoke;
2. seed Bob/Paul/Fable identities and runtimes;
3. daemon-only restart twice while Gateway remains;
4. Gateway-only restart twice;
5. combined restart;
6. persistent-cookie first post after boot replacement;
7. thread and DM subscribe/ack/event/reconnect/gap;
8. restored agent-session observe/input/steer/queue/interrupt/reconnect;
9. two concurrent lanes and topic isolation;
10. forced bounded backpressure -> `1013` -> cursor resume without loss/duplicate.

Every phase has a bounded timeout and writes redacted frames plus canonical REST/projection snapshots.

**Step 3: Prove no polling**

For thread/DM tests, record source diagnostics:

```json
{
  "initialReads": 1,
  "committedWakeReads": 1,
  "timerReads": 0,
  "events": 1
}
```

**Step 4: Verify fixture/script contracts**

```bash
scripts/test-nexus-v015-ws-acceptance
scripts/nexus-v015-ws-acceptance --help
```

**Step 5: Run source mode in the lab**

```bash
scripts/nexus-docker-test-env lab-gate --backend-only --mode source
```

**Step 6: Commit**

```bash
git add scripts/nexus-v015-ws-acceptance \
  scripts/test-nexus-v015-ws-acceptance scripts/fixtures/v015-ws \
  scripts/nexus-docker-test-env docs/release-regression.md
git commit -m "test: gate both websocket lanes over real processes"
```

## Task 13: Run the complete Electron ocular gate

**Repositories:** Nexus and Lens; no production edit is allowed during a release-evidence run.

**Step 1: Freeze the source candidate**

Record both repository SHAs and dirty manifests. Any source change invalidates the run and requires a new run ID.

**Step 2: Launch Lens inside the validator**

```bash
scripts/nexus-docker-test-env lab-up
scripts/nexus-docker-test-env lab-status
```

Verify Xvfb, Electron, CDP forward, daemon, Gateway, and built/source mode are all reported independently.

**Step 3: Run O1–O16 and the complete affordance inventory**

```bash
scripts/nexus-docker-test-env lab-gate --ocular --mode source
```

Every Lens-origin action must use the human CDP driver. Backend or renderer-internal mutation is a test failure.

**Step 4: Review evidence mechanically**

Fail on:

- missing case/artifact;
- `coverage_gap`;
- uncaught renderer error outside the graphics/dbus allowlist;
- invisible `subscribe.err`/gap;
- refresh required for a live update;
- timer-driven developer-event read;
- duplicate/missing bubble;
- host-isolation drift.

**Step 5: Run the packed/built replay**

```bash
scripts/nexus-docker-test-env lab-gate --all --mode packed
```

The packed replay uses empty install prefixes and a built Lens renderer.

## Task 14: Run the 30-minute dual-lane restart soak and release gates

**Files:**

- Create: `scripts/nexus-v015-ws-soak`
- Create: `scripts/test-nexus-v015-ws-soak`
- Modify: `scripts/nexus-docker-test-env`
- Modify: `docs/release-regression.md`

**Step 1: Add the soak script contract**

The default duration is 1,800 seconds. It accepts a shorter explicit duration only for script tests. It emits no periodic host Nexus messages; it writes samples to evidence and one terminal result.

**Step 2: Exercise both lanes continuously**

Keep at least one thread/DM subscription and two agent-session subscriptions active. Generate bounded human-like traffic through Lens. Restart Gateway once at minute 8 and daemon once at minute 18 inside the container.

Sample every 30 seconds:

- RSS/CPU;
- FDs/tasks/sockets;
- subscriber/reader counts;
- canonical cursors;
- queue depth/backpressure;
- Lens renderer responsiveness;
- duplicate/loss counters.

**Step 3: Define failure thresholds**

Fail immediately on silent loss, duplicate durable message, cross-lane event, unrecovered disconnect, dead-letter row, host drift, process crash, or unbounded monotonic resource growth confirmed across consecutive samples.

**Step 4: Verify script tests**

```bash
scripts/test-nexus-v015-ws-soak
scripts/nexus-v015-ws-soak --help
```

**Step 5: Run the exact packed candidate**

```bash
scripts/nexus-docker-test-env lab-gate --soak 1800 --mode packed
```

**Step 6: Run repository gates**

```bash
scripts/check core-test-layout
scripts/check release-identity
scripts/check architecture
scripts/check boundaries
scripts/check rust-workspace
pnpm --dir gateway typecheck
pnpm --dir gateway test
pnpm --dir gateway build
pnpm --dir gateway check:contracts
```

Run the relevant Lens unit/Python suites recorded by Fable, then the packed Gateway smoke again.

**Step 7: Obtain Bob's evidence review**

Bob reviews:

- split-store and stable-ID boundaries;
- human rebind/auth/CSRF;
- cursor/gap/backpressure behavior;
- packed module closure;
- host isolation;
- ocular completeness;
- resource trend and no-loss evidence.

No release-ready claim is made until the exact packed candidate, complete ocular matrix, and soak evidence all pass.

**Step 8: Commit gate wiring**

```bash
git add scripts/nexus-v015-ws-soak scripts/test-nexus-v015-ws-soak \
  scripts/nexus-docker-test-env docs/release-regression.md
git commit -m "test: add v0.1.5 websocket restart soak"
```

## Final evidence checklist

This checklist is an implementation aid. The normative ship rule is
[`docs/v0.1.5-acceptance.md`](../../v0.1.5-acceptance.md); any difference is resolved in favor of
that document.

- [ ] Host daemon/Gateway/Webconsole PID, boot ID, executable hash, listener owner, and socket inode unchanged.
- [ ] Exactly one validator container, capped at six CPUs, 8 GiB, and 1,024 PIDs.
- [ ] Full restored descriptors and exact roster reconciliation after two daemon restarts.
- [ ] Persistent human cookie rebinds before first accepted write in each new daemon boot.
- [ ] Thread/DM developer events originate from Gateway post-commit wake plus canonical rows.
- [ ] Zero timer-driven developer-event reads in O3a/O4a and backend acceptance.
- [ ] Agent-session stream has canonical non-null session IDs and lossless cursor reconnect.
- [ ] Cookie-authenticated WS mutations pass centralized CSRF; forged frame auth is rejected.
- [ ] Packed Gateway imports and serves both WS surfaces outside the source checkout.
- [ ] Webconsole missing-discovery case cannot spawn an `EADDRINUSE` loop.
- [ ] The packed `nexus webconsole` command passes help, start, status, url, launch, logs, restart,
      and stop parameter/lifecycle checks, including idempotency and discovery adoption.
- [ ] Webconsole thread, DM, queue, and control mutations remain attributed to the human browser
      principal; the target agent never replaces the caller.
- [ ] Every agent/WS-fed Lens surface is inventoried and exercised through human Electron interaction.
- [ ] O1–O18, W1–W8, and every additional affordance test pass with artifacts.
- [ ] PACTBIN2 persistence and damage states are visible and recoverable.
- [ ] 30-minute dual-lane restart soak passes without loss, duplicates, cross-route, or unbounded growth.
- [ ] Full Rust/Gateway/Lens/package gates are green for the exact candidate.
- [ ] Bob's review has no unresolved release blocker.
