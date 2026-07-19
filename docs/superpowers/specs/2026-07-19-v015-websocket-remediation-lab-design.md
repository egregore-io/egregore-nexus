# v0.1.5 WebSocket Remediation and Nexus–Lens Validation Lab Design

**Status:** Approved direction. Earl delegated execution to Paul and instructed the team to use an isolated container without touching the host Nexus runtime.

## Goal

Ship v0.1.5 with both Gateway WebSocket surfaces behaving as durable, reconnectable product contracts:

1. the untargeted developer-event subscription lane used for fleet and thread live updates; and
2. the targeted agent-session lane used for AG-UI output, input, steer, queue control, and reconnect.

Validation must run through the real Lens Electron renderer in one isolated Docker environment. The host daemon, Gateway, Webconsole, socket, databases, and `~/.nexus` remain outside the test boundary.

## Decisions

- Extend the existing single Nexus validator. Do not create a second Nexus validation container.
- Run daemon, Gateway, optional Webconsole package smoke, Lens Vite/Electron, Xvfb, CDP forwarding, and test probes as separate processes inside that one container.
- Cap the lab at 6 CPUs, 8 GiB memory, and 1,024 PIDs.
- Burn-down mode bind-mounts the current Nexus and Lens worktrees and records their exact Git and dirty-state manifests.
- The final release replay installs packed candidate artifacts and runs the built Lens renderer so release evidence does not depend on a Vite development server or source-tree imports.
- Keep all state container-local. Repository mounts may expose source, but the host Nexus home is never mounted.
- Use PACTBIN2 only for forward Lens persistence and damage fixtures. PACTBIN1 compatibility is out of scope.
- Treat `project` as metadata. Identity, routing, session materialization, cursors, and resurrection resolve globally by stable IDs.

## Alternatives Considered

### Separate Compose services

Separate Nexus and Lens containers would make process ownership visually neat, but it would violate the one-validator constraint, consume more host resources, and add network/orchestration failure modes that are not part of the v0.1.5 product contract.

### Protocol-only simulator

A simulator is useful for deterministic frame and backpressure tests, but it cannot detect Electron rendering, stale composer state, queue-chip disappearance, plugin credential reuse, or reconnect behavior in Lens. It remains a lower-layer test helper, not the release environment.

## Lab Topology

The existing `scripts/nexus-docker-test-env` remains the single lifecycle owner. It gains Lens-aware commands rather than spawning ad hoc processes from multiple scripts.

```text
host (live runtime remains untouched)
  |
  | bind source only
  v
nexus-v010-load51 (6 CPU / 8 GiB / 1024 PIDs)
  /work/egregore-nexus       current Nexus worktree
  /work/egregore-lens        current Lens worktree
  /work/egregore-v2          Lens Python path dependency
  /work/pactree-py           PACTBIN2 Python binding worktree
  /work/pactree              PACTBIN2 engine worktree
  /work/pactree-fs           pactree-fs worktree

  /tmp/nexus-lens-home/
    nexus/                   daemon/Gateway discovery and durable identity data
    gateway/                 canonical Gateway store
    lens-data/               Lens sessions, plugin storage, and PACTBIN2 files
    lens-venv/               container-built Python environment
    node/                    container-built Lens dependencies
    evidence/<run-id>/       manifest, frames, logs, screenshots, and reports

  daemon IPC/socket          container-local only
  Gateway internal           127.0.0.1:4101
  Gateway edge               0.0.0.0:4100 -> published host 127.0.0.1:4551
  Lens Vite burn-down        127.0.0.1:1422 (not published)
  Lens Electron CDP          127.0.0.1:9222
  CDP forward                0.0.0.0:9223 -> published host 127.0.0.1:4553
  Xvfb                       :99, 1920x1080x24
```

Lens receives `LENS_NEXUS_GATEWAY_URL=http://127.0.0.1:4100`, which deliberately outranks discovery. `NEXUS_HOME` and `LENS_DATA_HOME` both point beneath `/tmp/nexus-lens-home`; no path can fall through to the host home.

Electron runs with `--no-sandbox` and software rendering in the lab. CDP is exposed through a container-local `socat` forward because Electron binds DevTools to loopback. MESA, SwiftShader, and dbus warnings are allowlisted; uncaught renderer errors, crashes, and Nexus/Lens errors are not.

Node dependencies, Electron, the Lens Python environment, Pactree native modules, and pactree-fs are installed inside the container. Native artifacts are never reused from host `node_modules`, `.venv`, or site-packages.

## Source and Artifact Modes

### Burn-down mode

The two primary worktrees are bind-mounted read/write so Paul and Fable can iterate quickly. Each run records:

- container image ID and digest;
- Nexus, Lens, Egregore, Pactree, Pactree Python, and pactree-fs Git SHAs;
- dirty path lists and content hashes for dirty files;
- candidate package versions and packed tarball hashes;
- toolchain versions; and
- a redacted environment inventory.

### Release replay mode

The lab installs packed `@egregore/nexus`, `@egregore/nexus-cli`, and `@egregore/nexus-gateway` candidates into an empty prefix. The Gateway must start without importing from the source tree. Lens uses a built renderer instead of Vite. The same network and ocular gates then replay against those artifacts.

## Remediation Boundaries

### Current live-update defect

Lens already uses a WebSocket-shaped subscription chain, but thread and DM developer topics are
not push-driven end to end. The packaged Gateway currently schedules
`developerEventsSource.since(topic, cursor)` every 250 milliseconds for generic `sys.*` topics.
Only fleet status and tool-call topics use the daemon push socket. That timer loop leaves Lens
dependent on a stale Gateway projection and explains why a thread can be readable after refresh
while new posts do not appear live.

v0.1.5 removes that split behavior at the authority that owns each fact. Fleet status and ephemeral
tool calls remain daemon-push topics. Thread and DM message topics subscribe to the Gateway's
canonical post-commit change bus and read the matching `bus_messages` rows after a durable cursor.
There is no daemon `local.store.read` and no periodic `since()` timer in the live path. REST remains
a single bounded hydration/resync surface, never the liveness mechanism.

### 1. Restored runtime projection

At daemon boot, restored agents must emit complete identity and runtime descriptors sourced from identity authority, not the in-memory transport store. Payloads include stable agent ID, current name, owner/kind/tier, harness, runtime/session ID, transport, presence, and resurrection capsule fields.

Gateway treats a daemon boot epoch change as reconciliation:

- previously active runtime rows are retired before current replay is materialized;
- current restored rows are upserted from full descriptors;
- stale pre-boot identities may remain as audit records, but cannot appear online; and
- the canonical roster converges exactly to current daemon truth.

Agent-session targets resolve by stable agent ID/session ID first. A name is a mutable alias and display field, never internal identity authority. A valid stable ID must tolerate stale name metadata and project metadata differences.

### 2. Persistent human principal rebind

Gateway owns persistent browser login state; daemon registration is boot-local. Before Gateway accepts a human mutation after a daemon boot change, it must:

1. read the daemon boot ID;
2. re-register the stable human client key using a singleflight keyed by Nexus home, client key, and boot ID;
3. replace stale session/runtime fields with the daemon response; and
4. only then durably enqueue the requested command.

The same boot reuses the binding. A later boot rebinds once. Unknown cookies remain `401`. An unavailable daemon or boot manifest returns `503` and creates no accepted command. Registration is idempotent and must not create duplicate humans.

### 3. Developer-event subscription WebSocket

The untargeted `/api/agui/ws` socket accepts typed `subscribe` frames. It serves each topic from its
canonical committed authority:

- fleet status and ephemeral tool-call observations come from the daemon push lane; and
- thread and DM message events come from the Gateway canonical message store plus its post-commit
  change bus.

Both source types expose the same browser contract with:

- an explicit `subscribe.ack`;
- per-topic monotonic sequence cursors;
- topic isolation;
- bounded replay after `afterSeq`;
- boot-aware cursor reset/gap signaling;
- duplicate suppression; and
- bounded slow-consumer behavior with a typed close/gap and resumable cursor.

For message topics, the Gateway subscribes to the exact `gatewayChangeBus` key before its first
read, then reads canonical `bus_messages` rows after the browser cursor. The change bus is only a
wake signal: committed Gateway rows and their cursor remain the source of truth. A missed wake,
duplicate projection, reconnect, or daemon replacement therefore causes a bounded catch-up read,
not silent loss. A process registry shares one canonical reader per exact target across concurrent
browser subscribers.

`sys.message.thread.<name>` maps to the canonical thread target and `sys.dm.<name>` maps to the
canonical DM viewpoint. Unknown durable topic shapes fail with a typed `subscribe.err`; they never
fall back to daemon database reads. Fleet and tool-call topics keep their existing daemon-push
mapping and explicit boot-scoped resync behavior.

Lens performs one bounded REST hydration, then listens for event deltas. A committed thread message wakes the exact topic subscription and causes a bounded history refresh. Live behavior must never depend on periodic browser polling or manual refresh.

The lab exposes a test-only observation counter/log for canonical catch-up reads. O3/O4 evidence
must show the pushed browser frame aligned with the originating post/spawn, a change-bus wake for
the exact key, and zero timer-driven reads during the observation window. Reads caused by initial
subscribe, a committed wake, explicit resync, or reconnect are separately labelled and bounded.

### 4. Agent-session WebSocket

The targeted `/api/agui/ws?session=...` socket resolves a canonical session and stays open only when its stream projection is materialized. Its initial snapshot carries a non-null current session ID.

Output ordering preserves AG-UI `RUN_STARTED`, text/tool deltas, terminal message boundaries, and `RUN_FINISHED`/`RUN_ERROR`. Reconnect uses `afterId` plus boot/session identity to replay exactly the accepted gap without duplicates.

Input, steer, interrupt, compact, and queue mutations use existing Gateway command ingress. Acknowledgement occurs only after the relevant durable boundary. Browser WebSocket frames never carry credentials; the server derives the authenticated human principal from the upgrade cookie. For internal mutation dispatch, the Gateway supplies the matching CSRF value from the authenticated cookie contract rather than trusting a client frame.

Busy-turn queueing, queue mutation, interrupt, and slow-reader behavior are bounded. Backpressure closes with a typed retryable status and preserves the last accepted cursor.

### 5. Packaged Gateway and Webconsole lifecycle

The packed Gateway must contain every module loaded by its WebSocket entrypoint. A clean tarball install must start REST and both WS lanes without a source checkout.

Webconsole lifecycle must not spawn blindly when discovery is missing but a healthy process already owns the configured port. It either adopts/recreates typed discovery for the known healthy process or fails immediately with a typed occupied-port diagnosis. It must not create an `EADDRINUSE` crash loop.

## Error Contract

Every failure is observable at the closest useful boundary:

- authentication and stale cookies: `401` before enqueue;
- daemon unavailable or boot rebind unavailable: `503` before enqueue;
- unsupported/unknown WS frame: `input.err` or typed command error while the socket remains usable;
- unavailable session materialization: typed close/error naming the missing canonical session;
- replay cursor outside retained history: typed gap requiring bounded resync;
- slow consumer: typed retryable close with the last accepted cursor;
- PACTBIN2 corruption: recoverable `CorruptFile` UI state, never blank Lens or process panic; and
- process/lab failures: nonzero gate plus a complete evidence bundle.

No compatibility path may bypass Gateway command ingress, browser auth, projection ownership, or the daemon’s stable-ID routing rules.

## Validation Layers

### Unit and integration

- full restored projection payloads from split identity/transport stores;
- boot-epoch stale-runtime retirement and replay materialization;
- agent-ID-first resolution with stale name and mixed project metadata;
- human rebind once per boot, concurrent singleflight, failure before acceptance;
- developer-event topic isolation, cursor replay, epoch gap, duplicate suppression, and backpressure;
- agent-session input acknowledgement, queue/steer/control routing, reconnect, and backpressure;
- browser cookie/CSRF propagation on WS mutations;
- Gateway package manifest/import closure; and
- Webconsole missing-discovery/live-owner lifecycle.

### Real-network container gates

Mocks cannot satisfy release readiness. The lab must exercise real HTTP upgrades and independently restart daemon and Gateway processes.

1. Host-isolation preflight proves the container has no host Nexus mount and records the unchanged host daemon boot ID, PID, socket inode, and listener ownership. Host database contents are not hashed because legitimate live bus traffic may update them during the gate.
2. Packed artifact install/start validates version `0.1.5`, REST inventory, event WS, and agent-session WS without source fallback.
3. Boot restoration validates exact roster convergence, stale runtime retirement, and session materialization across daemon-only, Gateway-only, and combined restarts.
4. Persistent-cookie validation sends the first thread post after daemon replacement and proves rebind-before-enqueue plus exactly-once attribution.
5. Event WS validation covers ack, live event, cursor replay, boot gap, reconnect, and slow consumer.
6. Agent-session WS validation covers canonical materialization, progressive stream, input, steer, queue, interrupt, reconnect, isolation between two sessions, and slow consumer.
7. A 30-minute dual-lane soak keeps both subscriptions active, restarts Gateway once and daemon once inside the container, samples memory/FD/task counts, and fails on silent loss, duplicates, unbounded growth, or unrecovered disconnect.

## Lens CDP Ocular Catalog

Every ocular case produces pre/post JPEG screenshots, a bounded DOM snapshot, console warnings/errors with the graphics/dbus allowlist applied, health probes, WS frame excerpts, and relevant daemon/Gateway/Lens log slices.

| ID | Scenario | Pass condition |
|---|---|---|
| O1 | Boot/hydrate | Composer attaches within 30 seconds; no error toast or uncaught renderer error. |
| O2 | Progressive stream | User bubble appears immediately; assistant text grows across at least three samples; turn reaches completed. |
| O3 | Injected turn | A daemon-side DM/thread injection renders within 10 seconds without reload or session-not-found error. |
| O3a | Injected-turn mechanism | Timestamped CDP capture contains the pushed developer-event frame, aligned to the originating post; the Gateway reports zero periodic developer-event reads in the window. |
| O4 | Live projection | A daemon-side session spawn updates dashboard/session list without reload. |
| O4a | Projection mechanism | Timestamped CDP capture contains the pushed lifecycle frame, aligned to the spawn; the Gateway reports zero periodic developer-event reads in the window. |
| O5 | Steer mid-turn | Steer appears at the legal delivery point and changes output; no premature optimistic bubble. |
| O6 | Queue drain | Send while busy shows a queued chip, drains once at completion, and becomes one real bubble. |
| O7 | Queue-wedge regression | Visible queue state always matches the backend queue snapshot. |
| O8 | Queued cancel | Cancel removes the chip and backend item; no ghost send occurs. |
| O9 | Interrupt/stop | Terminal event is handled, composer re-enables, and no phantom/duplicate bubble appears. |
| O10 | Reload during stream | Hydration/replay resumes with no missing or duplicate bubbles. |
| O11 | Gateway restart/reconnect | Offline surface appears, clears after health recovery, and live injection works again without app restart. |
| O12 | Daemon restart/rehydrate | Transcript count and logical text hash match before/after; PACTBIN2 is readable. |
| O13 | Truncated PACTBIN2 | Lens shows recoverable `CorruptFile`; no blank pane, crash, or panic. |
| O14 | Bit-flipped PACTBIN2 | Same recoverable behavior as O13. |
| O15 | Invalid caller credential | A visible typed error appears; the action is never silently dropped. |
| O16 | Daemon down/up | Composer goes offline/disabled and recovers without restarting Lens. |

All cases are required before the lab is declared green. O1–O7, O3a, O4a, O10–O12, O15, and O16 are immediate v0.1.5 WebSocket blockers; O8, O9, O13, and O14 may run later in the same gate but are not waivable.

### Complete Lens affordance contract

O1–O16 are the focused WebSocket and persistence matrix, not the ceiling of frontend coverage.
Fable also owns a versioned inventory of every user-facing Lens affordance and an executable CDP
contract for each item, including:

- dashboard, session list, navigation, search/filter, attach, and reconnect actions;
- transcript rendering, thread and DM posting, composer modes, keyboard submission, focus, and
  disabled/loading states;
- queue inspect/reorder/redirect/cancel, steer, interrupt, compact, and retry controls;
- settings, status indicators, health/offline/error recovery, notifications, and empty states;
- reload, process restart, persistence, and back/forward navigation behavior; and
- accessibility-relevant labels, focus order, keyboard reachability, and disabled-state semantics.

Each affordance entry names its preconditions, user/CDP actions, DOM and state assertions,
network/WebSocket mechanism assertions when applicable, recovery case, screenshots/logs, and
stable test ID. The complete suite runs in the same isolated lab and shares the evidence contract.
An affordance with no test must be explicitly recorded as missing coverage; it cannot silently
disappear from the inventory.

Lens-origin actions must enter through the rendered Electron UI using the same mouse, focus,
typing, keyboard, scroll, navigation, and reload interactions available to a human. Tests may
inspect DOM state, captured frames, process logs, and canonical backend state as evidence, but may
not call React stores, plugin functions, bridge methods, or hidden app internals to manufacture an
action or result. External Nexus actions may create inbound facts; the pass condition is still the
human-visible Electron response. Every surface that consumes agent data or either WebSocket lane
must have a complete visible input/output/recovery path in the inventory.

## Evidence and Reporting

Each run writes `/tmp/nexus-lens-home/evidence/<run-id>/` with:

- `manifest.json` — source/package/image/toolchain identity, redacted environment, resource limits;
- `host-isolation-before.json` and `host-isolation-after.json`;
- `backend-results.json` and `ocular-results.json`;
- `frames/` — bounded, redacted event and session WS transcripts;
- `screens/` — O1–O16 pre/post captures;
- `logs/` — bounded daemon, Gateway, Webconsole, Electron, Vite, and ocular logs;
- `projection/` — canonical roster/session/message snapshots; and
- `resource-samples.jsonl` — CPU, RSS, FDs, tasks, and socket counts during the soak.

The lifecycle script reports one terminal PASS/FAIL summary and the evidence path. It posts no periodic noise to the host bus. A final notification is emitted only after the entire gate settles.

## Ownership

- **Paul:** authoritative spec/plan, Nexus lifecycle and identity fixes, Gateway event/session WS correctness, lab lifecycle, backend/network gates, integration, and release decision.
- **Fable:** Lens/Xvfb/CDP launch contract, O1–O16 scripts and artifacts, the complete frontend affordance inventory and CDP contract suite, PACTBIN2 fixtures/damage assertions, and frontend test review.
- **Bob:** read-only review of split-store projection, auth/rebind, cursor/backpressure, packaging, host isolation, evidence completeness, and no-loss deployment.

No one installs or restarts the host runtime during implementation or validation. Any eventual host upgrade is a separate, explicitly approved deployment step after the container candidate is green.

## Exit Criteria

v0.1.5 is ready only when:

- focused and full Rust/Gateway/Lens suites are green;
- packed artifact installation and route/module inventory are green;
- every backend real-network gate is green;
- O1–O16 are green with evidence;
- every inventoried Lens affordance has a passing CDP contract or an explicit release-blocking gap;
- the 30-minute dual-lane restart soak is green;
- Bob signs off on boundary and evidence review; and
- host-isolation evidence proves the live host runtime was not touched.
