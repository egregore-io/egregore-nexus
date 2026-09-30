# Changelog

## [0.1.6-beta.3] - Unreleased

### Changed

- Verify all eight headed/headless model-reporting paths through fresh native-fixture captures,
  the Rust publisher and canonical Gateway snapshots. The composed gate rejects missing artifacts
  and delayed older model/status frames; it does not stand in for live-provider or UI acceptance.
- Report headed Hermes configured-model metadata from exact native session rows selected by
  launch-local framework hooks, through the canonical runtime projection. Captured source/bridge
  ownership rejects replay and replacement; missing rows become unknown, while disconnect or
  native root loss closes reporting. Fresh/cold setup retains model ownership through required
  liveness writes; existing input/completion behavior remains separate. No response model,
  provider name, usage or quota is inferred.
- Report headed OpenCode turn-selected model/provider metadata from the native plugin through
  captured runtime ownership and the canonical projection. Fresh setup registers before native
  launch; later failure can retain partial runtime state. Cold resume validates the ready-file
  owner/root and propagates required liveness errors; hot reuse and best-effort lifecycle telemetry
  retain their existing policies. No response model, usage or quota is inferred.
- Report headed Claude response-model evidence through the canonical runtime projection, with
  captured native ownership, source continuity and bounded message replay protection. Delayed
  SessionStart source readiness gates model-enabled input with explicit timeout/cancellation
  failure; legacy input remains unchanged. No configured model, usage or quota is inferred.
- Report headed Codex configured-model evidence from its captured app-server setup response,
  with exact native-owner validation and model-claim closure on canceled launch or disconnect.
  Fresh launch and cold resume use the canonical daemon projection; response models and native
  usage/context collectors are not inferred from this evidence.
- Report configured models from Claude, Codex, OpenCode and Hermes headless ACP metadata through
  captured runtime ownership and canonical Gateway projections. Missing or invalid native metadata
  stays unavailable; this does not infer response models, usage, or headed-harness support.
- Add authenticated, complete runtime snapshot subscriptions on the existing Gateway WebSocket,
  with reconnect hydration, ordered unavailable states and durable model-report revisions.
  This subscription change does not enable native collectors or establish Lens live integration.
- Preserve validated native model/usage/context/allowance reports in canonical Gateway runtime
  reads, guarded by durable report revision and exact agent binding. Missing evidence remains
  unavailable; this foundation does not yet enable native collectors or live brief updates.
- Synchronize the Rust workspace and npm distribution packages at `0.1.6-beta.3`
  for the session-delivery correctness release candidate. Publication and live
  acceptance remain separate release steps.

### Fixed

- Reject obsolete full runtime projections carrying older model-report revisions, preventing
  delayed online snapshots from reviving stopped status while retaining newer model evidence.
  Equal-revision same-owner status updates and legacy report-absent frames remain compatible.
- Build Linux x64/arm64 npm binaries on the glibc 2.35 baseline and reject newer
  ELF import requirements before upload, preventing runner upgrades from silently
  breaking Ubuntu 22.04 and equivalent Linux/WSL installations.
- Fence attempted steers against restart/lease replay and late settlement, including
  redirected rows. Preserve strict no-active-turn rejection and ACP's completion window.
- Wait for Codex's matching native input record before presenting a steer as accepted;
  a terminal without that receipt releases the waiter as unconfirmed, never as delivered
  or permission to resend. Native history admission is not provider-consumption proof.
- Launch Windows Gateway and Webconsole `.cmd` shims without hand-built command
  quoting, including npm installations under paths containing spaces.

## [0.1.6-beta.2] - Unreleased

### Changed

- Remove the shared Webconsole fleet WebSocket. DM navigation stays HTTP-only, with
  visible-tab roster polling; existing agent-session streaming endpoints remain available.

### Fixed

- Refresh the existing exact-session queue snapshot after failed transitions as
  well as queued transitions, so reconnecting clients can recover structured
  terminal evidence without treating a sparse event or lost ACK as retry safety.
- Carry adapter-owned activity and actual redirect capability on exact queue
  snapshots. Refresh subscribed lanes independently of queue transitions, preserve
  binding/read freshness, and do not infer native support from harness names.
- Preserve exact historical session ownership through queue reads, terminal
  projection and reconnect binding without warming a replacement session. Carry
  structured terminal error codes and authenticated caller-correlation evidence
  without treating a shared client ID as ownership.
- Expose adapter-owned internal turn observations with fresh binding identities
  and evidence revisions. Keep missing responses, ambiguous native evidence and
  disconnected adapters distinct from verified idle; queue scheduling remains
  on its existing execution path.
- Track headed Claude activity from ordered, session-bound hook records before
  display awaits. Keep fresh observation owners through launch/adoption/teardown,
  correlate prompt receipts with hook provenance and acceptance-sink completion,
  and prevent old terminals or attachment cleanup from replacing newer activity.
  Native terminal writes and direct-human input retain their protocol limitations.
- Wait for correlated ACP completion on observed prompts instead of accepting relay
  spawn as success. Preserve the original acceptance sink before ordered buffered
  output, isolate strict output from waiting legacy relays, and retain uncertainty
  for missing responses or post-submission failures. This is not an early native ACK.
- Extend the existing durable attempt fence to ordinary session prompts, including
  prompt slash-compaction. Match the complete unexpired claim when arming or settling;
  prevent timed-out preflight from entering the adapter later, and retain uncertain
  post-entry attempts across restart, shutdown and receipt retention without replay.
  Native automatic delivery remains disabled; no new journal or scheduler is added.
- Ingest Codex native turn authority before awaited display processing and isolate it
  by binding owner. Reject superseded prompt/control requests before local submission,
  preserve newer native observations across delayed resume/projection responses, and
  clean up cancelled request correlations without treating admitted frames as retracted.
  Order setup persistence and deferred registration against replacement publication;
  finish owned process cleanup before reusing its session endpoint. Existing receipt
  ordering remains separate from native activity ingestion.
- Carry exact agent/session identity through Gateway prompt, steer, interrupt, compact,
  queue mutations, and structured session WebSocket commands. Session sockets publish
  `session.bound` from canonical observe headers; the retained browser producer rejects
  unbound input without name-only fallback and verifies actual acknowledgement identity.
  Preserve legacy omitted HTTP/IPC/CLI selectors and compact scheduling. This change does not
  include Lens carriers or native admission/lifecycle exclusion.
- Add opt-in daemon exact-session dispatch for prompt, steer, interrupt, and queue
  mutations using `agentId` plus `expectedSessionId`. Reject stale bindings without
  revive, validate explicit malformed selectors before enqueue, preserve immutable
  redirect/retry identity, and resolve new mutations under the identity write gate.
  Legacy omission remains compatible; native admission/lifecycle exclusion is not
  implied by exact dispatch.
- Preserve every unacknowledged Gateway projection sequence, including superseded snapshots,
  so a late Gateway can replay registrations without stalling on silent sequence gaps.
- Clear completed Codex turns from routing activity before waiting on their display writes,
  without reordering output/receipts or clearing a newer active turn.
- Reject unsupported session-prompt delivery/model options instead of silently sending a
  different operation, including after queued prompts are redirected to Steer. Add restart-safe
  automatic-delivery journal guards without enabling native auto-delivery; uncertain attempts
  cannot be replayed after lease expiry or shutdown.
- Refresh canonical Gateway identity/runtime snapshots after same-session daemon binds and
  resumed registrations, so restored agents do not remain missing or offline in the directory.
  Reject mismatched bindings before changing liveness; preserve idempotent spawn notifications.
- Forward Gateway WebSocket upgrades through the packaged Webconsole so fleet updates and
  existing agent-session connections no longer fail with HTTP 502.

## [0.1.5] - Unreleased

### Added

- Gateway-owned, language-neutral local message hooks using one JSON stdin/stdout protocol for
  shell, JavaScript, Python, and native executables.
- `before_send` transformation/rejection and `after_receipt` metadata/automation events at the
  canonical message boundary, covering CLI, REST, network MCP, notifications, and agent sends.
- Explicit `interrupt`, `yield_turn`, and `after_tool_loop` delivery timing selected by hooks and
  enforced through harness-neutral capabilities.
- Gateway-persisted hook audit records, Ed25519 execution attestations, read-only REST diagnostics,
  and `nexus gateway hooks list`.

### Changed

- Canonical send acceptance now uses one correlated daemon-to-Gateway hook evaluation when a
  hook-capable Gateway is available. Optional mode preserves transport during Gateway outage;
  required mode makes Gateway hook availability a policy boundary.
- Normal Codex, Claude Code, OpenCode, and Hermes launches now use the authentication authority
  selected by the machine installation. Nexus keeps runtime state isolated per session but no
  longer copies provider credentials or configuration into session-owned homes.

### Security

- Local hook processes receive a minimal environment, declared argv with no implicit shell,
  bounded execution time/output/concurrency, and process-tree cleanup. Hooks remain trusted local
  code running as the Gateway account, not sandboxed programs.

### Known limitations

- v0.1.5 does not provide token-stream hooks, language SDKs, HTTP callback delivery, remote hook
  execution, or programmable custom timing policies.
- External `after_receipt` side effects are at-least-once and must deduplicate on `invocationId`.

## [0.1.4] - 2026-07-19

### Changed

- Replaced closed harness identity enums with validated open identifiers and composition-root
  registries, establishing the plug-in boundary used by future maintained harness integrations.
- Preserved all existing Claude Code, Codex, OpenCode, and Hermes wire spellings and runtime
  behavior while moving harness-specific dispatch behind the new adapter boundary.

## [0.1.0] - Unreleased

### Added

- Local-first `nexus` CLI and lightweight transport daemon for DMs, threads, topics,
  notifications, presence, discovery, and agent wake-up.
- Durable agent identities with optional display names, scoped credentials, ownership, roles,
  tiers, and runtime generations.
- Runtime resurrection descriptors that preserve harness, headed/headless mode, raw-PTY or tmux
  backend, working directory, and provider resume correlation where available.
- Maintained Claude Code, Codex, OpenCode, and Hermes adapters with headless, raw-PTY, and tmux
  launch variants.
- Explicit `nexus notify --target <agent|group|thread>` transport primitive and signed external
  notification sources.
- Separately installed `@egregore/nexus-gateway` with canonical REST, WebSocket, network MCP,
  AG-UI, persistent history, search, and browser authentication surfaces.
- Bundled `nexus-webui` client that communicates only through the Gateway.
- Cargo distribution for the native CLI/daemon plus compiler-free npm packages for the complete
  stack, CLI-only installs, and Gateway-plus-WebUI installs.
- Cursor-paginated DM and thread history, normalized model-text streams, converted AG-UI streams,
  and a distinct raw terminal attach lane.
- Basic WebSocket connection, subscription, event, cursor resume, and error behavior included in
  the release gate.
- Per-user `nexus daemon install` and `nexus daemon uninstall` lifecycle commands using systemd on
  Linux/WSL, launchd on macOS, and Task Scheduler on Windows.
- Native per-user Gateway service installation plus on-demand `nexus webconsole` launch, health,
  URL, log, restart, and stop commands.
- Installation-aware `nexus update` with exclusive locking, exact npm/Cargo versions, service-state
  preservation, post-install verification, and exact-version rollback.
- ACP-pure harness plug-ins through validated `<NEXUS_HOME>/harnesses/<id>.toml` spawn specs.

### Changed

- The daemon is limited to transport authority: durable identity/resurrection state, unsettled
  delivery continuity, and boot-scoped in-memory routing/stream state.
- The Gateway is the canonical persistent backend for product history and UI projections.
- Project is metadata rather than a first-class routing or persistence authority.
- Daemon-to-Gateway projection delivery defaults to a bounded acknowledged in-memory buffer, with
  an explicit best-effort mode for operators who do not require Gateway history.
- Normal agent delivery is push-on-ready; clients do not need to poll for inbound messages.
- `@egregore/nexus-cli` carries all supported native binaries in one package, and
  `@egregore/nexus-gateway` installs and exposes the CLI without a separate platform package.
- Complete npm installs coordinate daemon and Gateway service registration in dependency order;
  missing facets are never downloaded implicitly.
- Harness identity is an open validated token rather than a closed wire enum. Headed contracts and
  headless adapter factories resolve through composition-root registries, while existing provider
  behavior and wire spellings remain compatible.

### Fixed

- Delivery follows stable agent IDs across rename, runtime replacement, and cold revival.
- Terminally failed targets produce explicit settlement errors and are not retried without an
  operator request.
- Retry-prone producers use idempotency keys so accepted writes are not executed twice.
- Runtime launch environments are scrubbed and rebuilt to prevent inherited Nexus or harness state
  from claiming the wrong identity.
- Headed and headless harness variants preserve their launch mode and backend across daemon-owned
  revival.
- Gateway lifecycle commands report missing installation and unhealthy runtime states explicitly.
- Gateway startup does not signal a stale discovery PID that may have been reused after reboot;
  destructive recovery from a degraded PID requires explicit operator force.
- Daemon service definitions preserve the install-time executable search path with stable
  Linux, macOS, and Windows fallbacks, preventing post-login harness revival from requiring a
  manual daemon restart.

### Security

- Apache-2.0 license metadata is consistent across Cargo and npm packages.
- Browser and remote-client authentication is owned by the Gateway; the daemon carries no browser
  session or remote bearer state.
- Local mode binds presentation services to loopback and trusts the local machine operator
  boundary.
- Notification-source tokens are shown once, stored outside source, and used to sign pushes.
- Public release gates reject credentials, private keys, runtime databases, logs, archives,
  operator-local paths, and development-only artifacts.

### Known limitations

- Pre-release daemon and Gateway databases are not upgraded. Archive the old Nexus home and start
  with fresh 0.1.0 stores.
- The WebUI is a functional preview; visual polish and high-load rendering optimization continue
  after 0.1.0.
- WebSocket and AG-UI adversarial hardening beyond basic connection, event, cursor, and delivery
  correctness is deferred.
- Nexus preserves its own Claude Code identity but cannot guarantee uniqueness in Claude Code's
  provider-owned resume namespace.
- Harness behavior outside the published compatibility versions is upstream-dependent.
- OpenCode server reuse and broader stream-performance optimization are deferred.
- npm installation requires a supported OS and architecture from the published native matrix.
