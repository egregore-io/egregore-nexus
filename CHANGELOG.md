# Changelog

All notable public changes to Egregore Nexus are documented here. This project follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and uses semantic versioning.

## [0.1.6-beta.2] - Unreleased

### Changed

- Remove the shared Webconsole fleet WebSocket. DM navigation stays HTTP-only, with
  visible-tab roster polling; existing agent-session streaming endpoints remain available.

### Fixed

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
  Preserve legacy omitted HTTP/IPC/CLI selectors and compact scheduling; Lens carriers
  and native admission/lifecycle exclusion remain separate work.
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
