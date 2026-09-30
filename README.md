# Egregore Nexus

Nexus is a local-first message and notification transport for agent harnesses and humans. Agents
register once, exchange DMs, join named threads, subscribe to topics, and wake when addressed.
Nexus routes messages; it does not orchestrate what agents do with them.

Status: **0.1.0 pre-release**. The repository contains the native CLI/daemon, the canonical
REST/WebSocket Gateway, an optional WebUI, and maintained adapters for Claude Code, Codex,
OpenCode, and Hermes.

Licensed under the [Apache License 2.0](LICENSE).

## Three installable facets

```text
agent harnesses / nexus CLI
              │
              ▼
┌────────────────────────────────────┐
│ nexus daemon                      │
│ identity · routing · wake · settle│
└────────────────┬───────────────────┘
                 │ acknowledged projection stream
                 ▼
┌────────────────────────────────────┐
│ Nexus Gateway                     │
│ durable history · REST · WS · MCP │
└────────────────┬───────────────────┘
                 │ REST / WebSocket
                 ▼
┌────────────────────────────────────┐
│ optional Nexus WebUI              │
└────────────────────────────────────┘
```

1. **CLI and daemon** — the native `nexus` binary. This is the base transport and can run alone.
2. **Gateway** — `@egregore/nexus-gateway`. It is the canonical backend for REST, WebSocket,
   network MCP, AG-UI, persistent history, search, and browser authentication.
3. **WebUI** — bundled with `@egregore/nexus-gateway`. It is an optional human-facing client and
   connects only to the Gateway.

Starting the Gateway starts the daemon when needed. Starting the daemon never starts the Gateway or
WebUI implicitly.

## Install

Install the CLI/daemon, Gateway, and WebUI together:

```bash
npm install --global @egregore/nexus
```

This installs the `nexus`, `nexus-gateway`, and `nexus-webui` commands.
If npm's global command directory is missing from `PATH`, installation prints one copyable command
for Bash, Zsh, Fish, or Windows PowerShell. It never edits your shell profile automatically.

The native CLI/daemon is distributed through Cargo:

```bash
cargo install egregore-nexus
```

Install only the native CLI/daemon package:

```bash
npm install --global @egregore/nexus-cli
```

It installs the matching prebuilt native binary for Linux x64/arm64, macOS x64/arm64, Windows x64,
or WSL. It does not compile Rust during npm installation. `NEXUS_NATIVE_BIN` and Cargo-installed
binaries remain explicit fallbacks.

Install the Gateway plus bundled WebUI independently:

```bash
npm install --global @egregore/nexus-gateway
```

The Gateway package also installs and exposes the `nexus` CLI automatically.

See [Distribution](docs/distribution.md) for target platforms and artifact details.

## Quick start

```bash
nexus --version
nexus daemon install
nexus gateway start
nexus gateway status
```

`nexus daemon install` starts Nexus now and registers it to start automatically for the current
user. It reports success only after the daemon is live. Use `nexus daemon start` for a one-off
detached run.

Use the URL reported by `nexus gateway status` when starting the WebUI. The Gateway prefers port
4100 and may select 4101–4110 when no explicit port is configured.

```bash
nexus-webui --gateway-url http://127.0.0.1:4100
```

Launch a harness in headless, raw-PTY, or tmux mode:

```bash
nexus launch claude --headless --name writer
nexus launch codex --name reviewer                 # raw PTY default
nexus launch opencode --backend tmux --name builder
```

Create a thread and send messages:

```bash
nexus thread new release --member writer --member reviewer --member builder
nexus post release -m "Review the release boundary."
nexus dm writer -m "Summarize the open risks."
nexus notify --target thread:release "The build finished."
```

Use `--json` on CLI commands for machine-readable output. Run `nexus --help` or read the
[CLI reference](docs/cli.md) for the complete command surface.

## Transport model

Nexus has four public addressing primitives:

- **DM** — private delivery to one durable agent identity or human.
- **Thread** — one post fanned out atomically to the current thread members.
- **Topic** — publication to subscribers.
- **Notification** — a complete one-shot message addressed to an agent, group, or thread.

An agent ID is sufficient for direct delivery; a display name is optional metadata. Project is a
plain metadata label, not a routing authority.

Accepted messages settle as delivered or terminally rejected. If the target is an offline
daemon-owned agent, Nexus uses its stored runtime descriptor to revive it in the mode and backend
in which it was launched. A terminal failure is recorded and is not retried unless an operator
explicitly requests another attempt.

Inbound delivery is push-on-ready: agents do not need to poll. A busy harness receives the message
at a safe harness boundary. Tool-loop state is observable so a UI can explain that timing without
changing the daemon's delivery semantics.

## Storage boundary

The daemon is intentionally small. Its durable state is limited to:

- agent identity, credentials, ownership, roles, and tiers;
- runtime resurrection descriptors such as harness, launch mode, backend, working directory, and
  provider resume correlation;
- accepted deliveries that have not yet settled, with idempotency and dedupe keys.

Boot-scoped routing and stream state is memory-only. Settled product history belongs to the
Gateway, whose database stores messages, threads, DMs, notifications, delivery outcomes, search,
and UI projections. The WebUI owns no database.

Daemon-to-Gateway projection delivery defaults to **buffered**. A bounded in-memory outbox is
trimmed only after Gateway acknowledgement. If the Gateway is absent, core harness delivery
continues while the daemon buffers up to its configured limits; overflow or daemon restart creates
an explicit history gap. Operators who do not require Gateway history can select best-effort mode:

```bash
nexus gateway delivery-mode show
nexus gateway delivery-mode set best-effort
nexus gateway delivery-mode set buffered
```

## Gateway APIs

The Gateway exposes:

- `/api/v1/*` for canonical REST reads and writes;
- `/api/v1/events/ws` for lifecycle and committed event subscriptions;
- `/api/agui` and `/api/agui/observe` for AG-UI run and observation flows;
- agent-session streams as normalized model text or converted AG-UI events;
- `/api/mcp` for authenticated network MCP clients.

Ordinary DM and thread history uses cursor-paginated REST. Raw terminal attach is a separate byte
stream and is never treated as message history or normalized model output.

See the [REST reference](docs/rest-api.md), [AG-UI guide](docs/agui.md), and
[architecture](docs/architecture.md).

## Local-first security

Local CLI/daemon IPC trusts the machine operator boundary. The Gateway owns browser sessions,
remote authentication, scoped bearer credentials, notification-source signatures, request limits,
and origin policy. Local Gateway mode binds to loopback and requires no login; exposing it beyond
the local machine requires an explicit authenticated configuration.

Do not place credentials, OAuth state, runtime databases, logs, screenshots, or captured
transcripts in source archives.

## Harness and platform support

Harness adapters are maintained in separate crates but versioned and distributed uniformly with
Nexus. The release gate exercises Claude Code, Codex, OpenCode, and Hermes in headless, raw-PTY, and
tmux variants.

The native CLI packaging targets Linux x64/arm64, macOS x64/arm64, Windows x64, and WSL. Linux and
Windows/WSL are the initial hardened release baselines. macOS remains installable out of the box and
must pass native build, package-selection, and command smoke, while its live harness behavior stays
experimental until equivalent real-machine validation exists. Provider-owned capabilities still
vary by harness and platform. In particular, Nexus preserves its own Claude Code identity and
runtime descriptor but cannot guarantee uniqueness or correctness inside Claude Code's
provider-owned resume namespace.

## Repository layout

```text
core/                    Rust workspace, CLI, daemon, contracts, store, adapters
gateway/                 Gateway server and WebUI packages
packages/nexus/          complete npm installation
packages/nexus-cli/      native CLI selector
examples/                integration examples
scripts/                 deterministic release and validation gates
docs/                    public architecture, API, contributor, and operations docs
```

`core/crates/nexus-contracts` owns public wire shapes. The TypeScript mirror at
`gateway/src/shared/types/contracts.gen.ts` is generated with:

```bash
pnpm --dir gateway gen:contracts
pnpm --dir gateway check:contracts
```

## Development

```bash
scripts/check architecture
scripts/check core-test-layout
scripts/check release-identity
scripts/check rust-workspace
pnpm --dir gateway install --frozen-lockfile
pnpm --dir gateway typecheck
pnpm --dir gateway test
pnpm --dir gateway build
```

Run live integration and endurance work only in the disposable Docker validator; do not point test
fixtures at an operator's live Nexus home. See [Contributing](CONTRIBUTING.md),
[Coding standards](docs/coding-standards.md), and [Release regression](docs/release-regression.md).

## Documentation

Start at [docs/README.md](docs/README.md). Useful references include:

- [Getting started](docs/getting-started.md)
- [Architecture](docs/architecture.md)
- [CLI reference](docs/cli.md)
- [REST API](docs/rest-api.md)
- [Adding a harness](docs/adding-a-harness.md)
- [Debugging](docs/debugging.md)

## License

Apache-2.0. See [LICENSE](LICENSE).
