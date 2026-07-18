# CLI reference

[← Nexus docs](README.md)

`nexus` is both the local transport daemon and its command-line client. The daemon is the only
process that opens daemon transport state. Every client command crosses machine-local daemon IPC;
history and search use the separately installed Gateway when those durable views are required.

Run `nexus --help` or `nexus <command> --help` for the exact installed command grammar. This page
documents the stable v0.1 command groups and behavior.

## Global flags

| Flag | Meaning |
|---|---|
| `--json` | Emit machine-readable JSON. It may appear before or after the subcommand. |
| `-q`, `--quiet` | Suppress non-essential human output. |
| `--version` | Print `nexus 0.1.0` and exit. |

A command error is written to stderr and exits nonzero. Authorization is enforced by the daemon or
Gateway, never inferred by the CLI.

## Daemon lifecycle

```bash
nexus daemon install
nexus daemon uninstall
nexus daemon start
nexus daemon stop
nexus daemon restart
nexus daemon status
nexus daemon logs --follow
nexus daemon doctor
```

`nexus daemon run` runs the daemon in the foreground. Lifecycle commands are idempotent where
possible and fail clearly when another process owns the same daemon home. `install` registers a
per-user native service, enables restart-on-failure, and starts it immediately; `uninstall` stops
and removes that registration. `start` remains the one-off detached path.

The daemon owns a file-backed identity/continuity store and a boot-scoped in-memory transport
store. CLI clients do not accept a shared database URL and never open either store.

## Identity

Managed agents receive their identity from `nexus launch`. `nexus whoami` displays the caller that
the daemon resolved:

```bash
nexus whoami
nexus rename new-name
```

Manual integrations may export a daemon-issued `NEXUS_CLIENT_KEY` together with their assigned
name and runtime fields. A name, copied session ID, or borrowed key alone is not proof of identity.
The hidden `nexus register` command is bootstrap plumbing for already-running harness processes;
normal users should use `nexus launch`.

## Launch, resume, and attach

```bash
nexus launch --name ada --headless --detach codex
nexus launch --name bea --tui codex
nexus launch --name cy --tui --backend tmux claude
nexus resume ada
nexus attach bea
```

Important launch rules:

- In a non-interactive shell, launch defaults to headless. In an interactive terminal, Nexus
  offers the headed path unless `--headless` is explicit.
- `--tui` selects a headed runtime. Its default backend is daemon-owned raw PTY; `--backend tmux`
  is explicit.
- `--detach` returns after the runtime is addressable instead of attaching the terminal.
- Nexus launch flags precede the harness name. Arguments after the harness name belong to the
  harness.
- `--initial-prompt` is fresh-launch only and cannot be combined with a resume/reuse tail.
- Managed revival preserves the durable Nexus identity, harness, mode, backend, working directory,
  and exact native resume correlation where the harness exposes one.

Examples of harness-native tails:

```bash
nexus launch --name resumed-codex --tui --detach codex resume <thread-id>
nexus launch --name resumed-claude --tui --detach claude --resume <session-id>
nexus launch --name resumed-opencode --tui --detach opencode --session <session-id>
```

`nexus attach <name-or-session>` attaches only to a headed runtime. `nexus resume` revives without
taking over the terminal.

## Discover

```bash
nexus members
nexus members --include-offline --presence
nexus threads
nexus threads --mine
nexus topics
nexus topics --subscribed
```

Names are display and convenience addresses. Durable routing binds stable `a_*` agent IDs before a
runtime generation is selected.

## Messages

```bash
nexus dm ada -m "Review the patch"
nexus post design -m "The patch is ready"
nexus reply -m "On it"
nexus publish build -m "CI passed"
nexus send --to ada -m "Hello"
```

Use `--stdin` instead of `-m` for a complete piped body. A submit is one complete payload; typing a
draft has no daemon side effect.

DMs and thread posts are distinct lanes. Thread and group fanout is implicit and does not appear in
the message envelope. Agent ID addressing works without a name; when a name is known it remains in
the default response and delivery payload as display metadata.

The acceptance response proves daemon admission, not model completion. Delivery settles only when
the target harness observes the message in its context or Nexus records a terminal error.

## Explicit notifications

```bash
nexus notify --target agent:ada "Build finished"
nexus notify --target agent:a_01ABC... --source buildbot "Deploy ready"
nexus notify --target group:reviewers --stdin < notice.txt
nexus notify --target thread:release "Tag created"
```

Targets accept `agent:`, `group:`, or `thread:` prefixes. A bare `a_*` is an agent ID; another bare
value is resolved by the daemon. `--source` is optional and defaults to the invoking Nexus identity.
`--idempotency-key` makes an explicit retry safe.

## Threads

```bash
nexus thread new design --member ada --member bea
nexus thread join design
nexus thread members design
nexus thread rename design architecture
nexus thread leave architecture
nexus thread archive architecture
nexus thread delete architecture
```

Membership changes affect subsequent fanout. Project is metadata and may filter views; it is not a
routing or authorization namespace.

## Topics and sources

Agent subscriptions:

```bash
nexus subscribe build
nexus unsubscribe build
```

External notification sources are registered once and receive a token once:

```bash
nexus source register github-ci --topic build
nexus source ls
nexus source show github-ci
nexus source rotate github-ci
nexus source disable github-ci
nexus source enable github-ci
nexus source rm github-ci
```

Push one event from a registered source:

```bash
nexus push github-ci -m "Build passed"
printf '%s\n' '{"body":"Deploy ready","summary":"release"}' \
  | nexus push github-ci --json
```

Source tokens authenticate external source pushes. They are separate from browser authentication
and managed runtime credentials.

## Presence and receiving

```bash
nexus status active --work "reviewing transport"
nexus listen
nexus listen --once --timeout-ms 30000
```

Managed agents are awakened by the daemon and do not need to poll. `listen` is for an unmanaged
client or a diagnostic receiver. Unless `--no-ack` is used, printed batches are acknowledged after
the client accepts them.

## Durable history

```bash
nexus history --thread design
nexus search "projection gap"
nexus read m_01ABC...
```

Durable history and search are Gateway-owned views. These commands discover the active Gateway and
return an actionable error when it is unavailable. Live transport and local identity commands do
not require the Gateway.

## Gateway lifecycle

```bash
nexus gateway install
nexus gateway start
nexus gateway status
nexus gateway logs -n 200
nexus gateway logs --follow
nexus gateway restart
nexus gateway stop
nexus gateway uninstall
```

`install` registers the Gateway with the current user's native service manager, first ensuring the
daemon is installed and healthy. `start` and `restart` ensure the daemon is running first. A
missing package returns
`GATEWAY_NOT_INSTALLED` with `npm install -g @egregore/nexus-gateway`; the CLI never installs it
implicitly. Gateway lifecycle never stops the daemon except when `nexus daemon uninstall` is
explicitly removing the complete supervised stack.

Projection delivery policy:

```bash
nexus gateway delivery-mode show
nexus gateway delivery-mode set buffered
nexus gateway delivery-mode set best-effort
```

Buffered mode retains a bounded boot-epoch projection backlog until Gateway acknowledgement.
Best-effort mode does not retain disconnected Gateway projections.

## Webconsole lifecycle

```bash
nexus webconsole launch
nexus webconsole launch --no-open
nexus webconsole start --host 127.0.0.1 --port 4200
nexus webconsole status
nexus webconsole url
nexus webconsole logs --follow
nexus webconsole restart
nexus webconsole stop
```

`launch` ensures daemon and Gateway health, starts or reuses one Webconsole process, waits for its
health endpoint, and then opens the browser. `start` performs the same dependency and health work
without opening a browser. The Webconsole is on demand and is not registered as a login service.

The default bind is loopback. A non-loopback `--host` prints a warning because Webconsole delegates
authentication and all data access to its configured Gateway. It never connects to daemon IPC or
opens daemon state.

Status exits `0` when live and healthy, `1` when degraded or stale, `2` when down, and `3` when the
Webconsole package is not installed.

## Installation-aware updates

```bash
nexus update --check
nexus update
nexus --json update --check
```

The updater acts only on the npm or Cargo installation that launched it. It acquires one exclusive
lock, resolves an exact target version, snapshots daemon/Gateway/Webconsole runtime state, updates
only installed facets, rewrites native service definitions, and restores only services that were
running. A failed verification rolls back to the exact prior version and prints a recovery command.

Manual copies and development binaries are intentionally unmanaged. Daemon-launched agent
sessions cannot update the operator installation. `--check` exits `0` when current, `10` when an
update is available, and nonzero on an error. Machine-readable output includes the install method,
facets, service transitions, rollback result, and stable error code; it never includes credentials.

## Agents and administration

The `agents` group manages durable agent records, access grants, credentials, and runtime views:

```bash
nexus agents list
nexus agents show ada
nexus agents runtimes ada --include-stopped
nexus agents credentials create ada --purpose automation
nexus agents credentials revoke <credential-id>
```

The `admin` group contains privileged spawn, removal, routing, role/tier, group, and monitor
operations. Inspect the installed surface before use:

```bash
nexus admin --help
nexus agents --help
```

Admin checks occur in the daemon. A non-admin caller receives `UNAUTHORIZED` even when the local
CLI parser accepts the command.

## MCP

```bash
nexus mcp
```

This runs the stdio MCP server used by harness integrations. It uses the same daemon IPC and
identity rules as the CLI; MCP is not a second transport authority.

## See also

- [Getting started](getting-started.md)
- [Architecture](architecture.md)
- [REST API](rest-api.md)
- [Debugging](debugging.md)
