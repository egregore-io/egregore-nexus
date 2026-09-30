# Getting started

[← Nexus docs](README.md)

Nexus ships as three runtime facets:

- `@egregore/nexus-cli`: the `nexus` CLI and local transport daemon;
- `@egregore/nexus-gateway`: the REST, WebSocket, MCP, AG-UI, history, and authentication backend;
- `nexus-webui`: the optional browser application bundled with `@egregore/nexus-gateway`.

`@egregore/nexus` installs all three. The CLI and daemon are sufficient for agent-to-agent
transport; the WebUI talks only to the Gateway.

## Prerequisites

- Linux x64 for the v0.1.5 release baseline; other packaged targets follow the status in
  [Distribution](distribution.md);
- one or more supported harnesses: Claude Code, Codex, OpenCode, or Hermes;
- `tmux` only when selecting the optional tmux headed backend.

Building from source additionally requires Rust, Node.js, and `pnpm`.

## 1. Install the CLI and daemon

Install from Cargo:

```bash
cargo install egregore-nexus --version 0.1.5
```

The npm CLI package carries the matching prebuilt native binary and does not compile Rust:

```bash
npm install -g @egregore/nexus-cli
```

Install the complete stack instead with `npm install -g @egregore/nexus`.

The global installer checks whether npm's command directory is already on `PATH`. If it is not, it
prints the exact command for the current shell without editing any profile. The equivalent manual
commands are:

```bash
# Bash
printf '\nexport PATH="%s:$PATH"\n' "$(npm prefix --global)/bin" >> "$HOME/.bashrc" && source "$HOME/.bashrc"

# Zsh
printf '\nexport PATH="%s:$PATH"\n' "$(npm prefix --global)/bin" >> "$HOME/.zshrc" && source "$HOME/.zshrc"

# Fish
fish_add_path --universal (npm prefix --global)/bin
```

On Windows, run this in PowerShell and then open a new terminal:

```powershell
$nexusBin = npm prefix --global
$userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
[Environment]::SetEnvironmentVariable('Path', (($userPath + ';' + $nexusBin).Trim(';')), 'User')
```

From a source checkout:

```bash
cargo build --manifest-path core/Cargo.toml -p egregore-nexus --release
```

## 2. Start Nexus transport

For an interactive first run, enter `nexus` and accept the single background-startup prompt to
enable all installed components now and at login. The complete package includes Webconsole;
its server starts without opening a browser. The choice is remembered per Nexus home, and
automated/agent commands never prompt. No additional setup command or package download is needed.

To configure services explicitly instead:

```bash
nexus daemon install
nexus gateway install       # if installed
nexus webconsole install    # if installed; requires healthy Gateway service
nexus daemon status
```

`nexus daemon install` starts the daemon immediately and registers it with the current user's
native service manager: systemd on Linux and WSL, launchd on macOS, or Task Scheduler on Windows.
It waits for the daemon to become healthy before reporting success; a failed start exits nonzero
and points to `nexus daemon logs`. Use `nexus daemon uninstall` to stop and remove that
registration. The generated service preserves the executable search path present during
installation and adds stable user/system fallbacks, so harnesses installed through npm, Homebrew,
or a user-local package manager remain discoverable after login and reboot. Rerunning
`nexus daemon install` refreshes that environment. When the current npm installation includes the
Gateway, daemon installation also registers and starts the Gateway after daemon health succeeds.
It never downloads a missing package. For a one-off detached run, use `nexus daemon start`.

For foreground development, use:

```bash
nexus daemon run
```

The daemon opens a machine-local IPC endpoint. CLI, MCP, Gateway, and harness adapters send
commands through that endpoint; they never open the daemon database directly.

The daemon keeps boot-scoped routing, presence, message, and stream state in memory. Its small
file-backed store contains only identity, runtime-resurrection descriptors, and unsettled delivery
continuity required across a restart.

## 3. Launch and message an agent

`nexus launch` creates an addressable identity and starts the selected harness:

```bash
nexus launch --name ada --headless --detach codex
nexus members
nexus dm ada -m "Reply with your Nexus name."
```

Headless is the default programmatic route. Use `--tui` for a headed harness. Headed launches use a
daemon-owned raw PTY by default; tmux is explicit:

```bash
nexus launch --name headed-codex --tui codex
nexus launch --name tmux-codex --tui --backend tmux codex
nexus attach headed-codex
```

Pass harness-native arguments after the harness name. Nexus preserves the original harness,
headless/headed shape, backend, working directory, and native resume correlation when it revives a
managed agent.

## 4. Use a thread

```bash
nexus thread new design --member ada
nexus post design -m "Review the transport boundary."
nexus history --thread design
```

Thread fanout is implicit. Membership selects recipients; callers do not provide a fanout count.
Messages to a stopped managed agent trigger revival. A target that cannot be revived settles to an
explicit error, and a terminal failure is retried only by an explicit operator action.

## 5. Receive messages and notifications

An unmanaged process can drain its own inbox with:

```bash
nexus listen
```

Managed harnesses are woken by the daemon and do not poll. Notifications use the same delivery
primitive with an explicit target:

```bash
nexus notify --target agent:ada "Build finished"
nexus notify --target thread:design "Release candidate is ready"
```

The source defaults to the invoking Nexus identity. Use `--source` only when a caller needs an
explicit attribution label.

## 6. Install and start the Gateway

```bash
npm install -g @egregore/nexus-gateway
nexus gateway install
nexus gateway status
```

`nexus gateway install` first ensures the daemon's per-user service, then registers the Gateway
with dependency ordering and restart-on-failure. Use `nexus gateway start` for a one-off detached
Gateway instead. If the Gateway package is missing, the CLI exits nonzero with
`GATEWAY_NOT_INSTALLED` and the exact install command; it never installs software implicitly.

The Gateway binds to loopback by default and writes discovery metadata under `NEXUS_HOME`. Manage
it with:

```bash
nexus gateway restart
nexus gateway logs --follow
nexus gateway stop
```

Gateway history uses its own local database. It consumes ordered daemon projections, commits each
fact, and acknowledges the projection after the transaction succeeds. No daemon and Gateway
process share a database connection or database file.

Buffered projection delivery is the default. Best-effort mode is available when Gateway history is
not required:

```bash
nexus gateway delivery-mode show
nexus gateway delivery-mode set buffered
nexus gateway delivery-mode set best-effort
```

## 7. Call the REST API

Local loopback mode requires no browser login. The Gateway stamps the local operator identity:

```bash
curl -sS -X POST http://127.0.0.1:4100/api/v1/messages \
  -H 'content-type: application/json' \
  -d '{"to":{"verb":"post","thread":"design"},"body":"Gateway is online"}'
```

Read a bounded page of history:

```bash
curl -sS 'http://127.0.0.1:4100/api/v1/threads/design/history?limit=50'
```

Remote mode is Gateway-authenticated. Use a scoped Gateway credential and configure its allowed
origins and bind address; do not expose local mode on an untrusted interface.

## 8. Start the bundled WebUI

```bash
nexus webconsole launch
```

`launch` ensures the daemon and Gateway, starts or reuses the Webconsole, waits for health, and then
opens the browser. On a headless or remote host, use `nexus webconsole launch --no-open` and print
the URL with `nexus webconsole url`. The WebUI owns no database and has no daemon IPC fallback. If
the Gateway is unavailable, the UI reports the backend failure rather than presenting stale daemon
state.

Binding to another interface is explicit:

```bash
nexus webconsole start --host 0.0.0.0 --port 4200
```

Nexus prints a warning for non-loopback binds. Use a firewall or trusted private network and keep
Gateway authentication enabled for remote access.

For source development, the Gateway and WebUI packages have separate entrypoints even when the
development server serves both from one checkout:

```bash
pnpm --dir gateway install
pnpm --dir gateway dev
```

## 9. Update the managed installation

```bash
nexus update --check
nexus update
```

The updater follows the package that launched it: CLI-only npm, Gateway npm, complete npm, or
Cargo. It updates only installed facets, preserves prior service state, verifies the replacement,
and rolls back to the exact previous version if a health check fails. Manual copies and development
binaries are not mutated automatically.

## Configuration

The daemon loads built-in defaults, `$NEXUS_HOME/nexus.toml`, a working-directory `nexus.toml`, and
then `NEXUS_*` environment overrides. `NEXUS_HOME` defaults to `~/.nexus`.

Common daemon settings:

| Setting | Purpose |
|---|---|
| `db_path` | File-backed identity, runtime, and unsettled-delivery continuity. |
| `launch_backend` | Default headed backend: `pty` or `tmux`. |
| `gateway_projection.delivery_mode` | `buffered` or `best_effort`. |
| `NEXUS_HOME` | Daemon state, IPC discovery, logs, and Gateway discovery root. |

Common Gateway settings:

| Environment variable | Purpose |
|---|---|
| `NEXUS_GATEWAY_DB` | Gateway-owned local `file:` database URL. |
| `NEXUS_GATEWAY_PORT` | Required port instead of automatic fallback. |
| `NEXUS_GATEWAY_BIND` | Bind address; loopback is the safe default. |

Pre-v0.1 shared-store URL and token settings are not part of the v0.1 architecture. Follow the
[database baseline procedure](database-baselines.md) to archive any unsupported pre-release home
and start v0.1.5 with fresh daemon and Gateway stores.

## Next steps

- [CLI reference](cli.md)
- [Architecture](architecture.md)
- [REST API](rest-api.md)
- [AG-UI and session streams](agui.md)
- [Distribution](distribution.md)
- [Debugging](debugging.md)
