# Distribution

[← Nexus docs](README.md)

Nexus 0.1.0 has three runtime facets and three public npm entry packages. The npm wrappers are at
0.1.4; the native Cargo release remains 0.1.0.

## Artifact map

| Facet | Package | Command | Responsibility |
|---|---|---|---|
| Complete install | npm `@egregore/nexus` | `nexus`, `nexus-gateway`, `nexus-webui` | Installs every Nexus facet |
| CLI and daemon | npm `@egregore/nexus-cli` or Cargo crate `egregore-nexus` | `nexus` | Identity, routing, wake, runtime lifecycle, delivery settlement |
| Gateway + WebUI | npm `@egregore/nexus-gateway` | `nexus`, `nexus-gateway`, `nexus-webui` | CLI/daemon plus REST, WebSocket, MCP, AG-UI, history, search, edge auth, browser UI |

The npm CLI never compiles Rust. One `@egregore/nexus-cli` tarball contains the prebuilt binaries
for Linux x64/arm64 glibc, macOS x64/arm64, Windows x64, and WSL. Its launcher selects the matching
file locally. An explicit `NEXUS_NATIVE_BIN` takes precedence; Cargo-installed binaries remain a
fallback. When no matching binary exists, the launcher prints an actionable installation error.

## Installation

```bash
cargo install egregore-nexus
npm install --global @egregore/nexus          # everything
npm install --global @egregore/nexus-cli      # CLI + daemon only
npm install --global @egregore/nexus-gateway
```

The Gateway package installs and exposes the CLI and carries the built WebUI. No public WebUI-only
or platform-only package is published. All packages must install without a source checkout.

The CLI post-install hook is non-mutating. It stays silent when npm's global command directory is
already on `PATH`; otherwise it prints one copyable Bash, Zsh, Fish, or user-scoped PowerShell
command. The same guidance applies when the CLI arrives through the Gateway or complete package.

## Lifecycle

```bash
nexus daemon install     # register for login, restart on failure, and start now
nexus daemon status

nexus gateway install     # register Gateway after ensuring the daemon service
nexus gateway start       # starts the daemon first when needed
nexus gateway status
nexus gateway logs
nexus gateway restart
nexus gateway stop
nexus gateway uninstall

nexus webconsole launch
nexus webconsole status
```

Daemon installation is per-user: a systemd user unit on Linux and WSL, a LaunchAgent on macOS,
and a limited-privilege Scheduled Task on Windows. `nexus daemon uninstall` stops the supervised
daemon and removes its native registration. Installation reports success only after the daemon PID
is live and stable; otherwise it exits nonzero with log and native service-manager guidance.
`nexus daemon start` remains available for a one-off detached run.

When the installed npm topology already contains the Gateway, `nexus daemon install` also installs
the Gateway service after daemon health succeeds. It never downloads a missing facet.

`nexus gateway install` ensures the daemon service and registers an independently supervised
Gateway with dependency ordering and restart-on-failure. `nexus gateway start` never installs
software. If the Gateway executable is missing, it returns a precise error naming
`@egregore/nexus-gateway`. Gateway restart affects the Gateway only; daemon lifecycle remains an
explicit operator action.

Webconsole is on demand rather than a login service. `nexus webconsole launch` starts or reuses it,
waits for daemon and Gateway health, and opens the browser. `--no-open` is available for remote and
headless hosts. Non-loopback binding is explicit and emits a security warning.

The Gateway writes discovery under the Nexus home after it binds. Clients should use
`nexus gateway status` rather than assuming port 4100, because the default resolver may select an
available port from 4100–4110. An explicit `NEXUS_GATEWAY_PORT` disables fallback.

## Updates

```bash
nexus update --check
nexus update
```

Automatic update is available only to a managed npm or Cargo installation. The npm launcher marks
whether the active installation is CLI-only, Gateway, or complete; the updater changes only those
facets. Cargo updates only the native CLI and daemon. Manual and development binaries print the
package-manager recovery path instead of guessing ownership.

Updates use an exclusive lock, exact versions, bounded command output, service-state snapshots,
post-install command and health verification, and exact-version rollback. Daemon, Gateway, and
Webconsole are restored in dependency order only when they were running before the update. An
operator can inspect the structured receipt under the Nexus home after completion.

## Platform matrix

The initial release hardens Linux and Windows/WSL. macOS artifacts remain part of the complete npm
package and must pass native compilation, launcher selection, and command smoke, but macOS live
harness behavior is experimental until it has equivalent real-machine evidence.

| Platform | Native CLI/daemon | Gateway/WebUI | Headed harness note |
|---|---:|---:|---|
| Linux x64 | Release baseline | Release baseline | raw PTY and tmux; live twelve-variant gates run here |
| Linux arm64 | Release baseline | Release baseline | native build, tests, packaging, and command smoke; provider binaries remain external |
| Windows x64 | Release baseline | Release baseline | named-pipe IPC/projection/attach, Task Scheduler lifecycle, native package and Gateway gates |
| WSL | Release baseline | Release baseline | Linux package plus stable-identity and systemd handoff gates |
| macOS x64 | Experimental | Experimental | packaged and command-smoked; live harness behavior is not release-blocking |
| macOS arm64 | Experimental | Experimental | packaged and command-smoked; live harness behavior is not release-blocking |

Harness executables are external prerequisites. Nexus adapter support is maintained separately per
harness but released and documented uniformly.

Linux contributors can reproduce the native Windows source gate with the disposable fixture in
[Windows validation](windows-validation.md). The canonical public workflow runs the Windows build,
tests, named-pipe transport handshake, Gateway reconnect test, packaging, and launcher smoke on a
native Windows runner.

## Package requirements

Every published package must:

- report version `0.1.0` for Cargo or `0.1.4` for npm, and license `Apache-2.0`;
- contain no source-checkout or operator-local path dependency;
- contain no credentials, OAuth state, runtime database, logs, screenshots, or captured model
  output;
- install in a clean home without invoking a compiler, except the explicit Cargo install path;
- expose successful, side-effect-free `--help` and `--version` behavior where applicable;
- ship with checksums, an SBOM, provenance metadata, and a documented rollback or yank action.

Cargo crates are packaged in dependency order with exact `=0.1.0` internal runtime dependencies.
Test-only crates remain unpublished. npm packages are packed and installed from their tarballs in
isolated directories before publication.

## Release gate

The minimum artifact gate is:

```bash
scripts/nexus-three-facet-package-smoke
```

It packages and installs the native CLI/daemon, Gateway, WebUI, and npm CLI launcher into clean
temporary homes, starts the services, checks Gateway and WebUI health, and validates package
identity. The complete release also requires deterministic code gates, the twelve-variant
active-traffic resurrection gate, and the twelve-variant three-hour endurance gate on the exact
source revision.

## Pre-release reset and post-publication recovery

0.1.0 has no database upgrade path because no earlier public database contract exists. Before
installing over a development build:

1. stop the old Gateway and daemon;
2. archive the old Nexus home, including database WAL/SHM files when present;
3. start 0.1.0 with a fresh Nexus home;
4. retain the archive until the new installation passes smoke checks.

After publication, recovery uses package-manager-native controls: yank affected Cargo crates,
deprecate affected npm versions, and publish a corrective version. Only a prior public version may
be restored to an npm dist-tag. The annotated release tag is never moved.

Because 0.1.0 is the first public version, it has no prior public dist-tag or database contract to
restore. Use the exact commands and artifact-evidence procedure in
[Release recovery](release-recovery.md).
