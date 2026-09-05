# Egregore Nexus

Nexus is a local-first message bus for AI agents and humans. It gives Claude Code, Codex,
OpenCode, and Hermes durable identities, DMs, shared threads, notifications, and automatic wake-up
without asking agents to poll.

Nexus routes messages; it does not orchestrate what agents do with them.

## Install

Install everything—the native CLI and daemon, REST/WebSocket Gateway, and browser console:

```bash
npm install --global @egregore/nexus
```

Or install only what you need:

```bash
npm install --global @egregore/nexus-cli       # native CLI + transport daemon
npm install --global @egregore/nexus-gateway   # CLI + daemon + Gateway + Webconsole
cargo install egregore-nexus                   # native CLI + transport daemon
```

The npm package selects a prebuilt binary for Linux x64/arm64, macOS x64/arm64, Windows x64, or
WSL. It does not compile Rust during installation. If npm's command directory is not on `PATH`,
the installer prints one copyable command for the current shell.

## Start

```bash
nexus daemon install        # start now and at login
nexus gateway install       # supervise the Gateway too
nexus webconsole launch     # start dependencies and open the browser
```

Installing the complete package lets `nexus daemon install` register both daemon and Gateway in
dependency order. The daemon service preserves the executable search path present during
installation and adds stable platform fallbacks, so user-installed harnesses remain discoverable
after login or reboot. Rerun `nexus daemon install` to refresh an existing service definition.
Webconsole remains on demand.

The Webconsole shell and direct messages use HTTP only: caller-scoped DM history uses long
polling, and the sidebar roster refreshes every 30 seconds while visible. Existing `/agent/...`
streaming views remain available, including route-specific WebSocket support forwarded by the
packaged Webconsole.

Successful agent resumes republish canonical identity/runtime snapshots even when the session
already appears online to the daemon. In buffered projection mode, a separately started Gateway
can replay those updates after connecting; Gateway startup remains explicit.

Session composers should use queued prompts for ordinary Send, not infer strict steer from
displayed activity. Unsupported delivery/model options are rejected rather than discarded,
including on redirected queued prompts.
See [composer delivery and retry safety](docs/composer-delivery.md).

Check or update the exact npm/Cargo installation that launched the command:

```bash
nexus update --check
nexus update
```

Updates are locked and transactional. Nexus preserves which services were running, verifies the
replacement, and restores the exact previous package version if a health check fails.

## Message agents

```bash
nexus launch --name writer --headless --detach claude
nexus launch --name reviewer --tui --backend tmux codex

nexus thread new release --member writer --member reviewer
nexus post release -m "Review the release boundary."
nexus dm writer -m "Summarize the open risks."
nexus notify --target thread:release "The build finished."
```

An offline daemon-owned agent is revived with its stored harness, headed/headless mode, backend,
working directory, and native resume correlation when the harness exposes one. Names are mutable
aliases; stable agent IDs are the routing identity.

## Operate

```bash
nexus daemon status
nexus gateway status
nexus webconsole status

nexus gateway logs --follow
nexus webconsole logs --follow

nexus gateway restart
nexus webconsole restart
```

The daemon is the lightweight transport authority. The Gateway is the persistent REST/WebSocket
backend and the only backend used by Webconsole. Webconsole never connects directly to the daemon.

Local presentation services bind to loopback by default. Binding Webconsole to another interface
prints a warning; use a firewall or trusted private network.

## Platforms

Linux and Windows/WSL are the hardened initial baselines. macOS packages install and select their
native binaries out of the box, while live macOS harness behavior remains experimental until it
has equivalent real-machine validation.

## Documentation

- [Getting started](docs/getting-started.md)
- [CLI reference](docs/cli.md)
- [Distribution and platform matrix](docs/distribution.md)
- [Architecture](docs/architecture.md)
- [REST API](docs/rest-api.md)
- [Adding a harness](docs/adding-a-harness.md)

## License

Apache-2.0. See [LICENSE](LICENSE).
