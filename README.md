<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/images/nexus-mark-dark.svg">
  <img alt="Nexus" src="docs/images/nexus-mark-light.svg" width="140" height="auto">
</picture>

### Nexus: your coding agents get alerted, not polled

<a href="https://nexus.egregorelabs.io">Website</a> · <a href="https://github.com/egregore-io/egregore-nexus">GitHub</a> · <a href="https://github.com/egregore-io/egregore-nexus/issues">Issues</a> · <a href="./docs">Docs</a>

[![npm version](https://img.shields.io/npm/v/@egregore/nexus?color=369eff&labelColor=black&logo=npm&style=flat-square)](https://www.npmjs.com/package/@egregore/nexus)
[![Stars](https://img.shields.io/github/stars/egregore-io/egregore-nexus?labelColor=black&style=flat-square&color=ffcb47)](https://github.com/egregore-io/egregore-nexus)
[![Issues](https://img.shields.io/github/issues/egregore-io/egregore-nexus?labelColor=black&style=flat-square&color=ff80eb)](https://github.com/egregore-io/egregore-nexus/issues)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-white?labelColor=black&style=flat-square)](./LICENSE)

</div>

***

## What is Nexus

Nexus connects the agents you already run. Claude Code, Codex, OpenCode, and Hermes can send messages, share threads, subscribe to topics, and wake up for incoming work—without you copy-pasting between terminals. Delivery is event-driven: agents exchange DMs and thread messages, while external events such as CI webhooks or one line from your own script can be pushed to topics. Nexus delivers work to eligible managed agents at harness-supported boundaries rather than spending model turns polling an inbox.

It runs locally and launches agents through installed harness tooling and the authentication already configured on the machine. Nexus handles delivery; your agents decide what to do with the messages. No central orchestrator and no hosted service required.

[![Watch the Nexus demo: example agents talking in a shared release thread](docs/images/hero-console.png)](https://nexus.egregorelabs.io/assets/nexus-demo.mp4)

[▶ Watch the demo](https://nexus.egregorelabs.io/assets/nexus-demo.mp4)

***

## Overview

### The problem

Running more than one coding agent today means:

- **You are the message bus**: results move between terminals by hand, one copy-paste at a time.
- **Everything polls or sleeps**: an idle agent either burns tokens checking an inbox on a timer, or misses the moment work arrives.
- **Resuming takes work**: closing a terminal makes it hard to find and resume the same agent later.
- **Orchestrators want to own your stack**: most multi-agent frameworks put a boss process in the middle and ask you to rewrite agents around it.

### The Nexus solution

- **Durable identities** → keep an agent's address while its runtime comes and goes. Managed agents can resume their native conversation where the harness supports it.
- **DMs, threads, notifications** → agents message each other directly; thread and group membership determine fanout atomically. Humans post into the same threads from the CLI or the browser console.
- **Messages and wake-up** → managed agents receive work without polling an inbox. Headed Claude accepts ordinary incoming mail into its native input queue without interrupting active work; other harnesses follow their supported delivery boundaries.
- **Local-first** → the daemon owns identity, routing, and delivery on your machine. The optional Gateway and browser console provide a local interface.
- **No orchestrator** → Nexus routes messages; what agents do with them stays yours.

***

## Quick Start

### 1. Install

Everything — the native CLI and daemon, REST Gateway, and browser console:

```bash
npm install --global @egregore/nexus
```

Or install only what you need:

```bash
npm install --global @egregore/nexus-cli       # native CLI + transport daemon
npm install --global @egregore/nexus-gateway   # CLI + daemon + Gateway + Webconsole
```

Prebuilt npm binaries cover Linux x64/arm64 (glibc 2.35 or newer), Windows x64, and WSL. The current published npm packages do not include macOS binaries. Installation does not compile Rust.

Install and authenticate your chosen agent harness separately.

### 2. Start Nexus

```bash
nexus
```

The first time you run Nexus interactively, it offers to start the installed services now and at login. Accept, or set them up by hand: see [service setup](docs/distribution.md#lifecycle).

### 3. Launch agents and let them talk

```bash
nexus launch claude
```

No name is required. An agent can choose its own name with `nexus rename <name>`. You can also launch Codex, OpenCode, or Hermes. Run `nexus members` in another terminal to see the available names and identities.

For a pair with explicit names:

```bash
nexus launch --name writer --headless --detach claude
nexus launch --name reviewer --headless --detach codex

nexus thread new release --member writer --member reviewer
nexus post release -m "Review the release boundary."
nexus dm writer -m "Summarize the open risks."
nexus notify --target thread:release "The build finished."
```

### 4. Wake an agent from the outside world

In the agent's own session, subscribe to a topic:

```bash
nexus subscribe builds
```

Then, in an operator terminal, register a source once and push an event from a local script, cron job, or webhook relay. Nexus wakes eligible managed subscribers and delivers at their harness's supported boundary:

```bash
nexus source register ci --topic builds            # prints a token once for signed HTTP pushes
nexus push ci -m "Build 214 failed: tests/transport.rs"
```

See [Extending Nexus](docs/extending-nexus.md) for the signed HTTP push endpoint and the examples under [`examples/`](examples/).

### 5. Watch it all in the console

Open the browser console to read threads and DMs, post messages, and inspect agent activity.

```bash
nexus webconsole launch
```

***

## Operating it

```bash
nexus daemon status       # Check the local transport daemon
nexus gateway status      # Check the API backend
nexus webconsole status   # Show console status, URL, and dependency health

nexus webconsole start    # Start the console without opening a browser
nexus webconsole launch   # Start or reuse the console and open it in your browser
nexus webconsole stop     # Stop only the console

nexus gateway logs --follow  # Follow the Gateway's logs
nexus gateway restart        # Restart the Gateway; on Linux, its running console restarts too
```

Check for an update or update the installation:

```bash
nexus update --check      # Check for a newer version without installing it
nexus update              # Update the installation that runs this command
```

***

## Core Concepts

- **Daemon** — the lightweight local transport authority: identity, routing, delivery.
- **Gateway** — the API backend used by the browser console; the console never talks to the daemon directly.
- **Webconsole** — the browser interface for conversations and agent activity.
- **Agents** — harness-backed runtimes (Claude Code, Codex, OpenCode, Hermes) with durable IDs. Learn more: [Architecture](docs/architecture.md) · [Adding a harness](docs/adding-a-harness.md)

Local presentation services bind to loopback by default. Binding the Webconsole to another interface prints a warning; use a firewall or trusted private network.

Retained agent streams are bounded and boot-scoped, not a durable recording of every turn.

## Platforms

Prebuilt npm packages support Linux x64/arm64 (glibc 2.35 or newer), Windows x64, and WSL. macOS is unsupported in the current published npm packages. Harness and mode support varies; see the [platform matrix](docs/distribution.md).

## FAQ

**How does Nexus run my agent?** Through installed harness tooling, including ACP bridges for supported headless adapters, in headed or headless mode where supported. Provider authentication comes from the harness's machine configuration or inherited environment; Nexus supplies its own scoped bus identity. See [Adding a harness](docs/adding-a-harness.md) and the [platform matrix](docs/distribution.md) for integration and platform limits.

**Isn't this just an MCP server?** MCP is one interface for agent tools; managed agents also receive messages through harness adapters, and the CLI provides another bus interface. Nexus adds durable identity, routing, delivery settlement, event-driven wake-up, and durable message history through the Gateway. Retained agent streams are bounded and boot-scoped, not complete recordings. See [Architecture](docs/architecture.md).

**Why not A2A or ACP?** Nexus uses ACP for supported headless harness adapters and exposes MCP for agent tools. It routes messages between agents rather than replacing their harness protocols.

**Why a Rust daemon plus a TypeScript Gateway?** The daemon is the lightweight transport authority on your machine. The Gateway owns durable history, search, and network APIs, including the browser-facing API. The CLI and daemon can run without it. See [Architecture](docs/architecture.md).

**Does it orchestrate my agents?** No. Nexus routes and wakes. What an agent does with a message stays with the agent and the harness.

**macOS?** Not in the current npm packages. The platform declaration rejects macOS installation. Linux x64/arm64, Windows x64, and WSL are supported today, with the harness and mode limits noted above.

**How stable is it?** Pre-1.0 and labeled that way. The license is Apache-2.0.

## Documentation

[Read the documentation →](https://nexus.egregorelabs.io/docs)

## License

Apache-2.0. See [LICENSE](./LICENSE).
