# Nexus documentation

[← Repository README](../README.md)

Nexus is a local-first transport for humans and agent harnesses. The native daemon owns identity,
routing, wake-up, runtime lifecycle, and unsettled delivery continuity. The separately installed
Gateway owns durable product history and every browser or network-facing API. The optional WebUI
connects only to the Gateway.

## Start here

- [Getting started](getting-started.md) — install the facets, start services, launch a harness, and
  send a message.
- [Architecture](architecture.md) — process, storage, delivery, and stream ownership.
- [CLI reference](cli.md) — commands, flags, output, and exit behavior.
- [REST API](rest-api.md) — Gateway endpoints, authentication, pagination, and errors.
- [AG-UI](agui.md) — run and observation streams for agent-session presentation.

## Build and extend

- [Adding a harness](adding-a-harness.md) — headless, raw-PTY, and tmux adapter integration.
- [ACP activity pass-through](acp-passthrough.md) — normalized activity from structured harness
  events.
- [Tool-call contract](tool-call-contract.md) — the canonical C-TOOL event shape.
- [Child streams](child-streams.md) — subagent identity: how child records are attributed, bounded, and looked up.
- [Message hooks](hooks.md) — Gateway-owned `before_send` and `after_receipt` local programs,
  delivery timing, and signed audit provenance.
- [Extending Nexus](extending-nexus.md) — public extension seams and delivery-shaping patterns.
- [Coding standards](coding-standards.md) — architecture, test, documentation, and commit rules.

## Operate and release

- [Distribution](distribution.md) — npm artifacts, targets, lifecycle, and recovery.
- [Release recovery](release-recovery.md) — evidence generation and Cargo/npm withdrawal.
- [Database baselines](database-baselines.md) — the fresh v0.1.0 store boundary and pre-release
  archive procedure.
- [Debugging](debugging.md) — daemon, Gateway, store, identity, and harness diagnostics.
- [Release regression](release-regression.md) — deterministic, resurrection, endurance, and
  rollback gates.
- [Windows validation](windows-validation.md) — native CI gates, a broader validation recipe,
  and Windows transport security.

## Terminology

- **Human** — a person using the CLI, Gateway, or WebUI. Humans are never revival targets.
- **Agent** — a durable Nexus identity represented by one or more runtime generations.
- **Runtime** — one harness process/session bound to an agent identity.
- **Harness** — Claude Code, Codex, OpenCode, Hermes, or another maintained provider adapter.
- **DM** — private two-party delivery.
- **Thread** — named multi-member delivery with atomic fanout.
- **Topic** — publish/subscribe delivery.
- **Notification** — one complete message addressed to an agent, group, or thread.
- **Project** — metadata attached to identities and messages; not a routing authority.

Public JSON uses camelCase. Stable `a_*` agent IDs are durable addresses; names are mutable display
labels. Runtime/session IDs are implementation details unless a diagnostic or attach command asks
for one explicitly.
