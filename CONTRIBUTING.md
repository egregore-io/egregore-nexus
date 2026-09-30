# Contributing to Nexus

Nexus is a local-first transport for messages and notifications between humans and agent
harnesses. Keep changes aligned with the boundaries below.

## Architecture invariants

- The daemon is the lightweight transport authority. It owns durable agent identity, runtime
  resurrection descriptors, and only the bounded journal needed to settle accepted deliveries.
- The Gateway is a separately installed process and the canonical REST/WebSocket backend. It owns
  durable product history, search, event projections, and browser-facing authentication.
- The WebUI talks only to the Gateway. It must not open the daemon database, daemon IPC, or runtime
  files directly.
- Projects are metadata labels, not routing authorities or durable first-class entities.
- DMs, threads, topics, and notifications share transport primitives but retain their distinct
  delivery and visibility rules.
- Agent-session presentation has two explicit views: normalized model text and converted AG-UI
  events. Raw terminal bytes are a separate attach stream, never canonical message history.
- Nexus routes and wakes; it does not orchestrate agent behavior.

## Contracts and generated code

`core/crates/nexus-contracts` is the source of truth for public request, response, event, and error
shapes. TypeScript contracts in `gateway/src/shared/types/contracts.gen.ts` are generated and must
not be edited by hand.

After a Rust contract change, run:

```bash
pnpm --dir gateway gen:contracts
pnpm --dir gateway check:contracts
```

Commit the Rust change, generated TypeScript, and matching golden fixtures together.

## Documentation

Documentation is part of the change:

- update rustdoc or source comments when an implementation boundary changes;
- update the relevant file under `docs/` for behavior, API, installation, or operations changes;
- update `README.md` and `CHANGELOG.md` for user-visible changes;
- describe compatibility and failure behavior precisely—do not promise unsupported provider or
  platform behavior.

## Verification

Use the narrowest relevant test while developing, then run the repository gates before proposing a
merge:

```bash
scripts/check core-test-layout
scripts/check release-identity
scripts/check architecture
scripts/check boundaries
scripts/check rust-workspace
pnpm --dir gateway typecheck
pnpm --dir gateway test
pnpm --dir gateway build
```

Release-oriented scripts must implement a successful, side-effect-free `--help`. Runtime and
endurance tests belong in the disposable Docker validator; never point them at an operator's live
Nexus home.

## Commits

Use an imperative subject and keep each commit to one concern. Keep commit messages focused on the change and its rationale.
