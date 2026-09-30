# Nexus coding standards

[← Nexus docs](README.md)

## Architecture

1. Keep the daemon lightweight and transport-focused. Durable product history belongs to the
   Gateway.
2. Keep the WebUI Gateway-only. Browser code must not open daemon state or daemon IPC directly.
3. Treat `nexus-contracts` as the public wire boundary. Generate TypeScript mirrors; do not
   hand-edit them.
4. Keep projects as metadata. Identity, routing, and authorization must not depend on a project
   table or project lifecycle.
5. Keep raw terminal bytes, normalized model text, and AG-UI presentation as separate streams.
6. Preserve stable agent identity across runtime replacement, rename, and revival.
7. Treat an agent name only as an ingress alias. Resolve it once to the immutable `agent_id`, then
   use that ID for authorization, routing, attribution, runtime lookup, delivery, and revival.

## Boundaries

Concrete harness knowledge (Claude, Codex, OpenCode, Hermes) is quarantined; the rest of the
system speaks harness-agnostic traits and contracts.

1. Concrete harness behavior lives in `core/crates/harness/*` behind the `nexus-harness-core`
   traits. Adding harness support means a new crate there, never a new special case in the daemon,
   CLI, PTY, or Gateway.
2. Only `core/crates/harness/*`, the workspace root manifest, and `core/tests` may reference
   `nexus-harness-claude`/`nexus-harness-codex` or reach into `crates/harness/` by path.
3. Core production sources outside `crates/harness/*` must not mention concrete harness names.
   Existing offenders are grandfathered in `scripts/nexus-boundary-gate.allow` as shrink-only
   ceilings: never add an entry or raise a ceiling; when touching an offender file, reduce its
   references and ratchet the ceiling down.
4. Enforced by `scripts/check boundaries` (runs `scripts/nexus-boundary-gate`), which fails on any
   new file or count above its ceiling.

## Rust

- Format with `cargo fmt --all`.
- Prefer small crates and explicit port traits over cross-layer imports.
- Keep production modules free of `#[cfg(test)]`, inline `#[test]`, and `mod tests`; place tests in
  the owning crate's `tests/` directory or a dedicated test crate.
- Return typed contract errors at public boundaries. Do not expose provider or database errors
  directly to callers.
- Bound queues, retries, reads, and stream buffers. Every wait needs a timeout or cancellation
  boundary.
- Treat delivery acknowledgement as a state transition, not a log message.

## TypeScript

- Run strict typechecking and keep server/browser imports separated.
- Gateway server code owns persistence and edge authentication. UI modules consume HTTP or
  WebSocket contracts only.
- Generated `contracts.gen.ts` is read-only; regenerate it from Rust.
- Tests may inject stores and clocks, but production must not contain a mock-runtime switch.
- Keep WebSocket events cursor-addressable and idempotent at presentation boundaries.

## Tests

- Add the smallest regression that fails for the reported defect before changing implementation.
- Prefer deterministic unit and contract tests to sleeps. Live harness behavior belongs in the
  clamped disposable Docker validator.
- Never run destructive lifecycle, migration, package, or endurance gates against a live operator
  home.
- A release-oriented script must return useful `--help` without invoking Docker, Cargo, Nexus,
  package managers, browsers, process cleanup, or runtime mutation.
- A test result from a different source revision is historical evidence, not release proof.

Common gates:

```bash
scripts/check architecture
scripts/check boundaries
scripts/check core-test-layout
scripts/check release-identity
scripts/check rust-workspace
pnpm --dir gateway typecheck
pnpm --dir gateway test
pnpm --dir gateway build
pnpm --dir gateway check:contracts
```

## Documentation

- Update rustdoc and source comments when an implementation invariant changes.
- Update public behavior and operations docs with the same change.
- Keep examples copy-pasteable and platform assumptions explicit.
- State accepted risks as limitations; do not disguise missing behavior as future tense.
- Remove operator-local paths, credentials, captured conversations, and development chronology from
  public files.

## Commits

- Use an imperative subject and keep one concern per commit.
- Keep commit messages focused on the change and its rationale.
- Do not commit generated runtime state, package stores, databases, logs, screenshots, browser
  profiles, OAuth state, or captured model output.
- Verify author and committer identity before publication.
