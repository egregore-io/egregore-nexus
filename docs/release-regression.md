# Release regression and rollback

[← Nexus docs](README.md) · [Distribution](distribution.md)

Release evidence is valid only for the exact source commit and packed artifacts being published.
A source change after a gate starts invalidates later evidence.

## Source gates

Run the structural and identity gates first:

```bash
scripts/check release-identity
scripts/check core-test-layout
scripts/check architecture
scripts/test-nexus-release-entrypoint-help
```

Then run the Rust and Gateway gates in a resource-bounded disposable environment:

```bash
scripts/check rust-workspace
scripts/check rust
scripts/check acceptance
scripts/check gateway
pnpm --dir gateway build
```

The public hygiene gate rejects private plans, operator paths, internal personas, secret-like state,
assistant workflow artifacts, and attribution trailers from the public tree.

## Fresh-schema gates

v0.1.0 supports fresh stores only. The release gate must prove:

- a fresh daemon identity/continuity store creates the complete baseline;
- the daemon transport store is boot-scoped and in memory;
- reopening the identity store is idempotent;
- a fresh Gateway store creates the complete product-history baseline;
- reopening the Gateway store is idempotent;
- unsupported pre-release schema versions fail closed;
- no package silently upgrades a pre-release database.

## Packed-artifact smoke

Validate what users install, not only the checkout:

- Cargo-pack and install the CLI/daemon graph from a clean directory;
- pack `@egregore/nexus` and the single multi-platform `@egregore/nexus-cli` tarball;
- pack and start `@egregore/nexus-gateway` plus its bundled WebUI without the source checkout;
- verify `nexus --version` reports `0.1.0` and the revision when available;
- exercise `gateway start|status|logs|restart|stop`;
- confirm a missing Gateway installation returns `GATEWAY_NOT_INSTALLED` and an install command.

Use `scripts/nexus-v01-release-validation` for the deterministic command ledger.

## Twelve-variant harness gate

The practical matrix covers every supported harness in three launch shapes:

| Harness | Headless | Headed raw PTY | Headed tmux |
|---|---:|---:|---:|
| Claude Code | required | required | required |
| Codex | required | required | required |
| OpenCode | required | required | required |
| Hermes | required | required | required |

Each participant must complete a five-turn direct-message exchange, one shared-thread exchange, and
a notification delivery with timestamps. Evidence includes logs and headed screenshots.

## Active-traffic resurrection gate

Run the resurrection fixture separately from the endurance soak:

```bash
scripts/nexus-v010-resurrection-run <candidate>
scripts/nexus-v010-resurrection-gate <candidate>
```

The daemon is restarted while thread traffic and unsettled delivery exist. Passing requires:

- all twelve stable Nexus identities remain bound correctly;
- each variant revives in its original headless/headed shape and backend;
- native resume correlation remains correct where the harness exposes it;
- messages accepted before or during restart settle exactly once;
- all participants demonstrate post-restart thread context;
- Gateway reconnects and resumes projections without fabricating settlement;
- zero unexplained dead-letter rows.

Provider identity caveats must be reported separately from Nexus identity failures.

## Three-hour endurance gate

Only after resurrection passes, run the exact candidate for 180 minutes:

```bash
scripts/nexus-v010-endurance-gate <candidate> 10800
```

The soak uses the same twelve variants in one thread. It records checkpoints, delivery state,
participant activity, resource use, Gateway/WebSocket observations, logs, and screenshots.

Required checkpoint cadence is:

- 5 minutes for the first early warning;
- three 10-minute intervals followed by one 15-minute interval;
- five 20-minute intervals;
- one final 30-minute interval.

Silence at a checkpoint requires diagnosis before an activity injection. Browser screenshot failure
is presentation evidence and does not automatically fail transport, but it must be recorded. A
delivery failure, unexplained participant loss, identity mismatch, or dead-letter row fails the run.

Only one endurance container may run at a time. Bound its CPU, memory, and process count so the host
remains usable.

## Release decision

Publish only when the exact candidate has:

- clean source, schema, and packed-artifact gates;
- a passing twelve-variant active-traffic resurrection run;
- a complete passing 180-minute endurance run;
- reviewed logs and screenshots;
- no unresolved release-blocking dead-letter rows;
- a public history free of private records and correct Apache-2.0 metadata.

## Rollback

List saved binaries and restore one by hash:

```bash
scripts/nexus-rollback
scripts/nexus-rollback <sha>
```

After rollback, perform one coordinated daemon restart and repeat daemon health, Gateway health, a
DM, a thread post, and a live-session observation. A rollback changes the running binary; it does
not make a newer store schema compatible with an older release.
