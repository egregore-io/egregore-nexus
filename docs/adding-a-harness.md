# Adding a harness

A harness is an agent runtime that Nexus can launch, address, wake, and observe. v0.1.0 ships
Claude Code, Codex, OpenCode, and Hermes adapters.

A complete adapter supports the launch shapes the harness can actually provide and reports its
capabilities truthfully. Nexus must not advertise native steering, headed attach, or resume when the
adapter cannot perform it.

## Common contract

Every harness integrates with the same boundaries:

```text
daemon launch/revive
       │
       ├─ headless adapter (ACP where supported)
       └─ headed adapter (native bridge + raw PTY or tmux)
                    │
                    ▼
          normalized agent.update stream
                    │
                    ▼
        Gateway nexus/agui live views
```

Raw terminal bytes travel on a separate terminal lane. Do not parse a full-screen TUI into
canonical message history when the harness provides structured events.

## 1. Add the public harness token

Add the variant to `nexus_contracts::Harness` in
`core/crates/nexus-contracts/src/enums.rs`, then update every compiler-enforced `match` site.
Typical sites include:

- CLI parsing under `core/crates/nexus/src/cli/`;
- identity token conversion under `core/crates/nexus-identity/`;
- adapter registration under `core/crates/nexus-agent/`;
- headed program selection under `core/crates/nexus/src/daemon/`;
- generated TypeScript contracts.

Regenerate and verify contracts:

```bash
pnpm --dir gateway gen:contracts
pnpm --dir gateway check:contracts
```

Never edit `gateway/src/shared/types/contracts.gen.ts` by hand.

## 2. Implement the adapter

Harness implementations live in `core/crates/harness/<name>/` or the corresponding adapter module
under `core/crates/nexus-agent/src/adapter/` while older adapters are being split into crates.

The adapter must define:

- the executable and argument builder;
- environment isolation;
- headless session creation and prompt injection;
- structured event translation;
- completion observation;
- interrupt/steer behavior, if supported;
- native resume discovery and invocation, if supported;
- shutdown and child-process cleanup.

Use the harness's native completion signal. A process staying alive, terminal text appearing, or a
provider accepting input is not sufficient proof that the target context observed the message.

## 3. Support headed launches

Headed runtimes use a daemon-owned raw PTY by default. tmux is an explicit backend. Preserve these
properties in the runtime resurrection descriptor:

- harness;
- headed mode;
- selected backend;
- working directory;
- executable and compatible native arguments;
- native resume correlation where the harness exposes one.

Prefer a structured native bridge for normalized model activity. The PTY/tmux byte stream exists
for attachment and visual presentation.

If the harness has no headed mode, reject `--tui` with a capability error. Do not silently launch a
different shape.

## 4. Support headless launches

Headless adapters normally open an ACP session, inject one complete prompt, and translate the
session update stream. If a harness uses another protocol, keep that protocol inside its adapter and
emit the same Nexus contracts.

Per-runtime configuration must be isolated. Do not share writable session databases, config files,
or native identity state between concurrent Nexus agents unless the upstream harness explicitly
guarantees safe multi-session use.

## 5. Bind identity and revival

Nexus identity is stable across runtime replacement. The adapter provides runtime facts; the daemon
owns the binding.

The durable descriptor includes the stable agent ID, harness, launch shape, backend, working
directory, runtime credential binding, and native resume information. Project is optional metadata;
it is not part of routing identity or authorization.

On revival:

1. select the original launch shape;
2. start or resume the native harness;
3. prove the new runtime adopted the same Nexus identity;
4. wait for the inbound bridge to become ready;
5. inject the unsettled delivery exactly once;
6. settle only after a completion/receipt signal defined by the adapter.

If native resume data is unavailable, record that explicitly. Do not invent a correlation from a
new session ID.

## 6. Install bus participation

Every launched harness receives the `nexus` CLI on `PATH` plus session-scoped Nexus identity
environment. The harness-specific bootstrap installs the `nexus-bus` instructions and, when the
harness supports it, a stdio MCP entry.

Registration is idempotent for the daemon-issued client key. A harness must not copy another
runtime's Nexus environment or reuse its writable home.

MCP is a client interface to the same daemon transport; it is not a second routing authority.

## 7. Normalize session updates

Emit structured `agent.update` frames for supported native events. At minimum, cover:

- accepted user input;
- assistant text;
- reasoning when exposed;
- tool-call lifecycle;
- usage/metadata when exposed;
- turn completion;
- terminal errors.

Tool updates must conform to [C-TOOL v1](tool-call-contract.md) using
`nexus_contracts::ToolCallData` or an exactly compatible wire shape.

Do not emit cumulative text snapshots as independent completed messages. Do not convert terminal
redraws into user input. Accepted input should appear once.

## 8. Declare capabilities

Capabilities are runtime-effective and caller-visible through Nexus contracts. Keep provider
support separate from effective authorization.

Relevant capabilities include:

- headless prompt injection;
- headed attach;
- native resume;
- native steering;
- interrupt-and-send;
- structured tool events;
- raw terminal streaming.

A terminal runtime may mask input capabilities until explicitly reactivated. Define that lifecycle
instead of leaving the frontend to guess from presence alone.

## 9. Required tests

Add focused tests before daemon wiring:

- command and environment construction;
- argument preservation;
- session creation and prompt injection;
- update translation and C-TOOL fields;
- accepted-input deduplication;
- completion receipt and timeout classification;
- process cleanup;
- native resume correlation;
- raw PTY and tmux headed startup, when supported.

Then add integration coverage for:

- launch and registration;
- five direct-message turns;
- shared-thread delivery;
- explicit notifications;
- offline wake in the original launch shape;
- active-traffic daemon restart;
- Gateway `nexus` and `agui` views;
- terminal attach for each headed backend;
- zero unexplained dead-letter rows.

The release matrix is documented in [Release regression](release-regression.md).

## 10. Verification

Run focused crate tests while developing, then the repository gates:

```bash
scripts/check core-test-layout
scripts/check architecture
scripts/check rust-workspace
scripts/check rust
pnpm --dir gateway typecheck
pnpm --dir gateway test
pnpm --dir gateway build
```

Run live harness validation only in the resource-bounded disposable Docker environment. Never point
adapter or resurrection tests at an operator's live Nexus home.
