# Adding a harness

Nexus identifies every agent runtime with an open-set
[`nexus_contracts::HarnessId`]. A harness ID is a lowercase ASCII token matching
`^[a-z][a-z0-9_-]{0,63}$`; it is not a closed enum. Adding an ID therefore does not require a
wire-contract or generated-TypeScript change.

The built-in adapters are `claude`, `codex`, `opencode`, and `hermes`. Integrate new providers
through the registries below instead of adding a new `match` ladder across the daemon.

## Architecture

Harness behavior has two separate registries:

```text
HarnessId
   |
   +-- contract registry -- headed program, launch policy, resume, slash commands
   |
   `-- adapter registry --- headless session, inject, stream, cancel, process lifecycle
```

The contract registry lives at the `egregore-nexus` composition root because it may depend on all
in-tree harness crates. The adapter registry lives in `nexus-agent` and maps a `HarnessId` to a
factory. Both registries are keyed by the validated string token.

An unknown but valid ID is accepted by the contract layer and resolves to a generic non-headed
contract. Launch still fails clearly unless an adapter factory was registered for that ID. There is
no implicit provider fallback and no normalization to a built-in ID.

There are two integration paths:

1. **Spawn-spec manifest** for an ACP-pure, headless runtime. This is the runtime plug-in surface and
   requires no Nexus rebuild.
2. **Harness crate** for a provider that needs headed launch, native resume, bootstrap, a structured
   bridge, or other provider-owned behavior. Rust harness crates are linked at build time; v0.1.4
   does not define a dynamic Rust ABI.

## Path 1: ACP spawn-spec manifest

Use a manifest when the complete integration is “start this command and speak ACP over stdio.”
Create `<NEXUS_HOME>/harnesses/<id>.toml`; the file stem is the harness ID.

```toml
[command]
program = "goose"
args = ["acp"]
cwd = "/opt/goose"                # optional; launch cwd is used when omitted
env = { GOOSE_MODE = "acp" }      # optional
```

Only `command.program` is required. Unknown fields and an empty program are rejected. At daemon
startup, manifests are read in deterministic ID order and installed into both registries:

- the contract side is a generic **non-headed** contract;
- the adapter side uses the shared `AcpEngine` with the manifest command.

The command belongs only to the ACP adapter. It is deliberately not advertised as a TUI program,
so `nexus launch --tui <id>` fails closed. Launch it explicitly in headless mode:

```bash
nexus launch --headless --name goose-worker goose
```

Manifest environment entries are applied first. Nexus then applies its per-runtime identity
environment so a manifest cannot replace `NEXUS_AGENT_ID`, `NEXUS_CLIENT_KEY`, or the other
daemon-issued launch values.

Invalid or unreadable manifests are logged and skipped independently. A missing manifest directory
means no external harnesses. A runtime that needs hook installation, provider configuration,
permission flags, or native session discovery has outgrown a manifest and should use a harness
crate.

Relevant files:

| Purpose | Path |
|---|---|
| Manifest parser and composition-root installation | `core/crates/nexus/src/spawn_spec.rs` |
| Generic ACP adapter | `core/crates/nexus-agent/src/adapter/spawn_spec.rs` |
| Manifest parser regressions | `core/crates/nexus/tests/spawn_spec.rs` |
| Adapter command regressions | `core/crates/nexus-agent/tests/spawn_spec.rs` |

## Path 2: code-owned harness crate

Use a crate under `core/crates/harness/<name>/` when the integration has provider-specific
behavior. Keep that behavior inside the crate and expose it through shared contracts.

### 1. Implement the headed contract

Implement `nexus_harness_core::Harness`. The most important methods are:

| Method | Responsibility |
|---|---|
| `program` | Native headed executable, or `""` when the harness has no headed mode |
| `agent_token` | Stable harness ID stored in runtime identity; normally override this explicitly |
| `headed_runtime_kind` | Existing generic screen path or an in-tree structured bridge |
| `resolve_tail` | Preserve provider-native argv or lift documented sidecar resume metadata |
| `headed_cli_command` / `headed_pty_command` | Build the headed command without invoking it |
| `launch_spec` | Resolve cwd isolation and provider-specific extra argv |
| `resume_style` | Exact native resume-key handling, or unsupported |
| `translate_slash_command` | Opt in only to native commands the provider actually supports |

The default tail contract is verbatim pass-through. Do not add friendly Nexus aliases for native
provider flags. The default slash-command contract is fail-fast unsupported.

Return an empty `program()` for an ACP-only harness. A non-empty program makes headed launch and
attach eligible, so it must name an executable that can actually run interactively on the current
platform.

### 2. Implement the headless adapter

Implement `nexus_agent::Adapter`, normally over the shared `AcpEngine`. The adapter owns:

- process creation and cleanup;
- ACP initialize and session create/load;
- complete-prompt injection;
- completion receipts and timeout classification;
- update translation;
- interrupt/steer behavior;
- provider session correlation;
- harness-specific bootstrap and configuration isolation.

`LaunchCtx` supplies the working directory and daemon-issued identity environment. Do not inherit
ambient `NEXUS_*`, provider-home, or another runtime's writable configuration.

Probe the provider's ACP MCP capabilities before injecting a stdio MCP server. Set
`LaunchCtx::suppress_acp_mcp` when the provider does not accept it, and use the `nexus` CLI on the
runtime `PATH` as the harness-neutral bus path.

### 3. Register both halves at the composition root

Export a function like this from the harness crate:

```rust
pub fn register(registry: &mut nexus_agent::AdapterRegistry) {
    registry.register(
        &nexus_contracts::HarnessId::new("acme").expect("valid built-in id"),
        std::sync::Arc::new(|ctx| {
            std::sync::Arc::new(AcmeAdapter::new(ctx)) as std::sync::Arc<dyn nexus_agent::Adapter>
        }),
    );
}
```

Then wire the contract and adapter at the two explicit composition-root sites:

| File | Change |
|---|---|
| `core/crates/nexus/src/harness_registry.rs` | Add the contract to `builtin_contracts()` |
| `core/crates/nexus/src/daemon/app.rs` | Call the harness crate's `register()` while building the adapter registry |

Do not add the ID to a contract enum; none exists. Do not hand-edit
`gateway/src/shared/types/contracts.gen.ts`.

## Runtime and identity invariants

The generic identity graph stays provider-neutral:

- `agents` owns the durable agent ID, mutable display name, role, tier, and metadata;
- runtime rows own launch mode, harness ID, backend, cwd, process ledger, and lifecycle state;
- messages, memberships, subscriptions, and grants resolve to stable agent IDs;
- project remains metadata, not a routing or authorization authority.

Harness-specific resume and bridge state belongs to the harness crate or its sidecar repository, not
in shared identity tables. Provider resume IDs are correlation keys, not Nexus identity.

On revive, preserve the original launch shape:

1. resolve the stable agent and runtime descriptor;
2. select the original headless/headed mode and terminal backend;
3. reuse an exact native resume key only when the provider supports it;
4. wait for the provider bridge to become ready;
5. inject the unsettled delivery once;
6. settle only after the adapter's completion/receipt signal.

Never substitute “continue latest” for a missing exact resume key.

## Message and activity boundaries

Every harness uses the same high-level lanes:

```text
message delivery -> harness-safe injection -> normalized agent.update events
                                      |
                                      `-> raw PTY/tmux attach bytes (headed only)
```

Raw terminal bytes are ephemeral attach data, not canonical model text. Prefer a structured native
bridge when the provider exposes one. The generic PTY reader is only a fallback for providers that
do not expose structured activity.

At minimum, normalized activity should cover:

- accepted user input, once;
- assistant text;
- reasoning when exposed;
- tool-call start/update/end;
- usage or provider metadata when exposed;
- turn completion and terminal failure.

Do not emit cumulative text snapshots as separate completed messages. Do not turn terminal redraws
into accepted user input.

Tool events must conform to [C-TOOL v1](tool-call-contract.md): `tool` is the machine tool name and
`input` is the structured argument object. Use `nexus_contracts::ToolCallData` where possible.

## Capabilities

Report only behavior the live adapter can perform. Relevant capabilities include:

- headless prompt injection;
- headed attach;
- native resume;
- native steer;
- interrupt-and-send;
- structured tool events;
- raw terminal streaming.

Presence alone is not an input capability. If a terminal runtime masks input or is inside a tool
loop, expose that runtime state instead of asking the frontend to infer it.

## Required verification

Keep tests outside production source files. Add focused coverage for:

- harness-ID validation and serialization;
- registry lookup and unknown-ID failure behavior;
- command, argv, cwd, and environment construction;
- ACP create/load/inject/cancel;
- accepted-input deduplication and completion receipts;
- update translation and C-TOOL fields;
- process cleanup and exact resume correlation;
- raw PTY and tmux startup when headed mode is supported.

Add integration coverage for:

- launch and registration;
- five direct-message turns;
- a shared-thread turn;
- offline wake in the original launch shape;
- active-traffic daemon restart;
- Gateway normalized-text and AG-UI views;
- terminal attach for every supported headed backend;
- zero unexplained dead-letter rows.

Run the repository gates:

```bash
scripts/check core-test-layout
scripts/check release-identity
scripts/check architecture
scripts/check boundaries
scripts/check rust-workspace
pnpm --dir gateway typecheck
pnpm --dir gateway test
pnpm --dir gateway build
pnpm --dir gateway check:contracts
```

Live provider and resurrection testing belongs in the resource-bounded disposable Docker validator.
Never point it at an operator's live Nexus home.

## File reference

| Purpose | Path |
|---|---|
| Open harness identifier | `core/crates/nexus-contracts/src/harness.rs` |
| Shared headed contract | `core/crates/harness/core/src/lib.rs` |
| Headless adapter registry | `core/crates/nexus-agent/src/registry.rs` |
| Contract registry | `core/crates/nexus/src/harness_registry.rs` |
| Launch routing | `core/crates/nexus/src/daemon/app/launch_orchestration.rs` |
| Headed supervisor | `core/crates/nexus/src/daemon/pty_supervisor.rs` |
| Shared ACP engine | `core/crates/nexus-agent/src/adapter/engine.rs` |
| PTY text fallback | `core/crates/nexus-pty/src/screen_text.rs` |
| Gateway stream fidelity gate | `gateway/src/server/agui/streamFidelity.test.ts` |

## Built-in status

| Harness | Headless | Headed | Structured headed activity |
|---|---|---|---|
| Claude Code | yes | raw PTY or tmux | native hook/transcript forwarder |
| Codex | yes | app-server-backed TUI | app-server notifications |
| OpenCode | yes | native plugin + raw PTY or tmux viewer | native plugin events |
| Hermes | yes | raw PTY or tmux | generic PTY fallback |

This table describes the adapters shipped with Nexus. It does not make the identifier set closed.

[`nexus_contracts::HarnessId`]: ../core/crates/nexus-contracts/src/harness.rs
