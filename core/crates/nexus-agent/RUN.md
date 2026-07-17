# nexus-agent — live ACP engine: run & smoke-test guide

This crate's `claude` / `codex` adapters drive a **real** [Agent Client Protocol (ACP)][acp]
session over a spawned harness subprocess, using Zed's official Rust SDK
[`agent-client-protocol`][sdk] (pinned to **0.15** — the latest on crates.io; the original
`0.4` workspace guess was wrong and has been corrected). One turn is:

```
spawn harness (stdio) → initialize → session/new (or session/load to resume)
   → session/prompt (the rendered turn) → session/update* (relayed as agent.update) → EndTurn
```

All protocol work lives in `src/adapter/engine.rs` (`AcpEngine`); the per-harness adapters just
resolve the spawn command and delegate.

## Exact commands the adapters run

The harness is launched over **stdio** as an ACP server. Neither the bare `claude` CLI nor the
bare `codex` CLI speaks ACP directly today (Claude Code exposes `--print --*-format stream-json`;
`codex` exposes App Server, not ACP) — the canonical ACP transport is each maintained **ACP
adapter package**. Defaults:

| Harness | Default command | Bridge package |
|---------|-----------------|----------------|
| **Codex**  | `npx -y @agentclientprotocol/codex-acp@1.1.2` | [`@agentclientprotocol/codex-acp`](https://www.npmjs.com/package/@agentclientprotocol/codex-acp) |
| **Claude** | `npx -y @agentclientprotocol/claude-agent-acp` | [`@agentclientprotocol/claude-agent-acp`](https://www.npmjs.com/package/@agentclientprotocol/claude-agent-acp) |

`npx -y` resolves/caches the package and runs its stdio ACP entrypoint. Requires Node on PATH.

### Overrides (env)

Set these to pin a vendored bridge, a specific runtime, or the stream-json fallback. Resolution
order per harness (`CODEX`/`CLAUDE`):

1. `NEXUS_<H>_ACP_CMD` — full program (e.g. `node`); `NEXUS_<H>_ACP_ARGS` (whitespace-split)
   supplies its args (e.g. an absolute bridge entrypoint).
2. else `NEXUS_<H>_ACP_PACKAGE` — overrides just the npm package passed to `npx -y`.
3. else the default in the table above.

Examples:

```bash
# Codex via a locally-installed maintained adapter entrypoint:
export NEXUS_CODEX_ACP_CMD=node
export NEXUS_CODEX_ACP_ARGS="/path/to/node_modules/@agentclientprotocol/codex-acp/dist/index.js"

# Claude via a vendored bridge:
export NEXUS_CLAUDE_ACP_CMD=node
export NEXUS_CLAUDE_ACP_ARGS="/path/to/node_modules/@agentclientprotocol/claude-agent-acp/dist/index.js"
```

### Claude stream-json fallback

Claude Code's CLI also speaks an equivalent newline-delimited JSON protocol:

```bash
claude --print --input-format stream-json --output-format stream-json --verbose
```

This is the documented fallback for environments without the ACP bridge. The ACP bridge is the
canonical path here because it lets Claude and Codex present an **identical** ACP shape on the
bus (same `session/prompt` in, same `session/update` out), which is what the live two-instance
test needs. (If you must use stream-json, wrap it as a bridge that exposes ACP over stdio and
point `NEXUS_CLAUDE_ACP_CMD`/`_ARGS` at it.)

## Build

`cargo build -p nexus-agent` compiles the ACP engine and the built-in `claude`/`codex`/`opencode`/
`hermes` adapters, which spawn the real bridge. The engine is fully tested against a **fake ACP
harness** (`src/bin/fake_acp_agent.rs`, a real subprocess speaking genuine ACP), so the hermetic
suite passes anywhere with no model/network.

## Tests

```bash
# Hermetic — passes anywhere, no model/network (fake ACP harness):
cargo test -p nexus-agent

# Real-binary smoke (ignored by default; skips if the runtime/binary is absent, never fails it):
cargo test -p nexus-agent --test live_smoke -- --ignored --nocapture --test-threads=1
```

## Real one-instance smoke test (machine with the binaries)

On a machine with Node + the bridge + valid credentials (`claude` logged in / `codex login`):

```bash
# 1. Confirm the bridge resolves (these print protocol banners / wait on stdin — Ctrl-C to exit):
npx -y @agentclientprotocol/codex-acp@1.1.2    # Codex ACP server
npx -y @agentclientprotocol/claude-agent-acp   # Claude ACP server

# 2. Drive one real turn through the adapter (spawn → initialize → session/new → prompt → update):
cargo test -p nexus-agent --test live_smoke \
    codex_live_one_turn  -- --ignored --nocapture
cargo test -p nexus-agent --test live_smoke \
    claude_live_one_turn -- --ignored --nocapture
```

A pass prints a non-empty relayed reply (e.g. `pong`). For the **two-instance** bus test, the
daemon launches one `claude` and one `codex` via `Launcher::launch` (each `open_session` does the
spawn + handshake + `session/new`); turns injected on the bus reach each via `session/prompt` and
replies stream back as `agent.update` — the two then converse through the bus.

## Cross-machine sanity checklist (before a live two-instance run)

- [ ] Node on PATH (`node --version`); `npx` works offline-cached or has network for first fetch.
- [ ] `claude` authenticated and `codex login` done; both bridges run from the shell (step 1 above).
- [ ] `cargo build -p nexus` succeeds on the target machine.
- [ ] `live_smoke` prints a non-empty reply for **both** harnesses.

[acp]: https://agentclientprotocol.com/
[sdk]: https://crates.io/crates/agent-client-protocol
