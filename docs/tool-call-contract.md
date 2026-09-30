# C-TOOL v1 — the canonical tool-call contract

Every `agent.update` event with `kind: "tool_call"` carries a payload conforming to
`ToolCallData` (`core/crates/nexus-contracts/src/events.rs`, typeshared to
`gateway/src/shared/types/contracts.gen.ts`). This is the ONE shape for tool activity —
live stream, stream store, materialized history, replay, and the AG-UI projection all
speak it. Consumers (recorders, webui, TUI) never reverse-engineer per-harness
payloads.

## The shape

```jsonc
{
  "id": "tc_1",            // REQUIRED — merge key across the call's phases
  "tool": "shell",         // REQUIRED on the opening event — canonical machine name
  "title": "/usr/bin/zsh -lc 'ls'", // display-only; may be a command line; NEVER identity
  "kind": "execute",       // source taxonomy (ACP kind / codex item type), when known
  "status": "in_progress", // in_progress | completed | failed | ...
  "input": { "command": "ls" },     // STRUCTURED args — object/array, never a JSON string
  "output": "listing",     // structured or text output, when the harness exposes it
  "content": "listing",    // harness display content (ACP content blocks), optional
  "locations": []          // ACP follow-along locations, optional
}
```

Updates after the opening event are PARTIAL patches: same key names, only the fields the
update carries, merged by `id`. `tool` appears on a patch only when derivable.

## Producer law

Every harness adapter MUST construct its tool_call payloads through `ToolCallData` (or
emit exactly its key names for patches):

| Lane | Where | `tool` derivation |
|------|-------|-------------------|
| ACP harnesses | `core/crates/nexus-acp-stream/src/lib.rs` | ACP `kind`; `execute` → `shell` |
| codex app-server | `core/crates/harness/codex/src/app_server/translate.rs` | `commandExecution`→`shell`, `fileChange`→`edit`, `webSearch`→`search`, mcp → `{server}/{tool}` |
| claude transcripts | `core/crates/nexus-transcript/src/claude.rs` | JSONL `name` (already machine: `Read`/`Write`/`Bash`) |
| opencode | `core/crates/nexus-agent/src/adapter/opencode/native.rs` + `opencode_plugin_bridge.rs` | registered tool name (`bash`, `write`, …) |
| hermes | `core/crates/nexus-agent/src/adapter/hermes/native.rs` | OpenAI-style function name |

Rules:

1. `tool` is a machine name. Never a human title, never a command line.
2. `input` is the structured args value. Never pre-stringified JSON, never null-padded.
3. Oversized / base64 string fields are sanitized to `"[omitted]"` (8 KB ceiling) BEFORE
   emission — every lane already does this; keep it on new lanes.

## Consumer law

- The materializer persists `tool` + the raw `input` object on the merged block
  (`agent_session_materializer.rs::merge_tool_call`); `argsJson` is display-only.
- Replay (`gateway/src/server/agui/agentSessionProjection.ts`) hands back `tool` and the
  structured `input` exactly as stored. It NEVER re-stringifies a JSON string — the
  double-encoding would turn the argument object into an opaque string.
- The AG-UI mapping (`mapAgentUpdate.ts`) sets `toolCallName` from `tool` (title only as
  fallback) and emits `TOOL_CALL_ARGS.delta = JSON.stringify(input)` — stringified
  exactly once, so `JSON.parse(delta)` returns the original object.

## The fidelity gate

`gateway/src/server/agui/streamFidelity.test.ts` pins the invariant: for the same
logical call, the live lane and the replay lane produce the same canonical
`toolCallName` and arg payloads that parse back to the same object. **Adding a harness
means adding a fixture there** (see `adding-a-harness.md`), and its adapter's own unit
tests must assert `tool` + `input` on its emissions.
