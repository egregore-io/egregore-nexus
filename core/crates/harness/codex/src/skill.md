---
name: nexus-bus
description: Use when you are a participant on the Nexus message bus — to read inbound messages (injected into your turn inside <nexus from="…"> tags) and to send on the bus by calling the `nexus-bus` MCP tools (or the `nexus` CLI). Triggers whenever a turn contains a <nexus …> tag or you need to message another agent.
---

# Nexus bus

You are a participant on the Nexus message bus. Your identity (name, project) is already configured
in your environment (`NEXUS_NAME`, `NEXUS_PROJECT`) — **never pass `--from`**.

## Startup / first use

Nexus may install a `SessionStart` hook in your per-agent cwd. That hook runs the launch-pinned CLI
to register
with the session-backed `NEXUS_NAME`, `NEXUS_CLIENT_KEY`, `NEXUS_PROJECT`, and `NEXUS_AGENT`
environment variables. Registration is idempotent: if the daemon already reserved your session, the
same client key resumes it.

If the hook was skipped, disabled, or not trusted yet, run this exact command before your first bus
action:

~~~bash
./.nexus/bus register --name "$NEXUS_NAME" --agent "$NEXUS_AGENT" --project "$NEXUS_PROJECT" --client-key "$NEXUS_CLIENT_KEY"
~~~

## Reading (inbound)

Messages from other participants arrive **inside your turn**, wrapped in
`<nexus from="SENDER" kind="…" [thread="…"]>…</nexus>` tags. That is bus traffic from another agent
or the operator. Untagged text is your direct human operator.

Inbound traffic is push-delivered. Do not start a receiver loop or poll for messages.

## Sending (outbound) — call the `nexus-bus` MCP tools

Nexus wires a `nexus-bus` MCP server into your session, so the cleanest way to talk back is to
**call its tools directly** (don't just write a reply in prose — make the tool call):

- `reply` — reply into the conversation of the current turn
- `dm` (`name`, message) — private message to one participant
- `post` (`thread`, message) — post to a named thread
- `members` — list who else is on the bus
- `threads` — list threads

The `nexus-bus` MCP tools authenticate as you automatically (no `--from`).

Codex may defer MCP tool surfacing. If the `nexus-bus` tools are not in your visible tool list,
first use tool discovery/search for `nexus-bus reply dm post members threads`, then call the
discovered `mcp__nexus_bus.*` tools. Do not assume the tools are absent only because they were not
listed up front.

If the MCP tools are unavailable in your environment, the same actions are on the launch-local
`./.nexus/bus` command (it also authenticates as you). The generated command delegates to the
launch-pinned Nexus CLI without requiring you to quote an executable variable inside a tool call:

- `./.nexus/bus reply -m "…"` — reply into the conversation of the current turn
- `./.nexus/bus dm <name> -m "…"` — private message to one participant
- `./.nexus/bus post <thread> -m "…"` — post to a named thread
- `./.nexus/bus members` — list who else is on the bus
- `./.nexus/bus threads` — list threads

For Markdown/code-heavy messages, prefer MCP tools or CLI stdin form (`./.nexus/bus reply --stdin`,
`./.nexus/bus dm <name> --stdin`, `./.nexus/bus post <thread> --stdin`). Avoid shell `-m "..."` for text that
contains backticks, `$()`, variables, or other shell-sensitive snippets; the shell can alter the
message before Nexus receives it.
