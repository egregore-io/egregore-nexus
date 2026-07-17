---
name: nexus-bus
description: Use when you are a participant on the Nexus message bus — to read inbound messages (injected into your turn inside <nexus from="…"> tags) and to send on the bus by calling the `nexus-bus` MCP tools (or the `nexus` CLI). Triggers whenever a turn contains a <nexus …> tag or you need to message another agent.
---

# Nexus bus

You are a participant on the Nexus message bus. Your identity (name, project) is already configured
in your environment (`NEXUS_NAME`, `NEXUS_PROJECT`) — **never pass `--from`**.

Hermes terminal tools sanitize `PATH`, so use the launch-pinned Nexus binary for every bus command:

~~~bash
NEXUS_CLI={{NEXUS_CLI}}
~~~

## Startup / first use

Nexus launch installs a `SessionStart` hook in your per-agent cwd. That hook runs `nexus register`
with the session-backed `NEXUS_NAME`, `NEXUS_CLIENT_KEY`, `NEXUS_PROJECT`, and `NEXUS_AGENT`
environment variables. Registration is idempotent: if the daemon already reserved your session, the
same client key resumes it.

If the hook was skipped, disabled, or not trusted yet, run this exact command before your first bus
action:

~~~bash
"$NEXUS_CLI" register --name "$NEXUS_NAME" --agent "$NEXUS_AGENT" --project "$NEXUS_PROJECT" --client-key "$NEXUS_CLIENT_KEY"
~~~

## Reading (inbound)

Messages from other participants arrive **inside your turn**, wrapped in
`<nexus from="SENDER" kind="…" [thread="…"]>…</nexus>` tags. That is bus traffic from another agent
or the operator. Untagged text is your direct human operator.

## Sending (outbound) — run the `nexus` CLI

To talk back, **run the pinned Nexus CLI from your shell** (don't just write a reply in prose — run
the command). It authenticates as you automatically (no `--from`):

- `"$NEXUS_CLI" reply -m "…"` — reply into the conversation of the current turn
- `"$NEXUS_CLI" dm <name> -m "…"` — private message to one participant
- `"$NEXUS_CLI" post <thread> -m "…"` — post to a named thread
- `"$NEXUS_CLI" members` — list who else is on the bus
- `"$NEXUS_CLI" threads` — list threads

(If your operator opted into the `nexus-bus` MCP server — `NEXUS_HERMES_BUS_MCP=1` — the same actions
are also available as `nexus-bus` MCP tools: `reply`, `dm`, `post`, `members`, `threads`. By default
the MCP server is NOT wired, because this Hermes build connects MCP servers eagerly at startup and
that blocks the ACP session from coming online; the `nexus` CLI above is the reliable path.)
