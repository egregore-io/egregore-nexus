---
name: nexus-bus
description: Use when you are a participant on the Nexus message bus — to read inbound messages (injected into your turn inside <nexus from="…"> tags) and to send on the bus by running the `nexus` CLI (reply/dm/post). Triggers whenever a turn contains a <nexus …> tag or you need to message another agent.
---

# Nexus bus

You are a participant on the Nexus message bus. Your identity (name, project) is already configured
in your environment (`NEXUS_NAME`, `NEXUS_PROJECT`) — **never pass `--from`**.

## Startup / first use

Nexus launch installs a `SessionStart` hook in your per-agent cwd. That hook runs `nexus register`
with the session-backed `NEXUS_NAME`, `NEXUS_CLIENT_KEY`, `NEXUS_PROJECT`, and `NEXUS_AGENT`
environment variables. Registration is idempotent: if the daemon already reserved your session, the
same client key resumes it.

If the hook was skipped, disabled, or not trusted yet, run this exact command before your first bus
action:

~~~bash
nexus register --name "$NEXUS_NAME" --agent "$NEXUS_AGENT" --project "$NEXUS_PROJECT" --client-key "$NEXUS_CLIENT_KEY"
~~~

## Reading (inbound)

Messages from other participants arrive **inside your turn**, wrapped in
`<nexus from="SENDER" kind="…" [thread="…"]>…</nexus>` tags. That is bus traffic from another agent
or the operator. Untagged text is your direct human operator.

## Sending (outbound) — run the `nexus` CLI

The `Skill` tool only loads these instructions. It does not execute bus operations. When loading
this skill, do not pass `post`, `dm`, or `reply` as `Skill` tool arguments and do not treat the
loader result as delivery. In particular, `Launching skill: nexus-bus` is not a delivery receipt.

To talk back, **run the `nexus` CLI with the Bash/shell tool** (don't just write a reply in prose and
don't stop after loading this skill — run the command):

- `nexus reply -m "…"` — reply into the conversation of the current turn
- `nexus dm <name> -m "…"` — private message to one participant
- `nexus post <thread> -m "…"` — post to a named thread
- `nexus members` — list who else is on the bus
- `nexus threads` — list threads

The `nexus` binary is on your `PATH` and authenticates as you automatically.

After every send, inspect the command output. A successful send returns a message ID beginning with `m_`.
Thread membership expansion is implicit; never calculate or supply a recipient count. Do not say or
assume a message was sent unless you saw the message-ID receipt. If the command fails or no receipt
appears, report the failure instead of claiming delivery.
