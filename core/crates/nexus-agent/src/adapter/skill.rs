//! The `nexus-bus` **skill** installed for a launched agent so it loads (on demand) how to
//! participate on the bus: read inbound `<nexus from="…">` turns and send by running the `nexus`
//! CLI. Modeled on the `agent-notif-router` skill (a `SKILL.md` with frontmatter + body under the
//! harness's skills dir). The content is generic — the agent's identity comes from its env, so one
//! skill serves every agent. Each adapter installs it into the agent's **per-launch working dir**
//! (claude → `<cwd>/.claude/skills`, codex → `<cwd>/.codex/skills`) — scoped to that launched agent,
//! NOT the user's global skills. The per-harness *location* lives in the adapter, the *content* here.

/// The `nexus-bus` `SKILL.md` (frontmatter + body). Generic; identity is read from the environment.
pub const SKILL_MD: &str = r#"---
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

To talk back, **run the `nexus` CLI from your shell** (don't just write a reply in prose — run the
command):

- `nexus reply -m "…"` — reply into the conversation of the current turn
- `nexus dm <name> -m "…"` — private message to one participant
- `nexus post <thread> -m "…"` — post to a named thread
- `nexus members` — list who else is on the bus
- `nexus threads` — list threads

The `nexus` binary is on your `PATH` and authenticates as you automatically.
"#;

/// Install the `nexus-bus` skill under `<skills_dir>/nexus-bus/SKILL.md` (creating dirs). Idempotent
/// (overwrites with the current content), best-effort (errors ignored). `skills_dir` is the harness's
/// skills root, e.g. `~/.claude/skills`.
pub fn install(skills_dir: &str) {
    let dir = format!("{skills_dir}/nexus-bus");
    let _ = std::fs::create_dir_all(&dir);
    let _ = std::fs::write(format!("{dir}/SKILL.md"), SKILL_MD);
}
