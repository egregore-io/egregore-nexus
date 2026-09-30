#!/usr/bin/env python3
"""Custom-app source (same box): your app emits its own events onto the Nexus bus.

The simplest source of all — no tap, no filter, because the app already knows what it wants
to say. It just calls `nexus push`. Use this when the app runs on the same machine as the
Nexus daemon (the CLI is trusted-local: no token, no HTTP).

    Setup (once):   nexus source register myapp --topic deploys
    A listener:     (in an agent's shell)  nexus subscribe deploys
    Use:            import this, call notify(...)

One registered source can fan to multiple topics via `topic=` — agents subscribe to just the
ones they care about (deploys / errors / alerts).
"""
import json
import subprocess

SOURCE = "myapp"


def notify(summary: str, body: str, meta: dict | None = None, topic: str | None = None) -> None:
    cmd = ["nexus", "push", SOURCE, "--json"] + (["--topic", topic] if topic else [])
    subprocess.run(
        cmd,
        input=json.dumps({"summary": summary, "body": body, "meta": meta or {}}),
        text=True,
        check=True,
    )


if __name__ == "__main__":
    # however your app already detects these events — emit them:
    notify("Deploy started", "web v1.4.2 → prod", {"service": "web", "version": "1.4.2"})
    notify("DB backup failed", "nightly dump timed out", {"job": "pg_dump"}, topic="errors")
