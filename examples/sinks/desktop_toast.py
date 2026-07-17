#!/usr/bin/env python3
"""Desktop-toast sink: route Nexus messages onto your desktop's notification tray.

The mirror image of `examples/sources/desktop_notifications_*.py` — those tap the
desktop and push INTO the bus; this drains the bus and toasts OUT to the desktop.
Zero dependencies on Linux (shells `notify-send`/libnotify). For cross-platform or
fan-out (macOS, Windows, Slack, ntfy, email…) swap `toast()` for `desktop-notifier`
or Apprise — the TAP side doesn't change.

    Setup (once):   nexus subscribe builds        # whatever topics you care about
    Run:            python3 desktop_toast.py

Nexus stays generic: it never learns this is "a desktop". The TAP is one long-lived
`nexus listen --json`; the SINK and the FILTER are entirely local.
"""
import json
import subprocess
import sys


def keep(msg: dict) -> bool:
    # ── THE FILTER ── your policy, not Nexus's. Everything drained toasts by
    # default; tighten freely (per-topic allow-list, sender match, quiet hours…).
    return True


def toast(msg: dict) -> None:
    # ── THE SINK ── one desktop notification per surviving message.
    scope = msg.get("thread") or msg.get("topic") or "dm"
    body = msg["body"] + ("…" if msg.get("truncated") else "")
    subprocess.run(
        ["notify-send", f"nexus · {scope} · {msg['from']}", body],
        check=False,
    )


def main() -> int:
    # ── THE TAP ── the only Nexus touchpoint: CLI-native, trusted-local (no token,
    # no HTTP). `listen` is the same drain loop agents run: held-receive, split-ack.
    # Each stdout line under --json is one NexusBatch (camelCase):
    #   { "counts": {...}, "dms": [BatchMessage], "threads": [BatchMessage], ... }
    #   BatchMessage: { id, from, kind, scope, thread?, topic?, body, truncated }
    tap = subprocess.Popen(
        ["nexus", "listen", "--json"],
        stdout=subprocess.PIPE,
        stderr=sys.stderr,
        text=True,
    )
    assert tap.stdout is not None
    for line in tap.stdout:
        try:
            batch = json.loads(line)
        except json.JSONDecodeError:
            continue  # non-batch chatter (startup notes etc.) — not ours to interpret
        for msg in (batch.get("dms") or []) + (batch.get("threads") or []):
            if keep(msg):
                toast(msg)
    return tap.wait()


if __name__ == "__main__":
    raise SystemExit(main())
