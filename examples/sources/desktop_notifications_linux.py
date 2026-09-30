#!/usr/bin/env python3
"""Linux desktop-notification source: route filtered notifications onto the Nexus bus.

Taps the freedesktop notification bus, keeps only the ones you care about (here: WhatsApp),
and pushes each survivor to a topic. Agents subscribed to that topic get woken.

    Setup (once):   nexus source register chrome --topic whatsapp
    A listener:     (in an agent's shell)  nexus subscribe whatsapp
    Run:            python3 desktop_notifications_linux.py

Nexus stays generic: it never learns this is "Chrome" or "WhatsApp". The TAP and the FILTER
are entirely local; the only Nexus surface is one `nexus push` per match.

Requires: python3-dbus + PyGObject  (Debian/Ubuntu: `apt install python3-dbus python3-gi`).

NOTE: receiving *other* apps' notifications on a locked-down session bus needs monitor mode.
The simplest portable variant is to shell `dbus-monitor` and parse it; this file uses the
direct add_match form for readability. If you see nothing, run with `dbus-monitor` instead
(see the bottom of this file).
"""
import json
import subprocess

import dbus
from dbus.mainloop.glib import DBusGMainLoop
from gi.repository import GLib

SOURCE = "chrome"     # the registered source name
TOPIC = "whatsapp"    # where it fans out (agents `nexus subscribe whatsapp`)


def keep(app: str, summary: str) -> bool:
    # ── THE FILTER ── your policy, not Nexus's. Swap freely (Slack, a sender allow-list, regex…).
    return "whatsapp" in f"{app} {summary}".lower()


def push(summary: str, body: str, meta: dict) -> None:
    # ── THE ONLY NEXUS TOUCHPOINT ── CLI-native, trusted-local (no token, no HTTP).
    subprocess.run(
        ["nexus", "push", SOURCE, "--topic", TOPIC, "--json"],
        input=json.dumps({"summary": summary, "body": body, "meta": meta}),
        text=True,
        check=True,
    )


def on_notify(_bus, msg) -> None:
    # freedesktop Notify(app_name, replaces_id, app_icon, summary, body, actions, hints, timeout)
    if msg.get_member() != "Notify":
        return
    args = msg.get_args_list()
    app, summary, body = str(args[0]), str(args[3]), str(args[4])
    if keep(app, summary):
        push(summary, body, {"app": app})


def main() -> None:
    DBusGMainLoop(set_as_default=True)
    bus = dbus.SessionBus()  # ── THE TAP ── the desktop notification stream
    bus.add_match_string("interface='org.freedesktop.Notifications',member='Notify'")
    bus.add_message_filter(on_notify)
    print(f"routing matched notifications → nexus push {SOURCE} --topic {TOPIC}")
    GLib.MainLoop().run()


if __name__ == "__main__":
    main()

# Portable fallback if add_match yields nothing (monitor mode), no extra deps:
#
#   dbus-monitor "interface='org.freedesktop.Notifications',member='Notify'" |
#   python3 - <<'PY'
#   import sys, re, subprocess
#   summary = None
#   for line in sys.stdin:
#       m = re.search(r'string "(.*)"', line)
#       if m:
#           summary = summary or m.group(1)        # first string ≈ app/summary
#       if line.strip() == "" and summary:
#           if "whatsapp" in summary.lower():
#               subprocess.run(["nexus","push","chrome","--topic","whatsapp","-m",summary])
#           summary = None
#   PY
