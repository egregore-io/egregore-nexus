#!/usr/bin/env python3
"""Windows desktop-notification source: route filtered toast notifications onto the Nexus bus.

Taps the Action Center via the WinRT UserNotificationListener, keeps only what you care about
(here: WhatsApp), and pushes each survivor to a topic. Agents subscribed to it get woken.

    Setup (once):   nexus source register chrome --topic whatsapp
                    pip install winsdk
                    grant access under Settings → Privacy → Notifications
    A listener:     (in an agent's shell)  nexus subscribe whatsapp
    Run:            python desktop_notifications_windows.py

Same three parts as every source — TAP → FILTER → PUSH. Only the tap is Windows-specific.

NOTE: the winsdk API surface shifts a little between versions; treat the calls below as the
shape, and adjust names if your winsdk differs. `nexus.exe` must be on PATH.
"""
import json
import subprocess
import time

from winsdk.windows.ui.notifications import KnownNotificationBindings, NotificationKinds
from winsdk.windows.ui.notifications.management import (
    UserNotificationListener,
    UserNotificationListenerAccessStatus,
)

SOURCE = "chrome"
TOPIC = "whatsapp"


def keep(app: str, title: str, body: str) -> bool:
    # ── THE FILTER ── your policy, not Nexus's.
    return "whatsapp" in f"{app} {title} {body}".lower()


def push(summary: str, body: str, meta: dict) -> None:
    # ── THE ONLY NEXUS TOUCHPOINT ── CLI-native, trusted-local (no token, no HTTP).
    subprocess.run(
        ["nexus", "push", SOURCE, "--topic", TOPIC, "--json"],
        input=json.dumps({"summary": summary, "body": body, "meta": meta}),
        text=True,
        check=True,
    )


def text_of(n) -> tuple[str, str, str]:
    app = n.app_info.display_info.display_name
    binding = n.notification.visual.get_binding(KnownNotificationBindings.get_toast_generic())
    lines = [t.text for t in binding.get_text_elements()] if binding else []
    title = lines[0] if lines else ""
    body = " ".join(lines[1:])
    return app, title, body


def main() -> None:
    listener = UserNotificationListener.current  # ── THE TAP ── Action Center
    if listener.request_access_async().get() != UserNotificationListenerAccessStatus.ALLOWED:
        raise SystemExit("grant notification access in Settings → Privacy → Notifications")

    print(f"routing matched notifications → nexus push {SOURCE} --topic {TOPIC}")
    seen: set[int] = set()
    while True:
        for n in listener.get_notifications_async(NotificationKinds.TOAST).get():
            if n.id in seen:
                continue
            seen.add(n.id)
            app, title, body = text_of(n)
            if keep(app, title, body):
                push(title or app, body or title, {"app": app})
        time.sleep(2)


if __name__ == "__main__":
    main()
