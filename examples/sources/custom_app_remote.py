#!/usr/bin/env python3
"""Custom-app source (remote): an app or server that can't run the CLI emits via signed HTTP.

Same event, same contract as the local example and as a GitHub/CI webhook — only the transport
differs. A remote producer authenticates with the source's TOKEN as the HMAC key.

    Setup (once):   nexus source register myapp --topic deploys     # copy the printed token
    A listener:     (in an agent's shell)  nexus subscribe deploys
    Use:            set NEXUS_URL + NEXUS_SOURCE_TOKEN, call notify(...)

Requires: requests  (`pip install requests`).
"""
import hashlib
import hmac
import json
import os
import time

import requests

NEXUS_URL = os.environ.get("NEXUS_URL", "https://nexus.example")
SOURCE = "myapp"
TOKEN = os.environ["NEXUS_SOURCE_TOKEN"]  # from `nexus source register myapp`


def notify(summary: str, body: str, meta: dict | None = None, topic: str | None = None) -> None:
    raw = json.dumps({"summary": summary, "body": body, "meta": meta or {}})
    ts = str(int(time.time()))
    # Signature = HMAC-SHA256(token, "<timestamp>." + rawBody). The timestamp is in the signed
    # material so a captured body can't be replayed outside the daemon's freshness window.
    sig = hmac.new(TOKEN.encode(), f"{ts}.{raw}".encode(), hashlib.sha256).hexdigest()
    response = requests.post(
        f"{NEXUS_URL}/api/v1/sources/{SOURCE}/push",
        params={"topic": topic} if topic else None,
        headers={
            "X-Nexus-Timestamp": ts,
            "X-Nexus-Signature": f"sha256={sig}",
            "Content-Type": "application/json",
        },
        data=raw,
        timeout=10,
    )
    response.raise_for_status()


if __name__ == "__main__":
    notify("Deploy started", "web v1.4.2 → prod", {"service": "web", "version": "1.4.2"})
