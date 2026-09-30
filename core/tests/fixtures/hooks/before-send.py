#!/usr/bin/env python3
"""Deterministic before_send fixture for the cross-source practical gate."""

from __future__ import annotations

import json
import re
import sys


MARKER = re.compile(
    r"^GATE\[(?P<source>[a-z0-9-]+)\|"
    r"(?P<timing>interrupt|yield_turn|after_tool_loop)\]\s*(?P<body>.*)$",
    re.DOTALL,
)


def main() -> None:
    invocation = json.load(sys.stdin)
    message = invocation.get("message", {})
    metadata = message.get("metadata", {}) if isinstance(message, dict) else {}
    body = message.get("body", "") if isinstance(message, dict) else ""
    match = MARKER.match(body) if isinstance(body, str) else None

    if match:
        source = match.group("source")
        timing = match.group("timing")
        clean_body = match.group("body")
    else:
        source = metadata.get("gateSource") if isinstance(metadata, dict) else None
        timing = metadata.get("gateTiming") if isinstance(metadata, dict) else None
        clean_body = body

    allowed_timings = {"interrupt", "yield_turn", "after_tool_loop"}
    if not isinstance(source, str) or timing not in allowed_timings:
        json.dump({}, sys.stdout, separators=(",", ":"))
        return

    json.dump(
        {
            "action": "continue",
            "message": {"body": f"[hook:{source}] {clean_body}"},
            "metadata": {
                "hookGate": {
                    "beforeSend": True,
                    "source": source,
                }
            },
            "timing": timing,
        },
        sys.stdout,
        separators=(",", ":"),
    )


if __name__ == "__main__":
    main()
