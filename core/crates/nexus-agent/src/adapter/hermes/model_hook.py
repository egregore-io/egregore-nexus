"""Exact framework-source model hook installed only in the launch-local profile."""

import asyncio
import json
import logging
import os
from pathlib import Path
import socket
import sqlite3
import threading

_lock = threading.Lock()
_root_context = None
_sequence = 0
_closed = False
_scope = {"platform": "nexus", "user_id": "nexus", "chat_id": "nexus",
          "thread_id": "", "chat_type": "dm"}


def _row(root):
    # Never create/migrate the native database or discover an owner by cwd/prefix.
    path = Path(os.environ["HERMES_HOME"]) / "state.db"
    with sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=0.25) as db:
        db.execute("BEGIN")
        record = db.execute(
            "SELECT id, model, parent_session_id, model_config, ended_at FROM sessions WHERE id = ?",
            (root,),
        ).fetchone()
        usage = None
        if record is not None:
            try:
                counters = db.execute(
                    "SELECT input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, "
                    "reasoning_tokens, api_call_count FROM sessions WHERE id = ?", (root,),
                ).fetchone()
                if counters is not None:
                    usage = dict(zip(("input_tokens", "output_tokens", "cache_read_tokens",
                                      "cache_write_tokens", "reasoning_tokens", "api_call_count"), counters))
            except sqlite3.Error:
                # Older schemas or an unavailable usage read cannot erase valid model metadata.
                # Both reads share this read-only snapshot; never guess zero or migrate the DB.
                pass
    if record is None:
        return None
    native_id, model, parent, raw_config, ended_at = record
    if raw_config is not None and (not isinstance(raw_config, str) or len(raw_config) > 65536):
        return None
    config = json.loads(raw_config) if raw_config is not None else {}
    if not isinstance(config, dict):
        return None
    # Read native lineage conservatively, but never forward config credentials or prompts.
    lineage = {key: config[key] for key in ("_delegate_from", "_branched_from") if key in config}
    return {"id": native_id, "model": model, "parent_session_id": parent,
            "model_config": json.dumps(lineage), "ended_at": ended_at, "usage": usage}


def _send(frame):
    frame["token"] = os.environ["NEXUS_HERMES_BRIDGE_TOKEN"]
    data = (json.dumps(frame, allow_nan=False) + "\n").encode()
    if len(data) > 65536:
        raise ValueError("native model frame exceeds bounded payload")
    endpoint = os.environ["NEXUS_HERMES_BRIDGE_SOCKET"]
    if endpoint.startswith("tcp://"):
        host, port = endpoint[6:].rsplit(":", 1)
        connection = socket.create_connection((host, int(port)), timeout=0.5)
    else:
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        connection.settimeout(0.5)
        connection.connect(endpoint)
    with connection:
        connection.sendall(data)
        reply = connection.makefile("rb").readline(1024)
    return json.loads(reply).get("accepted") is True


def _handle(event_type, context):
    global _root_context, _sequence, _closed
    # Serialise selection, exact lookup and publication within this source lifetime.
    with _lock:
        if _closed:
            return
        if event_type == "session:compress":
            if (_root_context is None or context.get("platform") != "nexus"
                    or context.get("old_session_id") != _root_context["session_id"]
                    or context.get("in_place") is not False):
                return
            selected = dict(_root_context)
            event_type = "source_closed"
            _closed = True
        else:
            if (event_type not in ("agent:start", "agent:end")
                    or any(context.get(key) != value for key, value in _scope.items())):
                return
            root = context.get("session_id")
            if not isinstance(root, str) or not root.strip() or len(root.encode()) > 1024:
                return
            selected = {**_scope, "session_id": root}
            if _root_context is None:
                if event_type != "agent:start":
                    return
                _root_context = selected
            elif root != _root_context["session_id"]:
                # Notify old ownership loss; never select a replacement root in this reporter.
                selected = dict(_root_context)
                event_type = "source_closed"
                _closed = True
        if _sequence >= 9007199254740991:
            _closed = True
            return
        _sequence += 1
        row = None
        if not _closed:
            try:
                row = _row(selected["session_id"])
            except (OSError, sqlite3.Error, ValueError, TypeError):
                # Missing/temporarily unreadable metadata remains unavailable, not guessed.
                pass
        try:
            _send({"t": "model_source", "sequence": _sequence,
                   "event": event_type, "context": selected, "row": row})
        except (OSError, ValueError, KeyError, TypeError):
            logging.getLogger(__name__).warning("captured native model metadata unavailable")


async def handle(event_type, context):
    # Hook execution must not block the gateway event loop on filesystem/socket work.
    await asyncio.to_thread(_handle, event_type, dict(context))
