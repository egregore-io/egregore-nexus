#!/usr/bin/env python3
"""Telegram text bridge for Nexus Transport Adapter Protocol v1."""

from __future__ import annotations

import json
import os
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Callable, Mapping, TextIO


PROTOCOL_VERSION = 1
MAX_FRAME_BYTES = 64 * 1024
DEFAULT_MAX_RETRIES = 3

Emit = Callable[[dict[str, Any]], None]
HttpRequest = Callable[[str, dict[str, Any]], object]
Sleep = Callable[[float], None]
Fault = Callable[[str], None]


class TelegramHttpError(RuntimeError):
    def __init__(
        self,
        status: int,
        description: str,
        *,
        retry_after: float | None = None,
    ) -> None:
        super().__init__(description)
        self.status = status
        self.retry_after = retry_after


class InjectedBridgeCrash(RuntimeError):
    """Test-only fault raised after a durable provider send journal entry."""


class TelegramApi:
    def __init__(
        self,
        token: str,
        *,
        request: HttpRequest | None,
        sleep: Sleep,
        max_retries: int,
    ) -> None:
        self._request = request or UrllibTelegramRequest(token)
        self._sleep = sleep
        self._max_retries = max(0, max_retries)

    def call(self, method: str, payload: dict[str, Any]) -> object:
        for attempt in range(self._max_retries + 1):
            try:
                return self._request(method, payload)
            except TelegramHttpError as error:
                if attempt >= self._max_retries:
                    raise
                if error.status == 429:
                    self._sleep(float(error.retry_after or 1.0))
                    continue
                if error.status >= 500:
                    self._sleep(min(2.0, 0.25 * (2**attempt)))
                    continue
                raise
        raise AssertionError("unreachable retry loop")


class UrllibTelegramRequest:
    def __init__(self, token: str) -> None:
        clean = token.strip()
        if not clean:
            raise ValueError("TELEGRAM_BOT_TOKEN is required")
        self._base = f"https://api.telegram.org/bot{clean}/"

    def __call__(self, method: str, payload: dict[str, Any]) -> object:
        encoded = urllib.parse.urlencode(_http_payload(payload)).encode("utf-8")
        request = urllib.request.Request(
            f"{self._base}{method}",
            data=encoded,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=40) as response:
                document = _decode_json(response.read())
        except urllib.error.HTTPError as error:
            document = _decode_json(error.read(), fallback={})
            raise _telegram_error(error.code, document) from None
        except urllib.error.URLError as error:
            raise TelegramHttpError(503, f"Telegram transport unavailable: {error.reason}") from None
        if not isinstance(document, dict) or document.get("ok") is not True:
            status = int(document.get("error_code", 502)) if isinstance(document, dict) else 502
            raise _telegram_error(status, document)
        return document.get("result")


class DurableState:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.state_path = root / "state.json"
        self.journal_path = root / "delivery-journal.jsonl"
        self.offset = -1
        self.bound_chats: set[str] = set()
        self.journal: dict[str, str] = {}
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(root, 0o700)
        self._load_state()
        self._load_journal()

    def flush(self) -> None:
        _atomic_json(self.state_path, {
            "boundChats": sorted(self.bound_chats),
            "offset": self.offset,
        })

    def record_delivery(self, obligation_id: str, external_message_id: str) -> None:
        current = self.journal.get(obligation_id)
        if current is not None:
            if current != external_message_id:
                raise RuntimeError("delivery journal conflict")
            return
        row = _canonical_json({
            "externalMessageId": external_message_id,
            "obligationId": obligation_id,
        }).encode("utf-8") + b"\n"
        descriptor = os.open(
            self.journal_path,
            os.O_WRONLY | os.O_APPEND | os.O_CREAT,
            0o600,
        )
        try:
            _write_all(descriptor, row)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.chmod(self.journal_path, 0o600)
        _fsync_directory(self.root)
        self.journal[obligation_id] = external_message_id

    def _load_state(self) -> None:
        if not self.state_path.exists():
            return
        document = json.loads(self.state_path.read_text(encoding="utf-8"))
        offset = document.get("offset", -1)
        bound_chats = document.get("boundChats", [])
        if not isinstance(offset, int) or offset < -1:
            raise RuntimeError("invalid Telegram offset state")
        if not isinstance(bound_chats, list) or not all(
            isinstance(chat, str) and chat for chat in bound_chats
        ):
            raise RuntimeError("invalid Telegram bound-chat state")
        self.offset = offset
        self.bound_chats = set(bound_chats)

    def _load_journal(self) -> None:
        if not self.journal_path.exists():
            return
        for line in self.journal_path.read_text(encoding="utf-8").splitlines():
            if not line:
                continue
            row = json.loads(line)
            obligation_id = _required_string(row.get("obligationId"), "obligationId")
            external_message_id = _required_string(
                row.get("externalMessageId"),
                "externalMessageId",
            )
            current = self.journal.get(obligation_id)
            if current is not None and current != external_message_id:
                raise RuntimeError("delivery journal contains conflicting entries")
            self.journal[obligation_id] = external_message_id


class TelegramBridge:
    def __init__(
        self,
        *,
        token: str,
        emit: Emit,
        http_request: HttpRequest | None = None,
        sleep: Sleep = time.sleep,
        max_retries: int = DEFAULT_MAX_RETRIES,
        fault: Fault | None = None,
        env: Mapping[str, str] | None = None,
    ) -> None:
        self._token = token
        self._emit = emit
        self._request = http_request
        self._sleep = sleep
        self._max_retries = max_retries
        self._fault = fault or (lambda _point: None)
        self._env = dict(env or os.environ)
        self._api: TelegramApi | None = None
        self._state: DurableState | None = None
        self._bot_user_id = ""
        self._group_thread = ""
        self._group_thread_prefix = ""
        self._group_threads: dict[str, str] = {}
        self._poll_timeout = 20
        self._poll_interval = 0.25
        self._stopping = threading.Event()

    def start(self, config: Mapping[str, object]) -> None:
        if config.get("media", "refuse") != "refuse":
            raise ValueError("Telegram TAP v1 supports media=refuse only")
        state_value = _required_string(config.get("stateDir"), "config.stateDir")
        state_path = Path(state_value).expanduser()
        if not state_path.is_absolute():
            state_path = Path(self._env.get("HOME", str(Path.home()))) / state_path
        self._state = DurableState(state_path.resolve())
        self._group_thread = _optional_string(config.get("groupThread"))
        self._group_thread_prefix = _optional_string(config.get("groupThreadPrefix"))
        raw_group_threads = config.get("groupThreads", {})
        if not isinstance(raw_group_threads, Mapping):
            raise ValueError("config.groupThreads must be an object")
        self._group_threads = {
            str(chat): _required_string(thread, "config.groupThreads value")
            for chat, thread in raw_group_threads.items()
        }
        self._poll_timeout = max(0, min(50, int(config.get("pollTimeoutSeconds", 20))))
        self._poll_interval = max(0.0, float(config.get("pollIntervalSeconds", 0.25)))
        self._api = TelegramApi(
            self._token,
            request=self._request,
            sleep=self._sleep,
            max_retries=self._max_retries,
        )
        identity = self._api.call("getMe", {})
        if not isinstance(identity, Mapping):
            raise RuntimeError("Telegram getMe returned an invalid result")
        self._bot_user_id = _required_string(identity.get("id"), "getMe.id")
        self._state.flush()

    def poll_once(self) -> int:
        state, api = self._ready()
        result = api.call("getUpdates", {
            "offset": state.offset + 1,
            "timeout": self._poll_timeout,
            "allowed_updates": ["message"],
        })
        if not isinstance(result, list):
            raise RuntimeError("Telegram getUpdates returned an invalid result")
        processed = 0
        for update in sorted(result, key=lambda row: int(row.get("update_id", -1))):
            if not isinstance(update, Mapping):
                continue
            update_id = int(update.get("update_id", -1))
            if update_id <= state.offset:
                continue
            self._process_update(update_id, update)
            state.offset = update_id
            state.flush()
            processed += 1
        return processed

    def poll_forever(self) -> None:
        while not self._stopping.is_set():
            try:
                self.poll_once()
            except TelegramHttpError as error:
                self._emit({
                    "t": "transport/log",
                    "level": "warn",
                    "message": f"Telegram polling failed with status {error.status}",
                })
            except Exception as error:  # protocol host owns restart policy
                self._emit({
                    "t": "transport/log",
                    "level": "error",
                    "message": f"Telegram polling failed: {type(error).__name__}",
                })
            self._stopping.wait(self._poll_interval)

    def deliver(self, frame: Mapping[str, object]) -> None:
        state, api = self._ready()
        obligation_id = _required_string(frame.get("obligationId"), "obligationId")
        external_chat_id = _required_string(frame.get("externalChatId"), "externalChatId")
        text = _required_string(frame.get("text"), "text")
        cached = state.journal.get(obligation_id)
        if cached is not None:
            self._receipt(obligation_id, cached)
            return
        result = api.call("sendMessage", {
            "chat_id": external_chat_id,
            "text": text,
        })
        if not isinstance(result, Mapping):
            raise RuntimeError("Telegram sendMessage returned an invalid result")
        external_message_id = _required_string(result.get("message_id"), "sendMessage.message_id")
        state.record_delivery(obligation_id, external_message_id)
        self._fault("after_provider_send_before_receipt")
        self._receipt(obligation_id, external_message_id)

    def shutdown(self) -> None:
        self._stopping.set()
        if self._state is not None:
            self._state.flush()

    def _process_update(self, update_id: int, update: Mapping[str, object]) -> None:
        state, _api = self._ready()
        message = update.get("message")
        if not isinstance(message, Mapping):
            self._emit({
                "t": "transport/log",
                "level": "info",
                "message": f"Telegram update {update_id} has no text message",
            })
            return
        author = message.get("from")
        chat = message.get("chat")
        if not isinstance(author, Mapping) or not isinstance(chat, Mapping):
            raise RuntimeError("Telegram message is missing author or chat")
        user_id = _required_string(author.get("id"), "message.from.id")
        if user_id == self._bot_user_id:
            return
        text = message.get("text")
        if not isinstance(text, str) or not text:
            self._emit({
                "t": "transport/log",
                "level": "warn",
                "message": f"Telegram update {update_id} media refused by text-only v1",
            })
            return
        chat_id = _required_string(chat.get("id"), "message.chat.id")
        chat_type = _required_string(chat.get("type"), "message.chat.type")
        if chat_type in {"group", "supergroup"} and chat_id not in state.bound_chats:
            lane_name = self._group_lane(chat_id)
            if lane_name:
                self._emit({
                    "t": "transport/bindLane",
                    "external": {"chatId": chat_id},
                    "lane": {"kind": "thread", "name": lane_name},
                })
                state.bound_chats.add(chat_id)
        self._emit({
            "t": "transport/ingress",
            "ingressId": str(update_id),
            "external": {
                "userId": user_id,
                "displayName": _display_name(author),
            },
            "chatId": chat_id,
            "text": text,
        })

    def _group_lane(self, chat_id: str) -> str:
        if chat_id in self._group_threads:
            return self._group_threads[chat_id]
        if self._group_thread:
            return self._group_thread
        if self._group_thread_prefix:
            return f"{self._group_thread_prefix}{chat_id}"
        return ""

    def _receipt(self, obligation_id: str, external_message_id: str) -> None:
        self._emit({
            "t": "transport/receipt",
            "obligationId": obligation_id,
            "externalMessageId": external_message_id,
        })

    def _ready(self) -> tuple[DurableState, TelegramApi]:
        if self._state is None or self._api is None:
            raise RuntimeError("Telegram bridge has not received transport/init")
        return self._state, self._api


def run_transport(
    *,
    stdin: TextIO,
    stdout: TextIO,
    env: Mapping[str, str],
    http_request: HttpRequest | None = None,
    sleep: Sleep = time.sleep,
) -> int:
    write_lock = threading.Lock()

    def emit(frame: dict[str, Any]) -> None:
        encoded = _canonical_json(frame)
        if len(encoded.encode("utf-8")) > MAX_FRAME_BYTES:
            raise RuntimeError("outbound transport frame exceeds 64 KiB")
        with write_lock:
            stdout.write(f"{encoded}\n")
            stdout.flush()

    emit({"t": "transport/hello", "protocolVersion": PROTOCOL_VERSION})
    bridge: TelegramBridge | None = None
    poll_thread: threading.Thread | None = None
    try:
        for raw_line in stdin:
            if len(raw_line.encode("utf-8")) > MAX_FRAME_BYTES:
                raise RuntimeError("inbound transport frame exceeds 64 KiB")
            if not raw_line.strip():
                continue
            frame = json.loads(raw_line)
            frame_type = frame.get("t")
            if frame_type == "transport/init":
                if bridge is not None:
                    raise RuntimeError("duplicate transport/init")
                if PROTOCOL_VERSION not in frame.get("protocolVersions", []):
                    raise RuntimeError("transport protocol v1 is unavailable")
                config = frame.get("config")
                if not isinstance(config, Mapping):
                    raise RuntimeError("transport/init config must be an object")
                bridge = TelegramBridge(
                    token=_required_string(env.get("TELEGRAM_BOT_TOKEN"), "TELEGRAM_BOT_TOKEN"),
                    emit=emit,
                    http_request=http_request,
                    sleep=sleep,
                    env=env,
                )
                bridge.start(config)
                poll_thread = threading.Thread(target=bridge.poll_forever, daemon=True)
                poll_thread.start()
            elif frame_type == "transport/deliver":
                if bridge is None:
                    raise RuntimeError("transport/deliver arrived before init")
                bridge.deliver(frame)
            elif frame_type == "transport/ping":
                continue
            elif frame_type == "transport/shutdown":
                if bridge is not None:
                    bridge.shutdown()
                if poll_thread is not None:
                    poll_thread.join(timeout=0.25)
                return 0
            else:
                raise RuntimeError("unsupported host transport frame")
        if bridge is not None:
            bridge.shutdown()
        return 0
    except Exception as error:
        if bridge is not None:
            bridge.shutdown()
        emit({
            "t": "transport/log",
            "level": "error",
            "message": f"Telegram bridge failed: {type(error).__name__}",
        })
        return 1


def main() -> int:
    return run_transport(stdin=sys.stdin, stdout=sys.stdout, env=os.environ)


def _telegram_error(status: int, document: object) -> TelegramHttpError:
    description = "Telegram request failed"
    retry_after = None
    if isinstance(document, Mapping):
        raw_description = document.get("description")
        if isinstance(raw_description, str) and raw_description:
            description = raw_description
        parameters = document.get("parameters")
        if isinstance(parameters, Mapping) and parameters.get("retry_after") is not None:
            retry_after = float(parameters["retry_after"])
    return TelegramHttpError(status, description, retry_after=retry_after)


def _http_payload(payload: Mapping[str, object]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in payload.items():
        if isinstance(value, (dict, list)):
            result[key] = _canonical_json(value)
        elif isinstance(value, bool):
            result[key] = "true" if value else "false"
        else:
            result[key] = value
    return result


def _decode_json(raw: bytes, fallback: object | None = None) -> object:
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        if fallback is not None:
            return fallback
        raise TelegramHttpError(502, "Telegram returned invalid JSON") from None


def _display_name(author: Mapping[str, object]) -> str:
    parts = [
        str(author.get("first_name", "")).strip(),
        str(author.get("last_name", "")).strip(),
    ]
    name = " ".join(part for part in parts if part)
    if name:
        return name
    username = str(author.get("username", "")).strip()
    return f"@{username}" if username else _required_string(author.get("id"), "message.from.id")


def _required_string(value: object, field: str) -> str:
    if value is None:
        raise ValueError(f"{field} is required")
    result = str(value).strip()
    if not result:
        raise ValueError(f"{field} is required")
    return result


def _optional_string(value: object) -> str:
    return "" if value is None else str(value).strip()


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _atomic_json(path: Path, value: object) -> None:
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}-{os.urandom(8).hex()}")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        payload = (_canonical_json(value) + "\n").encode("utf-8")
        _write_all(descriptor, payload)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    try:
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _write_all(descriptor: int, payload: bytes) -> None:
    remaining = memoryview(payload)
    while remaining:
        written = os.write(descriptor, remaining)
        if written <= 0:
            raise OSError("short write")
        remaining = remaining[written:]


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


if __name__ == "__main__":
    raise SystemExit(main())
