from __future__ import annotations

import io
import json
import os
import stat
import tempfile
import unittest
from pathlib import Path

from telegram_bridge import (
    InjectedBridgeCrash,
    TelegramBridge,
    TelegramHttpError,
    run_transport,
)


HERE = Path(__file__).resolve().parent


class FakeTelegram:
    def __init__(self, updates: list[dict] | None = None) -> None:
        self.updates = list(updates or [])
        self.calls: list[tuple[str, dict]] = []
        self.send_results: list[object] = []
        self.get_updates_results: list[object] = []

    def __call__(self, method: str, payload: dict) -> object:
        self.calls.append((method, dict(payload)))
        if method == "getMe":
            return {"id": 700, "is_bot": True, "username": "nexus_test_bot"}
        if method == "getUpdates":
            result = self.get_updates_results.pop(0) if self.get_updates_results else self.updates
            if isinstance(result, BaseException):
                raise result
            return result
        if method == "sendMessage":
            result = self.send_results.pop(0) if self.send_results else {"message_id": 9001}
            if isinstance(result, BaseException):
                raise result
            return result
        raise AssertionError(f"unexpected Telegram method {method}")

    def calls_for(self, method: str) -> list[dict]:
        return [payload for called, payload in self.calls if called == method]


class TelegramBridgeContractTests(unittest.TestCase):
    def test_offset_dedupe_and_first_sight_binding_survive_restart(self) -> None:
        update = telegram_update(
            update_id=100,
            user_id=42,
            chat_id=-100400,
            chat_type="supergroup",
            text="hello group",
        )
        with tempfile.TemporaryDirectory(prefix="nexus-telegram-offset-") as root:
            frames: list[dict] = []
            first_http = FakeTelegram([update, update])
            first = TelegramBridge(
                token="test-token",
                emit=frames.append,
                http_request=first_http,
                sleep=lambda _seconds: None,
            )
            first.start({"stateDir": root, "groupThread": "design"})
            first.poll_once()
            first.shutdown()

            self.assertEqual(
                [frame["t"] for frame in frames],
                ["transport/bindLane", "transport/ingress"],
            )
            self.assertEqual(frames[0], {
                "t": "transport/bindLane",
                "external": {"chatId": "-100400"},
                "lane": {"kind": "thread", "name": "design"},
            })
            self.assertEqual(frames[1]["ingressId"], "100")
            self.assertEqual(frames[1]["chatId"], "-100400")
            self.assertEqual(read_json(Path(root) / "state.json"), {
                "boundChats": ["-100400"],
                "offset": 100,
            })

            restarted_frames: list[dict] = []
            restarted_http = FakeTelegram([update])
            restarted = TelegramBridge(
                token="test-token",
                emit=restarted_frames.append,
                http_request=restarted_http,
                sleep=lambda _seconds: None,
            )
            restarted.start({"stateDir": root, "groupThread": "design"})
            restarted.poll_once()
            restarted.shutdown()

            self.assertEqual(restarted_frames, [])
            self.assertEqual(restarted_http.calls_for("getUpdates")[0]["offset"], 101)

    def test_429_and_5xx_retry_without_losing_the_delivery(self) -> None:
        with tempfile.TemporaryDirectory(prefix="nexus-telegram-retry-") as root:
            frames: list[dict] = []
            sleeps: list[float] = []
            http = FakeTelegram()
            http.send_results = [
                TelegramHttpError(429, "rate limited", retry_after=2),
                TelegramHttpError(503, "unavailable"),
                {"message_id": 77},
            ]
            bridge = TelegramBridge(
                token="test-token",
                emit=frames.append,
                http_request=http,
                sleep=sleeps.append,
            )
            bridge.start({"stateDir": root})
            bridge.deliver(delivery("ob_retry", "chat-id-verbatim"))
            bridge.shutdown()

            self.assertEqual(sleeps, [2.0, 0.5])
            self.assertEqual(len(http.calls_for("sendMessage")), 3)
            self.assertTrue(all(
                call["chat_id"] == "chat-id-verbatim"
                for call in http.calls_for("sendMessage")
            ))
            self.assertEqual(frames, [{
                "t": "transport/receipt",
                "obligationId": "ob_retry",
                "externalMessageId": "77",
            }])

    def test_5xx_retries_are_bounded_and_redelivery_remains_possible(self) -> None:
        with tempfile.TemporaryDirectory(prefix="nexus-telegram-bounded-") as root:
            frames: list[dict] = []
            http = FakeTelegram()
            http.send_results = [TelegramHttpError(500, "down") for _ in range(4)]
            bridge = TelegramBridge(
                token="test-token",
                emit=frames.append,
                http_request=http,
                sleep=lambda _seconds: None,
                max_retries=3,
            )
            bridge.start({"stateDir": root})

            with self.assertRaisesRegex(TelegramHttpError, "down"):
                bridge.deliver(delivery("ob_bounded", "chat-1"))
            self.assertEqual(len(http.calls_for("sendMessage")), 4)
            self.assertEqual(frames, [])

            http.send_results = [{"message_id": 88}]
            bridge.deliver(delivery("ob_bounded", "chat-1"))
            bridge.shutdown()
            self.assertEqual(len(http.calls_for("sendMessage")), 5)
            self.assertEqual(frames[0]["externalMessageId"], "88")

    def test_delivery_journal_prevents_duplicate_send_after_post_send_crash(self) -> None:
        with tempfile.TemporaryDirectory(prefix="nexus-telegram-journal-") as root:
            frames: list[dict] = []
            http = FakeTelegram()
            faults = 0

            def crash_once(point: str) -> None:
                nonlocal faults
                if point == "after_provider_send_before_receipt" and faults == 0:
                    faults += 1
                    raise InjectedBridgeCrash(point)

            first = TelegramBridge(
                token="test-token",
                emit=frames.append,
                http_request=http,
                sleep=lambda _seconds: None,
                fault=crash_once,
            )
            first.start({"stateDir": root})
            with self.assertRaises(InjectedBridgeCrash):
                first.deliver(delivery("ob_journal", "-909"))
            self.assertEqual(len(http.calls_for("sendMessage")), 1)
            self.assertEqual(frames, [])

            journal = (Path(root) / "delivery-journal.jsonl").read_text(encoding="utf-8")
            self.assertIn('"obligationId":"ob_journal"', journal)
            self.assertEqual(stat.S_IMODE((Path(root) / "delivery-journal.jsonl").stat().st_mode), 0o600)

            restarted = TelegramBridge(
                token="test-token",
                emit=frames.append,
                http_request=http,
                sleep=lambda _seconds: None,
            )
            restarted.start({"stateDir": root})
            restarted.deliver(delivery("ob_journal", "-909"))
            restarted.shutdown()

            self.assertEqual(len(http.calls_for("sendMessage")), 1)
            self.assertEqual(frames, [{
                "t": "transport/receipt",
                "obligationId": "ob_journal",
                "externalMessageId": "9001",
            }])

    def test_bot_messages_are_suppressed_but_advance_the_offset(self) -> None:
        update = telegram_update(
            update_id=200,
            user_id=700,
            chat_id=700,
            chat_type="private",
            text="echo",
        )
        with tempfile.TemporaryDirectory(prefix="nexus-telegram-loop-") as root:
            frames: list[dict] = []
            bridge = TelegramBridge(
                token="test-token",
                emit=frames.append,
                http_request=FakeTelegram([update]),
                sleep=lambda _seconds: None,
            )
            bridge.start({"stateDir": root})
            bridge.poll_once()
            bridge.shutdown()

            self.assertFalse(any(frame["t"] == "transport/ingress" for frame in frames))
            self.assertEqual(read_json(Path(root) / "state.json")["offset"], 200)

    def test_media_is_refused_without_replaying_the_update_forever(self) -> None:
        update = telegram_update(
            update_id=201,
            user_id=42,
            chat_id=42,
            chat_type="private",
            text="placeholder",
        )
        del update["message"]["text"]
        update["message"]["photo"] = [{"file_id": "not-downloaded"}]
        with tempfile.TemporaryDirectory(prefix="nexus-telegram-media-") as root:
            frames: list[dict] = []
            bridge = TelegramBridge(
                token="test-token",
                emit=frames.append,
                http_request=FakeTelegram([update]),
                sleep=lambda _seconds: None,
            )
            bridge.start({"stateDir": root})
            bridge.poll_once()
            bridge.shutdown()

            self.assertFalse(any(frame["t"] == "transport/ingress" for frame in frames))
            self.assertTrue(any("media refused" in frame["message"] for frame in frames))
            self.assertEqual(read_json(Path(root) / "state.json")["offset"], 201)

    def test_protocol_shutdown_flushes_state_and_returns_zero(self) -> None:
        with tempfile.TemporaryDirectory(prefix="nexus-telegram-shutdown-") as root:
            stdin = io.StringIO("\n".join([
                json.dumps({
                    "t": "transport/init",
                    "protocolVersions": [1],
                    "generation": 1,
                    "config": {"stateDir": root, "pollIntervalSeconds": 60},
                }),
                json.dumps({"t": "transport/shutdown"}),
                "",
            ]))
            stdout = io.StringIO()

            code = run_transport(
                stdin=stdin,
                stdout=stdout,
                env={"TELEGRAM_BOT_TOKEN": "test-token", "HOME": root},
                http_request=FakeTelegram(),
                sleep=lambda _seconds: None,
            )

            frames = [json.loads(line) for line in stdout.getvalue().splitlines()]
            self.assertEqual(code, 0)
            self.assertEqual(frames[0], {"t": "transport/hello", "protocolVersion": 1})
            self.assertTrue((Path(root) / "state.json").is_file())
            self.assertEqual(stat.S_IMODE((Path(root) / "state.json").stat().st_mode), 0o600)

    def test_protocol_errors_never_echo_the_bot_token(self) -> None:
        secret = "telegram-secret-DO_NOT_PRINT"

        def rejected(_method: str, _payload: dict) -> object:
            raise TelegramHttpError(401, f"provider echoed {secret}")

        with tempfile.TemporaryDirectory(prefix="nexus-telegram-redaction-") as root:
            stdin = io.StringIO(json.dumps({
                "t": "transport/init",
                "protocolVersions": [1],
                "generation": 1,
                "config": {"stateDir": root},
            }) + "\n")
            stdout = io.StringIO()
            code = run_transport(
                stdin=stdin,
                stdout=stdout,
                env={"TELEGRAM_BOT_TOKEN": secret, "HOME": root},
                http_request=rejected,
                sleep=lambda _seconds: None,
            )

            self.assertEqual(code, 1)
            self.assertNotIn(secret, stdout.getvalue())
            self.assertIn('"message":"Telegram bridge failed: TelegramHttpError"', stdout.getvalue())

    def test_manifest_uses_the_owned_secret_ref_and_text_only_policy(self) -> None:
        manifest = (HERE / "transport.toml").read_text(encoding="utf-8")
        self.assertIn('name = "telegram"', manifest)
        self.assertIn('provider = "telegram"', manifest)
        self.assertIn('entry = "telegram_bridge.py"', manifest)
        self.assertIn('[secretRefs]\nTELEGRAM_BOT_TOKEN = "transport.telegram.bot_token"', manifest)
        self.assertIn('stateDir = "gateway/transports/telegram"', manifest)
        self.assertIn('groupThread = "telegram"', manifest)
        self.assertIn('media = "refuse"', manifest)


def telegram_update(
    *,
    update_id: int,
    user_id: int,
    chat_id: int,
    chat_type: str,
    text: str,
) -> dict:
    return {
        "update_id": update_id,
        "message": {
            "message_id": update_id + 1000,
            "from": {
                "id": user_id,
                "is_bot": user_id == 700,
                "first_name": "External",
                "last_name": "Human",
                "username": "external_human",
            },
            "chat": {"id": chat_id, "type": chat_type, "title": "Design Group"},
            "text": text,
        },
    }


def delivery(obligation_id: str, external_chat_id: str) -> dict:
    return {
        "t": "transport/deliver",
        "obligationId": obligation_id,
        "externalChatId": external_chat_id,
        "lane": {"kind": "thread", "name": "design"},
        "text": "hello from Nexus",
    }


def read_json(path: Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
