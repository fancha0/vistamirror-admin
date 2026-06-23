import json
import pathlib
import tempfile
import unittest
from unittest.mock import patch

from backend_modules.telegram_commands import TelegramCommandService
from backend_modules.telegram_sender import TelegramSender


class FakeDrive115Service:
    transfer_error = ""

    def __init__(self, config):
        self.config = config

    def parse_share(self, *, share_url, receive_code=""):
        return {
            "shareCode": "abc123",
            "receiveCode": receive_code,
            "title": "测试资源",
            "fileCount": 1,
            "files": [{"id": "file-1", "name": "episode.mkv"}],
        }

    def transfer_share(self, **kwargs):
        if self.transfer_error:
            raise RuntimeError(self.transfer_error)
        return {"ok": True, "message": "请求已接收", "targetCid": kwargs.get("target_cid")}


class TelegramDrive115Tests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        root = pathlib.Path(self.temp_dir.name)
        self.store_path = root / "invites.json"
        self.store_path.write_text(
            json.dumps(
                {
                    "drive115Config": {
                        "enabled": True,
                        "cookie": "UID=admin; CID=test",
                        "defaultCid": "100",
                    }
                }
            ),
            encoding="utf-8",
        )
        self.service = TelegramCommandService(
            store_path=self.store_path,
            event_log_path=root / "events.jsonl",
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_private_share_transfers_immediately_and_replies_to_source(self):
        FakeDrive115Service.transfer_error = ""
        with patch("backend_modules.telegram_commands.Drive115Service", FakeDrive115Service):
            reply = self.service._cmd_drive115_transfer(
                "资源介绍 https://115.com/s/abc123?password=p1a5",
                reply_to_message_id=88,
            )

        self.assertEqual(reply["text"], "转存完成：成功 1 个，已存在 0 个，失败 0 个")
        self.assertEqual(reply["reply_to_message_id"], 88)
        self.assertNotIn("reply_markup", reply)

    def test_transfer_failure_uses_unified_statistics(self):
        FakeDrive115Service.transfer_error = "Cookie 已失效"
        with patch("backend_modules.telegram_commands.Drive115Service", FakeDrive115Service):
            reply = self.service._cmd_drive115_transfer(
                "https://115cdn.com/s/abc123?password=p1a5",
                reply_to_message_id=99,
            )

        self.assertIn("成功 0 个，已存在 0 个，失败 1 个", reply["text"])
        self.assertIn("原因：Cookie 已失效", reply["text"])
        self.assertEqual(reply["reply_to_message_id"], 99)

    def test_sender_uses_reply_parameters_and_allows_missing_source(self):
        sender = TelegramSender()
        captured = {}

        def fake_request(*, token, method, payload):
            captured.update({"token": token, "method": method, "payload": payload})
            return {"ok": True, "result": {"message_id": 1}}

        sender.api_request = fake_request
        sender.send_text(token="token", chat_id="10", text="done", reply_to_message_id=77)

        self.assertEqual(captured["method"], "sendMessage")
        self.assertEqual(captured["payload"]["reply_parameters"]["message_id"], 77)
        self.assertTrue(captured["payload"]["reply_parameters"]["allow_sending_without_reply"])

    def test_existing_transfer_uses_separate_statistics(self):
        FakeDrive115Service.transfer_error = ""

        def existing_transfer(self, **kwargs):
            return {"ok": True, "status": "exists", "message": "目标目录中已存在相同文件。"}

        with patch.object(FakeDrive115Service, "transfer_share", existing_transfer), patch(
            "backend_modules.telegram_commands.Drive115Service", FakeDrive115Service
        ):
            reply = self.service._cmd_drive115_transfer("https://115.com/s/abc123")

        self.assertEqual(reply["text"], "转存完成：成功 0 个，已存在 1 个，失败 0 个")

    def test_group_reply_command_replies_to_resource_message(self):
        sent = []

        class FakeSender:
            def send_chat_action(self, **kwargs):
                return None

            def send_text(self, **kwargs):
                sent.append(kwargs)
                return {"ok": True, "result": {"message_id": 100}}

        self.service.sender = FakeSender()
        self.service._cmd_drive115_transfer = lambda args, reply_to_message_id=0: {
            "text": "转存完成：成功 1 个，失败 0 个",
            "reply_to_message_id": reply_to_message_id,
        }
        self.service._handle_update(
            {
                "update_id": 1001,
                "message": {
                    "message_id": 20,
                    "text": "/zhuancun115",
                    "chat": {"id": -10, "type": "group"},
                    "from": {"id": 1},
                    "reply_to_message": {
                        "message_id": 12,
                        "caption": "资源 https://115cdn.com/s/abc123?password=p1a5",
                    },
                },
            },
            token="token",
        )

        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0]["reply_to_message_id"], 12)

    def test_group_plain_share_is_ignored(self):
        sent = []

        class FakeSender:
            def send_text(self, **kwargs):
                sent.append(kwargs)

        self.service.sender = FakeSender()
        self.service._handle_update(
            {
                "update_id": 1002,
                "message": {
                    "message_id": 21,
                    "text": "资源 https://115.com/s/abc123",
                    "chat": {"id": -10, "type": "group"},
                    "from": {"id": 1},
                },
            },
            token="token",
        )

        self.assertFalse(sent)


if __name__ == "__main__":
    unittest.main()
