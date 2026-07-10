import pathlib
import tempfile
import unittest

from backend_modules.ai_chat_service import AIChatService
from backend_modules.ai_host_adapter import AIHostAdapter
from backend_modules.telegram_commands import TelegramCommandService


class AIChatServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = pathlib.Path(self.temp_dir.name)
        self.service = TelegramCommandService(
            store_path=root / "invites.json",
            event_log_path=root / "events.jsonl",
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_streaming_context_status_uses_saved_conversation_state(self) -> None:
        conversation_key = "chat:ctx-status"
        self.service._ai_conversations.remember(conversation_key, question="完美世界多少集", answer="168 集")
        self.service._ai_conversations.set_active_media(
            conversation_key,
            {"title": "完美世界", "type": "series", "embySeriesId": "series-1", "tmdbId": "100"},
        )

        reply = AIChatService(
            self.service,
            question="当前上下文",
            ai_config={"enabled": True, "contextTokensK": 64},
            conversation_key=conversation_key,
            chat_id="100",
        ).run_streaming()

        self.assertIsInstance(reply, dict)
        self.assertIn("当前作品：完美世界", reply["text"])
        self.assertIn("已保存问答：1 轮", reply["text"])

    def test_streaming_context_reset_clears_chat_and_conversation_memory(self) -> None:
        conversation_key = "chat:ctx-reset"
        self.service._remember_ai_exchange(chat_id="100", question="完美世界多少集", answer="168 集")
        self.service._ai_conversations.remember(conversation_key, question="完美世界多少集", answer="168 集")

        reply = AIChatService(
            self.service,
            question="重置上下文",
            ai_config={"enabled": True},
            conversation_key=conversation_key,
            chat_id="100",
        ).run_streaming()

        self.assertIsInstance(reply, dict)
        self.assertIn("已清除当前聊天的 AI 上下文。", reply["text"])
        self.assertEqual(self.service._ai_chat_history.get("100"), None)
        self.assertEqual(self.service._ai_conversations.get(conversation_key), {})

    def test_streaming_context_status_accepts_host_adapter(self) -> None:
        conversation_key = "chat:ctx-host"
        self.service._ai_conversations.remember(conversation_key, question="仙逆多少集", answer="147 集")
        host = AIHostAdapter(self.service)

        reply = AIChatService(
            host,
            question="当前上下文",
            ai_config={"enabled": True, "contextTokensK": 64},
            conversation_key=conversation_key,
            chat_id="100",
        ).run_streaming()

        self.assertIsInstance(reply, dict)
        self.assertIn("已保存问答：1 轮", reply["text"])


if __name__ == "__main__":
    unittest.main()
