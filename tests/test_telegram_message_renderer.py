import pathlib
import tempfile
import unittest
from unittest.mock import patch

from backend_modules.telegram_commands import TelegramCommandService
from backend_modules.telegram_message_renderer import TelegramMessageRenderer


class TelegramMessageRendererTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = pathlib.Path(self.temp_dir.name)
        self.service = TelegramCommandService(
            store_path=root / "invites.json",
            event_log_path=root / "events.jsonl",
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_missing_report_registers_pending_search_via_host_adapter(self) -> None:
        renderer = TelegramMessageRenderer(self.service, chat_id="100")

        reply = renderer.missing_episode_report_reply(
            {
                "title": "遮天",
                "year": "2023",
                "seasonRows": [
                    {
                        "seasonLabel": "Season 1",
                        "existingText": "1-157、159-160",
                        "totalText": "162",
                        "missingText": "158、161",
                        "statusText": "⚠️ 部分缺失",
                    }
                ],
                "summaryText": "共缺失 2 集（E158、E161），Season 1 尚不完整。",
                "missingText": "E158、E161",
                "missingLabels": ["E158", "E161"],
                "searchCount": 1,
                "dataQueryCount": 4,
            }
        )

        self.assertIsNotNone(reply["reply_markup"])
        self.assertEqual(len(self.service._pending_missing_searches), 1)
        action = next(iter(self.service._pending_missing_searches.values()))
        self.assertEqual(action["title"], "遮天")
        self.assertEqual(action["labels"], ["E158", "E161"])
        self.assertEqual(action["chatId"], "100")

    def test_missing_report_with_low_mapping_confidence_has_no_button(self) -> None:
        renderer = TelegramMessageRenderer(self.service, chat_id="100")

        reply = renderer.missing_episode_report_reply(
            {
                "title": "仙逆",
                "year": "2023",
                "seasonRows": [
                    {
                        "seasonLabel": "Season 1",
                        "existingText": "1-10",
                        "totalText": "147",
                        "missingText": "11-147",
                        "statusText": "⚠️ 待确认",
                    }
                ],
                "summaryText": "当前编号映射异常，以下缺失仅供参考，不建议直接搜索。",
                "detailLines": [
                    "原因：编号映射异常，无法可靠判断缺集",
                    "参考缺失：S01E11-E147",
                ],
                "mappedCount": 10,
                "unmappedCount": 16,
                "localCount": 26,
                "registeredCount": 200,
                "airedCount": 147,
                "missingText": "无法可靠判断",
                "missingLabels": [],
                "referenceMissingText": "S01E11-E147",
                "mappingConfidence": "low",
                "mappingWarning": "编号映射异常，无法可靠判断缺集",
                "isReliable": False,
                "searchCount": 1,
                "dataQueryCount": 4,
            }
        )

        self.assertIsNone(reply["reply_markup"])
        self.assertIn("⚠️ 待确认", reply["fallback_text"])
        self.assertIn("当前编号映射异常，以下缺失仅供参考，不建议直接搜索。", reply["fallback_text"])
        self.assertIn("参考缺失：S01E11-E147", reply["fallback_text"])

    def test_missing_identity_mismatch_reply_has_action_buttons(self) -> None:
        renderer = TelegramMessageRenderer(self.service, chat_id="100")

        reply = renderer.missing_episode_identity_mismatch_reply(
            {
                "title": "仙逆",
                "targetTitle": "仙逆",
                "targetYear": "2023",
                "targetTmdbId": "223911",
                "confidenceReason": "需要先从 Emby 本地候选里确认具体条目，避免把错误条目拿去算缺集。",
                "candidates": [
                    {
                        "title": "仙逆",
                        "year": "2023",
                        "tmdbId": "223911",
                        "episodeCount": 147,
                        "scoreReason": "标题完全匹配 / TMDB 一致 / Series / 147 集",
                    }
                ],
                "actionId": "action-1",
            }
        )

        self.assertIn("缺集查询需要先确认作品身份", reply["fallback_text"])
        self.assertEqual(reply["reply_markup"]["inline_keyboard"][0][0]["callback_data"], "missing_identity:pick:action-1:0")
        self.assertEqual(reply["reply_markup"]["inline_keyboard"][1][0]["callback_data"], "missing_identity:candidates:action-1")
        self.assertEqual(reply["reply_markup"]["inline_keyboard"][2][0]["callback_data"], "missing_identity:reselect:action-1")

    def test_send_ai_message_extracts_message_id_through_host_adapter(self) -> None:
        sent = []

        class FakeSender:
            def send_text(self, **kwargs):
                sent.append(kwargs)
                return {"result": {"message_id": 42}}

        self.service.sender = FakeSender()
        renderer = TelegramMessageRenderer(self.service, chat_id="100")

        message_id = renderer.send_ai_message(
            token="token",
            chat_id="100",
            title="AI",
            body="hello",
        )

        self.assertEqual(message_id, 42)
        self.assertEqual(len(sent), 1)

    def test_stream_answer_uses_host_truncation_for_intermediate_and_final_text(self) -> None:
        edits = []

        class FakeSender:
            def edit_message_text(self, **kwargs):
                edits.append(kwargs)

        self.service.sender = FakeSender()
        renderer = TelegramMessageRenderer(self.service, chat_id="100")

        with patch("backend_modules.telegram_message_renderer.stream_chat_completion", return_value=iter(["abc", "def"])):
            answer = renderer.stream_ai_answer_to_telegram(
                token="token",
                chat_id="100",
                message_id=1,
                ai_config={},
                messages=[{"role": "user", "content": "hi"}],
            )

        self.assertEqual(answer, "abcdef")
        self.assertGreaterEqual(len(edits), 1)
        self.assertEqual(edits[-1]["text"], "abcdef")


if __name__ == "__main__":
    unittest.main()
