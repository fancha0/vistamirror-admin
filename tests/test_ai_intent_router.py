import pathlib
import tempfile
import unittest
from unittest.mock import patch

from backend_modules.ai_intent_router import AiIntentRouter
from backend_modules.telegram_commands import TelegramCommandService


class AiIntentRouterTests(unittest.TestCase):
    def test_llm_route_extracts_clean_media_title(self):
        def completion(**kwargs):
            return """```json
            {"intent":"media_missing_episodes","mediaTitle":"仙逆","mediaType":"tv","useActiveMedia":false,"isCorrection":false,"confidence":0.98}
            ```"""

        route = AiIntentRouter(completion).route(
            "看一下我仙逆的缺失集",
            config={"enabled": True},
            active_media={},
        )

        self.assertEqual(route["intent"], "media_missing_episodes")
        self.assertEqual(route["mediaTitle"], "仙逆")
        self.assertEqual(route["source"], "llm")

    def test_invalid_llm_json_falls_back_without_crashing(self):
        route = AiIntentRouter(lambda **kwargs: "not json").route(
            "查看一下仙逆的缺失集",
            config={"enabled": True},
            active_media={},
        )

        self.assertEqual(route["intent"], "media_missing_episodes")
        self.assertEqual(route["source"], "fallback")
        self.assertEqual(route["routerError"], "ValueError")

    def test_correction_sentence_only_extracts_real_title(self):
        title = AiIntentRouter.extract_correction_title("你识别错了，我说的是仙逆的缺失集，你识别的是啥")
        self.assertEqual(title, "仙逆")

    def test_title_candidates_preserve_real_titles_starting_with_me(self):
        for title in ("我的阿勒泰", "我推的孩子", "我是刑警"):
            candidates = AiIntentRouter.title_candidates(question=title, llm_title=title)
            self.assertEqual(candidates, [title])

    def test_suspicious_spoken_prefix_adds_corrected_candidate(self):
        candidates = AiIntentRouter.title_candidates(
            question="看一下我仙逆的缺失集",
            llm_title="我仙逆",
        )
        self.assertEqual(candidates, ["我仙逆", "仙逆"])


class TelegramAiRoutedQuestionTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        root = pathlib.Path(self.temp_dir.name)
        self.service = TelegramCommandService(
            store_path=root / "invites.json",
            event_log_path=root / "events.jsonl",
        )
        self.config = {
            "enabled": True,
            "baseUrl": "https://example.test/v1",
            "apiKey": "secret",
            "model": "test",
        }

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_bad_llm_title_retries_corrected_candidate(self):
        class FakeIdentityService:
            def resolve(self, query, preferred_type=""):
                if query == "仙逆":
                    return {
                        "identity": {"title": "仙逆", "type": "series", "tmdbId": "123", "year": "2023"},
                        "embyItem": {"Id": "series-1", "Name": "仙逆", "Type": "Series"},
                        "candidates": [],
                        "ambiguous": False,
                    }
                return {"identity": {}, "embyItem": {}, "candidates": [], "ambiguous": False}

        self.service._media_identity_service = lambda: FakeIdentityService()
        route = {
            "intent": "media_missing_episodes",
            "mediaTitle": "我仙逆",
            "mediaType": "tv",
            "useActiveMedia": False,
            "isCorrection": False,
            "confidence": 0.8,
            "source": "llm",
        }
        with patch.object(AiIntentRouter, "route", return_value=route):
            routed, immediate = self.service._prepare_ai_routed_question(
                "看一下我仙逆的缺失集",
                ai_config=self.config,
                conversation_key="chat:1",
            )

        self.assertEqual(routed, "查看一下仙逆的缺失集")
        self.assertEqual(immediate, "")

    def test_correction_updates_active_media_for_followup(self):
        class FakeIdentityService:
            def resolve(self, query, preferred_type=""):
                return {
                    "identity": {"title": "仙逆", "type": "series", "tmdbId": "123", "year": "2023"},
                    "embyItem": {"Id": "series-1", "Name": "仙逆", "Type": "Series"},
                    "candidates": [],
                    "ambiguous": False,
                }

        self.service._media_identity_service = lambda: FakeIdentityService()
        correction = {
            "intent": "media_correction",
            "mediaTitle": "仙逆",
            "mediaType": "tv",
            "useActiveMedia": False,
            "isCorrection": True,
            "confidence": 0.99,
            "source": "llm",
        }
        with patch.object(AiIntentRouter, "route", return_value=correction):
            _, immediate = self.service._prepare_ai_routed_question(
                "不是完美世界，是仙逆",
                ai_config=self.config,
                conversation_key="chat:2",
            )
        self.assertIn("《仙逆》", immediate)

        followup = {
            "intent": "media_missing_episodes",
            "mediaTitle": "",
            "mediaType": "tv",
            "useActiveMedia": True,
            "isCorrection": False,
            "confidence": 0.99,
            "source": "llm",
        }
        with patch.object(AiIntentRouter, "route", return_value=followup):
            routed, immediate = self.service._prepare_ai_routed_question(
                "它缺哪几集",
                ai_config=self.config,
                conversation_key="chat:2",
            )
        self.assertEqual(routed, "查看一下仙逆的缺失集")
        self.assertEqual(immediate, "")


if __name__ == "__main__":
    unittest.main()
