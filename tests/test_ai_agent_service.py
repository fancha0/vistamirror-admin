import pathlib
import tempfile
import unittest
from unittest.mock import patch

from backend_modules.ai_agent_service import AIAgentService
from backend_modules.ai_context_service import AIContextService
from backend_modules.ai_tool_provider import AIToolProvider
from backend_modules.ai_tool_registry import AIToolRegistry
from backend_modules.telegram_commands import TelegramCommandService


class AIAgentServiceTests(unittest.TestCase):
    def test_dispatches_read_tool_before_falling_back_to_chat(self) -> None:
        registry = AIToolRegistry()
        registry.register(
            name="query_playback_history",
            kind="read",
            description="查询最近播放",
            predicate=lambda text: "最近播放" in text,
            handler=lambda text: "播放历史摘要：最近播放 8 条",
        )
        agent = AIAgentService(
            registry=registry,
            prepare_question=lambda question: (question, ""),
            build_execution_proposal=lambda question: None,
        )

        result = agent.prepare_and_dispatch("最近播放是什么")

        self.assertTrue(result.handled)
        self.assertEqual(result.tool.name, "query_playback_history")
        self.assertEqual(result.reply, "播放历史摘要：最近播放 8 条")

    def test_route_reply_short_circuits_tool_dispatch(self) -> None:
        registry = AIToolRegistry()
        registry.register(
            name="query_playback_history",
            kind="read",
            description="查询最近播放",
            predicate=lambda text: True,
            handler=lambda text: "不应该执行",
        )
        agent = AIAgentService(
            registry=registry,
            prepare_question=lambda question: (question, "请带上要查询的影视名称。"),
            build_execution_proposal=lambda question: None,
        )

        result = agent.prepare_and_dispatch("它多少集")

        self.assertTrue(result.handled)
        self.assertEqual(result.source, "route")
        self.assertEqual(result.reply, "请带上要查询的影视名称。")


class TelegramAgentIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = pathlib.Path(self.temp_dir.name)
        self.service = TelegramCommandService(
            store_path=root / "invites.json",
            event_log_path=root / "events.jsonl",
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_agent_dispatches_playback_history_tool(self) -> None:
        with patch(
            "backend_modules.ai_query_service.AIQueryService.build_playback_history_context",
            return_value="播放历史摘要：最近记录 10 条",
        ):
            agent = self.service._dispatch_ai_agent(
                "最近播放历史",
                ai_config={},
                conversation_key="chat:100",
                chat_id="100",
                rich=True,
            )

            result = agent.prepare_and_dispatch("最近播放历史")

        self.assertTrue(result.handled)
        self.assertEqual(result.tool.name, "query_playback_history")
        self.assertEqual(result.reply, "播放历史摘要：最近记录 10 条")
        self.assertEqual(result.subagent, "playback-analyst")

    def test_agent_dispatches_missing_query_with_active_media_context(self) -> None:
        self.service._ai_conversations.set_active_media(
            "chat:missing",
            {"title": "完美世界", "type": "series", "embySeriesId": "perfect-world", "tmdbId": "100"},
        )

        class InventoryService:
            def query_media_detail(self, tmdb_id: str, media_type: str):
                return {
                    "ok": True,
                    "totalEpisodes": 3,
                    "seasonCounts": {1: 3},
                    "registeredSeasonMap": {1: {1, 2, 3}},
                    "airedSeasonMap": {1: {1, 2, 3}},
                    "futureSeasonMap": {},
                    "unknownAirDateMap": {},
                    "lastAiredDate": "2026-06-27",
                    "tmdbQueryCount": 2,
                }

            def query_library_exists(self, identity):
                return {
                    "ok": True,
                    "exists": True,
                    "embyItem": {"Id": "perfect-world", "Name": "完美世界", "Type": "Series"},
                    "seasonMap": {1: {1, 3}},
                    "specials": [],
                    "duplicates": [],
                    "embyQueryCount": 2,
                }

            def compare_episode_inventory(self, expected, existing):
                return {
                    "missing": [2],
                    "missingLabels": ["E02"],
                    "future": [],
                    "futureLabels": [],
                    "unknownAirDate": [],
                    "unknownAirDateLabels": [],
                    "extras": [],
                    "extraLabels": [],
                    "unmapped": [],
                    "specials": [],
                    "expectedCount": 3,
                    "registeredCount": 3,
                    "existingCount": 2,
                    "localEpisodeCount": 2,
                    "mode": "global",
                }

        self.service._media_identity_service = lambda: InventoryService()
        agent = self.service._dispatch_ai_agent(
            "查看一下缺失的集",
            ai_config={},
            conversation_key="chat:missing",
            chat_id="100",
            rich=True,
        )

        result = agent.prepare_and_dispatch("查看一下缺失的集")

        self.assertTrue(result.handled)
        self.assertEqual(result.tool.name, "query_missing_episodes")
        self.assertIsInstance(result.reply, dict)
        self.assertIn("完美世界", result.reply["fallback_text"])
        self.assertIn("E02", result.reply["fallback_text"])
        self.assertEqual(result.subagent, "media-librarian")

    def test_tool_provider_limits_tools_by_subagent(self) -> None:
        provider = AIToolProvider(
            self.service,
            conversation_key="chat:100",
            chat_id="100",
            rich=True,
        )

        playback_tools = provider.allowed_tool_names_for_question("最近谁看得最多")
        resource_tools = provider.allowed_tool_names_for_question("帮我转存这个115链接")

        self.assertEqual(playback_tools, ["query_playback_history"])
        self.assertEqual(resource_tools, ["search_hdhive_resource", "transfer_115_share"])

    def test_subagent_registry_exposes_instruction_metadata(self) -> None:
        provider = AIToolProvider(
            self.service,
            conversation_key="chat:100",
            chat_id="100",
            rich=True,
        )

        subagents = provider.build_subagents()
        media = subagents.get("media-librarian")
        playback = subagents.get("playback-analyst")
        resource = subagents.get("resource-operator")

        self.assertIsNotNone(media)
        self.assertIsNotNone(playback)
        self.assertIsNotNone(resource)
        self.assertIn("确认作品身份", media.instruction)
        self.assertIn("最近播放", playback.instruction)
        self.assertIn("资源搜索和转存", resource.instruction)

    def test_ai_context_includes_subagent_instruction_for_media_question(self) -> None:
        self.service._build_ai_library_stats_context = lambda: "当前媒体库统计：电影 1 部。"
        self.service._build_ai_focus_media_context = lambda question: ""
        self.service._build_ai_category_listing_context = lambda question: ""
        self.service._fetch_latest_items_with_fallback = lambda limit=8: ([], [], "")
        self.service._emby_get = lambda path: []

        context_service = AIContextService(
            self.service,
            conversation_key="chat:100",
            chat_id="100",
        )

        messages = context_service.build_messages("完美世界现在多少集", ai_config={})

        self.assertEqual(messages[0]["role"], "system")
        self.assertIn("当前子代理：media-librarian", messages[0]["content"])
        self.assertIn("先确认作品身份", messages[0]["content"])


if __name__ == "__main__":
    unittest.main()
