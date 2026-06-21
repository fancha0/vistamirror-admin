import pathlib
import tempfile
import time
import unittest

from backend_modules.telegram_commands import TelegramCommandService
from backend_modules.media_identity_service import MediaIdentityService


class TelegramAiMediaQueryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = pathlib.Path(self.temp_dir.name)
        self.service = TelegramCommandService(
            store_path=root / "invites.json",
            event_log_path=root / "events.jsonl",
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_extracts_title_before_library_and_latest_intent(self) -> None:
        question = "看一下完美世界我的库里现在最新的多少集"
        self.assertEqual(self.service._extract_ai_media_keywords(question), ["完美世界"])

    def test_extracts_total_and_update_queries(self) -> None:
        self.assertEqual(self.service._extract_ai_media_keyword("完美世界一共有多少集"), "完美世界")
        self.assertEqual(self.service._extract_ai_media_keyword("牧神记更新到哪一集了"), "牧神记")
        self.assertEqual(self.service._extract_ai_media_keyword("《光阴之外》现在多少集"), "光阴之外")

    def test_episode_summary_deduplicates_versions_and_keeps_latest_index(self) -> None:
        rows = [
            {"Id": "version-a", "SeriesId": "series-1", "ParentIndexNumber": 1, "IndexNumber": 84, "Name": "第84集"},
            {"Id": "version-b", "SeriesId": "series-1", "ParentIndexNumber": 1, "IndexNumber": 84, "Name": "第84集 4K"},
            {"Id": "episode-85", "SeriesId": "series-1", "ParentIndexNumber": 1, "IndexNumber": 85, "Name": "第85集"},
        ]

        seasons, actual_count, season_lines, latest = self.service._summarize_ai_episode_rows(rows, season_meta={1: "S1"})

        self.assertEqual(seasons, 1)
        self.assertEqual(actual_count, 2)
        self.assertEqual(season_lines, ["S1 2集"])
        self.assertTrue(latest.startswith("S01E85"))

    def test_recent_library_evidence_is_latest_index_not_total_count(self) -> None:
        self.service._fetch_latest_items_with_fallback = lambda limit: (
            [
                {
                    "Type": "Episode",
                    "SeriesName": "完美世界",
                    "ParentIndexNumber": 1,
                    "IndexNumber": 168,
                    "Name": "第168集",
                }
            ],
            [],
            None,
        )

        evidence = self.service._resolve_ai_recent_library_highest_episode("完美世界")

        self.assertEqual(evidence["latestEpisodeNumber"], 168)
        self.assertNotIn("episodeCount", evidence)

    def test_episode_query_uses_clean_title_and_separates_latest_from_total(self) -> None:
        episodes = [
            {"Id": "episode-167", "SeriesId": "series-1", "ParentIndexNumber": 1, "IndexNumber": 167, "Name": "第167集"},
        ]

        def fake_emby_get(path: str):
            if path.startswith("/Items?") and ("IncludeItemTypes=Series%2CMovie%2CEpisode" in path or "IncludeItemTypes=Series" in path):
                self.assertIn("SearchTerm=%E5%AE%8C%E7%BE%8E%E4%B8%96%E7%95%8C", path)
                return {"Items": [{"Id": "series-1", "Type": "Series", "Name": "完美世界", "ProviderIds": {}}]}
            if path.startswith("/Items/series-1"):
                return {"Id": "series-1", "Type": "Series", "Name": "完美世界", "ProviderIds": {}}
            if path.startswith("/Shows/series-1/Seasons"):
                return {"Items": [{"IndexNumber": 1, "Name": "Season 1"}]}
            if path.startswith("/Shows/series-1/Episodes"):
                return {"Items": episodes, "TotalRecordCount": len(episodes)}
            if path.startswith("/Items?") and "IncludeItemTypes=Episode" in path:
                return {"Items": episodes}
            raise AssertionError(f"unexpected Emby path: {path}")

        self.service._emby_get = fake_emby_get
        self.service._fetch_latest_items_with_fallback = lambda limit: (
            [
                {
                    "Type": "Episode",
                    "SeriesName": "完美世界",
                    "ParentIndexNumber": 1,
                    "IndexNumber": 168,
                    "Name": "第168集",
                }
            ],
            [],
            None,
        )

        reply = self.service._build_ai_episode_query_reply("看一下完美世界我的库里现在最新的多少集")

        self.assertIn("《完美世界》", reply)
        self.assertIn("S01E168（第168集）", reply)
        self.assertIn("实际可读取单集：1 集", reply)

    def test_structured_reply_persists_active_media_across_service_restart(self) -> None:
        episodes = [
            {"Id": "episode-1", "SeriesId": "series-1", "ParentIndexNumber": 1, "IndexNumber": 1, "Name": "第1集"},
            {"Id": "episode-3", "SeriesId": "series-1", "ParentIndexNumber": 1, "IndexNumber": 3, "Name": "第3集"},
        ]

        def fake_emby_get(path: str):
            if path.startswith("/Items?") and ("IncludeItemTypes=Series%2CMovie%2CEpisode" in path or "IncludeItemTypes=Series" in path):
                return {"Items": [{"Id": "series-1", "Type": "Series", "Name": "完美世界", "ProviderIds": {}}]}
            if path.startswith("/Items/series-1"):
                return {"Id": "series-1", "Type": "Series", "Name": "完美世界", "ProviderIds": {}}
            if path.startswith("/Shows/series-1/Seasons"):
                return {"Items": [{"IndexNumber": 1, "Name": "Season 1"}]}
            if path.startswith("/Shows/series-1/Episodes"):
                return {"Items": episodes, "TotalRecordCount": len(episodes)}
            if path.startswith("/Items?") and "IncludeItemTypes=Episode" in path:
                return {"Items": episodes}
            raise AssertionError(f"unexpected Emby path: {path}")

        self.service._emby_get = fake_emby_get
        self.service._fetch_latest_items_with_fallback = lambda limit: ([], [], None)
        conversation_key = "chat:100"
        first_reply = self.service._build_ai_episode_query_reply("完美世界现在多少集", conversation_key=conversation_key)
        self.service._ai_conversations.remember(conversation_key, question="完美世界现在多少集", answer=first_reply)
        self.service._ai_conversations.set_active_media(
            conversation_key,
            {"title": "完美世界", "type": "series", "embySeriesId": "series-1", "tmdbId": "100"},
        )

        restarted = TelegramCommandService(store_path=self.service.store_path, event_log_path=self.service.event_log_path)
        restarted._emby_get = fake_emby_get
        missing_reply = restarted._build_ai_missing_episode_reply("查看一下缺失的集", conversation_key=conversation_key)

        self.assertIn("《完美世界》", missing_reply)
        self.assertIn("本地内部断档：S01 E02", missing_reply)

    def test_context_budget_no_longer_has_120000_character_cap(self) -> None:
        context = "完" * 150000
        limited = self.service._limit_ai_context_text(
            context,
            ai_config={"contextTokensK": 1024, "maxTokens": 800},
        )
        self.assertEqual(limited, context)

    def test_group_conversation_keys_are_user_isolated(self) -> None:
        first = self.service._build_ai_conversation_key(chat_id=-100, user_id=1, chat_type="supergroup")
        second = self.service._build_ai_conversation_key(chat_id=-100, user_id=2, chat_type="supergroup")
        self.assertNotEqual(first, second)

    def test_media_detail_followup_reuses_active_identity(self) -> None:
        conversation_key = "chat:detail"
        self.service._ai_conversations.set_active_media(
            conversation_key,
            {"title": "完美世界", "type": "series", "embySeriesId": "series-1", "tmdbId": "100"},
        )

        class FakeIdentityService:
            def resolve(self, query: str, *, preferred_type: str = ""):
                if query != "完美世界":
                    raise AssertionError(f"unexpected query: {query}")
                return {
                    "identity": {"title": "完美世界", "year": "2021", "type": "series", "tmdbId": "100"},
                    "embyItem": {"Id": "series-1", "Name": "完美世界", "Type": "Series"},
                    "candidates": [],
                    "ambiguous": False,
                }

            def get_media_detail(self, tmdb_id: str, media_type: str):
                return {
                    "overview": "这是一段剧情简介。",
                    "vote_average": 8.5,
                    "credits": {"cast": [{"name": "演员甲"}, {"name": "演员乙"}]},
                }

        self.service._media_identity_service = lambda: FakeIdentityService()
        reply = self.service._build_ai_media_detail_reply("它简介呢", conversation_key=conversation_key)

        self.assertIn("《完美世界》详情", reply)
        self.assertIn("演员甲、演员乙", reply)
        self.assertIn("这是一段剧情简介", reply)

    def test_missing_request_extracts_explicit_title(self) -> None:
        parsed = self.service._parse_ai_missing_episode_request("查看一下遮天的缺失集")
        self.assertEqual(parsed, {"mode": "explicit", "title": "遮天"})

        followup = self.service._parse_ai_missing_episode_request("查看一下缺失的集")
        self.assertEqual(followup, {"mode": "context", "title": ""})
        self.assertEqual(self.service._extract_ai_media_keyword("查看一下遮天的缺失集"), "遮天")

    def test_explicit_missing_query_never_falls_back_to_active_media(self) -> None:
        conversation_key = "chat:no-cross-title"
        self.service._ai_conversations.set_active_media(
            conversation_key,
            {"title": "完美世界", "type": "series", "embySeriesId": "perfect-world"},
        )

        class NotFoundIdentityService:
            def search_media(self, query: str, *, media_type: str = ""):
                if query != "遮天":
                    raise AssertionError(f"unexpected query: {query}")
                return []

            def resolve(self, query: str, *, preferred_type: str = ""):
                if query != "遮天":
                    raise AssertionError(f"unexpected query: {query}")
                return {"identity": {}, "embyItem": {}, "candidates": [], "ambiguous": False}

        self.service._media_identity_service = lambda: NotFoundIdentityService()
        self.service._emby_get = lambda path: self.fail(f"must not read active series: {path}")

        reply = self.service._build_ai_missing_episode_reply("查看一下遮天的缺失集", conversation_key=conversation_key)

        self.assertIn("TMDB 没有找到《遮天》", reply)
        self.assertNotIn("完美世界", reply)

    def test_global_episode_numbering_does_not_require_each_season_to_start_at_one(self) -> None:
        episodes = [
            *(
                {"ParentIndexNumber": 1, "IndexNumber": number}
                for number in range(1, 254)
            ),
            *(
                {"ParentIndexNumber": 6, "IndexNumber": number}
                for number in range(254, 270)
            ),
        ]

        result = self.service._analyze_ai_local_episode_gaps(episodes)

        self.assertEqual(result["mode"], "global")
        self.assertEqual(result["missing"], {})

    def test_global_episode_numbering_reports_only_real_gap(self) -> None:
        episodes = [
            *(
                {"ParentIndexNumber": 1, "IndexNumber": number}
                for number in range(1, 254)
            ),
            *(
                {"ParentIndexNumber": 6, "IndexNumber": number}
                for number in range(255, 270)
            ),
        ]

        result = self.service._analyze_ai_local_episode_gaps(episodes)

        self.assertEqual(result["mode"], "global")
        self.assertEqual(result["missing"], {0: [254]})

    def test_missing_query_uses_tmdb_minus_emby_inventory(self) -> None:
        class InventoryService:
            def search_media(self, query: str, *, media_type: str = ""):
                return [{"title": "遮天", "year": "2023", "type": "series", "tmdbId": "224839", "score": 100, "rating": 8.8}]

            def query_media_detail(self, tmdb_id: str, media_type: str):
                return {
                    "ok": True,
                    "totalEpisodes": 175,
                    "seasonCounts": {1: 175},
                    "registeredSeasonMap": {1: set(range(1, 176))},
                    "airedSeasonMap": {1: set(range(1, 163))},
                    "futureSeasonMap": {1: set(range(163, 176))},
                    "unknownAirDateMap": {},
                    "lastAiredDate": "2026-05-26",
                    "tmdbQueryCount": 2,
                }

            def query_library_exists(self, identity):
                return {
                    "ok": True,
                    "exists": True,
                    "embyItem": {"Id": "zhe-tian", "Name": "遮天", "Type": "Series"},
                    "seasonMap": {1: set(range(1, 163))},
                    "specials": [],
                    "duplicates": [],
                    "embyQueryCount": 2,
                }

            def compare_episode_inventory(self, expected, existing):
                return MediaIdentityService.compare_episode_inventory(expected, existing)

        self.service._media_identity_service = lambda: InventoryService()
        reply = self.service._build_ai_missing_episode_reply("查看一下遮天的缺失集", conversation_key="chat:zhe-tian")

        self.assertIn("TMDB：已播出 162 集，登记 175 集", reply)
        self.assertIn("Emby：已有 162 集", reply)
        self.assertIn("缺失：无", reply)
        self.assertIn("未来未播：E163-E175（不计入缺失）", reply)

        rich = self.service._build_ai_missing_episode_reply(
            "查看一下遮天的缺失集",
            conversation_key="chat:zhe-tian",
            rich=True,
            chat_id="100",
        )
        self.assertIsInstance(rich, dict)
        self.assertIn("执行 1 次搜索，查询 4 次数据", rich["fallback_text"])
        self.assertIn("最后播出   | 2026-05-26", rich["fallback_text"])
        self.assertIsNone(rich["reply_markup"])

    def test_missing_report_uses_sectioned_markdown_and_button(self) -> None:
        reply = self.service._missing_episode_report_reply(
            {
                "title": "遮天",
                "year": "2023",
                "tmdbId": "224839",
                "registeredCount": 175,
                "airedCount": 162,
                "localCount": 160,
                "missingText": "E158、E161",
                "missingLabels": ["E158", "E161"],
                "futureText": "E163-E175",
                "unknownText": "",
                "lastAiredDate": "2026-05-26",
                "searchCount": 1,
                "dataQueryCount": 4,
                "memoryText": "遮天缺失 E158、E161",
            },
            chat_id="100",
        )

        self.assertEqual(reply["parse_mode"], "MarkdownV2")
        self.assertIn("执行 1 次搜索，查询 4 次数据", reply["text"])
        self.assertIn("```text\n项目       | 详情", reply["text"])
        self.assertIn("缺失的剧集：E158、E161", reply["fallback_text"])
        self.assertEqual(reply["reply_markup"]["inline_keyboard"][0][0]["text"], "生成搜索清单")

    def test_missing_report_without_missing_has_no_button(self) -> None:
        reply = self.service._missing_episode_report_reply(
            {
                "title": "遮天",
                "year": "2023",
                "missingText": "无",
                "missingLabels": [],
                "searchCount": 1,
                "dataQueryCount": 4,
            }
        )

        self.assertIsNone(reply["reply_markup"])
        self.assertNotIn("需要生成", reply["fallback_text"])

    def test_missing_search_callback_generates_at_most_fifty_keywords(self) -> None:
        sent = []
        answered = []

        class FakeSender:
            def answer_callback_query(self, **kwargs):
                answered.append(kwargs)

            def send_text(self, **kwargs):
                sent.append(kwargs)

        self.service.sender = FakeSender()
        self.service._pending_missing_searches["action"] = {
            "title": "遮天",
            "labels": [f"E{number}" for number in range(1, 56)],
            "chatId": "100",
            "createdAt": time.time(),
        }

        self.service._handle_missing_search_callback(
            data="missing_search:action",
            token="token",
            callback_id="callback",
            chat_id="100",
        )

        self.assertTrue(answered)
        self.assertEqual(len(sent), 1)
        self.assertIn("遮天 E50", sent[0]["text"])
        self.assertNotIn("遮天 E51\n", sent[0]["text"])
        self.assertIn("还有 5 集未展示", sent[0]["text"])

    def test_expired_missing_search_callback_does_not_generate_list(self) -> None:
        sent = []
        answered = []

        class FakeSender:
            def answer_callback_query(self, **kwargs):
                answered.append(kwargs)

            def send_text(self, **kwargs):
                sent.append(kwargs)

        self.service.sender = FakeSender()
        self.service._pending_missing_searches["expired"] = {
            "title": "遮天",
            "labels": ["E1"],
            "chatId": "100",
            "createdAt": time.time() - 901,
        }

        self.service._handle_missing_search_callback(
            data="missing_search:expired",
            token="token",
            callback_id="callback",
            chat_id="100",
        )

        self.assertFalse(sent)
        self.assertIn("已过期", answered[0]["text"])


if __name__ == "__main__":
    unittest.main()
