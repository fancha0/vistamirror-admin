import pathlib
import tempfile
import time
import unittest
import json

from backend_modules.ai_runtime_service import AIRuntimeService
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
            if path.startswith("/Items?Ids=series-1&"):
                return {"Items": [{"Id": "series-1", "Type": "Series", "Name": "完美世界", "ProviderIds": {}}]}
            if "ParentId=series-1" in path and "IncludeItemTypes=Season" in path:
                return {"Items": [{"IndexNumber": 1, "Name": "Season 1"}]}
            if "ParentId=series-1" in path and "IncludeItemTypes=Episode" in path:
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
            if path.startswith("/Items?Ids=series-1&"):
                return {"Items": [{"Id": "series-1", "Type": "Series", "Name": "完美世界", "ProviderIds": {}}]}
            if "ParentId=series-1" in path and "IncludeItemTypes=Season" in path:
                return {"Items": [{"IndexNumber": 1, "Name": "Season 1"}]}
            if "ParentId=series-1" in path and "IncludeItemTypes=Episode" in path:
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
        self.assertIn("本地内部断档：S01E02", missing_reply)

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

    def test_library_exists_query_keeps_going_when_resolution_has_identity_only(self) -> None:
        class InventoryService:
            def resolve(self, query: str, *, preferred_type: str = ""):
                if query != "釜山行":
                    raise AssertionError(f"unexpected query: {query}")
                return {
                    "identity": {"title": "釜山行", "year": "2016", "type": "movie", "tmdbId": "396535"},
                    "embyItem": {},
                    "candidates": [],
                    "ambiguous": False,
                }

            def query_library_exists(self, identity):
                if str(identity.get("tmdbId") or "") != "396535":
                    raise AssertionError(f"unexpected identity: {identity}")
                return {
                    "ok": True,
                    "exists": True,
                    "embyItem": {"Id": "busanhaeng", "Name": "釜山行", "Type": "Movie", "ProductionYear": 2016},
                    "seasonMap": {},
                }

        self.service._media_identity_service = lambda: InventoryService()
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:exists",
            chat_id="100",
            rich=True,
        )
        provider = runtime.tool_provider()
        registry = provider.build_registry()

        dispatched = registry.dispatch(
            "媒体库里釜山行有吗",
            allowed_names=provider.allowed_tool_names_for_question("媒体库里釜山行有吗"),
        )

        self.assertIsNotNone(dispatched)
        self.assertEqual(dispatched[0].name, "query_library_exists")
        self.assertIn("媒体库中已存在《釜山行》", dispatched[1])
        self.assertIn("Emby ID：busanhaeng", dispatched[1])

    def test_library_directory_query_dispatches_separately_from_exists_query(self) -> None:
        def fake_emby_get(path: str):
            if path == "/Sessions":
                return [{"UserId": "user-1"}]
            if path == "/Library/VirtualFolders":
                return [{"ItemId": "asia-lib", "Name": "亚洲电影", "CollectionType": "movies"}]
            if path.startswith("/Users/user-1/Items?") and "ParentId=asia-lib" in path:
                return {
                    "Items": [
                        {
                            "Id": "movie-1",
                            "Name": "前往釜山",
                            "Type": "Movie",
                            "ProductionYear": 2016,
                            "Path": "/媒体库/电影/亚洲电影/前往釜山 (2016)",
                            "Genres": ["惊悚"],
                        },
                        {
                            "Id": "movie-2",
                            "Name": "釜山行2：半岛",
                            "Type": "Movie",
                            "ProductionYear": 2020,
                            "Path": "/媒体库/电影/亚洲电影/釜山行2：半岛 (2020)",
                            "Genres": ["动作"],
                        },
                    ]
                }
            raise AssertionError(f"unexpected path: {path}")

        self.service._emby_get = fake_emby_get
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:directory",
            chat_id="100",
            rich=True,
        )
        provider = runtime.tool_provider()
        registry = provider.build_registry()

        dispatched = registry.dispatch(
            "我库里有什么亚洲电影",
            allowed_names=provider.allowed_tool_names_for_question("我库里有什么亚洲电影"),
        )

        self.assertIsNotNone(dispatched)
        self.assertEqual(dispatched[0].name, "query_library_directory")
        self.assertIn("分类资源查询：亚洲电影", dispatched[1])
        self.assertIn("查询来源：库节点目录", dispatched[1])
        self.assertIn("前往釜山", dispatched[1])
        self.assertIn("釜山行2：半岛", dispatched[1])

    def test_library_directory_query_reports_unconfigured_when_no_directory_or_library_matches(self) -> None:
        def fake_emby_get(path: str):
            if path == "/Sessions":
                return [{"UserId": "user-1"}]
            if path == "/Library/VirtualFolders":
                return [{"ItemId": "movies", "Name": "电影", "CollectionType": "movies"}]
            if path.startswith("/Users/user-1/Items?") and "ParentId=movies" in path:
                return {"Items": []}
            raise AssertionError(f"unexpected path: {path}")

        self.service._emby_get = fake_emby_get
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:directory-fallback",
            chat_id="100",
            rich=True,
        )
        provider = runtime.tool_provider()
        registry = provider.build_registry()

        dispatched = registry.dispatch(
            "我库里有什么亚洲电影",
            allowed_names=provider.allowed_tool_names_for_question("我库里有什么亚洲电影"),
        )

        self.assertIsNotNone(dispatched)
        self.assertEqual(dispatched[0].name, "query_library_directory")
        self.assertIn("未配置目录分类：亚洲电影", dispatched[1])
        self.assertIn("未找到对应目录或库节点", dispatched[1])
        self.assertNotIn("Emby 元数据匹配", dispatched[1])

    def test_library_directory_query_prefers_local_directory_config_when_present(self) -> None:
        media_root = pathlib.Path(self.temp_dir.name) / "media-root"
        asia_dir = media_root / "电影" / "亚洲电影"
        asia_dir.mkdir(parents=True)
        (asia_dir / "前往釜山 (2016)").mkdir()
        (asia_dir / "首尔之春 (2023)").mkdir()
        self.service.store_path.write_text(
            json.dumps(
                {
                    "libraryDirectoryConfig": {
                        "roots": [
                            {"path": str(media_root), "name": "本地媒体库", "enabled": True},
                        ]
                    }
                }
            ),
            encoding="utf-8",
        )
        self.service._emby_get = lambda path: self.fail(f"filesystem query should not hit emby: {path}")
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:directory-local",
            chat_id="100",
            rich=True,
        )
        provider = runtime.tool_provider()
        registry = provider.build_registry()

        dispatched = registry.dispatch(
            "我库里有什么亚洲电影",
            allowed_names=provider.allowed_tool_names_for_question("我库里有什么亚洲电影"),
        )

        self.assertIsNotNone(dispatched)
        self.assertEqual(dispatched[0].name, "query_library_directory")
        self.assertIn("分类资源查询：亚洲电影", dispatched[1])
        self.assertIn("查询来源：本地目录", dispatched[1])
        self.assertIn("前往釜山", dispatched[1])
        self.assertIn("首尔之春", dispatched[1])
        self.assertNotIn("Emby 元数据匹配", dispatched[1])

    def test_library_directory_query_supports_configured_alias_categories(self) -> None:
        media_root = pathlib.Path(self.temp_dir.name) / "media-root"
        korea_dir = media_root / "电影" / "亚洲电影" / "韩国电影"
        korea_dir.mkdir(parents=True)
        (korea_dir / "首尔之春 (2023)").mkdir()
        self.service.store_path.write_text(
            json.dumps(
                {
                    "libraryDirectoryConfig": {
                        "roots": [
                            {
                                "path": str(media_root),
                                "enabled": True,
                                "maxDepth": 4,
                                "categories": [
                                    {
                                        "label": "亚洲电影",
                                        "aliases": ["韩影", "韩国电影"],
                                        "path": "电影/亚洲电影/韩国电影",
                                    }
                                ],
                            }
                        ]
                    }
                }
            ),
            encoding="utf-8",
        )
        self.service._emby_get = lambda path: self.fail(f"alias-based filesystem query should not hit emby: {path}")
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:directory-alias",
            chat_id="100",
            rich=True,
        )
        provider = runtime.tool_provider()
        registry = provider.build_registry()

        dispatched = registry.dispatch(
            "我库里有什么韩影",
            allowed_names=provider.allowed_tool_names_for_question("我库里有什么韩影"),
        )

        self.assertIsNotNone(dispatched)
        self.assertEqual(dispatched[0].name, "query_library_directory")
        self.assertIn("查询来源：本地目录", dispatched[1])
        self.assertIn("首尔之春", dispatched[1])
        self.assertNotIn("Emby 元数据匹配", dispatched[1])

    def test_library_directory_query_can_follow_structured_alias_path_without_recursive_scan(self) -> None:
        media_root = pathlib.Path(self.temp_dir.name) / "media-root"
        korea_dir = media_root / "电影" / "亚洲电影" / "韩国电影"
        korea_dir.mkdir(parents=True)
        (korea_dir / "首尔之春 (2023)").mkdir()
        self.service.store_path.write_text(
            json.dumps(
                {
                    "libraryDirectoryConfig": {
                        "roots": [
                            {"path": str(media_root), "enabled": True},
                        ]
                    }
                }
            ),
            encoding="utf-8",
        )
        self.service._emby_get = lambda path: self.fail(f"structured filesystem query should not hit emby: {path}")
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:directory-structured-alias",
            chat_id="100",
            rich=True,
        )
        provider = runtime.tool_provider()
        registry = provider.build_registry()

        dispatched = registry.dispatch(
            "我库里有什么韩影",
            allowed_names=provider.allowed_tool_names_for_question("我库里有什么韩影"),
        )

        self.assertIsNotNone(dispatched)
        self.assertEqual(dispatched[0].name, "query_library_directory")
        self.assertIn("查询来源：本地目录", dispatched[1])
        self.assertIn("首尔之春", dispatched[1])
        self.assertNotIn("Emby 元数据匹配", dispatched[1])

    def test_library_directory_query_supports_legacy_directories_config_field(self) -> None:
        media_root = pathlib.Path(self.temp_dir.name) / "media-root"
        asia_dir = media_root / "电影" / "亚洲电影"
        asia_dir.mkdir(parents=True)
        (asia_dir / "前往釜山 (2016)").mkdir()
        self.service.store_path.write_text(
            json.dumps(
                {
                    "libraryDirectoryConfig": {
                        "directories": [
                            {"path": str(media_root), "enabled": True},
                        ]
                    }
                }
            ),
            encoding="utf-8",
        )
        self.service._emby_get = lambda path: self.fail(f"legacy filesystem query should not hit emby: {path}")
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:directory-legacy",
            chat_id="100",
            rich=True,
        )
        provider = runtime.tool_provider()
        registry = provider.build_registry()

        dispatched = registry.dispatch(
            "我库里有什么亚洲电影",
            allowed_names=provider.allowed_tool_names_for_question("我库里有什么亚洲电影"),
        )

        self.assertIsNotNone(dispatched)
        self.assertEqual(dispatched[0].name, "query_library_directory")
        self.assertIn("查询来源：本地目录", dispatched[1])
        self.assertIn("前往釜山", dispatched[1])
        self.assertNotIn("Emby 元数据匹配", dispatched[1])

    def test_library_directory_query_exact_emby_library_name_is_allowed_fallback(self) -> None:
        def fake_emby_get(path: str):
            if path == "/Sessions":
                return [{"UserId": "user-1"}]
            if path == "/Library/VirtualFolders":
                return [{"ItemId": "asia-lib", "Name": "亚洲电影", "CollectionType": "movies"}]
            if path.startswith("/Users/user-1/Items?") and "ParentId=asia-lib" in path:
                return {
                    "Items": [
                        {
                            "Id": "movie-1",
                            "Name": "前往釜山",
                            "Type": "Movie",
                            "ProductionYear": 2016,
                            "Path": "/媒体库/电影/亚洲电影/前往釜山 (2016)",
                            "Genres": ["惊悚"],
                        }
                    ]
                }
            raise AssertionError(f"unexpected path: {path}")

        self.service._emby_get = fake_emby_get
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:directory-library-only",
            chat_id="100",
            rich=True,
        )
        provider = runtime.tool_provider()
        registry = provider.build_registry()

        dispatched = registry.dispatch(
            "我库里有什么亚洲电影",
            allowed_names=provider.allowed_tool_names_for_question("我库里有什么亚洲电影"),
        )

        self.assertIsNotNone(dispatched)
        self.assertEqual(dispatched[0].name, "query_library_directory")
        self.assertIn("查询来源：库节点目录", dispatched[1])
        self.assertIn("前往釜山", dispatched[1])
        self.assertNotIn("Emby 元数据匹配", dispatched[1])

    def test_library_directory_query_strict_mode_does_not_match_deep_unconfigured_folder_by_name(self) -> None:
        media_root = pathlib.Path(self.temp_dir.name) / "media-root"
        deep_dir = media_root / "收藏" / "旧分类" / "亚洲电影"
        deep_dir.mkdir(parents=True)
        (deep_dir / "不该命中的影片 (2024)").mkdir()
        self.service.store_path.write_text(
            json.dumps(
                {
                    "libraryDirectoryConfig": {
                        "roots": [
                            {"path": str(media_root), "enabled": True},
                        ]
                    }
                }
            ),
            encoding="utf-8",
        )

        def fake_emby_get(path: str):
            if path == "/Sessions":
                return [{"UserId": "user-1"}]
            if path == "/Library/VirtualFolders":
                return [{"ItemId": "movies", "Name": "电影", "CollectionType": "movies"}]
            if path.startswith("/Users/user-1/Items?") and "ParentId=movies" in path:
                return {"Items": []}
            raise AssertionError(f"unexpected path: {path}")

        self.service._emby_get = fake_emby_get
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:directory-no-deep-scan",
            chat_id="100",
            rich=True,
        )
        provider = runtime.tool_provider()
        registry = provider.build_registry()

        dispatched = registry.dispatch(
            "我库里有什么亚洲电影",
            allowed_names=provider.allowed_tool_names_for_question("我库里有什么亚洲电影"),
        )

        self.assertIsNotNone(dispatched)
        self.assertEqual(dispatched[0].name, "query_library_directory")
        self.assertIn("未配置目录分类：亚洲电影", dispatched[1])
        self.assertNotIn("不该命中的影片", dispatched[1])

    def test_missing_request_extracts_explicit_title(self) -> None:
        parsed = self.service._parse_ai_missing_episode_request("查看一下遮天的缺失集")
        self.assertEqual(parsed, {"mode": "title", "title": "遮天"})
        parsed_with_year = self.service._parse_ai_missing_episode_request("查看一下《仙逆》(2023)缺失集")
        self.assertEqual(parsed_with_year, {"mode": "title", "title": "仙逆", "year": "2023"})
        meta = self.service._parse_ai_missing_episode_request("查询媒体缺失集的方式")
        self.assertEqual(meta, {})

        followup = self.service._parse_ai_missing_episode_request("查看一下缺失的集")
        self.assertEqual(followup, {"mode": "context", "title": ""})
        self.assertEqual(self.service._extract_ai_media_keyword("查看一下遮天的缺失集"), "遮天")
        self.assertEqual(self.service._extract_ai_media_keyword("查询媒体缺失集的方式"), "")

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

            def search_local_candidates(self, query: str, **kwargs):
                return [{"embyItemId": "zhe-tian", "title": "遮天", "year": "2023", "type": "series", "tmdbId": "224839", "episodeCount": 162, "score": 1180, "scoreReason": "标题完全匹配 / TMDB 一致 / Series / 162 集", "isTitleExact": True, "isSeries": True}]

            def query_library_exists_by_tmdb(self, identity):
                return {
                    "ok": True,
                    "exists": True,
                    "embyItem": {"Id": "zhe-tian", "Name": "遮天", "Type": "Series", "ProviderIds": {"Tmdb": "224839"}},
                    "seasonMap": {1: set(range(1, 163))},
                    "specials": [],
                    "duplicates": [],
                    "embyQueryCount": 2,
                }

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
        self.assertIn("成功映射 162 集", reply)
        self.assertIn("缺集判断：可靠", reply)

        rich = self.service._build_ai_missing_episode_reply(
            "查看一下遮天的缺失集",
            conversation_key="chat:zhe-tian",
            rich=True,
            chat_id="100",
        )
        self.assertIsInstance(rich, dict)
        self.assertIn("执行 1 次搜索，查询 4 次数据", rich["fallback_text"])
        self.assertIn("📝 《遮天》（2023）— 缺失集", rich["fallback_text"])
        self.assertIn("Emby 媒体库状态：", rich["fallback_text"])
        self.assertIn("Season 1", rich["fallback_text"])
        self.assertIn("未来未播：S01E163-E175（不计入缺失）", rich["fallback_text"])
        self.assertIsNone(rich["reply_markup"])

    def test_missing_query_ambiguous_tmdb_prefers_unique_emby_library_hit(self) -> None:
        class InventoryService:
            def search_media(self, query: str, *, media_type: str = ""):
                return [
                    {"title": "遮天", "year": "2023", "type": "series", "tmdbId": "224839", "score": 100, "rating": 8.8},
                    {"title": "遮天", "year": "2025", "type": "series", "tmdbId": "278875", "score": 100, "rating": 7.1},
                    {"title": "黑手遮天", "year": "2023", "type": "series", "tmdbId": "203202", "score": 70, "rating": 6.1},
                ]

            def query_library_exists_by_tmdb(self, identity):
                tmdb_id = str(identity.get("tmdbId") or "")
                if tmdb_id == "224839":
                    return {
                        "ok": True,
                        "exists": True,
                        "embyItem": {"Id": "zhe-tian", "Name": "遮天", "Type": "Series", "ProviderIds": {"Tmdb": "224839"}},
                        "seasonMap": {1: set(range(1, 163))},
                        "specials": [],
                        "duplicates": [],
                        "embyQueryCount": 2,
                    }
                return {"ok": True, "exists": False, "embyItem": {}, "embyQueryCount": 1}

            def search_local_candidates(self, query: str, **kwargs):
                raise AssertionError("unique tmdb library hit should bypass local title candidates")

            def query_media_detail(self, tmdb_id: str, media_type: str):
                return {
                    "ok": True,
                    "totalEpisodes": 175,
                    "registeredEpisodes": 175,
                    "airedEpisodes": 162,
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
                    "embyItem": {"Id": "zhe-tian", "Name": "遮天", "Type": "Series", "ProviderIds": {"Tmdb": "224839"}},
                    "seasonMap": {1: set(range(1, 163))},
                    "specials": [],
                    "duplicates": [],
                    "embyQueryCount": 2,
                }

            def compare_episode_inventory(self, expected, existing):
                return MediaIdentityService.compare_episode_inventory(expected, existing)

        self.service._media_identity_service = lambda: InventoryService()
        rich = self.service._build_ai_missing_episode_reply(
            "遮天缺失集",
            conversation_key="chat:zhe-tian",
            rich=True,
            chat_id="100",
        )

        self.assertIn("📝 《遮天》（2023）— 缺失集", rich["fallback_text"])
        self.assertNotIn("有多个同名候选", rich["fallback_text"])
        self.assertNotIn("黑手遮天", rich["fallback_text"])

    def test_missing_query_ambiguous_tmdb_reply_excludes_near_match_titles(self) -> None:
        class InventoryService:
            def search_media(self, query: str, *, media_type: str = ""):
                return [
                    {"title": "遮天", "year": "2023", "type": "series", "tmdbId": "224839", "score": 100, "rating": 8.8},
                    {"title": "遮天", "year": "2025", "type": "series", "tmdbId": "278875", "score": 100, "rating": 7.1},
                    {"title": "黑手遮天", "year": "2023", "type": "series", "tmdbId": "203202", "score": 70, "rating": 6.1},
                    {"title": "素手遮天", "year": "2018", "type": "series", "tmdbId": "196270", "score": 70, "rating": 6.0},
                ]

            def query_library_exists_by_tmdb(self, identity):
                return {"ok": True, "exists": False, "embyItem": {}, "embyQueryCount": 1}

        self.service._media_identity_service = lambda: InventoryService()
        reply = self.service._build_ai_missing_episode_reply("遮天缺失集", conversation_key="chat:zhe-tian")

        self.assertIn("《遮天》有多个同名候选", reply)
        self.assertIn("TMDB 224839", reply)
        self.assertIn("TMDB 278875", reply)
        self.assertNotIn("黑手遮天", reply)
        self.assertNotIn("素手遮天", reply)

    def test_missing_query_mapping_anomaly_does_not_emit_definite_gap(self) -> None:
        class InventoryService:
            def search_media(self, query: str, *, media_type: str = ""):
                return [{"title": "仙逆", "year": "2023", "type": "series", "tmdbId": "223911", "score": 100}]

            def search_local_candidates(self, query: str, **kwargs):
                return [{"embyItemId": "xian-ni", "title": "仙逆", "year": "2023", "type": "series", "tmdbId": "223911", "episodeCount": 26, "score": 1180, "scoreReason": "标题完全匹配 / TMDB 一致 / Series / 26 集", "isTitleExact": True, "isSeries": True}]

            def query_library_exists_by_tmdb(self, identity):
                return {
                    "ok": True,
                    "exists": True,
                    "embyItem": {"Id": "xian-ni", "Name": "仙逆", "Type": "Series", "ProviderIds": {"Tmdb": "223911"}},
                    "seasonMap": {1: set(range(1, 11))},
                    "episodeRows": 26,
                    "episodeItems": [
                        {"season": 1, "episode": number, "name": f"仙逆 第{number}集", "sortName": "", "originalTitle": "", "path": f"/xianni/{number}.mkv"}
                        for number in range(1, 11)
                    ]
                    + [
                        {"season": 0, "episode": 0, "name": "未映射条目", "sortName": "", "originalTitle": "", "path": f"/xianni/unknown-{number}.mkv"}
                        for number in range(11, 27)
                    ],
                    "specials": [],
                    "duplicates": [],
                    "embyQueryCount": 2,
                }

            def query_media_detail(self, tmdb_id: str, media_type: str):
                return {
                    "ok": True,
                    "totalEpisodes": 200,
                    "registeredEpisodes": 200,
                    "airedEpisodes": 147,
                    "seasonCounts": {1: 200},
                    "registeredSeasonMap": {1: set(range(1, 201))},
                    "airedSeasonMap": {1: set(range(1, 148))},
                    "futureSeasonMap": {1: set(range(148, 201))},
                    "unknownAirDateMap": {},
                    "lastAiredDate": "2026-06-28",
                    "tmdbQueryCount": 2,
                }

            def query_library_exists(self, identity):
                rows = [
                    {"season": 1, "episode": number, "name": f"仙逆 第{number}集", "sortName": "", "originalTitle": "", "path": f"/xianni/{number}.mkv"}
                    for number in range(1, 11)
                ]
                rows.extend(
                    {"season": 0, "episode": 0, "name": "未映射条目", "sortName": "", "originalTitle": "", "path": f"/xianni/unknown-{number}.mkv"}
                    for number in range(11, 27)
                )
                return {
                    "ok": True,
                    "exists": True,
                    "embyItem": {"Id": "xian-ni", "Name": "仙逆", "Type": "Series"},
                    "seasonMap": {1: set(range(1, 11))},
                    "episodeRows": 26,
                    "episodeItems": rows,
                    "specials": [],
                    "duplicates": [],
                    "embyQueryCount": 2,
                }

        self.service._media_identity_service = lambda: InventoryService()
        rich = self.service._build_ai_missing_episode_reply(
            "查看一下仙逆的缺失集",
            conversation_key="chat:xianni",
            rich=True,
            chat_id="100",
        )

        self.assertIn("⚠️ 待确认", rich["fallback_text"])
        self.assertIn("当前编号映射异常，以下缺失仅供参考，不建议直接搜索。", rich["fallback_text"])
        self.assertIn("原因：编号映射异常，无法可靠判断缺集", rich["fallback_text"])
        self.assertIn("参考缺失：S01E11-E147", rich["fallback_text"])
        self.assertNotIn("需要生成这些缺失集的搜索清单吗？", rich["fallback_text"])
        self.assertIsNone(rich["reply_markup"])

    def test_missing_query_prefers_exact_tmdb_library_hit_before_local_candidates(self) -> None:
        class InventoryService:
            def search_media(self, query: str, *, media_type: str = ""):
                return [{"title": "仙逆", "year": "2023", "type": "series", "tmdbId": "223911", "score": 100}]

            def query_library_exists_by_tmdb(self, identity):
                return {
                    "ok": True,
                    "exists": True,
                    "embyItem": {"Id": "xian-ni", "Name": "仙逆", "Type": "Series", "ProviderIds": {"Tmdb": "223911"}},
                    "seasonMap": {1: {1, 2, 3}},
                    "episodeRows": 3,
                    "episodeItems": [
                        {"season": 1, "episode": 1, "name": "仙逆 第1集", "sortName": "", "originalTitle": "", "path": "/xianni/1.mkv"},
                        {"season": 1, "episode": 2, "name": "仙逆 第2集", "sortName": "", "originalTitle": "", "path": "/xianni/2.mkv"},
                        {"season": 1, "episode": 3, "name": "仙逆 第3集", "sortName": "", "originalTitle": "", "path": "/xianni/3.mkv"},
                    ],
                    "specials": [],
                    "duplicates": [],
                    "embyQueryCount": 2,
                }

            def search_local_candidates(self, query: str, **kwargs):
                raise AssertionError("exact tmdb hit should bypass local title fallback")

            def query_media_detail(self, tmdb_id: str, media_type: str):
                return {
                    "ok": True,
                    "totalEpisodes": 3,
                    "registeredEpisodes": 3,
                    "airedEpisodes": 3,
                    "seasonCounts": {1: 3},
                    "registeredSeasonMap": {1: {1, 2, 3}},
                    "airedSeasonMap": {1: {1, 2, 3}},
                    "futureSeasonMap": {},
                    "unknownAirDateMap": {},
                    "lastAiredDate": "2026-06-28",
                    "tmdbQueryCount": 2,
                }

            def query_library_exists(self, identity):
                return self.query_library_exists_by_tmdb(identity)

        self.service._media_identity_service = lambda: InventoryService()
        reply = self.service._build_ai_missing_episode_reply("查看一下仙逆缺失集", conversation_key="chat:xianni")

        self.assertIn("《仙逆》缺集查询结果：", reply)
        self.assertNotIn("Emby 本地候选", reply)

    def test_missing_query_context_does_not_trust_stale_active_emby_item(self) -> None:
        conversation_key = "chat:xianni"
        self.service._ai_conversations.set_active_media(
            conversation_key,
            {"title": "仙逆", "year": "2023", "type": "series", "tmdbId": "223911", "embySeriesId": "dark"},
        )

        class InventoryService:
            def query_library_exists_by_tmdb(self, identity):
                self_identity = dict(identity)
                if str(self_identity.get("embyId") or "") == "dark":
                    return {"ok": True, "exists": False, "embyItem": {}, "embyQueryCount": 1}
                raise AssertionError(f"unexpected identity: {self_identity}")

            def search_local_candidates(self, query: str, **kwargs):
                return [
                    {"embyItemId": "dark", "title": "暗黑", "year": "2017", "type": "series", "tmdbId": "70523", "episodeCount": 26, "score": 920, "scoreReason": "标题不稳定 / TMDB 不一致 / Series / 26 集", "isTitleExact": False, "isSeries": True},
                    {"embyItemId": "xian-ni", "title": "仙逆", "year": "2023", "type": "series", "tmdbId": "223911", "episodeCount": 147, "score": 1180, "scoreReason": "标题完全匹配 / TMDB 一致 / Series / 147 集", "isTitleExact": True, "isSeries": True},
                ]

        self.service._media_identity_service = lambda: InventoryService()

        rich = self.service._build_ai_missing_episode_reply(
            "查看一下缺失的集",
            conversation_key=conversation_key,
            rich=True,
            chat_id="100",
        )

        self.assertIn("没有在 Emby 中找到与 TMDB 223911 完全一致的作品", rich["fallback_text"])
        self.assertIn("Emby 本地候选", rich["fallback_text"])
        self.assertNotIn("《仙逆》缺集查询结果：", rich["fallback_text"])

    def test_missing_query_does_not_auto_match_dark_for_xianni(self) -> None:
        class InventoryService:
            def search_media(self, query: str, *, media_type: str = ""):
                return [{"title": "仙逆", "year": "2023", "type": "series", "tmdbId": "223911", "score": 100}]

            def query_library_exists_by_tmdb(self, identity):
                return {"ok": True, "exists": False, "embyItem": {}, "embyQueryCount": 1}

            def search_local_candidates(self, query: str, **kwargs):
                return [
                    {"embyItemId": "dark", "title": "暗黑", "year": "2017", "type": "series", "tmdbId": "70523", "episodeCount": 26, "score": 920, "scoreReason": "标题不稳定 / TMDB 不一致 / Series / 26 集", "isTitleExact": False, "isSeries": True},
                    {"embyItemId": "xian-ni", "title": "仙逆", "year": "2023", "type": "series", "tmdbId": "223911", "episodeCount": 147, "score": 1180, "scoreReason": "标题完全匹配 / TMDB 一致 / Series / 147 集", "isTitleExact": True, "isSeries": True},
                ]

        self.service._media_identity_service = lambda: InventoryService()
        rich = self.service._build_ai_missing_episode_reply(
            "查看一下仙逆缺失集",
            conversation_key="chat:xianni",
            rich=True,
            chat_id="100",
        )

        self.assertIn("Emby 本地候选", rich["fallback_text"])
        self.assertIn("暗黑", rich["fallback_text"])
        self.assertIn("仙逆", rich["fallback_text"])
        self.assertEqual(rich["reply_markup"]["inline_keyboard"][0][0]["callback_data"].split(":")[1], "pick")

    def test_missing_query_prefers_emby_missing_episode_data_when_available(self) -> None:
        class InventoryService:
            def search_media(self, query: str, *, media_type: str = ""):
                return [{"title": "遮天", "year": "2023", "type": "series", "tmdbId": "224839", "score": 100}]

            def search_local_candidates(self, query: str, **kwargs):
                return [{"embyItemId": "zhe-tian", "title": "遮天", "year": "2023", "type": "series", "tmdbId": "224839", "episodeCount": 162, "score": 1180, "scoreReason": "标题完全匹配 / TMDB 一致 / Series / 162 集", "isTitleExact": True, "isSeries": True}]

            def query_library_exists_by_tmdb(self, identity):
                return {
                    "ok": True,
                    "exists": True,
                    "embyItem": {"Id": "zhe-tian", "Name": "遮天", "Type": "Series", "ProviderIds": {"Tmdb": "224839"}},
                    "seasonMap": {1: set(range(1, 161))},
                    "seasonItems": [{"season": 1, "name": "Season 1", "itemId": "season-1"}],
                    "episodeRows": 160,
                    "episodeItems": [
                        {"season": 1, "episode": number, "name": f"遮天 第{number}集", "sortName": "", "originalTitle": "", "path": f"/zhetian/{number}.mkv"}
                        for number in range(1, 161)
                    ],
                    "missingEpisodeMap": {1: {161, 162}},
                    "missingEpisodeItems": [
                        {"season": 1, "episode": 161, "name": "遮天 第161集", "sortName": "", "originalTitle": "", "path": "", "isMissing": True, "locationType": "Virtual"},
                        {"season": 1, "episode": 162, "name": "遮天 第162集", "sortName": "", "originalTitle": "", "path": "", "isMissing": True, "locationType": "Virtual"},
                    ],
                    "hasMissingEpisodeData": True,
                    "specials": [],
                    "duplicates": [],
                    "embyQueryCount": 2,
                }

            def query_media_detail(self, tmdb_id: str, media_type: str):
                return {
                    "ok": True,
                    "totalEpisodes": 175,
                    "registeredEpisodes": 175,
                    "airedEpisodes": 162,
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
                    "embyItem": {"Id": "zhe-tian", "Name": "遮天", "Type": "Series", "ProviderIds": {"Tmdb": "224839"}},
                    "seasonMap": {1: set(range(1, 161))},
                    "seasonItems": [{"season": 1, "name": "Season 1", "itemId": "season-1"}],
                    "episodeRows": 160,
                    "episodeItems": [
                        {"season": 1, "episode": number, "name": f"遮天 第{number}集", "sortName": "", "originalTitle": "", "path": f"/zhetian/{number}.mkv"}
                        for number in range(1, 161)
                    ],
                    "missingEpisodeMap": {1: {161, 162}},
                    "missingEpisodeItems": [
                        {"season": 1, "episode": 161, "name": "遮天 第161集", "sortName": "", "originalTitle": "", "path": "", "isMissing": True, "locationType": "Virtual"},
                        {"season": 1, "episode": 162, "name": "遮天 第162集", "sortName": "", "originalTitle": "", "path": "", "isMissing": True, "locationType": "Virtual"},
                    ],
                    "hasMissingEpisodeData": True,
                    "specials": [],
                    "duplicates": [],
                    "embyQueryCount": 3,
                }

        self.service._media_identity_service = lambda: InventoryService()
        rich = self.service._build_ai_missing_episode_reply(
            "查看一下遮天缺失集",
            conversation_key="chat:zhetian",
            rich=True,
            chat_id="100",
        )

        self.assertIn("📝 《遮天》（2023）— 缺失集", rich["fallback_text"])
        self.assertIn("161-162", rich["fallback_text"])

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

        self.assertEqual(reply["parse_mode"], "")
        self.assertIn("执行 1 次搜索，查询 4 次数据", reply["text"])
        self.assertIn("📝 《遮天》（2023）— 缺失集", reply["text"])
        self.assertIn("Emby 媒体库状态：", reply["text"])
        self.assertIn("共缺失", reply["fallback_text"])
        self.assertEqual(reply["reply_markup"]["inline_keyboard"][0][0]["text"], "生成搜索清单")

    def test_missing_report_without_missing_has_no_button(self) -> None:
        reply = self.service._missing_episode_report_reply(
            {
                "title": "遮天",
                "year": "2023",
                "seasonRows": [
                    {
                        "seasonLabel": "Season 1",
                        "existingText": "1-33",
                        "totalText": "33",
                        "missingText": "无",
                        "statusText": "✅ 完整",
                    }
                ],
                "summaryText": "全集 33 集均已入库，无缺失。",
                "missingText": "无",
                "missingLabels": [],
                "searchCount": 1,
                "dataQueryCount": 4,
            }
        )

        self.assertIsNone(reply["reply_markup"])
        self.assertNotIn("需要生成", reply["fallback_text"])
        self.assertIn("✅ 完整", reply["fallback_text"])
        self.assertIn("全集 33 集均已入库，无缺失。", reply["fallback_text"])

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

    def test_missing_identity_continue_callback_uses_emby_identity(self) -> None:
        answered = []
        edits = []
        case = self

        class FakeSender:
            def answer_callback_query(self, **kwargs):
                answered.append(kwargs)

            def edit_message_text(self, **kwargs):
                edits.append(kwargs)

        class InventoryService:
            def query_media_detail(self, tmdb_id: str, media_type: str):
                case.assertEqual(tmdb_id, "999999")
                return {
                    "ok": True,
                    "totalEpisodes": 2,
                    "registeredEpisodes": 2,
                    "airedEpisodes": 2,
                    "seasonCounts": {1: 2},
                    "registeredSeasonMap": {1: {1, 2}},
                    "airedSeasonMap": {1: {1, 2}},
                    "futureSeasonMap": {},
                    "unknownAirDateMap": {},
                    "lastAiredDate": "2026-06-28",
                    "tmdbQueryCount": 2,
                }

            def query_library_exists(self, identity):
                case.assertTrue(identity.get("forceEmbyItem"))
                case.assertEqual(identity.get("embyId"), "emby-1")
                return {
                    "ok": True,
                    "exists": True,
                    "embyItem": {"Id": "emby-1", "Name": "仙逆", "Type": "Series", "ProductionYear": 2024, "ProviderIds": {"Tmdb": "999999"}},
                    "seasonMap": {1: {1, 2}},
                    "episodeRows": 2,
                    "episodeItems": [
                        {"season": 1, "episode": 1, "name": "仙逆 第1集", "sortName": "", "originalTitle": "", "path": "/xianni/1.mkv"},
                        {"season": 1, "episode": 2, "name": "仙逆 第2集", "sortName": "", "originalTitle": "", "path": "/xianni/2.mkv"},
                    ],
                    "specials": [],
                    "duplicates": [],
                    "embyQueryCount": 2,
                }

        self.service.sender = FakeSender()
        self.service._media_identity_service = lambda: InventoryService()
        self.service._pending_ai_actions["identity-action"] = {
            "type": "missing_episode_identity",
            "chatId": "100",
            "conversationKey": "chat:xianni",
            "question": "查看一下仙逆缺失集",
            "targetIdentity": {"title": "仙逆", "year": "2023", "type": "series", "tmdbId": "223911"},
            "embyIdentity": {"title": "仙逆", "year": "2024", "type": "series", "tmdbId": "999999", "embyId": "emby-1", "forceEmbyItem": True},
            "candidates": [{"embyItemId": "emby-1", "title": "仙逆", "year": "2023", "type": "series", "tmdbId": "223911", "episodeCount": 2, "scoreReason": "标题完全匹配 / TMDB 一致 / Series / 2 集"}],
            "createdAt": time.time(),
        }

        self.service._handle_missing_identity_callback(
            data="missing_identity:continue:identity-action",
            token="token",
            callback_id="callback",
            chat_id="100",
            message_id=7,
        )

        self.assertIn("已按 Emby 条目继续查询", answered[0]["text"])
        self.assertTrue(edits)
        self.assertIn("已按 Emby 命中条目继续查询", edits[0]["text"])
        self.assertIn("⚠️ 待确认", edits[0]["text"])

    def test_missing_identity_reselect_callback_shows_candidate_buttons(self) -> None:
        answered = []
        edits = []

        class FakeSender:
            def answer_callback_query(self, **kwargs):
                answered.append(kwargs)

            def edit_message_text(self, **kwargs):
                edits.append(kwargs)

        self.service.sender = FakeSender()
        self.service._pending_ai_actions["identity-action"] = {
            "type": "missing_episode_identity",
            "chatId": "100",
            "conversationKey": "chat:xianni",
            "question": "查看一下仙逆缺失集",
            "targetIdentity": {"title": "仙逆", "year": "2023", "type": "series", "tmdbId": "223911"},
            "candidates": [
                {"embyItemId": "emby-1", "title": "仙逆", "year": "2023", "type": "series", "tmdbId": "223911", "episodeCount": 147, "scoreReason": "标题完全匹配 / TMDB 一致 / Series / 147 集"},
                {"embyItemId": "emby-2", "title": "仙逆 特别篇", "year": "2024", "type": "series", "tmdbId": "223912", "episodeCount": 8, "scoreReason": "标题相似 / TMDB 未绑定 / Series / 8 集"},
            ],
            "createdAt": time.time(),
        }

        self.service._handle_missing_identity_callback(
            data="missing_identity:reselect:identity-action",
            token="token",
            callback_id="callback",
            chat_id="100",
            message_id=7,
        )

        self.assertIn("请选择正确作品", answered[0]["text"])
        self.assertTrue(edits)
        self.assertIn("请选择正确作品", edits[0]["text"])
        self.assertEqual(edits[0]["reply_markup"]["inline_keyboard"][0][0]["callback_data"], "missing_identity:pick:identity-action:0")

    def test_callback_router_dispatches_missing_identity_actions(self) -> None:
        called = {}

        def fake_handler(**kwargs):
            called.update(kwargs)

        self.service._handle_missing_identity_callback = fake_handler
        self.service._handle_callback_query(
            {
                "id": "cb-1",
                "data": "missing_identity:pick:identity-action:0",
                "message": {
                    "message_id": 7,
                    "chat": {"id": 100},
                },
            },
            token="token",
        )

        self.assertEqual(called["data"], "missing_identity:pick:identity-action:0")
        self.assertEqual(called["token"], "token")
        self.assertEqual(called["callback_id"], "cb-1")
        self.assertEqual(called["chat_id"], "100")
        self.assertEqual(called["message_id"], 7)

    def test_ai_reply_normalizes_plain_text_without_title_or_text_label(self) -> None:
        reply = self.service._ai_markdown_reply("🧠 AI 媒体问答", "Text\n\n这是正常回答。")

        self.assertEqual(reply["parse_mode"], "")
        self.assertEqual(reply["text"], "这是正常回答。")
        self.assertNotIn("AI 媒体问答", reply["text"])
        self.assertNotIn("Text", reply["text"])


if __name__ == "__main__":
    unittest.main()
