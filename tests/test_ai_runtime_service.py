import pathlib
import tempfile
import time
import urllib.error
import unittest
import json
from unittest.mock import patch

from backend_modules.media_identity_service import MediaIdentityService
from backend_modules.ai_runtime_service import AIRuntimeService
from backend_modules.ai_tool_base import AIToolBase
from backend_modules.ai_tool_registry import AIToolRegistry
from backend_modules.telegram_commands import TelegramCommandService


class AIRuntimeServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = pathlib.Path(self.temp_dir.name)
        self.service = TelegramCommandService(
            store_path=root / "invites.json",
            event_log_path=root / "events.jsonl",
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_runtime_builds_tool_provider_and_registry(self) -> None:
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:runtime",
            chat_id="100",
            rich=True,
        )

        provider = runtime.tool_provider()
        registry = provider.build_registry()
        tool_names = [row.name for row in registry.definitions()]

        self.assertIn("search_media", tool_names)
        self.assertIn("query_missing_episodes", tool_names)
        self.assertIn("transfer_115_share", tool_names)

    def test_runtime_builds_agent_with_subagent_routing(self) -> None:
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:runtime",
            chat_id="100",
            rich=True,
        )
        agent = runtime.build_agent(ai_config={})

        result = agent.prepare_and_dispatch("最近播放历史")

        self.assertTrue(result.handled)
        self.assertEqual(result.tool.name, "query_playback_history")
        self.assertEqual(result.subagent, "playback-analyst")

    def test_runtime_agent_does_not_dispatch_missing_episode_tool_for_meta_question(self) -> None:
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:runtime",
            chat_id="100",
            rich=True,
        )
        agent = runtime.build_agent(ai_config={})

        result = agent.prepare_and_dispatch("查询媒体缺失集的方式")

        self.assertFalse(result.handled)

    def test_runtime_host_exposes_registry_gateway(self) -> None:
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:runtime",
            chat_id="100",
            rich=True,
        )

        registry = runtime.host.registry.tool_registry(
            conversation_key="chat:runtime",
            chat_id="100",
            rich=True,
        )
        subagent = runtime.host.registry.pick_subagent(
            "最近播放历史",
            conversation_key="chat:runtime",
            chat_id="100",
            rich=True,
        )

        self.assertIsNotNone(registry.get_tool("search_media"))
        self.assertEqual(getattr(subagent, "name", ""), "playback-analyst")

    def test_tool_registry_registers_standard_tool_and_looks_up_by_name(self) -> None:
        class DemoTool(AIToolBase):
            def __init__(self) -> None:
                super().__init__(
                    name="demo_tool",
                    description="测试工具",
                    schema={"type": "object", "properties": {"question": {"type": "string"}}},
                )

            def invoke(self, question: str):
                return f"echo:{question}"

        registry = AIToolRegistry()
        tool = DemoTool()
        registry.register_tool(tool, predicate=lambda text: "demo" in text)

        dispatched = registry.dispatch("demo question")

        self.assertIs(registry.get_tool("demo_tool"), tool)
        self.assertEqual(registry.get("demo_tool").name, "demo_tool")
        self.assertEqual(dispatched[0].name, "demo_tool")
        self.assertEqual(dispatched[1], "echo:demo question")

    def test_runtime_host_exposes_split_conversation_and_action_hosts(self) -> None:
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:runtime",
            chat_id="100",
            rich=True,
        )

        runtime.host.conversations.remember(
            "chat:runtime",
            question="完美世界多少集",
            answer="168 集",
        )
        runtime.host.actions.register_pending_ai_action("demo", {"createdAt": time.time(), "type": "scheduled_task"})

        session = runtime.host.conversations.get("chat:runtime")

        self.assertIn("recent", session)
        self.assertIn("demo", self.service._pending_ai_actions)

    def test_runtime_host_exposes_media_service(self) -> None:
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:runtime",
            chat_id="100",
            rich=True,
        )
        self.service._emby_get = lambda path: {"MovieCount": 3} if path == "/Items/Counts" else []

        counts = runtime.host.media_service.emby_get("/Items/Counts")

        self.assertEqual(counts["MovieCount"], 3)

    def test_media_service_reads_missing_scan_cache(self) -> None:
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:runtime",
            chat_id="100",
            rich=True,
        )
        cache_path = self.service.store_path.parent / "missing_scan.json"
        cache_path.write_text(json.dumps({"rows": [{"title": "完美世界"}]}, ensure_ascii=False), encoding="utf-8")

        payload = runtime.host.media_service.read_missing_scan_cache()

        self.assertEqual(payload["rows"][0]["title"], "完美世界")

    def test_media_service_matches_scheduled_task(self) -> None:
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:runtime",
            chat_id="100",
            rich=True,
        )
        self.service._emby_get = lambda path: [
            {"Name": "Scan Media Library", "Id": "scan-1"},
            {"Name": "Refresh Metadata", "Id": "meta-1"},
        ] if path == "/ScheduledTasks" else []

        task = runtime.host.media_service.match_scheduled_task_for_question("帮我运行媒体库扫描任务")

        self.assertEqual(task["Id"], "scan-1")

    def test_media_service_fetches_latest_items_with_user_fallback(self) -> None:
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:runtime",
            chat_id="100",
            rich=True,
        )

        def fake_emby_get(path: str):
            if path.startswith("/Items/Latest?"):
                raise RuntimeError("latest disabled")
            if path == "/Sessions":
                return [{"UserId": "u-1"}]
            if path.startswith("/Users/u-1/Items/Latest?"):
                return [{"Name": "完美世界", "Type": "Series"}]
            return []

        self.service._emby_get = fake_emby_get

        rows, tried_paths, error = runtime.host.media_service.fetch_latest_items_with_fallback(limit=5)

        self.assertEqual(rows[0]["Name"], "完美世界")
        self.assertGreaterEqual(len(tried_paths), 2)
        self.assertIsNone(error)

    def test_media_service_resolves_series_search_item_from_episode_parent(self) -> None:
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:runtime",
            chat_id="100",
            rich=True,
        )

        def fake_emby_get(path: str):
            if path.startswith("/Items/series-1?"):
                return {"Id": "series-1", "Type": "Series", "Name": "完美世界"}
            return []

        self.service._emby_get = fake_emby_get
        self.service._resolve_ai_series_search_item = lambda *args, **kwargs: {"Id": "legacy-should-not-be-used"}

        resolved = runtime.host.media_service.resolve_ai_series_search_item(
            {"Type": "Episode", "SeriesId": "series-1"},
            items=[],
            keyword="完美世界",
        )

        self.assertEqual(resolved["Id"], "series-1")

    def test_media_service_resolves_identity_with_emby_fallback(self) -> None:
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:runtime",
            chat_id="100",
            rich=True,
        )
        self.service._media_identity_service = lambda: type(
            "BrokenIdentityService",
            (),
            {"identity_from_emby_item": staticmethod(lambda item: (_ for _ in ()).throw(RuntimeError("boom")))},
        )()
        self.service._resolve_ai_media_identity = lambda *args, **kwargs: {"embyId": "legacy-should-not-be-used"}

        identity = runtime.host.media_service.resolve_ai_media_identity(
            keyword="完美世界",
            detail={"Id": "emby-1", "Name": "完美世界", "Type": "Series", "PremiereDate": "2024-01-01T00:00:00Z"},
        )

        self.assertEqual(identity["embyId"], "emby-1")
        self.assertEqual(identity["year"], "2024")
        self.assertEqual(identity["source"], "emby_fallback")

    def test_emby_tool_invokes_existing_media_service_flow(self) -> None:
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:runtime",
            chat_id="100",
            rich=True,
        )
        self.service._media_identity_service = lambda: type(
            "FakeIdentityService",
            (),
            {
                "resolve": staticmethod(
                    lambda keyword, preferred_type="": {
                        "identity": {"title": keyword, "year": "2024", "type": "series", "tmdbId": "100", "embyId": "series-1", "confidence": "测试确认"},
                        "embyItem": {"Id": "series-1", "Name": keyword, "Type": "Series"},
                    }
                )
            },
        )()

        registry = runtime.host.registry.tool_registry(
            conversation_key="chat:runtime",
            chat_id="100",
            rich=True,
        )
        tool = registry.get_tool("search_media")
        reply = tool.invoke("搜索一下《完美世界》")

        self.assertIn("已确认作品：完美世界", reply)
        self.assertIn("TMDB ID：100", reply)

    def test_hdhive_tool_invokes_existing_search_flow(self) -> None:
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:runtime",
            chat_id="100",
            rich=True,
        )
        self.service._cmd_hdhive_search = lambda keyword: {"ok": True, "source": "legacy_hdhive", "keyword": keyword}

        registry = runtime.host.registry.tool_registry(
            conversation_key="chat:runtime",
            chat_id="100",
            rich=True,
        )
        tool = registry.get_tool("search_hdhive_resource")
        reply = tool.invoke("帮我搜一下遮天影巢资源")

        self.assertEqual(reply["source"], "legacy_hdhive")
        self.assertIn("遮天", reply["keyword"])

    def test_drive115_tool_invokes_existing_transfer_flow(self) -> None:
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:runtime",
            chat_id="100",
            rich=True,
        )
        self.service._cmd_drive115_transfer = lambda question: {"ok": True, "source": "legacy_115", "question": question}

        registry = runtime.host.registry.tool_registry(
            conversation_key="chat:runtime",
            chat_id="100",
            rich=True,
        )
        tool = registry.get_tool("transfer_115_share")
        reply = tool.invoke("转存这个 115 链接 https://115.com/s/demo")

        self.assertEqual(reply["source"], "legacy_115")
        self.assertIn("115.com/s/demo", reply["question"])

    def test_media_service_fetches_series_episodes_with_paging(self) -> None:
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:runtime",
            chat_id="100",
            rich=True,
        )
        calls: list[str] = []

        def fake_emby_get(path: str):
            calls.append(path)
            if "StartIndex=0" in path:
                return {
                    "Items": [{"Id": f"ep{i}"} for i in range(1, 51)],
                    "TotalRecordCount": 51,
                }
            if "StartIndex=50" in path:
                return {
                    "Items": [{"Id": "ep51"}],
                    "TotalRecordCount": 51,
                }
            return {"Items": [], "TotalRecordCount": 51}

        self.service._emby_get = fake_emby_get
        self.service._fetch_ai_series_episodes = lambda *args, **kwargs: [{"Id": "legacy-should-not-be-used"}]

        episodes = runtime.host.media_service.fetch_ai_series_episodes(item_id="series-1", page_size=2)

        self.assertEqual(len(episodes), 51)
        self.assertEqual(episodes[0]["Id"], "ep1")
        self.assertEqual(episodes[-1]["Id"], "ep51")
        self.assertEqual(len(calls), 2)

    def test_media_service_resolves_series_counts_from_episode_rows(self) -> None:
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:runtime",
            chat_id="100",
            rich=True,
        )

        def fake_emby_get(path: str):
            if path.startswith("/Shows/series-1/Seasons?"):
                return {"Items": [{"IndexNumber": 1, "Name": "Season 1"}]}
            if path.startswith("/Shows/series-1/Episodes?"):
                return {
                    "Items": [
                        {"Id": "ep1", "SeriesId": "series-1", "ParentIndexNumber": 1, "IndexNumber": 1, "Name": "第一集"},
                        {"Id": "ep2", "SeriesId": "series-1", "ParentIndexNumber": 1, "IndexNumber": 2, "Name": "第二集"},
                    ],
                    "TotalRecordCount": 2,
                }
            if path.startswith("/Items?"):
                return {"Items": []}
            if path.startswith("/Items/Latest?"):
                return []
            if path == "/Sessions":
                return []
            return []

        self.service._emby_get = fake_emby_get
        self.service._resolve_ai_series_counts = lambda *args, **kwargs: (99, 99, [], "legacy", "legacy")

        season_count, episode_count, season_lines, latest_text, source_note = runtime.host.media_service.resolve_ai_series_counts(
            item_id="series-1",
            detail={"Name": "完美世界", "Type": "Series"},
            keyword="完美世界",
            title="完美世界",
            identity={},
        )

        self.assertEqual(season_count, 1)
        self.assertEqual(episode_count, 2)
        self.assertEqual(season_lines, ["S1 2集"])
        self.assertEqual(latest_text, "S01E02「第二集」")
        self.assertIn("最终判断：实际可读取单集 2 集", source_note)

    def test_media_service_resolves_series_counts_from_season_fallback(self) -> None:
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:runtime",
            chat_id="100",
            rich=True,
        )

        def fake_emby_get(path: str):
            if path.startswith("/Shows/series-2/Seasons?"):
                return {"Items": [{"IndexNumber": 1, "Name": "Season 1"}, {"IndexNumber": 2, "Name": "Season 2"}]}
            if path.startswith("/Shows/series-2/Episodes?"):
                return {"Items": [], "TotalRecordCount": 0}
            if path.startswith("/Items?"):
                return {"Items": []}
            if path.startswith("/Items/Latest?"):
                return []
            if path == "/Sessions":
                return []
            return []

        self.service._emby_get = fake_emby_get
        self.service._resolve_ai_series_counts = lambda *args, **kwargs: (88, 88, [], "legacy", "legacy")

        season_count, episode_count, season_lines, latest_text, source_note = runtime.host.media_service.resolve_ai_series_counts(
            item_id="series-2",
            detail={"Name": "遮天", "Type": "Series", "RecursiveItemCount": 12, "ChildCount": 2},
            keyword="遮天",
            title="遮天",
            identity={},
        )

        self.assertEqual(season_count, 2)
        self.assertEqual(episode_count, 12)
        self.assertEqual(season_lines, ["S1 6集", "S2 6集"])
        self.assertEqual(latest_text, "")
        self.assertIn("最终判断回退：使用季字段统计", source_note)

    def test_media_service_builds_default_identity_service(self) -> None:
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:runtime",
            chat_id="100",
            rich=True,
        )

        identity_service = runtime.host.media_service.media_identity_service()

        self.assertIsInstance(identity_service, MediaIdentityService)

    def test_media_service_builds_mp_style_per_season_missing_result(self) -> None:
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:runtime",
            chat_id="100",
            rich=True,
        )

        class InventoryService:
            def query_media_detail(self, tmdb_id: str, media_type: str):
                return {
                    "ok": True,
                    "totalEpisodes": 6,
                    "registeredEpisodes": 6,
                    "airedEpisodes": 6,
                    "seasonCounts": {1: 3, 2: 3},
                    "registeredSeasonMap": {1: {1, 2, 3}, 2: {1, 2, 3}},
                    "airedSeasonMap": {1: {1, 2, 3}, 2: {1, 2, 3}},
                    "futureSeasonMap": {},
                    "unknownAirDateMap": {},
                    "lastAiredDate": "2026-06-28",
                    "tmdbQueryCount": 2,
                }

            def query_library_exists(self, identity):
                return {
                    "ok": True,
                    "exists": True,
                    "embyItem": {"Id": "modern-family", "Name": "摩登家庭", "Type": "Series"},
                    "seasonMap": {1: {1, 2, 3}, 2: {1, 2, 3}},
                    "episodeRows": 6,
                    "episodeItems": [
                        {"season": 1, "episode": 1, "name": "S01E01", "sortName": "", "originalTitle": "", "path": "/s01e01.mkv"},
                        {"season": 1, "episode": 2, "name": "S01E02", "sortName": "", "originalTitle": "", "path": "/s01e02.mkv"},
                        {"season": 1, "episode": 3, "name": "S01E03", "sortName": "", "originalTitle": "", "path": "/s01e03.mkv"},
                        {"season": 2, "episode": 1, "name": "S02E01", "sortName": "", "originalTitle": "", "path": "/s02e01.mkv"},
                        {"season": 2, "episode": 2, "name": "S02E02", "sortName": "", "originalTitle": "", "path": "/s02e02.mkv"},
                        {"season": 2, "episode": 3, "name": "S02E03", "sortName": "", "originalTitle": "", "path": "/s02e03.mkv"},
                    ],
                    "specials": [],
                    "duplicates": [],
                    "embyQueryCount": 2,
                }

        self.service._media_identity_service = lambda: InventoryService()

        result = runtime.host.media_service.build_missing_episode_result(
            identity={"title": "摩登家庭", "year": "2009", "type": "series", "tmdbId": "1421"},
        )

        self.assertEqual(result.mapping_confidence, "high")
        self.assertEqual(result.missing_seasons, [])
        self.assertEqual(result.seasons[1].missing_episodes, [])
        self.assertEqual(result.seasons[2].existing_episodes, [1, 2, 3])

    def test_media_service_marks_mapping_anomaly_when_mapped_count_is_lower(self) -> None:
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:runtime",
            chat_id="100",
            rich=True,
        )

        class InventoryService:
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

        result = runtime.host.media_service.build_missing_episode_result(
            identity={"title": "仙逆", "year": "2023", "type": "series", "tmdbId": "223911"},
        )

        self.assertEqual(result.existing_episodes, 26)
        self.assertEqual(result.mapped_episodes, 10)
        self.assertEqual(result.unmapped_episodes, 16)
        self.assertEqual(result.mapping_confidence, "low")
        self.assertEqual(result.mapping_warning, "编号映射异常，无法可靠判断缺集")
        self.assertEqual(result.missing_episodes, [])
        self.assertIn("S01E11", result.reference_missing_episodes[0])

    def test_media_service_duplicate_episode_row_does_not_force_unreliable_result(self) -> None:
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:runtime",
            chat_id="100",
            rich=True,
        )

        class InventoryService:
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

            def query_library_exists_by_tmdb(self, identity):
                rows = [
                    {"season": 1, "episode": number, "name": f"仙逆 第{number}集", "sortName": "", "originalTitle": "", "path": f"/xianni/{number}.mkv"}
                    for number in range(1, 147)
                ]
                rows.append({"season": 1, "episode": 131, "name": "仙逆 第131集 4K", "sortName": "", "originalTitle": "", "path": "/xianni/131-4k.mkv"})
                return {
                    "ok": True,
                    "exists": True,
                    "embyItem": {"Id": "xian-ni", "Name": "仙逆", "Type": "Series", "ProviderIds": {"Tmdb": "223911"}},
                    "seasonMap": {1: set(range(1, 147))},
                    "episodeRows": 147,
                    "episodeItems": rows,
                    "specials": [],
                    "duplicates": [{"season": 1, "episode": 131, "name": "仙逆 第131集 4K", "path": "/xianni/131-4k.mkv"}],
                    "embyQueryCount": 2,
                }

            def query_library_exists(self, identity):
                return self.query_library_exists_by_tmdb(identity)

        self.service._media_identity_service = lambda: InventoryService()

        result = runtime.host.media_service.build_missing_episode_result(
            identity={"title": "仙逆", "year": "2023", "type": "series", "tmdbId": "223911"},
        )

        self.assertEqual(result.existing_episodes, 146)
        self.assertEqual(result.mapped_episodes, 146)
        self.assertEqual(result.unmapped_episodes, 0)
        self.assertEqual(result.mapping_confidence, "high")
        self.assertEqual(result.missing_episodes, ["S01E147"])

    def test_media_service_maps_absolute_episode_from_title_or_path(self) -> None:
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:runtime",
            chat_id="100",
            rich=True,
        )

        class InventoryService:
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
                return {
                    "ok": True,
                    "exists": True,
                    "embyItem": {"Id": "xian-ni", "Name": "仙逆", "Type": "Series"},
                    "seasonMap": {},
                    "episodeRows": 1,
                    "episodeItems": [
                        {
                            "season": 0,
                            "episode": 0,
                            "name": "仙逆 第147集",
                            "sortName": "",
                            "originalTitle": "",
                            "path": "/media/仙逆/仙逆-147.mkv",
                        }
                    ],
                    "specials": [],
                    "duplicates": [],
                    "embyQueryCount": 2,
                }

        self.service._media_identity_service = lambda: InventoryService()

        result = runtime.host.media_service.build_missing_episode_result(
            identity={"title": "仙逆", "year": "2023", "type": "series", "tmdbId": "223911"},
        )

        self.assertEqual(result.mapped_episodes, 1)
        self.assertIn(147, result.seasons[1].existing_episodes)

    def test_missing_episode_tool_requires_confirmed_tmdb_identity(self) -> None:
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:runtime",
            chat_id="100",
            rich=True,
        )

        class InventoryService:
            def search_media(self, query: str, *, media_type: str = ""):
                return [{"title": "仙逆", "year": "2023", "type": "series", "score": 100}]

        self.service._media_identity_service = lambda: InventoryService()
        tool = runtime.host.registry.tool_registry(
            conversation_key="chat:runtime",
            chat_id="100",
            rich=True,
        ).get_tool("query_missing_episodes")

        reply = tool.invoke("查看一下仙逆的缺失集")

        self.assertIn("请先确认作品身份", reply)

    def test_missing_episode_tool_returns_local_candidate_selection_reply(self) -> None:
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:runtime",
            chat_id="100",
            rich=True,
        )

        class InventoryService:
            def search_media(self, query: str, *, media_type: str = ""):
                return [{"title": "仙逆", "year": "2023", "type": "series", "tmdbId": "223911", "score": 100}]

            def query_library_exists_by_tmdb(self, identity):
                if str(identity.get("embyId") or "") == "122937":
                    return {
                        "ok": True,
                        "exists": True,
                        "embyItem": {"Id": "122937", "Name": "光阴之外", "Type": "Series", "ProductionYear": 2025, "ProviderIds": {"Tmdb": "281233"}},
                        "seasonMap": {1: set(range(1, 28))},
                        "episodeRows": 27,
                        "episodeItems": [
                            {"season": 1, "episode": number, "name": f"第{number}集", "sortName": "", "originalTitle": "", "path": f"/s01e{number:02d}.strm"}
                            for number in range(1, 28)
                        ],
                        "specials": [],
                        "duplicates": [],
                        "embyQueryCount": 2,
                    }
                return {"ok": True, "exists": False, "embyItem": {}, "embyQueryCount": 1}

            def search_local_candidates(self, query: str, **kwargs):
                return [
                    {"embyItemId": "dark", "title": "暗黑", "year": "2017", "type": "series", "tmdbId": "70523", "episodeCount": 26, "score": 920, "scoreReason": "标题不稳定 / TMDB 不一致 / Series / 26 集", "isTitleExact": False, "isSeries": True},
                    {"embyItemId": "xian-ni", "title": "仙逆", "year": "2023", "type": "series", "tmdbId": "223911", "episodeCount": 147, "score": 1180, "scoreReason": "标题完全匹配 / TMDB 一致 / Series / 147 集", "isTitleExact": True, "isSeries": True},
                ]

        self.service._media_identity_service = lambda: InventoryService()
        tool = runtime.host.registry.tool_registry(
            conversation_key="chat:runtime",
            chat_id="100",
            rich=True,
        ).get_tool("query_missing_episodes")

        reply = tool.invoke("查看一下仙逆的缺失集")

        self.assertIsInstance(reply, dict)
        self.assertIn("需要先确认作品身份", reply["fallback_text"])
        self.assertTrue(reply["reply_markup"]["inline_keyboard"][0][0]["callback_data"].startswith("missing_identity:pick:"))
        self.assertEqual(len(self.service._pending_ai_actions), 1)

    def test_missing_episode_tool_auto_continues_single_high_confidence_local_candidate(self) -> None:
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:runtime",
            chat_id="100",
            rich=True,
        )

        class InventoryService:
            def search_media(self, query: str, *, media_type: str = ""):
                return [{"title": "光阴之外", "year": "2025", "type": "series", "tmdbId": "281233", "score": 100}]

            def query_library_exists_by_tmdb(self, identity):
                if str(identity.get("embyId") or "") == "122937":
                    return {
                        "ok": True,
                        "exists": True,
                        "embyItem": {"Id": "122937", "Name": "光阴之外", "Type": "Series", "ProductionYear": 2025, "ProviderIds": {"Tmdb": "281233"}},
                        "seasonMap": {1: set(range(1, 28))},
                        "episodeRows": 27,
                        "episodeItems": [
                            {"season": 1, "episode": number, "name": f"第{number}集", "sortName": "", "originalTitle": "", "path": f"/s01e{number:02d}.strm"}
                            for number in range(1, 28)
                        ],
                        "specials": [],
                        "duplicates": [],
                        "embyQueryCount": 2,
                    }
                return {"ok": True, "exists": False, "embyItem": {}, "embyQueryCount": 1}

            def search_local_candidates(self, query: str, **kwargs):
                return [
                    {"embyItemId": "122937", "title": "光阴之外", "year": "2025", "type": "series", "tmdbId": "281233", "episodeCount": 0, "score": 1460, "scoreReason": "标题完全匹配 / TMDB 一致 / 年份一致 / Series", "isTitleExact": True, "isSeries": True},
                ]

            def query_media_detail(self, tmdb_id: str, media_type: str):
                return {
                    "ok": True,
                    "totalEpisodes": 31,
                    "registeredEpisodes": 31,
                    "airedEpisodes": 31,
                    "seasonCounts": {1: 31},
                    "registeredSeasonMap": {1: set(range(1, 32))},
                    "airedSeasonMap": {1: set(range(1, 32))},
                    "futureSeasonMap": {},
                    "unknownAirDateMap": {},
                    "lastAiredDate": "2026-06-28",
                    "tmdbQueryCount": 2,
                }

            def query_library_exists(self, identity):
                return self.query_library_exists_by_tmdb(identity)

        self.service._media_identity_service = lambda: InventoryService()
        tool = runtime.host.registry.tool_registry(
            conversation_key="chat:runtime",
            chat_id="100",
            rich=True,
        ).get_tool("query_missing_episodes")

        reply = tool.invoke("光阴之外缺失集")

        self.assertIsInstance(reply, dict)
        self.assertIn("《光阴之外》", reply["fallback_text"])
        self.assertNotIn("需要先确认作品身份", reply["fallback_text"])

    def test_media_service_missing_report_uses_renderer_path(self) -> None:
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:runtime",
            chat_id="100",
            rich=True,
        )

        reply = runtime.host.media_service.missing_episode_report_reply(
            {
                "title": "遮天",
                "year": "2023",
                "missingText": "E158、E161",
                "missingLabels": ["E158", "E161"],
                "searchCount": 1,
                "dataQueryCount": 4,
            },
            chat_id="100",
        )

        self.assertIsNotNone(reply["reply_markup"])
        self.assertEqual(len(self.service._pending_missing_searches), 1)

    def test_media_service_builds_scan_library_reply(self) -> None:
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:runtime",
            chat_id="100",
            rich=True,
        )

        def fake_emby_get(path: str):
            if path == "/Library/VirtualFolders":
                return [{"ItemId": "lib-1", "Name": "国产动漫", "CollectionType": "tvshows"}]
            return []

        self.service._emby_get = fake_emby_get

        reply = runtime.host.media_service.cmd_scan_library("动漫")

        self.assertIn("已按关键词“动漫”匹配到 1 个媒体库", reply["text"])
        self.assertEqual(
            reply["reply_markup"]["inline_keyboard"][0][0]["callback_data"],
            "scan_library:one:lib-1",
        )

    def test_media_service_builds_hdhive_search_reply(self) -> None:
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:runtime",
            chat_id="100",
            rich=True,
        )
        self.service.store_path.write_text(
            json.dumps(
                {
                    "hdhiveConfig": {"enabled": True, "clientId": "app", "appSecret": "secret", "accessToken": "token"},
                    "drive115Config": {"enabled": True, "cookie": "cookie", "defaultCid": "100"},
                }
            ),
            encoding="utf-8",
        )
        self.service._media_identity_service = lambda: type(
            "FakeIdentityService",
            (),
            {"resolve": staticmethod(lambda keyword: {"identity": {"title": "遮天", "year": "2023", "type": "series", "tmdbId": "224839"}, "embyItem": {}, "candidates": [], "ambiguous": False})},
        )()

        class FakeHDHiveService:
            def __init__(self, config, save_config=None):
                self.config = {"enabled": True, "accessToken": "token", **config}

            @property
            def is_broker(self):
                return False

            def search_resources(self, **kwargs):
                return {
                    "items": [
                        {"slug": "slug-115", "title": "遮天 4K", "pan_type": "115", "share_size": "10GB", "video_resolution": ["2160p"], "unlock_points": 3},
                        {"slug": "slug-other", "title": "遮天 夸克", "pan_type": "quark", "share_size": "8GB", "unlock_points": 1},
                    ]
                }

        with patch("backend_modules.hdhive_service.HDHiveService", FakeHDHiveService):
            reply = runtime.host.media_service.cmd_hdhive_search("遮天")

        self.assertIn("遮天 4K", reply["fallback_text"])
        self.assertIn("遮天 夸克", reply["fallback_text"])
        buttons = reply["reply_markup"]["inline_keyboard"]
        self.assertEqual(len(buttons), 1)
        self.assertTrue(buttons[0][0]["callback_data"].startswith("hdhive:pick:"))

    def test_media_service_builds_drive115_transfer_reply(self) -> None:
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:runtime",
            chat_id="100",
            rich=True,
        )
        self.service.store_path.write_text(
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

        class FakeDrive115Service:
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
                return {"ok": True, "message": "请求已接收", "targetCid": kwargs.get("target_cid")}

        with patch("backend_modules.drive115_service.Drive115Service", FakeDrive115Service):
            reply = runtime.host.media_service.cmd_drive115_transfer("资源介绍 https://115.com/s/abc123?password=p1a5")

        self.assertEqual(reply["text"], "转存完成：成功 1 个，已存在 0 个，失败 0 个")

    def test_media_service_formats_matched_series_context(self) -> None:
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:runtime",
            chat_id="100",
            rich=True,
        )

        def fake_emby_get(path: str):
            if path.startswith("/Items/series-3?"):
                return {
                    "Id": "series-3",
                    "Type": "Series",
                    "Name": "牧神记",
                    "PremiereDate": "2024-01-01T00:00:00Z",
                    "ProviderIds": {},
                    "Status": "Continuing",
                    "CommunityRating": 8.6,
                }
            if path.startswith("/Shows/series-3/Seasons?"):
                return {"Items": [{"IndexNumber": 1, "Name": "Season 1"}]}
            if path.startswith("/Shows/series-3/Episodes?"):
                return {
                    "Items": [
                        {"Id": "ep1", "SeriesId": "series-3", "ParentIndexNumber": 1, "IndexNumber": 1, "Name": "第一集"},
                        {"Id": "ep2", "SeriesId": "series-3", "ParentIndexNumber": 1, "IndexNumber": 2, "Name": "第二集"},
                    ],
                    "TotalRecordCount": 2,
                }
            if path.startswith("/Items?"):
                return {"Items": []}
            if path == "/Sessions":
                return []
            return []

        self.service._emby_get = fake_emby_get
        self.service._media_identity_service = lambda: type(
            "FakeIdentityService",
            (),
            {"identity_from_emby_item": staticmethod(lambda item: {"title": "牧神记", "year": "2024", "type": "series", "tmdbId": "200", "embyId": "series-3", "confidence": "测试匹配"})},
        )()

        lines = runtime.host.media_service.format_ai_matched_item_context(
            {"Id": "series-3", "Type": "Series", "Name": "牧神记"},
            keyword="牧神记",
        )
        reply = "\n".join(lines)

        self.assertIn("命中资源详情", reply)
        self.assertIn("共 1 季 / 2 集", reply)
        self.assertIn("S01E02「第二集」", reply)
        self.assertIn("用户评分：8.6", reply)

    def test_media_host_implements_pure_media_helpers(self) -> None:
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:runtime",
            chat_id="100",
            rich=True,
        )

        keyword = runtime.host.media.extract_ai_media_keyword("看一下《完美世界》现在多少集")
        reference = runtime.host.media.is_ai_reference_question("它简介呢")
        episode_query = runtime.host.media.is_ai_episode_count_question("现在多少集")
        year = runtime.host.media.resolve_year({"PremiereDate": "2024-02-03T00:00:00Z"})
        http_error = urllib.error.HTTPError("https://example.com", 404, "Not Found", hdrs=None, fp=None)
        error_text = runtime.host.media_service.format_emby_error(http_error)
        category_keywords = runtime.host.media.extract_ai_media_keywords("列出全部国产动漫")
        missing_map = runtime.host.media.format_missing_episode_map({1: [1, 2, 3], 2: [5]})
        inventory_labels = runtime.host.media.format_inventory_episode_labels(["S1E1", "S1E2", "S2E5"])
        latest_label = runtime.host.media_formatter.format_ai_latest_episode_label("S1E12「决战」")
        now_playing = runtime.host.media_formatter.format_now_playing_title(
            {"Type": "Episode", "SeriesName": "完美世界", "ParentIndexNumber": 2, "IndexNumber": 8, "Name": "新篇章"}
        )
        playback_row = runtime.host.media_formatter.format_recent_playback_row(
            {"startTime": "2024-03-04T12:34:56", "username": "sy", "title": "完美世界 - S02E08 - 新篇章"}
        )
        quality = runtime.host.media_formatter.format_media_quality(
            {
                "MediaSources": [
                    {
                        "Bitrate": 12000000,
                        "MediaStreams": [{"Type": "Video", "Width": 3840, "Height": 2160}],
                        "VideoRange": "HDR10+",
                    }
                ]
            }
        )
        identity_candidates = runtime.host.media.format_ai_identity_candidates(
            "三体",
            [
                {"title": "三体", "year": "2023", "type": "series", "tmdbId": "100"},
                {"title": "三体电影版", "year": "2024", "type": "movie", "tmdbId": "200"},
            ],
        )

        self.assertEqual(keyword, "完美世界")
        self.assertTrue(reference)
        self.assertTrue(episode_query)
        self.assertFalse(runtime.host.media.is_ai_episode_count_question("查询媒体缺失集的方式"))
        self.assertEqual(year, "2024")
        self.assertEqual(error_text, "HTTP 404")
        self.assertEqual(category_keywords, [])
        self.assertEqual(missing_map, "S01 E01-E03；S02 E05")
        self.assertEqual(inventory_labels, "S01E01-E02、S02E05")
        self.assertEqual(latest_label, "S01E12（第12集）")
        self.assertEqual(now_playing, "《完美世界》第2季 第8集「新篇章」")
        self.assertEqual(playback_row, "🔹 03-04 12:34 | 👤 sy | 📺「完美世界 - S02E08 - 新篇章」")
        self.assertEqual(quality, "4K HDR10+ | 12.0Mbps")
        self.assertIn("《三体》有多个同名候选", identity_candidates)
        self.assertIn("TMDB 100", identity_candidates)

    def test_runtime_host_exposes_platform_host(self) -> None:
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:runtime",
            chat_id="100",
            rich=True,
        )

        store = runtime.host.platform.read_store()
        tokens = runtime.host.platform.estimate_ai_tokens("完美世界abc")

        self.assertIn("aiConfig", store)
        self.assertGreater(tokens, 0)

    def test_action_host_cleans_expired_pending_items(self) -> None:
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:runtime",
            chat_id="100",
            rich=True,
        )
        self.service._pending_ai_actions["expired"] = {"createdAt": time.time() - 1000, "type": "scheduled_task"}
        self.service._pending_missing_searches["expired"] = {"createdAt": time.time() - 1000}

        runtime.host.actions.register_pending_ai_action("fresh", {"createdAt": time.time(), "type": "scheduled_task"})
        runtime.host.actions.register_pending_missing_search(title="遮天", labels=["E01"], chat_id="100")

        self.assertNotIn("expired", self.service._pending_ai_actions)
        self.assertNotIn("expired", self.service._pending_missing_searches)

    def test_platform_host_formats_event_detail_and_message_id(self) -> None:
        runtime = AIRuntimeService(
            self.service,
            conversation_key="chat:runtime",
            chat_id="100",
            rich=True,
        )

        message_id = runtime.host.platform.extract_telegram_message_id({"result": {"message_id": "42"}})
        detail = runtime.host.platform.format_ai_event_detail(
            action="telegram_ai_success",
            detail={"model": "gpt-4o-mini", "elapsedMs": 123, "streaming": True},
        )

        self.assertEqual(message_id, 42)
        self.assertIn("model=gpt-4o-mini", detail)
        self.assertIn("elapsedMs=123", detail)


if __name__ == "__main__":
    unittest.main()
