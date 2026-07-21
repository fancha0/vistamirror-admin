import json
import os
import unittest
from unittest.mock import patch

import dev_server

from backend_modules.moviepilot_config import (
    apply_moviepilot_env_overrides,
    default_moviepilot_config,
    env_managed_moviepilot_fields,
    merge_moviepilot_config_for_save,
    public_moviepilot_config,
    validate_moviepilot_config,
)
from backend_modules.moviepilot_service_adapter import (
    MoviePilotHttpError,
    MoviePilotServiceAdapter,
)
from backend_modules.ai_tools.moviepilot_tool import MoviePilotTool


class MoviePilotConfigTests(unittest.TestCase):
    def test_public_config_never_returns_api_token(self):
        public = public_moviepilot_config(
            {
                "enabled": True,
                "baseUrl": "http://moviepilot:3001",
                "apiToken": "secret-token",
                "timeoutSeconds": 20,
            }
        )

        self.assertNotIn("apiToken", public)
        self.assertTrue(public["hasApiToken"])
        self.assertEqual("http://moviepilot:3001", public["baseUrl"])

    def test_blank_token_update_preserves_existing_token(self):
        merged = merge_moviepilot_config_for_save(
            {
                "enabled": True,
                "baseUrl": "http://moviepilot:3001",
                "apiToken": "saved-token",
                "timeoutSeconds": 12,
            },
            {"baseUrl": "http://moviepilot:3001", "apiToken": ""},
        )

        self.assertEqual("saved-token", merged["apiToken"])

    @patch.dict(
        os.environ,
        {
            "MOVIEPILOT_ENABLED": "true",
            "MOVIEPILOT_BASE_URL": "https://mp.example",
            "MOVIEPILOT_API_TOKEN": "environment-token",
            "MOVIEPILOT_TIMEOUT_SECONDS": "18",
        },
        clear=True,
    )
    def test_environment_overrides_are_applied_and_reported(self):
        effective = apply_moviepilot_env_overrides(default_moviepilot_config())

        self.assertTrue(effective["enabled"])
        self.assertEqual("https://mp.example", effective["baseUrl"])
        self.assertEqual("environment-token", effective["apiToken"])
        self.assertEqual(18, effective["timeoutSeconds"])
        self.assertEqual(
            {"enabled", "baseUrl", "apiToken", "timeoutSeconds"},
            set(env_managed_moviepilot_fields()),
        )

    def test_enabled_config_requires_api_url_and_token(self):
        _, error = validate_moviepilot_config(
            {"enabled": True, "baseUrl": "", "apiToken": ""}
        )

        self.assertEqual("请填写 MoviePilot 地址。", error)


class MoviePilotServiceAdapterTests(unittest.TestCase):
    def _adapter(self, transport):
        return MoviePilotServiceAdapter(
            {
                "enabled": True,
                "baseUrl": "https://mp.example",
                "apiToken": "test-token",
                "timeoutSeconds": 9,
            },
            transport=transport,
        )

    def test_discovery_marks_only_safe_read_tools(self):
        def transport(method, url, headers, payload, timeout):
            self.assertEqual("GET", method)
            self.assertEqual("https://mp.example/api/v1/mcp/tools", url)
            self.assertEqual("test-token", headers["X-API-KEY"])
            self.assertIsNone(payload)
            self.assertEqual(9, timeout)
            return {
                "tools": [
                    {"name": "list_subscriptions", "description": "Read subscriptions"},
                    {"name": "query_tasks", "description": "Read tasks"},
                    {"name": "create_subscription", "description": "Write subscription"},
                    {"name": "run_task", "description": "Run task"},
                ]
            }

        tools = self._adapter(transport).discover_tools()

        self.assertEqual(
            {
                "list_subscriptions": True,
                "query_tasks": True,
                "create_subscription": False,
                "run_task": False,
            },
            {tool["name"]: tool["readOnly"] for tool in tools},
        )

    def test_query_subscribes_is_read_only_but_subscription_mutations_are_not(self):
        def transport(method, url, headers, payload, timeout):
            return {
                "tools": [
                    {"name": "query_subscribes", "description": "Query subscription status"},
                    {"name": "search_subscribe", "description": "Search and download resources"},
                    {"name": "add_subscribe", "description": "Add a subscription"},
                    {"name": "custom_lookup", "annotations": {"readOnlyHint": True}},
                ]
            }

        tools = self._adapter(transport).discover_tools()

        self.assertEqual(
            {
                "query_subscribes": True,
                "search_subscribe": False,
                "add_subscribe": False,
                "custom_lookup": True,
            },
            {tool["name"]: tool["readOnly"] for tool in tools},
        )

    def test_query_uses_mcp_gateway_for_a_safe_tool(self):
        requests = []

        def transport(method, url, headers, payload, timeout):
            requests.append((method, url, headers, payload, timeout))
            if method == "GET":
                return {"tools": [{"name": "list_subscriptions", "description": "Subscriptions"}]}
            return {"data": [{"title": "Example"}]}

        result = self._adapter(transport).query_first_read_tool(["subscription"])

        self.assertTrue(result["ok"])
        self.assertEqual("list_subscriptions", result["tool"])
        self.assertEqual(2, len(requests))
        method, url, headers, payload, timeout = requests[1]
        self.assertEqual("POST", method)
        self.assertEqual("https://mp.example/api/v1/mcp/tools/call", url)
        self.assertEqual("test-token", headers["X-API-KEY"])
        self.assertEqual({"name": "list_subscriptions", "arguments": {}}, payload)
        self.assertEqual(9, timeout)

    def test_query_subscriptions_matches_moviepilot_query_subscribes(self):
        requests = []

        def transport(method, url, headers, payload, timeout):
            requests.append((method, payload))
            if method == "GET":
                return {"tools": [{"name": "query_subscribes", "description": "Query subscription status"}]}
            return {"content": [{"type": "text", "text": "订阅列表"}]}

        result = self._adapter(transport).query_first_read_tool(
            ("subscription", "subscriptions", "subscribe", "订阅")
        )

        self.assertTrue(result["ok"])
        self.assertEqual("query_subscribes", result["tool"])
        self.assertEqual(("POST", {"name": "query_subscribes", "arguments": {}}), requests[1])

    def test_read_tool_call_retries_only_422_with_tool_name_payload(self):
        requests = []

        def transport(method, url, headers, payload, timeout):
            requests.append((method, payload))
            if method == "GET":
                return {"tools": [{"name": "query_subscribes", "description": "Query subscriptions"}]}
            if len(requests) == 2:
                raise MoviePilotHttpError(422)
            return {"content": [{"type": "text", "text": "订阅列表"}]}

        result = self._adapter(transport).query_first_read_tool(("subscription",))

        self.assertTrue(result["ok"])
        self.assertEqual(
            [
                ("POST", {"name": "query_subscribes", "arguments": {}}),
                ("POST", {"tool_name": "query_subscribes", "arguments": {}}),
            ],
            requests[1:],
        )

    def test_read_tool_call_does_not_retry_non_validation_http_errors(self):
        requests = []

        def transport(method, url, headers, payload, timeout):
            requests.append((method, payload))
            if method == "GET":
                return {"tools": [{"name": "query_subscribes", "description": "Query subscriptions"}]}
            raise MoviePilotHttpError(401)

        with self.assertRaises(MoviePilotHttpError) as captured:
            self._adapter(transport).query_first_read_tool(("subscription",))

        self.assertEqual(401, captured.exception.status_code)
        self.assertEqual(2, len(requests))

    def test_search_uses_safe_media_search_tool_with_its_keyword_field(self):
        requests = []

        def transport(method, url, headers, payload, timeout):
            requests.append((method, payload))
            if method == "GET":
                return {
                    "tools": [
                        {"name": "search_subscribe", "description": "Search and download"},
                        {
                            "name": "search_media",
                            "description": "Search movie and TV metadata",
                            "inputSchema": {"properties": {"keyword": {"type": "string"}}},
                        },
                    ]
                }
            return {"content": [{"type": "text", "text": "搜索结果：仙逆"}]}

        result = self._adapter(transport).query_search_tool("仙逆")

        self.assertTrue(result["ok"])
        self.assertEqual("search_media", result["tool"])
        self.assertEqual(
            ("POST", {"name": "search_media", "arguments": {"keyword": "仙逆"}}),
            requests[1],
        )

    def test_existing_api_v1_base_url_is_not_duplicated(self):
        adapter = MoviePilotServiceAdapter(
            {
                "enabled": True,
                "baseUrl": "https://mp.example/api/v1",
                "apiToken": "test-token",
            }
        )

        self.assertEqual("https://mp.example/api/v1", adapter.api_base_url)

    def test_normalize_search_results_reads_mcp_text_json_and_filters_urls(self):
        rows = MoviePilotServiceAdapter.normalize_search_results(
            {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(
                            [
                                {
                                    "title": "仙逆",
                                    "original_title": "Renegade Immortal",
                                    "year": "2023-09-25",
                                    "type": "电视剧",
                                    "vote_average": 8.6,
                                    "overview": "修仙故事",
                                    "poster_url": "https://image.example/x.jpg",
                                    "tmdb_id": 123,
                                    "url": "javascript:bad",
                                }
                            ],
                            ensure_ascii=False,
                        ),
                    }
                ]
            }
        )

        self.assertEqual(1, len(rows))
        self.assertEqual("仙逆", rows[0]["title"])
        self.assertEqual("tv", rows[0]["mediaType"])
        self.assertEqual("2023", rows[0]["year"])
        self.assertEqual(8.6, rows[0]["rating"])
        self.assertEqual("123", rows[0]["tmdbId"])
        self.assertEqual("", rows[0]["externalUrl"])

    def test_named_read_tool_allows_recommendations_but_never_a_write_tool(self):
        requests = []

        def transport(method, url, headers, payload, timeout):
            requests.append((method, payload))
            if method == "GET":
                return {"tools": [
                    {"name": "get_recommendations", "description": "Popular media", "readOnly": True},
                    {"name": "add_subscribe", "description": "Write subscription"},
                ]}
            return {"items": [{"title": "热门作品"}]}

        adapter = self._adapter(transport)
        result = adapter.query_named_read_tool("get_recommendations", {"source": "tmdb_trending"})

        self.assertTrue(result["ok"])
        self.assertEqual(("POST", {"name": "get_recommendations", "arguments": {"source": "tmdb_trending"}}), requests[1])
        self.assertFalse(adapter.query_named_read_tool("add_subscribe")["ok"])

    def test_recommendations_use_concrete_rest_source_before_mcp_fallback(self):
        requests = []

        def transport(method, url, headers, payload, timeout):
            requests.append((method, url, payload))
            return [{"title": "Bangumi 作品"}]

        result = self._adapter(transport).get_recommendations("bangumi_calendar", media_type="tv", page=2)
        self.assertEqual("rest", result["transport"])
        self.assertEqual([{"title": "Bangumi 作品"}], result["result"])
        self.assertEqual(("GET", "https://mp.example/api/v1/recommend/bangumi_calendar?page=2&count=30", None), requests[0])

    def test_full_tool_invoke_runs_write_tools_without_confirmation_gate(self):
        requests = []

        def transport(method, url, headers, payload, timeout):
            requests.append((method, payload))
            if method == "GET":
                return {"tools": [
                    {"name": "query_download_tasks", "readOnly": True},
                    {"name": "add_subscribe", "readOnly": False},
                ]}
            return {"status": "accepted"}

        adapter = self._adapter(transport)
        completed = adapter.invoke_named_tool("add_subscribe", {"title": "示例"})
        self.assertTrue(completed["ok"])
        self.assertFalse(completed["readOnly"])
        self.assertEqual("add_subscribe", requests[-1][1]["name"])

    def test_public_result_hides_credentials(self):
        result = MoviePilotServiceAdapter.public_result({"name": "站点", "cookie": "private", "api_key": "hidden", "rss": "https://site/rss?passkey=private", "nested": {"token": "x"}})
        self.assertEqual("站点", result["name"])
        self.assertEqual("[已隐藏]", result["cookie"])
        self.assertEqual("[已隐藏]", result["api_key"])
        self.assertEqual("[已隐藏]", result["rss"])
        self.assertEqual("[已隐藏]", result["nested"]["token"])

    def test_public_result_hides_credentials_inside_json_text(self):
        result = MoviePilotServiceAdapter.public_result({"result": json.dumps({"config": {"password": "private"}})})
        self.assertEqual("[已隐藏]", result["result"]["config"]["password"])

    def test_normalize_media_detail_keeps_only_safe_detail_fields(self):
        detail = MoviePilotServiceAdapter.normalize_media_detail({"content": [{"type": "text", "text": json.dumps({
            "title": "示例电影", "tmdb_id": 123, "type": "movie", "runtime": 108,
            "genres": [{"name": "惊悚"}], "production_countries": [{"name": "美国"}],
            "credits": {"directors": [{"name": "导演"}], "cast": [{"name": "演员"}]},
            "status": "Released", "release_date": "2026-05-13",
        }, ensure_ascii=False)}]})
        self.assertEqual("示例电影", detail["title"])
        self.assertEqual("108", detail["runtime"])
        self.assertEqual(["惊悚"], detail["genres"])
        self.assertEqual(["导演"], detail["credits"]["directors"])
        self.assertEqual("2026-05-13", detail["releaseDate"])

    def test_normalize_media_detail_keeps_nonempty_card_identifiers_as_fallback(self):
        detail = MoviePilotServiceAdapter.normalize_media_detail(
            {"title": "详情返回", "media_type": "movie"},
            {"title": "卡片标题", "tmdbId": "1084244", "mediaType": "movie"},
        )
        self.assertEqual("1084244", detail["tmdbId"])

    def test_tmdb_detail_enrichment_supplies_localized_title_and_overview(self):
        tmdb = MoviePilotServiceAdapter.normalize_tmdb_media_detail({
            "id": 259537, "name": "剑来", "original_name": "Jian Lai",
            "first_air_date": "2024-08-15", "overview": "天干世界，无奇不有。",
            "tagline": "大道朝天", "vote_average": 8.14,
            "poster_path": "/poster.jpg", "backdrop_path": "/backdrop.jpg",
            "genres": [{"name": "动画"}], "origin_country": ["CN"],
            "credits": {"cast": [{"name": "角色甲"}], "crew": [{"name": "导演甲", "job": "Director"}]},
            "external_ids": {"imdb_id": "tt123"},
        }, media_type="tv")
        detail = MoviePilotServiceAdapter.merge_media_details({"title": "MoviePilot 标题", "overview": ""}, tmdb)
        self.assertEqual("剑来", detail["title"])
        self.assertEqual("Jian Lai", detail["originalTitle"])
        self.assertEqual("天干世界，无奇不有。", detail["overview"])
        self.assertEqual("https://image.tmdb.org/t/p/original/poster.jpg", detail["posterUrl"])
        self.assertEqual(["导演甲"], detail["credits"]["directors"])

    def test_normalize_torrent_results_reads_mcp_json_text_and_filters_media(self):
        payload = {"result": json.dumps({"total_count": 2, "page": 1, "total_pages": 1, "filter_options": {"site": ["示例站"]}, "results": [
            {"torrent_info": {"title": "示例 2160p", "torrent_url": "abc:1", "site_name": "示例站", "seeders": 12, "volume_factor": "免费"}, "media_info": {"tmdb_id": 123}, "meta_info": {"resource_pix": "2160p", "edition": "WEB-DL"}},
            {"torrent_info": {"title": "其他", "torrent_url": "def:2"}, "media_info": {"tmdb_id": 456}, "meta_info": {}},
        ]})}
        result = MoviePilotServiceAdapter.normalize_torrent_results(payload, tmdb_id="123")
        self.assertEqual(1, len(result["items"]))
        self.assertEqual(1, result["totalCount"])
        self.assertEqual("abc:1", result["items"][0]["reference"])
        self.assertEqual("示例站", result["items"][0]["site"])
        self.assertEqual("免费", result["items"][0]["freeState"])


class MoviePilotToolFormattingTests(unittest.TestCase):
    def _tool(self, operation):
        tool = MoviePilotTool.__new__(MoviePilotTool)
        tool._operation = operation
        return tool

    def test_subscriptions_result_is_rendered_as_a_sendable_text_reply(self):
        reply = self._tool("subscriptions")._format_result(
            {
                "ok": True,
                "tool": "list_subscriptions",
                "result": {"content": [{"type": "text", "text": "当前订阅：沧元图"}]},
            }
        )

        self.assertIsInstance(reply, str)
        self.assertIn("MoviePilot 订阅列表", reply)
        self.assertIn("当前订阅：沧元图", reply)

    def test_empty_tasks_result_returns_a_clear_reply(self):
        reply = self._tool("tasks")._format_result(
            {"ok": True, "tool": "query_tasks", "result": {}}
        )

        self.assertEqual("MoviePilot 下载任务（query_tasks）\n\n暂无数据。", reply)

    def test_search_keyword_keeps_quoted_title_without_command_words(self):
        self.assertEqual(
            "仙逆",
            self._tool("search")._search_keyword("请在 MoviePilot 搜索《仙逆》"),
        )
        self.assertEqual(
            "凡人修仙传",
            self._tool("search")._search_keyword("请在 MoviePilot 搜 凡人修仙传"),
        )

    def test_search_result_is_rendered_as_a_sendable_text_reply(self):
        reply = self._tool("search")._format_result(
            {
                "ok": True,
                "tool": "search_media",
                "result": {"content": [{"type": "text", "text": "搜索结果：仙逆"}]},
            }
        )

        self.assertEqual("MoviePilot 搜索结果（search_media）\n\n搜索结果：仙逆", reply)

    def test_subscriptions_text_wrapped_json_is_rendered_as_readable_rows(self):
        reply = self._tool("subscriptions")._format_result(
            {
                "ok": True,
                "tool": "query_subscribes",
                "result": {
                    "content": [
                        {
                            "type": "text",
                            "text": (
                                "第 1/1 页，当前页 1 条结果，共 1 条。\n\n"
                                '[{"name":"仙逆","year":"2023","type":"电视剧",'
                                '"season":1,"resolution":"4K|2160p|x2160",'
                                '"total_episode":200,"start_episode":138,'
                                '"lack_episode":60,"state":"R",'
                                '"last_update":"2026-05-03 20:12:39",'
                                '"save_path":"/secret/path"}]'
                            ),
                        }
                    ]
                },
            }
        )

        self.assertIn("1. 仙逆 · 2023 · 电视剧 · 第 1 季", reply)
        self.assertIn("4K / 2160p / x2160", reply)
        self.assertIn("共 200 集 · 从 E138 开始订阅 · 缺 60 集", reply)
        self.assertIn("🟢 已启用 · 更新：2026-05-03 20:12:39", reply)
        self.assertNotIn("/secret/path", reply)

    def test_subscriptions_gateway_envelope_is_not_sent_as_raw_json(self):
        response = {
            "success": True,
            "result": (
                "第 1/1 页，当前页 1 条结果，共 1 条。\n\n"
                '[{"name":"仙逆","year":"2023","type":"电视剧",'
                '"season":1,"total_episode":200,"lack_episode":60,"state":"R"}]'
            ),
            "error": None,
        }
        reply = self._tool("subscriptions")._format_result(
            {"ok": True, "tool": "query_subscribes", "result": response}
        )

        self.assertIn("1. 仙逆 · 2023 · 电视剧 · 第 1 季", reply)
        self.assertIn("共 200 集 · 缺 60 集", reply)
        self.assertNotIn('"success"', reply)
        self.assertNotIn('"save_path"', reply)

    def test_serialized_subscription_gateway_envelope_is_not_sent_as_raw_json(self):
        response = json.dumps(
            {
                "success": True,
                "result": '[{"name":"凡人修仙传","season":1,"lack_episode":14,"state":"R"}]',
                "error": None,
            },
            ensure_ascii=False,
        )
        reply = self._tool("subscriptions")._format_result(
            {"ok": True, "tool": "query_subscribes", "result": response}
        )

        self.assertIn("1. 凡人修仙传 · 第 1 季", reply)
        self.assertIn("缺 14 集", reply)
        self.assertNotIn('"success"', reply)

    def test_full_mcp_url_is_normalized_to_api_base(self):
        adapter = MoviePilotServiceAdapter(
            {
                "enabled": True,
                "baseUrl": "https://mp.example/api/v1/mcp",
                "apiToken": "test-token",
            }
        )

        self.assertEqual("https://mp.example/api/v1", adapter.api_base_url)


class MoviePilotSearchApiTests(unittest.TestCase):
    def test_search_endpoint_rejects_disabled_connection_without_calling_moviepilot(self):
        responses = []
        handler = type("Handler", (), {})()
        handler._read_json_body = lambda: {"query": "仙逆"}
        handler._send_json = lambda status, payload: responses.append((status, payload))
        handler._log_event = lambda **kwargs: None

        with patch.object(dev_server, "_read_store_unlocked", return_value={"moviePilotConfig": {"enabled": False}}), patch.object(
            dev_server, "MoviePilotServiceAdapter"
        ) as adapter:
            dev_server.AppHandler._handle_moviepilot_search(handler)

        self.assertEqual(400, responses[0][0])
        self.assertIn("尚未启用", responses[0][1]["error"])
        adapter.assert_not_called()


if __name__ == "__main__":
    unittest.main()
