from __future__ import annotations

import pathlib
import sys
import unittest
from unittest.mock import patch


TOOL_DIR = pathlib.Path(__file__).resolve().parents[1] / "episode-checker"
sys.path.insert(0, str(TOOL_DIR))

from tmdb_client import TMDBClient  # noqa: E402


class FakeResponse:
    def __init__(self, payload: dict, status_code: int = 200) -> None:
        self.payload = payload
        self.status_code = status_code
        self.text = "ok"

    def json(self) -> dict:
        return self.payload


class EpisodeCheckerTMDBClientTest(unittest.TestCase):
    def test_api_key_mode_uses_query_parameter(self) -> None:
        calls: list[dict] = []

        def fake_get(url: str, *, params: dict, headers: dict, timeout: int):
            calls.append({"url": url, "params": params, "headers": headers, "timeout": timeout})
            return FakeResponse({"results": [{"id": 1, "name": "仙逆", "first_air_date": "2023-01-01"}]})

        with patch("requests.get", side_effect=fake_get):
            client = TMDBClient("api-key-1", auth_mode="api_key", language="zh-CN", region="CN")
            client.search_tv("仙逆")

        self.assertEqual(calls[0]["params"]["api_key"], "api-key-1")
        self.assertNotIn("Authorization", calls[0]["headers"])
        self.assertEqual(calls[0]["params"]["region"], "CN")

    def test_bearer_mode_uses_authorization_header(self) -> None:
        calls: list[dict] = []

        def fake_get(url: str, *, params: dict, headers: dict, timeout: int):
            calls.append({"url": url, "params": params, "headers": headers, "timeout": timeout})
            return FakeResponse(
                {
                    "id": 224839,
                    "name": "遮天",
                    "original_name": "遮天",
                    "overview": "",
                    "number_of_episodes": 175,
                    "seasons": [{"season_number": 1, "name": "第 1 季", "episode_count": 175, "air_date": "2023-05-03"}],
                }
            )

        with patch("requests.get", side_effect=fake_get):
            client = TMDBClient("bearer-token-1", auth_mode="bearer", language="zh-CN", region="CN")
            client.get_tv_detail(224839)

        self.assertEqual(calls[0]["headers"]["Authorization"], "Bearer bearer-token-1")
        self.assertNotIn("api_key", calls[0]["params"])
        self.assertEqual(calls[0]["params"]["language"], "zh-CN")


if __name__ == "__main__":
    unittest.main()
