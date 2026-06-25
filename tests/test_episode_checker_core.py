from __future__ import annotations

import argparse
import os
import pathlib
import sys
import tempfile
import unittest
from dataclasses import dataclass


TOOL_DIR = pathlib.Path(__file__).resolve().parents[1] / "episode-checker"
sys.path.insert(0, str(TOOL_DIR))

from checker import EpisodeChecker, format_text_result, result_to_json  # noqa: E402
from config import build_config, load_vistamirror_tmdb_config  # noqa: E402


@dataclass(frozen=True)
class FakeCandidate:
    id: int
    name: str
    year: str = "2023"


@dataclass(frozen=True)
class FakeSeason:
    number: int
    episode_count: int


@dataclass(frozen=True)
class FakeDetail:
    id: int
    name: str
    seasons: list[FakeSeason]


class FakeTMDBClient:
    def search_tv(self, name: str, page: int = 1) -> list[FakeCandidate]:
        return [FakeCandidate(id=10001, name=name)]

    def get_tv_detail(self, tmdb_id: int) -> FakeDetail:
        return FakeDetail(
            id=tmdb_id,
            name="测试剧",
            seasons=[FakeSeason(0, 2), FakeSeason(1, 5), FakeSeason(2, 3)],
        )


class EpisodeCheckerCoreTest(unittest.TestCase):
    def setUp(self) -> None:
        self.old_key = os.environ.get("TMDB_API_KEY")
        os.environ["TMDB_API_KEY"] = "test-key"

    def tearDown(self) -> None:
        if self.old_key is None:
            os.environ.pop("TMDB_API_KEY", None)
        else:
            os.environ["TMDB_API_KEY"] = self.old_key

    def _make_config(self, root: pathlib.Path, season: int | None = None) -> object:
        args = argparse.Namespace(
            directory=str(root),
            name="测试剧",
            season=season,
            config=None,
            exclude=None,
            verbose=False,
            json_output=False,
            include_season_0=False,
            use_vistamirror_config=False,
        )
        return build_config(args)

    def test_checker_compares_expected_and_local_episodes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            for name in ["测试剧 S01E01.mkv", "测试剧 S01E03.mkv", "测试剧 S01E05.mkv", "测试剧 S02E01.mkv"]:
                (root / name).write_text("", encoding="utf-8")

            result = EpisodeChecker(self._make_config(root), FakeTMDBClient()).run()

        self.assertEqual(result.total_expected, 8)
        self.assertEqual(result.total_existing, 4)
        self.assertEqual(result.seasons[0].missing, [2, 4])
        self.assertEqual(result.seasons[1].missing, [2, 3])
        self.assertIn("S01", format_text_result(result))
        self.assertIn('"totalMissing": 4', result_to_json(result))

    def test_season_filter_only_checks_requested_season(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            (root / "测试剧 S02E02.mkv").write_text("", encoding="utf-8")

            result = EpisodeChecker(self._make_config(root, season=2), FakeTMDBClient()).run()

        self.assertEqual([season.season for season in result.seasons], [2])
        self.assertEqual(result.total_expected, 3)
        self.assertEqual(result.total_existing, 1)
        self.assertEqual(result.seasons[0].missing, [1, 3])

    def test_build_config_prefers_environment_api_key(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            args = argparse.Namespace(
                directory=str(root),
                name="测试剧",
                season=None,
                config=None,
                exclude=None,
                verbose=False,
                json_output=False,
                include_season_0=False,
                use_vistamirror_config=True,
            )
            config = build_config(args)
        self.assertEqual(config.tmdb_auth_mode, "api_key")
        self.assertEqual(config.tmdb_credential, "test-key")

    def test_load_vistamirror_tmdb_config_reads_saved_token(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = pathlib.Path(tmp)
            data_dir = repo_root / "data"
            data_dir.mkdir(parents=True, exist_ok=True)
            (data_dir / "invites.json").write_text(
                '{"embyConfig":{"tmdbToken":"saved-token","tmdbLanguage":"ja-JP","tmdbRegion":"jp"}}',
                encoding="utf-8",
            )
            loaded = load_vistamirror_tmdb_config(repo_root)
        self.assertEqual(loaded["token"], "saved-token")
        self.assertEqual(loaded["language"], "ja-JP")
        self.assertEqual(loaded["region"], "JP")

    def test_build_config_can_use_saved_vistamirror_token(self) -> None:
        old_key = os.environ.pop("TMDB_API_KEY", None)
        try:
            with tempfile.TemporaryDirectory() as tmp:
                repo_root = pathlib.Path(tmp)
                data_dir = repo_root / "data"
                data_dir.mkdir(parents=True, exist_ok=True)
                (data_dir / "invites.json").write_text(
                    '{"embyConfig":{"tmdbToken":"saved-token","tmdbLanguage":"zh-CN","tmdbRegion":"cn"}}',
                    encoding="utf-8",
                )
                args = argparse.Namespace(
                    directory=str(repo_root),
                    name="测试剧",
                    season=None,
                    config=None,
                    exclude=None,
                    verbose=False,
                    json_output=False,
                    include_season_0=False,
                    use_vistamirror_config=True,
                )
                from unittest.mock import patch

                with patch("config.REPO_ROOT", repo_root):
                    config = build_config(args)
        finally:
            if old_key is not None:
                os.environ["TMDB_API_KEY"] = old_key
        self.assertEqual(config.tmdb_auth_mode, "bearer")
        self.assertEqual(config.tmdb_credential, "saved-token")


if __name__ == "__main__":
    unittest.main()
