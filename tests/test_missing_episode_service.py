from __future__ import annotations

import unittest
import urllib.parse

from backend_modules.missing_episode_service import MissingEpisodeService


class _FakeMissingEpisodeService(MissingEpisodeService):
    def __init__(self, *, emby_rows, tmdb_rows) -> None:
        super().__init__(emby_fetcher=self._emby_fetch, tmdb_token="test-token")
        self.emby_rows = emby_rows
        self.tmdb_rows = tmdb_rows

    def _emby_fetch(self, path: str):
        if path.startswith("/Items?"):
            query = urllib.parse.parse_qs(urllib.parse.urlsplit(path).query)
            include_types = ",".join(query.get("IncludeItemTypes") or [])
            if "Episode" in include_types:
                episodes = self.emby_rows[0].get("episodes", [])
                return {"Items": episodes, "TotalRecordCount": len(episodes)}
            if "Season" in include_types:
                return {"Items": [{"Id": "season-1", "Name": "Season 1", "IndexNumber": 1}]}
            return {"Items": self.emby_rows, "TotalRecordCount": len(self.emby_rows)}
        if path.startswith("/Shows/"):
            return {"Items": self.emby_rows[0].get("episodes", [])}
        return {"Items": []}

    def _tmdb_get_json(self, path_with_query: str):
        return self.tmdb_rows.get(path_with_query.split("?", 1)[0], {})


class MissingEpisodeServiceTests(unittest.TestCase):
    def _service(self, *, episodes, tmdb_season_episodes, provider_id="223911"):
        return _FakeMissingEpisodeService(
            emby_rows=[
                {
                    "Id": "series-1",
                    "Name": "沧元图",
                    "Type": "Series",
                    "ProductionYear": 2023,
                    "ProviderIds": {"Tmdb": provider_id} if provider_id else {},
                    "episodes": episodes,
                }
            ],
            tmdb_rows={
                f"/tv/{provider_id}": {"seasons": [{"season_number": 1, "episode_count": len(tmdb_season_episodes)}]},
                f"/tv/{provider_id}/season/1": {"episodes": tmdb_season_episodes},
            },
        )

    def test_does_not_mark_unreleased_or_unknown_air_date_episodes_as_missing(self) -> None:
        local = [{"ParentIndexNumber": 1, "IndexNumber": index} for index in range(1, 86)]
        tmdb = [{"episode_number": index, "air_date": "2025-01-01"} for index in range(1, 86)]
        tmdb.extend({"episode_number": index, "air_date": None} for index in range(86, 102))

        result = self._service(episodes=local, tmdb_season_episodes=tmdb).scan()

        self.assertEqual(result["summary"]["missingEpisodeCount"], 0)
        self.assertEqual(result["rows"], [])

    def test_reports_only_missing_episodes_that_have_already_aired(self) -> None:
        local = [{"ParentIndexNumber": 1, "IndexNumber": index} for index in [1, 2, 3, 5]]
        tmdb = [{"episode_number": index, "air_date": "2025-01-01"} for index in range(1, 6)]

        result = self._service(episodes=local, tmdb_season_episodes=tmdb).scan()

        self.assertEqual(len(result["rows"]), 1)
        row = result["rows"][0]
        self.assertEqual(row["missingEpisodes"], [4])
        self.assertEqual(row["completeness"], "4/5")

    def test_scan_does_not_use_title_guess_when_emby_has_no_tmdb_id(self) -> None:
        service = self._service(
            episodes=[{"ParentIndexNumber": 1, "IndexNumber": 1}],
            tmdb_season_episodes=[{"episode_number": 1, "air_date": "2025-01-01"}],
            provider_id="",
        )

        result = service.scan()

        self.assertEqual(result["summary"]["matchedTmdbSeries"], 0)
        self.assertEqual(result["rows"][0]["status"], "match_failed")

    def test_mapping_anomaly_never_becomes_a_definite_missing_result(self) -> None:
        local = [{"ParentIndexNumber": 1, "IndexNumber": index} for index in range(1, 11)]
        local.extend({"ParentIndexNumber": 1, "IndexNumber": index} for index in range(101, 117))
        tmdb = [{"episode_number": index, "air_date": "2025-01-01"} for index in range(1, 21)]

        result = self._service(episodes=local, tmdb_season_episodes=tmdb).scan()

        self.assertEqual(len(result["rows"]), 1)
        row = result["rows"][0]
        self.assertEqual(row["status"], "review")
        self.assertEqual(row["missingEpisodes"], [])
        self.assertTrue(row["referenceMissingLabels"])

    def test_scan_reports_progress_without_changing_strict_result(self) -> None:
        progress = []
        service = self._service(
            episodes=[{"ParentIndexNumber": 1, "IndexNumber": 1}],
            tmdb_season_episodes=[{"episode_number": 1, "air_date": "2025-01-01"}],
        )

        service.scan(progress_callback=progress.append)

        self.assertEqual(progress[0]["phase"], "loading_series")
        self.assertEqual(progress[-1]["phase"], "completed")
        self.assertEqual(progress[-1]["completed"], 1)

    def test_scan_bounds_worker_count_without_changing_strict_calculation(self) -> None:
        service = self._service(
            episodes=[{"ParentIndexNumber": 1, "IndexNumber": 1}],
            tmdb_season_episodes=[{"episode_number": 1, "air_date": "2025-01-01"}],
        )

        result = service.scan(scan_workers=99)

        self.assertEqual(result["debug"]["calculation"], "shared_strict_ai_result")
        self.assertEqual(result["debug"]["scanWorkers"], 1)


if __name__ == "__main__":
    unittest.main()
