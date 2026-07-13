from __future__ import annotations

import unittest

from backend_modules.missing_episode_service import MissingEpisodeService


class _FakeMissingEpisodeService(MissingEpisodeService):
    def __init__(self, *, emby_rows, tmdb_rows) -> None:
        super().__init__(emby_fetcher=self._emby_fetch, tmdb_token="test-token")
        self.emby_rows = emby_rows
        self.tmdb_rows = tmdb_rows

    def _emby_fetch(self, path: str):
        if path.startswith("/Items?"):
            return {"Items": self.emby_rows}
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


if __name__ == "__main__":
    unittest.main()
