import pathlib
import tempfile
import unittest

from backend_modules.media_identity_service import MediaIdentityService


class MediaIdentityServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.cache_path = pathlib.Path(self.temp_dir.name) / "media_identity_cache.json"

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_prefers_emby_provider_id_without_tmdb_search(self) -> None:
        tmdb_calls = []

        def emby_fetcher(path: str):
            return {
                "Items": [
                    {
                        "Id": "series-1",
                        "Name": "完美世界",
                        "Type": "Series",
                        "ProductionYear": 2021,
                        "ProviderIds": {"Tmdb": "100"},
                    }
                ]
            }

        def tmdb_fetcher(path: str):
            tmdb_calls.append(path)
            return {}

        service = MediaIdentityService(
            emby_fetcher=emby_fetcher,
            tmdb_fetcher=tmdb_fetcher,
            cache_path=self.cache_path,
        )
        result = service.resolve("完美世界", preferred_type="series")

        self.assertEqual(result["identity"]["tmdbId"], "100")
        self.assertEqual(result["embyItem"]["Id"], "series-1")
        self.assertEqual(tmdb_calls, [])

    def test_tmdb_identity_falls_back_to_emby_title_and_year(self) -> None:
        def emby_fetcher(path: str):
            return {
                "Items": [
                    {
                        "Id": "series-2",
                        "Name": "牧神记",
                        "Type": "Series",
                        "ProductionYear": 2024,
                        "ProviderIds": {},
                    }
                ]
            }

        def tmdb_fetcher(path: str):
            return {
                "results": [
                    {
                        "id": 200,
                        "name": "牧神记",
                        "first_air_date": "2024-10-01",
                        "vote_average": 8.1,
                    }
                ]
            }

        service = MediaIdentityService(
            emby_fetcher=emby_fetcher,
            tmdb_fetcher=tmdb_fetcher,
            cache_path=self.cache_path,
        )
        result = service.resolve("牧神记", preferred_type="series")

        self.assertEqual(result["identity"]["tmdbId"], "200")
        self.assertEqual(result["identity"]["embyId"], "series-2")

    def test_reports_ambiguous_same_title_candidates(self) -> None:
        def emby_fetcher(path: str):
            return {"Items": []}

        def tmdb_fetcher(path: str):
            return {
                "results": [
                    {"id": 1, "name": "同名作品", "first_air_date": "2020-01-01"},
                    {"id": 2, "name": "同名作品", "first_air_date": "2024-01-01"},
                ]
            }

        service = MediaIdentityService(
            emby_fetcher=emby_fetcher,
            tmdb_fetcher=tmdb_fetcher,
            cache_path=self.cache_path,
        )
        result = service.resolve("同名作品", preferred_type="series")

        self.assertTrue(result["ambiguous"])
        self.assertEqual(len(result["candidates"]), 2)

    def test_retries_emby_with_tmdb_standard_title_after_alias_search(self) -> None:
        def emby_fetcher(path: str):
            if "SearchTerm=%E5%85%89%E9%98%B4%E4%B9%8B%E5%A4%96" in path:
                return {
                    "Items": [
                        {
                            "Id": "series-alias",
                            "Name": "光阴之外",
                            "Type": "Series",
                            "ProductionYear": 2023,
                            "ProviderIds": {"Tmdb": "400"},
                        }
                    ]
                }
            if "SearchTerm=%E5%85%89%E9%98%B4" in path:
                return {"Items": []}
            return {"Items": []}

        def tmdb_fetcher(path: str):
            return {"results": [{"id": 400, "name": "光阴之外", "first_air_date": "2023-01-01"}]}

        service = MediaIdentityService(
            emby_fetcher=emby_fetcher,
            tmdb_fetcher=tmdb_fetcher,
            cache_path=self.cache_path,
        )
        result = service.resolve("光阴", preferred_type="series")

        self.assertEqual(result["embyItem"]["Id"], "series-alias")
        self.assertEqual(result["identity"]["tmdbId"], "400")

    def test_cache_restores_tmdb_to_emby_mapping(self) -> None:
        item = {"Id": "series-3", "Name": "光阴之外", "Type": "Series", "ProviderIds": {"Tmdb": "300"}}

        first = MediaIdentityService(
            emby_fetcher=lambda path: {"Items": [item]},
            tmdb_fetcher=None,
            cache_path=self.cache_path,
        )
        first.resolve("光阴之外", preferred_type="series")

        def cached_fetcher(path: str):
            if path.startswith("/Items/series-3"):
                return item
            return {"Items": []}

        second = MediaIdentityService(
            emby_fetcher=cached_fetcher,
            tmdb_fetcher=None,
            cache_path=self.cache_path,
        )
        resolved = second.find_emby_item({"tmdbId": "300", "type": "series", "title": "光阴之外"})

        self.assertEqual(resolved["Id"], "series-3")

    def test_tmdb_expected_175_minus_emby_1_to_162(self) -> None:
        expected = {"totalEpisodes": 175, "seasonCounts": {1: 175}}
        existing = {
            "seasonMap": {1: set(range(1, 163))},
            "specials": [],
            "duplicates": [],
        }

        compared = MediaIdentityService.compare_episode_inventory(expected, existing)

        self.assertEqual(compared["expectedCount"], 175)
        self.assertEqual(compared["existingCount"], 162)
        self.assertEqual(compared["missing"], list(range(163, 176)))

    def test_inventory_difference_ignores_duplicates_and_specials(self) -> None:
        expected = {"totalEpisodes": 5, "seasonCounts": {1: 5}}
        existing = {
            "seasonMap": {1: {1, 2, 4}},
            "specials": [{"season": 0, "episode": 1}],
            "duplicates": [{"season": 1, "episode": 2}],
        }

        compared = MediaIdentityService.compare_episode_inventory(expected, existing)

        self.assertEqual(compared["missing"], [3, 5])
        self.assertEqual(len(compared["specials"]), 1)
        self.assertEqual(len(compared["duplicates"]), 1)

    def test_standard_multi_season_inventory_uses_tmdb_offsets(self) -> None:
        expected = {"totalEpisodes": 5, "seasonCounts": {1: 3, 2: 2}}
        existing = {"seasonMap": {1: {1, 2, 3}, 2: {1}}, "specials": [], "duplicates": []}

        compared = MediaIdentityService.compare_episode_inventory(expected, existing)

        self.assertEqual(compared["mode"], "seasonal")
        self.assertEqual(compared["existingCount"], 4)
        self.assertEqual(compared["missing"], [5])

    def test_query_media_detail_normalizes_tmdb_episode_counts(self) -> None:
        def tmdb_fetcher(path: str):
            if path.startswith("/tv/224839/season/1?"):
                return {
                    "episodes": [
                        {"episode_number": 1, "air_date": "2000-01-01"},
                        {"episode_number": 2, "air_date": "2999-01-01"},
                        {"episode_number": 3, "air_date": None},
                    ]
                }
            return {
                "id": 224839,
                "name": "遮天",
                "first_air_date": "2023-01-01",
                "number_of_episodes": 3,
                "seasons": [{"season_number": 0, "episode_count": 2}, {"season_number": 1, "episode_count": 3}],
            }

        service = MediaIdentityService(
            emby_fetcher=lambda path: {},
            tmdb_fetcher=tmdb_fetcher,
            cache_path=self.cache_path,
        )

        detail = service.query_media_detail("224839", "tv")

        self.assertEqual(detail["totalEpisodes"], 3)
        self.assertEqual(detail["seasonCounts"], {1: 3})
        self.assertEqual(detail["specialEpisodeCount"], 2)
        self.assertEqual(detail["airedSeasonMap"], {1: {1}})
        self.assertEqual(detail["futureSeasonMap"], {1: {2}})
        self.assertEqual(detail["unknownAirDateMap"], {1: {3}})

    def test_query_library_inventory_excludes_specials_from_episode_rows(self) -> None:
        def emby_fetcher(path: str):
            if "AnyProviderIdEqualTo=tmdb%3A79481" in path:
                return {"Items": [{"Id": "doupo", "Name": "斗破苍穹", "Type": "Series", "ProviderIds": {"Tmdb": "79481"}}]}
            if "ParentId=doupo" in path and "IncludeItemTypes=Season" in path:
                return {"Items": [{"Id": "season-1", "IndexNumber": 1, "Name": "Season 1"}]}
            if "ParentId=doupo" in path and "IncludeItemTypes=Episode" in path:
                return {
                    "Items": [
                        {"ParentIndexNumber": 0, "IndexNumber": 1, "Name": "特别篇1", "LocationType": "", "IsMissing": False},
                        {"ParentIndexNumber": 1, "IndexNumber": 1, "Name": "正片1", "LocationType": "", "IsMissing": False},
                        {"ParentIndexNumber": 1, "IndexNumber": 2, "Name": "正片2", "LocationType": "", "IsMissing": False},
                    ],
                    "TotalRecordCount": 3,
                }
            raise AssertionError(f"unexpected path: {path}")

        service = MediaIdentityService(
            emby_fetcher=emby_fetcher,
            tmdb_fetcher=None,
            cache_path=self.cache_path,
        )

        payload = service.query_library_exists_by_tmdb({"title": "斗破苍穹", "type": "series", "tmdbId": "79481"})

        self.assertTrue(payload["exists"])
        self.assertEqual(payload["episodeRows"], 2)
        self.assertEqual(len(payload["specials"]), 1)

    def test_tmdb_provider_lookup_precedes_title_search(self) -> None:
        paths = []

        def emby_fetcher(path: str):
            paths.append(path)
            if "AnyProviderIdEqualTo=tmdb%3A224839" in path:
                return {"Items": [{"Id": "series-1", "Name": "遮天", "Type": "Series", "ProviderIds": {"Tmdb": "224839"}}]}
            raise AssertionError(f"unexpected fallback lookup: {path}")

        service = MediaIdentityService(emby_fetcher=emby_fetcher, tmdb_fetcher=None, cache_path=self.cache_path)
        item = service.find_emby_item({"title": "遮天", "year": "2023", "type": "series", "tmdbId": "224839"})

        self.assertEqual(item["Id"], "series-1")
        self.assertEqual(len(paths), 1)

    def test_local_candidates_rank_exact_series_above_wrong_work(self) -> None:
        def emby_fetcher(path: str):
            if "IncludeItemTypes=Series" not in path:
                raise AssertionError(f"unexpected path: {path}")
            return {
                "Items": [
                    {
                        "Id": "dark",
                        "Name": "暗黑",
                        "Type": "Series",
                        "ProductionYear": 2017,
                        "ProviderIds": {"Tmdb": "70523"},
                        "RecursiveItemCount": 26,
                    },
                    {
                        "Id": "xian-ni",
                        "Name": "仙逆",
                        "Type": "Series",
                        "ProductionYear": 2023,
                        "ProviderIds": {"Tmdb": "223911"},
                        "RecursiveItemCount": 147,
                    },
                ]
            }

        service = MediaIdentityService(emby_fetcher=emby_fetcher, tmdb_fetcher=None, cache_path=self.cache_path)
        candidates = service.search_local_candidates("仙逆", preferred_type="series", tmdb_id="223911", year="2023")

        self.assertEqual(candidates[0]["embyItemId"], "xian-ni")
        self.assertTrue(candidates[0]["isTitleExact"])
        self.assertEqual(candidates[0]["tmdbId"], "223911")

    def test_library_inventory_marks_virtual_missing_episodes(self) -> None:
        def emby_fetcher(path: str):
            if "AnyProviderIdEqualTo=tmdb%3A223911" in path:
                return {"Items": [{"Id": "xian-ni", "Name": "仙逆", "Type": "Series", "ProviderIds": {"Tmdb": "223911"}}]}
            if "ParentId=xian-ni" in path and "IncludeItemTypes=Season" in path:
                return {"Items": [{"Id": "season-1", "IndexNumber": 1, "Name": "Season 1"}]}
            if "ParentId=xian-ni" in path and "IncludeItemTypes=Episode" in path:
                return {
                    "Items": [
                        {"Id": "e1", "ParentIndexNumber": 1, "IndexNumber": 1, "Name": "第1集", "LocationType": "FileSystem", "IsMissing": False},
                        {"Id": "e2", "ParentIndexNumber": 1, "IndexNumber": 2, "Name": "第2集", "LocationType": "Virtual", "IsMissing": True},
                    ],
                    "TotalRecordCount": 2,
                }
            raise AssertionError(f"unexpected path: {path}")

        service = MediaIdentityService(emby_fetcher=emby_fetcher, tmdb_fetcher=None, cache_path=self.cache_path)
        result = service.query_library_exists({"title": "仙逆", "type": "series", "tmdbId": "223911"})

        self.assertTrue(result["hasMissingEpisodeData"])
        self.assertEqual(result["seasonMap"], {1: {1}})
        self.assertEqual(result["missingEpisodeMap"], {1: {2}})

    def test_query_library_exists_by_tmdb_uses_exact_provider_match(self) -> None:
        paths = []

        def emby_fetcher(path: str):
            paths.append(path)
            if "AnyProviderIdEqualTo=tmdb%3A223911" in path:
                return {"Items": [{"Id": "xian-ni", "Name": "仙逆", "Type": "Series", "ProviderIds": {"Tmdb": "223911"}}]}
            if "ParentId=xian-ni" in path and "IncludeItemTypes=Season" in path:
                return {"Items": [{"Id": "season-1", "IndexNumber": 1, "Name": "Season 1"}]}
            if "ParentId=xian-ni" in path and "IncludeItemTypes=Episode" in path:
                return {"Items": [{"Id": "e1", "ParentIndexNumber": 1, "IndexNumber": 1, "Name": "第1集"}], "TotalRecordCount": 1}
            raise AssertionError(f"unexpected path: {path}")

        service = MediaIdentityService(emby_fetcher=emby_fetcher, tmdb_fetcher=None, cache_path=self.cache_path)
        result = service.query_library_exists_by_tmdb({"title": "仙逆", "year": "2023", "type": "series", "tmdbId": "223911"})

        self.assertTrue(result["exists"])
        self.assertEqual(result["embyItem"]["Id"], "xian-ni")
        self.assertEqual(len(paths), 3)

    def test_query_library_exists_falls_back_to_title_only_after_exact_miss(self) -> None:
        paths = []

        def emby_fetcher(path: str):
            paths.append(path)
            if "AnyProviderIdEqualTo=tmdb%3A223911" in path:
                return {"Items": []}
            if "SearchTerm=%E4%BB%99%E9%80%86" in path:
                return {"Items": [{"Id": "xian-ni", "Name": "仙逆", "Type": "Series", "ProductionYear": 2023, "ProviderIds": {}}]}
            if "ParentId=xian-ni" in path and "IncludeItemTypes=Season" in path:
                return {"Items": [{"Id": "season-1", "IndexNumber": 1, "Name": "Season 1"}]}
            if "ParentId=xian-ni" in path and "IncludeItemTypes=Episode" in path:
                return {"Items": [{"Id": "e1", "ParentIndexNumber": 1, "IndexNumber": 1, "Name": "第1集"}], "TotalRecordCount": 1}
            raise AssertionError(f"unexpected path: {path}")

        service = MediaIdentityService(emby_fetcher=emby_fetcher, tmdb_fetcher=None, cache_path=self.cache_path)
        result = service.query_library_exists({"title": "仙逆", "year": "2023", "type": "series", "tmdbId": "223911"})

        self.assertTrue(result["exists"])
        self.assertIn("AnyProviderIdEqualTo=tmdb%3A223911", paths[0])
        self.assertTrue(any("SearchTerm=%E4%BB%99%E9%80%86" in path for path in paths))

    def test_library_inventory_reports_actual_emby_query_count(self) -> None:
        paths = []

        def emby_fetcher(path: str):
            paths.append(path)
            if "AnyProviderIdEqualTo=tmdb%3A224839" in path:
                return {"Items": [{"Id": "series-1", "Name": "遮天", "Type": "Series", "ProviderIds": {"Tmdb": "224839"}}]}
            if "ParentId=series-1" in path and "IncludeItemTypes=Season" in path:
                return {"Items": [{"Id": "season-1", "IndexNumber": 1, "Name": "Season 1"}]}
            if "ParentId=series-1" in path and "IncludeItemTypes=Episode" in path:
                return {"Items": [{"Id": "e1", "SeriesId": "series-1", "ParentIndexNumber": 1, "IndexNumber": 1}], "TotalRecordCount": 1}
            raise AssertionError(f"unexpected path: {path}")

        service = MediaIdentityService(emby_fetcher=emby_fetcher, tmdb_fetcher=None, cache_path=self.cache_path)
        result = service.query_library_exists({"title": "遮天", "type": "series", "tmdbId": "224839"})

        self.assertEqual(result["embyQueryCount"], 3)
        self.assertEqual(len(paths), 3)

    def test_query_library_exists_by_tmdb_filters_broken_provider_query_client_side(self) -> None:
        def emby_fetcher(path: str):
            if "AnyProviderIdEqualTo=tmdb%3A281233" in path:
                return {
                    "Items": [
                        {"Id": "dark", "Name": "暗黑", "Type": "Series", "ProviderIds": {"Tmdb": "70523"}},
                        {"Id": "guangyin", "Name": "光阴之外", "Type": "Series", "ProviderIds": {"Tmdb": "281233"}},
                    ]
                }
            if "ParentId=guangyin" in path and "IncludeItemTypes=Season" in path:
                return {"Items": [{"Id": "season-1", "IndexNumber": 1, "Name": "第 1 季"}]}
            if "ParentId=guangyin" in path and "IncludeItemTypes=Episode" in path:
                return {"Items": [{"Id": "ep1", "ParentIndexNumber": 1, "IndexNumber": 1, "Name": "第1集"}], "TotalRecordCount": 1}
            raise AssertionError(f"unexpected path: {path}")

        service = MediaIdentityService(emby_fetcher=emby_fetcher, tmdb_fetcher=None, cache_path=self.cache_path)
        result = service.query_library_exists_by_tmdb({"title": "光阴之外", "year": "2025", "type": "series", "tmdbId": "281233"})

        self.assertTrue(result["exists"])
        self.assertEqual(result["embyItem"]["Id"], "guangyin")

    def test_query_library_exists_by_tmdb_can_fetch_item_by_emby_id_via_items_query(self) -> None:
        def emby_fetcher(path: str):
            if path.startswith("/Items?Ids=122937&"):
                return {"Items": [{"Id": "122937", "Name": "光阴之外", "Type": "Series", "ProductionYear": 2025, "ProviderIds": {"Tmdb": "281233"}}]}
            if "ParentId=122937" in path and "IncludeItemTypes=Season" in path:
                return {"Items": [{"Id": "season-1", "IndexNumber": 1, "Name": "第 1 季"}]}
            if "ParentId=122937" in path and "IncludeItemTypes=Episode" in path:
                return {"Items": [{"Id": "ep1", "ParentIndexNumber": 1, "IndexNumber": 1, "Name": "第1集"}], "TotalRecordCount": 1}
            raise AssertionError(f"unexpected path: {path}")

        service = MediaIdentityService(emby_fetcher=emby_fetcher, tmdb_fetcher=None, cache_path=self.cache_path)
        result = service.query_library_exists_by_tmdb({"title": "光阴之外", "year": "2025", "type": "series", "tmdbId": "281233", "embyId": "122937", "forceEmbyItem": True})

        self.assertTrue(result["exists"])
        self.assertEqual(result["embyItem"]["Id"], "122937")

    def test_future_episodes_are_not_reported_missing(self) -> None:
        expected = {
            "totalEpisodes": 5,
            "seasonCounts": {1: 5},
            "registeredSeasonMap": {1: {1, 2, 3, 4, 5}},
            "airedSeasonMap": {1: {1, 2, 3}},
            "futureSeasonMap": {1: {4, 5}},
            "unknownAirDateMap": {},
        }
        existing = {"seasonMap": {1: {1, 2}}, "specials": [], "duplicates": []}

        compared = MediaIdentityService.compare_episode_inventory(expected, existing)

        self.assertEqual(compared["missingLabels"], ["E3"])
        self.assertEqual(compared["futureLabels"], ["E4", "E5"])

    def test_standard_multi_season_returns_season_labels(self) -> None:
        expected = {
            "totalEpisodes": 4,
            "seasonCounts": {1: 2, 2: 2},
            "registeredSeasonMap": {1: {1, 2}, 2: {1, 2}},
            "airedSeasonMap": {1: {1, 2}, 2: {1, 2}},
            "futureSeasonMap": {},
            "unknownAirDateMap": {},
        }
        existing = {"seasonMap": {1: {1}, 2: {1}}, "specials": [], "duplicates": []}

        compared = MediaIdentityService.compare_episode_inventory(expected, existing)

        self.assertEqual(compared["mode"], "seasonal")
        self.assertEqual(compared["missingLabels"], ["S01E02", "S02E02"])

    def test_multi_season_tmdb_with_single_global_emby_season_uses_global_numbers(self) -> None:
        expected = {
            "totalEpisodes": 4,
            "seasonCounts": {1: 2, 2: 2},
            "registeredSeasonMap": {1: {1, 2}, 2: {1, 2}},
            "airedSeasonMap": {1: {1, 2}, 2: {1, 2}},
            "futureSeasonMap": {},
            "unknownAirDateMap": {},
        }
        existing = {"seasonMap": {1: {1, 2, 4}}, "specials": [], "duplicates": []}

        compared = MediaIdentityService.compare_episode_inventory(expected, existing)

        self.assertEqual(compared["mode"], "global")
        self.assertEqual(compared["missingLabels"], ["E3"])


if __name__ == "__main__":
    unittest.main()
