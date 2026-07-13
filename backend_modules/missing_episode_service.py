from __future__ import annotations

from datetime import date, datetime
import json
import re
import urllib.parse
import urllib.request
from typing import Any, Callable, Union

EmbyFetcher = Callable[[str], Union[dict[str, Any], list[Any], str, None]]


class MissingEpisodeService:
    def __init__(
        self,
        *,
        emby_fetcher: EmbyFetcher,
        tmdb_token: str,
        tmdb_language: str = "zh-CN",
        tmdb_region: str = "CN",
        identity_resolver: Callable[[dict[str, Any]], str] | None = None,
    ) -> None:
        self.emby_fetcher = emby_fetcher
        self.tmdb_token = str(tmdb_token or "").strip()
        self.tmdb_language = str(tmdb_language or "zh-CN").strip() or "zh-CN"
        self.tmdb_region = str(tmdb_region or "CN").strip().upper() or "CN"
        self.identity_resolver = identity_resolver
        self._tmdb_detail_cache: dict[str, dict[str, Any]] = {}

    def scan(self, *, scan_limit: int = 1200) -> dict[str, Any]:
        safe_scan_limit = max(20, min(5000, int(scan_limit or 1200)))
        rows: list[dict[str, Any]] = []
        warnings: list[str] = []
        upstream_error = ""

        scanned_series = 0
        matched_tmdb_series = 0
        missing_series_ids: set[str] = set()
        missing_episode_count = 0
        unknown_match_count = 0
        tmdb_season_fetch_failures = 0
        scanned_at = datetime.now().isoformat(timespec="seconds")

        try:
            series_rows = self._fetch_all_series(limit=safe_scan_limit)
        except Exception as err:
            upstream_error = str(err)[:300]
            warnings.append(f"读取 Emby 剧集列表失败：{str(err)[:120]}")
            series_rows = []

        for series in series_rows:
            scanned_series += 1
            series_id = str(series.get("Id") or "").strip()
            series_name = str(series.get("Name") or "").strip() or "未命名剧集"
            if not series_id:
                continue

            try:
                local_episodes_by_season = self._fetch_local_episode_map(series_id)
            except Exception as err:
                warnings.append(f"{series_name}：读取 Emby 剧集失败（{str(err)[:120]}）")
                continue

            try:
                # A full-library scan has no user confirmation step.  Do not let a
                # title similarity guess turn an unrelated Emby series into a
                # definite missing-episode result.
                tmdb_id = self._match_tmdb_series_id(series, allow_title_fallback=False)
            except Exception as err:
                warnings.append(f"{series_name}：TMDB 匹配失败（{str(err)[:120]}）")
                tmdb_id = ""

            if not tmdb_id:
                unknown_match_count += 1
                rows.append(
                    {
                        "seriesName": series_name,
                        "seasonNo": 0,
                        "missingEpisodes": [],
                        "existingCount": sum(len(values) for values in local_episodes_by_season.values()),
                        "expectedCount": 0,
                        "completeness": "-",
                        "tmdbId": "",
                        "embySeriesId": series_id,
                        "status": "match_failed",
                        "reason": "未匹配到 TMDB 剧集",
                        "scannedAt": scanned_at,
                    }
                )
                continue

            matched_tmdb_series += 1
            # Missing management is an inventory-gap check, not a wish list of all
            # TMDB seasons.  Only compare seasons that Emby actually has episodes
            # for, and only against episodes that have already aired.
            local_seasons = {season for season, episodes in local_episodes_by_season.items() if season > 0 and episodes}
            if not local_seasons:
                continue

            try:
                tmdb_seasons = self._fetch_tmdb_season_inventory(tmdb_id, seasons=local_seasons)
            except Exception as err:
                tmdb_season_fetch_failures += 1
                unknown_match_count += 1
                warnings.append(f"{series_name}：TMDB 季详情读取失败（{str(err)[:120]}）")
                rows.append(
                    {
                        "seriesName": series_name,
                        "seasonNo": 0,
                        "missingEpisodes": [],
                        "existingCount": sum(len(values) for values in local_episodes_by_season.values()),
                        "expectedCount": 0,
                        "completeness": "-",
                        "tmdbId": tmdb_id,
                        "embySeriesId": series_id,
                        "status": "match_failed",
                        "reason": f"TMDB 季详情读取失败：{str(err)[:120]}",
                        "scannedAt": scanned_at,
                    }
                )
                continue
            if not tmdb_seasons:
                unknown_match_count += 1
                rows.append(
                    {
                        "seriesName": series_name,
                        "seasonNo": 0,
                        "missingEpisodes": [],
                        "existingCount": sum(len(values) for values in local_episodes_by_season.values()),
                        "expectedCount": 0,
                        "completeness": "-",
                        "tmdbId": tmdb_id,
                        "embySeriesId": series_id,
                        "status": "match_failed",
                        "reason": "TMDB 季数据为空或无有效季集信息",
                        "scannedAt": scanned_at,
                    }
                )
                continue

            for season_no in sorted(local_seasons):
                inventory = tmdb_seasons.get(season_no) if isinstance(tmdb_seasons, dict) else None
                if not isinstance(inventory, dict):
                    continue
                local_set = local_episodes_by_season.get(season_no, set())
                aired_set = set(inventory.get("airedEpisodes") or set())
                registered_set = set(inventory.get("registeredEpisodes") or set())
                # An episode with an unknown premiere date is deliberately not
                # treated as missing. This mirrors the AI tool's MP-style rule and
                # prevents long-running animations from reporting unreleased rows.
                if not aired_set:
                    continue
                expected_count = len(aired_set)
                expected_set = aired_set
                present_set = expected_set.intersection(local_set)
                missing_set = sorted(expected_set.difference(local_set))
                if not missing_set:
                    continue

                missing_series_ids.add(series_id)
                missing_episode_count += len(missing_set)
                rows.append(
                    {
                        "seriesName": series_name,
                        "seasonNo": season_no,
                        "missingEpisodes": missing_set,
                        "existingCount": len(present_set),
                        "expectedCount": expected_count,
                        "completeness": f"{len(present_set)}/{expected_count}",
                        "registeredCount": len(registered_set),
                        "futureCount": len(set(inventory.get("futureEpisodes") or set())),
                        "unknownAirDateCount": len(set(inventory.get("unknownAirDateEpisodes") or set())),
                        "tmdbId": tmdb_id,
                        "embySeriesId": series_id,
                        "status": "missing",
                        "reason": "",
                        "scannedAt": scanned_at,
                    }
                )

        rows.sort(
            key=lambda row: (
                0 if str(row.get("status") or "") == "missing" else 1,
                str(row.get("seriesName") or ""),
                int(row.get("seasonNo") or 0),
            )
        )
        summary = {
            "scannedSeries": scanned_series,
            "matchedTmdbSeries": matched_tmdb_series,
            "missingSeries": len(missing_series_ids),
            "missingEpisodeCount": missing_episode_count,
            "unknownMatchCount": unknown_match_count,
            "scannedAt": scanned_at,
        }
        debug = {
            "scanLimit": safe_scan_limit,
            "rowCount": len(rows),
            "tmdbSeasonFetchFailures": tmdb_season_fetch_failures,
            "upstreamError": upstream_error,
        }
        return {"summary": summary, "rows": rows, "warnings": warnings[:200], "debug": debug}

    def _fetch_all_series(self, *, limit: int) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        start_index = 0
        page_size = 200
        while len(rows) < limit:
            current_limit = min(page_size, limit - len(rows))
            path = (
                "/Items?Recursive=true&IncludeItemTypes=Series"
                "&Fields=Name,ProductionYear,PremiereDate,ProviderIds"
                f"&SortBy=SortName&SortOrder=Ascending&Limit={current_limit}&StartIndex={start_index}"
            )
            payload = self.emby_fetcher(path)
            items = payload.get("Items") if isinstance(payload, dict) else payload
            if not isinstance(items, list) or not items:
                break
            normalized = [item for item in items if isinstance(item, dict)]
            rows.extend(normalized)
            start_index += len(items)
            if len(items) < current_limit:
                break
        return rows

    def _fetch_local_episode_map(self, series_id: str) -> dict[int, set[int]]:
        safe_series_id = urllib.parse.quote(str(series_id or "").strip(), safe="")
        if not safe_series_id:
            return {}

        season_map: dict[int, set[int]] = {}
        start_index = 0
        page_size = 500
        while True:
            path = (
                f"/Shows/{safe_series_id}/Episodes"
                "?Fields=ParentIndexNumber,IndexNumber,Name,LocationType,IsMissing"
                f"&Limit={page_size}&StartIndex={start_index}"
            )
            payload = self.emby_fetcher(path)
            items = payload.get("Items") if isinstance(payload, dict) else payload
            if not isinstance(items, list) or not items:
                break
            for episode in items:
                if not isinstance(episode, dict):
                    continue
                if bool(episode.get("IsMissing")) or str(episode.get("LocationType") or "").strip().lower() == "virtual":
                    continue
                season_no = self._safe_int(episode.get("ParentIndexNumber"))
                episode_no = self._safe_int(episode.get("IndexNumber"))
                if season_no <= 0 or episode_no <= 0:
                    continue
                season_map.setdefault(season_no, set()).add(episode_no)
            start_index += len(items)
            if len(items) < page_size:
                break
        return season_map

    def _match_tmdb_series_id(self, series: dict[str, Any], *, allow_title_fallback: bool = True) -> str:
        provider_ids = series.get("ProviderIds")
        if isinstance(provider_ids, dict):
            direct_id = str(provider_ids.get("Tmdb") or provider_ids.get("tmdb") or "").strip()
            if direct_id.isdigit():
                return direct_id

        if self.identity_resolver:
            try:
                resolved_id = str(self.identity_resolver(series) or "").strip()
            except Exception:
                resolved_id = ""
            if resolved_id.isdigit():
                return resolved_id

        if not allow_title_fallback:
            return ""

        series_name = str(series.get("Name") or "").strip()
        if not series_name:
            return ""
        year = self._safe_int(series.get("ProductionYear")) or self._safe_int(str(series.get("PremiereDate") or "")[:4])

        params = {
            "query": series_name,
            "language": self.tmdb_language,
        }
        if year > 1900:
            params["first_air_date_year"] = str(year)
        if self.tmdb_region:
            params["region"] = self.tmdb_region

        payload = self._tmdb_get_json(f"/search/tv?{urllib.parse.urlencode(params)}")
        results = payload.get("results") if isinstance(payload, dict) else []
        if not isinstance(results, list):
            return ""
        best_score = -10**9
        best_id = ""
        normalized_target = self._normalize_name(series_name)
        for result in results[:20]:
            if not isinstance(result, dict):
                continue
            candidate_id = str(result.get("id") or "").strip()
            if not candidate_id:
                continue
            candidate_name = str(result.get("name") or result.get("original_name") or "").strip()
            if not candidate_name:
                continue
            score = 0
            normalized_name = self._normalize_name(candidate_name)
            if normalized_name == normalized_target:
                score += 80
            elif normalized_target and normalized_target in normalized_name:
                score += 45
            elif normalized_name and normalized_name in normalized_target:
                score += 35
            if year > 1900:
                first_air = str(result.get("first_air_date") or "")
                candidate_year = self._safe_int(first_air[:4])
                if candidate_year == year:
                    score += 20
                elif candidate_year and abs(candidate_year - year) <= 1:
                    score += 8
            popularity = float(result.get("popularity") or 0)
            score += min(10, int(popularity // 20))
            if score > best_score:
                best_score = score
                best_id = candidate_id
        return best_id

    def _fetch_tmdb_season_inventory(self, tmdb_id: str, *, seasons: set[int]) -> dict[int, dict[str, set[int]]]:
        safe_tmdb_id = str(tmdb_id or "").strip()
        requested_seasons = {int(season) for season in seasons if int(season) > 0}
        if not safe_tmdb_id or not requested_seasons:
            return {}
        if safe_tmdb_id in self._tmdb_detail_cache:
            detail = self._tmdb_detail_cache[safe_tmdb_id]
        else:
            params = {"language": self.tmdb_language}
            detail = self._tmdb_get_json(f"/tv/{urllib.parse.quote(safe_tmdb_id, safe='')}?{urllib.parse.urlencode(params)}")
            self._tmdb_detail_cache[safe_tmdb_id] = detail if isinstance(detail, dict) else {}

        seasons = detail.get("seasons") if isinstance(detail, dict) else []
        if not isinstance(seasons, list):
            return {}
        available_seasons: set[int] = set()
        for season in seasons:
            if not isinstance(season, dict):
                continue
            season_no = self._safe_int(season.get("season_number"))
            expected = self._safe_int(season.get("episode_count"))
            if season_no <= 0 or expected <= 0 or season_no not in requested_seasons:
                continue
            available_seasons.add(season_no)

        output: dict[int, dict[str, set[int]]] = {}
        today = date.today()
        for season_no in sorted(available_seasons):
            params = {"language": self.tmdb_language}
            payload = self._tmdb_get_json(
                f"/tv/{urllib.parse.quote(safe_tmdb_id, safe='')}/season/{season_no}?{urllib.parse.urlencode(params)}"
            )
            episodes = payload.get("episodes") if isinstance(payload, dict) else []
            if not isinstance(episodes, list):
                continue
            registered: set[int] = set()
            aired: set[int] = set()
            future: set[int] = set()
            unknown: set[int] = set()
            for episode in episodes:
                if not isinstance(episode, dict):
                    continue
                episode_no = self._safe_int(episode.get("episode_number"))
                if episode_no <= 0:
                    continue
                registered.add(episode_no)
                air_date_text = str(episode.get("air_date") or "").strip()[:10]
                if not air_date_text:
                    unknown.add(episode_no)
                    continue
                try:
                    air_date = date.fromisoformat(air_date_text)
                except ValueError:
                    unknown.add(episode_no)
                    continue
                (aired if air_date <= today else future).add(episode_no)
            if registered:
                output[season_no] = {
                    "registeredEpisodes": registered,
                    "airedEpisodes": aired,
                    "futureEpisodes": future,
                    "unknownAirDateEpisodes": unknown,
                }
        return output

    def _tmdb_get_json(self, path_with_query: str) -> dict[str, Any]:
        if not self.tmdb_token:
            raise ValueError("TMDB Token 未配置")
        target = f"https://api.themoviedb.org/3{path_with_query}"
        request = urllib.request.Request(
            target,
            method="GET",
            headers={
                "Authorization": f"Bearer {self.tmdb_token}",
                "Accept": "application/json",
            },
        )
        with urllib.request.urlopen(request, timeout=20) as response:
            raw = response.read()
        if not raw:
            return {}
        decoded = json.loads(raw.decode("utf-8", errors="replace"))
        return decoded if isinstance(decoded, dict) else {}

    @staticmethod
    def _safe_int(value: Any) -> int:
        try:
            parsed = int(value)
        except Exception:
            return 0
        return parsed

    @staticmethod
    def _normalize_name(value: str) -> str:
        text = str(value or "").strip().lower()
        if not text:
            return ""
        text = re.sub(r"[·•:：'\"“”‘’《》\[\]\(\)（）\-—_,.，。!?！？\s]+", "", text)
        return text
