from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
import json
import re
import urllib.parse
import urllib.request
from typing import Any, Callable, Union

from .ai_missing_episode_support import MissingEpisodeResult
from .media_identity_service import MediaIdentityService
from .missing_episode_inspector import MissingEpisodeInspector

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

    def scan(
        self,
        *,
        scan_limit: int = 1200,
        scan_workers: int = 6,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Scan with the same strict result model used by Telegram AI.

        The old dashboard scanner compared total counts independently and could
        turn TMDB future episodes into missing episodes.  This path deliberately
        delegates to the proven AI calculation, which only compares aired
        episodes after exact identity verification.
        """
        safe_scan_limit = max(20, min(5000, int(scan_limit or 1200)))
        rows: list[dict[str, Any]] = []
        warnings: list[str] = []
        upstream_error = ""

        scanned_series = 0
        matched_tmdb_series = 0
        missing_series_ids: set[str] = set()
        missing_episode_count = 0
        unknown_match_count = 0
        scanned_at = datetime.now().isoformat(timespec="seconds")

        def report_progress(**payload: Any) -> None:
            if progress_callback is None:
                return
            try:
                progress_callback(payload)
            except Exception:
                # Progress reporting must never interrupt the strict scan.
                pass

        try:
            report_progress(phase="loading_series", completed=0, total=0, currentTitle="")
            series_rows = self._fetch_all_series(limit=safe_scan_limit)
        except Exception as err:
            upstream_error = str(err)[:300]
            warnings.append(f"读取 Emby 剧集列表失败：{str(err)[:120]}")
            series_rows = []

        total_series = len(series_rows)
        report_progress(phase="comparing", completed=0, total=total_series, currentTitle="")
        # Each series uses a fresh identity service and only reads from Emby/TMDB.
        # A small worker pool keeps the dashboard responsive without changing the
        # strict identity -> aired episodes -> local mapped episodes calculation.
        worker_count = max(1, min(6, int(scan_workers or 6), total_series or 1))
        with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="missing-scan") as executor:
            futures = {
                executor.submit(self._scan_series_strict, series=series, scanned_at=scanned_at): series
                for series in series_rows
            }
            for future in as_completed(futures):
                series = futures[future]
                series_name = str(series.get("Name") or "").strip() or "未命名剧集"
                scanned_series += 1
                try:
                    item = future.result()
                except Exception as err:
                    # This is intentionally a final safety net. Per-series failures
                    # are represented as review rows instead of aborting the scan.
                    item = self._scan_failure_row(series=series, scanned_at=scanned_at, error=err)

                warning = str(item.get("warning") or "").strip()
                if warning:
                    warnings.append(warning)
                if item.get("matched"):
                    matched_tmdb_series += 1
                if item.get("unknown"):
                    unknown_match_count += 1
                if item.get("missing"):
                    series_id = str(series.get("Id") or "").strip()
                    if series_id:
                        missing_series_ids.add(series_id)
                    missing_episode_count += int(item.get("missingCount") or 0)
                row = item.get("row")
                if isinstance(row, dict):
                    rows.append(row)
                report_progress(
                    phase="comparing",
                    completed=scanned_series,
                    total=total_series,
                    currentTitle=series_name,
                )

        rows.sort(
            key=lambda row: (
                0 if str(row.get("status") or "") == "missing" else 1,
                str(row.get("seriesName") or ""),
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
            "scanWorkers": worker_count,
            "rowCount": len(rows),
            "calculation": "shared_strict_ai_result",
            "upstreamError": upstream_error,
        }
        report_progress(phase="completed", completed=scanned_series, total=total_series, currentTitle="")
        return {"summary": summary, "rows": rows, "warnings": warnings[:200], "debug": debug}

    def _scan_series_strict(self, *, series: dict[str, Any], scanned_at: str) -> dict[str, Any]:
        """Return one strict dashboard result without letting it stop other series."""
        series_id = str(series.get("Id") or "").strip()
        series_name = str(series.get("Name") or "").strip() or "未命名剧集"
        if not series_id:
            return {"row": None, "matched": False, "unknown": True, "missing": False, "missingCount": 0}

        try:
            # A full-library scan has no user confirmation step. Do not let a title
            # similarity guess turn an unrelated Emby series into a missing claim.
            tmdb_id = self._match_tmdb_series_id(series, allow_title_fallback=False)
        except Exception as err:
            return self._scan_failure_row(series=series, scanned_at=scanned_at, error=err, prefix="TMDB 匹配失败")

        if not tmdb_id:
            return {
                "row": self._unmatched_row(series=series, scanned_at=scanned_at),
                "matched": False,
                "unknown": True,
                "missing": False,
                "missingCount": 0,
            }

        try:
            result = self._build_shared_result(
                {
                    "tmdbId": tmdb_id,
                    "type": "series",
                    "title": series_name,
                    "year": self._series_year(series),
                    # The scan already has the exact Emby series. Avoid a second
                    # title/provider lookup for every item in the library.
                    "embyId": series_id,
                }
            )
        except Exception as err:
            return self._scan_failure_row(
                series=series,
                scanned_at=scanned_at,
                error=err,
                tmdb_id=tmdb_id,
                prefix="严格缺集计算失败",
            )

        matched = result.identity_status == "ok" and result.library_exists
        report = result.to_report_dict()
        official_missing = list(report.get("missingLabels") or [])
        status = "missing" if official_missing else "complete"
        if result.identity_status != "ok" or not result.library_exists:
            status = "match_failed"
        elif not result.reliable:
            status = "review"
        return {
            "row": None if status == "complete" else self._result_row(
                series=series,
                result=result,
                report=report,
                status=status,
                scanned_at=scanned_at,
            ),
            "matched": matched,
            "unknown": not matched,
            "missing": status == "missing",
            "missingCount": len(official_missing) if status == "missing" else 0,
        }

    def _unmatched_row(self, *, series: dict[str, Any], scanned_at: str) -> dict[str, Any]:
        series_id = str(series.get("Id") or "").strip()
        series_name = str(series.get("Name") or "").strip() or "未命名剧集"
        return {
            "seriesName": series_name,
            "title": series_name,
            "year": self._series_year(series),
            "missingEpisodes": [],
            "existingCount": 0,
            "expectedCount": 0,
            "completeness": "-",
            "tmdbId": "",
            "embySeriesId": series_id,
            "status": "match_failed",
            "reason": "未匹配到 TMDB 剧集",
            "identityStatus": "unmatched",
            "mappingConfidence": "low",
            "mappingWarning": "未匹配到 TMDB 剧集，已停止缺集计算",
            "seasonRows": [],
            **self._poster_fields(series),
            "scannedAt": scanned_at,
        }

    def _scan_failure_row(
        self,
        *,
        series: dict[str, Any],
        scanned_at: str,
        error: Exception,
        tmdb_id: str = "",
        prefix: str = "严格缺集计算失败",
    ) -> dict[str, Any]:
        series_id = str(series.get("Id") or "").strip()
        series_name = str(series.get("Name") or "").strip() or "未命名剧集"
        message = f"{prefix}（{str(error)[:120]}）"
        return {
            "row": {
                "seriesName": series_name,
                "title": series_name,
                "year": self._series_year(series),
                "missingEpisodes": [],
                "existingCount": 0,
                "expectedCount": 0,
                "completeness": "-",
                "tmdbId": tmdb_id,
                "embySeriesId": series_id,
                "status": "match_failed",
                "reason": message,
                "identityStatus": "unmatched",
                "mappingConfidence": "low",
                "mappingWarning": "数据读取失败，未输出缺失结论",
                "seasonRows": [],
                **self._poster_fields(series),
                "scannedAt": scanned_at,
            },
            "matched": False,
            "unknown": True,
            "missing": False,
            "missingCount": 0,
            "warning": f"{series_name}：{message}",
        }

    def _build_shared_result(self, identity: dict[str, Any]) -> MissingEpisodeResult:
        identity_service = MediaIdentityService(
            emby_fetcher=self.emby_fetcher,
            tmdb_fetcher=self._tmdb_get_json,
            language=self.tmdb_language,
            region=self.tmdb_region,
        )
        return MissingEpisodeInspector(identity_service=identity_service).inspect(identity=identity)

    @staticmethod
    def _series_year(series: dict[str, Any]) -> str:
        value = series.get("ProductionYear") or str(series.get("PremiereDate") or "")[:4]
        return str(value or "").strip()

    @staticmethod
    def _poster_fields(series: dict[str, Any]) -> dict[str, str]:
        image_tags = series.get("ImageTags") if isinstance(series.get("ImageTags"), dict) else {}
        return {
            "posterItemId": str(series.get("Id") or "").strip(),
            "posterImageTag": str(image_tags.get("Primary") or "").strip(),
            "posterSource": "series_primary",
        }

    def _result_row(
        self,
        *,
        series: dict[str, Any],
        result: MissingEpisodeResult,
        report: dict[str, Any],
        status: str,
        scanned_at: str,
    ) -> dict[str, Any]:
        season_rows: list[dict[str, Any]] = []
        for season_no in sorted(result.seasons):
            season = result.seasons[season_no]
            season_rows.append(
                {
                    "seasonNo": int(season_no),
                    "existingEpisodes": list(season.existing_episodes),
                    "airedEpisodes": list(season.aired_episodes),
                    "futureEpisodes": list(season.future_episodes),
                    "unknownEpisodes": list(season.unknown_episodes),
                    "missingEpisodes": list(season.missing_episodes) if result.reliable else [],
                    "referenceMissingEpisodes": list(season.missing_episodes) if not result.reliable else [],
                    "existingCount": len(season.existing_episodes),
                    "airedCount": len(season.aired_episodes),
                    "totalCount": int(season.total_episodes or 0),
                }
            )
        missing_labels = list(report.get("missingLabels") or [])
        missing_numbers = [
            int(value.rsplit("E", 1)[-1])
            for value in missing_labels
            if str(value).rsplit("E", 1)[-1].isdigit()
        ]
        return {
            "seriesName": result.title or str(series.get("Name") or "未命名剧集"),
            "title": result.title or str(series.get("Name") or "未命名剧集"),
            "year": result.year or self._series_year(series),
            "tmdbId": result.tmdb_id,
            "embySeriesId": str(result.emby_item_id or series.get("Id") or "").strip(),
            "status": status,
            "reason": result.mapping_warning or ("作品身份未确认，未输出缺失结论" if status == "match_failed" else ""),
            "identityStatus": result.identity_status,
            "mappingConfidence": result.mapping_confidence,
            "mappingWarning": result.mapping_warning,
            "existingCount": int(report.get("localCount") or 0),
            "mappedCount": int(report.get("mappedCount") or 0),
            "unmappedCount": int(report.get("unmappedCount") or 0),
            "expectedCount": int(report.get("airedCount") or 0),
            "registeredCount": int(report.get("registeredCount") or 0),
            "completeness": f"{int(report.get('mappedCount') or 0)}/{int(report.get('airedCount') or 0)}",
            "missingEpisodes": missing_numbers,
            "missingLabels": missing_labels,
            "referenceMissingLabels": list(report.get("referenceMissingLabels") or []),
            "futureLabels": list(report.get("futureLabels") or []),
            "unknownLabels": list(report.get("unknownLabels") or []),
            "lastAiredDate": str(report.get("lastAiredDate") or ""),
            "summaryText": str(report.get("summaryText") or ""),
            "detailLines": list(report.get("detailLines") or []),
            "seasonRows": season_rows,
            "libraryExists": bool(result.library_exists),
            **self._poster_fields(series),
            "scannedAt": scanned_at,
        }

    def _fetch_all_series(self, *, limit: int) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        start_index = 0
        page_size = 200
        while len(rows) < limit:
            current_limit = min(page_size, limit - len(rows))
            path = (
                "/Items?Recursive=true&IncludeItemTypes=Series"
                "&Fields=Name,ProductionYear,PremiereDate,ProviderIds,ImageTags,PrimaryImageItemId"
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
