from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
import logging
import os
import pathlib
import re
import secrets
import time
import urllib.parse
import urllib.request
from typing import TYPE_CHECKING, Any

from .ai_missing_episode_support import MissingEpisodeResult, MissingEpisodeSeason, compress_plain_episode_numbers

if TYPE_CHECKING:
    from .media_identity_service import MediaIdentityService
    from .telegram_commands import TelegramCommandService
    from .telegram_commands import CommandReply


LOGGER = logging.getLogger(__name__)


class AIMediaServiceAdapter:
    """Future business-execution adapter for Emby/TMDB/missing-episode/115 flows."""

    def __init__(self, service: "TelegramCommandService") -> None:
        self._service = service

    def emby_context(self) -> tuple[str, str]:
        return self._service._emby_context()

    def emby_get(self, path: str) -> dict[str, Any] | list[Any] | None:
        return self._service._emby_get(path)

    def format_emby_error(self, err: Exception) -> str:
        return self._format_emby_error(err)

    def fetch_latest_items_with_fallback(self, *, limit: int = 10) -> tuple[list[dict[str, Any]], list[dict[str, Any]], Any]:
        safe_limit = max(1, min(30, int(limit or 10)))
        legacy = getattr(self._service, "_fetch_latest_items_with_fallback", None)
        original = getattr(type(self._service), "_fetch_latest_items_with_fallback", None)
        legacy_func = getattr(legacy, "__func__", None)
        if callable(legacy) and legacy_func is not original:
            return legacy(limit=safe_limit)
        tried_paths: list[str] = []
        last_error: Exception | None = None

        direct_path = f"/Items/Latest?Limit={safe_limit}"
        rows, err = self._try_latest_path(direct_path)
        tried_paths.append(direct_path)
        if rows is not None:
            return rows, tried_paths, None
        last_error = err

        user_id = self._resolve_emby_user_id()
        if not user_id:
            return [], tried_paths, last_error

        user_paths = [
            f"/Users/{urllib.parse.quote(user_id, safe='')}/Items/Latest?Limit={safe_limit}",
            f"/Users/{urllib.parse.quote(user_id, safe='')}/Items/Latest?IncludeItemTypes=Movie,Series,Episode&Limit={safe_limit}",
        ]
        for path in user_paths:
            rows, err = self._try_latest_path(path)
            tried_paths.append(path)
            if rows is not None:
                return rows, tried_paths, None
            last_error = err
        return [], tried_paths, last_error

    def format_recent_library_row(self, row: dict[str, Any]) -> str:
        item_type = str(row.get("Type") or "").strip().lower()
        name = str(row.get("Name") or "").strip() or "未命名内容"

        if item_type == "movie":
            return f"• 🏷️《{name}》【电影完整版】✅已入库"

        if item_type == "episode":
            series_name = str(row.get("SeriesName") or "").strip() or "未知剧名"
            season = self._coerce_index_number(row.get("ParentIndexNumber"))
            episode = self._coerce_index_number(row.get("IndexNumber"))
            season_text = str(season) if season is not None else "X"
            episode_text = str(episode) if episode is not None else "X"
            return f"• 🏷️《{series_name}》第{season_text}季 第{episode_text}集「{name}」✅已入库"

        if item_type == "series":
            detail = self._resolve_latest_episode_for_series(row)
            if detail:
                season_text, episode_text, episode_title = detail
                return f"• 🏷️《{name}》第{season_text}季 第{episode_text}集「{episode_title}」✅已入库"
            return f"• 🏷️《{name}》第X季 第X集「最新更新」✅已入库"

        season = self._coerce_index_number(row.get("ParentIndexNumber"))
        episode = self._coerce_index_number(row.get("IndexNumber"))
        season_text = str(season) if season is not None else "X"
        episode_text = str(episode) if episode is not None else "X"
        return f"• 🏷️《{name}》第{season_text}季 第{episode_text}集「最新更新」✅已入库"

    def fetch_scheduled_tasks(self) -> list[dict[str, Any]]:
        payload = self.emby_get("/ScheduledTasks")
        rows = payload.get("Items") if isinstance(payload, dict) else payload
        return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []

    def match_scheduled_task_for_question(self, question: str) -> dict[str, Any] | None:
        try:
            tasks = self.fetch_scheduled_tasks()
        except Exception:
            return None
        if not tasks:
            return None
        text = str(question or "").lower()
        aliases = {
            "scan media library": ("媒体库", "扫描", "scan", "library"),
            "refresh metadata": ("元数据", "metadata"),
            "clean cache directory": ("缓存", "cache"),
            "clean transcode directory": ("转码", "transcode"),
        }
        scored: list[tuple[int, dict[str, Any]]] = []
        for task in tasks:
            name = str(task.get("Name") or task.get("Key") or "").strip()
            haystack = name.lower()
            score = 0
            if haystack and haystack in text:
                score += 20
            for alias, words in aliases.items():
                if alias in haystack and any(word in text for word in words):
                    score += 15
            for token in re.findall(r"[\w\u4e00-\u9fff]+", text):
                if len(token) >= 2 and token.lower() in haystack:
                    score += 1
            if score > 0:
                scored.append((score, task))
        if not scored and re.search(r"媒体库|扫描|scan", text):
            for task in tasks:
                name = str(task.get("Name") or "").lower()
                if "scan" in name and "library" in name:
                    return task
        scored.sort(key=lambda item: item[0], reverse=True)
        return scored[0][1] if scored else None

    def read_missing_scan_cache(self) -> dict[str, Any]:
        path = self.store_path.parent / "missing_scan.json"
        if not path.exists():
            return {}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        return payload if isinstance(payload, dict) else {}

    def media_identity_service(self) -> "MediaIdentityService":
        legacy = getattr(self._service, "_media_identity_service", None)
        original = getattr(type(self._service), "_media_identity_service", None)
        legacy_func = getattr(legacy, "__func__", None)
        if callable(legacy) and legacy_func is not original:
            return legacy()
        from .media_identity_service import MediaIdentityService

        token, language, region = self._tmdb_context()

        def tmdb_fetcher(path: str) -> dict[str, Any] | list[Any] | None:
            return self._tmdb_get_json(path, token=token)

        return MediaIdentityService(
            emby_fetcher=self.emby_get,
            tmdb_fetcher=tmdb_fetcher if token else None,
            cache_path=self.store_path.parent / "media_identity_cache.json",
            language=language,
            region=region,
        )

    def search_emby_media_candidates(self, keywords: list[str]) -> tuple[str, list[dict[str, Any]], str]:
        legacy = getattr(self._service, "_search_emby_media_candidates", None)
        original = getattr(type(self._service), "_search_emby_media_candidates", None)
        legacy_func = getattr(legacy, "__func__", None)
        if callable(legacy) and legacy_func is not original:
            return legacy(keywords)
        last_keyword = str(keywords[0] if keywords else "").strip()
        last_error = ""
        for candidate in keywords:
            keyword = str(candidate or "").strip()
            if not keyword:
                continue
            last_keyword = keyword
            try:
                items = self._search_emby_items(keyword=keyword, limit=12)
            except Exception as err:
                last_error = self._format_emby_error(err)
                continue
            if items:
                return keyword, items, ""
        return last_keyword, [], last_error

    def format_ai_matched_item_context(self, item: dict[str, Any], *, keyword: str) -> list[str]:
        legacy = getattr(self._service, "_format_ai_matched_item_context", None)
        original = getattr(type(self._service), "_format_ai_matched_item_context", None)
        legacy_func = getattr(legacy, "__func__", None)
        if callable(legacy) and legacy_func is not original:
            return legacy(item, keyword=keyword)
        item_id = str(item.get("Id") or "").strip()
        detail: dict[str, Any] = {}
        if item_id:
            try:
                payload = self.emby_get(
                    f"/Items/{urllib.parse.quote(item_id, safe='')}?Fields=Name,Type,ProductionYear,PremiereDate,ChildCount,RecursiveItemCount,ParentIndexNumber,IndexNumber,SeriesName,SeriesId,ProviderIds,Status,CommunityRating,CriticRating,Overview"
                )
                detail = payload if isinstance(payload, dict) else {}
            except Exception as err:
                LOGGER.warning("AI media item detail fallback: item_id=%s err=%s", item_id, err)
        joined = dict(item)
        joined.update(detail)
        item_type = str(joined.get("Type") or "").strip().lower()
        title = str(joined.get("Name") or keyword or "未知内容").strip()
        year = self._resolve_year(joined)
        type_label = {"series": "电视剧/剧集", "movie": "电影", "episode": "单集"}.get(item_type, item_type or "未知类型")
        lines = [
            "命中资源详情：",
            f"- 搜索关键词：{keyword}",
            f"- 最佳匹配：{title}（{year}，{type_label}）",
        ]

        if item_type == "series" and item_id:
            identity = self.resolve_ai_media_identity(keyword=keyword, detail=joined)
            season_count, episode_count, season_lines, latest_episode, source_note = self.resolve_ai_series_counts(
                item_id=item_id,
                detail=joined,
                keyword=keyword,
                title=title,
                identity=identity,
            )
            lines.extend(self._format_ai_identity_lines(identity))
            lines.append(f"- 剧集统计：共 {season_count} 季 / {episode_count} 集")
            if source_note:
                lines.append(f"- 统计来源：{source_note}")
            if latest_episode:
                lines.append(f"- 最新单集：{latest_episode}")
            if season_lines:
                lines.append(f"- 分季集数：{'；'.join(season_lines[:8])}")
            if episode_count <= 0:
                recent_hint = self._build_ai_recent_library_hint(title)
                if recent_hint:
                    lines.append(f"- 最近入库提示：{recent_hint}")
            status = str(joined.get("Status") or "").strip()
            if status:
                lines.append(f"- 剧集状态：{status}")
        elif item_type == "episode":
            season = joined.get("ParentIndexNumber")
            episode = joined.get("IndexNumber")
            series_name = str(joined.get("SeriesName") or "").strip()
            if series_name:
                lines.append(f"- 所属剧集：{series_name}")
            if isinstance(season, int) or isinstance(episode, int):
                lines.append(f"- 单集位置：第 {season if isinstance(season, int) else '?'} 季 第 {episode if isinstance(episode, int) else '?'} 集")
        elif item_type == "movie":
            lines.append("- 资源状态：电影已入库")

        rating = self._format_rating(joined)
        if rating != "N/A":
            lines.append(f"- 用户评分：{rating}")
        return lines

    def resolve_ai_series_search_item(
        self,
        item: dict[str, Any],
        *,
        items: list[dict[str, Any]] | None = None,
        keyword: str = "",
    ) -> dict[str, Any]:
        matched = item if isinstance(item, dict) else {}
        candidates = [row for row in (items or []) if isinstance(row, dict)]
        if str(matched.get("Type") or "").strip().lower() == "series":
            return matched
        if str(matched.get("Type") or "").strip().lower() == "episode":
            series_id = str(matched.get("SeriesId") or "").strip()
            if series_id:
                try:
                    payload = self.emby_get(
                        f"/Items/{urllib.parse.quote(series_id, safe='')}?Fields=Name,Type,ProductionYear,ProviderIds,Status"
                    )
                    if isinstance(payload, dict) and str(payload.get("Type") or "").strip().lower() == "series":
                        return payload
                except Exception as err:
                    LOGGER.warning("AI media episode parent series lookup failed: series_id=%s err=%s", series_id, err)
        normalized_keyword = self._normalize_ai_title(keyword)
        series_rows = [row for row in candidates if str(row.get("Type") or "").strip().lower() == "series"]
        exact = [row for row in series_rows if self._normalize_ai_title(str(row.get("Name") or "")) == normalized_keyword]
        return exact[0] if exact else (series_rows[0] if series_rows else {})

    def resolve_ai_media_identity(self, *, keyword: str, detail: dict[str, Any]) -> dict[str, Any]:
        try:
            identity = self.media_identity_service().identity_from_emby_item(detail)
        except Exception as err:
            LOGGER.warning("AI media identity fallback: title=%s err=%s", keyword, err)
            identity = {}
        if identity:
            return identity
        return {
            "title": str(detail.get("Name") or keyword or "").strip(),
            "year": self._resolve_year(detail),
            "type": str(detail.get("Type") or "series").strip().lower(),
            "tmdbId": "",
            "embyId": str(detail.get("Id") or "").strip(),
            "source": "emby_fallback",
            "confidence": "Emby 本地匹配",
        }

    def resolve_ai_series_counts(
        self,
        *,
        item_id: str,
        detail: dict[str, Any],
        keyword: str,
        title: str,
        identity: dict[str, Any] | None = None,
    ) -> tuple[int, int, list[str], str, str]:
        season_meta = self._fetch_ai_series_season_meta(item_id=item_id)
        sources: list[str] = []
        episode_count = 0
        season_count = 0
        season_lines: list[str] = []
        latest_text = ""
        try:
            episodes = self.fetch_ai_series_episodes(item_id=item_id)
        except Exception as err:
            LOGGER.warning("AI media series episodes failed: item_id=%s err=%s", item_id, err)
            status = getattr(err, "code", "")
            if str(status) in {"401", "403"}:
                sources.append("Episodes 单集列表读取失败：可能是 API Key 权限不足")
            else:
                sources.append(f"Episodes 单集列表读取失败：{self._format_emby_error(err)}")
            episodes = []

        if episodes:
            season_count, episode_count, season_lines, latest_text = self._summarize_ai_episode_rows(episodes, season_meta=season_meta)
            sources.append(f"Episodes 实际列表：{episode_count} 集{f'，最新 {latest_text}' if latest_text else ''}")

        direct_rows = self._search_ai_series_episode_rows(keyword=keyword or title, series_name=title or str(detail.get("Name") or ""))
        direct_summary = self._summarize_ai_episode_rows(direct_rows, season_meta=season_meta) if direct_rows else (0, 0, [], "")
        direct_season_count, direct_episode_count, direct_lines, direct_latest = direct_summary
        if direct_episode_count:
            sources.append(f"直接单集搜索：{direct_episode_count} 集{f'，最新 {direct_latest}' if direct_latest else ''}")

        recent = self._resolve_ai_recent_library_highest_episode(title or keyword)
        if recent.get("latestEpisodeNumber"):
            recent_latest = str(recent.get("latestText") or "")
            recent_suffix = f"，最新 {recent_latest}" if recent_latest else ""
            sources.append(f"最近入库：最高集号 {recent.get('latestEpisodeNumber')}{recent_suffix}（不作为实际单集总数）")

        tmdb_expected = self._fetch_ai_tmdb_expected_counts(identity or {}, local_title=title or keyword)
        if tmdb_expected.get("episodeCount"):
            tmdb_season_count = int(tmdb_expected.get("seasonCount") or 0)
            tmdb_suffix = f"，共 {tmdb_season_count} 季" if tmdb_season_count else ""
            sources.append(f"TMDB 公开总集数：{tmdb_expected.get('episodeCount')} 集{tmdb_suffix}")

        candidates = [
            ("Episodes", episode_count, season_count, season_lines, latest_text),
            ("直接单集搜索", direct_episode_count, direct_season_count, direct_lines, direct_latest),
        ]
        best_source, best_count, _best_seasons, best_lines, _best_latest = max(candidates, key=lambda row: row[1])
        if best_count > episode_count and best_lines:
            season_lines = best_lines
        elif not season_lines and best_lines:
            season_lines = best_lines
        final_episode_count = max(episode_count, direct_episode_count)
        final_season_count = max(season_count, direct_season_count, 1 if final_episode_count else 0)
        final_latest = self._pick_latest_episode_text(latest_text, direct_latest, str(recent.get("latestText") or ""))

        if final_episode_count <= 0:
            fallback_season_count, fallback_episode_count, fallback_lines = self._resolve_ai_series_counts_from_seasons(
                detail=detail,
                season_meta=season_meta,
            )
            final_episode_count = fallback_episode_count
            final_season_count = fallback_season_count
            season_lines = season_lines or fallback_lines
            sources.append("最终判断回退：使用季字段统计")

        missing_text = ""
        expected_count = int(tmdb_expected.get("episodeCount") or 0)
        if expected_count and final_episode_count:
            missing = max(0, expected_count - final_episode_count)
            missing_text = f"；TMDB 对照缺失 {missing} 集" if missing else "；TMDB 对照已齐"

        final_line = f"最终判断：实际可读取单集 {final_episode_count} 集"
        if final_latest:
            final_line = f"{final_line}，最新已到 {final_latest}"
        final_line = f"{final_line}{missing_text}"
        sources.append(final_line)
        if best_count > 0 and best_source != "Episodes":
            sources.append(f"冲突处理：{best_source} 高于 Episodes 聚合时，按更高的本地入库集数判断")

        try:
            self._service._log_project_event(
                level="info",
                module="webhook",
                action="ai_media_query_reconciled",
                message="AI 媒体库剧集查询已完成多来源合并。",
                detail={
                    "title": title or keyword,
                    "embySeriesId": item_id,
                    "tmdbId": (identity or {}).get("tmdbId"),
                    "episodesCount": episode_count,
                    "directEpisodeCount": direct_episode_count,
                    "recentLatestEpisodeNumber": recent.get("latestEpisodeNumber"),
                    "tmdbExpectedCount": tmdb_expected.get("episodeCount"),
                    "finalEpisodeCount": final_episode_count,
                },
            )
        except Exception:
            pass
        return max(0, final_season_count), max(0, final_episode_count), season_lines, final_latest, "；".join(sources)

    def fetch_ai_series_episodes(self, *, item_id: str, page_size: int = 1000) -> list[dict[str, Any]]:
        safe_id = urllib.parse.quote(str(item_id or "").strip(), safe="")
        if not safe_id:
            return []
        rows: list[dict[str, Any]] = []
        start = 0
        safe_page_size = max(50, min(2000, int(page_size or 1000)))
        while start < 10000:
            query = urllib.parse.urlencode(
                {
                    "Fields": "Name,SeasonId,ParentId,SeriesId,ParentIndexNumber,IndexNumber",
                    "StartIndex": str(start),
                    "Limit": str(safe_page_size),
                }
            )
            payload = self.emby_get(f"/Shows/{safe_id}/Episodes?{query}")
            items = payload.get("Items") if isinstance(payload, dict) else payload
            page = [row for row in items if isinstance(row, dict)] if isinstance(items, list) else []
            rows.extend(page)
            total = payload.get("TotalRecordCount") if isinstance(payload, dict) else None
            if isinstance(total, int) and len(rows) >= total:
                break
            if len(page) < safe_page_size:
                break
            start += safe_page_size
        return rows

    def build_missing_episode_result(self, *, identity: dict[str, Any], server: str = "emby") -> MissingEpisodeResult:
        service = self.media_identity_service()
        tmdb_id = str(identity.get("tmdbId") or "").strip()
        media_type = str(identity.get("type") or "series").strip().lower() or "series"
        library_lookup = getattr(service, "query_library_exists_by_tmdb", None)
        if not callable(library_lookup):
            library_lookup = getattr(service, "query_library_exists")
        with ThreadPoolExecutor(max_workers=2, thread_name_prefix="MissingInventory") as executor:
            detail_future = executor.submit(service.query_media_detail, tmdb_id, "tv")
            library_future = executor.submit(library_lookup, identity)
            expected = detail_future.result()
            existing = library_future.result()

        emby_item = existing.get("embyItem") if isinstance(existing.get("embyItem"), dict) else {}
        title = str(identity.get("title") or emby_item.get("Name") or "未知作品").strip()
        year = str(identity.get("year") or emby_item.get("ProductionYear") or "").strip()
        provider_ids = emby_item.get("ProviderIds") if isinstance(emby_item.get("ProviderIds"), dict) else {}
        provider_tmdb = str(provider_ids.get("Tmdb") or provider_ids.get("tmdb") or "").strip()
        target_title = str(identity.get("title") or title or "未知作品").strip()
        target_year = str(identity.get("year") or year or "").strip()
        emby_title = str(emby_item.get("Name") or target_title or "未知作品").strip()
        emby_year = str(emby_item.get("ProductionYear") or "").strip()
        emby_item_id = str(emby_item.get("Id") or identity.get("embyId") or "").strip()
        identity_status = "mismatch" if provider_tmdb and tmdb_id and provider_tmdb != tmdb_id else "ok"
        data_query_count = int(expected.get("tmdbQueryCount") or 0) + int(existing.get("embyQueryCount") or 0)
        explicit_missing_map = self._normalize_episode_map(existing.get("missingEpisodeMap"))
        explicit_missing_source = bool(existing.get("hasMissingEpisodeData")) and bool(explicit_missing_map)
        special_rows = self._build_special_season_rows(
            specials=existing.get("specials"),
            special_total=int(expected.get("specialEpisodeCount") or 0),
        )

        if not bool(existing.get("exists")):
            return MissingEpisodeResult(
                title=title,
                year=year,
                tmdb_id=tmdb_id,
                media_type=media_type,
                server=server,
                seasons={},
                existing_episodes=0,
                mapped_episodes=0,
                unmapped_episodes=0,
                total_episodes=int(expected.get("registeredEpisodes") or expected.get("totalEpisodes") or 0),
                missing_episodes=[],
                missing_seasons=[],
                mapping_confidence="low",
                mapping_warning="Emby 媒体库中未找到该剧集",
                emby_provider_tmdb_id=provider_tmdb,
                aired_episodes=int(expected.get("airedEpisodes") or 0),
                registered_episodes=int(expected.get("registeredEpisodes") or expected.get("totalEpisodes") or 0),
                last_aired_date=str(expected.get("lastAiredDate") or ""),
                library_exists=False,
                data_query_count=data_query_count,
                identity_status=identity_status if identity_status == "mismatch" else "unmatched",
                target_tmdb_id=tmdb_id,
                emby_tmdb_id=provider_tmdb,
                target_title=target_title,
                emby_title=emby_title,
                target_year=target_year,
                emby_year=emby_year,
                emby_item_id=emby_item_id,
                missing_source="",
                special_rows=special_rows,
            )

        raw_season_map = self._normalize_episode_map(existing.get("seasonMap"))
        episode_items = [
            row
            for row in (existing.get("episodeItems") if isinstance(existing.get("episodeItems"), list) else [])
            if isinstance(row, dict)
            and not bool(row.get("isMissing"))
            and str(row.get("locationType") or "").strip().lower() != "virtual"
        ]

        if not bool(expected.get("ok")):
            local_count = int(existing.get("episodeRows") or sum(len(values) for values in raw_season_map.values()))
            local_missing_map = explicit_missing_map if explicit_missing_source else self._analyze_local_season_gaps(raw_season_map)
            seasons = {
                season: MissingEpisodeSeason(
                    season_number=season,
                    existing_episodes=sorted(values),
                    total_episodes=max(
                        max(values) if values else 0,
                        max(local_missing_map.get(season, set()) or local_missing_map.get(season, []), default=0),
                    ),
                    missing_episodes=sorted(local_missing_map.get(season, [])),
                    aired_episodes=[],
                    future_episodes=[],
                    unknown_episodes=[],
                )
                for season, values in sorted(raw_season_map.items())
            }
            for season in sorted(local_missing_map):
                if season in seasons:
                    continue
                missing_values = sorted(local_missing_map.get(season, []))
                seasons[season] = MissingEpisodeSeason(
                    season_number=season,
                    existing_episodes=[],
                    total_episodes=max(missing_values) if missing_values else 0,
                    missing_episodes=missing_values,
                    aired_episodes=[],
                    future_episodes=[],
                    unknown_episodes=[],
                )
            return MissingEpisodeResult(
                title=title,
                year=year,
                tmdb_id=tmdb_id,
                media_type=media_type,
                server=server,
                seasons=seasons,
                existing_episodes=local_count,
                mapped_episodes=0,
                unmapped_episodes=local_count,
                total_episodes=0,
                missing_episodes=[],
                missing_seasons=[],
                mapping_confidence="low",
                mapping_warning="" if explicit_missing_source else "TMDB 详情不可用，无法可靠判断缺集",
                emby_provider_tmdb_id=provider_tmdb,
                reference_missing_episodes=[
                    f"S{season:02d}E{episode:02d}"
                    for season in sorted(local_missing_map)
                    for episode in sorted(local_missing_map[season])
                ],
                reference_missing_seasons=[season for season in sorted(local_missing_map) if local_missing_map[season]],
                data_query_count=data_query_count,
                identity_status=identity_status,
                target_tmdb_id=tmdb_id,
                emby_tmdb_id=provider_tmdb,
                target_title=target_title,
                emby_title=emby_title,
                target_year=target_year,
                emby_year=emby_year,
                emby_item_id=emby_item_id,
                missing_source="emby_missing" if explicit_missing_source else "",
                special_rows=special_rows,
            )

        registered_map = self._normalize_episode_map(expected.get("registeredSeasonMap"))
        aired_map = self._normalize_episode_map(expected.get("airedSeasonMap"))
        future_map = self._normalize_episode_map(expected.get("futureSeasonMap"))
        unknown_map = self._normalize_episode_map(expected.get("unknownAirDateMap"))
        special_total = int(expected.get("specialEpisodeCount") or 0)
        season_counts = (
            {
                int(key): int(value)
                for key, value in expected.get("seasonCounts", {}).items()
                if int(key) > 0 and int(value) > 0
            }
            if isinstance(expected.get("seasonCounts"), dict)
            else {}
        )
        if not registered_map and season_counts:
            registered_map = {season: set(range(1, count + 1)) for season, count in season_counts.items()}
        if not aired_map and not future_map and registered_map:
            aired_map = {season: set(values) for season, values in registered_map.items()}
        aired_total = int(expected.get("airedEpisodes") or sum(len(values) for values in aired_map.values()))
        registered_total = int(expected.get("registeredEpisodes") or expected.get("totalEpisodes") or sum(len(values) for values in registered_map.values()))

        if episode_items:
            mapped_payload = self._map_existing_episode_items(
                episode_items=episode_items,
                registered_map=registered_map,
                total_registered=max(0, registered_total),
            )
        else:
            mapped_payload = self._map_raw_season_map(raw_season_map=raw_season_map, registered_map=registered_map)

        mapped_map = mapped_payload["mappedMap"]
        mapped_count = sum(len(values) for values in mapped_map.values())
        duplicates = existing.get("duplicates") if isinstance(existing.get("duplicates"), list) else []
        duplicate_count = len(duplicates)
        raw_local_count = int(existing.get("episodeRows") or sum(len(values) for values in raw_season_map.values()))
        if raw_local_count <= 0:
            raw_local_count = sum(len(values) for values in raw_season_map.values())
        local_count = max(0, raw_local_count - duplicate_count)
        unmapped_count = max(0, local_count - mapped_count)
        comparison_reliable = unmapped_count == 0 and not mapped_payload["extraSamples"]
        mapping_warning = "" if comparison_reliable else "编号映射异常，无法可靠判断缺集"
        special_rows = self._build_special_season_rows(
            specials=existing.get("specials"),
            special_total=special_total,
        )

        seasons: dict[int, MissingEpisodeSeason] = {}
        missing_labels: list[str] = []
        missing_seasons: list[int] = []
        reference_missing_labels: list[str] = []
        reference_missing_seasons: list[int] = []
        season_keys = set(registered_map) | set(mapped_map) | set(explicit_missing_map)
        for season in sorted(season_keys):
            existing_values = sorted(mapped_map.get(season, set()))
            aired_values = sorted(aired_map.get(season, set()))
            future_values = sorted(future_map.get(season, set()))
            unknown_values = sorted(unknown_map.get(season, set()))
            missing_values = sorted(explicit_missing_map.get(season, set()) if explicit_missing_source else set(aired_values).difference(existing_values))
            season_total = max(len(registered_map.get(season, set())), int(season_counts.get(season) or 0))
            seasons[season] = MissingEpisodeSeason(
                season_number=season,
                existing_episodes=existing_values,
                total_episodes=season_total,
                missing_episodes=missing_values,
                aired_episodes=aired_values,
                future_episodes=future_values,
                unknown_episodes=unknown_values,
            )
            if missing_values:
                reference_missing_labels.extend(f"S{season:02d}E{episode:02d}" for episode in missing_values)
                if not existing_values:
                    reference_missing_seasons.append(season)
                if comparison_reliable:
                    missing_labels.extend(f"S{season:02d}E{episode:02d}" for episode in missing_values)
                    if not existing_values:
                        missing_seasons.append(season)

        return MissingEpisodeResult(
            title=title,
            year=year,
            tmdb_id=tmdb_id,
            media_type=media_type,
            server=server,
            seasons=seasons,
            existing_episodes=local_count,
            mapped_episodes=mapped_count,
            unmapped_episodes=unmapped_count,
            total_episodes=registered_total,
            missing_episodes=missing_labels,
            missing_seasons=missing_seasons,
            mapping_confidence="high" if comparison_reliable else "low",
            mapping_warning=mapping_warning,
            emby_provider_tmdb_id=provider_tmdb,
            aired_episodes=aired_total,
            registered_episodes=registered_total,
            last_aired_date=str(expected.get("lastAiredDate") or ""),
            reference_missing_episodes=reference_missing_labels,
            reference_missing_seasons=reference_missing_seasons,
            unmapped_samples=mapped_payload["unmappedSamples"],
            extra_samples=mapped_payload["extraSamples"],
            data_query_count=data_query_count,
            identity_status=identity_status,
            target_tmdb_id=tmdb_id,
            emby_tmdb_id=provider_tmdb,
            target_title=target_title,
            emby_title=emby_title,
            target_year=target_year,
            emby_year=emby_year,
            emby_item_id=emby_item_id,
            missing_source="emby_missing" if explicit_missing_source else "tmdb_aired_diff",
            special_rows=special_rows,
        )

    def missing_episode_report_reply(self, report: dict[str, Any], *, chat_id: str = "") -> "CommandReply":
        from .telegram_message_renderer import TelegramMessageRenderer

        return TelegramMessageRenderer(self._service, chat_id=chat_id).missing_episode_report_reply(report)

    def is_library_scan_request(self, text: str) -> bool:
        return self._service._is_library_scan_request(text)

    def extract_library_scan_keyword(self, text: str) -> str:
        return self._service._extract_library_scan_keyword(text)

    def cmd_scan_library(self, keyword: str) -> "CommandReply":
        legacy = getattr(self._service, "_cmd_scan_library", None)
        original = getattr(type(self._service), "_cmd_scan_library", None)
        legacy_func = getattr(legacy, "__func__", None)
        if callable(legacy) and legacy_func is not original:
            return legacy(keyword)

        from .telegram_message_renderer import TelegramMessageRenderer

        clean_keyword = str(keyword or "").strip()
        try:
            libraries = self._fetch_emby_libraries()
        except Exception as err:
            self._service._log_project_event(
                level="error",
                module="webhook",
                action="telegram_library_scan_list_failed",
                message="Telegram 扫描媒体库列表读取失败。",
                detail={"keyword": clean_keyword, "error": str(err)},
            )
            return TelegramMessageRenderer(self._service).ai_markdown_reply("🔄 扫描媒体库", f"读取媒体库失败：{err}")
        if not libraries:
            self._service._log_project_event(
                level="warning",
                module="webhook",
                action="telegram_library_scan_list_empty",
                message="Telegram 扫描媒体库未读取到可展示媒体库。",
                detail={"keyword": clean_keyword},
            )
            return TelegramMessageRenderer(self._service).ai_markdown_reply("🔄 扫描媒体库", "未读取到可扫描的 Emby 媒体库。")

        matched = self._filter_emby_libraries(libraries, clean_keyword) if clean_keyword else libraries
        display_rows = matched if matched else libraries
        visible_rows = display_rows[:20]
        intro_lines: list[str] = []
        if clean_keyword and matched:
            intro_lines.append(f"已按关键词“{clean_keyword}”匹配到 {len(matched)} 个媒体库。")
        elif clean_keyword:
            intro_lines.append(f"未匹配到“{clean_keyword}”，下面显示全部 {len(libraries)} 个媒体库。")
        else:
            intro_lines.append(f"当前可扫描媒体库：{len(libraries)} 个。")
        intro_lines.append("这里只展示扫描按钮，不会直接执行；点击按钮才会提交扫描。")
        intro_lines.append("")
        for idx, library in enumerate(visible_rows, start=1):
            name = str(library.get("name") or "未知媒体库").strip()
            lib_type = str(library.get("type") or "未知类型").strip()
            intro_lines.append(f"{idx}. {name}｜{lib_type}")
        if len(display_rows) > len(visible_rows):
            intro_lines.append(f"... 还有 {len(display_rows) - len(visible_rows)} 个媒体库未显示，可用 /saomiao 关键词 缩小范围。")
        self._service._log_project_event(
            level="info",
            module="webhook",
            action="telegram_library_scan_list_ready",
            message="Telegram 扫描媒体库列表已生成。",
            detail={
                "keyword": clean_keyword,
                "total": len(libraries),
                "matched": len(matched),
                "displayed": len(visible_rows),
                "fallbackToAll": bool(clean_keyword and not matched),
            },
        )
        return TelegramMessageRenderer(self._service).ai_markdown_reply(
            "🔄 扫描媒体库",
            "\n".join(intro_lines),
            reply_markup=self._build_scan_library_keyboard(visible_rows),
        )

    def cmd_hdhive_search(self, keyword: str) -> "CommandReply":
        legacy = getattr(self._service, "_cmd_hdhive_search", None)
        original = getattr(type(self._service), "_cmd_hdhive_search", None)
        legacy_func = getattr(legacy, "__func__", None)
        if callable(legacy) and legacy_func is not original:
            return legacy(keyword)

        from .telegram_message_renderer import TelegramMessageRenderer
        from .hdhive_service import HDHiveError

        safe_keyword = str(keyword or "").strip()
        renderer = TelegramMessageRenderer(self._service)
        if not safe_keyword:
            return renderer.ai_markdown_reply("🪺 影巢资源搜索", "用法：/hdhive 片名\n例如：/hdhive 遮天")

        service = self._hdhive_service()
        if not service.config.get("enabled"):
            return renderer.ai_markdown_reply("🪺 影巢未启用", "请先在后台“影巢搜索”页面保存并启用 OpenAPI 配置。")
        authorized = bool(service.config.get("user")) if service.is_broker else bool(service.config.get("accessToken") or service.config.get("refreshToken"))
        if not authorized:
            return renderer.ai_markdown_reply("🪺 影巢未授权", "请先在后台完成影巢 OAuth 授权。")
        try:
            resolution = self.media_identity_service().resolve(safe_keyword)
            if resolution.get("ambiguous"):
                candidates = resolution.get("candidates") if isinstance(resolution.get("candidates"), list) else []
                body = "找到多个同名作品，请带年份重新搜索：\n" + "\n".join(
                    f"- {row.get('title')}（{row.get('year') or '年份未知'}）" for row in candidates[:5]
                )
                return renderer.ai_markdown_reply("🪺 影巢资源搜索", body)
            identity = resolution.get("identity") if isinstance(resolution.get("identity"), dict) else {}
            tmdb_id = str(identity.get("tmdbId") or "").strip()
            if not tmdb_id:
                return renderer.ai_markdown_reply("🪺 影巢资源搜索", f"无法确认《{safe_keyword}》的 TMDB 身份，请检查 TMDB 配置或补充年份。")
            result = service.search_resources(media_type=str(identity.get("type") or ""), tmdb_id=tmdb_id)
            resources = [self._normalize_hdhive_resource(row) for row in result.get("items") or []]
            resources = [row for row in resources if row.get("slug")][:8]
            if not resources:
                return renderer.ai_markdown_reply("🪺 影巢资源搜索", f"《{identity.get('title') or safe_keyword}》暂未找到可用影巢资源。")
            store = self._read_store()
            drive_config = self._apply_drive115_env_overrides(store.get("drive115Config"))
            target_cid = str(drive_config.get("defaultCid") or "0")
            now = time.time()
            pending = getattr(self._service, "_pending_hdhive_actions", {})
            if not isinstance(pending, dict):
                pending = {}
            pending = {key: value for key, value in pending.items() if now - float(value.get("createdAt") or 0) <= 900}
            lines = [f"《{identity.get('title') or safe_keyword}》找到 {len(resources)} 条资源：", ""]
            buttons: list[list[dict[str, str]]] = []
            for index, resource in enumerate(resources, start=1):
                cost = "已解锁" if resource.get("isUnlocked") else f"{resource.get('unlockPoints') or 0} 积分"
                specs = " / ".join(str(item) for item in (resource.get("resolution") or []))
                lines.append(
                    f"{index}. {resource.get('title')}\n   {resource.get('panType') or '未知网盘'} · {resource.get('shareSize') or '大小未知'} · {specs or '规格未知'} · {cost}"
                )
                if resource.get("is115"):
                    action_id = secrets.token_urlsafe(7).replace("-", "").replace("_", "")[:10]
                    pending[action_id] = {"createdAt": now, "resource": resource, "targetCid": target_cid}
                    buttons.append([{"text": f"转存 #{index} · {cost}", "callback_data": f"hdhive:pick:{action_id}"}])
            setattr(self._service, "_pending_hdhive_actions", pending)
            if not buttons:
                lines.append("\n当前结果没有可转存的 115 资源。")
            self._service._log_project_event(
                level="info",
                module="hdhive",
                action="telegram_hdhive_search_success",
                message="Telegram 影巢资源搜索完成。",
                detail={"tmdbId": tmdb_id, "mediaType": identity.get("type"), "resultCount": len(resources)},
            )
            return renderer.ai_markdown_reply(
                "🪺 影巢资源搜索",
                "\n".join(lines),
                reply_markup={"inline_keyboard": buttons} if buttons else None,
            )
        except HDHiveError as err:
            self._service._log_project_event(
                level="warning",
                module="hdhive",
                action="telegram_hdhive_search_failed",
                message="Telegram 影巢资源搜索失败。",
                detail={"code": err.code, "error": str(err)},
            )
            suffix = f"\n请等待 {err.retry_after} 秒后重试。" if err.retry_after else ""
            return renderer.ai_markdown_reply("🪺 影巢搜索失败", f"{err}{suffix}")
        except Exception as err:
            return renderer.ai_markdown_reply("🪺 影巢搜索失败", str(err))

    def cmd_drive115_transfer(self, question: str) -> "CommandReply":
        legacy = getattr(self._service, "_cmd_drive115_transfer", None)
        original = getattr(type(self._service), "_cmd_drive115_transfer", None)
        legacy_func = getattr(legacy, "__func__", None)
        if callable(legacy) and legacy_func is not original:
            return legacy(question)

        from .drive115_service import Drive115Service, extract_115_share

        text = str(question or "").strip()
        if not text:
            from .telegram_message_renderer import TelegramMessageRenderer

            return TelegramMessageRenderer(self._service).ai_markdown_reply(
                "📦 115 链接转存",
                "用法：/zhuancun115 115分享链接\n\n私聊也可以直接发送包含 115 链接的资源消息，机器人识别后会立即转存。",
            )

        store = self._read_store()
        config = self._apply_drive115_env_overrides(store.get("drive115Config"))
        if not bool(config.get("enabled")):
            return self._drive115_transfer_result(
                success=False,
                reason="请先在后台“115网盘”页面启用并保存配置",
            )
        if not str(config.get("cookie") or "").strip():
            return self._drive115_transfer_result(
                success=False,
                reason="请先在后台“115网盘”页面填写并保存 115 Cookie",
            )
        share = extract_115_share(text)
        if not share.get("shareCode"):
            return self._drive115_transfer_result(
                success=False,
                reason="未在当前消息或回复消息中识别到 115 分享链接",
            )
        masked_share_code = self._mask_share_code(share.get("shareCode"))
        service = Drive115Service(config)
        try:
            parsed = service.parse_share(
                share_url=text,
                receive_code=str(share.get("receiveCode") or ""),
            )
        except Exception as err:
            self._service._log_project_event(
                level="error",
                module="drive115",
                action="telegram_drive115_parse_failed",
                message="Telegram 115 分享解析失败。",
                detail={
                    "shareCode": masked_share_code,
                    "error": str(err),
                    "successCount": 0,
                    "failureCount": 1,
                    "replyToMessage": False,
                },
            )
            return self._drive115_transfer_result(
                success=False,
                reason=str(err),
            )

        file_ids = [str(row.get("id") or "").strip() for row in parsed.get("files", []) if str(row.get("id") or "").strip()]
        target_cid = str(config.get("defaultCid") or "0").strip() or "0"
        detail = {
            "shareCode": self._mask_share_code(parsed.get("shareCode") or share.get("shareCode")),
            "targetCid": target_cid,
            "title": str(parsed.get("title") or "").strip(),
            "fileCount": int(parsed.get("fileCount") or len(file_ids) or 0),
            "replyToMessage": False,
        }
        try:
            result = service.transfer_share(
                share_code=str(parsed.get("shareCode") or share.get("shareCode") or ""),
                receive_code=str(parsed.get("receiveCode") or share.get("receiveCode") or ""),
                target_cid=target_cid,
                file_ids=file_ids,
                source_files=parsed.get("files") if isinstance(parsed.get("files"), list) else [],
            )
            if not bool(result.get("ok", True)):
                raise RuntimeError(str(result.get("message") or "115 未接受转存请求"))
            status = str(result.get("status") or "submitted")
            exists = status == "exists"
            detail.update({"successCount": 0 if exists else 1, "existsCount": 1 if exists else 0, "failureCount": 0, "status": status})
            self._service._log_project_event(
                level="info",
                module="drive115",
                action="telegram_drive115_transfer_submitted",
                message="Telegram 115 转存已提交。",
                detail=detail,
            )
            return self._drive115_transfer_result(status=status)
        except Exception as err:
            LOGGER.warning("AI media 115 direct transfer failed: %s", err)
            detail.update({"successCount": 0, "failureCount": 1, "error": str(err)})
            self._service._log_project_event(
                level="error",
                module="drive115",
                action="telegram_drive115_transfer_failed",
                message="Telegram 115 转存失败。",
                detail=detail,
            )
            return self._drive115_transfer_result(
                status="failed",
                reason=str(err),
            )

    @property
    def store_path(self) -> pathlib.Path:
        return pathlib.Path(self._service.store_path)

    def _try_latest_path(self, path: str) -> tuple[list[dict[str, Any]] | None, Exception | None]:
        try:
            payload = self.emby_get(path)
        except Exception as err:
            self._log_latest_attempt(path=path, err=err)
            return None, err
        rows = payload if isinstance(payload, list) else []
        self._log_latest_attempt(path=path, err=None, rows=len(rows))
        return rows, None

    def _resolve_latest_episode_for_series(self, row: dict[str, Any]) -> tuple[int, int, str] | None:
        series_id = str(row.get("Id") or "").strip()
        if not series_id:
            return None
        path = (
            "/Items?"
            f"ParentId={urllib.parse.quote(series_id, safe='')}"
            "&Recursive=true"
            "&IncludeItemTypes=Episode"
            "&SortBy=DateCreated"
            "&SortOrder=Descending"
            "&Limit=1"
        )
        try:
            payload = self.emby_get(path)
        except Exception as err:
            LOGGER.warning("AI recent-library latest episode lookup failed: series_id=%s err=%s", series_id, err)
            return None
        items = payload.get("Items") if isinstance(payload, dict) else payload
        if not isinstance(items, list) or not items:
            return None
        latest = items[0] if isinstance(items[0], dict) else {}
        season = self._coerce_index_number(latest.get("ParentIndexNumber"))
        episode = self._coerce_index_number(latest.get("IndexNumber"))
        if season is None or episode is None:
            return None
        title = str(latest.get("Name") or "").strip() or "最新更新"
        return season, episode, title

    def _resolve_emby_user_id(self) -> str:
        try:
            sessions = self.emby_get("/Sessions")
        except Exception as err:
            sessions = []
            LOGGER.warning("Resolve Emby user id from /Sessions failed: %s", err)
        if isinstance(sessions, list):
            for row in sessions:
                if not isinstance(row, dict):
                    continue
                user_id = str(row.get("UserId") or "").strip()
                if user_id:
                    return user_id
                user = row.get("User") if isinstance(row.get("User"), dict) else {}
                user_id = str(user.get("Id") or "").strip()
                if user_id:
                    return user_id

        for path in ("/Users", "/Users/Query"):
            try:
                payload = self.emby_get(path)
            except Exception as err:
                LOGGER.warning("Resolve Emby user id from %s failed: %s", path, err)
                continue
            rows = payload.get("Items") if isinstance(payload, dict) else payload
            if not isinstance(rows, list):
                continue
            for row in rows:
                if not isinstance(row, dict):
                    continue
                user_id = str(row.get("Id") or "").strip()
                if user_id:
                    return user_id
        return ""

    def _fetch_emby_libraries(self) -> list[dict[str, str]]:
        candidates: list[dict[str, str]] = []
        tried_errors: list[str] = []
        for path in ("/Library/VirtualFolders", "/Library/MediaFolders", "/UserViews"):
            try:
                payload = self.emby_get(path)
            except Exception as err:
                tried_errors.append(f"{path}: {self._format_emby_error(err)}")
                continue
            rows = payload.get("Items") if isinstance(payload, dict) else payload
            if not isinstance(rows, list):
                continue
            for row in rows:
                library = self._normalize_emby_library(row)
                if library and library["id"] not in {item["id"] for item in candidates}:
                    candidates.append(library)
            if candidates:
                return candidates
        if tried_errors:
            raise RuntimeError("；".join(tried_errors[:3]))
        return candidates

    @staticmethod
    def _normalize_emby_library(row: Any) -> dict[str, str]:
        if not isinstance(row, dict):
            return {}
        library_id = str(row.get("ItemId") or row.get("Id") or row.get("CollectionId") or "").strip()
        name = str(row.get("Name") or row.get("ItemName") or row.get("CollectionType") or "").strip()
        options = row.get("LibraryOptions") if isinstance(row.get("LibraryOptions"), dict) else {}
        lib_type = str(row.get("CollectionType") or row.get("Type") or options.get("ContentType") or "").strip()
        if not library_id or not name:
            return {}
        return {"id": library_id, "name": name, "type": lib_type or "媒体库"}

    @staticmethod
    def _filter_emby_libraries(libraries: list[dict[str, str]], keyword: str) -> list[dict[str, str]]:
        clean = str(keyword or "").strip().lower()
        if not clean:
            return libraries
        tokens = [token.lower() for token in re.findall(r"[\w\u4e00-\u9fff]+", clean) if token.strip()]
        if not tokens:
            return libraries
        matched: list[dict[str, str]] = []
        for library in libraries:
            haystack = f"{library.get('name', '')} {library.get('type', '')}".lower()
            if any(token in haystack for token in tokens) or clean in haystack:
                matched.append(library)
        return matched

    def _build_scan_library_keyboard(self, libraries: list[dict[str, str]]) -> dict[str, Any]:
        rows: list[list[dict[str, str]]] = []
        for library in libraries[:20]:
            library_id = str(library.get("id") or "").strip()
            name = str(library.get("name") or "未知媒体库").strip()
            if not library_id:
                continue
            encoded_id = urllib.parse.quote(library_id, safe="")
            rows.append([{"text": f"扫描：{name[:26]}", "callback_data": f"scan_library:one:{encoded_id}"}])
        rows.append([{"text": "扫描全库", "callback_data": "scan_library:all"}])
        return {"inline_keyboard": rows}

    @staticmethod
    def _mask_share_code(value: Any) -> str:
        text = str(value or "").strip()
        if len(text) <= 4:
            return text
        return f"{text[:3]}***{text[-2:]}"

    @staticmethod
    def _drive115_transfer_result(*, status: str = "", success: bool | None = None, reason: str = "", reply_to_message_id: int = 0) -> dict[str, Any]:
        normalized = str(status or ("submitted" if success else "failed")).strip().lower()
        if normalized == "exists":
            text = "转存完成：成功 0 个，已存在 1 个，失败 0 个"
        elif normalized == "submitted":
            text = "转存完成：成功 1 个，已存在 0 个，失败 0 个"
        else:
            safe_reason = str(reason or "115 未接受转存请求").strip()
            text = f"转存完成：成功 0 个，已存在 0 个，失败 1 个\n原因：{safe_reason}"
        return {
            "text": text,
            "fallback_text": text,
            "reply_to_message_id": int(reply_to_message_id or 0),
        }

    def _hdhive_service(self):
        from .hdhive_service import HDHiveService, apply_hdhive_env_overrides

        store = self._read_store()
        config = apply_hdhive_env_overrides(store.get("hdhiveConfig"))
        return HDHiveService(config, save_config=self._save_hdhive_runtime_config)

    def _save_hdhive_runtime_config(self, config: dict[str, Any]) -> None:
        from .hdhive_service import normalize_hdhive_config

        store_path = self.store_path
        try:
            raw = json.loads(store_path.read_text(encoding="utf-8")) if store_path.exists() else {}
        except Exception:
            raw = {}
        store = raw if isinstance(raw, dict) else {}
        current = normalize_hdhive_config(store.get("hdhiveConfig"))
        incoming = normalize_hdhive_config(config)
        for field in (
            "installationId",
            "installationSecret",
            "oauthSessionId",
            "oauthSessionExpiresAt",
            "accessToken",
            "refreshToken",
            "accessExpiresAt",
            "refreshExpiresAt",
            "scopes",
            "user",
            "lastCheckin",
            "lastCheckinDate",
            "updatedAt",
        ):
            current[field] = incoming.get(field)
        store["hdhiveConfig"] = normalize_hdhive_config(current)
        store_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = store_path.with_suffix(".hdhive.tmp")
        temp_path.write_text(json.dumps(store, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(temp_path, store_path)

    @staticmethod
    def _normalize_hdhive_resource(raw: Any) -> dict[str, Any]:
        source = raw if isinstance(raw, dict) else {}
        pan_type = str(source.get("pan_type") or source.get("website") or "").strip()
        return {
            "slug": str(source.get("slug") or "").strip(),
            "title": str(source.get("title") or "影巢资源").strip(),
            "panType": pan_type,
            "shareSize": str(source.get("share_size") or "").strip(),
            "resolution": source.get("video_resolution") or [],
            "source": source.get("source") or [],
            "unlockPoints": int(source.get("unlock_points") or 0),
            "isUnlocked": bool(source.get("is_unlocked")),
            "is115": "115" in pan_type.lower(),
        }

    def _log_latest_attempt(self, *, path: str, err: Exception | None, rows: int | None = None) -> None:
        if err is None:
            LOGGER.info("AI media latest fetch success path=%s rows=%s", path, rows if rows is not None else "-")
            return
        LOGGER.warning("AI media latest fetch failed path=%s error=%s", path, err)

    def _search_emby_items(self, *, keyword: str, limit: int = 8) -> list[dict[str, Any]]:
        query = urllib.parse.urlencode(
            {
                "Recursive": "true",
                "SearchTerm": keyword,
                "IncludeItemTypes": "Series,Movie,Episode",
                "Fields": "Name,Type,ProductionYear,Overview,Genres,PremiereDate,ChildCount,RecursiveItemCount,ParentIndexNumber,IndexNumber,SeriesName,SeriesId,ProviderIds,Status,CommunityRating,CriticRating,RunTimeTicks",
                "Limit": str(max(1, min(50, int(limit or 8)))),
            }
        )
        result = self.emby_get(f"/Items?{query}")
        items = result.get("Items") if isinstance(result, dict) else []
        return [row for row in items if isinstance(row, dict)] if isinstance(items, list) else []

    def _build_ai_recent_library_hint(self, title: str) -> str:
        safe_title = str(title or "").strip()
        if not safe_title:
            return ""
        try:
            latest, _tried, _err = self.fetch_latest_items_with_fallback(limit=20)
        except Exception:
            return ""
        rows = latest if isinstance(latest, list) else []
        for row in rows:
            if not isinstance(row, dict):
                continue
            item_type = str(row.get("Type") or "").strip().lower()
            row_title = str(row.get("SeriesName") if item_type == "episode" else row.get("Name") or "").strip()
            if not row_title or (safe_title not in row_title and row_title not in safe_title):
                continue
            return self.format_recent_library_row(row)
        return ""

    def _search_ai_series_episode_rows(self, *, keyword: str, series_name: str) -> list[dict[str, Any]]:
        keyword = str(keyword or series_name or "").strip()
        if not keyword:
            return []
        query = urllib.parse.urlencode(
            {
                "Recursive": "true",
                "SearchTerm": keyword,
                "IncludeItemTypes": "Episode",
                "Fields": "Name,Type,SeriesName,ParentIndexNumber,IndexNumber,SeriesId",
                "Limit": "1000",
            }
        )
        try:
            payload = self.emby_get(f"/Items?{query}")
        except Exception as err:
            LOGGER.warning("AI media direct episode search failed: keyword=%s err=%s", keyword, err)
            return []
        items = payload.get("Items") if isinstance(payload, dict) else payload
        rows = [row for row in items if isinstance(row, dict)] if isinstance(items, list) else []
        target = self._normalize_ai_title(series_name or keyword)
        filtered: list[dict[str, Any]] = []
        for row in rows:
            row_series = str(row.get("SeriesName") or "").strip()
            row_title = self._normalize_ai_title(row_series)
            if not target or row_title == target or target in row_title or row_title in target:
                filtered.append(row)
        return filtered

    def _resolve_ai_recent_library_highest_episode(self, title: str) -> dict[str, Any]:
        safe_title = str(title or "").strip()
        if not safe_title:
            return {}
        try:
            latest, _tried, _err = self.fetch_latest_items_with_fallback(limit=40)
        except Exception:
            return {}
        rows = latest if isinstance(latest, list) else []
        target = self._normalize_ai_title(safe_title)
        best: tuple[int, int, str] | None = None
        for row in rows:
            if not isinstance(row, dict):
                continue
            item_type = str(row.get("Type") or "").strip().lower()
            if item_type != "episode":
                continue
            series_name = str(row.get("SeriesName") or "").strip()
            if not series_name:
                continue
            row_target = self._normalize_ai_title(series_name)
            if target and target != row_target:
                continue
            season = self._coerce_index_number(row.get("ParentIndexNumber")) or 1
            episode = self._coerce_index_number(row.get("IndexNumber")) or 0
            if episode <= 0:
                continue
            name = str(row.get("Name") or "").strip()
            current = (season, episode, name)
            if best is None or (current[0], current[1]) > (best[0], best[1]):
                best = current
        if not best:
            return {}
        latest_text = f"S{best[0]:02d}E{best[1]:02d}"
        if best[2]:
            latest_text = f"{latest_text}「{best[2]}」"
        return {"seasonCount": best[0], "latestEpisodeNumber": best[1], "latestText": latest_text}

    @staticmethod
    def _format_ai_identity_lines(identity: dict[str, Any]) -> list[str]:
        if not identity:
            return []
        tmdb_id = str(identity.get("tmdbId") or "").strip() or "未识别"
        emby_id = str(identity.get("embyId") or "").strip() or "未识别"
        title = str(identity.get("title") or "未知作品").strip()
        year = str(identity.get("year") or "未知").strip()
        confidence = str(identity.get("confidence") or "本地匹配").strip()
        return [
            f"- 作品身份：{title}（{year}）/ TMDB {tmdb_id} / Emby {emby_id}",
            f"- 身份来源：{confidence}",
        ]

    def _fetch_ai_tmdb_expected_counts(self, identity: dict[str, Any], *, local_title: str = "") -> dict[str, Any]:
        tmdb_id = str(identity.get("tmdbId") or "").strip()
        if not tmdb_id:
            return {}
        token, language, _region = self._tmdb_context()
        if not token:
            return {}
        try:
            detail = self._tmdb_get_json(f"/tv/{urllib.parse.quote(tmdb_id, safe='')}?{urllib.parse.urlencode({'language': language})}", token=token)
        except Exception as err:
            LOGGER.warning("AI media TMDB expected count failed: title=%s tmdb=%s err=%s", local_title, tmdb_id, err)
            return {"error": self._format_emby_error(err)}
        seasons = detail.get("seasons") if isinstance(detail.get("seasons"), list) else []
        season_count = 0
        episode_count = 0
        season_lines: list[str] = []
        for season in seasons:
            if not isinstance(season, dict):
                continue
            season_no = self._coerce_index_number(season.get("season_number"))
            if season_no is None or season_no <= 0:
                continue
            count = self._coerce_index_number(season.get("episode_count")) or 0
            season_count += 1
            episode_count += max(0, count)
            season_lines.append(f"S{season_no} {count}集")
        return {
            "tmdbId": tmdb_id,
            "title": str(detail.get("name") or local_title or "").strip(),
            "seasonCount": season_count,
            "episodeCount": episode_count,
            "seasonLines": season_lines,
        }

    def _pick_latest_episode_text(self, *values: str) -> str:
        best: tuple[int, int, str] | None = None
        for value in values:
            text = str(value or "").strip()
            if not text:
                continue
            match = re.search(r"S(?P<season>\d{1,2})E(?P<episode>\d{1,4})", text, flags=re.IGNORECASE)
            if not match:
                continue
            season = int(match.group("season") or 0)
            episode = int(match.group("episode") or 0)
            current = (season, episode, text)
            if best is None or (season, episode) > (best[0], best[1]):
                best = current
        return best[2] if best else ""

    def _fetch_ai_series_season_meta(self, *, item_id: str) -> dict[int, str]:
        meta: dict[int, str] = {}
        try:
            seasons_rows = self.emby_get(
                f"/Shows/{urllib.parse.quote(item_id, safe='')}/Seasons?Fields=Name,ChildCount,IndexNumber,RecursiveItemCount"
            )
        except Exception as err:
            LOGGER.warning("AI media series seasons fallback: item_id=%s err=%s", item_id, err)
            return meta
        seasons = seasons_rows.get("Items") if isinstance(seasons_rows, dict) else seasons_rows
        if not isinstance(seasons, list):
            return meta
        for idx, season in enumerate(seasons):
            if not isinstance(season, dict):
                continue
            season_index = self._coerce_index_number(season.get("IndexNumber")) or 0
            season_name = str(season.get("Name") or "").strip()
            season_no = season_index if season_index > 0 else int(self._extract_season_number(season_name) or idx + 1)
            meta[season_no] = f"S{season_no}"
        return meta

    def _resolve_ai_series_counts_from_seasons(self, *, detail: dict[str, Any], season_meta: dict[int, str]) -> tuple[int, int, list[str]]:
        season_count = 0
        episode_count = 0
        lines: list[str] = []
        if episode_count <= 0:
            fallback_count = detail.get("RecursiveItemCount")
            if isinstance(fallback_count, int) and fallback_count > 0:
                episode_count = int(fallback_count)
        if season_count <= 0:
            child_count = detail.get("ChildCount")
            if isinstance(child_count, int) and child_count > 0:
                season_count = int(child_count)
        if season_count <= 0 and season_meta:
            season_count = len(season_meta)
        if season_count <= 0 and episode_count > 0:
            season_count = 1
        if season_meta and episode_count > 0:
            per_season = max(0, episode_count // max(1, len(season_meta)))
            for season_no in sorted(season_meta):
                lines.append(f"{season_meta[season_no]} {per_season}集")
        return max(0, season_count), max(0, episode_count), lines

    def _summarize_ai_episode_rows(self, rows: list[dict[str, Any]], *, season_meta: dict[int, str]) -> tuple[int, int, list[str], str]:
        season_counts: dict[int, int] = {}
        latest: tuple[int, int, str] | None = None
        unique_rows: list[dict[str, Any]] = []
        seen: set[str] = set()
        for row in rows:
            if not isinstance(row, dict):
                continue
            season_no = self._coerce_index_number(row.get("ParentIndexNumber")) or 0
            episode_no = self._coerce_index_number(row.get("IndexNumber")) or 0
            item_id = str(row.get("Id") or "").strip()
            series_id = str(row.get("SeriesId") or "").strip()
            if episode_no > 0:
                dedupe_key = f"index:{series_id}:{season_no}:{episode_no}"
            elif item_id:
                dedupe_key = f"id:{item_id}"
            else:
                dedupe_key = f"fallback:{series_id}:{season_no}:{episode_no}:{str(row.get('Name') or '').strip()}"
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            unique_rows.append(row)
            season_key = season_no if season_no > 0 else 1
            season_counts[season_key] = int(season_counts.get(season_key) or 0) + 1
            if episode_no > 0:
                name = str(row.get("Name") or "").strip()
                current = (season_key, episode_no, name)
                if latest is None or (current[0], current[1]) > (latest[0], latest[1]):
                    latest = current
        lines: list[str] = []
        for season_no in sorted(season_counts):
            count = int(season_counts.get(season_no) or 0)
            label = season_meta.get(season_no) or f"S{season_no}"
            lines.append(f"{label} {count}集")
        latest_text = ""
        if latest:
            latest_text = f"S{latest[0]:02d}E{latest[1]:02d}"
            if latest[2]:
                latest_text = f"{latest_text}「{latest[2]}」"
        return len(season_counts), len(unique_rows), lines, latest_text

    @staticmethod
    def _normalize_episode_map(raw: Any) -> dict[int, set[int]]:
        if not isinstance(raw, dict):
            return {}
        output: dict[int, set[int]] = {}
        for season_raw, values in raw.items():
            try:
                season = int(season_raw)
            except Exception:
                continue
            if season <= 0 or not isinstance(values, (set, list, tuple)):
                continue
            normalized: set[int] = set()
            for value in values:
                try:
                    episode = int(value)
                except Exception:
                    continue
                if episode > 0:
                    normalized.add(episode)
            if normalized:
                output[season] = normalized
        return output

    @staticmethod
    def _analyze_local_season_gaps(season_map: dict[int, set[int]]) -> dict[int, list[int]]:
        output: dict[int, list[int]] = {}
        for season, values in season_map.items():
            numbers = sorted(values)
            if not numbers:
                continue
            missing = [number for number in range(numbers[0], numbers[-1] + 1) if number not in values]
            if missing:
                output[season] = missing
        return output

    def _map_raw_season_map(self, *, raw_season_map: dict[int, set[int]], registered_map: dict[int, set[int]]) -> dict[str, Any]:
        mapped: dict[int, set[int]] = {}
        unmapped: list[dict[str, Any]] = []
        extras: list[dict[str, Any]] = []
        ordinal_map = self._episode_by_ordinal(registered_map)
        single_tmdb_season = len(registered_map) == 1
        only_season = next(iter(registered_map)) if single_tmdb_season else 0
        max_registered = max((max(values) for values in registered_map.values() if values), default=0)
        for season, values in raw_season_map.items():
            for episode in sorted(values):
                if episode in registered_map.get(season, set()):
                    mapped.setdefault(season, set()).add(episode)
                    continue
                if single_tmdb_season and episode in registered_map.get(only_season, set()):
                    mapped.setdefault(only_season, set()).add(episode)
                    continue
                ordinal_pair = ordinal_map.get(episode)
                if ordinal_pair and season == only_season:
                    mapped.setdefault(ordinal_pair[0], set()).add(ordinal_pair[1])
                    continue
                sample = {"rawSeason": season, "rawEpisode": episode, "name": "", "path": ""}
                if episode > max_registered:
                    extras.append(sample)
                else:
                    unmapped.append(sample)
        return {"mappedMap": mapped, "unmappedSamples": unmapped[:5], "extraSamples": extras[:5]}

    def _map_existing_episode_items(
        self,
        *,
        episode_items: list[dict[str, Any]],
        registered_map: dict[int, set[int]],
        total_registered: int,
    ) -> dict[str, Any]:
        mapped: dict[int, set[int]] = {}
        unmapped: list[dict[str, Any]] = []
        extras: list[dict[str, Any]] = []
        ordinal_map = self._episode_by_ordinal(registered_map)
        single_tmdb_season = len(registered_map) == 1
        only_season = next(iter(registered_map)) if single_tmdb_season else 0
        max_registered = max((max(values) for values in registered_map.values() if values), default=0)

        for item in episode_items:
            raw_season = self._coerce_index_number(item.get("season"))
            raw_episode = self._coerce_index_number(item.get("episode"))
            text_pair = self._extract_season_episode_pair(item)
            absolute_episode = self._extract_absolute_episode_number(item, max_episode=max(total_registered, max_registered))
            candidates: list[tuple[int, int]] = []
            if raw_season is not None and raw_episode is not None:
                candidates.append((raw_season, raw_episode))
            if text_pair and text_pair not in candidates:
                candidates.append(text_pair)
            if absolute_episode is not None:
                if single_tmdb_season and (only_season, absolute_episode) not in candidates:
                    candidates.append((only_season, absolute_episode))
                ordinal_pair = ordinal_map.get(absolute_episode)
                if ordinal_pair and ordinal_pair not in candidates:
                    candidates.append(ordinal_pair)
            if raw_episode is not None and single_tmdb_season and (only_season, raw_episode) not in candidates:
                candidates.append((only_season, raw_episode))

            mapped_pair: tuple[int, int] | None = None
            for season, episode in candidates:
                if episode in registered_map.get(season, set()):
                    mapped_pair = (season, episode)
                    break
            if mapped_pair:
                mapped.setdefault(mapped_pair[0], set()).add(mapped_pair[1])
                continue
            sample = {
                "rawSeason": raw_season or 0,
                "rawEpisode": raw_episode or 0,
                "name": str(item.get("name") or ""),
                "path": str(item.get("path") or ""),
            }
            if absolute_episode is not None and absolute_episode > max_registered:
                extras.append(sample)
            else:
                unmapped.append(sample)
        return {"mappedMap": mapped, "unmappedSamples": unmapped[:5], "extraSamples": extras[:5]}

    @staticmethod
    def _build_special_season_rows(*, specials: Any, special_total: int) -> list[dict[str, str]]:
        rows = [row for row in specials if isinstance(row, dict)] if isinstance(specials, list) else []
        existing_values = sorted(
            {
                int(row.get("episode") or 0)
                for row in rows
                if int(row.get("episode") or 0) > 0
                and not bool(row.get("isMissing"))
                and str(row.get("locationType") or "").strip().lower() != "virtual"
            }
        )
        total = max(int(special_total or 0), max(existing_values, default=0))
        if total <= 0 and not existing_values:
            return []
        return [
            {
                "seasonLabel": "S0 特别篇",
                "existingText": compress_plain_episode_numbers(existing_values) if existing_values else "未入库",
                "totalText": str(total or len(existing_values)),
                "missingText": "—",
                "statusText": "—",
            }
        ]

    @staticmethod
    def _episode_by_ordinal(registered_map: dict[int, set[int]]) -> dict[int, tuple[int, int]]:
        output: dict[int, tuple[int, int]] = {}
        ordinal = 0
        for season in sorted(registered_map):
            for episode in sorted(registered_map[season]):
                ordinal += 1
                output[ordinal] = (season, episode)
        return output

    @classmethod
    def _extract_season_episode_pair(cls, item: dict[str, Any]) -> tuple[int, int] | None:
        fields = [
            str(item.get("name") or "").strip(),
            str(item.get("sortName") or "").strip(),
            str(item.get("originalTitle") or "").strip(),
            os.path.basename(str(item.get("path") or "").strip()),
        ]
        patterns = (
            r"S(?P<season>\d{1,2})E(?P<episode>\d{1,4})",
            r"第\s*(?P<season>\d{1,2})\s*季\s*第\s*(?P<episode>\d{1,4})\s*[集话話]",
        )
        for value in fields:
            if not value:
                continue
            for pattern in patterns:
                match = re.search(pattern, value, flags=re.IGNORECASE)
                if not match:
                    continue
                season = cls._coerce_index_number(match.group("season"))
                episode = cls._coerce_index_number(match.group("episode"))
                if season is not None and episode is not None:
                    return season, episode
        return None

    @classmethod
    def _extract_absolute_episode_number(cls, item: dict[str, Any], *, max_episode: int = 0) -> int | None:
        raw_episode = cls._coerce_index_number(item.get("episode"))
        if raw_episode is not None and raw_episode > 0 and (max_episode <= 0 or raw_episode <= max_episode):
            return raw_episode
        fields = [
            str(item.get("name") or "").strip(),
            str(item.get("sortName") or "").strip(),
            str(item.get("originalTitle") or "").strip(),
            os.path.basename(str(item.get("path") or "").strip()),
            str(item.get("path") or "").strip(),
        ]
        strong_patterns = (
            r"第\s*(\d{1,4})\s*[集话話]",
            r"S\d{1,2}E(\d{1,4})",
            r"(?:^|[^A-Za-z0-9])E(?:P)?\s*0*(\d{1,4})(?:[^A-Za-z0-9]|$)",
        )
        for value in fields:
            if not value:
                continue
            for pattern in strong_patterns:
                match = re.search(pattern, value, flags=re.IGNORECASE)
                if not match:
                    continue
                episode = cls._coerce_index_number(match.group(1))
                if episode is not None and episode > 0 and (max_episode <= 0 or episode <= max_episode):
                    return episode
        for value in fields:
            if not value:
                continue
            numbers = [int(match) for match in re.findall(r"(?<!\d)(\d{1,4})(?!\d)", value)]
            filtered = [number for number in numbers if number not in {360, 480, 720, 1080, 2160, 2020, 2021, 2022, 2023, 2024, 2025, 2026}]
            viable = [number for number in filtered if number > 0 and (max_episode <= 0 or number <= max_episode)]
            if len(viable) == 1 and viable[0] >= 100:
                return viable[0]
        return None

    def _tmdb_context(self) -> tuple[str, str, str]:
        store = self._read_store()
        emby = self._apply_emby_env_overrides(store.get("embyConfig"))
        token = str(emby.get("tmdbToken") or os.environ.get("APP_TMDB_TOKEN") or os.environ.get("TMDB_TOKEN") or "").strip()
        language = str(emby.get("tmdbLanguage") or "zh-CN").strip() or "zh-CN"
        region = str(emby.get("tmdbRegion") or "CN").strip().upper() or "CN"
        return token, language, region

    def _tmdb_get_json(self, path_with_query: str, *, token: str) -> dict[str, Any]:
        target = f"https://api.themoviedb.org/3{path_with_query}"
        request = urllib.request.Request(
            target,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "User-Agent": "VistamirrorAI/1.0",
            },
        )
        with urllib.request.urlopen(request, timeout=12) as response:
            return json.loads(response.read().decode("utf-8", errors="replace"))

    def _read_store(self) -> dict[str, Any]:
        path = self.store_path
        if not path.exists():
            return {"embyConfig": {}, "hdhiveConfig": {}, "drive115Config": {}}
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            data = {}
        if not isinstance(data, dict):
            data = {}
        return {
            "embyConfig": data.get("embyConfig") if isinstance(data.get("embyConfig"), dict) else {},
            "hdhiveConfig": data.get("hdhiveConfig") if isinstance(data.get("hdhiveConfig"), dict) else {},
            "drive115Config": data.get("drive115Config") if isinstance(data.get("drive115Config"), dict) else {},
        }

    @staticmethod
    def _apply_emby_env_overrides(raw: Any) -> dict[str, Any]:
        source = raw if isinstance(raw, dict) else {}
        merged = {
            "tmdbToken": str(source.get("tmdbToken") or "").strip(),
            "tmdbLanguage": str(source.get("tmdbLanguage") or "zh-CN").strip() or "zh-CN",
            "tmdbRegion": str(source.get("tmdbRegion") or "CN").strip().upper() or "CN",
        }
        app_tmdb = str(os.environ.get("APP_TMDB_TOKEN") or "").strip()
        if app_tmdb:
            merged["tmdbToken"] = app_tmdb
        legacy_tmdb = str(os.environ.get("TMDB_TOKEN") or "").strip()
        if legacy_tmdb and not app_tmdb:
            merged["tmdbToken"] = legacy_tmdb
        return merged

    @staticmethod
    def _apply_drive115_env_overrides(raw: Any) -> dict[str, Any]:
        from .drive115_service import apply_drive115_env_overrides

        return apply_drive115_env_overrides(raw)

    @staticmethod
    def _format_emby_error(err: Exception) -> str:
        code = getattr(err, "code", None)
        if code:
            return f"HTTP {code}"
        return str(err)

    @staticmethod
    def _format_rating(detail: dict[str, Any]) -> str:
        for key in ("CommunityRating", "CriticRating"):
            value = detail.get(key)
            if isinstance(value, (int, float)) and value > 0:
                return f"{float(value):.1f}".rstrip("0").rstrip(".")
        return "N/A"

    @staticmethod
    def _coerce_index_number(value: Any) -> int | None:
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        text = str(value or "").strip()
        if text.isdigit():
            try:
                return int(text)
            except Exception:
                return None
        return None

    @staticmethod
    def _extract_season_number(text: str) -> str:
        match = re.search(r"(\d+)", str(text or ""))
        if not match:
            return ""
        return str(match.group(1) or "")

    @staticmethod
    def _resolve_year(detail: dict[str, Any]) -> str:
        year = detail.get("ProductionYear")
        if isinstance(year, int) and year > 0:
            return str(year)
        premiere = str(detail.get("PremiereDate") or "").strip()
        if re.match(r"^\d{4}", premiere):
            return premiere[:4]
        return "未知"

    @staticmethod
    def _normalize_ai_title(value: str) -> str:
        text = str(value or "").strip().lower()
        text = re.sub(r"[\s·._:：,，。!！?？'\"《》「」“”\-—–]+", "", text)
        return text
