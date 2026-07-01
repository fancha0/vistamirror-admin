from __future__ import annotations

from datetime import date, datetime
import json
import pathlib
import re
import threading
import urllib.parse
from typing import Any, Callable, Union


JsonFetcher = Callable[[str], Union[dict[str, Any], list[Any], None]]


class MediaIdentityService:
    def __init__(
        self,
        *,
        emby_fetcher: JsonFetcher,
        tmdb_fetcher: JsonFetcher | None = None,
        cache_path: pathlib.Path | None = None,
        language: str = "zh-CN",
        region: str = "CN",
    ) -> None:
        self.emby_fetcher = emby_fetcher
        self.tmdb_fetcher = tmdb_fetcher
        self.cache_path = cache_path
        self.language = str(language or "zh-CN").strip() or "zh-CN"
        self.region = str(region or "CN").strip().upper() or "CN"
        self._lock = threading.RLock()
        self._cache = self._load_cache()

    def resolve(self, query: str, *, preferred_type: str = "") -> dict[str, Any]:
        title = str(query or "").strip()
        if not title:
            return {"identity": {}, "embyItem": {}, "candidates": [], "ambiguous": False}

        local_rows = self._search_emby(title, preferred_type=preferred_type)
        local_best = self._pick_local(local_rows, title=title, preferred_type=preferred_type)
        local_tmdb = self._provider_tmdb_id(local_best)
        if local_tmdb:
            identity = self.identity_from_emby_item(local_best)
            self._remember(identity, local_best)
            return {"identity": identity, "embyItem": local_best, "candidates": [], "ambiguous": False}

        candidates = self.search_media(title, media_type=preferred_type)
        ambiguous = self._is_ambiguous(candidates)
        if ambiguous and local_best:
            local_year = str(local_best.get("ProductionYear") or "").strip()
            year_matches = [row for row in candidates if local_year and str(row.get("year") or "").strip() == local_year]
            if len(year_matches) == 1:
                selected = year_matches[0]
                candidates = [selected] + [row for row in candidates if row is not selected]
                ambiguous = False
        if ambiguous:
            return {"identity": {}, "embyItem": {}, "candidates": candidates[:5], "ambiguous": True}
        identity = dict(candidates[0]) if candidates else self.identity_from_emby_item(local_best, allow_tmdb_search=False)
        lookup_rows = list(local_rows)
        standard_title = str(identity.get("title") or "").strip() if identity else ""
        if identity and standard_title and (not lookup_rows or self.normalize_title(standard_title) != self.normalize_title(title)):
            try:
                standard_rows = self._search_emby(standard_title, preferred_type=str(identity.get("type") or preferred_type))
            except Exception:
                standard_rows = []
            known_ids = {str(row.get("Id") or "") for row in lookup_rows}
            lookup_rows.extend(row for row in standard_rows if str(row.get("Id") or "") not in known_ids)
        emby_item = self.find_emby_item(identity, local_candidates=lookup_rows or None) if identity else local_best
        if emby_item and not identity:
            identity = self.identity_from_emby_item(emby_item, allow_tmdb_search=False)
        if identity and emby_item:
            identity["embyId"] = str(emby_item.get("Id") or "").strip()
            self._remember(identity, emby_item)
        return {
            "identity": identity,
            "embyItem": emby_item,
            "candidates": candidates[:5],
            "ambiguous": ambiguous,
        }

    def search_local_candidates(
        self,
        query: str,
        *,
        preferred_type: str = "",
        tmdb_id: str = "",
        year: str = "",
        alternative_titles: list[str] | None = None,
        limit: int = 8,
    ) -> list[dict[str, Any]]:
        keywords: list[str] = []
        for value in [query, *(alternative_titles or [])]:
            keyword = str(value or "").strip()
            if keyword and keyword not in keywords:
                keywords.append(keyword)
        if not keywords:
            return []

        rows_by_id: dict[str, dict[str, Any]] = {}
        for keyword in keywords:
            try:
                rows = self._search_emby(keyword, preferred_type=preferred_type)
            except Exception:
                continue
            for row in rows:
                item_id = str(row.get("Id") or "").strip()
                if not item_id:
                    continue
                existing = rows_by_id.get(item_id)
                if existing is None:
                    rows_by_id[item_id] = dict(row)
                    continue
                existing_name = str(existing.get("Name") or existing.get("SeriesName") or "").strip()
                row_name = str(row.get("Name") or row.get("SeriesName") or "").strip()
                if len(row_name) > len(existing_name):
                    rows_by_id[item_id] = dict(row)

        target_title = str(alternative_titles[0] if alternative_titles else query or "").strip()
        normalized_target = self.normalize_title(target_title or query)
        desired_year = str(year or "").strip()
        desired_tmdb = str(tmdb_id or "").strip()
        candidates: list[dict[str, Any]] = []
        for row in rows_by_id.values():
            title = str(row.get("Name") or row.get("SeriesName") or "").strip()
            normalized_title = self.normalize_title(title)
            provider_tmdb = self._provider_tmdb_id(row)
            row_year = str(row.get("ProductionYear") or "")[:4]
            row_type = self._local_type(str(row.get("Type") or ""))
            is_series = row_type == "series"
            episode_count = max(
                self._coerce_int(row.get("RecursiveItemCount")),
                self._coerce_int(row.get("ChildCount")),
            ) if is_series else 0
            is_title_exact = bool(normalized_target and normalized_title == normalized_target)
            is_title_similar = bool(normalized_target and normalized_title and (normalized_target in normalized_title or normalized_title in normalized_target))
            year_delta = 99
            if desired_year.isdigit() and row_year.isdigit():
                year_delta = abs(int(desired_year) - int(row_year))
            score = 0
            if is_title_exact:
                score += 1000
            elif is_title_similar:
                score += 700
            if provider_tmdb and desired_tmdb and provider_tmdb == desired_tmdb:
                score += 300
            if desired_year and row_year == desired_year:
                score += 120
            elif year_delta == 1:
                score += 60
            if self._type_matches(str(row.get("Type") or ""), preferred_type):
                score += 80
            if is_series:
                score += 40
            score += min(40, max(0, episode_count))
            score_reason_parts: list[str] = []
            if is_title_exact:
                score_reason_parts.append("标题完全匹配")
            elif is_title_similar:
                score_reason_parts.append("标题相似")
            else:
                score_reason_parts.append("标题不稳定")
            if provider_tmdb and desired_tmdb and provider_tmdb == desired_tmdb:
                score_reason_parts.append("TMDB 一致")
            elif provider_tmdb and desired_tmdb and provider_tmdb != desired_tmdb:
                score_reason_parts.append("TMDB 不一致")
            else:
                score_reason_parts.append("TMDB 未绑定")
            if desired_year and row_year:
                if row_year == desired_year:
                    score_reason_parts.append("年份一致")
                elif year_delta == 1:
                    score_reason_parts.append("年份接近")
            if is_series:
                score_reason_parts.append("Series")
            if episode_count > 0:
                score_reason_parts.append(f"{episode_count} 集")
            candidates.append(
                {
                    "embyItemId": str(row.get("Id") or "").strip(),
                    "title": title or "未知作品",
                    "year": row_year,
                    "type": row_type or str(row.get("Type") or "").strip().lower() or "unknown",
                    "tmdbId": provider_tmdb,
                    "episodeCount": episode_count,
                    "score": score,
                    "scoreReason": " / ".join(score_reason_parts),
                    "isTitleExact": is_title_exact,
                    "isTitleSimilar": is_title_similar,
                    "isSeries": is_series,
                    "yearDelta": year_delta,
                }
            )
        candidates.sort(
            key=lambda row: (
                int(row.get("score") or 0),
                1 if row.get("isTitleExact") else 0,
                1 if row.get("tmdbId") and desired_tmdb and row.get("tmdbId") == desired_tmdb else 0,
                1 if row.get("isSeries") else 0,
                int(row.get("episodeCount") or 0),
            ),
            reverse=True,
        )
        return candidates[: max(1, min(20, int(limit or 8)))]

    def search_media(self, query: str, *, media_type: str = "") -> list[dict[str, Any]]:
        if not self.tmdb_fetcher:
            return []
        normalized_type = self._tmdb_type(media_type)
        types = [normalized_type] if normalized_type else ["tv", "movie"]
        output: list[dict[str, Any]] = []
        target = self.normalize_title(query)
        for item_type in types:
            params = {"query": str(query or "").strip(), "language": self.language}
            if self.region:
                params["region"] = self.region
            payload = self.tmdb_fetcher(f"/search/{item_type}?{urllib.parse.urlencode(params)}")
            rows = payload.get("results") if isinstance(payload, dict) else []
            for row in rows[:10] if isinstance(rows, list) else []:
                if not isinstance(row, dict):
                    continue
                name = str(row.get("name") or row.get("title") or "").strip()
                if not name:
                    continue
                year = str(row.get("first_air_date") or row.get("release_date") or "")[:4]
                normalized_name = self.normalize_title(name)
                score = 100 if target and normalized_name == target else 70 if target and (target in normalized_name or normalized_name in target) else 20
                output.append(
                    {
                        "title": name,
                        "originalTitle": str(row.get("original_name") or row.get("original_title") or "").strip(),
                        "year": year,
                        "type": "series" if item_type == "tv" else "movie",
                        "tmdbId": str(row.get("id") or "").strip(),
                        "overview": str(row.get("overview") or "").strip(),
                        "rating": row.get("vote_average"),
                        "score": score,
                        "confidence": "TMDB 标题精确匹配" if score >= 100 else "TMDB 标题候选匹配",
                    }
                )
        output.sort(key=lambda row: (int(row.get("score") or 0), float(row.get("rating") or 0)), reverse=True)
        return output

    def get_media_detail(self, tmdb_id: str, media_type: str) -> dict[str, Any]:
        safe_id = str(tmdb_id or "").strip()
        item_type = self._tmdb_type(media_type)
        if not safe_id or not item_type or not self.tmdb_fetcher:
            return {}
        params = {"language": self.language, "append_to_response": "credits,external_ids"}
        payload = self.tmdb_fetcher(f"/{item_type}/{urllib.parse.quote(safe_id, safe='')}?{urllib.parse.urlencode(params)}")
        return payload if isinstance(payload, dict) else {}

    def query_media_detail(self, tmdb_id: str, media_type: str) -> dict[str, Any]:
        tmdb_query_count = 1
        detail = self.get_media_detail(tmdb_id, media_type)
        if not detail:
            return {"ok": False, "tmdbId": str(tmdb_id or ""), "totalEpisodes": 0, "seasonCounts": {}, "tmdbQueryCount": tmdb_query_count}
        season_counts: dict[int, int] = {}
        special_episode_count = 0
        registered_season_map: dict[int, set[int]] = {}
        aired_season_map: dict[int, set[int]] = {}
        future_season_map: dict[int, set[int]] = {}
        unknown_air_date_map: dict[int, set[int]] = {}
        season_errors: list[dict[str, Any]] = []
        today = date.today()
        last_aired_date = ""
        seasons = detail.get("seasons") if isinstance(detail.get("seasons"), list) else []
        for season in seasons:
            if not isinstance(season, dict):
                continue
            try:
                season_no = int(season.get("season_number") or 0)
                episode_count = int(season.get("episode_count") or 0)
            except Exception:
                continue
            if season_no == 0 and episode_count > 0:
                special_episode_count = episode_count
                continue
            if season_no > 0 and episode_count > 0:
                season_counts[season_no] = episode_count
                try:
                    tmdb_query_count += 1
                    params = {"language": self.language}
                    payload = self.tmdb_fetcher(
                        f"/tv/{urllib.parse.quote(str(tmdb_id or '').strip(), safe='')}/season/{season_no}?{urllib.parse.urlencode(params)}"
                    ) if self.tmdb_fetcher else {}
                    rows = payload.get("episodes") if isinstance(payload, dict) else []
                    if not isinstance(rows, list):
                        rows = []
                    for row in rows:
                        if not isinstance(row, dict):
                            continue
                        try:
                            episode_no = int(row.get("episode_number") or 0)
                        except Exception:
                            continue
                        if episode_no <= 0:
                            continue
                        registered_season_map.setdefault(season_no, set()).add(episode_no)
                        air_date_raw = str(row.get("air_date") or "").strip()
                        if not air_date_raw:
                            unknown_air_date_map.setdefault(season_no, set()).add(episode_no)
                            continue
                        try:
                            air_date = date.fromisoformat(air_date_raw[:10])
                        except ValueError:
                            unknown_air_date_map.setdefault(season_no, set()).add(episode_no)
                            continue
                        target = aired_season_map if air_date <= today else future_season_map
                        target.setdefault(season_no, set()).add(episode_no)
                        if air_date <= today and air_date_raw[:10] > last_aired_date:
                            last_aired_date = air_date_raw[:10]
                except Exception as err:
                    season_errors.append({"season": season_no, "error": type(err).__name__})
        try:
            total_episodes = int(detail.get("number_of_episodes") or 0)
        except Exception:
            total_episodes = 0
        if total_episodes <= 0:
            total_episodes = sum(season_counts.values())
        registered_count = sum(len(values) for values in registered_season_map.values())
        aired_count = sum(len(values) for values in aired_season_map.values())
        future_count = sum(len(values) for values in future_season_map.values())
        unknown_count = sum(len(values) for values in unknown_air_date_map.values())
        return {
            "ok": registered_count > 0,
            "tmdbId": str(tmdb_id or ""),
            "title": str(detail.get("name") or detail.get("title") or "").strip(),
            "year": str(detail.get("first_air_date") or detail.get("release_date") or "")[:4],
            "rating": detail.get("vote_average"),
            "totalEpisodes": total_episodes,
            "seasonCounts": season_counts,
            "registeredEpisodes": registered_count,
            "airedEpisodes": aired_count,
            "futureEpisodes": future_count,
            "unknownAirDateEpisodes": unknown_count,
            "registeredSeasonMap": registered_season_map,
            "airedSeasonMap": aired_season_map,
            "futureSeasonMap": future_season_map,
            "unknownAirDateMap": unknown_air_date_map,
            "specialEpisodeCount": special_episode_count,
            "seasonErrors": season_errors,
            "lastAiredDate": last_aired_date,
            "tmdbQueryCount": tmdb_query_count,
            "overview": str(detail.get("overview") or "").strip(),
        }

    def query_library_exists_by_tmdb(self, identity: dict[str, Any]) -> dict[str, Any]:
        metrics = {"embyQueryCount": 0}
        tmdb_id = str(identity.get("tmdbId") or "").strip()
        media_type = str(identity.get("type") or "").strip()
        emby_id = str(identity.get("embyId") or "").strip()
        force_emby_item = bool(identity.get("forceEmbyItem"))
        title = str(identity.get("title") or "").strip()

        item: dict[str, Any] = {}
        if emby_id:
            try:
                payload = self._fetch_emby_item_by_id(emby_id, metrics=metrics)
                if isinstance(payload, dict) and payload:
                    provider_tmdb = self._provider_tmdb_id(payload)
                    if force_emby_item or not tmdb_id or provider_tmdb == tmdb_id:
                        item = payload
            except Exception:
                item = {}
        elif tmdb_id:
            try:
                self._increment_metric(metrics, "embyQueryCount")
                provider_rows = self._search_emby_by_tmdb_id(tmdb_id, preferred_type=media_type)
            except Exception:
                provider_rows = []
            if provider_rows:
                item = self._pick_local(provider_rows, title=title, preferred_type=media_type)
        if not item:
            return self._empty_library_inventory(metrics)
        return self._build_library_inventory(item, identity=identity, metrics=metrics)

    def query_library_exists(self, identity: dict[str, Any]) -> dict[str, Any]:
        exact = self.query_library_exists_by_tmdb(identity)
        if bool(exact.get("exists")) or bool(identity.get("forceEmbyItem")):
            return exact

        metrics = {"embyQueryCount": int(exact.get("embyQueryCount") or 0)}
        title = str(identity.get("title") or "").strip()
        media_type = str(identity.get("type") or "").strip()
        year = str(identity.get("year") or "").strip()
        tmdb_id = str(identity.get("tmdbId") or "").strip()
        if not title:
            return self._empty_library_inventory(metrics)
        try:
            self._increment_metric(metrics, "embyQueryCount")
            candidates = self._search_emby(title, preferred_type=media_type)
        except Exception:
            candidates = []
        item = self._pick_fallback_emby_item(
            candidates,
            title=title,
            year=year,
            tmdb_id=tmdb_id,
            preferred_type=media_type,
        )
        if not item:
            return self._empty_library_inventory(metrics)
        return self._build_library_inventory(item, identity=identity, metrics=metrics)

    def _build_library_inventory(
        self,
        item: dict[str, Any],
        *,
        identity: dict[str, Any],
        metrics: dict[str, int] | None = None,
    ) -> dict[str, Any]:
        item_id = str(item.get("Id") or "").strip()
        if self._local_type(str(item.get("Type") or identity.get("type") or "")) != "series":
            return {
                **self._empty_library_inventory(metrics),
                "exists": True,
                "embyItem": item,
            }
        seasons = self._fetch_emby_seasons(item_id, metrics=metrics)
        episodes = self._fetch_emby_episodes(item_id, metrics=metrics)
        season_map: dict[int, set[int]] = {}
        missing_episode_map: dict[int, set[int]] = {}
        specials: list[dict[str, Any]] = []
        duplicates: list[dict[str, Any]] = []
        seen: set[tuple[int, int]] = set()
        episode_items: list[dict[str, Any]] = []
        missing_episode_items: list[dict[str, Any]] = []
        present_episode_rows = 0
        for row in episodes:
            if not isinstance(row, dict):
                continue
            normalized = self._normalize_episode_item(row)
            episode_items.append(normalized)
            try:
                season = int(row.get("ParentIndexNumber") or 0)
                episode = int(row.get("IndexNumber") or 0)
            except Exception:
                continue
            if episode <= 0:
                continue
            if season <= 0:
                specials.append(
                    {
                        "season": season,
                        "episode": episode,
                        "name": str(row.get("Name") or ""),
                        "path": str(row.get("Path") or ""),
                        "isMissing": bool(normalized.get("isMissing")),
                        "locationType": str(normalized.get("locationType") or "").strip(),
                    }
                )
                continue
            if normalized.get("isMissing") or str(normalized.get("locationType") or "").strip().lower() == "virtual":
                missing_episode_map.setdefault(season, set()).add(episode)
                missing_episode_items.append(normalized)
                continue
            present_episode_rows += 1
            key = (season, episode)
            if key in seen:
                duplicates.append(
                    {
                        "season": season,
                        "episode": episode,
                        "name": str(row.get("Name") or ""),
                        "path": str(row.get("Path") or ""),
                    }
                )
                continue
            seen.add(key)
            season_map.setdefault(season, set()).add(episode)
        return {
            "ok": True,
            "exists": True,
            "embyItem": item,
            "seasonMap": season_map,
            "seasonItems": seasons,
            "specials": specials,
            "duplicates": duplicates,
            "episodeRows": present_episode_rows,
            "episodeItems": episode_items,
            "missingEpisodeMap": missing_episode_map,
            "missingEpisodeItems": missing_episode_items,
            "hasMissingEpisodeData": bool(missing_episode_map),
            **metrics,
        }

    @staticmethod
    def _empty_library_inventory(metrics: dict[str, int] | None = None) -> dict[str, Any]:
        return {
            "ok": True,
            "exists": False,
            "embyItem": {},
            "seasonMap": {},
            "seasonItems": [],
            "specials": [],
            "duplicates": [],
            "episodeItems": [],
            "missingEpisodeMap": {},
            "missingEpisodeItems": [],
            "hasMissingEpisodeData": False,
            **(metrics or {}),
        }

    @classmethod
    def compare_episode_inventory(cls, expected: dict[str, Any], existing: dict[str, Any]) -> dict[str, Any]:
        total = int(expected.get("totalEpisodes") or 0)
        season_counts = expected.get("seasonCounts") if isinstance(expected.get("seasonCounts"), dict) else {}
        season_counts = {int(key): int(value) for key, value in season_counts.items() if int(key) > 0 and int(value) > 0}
        registered_map = cls._normalize_episode_map(expected.get("registeredSeasonMap"))
        aired_map = cls._normalize_episode_map(expected.get("airedSeasonMap"))
        future_map = cls._normalize_episode_map(expected.get("futureSeasonMap"))
        unknown_map = cls._normalize_episode_map(expected.get("unknownAirDateMap"))
        if not registered_map and season_counts:
            registered_map = {season: set(range(1, count + 1)) for season, count in season_counts.items()}
        if not aired_map and not future_map and not unknown_map:
            aired_map = {season: set(values) for season, values in registered_map.items()}
        season_map = existing.get("seasonMap") if isinstance(existing.get("seasonMap"), dict) else {}
        season_map = {int(key): {int(value) for value in values if int(value) > 0} for key, values in season_map.items() if isinstance(values, (set, list, tuple))}
        mode = cls._detect_episode_numbering_mode(season_map)
        if len(season_map) == 1:
            emby_season, emby_values = next(iter(season_map.items()))
            tmdb_season_max = max(registered_map.get(emby_season, set()), default=0)
            if len(registered_map) == 1 or (emby_values and max(emby_values) > tmdb_season_max):
                mode = "global"
        ordinal_by_episode: dict[tuple[int, int], int] = {}
        episode_by_ordinal: dict[int, tuple[int, int]] = {}
        ordinal = 0
        for season in sorted(registered_map):
            for episode in sorted(registered_map[season]):
                ordinal += 1
                ordinal_by_episode[(season, episode)] = ordinal
                episode_by_ordinal[ordinal] = (season, episode)
        expected_set = {ordinal_by_episode[key] for season, values in aired_map.items() for key in ((season, episode) for episode in values) if key in ordinal_by_episode}
        future_set = {ordinal_by_episode[key] for season, values in future_map.items() for key in ((season, episode) for episode in values) if key in ordinal_by_episode}
        unknown_set = {ordinal_by_episode[key] for season, values in unknown_map.items() for key in ((season, episode) for episode in values) if key in ordinal_by_episode}
        existing_set: set[int] = set()
        unmapped: list[dict[str, int]] = []
        if mode == "global":
            for values in season_map.values():
                existing_set.update(values)
        else:
            for season, values in season_map.items():
                for episode in values:
                    mapped = ordinal_by_episode.get((season, episode))
                    if mapped:
                        existing_set.add(mapped)
                    else:
                        unmapped.append({"season": season, "episode": episode})
        missing = sorted(expected_set.difference(existing_set))
        registered_set = set(episode_by_ordinal)
        extras = sorted(existing_set.difference(registered_set))
        label = lambda value: cls._format_episode_label(value, mode=mode, episode_by_ordinal=episode_by_ordinal)
        return {
            "mode": mode,
            "expectedCount": len(expected_set),
            "existingCount": len(existing_set.intersection(expected_set)),
            "localEpisodeCount": sum(len(values) for values in season_map.values()),
            "registeredCount": len(registered_set) if registered_set else total,
            "missing": missing,
            "missingLabels": [label(value) for value in missing],
            "future": sorted(future_set),
            "futureLabels": [label(value) for value in sorted(future_set)],
            "unknownAirDate": sorted(unknown_set),
            "unknownAirDateLabels": [label(value) for value in sorted(unknown_set)],
            "extras": extras,
            "extraLabels": [label(value) for value in extras],
            "unmapped": unmapped,
            "specials": existing.get("specials") if isinstance(existing.get("specials"), list) else [],
            "duplicates": existing.get("duplicates") if isinstance(existing.get("duplicates"), list) else [],
        }

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
    def _format_episode_label(value: int, *, mode: str, episode_by_ordinal: dict[int, tuple[int, int]]) -> str:
        if mode == "global":
            return f"E{int(value)}"
        season_episode = episode_by_ordinal.get(int(value))
        if not season_episode:
            return f"E{int(value)}"
        return f"S{season_episode[0]:02d}E{season_episode[1]:02d}"

    def find_emby_item(
        self,
        identity: dict[str, Any],
        *,
        local_candidates: list[dict[str, Any]] | None = None,
        metrics: dict[str, int] | None = None,
    ) -> dict[str, Any]:
        tmdb_id = str(identity.get("tmdbId") or "").strip()
        emby_id = str(identity.get("embyId") or "").strip()
        media_type = str(identity.get("type") or "").strip()
        title = str(identity.get("title") or "").strip()
        year = str(identity.get("year") or "").strip()
        force_emby_item = bool(identity.get("forceEmbyItem"))
        cache_key = self._cache_key(tmdb_id, media_type)
        cached = self._cache.get(cache_key) if cache_key else None
        if isinstance(cached, dict) and cached.get("embyId"):
            try:
                self._increment_metric(metrics, "embyQueryCount")
                payload = self.emby_fetcher(
                    f"/Items/{urllib.parse.quote(str(cached['embyId']), safe='')}?Fields=Name,Type,ProductionYear,ProviderIds,Overview,CommunityRating"
                )
                if isinstance(payload, dict) and (not tmdb_id or self._provider_tmdb_id(payload) == tmdb_id):
                    return payload
            except Exception:
                pass

        if emby_id:
            try:
                self._increment_metric(metrics, "embyQueryCount")
                payload = self.emby_fetcher(
                    f"/Items/{urllib.parse.quote(emby_id, safe='')}?Fields=Name,Type,ProductionYear,ProviderIds,Overview,CommunityRating"
                )
                if isinstance(payload, dict) and payload:
                    if force_emby_item or not tmdb_id or self._provider_tmdb_id(payload) == tmdb_id:
                        return payload
            except Exception:
                pass

        if tmdb_id:
            try:
                self._increment_metric(metrics, "embyQueryCount")
                provider_rows = self._search_emby_by_tmdb_id(tmdb_id, preferred_type=media_type)
            except Exception:
                provider_rows = []
            if provider_rows:
                return self._pick_local(provider_rows, title=title, preferred_type=media_type)

        if isinstance(local_candidates, list):
            candidates = local_candidates
        else:
            self._increment_metric(metrics, "embyQueryCount")
            candidates = self._search_emby(title, preferred_type=media_type)
        if tmdb_id:
            exact_provider = [row for row in candidates if self._provider_tmdb_id(row) == tmdb_id]
            if exact_provider:
                return self._pick_local(exact_provider, title=title, preferred_type=media_type)
        scored: list[tuple[int, dict[str, Any]]] = []
        normalized_title = self.normalize_title(title)
        for row in candidates:
            row_name = self.normalize_title(str(row.get("Name") or row.get("SeriesName") or ""))
            score = 0
            if normalized_title and row_name == normalized_title:
                score += 80
            elif normalized_title and (normalized_title in row_name or row_name in normalized_title):
                score += 40
            row_year = str(row.get("ProductionYear") or "").strip()
            if year and row_year == year:
                score += 20
            if self._type_matches(str(row.get("Type") or ""), media_type):
                score += 15
            scored.append((score, row))
        scored.sort(key=lambda item: item[0], reverse=True)
        return scored[0][1] if scored and scored[0][0] >= 55 else {}

    def identity_from_emby_item(self, item: dict[str, Any], *, allow_tmdb_search: bool = True) -> dict[str, Any]:
        if not isinstance(item, dict) or not item:
            return {}
        item_type = self._local_type(str(item.get("Type") or ""))
        title = str(item.get("Name") or item.get("SeriesName") or "").strip()
        year = str(item.get("ProductionYear") or "").strip()
        tmdb_id = self._provider_tmdb_id(item)
        confidence = "Emby ProviderIds" if tmdb_id else "Emby 标题匹配"
        if not tmdb_id and allow_tmdb_search and title:
            candidates = self.search_media(title, media_type=item_type)
            if candidates:
                best = candidates[0]
                tmdb_id = str(best.get("tmdbId") or "")
                title = str(best.get("title") or title)
                year = str(best.get("year") or year)
                confidence = str(best.get("confidence") or "TMDB 搜索匹配")
        return {
            "title": title,
            "year": year,
            "type": item_type,
            "tmdbId": tmdb_id,
            "embyId": str(item.get("Id") or "").strip(),
            "source": "emby_provider" if self._provider_tmdb_id(item) else "matched",
            "confidence": confidence,
        }

    def _search_emby(self, title: str, *, preferred_type: str = "") -> list[dict[str, Any]]:
        include_types = "Series" if self._local_type(preferred_type) == "series" else "Movie" if self._local_type(preferred_type) == "movie" else "Series,Movie,Episode"
        query = urllib.parse.urlencode(
            {
                "Recursive": "true",
                "SearchTerm": str(title or "").strip(),
                "IncludeItemTypes": include_types,
                "Fields": "Name,Type,SeriesName,SeriesId,ProductionYear,PremiereDate,ProviderIds,Overview,CommunityRating,ChildCount,RecursiveItemCount",
                "Limit": "30",
            }
        )
        payload = self.emby_fetcher(f"/Items?{query}")
        rows = payload.get("Items") if isinstance(payload, dict) else payload
        return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []

    def _search_emby_by_tmdb_id(self, tmdb_id: str, *, preferred_type: str = "") -> list[dict[str, Any]]:
        include_types = "Series" if self._local_type(preferred_type) == "series" else "Movie" if self._local_type(preferred_type) == "movie" else "Series,Movie"
        query = urllib.parse.urlencode(
            {
                "AnyProviderIdEqualTo": f"tmdb:{str(tmdb_id or '').strip()}",
                "Recursive": "true",
                "IncludeItemTypes": include_types,
                "Fields": "Name,Type,ProductionYear,PremiereDate,ProviderIds,Overview,CommunityRating,ChildCount,RecursiveItemCount",
                "Limit": "200",
            }
        )
        payload = self.emby_fetcher(f"/Items?{query}")
        rows = payload.get("Items") if isinstance(payload, dict) else payload
        candidates = [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []
        safe_tmdb = str(tmdb_id or "").strip()
        if not safe_tmdb:
            return candidates
        return [row for row in candidates if self._provider_tmdb_id(row) == safe_tmdb]

    def _fetch_emby_item_by_id(self, item_id: str, *, metrics: dict[str, int] | None = None) -> dict[str, Any]:
        safe_id = str(item_id or "").strip()
        if not safe_id:
            return {}
        query = urllib.parse.urlencode(
            {
                "Ids": safe_id,
                "Fields": "Name,Type,ProductionYear,ProviderIds,Overview,CommunityRating,ChildCount,RecursiveItemCount",
            }
        )
        self._increment_metric(metrics, "embyQueryCount")
        payload = self.emby_fetcher(f"/Items?{query}")
        rows = payload.get("Items") if isinstance(payload, dict) else payload
        items = [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []
        return items[0] if items else {}

    def _fetch_emby_seasons(self, series_id: str, *, metrics: dict[str, int] | None = None) -> list[dict[str, Any]]:
        safe_id = urllib.parse.quote(str(series_id or "").strip(), safe="")
        if not safe_id:
            return []
        self._increment_metric(metrics, "embyQueryCount")
        query = urllib.parse.urlencode(
            {
                "ParentId": str(series_id or "").strip(),
                "IncludeItemTypes": "Season",
                "Fields": "Name,IndexNumber,Id",
                "Recursive": "true",
                "Limit": "100",
            }
        )
        payload = self.emby_fetcher(f"/Items?{query}")
        rows = payload.get("Items") if isinstance(payload, dict) else payload
        if not isinstance(rows, list):
            return []
        output: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            output.append(
                {
                    "season": self._coerce_int(row.get("IndexNumber")),
                    "name": str(row.get("Name") or "").strip(),
                    "itemId": str(row.get("Id") or "").strip(),
                }
            )
        return output

    def _fetch_emby_episodes(self, series_id: str, *, metrics: dict[str, int] | None = None) -> list[dict[str, Any]]:
        safe_id = urllib.parse.quote(str(series_id or "").strip(), safe="")
        if not safe_id:
            return []
        output: list[dict[str, Any]] = []
        start = 0
        page_size = 1000
        while start < 10000:
            query = urllib.parse.urlencode(
                {
                    "ParentId": str(series_id or "").strip(),
                    "IncludeItemTypes": "Episode",
                    "Recursive": "true",
                    "Fields": "Name,SortName,OriginalTitle,Path,SeriesId,ParentIndexNumber,IndexNumber,LocationType,IsMissing,PremiereDate",
                    "StartIndex": str(start),
                    "Limit": str(page_size),
                }
            )
            self._increment_metric(metrics, "embyQueryCount")
            payload = self.emby_fetcher(f"/Items?{query}")
            rows = payload.get("Items") if isinstance(payload, dict) else payload
            page = [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []
            output.extend(page)
            total = payload.get("TotalRecordCount") if isinstance(payload, dict) else None
            if isinstance(total, int) and len(output) >= total:
                break
            if len(page) < page_size:
                break
            start += page_size
        return output

    @staticmethod
    def _normalize_episode_item(row: dict[str, Any]) -> dict[str, Any]:
        return {
            "season": MediaIdentityService._coerce_int(row.get("ParentIndexNumber")),
            "episode": MediaIdentityService._coerce_int(row.get("IndexNumber")),
            "name": str(row.get("Name") or "").strip(),
            "sortName": str(row.get("SortName") or "").strip(),
            "originalTitle": str(row.get("OriginalTitle") or "").strip(),
            "path": str(row.get("Path") or "").strip(),
            "locationType": str(row.get("LocationType") or "").strip(),
            "isMissing": bool(row.get("IsMissing")),
            "premiereDate": str(row.get("PremiereDate") or "").strip(),
        }

    @staticmethod
    def _coerce_int(value: Any) -> int:
        try:
            return int(value or 0)
        except Exception:
            return 0

    @staticmethod
    def _increment_metric(metrics: dict[str, int] | None, key: str) -> None:
        if isinstance(metrics, dict):
            metrics[key] = int(metrics.get(key) or 0) + 1

    @staticmethod
    def _detect_episode_numbering_mode(season_map: dict[int, set[int]]) -> str:
        blocks = sorted(
            ((season, min(values), max(values)) for season, values in season_map.items() if values),
            key=lambda row: (row[1], row[2], row[0]),
        )
        if len(blocks) >= 2 and blocks[0][1] == 1 and all(current[1] > previous[2] for previous, current in zip(blocks, blocks[1:])):
            return "global"
        if len(blocks) == 1 and blocks[0][1] > 1:
            return "global"
        return "seasonal"

    def _pick_local(self, rows: list[dict[str, Any]], *, title: str, preferred_type: str) -> dict[str, Any]:
        target = self.normalize_title(title)
        candidates = [row for row in rows if isinstance(row, dict)]
        candidates.sort(
            key=lambda row: (
                2 if self.normalize_title(str(row.get("Name") or row.get("SeriesName") or "")) == target else 1,
                1 if self._type_matches(str(row.get("Type") or ""), preferred_type) else 0,
                1 if str(row.get("Type") or "").lower() == "series" else 0,
            ),
            reverse=True,
        )
        return candidates[0] if candidates else {}

    def _pick_fallback_emby_item(
        self,
        rows: list[dict[str, Any]],
        *,
        title: str,
        year: str,
        tmdb_id: str,
        preferred_type: str,
    ) -> dict[str, Any]:
        normalized_title = self.normalize_title(title)
        scored: list[tuple[int, dict[str, Any]]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            row_title = self.normalize_title(str(row.get("Name") or row.get("SeriesName") or ""))
            row_tmdb = self._provider_tmdb_id(row)
            row_year = str(row.get("ProductionYear") or "").strip()
            score = 0
            if normalized_title and row_title == normalized_title:
                score += 120
            elif normalized_title and row_title and (normalized_title in row_title or row_title in normalized_title):
                score += 70
            if year and row_year == year:
                score += 25
            if self._type_matches(str(row.get("Type") or ""), preferred_type):
                score += 20
            if str(row.get("Type") or "").strip().lower() == "series":
                score += 15
            if tmdb_id and row_tmdb == tmdb_id:
                score += 40
            scored.append((score, row))
        scored.sort(key=lambda item: item[0], reverse=True)
        return scored[0][1] if scored and scored[0][0] >= 55 else {}

    @staticmethod
    def _is_ambiguous(candidates: list[dict[str, Any]]) -> bool:
        if len(candidates) < 2:
            return False
        first, second = candidates[0], candidates[1]
        return int(first.get("score") or 0) == int(second.get("score") or 0) and (
            first.get("year") != second.get("year") or first.get("type") != second.get("type")
        )

    @staticmethod
    def _provider_tmdb_id(item: dict[str, Any]) -> str:
        providers = item.get("ProviderIds") if isinstance(item.get("ProviderIds"), dict) else {}
        return str(providers.get("Tmdb") or providers.get("tmdb") or "").strip()

    @staticmethod
    def normalize_title(value: str) -> str:
        return re.sub(r"[\s·._:：,，。!！?？'\"《》「」“”\-—–]+", "", str(value or "").strip().lower())

    @staticmethod
    def _local_type(value: str) -> str:
        lowered = str(value or "").strip().lower()
        if lowered in {"series", "episode", "tv"}:
            return "series"
        if lowered in {"movie", "film"}:
            return "movie"
        return ""

    @classmethod
    def _tmdb_type(cls, value: str) -> str:
        local = cls._local_type(value)
        return "tv" if local == "series" else "movie" if local == "movie" else ""

    @classmethod
    def _type_matches(cls, local_type: str, desired_type: str) -> bool:
        desired = cls._local_type(desired_type)
        return not desired or cls._local_type(local_type) == desired

    @staticmethod
    def _cache_key(tmdb_id: str, media_type: str) -> str:
        safe_id = str(tmdb_id or "").strip()
        return f"{MediaIdentityService._local_type(media_type)}:{safe_id}" if safe_id else ""

    def _remember(self, identity: dict[str, Any], item: dict[str, Any]) -> None:
        key = self._cache_key(str(identity.get("tmdbId") or ""), str(identity.get("type") or ""))
        if not key:
            return
        with self._lock:
            self._cache[key] = {
                "tmdbId": identity.get("tmdbId"),
                "embyId": str(item.get("Id") or "").strip(),
                "title": identity.get("title"),
                "year": identity.get("year"),
                "type": identity.get("type"),
                "updatedAt": datetime.now().isoformat(timespec="seconds"),
            }
            self._save_cache()

    def _load_cache(self) -> dict[str, dict[str, Any]]:
        if not self.cache_path or not self.cache_path.exists():
            return {}
        try:
            payload = json.loads(self.cache_path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        rows = payload.get("items") if isinstance(payload, dict) else None
        return rows if isinstance(rows, dict) else {}

    def _save_cache(self) -> None:
        if not self.cache_path:
            return
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        temp = self.cache_path.with_suffix(f"{self.cache_path.suffix}.tmp")
        temp.write_text(json.dumps({"version": 1, "items": self._cache}, ensure_ascii=False, indent=2), encoding="utf-8")
        temp.replace(self.cache_path)
