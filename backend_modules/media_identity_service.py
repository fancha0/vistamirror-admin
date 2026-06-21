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
            "seasonErrors": season_errors,
            "lastAiredDate": last_aired_date,
            "tmdbQueryCount": tmdb_query_count,
            "overview": str(detail.get("overview") or "").strip(),
        }

    def query_library_exists(self, identity: dict[str, Any]) -> dict[str, Any]:
        metrics = {"embyQueryCount": 0}
        item = self.find_emby_item(identity, metrics=metrics)
        if not item:
            return {"ok": True, "exists": False, "embyItem": {}, "seasonMap": {}, "specials": [], "duplicates": [], **metrics}
        item_id = str(item.get("Id") or "").strip()
        if self._local_type(str(item.get("Type") or identity.get("type") or "")) != "series":
            return {"ok": True, "exists": True, "embyItem": item, "seasonMap": {}, "specials": [], "duplicates": [], **metrics}
        episodes = self._fetch_emby_episodes(item_id, metrics=metrics)
        season_map: dict[int, set[int]] = {}
        specials: list[dict[str, Any]] = []
        duplicates: list[dict[str, int]] = []
        seen: set[tuple[int, int]] = set()
        for row in episodes:
            try:
                season = int(row.get("ParentIndexNumber") or 0)
                episode = int(row.get("IndexNumber") or 0)
            except Exception:
                continue
            if episode <= 0:
                continue
            if season <= 0:
                specials.append({"season": season, "episode": episode, "name": str(row.get("Name") or "")})
                continue
            key = (season, episode)
            if key in seen:
                duplicates.append({"season": season, "episode": episode})
                continue
            seen.add(key)
            season_map.setdefault(season, set()).add(episode)
        return {
            "ok": True,
            "exists": True,
            "embyItem": item,
            "seasonMap": season_map,
            "specials": specials,
            "duplicates": duplicates,
            "episodeRows": len(episodes),
            **metrics,
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
        media_type = str(identity.get("type") or "").strip()
        title = str(identity.get("title") or "").strip()
        year = str(identity.get("year") or "").strip()
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
                "Fields": "Name,Type,SeriesName,SeriesId,ProductionYear,PremiereDate,ProviderIds,Overview,CommunityRating",
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
                "Fields": "Name,Type,ProductionYear,PremiereDate,ProviderIds,Overview,CommunityRating",
                "Limit": "30",
            }
        )
        payload = self.emby_fetcher(f"/Items?{query}")
        rows = payload.get("Items") if isinstance(payload, dict) else payload
        return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []

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
                    "Fields": "Name,SeriesId,ParentIndexNumber,IndexNumber",
                    "StartIndex": str(start),
                    "Limit": str(page_size),
                }
            )
            self._increment_metric(metrics, "embyQueryCount")
            payload = self.emby_fetcher(f"/Shows/{safe_id}/Episodes?{query}")
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
