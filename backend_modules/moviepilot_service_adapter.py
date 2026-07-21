from __future__ import annotations

import json
import re
from typing import Any, Callable, Iterable
from urllib import error, parse, request


class MoviePilotServiceError(RuntimeError):
    """A safe, user-facing error raised by the MoviePilot HTTP adapter."""


class MoviePilotHttpError(MoviePilotServiceError):
    """HTTP failure with a status code for safe compatibility fallbacks."""

    def __init__(self, status_code: int) -> None:
        self.status_code = int(status_code)
        super().__init__(f"MoviePilot 返回 HTTP {self.status_code}。")


class MoviePilotServiceAdapter:
    """Small read-only adapter for MoviePilot's MCP tool gateway."""

    _READ_PREFIXES = (
        "get",
        "list",
        "query",
        "search",
        "find",
        "lookup",
        "status",
        "inspect",
        "show",
        "fetch",
        "read",
    )
    _WRITE_MARKERS = (
        "delete",
        "remove",
        "add",
        "create",
        "update",
        "edit",
        "transfer",
        "unsubscribe",
        "add_subscribe",
        "update_subscribe",
        "delete_subscribe",
        # MoviePilot's search_subscribe starts an automatic resource search.
        "search_subscribe",
        "start",
        "stop",
        "execute",
        "run",
        "删除",
        "移除",
        "添加",
        "创建",
        "更新",
        "编辑",
        "转存",
        "取消订阅",
        "搜索订阅",
    )

    def __init__(
        self,
        config: dict[str, Any],
        *,
        transport: Callable[[str, str, dict[str, str], dict[str, Any] | None, int], Any] | None = None,
    ) -> None:
        self._config = dict(config or {})
        self._transport = transport

    @property
    def api_base_url(self) -> str:
        base = str(self._config.get("baseUrl") or "").strip().rstrip("/")
        if base.endswith("/api/v1/mcp"):
            return base[: -len("/mcp")]
        if base.endswith("/api/v1"):
            return base
        return f"{base}/api/v1"

    def discover_tools(self) -> list[dict[str, Any]]:
        payload = self._request("GET", "/mcp/tools")
        rows = self._extract_rows(payload)
        tools: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            name = str(row.get("name") or row.get("toolName") or row.get("id") or "").strip()
            if not name:
                continue
            description = str(row.get("description") or row.get("title") or "").strip()
            tools.append(
                {
                    "name": name,
                    "description": description,
                    "readOnly": self.is_read_tool(name, description, row),
                    "inputSchema": self._tool_input_schema(row),
                }
            )
        return tools

    def get_recommendations(self, source: str, *, media_type: str = "all", page: int = 1, count: int = 30) -> dict[str, Any]:
        """Read one concrete MoviePilot recommendation source.

        MoviePilot exposes these lists as first-class REST endpoints under
        ``/recommend``.  Calling the endpoint directly is important: several
        MCP bridge versions accept a ``source`` argument but silently return
        TMDB trending for every value.  Older deployments can still use the
        generic MCP tool through the narrow 404 fallback.
        """
        source_name = str(source or "").strip()
        query = parse.urlencode({"page": max(1, int(page or 1)), "count": max(1, min(100, int(count or 30)))})
        try:
            return {"result": self._request("GET", f"/recommend/{source_name}?{query}"), "transport": "rest"}
        except MoviePilotHttpError as exc:
            if exc.status_code != 404:
                raise
        fallback = self.query_named_read_tool(
            "get_recommendations",
            {"source": source_name, "media_type": str(media_type or "all"), "page": max(1, int(page or 1))},
        )
        if not fallback.get("ok"):
            raise MoviePilotServiceError(str(fallback.get("message") or "MoviePilot 未返回探索内容。"))
        return {"result": fallback.get("result"), "transport": "mcp"}

    @classmethod
    def is_read_tool(
        cls,
        name: str,
        description: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> bool:
        """Accept read-only MCP tools without treating all subscriptions as writes."""
        normalized = str(name or "").strip().lower()
        if not normalized:
            return False
        if any(marker in normalized for marker in cls._WRITE_MARKERS):
            return False

        explicit_read_only = cls._read_only_hint(metadata)
        if explicit_read_only is not None:
            return explicit_read_only

        # Tool names are the reliable safety boundary. Descriptions are only
        # used for matching a requested domain after this classification.
        return normalized.startswith(cls._READ_PREFIXES)

    @staticmethod
    def _read_only_hint(metadata: dict[str, Any] | None) -> bool | None:
        if not isinstance(metadata, dict):
            return None
        annotations = metadata.get("annotations")
        candidates = (metadata, annotations) if isinstance(annotations, dict) else (metadata,)
        for source in candidates:
            for key in ("readOnly", "readonly", "read_only", "readOnlyHint"):
                value = source.get(key)
                if isinstance(value, bool):
                    return value
                if isinstance(value, str) and value.strip().lower() in {"true", "false"}:
                    return value.strip().lower() == "true"
        return None

    def capabilities(self) -> dict[str, Any]:
        tools = self.discover_tools()
        return {
            "ok": True,
            "toolCount": len(tools),
            "readToolCount": sum(1 for tool in tools if tool["readOnly"]),
            "tools": tools,
        }

    def query_first_read_tool(self, terms: Iterable[str]) -> dict[str, Any]:
        normalized_terms = tuple(str(term or "").strip().lower() for term in terms if str(term or "").strip())
        tools = self.discover_tools()
        for tool in tools:
            name = str(tool["name"])
            text = f"{name} {tool.get('description') or ''}".lower()
            if tool["readOnly"] and any(term in text for term in normalized_terms):
                result = self._call_read_tool(name)
                return {"ok": True, "tool": name, "result": result}
        available = [str(tool.get("name") or "") for tool in tools if tool.get("readOnly")]
        detail = "、".join(name for name in available[:8] if name)
        suffix = f" 当前已识别只读工具：{detail}。" if detail else ""
        return {
            "ok": False,
            "message": "MoviePilot 未发现可用于本次查询的只读工具，请确认已启用 MCP 并暴露订阅/任务查询工具。" + suffix,
        }

    def query_search_tool(self, query: str) -> dict[str, Any]:
        keyword = str(query or "").strip()
        if not keyword:
            return {"ok": False, "message": "请提供要在 MoviePilot 中搜索的影视名称。"}

        ranked = sorted(
            (
                (self._search_tool_score(tool), tool)
                for tool in self.discover_tools()
                if tool.get("readOnly")
            ),
            key=lambda item: item[0],
            reverse=True,
        )
        if not ranked or ranked[0][0] <= 0:
            return {
                "ok": False,
                "message": "MoviePilot 未发现可安全调用的媒体搜索工具，请确认已启用 MCP 并暴露搜索工具。",
            }

        _, tool = ranked[0]
        tool_name = str(tool.get("name") or "")
        try:
            result = self._call_read_tool(
                tool_name,
                self._build_search_arguments(tool.get("inputSchema"), keyword),
            )
        except MoviePilotServiceError as exc:
            return {"ok": False, "tool": tool_name, "message": str(exc)}
        return {"ok": True, "tool": tool_name, "query": keyword, "result": result}

    def query_named_read_tool(self, name: str, arguments: dict[str, Any] | None = None) -> dict[str, Any]:
        """Call one explicitly named, discovered read-only MCP tool."""
        requested = str(name or "").strip()
        tool = next((row for row in self.discover_tools() if row.get("name") == requested), None)
        if not tool or not tool.get("readOnly"):
            return {"ok": False, "message": "MoviePilot 未暴露可安全调用的探索推荐工具。"}
        try:
            result = self._call_read_tool(requested, dict(arguments or {}))
        except MoviePilotServiceError as exc:
            return {"ok": False, "tool": requested, "message": str(exc)}
        return {"ok": True, "tool": requested, "result": result}

    def invoke_named_tool(
        self,
        name: str,
        arguments: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Invoke any capability currently exposed by MoviePilot MCP."""
        requested = str(name or "").strip()
        tool = next((row for row in self.discover_tools() if row.get("name") == requested), None)
        if not tool:
            return {"ok": False, "message": "MoviePilot 未暴露该功能，请先刷新功能中心。"}
        is_read_only = bool(tool.get("readOnly"))
        try:
            result = self._call_read_tool(requested, dict(arguments or {}))
        except MoviePilotServiceError as exc:
            return {"ok": False, "tool": requested, "message": str(exc)}
        return {"ok": True, "tool": requested, "readOnly": is_read_only, "result": result}

    @classmethod
    def public_result(cls, payload: Any, *, max_depth: int = 7, max_items: int = 120) -> Any:
        """Keep useful MCP results while never reflecting credentials to the UI."""
        secret_markers = ("password", "passwd", "cookie", "token", "apikey", "api_key", "authorization", "secret", "rss", "passkey")

        def clean(value: Any, depth: int = 0) -> Any:
            if depth >= max_depth:
                return "…"
            if isinstance(value, dict):
                result: dict[str, Any] = {}
                for index, (key, child) in enumerate(value.items()):
                    if index >= max_items:
                        result["truncated"] = True
                        break
                    text_key = str(key)
                    if any(marker in text_key.lower() for marker in secret_markers):
                        result[text_key] = "[已隐藏]"
                    else:
                        result[text_key] = clean(child, depth + 1)
                return result
            if isinstance(value, (list, tuple)):
                rows = [clean(child, depth + 1) for child in value[:max_items]]
                return rows + (["…"] if len(value) > max_items else [])
            if isinstance(value, str):
                text = value[:12000]
                # Several MoviePilot tools wrap JSON in an MCP text/result
                # string. Parse it before returning so nested credentials do
                # not bypass the dict-key redaction above.
                if text[:1] in {"{", "["}:
                    try:
                        decoded = json.loads(text)
                    except (TypeError, ValueError):
                        decoded = None
                    if isinstance(decoded, (dict, list)):
                        return clean(decoded, depth + 1)
                text = re.sub(
                    r'(?i)("?(?:password|passwd|cookie|token|apikey|api_key|authorization|secret)"?\s*[:=]\s*["\']?)([^,\s"\'}\]]+)',
                    r"\1[已隐藏]",
                    text,
                )
                return text + ("…" if len(value) > 12000 else "")
            if value is None or isinstance(value, (bool, int, float)):
                return value
            return str(value)[:12000]

        return clean(payload)

    @classmethod
    def normalize_search_results(cls, payload: Any) -> list[dict[str, Any]]:
        """Turn MoviePilot MCP's varied search payloads into safe catalog cards."""
        rows = cls._search_rows_from_payload(payload)
        results: list[dict[str, Any]] = []
        seen: set[str] = set()
        for row in rows:
            item = cls._normalize_search_row(row)
            if not item:
                continue
            key = item["tmdbId"] or item["imdbId"] or f"{item['title']}|{item['year']}|{item['mediaType']}"
            if key in seen:
                continue
            seen.add(key)
            results.append(item)
        return results[:80]

    @classmethod
    def normalize_torrent_results(cls, payload: Any, *, tmdb_id: str = "") -> dict[str, Any]:
        """Normalize MoviePilot's cached ``get_search_results`` response."""
        def decode(value: Any) -> Any:
            if isinstance(value, str):
                try:
                    return json.loads(value)
                except (TypeError, ValueError):
                    return value
            return value

        def find_result(value: Any) -> dict[str, Any]:
            value = decode(value)
            if isinstance(value, dict):
                if isinstance(value.get("results"), list):
                    return value
                for key in ("result", "data", "content", "text"):
                    found = find_result(value.get(key)) if value.get(key) is not None else {}
                    if found:
                        return found
            if isinstance(value, list):
                for child in value:
                    found = find_result(child)
                    if found:
                        return found
            return {}

        source = find_result(payload)
        requested_id = str(tmdb_id or "").strip()
        items: list[dict[str, Any]] = []
        for index, row in enumerate(source.get("results") if isinstance(source.get("results"), list) else []):
            if not isinstance(row, dict):
                continue
            torrent = row.get("torrent_info") if isinstance(row.get("torrent_info"), dict) else {}
            media = row.get("media_info") if isinstance(row.get("media_info"), dict) else {}
            meta = row.get("meta_info") if isinstance(row.get("meta_info"), dict) else {}
            row_tmdb = cls._first_text(media, "tmdb_id", "tmdbId")
            if requested_id and row_tmdb and row_tmdb != requested_id:
                continue
            reference = cls._first_text(torrent, "torrent_url", "torrentUrl", "download_url", "downloadUrl")
            if not reference:
                continue
            items.append({
                "id": f"{reference}|{index}",
                "reference": reference,
                "title": cls._first_text(torrent, "title") or cls._first_text(media, "title"),
                "site": cls._first_text(torrent, "site_name", "site", "siteName"),
                "size": cls._first_text(torrent, "size"),
                "seeders": cls._first_text(torrent, "seeders"),
                "peers": cls._first_text(torrent, "peers", "leechers"),
                "freeState": cls._first_text(torrent, "volume_factor", "free_state", "freeState"),
                "freeUntil": cls._first_text(torrent, "freedate_diff", "free_until", "freeUntil"),
                "publishedAt": cls._first_text(torrent, "pubdate", "publish_time", "published_at"),
                "resolution": cls._first_text(meta, "resource_pix", "resolution"),
                "videoCodec": cls._first_text(meta, "video_encode", "videoCodec"),
                "edition": cls._first_text(meta, "edition"),
                "releaseGroup": cls._first_text(meta, "resource_team", "release_group", "releaseGroup"),
                "season": cls._first_text(media, "season") or cls._first_text(meta, "season_episode"),
                "pageUrl": cls._safe_url(cls._first_text(torrent, "page_url", "pageUrl", "details")),
            })
        filters = source.get("filter_options") if isinstance(source.get("filter_options"), dict) else {}
        return {
            "items": items,
            # MoviePilot's cached total covers the entire previous search.  Once
            # a caller asks for one TMDB item, that raw number can describe a
            # different title and must not be presented as its result count.
            "totalCount": len(items) if requested_id else int(source.get("total_count") or len(items) or 0),
            "page": int(source.get("page") or 1),
            "totalPages": int(source.get("total_pages") or 1),
            "filters": cls.public_result(filters),
        }

    @classmethod
    def normalize_media_detail(cls, payload: Any, fallback: dict[str, Any] | None = None) -> dict[str, Any]:
        """Normalize ``query_media_detail`` output without leaking provider payloads.

        MoviePilot versions return either a media object directly or a JSON text
        MCP content block.  Keeping this conversion here lets the browser use a
        stable, deliberately read-only detail model.
        """
        rows = cls._search_rows_from_payload(payload)
        row = dict(rows[0]) if rows else {}
        fallback = dict(fallback or {})
        base = cls._normalize_search_row(row) or {}
        for key, value in fallback.items():
            if value not in (None, "", [], {}) and base.get(key) in (None, "", [], {}):
                base[key] = value
        if not base.get("title"):
            base["title"] = cls._first_text(row, "title", "name") or str(fallback.get("title") or "")
        if not base.get("mediaType") or base.get("mediaType") == "other":
            base["mediaType"] = cls._normalize_media_type(cls._first_text(row, "media_type", "mediaType", "type")) or str(fallback.get("mediaType") or "other")
        base["tagline"] = cls._first_text(row, "tagline", "tag_line", "slogan")
        base["runtime"] = cls._first_text(row, "runtime", "run_time", "duration", "runtime_minutes")
        base["genres"] = cls._text_list(row.get("genres") or row.get("genre_names") or row.get("genre"))
        base["status"] = cls._first_text(row, "status", "release_status")
        base["releaseDate"] = cls._first_text(row, "release_date", "releaseDate", "premiere_date", "premiereDate", "first_air_date")
        base["digitalReleaseDate"] = cls._first_text(row, "digital_release_date", "digitalReleaseDate")
        base["originalLanguage"] = cls._first_text(row, "original_language", "originalLanguage", "language")
        base["countries"] = cls._text_list(row.get("production_countries") or row.get("countries") or row.get("country"))
        base["productionCompanies"] = cls._text_list(row.get("production_companies") or row.get("productionCompanies") or row.get("companies"))
        credits = row.get("credits") if isinstance(row.get("credits"), dict) else {}
        base["credits"] = {
            "directors": cls._text_list(row.get("directors") or row.get("director") or credits.get("directors")),
            "writers": cls._text_list(row.get("writers") or row.get("writer") or credits.get("writers")),
            "creators": cls._text_list(row.get("creators") or row.get("creator") or credits.get("creators")),
            "cast": cls._text_list(row.get("actors") or row.get("cast") or credits.get("cast")),
        }
        seasons = row.get("seasons") or row.get("season_list") or []
        base["seasons"] = [
            {
                "number": cls._first_text(season, "season_number", "seasonNumber", "number"),
                "name": cls._first_text(season, "name", "title"),
                "episodeCount": cls._first_text(season, "episode_count", "episodeCount", "episodes"),
            }
            for season in seasons if isinstance(season, dict)
        ][:30] if isinstance(seasons, list) else []
        return base

    @classmethod
    def normalize_tmdb_media_detail(cls, payload: Any, *, media_type: str = "") -> dict[str, Any]:
        """Return the presentation-safe fields used by the MoviePilot detail view.

        MoviePilot's MCP response is useful for its library state, but some
        versions intentionally return only a small media card.  TMDB is the
        richer metadata source for localized title, original title, overview,
        artwork and credits.  This method keeps the browser independent of
        TMDB's wire format.
        """
        row = payload if isinstance(payload, dict) else {}
        is_tv = str(media_type or "").lower() in {"tv", "anime", "series"}
        release_date = cls._first_text(row, "first_air_date", "release_date") if is_tv else cls._first_text(row, "release_date", "first_air_date")
        year_match = re.search(r"(?:19|20)\d{2}", release_date)
        vote_average = cls._first_text(row, "vote_average")
        try:
            rating = round(float(vote_average), 1) if vote_average else None
        except (TypeError, ValueError):
            rating = None
        external_ids = row.get("external_ids") if isinstance(row.get("external_ids"), dict) else {}
        countries = cls._text_list(row.get("production_countries"))
        if not countries:
            countries = cls._text_list(row.get("origin_country"))
        credits = row.get("credits") if isinstance(row.get("credits"), dict) else {}
        crew = credits.get("crew") if isinstance(credits.get("crew"), list) else []
        cast = credits.get("cast") if isinstance(credits.get("cast"), list) else []

        def people(job: str) -> list[str]:
            return [cls._first_text(member, "name") for member in crew if isinstance(member, dict) and cls._first_text(member, "job") == job and cls._first_text(member, "name")]

        def image_url(value: str) -> str:
            url = cls._safe_url(value)
            if url:
                return url
            return f"https://image.tmdb.org/t/p/original{value}" if str(value or "").startswith("/") else ""

        seasons = row.get("seasons") if isinstance(row.get("seasons"), list) else []
        return {
            "title": cls._first_text(row, "name", "title"),
            "originalTitle": cls._first_text(row, "original_name", "original_title"),
            "year": year_match.group(0) if year_match else "",
            "mediaType": "tv" if is_tv else "movie",
            "rating": rating,
            "overview": cls._first_text(row, "overview"),
            "tagline": cls._first_text(row, "tagline"),
            "posterUrl": image_url(cls._first_text(row, "poster_path", "posterUrl")),
            "backdropUrl": image_url(cls._first_text(row, "backdrop_path", "backdropUrl")),
            "tmdbId": cls._first_text(row, "id", "tmdb_id", "tmdbId"),
            "imdbId": cls._first_text(external_ids, "imdb_id", "imdbId"),
            "runtime": cls._first_text(row, "episode_run_time", "runtime"),
            "genres": cls._text_list(row.get("genres")),
            "status": cls._first_text(row, "status"),
            "releaseDate": release_date,
            "originalLanguage": cls._first_text(row, "original_language"),
            "countries": countries,
            "productionCompanies": cls._text_list(row.get("production_companies")),
            "credits": {
                "directors": people("Director"),
                "writers": people("Writer") + people("Screenplay"),
                "creators": cls._text_list(row.get("created_by")),
                "cast": cls._text_list(cast),
            },
            "seasons": [
                {
                    "number": cls._first_text(season, "season_number"),
                    "name": cls._first_text(season, "name"),
                    "episodeCount": cls._first_text(season, "episode_count"),
                }
                for season in seasons if isinstance(season, dict)
            ][:30],
        }

    @staticmethod
    def merge_media_details(base: dict[str, Any], preferred: dict[str, Any]) -> dict[str, Any]:
        """Merge non-empty TMDB fields over MoviePilot fields, including credits."""
        merged = dict(base or {})
        for key, value in (preferred or {}).items():
            if key == "credits":
                current = dict(merged.get("credits") or {})
                for credit_key, credit_value in (value or {}).items():
                    if credit_value:
                        current[credit_key] = credit_value
                merged["credits"] = current
            elif value not in (None, "", [], {}):
                merged[key] = value
        return merged

    @staticmethod
    def _text_list(value: Any) -> list[str]:
        if isinstance(value, str):
            return [part.strip() for part in re.split(r"[,、/|]", value) if part.strip()]
        if isinstance(value, dict):
            value = [value]
        if not isinstance(value, list):
            return []
        output: list[str] = []
        for row in value:
            text = MoviePilotServiceAdapter._first_text(row, "name", "title", "person_name", "personName") if isinstance(row, dict) else str(row or "").strip()
            if text and text not in output:
                output.append(text)
        return output

    @classmethod
    def _search_rows_from_payload(cls, payload: Any) -> list[dict[str, Any]]:
        def decode(value: Any) -> Any:
            if not isinstance(value, str):
                return value
            text = value.strip()
            if not text:
                return value
            try:
                return json.loads(text)
            except (TypeError, ValueError):
                match = re.search(r"(\[[\s\S]*\]|\{[\s\S]*\})", text)
                if not match:
                    return value
                try:
                    return json.loads(match.group(1))
                except (TypeError, ValueError):
                    return value

        def visit(value: Any, output: list[dict[str, Any]]) -> None:
            value = decode(value)
            if isinstance(value, list):
                for child in value:
                    visit(child, output)
                return
            if not isinstance(value, dict):
                return
            if cls._looks_like_search_row(value):
                output.append(value)
                return
            for key in ("result", "data", "items", "results", "medias", "media", "content"):
                child = value.get(key)
                if child is not None:
                    visit(child, output)
            text = value.get("text")
            if text is not None:
                visit(text, output)

        output: list[dict[str, Any]] = []
        visit(payload, output)
        return output

    @staticmethod
    def _looks_like_search_row(row: dict[str, Any]) -> bool:
        return any(
            str(row.get(key) or "").strip()
            for key in ("title", "name", "media_name", "mediaName", "title_zh", "title_cn", "original_title")
        )

    @classmethod
    def _normalize_search_row(cls, row: dict[str, Any]) -> dict[str, Any] | None:
        title = cls._first_text(row, "title", "name", "media_name", "mediaName", "title_zh", "title_cn")
        if not title:
            return None
        providers = row.get("provider_ids") or row.get("providerIds") or row.get("providers") or {}
        providers = providers if isinstance(providers, dict) else {}
        media_type = cls._normalize_media_type(cls._first_text(row, "media_type", "mediaType", "type", "category", "kind"))
        year = cls._first_text(row, "year", "release_year", "releaseYear", "production_year", "productionYear", "date")
        year_match = re.search(r"(?:19|20)\d{2}", year)
        rating = cls._first_text(row, "rating", "vote_average", "voteAverage", "score", "tmdb_rating")
        try:
            rating_number = round(float(rating), 1) if rating else None
        except (TypeError, ValueError):
            rating_number = None
        return {
            "title": title,
            "originalTitle": cls._first_text(row, "original_title", "originalTitle", "title_original", "en_title"),
            "year": year_match.group(0) if year_match else "",
            "mediaType": media_type,
            "rating": rating_number,
            "overview": cls._first_text(row, "overview", "description", "summary", "plot", "intro"),
            "posterUrl": cls._safe_url(cls._first_text(row, "poster_path", "posterPath", "poster", "poster_url", "posterUrl", "image", "image_url", "imageUrl")),
            "backdropUrl": cls._safe_url(cls._first_text(row, "backdrop_path", "backdropPath", "backdrop", "backdrop_url", "backdropUrl")),
            "tmdbId": cls._first_text(row, "tmdb_id", "tmdbId") or cls._first_text(providers, "tmdb", "Tmdb", "TMDB"),
            "imdbId": cls._first_text(row, "imdb_id", "imdbId") or cls._first_text(providers, "imdb", "Imdb", "IMDB"),
            "externalUrl": cls._safe_url(cls._first_text(row, "url", "link", "web_url", "webUrl", "detail_url", "detailUrl")),
        }

    @staticmethod
    def _first_text(row: dict[str, Any], *keys: str) -> str:
        for key in keys:
            value = row.get(key)
            if value is not None and str(value).strip():
                return str(value).strip()
        return ""

    @staticmethod
    def _safe_url(value: str) -> str:
        url = str(value or "").strip()
        return url if re.match(r"^https?://", url, flags=re.IGNORECASE) else ""

    @staticmethod
    def _normalize_media_type(value: str) -> str:
        raw = str(value or "").strip().lower()
        if any(token in raw for token in ("anime", "animation", "动漫", "动画")):
            return "anime"
        if any(token in raw for token in ("tv", "series", "show", "电视剧", "剧集")):
            return "tv"
        if any(token in raw for token in ("movie", "film", "电影")):
            return "movie"
        return "other"

    def _call_read_tool(self, name: str, arguments: dict[str, Any] | None = None) -> Any:
        """Call a read-only MCP tool across MoviePilot REST payload variants.

        MoviePilot releases have used both ``name`` and ``tool_name`` in their
        REST gateway models. Only a validation failure may trigger the next
        shape, so this never retries a failed read for authentication, network,
        or tool-execution reasons.
        """
        call_arguments = dict(arguments or {})
        payloads = (
            {"name": name, "arguments": call_arguments},
            {"tool_name": name, "arguments": call_arguments},
            {"name": name, "params": call_arguments},
            {"tool_name": name, "params": call_arguments},
        )
        last_error: MoviePilotHttpError | None = None
        for payload in payloads:
            try:
                return self._request("POST", "/mcp/tools/call", payload)
            except MoviePilotHttpError as exc:
                if exc.status_code != 422:
                    raise
                last_error = exc
        raise MoviePilotServiceError(
            "MoviePilot MCP 工具调用格式不兼容（HTTP 422）。请升级 MoviePilot 后重试。"
        ) from last_error

    @classmethod
    def _search_tool_score(cls, tool: dict[str, Any]) -> int:
        name = str(tool.get("name") or "").lower()
        description = str(tool.get("description") or "").lower()
        text = f"{name} {description}"
        if any(marker in text for marker in cls._WRITE_MARKERS):
            return -1

        score = 0
        if name in {"search_media", "search_medias", "search_media_info"}:
            score += 100
        if any(token in text for token in ("search", "query", "find", "lookup", "搜索", "查找")):
            score += 20
        if any(token in text for token in ("media", "movie", "tv", "影视", "电影", "电视剧")):
            score += 10
        return score

    @staticmethod
    def _build_search_arguments(input_schema: Any, keyword: str) -> dict[str, Any]:
        schema = input_schema if isinstance(input_schema, dict) else {}
        properties = schema.get("properties") if isinstance(schema.get("properties"), dict) else {}
        for field_name in (
            "keyword",
            "query",
            "search",
            "title",
            "name",
            "media_name",
            "mediaName",
        ):
            if field_name in properties:
                return {field_name: keyword}
        if properties:
            return {str(next(iter(properties))): keyword}
        return {"query": keyword}

    @staticmethod
    def _tool_input_schema(row: dict[str, Any]) -> dict[str, Any]:
        for key in ("inputSchema", "input_schema", "parameters", "paramsSchema"):
            value = row.get(key)
            if isinstance(value, str):
                try:
                    value = json.loads(value)
                except (TypeError, ValueError):
                    continue
            if isinstance(value, dict):
                nested = value.get("inputSchema")
                return nested if isinstance(nested, dict) else value
        function = row.get("function")
        if isinstance(function, dict):
            return MoviePilotServiceAdapter._tool_input_schema(function)
        return {}

    def _request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> Any:
        base = self.api_base_url
        token = str(self._config.get("apiToken") or "").strip()
        if not base or not token:
            raise MoviePilotServiceError("MoviePilot 地址或 API Token 未配置。")
        url = f"{base}{path}"
        headers = {"X-API-KEY": token, "Accept": "application/json"}
        timeout = int(self._config.get("timeoutSeconds") or 12)
        if self._transport:
            return self._transport(method, url, headers, payload, timeout)
        body = None
        if payload is not None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = request.Request(url, data=body, headers=headers, method=method)
        try:
            with request.urlopen(req, timeout=timeout) as response:  # nosec B310 - user-managed private service endpoint
                raw = response.read().decode("utf-8", errors="replace")
        except error.HTTPError as exc:
            raise MoviePilotHttpError(exc.code) from exc
        except error.URLError as exc:
            raise MoviePilotServiceError("无法连接 MoviePilot，请检查地址、网络和反向代理。") from exc
        except OSError as exc:
            raise MoviePilotServiceError("MoviePilot 请求失败，请检查服务是否可访问。") from exc
        try:
            return json.loads(raw) if raw else {}
        except json.JSONDecodeError as exc:
            raise MoviePilotServiceError("MoviePilot 返回了非 JSON 响应。") from exc

    @staticmethod
    def _extract_rows(payload: Any) -> list[Any]:
        if isinstance(payload, list):
            return payload
        if not isinstance(payload, dict):
            return []
        for key in ("tools", "data", "items", "result"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
            if isinstance(value, dict):
                nested = value.get("tools") or value.get("items")
                if isinstance(nested, list):
                    return nested
        return []
