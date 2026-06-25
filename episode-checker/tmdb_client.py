"""Small TMDB client for TV episode metadata."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


class TMDBError(RuntimeError):
    """Raised when TMDB cannot return a usable response."""


@dataclass(frozen=True)
class TMDBSearchResult:
    """One TV search result from TMDB."""

    id: int
    name: str
    original_name: str
    first_air_date: str
    overview: str
    vote_average: float

    @property
    def year(self) -> str:
        return self.first_air_date[:4] if self.first_air_date else ""


@dataclass(frozen=True)
class TMDBSeason:
    """Season metadata from /tv/{id}."""

    number: int
    name: str
    episode_count: int
    air_date: str | None


@dataclass(frozen=True)
class TMDBSeriesDetail:
    """Series details needed for missing episode comparison."""

    id: int
    name: str
    original_name: str
    overview: str
    total_episodes: int
    seasons: list[TMDBSeason]


class TMDBClient:
    """A minimal TMDB wrapper supporting v3 api_key and v4 bearer auth."""

    BASE_URL = "https://api.themoviedb.org/3"

    def __init__(
        self,
        credential: str,
        *,
        auth_mode: str = "api_key",
        language: str = "zh-CN",
        region: str = "CN",
        timeout: int = 15,
    ) -> None:
        if not credential:
            raise TMDBError("缺少 TMDB 凭据。")
        if auth_mode not in {"api_key", "bearer"}:
            raise TMDBError(f"不支持的 TMDB 鉴权方式：{auth_mode}")
        self.credential = credential
        self.auth_mode = auth_mode
        self.language = language
        self.region = region
        self.timeout = timeout

    def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        query = dict(params or {})
        headers: dict[str, str] = {"accept": "application/json"}
        if self.auth_mode == "api_key":
            query["api_key"] = self.credential
        else:
            headers["Authorization"] = f"Bearer {self.credential}"
        query.setdefault("language", self.language)
        try:
            import requests
        except ImportError as exc:
            raise TMDBError("缺少依赖 requests，请先执行：pip install -r requirements.txt") from exc

        try:
            response = requests.get(f"{self.BASE_URL}{path}", params=query, headers=headers, timeout=self.timeout)
        except requests.RequestException as exc:
            raise TMDBError(f"TMDB 网络请求失败：{exc}") from exc

        if response.status_code == 401:
            if self.auth_mode == "bearer":
                raise TMDBError("TMDB Bearer Token 无效或未授权。")
            raise TMDBError("TMDB API Key 无效或未授权。")
        if response.status_code >= 400:
            raise TMDBError(f"TMDB 请求失败：HTTP {response.status_code} {response.text[:120]}")
        try:
            return response.json()
        except ValueError as exc:
            raise TMDBError("TMDB 返回的不是有效 JSON。") from exc

    def search_tv(self, name: str, page: int = 1) -> list[TMDBSearchResult]:
        """Search TV series by Chinese or original title."""

        params: dict[str, Any] = {"query": name, "page": page}
        if self.region:
            params["region"] = self.region
        data = self._get("/search/tv", params)
        results = data.get("results") or []
        parsed: list[TMDBSearchResult] = []
        for item in results:
            tmdb_id = item.get("id")
            title = item.get("name") or item.get("original_name")
            if not tmdb_id or not title:
                continue
            parsed.append(
                TMDBSearchResult(
                    id=int(tmdb_id),
                    name=str(title),
                    original_name=str(item.get("original_name") or ""),
                    first_air_date=str(item.get("first_air_date") or ""),
                    overview=str(item.get("overview") or ""),
                    vote_average=float(item.get("vote_average") or 0),
                )
            )
        if not parsed:
            raise TMDBError(f"TMDB 未找到剧集：{name}")
        return parsed

    def get_tv_detail(self, tmdb_id: int) -> TMDBSeriesDetail:
        """Fetch /tv/{id} and normalize season counts."""

        data = self._get(f"/tv/{tmdb_id}")
        seasons: list[TMDBSeason] = []
        for season in data.get("seasons") or []:
            number = season.get("season_number")
            if number is None:
                continue
            seasons.append(
                TMDBSeason(
                    number=int(number),
                    name=str(season.get("name") or f"S{int(number):02d}"),
                    episode_count=int(season.get("episode_count") or 0),
                    air_date=season.get("air_date"),
                )
            )
        return TMDBSeriesDetail(
            id=int(data["id"]),
            name=str(data.get("name") or data.get("original_name") or tmdb_id),
            original_name=str(data.get("original_name") or ""),
            overview=str(data.get("overview") or ""),
            total_episodes=int(data.get("number_of_episodes") or 0),
            seasons=seasons,
        )
