"""Core missing episode checker coordination and report formatting."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from config import CheckerConfig
from file_scanner import ScanRecord, ScanResult, scan_directory


class TMDBLikeClient(Protocol):
    """Protocol used to keep the checker easy to unit-test."""

    def search_tv(self, name: str, page: int = 1) -> list[Any]:
        ...

    def get_tv_detail(self, tmdb_id: int) -> Any:
        ...


@dataclass(frozen=True)
class SeasonCheckResult:
    """Comparison result for one season."""

    season: int
    expected_count: int
    existing: list[int]
    missing: list[int]

    @property
    def existing_count(self) -> int:
        return len(self.existing)

    @property
    def missing_count(self) -> int:
        return len(self.missing)


@dataclass(frozen=True)
class CheckResult:
    """Full check result ready for text or JSON rendering."""

    query_name: str
    series_name: str
    tmdb_id: int
    tmdb_year: str
    seasons: list[SeasonCheckResult]
    records: list[ScanRecord]
    chosen_candidate: Any

    @property
    def total_expected(self) -> int:
        return sum(season.expected_count for season in self.seasons)

    @property
    def total_existing(self) -> int:
        return sum(season.existing_count for season in self.seasons)

    @property
    def total_missing(self) -> int:
        return sum(season.missing_count for season in self.seasons)


class EpisodeChecker:
    """High-level checker: TMDB search -> local scan -> set comparison."""

    def __init__(
        self,
        config: CheckerConfig,
        tmdb_client: TMDBLikeClient,
    ) -> None:
        self.config = config
        self.tmdb_client = tmdb_client

    def run(self) -> CheckResult:
        """Run a complete missing episode check."""

        candidates = self.tmdb_client.search_tv(self.config.name)
        chosen = candidates[0]
        detail = self.tmdb_client.get_tv_detail(chosen.id)
        scan_result = scan_directory(
            self.config.directory,
            self.config.video_extensions,
            self.config.exclude_keywords,
        )
        seasons = self._compare(detail, scan_result)
        year = getattr(chosen, "year", "") or getattr(chosen, "first_air_date", "")[:4]
        return CheckResult(
            query_name=self.config.name,
            series_name=getattr(detail, "name", self.config.name),
            tmdb_id=int(getattr(detail, "id", chosen.id)),
            tmdb_year=year,
            seasons=seasons,
            records=scan_result.records,
            chosen_candidate=chosen,
        )

    def _compare(self, detail: Any, scan_result: ScanResult) -> list[SeasonCheckResult]:
        seasons: list[SeasonCheckResult] = []
        for season in getattr(detail, "seasons", []):
            season_number = int(getattr(season, "number"))
            if season_number == 0 and not self.config.include_season_0:
                continue
            if self.config.season is not None and season_number != self.config.season:
                continue

            expected_count = int(getattr(season, "episode_count", 0) or 0)
            expected = set(range(1, expected_count + 1))
            local = scan_result.episodes_by_season.get(season_number, set())
            existing = sorted(local & expected)
            missing = sorted(expected - local)
            seasons.append(
                SeasonCheckResult(
                    season=season_number,
                    expected_count=expected_count,
                    existing=existing,
                    missing=missing,
                )
            )
        return seasons


def _format_episode_ranges(values: list[int], chinese: bool = False) -> str:
    """Format episode numbers compactly while keeping Chinese-friendly labels."""

    if not values:
        return "无"
    if chinese:
        return "、".join(f"第{value}集" for value in values)
    return "、".join(f"E{value:02d}" for value in values)


def format_verbose_records(records: list[ScanRecord], base_dir: Path) -> str:
    """Render per-file parse details for --verbose."""

    lines = ["", "逐文件解析："]
    for record in records:
        try:
            label = str(record.path.relative_to(base_dir))
        except ValueError:
            label = str(record.path)
        if record.status == "parsed":
            lines.append(f"  [OK] {label} -> S{record.season:02d}E{record.episode:02d}（规则：{record.rule}）")
        elif record.status == "skipped":
            lines.append(f"  [跳过] {label} -> {record.reason}")
        else:
            lines.append(f"  [未识别] {label} -> {record.reason}")
    return "\n".join(lines)


def format_text_result(result: CheckResult, verbose: bool = False, base_dir: Path | None = None) -> str:
    """Render a human-readable Chinese report."""

    title = f"《{result.series_name}》缺失集数检测结果："
    if result.tmdb_year:
        title = f"《{result.series_name}》（{result.tmdb_year}）缺失集数检测结果："

    lines = [
        title,
        f"  TMDB ID：{result.tmdb_id}",
        f"  总集数：{result.total_expected}集",
        f"  已有：{result.total_existing}集",
        f"  缺失：{result.total_missing}集",
    ]

    if len(result.seasons) == 1:
        season = result.seasons[0]
        lines.append(
            f"  S{season.season:02d} 缺失：{_format_episode_ranges(season.missing, chinese=season.season == 1)}"
        )
    else:
        lines.append("")
        lines.append("按季详情：")
        for season in result.seasons:
            lines.append(
                f"  S{season.season:02d}：总 {season.expected_count} 集，"
                f"已有 {season.existing_count} 集，缺失 {season.missing_count} 集"
            )
            if season.missing:
                lines.append(f"    缺失：{_format_episode_ranges(season.missing)}")

    if verbose and base_dir is not None:
        lines.append(format_verbose_records(result.records, base_dir))
    return "\n".join(lines)


def result_to_json(result: CheckResult) -> str:
    """Render structured JSON output for integrations."""

    payload = {
        "queryName": result.query_name,
        "seriesName": result.series_name,
        "tmdbId": result.tmdb_id,
        "year": result.tmdb_year,
        "totalExpected": result.total_expected,
        "totalExisting": result.total_existing,
        "totalMissing": result.total_missing,
        "seasons": [
            {
                "season": season.season,
                "expectedCount": season.expected_count,
                "existing": season.existing,
                "missing": season.missing,
            }
            for season in result.seasons
        ],
    }
    return json.dumps(payload, ensure_ascii=False, indent=2)
