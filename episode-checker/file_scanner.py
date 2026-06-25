"""Recursive video scanner and Chinese episode filename parser."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class ParsedEpisode:
    """A parsed episode identity from one filename."""

    season: int
    episode: int
    rule: str


@dataclass(frozen=True)
class ScanRecord:
    """Verbose scan record for one filesystem item."""

    path: Path
    status: str
    season: int | None = None
    episode: int | None = None
    rule: str = ""
    reason: str = ""


@dataclass(frozen=True)
class ScanResult:
    """All recognized local episodes and per-file scan records."""

    episodes_by_season: dict[int, set[int]]
    records: list[ScanRecord]


VIDEO_EXTENSIONS = {".mkv", ".mp4", ".avi", ".rmvb", ".ts"}
YEAR_LIKE = re.compile(r"^(19|20)\d{2}$")
QUALITY_NUMBERS = {"480", "720", "1080", "2160", "4320"}


def normalize_extensions(extensions: Iterable[str]) -> set[str]:
    """Return lowercase extensions with a leading dot."""

    normalized: set[str] = set()
    for extension in extensions:
        value = extension.strip().lower()
        if not value:
            continue
        normalized.add(value if value.startswith(".") else f".{value}")
    return normalized or set(VIDEO_EXTENSIONS)


def is_video_file(path: Path, extensions: Iterable[str]) -> bool:
    """Check whether a path looks like a supported video file."""

    return path.is_file() and path.suffix.lower() in normalize_extensions(extensions)


def _strip_common_prefixes(stem: str) -> str:
    """Remove common subtitle group or quality tags before parsing."""

    value = stem.strip()
    # Repeatedly remove leading tags such as [SUB], [1080p], 【字幕组】.
    while True:
        new_value = re.sub(r"^\s*(\[[^\]]+\]|【[^】]+】|\([^)]*\))\s*", "", value).strip()
        if new_value == value:
            return value
        value = new_value


def _valid_episode_number(raw: str) -> bool:
    """Avoid treating years and quality marks as episode numbers."""

    if not raw.isdigit():
        return False
    if raw in QUALITY_NUMBERS or YEAR_LIKE.match(raw):
        return False
    value = int(raw)
    return 0 < value <= 9999


def parse_episode_from_name(name: str) -> ParsedEpisode | None:
    """Parse a video filename into season and episode numbers.

    Supported examples:
    - 仙逆 - S01E01.mkv
    - 仙逆.E01.mkv
    - 仙逆 第01集.mp4
    - [SUB]仙逆 01.mkv
    - 仙逆_01.mkv
    """

    stem = _strip_common_prefixes(Path(name).stem)

    patterns: list[tuple[str, re.Pattern[str]]] = [
        (
            "SxxEyy",
            re.compile(r"(?i)(?:^|[^A-Z0-9])S(?P<season>\d{1,2})\s*[-_. ]?\s*E(?P<episode>\d{1,4})(?!\d)"),
        ),
        (
            "第x季第y集",
            re.compile(r"第\s*(?P<season>\d{1,2})\s*季.*?第\s*(?P<episode>\d{1,4})\s*集"),
        ),
        (
            "Exx",
            re.compile(r"(?i)(?:^|[^A-Z0-9])E(?P<episode>\d{1,4})(?!\d)"),
        ),
        (
            "第x集",
            re.compile(r"第\s*(?P<episode>\d{1,4})\s*集"),
        ),
    ]

    for rule, pattern in patterns:
        match = pattern.search(stem)
        if not match:
            continue
        episode = match.group("episode")
        if not _valid_episode_number(episode):
            continue
        season = match.groupdict().get("season") or "1"
        return ParsedEpisode(season=int(season), episode=int(episode), rule=rule)

    # Last resort: common Chinese anime filenames often end with a bare number.
    tokens = [token for token in re.split(r"[\s._\-]+", stem) if token]
    for token in reversed(tokens):
        token_match = re.fullmatch(r"\d{1,4}", token)
        if token_match and _valid_episode_number(token):
            return ParsedEpisode(season=1, episode=int(token), rule="裸数字")
    return None


def scan_directory(
    directory: Path,
    extensions: Iterable[str],
    exclude_keywords: Iterable[str],
) -> ScanResult:
    """Recursively scan a directory and parse all supported video files."""

    normalized_extensions = normalize_extensions(extensions)
    keywords = tuple(keyword for keyword in exclude_keywords if keyword)
    episodes_by_season: dict[int, set[int]] = {}
    records: list[ScanRecord] = []

    for path in sorted(directory.rglob("*")):
        if not path.is_file():
            continue
        if path.suffix.lower() not in normalized_extensions:
            continue

        name = path.name
        matched_keyword = next((keyword for keyword in keywords if keyword.lower() in name.lower()), None)
        if matched_keyword:
            records.append(ScanRecord(path=path, status="skipped", reason=f"命中排除关键词：{matched_keyword}"))
            continue

        parsed = parse_episode_from_name(name)
        if not parsed:
            records.append(ScanRecord(path=path, status="unparsed", reason="无法识别季号/集号"))
            continue

        episodes_by_season.setdefault(parsed.season, set()).add(parsed.episode)
        records.append(
            ScanRecord(
                path=path,
                status="parsed",
                season=parsed.season,
                episode=parsed.episode,
                rule=parsed.rule,
            )
        )

    return ScanResult(episodes_by_season=episodes_by_season, records=records)
