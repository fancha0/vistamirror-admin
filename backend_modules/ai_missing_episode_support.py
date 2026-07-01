from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any


def is_missing_episode_meta_question(question: str) -> bool:
    text = str(question or "").strip()
    if not text:
        return False
    if not re.search(r"缺失|缺少|漏掉|漏集|缺哪|缺.*集", text):
        return False
    return bool(
        re.search(r"(?:缺失集|缺集|缺少的?集|漏集).{0,8}(?:的)?(?:方式|流程|逻辑|原理|方法|规则|说明|教程|文档)", text)
        or re.search(r"(?:怎么|如何|怎样).{0,12}(?:查|查询|判断|识别|计算|统计).{0,12}(?:缺失集|缺集|缺少的?集|漏集)", text)
        or re.search(r"(?:查询|查看|识别|判断)(?:媒体|影视|作品|剧集)?缺失集(?:的)?(?:方式|流程|逻辑|原理|方法|规则|说明)", text)
        or re.search(r"(?:缺失集|缺集).{0,8}(?:怎么|如何|怎样|为啥|为什么)", text)
    )


def parse_missing_episode_request(question: str) -> dict[str, str]:
    def _pack(mode: str, title: str = "", year: str = "") -> dict[str, str]:
        payload = {"mode": mode, "title": str(title or "").strip()}
        if str(year or "").strip():
            payload["year"] = str(year or "").strip()
        return payload

    def _split_title_year(value: str) -> tuple[str, str]:
        text_value = str(value or "").strip()
        if not text_value:
            return "", ""
        trailing = re.search(r"^(?P<title>.*?)(?:[\s(（\[]+)(?P<year>(?:19|20)\d{2})(?:[\s)）\]]*)$", text_value)
        if trailing:
            return str(trailing.group("title") or "").strip(), str(trailing.group("year") or "").strip()
        bare = re.search(r"^(?P<title>.*?)(?:\s+)(?P<year>(?:19|20)\d{2})$", text_value)
        if bare:
            return str(bare.group("title") or "").strip(), str(bare.group("year") or "").strip()
        return text_value, ""

    text = str(question or "").strip()
    if not text:
        return {}
    if not re.search(r"缺失|缺少|漏掉|漏集|缺哪|缺.*集", text):
        return {}
    if is_missing_episode_meta_question(text):
        return {}
    if re.search(r"^(?:帮我|请|看一下|查一下|查看一下|继续查)?(?:它|这个|这部|那个|那部|缺失的?集)", text):
        return _pack("context")
    quoted = re.search(r"[《「“\"](?P<title>[^》」”\"]{1,80})[》」”\"]\s*(?:[\(（\[]?(?P<year>(?:19|20)\d{2})[\)）\]]?)?", text)
    if quoted:
        title, fallback_year = _split_title_year(str(quoted.group("title") or ""))
        return _pack("title", title, str(quoted.group("year") or fallback_year))
    direct = re.search(
        r"^(?:查看一下|看一下|查一下|帮我看下|帮我查下|看看|查查)?\s*(?P<title>.+?)\s*(?:的)?"
        r"(?:缺失|缺少|漏掉|漏|缺哪)(?:的)?集(?:[吧吗呢呀啊啦！!？?。.\s]*)$",
        text,
    )
    if direct:
        title, year = _split_title_year(str(direct.group("title") or ""))
        return _pack("title", title, year)
    return {}


def compress_episode_numbers(values: list[int]) -> str:
    numbers = sorted({int(value) for value in values if int(value) > 0})
    if not numbers:
        return "无"
    ranges: list[str] = []
    start = previous = numbers[0]
    for number in numbers[1:]:
        if number == previous + 1:
            previous = number
            continue
        ranges.append(f"E{start:02d}" if start == previous else f"E{start:02d}-E{previous:02d}")
        start = previous = number
    ranges.append(f"E{start:02d}" if start == previous else f"E{start:02d}-E{previous:02d}")
    return "、".join(ranges)


def compress_plain_episode_numbers(values: list[int]) -> str:
    numbers = sorted({int(value) for value in values if int(value) > 0})
    if not numbers:
        return "无"
    ranges: list[str] = []
    start = previous = numbers[0]
    for number in numbers[1:]:
        if number == previous + 1:
            previous = number
            continue
        ranges.append(str(start) if start == previous else f"{start}-{previous}")
        start = previous = number
    ranges.append(str(start) if start == previous else f"{start}-{previous}")
    return "、".join(ranges)


def format_episode_labels(labels: list[str]) -> str:
    rows = [str(value or "").strip() for value in labels if str(value or "").strip()]
    if not rows:
        return "无"
    global_values: list[int] = []
    seasonal: dict[int, list[int]] = {}
    for value in rows:
        global_match = re.fullmatch(r"E(\d+)", value, flags=re.IGNORECASE)
        season_match = re.fullmatch(r"S(\d+)E(\d+)", value, flags=re.IGNORECASE)
        if global_match:
            global_values.append(int(global_match.group(1)))
        elif season_match:
            seasonal.setdefault(int(season_match.group(1)), []).append(int(season_match.group(2)))
    if global_values and not seasonal:
        return compress_episode_numbers(global_values)
    if seasonal and not global_values:
        parts: list[str] = []
        for season in sorted(seasonal):
            parts.append(f"S{season:02d}{compress_episode_numbers(seasonal[season])}")
        return "、".join(parts)
    return "、".join(rows)


def sample_text(sample: dict[str, Any]) -> str:
    title = str(sample.get("name") or sample.get("title") or "").strip()
    path = str(sample.get("path") or "").strip()
    raw_season = str(sample.get("rawSeason") or sample.get("season") or "").strip()
    raw_episode = str(sample.get("rawEpisode") or sample.get("episode") or "").strip()
    prefix = f"S{raw_season}E{raw_episode}" if raw_season and raw_episode else f"E{raw_episode}" if raw_episode else "-"
    if title and path:
        return f"{prefix} | {title} | {path}"
    if title:
        return f"{prefix} | {title}"
    if path:
        return f"{prefix} | {path}"
    return prefix


@dataclass
class MissingEpisodeSeason:
    season_number: int
    existing_episodes: list[int] = field(default_factory=list)
    total_episodes: int = 0
    missing_episodes: list[int] = field(default_factory=list)
    aired_episodes: list[int] = field(default_factory=list)
    future_episodes: list[int] = field(default_factory=list)
    unknown_episodes: list[int] = field(default_factory=list)

    def summary_line(self, *, reliable: bool, identity_note: str = "", mapping_warning: str = "") -> str:
        season_label = f"S{int(self.season_number):02d}"
        mapped_count = len(self.existing_episodes)
        aired_count = len(self.aired_episodes)
        if reliable:
            if self.missing_episodes:
                return (
                    f"- {season_label}：已映射 {mapped_count} / 已播出 {aired_count} / 总 {int(self.total_episodes)}"
                    f"，缺 {compress_episode_numbers(self.missing_episodes)}"
                )
            return f"- {season_label}：已映射 {mapped_count} / 已播出 {aired_count} / 总 {int(self.total_episodes)}，完整"
        reference = compress_episode_numbers(self.missing_episodes) if self.missing_episodes else "无"
        if identity_note and not mapping_warning:
            return f"- {season_label}：已映射 {mapped_count} / 已播出 {aired_count} / 总 {int(self.total_episodes)}，身份已切换（参考缺 {reference}）"
        if mapping_warning:
            return f"- {season_label}：已映射 {mapped_count} / 已播出 {aired_count} / 总 {int(self.total_episodes)}，映射异常（参考缺 {reference}）"
        return f"- {season_label}：已映射 {mapped_count} / 已播出 {aired_count} / 总 {int(self.total_episodes)}，需人工确认（参考缺 {reference}）"

    def display_row(self, *, reliable: bool) -> dict[str, str]:
        existing_text = compress_plain_episode_numbers(self.existing_episodes) if self.existing_episodes else "0"
        if reliable:
            if self.missing_episodes:
                missing_text = compress_plain_episode_numbers(self.missing_episodes)
                if not self.existing_episodes:
                    status_text = "❌ 整季缺失"
                else:
                    status_text = "⚠️ 部分缺失"
            else:
                missing_text = "无"
                status_text = "✅ 完整"
        else:
            missing_text = compress_plain_episode_numbers(self.missing_episodes) if self.missing_episodes else "待确认"
            status_text = "⚠️ 待确认"
        return {
            "seasonLabel": f"Season {int(self.season_number)}",
            "existingText": existing_text,
            "totalText": str(int(self.total_episodes or 0)),
            "missingText": missing_text,
            "statusText": status_text,
        }


@dataclass
class MissingEpisodeResult:
    title: str
    year: str
    tmdb_id: str
    media_type: str
    server: str
    seasons: dict[int, MissingEpisodeSeason]
    existing_episodes: int
    mapped_episodes: int
    unmapped_episodes: int
    total_episodes: int
    missing_episodes: list[str]
    missing_seasons: list[int]
    mapping_confidence: str
    mapping_warning: str
    emby_provider_tmdb_id: str = ""
    aired_episodes: int = 0
    registered_episodes: int = 0
    last_aired_date: str = ""
    reference_missing_episodes: list[str] = field(default_factory=list)
    reference_missing_seasons: list[int] = field(default_factory=list)
    unmapped_samples: list[dict[str, Any]] = field(default_factory=list)
    extra_samples: list[dict[str, Any]] = field(default_factory=list)
    library_exists: bool = True
    data_query_count: int = 0
    identity_status: str = "ok"
    target_tmdb_id: str = ""
    emby_tmdb_id: str = ""
    target_title: str = ""
    emby_title: str = ""
    target_year: str = ""
    emby_year: str = ""
    confidence_reason: str = ""
    candidates: list[dict[str, Any]] = field(default_factory=list)
    emby_item_id: str = ""
    identity_note: str = ""
    missing_source: str = ""
    candidate_count: int = 0
    special_rows: list[dict[str, str]] = field(default_factory=list)

    @property
    def reliable(self) -> bool:
        return str(self.mapping_confidence or "").strip().lower() == "high" and not str(self.mapping_warning or "").strip()

    def _summary_text(self) -> str:
        if not self.reliable:
            return "当前编号映射异常，以下缺失仅供参考，不建议直接搜索。"
        if not self.missing_episodes and not self.missing_seasons:
            total_count = int(self.aired_episodes or self.registered_episodes or self.total_episodes or self.existing_episodes)
            return f"全集 {total_count} 集均已入库，无缺失。"
        missing_count = len(self.missing_episodes)
        missing_text = format_episode_labels(self.missing_episodes) if self.missing_episodes else "无"
        affected = [f"Season {season}" for season in sorted(self.seasons) if self.seasons[season].missing_episodes]
        if not affected and self.missing_seasons:
            affected = [f"Season {season}" for season in sorted(self.missing_seasons)]
        if not affected:
            return f"共缺失 {missing_count} 集（{missing_text}）。"
        if len(affected) == 1:
            return f"共缺失 {missing_count} 集（{missing_text}），{affected[0]} 尚不完整。"
        return f"共缺失 {missing_count} 集（{missing_text}），{'、'.join(affected)} 尚不完整。"

    def _detail_lines(self) -> list[str]:
        if self.reliable:
            return []
        reason_parts = [value for value in [self.mapping_warning, self.identity_note] if str(value or "").strip()]
        lines: list[str] = []
        if reason_parts:
            lines.append(f"原因：{' / '.join(str(value).strip() for value in reason_parts)}")
        reference_labels = self.reference_missing_episodes or self.missing_episodes
        if reference_labels:
            lines.append(f"参考缺失：{format_episode_labels(reference_labels)}")
        lines.append(f"TMDB 已播出：{int(self.aired_episodes or 0)} 集")
        lines.append(f"Emby 实际找到：{int(self.existing_episodes or 0)} 集")
        lines.append(f"成功映射：{int(self.mapped_episodes or 0)} 集")
        lines.append(f"未映射：{int(self.unmapped_episodes or 0)} 集")
        if self.last_aired_date:
            lines.append(f"最后播出：{self.last_aired_date}")
        if self.missing_source:
            source_text = "Emby 自带缺失数据" if self.missing_source == "emby_missing" else "TMDB 已播集对比"
            lines.append(f"缺集来源：{source_text}")
        return lines

    def to_report_dict(self, *, search_count: int = 0, data_query_count: int | None = None) -> dict[str, Any]:
        final_data_query_count = self.data_query_count if data_query_count is None else int(data_query_count or 0)
        missing_labels = list(self.missing_episodes) if self.reliable else []
        reference_labels = list(self.reference_missing_episodes if not self.reliable else self.missing_episodes)
        season_lines = [
            self.seasons[season].summary_line(
                reliable=self.reliable,
                identity_note=self.identity_note,
                mapping_warning=self.mapping_warning,
            )
            for season in sorted(self.seasons)
        ]
        missing_text = format_episode_labels(missing_labels) if missing_labels else ("无" if self.reliable else "无法可靠判断")
        reference_text = format_episode_labels(reference_labels) if reference_labels else ""
        future_labels = [
            f"S{season:02d}E{episode:02d}"
            for season in sorted(self.seasons)
            for episode in self.seasons[season].future_episodes
        ]
        unknown_labels = [
            f"S{season:02d}E{episode:02d}"
            for season in sorted(self.seasons)
            for episode in self.seasons[season].unknown_episodes
        ]
        season_rows = list(self.special_rows)
        season_rows.extend(self.seasons[season].display_row(reliable=self.reliable) for season in sorted(self.seasons))
        return {
            "title": self.title,
            "year": self.year,
            "tmdbId": self.tmdb_id,
            "mediaType": self.media_type,
            "server": self.server,
            "registeredCount": self.registered_episodes or self.total_episodes,
            "airedCount": self.aired_episodes,
            "localCount": self.existing_episodes,
            "mappedCount": self.mapped_episodes,
            "unmappedCount": self.unmapped_episodes,
            "missingText": missing_text,
            "missingLabels": missing_labels,
            "missingSeasons": list(self.missing_seasons) if self.reliable else [],
            "referenceMissingText": reference_text,
            "referenceMissingLabels": reference_labels if not self.reliable else [],
            "referenceMissingSeasons": list(self.reference_missing_seasons),
            "futureText": format_episode_labels(future_labels) if future_labels else "",
            "futureLabels": future_labels,
            "unknownText": format_episode_labels(unknown_labels) if unknown_labels else "",
            "unknownLabels": unknown_labels,
            "mappingConfidence": self.mapping_confidence,
            "mappingWarning": self.mapping_warning,
            "lastAiredDate": self.last_aired_date,
            "searchCount": max(0, int(search_count or 0)),
            "dataQueryCount": max(0, int(final_data_query_count or 0)),
            "seasonSummaryLines": season_lines,
            "seasonRows": season_rows,
            "summaryText": self._summary_text(),
            "detailLines": self._detail_lines(),
            "unmappedSamples": list(self.unmapped_samples[:5]),
            "extraSamples": list(self.extra_samples[:5]),
            "isReliable": self.reliable,
            "embyProviderTmdbId": self.emby_provider_tmdb_id,
            "identityStatus": self.identity_status,
            "targetTmdbId": self.target_tmdb_id or self.tmdb_id,
            "embyTmdbId": self.emby_tmdb_id or self.emby_provider_tmdb_id,
            "targetTitle": self.target_title or self.title,
            "embyTitle": self.emby_title or self.title,
            "targetYear": self.target_year or self.year,
            "embyYear": self.emby_year or self.year,
            "confidenceReason": self.confidence_reason,
            "candidates": list(self.candidates[:5]),
            "embyItemId": self.emby_item_id,
            "identityNote": self.identity_note,
            "missingSource": self.missing_source,
            "candidateCount": int(self.candidate_count or len(self.candidates)),
        }
