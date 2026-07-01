from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from .ai_missing_episode_support import is_missing_episode_meta_question

if TYPE_CHECKING:
    from .telegram_commands import TelegramCommandService


class AIMediaHost:
    def __init__(self, service: "TelegramCommandService") -> None:
        self._service = service

    def resolve_year(self, item: dict[str, Any]) -> str:
        year = item.get("ProductionYear")
        if isinstance(year, int) and year > 0:
            return str(year)
        premiere = str(item.get("PremiereDate") or "").strip()
        if re.match(r"^\d{4}", premiere):
            return premiere[:4]
        return "未知"

    def extract_ai_media_keyword(self, question: str) -> str:
        keywords = self.extract_ai_media_keywords(question)
        return keywords[0] if keywords else ""

    def extract_ai_media_keywords(self, question: str) -> list[str]:
        text = str(question or "").strip()
        if not text:
            return []
        if is_missing_episode_meta_question(text):
            return []
        quoted = re.search(r"[《「“\"]([^》」”\"]{1,80})[》」”\"]", text)
        if quoted:
            keyword = self._clean_ai_keyword(str(quoted.group(1) or ""))
            return [keyword] if keyword else []

        general_patterns = (
            r"一共.*(多少|几).*(影视|资源|电影|剧集|影片|媒体)",
            r"总共.*(多少|几).*(影视|资源|电影|剧集|影片|媒体)",
            r"现在.*(多少|几).*(影视|资源|电影|剧集|影片|媒体)",
            r"(媒体库|资源库).*(总数|总量|数量|统计)",
        )
        if any(re.search(pattern, text) for pattern in general_patterns):
            return []
        if self._looks_like_category_listing_request(text):
            return []

        value = re.sub(r"^/ai(?:@\w+)?\s*", "", text, flags=re.IGNORECASE).strip()
        value = re.sub(
            r"^(?:帮我|请|麻烦你|你帮我)?\s*(?:查看一下|看一下|查一下|看下|查下|查查|搜索一下|搜索|找一下|看看)?\s*",
            "",
            value,
            flags=re.IGNORECASE,
        )
        value = re.sub(
            r"^(?:我(?:的)?|[咱俺](?:的)?)?(?:媒体库|资源库|库)(?:里|中|里面)?\s*(?:的)?\s*",
            "",
            value,
            flags=re.IGNORECASE,
        )

        tail_patterns = (
            r"(?:我(?:的)?|[咱俺](?:的)?)?(?:媒体库|资源库|库)(?:里|中|里面)?",
            r"(?:现在|目前)?(?:最新的?|已经)?(?:更新到)?(?:第几集|哪一集|哪集|多少集|几集)",
            r"(?:一共有?|总共有?|共有|现在有|目前有|有)(?:多少集|几集)",
            r"更新到",
            r"最新(?:的)?(?:是)?",
            r"(?:缺失|缺少|缺哪|漏掉|漏)(?:的)?集",
            r"(?:的)?(?:简介|剧情|详情|演员|主演|评分)",
            r"有没有|是否有",
        )
        split_at = len(value)
        for pattern in tail_patterns:
            match = re.search(pattern, value, flags=re.IGNORECASE)
            if match and match.start() >= 2:
                split_at = min(split_at, match.start())
        primary_text = value[:split_at].strip()
        if split_at < len(value):
            primary_text = re.sub(r"的$", "", primary_text).strip()
        primary = self._clean_ai_keyword(primary_text)

        fallback = value
        fallback = re.sub(
            r"(?:我(?:的)?|[咱俺](?:的)?)?(?:媒体库|资源库|库)(?:里|中|里面)?",
            " ",
            fallback,
            flags=re.IGNORECASE,
        )
        fallback = re.sub(
            r"(?:现在|目前|最新的?|已经|一共有?|总共有?|共有|更新到|第几集|哪一集|哪集|有多少集|多少集|有几集|几集|缺失的?集|缺少的?集|缺哪几集|漏掉的?集|的?简介|的?剧情|的?详情|的?演员|的?主演|的?评分|有没有|是否有|有吗|吗)",
            " ",
            fallback,
            flags=re.IGNORECASE,
        )
        fallback = self._clean_ai_keyword(fallback)
        keywords: list[str] = []
        for candidate in (primary, fallback):
            if candidate and candidate not in keywords:
                keywords.append(candidate)
        return keywords

    def is_ai_reference_question(self, question: str) -> bool:
        text = str(question or "").strip()
        return bool(
            re.search(r"^(?:帮我|请|看一下|查一下|查看一下|继续查)?(?:它|这个|这部|那个|那部)", text)
            or re.search(r"^(?:查看一下|看一下|查一下|继续查)?(?:缺失|缺少|漏掉|漏)的?集", text)
        )

    def is_ai_episode_count_question(self, question: str) -> bool:
        text = str(question or "").strip()
        if is_missing_episode_meta_question(text):
            return False
        return bool(re.search(r"多少集|几集|第几集|哪一集|哪集|更新到|最新.*集|缺.*集", text))

    def format_ai_identity_candidates(self, keyword: str, candidates: Any) -> str:
        rows = [row for row in candidates if isinstance(row, dict)] if isinstance(candidates, list) else []
        lines = [f"《{keyword}》有多个同名候选，请带上年份重新查询："]
        for row in rows[:5]:
            type_label = "剧集" if str(row.get("type") or "") == "series" else "电影"
            lines.append(
                f"- {row.get('title') or keyword}（{row.get('year') or '年份未知'}，{type_label}，TMDB {row.get('tmdbId') or '-'}）"
            )
        return "\n".join(lines)

    def format_missing_episode_map(self, missing_map: dict[int, list[int]]) -> str:
        parts: list[str] = []
        for season in sorted(missing_map):
            label = "全局" if season == 0 else f"S{season:02d}"
            parts.append(f"{label} " + self._compress_number_ranges(missing_map[season]))
        return "；".join(parts) if parts else "无"

    def format_inventory_episode_labels(self, labels: Any, *, fallback: list[int] | None = None) -> str:
        rows = [str(value or "").strip() for value in labels] if isinstance(labels, list) else []
        rows = [value for value in rows if value]
        if not rows:
            return self._compress_number_ranges(fallback or [])
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
            return self._compress_number_ranges(global_values)
        if seasonal and not global_values:
            parts: list[str] = []
            for season in sorted(seasonal):
                ranges = self._compress_number_ranges(seasonal[season])
                parts.append(f"S{season:02d}{ranges}")
            return "、".join(parts)
        return "、".join(rows)

    @staticmethod
    def _clean_ai_keyword(value: str) -> str:
        clean = str(value or "").strip()
        clean = clean.strip(" ，。！？?：:；;、|/\\[]()（）【】《》「」“”\"'")
        clean = re.sub(r"\s+", " ", clean).strip()
        clean = re.sub(r"(?:了|呢|啊|呀)$", "", clean).strip()
        if len(clean) < 2:
            return ""
        if re.fullmatch(r"(?:缺失|缺少|缺哪|漏掉|漏)(?:的)?集", clean):
            return ""
        if re.fullmatch(r"(?:简介|剧情|详情|演员|主演|评分)(?:详情)?", clean):
            return ""
        if re.fullmatch(r"(?:最新(?:的)?|更新到|多少集|几集|第几集|哪一集|哪集)", clean):
            return ""
        if clean in {"影视资源", "资源", "媒体", "电影", "剧集", "影片", "数量", "总数"}:
            return ""
        if clean in {"查询媒体", "查看媒体", "媒体缺失集", "缺失集查询", "缺集查询"}:
            return ""
        return clean[:80]

    @staticmethod
    def _compress_number_ranges(values: list[int]) -> str:
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

    @staticmethod
    def _looks_like_category_listing_request(question: str) -> bool:
        text = str(question or "").strip()
        if not text:
            return False
        if not re.search(r"列出|列出来|全部|有哪些|查询|查找|看看|看一下|显示|统计|资源|片单|清单|扫描一下", text):
            return False
        lowered = text.lower()
        needles = (
            "国产动漫",
            "国漫",
            "中国动漫",
            "华语动漫",
            "动漫剧集",
            "动画剧集",
            "动漫",
            "动画",
            "anime",
            "animation",
            "纪录片",
            "documentary",
            "华语电影",
            "国产电影",
            "中文电影",
            "电影",
            "影片",
            "剧集",
            "电视剧",
            "连续剧",
        )
        return any(needle.lower() in lowered for needle in needles)
