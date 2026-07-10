from __future__ import annotations

import logging
import re
import secrets
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any

from .ai_host_adapter import AIHostAdapter
from .ai_library_directory_service import AILibraryDirectoryService
from .ai_missing_episode_support import parse_missing_episode_request
from .playback_history_service import PlaybackHistoryService

if TYPE_CHECKING:
    from .telegram_commands import TelegramCommandService
    from .ai_tool_registry import CommandReply


LOGGER = logging.getLogger(__name__)


class AIQueryService:
    def __init__(
        self,
        host: "AIHostAdapter | TelegramCommandService",
        *,
        conversation_key: str = "",
        chat_id: str = "",
        rich: bool = False,
    ) -> None:
        self.host = AIHostAdapter.coerce(host)
        self.conversation_key = str(conversation_key or "").strip()
        self.chat_id = str(chat_id or "").strip()
        self.rich = bool(rich)

    def build_playback_history_context(self) -> str:
        try:
            service = PlaybackHistoryService(fetcher=self.host.media_service.emby_get, event_logger=None)
            result = service.collect(limit=20, scan_limit=800)
            rows = result.get("rows") if isinstance(result, dict) else []
        except Exception as err:
            return f"播放历史：读取失败（{err}）。"
        if not isinstance(rows, list) or not rows:
            return "播放历史：暂无可用记录。"
        media_counts: dict[str, int] = {}
        user_counts: dict[str, int] = {}
        lines: list[str] = []
        for row in rows[:10]:
            if not isinstance(row, dict):
                continue
            media = self.host.media_formatter.format_recent_playback_filename_with_status(row)[0]
            user = str(row.get("username") or row.get("user") or "未知用户").strip()
            media_counts[media] = int(media_counts.get(media) or 0) + 1
            user_counts[user] = int(user_counts.get(user) or 0) + 1
            lines.append(self.host.media_formatter.format_recent_playback_row(row))
        top_media = sorted(media_counts.items(), key=lambda item: item[1], reverse=True)[:5]
        top_users = sorted(user_counts.items(), key=lambda item: item[1], reverse=True)[:5]
        summary = [
            "播放历史摘要：",
            f"- 最近记录数：{len(rows)}",
            "- 最近记录：",
            *[f"  {line}" for line in lines[:8]],
        ]
        if top_media:
            summary.append("- 最近高频影片：" + "、".join(f"{name} {count}次" for name, count in top_media))
        if top_users:
            summary.append("- 最近活跃用户：" + "、".join(f"{name} {count}次" for name, count in top_users))
        return "\n".join(summary)

    def build_recent_library_summary_reply(self) -> str:
        try:
            rows, _fallback_rows, _error = self.host.media_service.fetch_latest_items_with_fallback(limit=12)
        except Exception as err:
            return f"最近入库：读取失败（{self.host.media_service.format_emby_error(err)}）。"
        if not rows:
            return "最近入库：暂无可展示记录。"
        lines = ["最近入库摘要："]
        for row in rows[:8]:
            if not isinstance(row, dict):
                continue
            lines.append(f"- {self.host.media_service.format_recent_library_row(row)}")
        return "\n".join(lines)

    def build_focus_media_context(self, question: str) -> str:
        keywords = self.host.media.extract_ai_media_keywords(question)
        if not keywords:
            return ""
        matched_keyword, items, error = self.host.media_service.search_emby_media_candidates(keywords)
        if error and not items:
            return f"媒体搜索：读取失败（{error}）。"
        if not items:
            recent_hint = self.build_recent_library_hint(keywords[0])
            if recent_hint:
                return f"媒体搜索：当前 Emby 未直接命中《{keywords[0]}》，但最近入库提示：{recent_hint}"
            return ""
        resolved = self.host.media_service.resolve_ai_series_search_item(items[0], items=items, keyword=matched_keyword)
        lines = self.host.media_service.format_ai_matched_item_context(resolved, keyword=matched_keyword)
        return "\n".join(lines) if lines else ""

    def build_category_listing_context(self, question: str) -> str:
        spec = self._parse_library_directory_request(question)
        if not spec:
            return ""
        label = str(spec.get("label") or "媒体资源").strip()
        try:
            items, total, source, note = self._library_directory_service().fetch_category_items(spec=spec, limit=30)
        except Exception as err:
            return f"分类资源查询：读取“{label}”失败（{self.host.media_service.format_emby_error(err)}）。"
        if not items:
            return str(note or f"未配置目录分类：{label}\n未找到对应目录或库节点，请先配置 libraryDirectoryConfig.categories。").strip()
        lines = [
            f"分类资源查询：{label}",
            f"- 匹配数量：{total}",
            f"- 已显示前 {len(items)} 条",
            f"- 查询来源：{self._directory_source_label(source)}",
            "- 资源列表：",
        ]
        for idx, item in enumerate(items, start=1):
            lines.append(f"  {idx}. {self._format_category_item_line(item)}")
        if total > len(items):
            lines.append("- 提示：结果较多，建议继续缩小关键词或分类。")
        return "\n".join(lines)

    def build_library_directory_reply(self, question: str) -> str:
        return self.build_category_listing_context(question)

    def _library_directory_service(self) -> AILibraryDirectoryService:
        return AILibraryDirectoryService(
            self.host.media_service,
            store_reader=self.host.platform.read_store,
        )

    def build_recent_library_hint(self, title: str) -> str:
        safe_title = str(title or "").strip()
        if not safe_title:
            return ""
        try:
            latest, _tried, _err = self.host.media_service.fetch_latest_items_with_fallback(limit=20)
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
            return self.host.media_service.format_recent_library_row(row)
        return ""

    def build_media_detail_reply(self, question: str) -> str:
        text = str(question or "").strip()
        if not re.search(r"简介|剧情|详情|演员|主演|评分", text):
            return ""
        active = self.host.conversations.get_active_media(self.conversation_key)
        if self.host.media.is_ai_reference_question(text) and active.get("title"):
            keywords = [str(active.get("title") or "").strip()]
            preferred_type = str(active.get("type") or "")
        else:
            keywords = self.host.media.extract_ai_media_keywords(text)
            preferred_type = ""
        if not keywords:
            return "当前没有记住正在讨论的作品，请带上片名再查询。"
        keyword = keywords[0]
        try:
            service = self.host.media_service.media_identity_service()
            resolution = service.resolve(keyword, preferred_type=preferred_type)
        except Exception as err:
            return f"查询《{keyword}》详情失败：{self.host.media_service.format_emby_error(err)}"
        if resolution.get("ambiguous") and not resolution.get("embyItem"):
            return self.host.media.format_ai_identity_candidates(keyword, resolution.get("candidates"))
        identity = resolution.get("identity") if isinstance(resolution.get("identity"), dict) else {}
        emby_item = resolution.get("embyItem") if isinstance(resolution.get("embyItem"), dict) else {}
        if not identity and not emby_item:
            return f"当前 TMDB 和 Emby 都没有找到《{keyword}》。"
        tmdb_detail: dict[str, Any] = {}
        try:
            tmdb_detail = service.get_media_detail(str(identity.get("tmdbId") or ""), str(identity.get("type") or ""))
        except Exception as err:
            LOGGER.warning("TG AI TMDB detail fallback: title=%s err=%s", keyword, err)
        title = str(identity.get("title") or emby_item.get("Name") or keyword).strip()
        year = str(identity.get("year") or emby_item.get("ProductionYear") or "").strip()
        overview = str(tmdb_detail.get("overview") or emby_item.get("Overview") or identity.get("overview") or "").strip()
        rating = tmdb_detail.get("vote_average")
        if rating in (None, ""):
            rating = emby_item.get("CommunityRating") or identity.get("rating")
        credits = tmdb_detail.get("credits") if isinstance(tmdb_detail.get("credits"), dict) else {}
        cast = credits.get("cast") if isinstance(credits.get("cast"), list) else []
        cast_names = [str(row.get("name") or "").strip() for row in cast[:8] if isinstance(row, dict) and str(row.get("name") or "").strip()]
        type_label = "剧集" if str(identity.get("type") or "") == "series" else "电影"
        lines = [f"《{title}》详情：", f"- 年份：{year or '未知'}", f"- 类型：{type_label}"]
        if rating not in (None, ""):
            try:
                lines.append(f"- 评分：{float(rating):.1f}")
            except Exception:
                lines.append(f"- 评分：{rating}")
        if cast_names:
            lines.append(f"- 主演：{'、'.join(cast_names)}")
        lines.append(f"- 简介：{overview[:900] if overview else '暂无简介'}")
        lines.append(f"- 本地状态：{'已入库' if emby_item else '未入库'}")
        self.host.conversations.set_active_media(
            self.conversation_key,
            {
                "title": title,
                "year": year,
                "type": identity.get("type"),
                "embySeriesId": emby_item.get("Id") if str(identity.get("type") or "") == "series" else "",
                "tmdbId": identity.get("tmdbId"),
            },
        )
        return "\n".join(lines)

    def build_episode_query_reply(self, question: str) -> str:
        text = str(question or "").strip()
        if not self.host.media.is_ai_episode_count_question(text):
            return ""
        keywords = self.host.media.extract_ai_media_keywords(text)
        active_media = self.host.conversations.get_active_media(self.conversation_key)
        if self.host.media.is_ai_reference_question(text) and active_media.get("title"):
            keywords = [str(active_media.get("title") or "").strip()]
        if not keywords:
            return "没有识别到要查询的剧集名称，请带上片名再试。"
        keyword = keywords[0]
        try:
            resolution = self.host.media_service.media_identity_service().resolve(keyword, preferred_type="series")
        except Exception as err:
            return f"查询《{keyword}》失败：{self.host.media_service.format_emby_error(err)}"
        if resolution.get("ambiguous") and not resolution.get("embyItem"):
            return self.host.media.format_ai_identity_candidates(keyword, resolution.get("candidates"))
        series = resolution.get("embyItem") if isinstance(resolution.get("embyItem"), dict) else {}
        identity = resolution.get("identity") if isinstance(resolution.get("identity"), dict) else {}
        if not series:
            self.host.platform.log_ai_media_query_diagnostic(
                question=text,
                keyword=keyword,
                candidates=keywords,
                detail={"result": "not_found"},
            )
            return f"当前 Emby 可读范围内没有找到《{keyword}》。"
        if str(series.get("Type") or "").strip().lower() == "episode":
            series = self.host.media_service.resolve_ai_series_search_item(series, items=[series], keyword=keyword)
        if str(series.get("Type") or "").strip().lower() != "series":
            title = str(series.get("Name") or keyword).strip()
            return f"媒体库中找到了《{title}》，但它不是可统计单集的剧集资源。"
        item_id = str(series.get("Id") or "").strip()
        detail = dict(series)
        if item_id:
            try:
                payload = self.host.media_service.emby_get(
                    f"/Items/{urllib.parse.quote(item_id, safe='')}?Fields=Name,Type,ProductionYear,PremiereDate,ChildCount,RecursiveItemCount,Status,ProviderIds"
                )
                if isinstance(payload, dict):
                    detail.update(payload)
            except Exception as err:
                LOGGER.warning("TG AI series detail fallback: item_id=%s err=%s", item_id, err)
        title = str(detail.get("Name") or keyword).strip()
        identity = identity or self.host.media_service.resolve_ai_media_identity(keyword=keyword, detail=detail)
        season_count, actual_count, _season_lines, latest_text, source_note = self.host.media_service.resolve_ai_series_counts(
            item_id=item_id,
            detail=detail,
            keyword=keyword,
            title=title,
            identity=identity,
        )
        latest_label = self.host.media_formatter.format_ai_latest_episode_label(latest_text)
        lines = [f"《{title}》媒体库查询结果："]
        if latest_label:
            lines.append(f"- 本地最新已入库：{latest_label}")
        else:
            lines.append("- 本地最新已入库：未读取到有效季集编号")
        lines.append(f"- 实际可读取单集：{actual_count} 集")
        if season_count > 1:
            lines.append(f"- 可读取季数：{season_count} 季")
        if source_note:
            lines.append(f"- 核对说明：{source_note}")
        self.host.platform.log_ai_media_query_diagnostic(
            question=text,
            keyword=keyword,
            candidates=keywords,
            detail={
                "result": "matched",
                "title": title,
                "embySeriesId": item_id,
                "actualEpisodeCount": actual_count,
                "latestEpisode": latest_text,
            },
        )
        self.host.conversations.set_active_media(
            self.conversation_key,
            {
                "title": title,
                "year": identity.get("year"),
                "type": "series",
                "embySeriesId": item_id,
                "tmdbId": identity.get("tmdbId"),
                "latestEpisode": latest_text,
                "actualEpisodeCount": actual_count,
                "seasonCount": season_count,
            },
        )
        return "\n".join(lines)

    def build_missing_episode_reply(self, question: str) -> "CommandReply":
        from .ai_tools.missing_episode_tool import MissingEpisodeTool

        return MissingEpisodeTool(
            self.host,
            conversation_key=self.conversation_key,
            chat_id=self.chat_id,
            rich=self.rich,
        ).invoke(question)

    def _parse_library_directory_request(self, question: str) -> dict[str, Any] | None:
        text = str(question or "").strip()
        if not text:
            return None
        if not re.search(r"列出|列出来|全部|有哪些|有什么|有啥|查询|查找|看看|看一下|显示|统计|资源|片单|清单|扫描一下", text):
            return None

        lowered = text.lower()
        specs: list[dict[str, Any]] = [
            {
                "label": "国产动漫",
                "needles": ["国产动漫", "国漫", "中国动漫", "华语动漫"],
                "includeTypes": "Series,Movie",
                "queryMode": "directory_strict",
                "match": ["国产动漫", "国漫", "中国动漫", "华语动漫", "动漫", "动画", "animation", "anime"],
                "prefer": ["国产", "中国", "华语", "大陆", "cn"],
            },
            {
                "label": "动漫剧集",
                "needles": ["动漫剧集", "动画剧集"],
                "includeTypes": "Series",
                "queryMode": "directory_strict",
                "match": ["动漫", "动画", "animation", "anime"],
            },
            {
                "label": "动漫",
                "needles": ["动漫", "动画", "anime", "animation"],
                "includeTypes": "Series,Movie",
                "queryMode": "directory_strict",
                "match": ["动漫", "动画", "animation", "anime"],
            },
            {
                "label": "纪录片",
                "needles": ["纪录片", "documentary"],
                "includeTypes": "Series,Movie",
                "queryMode": "directory_strict",
                "match": ["纪录片", "documentary"],
            },
            {
                "label": "华语电影",
                "needles": ["华语电影", "国产电影", "中文电影"],
                "includeTypes": "Movie",
                "queryMode": "directory_strict",
                "match": ["华语", "国产", "中国", "大陆", "中文"],
            },
            {
                "label": "亚洲电影",
                "needles": ["亚洲电影", "亚洲影片", "亚洲片", "韩影", "日影", "日韩电影", "韩国电影", "日本电影"],
                "includeTypes": "Movie",
                "queryMode": "directory_strict",
                "match": [
                    "亚洲电影",
                    "亚洲影片",
                    "亚洲片",
                    "中国",
                    "大陆",
                    "香港",
                    "台湾",
                    "日本",
                    "韩国",
                    "泰国",
                    "印度",
                    "新加坡",
                    "马来西亚",
                    "越南",
                    "印尼",
                ],
            },
            {
                "label": "电影",
                "needles": ["电影", "影片"],
                "includeTypes": "Movie",
                "queryMode": "directory_strict",
                "match": [],
            },
            {
                "label": "剧集",
                "needles": ["剧集", "电视剧", "连续剧"],
                "includeTypes": "Series",
                "queryMode": "directory_strict",
                "match": [],
            },
        ]
        for spec in specs:
            for needle in spec["needles"]:
                if needle.lower() in lowered:
                    return {
                        **spec,
                        "matchedNeedle": needle,
                    }
        return None

    def _parse_category_listing_request(self, question: str) -> dict[str, Any] | None:
        return self._parse_library_directory_request(question)

    def _format_category_item_line(self, item: dict[str, Any]) -> str:
        title = str(item.get("Name") or item.get("SeriesName") or "未知标题").strip()
        year = self.host.media.resolve_year(item)
        item_type = str(item.get("Type") or "").strip().lower()
        type_label = {"series": "剧集", "movie": "电影", "episode": "单集"}.get(item_type, item_type or "资源")
        if item_type == "series":
            count = int(item.get("RecursiveItemCount") or item.get("ChildCount") or 0)
            pack = f"约 {count} 集" if count > 0 else "集数待查"
        elif item_type == "movie":
            pack = "电影完整版"
        else:
            pack = "资源"
        genres = item.get("Genres") if isinstance(item.get("Genres"), list) else []
        genre_text = " / ".join(str(genre).strip() for genre in genres[:3] if str(genre).strip())
        suffix = f"｜{genre_text}" if genre_text else ""
        return f"《{title}》({year})｜{type_label}｜{pack}{suffix}"

    @staticmethod
    def _directory_source_label(source: str) -> str:
        normalized = str(source or "").strip().lower()
        if normalized == "filesystem":
            return "本地目录"
        if normalized == "library":
            return "库节点目录"
        return "Emby 元数据匹配"

    @staticmethod
    def parse_missing_episode_request(question: str) -> dict[str, str]:
        return parse_missing_episode_request(question)

    @classmethod
    def analyze_local_episode_gaps(cls, episodes: list[dict[str, Any]]) -> dict[str, Any]:
        season_map: dict[int, set[int]] = {}
        for row in episodes:
            if not isinstance(row, dict):
                continue
            season = cls._coerce_index_number(row.get("ParentIndexNumber"))
            episode = cls._coerce_index_number(row.get("IndexNumber"))
            if season is None or episode is None or season <= 0 or episode <= 0:
                continue
            season_map.setdefault(season, set()).add(episode)
        blocks = sorted(
            ((season, min(values), max(values)) for season, values in season_map.items() if values),
            key=lambda row: (row[1], row[2], row[0]),
        )
        global_mode = False
        if len(blocks) >= 2 and blocks[0][1] == 1:
            global_mode = all(current[1] > previous[2] for previous, current in zip(blocks, blocks[1:]))
        elif len(blocks) == 1 and blocks[0][1] > 1:
            global_mode = True
        missing: dict[int, list[int]] = {}
        if global_mode:
            present = set().union(*season_map.values()) if season_map else set()
            if present:
                gaps = sorted(set(range(min(present), max(present) + 1)).difference(present))
                if gaps:
                    missing[0] = gaps
        else:
            for season, present in season_map.items():
                if not present:
                    continue
                gaps = sorted(set(range(min(present), max(present) + 1)).difference(present))
                if gaps:
                    missing[season] = gaps
        return {"mode": "global" if global_mode else "seasonal", "seasonMap": season_map, "missing": missing}

    @staticmethod
    def _coerce_index_number(value: Any) -> int | None:
        try:
            number = int(value)
        except (TypeError, ValueError):
            return None
        return number if number >= 0 else None
