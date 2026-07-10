from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from .ai_host_adapter import AIHostAdapter
from .ai_query_service import AIQueryService

if TYPE_CHECKING:
    from .telegram_commands import TelegramCommandService


class AIToolsetBase:
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
        self.query_service = AIQueryService(
            self.host,
            conversation_key=self.conversation_key,
            chat_id=self.chat_id,
            rich=self.rich,
        )

    def extract_media_keyword(self, question: str) -> str:
        route = {"mediaTitle": "", "useActiveMedia": False, "intent": ""}
        try:
            route["mediaTitle"] = self.host.media.extract_ai_media_keyword(question)
        except Exception:
            route["mediaTitle"] = ""
        title = str(route.get("mediaTitle") or "").strip()
        if title:
            return title
        active = self.host.conversations.get_active_media(self.conversation_key)
        if self.host.media.is_ai_reference_question(question):
            return str(active.get("title") or "").strip()
        return str(question or "").strip()


class MediaLibrarianToolset(AIToolsetBase):
    def search_media(self, question: str) -> str:
        keyword = self.extract_media_keyword(question)
        if not keyword:
            return "请告诉我要搜索的影视名称。"
        resolution = self.host.media_service.media_identity_service().resolve(keyword)
        if resolution.get("ambiguous"):
            candidates = resolution.get("candidates") if isinstance(resolution.get("candidates"), list) else []
            if not candidates:
                return f"没有为《{keyword}》找到可用候选。"
            lines = [f"《{keyword}》找到多个候选："]
            for row in candidates[:5]:
                lines.append(
                    f"- {row.get('title') or '未知标题'}（{row.get('year') or '年份未知'}）"
                    f" · {row.get('type') or '未知类型'} · TMDB {row.get('tmdbId') or '-'}"
                )
            return "\n".join(lines)
        identity = resolution.get("identity") if isinstance(resolution.get("identity"), dict) else {}
        emby_item = resolution.get("embyItem") if isinstance(resolution.get("embyItem"), dict) else {}
        if not identity and not emby_item:
            return f"没有在 Emby 或 TMDB 中确认《{keyword}》。"
        title = str(identity.get("title") or emby_item.get("Name") or keyword).strip()
        year = str(identity.get("year") or emby_item.get("ProductionYear") or "年份未知").strip()
        media_type = "剧集" if str(identity.get("type") or emby_item.get("Type") or "").lower() in {"series", "tv"} else "电影"
        tmdb_id = str(identity.get("tmdbId") or "").strip() or "-"
        emby_id = str(emby_item.get("Id") or identity.get("embyId") or "").strip() or "-"
        confidence = str(identity.get("confidence") or "已确认").strip()
        return "\n".join(
            [
                f"已确认作品：{title}（{year}）",
                f"- 类型：{media_type}",
                f"- TMDB ID：{tmdb_id}",
                f"- Emby ID：{emby_id}",
                f"- 识别依据：{confidence}",
            ]
        )

    def query_library_exists(self, question: str) -> str:
        keyword = self.extract_media_keyword(question)
        if not keyword:
            return "请带上要确认的影视名称。"
        service = self.host.media_service.media_identity_service()
        resolution = service.resolve(keyword)
        if resolution.get("ambiguous"):
            candidates = resolution.get("candidates") if isinstance(resolution.get("candidates"), list) else []
            return self.host.media.format_ai_identity_candidates(keyword, candidates)
        identity = resolution.get("identity") if isinstance(resolution.get("identity"), dict) else {}
        emby_item = resolution.get("embyItem") if isinstance(resolution.get("embyItem"), dict) else {}
        if not identity and not emby_item:
            return f"媒体库中未确认《{keyword}》的有效资源。"
        if not identity and emby_item:
            identity = {
                "title": str(emby_item.get("Name") or keyword).strip(),
                "year": str(emby_item.get("ProductionYear") or "").strip(),
                "type": "series" if str(emby_item.get("Type") or "").lower() in {"series", "tv"} else "movie",
                "tmdbId": "",
                "embyId": str(emby_item.get("Id") or "").strip(),
                "confidence": "Emby 本地命中",
            }
        inventory = service.query_library_exists(identity)
        matched_item = inventory.get("embyItem") if isinstance(inventory.get("embyItem"), dict) else {}
        title = str(identity.get("title") or emby_item.get("Name") or matched_item.get("Name") or keyword).strip()
        if not inventory.get("exists"):
            return f"媒体库中未找到《{title}》。"
        media_type = "剧集" if str(identity.get("type") or matched_item.get("Type") or "").lower() in {"series", "tv"} else "电影"
        lines = [
            f"媒体库中已存在《{title}》。",
            f"- 类型：{media_type}",
            f"- Emby ID：{matched_item.get('Id') or emby_item.get('Id') or identity.get('embyId') or '-'}",
        ]
        season_map = inventory.get("seasonMap") if isinstance(inventory.get("seasonMap"), dict) else {}
        if season_map:
            season_total = sum(len(values) for values in season_map.values() if isinstance(values, set))
            lines.append(f"- 实际可读取单集：{season_total} 集")
        return "\n".join(lines)

    def query_library_directory(self, question: str) -> str:
        reply = self.query_service.build_library_directory_reply(question)
        if reply:
            return reply
        return "当前没有识别到有效目录分类。请直接说“我库里有什么亚洲电影 / 华语电影 / 国产动漫 / 剧集”。"

    def query_media_detail(self, question: str) -> str:
        return self.query_service.build_media_detail_reply(question)

    def query_missing_episodes(self, question: str) -> str | dict[str, Any]:
        return self.query_service.build_missing_episode_reply(question) or self.query_service.build_episode_query_reply(question)


class PlaybackAnalystToolset(AIToolsetBase):
    def query_playback_history(self, _question: str) -> str:
        return self.query_service.build_playback_history_context()


class ResourceOperatorToolset(AIToolsetBase):
    def search_hdhive_resource(self, question: str) -> dict[str, Any] | str:
        keyword = self.extract_media_keyword(question) or str(question or "").strip()
        return self.host.media_service.cmd_hdhive_search(keyword)

    def transfer_115_share(self, question: str) -> dict[str, Any] | str:
        return self.host.media_service.cmd_drive115_transfer(question)


def is_media_detail_question(text: str) -> bool:
    return bool(re.search(r"简介|剧情|详情|演员|主演|评分", str(text or "")))


def is_playback_question(text: str) -> bool:
    return bool(re.search(r"播放历史|最近.*看|看了什么|播放记录|播放最多|观看历史", str(text or "")))


def is_hdhive_question(text: str) -> bool:
    return bool(re.search(r"影巢|hdhive", str(text or "")))


def is_library_exists_question(text: str) -> bool:
    clean = str(text or "").strip()
    if not clean:
        return False
    if is_library_directory_question(clean):
        return False
    return bool(
        re.search(
            r"媒体库里有没有|库里有没有|是否存在|存在吗|库里有吗|本地有没有|媒体库里.*有吗|库里.*有吗|有没有.*(这部|这片|这剧|这部片|这部剧)?",
            clean,
        )
    )


def is_library_directory_question(text: str) -> bool:
    clean = str(text or "").strip()
    if not clean:
        return False
    return bool(
        re.search(
            r"我库里有什么|媒体库里有什么|库里有什么|库里有哪些|媒体库里有哪些|列出.*(电影|剧集|动漫|纪录片|资源|片单)|.*(电影|剧集|动漫|纪录片|资源|片单).*(有哪些|有什么|列表|清单)",
            clean,
            flags=re.IGNORECASE,
        )
    )


def is_search_media_question(host: "AIHostAdapter | TelegramCommandService", text: str) -> bool:
    adapter = AIHostAdapter.coerce(host)
    clean = str(text or "").strip()
    if not clean:
        return False
    if is_library_exists_question(clean):
        return False
    if is_library_directory_question(clean):
        return False
    if adapter.media.is_ai_episode_count_question(clean) or bool(AIQueryService.parse_missing_episode_request(clean)):
        return False
    return bool(re.search(r"搜索.*(影视|作品|片名|资源)|查找.*(影视|作品|片名)|确认.*(片名|作品)|这是什么片", clean))
