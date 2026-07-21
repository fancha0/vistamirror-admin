from __future__ import annotations

from typing import TYPE_CHECKING, Callable

from .ai_host_adapter import AIHostAdapter
from .ai_query_service import AIQueryService
from .ai_tool_base import AIToolBase
from .ai_tool_handlers import (
    is_hdhive_question,
    is_library_directory_question,
    is_library_exists_question,
    is_media_detail_question,
    is_playback_question,
    is_search_media_question,
)
from .ai_subagent_registry import AISubagentRegistry
from .ai_tool_registry import AIToolRegistry
from .ai_tools import Drive115TransferTool, EmbyMediaTool, HDHiveSearchTool, MissingEpisodeTool, MoviePilotTool

if TYPE_CHECKING:
    from .telegram_commands import TelegramCommandService


class AIToolProvider:
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

    def build_registry(self) -> AIToolRegistry:
        registry = AIToolRegistry()
        predicates = self._tool_predicates()
        for tool in self.build_tools():
            registry.register_tool(tool, predicate=predicates.get(tool.name))
        return registry

    def build_tools(self) -> list[AIToolBase]:
        return [
            EmbyMediaTool(
                self.host,
                name="search_media",
                description="通过 TMDB 与 Emby ProviderIds 确认作品身份，并返回候选作品。",
                operation="search_media",
                conversation_key=self.conversation_key,
                chat_id=self.chat_id,
                rich=self.rich,
            ),
            EmbyMediaTool(
                self.host,
                name="query_library_exists",
                description="查询作品是否已在媒体库中存在，并返回 Emby 命中结果。",
                operation="query_library_exists",
                conversation_key=self.conversation_key,
                chat_id=self.chat_id,
                rich=self.rich,
            ),
            EmbyMediaTool(
                self.host,
                name="query_library_directory",
                description="查询媒体库中某类目录或分类下有哪些资源。",
                operation="query_library_directory",
                conversation_key=self.conversation_key,
                chat_id=self.chat_id,
                rich=self.rich,
            ),
            EmbyMediaTool(
                self.host,
                name="query_media_detail",
                description="查询作品简介、演员、评分和本地入库状态。",
                operation="query_media_detail",
                conversation_key=self.conversation_key,
                chat_id=self.chat_id,
                rich=self.rich,
            ),
            EmbyMediaTool(
                self.host,
                name="query_playback_history",
                description="查询最近播放、用户观看和播放排行。",
                operation="query_playback_history",
                conversation_key=self.conversation_key,
                chat_id=self.chat_id,
                rich=self.rich,
            ),
            MissingEpisodeTool(
                self.host,
                conversation_key=self.conversation_key,
                chat_id=self.chat_id,
                rich=self.rich,
            ),
            HDHiveSearchTool(
                self.host,
                conversation_key=self.conversation_key,
                chat_id=self.chat_id,
                rich=self.rich,
            ),
            Drive115TransferTool(
                self.host,
                conversation_key=self.conversation_key,
                chat_id=self.chat_id,
                rich=self.rich,
            ),
            MoviePilotTool(
                self.host,
                name="moviepilot_capabilities",
                description="查看 MoviePilot 已暴露的 MCP 只读工具能力。",
                operation="capabilities",
            ),
            MoviePilotTool(
                self.host,
                name="moviepilot_search",
                description="在 MoviePilot 中搜索电影、电视剧或动漫信息，仅调用只读 MCP 搜索工具。",
                operation="search",
            ),
            MoviePilotTool(
                self.host,
                name="moviepilot_subscriptions",
                description="查询 MoviePilot 当前订阅列表，仅调用只读 MCP 工具。",
                operation="subscriptions",
            ),
            MoviePilotTool(
                self.host,
                name="moviepilot_tasks",
                description="查询 MoviePilot 下载或任务队列，仅调用只读 MCP 工具。",
                operation="tasks",
            ),
        ]

    def build_subagents(self) -> AISubagentRegistry:
        registry = AISubagentRegistry()
        registry.register(
            name="media-librarian",
            description="负责媒体身份识别、媒体库存在性、详情、集数和缺集查询。",
            tool_names=[
                "search_media",
                "query_library_exists",
                "query_library_directory",
                "query_media_detail",
                "query_missing_episodes",
            ],
            instruction="先确认作品身份，再用媒体库与 TMDB/Emby 数据回答；回答数字时优先引用结构化结果，不要猜测。",
        )
        registry.register(
            name="playback-analyst",
            description="负责播放历史、观看行为和近期活跃摘要。",
            tool_names=["query_playback_history"],
            instruction="优先总结最近播放、活跃用户和观看趋势；如果没有数据就直接说明样本不足。",
        )
        registry.register(
            name="resource-operator",
            description="负责影巢资源搜索和 115 转存相关操作。",
            tool_names=["search_hdhive_resource", "transfer_115_share"],
            instruction="只处理资源搜索和转存；涉及执行动作时保持谨慎，优先走确认流程或返回明确结果。",
        )
        registry.register(
            name="moviepilot-operator",
            description="负责 MoviePilot 的影视搜索、能力、订阅和任务只读查询。",
            tool_names=["moviepilot_search", "moviepilot_capabilities", "moviepilot_subscriptions", "moviepilot_tasks"],
            instruction="只查询 MoviePilot 已暴露的只读能力；搜索时返回影视信息，不要创建订阅、开始下载或执行任何写操作。",
        )
        return registry

    def allowed_tool_names_for_question(self, question: str) -> list[str]:
        subagent = self.build_subagents().pick(question)
        if subagent:
            return list(subagent.tool_names)
        return [tool.name for tool in self.build_registry().definitions() if tool.kind == "read"]

    def _tool_predicates(self) -> dict[str, Callable[[str], bool]]:
        return {
            "search_media": lambda text: self._is_search_media_question(text),
            "query_library_exists": lambda text: self._is_library_exists_question(text),
            "query_library_directory": lambda text: self._is_library_directory_question(text),
            "query_media_detail": lambda text: self._is_media_detail_question(text),
            "query_playback_history": lambda text: self._is_playback_question(text),
            "query_missing_episodes": lambda text: self.host.media.is_ai_episode_count_question(text) or bool(AIQueryService.parse_missing_episode_request(text)),
            "search_hdhive_resource": lambda text: self._is_hdhive_question(text),
            "moviepilot_search": lambda text: self._is_moviepilot_search_question(text),
            "moviepilot_capabilities": lambda text: self._is_moviepilot_capabilities_question(text),
            "moviepilot_subscriptions": lambda text: self._is_moviepilot_subscriptions_question(text),
            "moviepilot_tasks": lambda text: self._is_moviepilot_tasks_question(text),
        }

    @staticmethod
    def _is_media_detail_question(text: str) -> bool:
        return bool(is_media_detail_question(text))

    @staticmethod
    def _is_moviepilot_question(text: str) -> bool:
        value = str(text or "")
        return bool(
            __import__("re").search(r"moviepilot|媒体订阅|订阅列表|下载任务|任务队列|mp订阅|mp任务", value, flags=__import__("re").IGNORECASE)
        )

    @classmethod
    def _is_moviepilot_search_question(cls, text: str) -> bool:
        value = str(text or "")
        return cls._is_moviepilot_question(value) and bool(
            __import__("re").search(r"搜索|搜一下|搜|查一下|查找|找一下|查询|search|lookup|find", value, flags=__import__("re").IGNORECASE)
        )

    @classmethod
    def _is_moviepilot_subscriptions_question(cls, text: str) -> bool:
        value = str(text or "")
        return bool(
            __import__("re").search(r"订阅列表|媒体订阅|mp订阅|moviepilot.*订阅|订阅.*moviepilot", value, flags=__import__("re").IGNORECASE)
        )

    @classmethod
    def _is_moviepilot_tasks_question(cls, text: str) -> bool:
        value = str(text or "")
        return bool(
            __import__("re").search(r"下载任务|任务队列|mp任务|moviepilot.*(?:下载|任务)|(?:下载|任务).*moviepilot", value, flags=__import__("re").IGNORECASE)
        )

    @classmethod
    def _is_moviepilot_capabilities_question(cls, text: str) -> bool:
        return cls._is_moviepilot_question(text) and not (
            cls._is_moviepilot_search_question(text)
            or cls._is_moviepilot_subscriptions_question(text)
            or cls._is_moviepilot_tasks_question(text)
        )

    @staticmethod
    def _is_playback_question(text: str) -> bool:
        return bool(is_playback_question(text))

    @staticmethod
    def _is_hdhive_question(text: str) -> bool:
        return bool(is_hdhive_question(text))

    @staticmethod
    def _is_library_exists_question(text: str) -> bool:
        return bool(is_library_exists_question(text))

    @staticmethod
    def _is_library_directory_question(text: str) -> bool:
        return bool(is_library_directory_question(text))

    def _is_search_media_question(self, text: str) -> bool:
        return bool(is_search_media_question(self.host, text))
