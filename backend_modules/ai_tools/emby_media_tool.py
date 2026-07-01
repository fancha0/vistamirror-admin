from __future__ import annotations

from typing import TYPE_CHECKING

from ..ai_host_adapter import AIHostAdapter
from ..ai_tool_base import AIToolBase, CommandReply
from ..ai_tool_handlers import MediaLibrarianToolset, PlaybackAnalystToolset

if TYPE_CHECKING:
    from ..telegram_commands import TelegramCommandService


class EmbyMediaTool(AIToolBase):
    def __init__(
        self,
        host: "AIHostAdapter | TelegramCommandService",
        *,
        name: str,
        description: str,
        operation: str,
        conversation_key: str = "",
        chat_id: str = "",
        rich: bool = False,
        kind: str = "read",
        schema: dict | None = None,
    ) -> None:
        super().__init__(
            name=name,
            description=description,
            schema=schema
            or {
                "type": "object",
                "properties": {"question": {"type": "string"}},
                "required": ["question"],
            },
            kind=kind,
        )
        self._operation = str(operation or "").strip()
        adapter = AIHostAdapter.coerce(host)
        self._media_tools = MediaLibrarianToolset(
            adapter,
            conversation_key=conversation_key,
            chat_id=chat_id,
            rich=rich,
        )
        self._playback_tools = PlaybackAnalystToolset(
            adapter,
            conversation_key=conversation_key,
            chat_id=chat_id,
            rich=rich,
        )

    def invoke(self, question: str) -> CommandReply:
        if self._operation == "search_media":
            return self._media_tools.search_media(question)
        if self._operation == "query_library_exists":
            return self._media_tools.query_library_exists(question)
        if self._operation == "query_media_detail":
            return self._media_tools.query_media_detail(question)
        if self._operation == "query_missing_episodes":
            return self._media_tools.query_missing_episodes(question)
        if self._operation == "query_playback_history":
            return self._playback_tools.query_playback_history(question)
        raise ValueError(f"Unsupported Emby media tool operation: {self._operation}")
