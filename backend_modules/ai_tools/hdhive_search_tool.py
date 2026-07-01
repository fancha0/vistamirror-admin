from __future__ import annotations

from typing import TYPE_CHECKING

from ..ai_host_adapter import AIHostAdapter
from ..ai_tool_base import AIToolBase, CommandReply
from ..ai_tool_handlers import ResourceOperatorToolset

if TYPE_CHECKING:
    from ..telegram_commands import TelegramCommandService


class HDHiveSearchTool(AIToolBase):
    def __init__(
        self,
        host: "AIHostAdapter | TelegramCommandService",
        *,
        name: str = "search_hdhive_resource",
        description: str = "使用影巢搜索资源并返回候选条目。",
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
        self._toolset = ResourceOperatorToolset(
            AIHostAdapter.coerce(host),
            conversation_key=conversation_key,
            chat_id=chat_id,
            rich=rich,
        )

    def invoke(self, question: str) -> CommandReply:
        return self._toolset.search_hdhive_resource(question)
