from __future__ import annotations

from typing import TYPE_CHECKING

from ..ai_host_adapter import AIHostAdapter
from ..ai_tool_base import AIToolBase, CommandReply
from ..ai_tool_handlers import ResourceOperatorToolset

if TYPE_CHECKING:
    from ..telegram_commands import TelegramCommandService


class Drive115TransferTool(AIToolBase):
    def __init__(
        self,
        host: "AIHostAdapter | TelegramCommandService",
        *,
        name: str = "transfer_115_share",
        description: str = "确认后转存 115 分享链接。",
        conversation_key: str = "",
        chat_id: str = "",
        rich: bool = False,
        kind: str = "confirm",
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
        return self._toolset.transfer_115_share(question)
