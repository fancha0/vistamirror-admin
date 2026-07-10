from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .ai_subagent_registry import AISubagentRegistry
    from .ai_tool_provider import AIToolProvider
    from .ai_tool_registry import AIToolRegistry


class AIRegistryHost:
    def __init__(self, service: Any) -> None:
        self._service = service

    def tool_provider(
        self,
        *,
        conversation_key: str = "",
        chat_id: str = "",
        rich: bool = False,
    ) -> "AIToolProvider":
        from .ai_tool_provider import AIToolProvider

        return AIToolProvider(
            self._service,
            conversation_key=conversation_key,
            chat_id=chat_id,
            rich=rich,
        )

    def tool_registry(
        self,
        *,
        conversation_key: str = "",
        chat_id: str = "",
        rich: bool = False,
    ) -> "AIToolRegistry":
        return self.tool_provider(
            conversation_key=conversation_key,
            chat_id=chat_id,
            rich=rich,
        ).build_registry()

    def subagent_registry(
        self,
        *,
        conversation_key: str = "",
        chat_id: str = "",
        rich: bool = False,
    ) -> "AISubagentRegistry":
        return self.tool_provider(
            conversation_key=conversation_key,
            chat_id=chat_id,
            rich=rich,
        ).build_subagents()

    def pick_subagent(
        self,
        question: str,
        *,
        conversation_key: str = "",
        chat_id: str = "",
        rich: bool = False,
    ):
        return self.subagent_registry(
            conversation_key=conversation_key,
            chat_id=chat_id,
            rich=rich,
        ).pick(question)
