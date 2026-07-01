from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .ai_agent_service import AIAgentService
from .ai_config_service import AIConfigService
from .ai_context_service import AIContextService
from .ai_host_adapter import AIHostAdapter
from .ai_orchestrator import AIOrchestrator
from .ai_query_service import AIQueryService
from .ai_reply_service import AIReplyService
from .ai_support_service import AISupportService
from .ai_tool_provider import AIToolProvider

if TYPE_CHECKING:
    from .telegram_commands import TelegramCommandService


class AIRuntimeService:
    def __init__(
        self,
        service: "TelegramCommandService",
        *,
        conversation_key: str = "",
        chat_id: str = "",
        rich: bool = False,
    ) -> None:
        self.service = service
        self.host = AIHostAdapter(service)
        self.conversation_key = str(conversation_key or "").strip()
        self.chat_id = str(chat_id or "").strip()
        self.rich = bool(rich)

    def query_service(self) -> AIQueryService:
        return AIQueryService(
            self.host,
            conversation_key=self.conversation_key,
            chat_id=self.chat_id,
            rich=self.rich,
        )

    def context_service(self) -> AIContextService:
        return AIContextService(
            self.host,
            conversation_key=self.conversation_key,
            chat_id=self.chat_id,
        )

    def config_service(self) -> AIConfigService:
        return AIConfigService(
            self.host,
            chat_id=self.chat_id,
        )

    def orchestrator(self) -> AIOrchestrator:
        return AIOrchestrator(
            self.host,
            conversation_key=self.conversation_key,
        )

    def support_service(self) -> AISupportService:
        return AISupportService(self.host)

    def reply_service(self) -> AIReplyService:
        return AIReplyService(
            self.host,
            conversation_key=self.conversation_key,
            chat_id=self.chat_id,
        )

    def tool_provider(self) -> AIToolProvider:
        return AIToolProvider(
            self.host,
            conversation_key=self.conversation_key,
            chat_id=self.chat_id,
            rich=self.rich,
        )

    def build_agent(self, *, ai_config: dict[str, Any]) -> AIAgentService:
        registry_host = self.host.registry
        registry = registry_host.tool_registry(
            conversation_key=self.conversation_key,
            chat_id=self.chat_id,
            rich=self.rich,
        )
        subagents = registry_host.subagent_registry(
            conversation_key=self.conversation_key,
            chat_id=self.chat_id,
            rich=self.rich,
        )
        orchestrator = self.orchestrator()
        return AIAgentService(
            registry=registry,
            prepare_question=lambda text: orchestrator.prepare_routed_question(
                text,
                ai_config=ai_config,
            ),
            build_execution_proposal=orchestrator.build_execution_proposal,
            select_allowed_tools=lambda text: self.tool_provider().allowed_tool_names_for_question(text),
            select_subagent=lambda text: str((picked.name if (picked := subagents.pick(text)) else "")).strip(),
        )
