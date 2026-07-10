from __future__ import annotations

import pathlib
from typing import TYPE_CHECKING, Any

from .ai_action_host import AIActionHost
from .ai_conversation_host import AIConversationHost
from .ai_media_formatter import AIMediaFormatter
from .ai_media_host import AIMediaHost
from .ai_media_service_adapter import AIMediaServiceAdapter
from .ai_platform_host import AIPlatformHost
from .ai_registry_host import AIRegistryHost
from .ai_runtime_interfaces import AIRuntimeHostProtocol
from .ai_telegram_runtime_bridge import (
    TelegramAIActionRuntime,
    TelegramAIConversationRuntime,
    TelegramAIMediaRuntime,
    TelegramAIPlatformRuntime,
)

if TYPE_CHECKING:
    from .telegram_commands import TelegramCommandService


class AIHostAdapter:
    def __init__(self, host_or_service: "AIRuntimeHostProtocol | TelegramCommandService") -> None:
        if isinstance(host_or_service, AIRuntimeHostProtocol):
            self._service = getattr(host_or_service, "_service", None)
            self.platform = host_or_service.platform
            self.registry = host_or_service.registry
            self.conversations = host_or_service.conversations
            self.actions = host_or_service.actions
            self.media = host_or_service.media
            self.media_service = host_or_service.media_service
            self.media_formatter = host_or_service.media_formatter
            return

        service = host_or_service
        self._service = service
        conversations = AIConversationHost(TelegramAIConversationRuntime(service))
        self.platform = AIPlatformHost(
            TelegramAIPlatformRuntime(service),
            conversations=conversations,
        )
        self.registry = AIRegistryHost(service)
        self.conversations = conversations
        self.actions = AIActionHost(TelegramAIActionRuntime(service))
        self.media = AIMediaHost()
        self.media_service = AIMediaServiceAdapter(TelegramAIMediaRuntime(service))
        self.media_formatter = AIMediaFormatter()

    @classmethod
    def coerce(cls, host_or_service: "AIHostAdapter | AIRuntimeHostProtocol | TelegramCommandService") -> "AIHostAdapter":
        if isinstance(host_or_service, cls):
            return host_or_service
        return cls(host_or_service)

    @property
    def store_path(self) -> pathlib.Path:
        return self.platform.store_path

    @property
    def event_log_path(self) -> pathlib.Path:
        return self.platform.event_log_path

    @property
    def sender(self) -> Any:
        return self.platform.sender
