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

if TYPE_CHECKING:
    from .telegram_commands import TelegramCommandService


class AIHostAdapter:
    def __init__(self, service: "TelegramCommandService") -> None:
        self._service = service
        self.platform = AIPlatformHost(service)
        self.registry = AIRegistryHost(service)
        self.conversations = AIConversationHost(service)
        self.actions = AIActionHost(service)
        self.media = AIMediaHost(service)
        self.media_service = AIMediaServiceAdapter(service)
        self.media_formatter = AIMediaFormatter()

    @classmethod
    def coerce(cls, host_or_service: "AIHostAdapter | TelegramCommandService") -> "AIHostAdapter":
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
