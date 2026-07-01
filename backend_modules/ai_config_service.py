from __future__ import annotations

from typing import TYPE_CHECKING

from .ai_assistant import apply_ai_env_overrides
from .ai_host_adapter import AIHostAdapter

if TYPE_CHECKING:
    from .telegram_commands import CommandReply, TelegramCommandService


class AIConfigService:
    def __init__(self, service: "AIHostAdapter | TelegramCommandService", *, chat_id: str = "") -> None:
        self.host = AIHostAdapter.coerce(service)
        self.chat_id = str(chat_id or "").strip()

    def load(self) -> dict:
        store = self.host.platform.read_store()
        return apply_ai_env_overrides(store.get("aiConfig"))

    def validate(self, ai_config: dict) -> "CommandReply | None":
        renderer = self.host.platform.telegram_renderer(chat_id=self.chat_id)
        if not bool(ai_config.get("enabled")):
            return renderer.ai_markdown_reply("AI 助手未启用", "请先在系统设置里启用并保存 AI 配置。")
        if not str(ai_config.get("apiKey") or "").strip() or not str(ai_config.get("baseUrl") or "").strip():
            return renderer.ai_markdown_reply("AI 配置不完整", "请先填写 Base URL、API Key 和模型名称。")
        return None
