from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .ai_host_adapter import AIHostAdapter

if TYPE_CHECKING:
    from .telegram_commands import CommandReply, TelegramCommandService


class AIReplyService:
    def __init__(
        self,
        service: "AIHostAdapter | TelegramCommandService",
        *,
        conversation_key: str = "",
        chat_id: str = "",
    ) -> None:
        self.host = AIHostAdapter.coerce(service)
        self.conversation_key = str(conversation_key or "").strip()
        self.chat_id = str(chat_id or "").strip()

    def build(self, body: Any, *, title: str = "", reply_markup: dict[str, Any] | None = None) -> dict[str, Any]:
        return self.host.platform.telegram_renderer(chat_id=self.chat_id).ai_markdown_reply(
            title,
            body,
            reply_markup=reply_markup,
        )

    def coerce(self, reply: object) -> "CommandReply":
        if isinstance(reply, dict):
            return reply
        return self.build(str(reply or ""))

    def build_context_status(self, *, ai_config: dict[str, Any]) -> dict[str, Any]:
        return self.build(
            self.host.platform.format_ai_context_status(self.conversation_key, ai_config=ai_config),
            title="AI 当前上下文",
        )

    def build_context_reset(self) -> dict[str, Any]:
        existed = self.host.conversations.clear(self.conversation_key)
        self.host.conversations.clear_chat_history(self.chat_id)
        message = "已清除当前聊天的 AI 上下文。" if existed else "当前聊天没有可清除的 AI 上下文。"
        return self.build(message, title="AI 上下文")
