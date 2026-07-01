from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .telegram_commands import TelegramCommandService


class AIConversationHost:
    def __init__(self, service: "TelegramCommandService") -> None:
        self._service = service

    def get(self, conversation_key: str) -> dict[str, Any]:
        return self._service._ai_conversations.get(conversation_key)

    def get_chat_history(self, chat_id: str) -> list[dict[str, str]]:
        rows = self._service._ai_chat_history.get(chat_id)
        return list(rows) if isinstance(rows, list) else []

    def get_active_media(self, conversation_key: str) -> dict[str, Any]:
        session = self.get(conversation_key)
        media = session.get("activeMedia") if isinstance(session.get("activeMedia"), dict) else {}
        return dict(media) if media else {}

    def set_active_media(self, conversation_key: str, media: dict[str, Any]) -> None:
        self._service._ai_conversations.set_active_media(conversation_key, media)

    def remember(self, conversation_key: str, *, question: str, answer: str) -> None:
        if conversation_key:
            self._service._ai_conversations.remember(conversation_key, question=question, answer=answer)

    def clear(self, conversation_key: str) -> bool:
        return bool(self._service._ai_conversations.clear(conversation_key))

    def remember_exchange(self, *, chat_id: str, question: str, answer: str) -> None:
        safe_chat_id = str(chat_id or "").strip()
        if not safe_chat_id:
            return
        rows = self._service._ai_chat_history.setdefault(safe_chat_id, [])
        now = datetime.now().strftime("%m-%d %H:%M")
        rows.append(
            {
                "time": now,
                "user": self._service._truncate_text(str(question or "").strip(), 500),
                "assistant": self._service._truncate_text(str(answer or "").strip(), 900),
            }
        )
        self._service._ai_chat_history[safe_chat_id] = rows[-10:]

    def clear_chat_history(self, chat_id: str) -> None:
        safe_chat_id = str(chat_id or "").strip()
        if safe_chat_id:
            self._service._ai_chat_history.pop(safe_chat_id, None)
