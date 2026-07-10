from __future__ import annotations

from typing import Any

from .ai_runtime_interfaces import AIConversationRuntime


class AIConversationHost:
    def __init__(self, runtime: AIConversationRuntime) -> None:
        self._runtime = runtime

    def get(self, conversation_key: str) -> dict[str, Any]:
        return self._runtime.get(conversation_key)

    def get_chat_history(self, chat_id: str) -> list[dict[str, str]]:
        return self._runtime.get_chat_history(chat_id)

    def get_active_media(self, conversation_key: str) -> dict[str, Any]:
        session = self.get(conversation_key)
        media = session.get("activeMedia") if isinstance(session.get("activeMedia"), dict) else {}
        return dict(media) if media else {}

    def set_active_media(self, conversation_key: str, media: dict[str, Any]) -> None:
        self._runtime.set_active_media(conversation_key, media)

    def remember(self, conversation_key: str, *, question: str, answer: str) -> None:
        self._runtime.remember(conversation_key, question=question, answer=answer)

    def clear(self, conversation_key: str) -> bool:
        return bool(self._runtime.clear(conversation_key))

    def remember_exchange(self, *, chat_id: str, question: str, answer: str) -> None:
        self._runtime.remember_exchange(chat_id=chat_id, question=question, answer=answer)

    def clear_chat_history(self, chat_id: str) -> None:
        self._runtime.clear_chat_history(chat_id)
