from __future__ import annotations

import secrets
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .telegram_commands import TelegramCommandService


class AIActionHost:
    def __init__(self, service: "TelegramCommandService") -> None:
        self._service = service

    def register_pending_ai_action(self, action_id: str, action: dict[str, Any]) -> None:
        self._service._pending_ai_actions[action_id] = action
        self._cleanup_pending_ai_actions()

    def register_pending_missing_search(self, *, title: str, labels: list[str], chat_id: str) -> str:
        action_id = secrets.token_urlsafe(8)
        self._service._pending_missing_searches[action_id] = {
            "title": title,
            "labels": labels,
            "chatId": str(chat_id or "").strip(),
            "createdAt": time.time(),
        }
        self._cleanup_pending_missing_searches()
        return action_id

    def register_pending_missing_identity(self, payload: dict[str, Any]) -> str:
        action_id = secrets.token_urlsafe(8)
        action = dict(payload or {})
        action.setdefault("type", "missing_episode_identity")
        action.setdefault("createdAt", time.time())
        self._service._pending_ai_actions[action_id] = action
        self._cleanup_pending_ai_actions()
        return action_id

    def _cleanup_pending_ai_actions(self) -> None:
        now = time.time()
        expired = [
            key
            for key, value in self._service._pending_ai_actions.items()
            if now - float(value.get("createdAt") or 0) > 600
        ]
        for key in expired:
            self._service._pending_ai_actions.pop(key, None)

    def _cleanup_pending_missing_searches(self) -> None:
        now = time.time()
        expired = [
            key
            for key, value in self._service._pending_missing_searches.items()
            if now - float(value.get("createdAt") or 0) > 900
        ]
        for key in expired:
            self._service._pending_missing_searches.pop(key, None)
        overflow = len(self._service._pending_missing_searches) - 100
        if overflow > 0:
            for key in list(self._service._pending_missing_searches.keys())[:overflow]:
                self._service._pending_missing_searches.pop(key, None)
