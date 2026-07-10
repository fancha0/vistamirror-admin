from __future__ import annotations

from typing import Any

from .ai_runtime_interfaces import AIActionRuntime


class AIActionHost:
    def __init__(self, runtime: AIActionRuntime) -> None:
        self._runtime = runtime

    def register_pending_ai_action(self, action_id: str, action: dict[str, Any]) -> None:
        self._runtime.register_pending_ai_action(action_id, action)

    def register_pending_missing_search(self, *, title: str, labels: list[str], chat_id: str) -> str:
        return self._runtime.register_pending_missing_search(title=title, labels=labels, chat_id=chat_id)

    def register_pending_missing_identity(self, payload: dict[str, Any]) -> str:
        return self._runtime.register_pending_missing_identity(payload)
