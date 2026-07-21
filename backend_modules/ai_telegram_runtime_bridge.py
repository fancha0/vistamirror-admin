from __future__ import annotations

import time
from datetime import datetime
from typing import TYPE_CHECKING, Any

from .ai_assistant import apply_ai_env_overrides
from .moviepilot_config import apply_moviepilot_env_overrides

if TYPE_CHECKING:
    from .telegram_commands import TelegramCommandService
    from .telegram_message_renderer import TelegramMessageRenderer


class TelegramAIConversationRuntime:
    def __init__(self, service: "TelegramCommandService") -> None:
        self._service = service

    def get(self, conversation_key: str) -> dict[str, Any]:
        return self._service._ai_conversations.get(conversation_key)

    def get_chat_history(self, chat_id: str) -> list[dict[str, str]]:
        rows = self._service._ai_chat_history.get(chat_id)
        return list(rows) if isinstance(rows, list) else []

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


class TelegramAIActionRuntime:
    def __init__(self, service: "TelegramCommandService") -> None:
        self._service = service

    def register_pending_ai_action(self, action_id: str, action: dict[str, Any]) -> None:
        self._service._pending_ai_actions[action_id] = action
        self._cleanup_pending_ai_actions()

    def register_pending_missing_search(self, *, title: str, labels: list[str], chat_id: str) -> str:
        import secrets

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
        import secrets

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


class TelegramAIPlatformRuntime:
    def __init__(self, service: "TelegramCommandService") -> None:
        self._service = service

    @property
    def store_path(self):
        return self._service.store_path

    @property
    def event_log_path(self):
        return self._service.event_log_path

    @property
    def sender(self) -> Any:
        return self._service.sender

    def read_store(self) -> dict[str, Any]:
        from .telegram_commands import _read_store

        return _read_store(self.store_path)

    def apply_emby_env_overrides(self, raw: Any) -> dict[str, Any]:
        from .telegram_commands import _apply_emby_env_overrides

        return _apply_emby_env_overrides(raw)

    def load_ai_config(self, *, chat_id: str = "") -> dict[str, Any]:
        store = self.read_store()
        return apply_ai_env_overrides(store.get("aiConfig"))

    def load_moviepilot_config(self) -> dict[str, Any]:
        store = self.read_store()
        return apply_moviepilot_env_overrides(store.get("moviePilotConfig"))

    def build_library_stats_context(self) -> str:
        from .ai_support_service import AISupportService

        return AISupportService(self._service).build_library_stats_context()

    def truncate_text(self, text: str, limit: int) -> str:
        return self._service._truncate_text(text, limit)

    def extract_telegram_message_id(self, result: Any) -> int:
        payload = result if isinstance(result, dict) else {}
        message = payload.get("result") if isinstance(payload.get("result"), dict) else {}
        try:
            return int(message.get("message_id") or 0)
        except Exception:
            return 0

    def format_ai_event_detail(self, *, action: str, detail: dict[str, Any]) -> str:
        keys_by_action = {
            "telegram_library_scan_submitted": ("libraryName", "libraryId", "target"),
            "telegram_library_scan_failed": ("libraryName", "libraryId", "target", "error"),
            "telegram_library_scan_reply_sent": ("command",),
            "telegram_library_scan_list_ready": ("keyword", "total", "matched", "displayed"),
            "telegram_command_reply_failed": ("command", "error"),
            "telegram_ai_success": ("model", "elapsedMs", "streaming"),
            "telegram_ai_failed": ("model", "error"),
            "telegram_callback_failed": ("callback", "error"),
            "client_sync_event": ("description",),
        }
        parts: list[str] = []
        for key in keys_by_action.get(action, ()):
            value = detail.get(key)
            if value in (None, ""):
                continue
            parts.append(f"{key}={str(value)[:160]}")
        return "，".join(parts)

    def log_project_event(
        self,
        *,
        level: str = "info",
        module: str = "system",
        action: str = "",
        message: str = "",
        detail: dict[str, Any] | None = None,
    ) -> None:
        self._service._log_project_event(
            level=level,
            module=module,
            action=action,
            message=message,
            detail=detail or {},
        )

    def log_ai_media_query_diagnostic(
        self,
        *,
        question: str,
        keyword: str,
        candidates: list[str],
        detail: dict[str, Any],
    ) -> None:
        payload = {
            "question": self.truncate_text(str(question or ""), 160),
            "keyword": keyword,
            "candidates": candidates[:5],
        }
        payload.update(detail)
        self.log_project_event(
            level="info",
            module="webhook",
            action="ai_media_query_diagnostic",
            message="AI 媒体库片名识别与集数查询已完成。",
            detail=payload,
        )

    def log_ai_missing_query_diagnostic(
        self,
        *,
        question: str,
        keyword: str,
        used_context: bool,
        series_id: str,
        result: str,
        numbering_mode: str = "",
        tmdb_id: str = "",
        expected_count: int = 0,
        existing_count: int = 0,
        missing_count: int = 0,
        elapsed_ms: int = 0,
    ) -> None:
        self.log_project_event(
            level="info",
            module="webhook",
            action="ai_missing_query_diagnostic",
            message="AI 单剧缺集查询已完成。",
            detail={
                "question": self.truncate_text(str(question or ""), 160),
                "keyword": keyword,
                "usedContext": used_context,
                "embySeriesId": series_id,
                "result": result,
                "numberingMode": numbering_mode,
                "tmdbId": tmdb_id,
                "expectedCount": expected_count,
                "existingCount": existing_count,
                "missingCount": missing_count,
                "elapsedMs": elapsed_ms,
            },
        )

    def telegram_renderer(self, *, chat_id: str = "") -> "TelegramMessageRenderer":
        return self._service._telegram_renderer(chat_id=chat_id)


class TelegramAIMediaRuntime:
    def __init__(self, service: "TelegramCommandService") -> None:
        self._service = service

    @property
    def store_path(self):
        return self._service.store_path

    def emby_context(self) -> tuple[str, str]:
        return self._service._emby_context()

    def emby_get(self, path: str) -> dict[str, Any] | list[Any] | None:
        return self._service._emby_get(path)

    def get_legacy_callable(self, name: str) -> Any:
        legacy = getattr(self._service, name, None)
        original = getattr(type(self._service), name, None)
        legacy_func = getattr(legacy, "__func__", None)
        if callable(legacy) and legacy_func is not original:
            return legacy
        return None

    def log_project_event(
        self,
        *,
        level: str = "info",
        module: str = "system",
        action: str = "",
        message: str = "",
        detail: dict[str, Any] | None = None,
    ) -> None:
        self._service._log_project_event(
            level=level,
            module=module,
            action=action,
            message=message,
            detail=detail or {},
        )

    def is_library_scan_request(self, text: str) -> bool:
        return self._service._is_library_scan_request(text)

    def extract_library_scan_keyword(self, text: str) -> str:
        return self._service._extract_library_scan_keyword(text)

    def telegram_renderer(self, *, chat_id: str = "") -> "TelegramMessageRenderer":
        return self._service._telegram_renderer(chat_id=chat_id)

    def get_pending_hdhive_actions(self) -> dict[str, Any]:
        pending = getattr(self._service, "_pending_hdhive_actions", {})
        return dict(pending) if isinstance(pending, dict) else {}

    def set_pending_hdhive_actions(self, actions: dict[str, Any]) -> None:
        setattr(self._service, "_pending_hdhive_actions", dict(actions or {}))
