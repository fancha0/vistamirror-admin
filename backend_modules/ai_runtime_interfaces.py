from __future__ import annotations

import pathlib
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class AIConversationRuntime(Protocol):
    def get(self, conversation_key: str) -> dict[str, Any]:
        ...

    def get_chat_history(self, chat_id: str) -> list[dict[str, str]]:
        ...

    def set_active_media(self, conversation_key: str, media: dict[str, Any]) -> None:
        ...

    def remember(self, conversation_key: str, *, question: str, answer: str) -> None:
        ...

    def clear(self, conversation_key: str) -> bool:
        ...

    def remember_exchange(self, *, chat_id: str, question: str, answer: str) -> None:
        ...

    def clear_chat_history(self, chat_id: str) -> None:
        ...


@runtime_checkable
class AIActionRuntime(Protocol):
    def register_pending_ai_action(self, action_id: str, action: dict[str, Any]) -> None:
        ...

    def register_pending_missing_search(self, *, title: str, labels: list[str], chat_id: str) -> str:
        ...

    def register_pending_missing_identity(self, payload: dict[str, Any]) -> str:
        ...


@runtime_checkable
class AIPlatformRuntime(Protocol):
    @property
    def store_path(self) -> pathlib.Path:
        ...

    @property
    def event_log_path(self) -> pathlib.Path:
        ...

    @property
    def sender(self) -> Any:
        ...

    def read_store(self) -> dict[str, Any]:
        ...

    def apply_emby_env_overrides(self, raw: Any) -> dict[str, Any]:
        ...

    def load_ai_config(self, *, chat_id: str = "") -> dict[str, Any]:
        ...

    def load_moviepilot_config(self) -> dict[str, Any]:
        ...

    def build_library_stats_context(self) -> str:
        ...

    def truncate_text(self, text: str, limit: int) -> str:
        ...

    def extract_telegram_message_id(self, result: Any) -> int:
        ...

    def format_ai_event_detail(self, *, action: str, detail: dict[str, Any]) -> str:
        ...

    def log_project_event(
        self,
        *,
        level: str = "info",
        module: str = "system",
        action: str = "",
        message: str = "",
        detail: dict[str, Any] | None = None,
    ) -> None:
        ...

    def log_ai_media_query_diagnostic(
        self,
        *,
        question: str,
        keyword: str,
        candidates: list[str],
        detail: dict[str, Any],
    ) -> None:
        ...

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
        ...

    def telegram_renderer(self, *, chat_id: str = "") -> Any:
        ...


@runtime_checkable
class AIMediaRuntime(Protocol):
    @property
    def store_path(self) -> pathlib.Path:
        ...

    def emby_context(self) -> tuple[str, str]:
        ...

    def emby_get(self, path: str) -> dict[str, Any] | list[Any] | None:
        ...

    def get_legacy_callable(self, name: str) -> Any:
        ...

    def log_project_event(
        self,
        *,
        level: str = "info",
        module: str = "system",
        action: str = "",
        message: str = "",
        detail: dict[str, Any] | None = None,
    ) -> None:
        ...

    def is_library_scan_request(self, text: str) -> bool:
        ...

    def extract_library_scan_keyword(self, text: str) -> str:
        ...

    def telegram_renderer(self, *, chat_id: str = "") -> Any:
        ...

    def get_pending_hdhive_actions(self) -> dict[str, Any]:
        ...

    def set_pending_hdhive_actions(self, actions: dict[str, Any]) -> None:
        ...


@runtime_checkable
class AIRuntimeHostProtocol(Protocol):
    platform: Any
    registry: Any
    conversations: Any
    actions: Any
    media: Any
    media_service: Any
    media_formatter: Any
