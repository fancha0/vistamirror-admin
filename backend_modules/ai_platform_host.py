from __future__ import annotations

import pathlib
import re
from typing import TYPE_CHECKING, Any

from .ai_conversation_host import AIConversationHost
from .ai_runtime_interfaces import AIPlatformRuntime

if TYPE_CHECKING:
    from .telegram_message_renderer import TelegramMessageRenderer


class AIPlatformHost:
    def __init__(self, runtime: AIPlatformRuntime, *, conversations: AIConversationHost) -> None:
        self._runtime = runtime
        self._conversations = conversations

    @property
    def store_path(self) -> pathlib.Path:
        return self._runtime.store_path

    @property
    def event_log_path(self) -> pathlib.Path:
        return self._runtime.event_log_path

    @property
    def sender(self) -> Any:
        return self._runtime.sender

    def read_store(self) -> dict[str, Any]:
        return self._runtime.read_store()

    def apply_emby_env_overrides(self, raw: Any) -> dict[str, Any]:
        return self._runtime.apply_emby_env_overrides(raw)

    def load_ai_config(self, *, chat_id: str = "") -> dict[str, Any]:
        return self._runtime.load_ai_config(chat_id=chat_id)

    def load_moviepilot_config(self) -> dict[str, Any]:
        return self._runtime.load_moviepilot_config()

    def build_library_stats_context(self) -> str:
        return self._runtime.build_library_stats_context()

    def limit_ai_context_text(self, text: str, *, ai_config: dict[str, Any] | None = None) -> str:
        config = ai_config if isinstance(ai_config, dict) else {}
        try:
            context_tokens_k = int(config.get("contextTokensK") or 64)
        except (TypeError, ValueError):
            context_tokens_k = 64
        context_tokens_k = max(4, min(1024, context_tokens_k))
        reserved_tokens = max(2048, int(config.get("maxTokens") or 800) + 1200)
        max_tokens = max(2000, context_tokens_k * 1000 - reserved_tokens)
        value = str(text or "")
        if self.estimate_ai_tokens(value) <= max_tokens:
            return value
        low = 0
        high = len(value)
        while low < high:
            middle = (low + high + 1) // 2
            if self.estimate_ai_tokens(value[-middle:]) <= max_tokens:
                low = middle
            else:
                high = middle - 1
        tail = value[-low:] if low > 0 else ""
        return "[较早上下文已按 Token 预算压缩]\n" + tail

    def estimate_ai_tokens(self, text: str) -> int:
        value = str(text or "")
        cjk = len(re.findall(r"[\u3400-\u9fff\uf900-\ufaff]", value))
        other = max(0, len(value) - cjk)
        return cjk + (other + 3) // 4

    def is_ai_context_limit_error(self, error: Any) -> bool:
        text = str(error or "").lower()
        return any(
            marker in text
            for marker in (
                "context length",
                "context_length",
                "maximum context",
                "too many tokens",
                "token limit",
                "上下文超限",
            )
        )

    def shrink_ai_messages(self, messages: list[dict[str, str]]) -> list[dict[str, str]]:
        output: list[dict[str, str]] = []
        for message in messages:
            row = dict(message)
            content = str(row.get("content") or "")
            if row.get("role") == "user" and len(content) > 2000:
                keep = max(1000, len(content) // 2)
                row["content"] = "[较早上下文已因模型限制缩减]\n" + content[-keep:]
            output.append(row)
        return output

    def is_ai_context_status_request(self, question: str) -> bool:
        return bool(re.fullmatch(r"(?:/ai\s*)?(?:当前上下文|查看上下文|上下文状态)", str(question or "").strip()))

    def is_ai_context_reset_request(self, question: str) -> bool:
        return bool(re.fullmatch(r"(?:/ai\s*)?(?:重置上下文|清除上下文|忘记当前话题)", str(question or "").strip()))

    def format_ai_context_status(self, conversation_key: str, *, ai_config: dict[str, Any]) -> str:
        session = self._conversations.get(conversation_key)
        media = session.get("activeMedia") if isinstance(session.get("activeMedia"), dict) else {}
        recent = session.get("recent") if isinstance(session.get("recent"), list) else []
        summary = str(session.get("summary") or "")
        context_lines: list[str] = []
        for row in recent:
            if not isinstance(row, dict):
                continue
            context_lines.append(f"- {row.get('time', '')} 用户：{row.get('user', '')}")
            context_lines.append(f"  AI：{row.get('assistant', '')}")
        context_text = "\n".join(context_lines)
        return "\n".join(
            [
                f"当前作品：{media.get('title') or '未设置'}",
                f"已保存问答：{len(recent)} 轮",
                f"长期摘要：{'已生成' if summary else '暂无'}",
                f"当前记忆估算：{self.estimate_ai_tokens(context_text)} Token",
                f"配置上限：{int(ai_config.get('contextTokensK') or 64)}K Token",
            ]
        )

    def truncate_text(self, text: str, limit: int) -> str:
        return self._runtime.truncate_text(text, limit)

    def extract_telegram_message_id(self, result: Any) -> int:
        return self._runtime.extract_telegram_message_id(result)

    def format_ai_event_detail(self, *, action: str, detail: dict[str, Any]) -> str:
        return self._runtime.format_ai_event_detail(action=action, detail=detail)

    def log_project_event(
        self,
        *,
        level: str = "info",
        module: str = "system",
        action: str = "",
        message: str = "",
        detail: dict[str, Any] | None = None,
    ) -> None:
        self._runtime.log_project_event(
            level=level,
            module=module,
            action=action,
            message=message,
            detail=detail,
        )

    def log_ai_media_query_diagnostic(
        self,
        *,
        question: str,
        keyword: str,
        candidates: list[str],
        detail: dict[str, Any],
    ) -> None:
        self._runtime.log_ai_media_query_diagnostic(
            question=question,
            keyword=keyword,
            candidates=candidates,
            detail=detail,
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
        self._runtime.log_ai_missing_query_diagnostic(
            question=question,
            keyword=keyword,
            used_context=used_context,
            series_id=series_id,
            result=result,
            numbering_mode=numbering_mode,
            tmdb_id=tmdb_id,
            expected_count=expected_count,
            existing_count=existing_count,
            missing_count=missing_count,
            elapsed_ms=elapsed_ms,
        )

    def telegram_renderer(self, *, chat_id: str = "") -> "TelegramMessageRenderer":
        return self._runtime.telegram_renderer(chat_id=chat_id)
