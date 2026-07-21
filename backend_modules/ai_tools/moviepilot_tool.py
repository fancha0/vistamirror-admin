from __future__ import annotations

import json
from typing import Any

from ..ai_host_adapter import AIHostAdapter
from ..ai_tool_base import AIToolBase, CommandReply
from ..moviepilot_config import apply_moviepilot_env_overrides
from ..moviepilot_service_adapter import MoviePilotServiceAdapter, MoviePilotServiceError


class MoviePilotTool(AIToolBase):
    """Expose only safe MoviePilot MCP capabilities to the AI runtime."""

    def __init__(self, host: AIHostAdapter, *, name: str, description: str, operation: str) -> None:
        super().__init__(name=name, description=description, schema={"type": "object", "properties": {}})
        self._host = host
        self._operation = operation

    def invoke(self, question: str) -> CommandReply:
        config = apply_moviepilot_env_overrides(self._host.platform.load_moviepilot_config())
        if not config.get("enabled"):
            return {"ok": False, "message": "MoviePilot 尚未启用，请先在 AI 配置中完成连接设置。"}
        try:
            adapter = MoviePilotServiceAdapter(config)
            if self._operation == "capabilities":
                result = adapter.capabilities()
            elif self._operation == "subscriptions":
                result = adapter.query_first_read_tool(
                    ("subscription", "subscriptions", "subscribe", "订阅", "订阅列表")
                )
            elif self._operation == "search":
                result = adapter.query_search_tool(self._search_keyword(question))
            else:
                result = adapter.query_first_read_tool(
                    ("task", "tasks", "任务", "download", "downloads", "下载", "下载任务")
                )
        except MoviePilotServiceError as exc:
            self._host.platform.log_project_event(
                level="warning",
                module="ai",
                action="moviepilot_tool_failed",
                message="MoviePilot 只读工具调用失败。",
                detail={"operation": self._operation, "error": str(exc)},
            )
            return f"MoviePilot 查询失败：{exc}"
        self._host.platform.log_project_event(
            module="ai",
            action="moviepilot_tool_completed",
            message="MoviePilot 只读工具调用完成。",
            detail={"operation": self._operation, "ok": bool(result.get("ok", True)) if isinstance(result, dict) else True},
        )
        return self._format_result(result)

    def _format_result(self, result: object) -> str:
        if not isinstance(result, dict):
            return self._format_payload(result, title=self._title)
        if not result.get("ok", True):
            return f"{self._title}查询失败：{str(result.get('message') or 'MoviePilot 未返回可用结果。')}"

        if self._operation == "capabilities":
            tools = result.get("tools") if isinstance(result.get("tools"), list) else []
            read_tools = [row for row in tools if isinstance(row, dict) and row.get("readOnly")]
            if not read_tools:
                return "MoviePilot 当前没有暴露可安全调用的只读功能。"
            lines = [
                f"MoviePilot 当前可用功能（{len(read_tools)} 项）：",
            ]
            for row in read_tools:
                name = str(row.get("name") or "未命名功能")
                description = str(row.get("description") or "只读查询")
                lines.append(f"- {name}：{description}")
            return "\n".join(lines)

        tool_name = str(result.get("tool") or "").strip()
        prefix = self._title + (f"（{tool_name}）" if tool_name else "")
        if self._operation == "subscriptions":
            return self._format_subscriptions(result.get("result"), title=prefix)
        return self._format_payload(result.get("result"), title=prefix)

    @property
    def _title(self) -> str:
        return {
            "capabilities": "MoviePilot 功能",
            "search": "MoviePilot 搜索结果",
            "subscriptions": "MoviePilot 订阅列表",
            "tasks": "MoviePilot 下载任务",
        }.get(self._operation, "MoviePilot 查询结果")

    @staticmethod
    def _search_keyword(question: str) -> str:
        """Keep the media title while discarding the explicit MP command words."""
        text = str(question or "").strip()
        if not text:
            return ""

        quoted = __import__("re").search(r"[《\"']\s*([^》\"']{1,160}?)\s*[》\"']", text)
        if quoted:
            return quoted.group(1).strip()

        cleaned = __import__("re").sub(
            r"(?i)\b(?:moviepilot|movie\s*pilot|mp)\b|"
            r"(?:请|帮我|麻烦)?\s*在?\s*MoviePilot\s*(?:里|中)?|"
            r"(?:帮我|请|麻烦)?\s*(?:搜索|搜一下|搜|查一下|查找|找一下|查询)\s*|"
            r"(?:电影|剧集|影视)?\s*(?:资源|内容)?\s*(?:一下|看看)?$",
            " ",
            text,
        )
        return " ".join(cleaned.split()).strip()

    @classmethod
    def _format_payload(cls, payload: object, *, title: str) -> str:
        text = cls._extract_text(payload)
        if text:
            return f"{title}\n\n{text.strip()}"
        if payload in (None, "", [], {}):
            return f"{title}\n\n暂无数据。"
        try:
            rendered = json.dumps(payload, ensure_ascii=False, indent=2, default=str)
        except (TypeError, ValueError):
            rendered = str(payload)
        return f"{title}\n\n{rendered}"

    @classmethod
    def _format_subscriptions(cls, payload: object, *, title: str) -> str:
        """Render MoviePilot's text-wrapped subscription JSON as a readable list."""
        # MoviePilot's REST MCP gateway wraps some tool replies in an extra
        # {success, result, error} envelope. Unwrap it before parsing so that
        # the Telegram reply never falls back to dumping the raw protocol body.
        normalized_payload = cls._unwrap_subscription_payload(payload)
        if isinstance(normalized_payload, list):
            rows = [item for item in normalized_payload if isinstance(item, dict)]
            page_summary = ""
        else:
            text = cls._extract_text(normalized_payload)
            rows, page_summary = cls._parse_subscription_rows(text)
        if not rows:
            return cls._format_payload(normalized_payload, title=title)

        lines = [title]
        if page_summary:
            lines.extend(("", page_summary))
        for index, row in enumerate(rows, start=1):
            name = str(row.get("name") or "未命名订阅").strip()
            year = str(row.get("year") or "").strip()
            media_type = str(row.get("type") or "").strip()
            season = cls._format_season(row.get("season"))
            heading = " · ".join(part for part in (name, year, media_type, season) if part)

            details: list[str] = []
            resolution = str(row.get("resolution") or "").replace("|", " / ").strip()
            quality = str(row.get("quality") or "").strip()
            if quality or resolution:
                details.append(" · ".join(part for part in (quality, resolution) if part))

            total = cls._as_positive_int(row.get("total_episode"))
            start = cls._as_nonnegative_int(row.get("start_episode"))
            lacking = cls._as_nonnegative_int(row.get("lack_episode"))
            episode_parts: list[str] = []
            if total is not None:
                episode_parts.append(f"共 {total} 集")
            if start is not None and start > 0:
                episode_parts.append(f"从 E{start} 开始订阅")
            if lacking is not None:
                episode_parts.append(f"缺 {lacking} 集")
            if episode_parts:
                details.append(" · ".join(episode_parts))

            state = cls._format_subscription_state(row.get("state"))
            updated_at = str(row.get("last_update") or "").strip()
            status_parts = [part for part in (state, f"更新：{updated_at}" if updated_at else "") if part]
            if status_parts:
                details.append(" · ".join(status_parts))

            lines.append(f"{index}. {heading}")
            lines.extend(f"   {detail}" for detail in details)

        return "\n".join(lines)

    @classmethod
    def _unwrap_subscription_payload(cls, payload: object) -> object:
        """Return the actual subscription response from common MCP envelopes."""
        current = payload
        for _ in range(3):
            if isinstance(current, str):
                value = current.strip()
                if not value or value[0] not in "[{":
                    return current
                try:
                    decoded = json.loads(value)
                except (TypeError, ValueError):
                    return current
                current = decoded
                continue

            if not isinstance(current, dict):
                return current
            nested = current.get("result")
            if nested is None:
                return current
            current = nested
        return current

    @staticmethod
    def _format_season(value: object) -> str:
        season = MoviePilotTool._as_positive_int(value)
        return f"第 {season} 季" if season is not None else ""

    @staticmethod
    def _as_positive_int(value: object) -> int | None:
        try:
            number = int(value)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return None
        return number if number > 0 else None

    @staticmethod
    def _as_nonnegative_int(value: object) -> int | None:
        try:
            number = int(value)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return None
        return number if number >= 0 else None

    @staticmethod
    def _format_subscription_state(value: object) -> str:
        state = str(value or "").strip().upper()
        return {"R": "🟢 已启用", "S": "🟡 已暂停"}.get(state, f"状态：{state}" if state else "")

    @staticmethod
    def _parse_subscription_rows(text: str) -> tuple[list[dict[str, Any]], str]:
        """Extract the JSON list returned inside MoviePilot's MCP text result."""
        if not text:
            return [], ""
        start = text.find("[")
        if start < 0:
            return [], ""
        try:
            parsed = json.loads(text[start:])
        except (TypeError, ValueError):
            return [], ""
        if not isinstance(parsed, list):
            return [], ""
        rows = [row for row in parsed if isinstance(row, dict)]
        return rows, text[:start].strip()

    @classmethod
    def _extract_text(cls, payload: object) -> str:
        """Read standard MCP text blocks before falling back to structured JSON."""
        if isinstance(payload, str):
            return payload
        if not isinstance(payload, dict):
            return ""

        content = payload.get("content")
        if isinstance(content, list):
            parts = [
                str(row.get("text") or "").strip()
                for row in content
                if isinstance(row, dict) and str(row.get("type") or "text") == "text"
            ]
            text = "\n".join(part for part in parts if part)
            if text:
                return text

        for key in ("text", "message", "markdown"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value
        return ""
