from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import json
import logging
import os
import pathlib
import re
import secrets
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from typing import Any, Callable, Union

from .ai_assistant import apply_ai_env_overrides, chat_completion, normalize_ai_config, stream_chat_completion
from .ai_conversation_store import AiConversationStore
from .ai_intent_router import AiIntentRouter
from .drive115_service import Drive115Service, apply_drive115_env_overrides, default_drive115_config, extract_115_share, normalize_drive115_config
from .hdhive_service import HDHiveError, HDHiveService, apply_hdhive_env_overrides, default_hdhive_config, normalize_hdhive_config
from .media_identity_service import MediaIdentityService
from .missing_episode_service import MissingEpisodeService
from .notification_config import normalize_bot_config
from .playback_event_logger import read_recent_playback_events
from .playback_history_service import PlaybackHistoryService
from .project_event_logger import read_project_events
from .telegram_sender import TelegramSender

COMMAND_MENU: list[dict[str, str]] = [
    {"command": "sousuo", "description": "🔍 搜索资源"},
    {"command": "ribaoday", "description": "📊 今日日报"},
    {"command": "zoubaoday", "description": "📅 本周周报"},
    {"command": "yuebaoday", "description": "🗓 本月月报"},
    {"command": "niandu", "description": "📜 年度总结"},
    {"command": "zhengzaibofang", "description": "🟢 正在播放"},
    {"command": "zuijinruku", "description": "🆕 最近入库"},
    {"command": "zuijinbofangjilu", "description": "📜 最近播放记录"},
    {"command": "saomiao", "description": "🔄 扫描媒体库"},
    {"command": "zhuancun115", "description": "📦 115 链接转存"},
    {"command": "hdhive", "description": "🪺 影巢资源搜索"},
    {"command": "ai", "description": "🧠 AI 媒体问答"},
    {"command": "help", "description": "🤖 帮助菜单"},
    {"command": "start", "description": "🚀 启动机器人"},
    {"command": "check", "description": "📡 系统探针"},
]

LOGGER = logging.getLogger(__name__)
CommandReply = Union[str, dict[str, Any]]
DEFAULT_EMBY_CLIENT_NAME = "镜界Vistamirror User Console"
MARKDOWN_V2_SPECIALS = r"_*[]()~`>#+-=|{}.!"
AI_TOOL_REGISTRY: list[dict[str, Any]] = [
    {"name": "media.identity", "kind": "read", "description": "通过 TMDB 与 Emby ProviderIds 确认作品身份。"},
    {"name": "media.detail", "kind": "read", "description": "查询作品简介、演员、评分和本地入库状态。"},
    {"name": "series.inventory", "kind": "read", "description": "查询 Emby 实际单集、最新集和缺失集。"},
    {"name": "library.search", "kind": "read", "description": "查询媒体库资源、简介、评分、季集数和分类列表。"},
    {"name": "playback.history", "kind": "read", "description": "查询最近播放、用户观看和播放排行。"},
    {"name": "library.latest", "kind": "read", "description": "查询最近入库资源。"},
    {"name": "playback.now", "kind": "read", "description": "查询当前正在播放。"},
    {"name": "missing.summary", "kind": "read", "description": "查询缺集巡检缓存摘要。"},
    {"name": "tasks.status", "kind": "read", "description": "查询任务中心状态。"},
    {"name": "logs.summary", "kind": "read", "description": "查询系统日志摘要。"},
    {"name": "invites.summary", "kind": "read", "description": "查询邀请码和注册入口状态。"},
    {"name": "users.summary", "kind": "read", "description": "查询 Emby 用户摘要。"},
    {"name": "ranking.annual", "kind": "read", "description": "查询年度/近期播放排行摘要。"},
    {"name": "quality.summary", "kind": "read", "description": "查询媒体质量、分辨率和编码摘要。"},
    {"name": "risk.summary", "kind": "read", "description": "查询异常日志、失败任务和风险概览。"},
    {"name": "clients.summary", "kind": "read", "description": "查询客户端、设备和在线会话。"},
    {"name": "settings.summary", "kind": "read", "description": "查询系统、机器人、AI 配置摘要，仅返回脱敏状态。"},
    {"name": "tasks.run", "kind": "confirm", "description": "确认后运行 Emby 计划任务。"},
    {"name": "missing.scan", "kind": "confirm", "description": "确认后触发缺集巡检。"},
    {"name": "invites.generate", "kind": "confirm", "description": "确认后生成邀请码。"},
    {"name": "invites.sync", "kind": "confirm", "description": "确认后同步邀请码状态摘要。"},
    {"name": "bot.status", "kind": "confirm", "description": "确认后刷新机器人状态摘要。"},
]
EMBY_ENV_FIELD_MAP: dict[str, str] = {
    "serverUrl": "APP_EMBY_SERVER_URL",
    "apiKey": "APP_EMBY_API_KEY",
    "clientName": "APP_EMBY_CLIENT_NAME",
    "tmdbToken": "APP_TMDB_TOKEN",
}


def _read_store(store_path: pathlib.Path) -> dict[str, Any]:
    if not store_path.exists():
        return {
            "embyConfig": {},
            "invites": [],
            "botConfig": normalize_bot_config({}),
            "aiConfig": normalize_ai_config({}),
            "drive115Config": default_drive115_config(),
            "hdhiveConfig": default_hdhive_config(),
        }
    try:
        data = json.loads(store_path.read_text(encoding="utf-8"))
    except Exception:
        data = {}
    if not isinstance(data, dict):
        data = {}
    return {
        "embyConfig": data.get("embyConfig") if isinstance(data.get("embyConfig"), dict) else {},
        "invites": data.get("invites") if isinstance(data.get("invites"), list) else [],
        "botConfig": normalize_bot_config(data.get("botConfig")),
        "aiConfig": normalize_ai_config(data.get("aiConfig")),
        "drive115Config": normalize_drive115_config(data.get("drive115Config")),
        "hdhiveConfig": normalize_hdhive_config(data.get("hdhiveConfig")),
    }


def _help_text() -> str:
    lines = ["🤖 镜界 Vistamirror Bot 帮助菜单", ""]
    lines.extend([f"/{row['command']} - {row['description']}" for row in COMMAND_MENU])
    return "\n".join(lines)


def _escape_markdown_v2(text: Any) -> str:
    value = str(text or "")
    return "".join(f"\\{char}" if char in MARKDOWN_V2_SPECIALS else char for char in value)


def _escape_markdown_v2_code(text: Any) -> str:
    value = str(text or "")
    return value.replace("\\", "\\\\").replace("`", "\\`")


def _format_copy_block(title: str, body: Any) -> str:
    safe_title = _escape_markdown_v2(title)
    safe_body = _escape_markdown_v2_code(body)
    return f"*{safe_title}*\n```text\n{safe_body}\n```"


def _apply_emby_env_overrides(raw: Any) -> dict[str, Any]:
    source = raw if isinstance(raw, dict) else {}
    merged = {
        "serverUrl": str(source.get("serverUrl") or "").strip(),
        "apiKey": str(source.get("apiKey") or "").strip(),
        "clientName": str(source.get("clientName") or "").strip() or DEFAULT_EMBY_CLIENT_NAME,
        "tmdbEnabled": bool(source.get("tmdbEnabled")),
        "tmdbToken": str(source.get("tmdbToken") or "").strip(),
        "tmdbLanguage": str(source.get("tmdbLanguage") or "zh-CN").strip() or "zh-CN",
        "tmdbRegion": str(source.get("tmdbRegion") or "CN").strip().upper() or "CN",
        "updatedAt": str(source.get("updatedAt") or "").strip(),
    }
    for field, env_name in EMBY_ENV_FIELD_MAP.items():
        env_value = str(os.environ.get(env_name) or "").strip()
        if env_value:
            merged[field] = env_value
    legacy_tmdb_token = str(os.environ.get("TMDB_TOKEN") or "").strip()
    if legacy_tmdb_token and not str(os.environ.get("APP_TMDB_TOKEN") or "").strip():
        merged["tmdbToken"] = legacy_tmdb_token
    if merged["tmdbToken"] and (legacy_tmdb_token or str(os.environ.get("APP_TMDB_TOKEN") or "").strip()):
        merged["tmdbEnabled"] = True
    return merged


class TelegramCommandService:
    def __init__(
        self,
        *,
        store_path: pathlib.Path,
        event_log_path: pathlib.Path,
        event_logger: Callable[..., None] | None = None,
    ) -> None:
        self.store_path = store_path
        self.event_log_path = event_log_path
        self.event_logger = event_logger
        self.sender = TelegramSender()
        self._stop_event = threading.Event()
        self._wakeup_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._offset = 0
        self._last_token = ""
        self._commands_registered_token = ""
        self._pending_ai_actions: dict[str, dict[str, Any]] = {}
        self._pending_drive115_transfers: dict[str, dict[str, Any]] = {}
        self._pending_hdhive_actions: dict[str, dict[str, Any]] = {}
        self._pending_missing_searches: dict[str, dict[str, Any]] = {}
        self._ai_chat_history: dict[str, list[dict[str, str]]] = {}
        self._ai_conversations = AiConversationStore(self.store_path.parent / "ai_conversations.json")
        self._telegram_dedupe_lock = threading.Lock()
        self._recent_update_ids: dict[str, float] = {}
        self._recent_ai_message_keys: dict[str, float] = {}
        self._library_thread: threading.Thread | None = None
        self._library_state_lock = threading.Lock()
        self._library_notify_lock = threading.RLock()
        self._library_state_path = self.store_path.parent / "library_notification_state.json"
        self._library_group_window_seconds = 60

    def _log_project_event(
        self,
        *,
        level: str = "info",
        module: str = "system",
        action: str = "",
        message: str = "",
        detail: Any = None,
    ) -> None:
        if not self.event_logger:
            return
        try:
            self.event_logger(
                level=level,
                module=module,
                action=action,
                message=message,
                detail=detail if detail is not None else {},
            )
        except Exception:
            return

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run_loop, name="TelegramCommandService", daemon=True)
        self._thread.start()
        self._library_thread = threading.Thread(
            target=self._run_library_notification_loop,
            name="LibraryNotificationMonitor",
            daemon=True,
        )
        self._library_thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._wakeup_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)
        if self._library_thread and self._library_thread.is_alive():
            self._library_thread.join(timeout=2.0)

    def wakeup(self) -> None:
        self._wakeup_event.set()

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            store = _read_store(self.store_path)
            bot = store["botConfig"]
            token = str(bot.get("telegramToken") or "").strip()
            if not token or not bool(bot.get("enableCommands", True)):
                self._wait_tick(5.0)
                continue
            if token != self._last_token:
                self._offset = 0
                self._last_token = token
                self._commands_registered_token = ""

            if self._commands_registered_token != token:
                try:
                    self.sender.set_my_commands(token=token, commands=COMMAND_MENU)
                    self._commands_registered_token = token
                except Exception:
                    self._wait_tick(5.0)
                    continue

            try:
                updates = self.sender.get_updates(token=token, offset=max(1, self._offset + 1), timeout_seconds=20)
            except Exception:
                self._wait_tick(3.0)
                continue
            for update in updates:
                update_id = int(update.get("update_id") or 0)
                if update_id > self._offset:
                    self._offset = update_id
                self._handle_update(update, token)

    def _wait_tick(self, seconds: float) -> None:
        self._wakeup_event.wait(timeout=max(0.2, seconds))
        self._wakeup_event.clear()

    def _run_library_notification_loop(self) -> None:
        next_poll_at = 0.0
        while not self._stop_event.is_set():
            now_ts = time.time()
            try:
                if now_ts >= next_poll_at:
                    self._poll_library_notifications_once()
                    next_poll_at = time.time() + 60.0
            except Exception as err:
                LOGGER.warning("Library notification poll failed: %s", err)
                self._log_project_event(
                    level="warning",
                    module="webhook",
                    action="library_notification_poll_failed",
                    message="新入库定时检测失败，将在下一轮重试。",
                    detail={"error": str(err)},
                )
            try:
                self._flush_library_notification_groups_due()
            except Exception as err:
                LOGGER.warning("Library notification flush failed: %s", err)
                self._log_project_event(
                    level="warning",
                    module="webhook",
                    action="library_notification_group_flush_failed",
                    message="新入库聚合通知发送失败，将在下一轮重试。",
                    detail={"error": str(err)},
                )
            self._wait_tick(2.0)

    def _library_notification_config(self) -> tuple[dict[str, Any], dict[str, Any]]:
        store = _read_store(self.store_path)
        return normalize_bot_config(store.get("botConfig")), _apply_emby_env_overrides(store.get("embyConfig"))

    def _read_library_notification_state(self) -> dict[str, Any]:
        with self._library_state_lock:
            try:
                raw = json.loads(self._library_state_path.read_text(encoding="utf-8"))
            except Exception:
                raw = {}
            seen = raw.get("seen") if isinstance(raw.get("seen"), dict) else {}
            pending_raw = raw.get("pendingSeries") if isinstance(raw.get("pendingSeries"), dict) else {}
            pending: dict[str, dict[str, Any]] = {}
            for key, value in pending_raw.items():
                if not str(key).strip() or not isinstance(value, dict):
                    continue
                items = value.get("items") if isinstance(value.get("items"), list) else []
                normalized_items = []
                for row in items:
                    if isinstance(row, dict) and str(row.get("Id") or "").strip():
                        normalized_items.append(dict(row))
                if not normalized_items:
                    continue
                pending[str(key)] = {
                    "seriesId": str(value.get("seriesId") or key).strip(),
                    "seriesName": str(value.get("seriesName") or "").strip(),
                    "firstSeenAt": str(value.get("firstSeenAt") or ""),
                    "lastSeenAt": str(value.get("lastSeenAt") or ""),
                    "sources": [str(source).strip() for source in value.get("sources", []) if str(source).strip()],
                    "items": normalized_items,
                }
            return {
                "version": 1,
                "active": bool(raw.get("active")),
                "lastPollAt": str(raw.get("lastPollAt") or ""),
                "seen": {str(key): str(value or "") for key, value in seen.items() if str(key).strip()},
                "pendingSeries": pending,
            }

    def _write_library_notification_state(self, state: dict[str, Any]) -> None:
        with self._library_state_lock:
            self._library_state_path.parent.mkdir(parents=True, exist_ok=True)
            seen = state.get("seen") if isinstance(state.get("seen"), dict) else {}
            if len(seen) > 5000:
                ordered = sorted(seen.items(), key=lambda row: str(row[1] or ""), reverse=True)[:5000]
                state["seen"] = dict(ordered)
            pending_raw = state.get("pendingSeries") if isinstance(state.get("pendingSeries"), dict) else {}
            pending: dict[str, dict[str, Any]] = {}
            for key, value in pending_raw.items():
                if not str(key).strip() or not isinstance(value, dict):
                    continue
                items = value.get("items") if isinstance(value.get("items"), list) else []
                normalized_items = []
                seen_item_ids: set[str] = set()
                for row in items:
                    if not isinstance(row, dict):
                        continue
                    item_id = str(row.get("Id") or "").strip()
                    if not item_id or item_id in seen_item_ids:
                        continue
                    seen_item_ids.add(item_id)
                    normalized_items.append(dict(row))
                if not normalized_items:
                    continue
                pending[str(key)] = {
                    "seriesId": str(value.get("seriesId") or key).strip(),
                    "seriesName": str(value.get("seriesName") or "").strip(),
                    "firstSeenAt": str(value.get("firstSeenAt") or ""),
                    "lastSeenAt": str(value.get("lastSeenAt") or ""),
                    "sources": [str(source).strip() for source in value.get("sources", []) if str(source).strip()],
                    "items": normalized_items,
                }
            state["pendingSeries"] = pending
            temp_path = self._library_state_path.with_suffix(".tmp")
            temp_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
            temp_path.replace(self._library_state_path)

    def _set_library_monitor_active(self, active: bool) -> None:
        state = self._read_library_notification_state()
        if bool(state.get("active")) == bool(active):
            return
        state["active"] = bool(active)
        state["lastPollAt"] = datetime.now().isoformat(timespec="seconds")
        self._write_library_notification_state(state)

    def _fetch_library_latest_rows(self, limit: int = 200) -> list[dict[str, Any]]:
        fields = ",".join(
            (
                "Name",
                "Type",
                "SeriesName",
                "SeriesId",
                "ParentIndexNumber",
                "IndexNumber",
                "DateCreated",
                "ProductionYear",
                "CommunityRating",
                "Overview",
                "Genres",
                "People",
                "Status",
                "MediaSources",
                "MediaStreams",
                "Width",
                "Height",
                "ImageTags",
                "PrimaryImageItemId",
            )
        )
        query = urllib.parse.urlencode(
            {
                "Recursive": "true",
                "IncludeItemTypes": "Movie,Episode",
                "SortBy": "DateCreated",
                "SortOrder": "Descending",
                "Limit": str(max(1, min(500, int(limit or 200)))),
                "Fields": fields,
            }
        )
        payload = self._emby_get(f"/Items?{query}")
        rows = payload.get("Items") if isinstance(payload, dict) else payload
        return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []

    @staticmethod
    def _library_notification_now_iso() -> str:
        return datetime.now().isoformat(timespec="seconds")

    @staticmethod
    def _library_notification_group_key(payload: dict[str, Any]) -> str:
        if str(payload.get("Type") or "").strip().lower() != "episode":
            return ""
        return str(payload.get("SeriesId") or "").strip()

    def _buffer_library_episode_group(
        self,
        *,
        state: dict[str, Any],
        payload: dict[str, Any],
        source: str,
    ) -> dict[str, Any]:
        item_id = str(payload.get("Id") or "").strip()
        series_id = self._library_notification_group_key(payload)
        if not item_id:
            return {"ok": False, "status": "missing_item_id"}
        if item_id in state["seen"]:
            return {"ok": True, "status": "duplicate"}
        if not series_id:
            return {"ok": True, "status": "single_fallback"}
        pending = state.get("pendingSeries") if isinstance(state.get("pendingSeries"), dict) else {}
        group = pending.get(series_id) if isinstance(pending.get(series_id), dict) else {}
        items = group.get("items") if isinstance(group.get("items"), list) else []
        if any(str(row.get("Id") or "").strip() == item_id for row in items if isinstance(row, dict)):
            return {"ok": True, "status": "buffered_duplicate", "seriesId": series_id}
        now = self._library_notification_now_iso()
        normalized = dict(payload)
        group = {
            "seriesId": series_id,
            "seriesName": str(payload.get("SeriesName") or group.get("seriesName") or "").strip(),
            "firstSeenAt": str(group.get("firstSeenAt") or now),
            "lastSeenAt": now,
            "sources": sorted(
                {
                    *(str(source_name).strip() for source_name in group.get("sources", []) if str(source_name).strip()),
                    str(source or "").strip() or "webhook",
                }
            ),
            "items": [*items, normalized],
        }
        pending[series_id] = group
        state["pendingSeries"] = pending
        self._log_project_event(
            level="info",
            module="webhook",
            action="library_notification_group_buffered",
            message="新入库剧集已加入聚合缓冲区。",
            detail={
                "seriesId": series_id,
                "seriesName": group["seriesName"],
                "itemId": item_id,
                "episodeCount": len(group["items"]),
                "source": source,
            },
        )
        return {"ok": True, "status": "buffered", "seriesId": series_id, "episodeCount": len(group["items"])}

    @staticmethod
    def _library_notification_iso_to_ts(raw: Any) -> float:
        value = str(raw or "").strip()
        if not value:
            return 0.0
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        except Exception:
            return 0.0

    def _collect_due_library_groups(
        self,
        *,
        state: dict[str, Any],
        force_series_ids: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        pending = state.get("pendingSeries") if isinstance(state.get("pendingSeries"), dict) else {}
        if not pending:
            return []
        due: list[dict[str, Any]] = []
        now_ts = time.time()
        forced = force_series_ids or set()
        for series_id, group in list(pending.items()):
            if not isinstance(group, dict):
                continue
            items = group.get("items") if isinstance(group.get("items"), list) else []
            if not items:
                pending.pop(series_id, None)
                continue
            last_seen_ts = self._library_notification_iso_to_ts(group.get("lastSeenAt"))
            has_poll_source = "poll" in {str(value).strip() for value in group.get("sources", []) if str(value).strip()}
            should_flush = (
                series_id in forced
                or has_poll_source
                or (last_seen_ts > 0 and now_ts - last_seen_ts >= float(self._library_group_window_seconds))
            )
            if should_flush:
                due.append(dict(group))
                pending.pop(series_id, None)
        state["pendingSeries"] = pending
        return due

    def _flush_library_notification_groups_due(self) -> None:
        with self._library_notify_lock:
            state = self._read_library_notification_state()
            due_groups = self._collect_due_library_groups(state=state)
            if not due_groups:
                return
            self._write_library_notification_state(state)
            self._flush_library_notification_groups_unlocked(due_groups)

    def _flush_library_notification_groups_unlocked(self, groups: list[dict[str, Any]]) -> None:
        if not groups:
            return
        for index, group in enumerate(groups):
            try:
                self._send_library_notification_group_unlocked(group)
            except Exception as err:
                self._restore_library_notification_group(group)
                self._log_project_event(
                    level="warning",
                    module="webhook",
                    action="library_notification_group_flush_failed",
                    message="剧集聚合通知发送失败，该分组将在下一轮重试。",
                    detail={
                        "seriesId": str(group.get("seriesId") or ""),
                        "seriesName": str(group.get("seriesName") or ""),
                        "episodeCount": len(group.get("items") or []),
                        "error": str(err),
                    },
                )
            if index + 1 < len(groups):
                self._stop_event.wait(0.75)

    def _restore_library_notification_group(self, group: dict[str, Any]) -> None:
        series_id = str(group.get("seriesId") or "").strip()
        items = group.get("items") if isinstance(group.get("items"), list) else []
        if not series_id or not items:
            return
        state = self._read_library_notification_state()
        pending = state.get("pendingSeries") if isinstance(state.get("pendingSeries"), dict) else {}
        existing = pending.get(series_id) if isinstance(pending.get(series_id), dict) else {}
        existing_items = existing.get("items") if isinstance(existing.get("items"), list) else []
        existing_ids = {str(row.get("Id") or "").strip() for row in existing_items if isinstance(row, dict)}
        merged_items = [*existing_items]
        for row in items:
            if not isinstance(row, dict):
                continue
            item_id = str(row.get("Id") or "").strip()
            if not item_id or item_id in existing_ids:
                continue
            existing_ids.add(item_id)
            merged_items.append(dict(row))
        pending[series_id] = {
            "seriesId": series_id,
            "seriesName": str(existing.get("seriesName") or group.get("seriesName") or "").strip(),
            "firstSeenAt": str(existing.get("firstSeenAt") or group.get("firstSeenAt") or self._library_notification_now_iso()),
            "lastSeenAt": str(group.get("lastSeenAt") or existing.get("lastSeenAt") or self._library_notification_now_iso()),
            "sources": sorted(
                {
                    *(str(value).strip() for value in existing.get("sources", []) if str(value).strip()),
                    *(str(value).strip() for value in group.get("sources", []) if str(value).strip()),
                }
            ),
            "items": merged_items,
        }
        state["pendingSeries"] = pending
        self._write_library_notification_state(state)

    def _poll_library_notifications_once(self) -> None:
        with self._library_notify_lock:
            self._poll_library_notifications_unlocked()

    def _poll_library_notifications_unlocked(self) -> None:
        bot, emby = self._library_notification_config()
        enabled = bool(bot.get("enableCore", True) and bot.get("enableLibrary", True))
        configured = bool(
            str(bot.get("telegramToken") or "").strip()
            and str(bot.get("telegramChatId") or "").strip()
            and str(emby.get("serverUrl") or "").strip()
            and str(emby.get("apiKey") or "").strip()
        )
        if not enabled or not configured:
            self._set_library_monitor_active(False)
            return

        rows = self._fetch_library_latest_rows(limit=500)
        state = self._read_library_notification_state()
        now = datetime.now().isoformat(timespec="seconds")
        if not state.get("active"):
            for row in rows:
                item_id = str(row.get("Id") or "").strip()
                if item_id:
                    state["seen"][item_id] = now
            state["active"] = True
            state["lastPollAt"] = now
            self._write_library_notification_state(state)
            self._log_project_event(
                level="info",
                module="webhook",
                action="library_notification_baseline_created",
                message="新入库自动检测基线已建立。",
                detail={"baselineItems": len(state["seen"]), "intervalSeconds": 60},
            )
            return

        pending = [row for row in rows if str(row.get("Id") or "").strip() not in state["seen"]]
        pending.sort(key=lambda row: str(row.get("DateCreated") or ""))
        batch = pending[:20]
        if pending:
            self._log_project_event(
                level="info",
                module="webhook",
                action="library_notification_poll_completed",
                message="新入库定时检测发现新增资源。",
                detail={"scanned": len(rows), "newItems": len(pending), "batchItems": len(batch), "intervalSeconds": 60},
            )
        state = self._read_library_notification_state()
        success_count = 0
        failure_count = 0
        poll_series_ids: set[str] = set()
        for index, row in enumerate(batch):
            item_id = str(row.get("Id") or "").strip()
            try:
                result = self._notify_library_item_unlocked(item_id=item_id, payload=row, source="poll", state=state)
                result_status = str(result.get("status") or "")
                if result_status == "buffered":
                    series_id = str(result.get("seriesId") or "").strip()
                    if series_id:
                        poll_series_ids.add(series_id)
                if bool(result.get("ok")) and result_status in {"sent", "duplicate", "filtered", "buffered", "buffered_duplicate"}:
                    success_count += 1
                else:
                    failure_count += 1
            except Exception as err:
                failure_count += 1
                self._log_project_event(
                    level="warning",
                    module="webhook",
                    action="library_notification_item_failed",
                    message="单个新入库资源通知失败，其他资源将继续处理。",
                    detail={"itemId": item_id, "itemType": str(row.get("Type") or ""), "error": str(err)},
                )
            if index + 1 < len(batch):
                self._stop_event.wait(0.75)
        due_groups = self._collect_due_library_groups(state=state, force_series_ids=poll_series_ids)
        state["lastPollAt"] = now
        self._write_library_notification_state(state)
        if due_groups:
            flushed_success = 0
            flushed_failure = 0
            for group in due_groups:
                try:
                    self._send_library_notification_group_unlocked(group)
                    flushed_success += 1
                except Exception as err:
                    flushed_failure += 1
                    self._restore_library_notification_group(group)
                    self._log_project_event(
                        level="warning",
                        module="webhook",
                        action="library_notification_group_flush_failed",
                        message="轮询聚合剧集通知发送失败，该分组将在下一轮重试。",
                        detail={
                            "seriesId": str(group.get("seriesId") or ""),
                            "seriesName": str(group.get("seriesName") or ""),
                            "episodeCount": len(group.get("items") or []),
                            "error": str(err),
                        },
                    )
            success_count += flushed_success
            failure_count += flushed_failure
        if batch:
            self._log_project_event(
                level="info" if failure_count == 0 else "warning",
                module="webhook",
                action="library_notification_batch_finished",
                message="新入库通知批次处理完成。",
                detail={
                    "detected": len(pending),
                    "processed": len(batch),
                    "success": success_count,
                    "failed": failure_count,
                    "remaining": max(0, len(pending) - len(batch)),
                },
            )

    def notify_library_item(self, *, item_id: str, payload: dict[str, Any] | None = None, source: str = "webhook") -> dict[str, Any]:
        with self._library_notify_lock:
            state = self._read_library_notification_state()
            result = self._notify_library_item_unlocked(item_id=item_id, payload=payload, source=source, state=state)
            self._write_library_notification_state(state)
            return result

    def _notify_library_item_unlocked(
        self,
        *,
        item_id: str,
        payload: dict[str, Any] | None = None,
        source: str = "webhook",
        state: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        safe_item_id = str(item_id or "").strip()
        if not safe_item_id:
            return {"ok": False, "status": "missing_item_id"}
        bot, emby = self._library_notification_config()
        if not bool(bot.get("enableCore", True) and bot.get("enableLibrary", True)):
            return {"ok": True, "status": "disabled"}
        token = str(bot.get("telegramToken") or "").strip()
        chat_id = str(bot.get("telegramChatId") or "").strip()
        if not token or not chat_id:
            return {"ok": False, "status": "telegram_not_configured"}

        state = state if isinstance(state, dict) else self._read_library_notification_state()
        if safe_item_id in state["seen"]:
            self._log_project_event(
                level="info",
                module="webhook",
                action="library_notification_duplicate_skipped",
                message="重复入库资源通知已跳过。",
                detail={"itemId": safe_item_id, "source": source},
            )
            return {"ok": True, "status": "duplicate"}

        try:
            detail = self._fetch_library_item_detail(safe_item_id)
        except Exception as err:
            detail = {}
            self._log_project_event(
                level="warning",
                module="webhook",
                action="library_notification_detail_fallback",
                message="入库资源详情读取失败，使用事件基础资料继续发送。",
                detail={"itemId": safe_item_id, "source": source, "error": str(err)},
            )
        if isinstance(payload, dict):
            merged = dict(payload)
            merged.update(detail)
            detail = merged
        item_type = str(detail.get("Type") or "").strip().lower()
        if item_type not in {"movie", "episode"}:
            return {"ok": True, "status": "filtered", "itemType": item_type}
        if item_type == "episode":
            buffered = self._buffer_library_episode_group(state=state, payload=detail, source=source)
            if str(buffered.get("status") or "") == "single_fallback":
                pass
            else:
                return buffered

        return self._send_library_single_notification_unlocked(
            item_id=safe_item_id,
            detail=detail,
            source=source,
            state=state,
        )

    def _send_library_single_notification_unlocked(
        self,
        *,
        item_id: str,
        detail: dict[str, Any],
        source: str,
        state: dict[str, Any],
    ) -> dict[str, Any]:
        safe_item_id = str(item_id or "").strip()
        bot, _emby = self._library_notification_config()
        token = str(bot.get("telegramToken") or "").strip()
        chat_id = str(bot.get("telegramChatId") or "").strip()
        item_type = str(detail.get("Type") or "").strip().lower()
        series_detail: dict[str, Any] = {}
        if item_type == "episode":
            series_id = str(detail.get("SeriesId") or "").strip()
            if series_id:
                try:
                    series_detail = self._fetch_library_item_detail(series_id)
                except Exception as err:
                    self._log_project_event(
                        level="warning",
                        module="webhook",
                        action="library_notification_series_detail_fallback",
                        message="所属剧集资料读取失败，继续发送单集通知。",
                        detail={"itemId": safe_item_id, "seriesId": series_id, "source": source, "error": str(err)},
                    )
        photo_sent = self._send_library_caption(
            token=token,
            chat_id=chat_id,
            caption=self._format_library_notification_caption(detail, series_detail),
            image_item_ids=self._library_notification_image_ids(detail, series_detail),
            item_id=safe_item_id,
            source=source,
        )
        state["active"] = True
        state["seen"][safe_item_id] = self._library_notification_now_iso()
        self._write_library_notification_state(state)
        self._log_project_event(
            level="info",
            module="webhook",
            action="library_notification_sent",
            message="Telegram 新入库海报通知已发送。",
            detail={
                "itemId": safe_item_id,
                "itemType": item_type,
                "source": source,
                "photo": photo_sent,
                "detailSource": str(detail.get("_detailSource") or "event_payload"),
                "seriesDetailSource": str(series_detail.get("_detailSource") or ""),
            },
        )
        return {"ok": True, "status": "sent", "photo": photo_sent, "itemType": item_type}

    def _send_library_caption(
        self,
        *,
        token: str,
        chat_id: str,
        caption: str,
        image_item_ids: list[str],
        item_id: str,
        source: str,
    ) -> bool:
        photo_sent = False
        for image_item_id in image_item_ids:
            try:
                photo_bytes = self._fetch_emby_primary_image(image_item_id)
                if not photo_bytes:
                    continue
                self.sender.send_photo_file(
                    token=token,
                    chat_id=chat_id,
                    photo_bytes=photo_bytes,
                    caption=caption,
                    filename=f"{image_item_id}.jpg",
                    content_type="image/jpeg",
                )
                photo_sent = True
                break
            except Exception as err:
                self._log_project_event(
                    level="warning",
                    module="webhook",
                    action="library_notification_photo_fallback",
                    message="入库海报发送失败，尝试文字通知。",
                    detail={"itemId": item_id, "source": source, "error": str(err)},
                )
        if not photo_sent:
            self.sender.send_text(token=token, chat_id=chat_id, text=caption)
        return photo_sent

    def _fetch_library_item_detail(self, item_id: str) -> dict[str, Any]:
        safe_item_id = str(item_id or "").strip()
        if not safe_item_id:
            return {}
        fields = ",".join(
            (
                "Overview",
                "CommunityRating",
                "CriticRating",
                "Type",
                "Name",
                "SeriesName",
                "SeriesId",
                "ParentIndexNumber",
                "IndexNumber",
                "ProductionYear",
                "PremiereDate",
                "DateCreated",
                "Genres",
                "People",
                "Status",
                "Taglines",
                "Tagline",
                "MediaSources",
                "MediaStreams",
                "Width",
                "Height",
                "ImageTags",
                "PrimaryImageItemId",
            )
        )
        query = urllib.parse.urlencode({"Ids": safe_item_id, "Fields": fields})
        collection_error = ""
        try:
            payload = self._emby_get(f"/Items?{query}")
            rows = payload.get("Items") if isinstance(payload, dict) else payload
            if isinstance(rows, list):
                match = next((row for row in rows if isinstance(row, dict) and str(row.get("Id") or "") == safe_item_id), None)
                if isinstance(match, dict):
                    result = dict(match)
                    result["_detailSource"] = "items_by_id"
                    return result
        except Exception as err:
            collection_error = str(err)

        user_error = ""
        try:
            user_id = self._resolve_emby_user_id()
            if user_id:
                user_query = urllib.parse.urlencode({"Fields": fields})
                payload = self._emby_get(
                    f"/Users/{urllib.parse.quote(user_id, safe='')}/Items/{urllib.parse.quote(safe_item_id, safe='')}?{user_query}"
                )
                if isinstance(payload, dict) and str(payload.get("Id") or safe_item_id) == safe_item_id:
                    result = dict(payload)
                    result["_detailSource"] = "user_item"
                    return result
        except Exception as err:
            user_error = str(err)

        if collection_error or user_error:
            self._log_project_event(
                level="warning",
                module="webhook",
                action="library_notification_detail_unavailable",
                message="Emby 详情兼容接口均不可用，将使用已有基础资料。",
                detail={
                    "itemId": safe_item_id,
                    "itemsByIdError": collection_error,
                    "userItemError": user_error,
                },
            )
        return {}

    def _fetch_emby_primary_image(self, item_id: str) -> bytes:
        base_url, api_key = self._emby_context()
        if not base_url or not api_key or not str(item_id or "").strip():
            return b""
        query = urllib.parse.urlencode({"maxWidth": "1000", "quality": "90"})
        request = urllib.request.Request(
            f"{base_url}/Items/{urllib.parse.quote(str(item_id), safe='')}/Images/Primary?{query}",
            method="GET",
            headers={"X-Emby-Token": api_key},
        )
        ctx = ssl._create_unverified_context()
        with urllib.request.urlopen(request, context=ctx, timeout=20) as response:
            return response.read()

    @staticmethod
    def _library_notification_image_ids(detail: dict[str, Any], series_detail: dict[str, Any]) -> list[str]:
        candidates = []
        if str(detail.get("Type") or "").strip().lower() == "episode":
            candidates.extend((detail.get("SeriesId"), series_detail.get("Id"), detail.get("PrimaryImageItemId"), detail.get("Id")))
        else:
            candidates.extend((detail.get("PrimaryImageItemId"), detail.get("Id")))
        seen: set[str] = set()
        result: list[str] = []
        for value in candidates:
            item_id = str(value or "").strip()
            if item_id and item_id not in seen:
                seen.add(item_id)
                result.append(item_id)
        return result

    @staticmethod
    def _library_notification_datetime(raw: Any) -> str:
        value = str(raw or "").strip()
        if not value:
            return datetime.now().strftime("%Y-%m-%d %H:%M")
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone().strftime("%Y-%m-%d %H:%M")
        except Exception:
            return value[:16].replace("T", " ")

    @classmethod
    def _truncate_library_notification_text(cls, text: str, limit: int = 1000) -> str:
        safe = str(text or "").strip()
        if len(safe) <= limit:
            return safe
        return safe[: max(0, limit - 3)].rstrip() + "..."

    @staticmethod
    def _library_notification_genres(detail: dict[str, Any]) -> str:
        genres = detail.get("Genres") if isinstance(detail.get("Genres"), list) else []
        rows = [str(row).strip() for row in genres if str(row).strip()]
        return " / ".join(rows[:4]) or "未分类"

    @staticmethod
    def _library_notification_people(detail: dict[str, Any]) -> str:
        people = detail.get("People") if isinstance(detail.get("People"), list) else []
        rows: list[str] = []
        for person in people:
            if not isinstance(person, dict):
                continue
            name = str(person.get("Name") or "").strip()
            if name:
                rows.append(name)
        return "、".join(rows[:8]) or "暂无"

    @classmethod
    def _library_notification_overview(cls, detail: dict[str, Any]) -> str:
        overview = str(detail.get("Overview") or "").replace("\n", " ").strip()
        overview = re.sub(r"\s+", " ", overview)
        if not overview:
            return "暂无简介"
        return cls._truncate_library_notification_text(overview, limit=300)

    @classmethod
    def _library_notification_tagline_text(cls, detail: dict[str, Any]) -> str:
        tagline = detail.get("Tagline")
        if not tagline and isinstance(detail.get("Taglines"), list):
            taglines = [str(row).strip() for row in detail.get("Taglines") if str(row).strip()]
            tagline = taglines[0] if taglines else ""
        text = str(tagline or "").strip()
        if text:
            return cls._truncate_library_notification_text(text, limit=80)
        overview = cls._library_notification_overview(detail)
        if overview and overview != "暂无简介":
            first_sentence = re.split(r"[。！？!?]", overview, maxsplit=1)[0].strip()
            if first_sentence:
                return cls._truncate_library_notification_text(first_sentence, limit=80)
        return "暂无一句话简介"

    @staticmethod
    def _library_notification_media_type(detail: dict[str, Any]) -> str:
        item_type = str(detail.get("Type") or "").strip().lower()
        if item_type == "movie":
            return "电影"
        if item_type in {"episode", "series"}:
            return "电视剧"
        return "影视"

    @classmethod
    def _library_notification_year(cls, detail: dict[str, Any]) -> str:
        year = detail.get("ProductionYear")
        if isinstance(year, (int, float)) and int(year) > 0:
            return str(int(year))
        premiere = str(detail.get("PremiereDate") or "").strip()
        if re.match(r"^\d{4}", premiere):
            return premiere[:4]
        return "未知"

    @staticmethod
    def _library_notification_rating(detail: dict[str, Any]) -> str:
        for key in ("CommunityRating", "CriticRating"):
            value = detail.get(key)
            if isinstance(value, (int, float)) and float(value) > 0:
                return f"{float(value):.1f}".rstrip("0").rstrip(".")
        return "暂无"

    @staticmethod
    def _library_notification_status_text(detail: dict[str, Any]) -> str:
        raw = str(detail.get("Status") or "").strip().lower()
        mapping = {
            "continuing": "连载中",
            "returningseries": "连载中",
            "inproduction": "制作中",
            "ended": "已完结",
            "released": "已发布",
            "postproduction": "后期制作",
            "planned": "待上线",
            "canceled": "已取消",
            "cancelled": "已取消",
        }
        return mapping.get(raw, "整理完成")

    @staticmethod
    def _library_notification_episode_info(detail: dict[str, Any]) -> str:
        item_type = str(detail.get("Type") or "").strip().lower()
        if item_type == "movie":
            return "电影"
        if item_type == "episode":
            try:
                season = int(detail.get("ParentIndexNumber"))
                episode = int(detail.get("IndexNumber"))
                return f"S{season:02d}E{episode:02d}"
            except (TypeError, ValueError):
                return "单集"
        return "未提供"

    def _library_notification_quality_text(self, detail: dict[str, Any]) -> str:
        quality = self._format_media_quality(detail)
        if str(quality or "").strip() in {"", "质量未知"}:
            return "质量未知"
        return quality

    def _build_library_notification_caption(
        self,
        *,
        title: str,
        year: str,
        detail: dict[str, Any],
        episode_info: str,
        count: int,
        latest_created_at: str,
    ) -> str:
        tagline = self._library_notification_tagline_text(detail)
        rating = self._library_notification_rating(detail)
        status = self._library_notification_status_text(detail)
        processed_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        media_type = self._library_notification_media_type(detail)
        category = self._library_notification_genres(detail)
        quality = self._library_notification_quality_text(detail)
        actors = self._library_notification_people(detail)
        overview = self._library_notification_overview(detail)

        lines = [
            f"🎬 新入库｜{title}（{year}）" if year and year != "未知" else f"🎬 新入库｜{title}",
            "",
            f"> **“{tagline}”**",
            f"> 评分：⭐ **{rating}** / 10 · 状态：{status}",
            "",
            "---",
            "=== 基本信息 ===",
            f"🕘 整理时间｜{processed_at}",
            f"📺 内容类型｜{media_type} · {category}",
            f"🎞 入库信息｜{episode_info}",
            f"📅 入库时间｜{latest_created_at}",
            "",
            "=== 资源详情 ===",
            f"🧩 资源规格｜{quality}",
            f"📦 文件数量｜{max(1, int(count or 1))} 个",
            "",
            "=== 创作信息 ===",
            f"👥 主演阵容｜{actors}",
            "",
            "=== 内容简介 ===",
            f"📜 {overview}",
        ]
        return self._truncate_library_notification_text("\n".join(lines), limit=1000)

    def _format_library_notification_caption(self, detail: dict[str, Any], series_detail: dict[str, Any]) -> str:
        item_type = str(detail.get("Type") or "").strip().lower()
        fallback = series_detail if isinstance(series_detail, dict) else {}
        primary = detail if item_type == "movie" else (fallback or detail)
        title = str(
            (detail.get("Name") if item_type == "movie" else (detail.get("SeriesName") or fallback.get("Name")))
            or "未命名内容"
        ).strip()
        year = self._library_notification_year(primary)
        latest_created_at = self._library_notification_datetime(detail.get("DateCreated") or fallback.get("DateCreated"))
        episode_info = self._library_notification_episode_info(detail)
        return self._build_library_notification_caption(
            title=title,
            year=year,
            detail=primary if isinstance(primary, dict) else {},
            episode_info=episode_info,
            count=1,
            latest_created_at=latest_created_at,
        )

    def _send_library_notification_group_unlocked(self, group: dict[str, Any]) -> None:
        bot, _emby = self._library_notification_config()
        token = str(bot.get("telegramToken") or "").strip()
        chat_id = str(bot.get("telegramChatId") or "").strip()
        if not token or not chat_id:
            raise RuntimeError("telegram_not_configured")

        items = [dict(row) for row in group.get("items", []) if isinstance(row, dict) and str(row.get("Id") or "").strip()]
        if not items:
            return
        series_id = str(group.get("seriesId") or "").strip()
        sources = sorted({str(value).strip() for value in group.get("sources", []) if str(value).strip()})
        resolved_items = [self._resolve_library_group_item(row) for row in items]
        if len(resolved_items) <= 1:
            row = resolved_items[0]
            result = self._send_library_single_notification_unlocked(
                item_id=str(row.get("Id") or "").strip(),
                detail=row,
                source="+".join(sources) or "group",
                state=self._read_library_notification_state(),
            )
            if not bool(result.get("ok")):
                raise RuntimeError(f"group_single_fallback_failed:{result.get('status')}")
            return

        series_detail = self._fetch_library_item_detail(series_id) if series_id else {}
        caption = self._format_library_group_notification_caption(group=group, items=resolved_items, series_detail=series_detail)
        image_item_ids = self._library_notification_image_ids(
            {"Type": "Episode", "SeriesId": series_id, "PrimaryImageItemId": series_detail.get("PrimaryImageItemId")},
            series_detail,
        )
        item_ids = [str(row.get("Id") or "").strip() for row in resolved_items if str(row.get("Id") or "").strip()]
        photo_sent = self._send_library_caption(
            token=token,
            chat_id=chat_id,
            caption=caption,
            image_item_ids=image_item_ids,
            item_id=series_id or ",".join(item_ids),
            source="+".join(sources) or "group",
        )
        state = self._read_library_notification_state()
        state["active"] = True
        sent_at = self._library_notification_now_iso()
        for item_id in item_ids:
            state["seen"][item_id] = sent_at
        self._write_library_notification_state(state)
        range_text = self._library_group_episode_range_text(resolved_items)
        self._log_project_event(
            level="info",
            module="webhook",
            action="library_notification_group_sent",
            message="Telegram 剧集合并入库通知已发送。",
            detail={
                "seriesId": series_id,
                "seriesName": str(series_detail.get("Name") or group.get("seriesName") or ""),
                "episodeCount": len(item_ids),
                "seasonCount": self._library_group_season_count(resolved_items),
                "rangeText": range_text,
                "photo": photo_sent,
                "source": "+".join(sources) or "group",
            },
        )

    def _resolve_library_group_item(self, row: dict[str, Any]) -> dict[str, Any]:
        if self._library_group_has_episode_identity(row):
            return dict(row)
        item_id = str(row.get("Id") or "").strip()
        if not item_id:
            return dict(row)
        try:
            detail = self._fetch_library_item_detail(item_id)
        except Exception:
            detail = {}
        merged = dict(row)
        if isinstance(detail, dict):
            merged.update(detail)
        return merged

    @staticmethod
    def _library_group_has_episode_identity(row: dict[str, Any]) -> bool:
        try:
            int(row.get("ParentIndexNumber"))
            int(row.get("IndexNumber"))
            return True
        except (TypeError, ValueError):
            return False

    @classmethod
    def _library_group_episode_pairs(cls, items: list[dict[str, Any]]) -> dict[int, list[int]]:
        grouped: dict[int, list[int]] = {}
        for row in items:
            season = cls._library_group_int(row.get("ParentIndexNumber"))
            episode = cls._library_group_int(row.get("IndexNumber"))
            if season is None or episode is None:
                continue
            grouped.setdefault(season, [])
            if episode not in grouped[season]:
                grouped[season].append(episode)
        for season in grouped:
            grouped[season] = sorted(grouped[season])
        return grouped

    @classmethod
    def _library_group_episode_range_text(cls, items: list[dict[str, Any]]) -> str:
        grouped = cls._library_group_episode_pairs(items)
        if not grouped:
            return f"共{len(items)}集"
        parts: list[str] = []
        for season in sorted(grouped):
            compressed = cls._compress_number_ranges(grouped[season])
            if "、" in compressed:
                parts.append(f"S{season:02d} {compressed}")
                continue
            if compressed.startswith("E") and "-E" in compressed:
                start_text, _, end_tail = compressed.partition("-")
                end_text = f"E{end_tail[1:]}" if end_tail.startswith("E") else end_tail
                parts.append(f"S{season:02d} {start_text}-{end_text}")
            else:
                parts.append(f"S{season:02d} {compressed}")
        return " / ".join(parts)

    @classmethod
    def _library_group_season_count(cls, items: list[dict[str, Any]]) -> int:
        return len(cls._library_group_episode_pairs(items))

    @staticmethod
    def _library_group_int(raw: Any) -> int | None:
        try:
            value = int(float(raw))
        except (TypeError, ValueError):
            return None
        return value if value >= 0 else None

    def _format_library_group_notification_caption(
        self,
        *,
        group: dict[str, Any],
        items: list[dict[str, Any]],
        series_detail: dict[str, Any],
    ) -> str:
        fallback = series_detail if isinstance(series_detail, dict) else {}
        series_name = str(fallback.get("Name") or group.get("seriesName") or items[0].get("SeriesName") or "未命名剧集").strip()
        year = self._library_notification_year(fallback or items[0])
        latest_created_raw = max((str(row.get("DateCreated") or "") for row in items), default="")
        created_at = self._library_notification_datetime(latest_created_raw)
        episode_text = self._library_group_episode_range_text(items)
        item_count = len({str(row.get("Id") or "").strip() for row in items if str(row.get("Id") or "").strip()})
        primary_detail = dict(fallback or items[0] or {})
        if items:
            best_quality_item = max(items, key=self._media_quality_score)
            if isinstance(best_quality_item, dict):
                primary_detail["MediaSources"] = best_quality_item.get("MediaSources")
                primary_detail["MediaStreams"] = best_quality_item.get("MediaStreams")
                primary_detail["Width"] = best_quality_item.get("Width")
                primary_detail["Height"] = best_quality_item.get("Height")
        return self._build_library_notification_caption(
            title=series_name,
            year=year,
            detail=primary_detail,
            episode_info=episode_text,
            count=item_count,
            latest_created_at=created_at,
        )

    def _handle_update(self, update: dict[str, Any], token: str) -> None:
        update_id = str(update.get("update_id") or "").strip()
        if update_id and not self._claim_recent_key(self._recent_update_ids, update_id, limit=1000):
            self._log_project_event(
                level="info",
                module="webhook",
                action="telegram_duplicate_update_ignored",
                message="重复 Telegram 更新已忽略。",
                detail={"updateId": update_id},
            )
            return

        callback = update.get("callback_query")
        if isinstance(callback, dict):
            self._handle_callback_query(callback, token)
            return

        message = update.get("message")
        if not isinstance(message, dict):
            return
        chat = message.get("chat") if isinstance(message.get("chat"), dict) else {}
        chat_id = chat.get("id")
        if chat_id in (None, ""):
            return
        message_id = message.get("message_id")
        chat_type = str(chat.get("type") or "").strip().lower()
        sender = message.get("from") if isinstance(message.get("from"), dict) else {}
        conversation_key = self._build_ai_conversation_key(
            chat_id=chat_id,
            user_id=sender.get("id"),
            chat_type=chat_type,
        )
        own_text_candidates = self._message_text_candidates(message, include_reply=False)
        text = str(message.get("text") or message.get("caption") or "").strip()
        if not text and not own_text_candidates:
            return
        is_private = not chat_type or chat_type == "private"
        is_command = text.startswith("/")
        cmd_name = ""
        args = text
        drive115_reply_message_id = int(message_id) if isinstance(message_id, int) else 0
        if is_command:
            cmd_text, _, args = text.partition(" ")
            cmd_name = cmd_text.split("@", 1)[0].lower().strip("/")
        elif not is_private:
            return
        if not is_command and is_private:
            drive115_candidate = self._find_115_share_candidate(own_text_candidates)
            if drive115_candidate:
                cmd_name = "zhuancun115"
                args = str(drive115_candidate.get("text") or text).strip()
            elif re.match(r"^\s*(?:请)?(?:用)?(?:影巢|hdhive)(?:搜索|搜|查找|查)\s*", text, flags=re.IGNORECASE):
                cmd_name = "hdhive"
                args = re.sub(r"^\s*(?:请)?(?:用)?(?:影巢|hdhive)(?:搜索|搜|查找|查)\s*", "", text, flags=re.IGNORECASE).strip()
            else:
                cmd_name = "ai"
                args = text
        if not is_private and cmd_name not in {"ai", "zhuancun115", "hdhive"}:
            return
        if cmd_name == "zhuancun115" and not str(args or "").strip():
            all_candidates = self._message_text_candidates(message, include_reply=True)
            drive115_candidate = self._find_115_share_candidate(all_candidates)
            if drive115_candidate:
                args = str(drive115_candidate.get("text") or "").strip()
                if str(drive115_candidate.get("source") or "").startswith("reply."):
                    replied = message.get("reply_to_message") if isinstance(message.get("reply_to_message"), dict) else {}
                    replied_id = replied.get("message_id")
                    if isinstance(replied_id, int):
                        drive115_reply_message_id = replied_id
        if cmd_name == "zhuancun115" and self._looks_like_115_share(str(args or "")):
            share = extract_115_share(str(args or ""))
            self._log_project_event(
                level="info",
                module="drive115",
                action="telegram_drive115_link_detected",
                message="Telegram 消息中识别到 115 分享链接。",
                detail={
                    "chatType": chat_type or "private",
                    "command": cmd_name,
                    "shareCode": self._mask_share_code(share.get("shareCode")),
                    "hasReceiveCode": bool(share.get("receiveCode")),
                },
            )
        ai_message_key = ""
        if cmd_name == "ai":
            ai_message_key = self._build_ai_message_key(chat_id=chat_id, message_id=message_id)
            if ai_message_key and not self._claim_recent_key(self._recent_ai_message_keys, ai_message_key, limit=1000):
                self._log_project_event(
                    level="info",
                    module="webhook",
                    action="telegram_duplicate_ai_message_ignored",
                    message="重复 Telegram AI 消息已忽略。",
                    detail={"updateId": update_id, "chatId": str(chat_id), "messageId": str(message_id or "")},
                )
                return
        self._log_project_event(
            level="info",
            module="webhook",
            action="telegram_command_received",
            message=f"收到 Telegram 指令：/{cmd_name}",
            detail={"command": cmd_name, "chatType": chat_type or "private"},
        )
        typing_stop = self._start_typing_indicator(token=token, chat_id=str(chat_id))
        try:
            if cmd_name == "ai":
                reply = self._dispatch_ai_streaming(
                    args.strip(),
                    token=token,
                    chat_id=str(chat_id),
                    conversation_key=conversation_key,
                )
            elif cmd_name == "zhuancun115":
                reply = self._cmd_drive115_transfer(
                    args.strip(),
                    reply_to_message_id=drive115_reply_message_id,
                )
            elif cmd_name == "hdhive":
                reply = self._cmd_hdhive_search(args.strip())
            else:
                reply = self._dispatch_command(cmd_name, args.strip())
        except Exception as err:
            LOGGER.exception("Telegram command dispatch failed: cmd=%s err=%s", cmd_name, err)
            self._log_project_event(
                level="error",
                module="webhook",
                action="telegram_command_failed",
                message=f"Telegram 指令处理失败：/{cmd_name}",
                detail={"command": cmd_name, "error": str(err)},
            )
            reply = "⚠️ 指令处理失败，请稍后重试或发送 /help"
        finally:
            typing_stop()
        if not reply:
            return
        try:
            self._send_command_reply(token=token, chat_id=str(chat_id), reply=reply)
            if cmd_name == "saomiao":
                self._log_project_event(
                    level="info",
                    module="webhook",
                    action="telegram_library_scan_reply_sent",
                    message="Telegram 扫描媒体库列表已发送。",
                    detail={"command": cmd_name},
                )
        except Exception as err:
            LOGGER.exception("Telegram command reply failed: cmd=%s err=%s", cmd_name, err)
            self._log_project_event(
                level="error",
                module="webhook",
                action="telegram_command_reply_failed",
                message=f"Telegram 指令回复发送失败：/{cmd_name}",
                detail={"command": cmd_name, "error": str(err)},
            )
            return

    def _claim_recent_key(self, store: dict[str, float], key: str, *, limit: int) -> bool:
        safe_key = str(key or "").strip()
        if not safe_key:
            return True
        with self._telegram_dedupe_lock:
            if safe_key in store:
                return False
            store[safe_key] = time.time()
            overflow = len(store) - max(1, int(limit))
            if overflow > 0:
                for old_key in list(store.keys())[:overflow]:
                    store.pop(old_key, None)
            return True

    @staticmethod
    def _build_ai_message_key(*, chat_id: Any, message_id: Any) -> str:
        safe_chat_id = str(chat_id or "").strip()
        safe_message_id = str(message_id or "").strip()
        if not safe_chat_id or not safe_message_id:
            return ""
        return f"{safe_chat_id}:{safe_message_id}"

    @staticmethod
    def _build_ai_conversation_key(*, chat_id: Any, user_id: Any, chat_type: str) -> str:
        safe_chat_id = str(chat_id or "").strip()
        if not safe_chat_id:
            return ""
        if str(chat_type or "").strip().lower() in {"group", "supergroup"}:
            return f"chat:{safe_chat_id}:user:{str(user_id or 'unknown').strip()}"
        return f"chat:{safe_chat_id}"

    def _start_typing_indicator(self, *, token: str, chat_id: str) -> Callable[[], None]:
        stopped = threading.Event()

        def _send_once() -> None:
            try:
                self.sender.send_chat_action(token=token, chat_id=chat_id, action="typing")
            except Exception as err:
                LOGGER.warning("Telegram sendChatAction failed: %s", err)

        def _loop() -> None:
            _send_once()
            while not stopped.wait(4.0):
                _send_once()

        worker = threading.Thread(target=_loop, name="TelegramTypingIndicator", daemon=True)
        worker.start()

        def _stop() -> None:
            stopped.set()
            if worker.is_alive():
                worker.join(timeout=0.2)

        return _stop

    def _handle_callback_query(self, callback: dict[str, Any], token: str) -> None:
        callback_id = str(callback.get("id") or "").strip()
        data = str(callback.get("data") or "").strip()
        message = callback.get("message") if isinstance(callback.get("message"), dict) else {}
        chat = message.get("chat") if isinstance(message.get("chat"), dict) else {}
        chat_id = chat.get("id")
        message_id = message.get("message_id")
        if not callback_id:
            return
        if data.startswith("ai_exec:"):
            self._handle_ai_action_callback(
                data=data,
                token=token,
                callback_id=callback_id,
                chat_id=str(chat_id) if chat_id not in (None, "") else "",
                message_id=message_id if isinstance(message_id, int) else 0,
            )
            return
        if data.startswith("scan_library:"):
            self._handle_library_scan_callback(
                data=data,
                token=token,
                callback_id=callback_id,
                chat_id=str(chat_id) if chat_id not in (None, "") else "",
                message_id=message_id if isinstance(message_id, int) else 0,
            )
            return
        if data.startswith("drive115:"):
            self._handle_drive115_callback(
                data=data,
                token=token,
                callback_id=callback_id,
                chat_id=str(chat_id) if chat_id not in (None, "") else "",
                message_id=message_id if isinstance(message_id, int) else 0,
            )
            return
        if data.startswith("hdhive:"):
            self._handle_hdhive_callback(
                data=data,
                token=token,
                callback_id=callback_id,
                chat_id=str(chat_id) if chat_id not in (None, "") else "",
                message_id=message_id if isinstance(message_id, int) else 0,
            )
            return
        if data.startswith("missing_search:"):
            self._handle_missing_search_callback(
                data=data,
                token=token,
                callback_id=callback_id,
                chat_id=str(chat_id) if chat_id not in (None, "") else "",
            )
            return
        if data != "recent_playback:refresh":
            try:
                self.sender.answer_callback_query(token=token, callback_query_id=callback_id, text="暂不支持该操作")
            except Exception:
                pass
            return
        try:
            reply = self._cmd_recent_playback()
            if isinstance(reply, dict) and chat_id not in (None, "") and isinstance(message_id, int):
                try:
                    self.sender.edit_message_text(
                        token=token,
                        chat_id=str(chat_id),
                        message_id=message_id,
                        text=str(reply.get("text") or "📜 最近播放记录\n暂无记录。"),
                        reply_markup=reply.get("reply_markup") if isinstance(reply.get("reply_markup"), dict) else None,
                    )
                except Exception as edit_err:
                    err_text = str(edit_err)
                    if "message is not modified" not in err_text.lower():
                        raise
                    self._log_project_event(
                        level="info",
                        module="playback",
                        action="telegram_recent_playback_unchanged",
                        message="最近播放刷新完成，内容没有变化。",
                        detail={"callback": data},
                    )
            self.sender.answer_callback_query(token=token, callback_query_id=callback_id, text="已刷新最近播放")
        except Exception as callback_err:
            LOGGER.exception("Telegram callback failed: data=%s", data)
            self._log_project_event(
                level="error",
                module="webhook",
                action="telegram_callback_failed",
                message="Telegram 按钮回调处理失败。",
                detail={"callback": data, "error": str(callback_err)},
            )
            try:
                self.sender.answer_callback_query(token=token, callback_query_id=callback_id, text="刷新失败，请稍后重试")
            except Exception:
                pass

    def _handle_ai_action_callback(self, *, data: str, token: str, callback_id: str, chat_id: str, message_id: int) -> None:
        parts = str(data or "").split(":")
        decision = parts[1] if len(parts) >= 3 else ""
        action_id = parts[2] if len(parts) >= 3 else ""
        action = self._pending_ai_actions.pop(action_id, None)
        if decision == "cancel":
            try:
                self.sender.answer_callback_query(token=token, callback_query_id=callback_id, text="已取消")
                if chat_id and message_id:
                    self._edit_ai_markdown_message(
                        token=token,
                        chat_id=chat_id,
                        message_id=message_id,
                        title="🧠 AI 执行已取消",
                        body="已取消，本次没有执行任何操作。",
                    )
            except Exception:
                pass
            return
        if decision != "ok" or not isinstance(action, dict):
            try:
                self.sender.answer_callback_query(token=token, callback_query_id=callback_id, text="操作已过期或无效")
            except Exception:
                pass
            return
        try:
            result_text = self._execute_ai_confirmed_action(action)
            self.sender.answer_callback_query(token=token, callback_query_id=callback_id, text="已执行")
            if chat_id and message_id:
                self._edit_ai_markdown_message(
                    token=token,
                    chat_id=chat_id,
                    message_id=message_id,
                    title="🧠 AI 执行结果",
                    body=result_text,
                )
        except Exception as err:
            LOGGER.exception("AI confirmed action failed: %s", err)
            try:
                self.sender.answer_callback_query(token=token, callback_query_id=callback_id, text="执行失败")
                if chat_id and message_id:
                    self._edit_ai_markdown_message(
                        token=token,
                        chat_id=chat_id,
                        message_id=message_id,
                        title="🧠 AI 执行失败",
                        body=str(err),
                    )
            except Exception:
                pass

    def _handle_missing_search_callback(self, *, data: str, token: str, callback_id: str, chat_id: str) -> None:
        action_id = str(data or "").split(":", 1)[1] if ":" in str(data or "") else ""
        self._cleanup_pending_missing_searches()
        pending = self._pending_missing_searches.get(action_id)
        if not isinstance(pending, dict) or (pending.get("chatId") and str(pending.get("chatId")) != str(chat_id)):
            try:
                self.sender.answer_callback_query(token=token, callback_query_id=callback_id, text="搜索清单已过期，请重新查询缺集")
            except Exception:
                pass
            return
        title = str(pending.get("title") or "未知作品").strip()
        labels = [str(value or "").strip() for value in pending.get("labels", []) if str(value or "").strip()]
        visible = labels[:50]
        lines = [f"{title} {label}" for label in visible]
        if len(labels) > len(visible):
            lines.append(f"还有 {len(labels) - len(visible)} 集未展示，请缩小范围后重新查询。")
        reply = self._ai_markdown_reply("🔍 缺集搜索清单", "\n".join(lines) or "当前没有缺失集。")
        try:
            self.sender.answer_callback_query(token=token, callback_query_id=callback_id, text="已生成搜索清单")
            self._send_command_reply(token=token, chat_id=chat_id, reply=reply)
            self._log_project_event(
                level="info",
                module="webhook",
                action="telegram_missing_search_list_generated",
                message="Telegram 缺集搜索清单已生成。",
                detail={"title": title, "count": len(visible), "remaining": max(0, len(labels) - len(visible))},
            )
        except Exception as err:
            LOGGER.warning("Telegram missing search list callback failed: %s", err)
            try:
                self.sender.answer_callback_query(token=token, callback_query_id=callback_id, text="生成失败，请稍后重试")
            except Exception:
                pass

    def _cleanup_pending_missing_searches(self) -> None:
        now = time.time()
        expired = [key for key, value in self._pending_missing_searches.items() if now - float(value.get("createdAt") or 0) > 900]
        for key in expired:
            self._pending_missing_searches.pop(key, None)
        overflow = len(self._pending_missing_searches) - 100
        if overflow > 0:
            for key in list(self._pending_missing_searches.keys())[:overflow]:
                self._pending_missing_searches.pop(key, None)

    def _handle_library_scan_callback(self, *, data: str, token: str, callback_id: str, chat_id: str, message_id: int) -> None:
        parts = str(data or "").split(":", 2)
        target = parts[1] if len(parts) >= 2 else ""
        encoded_id = parts[2] if len(parts) >= 3 else ""
        detail: dict[str, Any] = {"target": target}
        try:
            self.sender.answer_callback_query(token=token, callback_query_id=callback_id, text="已提交扫描")
            if target == "all":
                result_text = self._execute_full_library_scan()
                detail["libraryName"] = "全库"
            elif target == "one" and encoded_id:
                library_id = urllib.parse.unquote(encoded_id)
                library = self._find_emby_library_by_id(library_id)
                if not library:
                    raise RuntimeError("未找到这个媒体库，请重新发送 /saomiao 获取最新列表。")
                detail["libraryId"] = library_id
                detail["libraryName"] = str(library.get("name") or "")
                result_text = self._execute_single_library_scan(library)
            else:
                raise RuntimeError("扫描目标无效，请重新发送 /saomiao。")
            self._log_project_event(
                level="info",
                module="webhook",
                action="telegram_library_scan_submitted",
                message="Telegram 媒体库扫描已提交。",
                detail=detail,
            )
            if chat_id and message_id:
                self._edit_ai_markdown_message(
                    token=token,
                    chat_id=chat_id,
                    message_id=message_id,
                    title="🔄 媒体库扫描",
                    body=result_text,
                )
        except Exception as err:
            LOGGER.exception("Telegram library scan callback failed: %s", err)
            detail["error"] = str(err)
            self._log_project_event(
                level="error",
                module="webhook",
                action="telegram_library_scan_failed",
                message="Telegram 媒体库扫描提交失败。",
                detail=detail,
            )
            try:
                self.sender.answer_callback_query(token=token, callback_query_id=callback_id, text="扫描提交失败")
                if chat_id and message_id:
                    self._edit_ai_markdown_message(
                        token=token,
                        chat_id=chat_id,
                        message_id=message_id,
                        title="🔄 媒体库扫描失败",
                        body=str(err),
                    )
            except Exception:
                pass

    def _handle_drive115_callback(self, *, data: str, token: str, callback_id: str, chat_id: str, message_id: int) -> None:
        parts = str(data or "").split(":", 2)
        decision = parts[1] if len(parts) >= 2 else ""
        transfer_id = parts[2] if len(parts) >= 3 else ""
        pending = self._pending_drive115_transfers.pop(transfer_id, None)
        if decision == "cancel":
            try:
                self.sender.answer_callback_query(token=token, callback_query_id=callback_id, text="已取消")
                if chat_id and message_id:
                    self._edit_ai_markdown_message(token=token, chat_id=chat_id, message_id=message_id, title="📦 115 转存已取消", body="已取消，本次没有提交 115 转存。")
            except Exception:
                pass
            return
        if decision != "ok" or not isinstance(pending, dict):
            try:
                self.sender.answer_callback_query(token=token, callback_query_id=callback_id, text="操作已过期，请重新发送链接")
            except Exception:
                pass
            return
        detail = {"shareCode": pending.get("shareCode"), "targetCid": pending.get("targetCid"), "title": pending.get("title")}
        try:
            result = self._drive115_service().transfer_share(
                share_code=str(pending.get("shareCode") or ""),
                receive_code=str(pending.get("receiveCode") or ""),
                target_cid=str(pending.get("targetCid") or ""),
                file_ids=pending.get("fileIds") if isinstance(pending.get("fileIds"), list) else [],
                source_files=pending.get("sourceFiles") if isinstance(pending.get("sourceFiles"), list) else [],
            )
            self.sender.answer_callback_query(token=token, callback_query_id=callback_id, text="已提交转存")
            self._log_project_event(level="info", module="drive115", action="telegram_drive115_transfer_submitted", message="Telegram 115 转存已提交。", detail=detail)
            if chat_id and message_id:
                self._edit_ai_markdown_message(token=token, chat_id=chat_id, message_id=message_id, title="📦 115 转存", body=f"已提交 115 转存。\n资源：{pending.get('title') or pending.get('shareCode')}\n目录：{result.get('targetCid') or pending.get('targetCid') or '0'}\n结果：115 已收到转存请求。")
        except Exception as err:
            detail["error"] = str(err)
            self._log_project_event(level="error", module="drive115", action="telegram_drive115_transfer_failed", message="Telegram 115 转存失败。", detail=detail)
            try:
                self.sender.answer_callback_query(token=token, callback_query_id=callback_id, text="转存失败")
                if chat_id and message_id:
                    self._edit_ai_markdown_message(token=token, chat_id=chat_id, message_id=message_id, title="📦 115 转存失败", body=str(err))
            except Exception:
                pass

    def _handle_hdhive_callback(self, *, data: str, token: str, callback_id: str, chat_id: str, message_id: int) -> None:
        parts = str(data or "").split(":", 2)
        decision = parts[1] if len(parts) > 1 else ""
        action_id = parts[2] if len(parts) > 2 else ""
        pending = self._pending_hdhive_actions.get(action_id)
        if not isinstance(pending, dict) or time.time() - float(pending.get("createdAt") or 0) > 900:
            self._pending_hdhive_actions.pop(action_id, None)
            try:
                self.sender.answer_callback_query(token=token, callback_query_id=callback_id, text="操作已过期，请重新搜索")
            except Exception:
                pass
            return
        if decision == "cancel":
            self._pending_hdhive_actions.pop(action_id, None)
            self.sender.answer_callback_query(token=token, callback_query_id=callback_id, text="已取消")
            if chat_id and message_id:
                self._edit_ai_markdown_message(token=token, chat_id=chat_id, message_id=message_id, title="🪺 影巢转存已取消", body="未解锁资源，也没有消耗积分。")
            return
        if decision == "pick":
            resource = pending.get("resource") if isinstance(pending.get("resource"), dict) else {}
            cost = "已解锁，不重复扣积分" if resource.get("isUnlocked") else f"预计消耗 {int(resource.get('unlockPoints') or 0)} 积分"
            body = f"资源：{resource.get('title') or '影巢资源'}\n网盘：{resource.get('panType') or '-'}\n费用：{cost}\n目录：{pending.get('targetCid') or '0'}\n\n确认后才会解锁并提交 115 转存。"
            markup = {"inline_keyboard": [[{"text": "确认解锁并转存", "callback_data": f"hdhive:ok:{action_id}"}, {"text": "取消", "callback_data": f"hdhive:cancel:{action_id}"}]]}
            self.sender.answer_callback_query(token=token, callback_query_id=callback_id, text="请确认积分消耗")
            if chat_id and message_id:
                reply = self._ai_markdown_reply("🪺 影巢转存确认", body, reply_markup=markup)
                self.sender.edit_message_text(token=token, chat_id=chat_id, message_id=message_id, text=str(reply["text"]), reply_markup=markup, parse_mode="MarkdownV2")
            return
        if decision != "ok":
            return
        self._pending_hdhive_actions.pop(action_id, None)
        resource = pending.get("resource") if isinstance(pending.get("resource"), dict) else {}
        try:
            unlocked = self._hdhive_service().unlock(str(resource.get("slug") or ""))
            full_url = str(unlocked.get("full_url") or unlocked.get("url") or "").strip()
            access_code = str(unlocked.get("access_code") or "").strip()
            share = extract_115_share(full_url, access_code)
            if not share.get("shareCode"):
                raise RuntimeError("该资源不是可识别的 115 分享链接。")
            parsed = self._drive115_service().parse_share(share_url=full_url, receive_code=access_code)
            parsed_files = parsed.get("files") if isinstance(parsed.get("files"), list) else []
            result = self._drive115_service().transfer_share(
                share_code=str(parsed.get("shareCode") or share.get("shareCode") or ""),
                receive_code=str(parsed.get("receiveCode") or access_code),
                target_cid=str(pending.get("targetCid") or "0"),
                file_ids=[str(row.get("id") or "").strip() for row in parsed_files if isinstance(row, dict) and str(row.get("id") or "").strip()],
                source_files=parsed_files,
            )
            self.sender.answer_callback_query(token=token, callback_query_id=callback_id, text="已提交 115 转存")
            self._log_project_event(level="info", module="hdhive", action="telegram_hdhive_transfer_success", message="Telegram 影巢资源已解锁并提交 115 转存。", detail={"slug": resource.get("slug"), "targetCid": result.get("targetCid"), "alreadyOwned": bool(unlocked.get("already_owned"))})
            if chat_id and message_id:
                self._edit_ai_markdown_message(token=token, chat_id=chat_id, message_id=message_id, title="🪺 影巢转存完成", body=f"资源：{resource.get('title') or '影巢资源'}\n结果：115 已收到转存请求\n目录：{result.get('targetCid') or pending.get('targetCid') or '0'}")
        except Exception as err:
            self._log_project_event(level="error", module="hdhive", action="telegram_hdhive_transfer_failed", message="Telegram 影巢资源转存失败。", detail={"slug": resource.get("slug"), "error": str(err)})
            try:
                self.sender.answer_callback_query(token=token, callback_query_id=callback_id, text="转存失败")
                if chat_id and message_id:
                    self._edit_ai_markdown_message(token=token, chat_id=chat_id, message_id=message_id, title="🪺 影巢转存失败", body=str(err))
            except Exception:
                pass

    def _edit_ai_markdown_message(
        self,
        *,
        token: str,
        chat_id: str,
        message_id: int,
        title: str,
        body: Any,
        reply_markup: dict[str, Any] | None = None,
    ) -> None:
        reply = self._ai_markdown_reply(title, body, reply_markup=reply_markup)
        text = str(reply.get("text") or "")
        fallback_text = str(reply.get("fallback_text") or text)
        safe_reply_markup = reply.get("reply_markup") if isinstance(reply.get("reply_markup"), dict) else None
        try:
            self.sender.edit_message_text(
                token=token,
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                reply_markup=safe_reply_markup,
                parse_mode="MarkdownV2",
            )
        except Exception as err:
            if "message is not modified" in str(err).lower():
                return
            LOGGER.warning("Telegram MarkdownV2 edit failed, fallback to plain text: %s", err)
            try:
                self.sender.edit_message_text(
                    token=token,
                    chat_id=chat_id,
                    message_id=message_id,
                    text=fallback_text,
                    reply_markup=safe_reply_markup,
                )
            except Exception as fallback_err:
                if "message is not modified" not in str(fallback_err).lower():
                    raise

    def _send_command_reply(self, *, token: str, chat_id: str, reply: CommandReply) -> None:
        if isinstance(reply, str):
            self.sender.send_text(token=token, chat_id=chat_id, text=reply)
            return
        if not isinstance(reply, dict):
            return

        photo_url = str(reply.get("photo_url") or "").strip()
        photo_caption = str(reply.get("photo_caption") or "").strip()
        photo_bytes = reply.get("photo_bytes") if isinstance(reply.get("photo_bytes"), bytes) else b""
        photo_filename = str(reply.get("photo_filename") or "poster.jpg").strip() or "poster.jpg"
        photo_mime = str(reply.get("photo_mime") or "image/jpeg").strip() or "image/jpeg"
        text = str(reply.get("text") or "").strip()
        reply_markup = reply.get("reply_markup") if isinstance(reply.get("reply_markup"), dict) else None
        parse_mode = str(reply.get("parse_mode") or "").strip()
        fallback_text = str(reply.get("fallback_text") or text).strip()
        reply_to_message_id = int(reply.get("reply_to_message_id") or 0)
        if photo_bytes:
            try:
                self.sender.send_photo_file(
                    token=token,
                    chat_id=chat_id,
                    photo_bytes=photo_bytes,
                    caption=photo_caption or text or "🖼 封面预览",
                    filename=photo_filename,
                    content_type=photo_mime,
                    reply_markup=reply_markup,
                )
                return
            except Exception as err:
                self._log_project_event(
                    level="error",
                    module="webhook",
                    action="telegram_photo_upload_failed",
                    message="Telegram 图片上传发送失败，已回退为纯文本。",
                    detail={"error": str(err), "filename": photo_filename, "mime": photo_mime, "bytes": len(photo_bytes)},
                )
                text = photo_caption or text
        if photo_url:
            try:
                self.sender.send_photo(
                    token=token,
                    chat_id=chat_id,
                    photo_url=photo_url,
                    caption=photo_caption or "🖼 封面预览",
                )
                return
            except Exception as err:
                self._log_project_event(
                    level="error",
                    module="webhook",
                    action="telegram_photo_url_failed",
                    message="Telegram 通过 URL 发送图片失败，已回退为纯文本。",
                    detail={"error": str(err)},
                )
                if photo_caption:
                    text = f"{photo_caption}\n\n{text}" if text else photo_caption
        if text:
            try:
                self.sender.send_text(
                    token=token,
                    chat_id=chat_id,
                    text=text,
                    reply_markup=reply_markup,
                    parse_mode=parse_mode,
                    reply_to_message_id=reply_to_message_id,
                )
            except Exception as err:
                if parse_mode == "MarkdownV2":
                    LOGGER.warning("Telegram MarkdownV2 send failed, fallback to plain text: %s", err)
                    try:
                        self.sender.send_text(
                            token=token,
                            chat_id=chat_id,
                            text=fallback_text or text,
                            reply_markup=reply_markup,
                            reply_to_message_id=reply_to_message_id,
                        )
                        return
                    except Exception:
                        if not reply_to_message_id:
                            raise
                elif not reply_to_message_id:
                    raise
                LOGGER.warning("Telegram referenced reply failed, fallback to standalone message: %s", err)
                self.sender.send_text(
                    token=token,
                    chat_id=chat_id,
                    text=fallback_text or text,
                    reply_markup=reply_markup,
                    parse_mode="" if parse_mode == "MarkdownV2" else parse_mode,
                )

    def _dispatch_command(self, cmd: str, args: str) -> CommandReply:
        if cmd in {"start", "help"}:
            return _help_text() if cmd == "help" else "🚀 镜界机器人已启动\n\n" + _help_text()
        if cmd == "check":
            return self._cmd_check()
        if cmd == "zhengzaibofang":
            return self._cmd_now_playing()
        if cmd == "zuijinbofangjilu":
            return self._cmd_recent_playback()
        if cmd == "zuijinruku":
            return self._cmd_recent_library()
        if cmd == "saomiao":
            return self._cmd_scan_library(args)
        if cmd == "zhuancun115":
            return self._cmd_drive115_transfer(args)
        if cmd == "hdhive":
            return self._cmd_hdhive_search(args)
        if cmd == "ai":
            return self._cmd_ai(args)
        if cmd == "sousuo":
            return self._cmd_search(args)
        if cmd == "ribaoday":
            return self._cmd_report("day")
        if cmd == "zoubaoday":
            return self._cmd_report("week")
        if cmd == "yuebaoday":
            return self._cmd_report("month")
        if cmd == "niandu":
            return self._cmd_report("year")
        return "未知指令，请发送 /help 查看帮助"

    def _emby_context(self) -> tuple[str, str]:
        store = _read_store(self.store_path)
        emby = _apply_emby_env_overrides(store["embyConfig"])
        server_url = str(emby.get("serverUrl") or "").strip().rstrip("/")
        if server_url and not server_url.lower().endswith("/emby"):
            server_url = f"{server_url}/emby"
        return server_url, str(emby.get("apiKey") or "").strip()

    def _emby_get(self, path: str) -> dict[str, Any] | list[Any] | None:
        base_url, api_key = self._emby_context()
        if not base_url or not api_key:
            return None
        request = urllib.request.Request(
            f"{base_url}{path}",
            method="GET",
            headers={"X-Emby-Token": api_key},
        )
        ctx = ssl._create_unverified_context()
        with urllib.request.urlopen(request, context=ctx, timeout=20) as response:
            content = response.read().decode("utf-8", errors="replace")
        if not content.strip():
            return None
        return json.loads(content)

    def _emby_post(self, path: str) -> dict[str, Any] | list[Any] | None:
        base_url, api_key = self._emby_context()
        if not base_url or not api_key:
            raise RuntimeError("Emby 未配置完整")
        request = urllib.request.Request(
            f"{base_url}{path}",
            data=b"",
            method="POST",
            headers={"X-Emby-Token": api_key},
        )
        ctx = ssl._create_unverified_context()
        with urllib.request.urlopen(request, context=ctx, timeout=20) as response:
            content = response.read().decode("utf-8", errors="replace")
        if not content.strip():
            return None
        return json.loads(content)

    def _media_identity_service(self) -> MediaIdentityService:
        token, language, region = self._tmdb_context()

        def tmdb_fetcher(path: str) -> dict[str, Any] | list[Any] | None:
            return self._tmdb_get_json(path, token=token)

        return MediaIdentityService(
            emby_fetcher=self._emby_get,
            tmdb_fetcher=tmdb_fetcher if token else None,
            cache_path=self.store_path.parent / "media_identity_cache.json",
            language=language,
            region=region,
        )

    def _emby_fetch_bytes(self, path: str, *, max_bytes: int = 10 * 1024 * 1024) -> tuple[bytes, str]:
        base_url, api_key = self._emby_context()
        if not base_url or not api_key:
            raise RuntimeError("Emby 未配置完整")
        request = urllib.request.Request(
            f"{base_url}{path}",
            method="GET",
            headers={"X-Emby-Token": api_key},
        )
        ctx = ssl._create_unverified_context()
        with urllib.request.urlopen(request, context=ctx, timeout=20) as response:
            content_type = str(response.headers.get("Content-Type") or "application/octet-stream").split(";", 1)[0].strip()
            content = response.read(max_bytes + 1)
        if len(content) > max_bytes:
            raise RuntimeError(f"图片超过大小限制：>{max_bytes} bytes")
        if not content:
            raise RuntimeError("图片内容为空")
        if not content_type.startswith("image/"):
            raise RuntimeError(f"返回内容不是图片：{content_type}")
        return content, content_type

    def _cmd_check(self) -> str:
        store = _read_store(self.store_path)
        bot = store["botConfig"]
        token_ok = bool(str(bot.get("telegramToken") or "").strip())
        chat_ok = bool(str(bot.get("telegramChatId") or "").strip())
        emby_ok = False
        emby_msg = "未连接"
        try:
            info = self._emby_get("/System/Info/Public")
            if isinstance(info, dict):
                emby_ok = True
                emby_msg = str(info.get("ServerName") or "连接正常")
        except Exception as err:
            emby_msg = f"异常: {err}"
        db_ok = self.store_path.exists()
        log_ok = self.event_log_path.exists()
        return "\n".join(
            [
                "📡 系统探针",
                f"机器人状态：{'正常' if token_ok and chat_ok else '未配置完整'}",
                f"Emby连接：{'正常' if emby_ok else '异常'} ({emby_msg})",
                f"数据库状态：{'正常' if db_ok else '缺失'}",
                f"播放日志：{'正常' if log_ok else '未生成'}",
            ]
        )

    def _cmd_now_playing(self) -> str:
        try:
            sessions = self._emby_get("/Sessions")
        except Exception as err:
            return f"🟢 正在播放\n获取失败：{err}"
        rows = sessions if isinstance(sessions, list) else []
        active_blocks: list[str] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            now_item = row.get("NowPlayingItem") if isinstance(row.get("NowPlayingItem"), dict) else {}
            name = str(now_item.get("Name") or "").strip()
            if not name:
                continue

            user_name = str(row.get("UserName") or row.get("UserId") or "未知用户").strip()
            raw_player = str(row.get("Client") or row.get("DeviceName") or "未知播放器").strip()
            player = self._normalize_player_name(raw_player)
            drama_title = self._format_now_playing_title(now_item)
            percent = self._calc_progress_percent(row, now_item)
            progress_bar = self._build_progress_bar(percent)
            active_blocks.append(
                "\n".join(
                    [
                        f"📱 播放器：{player}",
                        f"👤 用户：{user_name or '未知用户'}",
                        f"📺 {drama_title}",
                        f"🕰 播放进度：{progress_bar} {percent}%",
                    ]
                )
            )

        if not active_blocks:
            return "🟢 当前正在播放（0人）\n\n暂无正在播放记录。"
        return f"🟢 当前正在播放（{len(active_blocks)}人）\n\n" + "\n\n".join(active_blocks[:12])

    @staticmethod
    def _normalize_player_name(raw: str) -> str:
        text = str(raw or "").strip()
        if not text:
            return "未知播放器"
        if "/(" in text:
            text = text.split("/(", 1)[0].strip()
        elif "(" in text:
            text = text.split("(", 1)[0].strip()
        return text or "未知播放器"

    @staticmethod
    def _format_now_playing_title(now_item: dict[str, Any]) -> str:
        item_name = str(now_item.get("Name") or "").strip() or "未知内容"
        item_type = str(now_item.get("Type") or "").strip().lower()
        if item_type == "episode":
            series_name = str(now_item.get("SeriesName") or "").strip() or "未知剧名"
            season_number = now_item.get("ParentIndexNumber")
            episode_number = now_item.get("IndexNumber")
            season_text = str(season_number) if isinstance(season_number, int) else "X"
            episode_text = str(episode_number) if isinstance(episode_number, int) else "X"
            return f"《{series_name}》第{season_text}季 第{episode_text}集「{item_name}」"
        return f"《{item_name}》"

    @staticmethod
    def _calc_progress_percent(session_row: dict[str, Any], now_item: dict[str, Any]) -> int:
        play_state = session_row.get("PlayState") if isinstance(session_row.get("PlayState"), dict) else {}
        position_ticks = play_state.get("PositionTicks")
        runtime_ticks = now_item.get("RunTimeTicks")
        if not isinstance(position_ticks, (int, float)) or not isinstance(runtime_ticks, (int, float)):
            return 0
        if runtime_ticks <= 0:
            return 0
        percent = int((float(position_ticks) / float(runtime_ticks)) * 100)
        return max(0, min(100, percent))

    @staticmethod
    def _build_progress_bar(percent: int) -> str:
        safe_percent = max(0, min(100, int(percent)))
        played = min(10, max(0, safe_percent // 10))
        unplayed = 10 - played
        return ("🟥" * played) + ("🟩" * unplayed)

    def _cmd_recent_playback(self) -> CommandReply:
        rows, activity_error = self._collect_recent_playback_rows(limit=15)
        lines = ["📜 最近播放记录", ""]
        if rows:
            parsed_episode_count = 0
            fallback_title_count = 0
            missing_time_count = 0
            for row in rows:
                line, meta = self._format_recent_playback_row_with_meta(row)
                lines.append(line)
                if bool(meta.get("parsedEpisode")):
                    parsed_episode_count += 1
                if bool(meta.get("fallbackTitle")):
                    fallback_title_count += 1
                if bool(meta.get("missingTime")):
                    missing_time_count += 1
            self._log_project_event(
                level="info",
                module="playback",
                action="telegram_recent_playback_format_stats",
                message="最近播放记录已完成模板格式化。",
                detail={
                    "rows": len(rows),
                    "parsedEpisodeCount": parsed_episode_count,
                    "fallbackTitleCount": fallback_title_count,
                    "missingTimeCount": missing_time_count,
                },
            )
            if activity_error:
                lines.extend(["", f"提示：Emby 活动日志暂不可用，仅显示本地记录。"])
            return {
                "text": "\n".join(lines),
                "reply_markup": self._recent_playback_keyboard(),
            }
        if activity_error:
            lines.append(f"获取失败：{activity_error}")
        else:
            lines.append("暂无记录。")
        return {
            "text": "\n".join(lines),
            "reply_markup": self._recent_playback_keyboard(),
        }

    @staticmethod
    def _recent_playback_keyboard() -> dict[str, Any]:
        return {
            "inline_keyboard": [
                [{"text": "刷新最近播放", "callback_data": "recent_playback:refresh"}],
            ],
        }

    def _collect_recent_playback_rows(self, *, limit: int = 10) -> tuple[list[dict[str, Any]], str]:
        max_rows = max(1, int(limit or 10))
        activity_error = ""
        try:
            service = PlaybackHistoryService(fetcher=self._emby_get, event_logger=None)
            result = service.collect(limit=max_rows, scan_limit=2000)
            rows = result.get("rows") if isinstance(result, dict) else []
            warning = str(result.get("warning") or "").strip() if isinstance(result, dict) else ""
            debug = result.get("debug") if isinstance(result, dict) else {}
            mode = str((debug or {}).get("mode") or "strict").strip().lower()
            action = "telegram_recent_playback_success"
            if mode == "relaxed":
                action = "telegram_recent_playback_relaxed_fallback"
            self._log_project_event(
                level="warning" if warning else "info",
                module="playback",
                action=action,
                message=warning or "最近播放记录已从统一数据源返回。",
                detail={
                    "source": "playback_history_service",
                    "rows": len(rows) if isinstance(rows, list) else 0,
                    "debug": debug if isinstance(debug, dict) else {},
                },
            )
            if isinstance(rows, list) and rows:
                return rows, ""

            if warning:
                activity_error = warning
            elif isinstance(debug, dict):
                activity_error = (
                    "统一数据源暂无结果"
                    f"（scanned={int(debug.get('scanned') or 0)}, strictMatched={int(debug.get('strictMatched') or 0)},"
                    f" relaxedMatched={int(debug.get('relaxedMatched') or 0)}）"
                )
            else:
                activity_error = "统一数据源暂无结果"
            return [], activity_error
        except Exception as err:
            activity_error = str(err)
            local_rows = read_recent_playback_events(self.event_log_path, limit=max_rows * 3)
            result = local_rows[:max_rows]
            self._log_project_event(
                level="error",
                module="playback",
                action="telegram_recent_playback_local_fallback",
                message="统一播放历史读取失败，已回退本地日志。",
                detail={"error": activity_error, "localRows": len(local_rows), "eventLogPath": str(self.event_log_path)},
            )
            return result, f"Emby 活动日志暂不可用，仅显示本地记录。"

    def _build_recent_playback_rows_from_activity_log(self, *, limit: int = 30) -> list[dict[str, Any]]:
        logs = self._emby_get(f"/System/ActivityLog/Entries?Limit={max(10, int(limit or 30))}&StartIndex=0")
        items = logs.get("Items") if isinstance(logs, dict) else []
        if not isinstance(items, list):
            self._log_project_event(
                level="warning",
                module="playback",
                action="telegram_recent_playback_activity_unexpected",
                message="Emby 活动日志返回格式不是列表。",
                detail={"payloadType": type(logs).__name__},
            )
            return []
        rows: list[dict[str, Any]] = []
        skipped = 0
        for log in items:
            if not isinstance(log, dict):
                continue
            played_at = self._parse_activity_log_datetime(log)
            if not played_at:
                skipped += 1
                continue
            text = self._activity_log_text(log)
            if not self._is_playback_activity_log(log, text):
                skipped += 1
                continue
            media_name = self._extract_activity_log_media_name(log, text)
            raw: dict[str, Any] = {"Name": media_name}
            rows.append(
                {
                    "at": played_at.isoformat(timespec="seconds"),
                    "username": self._extract_activity_log_username(log, text),
                    "mediaName": media_name,
                    "raw": raw,
                }
            )
        rows.sort(key=lambda row: str(row.get("at") or ""), reverse=True)
        self._log_project_event(
            level="info" if rows else "warning",
            module="playback",
            action="telegram_recent_playback_activity_scanned",
            message="已扫描 Emby 活动日志用于最近播放。",
            detail={"activityItems": len(items), "playbackRows": len(rows), "skippedRows": skipped},
        )
        return rows

    def _recent_playback_dedupe_key(self, row: dict[str, Any]) -> str:
        at_text = str(row.get("at") or "").strip()
        try:
            at = datetime.fromisoformat(at_text)
            minute = at.strftime("%Y-%m-%d %H:%M")
        except Exception:
            minute = at_text[:16]
        username = str(row.get("username") or "").strip().lower()
        media = self._format_episode_title_from_row(row).lower()
        return f"{minute}|{username}|{media}"

    def _cmd_recent_library(self) -> str:
        latest, tried_paths, last_error = self._fetch_latest_items_with_fallback(limit=10)
        rows = latest if isinstance(latest, list) else []
        if not rows:
            if last_error:
                return self._build_latest_error_message(tried_paths=tried_paths, last_error=last_error)
            return "📥 🆕 最近入库\n━━━━━━━━━━━━━━\n暂无入库数据。"
        lines = ["📥 🆕 最近入库", "━━━━━━━━━━━━━━"]
        for row in rows[:10]:
            if not isinstance(row, dict):
                continue
            lines.append(self._format_recent_library_row(row))
        return "\n".join(lines)

    def _format_recent_library_row(self, row: dict[str, Any]) -> str:
        item_type = str(row.get("Type") or "").strip().lower()
        name = str(row.get("Name") or "").strip() or "未命名内容"

        if item_type == "movie":
            return f"• 🏷️《{name}》【电影完整版】✅已入库"

        if item_type == "episode":
            series_name = str(row.get("SeriesName") or "").strip() or "未知剧名"
            season = self._coerce_index_number(row.get("ParentIndexNumber"))
            episode = self._coerce_index_number(row.get("IndexNumber"))
            season_text = str(season) if season is not None else "X"
            episode_text = str(episode) if episode is not None else "X"
            return f"• 🏷️《{series_name}》第{season_text}季 第{episode_text}集「{name}」✅已入库"

        # Series 类型通常只有剧名，需要补查“最新一集”来拿季/集和标题。
        if item_type == "series":
            series_name = name
            detail = self._resolve_latest_episode_for_series(row)
            if detail:
                season_text, episode_text, episode_title = detail
                return f"• 🏷️《{series_name}》第{season_text}季 第{episode_text}集「{episode_title}」✅已入库"
            return f"• 🏷️《{series_name}》第X季 第X集「最新更新」✅已入库"

        season = self._coerce_index_number(row.get("ParentIndexNumber"))
        episode = self._coerce_index_number(row.get("IndexNumber"))
        season_text = str(season) if season is not None else "X"
        episode_text = str(episode) if episode is not None else "X"
        return f"• 🏷️《{name}》第{season_text}季 第{episode_text}集「最新更新」✅已入库"

    def _resolve_latest_episode_for_series(self, row: dict[str, Any]) -> tuple[int, int, str] | None:
        series_id = str(row.get("Id") or "").strip()
        if not series_id:
            return None
        path = (
            "/Items?"
            f"ParentId={urllib.parse.quote(series_id, safe='')}"
            "&Recursive=true"
            "&IncludeItemTypes=Episode"
            "&SortBy=DateCreated"
            "&SortOrder=Descending"
            "&Limit=1"
            "&Fields=Name,SeriesName,ParentIndexNumber,IndexNumber"
        )
        try:
            payload = self._emby_get(path)
        except Exception:
            return None
        items = payload.get("Items") if isinstance(payload, dict) else []
        if not isinstance(items, list) or not items:
            return None
        first = items[0] if isinstance(items[0], dict) else {}
        season = self._coerce_index_number(first.get("ParentIndexNumber"))
        episode = self._coerce_index_number(first.get("IndexNumber"))
        if season is None or episode is None:
            return None
        title = str(first.get("Name") or "最新更新").strip() or "最新更新"
        return season, episode, title

    @staticmethod
    def _coerce_index_number(value: Any) -> int | None:
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        text = str(value or "").strip()
        if text.isdigit():
            try:
                return int(text)
            except Exception:
                return None
        return None

    def _fetch_latest_items_with_fallback(
        self,
        *,
        limit: int,
    ) -> tuple[list[dict[str, Any]], list[str], Exception | None]:
        safe_limit = max(1, min(30, int(limit or 10)))
        tried_paths: list[str] = []
        last_error: Exception | None = None

        direct_path = f"/Items/Latest?Limit={safe_limit}"
        rows, err = self._try_latest_path(direct_path)
        tried_paths.append(direct_path)
        if rows is not None:
            return rows, tried_paths, None
        last_error = err

        user_id = self._resolve_emby_user_id()
        if not user_id:
            return [], tried_paths, last_error

        user_paths = [
            f"/Users/{urllib.parse.quote(user_id, safe='')}/Items/Latest?Limit={safe_limit}",
            f"/Users/{urllib.parse.quote(user_id, safe='')}/Items/Latest?IncludeItemTypes=Movie,Series,Episode&Limit={safe_limit}",
        ]
        for path in user_paths:
            rows, err = self._try_latest_path(path)
            tried_paths.append(path)
            if rows is not None:
                return rows, tried_paths, None
            last_error = err
        return [], tried_paths, last_error

    def _try_latest_path(self, path: str) -> tuple[list[dict[str, Any]] | None, Exception | None]:
        try:
            payload = self._emby_get(path)
        except Exception as err:
            self._log_latest_attempt(path=path, err=err)
            return None, err
        rows = payload if isinstance(payload, list) else []
        self._log_latest_attempt(path=path, err=None, rows=len(rows))
        return rows, None

    def _resolve_emby_user_id(self) -> str:
        # 1) 优先活跃会话用户
        try:
            sessions = self._emby_get("/Sessions")
        except Exception as err:
            sessions = []
            LOGGER.warning("Resolve Emby user id from /Sessions failed: %s", err)
        if isinstance(sessions, list):
            for row in sessions:
                if not isinstance(row, dict):
                    continue
                user_id = str(row.get("UserId") or "").strip()
                if user_id:
                    return user_id
                user = row.get("User") if isinstance(row.get("User"), dict) else {}
                user_id = str(user.get("Id") or "").strip()
                if user_id:
                    return user_id

        # 2) 回退 /Users
        for path in ("/Users", "/Users/Query"):
            try:
                payload = self._emby_get(path)
            except Exception as err:
                LOGGER.warning("Resolve Emby user id from %s failed: %s", path, err)
                continue
            rows = payload.get("Items") if isinstance(payload, dict) else payload
            if not isinstance(rows, list):
                continue
            for row in rows:
                if not isinstance(row, dict):
                    continue
                user_id = str(row.get("Id") or "").strip()
                if user_id:
                    return user_id
        return ""

    def _build_latest_error_message(self, *, tried_paths: list[str], last_error: Exception) -> str:
        detail = self._format_emby_error(last_error)
        tried = "；".join(tried_paths[:3]) if tried_paths else "-"
        return (
            "🆕 最近入库\n"
            "最近入库获取失败：Emby Latest 接口不可用\n"
            f"诊断：尝试路径 {tried}\n"
            f"最后错误：{detail}"
        )

    @staticmethod
    def _format_emby_error(err: Exception) -> str:
        if isinstance(err, urllib.error.HTTPError):
            return f"HTTP {err.code}"
        return str(err)

    def _log_latest_attempt(self, *, path: str, err: Exception | None, rows: int | None = None) -> None:
        if err is None:
            LOGGER.info("TG /zuijinruku latest fetch success path=%s rows=%s", path, rows if rows is not None else "-")
            return
        if isinstance(err, urllib.error.HTTPError):
            LOGGER.warning("TG /zuijinruku latest fetch failed path=%s status=%s", path, err.code)
            return
        LOGGER.warning("TG /zuijinruku latest fetch failed path=%s error=%s", path, err)

    def _cmd_search(self, args: str) -> CommandReply:
        keyword = str(args or "").strip()
        if not keyword:
            return "🔍 搜索资源\n用法：/sousuo 关键词"
        query = urllib.parse.urlencode(
            {
                "Recursive": "true",
                "SearchTerm": keyword,
                "IncludeItemTypes": "Series,Movie,Episode",
                "Fields": "Name,Type,ProductionYear,Overview,Genres,People,PremiereDate,ChildCount,RecursiveItemCount,ParentIndexNumber,IndexNumber,SeriesName,ImageTags,PrimaryImageItemId,Studios,ProductionLocations,OriginalTitle,Status,CommunityRating,CriticRating,RunTimeTicks,MediaSources,MediaStreams,Width,Height",
                "Limit": "20",
            }
        )
        try:
            result = self._emby_get(f"/Items?{query}")
        except Exception as err:
            self._log_project_event(
                level="error",
                module="webhook",
                action="telegram_search_failed",
                message="Telegram 搜索 Emby 资源失败。",
                detail={"keyword": keyword, "error": str(err)},
            )
            return f"🔍 搜索资源\n搜索失败：{err}"
        items = result.get("Items") if isinstance(result, dict) else []
        if not isinstance(items, list) or not items:
            self._log_project_event(
                level="warning",
                module="webhook",
                action="telegram_search_empty",
                message="Telegram 搜索未找到资源。",
                detail={"keyword": keyword},
            )
            return f"🔍 搜索资源\n未找到“{keyword}”"
        first = self._pick_best_search_item(items=items, keyword=keyword)
        if not first:
            self._log_project_event(
                level="warning",
                module="webhook",
                action="telegram_search_empty",
                message="Telegram 搜索未选出可用结果。",
                detail={"keyword": keyword, "resultCount": len(items)},
            )
            return f"🔍 搜索资源\n未找到“{keyword}”"
        return self._format_search_result(first, keyword=keyword)

    def _cmd_scan_library(self, args: str = "") -> CommandReply:
        keyword = str(args or "").strip()
        try:
            libraries = self._fetch_emby_libraries()
        except Exception as err:
            self._log_project_event(
                level="error",
                module="webhook",
                action="telegram_library_scan_list_failed",
                message="Telegram 扫描媒体库列表读取失败。",
                detail={"keyword": keyword, "error": str(err)},
            )
            return self._ai_markdown_reply("🔄 扫描媒体库", f"读取媒体库失败：{err}")
        if not libraries:
            self._log_project_event(
                level="warning",
                module="webhook",
                action="telegram_library_scan_list_empty",
                message="Telegram 扫描媒体库未读取到可展示媒体库。",
                detail={"keyword": keyword},
            )
            return self._ai_markdown_reply("🔄 扫描媒体库", "未读取到可扫描的 Emby 媒体库。")

        matched = self._filter_emby_libraries(libraries, keyword) if keyword else libraries
        display_rows = matched if matched else libraries
        visible_rows = display_rows[:20]
        intro_lines: list[str] = []
        if keyword and matched:
            intro_lines.append(f"已按关键词“{keyword}”匹配到 {len(matched)} 个媒体库。")
        elif keyword:
            intro_lines.append(f"未匹配到“{keyword}”，下面显示全部 {len(libraries)} 个媒体库。")
        else:
            intro_lines.append(f"当前可扫描媒体库：{len(libraries)} 个。")
        intro_lines.append("这里只展示扫描按钮，不会直接执行；点击按钮才会提交扫描。")
        intro_lines.append("")
        for idx, library in enumerate(visible_rows, start=1):
            name = str(library.get("name") or "未知媒体库").strip()
            lib_type = str(library.get("type") or "未知类型").strip()
            intro_lines.append(f"{idx}. {name}｜{lib_type}")
        if len(display_rows) > len(visible_rows):
            intro_lines.append(f"... 还有 {len(display_rows) - len(visible_rows)} 个媒体库未显示，可用 /saomiao 关键词 缩小范围。")
        self._log_project_event(
            level="info",
            module="webhook",
            action="telegram_library_scan_list_ready",
            message="Telegram 扫描媒体库列表已生成。",
            detail={
                "keyword": keyword,
                "total": len(libraries),
                "matched": len(matched),
                "displayed": len(visible_rows),
                "fallbackToAll": bool(keyword and not matched),
            },
        )
        return self._ai_markdown_reply(
            "🔄 扫描媒体库",
            "\n".join(intro_lines),
            reply_markup=self._build_scan_library_keyboard(visible_rows),
        )

    @staticmethod
    def _looks_like_115_share(text: str) -> bool:
        value = str(text or "").strip()
        return bool(re.search(r"(115\.com/s/|115cdn\.com/s/|anxia\.com/s/|share_code=)", value, flags=re.IGNORECASE))

    def _message_text_candidates(self, message: dict[str, Any], *, include_reply: bool = False) -> list[dict[str, str]]:
        candidates: list[dict[str, str]] = []
        seen: set[str] = set()

        def add(value: Any, source: str) -> None:
            text = str(value or "").strip()
            if not text or text in seen:
                return
            seen.add(text)
            candidates.append({"text": text, "source": source})

        def collect(current: dict[str, Any], prefix: str) -> None:
            body = str(current.get("text") or "").strip()
            caption = str(current.get("caption") or "").strip()
            add(body, f"{prefix}.text")
            add(caption, f"{prefix}.caption")
            self._collect_entity_urls(body, current.get("entities"), candidates, seen, f"{prefix}.text_entity")
            self._collect_entity_urls(caption, current.get("caption_entities"), candidates, seen, f"{prefix}.caption_entity")

        collect(message, "message")
        if include_reply:
            reply = message.get("reply_to_message")
            if isinstance(reply, dict):
                collect(reply, "reply")
        return candidates

    @staticmethod
    def _collect_entity_urls(
        text: str,
        entities: Any,
        candidates: list[dict[str, str]],
        seen: set[str],
        source: str,
    ) -> None:
        if not isinstance(entities, list):
            return
        for entity in entities:
            if not isinstance(entity, dict):
                continue
            entity_type = str(entity.get("type") or "").strip()
            value = ""
            if entity_type == "text_link":
                value = str(entity.get("url") or "").strip()
            elif entity_type == "url":
                try:
                    offset = int(entity.get("offset") or 0)
                    length = int(entity.get("length") or 0)
                    value = text[offset : offset + length].strip()
                except Exception:
                    value = ""
            if value and value not in seen:
                seen.add(value)
                candidates.append({"text": value, "source": source})

    def _find_115_share_candidate(self, candidates: list[dict[str, str]]) -> dict[str, str] | None:
        for candidate in candidates:
            text = str(candidate.get("text") or "").strip()
            if self._looks_like_115_share(text) and extract_115_share(text).get("shareCode"):
                return candidate
        return None

    @staticmethod
    def _mask_share_code(value: Any) -> str:
        text = str(value or "").strip()
        if len(text) <= 4:
            return text
        return f"{text[:3]}***{text[-2:]}"

    def _drive115_service(self) -> Drive115Service:
        store = _read_store(self.store_path)
        config = apply_drive115_env_overrides(store.get("drive115Config"))
        return Drive115Service(config)

    def _save_hdhive_runtime_config(self, config: dict[str, Any]) -> None:
        store = _read_store(self.store_path)
        current = normalize_hdhive_config(store.get("hdhiveConfig"))
        incoming = normalize_hdhive_config(config)
        for field in (
            "installationId", "installationSecret", "oauthSessionId", "oauthSessionExpiresAt",
            "accessToken", "refreshToken", "accessExpiresAt", "refreshExpiresAt",
            "scopes", "user", "lastCheckin", "lastCheckinDate", "updatedAt",
        ):
            current[field] = incoming.get(field)
        store["hdhiveConfig"] = normalize_hdhive_config(current)
        self.store_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.store_path.with_suffix(".hdhive.tmp")
        temp_path.write_text(json.dumps(store, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(temp_path, self.store_path)

    def _hdhive_service(self) -> HDHiveService:
        store = _read_store(self.store_path)
        config = apply_hdhive_env_overrides(store.get("hdhiveConfig"))
        return HDHiveService(config, save_config=self._save_hdhive_runtime_config)

    @staticmethod
    def _normalize_hdhive_resource(raw: Any) -> dict[str, Any]:
        source = raw if isinstance(raw, dict) else {}
        pan_type = str(source.get("pan_type") or source.get("website") or "").strip()
        return {
            "slug": str(source.get("slug") or "").strip(),
            "title": str(source.get("title") or "影巢资源").strip(),
            "panType": pan_type,
            "shareSize": str(source.get("share_size") or "").strip(),
            "resolution": source.get("video_resolution") or [],
            "source": source.get("source") or [],
            "unlockPoints": int(source.get("unlock_points") or 0),
            "isUnlocked": bool(source.get("is_unlocked")),
            "is115": "115" in pan_type.lower(),
        }

    def _cmd_hdhive_search(self, args: str = "") -> CommandReply:
        keyword = str(args or "").strip()
        if not keyword:
            return self._ai_markdown_reply("🪺 影巢资源搜索", "用法：/hdhive 片名\n例如：/hdhive 遮天")
        service = self._hdhive_service()
        if not service.config.get("enabled"):
            return self._ai_markdown_reply("🪺 影巢未启用", "请先在后台“影巢搜索”页面保存并启用 OpenAPI 配置。")
        authorized = bool(service.config.get("user")) if service.is_broker else bool(service.config.get("accessToken") or service.config.get("refreshToken"))
        if not authorized:
            return self._ai_markdown_reply("🪺 影巢未授权", "请先在后台完成影巢 OAuth 授权。")
        try:
            resolution = self._media_identity_service().resolve(keyword)
            if resolution.get("ambiguous"):
                candidates = resolution.get("candidates") if isinstance(resolution.get("candidates"), list) else []
                body = "找到多个同名作品，请带年份重新搜索：\n" + "\n".join(f"- {row.get('title')}（{row.get('year') or '年份未知'}）" for row in candidates[:5])
                return self._ai_markdown_reply("🪺 影巢资源搜索", body)
            identity = resolution.get("identity") if isinstance(resolution.get("identity"), dict) else {}
            tmdb_id = str(identity.get("tmdbId") or "").strip()
            if not tmdb_id:
                return self._ai_markdown_reply("🪺 影巢资源搜索", f"无法确认《{keyword}》的 TMDB 身份，请检查 TMDB 配置或补充年份。")
            result = service.search_resources(media_type=str(identity.get("type") or ""), tmdb_id=tmdb_id)
            resources = [self._normalize_hdhive_resource(row) for row in result.get("items") or []]
            resources = [row for row in resources if row.get("slug")][:8]
            if not resources:
                return self._ai_markdown_reply("🪺 影巢资源搜索", f"《{identity.get('title') or keyword}》暂未找到可用影巢资源。")
            store = _read_store(self.store_path)
            drive_config = apply_drive115_env_overrides(store.get("drive115Config"))
            target_cid = str(drive_config.get("defaultCid") or "0")
            now = time.time()
            self._pending_hdhive_actions = {key: value for key, value in self._pending_hdhive_actions.items() if now - float(value.get("createdAt") or 0) <= 900}
            lines = [f"《{identity.get('title') or keyword}》找到 {len(resources)} 条资源：", ""]
            buttons: list[list[dict[str, str]]] = []
            for index, resource in enumerate(resources, start=1):
                cost = "已解锁" if resource.get("isUnlocked") else f"{resource.get('unlockPoints') or 0} 积分"
                specs = " / ".join(str(item) for item in (resource.get("resolution") or []))
                lines.append(f"{index}. {resource.get('title')}\n   {resource.get('panType') or '未知网盘'} · {resource.get('shareSize') or '大小未知'} · {specs or '规格未知'} · {cost}")
                if resource.get("is115"):
                    action_id = secrets.token_urlsafe(7).replace("-", "").replace("_", "")[:10]
                    self._pending_hdhive_actions[action_id] = {"createdAt": now, "resource": resource, "targetCid": target_cid}
                    buttons.append([{"text": f"转存 #{index} · {cost}", "callback_data": f"hdhive:pick:{action_id}"}])
            if not buttons:
                lines.append("\n当前结果没有可转存的 115 资源。")
            self._log_project_event(level="info", module="hdhive", action="telegram_hdhive_search_success", message="Telegram 影巢资源搜索完成。", detail={"tmdbId": tmdb_id, "mediaType": identity.get("type"), "resultCount": len(resources)})
            return self._ai_markdown_reply("🪺 影巢资源搜索", "\n".join(lines), reply_markup={"inline_keyboard": buttons} if buttons else None)
        except HDHiveError as err:
            self._log_project_event(level="warning", module="hdhive", action="telegram_hdhive_search_failed", message="Telegram 影巢资源搜索失败。", detail={"code": err.code, "error": str(err)})
            suffix = f"\n请等待 {err.retry_after} 秒后重试。" if err.retry_after else ""
            return self._ai_markdown_reply("🪺 影巢搜索失败", f"{err}{suffix}")
        except Exception as err:
            return self._ai_markdown_reply("🪺 影巢搜索失败", str(err))

    def _cmd_drive115_transfer(self, args: str = "", *, reply_to_message_id: int = 0) -> CommandReply:
        text = str(args or "").strip()
        if not text:
            return self._ai_markdown_reply(
                "📦 115 链接转存",
                "用法：/zhuancun115 115分享链接\n\n私聊也可以直接发送包含 115 链接的资源消息，机器人识别后会立即转存。",
            )
        store = _read_store(self.store_path)
        config = apply_drive115_env_overrides(store.get("drive115Config"))
        if not bool(config.get("enabled")):
            return self._drive115_transfer_result(
                success=False,
                reason="请先在后台“115网盘”页面启用并保存配置",
                reply_to_message_id=reply_to_message_id,
            )
        if not str(config.get("cookie") or "").strip():
            return self._drive115_transfer_result(
                success=False,
                reason="请先在后台“115网盘”页面填写并保存 115 Cookie",
                reply_to_message_id=reply_to_message_id,
            )
        share = extract_115_share(text)
        if not share.get("shareCode"):
            return self._drive115_transfer_result(
                success=False,
                reason="未在当前消息或回复消息中识别到 115 分享链接",
                reply_to_message_id=reply_to_message_id,
            )
        masked_share_code = self._mask_share_code(share.get("shareCode"))
        service = Drive115Service(config)
        try:
            parsed = service.parse_share(
                share_url=text,
                receive_code=str(share.get("receiveCode") or ""),
            )
        except Exception as err:
            self._log_project_event(
                level="error",
                module="drive115",
                action="telegram_drive115_parse_failed",
                message="Telegram 115 分享解析失败。",
                detail={
                    "shareCode": masked_share_code,
                    "error": str(err),
                    "successCount": 0,
                    "failureCount": 1,
                    "replyToMessage": bool(reply_to_message_id),
                },
            )
            return self._drive115_transfer_result(
                success=False,
                reason=str(err),
                reply_to_message_id=reply_to_message_id,
            )

        file_ids = [str(row.get("id") or "").strip() for row in parsed.get("files", []) if str(row.get("id") or "").strip()]
        target_cid = str(config.get("defaultCid") or "0").strip() or "0"
        detail = {
            "shareCode": self._mask_share_code(parsed.get("shareCode") or share.get("shareCode")),
            "targetCid": target_cid,
            "title": str(parsed.get("title") or "").strip(),
            "fileCount": int(parsed.get("fileCount") or len(file_ids) or 0),
            "replyToMessage": bool(reply_to_message_id),
        }
        try:
            result = service.transfer_share(
                share_code=str(parsed.get("shareCode") or share.get("shareCode") or ""),
                receive_code=str(parsed.get("receiveCode") or share.get("receiveCode") or ""),
                target_cid=target_cid,
                file_ids=file_ids,
                source_files=parsed.get("files") if isinstance(parsed.get("files"), list) else [],
            )
            if not bool(result.get("ok", True)):
                raise RuntimeError(str(result.get("message") or "115 未接受转存请求"))
            status = str(result.get("status") or "submitted")
            exists = status == "exists"
            detail.update({"successCount": 0 if exists else 1, "existsCount": 1 if exists else 0, "failureCount": 0, "status": status})
            self._log_project_event(
                level="info",
                module="drive115",
                action="telegram_drive115_transfer_submitted",
                message="Telegram 115 转存已提交。",
                detail=detail,
            )
            return self._drive115_transfer_result(
                status=status,
                reply_to_message_id=reply_to_message_id,
            )
        except Exception as err:
            LOGGER.warning("Telegram 115 direct transfer failed: %s", err)
            detail.update({"successCount": 0, "failureCount": 1, "error": str(err)})
            self._log_project_event(
                level="error",
                module="drive115",
                action="telegram_drive115_transfer_failed",
                message="Telegram 115 转存失败。",
                detail=detail,
            )
            return self._drive115_transfer_result(
                status="failed",
                reason=str(err),
                reply_to_message_id=reply_to_message_id,
            )

    @staticmethod
    def _drive115_transfer_result(*, status: str = "", success: bool | None = None, reason: str = "", reply_to_message_id: int = 0) -> dict[str, Any]:
        normalized = str(status or ("submitted" if success else "failed")).strip().lower()
        if normalized == "exists":
            text = "转存完成：成功 0 个，已存在 1 个，失败 0 个"
        elif normalized == "submitted":
            text = "转存完成：成功 1 个，已存在 0 个，失败 0 个"
        else:
            safe_reason = str(reason or "115 未接受转存请求").strip()
            text = f"转存完成：成功 0 个，已存在 0 个，失败 1 个\n原因：{safe_reason}"
        return {
            "text": text,
            "fallback_text": text,
            "reply_to_message_id": int(reply_to_message_id or 0),
        }

    def _cleanup_pending_drive115_transfers(self) -> None:
        now = time.time()
        expired = [key for key, value in self._pending_drive115_transfers.items() if now - float(value.get("createdAt") or 0) > 900]
        for key in expired:
            self._pending_drive115_transfers.pop(key, None)
        overflow = len(self._pending_drive115_transfers) - 100
        if overflow > 0:
            for key in list(self._pending_drive115_transfers.keys())[:overflow]:
                self._pending_drive115_transfers.pop(key, None)

    def _cmd_ai(self, args: str) -> CommandReply:
        question = str(args or "").strip()
        if not question:
            return self._ai_markdown_reply("🧠 AI 媒体问答", "用法：/ai 推荐一部最近入库的动漫")
        hdhive_match = re.match(r"^\s*(?:请)?(?:用)?(?:影巢|hdhive)(?:搜索|搜|查找|查)\s*(.+)$", question, flags=re.IGNORECASE)
        if hdhive_match:
            return self._cmd_hdhive_search(hdhive_match.group(1).strip())
        store = _read_store(self.store_path)
        ai_config = apply_ai_env_overrides(store.get("aiConfig"))
        if not bool(ai_config.get("enabled")):
            return self._ai_markdown_reply("🧠 AI 助手未启用", "请先在系统设置里启用并保存 AI 配置。")
        if not str(ai_config.get("apiKey") or "").strip() or not str(ai_config.get("baseUrl") or "").strip():
            return self._ai_markdown_reply("🧠 AI 配置不完整", "请先填写 Base URL、API Key 和模型名称。")

        safety_reply = self._build_ai_safety_reply(question)
        if safety_reply:
            return self._ai_markdown_reply("🧠 AI 安全限制", safety_reply)

        question, route_reply = self._prepare_ai_routed_question(
            question,
            ai_config=ai_config,
            conversation_key="",
        )
        if route_reply:
            return self._ai_markdown_reply("🧠 AI 媒体问答", route_reply)

        execution_reply = self._build_ai_execution_proposal(question)
        if execution_reply:
            return execution_reply

        episode_reply = self._build_ai_episode_query_reply(question)
        if episode_reply:
            return self._ai_markdown_reply("🧠 AI 媒体问答", episode_reply)

        messages = self._build_ai_messages(question, ai_config=ai_config)
        try:
            started = time.time()
            answer = chat_completion(config=ai_config, messages=messages, timeout_seconds=45)
            elapsed_ms = int((time.time() - started) * 1000)
        except Exception as err:
            if self._is_ai_context_limit_error(err):
                try:
                    messages = self._shrink_ai_messages(messages)
                    answer = chat_completion(config=ai_config, messages=messages, timeout_seconds=45)
                    elapsed_ms = int((time.time() - started) * 1000)
                except Exception as retry_err:
                    err = retry_err
                else:
                    err = None
            if err is None:
                pass
            else:
                self._log_project_event(
                    level="warning",
                    module="webhook",
                    action="telegram_ai_failed",
                    message="Telegram AI 问答失败。",
                    detail={"model": str(ai_config.get("model") or ""), "error": str(err)},
                )
                return self._ai_markdown_reply("🧠 AI 问答失败", str(err))

        self._log_project_event(
            level="info",
            module="webhook",
            action="telegram_ai_success",
            message="Telegram AI 问答已返回。",
            detail={
                "model": str(ai_config.get("model") or ""),
                "elapsedMs": elapsed_ms,
                "estimatedContextTokens": sum(self._estimate_ai_tokens(str(row.get("content") or "")) for row in messages),
            },
        )
        self._remember_ai_exchange(chat_id="", question=question, answer=answer)
        return self._ai_markdown_reply("🧠 AI 媒体问答", self._truncate_text(answer, 3400))

    def _dispatch_ai_streaming(
        self,
        args: str,
        *,
        token: str,
        chat_id: str,
        conversation_key: str = "",
    ) -> CommandReply | None:
        question = str(args or "").strip()
        if not question:
            return self._ai_markdown_reply("🧠 AI 媒体问答", "用法：/ai 推荐一部最近入库的动漫")
        hdhive_match = re.match(r"^\s*(?:请)?(?:用)?(?:影巢|hdhive)(?:搜索|搜|查找|查)\s*(.+)$", question, flags=re.IGNORECASE)
        if hdhive_match:
            return self._cmd_hdhive_search(hdhive_match.group(1).strip())
        store = _read_store(self.store_path)
        ai_config = apply_ai_env_overrides(store.get("aiConfig"))
        if not bool(ai_config.get("enabled")):
            return self._ai_markdown_reply("🧠 AI 助手未启用", "请先在系统设置里启用并保存 AI 配置。")
        if not str(ai_config.get("apiKey") or "").strip() or not str(ai_config.get("baseUrl") or "").strip():
            return self._ai_markdown_reply("🧠 AI 配置不完整", "请先填写 Base URL、API Key 和模型名称。")

        if self._is_ai_context_status_request(question):
            return self._ai_markdown_reply("🧠 AI 当前上下文", self._format_ai_context_status(conversation_key, ai_config=ai_config))
        if self._is_ai_context_reset_request(question):
            existed = self._ai_conversations.clear(conversation_key)
            self._ai_chat_history.pop(chat_id, None)
            message = "已清除当前聊天的 AI 上下文。" if existed else "当前聊天没有可清除的 AI 上下文。"
            return self._ai_markdown_reply("🧠 AI 上下文", message)

        safety_reply = self._build_ai_safety_reply(question)
        if safety_reply:
            return self._ai_markdown_reply("🧠 AI 安全限制", safety_reply)

        question, route_reply = self._prepare_ai_routed_question(
            question,
            ai_config=ai_config,
            conversation_key=conversation_key,
        )
        if route_reply:
            self._remember_ai_exchange(chat_id=chat_id, question=str(args or "").strip(), answer=route_reply)
            self._ai_conversations.remember(conversation_key, question=str(args or "").strip(), answer=route_reply)
            return self._ai_markdown_reply("🧠 AI 媒体问答", route_reply)

        execution_reply = self._build_ai_execution_proposal(question)
        if execution_reply:
            return execution_reply

        media_detail_reply = self._build_ai_media_detail_reply(question, conversation_key=conversation_key)
        if media_detail_reply:
            self._remember_ai_exchange(chat_id=chat_id, question=question, answer=media_detail_reply)
            self._ai_conversations.remember(conversation_key, question=question, answer=media_detail_reply)
            return self._ai_markdown_reply("🧠 AI 媒体问答", media_detail_reply)

        missing_reply = self._build_ai_missing_episode_reply(question, conversation_key=conversation_key, rich=True, chat_id=chat_id)
        if missing_reply:
            memory_text = str(missing_reply.get("memory_text") or missing_reply.get("fallback_text") or "") if isinstance(missing_reply, dict) else str(missing_reply)
            self._remember_ai_exchange(chat_id=chat_id, question=question, answer=memory_text)
            self._ai_conversations.remember(conversation_key, question=question, answer=memory_text)
            return missing_reply if isinstance(missing_reply, dict) else self._ai_markdown_reply("🧠 AI 媒体问答", missing_reply)

        episode_reply = self._build_ai_episode_query_reply(question, conversation_key=conversation_key)
        if episode_reply:
            self._remember_ai_exchange(chat_id=chat_id, question=question, answer=episode_reply)
            self._ai_conversations.remember(conversation_key, question=question, answer=episode_reply)
            return self._ai_markdown_reply("🧠 AI 媒体问答", episode_reply)

        messages = self._build_ai_messages(question, chat_id=chat_id, conversation_key=conversation_key, ai_config=ai_config)
        message_id = self._send_ai_markdown_message(
            token=token,
            chat_id=chat_id,
            title="🧠 AI 媒体问答",
            body="正在思考...",
        )
        if not message_id:
            return self._cmd_ai(args)
        started = time.time()
        try:
            answer = self._stream_ai_answer_to_telegram(
                token=token,
                chat_id=chat_id,
                message_id=message_id,
                ai_config=ai_config,
                messages=messages,
            )
        except Exception as stream_err:
            LOGGER.warning("Telegram AI streaming failed, fallback to plain completion: %s", stream_err)
            try:
                fallback_messages = self._shrink_ai_messages(messages) if self._is_ai_context_limit_error(stream_err) else messages
                answer = chat_completion(config=ai_config, messages=fallback_messages, timeout_seconds=45)
                self._edit_ai_markdown_message(
                    token=token,
                    chat_id=chat_id,
                    message_id=message_id,
                    title="🧠 AI 媒体问答",
                    body=self._truncate_text(answer, 3400),
                )
                self._remember_ai_exchange(chat_id=chat_id, question=question, answer=answer)
                self._ai_conversations.remember(conversation_key, question=question, answer=answer)
            except Exception as err:
                self._log_project_event(
                    level="warning",
                    module="webhook",
                    action="telegram_ai_failed",
                    message="Telegram AI 问答失败。",
                    detail={"model": str(ai_config.get("model") or ""), "error": str(err)},
                )
                self._edit_ai_markdown_message(
                    token=token,
                    chat_id=chat_id,
                    message_id=message_id,
                    title="🧠 AI 问答失败",
                    body=str(err),
                )
                return None

        elapsed_ms = int((time.time() - started) * 1000)
        self._log_project_event(
            level="info",
            module="webhook",
            action="telegram_ai_success",
            message="Telegram AI 问答已返回。",
            detail={
                "model": str(ai_config.get("model") or ""),
                "elapsedMs": elapsed_ms,
                "streaming": True,
                "estimatedContextTokens": sum(self._estimate_ai_tokens(str(row.get("content") or "")) for row in messages),
            },
        )
        self._remember_ai_exchange(chat_id=chat_id, question=question, answer=answer)
        self._ai_conversations.remember(conversation_key, question=question, answer=answer)
        return None

    def _prepare_ai_routed_question(
        self,
        question: str,
        *,
        ai_config: dict[str, Any],
        conversation_key: str,
    ) -> tuple[str, str]:
        original = str(question or "").strip()
        active = self._get_ai_active_media(conversation_key)
        router = AiIntentRouter(chat_completion)
        route = router.route(original, config=ai_config, active_media=active)
        intent = str(route.get("intent") or "general_chat")
        media_intents = {
            "media_missing_episodes",
            "media_episode_progress",
            "media_detail",
            "media_search",
            "media_correction",
        }
        immediate = ""
        if intent in media_intents:
            route, immediate = self._validate_ai_media_route(
                original,
                route=route,
                conversation_key=conversation_key,
            )
        self._log_project_event(
            level="info",
            module="webhook",
            action="telegram_ai_intent_routed",
            message="Telegram AI 意图识别完成。",
            detail={
                "intent": intent,
                "mediaTitle": str(route.get("mediaTitle") or ""),
                "useActiveMedia": bool(route.get("useActiveMedia")),
                "isCorrection": bool(route.get("isCorrection")),
                "confidence": route.get("confidence"),
                "source": str(route.get("source") or "fallback"),
                "validated": bool(route.get("validated")),
                "routerError": str(route.get("routerError") or ""),
            },
        )
        if immediate:
            return original, immediate

        title = str(route.get("mediaTitle") or "").strip()
        if bool(route.get("useActiveMedia")) and not title:
            title = str(active.get("title") or "").strip()
        if intent == "media_missing_episodes":
            return (f"查看一下{title}的缺失集" if title else "查看一下缺失的集"), ""
        if intent == "media_episode_progress":
            return (f"{title}现在最新多少集" if title else "它现在最新多少集"), ""
        if intent == "media_detail":
            return (f"{title}的简介详情" if title else "它的简介详情"), ""
        if intent == "media_search" and title:
            return f"媒体库里有没有{title}", ""
        if intent == "media_correction" and title:
            return original, f"已确认，你要查询的是《{title}》。"
        return original, ""

    def _validate_ai_media_route(
        self,
        question: str,
        *,
        route: dict[str, Any],
        conversation_key: str,
    ) -> tuple[dict[str, Any], str]:
        output = dict(route)
        active = self._get_ai_active_media(conversation_key)
        if bool(output.get("useActiveMedia")):
            active_title = str(active.get("title") or "").strip()
            if not active_title:
                return output, "当前没有记住正在讨论的作品，请带上片名再查询。"
            output["mediaTitle"] = active_title
            output["validated"] = True
            return output, ""

        llm_title = str(output.get("mediaTitle") or "").strip()
        rule_title = self._extract_ai_media_keyword(question)
        candidates = AiIntentRouter.title_candidates(
            question=question,
            llm_title=llm_title,
            rule_title=rule_title,
        )
        if not candidates:
            if bool(output.get("isCorrection")):
                return output, "我知道刚才理解错了，但还没识别出你要查询的片名。请直接发送作品名称。"
            return output, "请带上要查询的影视名称。"

        service = self._media_identity_service()
        preferred_type = "series" if str(output.get("mediaType") or "") == "tv" else str(output.get("mediaType") or "")
        ambiguous_candidates: list[dict[str, Any]] = []
        for candidate in candidates:
            try:
                resolution = service.resolve(candidate, preferred_type=preferred_type)
            except Exception:
                continue
            rows = resolution.get("candidates") if isinstance(resolution.get("candidates"), list) else []
            if resolution.get("ambiguous") and rows:
                ambiguous_candidates = rows
                continue
            identity = resolution.get("identity") if isinstance(resolution.get("identity"), dict) else {}
            emby_item = resolution.get("embyItem") if isinstance(resolution.get("embyItem"), dict) else {}
            resolved_title = str(identity.get("title") or emby_item.get("Name") or "").strip()
            if not resolved_title:
                continue
            output["mediaTitle"] = resolved_title
            output["mediaType"] = "tv" if str(identity.get("type") or emby_item.get("Type") or "").lower() in {"series", "tv"} else "movie"
            output["validated"] = True
            output["resolvedIdentity"] = {
                "title": resolved_title,
                "year": identity.get("year") or emby_item.get("ProductionYear"),
                "type": str(identity.get("type") or "series"),
                "tmdbId": str(identity.get("tmdbId") or ""),
                "embySeriesId": str(emby_item.get("Id") or identity.get("embyId") or ""),
            }
            if bool(output.get("isCorrection")) or str(output.get("intent")) == "media_correction":
                self._ai_conversations.set_active_media(conversation_key, output["resolvedIdentity"])
            return output, ""

        if ambiguous_candidates:
            return output, self._format_ai_identity_candidates(candidates[0], ambiguous_candidates)
        shown = candidates[-1] if len(candidates) > 1 else candidates[0]
        return output, f"没有在 Emby 或 TMDB 中确认《{shown}》。请检查片名，或用书名号明确输入，例如《仙逆》。"

    def _build_ai_messages(
        self,
        question: str,
        *,
        chat_id: str = "",
        conversation_key: str = "",
        ai_config: dict[str, Any] | None = None,
    ) -> list[dict[str, str]]:
        context_parts = [
            self._build_ai_media_context(question=question),
            self._build_ai_recent_operations_context(),
        ]
        active_context = self._build_ai_active_media_context(conversation_key)
        conversation_context = self._build_ai_conversation_context(chat_id=chat_id, conversation_key=conversation_key)
        if conversation_context:
            context_parts.append(conversation_context)
        if active_context:
            context_parts.append(active_context)
        context = self._limit_ai_context_text("\n\n".join(part for part in context_parts if part), ai_config=ai_config)
        return [
            {
                "role": "system",
                "content": (
                    "你是镜界 Vistamirror 的媒体库 AI 助手。"
                    "请用简洁中文回答，优先依据提供的媒体库上下文。"
                    "如果上下文里有“当前媒体库统计”或“命中资源详情”，必须直接使用这些准确数字回答；"
                    "如果上下文里有“最终判断”，必须按最终判断回答，并可简短说明冲突来源；"
                    "如果上下文里包含任务、日志、缺集或播放历史摘要，也要优先引用这些项目资料。"
                    "如果用户问“刚才”“执行了吗”“扫描了吗”“上一个操作怎么样”，必须优先查看最近项目操作和会话历史。"
                    "如果最近操作显示已提交扫描或任务，直接说明目标、时间和结果。"
                    "如果用户用“它/这个/那个”追问，要结合最近会话历史判断指代对象。"
                    "如果上下文不足，要明确说明不确定，不要编造不存在的片名或数据。"
                    "不要输出任何 Token、API Key、密码或密钥。"
                    "如果用户请求删除、清空、改密钥或改密码，应拒绝并建议到后台手动操作。"
                    "最终回答要适合 Telegram MarkdownV2 代码块展示，内容短段落、列表清晰。"
                    "不要输出 Markdown 包裹语法，由后端统一包装。"
                ),
            },
            {"role": "user", "content": f"媒体库上下文：\n{context}\n\n用户问题：{question}"},
        ]

    def _remember_ai_exchange(self, *, chat_id: str, question: str, answer: str) -> None:
        safe_chat_id = str(chat_id or "").strip()
        if not safe_chat_id:
            return
        rows = self._ai_chat_history.setdefault(safe_chat_id, [])
        now = datetime.now().strftime("%m-%d %H:%M")
        rows.append(
            {
                "time": now,
                "user": self._truncate_text(str(question or "").strip(), 500),
                "assistant": self._truncate_text(str(answer or "").strip(), 900),
            }
        )
        self._ai_chat_history[safe_chat_id] = rows[-10:]

    def _build_ai_conversation_context(self, *, chat_id: str, conversation_key: str = "") -> str:
        persistent = self._ai_conversations.get(conversation_key)
        persistent_rows = persistent.get("recent") if isinstance(persistent.get("recent"), list) else []
        if persistent_rows:
            lines = ["最近 Telegram 对话上下文："]
            summary = str(persistent.get("summary") or "").strip()
            if summary:
                lines.append("长期摘要：")
                lines.append(summary)
            for row in persistent_rows:
                if not isinstance(row, dict):
                    continue
                lines.append(f"- {row.get('time', '')} 用户：{row.get('user', '')}")
                lines.append(f"  AI：{row.get('assistant', '')}")
            return "\n".join(lines)
        safe_chat_id = str(chat_id or "").strip()
        rows = self._ai_chat_history.get(safe_chat_id) if safe_chat_id else []
        if not rows:
            return ""
        lines = ["最近 Telegram 对话上下文："]
        for row in rows[-6:]:
            if not isinstance(row, dict):
                continue
            lines.append(f"- {row.get('time', '')} 用户：{row.get('user', '')}")
            lines.append(f"  AI：{row.get('assistant', '')}")
        return "\n".join(lines)

    def _build_ai_active_media_context(self, conversation_key: str) -> str:
        session = self._ai_conversations.get(conversation_key)
        media = session.get("activeMedia") if isinstance(session.get("activeMedia"), dict) else {}
        if not media:
            return ""
        return "\n".join(
            [
                "当前对话媒体：",
                f"- 标题：{media.get('title') or '未知'}",
                f"- Emby Series ID：{media.get('embySeriesId') or '未知'}",
                f"- TMDB ID：{media.get('tmdbId') or '未知'}",
                f"- 最新单集：{media.get('latestEpisode') or '未知'}",
                f"- 实际可读取：{media.get('actualEpisodeCount') or 0} 集",
                f"- TMDB 应有：{media.get('expectedEpisodeCount') or 0} 集",
                f"- 当前缺失：{media.get('missingEpisodeCount') or 0} 集",
                "- 规则：用户说“它/这部/这个/缺失的集”时指向该媒体。",
            ]
        )

    def _build_ai_recent_operations_context(self) -> str:
        try:
            events, _total = read_project_events(self.event_log_path, limit=60)
        except Exception as err:
            return f"最近项目操作：读取失败（{err}）。"
        important_actions = {
            "telegram_library_scan_submitted",
            "telegram_library_scan_failed",
            "telegram_library_scan_reply_sent",
            "telegram_library_scan_list_ready",
            "telegram_command_reply_failed",
            "telegram_ai_success",
            "telegram_ai_failed",
            "telegram_callback_failed",
            "client_sync_event",
        }
        rows = [event for event in events if isinstance(event, dict) and str(event.get("action") or "") in important_actions]
        if not rows:
            return "最近项目操作：暂无可用记录。"
        lines = ["最近项目操作："]
        for event in rows[:12]:
            action = str(event.get("action") or "").strip()
            message = str(event.get("message") or "").strip()
            detail = event.get("detail") if isinstance(event.get("detail"), dict) else {}
            summary = self._format_ai_event_detail(action=action, detail=detail)
            suffix = f"；{summary}" if summary else ""
            lines.append(f"- {event.get('time', '')} {message or action}{suffix}")
        return "\n".join(lines)

    @staticmethod
    def _format_ai_event_detail(*, action: str, detail: dict[str, Any]) -> str:
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

    @classmethod
    def _limit_ai_context_text(cls, text: str, *, ai_config: dict[str, Any] | None = None) -> str:
        config = ai_config if isinstance(ai_config, dict) else {}
        try:
            context_tokens_k = int(config.get("contextTokensK") or 64)
        except (TypeError, ValueError):
            context_tokens_k = 64
        context_tokens_k = max(4, min(1024, context_tokens_k))
        reserved_tokens = max(2048, int(config.get("maxTokens") or 800) + 1200)
        max_tokens = max(2000, context_tokens_k * 1000 - reserved_tokens)
        value = str(text or "")
        if cls._estimate_ai_tokens(value) <= max_tokens:
            return value
        low = 0
        high = len(value)
        while low < high:
            middle = (low + high + 1) // 2
            if cls._estimate_ai_tokens(value[-middle:]) <= max_tokens:
                low = middle
            else:
                high = middle - 1
        tail = value[-low:] if low > 0 else ""
        return "[较早上下文已按 Token 预算压缩]\n" + tail

    @staticmethod
    def _estimate_ai_tokens(text: str) -> int:
        value = str(text or "")
        cjk = len(re.findall(r"[\u3400-\u9fff\uf900-\ufaff]", value))
        other = max(0, len(value) - cjk)
        return cjk + (other + 3) // 4

    @staticmethod
    def _is_ai_context_limit_error(error: Any) -> bool:
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

    @classmethod
    def _shrink_ai_messages(cls, messages: list[dict[str, str]]) -> list[dict[str, str]]:
        output: list[dict[str, str]] = []
        for message in messages:
            row = dict(message)
            content = str(row.get("content") or "")
            if row.get("role") == "user" and len(content) > 2000:
                keep = max(1000, len(content) // 2)
                row["content"] = "[较早上下文已因模型限制缩减]\n" + content[-keep:]
            output.append(row)
        return output

    @staticmethod
    def _is_ai_context_status_request(question: str) -> bool:
        return bool(re.fullmatch(r"(?:/ai\s*)?(?:当前上下文|查看上下文|上下文状态)", str(question or "").strip()))

    @staticmethod
    def _is_ai_context_reset_request(question: str) -> bool:
        return bool(re.fullmatch(r"(?:/ai\s*)?(?:重置上下文|清除上下文|忘记当前话题)", str(question or "").strip()))

    def _format_ai_context_status(self, conversation_key: str, *, ai_config: dict[str, Any]) -> str:
        session = self._ai_conversations.get(conversation_key)
        media = session.get("activeMedia") if isinstance(session.get("activeMedia"), dict) else {}
        recent = session.get("recent") if isinstance(session.get("recent"), list) else []
        summary = str(session.get("summary") or "")
        context_text = self._build_ai_conversation_context(chat_id="", conversation_key=conversation_key)
        return "\n".join(
            [
                f"当前作品：{media.get('title') or '未设置'}",
                f"已保存问答：{len(recent)} 轮",
                f"长期摘要：{'已生成' if summary else '暂无'}",
                f"当前记忆估算：{self._estimate_ai_tokens(context_text)} Token",
                f"配置上限：{int(ai_config.get('contextTokensK') or 64)}K Token",
            ]
        )

    def _send_ai_markdown_message(self, *, token: str, chat_id: str, title: str, body: Any) -> int:
        reply = self._ai_markdown_reply(title, body)
        text = str(reply.get("text") or "")
        fallback_text = str(reply.get("fallback_text") or text)
        try:
            result = self.sender.send_text(token=token, chat_id=chat_id, text=text, parse_mode="MarkdownV2")
        except Exception as err:
            LOGGER.warning("Telegram MarkdownV2 placeholder failed, fallback to plain text: %s", err)
            result = self.sender.send_text(token=token, chat_id=chat_id, text=fallback_text)
        return self._extract_telegram_message_id(result)

    @staticmethod
    def _extract_telegram_message_id(result: Any) -> int:
        payload = result if isinstance(result, dict) else {}
        message = payload.get("result") if isinstance(payload.get("result"), dict) else {}
        try:
            return int(message.get("message_id") or 0)
        except Exception:
            return 0

    def _stream_ai_answer_to_telegram(
        self,
        *,
        token: str,
        chat_id: str,
        message_id: int,
        ai_config: dict[str, Any],
        messages: list[dict[str, str]],
    ) -> str:
        if not message_id:
            raise RuntimeError("Telegram 占位消息发送失败，无法流式编辑")
        chunks: list[str] = []
        last_edit_at = 0.0
        min_interval = 1.8
        for chunk in stream_chat_completion(config=ai_config, messages=messages, timeout_seconds=60):
            chunks.append(chunk)
            answer = self._truncate_text("".join(chunks), 3400)
            now = time.time()
            if now - last_edit_at < min_interval:
                continue
            self._edit_ai_markdown_message(
                token=token,
                chat_id=chat_id,
                message_id=message_id,
                title="🧠 AI 媒体问答",
                body=answer or "正在生成...",
            )
            last_edit_at = now
        final_answer = self._truncate_text("".join(chunks).strip(), 3400)
        if not final_answer:
            raise RuntimeError("AI 流式返回内容为空")
        self._edit_ai_markdown_message(
            token=token,
            chat_id=chat_id,
            message_id=message_id,
            title="🧠 AI 媒体问答",
            body=final_answer,
        )
        return final_answer

    def _ai_markdown_reply(self, title: str, body: Any, *, reply_markup: dict[str, Any] | None = None) -> dict[str, Any]:
        plain = f"{title}\n\n{body}"
        return {
            "text": _format_copy_block(title, body),
            "parse_mode": "MarkdownV2",
            "fallback_text": plain,
            "reply_markup": reply_markup if isinstance(reply_markup, dict) else None,
        }

    def _missing_episode_report_reply(self, report: dict[str, Any], *, chat_id: str = "") -> dict[str, Any]:
        title = str(report.get("title") or "未知作品").strip()
        year = str(report.get("year") or "年份未知").strip()
        missing_text = str(report.get("missingText") or "无").strip()
        future_text = str(report.get("futureText") or "").strip()
        unknown_text = str(report.get("unknownText") or "").strip()
        search_count = max(0, int(report.get("searchCount") or 0))
        data_query_count = max(0, int(report.get("dataQueryCount") or 0))
        table = "\n".join(
            [
                "项目       | 详情",
                "-----------+----------------",
                f"TMDB ID    | {report.get('tmdbId') or '-'}",
                f"登记       | {int(report.get('registeredCount') or 0)} 集",
                f"已播出     | {int(report.get('airedCount') or 0)} 集",
                f"Emby 已有  | {int(report.get('localCount') or 0)} 集",
                f"缺失       | {missing_text}",
                f"最后播出   | {report.get('lastAiredDate') or '-'}",
            ]
        )
        intro = _escape_markdown_v2(f"（执行 {search_count} 次搜索，查询 {data_query_count} 次数据）")
        heading = _escape_markdown_v2(f"《{title}》（{year}）缺集情况：")
        outside = [f"缺失的剧集：{missing_text if report.get('missingLabels') else '无'}"]
        if future_text:
            outside.append(f"未来未播：{future_text}（不计入缺失）")
        if unknown_text:
            outside.append(f"播出日期未知：{unknown_text}（暂不计入缺失）")
        if report.get("missingLabels"):
            outside.extend(["", "需要生成这些缺失集的搜索清单吗？"])
        markdown = f"{intro}\n\n*{heading}*\n\n```text\n{_escape_markdown_v2_code(table)}\n```"
        if outside:
            markdown += "\n" + "\n".join(_escape_markdown_v2(line) for line in outside)
        fallback = f"（执行 {search_count} 次搜索，查询 {data_query_count} 次数据）\n\n《{title}》（{year}）缺集情况：\n\n{table}\n\n" + "\n".join(outside)
        reply_markup = None
        labels = [str(value or "").strip() for value in report.get("missingLabels", []) if str(value or "").strip()]
        if labels:
            action_id = secrets.token_urlsafe(8)
            self._pending_missing_searches[action_id] = {
                "title": title,
                "labels": labels,
                "chatId": str(chat_id or ""),
                "createdAt": time.time(),
            }
            self._cleanup_pending_missing_searches()
            reply_markup = {"inline_keyboard": [[{"text": "生成搜索清单", "callback_data": f"missing_search:{action_id}"}]]}
        return {
            "text": markdown,
            "parse_mode": "MarkdownV2",
            "fallback_text": fallback,
            "reply_markup": reply_markup,
            "memory_text": str(report.get("memoryText") or fallback),
        }

    def _build_ai_media_context(self, *, question: str = "") -> str:
        parts: list[str] = []
        base_url, api_key = self._emby_context()
        if base_url and api_key:
            parts.append("Emby 连接状态：已配置。")
        else:
            parts.append("Emby 连接状态：未配置或不可用。")
        parts.append(self._build_ai_tool_registry_context())

        stats_context = self._build_ai_library_stats_context()
        if stats_context:
            parts.append(stats_context)

        focus_context = self._build_ai_focus_media_context(question)
        if focus_context:
            parts.append(focus_context)

        listing_context = self._build_ai_category_listing_context(question)
        if listing_context:
            parts.append(listing_context)

        project_contexts = self._build_ai_project_tool_contexts(question)
        parts.extend(project_contexts)

        try:
            latest, _tried, _err = self._fetch_latest_items_with_fallback(limit=8)
            rows = latest if isinstance(latest, list) else []
            if rows:
                latest_lines = [self._format_recent_library_row(row) for row in rows[:8] if isinstance(row, dict)]
                parts.append("最近入库：\n" + "\n".join(latest_lines))
            else:
                parts.append("最近入库：暂无可用数据。")
        except Exception as err:
            parts.append(f"最近入库：读取失败（{err}）。")

        try:
            service = PlaybackHistoryService(fetcher=self._emby_get, event_logger=None)
            result = service.collect(limit=8, scan_limit=600)
            rows = result.get("rows") if isinstance(result, dict) else []
            if rows:
                playback_lines = [self._format_recent_playback_row(row) for row in rows[:8] if isinstance(row, dict)]
                parts.append("最近播放：\n" + "\n".join(playback_lines))
            else:
                parts.append("最近播放：暂无可用数据。")
        except Exception as err:
            parts.append(f"最近播放：读取失败（{err}）。")

        return "\n\n".join(parts)

    def _build_ai_project_tool_contexts(self, question: str) -> list[str]:
        text = str(question or "").lower()
        contexts: list[str] = []
        if re.search(r"正在播放|当前播放|谁在看|在线|播放中", text):
            contexts.append(self._build_ai_now_playing_context())
        if re.search(r"播放历史|最近.*看|看了什么|播放记录|播放最多|观看历史", text):
            contexts.append(self._build_ai_playback_history_context())
        if re.search(r"任务|扫描|计划任务|后台任务|task|scheduled", text):
            contexts.append(self._build_ai_tasks_context())
        if re.search(r"缺集|缺少|漏集|巡检|missing", text):
            contexts.append(self._build_ai_missing_context())
        if re.search(r"日志|报错|错误|失败|异常|log|error", text):
            contexts.append(self._build_ai_logs_context())
        if re.search(r"邀请|邀请码|注册|invite", text):
            contexts.append(self._build_ai_invites_context())
        if re.search(r"用户|账号|会员|user", text):
            contexts.append(self._build_ai_users_context())
        if re.search(r"排行|排名|年度|最多|榜|ranking", text):
            contexts.append(self._build_ai_ranking_context())
        if re.search(r"质量|分辨率|码率|编码|4k|1080|720|hdr|quality", text):
            contexts.append(self._build_ai_quality_context())
        if re.search(r"风险|异常|失败|告警|问题|健康|risk", text):
            contexts.append(self._build_ai_risk_context())
        if re.search(r"客户端|设备|终端|在线|client|device|session", text):
            contexts.append(self._build_ai_clients_context())
        if re.search(r"配置|设置|token|api key|apikey|密码|密钥|secret|config", text):
            contexts.append(self._build_ai_settings_context())
        return [item for item in contexts if item]

    def _build_ai_tool_registry_context(self) -> str:
        readable = [tool for tool in AI_TOOL_REGISTRY if tool.get("kind") == "read"]
        confirm = [tool for tool in AI_TOOL_REGISTRY if tool.get("kind") == "confirm"]
        return "\n".join(
            [
                "AI 工具注册表：",
                "- 只读工具：" + "、".join(str(tool.get("name")) for tool in readable),
                "- 需确认工具：" + "、".join(str(tool.get("name")) for tool in confirm),
                "- 规则：查询类直接回答；执行类必须让用户点击确认；敏感字段只说明已配置/未配置。",
            ]
        )

    def _build_ai_now_playing_context(self) -> str:
        try:
            sessions = self._emby_get("/Sessions")
        except Exception as err:
            return f"当前播放：读取失败（{self._format_emby_error(err)}）。"
        rows = sessions if isinstance(sessions, list) else []
        active: list[str] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            item = row.get("NowPlayingItem") if isinstance(row.get("NowPlayingItem"), dict) else {}
            name = str(item.get("Name") or "").strip()
            if not name:
                continue
            user_name = str(row.get("UserName") or row.get("UserId") or "未知用户").strip()
            client = str(row.get("Client") or row.get("DeviceName") or "未知客户端").strip()
            active.append(f"- {user_name} 正在看 {self._format_now_playing_title(item)}（{client}）")
        return "当前播放：\n" + ("\n".join(active[:8]) if active else "暂无活跃播放。")

    def _build_ai_playback_history_context(self) -> str:
        try:
            service = PlaybackHistoryService(fetcher=self._emby_get, event_logger=None)
            result = service.collect(limit=20, scan_limit=800)
            rows = result.get("rows") if isinstance(result, dict) else []
        except Exception as err:
            return f"播放历史：读取失败（{err}）。"
        if not isinstance(rows, list) or not rows:
            return "播放历史：暂无可用记录。"
        media_counts: dict[str, int] = {}
        user_counts: dict[str, int] = {}
        lines = []
        for row in rows[:10]:
            if not isinstance(row, dict):
                continue
            media = self._format_recent_playback_filename_with_status(row)[0]
            user = str(row.get("username") or row.get("user") or "未知用户").strip()
            media_counts[media] = int(media_counts.get(media) or 0) + 1
            user_counts[user] = int(user_counts.get(user) or 0) + 1
            lines.append(self._format_recent_playback_row(row))
        top_media = sorted(media_counts.items(), key=lambda item: item[1], reverse=True)[:5]
        top_users = sorted(user_counts.items(), key=lambda item: item[1], reverse=True)[:5]
        summary = [
            "播放历史摘要：",
            f"- 最近记录数：{len(rows)}",
            "- 最近记录：",
            *[f"  {line}" for line in lines[:8]],
        ]
        if top_media:
            summary.append("- 最近高频影片：" + "、".join(f"{name} {count}次" for name, count in top_media))
        if top_users:
            summary.append("- 最近活跃用户：" + "、".join(f"{name} {count}次" for name, count in top_users))
        return "\n".join(summary)

    def _build_ai_tasks_context(self) -> str:
        try:
            tasks = self._fetch_scheduled_tasks()
        except Exception as err:
            return f"任务中心：读取失败（{self._format_emby_error(err)}）。"
        if not tasks:
            return "任务中心：没有读取到计划任务。"
        running = [task for task in tasks if str(task.get("State") or "").lower() == "running"]
        lines = [
            "任务中心：",
            f"- 任务总数：{len(tasks)}",
            f"- 正在运行：{len(running)} 个",
        ]
        for task in tasks[:12]:
            if not isinstance(task, dict):
                continue
            name = str(task.get("Name") or task.get("Key") or task.get("Id") or "未命名任务").strip()
            state = str(task.get("State") or "Idle").strip()
            last = task.get("LastExecutionResult") if isinstance(task.get("LastExecutionResult"), dict) else {}
            last_status = str(last.get("Status") or "").strip() or "无记录"
            last_end = str(last.get("EndTimeUtc") or last.get("EndTime") or "").strip()
            lines.append(f"- {name}：{state}，上次结果 {last_status}{f'，结束 {last_end}' if last_end else ''}")
        return "\n".join(lines)

    def _build_ai_missing_context(self) -> str:
        cache = self._read_missing_scan_cache()
        if not cache:
            return "缺集巡检：暂无缓存，请先在后台或通过确认执行触发缺集巡检。"
        summary = cache.get("summary") if isinstance(cache.get("summary"), dict) else {}
        rows = cache.get("rows") if isinstance(cache.get("rows"), list) else []
        lines = [
            "缺集巡检摘要：",
            f"- 扫描剧集：{summary.get('scannedSeries', 0)}",
            f"- 缺集剧集：{summary.get('missingSeries', 0)}",
            f"- 缺失集数：{summary.get('missingEpisodeCount', 0)}",
            f"- 未匹配：{summary.get('unknownMatchCount', 0)}",
        ]
        for row in rows[:8]:
            if not isinstance(row, dict):
                continue
            name = str(row.get("seriesName") or "未知剧集").strip()
            season = row.get("seasonNo")
            missing = row.get("missingEpisodes") if isinstance(row.get("missingEpisodes"), list) else []
            status = str(row.get("status") or "").strip()
            lines.append(f"- {name} S{season}：{status}，缺 {missing[:12]}")
        return "\n".join(lines)

    def _build_ai_logs_context(self) -> str:
        try:
            events, total = read_project_events(self.event_log_path, limit=30)
        except Exception as err:
            return f"系统日志摘要：读取失败（{err}）。"
        if not events:
            return "系统日志摘要：暂无日志。"
        levels: dict[str, int] = {}
        modules: dict[str, int] = {}
        lines = ["系统日志摘要：", f"- 日志总数：{total}"]
        for event in events:
            level = str(event.get("level") or "info")
            module = str(event.get("module") or "system")
            levels[level] = int(levels.get(level) or 0) + 1
            modules[module] = int(modules.get(module) or 0) + 1
        lines.append("- 级别分布：" + "、".join(f"{key} {value}" for key, value in sorted(levels.items())))
        lines.append("- 模块分布：" + "、".join(f"{key} {value}" for key, value in sorted(modules.items())))
        lines.append("- 最近日志：")
        for event in events[:8]:
            message = str(event.get("message") or event.get("action") or "").strip()
            lines.append(f"  {event.get('time', '')} [{event.get('level', '')}/{event.get('module', '')}] {message[:120]}")
        return "\n".join(lines)

    def _build_ai_invites_context(self) -> str:
        store = _read_store(self.store_path)
        invites = store.get("invites") if isinstance(store.get("invites"), list) else []
        active = used = expired = 0
        rows = []
        now = datetime.now()
        for invite in invites:
            if not isinstance(invite, dict):
                continue
            status = str(invite.get("status") or "").strip()
            expires_at = str(invite.get("expiresAt") or "").strip()
            is_used = bool(str(invite.get("usedAt") or invite.get("createdUserId") or invite.get("usedUsername") or "").strip()) or status in {"已用", "used"}
            is_expired = False
            if expires_at:
                try:
                    is_expired = datetime.fromisoformat(expires_at).date() < now.date()
                except Exception:
                    is_expired = False
            if is_used:
                used += 1
                status_text = "已用"
            elif is_expired:
                expired += 1
                status_text = "过期"
            else:
                active += 1
                status_text = "可用"
            rows.append(f"- {invite.get('code', '')}：{status_text}，标签 {invite.get('label') or '-'}，到期 {expires_at or '未设置'}")
        return "\n".join(["邀请码摘要：", f"- 总数：{len(invites)}", f"- 可用：{active}", f"- 已用：{used}", f"- 过期：{expired}", *rows[:10]])

    def _build_ai_users_context(self) -> str:
        try:
            payload = self._emby_get("/Users")
        except Exception as err:
            return f"用户管理摘要：读取失败（{self._format_emby_error(err)}）。"
        rows = payload.get("Items") if isinstance(payload, dict) else payload
        users = [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []
        lines = ["用户管理摘要：", f"- 用户总数：{len(users)}"]
        for user in users[:15]:
            name = str(user.get("Name") or user.get("Username") or user.get("Id") or "未知用户").strip()
            policy = user.get("Policy") if isinstance(user.get("Policy"), dict) else {}
            disabled = bool(policy.get("IsDisabled"))
            admin = bool(policy.get("IsAdministrator"))
            lines.append(f"- {name}：{'管理员' if admin else '普通用户'}，{'禁用' if disabled else '启用'}")
        return "\n".join(lines)

    def _build_ai_ranking_context(self) -> str:
        try:
            service = PlaybackHistoryService(fetcher=self._emby_get, event_logger=None)
            result = service.collect(limit=80, scan_limit=2000)
            rows = result.get("rows") if isinstance(result, dict) else []
        except Exception as err:
            return f"播放排行摘要：读取失败（{err}）。"
        media_counts: dict[str, int] = {}
        user_counts: dict[str, int] = {}
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict):
                continue
            media = self._format_recent_playback_filename_with_status(row)[0]
            user = str(row.get("username") or row.get("user") or "未知用户").strip()
            media_counts[media] = int(media_counts.get(media) or 0) + 1
            user_counts[user] = int(user_counts.get(user) or 0) + 1
        media_top = sorted(media_counts.items(), key=lambda item: item[1], reverse=True)[:10]
        user_top = sorted(user_counts.items(), key=lambda item: item[1], reverse=True)[:10]
        lines = ["播放排行摘要：", f"- 样本记录：{len(rows) if isinstance(rows, list) else 0}"]
        lines.append("- 影片排行：" + ("、".join(f"{name} {count}次" for name, count in media_top) if media_top else "暂无"))
        lines.append("- 用户排行：" + ("、".join(f"{name} {count}次" for name, count in user_top) if user_top else "暂无"))
        return "\n".join(lines)

    def _build_ai_quality_context(self) -> str:
        try:
            query = urllib.parse.urlencode(
                {
                    "Recursive": "true",
                    "IncludeItemTypes": "Movie,Episode",
                    "Fields": "Name,Type,SeriesName,MediaSources,MediaStreams,Width,Height",
                    "Limit": "800",
                }
            )
            payload = self._emby_get(f"/Items?{query}")
        except Exception as err:
            return f"质量盘点摘要：读取失败（{self._format_emby_error(err)}）。"
        rows = payload.get("Items") if isinstance(payload, dict) else payload
        items = [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []
        buckets = {"4K": 0, "1080p": 0, "720p": 0, "其他": 0}
        hdr_count = 0
        for item in items:
            quality = self._format_media_quality(item)
            low = quality.lower()
            if "4k" in low:
                buckets["4K"] += 1
            elif "1080" in low:
                buckets["1080p"] += 1
            elif "720" in low:
                buckets["720p"] += 1
            else:
                buckets["其他"] += 1
            if "hdr" in low or "dovi" in low:
                hdr_count += 1
        return "\n".join(
            [
                "质量盘点摘要：",
                f"- 样本资源：{len(items)}",
                f"- 4K：{buckets['4K']}",
                f"- 1080p：{buckets['1080p']}",
                f"- 720p：{buckets['720p']}",
                f"- 其他/未知：{buckets['其他']}",
                f"- HDR/DoVi：{hdr_count}",
            ]
        )

    def _build_ai_risk_context(self) -> str:
        contexts = [self._build_ai_logs_context(), self._build_ai_tasks_context()]
        try:
            events, _total = read_project_events(self.event_log_path, limit=100)
            error_count = sum(1 for event in events if str(event.get("level") or "") == "error")
            warning_count = sum(1 for event in events if str(event.get("level") or "") == "warning")
            contexts.insert(0, f"风险概览：最近日志中 error {error_count} 条，warning {warning_count} 条。")
        except Exception:
            pass
        return "\n\n".join(contexts)

    def _build_ai_clients_context(self) -> str:
        try:
            sessions = self._emby_get("/Sessions")
        except Exception as err:
            return f"客户端摘要：读取失败（{self._format_emby_error(err)}）。"
        rows = sessions if isinstance(sessions, list) else []
        lines = ["客户端摘要：", f"- 在线会话：{len(rows)}"]
        for row in rows[:12]:
            if not isinstance(row, dict):
                continue
            user = str(row.get("UserName") or row.get("UserId") or "未知用户").strip()
            client = str(row.get("Client") or "未知客户端").strip()
            device = str(row.get("DeviceName") or row.get("DeviceId") or "未知设备").strip()
            item = row.get("NowPlayingItem") if isinstance(row.get("NowPlayingItem"), dict) else {}
            now = self._format_now_playing_title(item) if item else "未播放"
            lines.append(f"- {user}：{client} / {device} / {now}")
        return "\n".join(lines)

    def _build_ai_settings_context(self) -> str:
        store = _read_store(self.store_path)
        emby = _apply_emby_env_overrides(store.get("embyConfig"))
        bot = normalize_bot_config(store.get("botConfig"))
        ai = apply_ai_env_overrides(store.get("aiConfig"))
        return "\n".join(
            [
                "系统设置摘要（已脱敏）：",
                f"- Emby 地址：{'已配置' if str(emby.get('serverUrl') or '').strip() else '未配置'}",
                f"- Emby API Key：{'已配置' if str(emby.get('apiKey') or '').strip() else '未配置'}",
                f"- TMDB Token：{'已配置' if str(emby.get('tmdbToken') or os.environ.get('APP_TMDB_TOKEN') or os.environ.get('TMDB_TOKEN') or '').strip() else '未配置'}",
                f"- Telegram Token：{'已配置' if str(bot.get('telegramToken') or '').strip() else '未配置'}",
                f"- Telegram Chat ID：{'已配置' if str(bot.get('telegramChatId') or '').strip() else '未配置'}",
                f"- AI 助手：{'启用' if ai.get('enabled') else '关闭'}",
                f"- AI Base URL：{'已配置' if str(ai.get('baseUrl') or '').strip() else '未配置'}",
                f"- AI API Key：{'已配置' if str(ai.get('apiKey') or '').strip() else '未配置'}",
                f"- AI 模型：{str(ai.get('model') or '未配置')}",
            ]
        )

    def _build_ai_execution_proposal(self, question: str) -> CommandReply | None:
        text = str(question or "").strip()
        query_intent = bool(re.search(r"列出|列出来|全部列|查询|查找|看看|看一下|有哪些|哪些|显示|统计|清单|列表|片单|多少|什么|是否|有没有", text, flags=re.IGNORECASE))
        if query_intent:
            return None
        if self._is_library_scan_request(text):
            keyword = self._extract_library_scan_keyword(text)
            return self._cmd_scan_library(keyword)
        has_execute_intent = bool(re.search(r"执行|触发|启动|运行|生成|创建|新增|同步|开始.*任务|运行.*任务|执行.*任务|触发.*任务|run|start|generate|create|sync", text, flags=re.IGNORECASE))
        scan_execute_intent = bool(re.search(r"(开始|执行|运行|触发).{0,12}(媒体库)?扫描(任务)?", text, flags=re.IGNORECASE))
        refresh_execute_intent = bool(re.search(r"(刷新|执行|运行|触发).{0,12}(缺集|巡检|webhook|机器人.*状态)", text, flags=re.IGNORECASE))
        if not (has_execute_intent or scan_execute_intent or refresh_execute_intent):
            return None

        action: dict[str, Any] | None = None
        if re.search(r"邀请码|邀请|invite", text, flags=re.IGNORECASE) and re.search(r"生成|创建|新增|发一个|来一个|generate|create", text, flags=re.IGNORECASE):
            quantity_match = re.search(r"(\d{1,2})\s*(个|条|枚)?", text)
            quantity = int(quantity_match.group(1)) if quantity_match else 1
            quantity = max(1, min(quantity, 10))
            action = {
                "type": "invite_generate",
                "label": f"生成 {quantity} 个邀请码",
                "summary": "将生成新的可用邀请码并保存到后端。",
                "quantity": quantity,
            }
        elif re.search(r"邀请码|邀请|invite", text, flags=re.IGNORECASE) and re.search(r"同步|刷新|sync", text, flags=re.IGNORECASE):
            action = {
                "type": "invite_sync",
                "label": "同步邀请码状态摘要",
                "summary": "将重新读取邀请码存储并返回可用/已用/过期统计，不修改媒体服务器配置。",
            }
        elif re.search(r"缺集|漏集|巡检|missing", text, flags=re.IGNORECASE):
            action = {
                "type": "missing_scan",
                "label": "触发缺集巡检",
                "summary": "将扫描 Emby 剧集并结合 TMDB 判断缺失集数。耗时可能较长。",
            }
        elif re.search(r"webhook|机器人.*状态|状态刷新", text, flags=re.IGNORECASE):
            action = {
                "type": "webhook_status",
                "label": "刷新机器人 Webhook 状态",
                "summary": "将读取当前 Bot Webhook/轮询状态并返回摘要，不修改配置。",
            }
        elif re.search(r"任务|扫描|计划任务|媒体库|scheduled|task", text, flags=re.IGNORECASE):
            task = self._match_scheduled_task_for_question(text)
            if task:
                task_id = str(task.get("Id") or task.get("Key") or "").strip()
                task_name = str(task.get("Name") or task.get("Key") or task_id).strip()
                action = {
                    "type": "scheduled_task",
                    "label": f"运行计划任务：{task_name}",
                    "summary": f"将触发 Emby 计划任务“{task_name}”。",
                    "taskId": task_id,
                    "taskName": task_name,
                }
            else:
                return self._ai_markdown_reply(
                    "🧠 AI 执行建议",
                    "没有匹配到可执行的 Emby 计划任务。\n你可以说：帮我运行媒体库扫描任务。",
                )

        if not action:
            return None

        action_id = uuid.uuid4().hex[:12]
        action["createdAt"] = time.time()
        self._pending_ai_actions[action_id] = action
        self._cleanup_pending_ai_actions()
        body_lines = [
            f"操作：{action['label']}",
            f"说明：{action['summary']}",
            "",
            "点击“确认执行”才会真正运行；取消则不做任何改变。",
        ]
        return self._ai_markdown_reply(
            "🧠 AI 需要你确认后再执行",
            "\n".join(body_lines),
            reply_markup={
                "inline_keyboard": [
                    [
                        {"text": "确认执行", "callback_data": f"ai_exec:ok:{action_id}"},
                        {"text": "取消", "callback_data": f"ai_exec:cancel:{action_id}"},
                    ]
                ]
            },
        )

    def _cleanup_pending_ai_actions(self) -> None:
        now = time.time()
        expired = [key for key, value in self._pending_ai_actions.items() if now - float(value.get("createdAt") or 0) > 600]
        for key in expired:
            self._pending_ai_actions.pop(key, None)

    def _execute_ai_confirmed_action(self, action: dict[str, Any]) -> str:
        action_type = str(action.get("type") or "").strip()
        if action_type == "scheduled_task":
            task_id = str(action.get("taskId") or "").strip()
            task_name = str(action.get("taskName") or task_id).strip()
            if not task_id:
                raise RuntimeError("缺少任务 ID")
            self._emby_post(f"/ScheduledTasks/Running/{urllib.parse.quote(task_id, safe='')}")
            return f"🧠 AI 已提交执行\n任务：{task_name}\n结果：已向 Emby 提交运行请求。"
        if action_type == "webhook_status":
            return "🧠 AI 已刷新状态\n" + self._build_ai_bot_status_context()
        if action_type == "invite_generate":
            rows = self._generate_ai_invites(quantity=int(action.get("quantity") or 1))
            codes = [str(row.get("code") or "") for row in rows]
            return "🧠 AI 已生成邀请码\n" + "\n".join(f"- {code}" for code in codes)
        if action_type == "invite_sync":
            return "🧠 AI 已同步邀请码摘要\n" + self._build_ai_invites_context()
        if action_type == "missing_scan":
            result = self._run_ai_missing_scan()
            summary = result.get("summary") if isinstance(result, dict) else {}
            return "\n".join(
                [
                    "🧠 AI 已完成缺集巡检",
                    f"扫描剧集：{summary.get('scannedSeries', 0)}",
                    f"缺集剧集：{summary.get('missingSeries', 0)}",
                    f"缺失集数：{summary.get('missingEpisodeCount', 0)}",
                    f"未匹配：{summary.get('unknownMatchCount', 0)}",
                ]
            )
        raise RuntimeError("不支持的 AI 执行动作")

    def _fetch_emby_libraries(self) -> list[dict[str, str]]:
        candidates: list[dict[str, str]] = []
        tried_errors: list[str] = []
        for path in ("/Library/VirtualFolders", "/Library/MediaFolders", "/UserViews"):
            try:
                payload = self._emby_get(path)
            except Exception as err:
                tried_errors.append(f"{path}: {self._format_emby_error(err)}")
                continue
            rows = payload.get("Items") if isinstance(payload, dict) else payload
            if not isinstance(rows, list):
                continue
            for row in rows:
                library = self._normalize_emby_library(row)
                if library and library["id"] not in {item["id"] for item in candidates}:
                    candidates.append(library)
            if candidates:
                return candidates
        if tried_errors:
            raise RuntimeError("；".join(tried_errors[:3]))
        return candidates

    @staticmethod
    def _normalize_emby_library(row: Any) -> dict[str, str]:
        if not isinstance(row, dict):
            return {}
        library_id = str(row.get("ItemId") or row.get("Id") or row.get("CollectionId") or "").strip()
        name = str(row.get("Name") or row.get("ItemName") or row.get("CollectionType") or "").strip()
        options = row.get("LibraryOptions") if isinstance(row.get("LibraryOptions"), dict) else {}
        lib_type = str(row.get("CollectionType") or row.get("Type") or options.get("ContentType") or "").strip()
        if not library_id or not name:
            return {}
        return {"id": library_id, "name": name, "type": lib_type or "媒体库"}

    @staticmethod
    def _filter_emby_libraries(libraries: list[dict[str, str]], keyword: str) -> list[dict[str, str]]:
        clean = str(keyword or "").strip().lower()
        if not clean:
            return libraries
        tokens = [token.lower() for token in re.findall(r"[\w\u4e00-\u9fff]+", clean) if token.strip()]
        if not tokens:
            return libraries
        matched: list[dict[str, str]] = []
        for library in libraries:
            haystack = f"{library.get('name', '')} {library.get('type', '')}".lower()
            if any(token in haystack for token in tokens) or clean in haystack:
                matched.append(library)
        return matched

    def _find_emby_library_by_id(self, library_id: str) -> dict[str, str]:
        safe_id = str(library_id or "").strip()
        if not safe_id:
            return {}
        for library in self._fetch_emby_libraries():
            if str(library.get("id") or "") == safe_id:
                return library
        return {}

    def _build_scan_library_keyboard(self, libraries: list[dict[str, str]]) -> dict[str, Any]:
        rows: list[list[dict[str, str]]] = []
        for library in libraries[:20]:
            library_id = str(library.get("id") or "").strip()
            name = str(library.get("name") or "未知媒体库").strip()
            if not library_id:
                continue
            encoded_id = urllib.parse.quote(library_id, safe="")
            rows.append([{"text": f"扫描：{name[:26]}", "callback_data": f"scan_library:one:{encoded_id}"}])
        rows.append([{"text": "扫描全库", "callback_data": "scan_library:all"}])
        return {"inline_keyboard": rows}

    def _execute_single_library_scan(self, library: dict[str, str]) -> str:
        library_id = str(library.get("id") or "").strip()
        name = str(library.get("name") or library_id).strip()
        if not library_id:
            raise RuntimeError("缺少媒体库 ID")
        query = urllib.parse.urlencode(
            {
                "Recursive": "true",
                "MetadataRefreshMode": "Default",
                "ImageRefreshMode": "Default",
            }
        )
        self._emby_post(f"/Items/{urllib.parse.quote(library_id, safe='')}/Refresh?{query}")
        return f"已提交单库扫描。\n目标：{name}\n结果：Emby 已收到刷新请求。"

    def _execute_full_library_scan(self) -> str:
        task = self._match_scheduled_task_for_question("运行 scan media library 媒体库扫描任务")
        if task:
            task_id = str(task.get("Id") or task.get("Key") or "").strip()
            task_name = str(task.get("Name") or task.get("Key") or task_id).strip()
            if task_id:
                self._emby_post(f"/ScheduledTasks/Running/{urllib.parse.quote(task_id, safe='')}")
                return f"已提交全库扫描。\n任务：{task_name}\n结果：Emby 已收到计划任务运行请求。"
        self._emby_post("/Library/Refresh")
        return "已提交全库扫描。\n方式：Library Refresh\n结果：Emby 已收到全库刷新请求。"

    @staticmethod
    def _is_library_scan_request(text: str) -> bool:
        value = str(text or "")
        if not re.search(r"扫描|刷新|scan|refresh", value, flags=re.IGNORECASE):
            return False
        if re.search(r"缺集|漏集|webhook|机器人.*状态", value, flags=re.IGNORECASE):
            return False
        return bool(re.search(r"媒体库|库|国产动漫|动漫|华语|剧集|电影|纪录片|library", value, flags=re.IGNORECASE))

    @staticmethod
    def _extract_library_scan_keyword(text: str) -> str:
        value = str(text or "").strip()
        replacements = [
            r"^/ai\s*",
            r"帮我",
            r"请",
            r"扫描一下",
            r"扫描",
            r"刷新一下",
            r"刷新",
            r"媒体库",
            r"这个库",
            r"这个",
            r"一下",
            r"吧",
            r"please",
            r"scan",
            r"refresh",
            r"library",
        ]
        for pattern in replacements:
            value = re.sub(pattern, "", value, flags=re.IGNORECASE)
        value = value.strip(" ，。！？?：:；;、|/\\[]()（）【】《》「」“”\"'")
        value = re.sub(r"\s+", " ", value).strip()
        return value[:80]

    def _build_ai_safety_reply(self, question: str) -> str:
        text = str(question or "").strip()
        if re.search(r"删除|清空|重置|改.*(token|api key|apikey|密钥|密码)|修改.*(token|api key|apikey|密钥|密码)|显示.*(token|api key|apikey|密钥|密码)|告诉我.*(token|api key|apikey|密钥|密码)", text, flags=re.IGNORECASE):
            return "这类删除、清空、重置、修改或查看密钥/密码的操作不能由 AI 执行。\n请到后台页面手动操作；我只能告诉你这些敏感项是否已配置。"
        return ""

    def _fetch_scheduled_tasks(self) -> list[dict[str, Any]]:
        payload = self._emby_get("/ScheduledTasks")
        rows = payload.get("Items") if isinstance(payload, dict) else payload
        return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []

    def _match_scheduled_task_for_question(self, question: str) -> dict[str, Any] | None:
        try:
            tasks = self._fetch_scheduled_tasks()
        except Exception:
            return None
        if not tasks:
            return None
        text = str(question or "").lower()
        aliases = {
            "scan media library": ("媒体库", "扫描", "scan", "library"),
            "refresh metadata": ("元数据", "metadata"),
            "clean cache directory": ("缓存", "cache"),
            "clean transcode directory": ("转码", "transcode"),
        }
        scored: list[tuple[int, dict[str, Any]]] = []
        for task in tasks:
            name = str(task.get("Name") or task.get("Key") or "").strip()
            haystack = name.lower()
            score = 0
            if haystack and haystack in text:
                score += 20
            for alias, words in aliases.items():
                if alias in haystack and any(word in text for word in words):
                    score += 15
            for token in re.findall(r"[\w\u4e00-\u9fff]+", text):
                if len(token) >= 2 and token.lower() in haystack:
                    score += 1
            if score > 0:
                scored.append((score, task))
        if not scored and re.search(r"媒体库|扫描|scan", text):
            for task in tasks:
                name = str(task.get("Name") or "").lower()
                if "scan" in name and "library" in name:
                    return task
        scored.sort(key=lambda item: item[0], reverse=True)
        return scored[0][1] if scored else None

    def _build_ai_bot_status_context(self) -> str:
        store = _read_store(self.store_path)
        bot = normalize_bot_config(store.get("botConfig"))
        return "\n".join(
            [
                "机器人状态：",
                f"- 指令轮询：{'开启' if bot.get('enableCommands', True) else '关闭'}",
                f"- 播放通知：{'开启' if bot.get('enablePlayback', True) else '关闭'}",
                f"- 入库通知：{'开启' if bot.get('enableLibrary', True) else '关闭'}",
                f"- Telegram Token：{'已配置' if str(bot.get('telegramToken') or '').strip() else '未配置'}",
                f"- Chat ID：{'已配置' if str(bot.get('telegramChatId') or '').strip() else '未配置'}",
            ]
        )

    def _generate_ai_invites(self, *, quantity: int = 1) -> list[dict[str, Any]]:
        safe_quantity = max(1, min(10, int(quantity or 1)))
        store = _read_store(self.store_path)
        invites = store.get("invites") if isinstance(store.get("invites"), list) else []
        existing = {str(row.get("code") or "").strip().lower() for row in invites if isinstance(row, dict)}
        generated: list[dict[str, Any]] = []
        for _ in range(safe_quantity):
            code = self._generate_ai_invite_code(existing)
            existing.add(code.lower())
            row = {
                "id": secrets.token_hex(8),
                "code": code,
                "label": "AI生成",
                "username": "",
                "plan": "",
                "initialDays": None,
                "duration": None,
                "expiresAt": "",
                "status": "空闲",
                "createdAt": datetime.now().isoformat(timespec="seconds"),
                "usedAt": "",
                "createdUserId": "",
                "usedUsername": "",
            }
            invites.append(row)
            generated.append(row)
        store["invites"] = invites
        self.store_path.parent.mkdir(parents=True, exist_ok=True)
        self.store_path.write_text(json.dumps(store, ensure_ascii=False, indent=2), encoding="utf-8")
        self._log_project_event(
            level="info",
            module="invite",
            action="ai_invite_created",
            message=f"AI 已生成 {len(generated)} 个邀请码。",
            detail={"quantity": len(generated), "codes": [row.get("code") for row in generated]},
        )
        return generated

    @staticmethod
    def _generate_ai_invite_code(existing: set[str]) -> str:
        alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
        while True:
            code = "".join(secrets.choice(alphabet) for _ in range(8))
            if code.lower() not in existing:
                return code

    def _read_missing_scan_cache(self) -> dict[str, Any]:
        path = self.store_path.parent / "missing_scan.json"
        if not path.exists():
            return {}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        return payload if isinstance(payload, dict) else {}

    def _run_ai_missing_scan(self) -> dict[str, Any]:
        store = _read_store(self.store_path)
        emby = _apply_emby_env_overrides(store.get("embyConfig"))
        tmdb_token = str(
            emby.get("tmdbToken")
            or os.environ.get("APP_TMDB_TOKEN")
            or os.environ.get("TMDB_TOKEN")
            or ""
        ).strip()
        if not tmdb_token:
            raise RuntimeError("缺少 TMDB Token，请先在系统设置里填写并保存。")
        identity_service = self._media_identity_service()
        service = MissingEpisodeService(
            emby_fetcher=self._emby_get,
            tmdb_token=tmdb_token,
            tmdb_language=str(emby.get("tmdbLanguage") or "zh-CN"),
            tmdb_region=str(emby.get("tmdbRegion") or "CN"),
            identity_resolver=lambda item: str(identity_service.identity_from_emby_item(item).get("tmdbId") or ""),
        )
        result = service.scan(scan_limit=1200)
        output = {
            "ok": True,
            "summary": result.get("summary") if isinstance(result, dict) else {},
            "rows": result.get("rows") if isinstance(result, dict) else [],
            "warnings": result.get("warnings") if isinstance(result, dict) else [],
            "debug": result.get("debug") if isinstance(result, dict) else {},
        }
        try:
            path = self.store_path.parent / "missing_scan.json"
            path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass
        return output

    def _build_ai_library_stats_context(self) -> str:
        try:
            counts = self._emby_get("/Items/Counts")
        except Exception as err:
            return f"当前媒体库统计：读取失败（{self._format_emby_error(err)}）。"
        if not isinstance(counts, dict):
            return "当前媒体库统计：暂无可用数据。"

        def _count(*keys: str) -> int:
            for key in keys:
                value = counts.get(key)
                if isinstance(value, (int, float)) and int(value) >= 0:
                    return int(value)
            return 0

        movie_count = _count("MovieCount", "movies", "Movies")
        series_count = _count("SeriesCount", "series", "Series")
        episode_count = _count("EpisodeCount", "episodes", "Episodes")
        music_video_count = _count("MusicVideoCount", "musicVideos", "MusicVideos")
        trailer_count = _count("TrailerCount", "trailers", "Trailers")
        total = movie_count + series_count + episode_count + music_video_count + trailer_count
        if total <= 0:
            total = _count("ItemCount", "TotalItemCount", "total", "Total")

        rows = [
            "当前媒体库统计：",
            f"- 电影：{movie_count} 部",
            f"- 剧集：{series_count} 部",
            f"- 单集：{episode_count} 集",
        ]
        if music_video_count:
            rows.append(f"- 音乐视频：{music_video_count} 个")
        if trailer_count:
            rows.append(f"- 预告片：{trailer_count} 个")
        if total:
            rows.append(f"- 可统计资源合计：{total} 个")
        return "\n".join(rows)

    def _build_ai_media_detail_reply(self, question: str, *, conversation_key: str = "") -> str:
        text = str(question or "").strip()
        if not re.search(r"简介|剧情|详情|演员|主演|评分", text):
            return ""
        active = self._get_ai_active_media(conversation_key)
        if self._is_ai_reference_question(text) and active.get("title"):
            keywords = [str(active.get("title") or "").strip()]
            preferred_type = str(active.get("type") or "")
        else:
            keywords = self._extract_ai_media_keywords(text)
            preferred_type = ""
        if not keywords:
            return "当前没有记住正在讨论的作品，请带上片名再查询。"
        keyword = keywords[0]
        try:
            service = self._media_identity_service()
            resolution = service.resolve(keyword, preferred_type=preferred_type)
        except Exception as err:
            return f"查询《{keyword}》详情失败：{self._format_emby_error(err)}"
        if resolution.get("ambiguous") and not resolution.get("embyItem"):
            return self._format_ai_identity_candidates(keyword, resolution.get("candidates"))
        identity = resolution.get("identity") if isinstance(resolution.get("identity"), dict) else {}
        emby_item = resolution.get("embyItem") if isinstance(resolution.get("embyItem"), dict) else {}
        if not identity and not emby_item:
            return f"当前 TMDB 和 Emby 都没有找到《{keyword}》。"
        tmdb_detail: dict[str, Any] = {}
        try:
            tmdb_detail = service.get_media_detail(str(identity.get("tmdbId") or ""), str(identity.get("type") or ""))
        except Exception as err:
            LOGGER.warning("TG AI TMDB detail fallback: title=%s err=%s", keyword, err)
        title = str(identity.get("title") or emby_item.get("Name") or keyword).strip()
        year = str(identity.get("year") or emby_item.get("ProductionYear") or "").strip()
        overview = str(tmdb_detail.get("overview") or emby_item.get("Overview") or identity.get("overview") or "").strip()
        rating = tmdb_detail.get("vote_average")
        if rating in (None, ""):
            rating = emby_item.get("CommunityRating") or identity.get("rating")
        credits = tmdb_detail.get("credits") if isinstance(tmdb_detail.get("credits"), dict) else {}
        cast = credits.get("cast") if isinstance(credits.get("cast"), list) else []
        cast_names = [str(row.get("name") or "").strip() for row in cast[:8] if isinstance(row, dict) and str(row.get("name") or "").strip()]
        type_label = "剧集" if str(identity.get("type") or "") == "series" else "电影"
        lines = [f"《{title}》详情：", f"- 年份：{year or '未知'}", f"- 类型：{type_label}"]
        if rating not in (None, ""):
            try:
                lines.append(f"- 评分：{float(rating):.1f}")
            except Exception:
                lines.append(f"- 评分：{rating}")
        if cast_names:
            lines.append(f"- 主演：{'、'.join(cast_names)}")
        lines.append(f"- 简介：{overview[:900] if overview else '暂无简介'}")
        lines.append(f"- 本地状态：{'已入库' if emby_item else '未入库'}")
        self._ai_conversations.set_active_media(
            conversation_key,
            {
                "title": title,
                "year": year,
                "type": identity.get("type"),
                "embySeriesId": emby_item.get("Id") if str(identity.get("type") or "") == "series" else "",
                "tmdbId": identity.get("tmdbId"),
            },
        )
        return "\n".join(lines)

    def _build_ai_episode_query_reply(self, question: str, *, conversation_key: str = "") -> str:
        text = str(question or "").strip()
        if not self._is_ai_episode_count_question(text):
            return ""
        keywords = self._extract_ai_media_keywords(text)
        active_media = self._get_ai_active_media(conversation_key)
        if self._is_ai_reference_question(text) and active_media.get("title"):
            keywords = [str(active_media.get("title") or "").strip()]
        if not keywords:
            return "没有识别到要查询的剧集名称，请带上片名再试。"

        keyword = keywords[0]
        try:
            resolution = self._media_identity_service().resolve(keyword, preferred_type="series")
        except Exception as err:
            return f"查询《{keyword}》失败：{self._format_emby_error(err)}"
        if resolution.get("ambiguous") and not resolution.get("embyItem"):
            return self._format_ai_identity_candidates(keyword, resolution.get("candidates"))
        series = resolution.get("embyItem") if isinstance(resolution.get("embyItem"), dict) else {}
        identity = resolution.get("identity") if isinstance(resolution.get("identity"), dict) else {}
        if not series:
            self._log_ai_media_query_diagnostic(
                question=text,
                keyword=keyword,
                candidates=keywords,
                detail={"result": "not_found"},
            )
            return f"当前 Emby 可读范围内没有找到《{keyword}》。"
        if str(series.get("Type") or "").strip().lower() == "episode":
            series = self._resolve_ai_series_search_item(series, items=[series], keyword=keyword)
        if str(series.get("Type") or "").strip().lower() != "series":
            title = str(series.get("Name") or keyword).strip()
            return f"媒体库中找到了《{title}》，但它不是可统计单集的剧集资源。"

        item_id = str(series.get("Id") or "").strip()
        detail = dict(series)
        if item_id:
            try:
                payload = self._emby_get(
                    f"/Items/{urllib.parse.quote(item_id, safe='')}?Fields=Name,Type,ProductionYear,PremiereDate,ChildCount,RecursiveItemCount,Status,ProviderIds"
                )
                if isinstance(payload, dict):
                    detail.update(payload)
            except Exception as err:
                LOGGER.warning("TG AI series detail fallback: item_id=%s err=%s", item_id, err)

        title = str(detail.get("Name") or keyword).strip()
        identity = identity or self._resolve_ai_media_identity(keyword=keyword, detail=detail)
        season_count, actual_count, _season_lines, latest_text, source_note = self._resolve_ai_series_counts(
            item_id=item_id,
            detail=detail,
            keyword=keyword,
            title=title,
            identity=identity,
        )
        latest_label = self._format_ai_latest_episode_label(latest_text)
        lines = [f"《{title}》媒体库查询结果："]
        if latest_label:
            lines.append(f"- 本地最新已入库：{latest_label}")
        else:
            lines.append("- 本地最新已入库：未读取到有效季集编号")
        lines.append(f"- 实际可读取单集：{actual_count} 集")
        if season_count > 1:
            lines.append(f"- 可读取季数：{season_count} 季")
        if source_note:
            lines.append(f"- 核对说明：{source_note}")
        self._log_ai_media_query_diagnostic(
            question=text,
            keyword=keyword,
            candidates=keywords,
            detail={
                "result": "matched",
                "title": title,
                "embySeriesId": item_id,
                "actualEpisodeCount": actual_count,
                "latestEpisode": latest_text,
            },
        )
        self._ai_conversations.set_active_media(
            conversation_key,
            {
                "title": title,
                "year": identity.get("year"),
                "type": "series",
                "embySeriesId": item_id,
                "tmdbId": identity.get("tmdbId"),
                "latestEpisode": latest_text,
                "actualEpisodeCount": actual_count,
                "seasonCount": season_count,
            },
        )
        return "\n".join(lines)

    def _build_ai_missing_episode_reply(
        self,
        question: str,
        *,
        conversation_key: str = "",
        rich: bool = False,
        chat_id: str = "",
    ) -> CommandReply:
        text = str(question or "").strip()
        request = self._parse_ai_missing_episode_request(text)
        if not request:
            return ""

        started = time.time()
        active = self._get_ai_active_media(conversation_key)
        use_active = request.get("mode") == "context"
        keyword = str(active.get("title") if use_active else request.get("title") or "").strip()
        if not keyword:
            return "当前没有记住正在讨论的剧集，请带上片名。"

        service = self._media_identity_service()
        search_count = 0
        identity: dict[str, Any] = {}
        if use_active and active.get("tmdbId"):
            identity = {
                "title": keyword,
                "year": active.get("year"),
                "type": "series",
                "tmdbId": str(active.get("tmdbId") or ""),
                "embyId": str(active.get("embySeriesId") or ""),
            }
        else:
            try:
                search_count = 1
                candidates = service.search_media(keyword, media_type="tv")
            except Exception as err:
                return f"TMDB 搜索《{keyword}》失败：{self._format_emby_error(err)}"
            if not candidates:
                return f"TMDB 没有找到《{keyword}》，无法计算预期缺集。"
            if len(candidates) > 1 and int(candidates[0].get("score") or 0) == int(candidates[1].get("score") or 0) and candidates[0].get("year") != candidates[1].get("year"):
                return self._format_ai_identity_candidates(keyword, candidates)
            identity = dict(candidates[0])

        try:
            with ThreadPoolExecutor(max_workers=2, thread_name_prefix="MediaInventory") as executor:
                detail_future = executor.submit(service.query_media_detail, str(identity.get("tmdbId") or ""), "tv")
                library_future = executor.submit(service.query_library_exists, identity)
                expected = detail_future.result()
                existing = library_future.result()
        except Exception as err:
            return f"查询《{keyword}》集数失败：{self._format_emby_error(err)}"

        if not existing.get("exists"):
            return f"TMDB 已识别《{identity.get('title') or keyword}》，但 Emby 媒体库中未找到该剧集。"
        emby_item = existing.get("embyItem") if isinstance(existing.get("embyItem"), dict) else {}
        series_id = str(emby_item.get("Id") or identity.get("embyId") or "").strip()
        title = str(identity.get("title") or emby_item.get("Name") or keyword).strip()

        if expected.get("ok"):
            comparison = service.compare_episode_inventory(expected, existing)
            missing = comparison.get("missing") if isinstance(comparison.get("missing"), list) else []
            missing_text = self._format_inventory_episode_labels(comparison.get("missingLabels"), fallback=missing)
            future = comparison.get("future") if isinstance(comparison.get("future"), list) else []
            future_text = self._format_inventory_episode_labels(comparison.get("futureLabels"), fallback=future)
            unknown_air_date = comparison.get("unknownAirDate") if isinstance(comparison.get("unknownAirDate"), list) else []
            unknown_text = self._format_inventory_episode_labels(comparison.get("unknownAirDateLabels"), fallback=unknown_air_date)
            lines = [
                f"《{title}》缺集查询结果：",
                f"- TMDB：已播出 {comparison.get('expectedCount', 0)} 集，登记 {comparison.get('registeredCount', 0)} 集",
                f"- Emby：已有 {comparison.get('localEpisodeCount', comparison.get('existingCount', 0))} 集",
                f"- 缺失：{missing_text if missing else '无'}",
                f"- 编号模式：{'全局连续编号' if comparison.get('mode') == 'global' else '标准分季编号'}",
            ]
            if future:
                lines.append(f"- 未来未播：{future_text}（不计入缺失）")
            if unknown_air_date:
                lines.append(f"- 播出日期未知：{unknown_text}（暂不计入缺失）")
            extras = comparison.get("extras") if isinstance(comparison.get("extras"), list) else []
            if extras:
                lines.append(
                    f"- 超出 TMDB 范围：{self._format_inventory_episode_labels(comparison.get('extraLabels'), fallback=extras)}"
                )
            unmapped = comparison.get("unmapped") if isinstance(comparison.get("unmapped"), list) else []
            if unmapped:
                lines.append(f"- 无法映射：{len(unmapped)} 集（Emby 与 TMDB 季集编号不一致）")
            if comparison.get("specials"):
                lines.append(f"- 特别篇：{len(comparison.get('specials') or [])} 集（不参与缺集计算）")
            expected_count = int(comparison.get("expectedCount") or 0)
            existing_count = int(comparison.get("existingCount") or 0)
            missing_count = len(missing)
            numbering_mode = str(comparison.get("mode") or "")
        else:
            episodes = self._fetch_ai_series_episodes(item_id=series_id)
            fallback = self._analyze_ai_local_episode_gaps(episodes)
            local_count = sum(len(values) for values in fallback["seasonMap"].values())
            lines = [
                f"《{title}》缺集查询结果：",
                "- TMDB：详情不可用，无法计算预期差集",
                f"- Emby：已有 {local_count} 集",
                f"- 本地内部断档：{self._format_missing_episode_map(fallback['missing']) if fallback['missing'] else '无'}",
            ]
            expected_count = 0
            existing_count = local_count
            missing_count = sum(len(values) for values in fallback["missing"].values())
            numbering_mode = str(fallback["mode"])

        self._log_ai_missing_query_diagnostic(
            question=text,
            keyword=keyword,
            used_context=bool(use_active),
            series_id=series_id,
            result="matched",
            numbering_mode=numbering_mode,
            tmdb_id=str(identity.get("tmdbId") or ""),
            expected_count=expected_count,
            existing_count=existing_count,
            missing_count=missing_count,
            elapsed_ms=int((time.time() - started) * 1000),
        )

        data_query_count = int(expected.get("tmdbQueryCount") or 0) + int(existing.get("embyQueryCount") or 0)
        self._log_project_event(
            level="info",
            module="webhook",
            action="telegram_missing_report_ready",
            message="Telegram 缺集富文本报告已生成。",
            detail={
                "title": title,
                "tmdbId": str(identity.get("tmdbId") or ""),
                "searchCount": search_count,
                "tmdbQueryCount": int(expected.get("tmdbQueryCount") or 0),
                "embyQueryCount": int(existing.get("embyQueryCount") or 0),
                "dataQueryCount": data_query_count,
                "missingCount": missing_count,
            },
        )

        self._ai_conversations.set_active_media(
            conversation_key,
            {
                "title": title,
                "year": identity.get("year"),
                "type": "series",
                "embySeriesId": series_id,
                "tmdbId": identity.get("tmdbId"),
                "actualEpisodeCount": existing_count,
                "expectedEpisodeCount": expected_count,
                "missingEpisodeCount": missing_count,
                "seasonCount": len(existing.get("seasonMap") or {}),
            },
        )
        memory_text = "\n".join(lines)
        if rich and expected.get("ok"):
            return self._missing_episode_report_reply(
                {
                    "title": title,
                    "year": identity.get("year"),
                    "tmdbId": identity.get("tmdbId"),
                    "registeredCount": comparison.get("registeredCount", 0),
                    "airedCount": comparison.get("expectedCount", 0),
                    "localCount": comparison.get("localEpisodeCount", comparison.get("existingCount", 0)),
                    "missingText": missing_text if missing else "无",
                    "missingLabels": comparison.get("missingLabels") if missing else [],
                    "futureText": future_text if future else "",
                    "unknownText": unknown_text if unknown_air_date else "",
                    "lastAiredDate": expected.get("lastAiredDate"),
                    "searchCount": search_count,
                    "dataQueryCount": data_query_count,
                    "memoryText": memory_text,
                },
                chat_id=chat_id,
            )
        return memory_text

    @staticmethod
    def _parse_ai_missing_episode_request(question: str) -> dict[str, str]:
        text = re.sub(r"^/ai(?:@\w+)?\s*", "", str(question or "").strip(), flags=re.IGNORECASE)
        if not re.search(r"缺失.*集|缺少.*集|缺哪.*集|漏.*集|缺集", text):
            return {}
        value = re.sub(
            r"^(?:帮我|请|麻烦你|你帮我)?\s*(?:查看一下|看一下|查一下|看下|查下|看看|继续查)?\s*",
            "",
            text,
            flags=re.IGNORECASE,
        )
        if re.match(r"^(?:它|这个|这部|那个|那部)?\s*(?:的)?(?:缺失|缺少|缺哪|漏掉|漏)(?:的|哪几|哪些)?集", value):
            return {"mode": "context", "title": ""}
        match = re.match(
            r"^(?P<title>.+?)(?:的)?(?:缺失|缺少|缺哪|漏掉|漏)(?:的|哪几|哪些)?集",
            value,
        )
        if not match:
            return {}
        title = str(match.group("title") or "").strip().strip("，。！？?：:；;、《》「」“”\"'")
        title = re.sub(r"的$", "", title).strip()
        return {"mode": "explicit", "title": title} if title else {"mode": "context", "title": ""}

    @classmethod
    def _analyze_ai_local_episode_gaps(cls, episodes: list[dict[str, Any]]) -> dict[str, Any]:
        season_map: dict[int, set[int]] = {}
        for row in episodes:
            if not isinstance(row, dict):
                continue
            season = cls._coerce_index_number(row.get("ParentIndexNumber"))
            episode = cls._coerce_index_number(row.get("IndexNumber"))
            if season is None or episode is None or season <= 0 or episode <= 0:
                continue
            season_map.setdefault(season, set()).add(episode)
        blocks = sorted(
            ((season, min(values), max(values)) for season, values in season_map.items() if values),
            key=lambda row: (row[1], row[2], row[0]),
        )
        global_mode = False
        if len(blocks) >= 2 and blocks[0][1] == 1:
            global_mode = all(current[1] > previous[2] for previous, current in zip(blocks, blocks[1:]))
        elif len(blocks) == 1 and blocks[0][1] > 1:
            global_mode = True

        missing: dict[int, list[int]] = {}
        if global_mode:
            present = set().union(*season_map.values()) if season_map else set()
            if present:
                gaps = sorted(set(range(min(present), max(present) + 1)).difference(present))
                if gaps:
                    missing[0] = gaps
        else:
            for season, present in season_map.items():
                if not present:
                    continue
                gaps = sorted(set(range(min(present), max(present) + 1)).difference(present))
                if gaps:
                    missing[season] = gaps
        return {"mode": "global" if global_mode else "seasonal", "seasonMap": season_map, "missing": missing}

    def _log_ai_missing_query_diagnostic(
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
        self._log_project_event(
            level="info",
            module="webhook",
            action="ai_missing_query_diagnostic",
            message="AI 单剧缺集查询已完成。",
            detail={
                "question": self._truncate_text(str(question or ""), 160),
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

    @classmethod
    def _format_missing_episode_map(cls, missing_map: dict[int, list[int]]) -> str:
        parts: list[str] = []
        for season in sorted(missing_map):
            label = "全局" if season == 0 else f"S{season:02d}"
            parts.append(f"{label} " + cls._compress_number_ranges(missing_map[season]))
        return "；".join(parts) if parts else "无"

    @staticmethod
    def _compress_number_ranges(values: list[int]) -> str:
        numbers = sorted({int(value) for value in values if int(value) > 0})
        if not numbers:
            return "无"
        ranges: list[str] = []
        start = previous = numbers[0]
        for number in numbers[1:]:
            if number == previous + 1:
                previous = number
                continue
            ranges.append(f"E{start:02d}" if start == previous else f"E{start:02d}-E{previous:02d}")
            start = previous = number
        ranges.append(f"E{start:02d}" if start == previous else f"E{start:02d}-E{previous:02d}")
        return "、".join(ranges)

    @classmethod
    def _format_inventory_episode_labels(cls, labels: Any, *, fallback: list[int] | None = None) -> str:
        rows = [str(value or "").strip() for value in labels] if isinstance(labels, list) else []
        rows = [value for value in rows if value]
        if not rows:
            return cls._compress_number_ranges(fallback or [])
        global_values: list[int] = []
        seasonal: dict[int, list[int]] = {}
        for value in rows:
            global_match = re.fullmatch(r"E(\d+)", value, flags=re.IGNORECASE)
            season_match = re.fullmatch(r"S(\d+)E(\d+)", value, flags=re.IGNORECASE)
            if global_match:
                global_values.append(int(global_match.group(1)))
            elif season_match:
                seasonal.setdefault(int(season_match.group(1)), []).append(int(season_match.group(2)))
        if global_values and not seasonal:
            return cls._compress_number_ranges(global_values)
        if seasonal and not global_values:
            parts: list[str] = []
            for season in sorted(seasonal):
                ranges = cls._compress_number_ranges(seasonal[season])
                parts.append(f"S{season:02d}{ranges}")
            return "、".join(parts)
        return "、".join(rows)

    @staticmethod
    def _is_ai_episode_count_question(question: str) -> bool:
        text = str(question or "").strip()
        return bool(re.search(r"多少集|几集|第几集|哪一集|哪集|更新到|最新.*集|缺.*集", text))

    def _get_ai_active_media(self, conversation_key: str) -> dict[str, Any]:
        session = self._ai_conversations.get(conversation_key)
        media = session.get("activeMedia") if isinstance(session.get("activeMedia"), dict) else {}
        return media

    @staticmethod
    def _is_ai_reference_question(question: str) -> bool:
        text = str(question or "").strip()
        return bool(
            re.search(r"^(?:帮我|请|看一下|查一下|查看一下|继续查)?(?:它|这个|这部|那个|那部)", text)
            or re.search(r"^(?:查看一下|看一下|查一下|继续查)?(?:缺失|缺少|漏掉|漏)的?集", text)
        )

    def _resolve_ai_series_search_item(
        self,
        matched: dict[str, Any],
        *,
        items: list[dict[str, Any]],
        keyword: str,
    ) -> dict[str, Any]:
        if str(matched.get("Type") or "").strip().lower() == "series":
            return matched
        if str(matched.get("Type") or "").strip().lower() == "episode":
            series_id = str(matched.get("SeriesId") or "").strip()
            if series_id:
                try:
                    payload = self._emby_get(
                        f"/Items/{urllib.parse.quote(series_id, safe='')}?Fields=Name,Type,ProductionYear,ProviderIds,Status"
                    )
                    if isinstance(payload, dict) and str(payload.get("Type") or "").strip().lower() == "series":
                        return payload
                except Exception as err:
                    LOGGER.warning("TG AI episode parent series lookup failed: series_id=%s err=%s", series_id, err)
        normalized_keyword = self._normalize_ai_title(keyword)
        series_rows = [row for row in items if str(row.get("Type") or "").strip().lower() == "series"]
        exact = [row for row in series_rows if self._normalize_ai_title(str(row.get("Name") or "")) == normalized_keyword]
        return exact[0] if exact else (series_rows[0] if series_rows else {})

    @staticmethod
    def _format_ai_identity_candidates(keyword: str, candidates: Any) -> str:
        rows = [row for row in candidates if isinstance(row, dict)] if isinstance(candidates, list) else []
        lines = [f"《{keyword}》有多个同名候选，请带上年份重新查询："]
        for row in rows[:5]:
            type_label = "剧集" if str(row.get("type") or "") == "series" else "电影"
            lines.append(f"- {row.get('title') or keyword}（{row.get('year') or '年份未知'}，{type_label}，TMDB {row.get('tmdbId') or '-'}）")
        return "\n".join(lines)

    @staticmethod
    def _format_ai_latest_episode_label(latest_text: str) -> str:
        text = str(latest_text or "").strip()
        match = re.search(r"S(?P<season>\d{1,2})E(?P<episode>\d{1,4})", text, flags=re.IGNORECASE)
        if not match:
            return text
        season = int(match.group("season") or 0)
        episode = int(match.group("episode") or 0)
        return f"S{season:02d}E{episode:02d}（第{episode}集）"

    def _log_ai_media_query_diagnostic(
        self,
        *,
        question: str,
        keyword: str,
        candidates: list[str],
        detail: dict[str, Any],
    ) -> None:
        payload = {
            "question": self._truncate_text(str(question or ""), 160),
            "keyword": keyword,
            "candidates": candidates[:5],
        }
        payload.update(detail)
        self._log_project_event(
            level="info",
            module="webhook",
            action="ai_media_query_diagnostic",
            message="AI 媒体库片名识别与集数查询已完成。",
            detail=payload,
        )

    def _build_ai_focus_media_context(self, question: str) -> str:
        keywords = self._extract_ai_media_keywords(question)
        if not keywords:
            return ""
        keyword, items, search_error = self._search_emby_media_candidates(keywords)
        if search_error:
            return f"命中资源详情：搜索“{keyword}”失败（{search_error}）。"
        if not items:
            return f"命中资源详情：媒体库里没有搜索到“{keyword}”。"

        first = self._pick_best_search_item(items=items, keyword=keyword)
        if not first:
            return f"命中资源详情：媒体库里没有搜索到“{keyword}”。"
        matched_lines = self._format_ai_matched_item_context(first, keyword=keyword)
        if len(items) > 1:
            other_names = []
            first_id = str(first.get("Id") or "").strip()
            for row in items:
                if not isinstance(row, dict):
                    continue
                if str(row.get("Id") or "").strip() == first_id:
                    continue
                name = str(row.get("Name") or "").strip()
                if name and name not in other_names:
                    other_names.append(name)
            if other_names:
                matched_lines.append(f"其他可能匹配：{'、'.join(other_names[:5])}")
        return "\n".join(matched_lines)

    def _search_emby_media_candidates(self, keywords: list[str]) -> tuple[str, list[dict[str, Any]], str]:
        last_keyword = str(keywords[0] if keywords else "").strip()
        last_error = ""
        for candidate in keywords:
            keyword = str(candidate or "").strip()
            if not keyword:
                continue
            last_keyword = keyword
            try:
                items = self._search_emby_items(keyword=keyword, limit=12)
            except Exception as err:
                last_error = self._format_emby_error(err)
                continue
            if items:
                return keyword, items, ""
        return last_keyword, [], last_error

    def _search_emby_items(self, *, keyword: str, limit: int = 8) -> list[dict[str, Any]]:
        query = urllib.parse.urlencode(
            {
                "Recursive": "true",
                "SearchTerm": keyword,
                "IncludeItemTypes": "Series,Movie,Episode",
                "Fields": "Name,Type,ProductionYear,Overview,Genres,PremiereDate,ChildCount,RecursiveItemCount,ParentIndexNumber,IndexNumber,SeriesName,SeriesId,ProviderIds,Status,CommunityRating,CriticRating,RunTimeTicks",
                "Limit": str(max(1, min(50, int(limit or 8)))),
            }
        )
        result = self._emby_get(f"/Items?{query}")
        items = result.get("Items") if isinstance(result, dict) else []
        return [row for row in items if isinstance(row, dict)] if isinstance(items, list) else []

    def _format_ai_matched_item_context(self, item: dict[str, Any], *, keyword: str) -> list[str]:
        item_id = str(item.get("Id") or "").strip()
        detail: dict[str, Any] = {}
        if item_id:
            try:
                payload = self._emby_get(
                    f"/Items/{urllib.parse.quote(item_id, safe='')}?Fields=Name,Type,ProductionYear,PremiereDate,ChildCount,RecursiveItemCount,ParentIndexNumber,IndexNumber,SeriesName,SeriesId,ProviderIds,Status,CommunityRating,CriticRating,Overview"
                )
                detail = payload if isinstance(payload, dict) else {}
            except Exception as err:
                LOGGER.warning("TG AI item detail fallback: item_id=%s err=%s", item_id, err)
        joined = dict(item)
        joined.update(detail)
        item_type = str(joined.get("Type") or "").strip().lower()
        title = str(joined.get("Name") or keyword or "未知内容").strip()
        year = self._resolve_year(joined)
        type_label = {"series": "电视剧/剧集", "movie": "电影", "episode": "单集"}.get(item_type, item_type or "未知类型")
        lines = [
            "命中资源详情：",
            f"- 搜索关键词：{keyword}",
            f"- 最佳匹配：{title}（{year}，{type_label}）",
        ]

        if item_type == "series" and item_id:
            identity = self._resolve_ai_media_identity(keyword=keyword, detail=joined)
            season_count, episode_count, season_lines, latest_episode, source_note = self._resolve_ai_series_counts(
                item_id=item_id,
                detail=joined,
                keyword=keyword,
                title=title,
                identity=identity,
            )
            identity_lines = self._format_ai_identity_lines(identity)
            lines.extend(identity_lines)
            lines.append(f"- 剧集统计：共 {season_count} 季 / {episode_count} 集")
            if source_note:
                lines.append(f"- 统计来源：{source_note}")
            if latest_episode:
                lines.append(f"- 最新单集：{latest_episode}")
            if season_lines:
                lines.append(f"- 分季集数：{'；'.join(season_lines[:8])}")
            if episode_count <= 0:
                recent_hint = self._build_ai_recent_library_hint(title)
                if recent_hint:
                    lines.append(f"- 最近入库提示：{recent_hint}")
            status = str(joined.get("Status") or "").strip()
            if status:
                lines.append(f"- 剧集状态：{status}")
        elif item_type == "episode":
            season = joined.get("ParentIndexNumber")
            episode = joined.get("IndexNumber")
            series_name = str(joined.get("SeriesName") or "").strip()
            if series_name:
                lines.append(f"- 所属剧集：{series_name}")
            if isinstance(season, int) or isinstance(episode, int):
                lines.append(f"- 单集位置：第 {season if isinstance(season, int) else '?'} 季 第 {episode if isinstance(episode, int) else '?'} 集")
        elif item_type == "movie":
            lines.append("- 资源状态：电影已入库")

        rating = self._format_rating(joined)
        if rating != "N/A":
            lines.append(f"- 用户评分：{rating}")
        return lines

    def _build_ai_category_listing_context(self, question: str) -> str:
        spec = self._parse_ai_category_listing_request(question)
        if not spec:
            return ""
        label = str(spec.get("label") or "媒体资源").strip()
        try:
            items, total = self._fetch_ai_category_items(spec=spec, limit=30)
        except Exception as err:
            return f"分类资源查询：读取“{label}”失败（{self._format_emby_error(err)}）。"
        if not items:
            return f"分类资源查询：当前 Emby 可读范围内未匹配到“{label}”资源。"
        lines = [
            f"分类资源查询：{label}",
            f"- 匹配数量：{total}",
            f"- 已显示前 {len(items)} 条",
            "- 资源列表：",
        ]
        for idx, item in enumerate(items, start=1):
            lines.append(f"  {idx}. {self._format_ai_category_item_line(item)}")
        if total > len(items):
            lines.append("- 提示：结果较多，建议继续缩小关键词或分类。")
        return "\n".join(lines)

    def _parse_ai_category_listing_request(self, question: str) -> dict[str, Any] | None:
        text = str(question or "").strip()
        if not text:
            return None
        if not re.search(r"列出|列出来|全部|有哪些|查询|查找|看看|看一下|显示|统计|资源|片单|清单|扫描一下", text):
            return None

        lowered = text.lower()
        specs: list[dict[str, Any]] = [
            {
                "label": "国产动漫",
                "needles": ["国产动漫", "国漫", "中国动漫", "华语动漫"],
                "includeTypes": "Series,Movie",
                "match": ["国产动漫", "国漫", "中国动漫", "华语动漫", "动漫", "动画", "animation", "anime"],
                "prefer": ["国产", "中国", "华语", "大陆", "cn"],
            },
            {
                "label": "动漫剧集",
                "needles": ["动漫剧集", "动画剧集"],
                "includeTypes": "Series",
                "match": ["动漫", "动画", "animation", "anime"],
            },
            {
                "label": "动漫",
                "needles": ["动漫", "动画", "anime", "animation"],
                "includeTypes": "Series,Movie",
                "match": ["动漫", "动画", "animation", "anime"],
            },
            {
                "label": "纪录片",
                "needles": ["纪录片", "documentary"],
                "includeTypes": "Series,Movie",
                "match": ["纪录片", "documentary"],
            },
            {
                "label": "华语电影",
                "needles": ["华语电影", "国产电影", "中文电影"],
                "includeTypes": "Movie",
                "match": ["华语", "国产", "中国", "大陆", "中文"],
            },
            {
                "label": "电影",
                "needles": ["电影", "影片"],
                "includeTypes": "Movie",
                "match": [],
            },
            {
                "label": "剧集",
                "needles": ["剧集", "电视剧", "连续剧"],
                "includeTypes": "Series",
                "match": [],
            },
        ]
        for spec in specs:
            if any(needle.lower() in lowered for needle in spec["needles"]):
                return spec
        return None

    def _fetch_ai_category_items(self, *, spec: dict[str, Any], limit: int = 30) -> tuple[list[dict[str, Any]], int]:
        include_types = str(spec.get("includeTypes") or "Series,Movie")
        query = urllib.parse.urlencode(
            {
                "Recursive": "true",
                "IncludeItemTypes": include_types,
                "Fields": "Name,Type,ProductionYear,PremiereDate,Genres,Tags,Path,Overview,ChildCount,RecursiveItemCount,ProviderIds,Studios",
                "SortBy": "SortName",
                "SortOrder": "Ascending",
                "Limit": "500",
            }
        )
        payload = self._emby_get(f"/Items?{query}")
        rows = payload.get("Items") if isinstance(payload, dict) else payload
        candidates = [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []
        matched = [row for row in candidates if self._matches_ai_category_spec(row, spec)]
        if not matched and not spec.get("match"):
            matched = candidates
        return matched[: max(1, min(50, int(limit or 30)))], len(matched)

    def _matches_ai_category_spec(self, item: dict[str, Any], spec: dict[str, Any]) -> bool:
        match_words = [str(word).lower() for word in spec.get("match", []) if str(word).strip()]
        prefer_words = [str(word).lower() for word in spec.get("prefer", []) if str(word).strip()]
        if not match_words and not prefer_words:
            return True
        haystack_parts: list[str] = [
            str(item.get("Name") or ""),
            str(item.get("SeriesName") or ""),
            str(item.get("Type") or ""),
            str(item.get("Path") or ""),
            str(item.get("Overview") or ""),
        ]
        for key in ("Genres", "Tags", "Studios"):
            values = item.get(key)
            if isinstance(values, list):
                haystack_parts.extend(str(value) for value in values)
        provider_ids = item.get("ProviderIds") if isinstance(item.get("ProviderIds"), dict) else {}
        haystack_parts.extend(str(value) for value in provider_ids.values())
        haystack = " ".join(haystack_parts).lower()
        has_match = not match_words or any(word in haystack for word in match_words)
        has_prefer = not prefer_words or any(word in haystack for word in prefer_words)
        return has_match and has_prefer

    def _format_ai_category_item_line(self, item: dict[str, Any]) -> str:
        title = str(item.get("Name") or item.get("SeriesName") or "未知标题").strip()
        year = self._resolve_year(item)
        item_type = str(item.get("Type") or "").strip().lower()
        type_label = {"series": "剧集", "movie": "电影", "episode": "单集"}.get(item_type, item_type or "资源")
        if item_type == "series":
            count = int(item.get("RecursiveItemCount") or item.get("ChildCount") or 0)
            pack = f"约 {count} 集" if count > 0 else "集数待查"
        elif item_type == "movie":
            pack = "电影完整版"
        else:
            pack = "资源"
        genres = item.get("Genres") if isinstance(item.get("Genres"), list) else []
        genre_text = " / ".join(str(genre).strip() for genre in genres[:3] if str(genre).strip())
        suffix = f"｜{genre_text}" if genre_text else ""
        return f"《{title}》({year})｜{type_label}｜{pack}{suffix}"

    def _build_ai_recent_library_hint(self, title: str) -> str:
        safe_title = str(title or "").strip()
        if not safe_title:
            return ""
        try:
            latest, _tried, _err = self._fetch_latest_items_with_fallback(limit=20)
        except Exception:
            return ""
        rows = latest if isinstance(latest, list) else []
        for row in rows:
            if not isinstance(row, dict):
                continue
            item_type = str(row.get("Type") or "").strip().lower()
            row_title = str(row.get("SeriesName") if item_type == "episode" else row.get("Name") or "").strip()
            if not row_title or safe_title not in row_title and row_title not in safe_title:
                continue
            return self._format_recent_library_row(row)
        return ""

    def _resolve_ai_media_identity(self, *, keyword: str, detail: dict[str, Any]) -> dict[str, Any]:
        try:
            identity = self._media_identity_service().identity_from_emby_item(detail)
        except Exception as err:
            LOGGER.warning("TG AI media identity fallback: title=%s err=%s", keyword, err)
            identity = {}
        if identity:
            return identity
        return {
            "title": str(detail.get("Name") or keyword or "").strip(),
            "year": self._resolve_year(detail),
            "type": str(detail.get("Type") or "series").strip().lower(),
            "tmdbId": "",
            "embyId": str(detail.get("Id") or "").strip(),
            "source": "emby_fallback",
            "confidence": "Emby 本地匹配",
        }

    def _format_ai_identity_lines(self, identity: dict[str, Any]) -> list[str]:
        if not identity:
            return []
        tmdb_id = str(identity.get("tmdbId") or "").strip() or "未识别"
        emby_id = str(identity.get("embyId") or "").strip() or "未识别"
        title = str(identity.get("title") or "未知作品").strip()
        year = str(identity.get("year") or "未知").strip()
        confidence = str(identity.get("confidence") or "本地匹配").strip()
        return [
            f"- 作品身份：{title}（{year}）/ TMDB {tmdb_id} / Emby {emby_id}",
            f"- 身份来源：{confidence}",
        ]

    def _search_tmdb_identity(self, *, title: str, item_type: str, year: str = "") -> dict[str, Any]:
        token, language, region = self._tmdb_context()
        if not token:
            return {"error": "TMDB Token 未配置"}
        media_type = "tv" if item_type in {"series", "episode"} else "movie"
        params: dict[str, str] = {"query": title, "language": language}
        if year and year.isdigit():
            if media_type == "tv":
                params["first_air_date_year"] = year
            else:
                params["year"] = year
        if region:
            params["region"] = region
        payload = self._tmdb_get_json(f"/search/{media_type}?{urllib.parse.urlencode(params)}", token=token)
        rows = payload.get("results") if isinstance(payload, dict) else []
        if not isinstance(rows, list) or not rows:
            return {}
        target = self._normalize_ai_title(title)
        scored: list[tuple[int, dict[str, Any]]] = []
        for row in rows[:8]:
            if not isinstance(row, dict):
                continue
            name = str(row.get("name") or row.get("title") or "").strip()
            norm = self._normalize_ai_title(name)
            score = 0
            if target and norm == target:
                score += 20
            elif target and (target in norm or norm in target):
                score += 10
            row_year = str(row.get("first_air_date") or row.get("release_date") or "")[:4]
            if year and row_year == year:
                score += 5
            scored.append((score, row))
        scored.sort(key=lambda item: item[0], reverse=True)
        best = scored[0][1]
        best_name = str(best.get("name") or best.get("title") or title).strip()
        best_year = str(best.get("first_air_date") or best.get("release_date") or "")[:4]
        return {
            "id": str(best.get("id") or "").strip(),
            "name": best_name,
            "year": best_year or year,
            "confidence": "TMDB 标题搜索匹配" if scored[0][0] >= 10 else "TMDB 候选匹配",
        }

    def _tmdb_context(self) -> tuple[str, str, str]:
        store = _read_store(self.store_path)
        emby = _apply_emby_env_overrides(store.get("embyConfig"))
        token = str(emby.get("tmdbToken") or os.environ.get("APP_TMDB_TOKEN") or os.environ.get("TMDB_TOKEN") or "").strip()
        language = str(emby.get("tmdbLanguage") or "zh-CN").strip() or "zh-CN"
        region = str(emby.get("tmdbRegion") or "CN").strip().upper() or "CN"
        return token, language, region

    def _tmdb_get_json(self, path_with_query: str, *, token: str) -> dict[str, Any]:
        target = f"https://api.themoviedb.org/3{path_with_query}"
        request = urllib.request.Request(
            target,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "User-Agent": "VistamirrorAI/1.0",
            },
        )
        with urllib.request.urlopen(request, timeout=12) as response:
            return json.loads(response.read().decode("utf-8", errors="replace"))

    @staticmethod
    def _normalize_ai_title(value: str) -> str:
        text = str(value or "").strip().lower()
        text = re.sub(r"[\s·._:：,，。!！?？'\"《》「」“”\-—–]+", "", text)
        return text

    def _resolve_ai_series_counts(
        self,
        *,
        item_id: str,
        detail: dict[str, Any],
        keyword: str = "",
        title: str = "",
        identity: dict[str, Any] | None = None,
    ) -> tuple[int, int, list[str], str, str]:
        season_meta = self._fetch_ai_series_season_meta(item_id=item_id)
        sources: list[str] = []
        episode_count = 0
        season_count = 0
        season_lines: list[str] = []
        latest_text = ""
        try:
            episodes = self._fetch_ai_series_episodes(item_id=item_id)
        except Exception as err:
            LOGGER.warning("TG AI series episodes failed: item_id=%s err=%s", item_id, err)
            status = getattr(err, "code", "")
            if str(status) in {"401", "403"}:
                sources.append("Episodes 单集列表读取失败：可能是 API Key 权限不足")
            else:
                sources.append(f"Episodes 单集列表读取失败：{self._format_emby_error(err)}")
            episodes = []

        if episodes:
            season_count, episode_count, season_lines, latest_text = self._summarize_ai_episode_rows(episodes, season_meta=season_meta)
            sources.append(f"Episodes 实际列表：{episode_count} 集{f'，最新 {latest_text}' if latest_text else ''}")

        direct_rows = self._search_ai_series_episode_rows(keyword=keyword or title, series_name=title or str(detail.get("Name") or ""))
        direct_summary = self._summarize_ai_episode_rows(direct_rows, season_meta=season_meta) if direct_rows else (0, 0, [], "")
        direct_season_count, direct_episode_count, direct_lines, direct_latest = direct_summary
        if direct_episode_count:
            sources.append(f"直接单集搜索：{direct_episode_count} 集{f'，最新 {direct_latest}' if direct_latest else ''}")

        recent = self._resolve_ai_recent_library_highest_episode(title or keyword)
        if recent.get("latestEpisodeNumber"):
            recent_latest = str(recent.get("latestText") or "")
            recent_suffix = f"，最新 {recent_latest}" if recent_latest else ""
            sources.append(f"最近入库：最高集号 {recent.get('latestEpisodeNumber')}{recent_suffix}（不作为实际单集总数）")

        tmdb_expected = self._fetch_ai_tmdb_expected_counts(identity or {}, local_title=title or keyword)
        if tmdb_expected.get("episodeCount"):
            tmdb_season_count = int(tmdb_expected.get("seasonCount") or 0)
            tmdb_suffix = f"，共 {tmdb_season_count} 季" if tmdb_season_count else ""
            sources.append(f"TMDB 公开总集数：{tmdb_expected.get('episodeCount')} 集{tmdb_suffix}")

        candidates = [
            ("Episodes", episode_count, season_count, season_lines, latest_text),
            ("直接单集搜索", direct_episode_count, direct_season_count, direct_lines, direct_latest),
        ]
        best_source, best_count, best_seasons, best_lines, best_latest = max(candidates, key=lambda row: row[1])
        if best_count > episode_count and best_lines:
            season_lines = best_lines
        elif not season_lines and best_lines:
            season_lines = best_lines
        final_episode_count = max(episode_count, direct_episode_count)
        final_season_count = max(season_count, direct_season_count, 1 if final_episode_count else 0)
        final_latest = self._pick_latest_episode_text(latest_text, direct_latest, str(recent.get("latestText") or ""))

        if final_episode_count <= 0:
            fallback_season_count, fallback_episode_count, fallback_lines = self._resolve_ai_series_counts_from_seasons(
                detail=detail,
                season_meta=season_meta,
            )
            final_episode_count = fallback_episode_count
            final_season_count = fallback_season_count
            season_lines = season_lines or fallback_lines
            sources.append("最终判断回退：使用季字段统计")

        missing_text = ""
        expected_count = int(tmdb_expected.get("episodeCount") or 0)
        if expected_count and final_episode_count:
            missing = max(0, expected_count - final_episode_count)
            missing_text = f"；TMDB 对照缺失 {missing} 集" if missing else "；TMDB 对照已齐"

        final_line = f"最终判断：实际可读取单集 {final_episode_count} 集"
        if final_latest:
            final_line = f"{final_line}，最新已到 {final_latest}"
        final_line = f"{final_line}{missing_text}"
        sources.append(final_line)
        if best_count > 0 and best_source != "Episodes":
            sources.append(f"冲突处理：{best_source} 高于 Episodes 聚合时，按更高的本地入库集数判断")

        self._log_project_event(
            level="info",
            module="webhook",
            action="ai_media_query_reconciled",
            message="AI 媒体库剧集查询已完成多来源合并。",
            detail={
                "title": title or keyword,
                "embySeriesId": item_id,
                "tmdbId": (identity or {}).get("tmdbId"),
                "episodesCount": episode_count,
                "directEpisodeCount": direct_episode_count,
                "recentLatestEpisodeNumber": recent.get("latestEpisodeNumber"),
                "tmdbExpectedCount": tmdb_expected.get("episodeCount"),
                "finalEpisodeCount": final_episode_count,
            },
        )
        return max(0, final_season_count), max(0, final_episode_count), season_lines, final_latest, "；".join(sources)

    def _fetch_ai_series_episodes(self, *, item_id: str, page_size: int = 1000) -> list[dict[str, Any]]:
        safe_id = urllib.parse.quote(str(item_id or "").strip(), safe="")
        if not safe_id:
            return []
        rows: list[dict[str, Any]] = []
        start = 0
        safe_page_size = max(50, min(2000, int(page_size or 1000)))
        while start < 10000:
            query = urllib.parse.urlencode(
                {
                    "Fields": "Name,SeasonId,ParentId,SeriesId,ParentIndexNumber,IndexNumber",
                    "StartIndex": str(start),
                    "Limit": str(safe_page_size),
                }
            )
            payload = self._emby_get(f"/Shows/{safe_id}/Episodes?{query}")
            items = payload.get("Items") if isinstance(payload, dict) else payload
            page = [row for row in items if isinstance(row, dict)] if isinstance(items, list) else []
            rows.extend(page)
            total = payload.get("TotalRecordCount") if isinstance(payload, dict) else None
            if isinstance(total, int) and len(rows) >= total:
                break
            if len(page) < safe_page_size:
                break
            start += safe_page_size
        return rows

    def _summarize_ai_episode_rows(self, rows: list[dict[str, Any]], *, season_meta: dict[int, str]) -> tuple[int, int, list[str], str]:
        season_counts: dict[int, int] = {}
        latest: tuple[int, int, str] | None = None
        unique_rows: list[dict[str, Any]] = []
        seen: set[str] = set()
        for row in rows:
            if not isinstance(row, dict):
                continue
            season_no = self._coerce_index_number(row.get("ParentIndexNumber")) or 0
            episode_no = self._coerce_index_number(row.get("IndexNumber")) or 0
            item_id = str(row.get("Id") or "").strip()
            series_id = str(row.get("SeriesId") or "").strip()
            if episode_no > 0:
                dedupe_key = f"index:{series_id}:{season_no}:{episode_no}"
            elif item_id:
                dedupe_key = f"id:{item_id}"
            else:
                dedupe_key = f"fallback:{series_id}:{season_no}:{episode_no}:{str(row.get('Name') or '').strip()}"
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            unique_rows.append(row)
            season_key = season_no if season_no > 0 else 1
            season_counts[season_key] = int(season_counts.get(season_key) or 0) + 1
            if episode_no > 0:
                name = str(row.get("Name") or "").strip()
                current = (season_key, episode_no, name)
                if latest is None or (current[0], current[1]) > (latest[0], latest[1]):
                    latest = current
        lines: list[str] = []
        for season_no in sorted(season_counts):
            count = int(season_counts.get(season_no) or 0)
            label = season_meta.get(season_no) or f"S{season_no}"
            lines.append(f"{label} {count}集")
        latest_text = ""
        if latest:
            latest_text = f"S{latest[0]:02d}E{latest[1]:02d}"
            if latest[2]:
                latest_text = f"{latest_text}「{latest[2]}」"
        return len(season_counts), len(unique_rows), lines, latest_text

    def _search_ai_series_episode_rows(self, *, keyword: str, series_name: str) -> list[dict[str, Any]]:
        keyword = str(keyword or series_name or "").strip()
        if not keyword:
            return []
        query = urllib.parse.urlencode(
            {
                "Recursive": "true",
                "SearchTerm": keyword,
                "IncludeItemTypes": "Episode",
                "Fields": "Name,Type,SeriesName,ParentIndexNumber,IndexNumber,SeriesId",
                "Limit": "1000",
            }
        )
        try:
            payload = self._emby_get(f"/Items?{query}")
        except Exception as err:
            LOGGER.warning("TG AI direct episode search failed: keyword=%s err=%s", keyword, err)
            return []
        items = payload.get("Items") if isinstance(payload, dict) else payload
        rows = [row for row in items if isinstance(row, dict)] if isinstance(items, list) else []
        target = self._normalize_ai_title(series_name or keyword)
        filtered: list[dict[str, Any]] = []
        for row in rows:
            row_series = str(row.get("SeriesName") or "").strip()
            row_title = self._normalize_ai_title(row_series)
            if not target or row_title == target or target in row_title or row_title in target:
                filtered.append(row)
        return filtered

    def _resolve_ai_recent_library_highest_episode(self, title: str) -> dict[str, Any]:
        safe_title = str(title or "").strip()
        if not safe_title:
            return {}
        try:
            latest, _tried, _err = self._fetch_latest_items_with_fallback(limit=40)
        except Exception:
            return {}
        rows = latest if isinstance(latest, list) else []
        target = self._normalize_ai_title(safe_title)
        best: tuple[int, int, str] | None = None
        for row in rows:
            if not isinstance(row, dict):
                continue
            item_type = str(row.get("Type") or "").strip().lower()
            if item_type != "episode":
                continue
            series_name = str(row.get("SeriesName") or "").strip()
            if not series_name:
                continue
            row_target = self._normalize_ai_title(series_name)
            if target and target != row_target:
                continue
            season = self._coerce_index_number(row.get("ParentIndexNumber")) or 1
            episode = self._coerce_index_number(row.get("IndexNumber")) or 0
            if episode <= 0:
                continue
            name = str(row.get("Name") or "").strip()
            current = (season, episode, name)
            if best is None or (current[0], current[1]) > (best[0], best[1]):
                best = current
        if not best:
            return {}
        latest_text = f"S{best[0]:02d}E{best[1]:02d}"
        if best[2]:
            latest_text = f"{latest_text}「{best[2]}」"
        return {"seasonCount": best[0], "latestEpisodeNumber": best[1], "latestText": latest_text}

    def _fetch_ai_tmdb_expected_counts(self, identity: dict[str, Any], *, local_title: str = "") -> dict[str, Any]:
        tmdb_id = str(identity.get("tmdbId") or "").strip()
        if not tmdb_id:
            return {}
        token, language, _region = self._tmdb_context()
        if not token:
            return {}
        try:
            detail = self._tmdb_get_json(f"/tv/{urllib.parse.quote(tmdb_id, safe='')}?{urllib.parse.urlencode({'language': language})}", token=token)
        except Exception as err:
            LOGGER.warning("TG AI TMDB expected count failed: title=%s tmdb=%s err=%s", local_title, tmdb_id, err)
            return {"error": self._format_emby_error(err)}
        seasons = detail.get("seasons") if isinstance(detail.get("seasons"), list) else []
        season_count = 0
        episode_count = 0
        season_lines: list[str] = []
        for season in seasons:
            if not isinstance(season, dict):
                continue
            season_no = self._coerce_index_number(season.get("season_number"))
            if season_no is None or season_no <= 0:
                continue
            count = self._coerce_index_number(season.get("episode_count")) or 0
            season_count += 1
            episode_count += max(0, count)
            season_lines.append(f"S{season_no} {count}集")
        return {
            "tmdbId": tmdb_id,
            "title": str(detail.get("name") or local_title or "").strip(),
            "seasonCount": season_count,
            "episodeCount": episode_count,
            "seasonLines": season_lines,
        }

    def _pick_latest_episode_text(self, *values: str) -> str:
        best: tuple[int, int, str] | None = None
        for value in values:
            text = str(value or "").strip()
            if not text:
                continue
            match = re.search(r"S(?P<season>\d{1,2})E(?P<episode>\d{1,4})", text, flags=re.IGNORECASE)
            if not match:
                continue
            season = int(match.group("season") or 0)
            episode = int(match.group("episode") or 0)
            current = (season, episode, text)
            if best is None or (season, episode) > (best[0], best[1]):
                best = current
        return best[2] if best else ""

    def _fetch_ai_series_season_meta(self, *, item_id: str) -> dict[int, str]:
        meta: dict[int, str] = {}
        try:
            seasons_rows = self._emby_get(
                f"/Shows/{urllib.parse.quote(item_id, safe='')}/Seasons?Fields=Name,ChildCount,IndexNumber,RecursiveItemCount"
            )
        except Exception as err:
            LOGGER.warning("TG AI series seasons fallback: item_id=%s err=%s", item_id, err)
            return meta
        seasons = seasons_rows.get("Items") if isinstance(seasons_rows, dict) else seasons_rows
        if not isinstance(seasons, list):
            return meta
        for idx, season in enumerate(seasons):
            if not isinstance(season, dict):
                continue
            season_index = self._coerce_index_number(season.get("IndexNumber")) or 0
            season_name = str(season.get("Name") or "").strip()
            season_no = season_index if season_index > 0 else int(self._extract_season_number(season_name) or idx + 1)
            meta[season_no] = f"S{season_no}"
        return meta

    def _resolve_ai_series_counts_from_seasons(self, *, detail: dict[str, Any], season_meta: dict[int, str]) -> tuple[int, int, list[str]]:
        season_count = 0
        episode_count = 0
        lines: list[str] = []
        if episode_count <= 0:
            fallback_count = detail.get("RecursiveItemCount")
            if isinstance(fallback_count, int) and fallback_count > 0:
                episode_count = int(fallback_count)
        if season_count <= 0:
            child_count = detail.get("ChildCount")
            if isinstance(child_count, int) and child_count > 0:
                season_count = int(child_count)
        if season_count <= 0 and season_meta:
            season_count = len(season_meta)
        if season_count <= 0 and episode_count > 0:
            season_count = 1
        if season_meta and episode_count > 0:
            per_season = max(0, episode_count // max(1, len(season_meta)))
            for season_no in sorted(season_meta):
                lines.append(f"{season_meta[season_no]} {per_season}集")
        return max(0, season_count), max(0, episode_count), lines

    def _extract_ai_media_keyword(self, question: str) -> str:
        keywords = self._extract_ai_media_keywords(question)
        return keywords[0] if keywords else ""

    def _extract_ai_media_keywords(self, question: str) -> list[str]:
        text = str(question or "").strip()
        if not text:
            return []
        quoted = re.search(r"[《「“\"]([^》」”\"]{1,80})[》」”\"]", text)
        if quoted:
            keyword = self._clean_ai_keyword(str(quoted.group(1) or ""))
            return [keyword] if keyword else []

        general_patterns = (
            r"一共.*(多少|几).*(影视|资源|电影|剧集|影片|媒体)",
            r"总共.*(多少|几).*(影视|资源|电影|剧集|影片|媒体)",
            r"现在.*(多少|几).*(影视|资源|电影|剧集|影片|媒体)",
            r"(媒体库|资源库).*(总数|总量|数量|统计)",
        )
        if any(re.search(pattern, text) for pattern in general_patterns):
            return []
        if self._parse_ai_category_listing_request(text):
            return []

        value = re.sub(r"^/ai(?:@\w+)?\s*", "", text, flags=re.IGNORECASE).strip()
        value = re.sub(
            r"^(?:帮我|请|麻烦你|你帮我)?\s*(?:查看一下|看一下|查一下|看下|查下|查查|搜索一下|搜索|找一下|看看)?\s*",
            "",
            value,
            flags=re.IGNORECASE,
        )
        value = re.sub(
            r"^(?:我(?:的)?|[咱俺](?:的)?)?(?:媒体库|资源库|库)(?:里|中|里面)?\s*(?:的)?\s*",
            "",
            value,
            flags=re.IGNORECASE,
        )

        tail_patterns = (
            r"(?:我(?:的)?|[咱俺](?:的)?)?(?:媒体库|资源库|库)(?:里|中|里面)?",
            r"(?:现在|目前)?(?:最新的?|已经)?(?:更新到)?(?:第几集|哪一集|哪集|多少集|几集)",
            r"(?:一共有?|总共有?|共有|现在有|目前有|有)(?:多少集|几集)",
            r"更新到",
            r"最新(?:的)?(?:是)?",
            r"(?:缺失|缺少|缺哪|漏掉|漏)(?:的)?集",
            r"(?:的)?(?:简介|剧情|详情|演员|主演|评分)",
            r"有没有|是否有",
        )
        split_at = len(value)
        for pattern in tail_patterns:
            match = re.search(pattern, value, flags=re.IGNORECASE)
            if match and match.start() >= 2:
                split_at = min(split_at, match.start())
        primary_text = value[:split_at].strip()
        if split_at < len(value):
            primary_text = re.sub(r"的$", "", primary_text).strip()
        primary = self._clean_ai_keyword(primary_text)

        fallback = value
        fallback = re.sub(
            r"(?:我(?:的)?|[咱俺](?:的)?)?(?:媒体库|资源库|库)(?:里|中|里面)?",
            " ",
            fallback,
            flags=re.IGNORECASE,
        )
        fallback = re.sub(
            r"(?:现在|目前|最新的?|已经|一共有?|总共有?|共有|更新到|第几集|哪一集|哪集|有多少集|多少集|有几集|几集|缺失的?集|缺少的?集|缺哪几集|漏掉的?集|的?简介|的?剧情|的?详情|的?演员|的?主演|的?评分|有没有|是否有|有吗|吗)",
            " ",
            fallback,
            flags=re.IGNORECASE,
        )
        fallback = self._clean_ai_keyword(fallback)
        keywords: list[str] = []
        for candidate in (primary, fallback):
            if candidate and candidate not in keywords:
                keywords.append(candidate)
        return keywords

    @staticmethod
    def _clean_ai_keyword(value: str) -> str:
        clean = str(value or "").strip()
        clean = clean.strip(" ，。！？?：:；;、|/\\[]()（）【】《》「」“”\"'")
        clean = re.sub(r"\s+", " ", clean).strip()
        clean = re.sub(r"(?:了|呢|啊|呀)$", "", clean).strip()
        if len(clean) < 2:
            return ""
        if clean in {"影视资源", "资源", "媒体", "电影", "剧集", "影片", "数量", "总数"}:
            return ""
        return clean[:80]

    def _pick_best_search_item(self, *, items: list[Any], keyword: str) -> dict[str, Any]:
        normalized_keyword = self._normalize_ai_title(keyword)
        candidates = [row for row in items if isinstance(row, dict)]
        if not candidates:
            return {}

        def _score(row: dict[str, Any]) -> tuple[int, int, int]:
            row_type = str(row.get("Type") or "").strip().lower()
            name = self._normalize_ai_title(str(row.get("Name") or row.get("SeriesName") or ""))
            is_exact = 2 if normalized_keyword and name == normalized_keyword else 0
            is_contains = 1 if normalized_keyword and normalized_keyword in name else 0
            type_priority = {"series": 4, "movie": 3, "episode": 2}.get(row_type, 1)
            return is_exact, is_contains, type_priority

        candidates.sort(key=_score, reverse=True)
        return candidates[0]

    def _cmd_report(self, period: str) -> str:
        now = datetime.now()
        if period == "day":
            start = datetime(now.year, now.month, now.day)
            title = "📊 今日日报"
        elif period == "week":
            start = datetime(now.year, now.month, now.day) - timedelta(days=now.weekday())
            title = "📅 本周周报"
        elif period == "month":
            start = datetime(now.year, now.month, 1)
            title = "🗓 本月月报"
        else:
            start = datetime(now.year, 1, 1)
            title = "📜 年度总结"
        rows = read_recent_playback_events(self.event_log_path, limit=5000)
        filtered = self._filter_events_by_window(rows, start_at=start, end_at=now)
        if not filtered:
            filtered = self._build_report_rows_from_activity_log(start_at=start, end_at=now)
        total_events = len(filtered)
        users = sorted({str(row.get("username") or "").strip() for row in filtered if str(row.get("username") or "").strip()})
        media = sorted({self._format_episode_title_from_row(row) for row in filtered if self._format_episode_title_from_row(row)})
        total_seconds, user_seconds, media_plays, media_users = self._build_report_stats(filtered)
        rank_rows = self._rank_icons()
        lines = [
            title,
            f"🗓 时间范围：{start.strftime('%Y-%m-%d %H:%M')} ~ {now.strftime('%H:%M')}",
            "",
            "━━━━━━━━━━━━━━━",
            "📈 今日概览" if period == "day" else "📈 统计概览",
            "━━━━━━━━━━━━━━━",
            f"▶️ 总播放次数：{total_events} 次",
            f"👥 活跃用户数：{len(users)} 人",
            f"🎬 涉及影片数：{len(media)} 部",
            f"⏱ 观看总时长：{self._format_hours_minutes(total_seconds)}",
            "",
            "━━━━━━━━━━━━━━━",
            "🏆 用户观看排行榜（前十名）",
            "━━━━━━━━━━━━━━━",
        ]
        sorted_user = sorted(user_seconds.items(), key=lambda x: x[1], reverse=True)
        if sorted_user:
            for idx, (name, seconds) in enumerate(sorted_user[:10], start=1):
                lines.append(f"{rank_rows[idx-1]} {name} — {self._format_hours_minutes(seconds)}")
        else:
            lines.append("🥇 暂无数据 — 0小时0分钟")
        lines.extend(
            [
                "",
                "━━━━━━━━━━━━━━━",
                "🎬 影片播放排行榜（前十名）",
                "━━━━━━━━━━━━━━━",
            ]
        )
        sorted_media = sorted(media_plays.items(), key=lambda x: x[1], reverse=True)
        if sorted_media:
            for idx, (name, count) in enumerate(sorted_media[:10], start=1):
                viewer_count = len(media_users.get(name) or set())
                lines.append(f"{rank_rows[idx-1]} {name} — {count}次 / {viewer_count}人观看")
        else:
            lines.append("🥇 暂无数据 — 0次 / 0人观看")
        return "\n".join(lines)

    def _build_report_rows_from_activity_log(self, *, start_at: datetime, end_at: datetime) -> list[dict[str, Any]]:
        try:
            payload = self._emby_get("/System/ActivityLog/Entries?Limit=2000&StartIndex=0")
        except Exception:
            return []
        logs = payload.get("Items") if isinstance(payload, dict) else payload
        if not isinstance(logs, list):
            return []

        rows: list[dict[str, Any]] = []
        dedupe: set[str] = set()
        for log in logs:
            if not isinstance(log, dict):
                continue
            played_at = self._parse_activity_log_datetime(log)
            if not played_at:
                continue
            if played_at < start_at or played_at > end_at:
                continue
            text = self._activity_log_text(log)
            if not self._is_playback_activity_log(log, text):
                continue

            username = self._extract_activity_log_username(log, text)
            media_name = self._extract_activity_log_media_name(log, text)
            seconds = self._extract_activity_log_duration_seconds(log, text)

            dedupe_key = f"{played_at.strftime('%Y-%m-%d %H:%M')}|{username}|{media_name}"
            if dedupe_key in dedupe:
                continue
            dedupe.add(dedupe_key)

            raw: dict[str, Any] = {"Name": media_name}
            if seconds > 0:
                raw["PositionTicks"] = int(seconds * 10_000_000)
            rows.append(
                {
                    "at": played_at.isoformat(timespec="seconds"),
                    "username": username,
                    "mediaName": media_name,
                    "raw": raw,
                }
            )
        rows.sort(key=lambda row: str(row.get("at") or ""), reverse=False)
        return rows

    @staticmethod
    def _activity_log_text(log: dict[str, Any]) -> str:
        return " ".join(
            [
                str(log.get("Name") or ""),
                str(log.get("ShortOverview") or ""),
                str(log.get("Overview") or ""),
                str(log.get("Type") or ""),
                str(log.get("Event") or ""),
                str(log.get("EventName") or ""),
            ]
        ).strip()

    @staticmethod
    def _parse_activity_log_datetime(log: dict[str, Any]) -> datetime | None:
        value = str(log.get("Date") or log.get("StartDate") or log.get("DateCreated") or "").strip()
        if not value:
            return None
        normalized = value.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(normalized)
        except Exception:
            return None
        if dt.tzinfo is not None:
            dt = dt.astimezone().replace(tzinfo=None)
        return dt

    @staticmethod
    def _is_playback_activity_log(log: dict[str, Any], text: str) -> bool:
        lower = str(text or "").lower()
        event_text = " ".join(
            [
                str(log.get("Event") or "").lower(),
                str(log.get("EventName") or "").lower(),
                str(log.get("Type") or "").lower(),
            ]
        )
        if any(
            token in event_text
            for token in (
                "playbackstart",
                "playbackstopped",
                "playbackprogress",
                "userstartedplaying",
                "userstoppedplaying",
                "sessionstart",
                "sessionend",
            )
        ):
            return True
        play_tokens = (
            "开始播放",
            "停止播放",
            "正在播放",
            "已播放",
            "观看",
            "播放",
            "started playing",
            "stopped playing",
            "playing",
            "playback",
            "watched",
        )
        if any(token in lower for token in play_tokens):
            # 过滤纯上线日志，避免误统计
            if "is online from" in lower and "playing" not in lower:
                return False
            return True
        return False

    @staticmethod
    def _extract_activity_log_username(log: dict[str, Any], text: str) -> str:
        for key in ("UserName", "ByUserName", "Client", "DeviceName"):
            value = str(log.get(key) or "").strip()
            if value:
                return value
        match = re.match(r"^([^\s:：]+)\s+(?:started playing|stopped playing|is playing|正在播放|开始播放|停止播放)", str(text or ""), flags=re.IGNORECASE)
        if match:
            return str(match.group(1) or "").strip() or "未知用户"
        return "未知用户"

    @staticmethod
    def _extract_activity_log_media_name(log: dict[str, Any], text: str) -> str:
        item = log.get("Item") if isinstance(log.get("Item"), dict) else {}
        for key in ("ItemName", "Name"):
            value = str(item.get(key) or "").strip()
            if value:
                return value
        # 常见英文描述：xxx started playing <title> on ...
        match = re.search(r"(?:started playing|stopped playing|playing)\s+(.+?)(?:\s+on\s+|\s+using\s+|$)", str(text or ""), flags=re.IGNORECASE)
        if match:
            value = str(match.group(1) or "").strip(" .")
            if value:
                return value
        # 常见中文描述：开始播放/停止播放 xxx
        match = re.search(r"(?:开始播放|停止播放|正在播放|播放)\s*[：: ]?\s*(.+)$", str(text or ""))
        if match:
            value = str(match.group(1) or "").strip(" 。.")
            if value:
                return value
        fallback = str(log.get("Name") or log.get("ShortOverview") or log.get("Overview") or "").strip()
        return fallback or "未知内容"

    @staticmethod
    def _extract_activity_log_duration_seconds(log: dict[str, Any], text: str) -> int:
        candidates: list[Any] = [
            log.get("PlaybackPositionTicks"),
            log.get("PositionTicks"),
            log.get("StopPositionTicks"),
            log.get("LastPositionTicks"),
            log.get("RunTimeTicks"),
        ]
        for value in candidates:
            if isinstance(value, (int, float)) and value > 0:
                return int(float(value) / 10_000_000)

        raw = str(text or "")
        hms = re.search(r"(\d{1,2}):(\d{2}):(\d{2})", raw)
        if hms:
            return int(hms.group(1)) * 3600 + int(hms.group(2)) * 60 + int(hms.group(3))
        zh = re.search(r"(?:(\d+)\s*小时)?\s*(?:(\d+)\s*分钟)?\s*(?:(\d+)\s*秒)?", raw)
        if zh:
            hh = int(zh.group(1) or 0)
            mm = int(zh.group(2) or 0)
            ss = int(zh.group(3) or 0)
            total = hh * 3600 + mm * 60 + ss
            if total > 0:
                return total
        return 0

    def _format_recent_playback_row(self, row: dict[str, Any]) -> str:
        line, _meta = self._format_recent_playback_row_with_meta(row)
        return line

    def _format_recent_playback_row_with_meta(self, row: dict[str, Any]) -> tuple[str, dict[str, bool]]:
        start_value = str(row.get("startTime") or "").strip()
        point_value = str(row.get("at") or row.get("time") or "").strip()
        primary_time_value = start_value or point_value
        date_text = self._iso_to_mmdd(primary_time_value) or "--/--"
        time_text = self._iso_to_hhmm(primary_time_value) or "--:--"
        missing_time = not primary_time_value

        username = str(row.get("username") or row.get("user") or "").strip() or "未知用户"
        filename, parsed_episode, fallback_title = self._format_recent_playback_filename_with_status(row)
        return (
            f"🔹 {date_text} {time_text} | 👤 {username} | 📺「{filename}」",
            {
                "parsedEpisode": parsed_episode,
                "fallbackTitle": fallback_title,
                "missingTime": missing_time,
            },
        )

    @staticmethod
    def _iso_to_hhmm(value: str) -> str:
        safe = str(value or "").strip()
        if not safe:
            return ""
        try:
            return datetime.fromisoformat(safe).strftime("%H:%M")
        except Exception:
            return ""

    @staticmethod
    def _iso_to_mmdd(value: str) -> str:
        safe = str(value or "").strip()
        if not safe:
            return ""
        try:
            return datetime.fromisoformat(safe).strftime("%m-%d")
        except Exception:
            return ""

    @staticmethod
    def _format_duration_for_tg(row: dict[str, Any]) -> str:
        duration_seconds = 0
        raw_seconds = row.get("durationSec")
        if isinstance(raw_seconds, (int, float)):
            duration_seconds = max(0, int(raw_seconds))
        if duration_seconds <= 0:
            raw_minutes = row.get("durationMin")
            if not isinstance(raw_minutes, (int, float)):
                raw_minutes = row.get("duration")
            if isinstance(raw_minutes, (int, float)) and float(raw_minutes) > 0:
                duration_seconds = int(float(raw_minutes) * 60)
        if duration_seconds <= 0:
            return ""
        hours = duration_seconds // 3600
        minutes = (duration_seconds % 3600) // 60
        seconds = duration_seconds % 60
        if hours > 0:
            return f"{hours}小时{minutes:02d}分{seconds:02d}秒"
        return f"{minutes}分{seconds:02d}秒"

    def _format_recent_playback_filename_with_status(self, row: dict[str, Any]) -> tuple[str, bool, bool]:
        raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
        if raw:
            episode_filename = self._build_recent_playback_episode_filename(raw)
            if episode_filename:
                return episode_filename, True, False
            for key in ("ItemName", "Name", "FileName", "Filename"):
                value = str(raw.get(key) or "").strip()
                if value:
                    parsed = self._parse_episode_from_text(value)
                    if parsed:
                        return self._build_recent_playback_episode_filename(parsed), True, False
                    return self._clean_recent_playback_filename(value), False, False

        unified_title = str(row.get("title") or "").strip()
        if unified_title:
            parsed = self._parse_episode_from_text(unified_title)
            if parsed:
                return self._build_recent_playback_episode_filename(parsed), True, False
            return self._clean_recent_playback_filename(unified_title), False, True

        fallback = str(row.get("mediaName") or "").strip()
        if fallback:
            parsed = self._parse_episode_from_text(fallback)
            if parsed:
                return self._build_recent_playback_episode_filename(parsed), True, False
            return self._clean_recent_playback_filename(fallback), False, True

        return "未知内容", False, True

    @classmethod
    def _build_recent_playback_episode_filename(cls, source: dict[str, Any]) -> str:
        series_name = str(source.get("SeriesName") or source.get("series") or "").strip()
        item_name = str(
            source.get("Name")
            or source.get("ItemName")
            or source.get("episodeTitle")
            or source.get("FileName")
            or source.get("Filename")
            or ""
        ).strip()
        season_number = cls._coerce_index_number(source.get("ParentIndexNumber") if "ParentIndexNumber" in source else source.get("season"))
        episode_number = cls._coerce_index_number(source.get("IndexNumber") if "IndexNumber" in source else source.get("episode"))
        item_type = str(source.get("Type") or "").strip().lower()
        looks_like_episode = bool(series_name and (season_number or episode_number or item_type == "episode"))
        if not looks_like_episode:
            return ""
        if not episode_number:
            parsed = cls._parse_episode_from_text(item_name)
            if parsed:
                episode_number = cls._coerce_index_number(parsed.get("episode"))
                season_number = season_number or cls._coerce_index_number(parsed.get("season"))
                item_name = str(parsed.get("episodeTitle") or item_name).strip()
        if not episode_number:
            return ""
        season_number = season_number or 1
        clean_series = cls._clean_recent_playback_filename(series_name)
        clean_title = cls._clean_recent_playback_episode_title(item_name, series_name=clean_series, season_number=season_number, episode_number=episode_number)
        episode_code = f"S{season_number:02d}E{episode_number:02d}"
        return f"{clean_series} - {episode_code} - {clean_title}"

    @classmethod
    def _clean_recent_playback_episode_title(cls, value: str, *, series_name: str, season_number: int, episode_number: int) -> str:
        clean = cls._clean_recent_playback_filename(value)
        if not clean or clean == "未知内容":
            return f"第 {episode_number} 集"
        code = f"S{season_number:02d}E{episode_number:02d}"
        loose_code = rf"S0?{season_number}\s*[,，]?\s*Ep?0?{episode_number}"
        compact_code = rf"S0?{season_number}E0?{episode_number}"
        if series_name:
            clean = re.sub(rf"^{re.escape(series_name)}\s*[-—–]\s*", "", clean, flags=re.IGNORECASE).strip()
        clean = re.sub(rf"^{loose_code}\s*[-—–]\s*", "", clean, flags=re.IGNORECASE).strip()
        clean = re.sub(rf"^{compact_code}\s*[-—–]\s*", "", clean, flags=re.IGNORECASE).strip()
        if series_name:
            clean = re.sub(rf"^{re.escape(series_name)}\s*[-—–]\s*", "", clean, flags=re.IGNORECASE).strip()
        return clean or f"第 {episode_number} 集"

    @staticmethod
    def _clean_recent_playback_filename(value: str) -> str:
        clean = str(value or "").strip()
        clean = clean.strip("《》「」")
        clean = re.sub(r"\s+", " ", clean).strip()
        return clean or "未知内容"

    def _format_episode_title_from_row(self, row: dict[str, Any]) -> str:
        media, _parsed, _fallback = self._format_episode_title_with_status(row)
        return media

    def _format_episode_title_with_status(self, row: dict[str, Any]) -> tuple[str, bool, bool]:
        unified_title = str(row.get("title") or "").strip()
        if unified_title:
            parsed = self._parse_episode_from_text(unified_title)
            if parsed:
                return self._build_episode_display_text(parsed), True, False
            return f"《{unified_title}》", False, True
        raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
        if raw:
            item_name = str(raw.get("ItemName") or raw.get("Name") or "").strip()
            series_name = str(raw.get("SeriesName") or "").strip()
            season_num = raw.get("ParentIndexNumber")
            episode_num = raw.get("IndexNumber")
            if series_name:
                season_text = str(season_num) if isinstance(season_num, int) else "X"
                episode_text = str(episode_num) if isinstance(episode_num, int) else "X"
                title = item_name or "未知标题"
                return f"《{series_name}》第{season_text}季 第{episode_text}集「{title}」", True, False
            if item_name:
                parsed = self._parse_episode_from_text(item_name)
                if parsed:
                    return self._build_episode_display_text(parsed), True, False
                return f"《{item_name}》", False, True
        fallback = str(row.get("mediaName") or "").strip() or "未知内容"
        parsed = self._parse_episode_from_text(fallback)
        if parsed:
            return self._build_episode_display_text(parsed), True, False
        return f"《{fallback}》", False, True

    @staticmethod
    def _parse_episode_from_text(text: str) -> dict[str, str] | None:
        value = str(text or "").strip()
        if not value:
            return None

        normalized = re.sub(r"\s+", " ", value).strip()

        zh_match = re.match(
            r"^(?P<series>.+?)\s*第\s*(?P<season>\d{1,2})\s*季\s*第\s*(?P<episode>\d{1,3})\s*集(?:\s*[「《](?P<title>.+?)[》」])?$",
            normalized,
            flags=re.IGNORECASE,
        )
        if zh_match:
            series = str(zh_match.group("series") or "").strip(" -—·")
            season = str(zh_match.group("season") or "").strip()
            episode = str(zh_match.group("episode") or "").strip()
            title = str(zh_match.group("title") or "").strip(" -—·")
            return {
                "series": series,
                "season": season,
                "episode": episode,
                "episodeTitle": title or "最新更新",
            }

        pattern = re.match(
            r"^(?P<series>.+?)\s*[-—–]\s*S(?P<season>\d{1,2})\s*[,，]?\s*Ep?(?P<episode>\d{1,3})(?:\s*[-—–]\s*(?P<title>.+))?$",
            normalized,
            flags=re.IGNORECASE,
        )
        if pattern:
            series = str(pattern.group("series") or "").strip(" -—·")
            season = str(pattern.group("season") or "").strip()
            episode = str(pattern.group("episode") or "").strip()
            title = str(pattern.group("title") or "").strip(" -—·")
            return {
                "series": series,
                "season": season,
                "episode": episode,
                "episodeTitle": title or "最新更新",
            }

        compact = re.match(
            r"^(?P<series>.+?)\s*S(?P<season>\d{1,2})E(?P<episode>\d{1,3})(?:\s*[-—–]?\s*(?P<title>.+))?$",
            normalized,
            flags=re.IGNORECASE,
        )
        if compact:
            series = str(compact.group("series") or "").strip(" -—·")
            season = str(compact.group("season") or "").strip()
            episode = str(compact.group("episode") or "").strip()
            title = str(compact.group("title") or "").strip(" -—·")
            return {
                "series": series,
                "season": season,
                "episode": episode,
                "episodeTitle": title or "最新更新",
            }

        fuzzy = re.match(
            r"^(?P<series>.+?)\s*[-—–]\s*(?P<tail>.+)$",
            normalized,
            flags=re.IGNORECASE,
        )
        if fuzzy:
            series = str(fuzzy.group("series") or "").strip(" -—·")
            tail = str(fuzzy.group("tail") or "").strip()
            tail_match = re.match(r"S(?P<season>\d{1,2})\s*[,，]?\s*Ep?(?P<episode>\d{1,3})(?:\s*[-—–]?\s*(?P<title>.+))?$", tail, flags=re.IGNORECASE)
            if tail_match:
                season = str(tail_match.group("season") or "").strip()
                episode = str(tail_match.group("episode") or "").strip()
                title = str(tail_match.group("title") or "").strip(" -—·")
                return {
                    "series": series,
                    "season": season,
                    "episode": episode,
                    "episodeTitle": title or "最新更新",
                }
        return None

    @staticmethod
    def _build_episode_display_text(parsed: dict[str, str]) -> str:
        series = str(parsed.get("series") or "").strip() or "未知作品"
        season = str(parsed.get("season") or "").strip() or "X"
        episode = str(parsed.get("episode") or "").strip() or "X"
        episode_title = str(parsed.get("episodeTitle") or "").strip() or "最新更新"
        return f"《{series}》第{season}季 第{episode}集「{episode_title}」"

    @staticmethod
    def _filter_events_by_window(rows: list[dict[str, Any]], *, start_at: datetime, end_at: datetime) -> list[dict[str, Any]]:
        filtered: list[dict[str, Any]] = []
        for row in rows:
            at_text = str(row.get("at") or "").strip()
            if not at_text:
                continue
            try:
                at = datetime.fromisoformat(at_text)
            except Exception:
                continue
            if start_at <= at <= end_at:
                filtered.append(row)
        return filtered

    def _build_report_stats(
        self,
        rows: list[dict[str, Any]],
    ) -> tuple[int, dict[str, int], dict[str, int], dict[str, set[str]]]:
        per_user_media_seconds: dict[tuple[str, str], int] = {}
        media_plays: dict[str, int] = {}
        media_users: dict[str, set[str]] = {}
        for row in rows:
            username = str(row.get("username") or "").strip() or "未知用户"
            media = self._format_episode_title_from_row(row)
            media_plays[media] = int(media_plays.get(media) or 0) + 1
            media_users.setdefault(media, set()).add(username)
            seconds = self._extract_seconds_from_row(row)
            key = (username, media)
            current = int(per_user_media_seconds.get(key) or 0)
            if seconds > current:
                per_user_media_seconds[key] = seconds
        user_seconds: dict[str, int] = {}
        total_seconds = 0
        for (username, _media), seconds in per_user_media_seconds.items():
            user_seconds[username] = int(user_seconds.get(username) or 0) + int(seconds)
            total_seconds += int(seconds)
        return total_seconds, user_seconds, media_plays, media_users

    @staticmethod
    def _extract_seconds_from_row(row: dict[str, Any]) -> int:
        raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
        if not raw:
            return 0
        candidates: list[Any] = [
            raw.get("PositionTicks"),
            raw.get("positionTicks"),
            (raw.get("PlayState") or {}).get("PositionTicks") if isinstance(raw.get("PlayState"), dict) else None,
            (raw.get("playState") or {}).get("positionTicks") if isinstance(raw.get("playState"), dict) else None,
        ]
        for value in candidates:
            if isinstance(value, (int, float)) and value > 0:
                return int(float(value) / 10_000_000)
        return 0

    @staticmethod
    def _format_hours_minutes(seconds: int) -> str:
        safe = max(0, int(seconds))
        hours = safe // 3600
        minutes = (safe % 3600) // 60
        return f"{hours}小时{minutes}分钟"

    @staticmethod
    def _rank_icons() -> list[str]:
        return ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]

    def _format_search_result(self, item: dict[str, Any], *, keyword: str) -> CommandReply:
        item_id = str(item.get("Id") or "").strip()
        try:
            detail = self._emby_get(
                f"/Items/{urllib.parse.quote(item_id, safe='')}?Fields=Overview,Genres,People,PremiereDate,ProductionYear,ChildCount,RecursiveItemCount,SeriesName,ParentIndexNumber,IndexNumber,Studios,ProductionLocations,OriginalTitle,Status,ImageTags,PrimaryImageItemId,CommunityRating,CriticRating,RunTimeTicks,MediaSources,MediaStreams,Width,Height"
            )
        except Exception as err:
            LOGGER.warning("TG /sousuo item detail fallback: item_id=%s err=%s", item_id, err)
            detail = {}
        detail = detail if isinstance(detail, dict) else {}
        joined = dict(item)
        joined.update(detail)

        item_type = str(joined.get("Type") or "").strip().lower()
        title = str(joined.get("Name") or keyword or "未知内容").strip()
        genres = joined.get("Genres") if isinstance(joined.get("Genres"), list) else []
        genre_text = str(genres[0]).strip() if genres and str(genres[0]).strip() else "未分类"
        year = self._resolve_year(joined)
        overview_raw = str(joined.get("Overview") or "").strip().replace("\n", " ")
        overview = self._truncate_text(overview_raw or "暂无简介", 180)
        rating = self._format_rating(joined)
        pack_text, quality_text = self._resolve_search_media_summary(item_type=item_type, item_id=item_id, joined=joined)

        text = "\n".join(
            [
                f"📺 {title} ({year})",
                f"⭐ {rating}  |  🎭 {genre_text}",
                f"💿 📊 {pack_text} | {quality_text}",
                "",
                "📝 剧情简介:",
                overview,
            ]
        )

        poster = self._fetch_item_poster(item_id=item_id, joined=joined)
        self._log_project_event(
            level="info",
            module="webhook",
            action="telegram_search_success",
            message="Telegram 搜索返回资源。",
            detail={
                "keyword": keyword,
                "itemId": item_id,
                "itemType": item_type,
                "title": title,
                "hasPoster": bool(poster),
            },
        )
        if poster:
            photo_bytes, photo_mime, image_item_id = poster
            return {
                "photo_bytes": photo_bytes,
                "photo_mime": photo_mime,
                "photo_filename": self._poster_filename(image_item_id=image_item_id, content_type=photo_mime),
                "photo_caption": self._truncate_text(text, 980),
            }
        return text

    def _resolve_item_poster_path(self, *, item_id: str, joined: dict[str, Any]) -> tuple[str, str]:
        image_item_id = str(joined.get("PrimaryImageItemId") or item_id or "").strip()
        if not image_item_id:
            return "", ""
        image_tags = joined.get("ImageTags") if isinstance(joined.get("ImageTags"), dict) else {}
        primary_tag = str(image_tags.get("Primary") or "").strip() if image_tags else ""
        params: dict[str, str] = {"quality": "90", "maxWidth": "720"}
        if primary_tag:
            params["tag"] = primary_tag
        return f"/Items/{urllib.parse.quote(image_item_id, safe='')}/Images/Primary?{urllib.parse.urlencode(params)}", image_item_id

    def _fetch_item_poster(self, *, item_id: str, joined: dict[str, Any]) -> tuple[bytes, str, str] | None:
        path, image_item_id = self._resolve_item_poster_path(item_id=item_id, joined=joined)
        if not path:
            self._log_project_event(
                level="warning",
                module="webhook",
                action="telegram_search_poster_missing",
                message="Telegram 搜索结果没有可用海报 ID。",
                detail={"itemId": item_id},
            )
            return None
        try:
            photo_bytes, content_type = self._emby_fetch_bytes(path)
            return photo_bytes, content_type, image_item_id
        except Exception as err:
            status = getattr(err, "code", "")
            self._log_project_event(
                level="warning",
                module="webhook",
                action="telegram_search_poster_fetch_failed",
                message="Telegram 搜索海报拉取失败，将回退为纯文本。",
                detail={"itemId": item_id, "imageItemId": image_item_id, "status": status, "error": str(err)},
            )
            return None

    @staticmethod
    def _poster_filename(*, image_item_id: str, content_type: str) -> str:
        extension = "jpg"
        mime = str(content_type or "").lower()
        if "png" in mime:
            extension = "png"
        elif "webp" in mime:
            extension = "webp"
        return f"poster-{re.sub(r'[^A-Za-z0-9_-]+', '', image_item_id) or 'item'}.{extension}"

    def _resolve_search_media_summary(self, *, item_type: str, item_id: str, joined: dict[str, Any]) -> tuple[str, str]:
        if item_type == "series":
            episode_count = int(joined.get("RecursiveItemCount") or joined.get("ChildCount") or 0)
            media_source = self._pick_series_media_source(item_id=item_id)
            pack_text = f"共 {episode_count if episode_count > 0 else '?'} 集"
            return pack_text, self._format_media_quality(media_source)
        if item_type == "episode":
            pack_text = "单集"
            season = joined.get("ParentIndexNumber")
            episode = joined.get("IndexNumber")
            if isinstance(season, int) and isinstance(episode, int):
                pack_text = f"第 {season} 季 第 {episode} 集"
            return pack_text, self._format_media_quality(joined)
        return "电影完整版", self._format_media_quality(joined)

    def _pick_series_media_source(self, *, item_id: str) -> dict[str, Any]:
        if not item_id:
            return {}
        try:
            payload = self._emby_get(
                f"/Shows/{urllib.parse.quote(item_id, safe='')}/Episodes?Fields=MediaSources,MediaStreams,Width,Height,RunTimeTicks&Limit=80"
            )
        except Exception as err:
            LOGGER.warning("TG /sousuo series media fallback: item_id=%s err=%s", item_id, err)
            return {}
        items = payload.get("Items") if isinstance(payload, dict) else payload
        if not isinstance(items, list):
            return {}
        best: dict[str, Any] = {}
        best_score = -1
        for row in items:
            if not isinstance(row, dict):
                continue
            score = self._media_quality_score(row)
            if score > best_score:
                best = row
                best_score = score
        return best

    @staticmethod
    def _format_rating(detail: dict[str, Any]) -> str:
        for key in ("CommunityRating", "CriticRating"):
            value = detail.get(key)
            if isinstance(value, (int, float)) and value > 0:
                return f"{float(value):.1f}".rstrip("0").rstrip(".")
        return "N/A"

    @classmethod
    def _media_quality_score(cls, item: dict[str, Any]) -> int:
        media = cls._best_media_source(item)
        width, height = cls._media_dimensions(item, media)
        bitrate = cls._media_bitrate(media)
        return (height * 100000) + width + bitrate

    @classmethod
    def _format_media_quality(cls, item: dict[str, Any]) -> str:
        media = cls._best_media_source(item)
        width, height = cls._media_dimensions(item, media)
        resolution = cls._format_resolution(width=width, height=height)
        hdr = cls._format_hdr(media)
        bitrate = cls._format_bitrate(cls._media_bitrate(media))
        video_label = " ".join([part for part in (resolution, hdr) if part])
        parts = [part for part in (video_label, bitrate) if part]
        return " | ".join(parts) if parts else "质量未知"

    @staticmethod
    def _best_media_source(item: dict[str, Any]) -> dict[str, Any]:
        sources = item.get("MediaSources") if isinstance(item.get("MediaSources"), list) else []
        best: dict[str, Any] = {}
        best_bitrate = -1
        for source in sources:
            if not isinstance(source, dict):
                continue
            bitrate = TelegramCommandService._media_bitrate(source)
            if bitrate > best_bitrate:
                best = source
                best_bitrate = bitrate
        if best:
            return best
        return item

    @staticmethod
    def _media_dimensions(item: dict[str, Any], media: dict[str, Any]) -> tuple[int, int]:
        width = int(media.get("Width") or item.get("Width") or 0)
        height = int(media.get("Height") or item.get("Height") or 0)
        streams = media.get("MediaStreams") if isinstance(media.get("MediaStreams"), list) else item.get("MediaStreams")
        if isinstance(streams, list):
            for stream in streams:
                if not isinstance(stream, dict):
                    continue
                if str(stream.get("Type") or "").lower() != "video":
                    continue
                width = int(stream.get("Width") or width or 0)
                height = int(stream.get("Height") or height or 0)
                break
        return width, height

    @staticmethod
    def _media_bitrate(media: dict[str, Any]) -> int:
        for key in ("Bitrate", "bitrate"):
            value = media.get(key)
            if isinstance(value, (int, float)) and value > 0:
                return int(value)
        streams = media.get("MediaStreams") if isinstance(media.get("MediaStreams"), list) else []
        total = 0
        for stream in streams:
            if isinstance(stream, dict) and isinstance(stream.get("BitRate"), (int, float)):
                total += int(stream.get("BitRate") or 0)
        return total

    @staticmethod
    def _format_resolution(*, width: int, height: int) -> str:
        safe_height = max(0, int(height or 0))
        safe_width = max(0, int(width or 0))
        if safe_height >= 2160 or safe_width >= 3800:
            return "4K"
        if safe_height >= 1440:
            return "2K"
        if safe_height >= 1080:
            return "1080p"
        if safe_height >= 720:
            return "720p"
        return ""

    @staticmethod
    def _format_hdr(media: dict[str, Any]) -> str:
        text = json.dumps(media, ensure_ascii=False).lower()
        if "dolbyvision" in text or "dolby vision" in text or "dovi" in text:
            return "DoVi"
        if "hdr10+" in text:
            return "HDR10+"
        if "hdr" in text:
            return "HDR"
        return ""

    @staticmethod
    def _format_bitrate(value: int) -> str:
        safe = max(0, int(value or 0))
        if safe <= 0:
            return ""
        mbps = safe / 1_000_000
        return f"{mbps:.1f}Mbps"

    def _format_season_overview_lines(self, *, item_type: str, item_id: str, joined: dict[str, Any]) -> list[str]:
        if item_type == "movie":
            return ["电影 · 完整版 ✅"]
        if item_type == "episode":
            season = joined.get("ParentIndexNumber")
            episode = joined.get("IndexNumber")
            season_text = str(season) if isinstance(season, int) and season > 0 else "1"
            episode_text = str(episode) if isinstance(episode, int) and episode > 0 else "1"
            return [f"S{season_text} · 第 {episode_text} 集 ✅"]
        if item_type != "series" or not item_id:
            total = max(1, int(joined.get("RecursiveItemCount") or joined.get("ChildCount") or 1))
            return [f"S1 · 全 {total} 集 ✅"]

        seasons_rows = self._emby_get(
            f"/Shows/{urllib.parse.quote(item_id, safe='')}/Seasons?Fields=Name,ChildCount,IndexNumber,RecursiveItemCount"
        )
        seasons = seasons_rows.get("Items") if isinstance(seasons_rows, dict) else seasons_rows
        if not isinstance(seasons, list) or not seasons:
            total = max(1, int(joined.get("RecursiveItemCount") or joined.get("ChildCount") or 1))
            return [f"S1 · 全 {total} 集 ✅"]

        lines: list[str] = []
        status_raw = str(joined.get("Status") or "").strip().lower()
        is_continuing_series = status_raw in {"continuing", "returningseries", "inproduction"}
        last_season_index = len(seasons) - 1
        for idx, season in enumerate(seasons[:8]):
            if not isinstance(season, dict):
                continue
            season_index = season.get("IndexNumber")
            season_name = str(season.get("Name") or "").strip()
            season_no = str(season_index) if isinstance(season_index, int) and season_index > 0 else self._extract_season_number(season_name) or str(idx + 1)
            episode_count = season.get("RecursiveItemCount")
            if not isinstance(episode_count, int) or episode_count < 0:
                child_count = season.get("ChildCount")
                episode_count = child_count if isinstance(child_count, int) and child_count >= 0 else 0

            if episode_count <= 0:
                lines.append(f"S{season_no} · 待播 ⏳")
                continue
            if is_continuing_series and idx == last_season_index:
                lines.append(f"S{season_no} · 更新至第 {episode_count} 集 🔄")
            else:
                lines.append(f"S{season_no} · 全 {episode_count} 集 ✅")
        return lines or ["S1 · 待播 ⏳"]

    @staticmethod
    def _extract_season_number(text: str) -> str:
        match = re.search(r"(\d+)", str(text or ""))
        if not match:
            return ""
        return str(match.group(1) or "")

    def _resolve_pack_count(self, *, item_type: str, item_id: str, detail: dict[str, Any]) -> tuple[int, int]:
        if item_type == "movie":
            return 0, 1
        if item_type == "episode":
            return 1, 1
        if item_type != "series" or not item_id:
            return 0, 1
        seasons_rows = self._emby_get(f"/Shows/{urllib.parse.quote(item_id, safe='')}/Seasons")
        seasons = seasons_rows.get("Items") if isinstance(seasons_rows, dict) else seasons_rows
        season_count = len(seasons) if isinstance(seasons, list) else int(detail.get("ChildCount") or 0)
        episode_count = 0
        if isinstance(seasons, list):
            for season in seasons:
                if not isinstance(season, dict):
                    continue
                child = season.get("ChildCount")
                if isinstance(child, int):
                    episode_count += max(0, child)
        if episode_count <= 0:
            recursive = detail.get("RecursiveItemCount")
            if isinstance(recursive, int) and recursive > 0:
                episode_count = recursive
        return max(0, season_count), max(0, episode_count)

    @staticmethod
    def _resolve_year(detail: dict[str, Any]) -> str:
        year = detail.get("ProductionYear")
        if isinstance(year, int) and year > 0:
            return str(year)
        premiere = str(detail.get("PremiereDate") or "").strip()
        if re.match(r"^\d{4}", premiere):
            return premiere[:4]
        return "未知"

    @staticmethod
    def _format_pack_text(*, item_type: str, season_count: int, episode_count: int) -> str:
        if item_type == "series":
            return f"共{max(1, season_count)}季/{max(1, episode_count)}集"
        return f"共{max(1, episode_count)}集"

    @staticmethod
    def _format_library_line(*, item_type: str, season_count: int, episode_count: int, joined: dict[str, Any]) -> str:
        if item_type == "episode":
            season = joined.get("ParentIndexNumber")
            episode = joined.get("IndexNumber")
            season_text = str(season) if isinstance(season, int) else "X"
            episode_text = str(episode) if isinstance(episode, int) else "X"
            name = str(joined.get("Name") or "未知标题").strip()
            return f"第{season_text}季  第{episode_text}集「{name}」  ✅已入库"
        if item_type == "series":
            return f"共{max(1, season_count)}季  共{max(1, episode_count)}集  ✅已入库"
        return f"共{max(1, episode_count)}集  ✅已入库"

    @staticmethod
    def _join_names(values: list[str]) -> str:
        rows = [str(v).strip() for v in values if str(v).strip()]
        return " / ".join(rows[:5]) if rows else "未知"

    @staticmethod
    def _truncate_text(text: str, limit: int) -> str:
        value = str(text or "").strip()
        if len(value) <= max(1, int(limit)):
            return value
        return value[: max(1, int(limit)) - 1] + "…"
