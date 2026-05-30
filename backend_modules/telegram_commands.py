from __future__ import annotations

from datetime import datetime, timedelta
import json
import logging
import os
import pathlib
import re
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable, Union

from .notification_config import normalize_bot_config
from .playback_event_logger import read_recent_playback_events
from .playback_history_service import PlaybackHistoryService
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
    {"command": "help", "description": "🤖 帮助菜单"},
    {"command": "start", "description": "🚀 启动机器人"},
    {"command": "check", "description": "📡 系统探针"},
]

LOGGER = logging.getLogger(__name__)
CommandReply = Union[str, dict[str, Any]]
DEFAULT_EMBY_CLIENT_NAME = "镜界Vistamirror User Console"
EMBY_ENV_FIELD_MAP: dict[str, str] = {
    "serverUrl": "APP_EMBY_SERVER_URL",
    "apiKey": "APP_EMBY_API_KEY",
    "clientName": "APP_EMBY_CLIENT_NAME",
}


def _read_store(store_path: pathlib.Path) -> dict[str, Any]:
    if not store_path.exists():
        return {"embyConfig": {}, "invites": [], "botConfig": normalize_bot_config({})}
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
    }


def _help_text() -> str:
    lines = ["🤖 镜界 Vistamirror Bot 帮助菜单", ""]
    lines.extend([f"/{row['command']} - {row['description']}" for row in COMMAND_MENU])
    return "\n".join(lines)


def _apply_emby_env_overrides(raw: Any) -> dict[str, Any]:
    source = raw if isinstance(raw, dict) else {}
    merged = {
        "serverUrl": str(source.get("serverUrl") or "").strip(),
        "apiKey": str(source.get("apiKey") or "").strip(),
        "clientName": str(source.get("clientName") or "").strip() or DEFAULT_EMBY_CLIENT_NAME,
        "updatedAt": str(source.get("updatedAt") or "").strip(),
    }
    for field, env_name in EMBY_ENV_FIELD_MAP.items():
        env_value = str(os.environ.get(env_name) or "").strip()
        if env_value:
            merged[field] = env_value
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

    def stop(self) -> None:
        self._stop_event.set()
        self._wakeup_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)

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

    def _handle_update(self, update: dict[str, Any], token: str) -> None:
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
        chat_type = str(chat.get("type") or "").strip().lower()
        if chat_type and chat_type != "private":
            return
        text = str(message.get("text") or "").strip()
        if not text.startswith("/"):
            return
        cmd_text, _, args = text.partition(" ")
        cmd_name = cmd_text.split("@", 1)[0].lower().strip("/")
        self._log_project_event(
            level="info",
            module="webhook",
            action="telegram_command_received",
            message=f"收到 Telegram 指令：/{cmd_name}",
            detail={"command": cmd_name, "chatType": chat_type or "private"},
        )
        try:
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
        if not reply:
            return
        try:
            self._send_command_reply(token=token, chat_id=str(chat_id), reply=reply)
        except Exception:
            return

    def _handle_callback_query(self, callback: dict[str, Any], token: str) -> None:
        callback_id = str(callback.get("id") or "").strip()
        data = str(callback.get("data") or "").strip()
        message = callback.get("message") if isinstance(callback.get("message"), dict) else {}
        chat = message.get("chat") if isinstance(message.get("chat"), dict) else {}
        chat_id = chat.get("id")
        message_id = message.get("message_id")
        if not callback_id:
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
            self.sender.send_text(token=token, chat_id=chat_id, text=text, reply_markup=reply_markup)

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

    def _pick_best_search_item(self, *, items: list[Any], keyword: str) -> dict[str, Any]:
        normalized_keyword = str(keyword or "").strip().lower()
        candidates = [row for row in items if isinstance(row, dict)]
        if not candidates:
            return {}

        def _score(row: dict[str, Any]) -> tuple[int, int]:
            row_type = str(row.get("Type") or "").strip().lower()
            name = str(row.get("Name") or "").strip().lower()
            is_exact = 1 if normalized_keyword and name == normalized_keyword else 0
            type_priority = {"series": 4, "movie": 3, "episode": 2}.get(row_type, 1)
            return is_exact, type_priority

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
        start_hhmm = self._iso_to_hhmm(str(row.get("startTime") or "").strip())
        end_hhmm = self._iso_to_hhmm(str(row.get("endTime") or "").strip())
        point_hhmm = self._iso_to_hhmm(str(row.get("at") or row.get("time") or "").strip())
        missing_time = False
        if start_hhmm and end_hhmm:
            time_text = f"{start_hhmm}-{end_hhmm}"
        elif start_hhmm:
            time_text = start_hhmm
        elif end_hhmm:
            time_text = end_hhmm
        elif point_hhmm:
            time_text = point_hhmm
        else:
            time_text = "--:--"
            missing_time = True

        duration_text = self._format_duration_for_tg(row)
        if duration_text:
            time_text = f"{time_text}（{duration_text}）"

        username = str(row.get("username") or row.get("user") or "").strip() or "未知用户"
        media, parsed_episode, fallback_title = self._format_episode_title_with_status(row)
        return (
            f"🕰 {time_text} » 👤 {username} » 📺 {media}",
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
