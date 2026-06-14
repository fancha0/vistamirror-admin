from __future__ import annotations

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
}


def _read_store(store_path: pathlib.Path) -> dict[str, Any]:
    if not store_path.exists():
        return {"embyConfig": {}, "invites": [], "botConfig": normalize_bot_config({}), "aiConfig": normalize_ai_config({})}
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
        self._pending_ai_actions: dict[str, dict[str, Any]] = {}

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
        text = str(message.get("text") or "").strip()
        if not text:
            return
        is_private = not chat_type or chat_type == "private"
        is_command = text.startswith("/")
        cmd_name = ""
        args = text
        if is_command:
            cmd_text, _, args = text.partition(" ")
            cmd_name = cmd_text.split("@", 1)[0].lower().strip("/")
        elif not is_private:
            return
        if not is_command and is_private:
            cmd_name = "ai"
        if not is_private and cmd_name != "ai":
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
                reply = self._dispatch_ai_streaming(args.strip(), token=token, chat_id=str(chat_id))
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
        except Exception:
            return

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
                self.sender.send_text(token=token, chat_id=chat_id, text=text, reply_markup=reply_markup, parse_mode=parse_mode)
            except Exception as err:
                if parse_mode != "MarkdownV2":
                    raise
                LOGGER.warning("Telegram MarkdownV2 send failed, fallback to plain text: %s", err)
                self.sender.send_text(token=token, chat_id=chat_id, text=fallback_text or text, reply_markup=reply_markup)

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

    def _cmd_ai(self, args: str) -> CommandReply:
        question = str(args or "").strip()
        if not question:
            return self._ai_markdown_reply("🧠 AI 媒体问答", "用法：/ai 推荐一部最近入库的动漫")
        store = _read_store(self.store_path)
        ai_config = apply_ai_env_overrides(store.get("aiConfig"))
        if not bool(ai_config.get("enabled")):
            return self._ai_markdown_reply("🧠 AI 助手未启用", "请先在系统设置里启用并保存 AI 配置。")
        if not str(ai_config.get("apiKey") or "").strip() or not str(ai_config.get("baseUrl") or "").strip():
            return self._ai_markdown_reply("🧠 AI 配置不完整", "请先填写 Base URL、API Key 和模型名称。")

        safety_reply = self._build_ai_safety_reply(question)
        if safety_reply:
            return self._ai_markdown_reply("🧠 AI 安全限制", safety_reply)

        execution_reply = self._build_ai_execution_proposal(question)
        if execution_reply:
            return execution_reply

        messages = self._build_ai_messages(question)
        try:
            started = time.time()
            answer = chat_completion(config=ai_config, messages=messages, timeout_seconds=45)
            elapsed_ms = int((time.time() - started) * 1000)
        except Exception as err:
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
            detail={"model": str(ai_config.get("model") or ""), "elapsedMs": elapsed_ms},
        )
        return self._ai_markdown_reply("🧠 AI 媒体问答", self._truncate_text(answer, 3400))

    def _dispatch_ai_streaming(self, args: str, *, token: str, chat_id: str) -> CommandReply | None:
        question = str(args or "").strip()
        if not question:
            return self._ai_markdown_reply("🧠 AI 媒体问答", "用法：/ai 推荐一部最近入库的动漫")
        store = _read_store(self.store_path)
        ai_config = apply_ai_env_overrides(store.get("aiConfig"))
        if not bool(ai_config.get("enabled")):
            return self._ai_markdown_reply("🧠 AI 助手未启用", "请先在系统设置里启用并保存 AI 配置。")
        if not str(ai_config.get("apiKey") or "").strip() or not str(ai_config.get("baseUrl") or "").strip():
            return self._ai_markdown_reply("🧠 AI 配置不完整", "请先填写 Base URL、API Key 和模型名称。")

        safety_reply = self._build_ai_safety_reply(question)
        if safety_reply:
            return self._ai_markdown_reply("🧠 AI 安全限制", safety_reply)

        execution_reply = self._build_ai_execution_proposal(question)
        if execution_reply:
            return execution_reply

        messages = self._build_ai_messages(question)
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
                answer = chat_completion(config=ai_config, messages=messages, timeout_seconds=45)
                self._edit_ai_markdown_message(
                    token=token,
                    chat_id=chat_id,
                    message_id=message_id,
                    title="🧠 AI 媒体问答",
                    body=self._truncate_text(answer, 3400),
                )
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
            detail={"model": str(ai_config.get("model") or ""), "elapsedMs": elapsed_ms, "streaming": True},
        )
        return None

    def _build_ai_messages(self, question: str) -> list[dict[str, str]]:
        context = self._build_ai_media_context(question=question)
        return [
            {
                "role": "system",
                "content": (
                    "你是镜界 Vistamirror 的媒体库 AI 助手。"
                    "请用简洁中文回答，优先依据提供的媒体库上下文。"
                    "如果上下文里有“当前媒体库统计”或“命中资源详情”，必须直接使用这些准确数字回答；"
                    "如果上下文里包含任务、日志、缺集或播放历史摘要，也要优先引用这些项目资料。"
                    "如果上下文不足，要明确说明不确定，不要编造不存在的片名或数据。"
                    "不要输出任何 Token、API Key、密码或密钥。"
                    "如果用户请求删除、清空、改密钥或改密码，应拒绝并建议到后台手动操作。"
                    "最终回答要适合 Telegram MarkdownV2 代码块展示，内容短段落、列表清晰。"
                    "不要输出 Markdown 包裹语法，由后端统一包装。"
                ),
            },
            {"role": "user", "content": f"媒体库上下文：\n{context}\n\n用户问题：{question}"},
        ]

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
        service = MissingEpisodeService(
            emby_fetcher=self._emby_get,
            tmdb_token=tmdb_token,
            tmdb_language=str(emby.get("tmdbLanguage") or "zh-CN"),
            tmdb_region=str(emby.get("tmdbRegion") or "CN"),
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

    def _build_ai_focus_media_context(self, question: str) -> str:
        keyword = self._extract_ai_media_keyword(question)
        if not keyword:
            return ""
        try:
            items = self._search_emby_items(keyword=keyword, limit=8)
        except Exception as err:
            return f"命中资源详情：搜索“{keyword}”失败（{self._format_emby_error(err)}）。"
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

    def _search_emby_items(self, *, keyword: str, limit: int = 8) -> list[dict[str, Any]]:
        query = urllib.parse.urlencode(
            {
                "Recursive": "true",
                "SearchTerm": keyword,
                "IncludeItemTypes": "Series,Movie,Episode",
                "Fields": "Name,Type,ProductionYear,Overview,Genres,PremiereDate,ChildCount,RecursiveItemCount,ParentIndexNumber,IndexNumber,SeriesName,Status,CommunityRating,CriticRating,RunTimeTicks",
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
                    f"/Items/{urllib.parse.quote(item_id, safe='')}?Fields=Name,Type,ProductionYear,PremiereDate,ChildCount,RecursiveItemCount,ParentIndexNumber,IndexNumber,SeriesName,Status,CommunityRating,CriticRating,Overview"
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
            season_count, episode_count, season_lines, latest_episode, source_note = self._resolve_ai_series_counts(item_id=item_id, detail=joined)
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

    def _resolve_ai_series_counts(self, *, item_id: str, detail: dict[str, Any]) -> tuple[int, int, list[str], str, str]:
        season_meta = self._fetch_ai_series_season_meta(item_id=item_id)
        try:
            episodes_payload = self._emby_get(
                f"/Shows/{urllib.parse.quote(item_id, safe='')}/Episodes?Fields=Name,SeasonId,ParentId,SeriesId,ParentIndexNumber,IndexNumber&Limit=2000"
            )
        except Exception as err:
            LOGGER.warning("TG AI series episodes failed: item_id=%s err=%s", item_id, err)
            season_count, episode_count, season_lines = self._resolve_ai_series_counts_from_seasons(
                detail=detail,
                season_meta=season_meta,
            )
            status = getattr(err, "code", "")
            if str(status) in {"401", "403"}:
                note = "无法读取 Episodes 单集列表，可能是 API Key 权限不足；已回退季字段统计。"
            else:
                note = f"无法读取 Episodes 单集列表（{self._format_emby_error(err)}），已回退季字段统计。"
            return season_count, episode_count, season_lines, "", note

        episodes = episodes_payload.get("Items") if isinstance(episodes_payload, dict) else episodes_payload
        if isinstance(episodes, list) and episodes:
            season_counts: dict[int, int] = {}
            latest: tuple[int, int, str] | None = None
            for row in episodes:
                if not isinstance(row, dict):
                    continue
                season_no = self._coerce_index_number(row.get("ParentIndexNumber")) or 0
                episode_no = self._coerce_index_number(row.get("IndexNumber")) or 0
                season_key = season_no if season_no > 0 else 1
                season_counts[season_key] = int(season_counts.get(season_key) or 0) + 1
                if episode_no > 0:
                    name = str(row.get("Name") or "").strip()
                    current = (season_key, episode_no, name)
                    if latest is None or (current[0], current[1]) > (latest[0], latest[1]):
                        latest = current

            season_lines = []
            for season_no in sorted(season_counts):
                count = int(season_counts.get(season_no) or 0)
                label = season_meta.get(season_no) or f"S{season_no}"
                season_lines.append(f"{label} {count}集")
            episode_count = len([row for row in episodes if isinstance(row, dict)])
            season_count = len(season_counts) if season_counts else max(1, int(detail.get("ChildCount") or 0))
            latest_text = ""
            if latest:
                latest_text = f"S{latest[0]:02d}E{latest[1]:02d}"
                if latest[2]:
                    latest_text = f"{latest_text}「{latest[2]}」"
            return max(0, season_count), max(0, episode_count), season_lines, latest_text, "以 Episodes 实际单集列表为准。"

        season_count, episode_count, season_lines = self._resolve_ai_series_counts_from_seasons(
            detail=detail,
            season_meta=season_meta,
        )
        note = "Episodes 单集列表为空；未把季字段里的 0 当作准确集数。"
        return season_count, episode_count, season_lines, "", note

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
        text = str(question or "").strip()
        if not text:
            return ""
        quoted = re.search(r"[《「“\"]([^》」”\"]{1,80})[》」”\"]", text)
        if quoted:
            return self._clean_ai_keyword(str(quoted.group(1) or ""))

        general_patterns = (
            r"一共.*(多少|几).*(影视|资源|电影|剧集|影片|媒体)",
            r"总共.*(多少|几).*(影视|资源|电影|剧集|影片|媒体)",
            r"现在.*(多少|几).*(影视|资源|电影|剧集|影片|媒体)",
            r"(媒体库|资源库).*(总数|总量|数量|统计)",
        )
        if any(re.search(pattern, text) for pattern in general_patterns):
            return ""
        if self._parse_ai_category_listing_request(text):
            return ""

        value = text
        replacements = [
            r"^/ai\s*",
            r"帮我",
            r"请",
            r"看一下",
            r"查一下",
            r"查查",
            r"搜索",
            r"找一下",
            r"我的",
            r"媒体库里",
            r"媒体库",
            r"资源库里",
            r"资源库",
            r"里面",
            r"现在",
            r"目前",
            r"一共",
            r"总共",
            r"已经",
            r"有多少集了?",
            r"有多少集",
            r"多少集了?",
            r"多少集",
            r"更新到第几集了?",
            r"更新到第几集",
            r"更新到多少集了?",
            r"更新到多少集",
            r"有没有",
            r"是否有",
            r"有吗",
            r"吗",
            r"？",
            r"\?",
        ]
        for pattern in replacements:
            value = re.sub(pattern, "", value, flags=re.IGNORECASE)
        return self._clean_ai_keyword(value)

    @staticmethod
    def _clean_ai_keyword(value: str) -> str:
        clean = str(value or "").strip()
        clean = clean.strip(" ，。！？?：:；;、|/\\[]()（）【】《》「」“”\"'")
        clean = re.sub(r"\s+", " ", clean).strip()
        if len(clean) < 2:
            return ""
        if clean in {"影视资源", "资源", "媒体", "电影", "剧集", "影片", "数量", "总数"}:
            return ""
        return clean[:80]

    def _pick_best_search_item(self, *, items: list[Any], keyword: str) -> dict[str, Any]:
        normalized_keyword = str(keyword or "").strip().lower()
        candidates = [row for row in items if isinstance(row, dict)]
        if not candidates:
            return {}

        def _score(row: dict[str, Any]) -> tuple[int, int, int]:
            row_type = str(row.get("Type") or "").strip().lower()
            name = str(row.get("Name") or "").strip().lower()
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
            for key in ("ItemName", "Name", "FileName", "Filename"):
                value = str(raw.get(key) or "").strip()
                if value:
                    return self._clean_recent_playback_filename(value), False, False

        unified_title = str(row.get("title") or "").strip()
        if unified_title:
            parsed = self._parse_episode_from_text(unified_title)
            if parsed:
                return self._clean_recent_playback_filename(str(parsed.get("episodeTitle") or "")), True, False
            return self._clean_recent_playback_filename(unified_title), False, True

        fallback = str(row.get("mediaName") or "").strip()
        if fallback:
            parsed = self._parse_episode_from_text(fallback)
            if parsed:
                return self._clean_recent_playback_filename(str(parsed.get("episodeTitle") or "")), True, False
            return self._clean_recent_playback_filename(fallback), False, True

        return "未知内容", False, True

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
