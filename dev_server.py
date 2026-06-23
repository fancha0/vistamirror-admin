#!/usr/bin/env python3
"""
Local development server for Emby Pulse UI.

Features:
- Serves static files from current directory.
- Proxies /api/emby/* requests to target Emby server.
- Avoids browser CORS issues by using same-origin proxy.
- Provides invite registration APIs and invite landing page routing.
"""

from __future__ import annotations

import argparse
import base64
import concurrent.futures
import atexit
from functools import partial
import hashlib
import html
import ipaddress
import io
import json
import os
import pathlib
import re
import secrets
import socket
import ssl
import struct
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from backend_modules.ai_assistant import (
    apply_ai_env_overrides,
    chat_completion,
    default_ai_config as module_default_ai_config,
    env_managed_ai_fields,
    normalize_ai_config as module_normalize_ai_config,
    validate_ai_config as module_validate_ai_config,
)
from backend_modules.drive115_service import (
    Drive115Service,
    apply_drive115_env_overrides,
    default_drive115_config as module_default_drive115_config,
    drive115_qrcode_clients,
    env_managed_drive115_fields,
    extract_115_share,
    admin_drive115_config,
    merge_drive115_config_for_save,
    normalize_drive115_config as module_normalize_drive115_config,
    public_drive115_config,
    redact_drive115_payload,
)
from backend_modules.hdhive_service import (
    HDHiveError,
    HDHiveService,
    apply_hdhive_env_overrides,
    default_hdhive_config as module_default_hdhive_config,
    env_managed_hdhive_fields,
    merge_hdhive_config_for_save,
    normalize_hdhive_config as module_normalize_hdhive_config,
    public_hdhive_config,
)
from backend_modules.ip_locator import build_ip_display
from backend_modules.media_identity_service import MediaIdentityService
from backend_modules.message_formatter import compose_playback_message, normalize_content_type, ticks_to_seconds
from backend_modules.missing_episode_service import MissingEpisodeService
from backend_modules.admin_auth_service import AdminAuthService, AuthConfig
from backend_modules.notification_config import (
    default_bot_config as module_default_bot_config,
    normalize_bot_config as module_normalize_bot_config,
    validate_bot_config as module_validate_bot_config,
)
from backend_modules.playback_event_logger import append_playback_event
from backend_modules.playback_history_service import PlaybackHistoryService
from backend_modules.project_event_logger import append_project_event, clear_project_events, read_project_events, redact_sensitive
from backend_modules.telegram_commands import TelegramCommandService
from backend_modules.telegram_sender import TelegramSender
from backend_modules.webhook_receiver import build_dedupe_key, detect_playback_action, event_enabled, maybe_extract_media_name

try:
    from Crypto.Cipher import AES as _AES  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    _AES = None


PASS_HEADERS = {
    "Content-Type",
    "X-Emby-Client",
    "X-Emby-Device-Name",
    "X-Emby-Device-Id",
    "X-Emby-Client-Version",
}

INVITE_PAGE_PATH_PATTERN = re.compile(r"^/invite(?:/.*)?$")
INVITE_CODE_PAGE_PATTERN = re.compile(r"^/invite/([^/]+)$")
INVITE_API_GET_PATTERN = re.compile(r"^/api/invite/([^/]+)$")
INVITE_API_REGISTER_PATTERN = re.compile(r"^/api/invite/([^/]+)/register$")
TELEGRAM_CHAT_ID_PATTERN = re.compile(r"^-?\d+$")

STORE_LOCK = threading.Lock()
LAST_WEBHOOK_LOCK = threading.Lock()
ANNUAL_RANKING_CACHE_LOCK = threading.Lock()
STORE_FILE_NAME = "invites.json"
LEGACY_STORE_FILE_NAME = "invite_store.json"
BASE_DIR = pathlib.Path(__file__).resolve().parent
DEFAULT_EMBY_CLIENT_NAME = "镜界Vistamirror User Console"
RUNTIME_DIR = pathlib.Path(str(os.environ.get("APP_RUNTIME_DIR") or (BASE_DIR / "runtime"))).expanduser()
DATA_DIR = pathlib.Path(str(os.environ.get("APP_DATA_DIR") or (BASE_DIR / "data"))).expanduser()
PLAYBACK_EVENT_LOG_FILE = DATA_DIR / "playback_events.jsonl"
PROJECT_EVENT_LOG_FILE = DATA_DIR / "project_events.jsonl"
PROJECT_EVENT_STATE_FILE = DATA_DIR / ".project_events_state.json"
MISSING_SCAN_CACHE_FILE = DATA_DIR / "missing_scan.json"
DEFAULT_WEBHOOK_TOKEN = "vistamirror"
LAST_WEBHOOK_STATE: dict[str, Any] = {"lastReceivedAt": "", "lastProcessed": None}
RECENT_WEBHOOK_EVENTS: dict[str, float] = {}
ANNUAL_RANKING_MEMORY_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
ANNUAL_RANKING_REDIS_URL = str(os.environ.get("REDIS_URL") or "").strip()
ANNUAL_RANKING_CACHE_TTL_SECONDS = max(
    60,
    min(300, int(str(os.environ.get("RANKING_CACHE_TTL_SECONDS") or "180").strip() or "180")),
)
TELEGRAM_SENDER = TelegramSender()
TELEGRAM_COMMAND_SERVICE: TelegramCommandService | None = None
ADMIN_AUTH_SERVICE: AdminAuthService | None = None
DRIVE115_QRCODE_LOCK = threading.Lock()
DRIVE115_QRCODE_SESSIONS: dict[str, dict[str, Any]] = {}
HDHIVE_OAUTH_LOCK = threading.Lock()
HDHIVE_OAUTH_STATES: dict[str, dict[str, Any]] = {}
HDHIVE_CHECKIN_STOP = threading.Event()

EMBY_ENV_FIELD_MAP: dict[str, str] = {
    "serverUrl": "APP_EMBY_SERVER_URL",
    "apiKey": "APP_EMBY_API_KEY",
    "clientName": "APP_EMBY_CLIENT_NAME",
    "tmdbToken": "APP_TMDB_TOKEN",
}
BOT_ENV_FIELD_MAP: dict[str, str] = {
    "telegramToken": "APP_BOT_TELEGRAM_TOKEN",
    "telegramChatId": "APP_BOT_TELEGRAM_CHAT_ID",
}
ADMIN_AUTH_ENV_FIELD_MAP: dict[str, str] = {
    "username": "APP_ADMIN_USERNAME",
    "password": "APP_ADMIN_PASSWORD",
    "passwordHash": "APP_ADMIN_PASSWORD_HASH",
}

PLAYBACK_EVENT_KEYWORDS = (
    "playback",
    "sessionstart",
    "sessionend",
    "play",
    "pause",
    "resume",
    "stop",
    "播放",
)
LIBRARY_EVENT_KEYWORDS = (
    "library",
    "itemadded",
    "newitem",
    "newitems",
    "scanfinished",
    "scancompleted",
    "新增",
    "入库",
    "媒体库",
)


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _env_bool(name: str, default: bool = False) -> bool:
    raw = str(os.environ.get(name) or "").strip().lower()
    if not raw:
        return bool(default)
    return raw in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw = str(os.environ.get(name) or "").strip()
    if not raw:
        return int(default)
    try:
        return int(raw)
    except Exception:
        return int(default)


def _write_project_event(
    *,
    level: str = "info",
    module: str = "system",
    action: str = "",
    message: str = "",
    user_id: str = "",
    ip: str = "",
    request_path: str = "",
    status: int | str = "",
    detail: Any = None,
) -> None:
    try:
        append_project_event(
            PROJECT_EVENT_LOG_FILE,
            level=level,
            module=module,
            action=action,
            message=message,
            user_id=user_id,
            ip=ip,
            request_path=request_path,
            status=status,
            detail=detail,
        )
    except Exception as err:  # pragma: no cover
        print(f"[project_log] write failed: {err}")


def _record_service_start(host: str, port: int) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if PROJECT_EVENT_STATE_FILE.exists():
        try:
            previous = json.loads(PROJECT_EVENT_STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            previous = {}
        if isinstance(previous, dict) and previous.get("state") == "running":
            _write_project_event(
                level="warning",
                module="docker",
                action="previous_exit_unclean",
                message="检测到上次服务未正常停止，可能发生过异常退出或容器重启。",
                detail=previous,
            )
    state = {"state": "running", "pid": os.getpid(), "startedAt": _now_iso(), "host": host, "port": port}
    PROJECT_EVENT_STATE_FILE.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")
    _write_project_event(
        level="info",
        module="docker",
        action="service_started",
        message=f"服务已启动：http://{host}:{port}",
        status=200,
        detail={"pid": os.getpid(), "host": host, "port": port},
    )


def _record_service_stop(reason: str = "shutdown") -> None:
    if not PROJECT_EVENT_STATE_FILE.exists():
        return
    _write_project_event(
        level="info",
        module="docker",
        action="service_stopped",
        message="服务已正常停止。",
        detail={"pid": os.getpid(), "reason": reason},
    )
    try:
        PROJECT_EVENT_STATE_FILE.unlink(missing_ok=True)
    except OSError:
        pass


def _mark_webhook_received() -> None:
    with LAST_WEBHOOK_LOCK:
        LAST_WEBHOOK_STATE["lastReceivedAt"] = _now_iso()


def _set_last_webhook_processed(*, event_type: str, result: str, detail: str = "") -> dict[str, str]:
    record = {
        "at": _now_iso(),
        "eventType": str(event_type or "").strip() or "unknown",
        "result": str(result or "").strip() or "unknown",
        "detail": str(detail or "").strip()[:320],
    }
    with LAST_WEBHOOK_LOCK:
        LAST_WEBHOOK_STATE["lastProcessed"] = record
    return record


def _build_webhook_status_payload() -> dict[str, Any]:
    with LAST_WEBHOOK_LOCK:
        last_received_at = str(LAST_WEBHOOK_STATE.get("lastReceivedAt") or "").strip()
        last_processed_raw = LAST_WEBHOOK_STATE.get("lastProcessed")
        last_processed = dict(last_processed_raw) if isinstance(last_processed_raw, dict) else None
    return {
        "lastReceivedAt": last_received_at or None,
        "lastProcessed": last_processed,
        # backward compatibility for current UI logic
        "lastWebhook": last_processed,
    }


def _json_clone(payload: dict[str, Any]) -> dict[str, Any]:
    return json.loads(json.dumps(payload, ensure_ascii=False))


def _redis_pack_command(parts: tuple[str, ...]) -> bytes:
    chunks: list[bytes] = [f"*{len(parts)}\r\n".encode("utf-8")]
    for part in parts:
        raw = str(part).encode("utf-8")
        chunks.append(f"${len(raw)}\r\n".encode("utf-8"))
        chunks.append(raw)
        chunks.append(b"\r\n")
    return b"".join(chunks)


def _redis_read_exact(sock: socket.socket, length: int) -> bytes:
    chunks: list[bytes] = []
    remaining = length
    while remaining > 0:
        chunk = sock.recv(remaining)
        if not chunk:
            raise ConnectionError("Redis connection closed")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _redis_read_line(sock: socket.socket) -> bytes:
    chunks: list[bytes] = []
    while True:
        char = sock.recv(1)
        if not char:
            raise ConnectionError("Redis connection closed")
        if char == b"\r":
            lf = sock.recv(1)
            if lf != b"\n":
                raise ConnectionError("Invalid Redis response")
            return b"".join(chunks)
        chunks.append(char)


def _redis_read_response(sock: socket.socket) -> Any:
    prefix = sock.recv(1)
    if not prefix:
        raise ConnectionError("Redis connection closed")
    if prefix == b"+":
        return _redis_read_line(sock).decode("utf-8", errors="replace")
    if prefix == b"-":
        raise RuntimeError(_redis_read_line(sock).decode("utf-8", errors="replace"))
    if prefix == b":":
        return int(_redis_read_line(sock).decode("ascii", errors="replace") or "0")
    if prefix == b"$":
        length = int(_redis_read_line(sock).decode("ascii", errors="replace") or "-1")
        if length < 0:
            return None
        data = _redis_read_exact(sock, length)
        _redis_read_exact(sock, 2)
        return data.decode("utf-8", errors="replace")
    if prefix == b"*":
        length = int(_redis_read_line(sock).decode("ascii", errors="replace") or "0")
        return [_redis_read_response(sock) for _ in range(max(0, length))]
    raise ConnectionError("Unsupported Redis response")


def _redis_command(*parts: str) -> Any:
    if not ANNUAL_RANKING_REDIS_URL:
        return None
    parsed = urllib.parse.urlsplit(ANNUAL_RANKING_REDIS_URL)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or (6380 if parsed.scheme == "rediss" else 6379)
    password = urllib.parse.unquote(parsed.password or "")
    db_raw = parsed.path.strip("/")
    db_index = int(db_raw or "0") if db_raw.isdigit() else 0

    raw_sock = socket.create_connection((host, port), timeout=1.2)
    sock: socket.socket
    if parsed.scheme == "rediss":
        sock = ssl.create_default_context().wrap_socket(raw_sock, server_hostname=host)
    else:
        sock = raw_sock
    with sock:
        if password:
            sock.sendall(_redis_pack_command(("AUTH", password)))
            _redis_read_response(sock)
        if db_index:
            sock.sendall(_redis_pack_command(("SELECT", str(db_index))))
            _redis_read_response(sock)
        sock.sendall(_redis_pack_command(tuple(str(part) for part in parts)))
        return _redis_read_response(sock)


def _annual_cache_memory_get(cache_key: str) -> dict[str, Any] | None:
    now = time.time()
    with ANNUAL_RANKING_CACHE_LOCK:
        entry = ANNUAL_RANKING_MEMORY_CACHE.get(cache_key)
        if not entry:
            return None
        expires_at, payload = entry
        if expires_at <= now:
            ANNUAL_RANKING_MEMORY_CACHE.pop(cache_key, None)
            return None
        cached = _json_clone(payload)
    cached["cached"] = True
    cached["cacheStore"] = "memory"
    return cached


def _annual_cache_memory_set(cache_key: str, payload: dict[str, Any], ttl_seconds: int) -> None:
    expires_at = time.time() + max(1, ttl_seconds)
    with ANNUAL_RANKING_CACHE_LOCK:
        ANNUAL_RANKING_MEMORY_CACHE[cache_key] = (expires_at, _json_clone(payload))


def _annual_cache_get(cache_key: str) -> dict[str, Any] | None:
    redis_key = f"vistamirror:annual-ranking:{cache_key}"
    if ANNUAL_RANKING_REDIS_URL:
        try:
            raw = _redis_command("GET", redis_key)
            if raw:
                cached = json.loads(str(raw))
                if isinstance(cached, dict):
                    cached["cached"] = True
                    cached["cacheStore"] = "redis"
                    return cached
        except Exception:
            pass
    return _annual_cache_memory_get(cache_key)


def _annual_cache_set(cache_key: str, payload: dict[str, Any], ttl_seconds: int) -> None:
    redis_key = f"vistamirror:annual-ranking:{cache_key}"
    stored = _json_clone(payload)
    stored["cached"] = False
    if ANNUAL_RANKING_REDIS_URL:
        try:
            _redis_command("SETEX", redis_key, str(max(1, ttl_seconds)), json.dumps(stored, ensure_ascii=False))
        except Exception:
            pass
    _annual_cache_memory_set(cache_key, stored, ttl_seconds)


def _normalize_invite_code(code: str) -> str:
    return str(code or "").strip().lower()


def _parse_expiry(value: str) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _effective_invite_status(invite: dict[str, Any]) -> str:
    raw_status = str(invite.get("status") or "active").lower()
    if raw_status in {"used", "已用"}:
        return "used"
    if raw_status not in {"active", "空闲", "idle", "free"}:
        return "invalid"

    expiry = _parse_expiry(str(invite.get("expiresAt") or ""))
    if expiry and expiry < date.today():
        return "expired"
    return "active"


def _default_bot_config() -> dict[str, Any]:
    return module_default_bot_config()


def _default_ai_config() -> dict[str, Any]:
    return module_default_ai_config()


def _normalize_bot_config(raw: Any) -> dict[str, Any]:
    return module_normalize_bot_config(raw)


def _normalize_ai_config(raw: Any) -> dict[str, Any]:
    return module_normalize_ai_config(raw)


def _validate_bot_config(raw: Any) -> tuple[dict[str, Any] | None, str | None]:
    return module_validate_bot_config(raw)


def _validate_ai_config(raw: Any) -> tuple[dict[str, Any] | None, str | None]:
    return module_validate_ai_config(raw)


def _env_override_value(env_name: str) -> str:
    return str(os.environ.get(env_name) or "").strip()


def _env_managed_emby_fields() -> list[str]:
    managed: list[str] = []
    for field, env_name in EMBY_ENV_FIELD_MAP.items():
        if _env_override_value(env_name):
            managed.append(field)
    return managed


def _env_managed_bot_fields() -> list[str]:
    managed: list[str] = []
    for field, env_name in BOT_ENV_FIELD_MAP.items():
        if _env_override_value(env_name):
            managed.append(field)
    return managed


def _env_managed_ai_fields() -> list[str]:
    return env_managed_ai_fields()


def _env_managed_drive115_fields() -> list[str]:
    return env_managed_drive115_fields()


def _default_hdhive_config() -> dict[str, Any]:
    return module_default_hdhive_config()


def _normalize_hdhive_config(raw: Any) -> dict[str, Any]:
    return module_normalize_hdhive_config(raw)


def _apply_hdhive_env_overrides(raw: Any) -> dict[str, Any]:
    return apply_hdhive_env_overrides(raw)


def _env_managed_hdhive_fields() -> list[str]:
    return env_managed_hdhive_fields()


def _save_hdhive_background_config(config: dict[str, Any]) -> None:
    with STORE_LOCK:
        store = _read_store_unlocked()
        stored = _normalize_hdhive_config(config)
        current = _normalize_hdhive_config(store.get("hdhiveConfig"))
        for field in _env_managed_hdhive_fields():
            stored[field] = current.get(field)
        store["hdhiveConfig"] = stored
        _write_store_unlocked(store)


def _hdhive_direct_checkin_loop() -> None:
    while not HDHIVE_CHECKIN_STOP.wait(60):
        try:
            with STORE_LOCK:
                store = _read_store_unlocked()
                config = _apply_hdhive_env_overrides(store.get("hdhiveConfig"))
            if not config.get("enabled") or str(config.get("authMode") or "") != "direct" or not config.get("autoCheckin"):
                continue
            if not config.get("accessToken") or "write" not in str(config.get("scopes") or "").split():
                continue
            try:
                from zoneinfo import ZoneInfo
                date_key = datetime.now(ZoneInfo(str(config.get("timezone") or "Asia/Shanghai"))).date().isoformat()
            except Exception:
                date_key = datetime.now(timezone.utc).date().isoformat()
            if str(config.get("lastCheckinDate") or "") == date_key:
                continue
            service = HDHiveService(config, save_config=_save_hdhive_background_config)
            result = service.checkin()
            _write_project_event(
                level="info", module="hdhive", action="hdhive_auto_checkin",
                message="影巢每日普通签到已执行。",
                detail={"checkedIn": bool(result.get("checked_in")), "points": result.get("points")},
            )
        except Exception as err:
            _write_project_event(
                level="warning", module="hdhive", action="hdhive_auto_checkin_failed",
                message="影巢每日签到失败。", detail={"error": str(err)},
            )


def _env_managed_admin_auth_fields() -> list[str]:
    managed: list[str] = []
    for field, env_name in ADMIN_AUTH_ENV_FIELD_MAP.items():
        if _env_override_value(env_name):
            managed.append(field)
    return managed


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
        env_value = _env_override_value(env_name)
        if env_value:
            merged[field] = env_value
    legacy_tmdb_token = _env_override_value("TMDB_TOKEN")
    if legacy_tmdb_token and not _env_override_value("APP_TMDB_TOKEN"):
        merged["tmdbToken"] = legacy_tmdb_token
    if merged["tmdbToken"] and (legacy_tmdb_token or _env_override_value("APP_TMDB_TOKEN")):
        merged["tmdbEnabled"] = True
    return merged


def _merge_emby_config_for_save(current: Any, submitted: Any) -> dict[str, Any]:
    existing = current if isinstance(current, dict) else {}
    incoming = submitted if isinstance(submitted, dict) else {}
    submitted_tmdb_token = str(incoming.get("tmdbToken") or "").strip()
    return {
        "serverUrl": str(incoming.get("serverUrl") or "").strip(),
        "apiKey": str(incoming.get("apiKey") or "").strip(),
        "clientName": str(incoming.get("clientName") or "").strip(),
        "tmdbEnabled": bool(incoming.get("tmdbEnabled")),
        "tmdbToken": submitted_tmdb_token or str(existing.get("tmdbToken") or "").strip(),
        "tmdbLanguage": str(incoming.get("tmdbLanguage") or existing.get("tmdbLanguage") or "zh-CN").strip() or "zh-CN",
        "tmdbRegion": str(incoming.get("tmdbRegion") or existing.get("tmdbRegion") or "CN").strip().upper() or "CN",
        "updatedAt": _now_iso(),
    }


def _apply_bot_env_overrides(raw: Any) -> dict[str, Any]:
    source = _normalize_bot_config(raw)
    merged = dict(source)
    for field, env_name in BOT_ENV_FIELD_MAP.items():
        env_value = _env_override_value(env_name)
        if env_value:
            merged[field] = env_value
    return _normalize_bot_config(merged)


def _apply_ai_env_overrides(raw: Any) -> dict[str, Any]:
    return apply_ai_env_overrides(raw)


def _default_drive115_config() -> dict[str, Any]:
    return module_default_drive115_config()


def _normalize_drive115_config(raw: Any) -> dict[str, Any]:
    return module_normalize_drive115_config(raw)


def _apply_drive115_env_overrides(raw: Any) -> dict[str, Any]:
    return apply_drive115_env_overrides(raw)


def _env_controlled_fields_payload() -> dict[str, list[str]]:
    return {
        "embyConfig": _env_managed_emby_fields(),
        "botConfig": _env_managed_bot_fields(),
        "aiConfig": _env_managed_ai_fields(),
        "drive115Config": _env_managed_drive115_fields(),
        "hdhiveConfig": _env_managed_hdhive_fields(),
        "adminAuth": _env_managed_admin_auth_fields(),
    }


def _extract_json_error_message(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return "请求失败"
    try:
        payload = json.loads(text)
    except Exception:
        return text[:240]
    if isinstance(payload, dict):
        for key in ("description", "error", "message", "detail"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return text[:240]


def _is_local_or_private_host(host: str) -> bool:
    text = str(host or "").strip().lower()
    if not text:
        return True
    if text in {"localhost", "127.0.0.1", "::1", "0.0.0.0", "::"}:
        return True
    try:
        ip = ipaddress.ip_address(text)
        return bool(ip.is_private or ip.is_loopback or ip.is_link_local)
    except ValueError:
        pass
    if text.endswith(".in-addr.arpa") or text.endswith(".ip6.arpa"):
        return True
    if text.endswith(".local"):
        return True
    return False


def _parse_origin_from_url(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    parsed = urllib.parse.urlsplit(raw)
    if parsed.scheme in {"http", "https"} and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}".rstrip("/")
    return ""


def _extract_host_from_origin(origin: str) -> str:
    parsed = urllib.parse.urlsplit(str(origin or "").strip())
    return str(parsed.hostname or "").strip().lower()


def _store_path() -> pathlib.Path:
    current = DATA_DIR / STORE_FILE_NAME
    legacy_current = BASE_DIR / STORE_FILE_NAME
    legacy = BASE_DIR / LEGACY_STORE_FILE_NAME
    if current.exists():
        return current
    current.parent.mkdir(parents=True, exist_ok=True)
    if legacy_current.exists():
        try:
            os.replace(legacy_current, current)
            return current
        except OSError:
            return legacy_current
    if legacy.exists():
        try:
            os.replace(legacy, current)
            return current
        except OSError:
            return legacy
    return current


def _read_store_unlocked() -> dict[str, Any]:
    path = _store_path()
    if not path.exists():
        return {
            "embyConfig": {},
            "invites": [],
            "botConfig": _default_bot_config(),
            "aiConfig": _default_ai_config(),
            "drive115Config": _default_drive115_config(),
            "hdhiveConfig": _default_hdhive_config(),
        }

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {
            "embyConfig": {},
            "invites": [],
            "botConfig": _default_bot_config(),
            "aiConfig": _default_ai_config(),
            "drive115Config": _default_drive115_config(),
            "hdhiveConfig": _default_hdhive_config(),
        }

    emby_config = data.get("embyConfig") if isinstance(data, dict) else {}
    invites = data.get("invites") if isinstance(data, dict) else []
    bot_config = data.get("botConfig") if isinstance(data, dict) else {}
    ai_config = data.get("aiConfig") if isinstance(data, dict) else {}
    drive115_config = data.get("drive115Config") if isinstance(data, dict) else {}
    hdhive_config = data.get("hdhiveConfig") if isinstance(data, dict) else {}
    if not isinstance(emby_config, dict):
        emby_config = {}
    if not isinstance(invites, list):
        invites = []
    return {
        "embyConfig": emby_config,
        "invites": invites,
        "botConfig": _normalize_bot_config(bot_config),
        "aiConfig": _normalize_ai_config(ai_config),
        "drive115Config": _normalize_drive115_config(drive115_config),
        "hdhiveConfig": _normalize_hdhive_config(hdhive_config),
    }


def _write_store_unlocked(store: dict[str, Any]) -> None:
    path = _store_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    payload = json.dumps(store, ensure_ascii=False, indent=2)
    tmp.write_text(payload, encoding="utf-8")
    os.replace(tmp, path)


def _sanitize_invite_record(raw: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None

    code = str(raw.get("code") or "").strip()
    if not code:
        return None

    status = str(raw.get("status") or "active").strip().lower()
    if status in {"已用", "used"}:
        status = "已用"
    elif status in {"active", "空闲", "idle", "free"}:
        status = "空闲"
    else:
        status = "空闲"

    expires_at = str(raw.get("expiresAt") or "").strip()
    if expires_at and _parse_expiry(expires_at) is None:
        expires_at = ""

    initial_days_raw = raw.get("initialDays")
    if initial_days_raw in (None, "", "permanent"):
        initial_days_raw = raw.get("duration")
    initial_days: int | None
    if initial_days_raw in (None, "", "permanent"):
        initial_days = None
    else:
        try:
            parsed_days = int(initial_days_raw)
            initial_days = parsed_days if parsed_days > 0 else None
        except (TypeError, ValueError):
            initial_days = None

    return {
        "id": str(raw.get("id") or ""),
        "code": code,
        "label": str(raw.get("label") or "").strip(),
        "username": str(raw.get("username") or "").strip(),
        "plan": str(raw.get("plan") or "").strip(),
        "initialDays": initial_days,
        "duration": initial_days,
        "expiresAt": expires_at,
        "status": status,
        "createdAt": str(raw.get("createdAt") or _now_iso()),
        "usedAt": str(raw.get("usedAt") or "").strip(),
        "createdUserId": str(raw.get("createdUserId") or "").strip(),
        "usedUsername": str(raw.get("usedUsername") or "").strip(),
    }


def _merge_invites(existing: list[dict[str, Any]], incoming: list[dict[str, Any]]) -> list[dict[str, Any]]:
    existing_map: dict[str, dict[str, Any]] = {}
    for invite in existing:
        code_key = _normalize_invite_code(invite.get("code"))
        if code_key:
            existing_map[code_key] = invite

    merged_map: dict[str, dict[str, Any]] = {}
    for invite in incoming:
        code_key = _normalize_invite_code(invite.get("code"))
        if not code_key:
            continue

        prev = existing_map.get(code_key)
        if prev and _effective_invite_status(prev) == "used":
            invite["status"] = "已用"
            invite["usedAt"] = str(prev.get("usedAt") or _now_iso())
            invite["createdUserId"] = str(prev.get("createdUserId") or invite.get("createdUserId") or "")
            invite["usedUsername"] = str(prev.get("usedUsername") or invite.get("usedUsername") or "")
        merged_map[code_key] = invite

    for code_key, prev in existing_map.items():
        if code_key not in merged_map:
            merged_map[code_key] = prev

    return list(merged_map.values())


def _parse_positive_int(raw: Any) -> int | None:
    if raw in (None, "", "permanent"):
        return None
    try:
        parsed = int(raw)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _resolve_invite_days(payload: dict[str, Any]) -> int | None:
    if payload.get("permanent") is True:
        return None
    for key in ("duration", "days", "initialDays"):
        parsed = _parse_positive_int(payload.get(key))
        if parsed is not None:
            return parsed
    return 7


def _generate_invite_code(existing_codes: set[str]) -> str:
    alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
    for _ in range(80):
        code = "".join(secrets.choice(alphabet) for _ in range(6))
        if code not in existing_codes:
            return code
    while True:
        code = secrets.token_hex(4)[:8].lower()
        if code not in existing_codes:
            return code


def _public_emby_server_hint(emby_config: dict[str, Any] | None = None) -> dict[str, str] | None:
    raw = str(os.environ.get("EMBY_PUBLIC_WEB_URL") or "").strip().rstrip("/")
    if not raw and emby_config:
        raw = str(emby_config.get("serverUrl") or "").strip().rstrip("/")
    if not raw:
        return None

    if raw.lower().endswith("/emby"):
        raw = raw[:-5].rstrip("/")
    web_url = raw if re.match(r"^https?://", raw, re.IGNORECASE) else f"https://{raw}"
    parsed = urllib.parse.urlsplit(web_url)
    host = parsed.hostname or ""
    if not host:
        return None

    try:
        parsed_port = parsed.port
    except ValueError:
        return None

    if parsed_port:
        port = str(parsed_port)
    elif parsed.scheme.lower() == "http":
        port = "80"
    else:
        port = "443"

    return {
        "webUrl": web_url,
        "host": host,
        "port": port,
        "label": "极速主线路",
    }


class AppHandler(SimpleHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def __init__(self, *args, **kwargs):
        runtime_dir = RUNTIME_DIR.resolve()
        runtime_dir.mkdir(parents=True, exist_ok=True)
        super().__init__(*args, directory=str(runtime_dir), **kwargs)

    def do_GET(self) -> None:
        parsed = urllib.parse.urlsplit(self.path)
        path = parsed.path
        if not self._enforce_admin_auth(method="GET", path=path):
            return

        if path == "/api/auth/me":
            self._handle_auth_me()
            return
        if path == "/api/auth/admin-credential-meta":
            self._handle_auth_admin_credential_meta()
            return

        if path == "/api/bot/config":
            self._handle_bot_config_get()
            return
        if path == "/api/ai/config":
            self._handle_ai_config_get()
            return
        if path == "/api/drive115/config":
            self._handle_drive115_config_get()
            return
        if path == "/api/drive115/qrcode/status":
            self._handle_drive115_qrcode_status(parsed.query)
            return
        if path == "/api/hdhive/config":
            self._handle_hdhive_config_get()
            return
        if path == "/api/hdhive/oauth/start":
            self._handle_hdhive_oauth_start()
            return
        if path == "/api/hdhive/oauth/status":
            self._handle_hdhive_oauth_status(parsed.query)
            return
        if path == "/api/hdhive/oauth/callback":
            self._handle_hdhive_oauth_callback(parsed.query)
            return
        if path == "/api/bot/webhook-url":
            self._handle_bot_webhook_url_get()
            return
        if path == "/api/bot/webhook-status":
            self._handle_bot_webhook_status_get()
            return
        if path == "/api/logs":
            self._handle_project_logs_query(parsed.query)
            return
        if path == "/api/logs/download":
            self._handle_project_logs_download()
            return
        if path.startswith("/api/bot/wecom_webhook"):
            self._handle_wecom_verify()
            return
        if path == "/api/ranking/annual":
            self._handle_annual_ranking(parsed.query)
            return
        if path == "/api/playback/history":
            self._handle_playback_history_query(parsed.query)
            return
        if path == "/api/missing/list":
            self._handle_missing_list_query(parsed.query)
            return
        if path.startswith("/api/emby"):
            self._proxy_emby()
            return
        if path == "/api/invite/sync-status":
            self._handle_invite_sync_status()
            return
        if path == "/api/invite/list":
            self._handle_invite_list()
            return
        if path.startswith("/api/invite/"):
            self._handle_invite_query(path)
            return
        if INVITE_PAGE_PATH_PATTERN.match(path):
            self._handle_invite_register_page(path, parsed.query)
            return
        super().do_GET()

    def do_POST(self) -> None:
        parsed = urllib.parse.urlsplit(self.path)
        path = parsed.path
        if not self._enforce_admin_auth(method="POST", path=path):
            return

        if path == "/api/auth/login":
            self._handle_auth_login()
            return
        if path == "/api/auth/logout":
            self._handle_auth_logout()
            return
        if path == "/api/auth/admin-credentials":
            self._handle_auth_admin_credentials_update()
            return

        if path == "/api/bot/config":
            self._handle_bot_config_save()
            return
        if path == "/api/ai/config":
            self._handle_ai_config_save()
            return
        if path == "/api/ai/test":
            self._handle_ai_config_test()
            return
        if path == "/api/tmdb/test":
            self._handle_tmdb_test()
            return
        if path == "/api/drive115/config":
            self._handle_drive115_config_save()
            return
        if path == "/api/drive115/test":
            self._handle_drive115_test()
            return
        if path == "/api/drive115/parse":
            self._handle_drive115_parse()
            return
        if path == "/api/drive115/transfer":
            self._handle_drive115_transfer()
            return
        if path == "/api/drive115/qrcode/start":
            self._handle_drive115_qrcode_start()
            return
        if path == "/api/drive115/qrcode/stop":
            self._handle_drive115_qrcode_stop()
            return
        if path == "/api/hdhive/config":
            self._handle_hdhive_config_save()
            return
        if path == "/api/hdhive/test":
            self._handle_hdhive_test()
            return
        if path == "/api/hdhive/oauth/disconnect":
            self._handle_hdhive_disconnect()
            return
        if path == "/api/hdhive/checkin":
            self._handle_hdhive_checkin()
            return
        if path == "/api/hdhive/search":
            self._handle_hdhive_search()
            return
        if path == "/api/hdhive/transfer":
            self._handle_hdhive_transfer()
            return
        if path == "/api/bot/test":
            self._handle_bot_test()
            return
        if path == "/api/logs/client":
            self._handle_project_logs_client()
            return
        if path == "/api/v1/webhook":
            self._handle_emby_webhook(parsed.query)
            return
        if path.startswith("/api/bot/wecom_webhook"):
            self._handle_wecom_event()
            return
        if path.startswith("/api/emby"):
            self._proxy_emby()
            return
        if path == "/api/register":
            self._handle_register_api()
            return
        if path == "/api/missing/scan":
            self._handle_missing_scan()
            return
        if path == "/api/invite/generate":
            self._handle_invite_generate()
            return
        if path == "/api/invite/sync":
            self._handle_invite_sync()
            return
        register_match = INVITE_API_REGISTER_PATTERN.match(path)
        if register_match:
            code = urllib.parse.unquote(register_match.group(1))
            self._handle_invite_register(code)
            return
        self.send_error(405, "Method Not Allowed")

    def do_PUT(self) -> None:
        parsed = urllib.parse.urlsplit(self.path)
        path = parsed.path
        if not self._enforce_admin_auth(method="PUT", path=path):
            return
        if self.path.startswith("/api/emby"):
            self._proxy_emby()
            return
        self.send_error(405, "Method Not Allowed")

    def do_DELETE(self) -> None:
        parsed = urllib.parse.urlsplit(self.path)
        if not self._enforce_admin_auth(method="DELETE", path=parsed.path):
            return
        if parsed.path == "/api/logs":
            self._handle_project_logs_clear()
            return
        if self.path.startswith("/api/emby"):
            self._proxy_emby()
            return
        self.send_error(405, "Method Not Allowed")

    def _client_ip(self) -> str:
        forwarded = str(self.headers.get("X-Forwarded-For") or "").split(",", 1)[0].strip()
        if forwarded:
            return forwarded
        return str(self.client_address[0] if self.client_address else "")

    def _is_https_request(self) -> bool:
        forwarded_proto = str(self.headers.get("X-Forwarded-Proto") or "").strip().lower()
        if forwarded_proto:
            return forwarded_proto == "https"
        return bool(getattr(self.connection, "cipher", None))

    def _is_static_path(self, path: str) -> bool:
        lower = str(path or "").strip().lower()
        if not lower:
            return False
        if lower in {"/", "/index.html"}:
            return True
        if lower.startswith("/assets/"):
            return True
        if lower.startswith("/runtime/"):
            return True
        static_ext = (
            ".css",
            ".js",
            ".png",
            ".jpg",
            ".jpeg",
            ".webp",
            ".svg",
            ".ico",
            ".map",
            ".woff",
            ".woff2",
            ".ttf",
            ".otf",
            ".json",
            ".txt",
        )
        return lower.endswith(static_ext)

    def _is_auth_exempt(self, *, method: str, path: str) -> bool:
        verb = str(method or "GET").upper()
        normalized = str(path or "").strip()
        if not normalized.startswith("/api/"):
            return True

        if normalized in {"/api/auth/me", "/api/auth/login", "/api/auth/logout"}:
            return True
        if verb == "GET" and INVITE_API_GET_PATTERN.match(normalized):
            return True
        if verb == "POST" and (INVITE_API_REGISTER_PATTERN.match(normalized) or normalized == "/api/register"):
            return True
        if normalized == "/api/v1/webhook":
            return True
        if normalized.startswith("/api/bot/wecom_webhook"):
            return True
        return False

    def _enforce_admin_auth(self, *, method: str, path: str) -> bool:
        service = ADMIN_AUTH_SERVICE
        if service is None or not service.enabled:
            return True
        if self._is_auth_exempt(method=method, path=path):
            return True

        authorized, username = service.auth_me(
            cookie_header=str(self.headers.get("Cookie") or ""),
            client_ip=self._client_ip(),
        )
        if authorized:
            return True

        self._log_event(
            level="warning",
            module="auth",
            action="admin_auth_required",
            message="后台接口访问被拦截：未登录或会话过期。",
            user_id=username,
            status=401,
            detail={"method": method, "path": path},
        )
        self._send_json(401, {"ok": False, "error": "unauthorized", "authEnabled": True})
        return False

    def _log_event(
        self,
        *,
        level: str = "info",
        module: str = "system",
        action: str = "",
        message: str = "",
        user_id: str = "",
        status: int | str = "",
        detail: Any = None,
    ) -> None:
        _write_project_event(
            level=level,
            module=module,
            action=action,
            message=message,
            user_id=user_id,
            ip=self._client_ip(),
            request_path=urllib.parse.urlsplit(self.path).path,
            status=status,
            detail=detail,
        )

    def _handle_project_logs_query(self, raw_query: str) -> None:
        params = urllib.parse.parse_qs(raw_query or "")
        level = str((params.get("level") or [""])[0]).strip()
        module = str((params.get("module") or [""])[0]).strip()
        keyword = str((params.get("q") or [""])[0]).strip()
        try:
            limit = int(str((params.get("limit") or ["200"])[0]).strip() or "200")
        except ValueError:
            limit = 200
        events, total = read_project_events(
            PROJECT_EVENT_LOG_FILE,
            level=level,
            module=module,
            keyword=keyword,
            limit=limit,
        )
        self._send_json(200, {"ok": True, "events": events, "total": total, "returned": len(events)})

    def _handle_playback_history_query(self, raw_query: str) -> None:
        params = urllib.parse.parse_qs(raw_query or "")
        try:
            limit = int(str((params.get("limit") or ["300"])[0]).strip() or "300")
        except Exception:
            limit = 300
        try:
            scan_limit = int(str((params.get("scanLimit") or ["2000"])[0]).strip() or "2000")
        except Exception:
            scan_limit = 2000
        keyword = str((params.get("q") or [""])[0]).strip()
        username = str((params.get("user") or [""])[0]).strip()

        with STORE_LOCK:
            store = _read_store_unlocked()
            emby_config = _apply_emby_env_overrides(store.get("embyConfig"))
        server_url = str(emby_config.get("serverUrl") or "").strip()
        api_key = str(emby_config.get("apiKey") or "").strip()
        if not server_url or not api_key:
            self._log_event(
                level="warning",
                module="playback",
                action="playback_history_api_failed",
                message="读取播放历史失败：Emby 配置缺失。",
                status=400,
                detail={"limit": limit, "scanLimit": scan_limit},
            )
            self._send_json(400, {"ok": False, "error": "请先在系统设置里配置 Emby 地址和 API Key。", "rows": [], "debug": {}})
            return
        if not server_url.lower().endswith("/emby"):
            server_url = f"{server_url.rstrip('/')}/emby"

        service = PlaybackHistoryService(
            fetcher=lambda path: self._emby_request(base_url=server_url, api_key=api_key, path=path, method="GET")
        )
        try:
            result = service.collect(limit=limit, scan_limit=scan_limit, keyword=keyword, username=username)
        except Exception as err:
            self._log_event(
                level="error",
                module="playback",
                action="playback_history_api_failed",
                message="读取播放历史失败：Emby 活动日志请求异常。",
                status=502,
                detail={"error": str(err)[:300], "limit": limit, "scanLimit": scan_limit},
            )
            self._send_json(502, {"ok": False, "error": f"读取播放历史失败：{err}", "rows": [], "debug": {}})
            return

        rows = result.get("rows") if isinstance(result, dict) else []
        debug = result.get("debug") if isinstance(result, dict) else {}
        warning = str(result.get("warning") or "").strip() if isinstance(result, dict) else ""
        self._log_event(
            level="warning" if warning else "info",
            module="playback",
            action="playback_history_api_collected",
            message=warning or "播放历史统一接口已返回结果。",
            status=200,
            detail={
                "rows": len(rows) if isinstance(rows, list) else 0,
                "debug": debug if isinstance(debug, dict) else {},
                "query": {"q": keyword, "user": username, "limit": limit, "scanLimit": scan_limit},
            },
        )
        self._send_json(
            200,
            {
                "ok": True,
                "rows": rows if isinstance(rows, list) else [],
                "debug": debug if isinstance(debug, dict) else {},
                "warning": warning,
            },
        )

    def _handle_missing_scan(self) -> None:
        payload = self._read_json_body()
        if payload is None:
            return

        with STORE_LOCK:
            store = _read_store_unlocked()
            emby_config = _apply_emby_env_overrides(store.get("embyConfig"))
        server_url = str(emby_config.get("serverUrl") or "").strip()
        api_key = str(emby_config.get("apiKey") or "").strip()
        if not server_url or not api_key:
            self._send_json(400, {"ok": False, "error": "请先在系统设置里配置 Emby 地址和 API Key。"})
            return
        if not server_url.lower().endswith("/emby"):
            server_url = f"{server_url.rstrip('/')}/emby"

        tmdb_token = str(
            payload.get("tmdbToken")
            or os.environ.get("APP_TMDB_TOKEN")
            or os.environ.get("TMDB_TOKEN")
            or ""
        ).strip()
        tmdb_language = str(payload.get("tmdbLanguage") or "zh-CN").strip() or "zh-CN"
        tmdb_region = str(payload.get("tmdbRegion") or "CN").strip().upper() or "CN"
        try:
            scan_limit = int(str(payload.get("scanLimit") or "1200").strip() or "1200")
        except Exception:
            scan_limit = 1200

        if not tmdb_token:
            self._send_json(400, {"ok": False, "error": "缺少 TMDB Token，请先在系统设置里填写并保存。"})
            return

        self._log_event(
            level="info",
            module="system",
            action="missing_scan_started",
            message="缺集巡检已开始。",
            status=200,
            detail={"scanLimit": scan_limit},
        )
        service = MissingEpisodeService(
            emby_fetcher=lambda path: self._emby_request(base_url=server_url, api_key=api_key, path=path, method="GET"),
            tmdb_token=tmdb_token,
            tmdb_language=tmdb_language,
            tmdb_region=tmdb_region,
        )
        try:
            result = service.scan(scan_limit=scan_limit)
        except urllib.error.HTTPError as err:
            raw_detail = err.read().decode("utf-8", errors="replace")
            detail = {"status": err.code, "error": raw_detail[:300], "scanLimit": scan_limit}
            self._log_event(
                level="error",
                module="system",
                action="missing_scan_failed",
                message="缺集巡检失败：上游请求返回异常。",
                status=502,
                detail=detail,
            )
            self._send_json(502, {"ok": False, "error": "缺集巡检失败：上游请求异常。", "detail": detail})
            return
        except Exception as err:
            detail = {"error": str(err)[:300], "scanLimit": scan_limit}
            self._log_event(
                level="error",
                module="system",
                action="missing_scan_failed",
                message="缺集巡检失败：服务异常。",
                status=502,
                detail=detail,
            )
            self._send_json(502, {"ok": False, "error": "缺集巡检失败，请检查 Emby/TMDB 配置。", "detail": detail})
            return

        output = {
            "ok": True,
            "summary": result.get("summary") if isinstance(result, dict) else {},
            "rows": result.get("rows") if isinstance(result, dict) else [],
            "warnings": result.get("warnings") if isinstance(result, dict) else [],
            "debug": result.get("debug") if isinstance(result, dict) else {},
        }
        try:
            MISSING_SCAN_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
            MISSING_SCAN_CACHE_FILE.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            # 缓存写入失败不阻断接口返回
            pass

        summary = output.get("summary") if isinstance(output, dict) else {}
        self._log_event(
            level="info",
            module="system",
            action="missing_scan_completed",
            message="缺集巡检已完成。",
            status=200,
            detail={
                "scannedSeries": summary.get("scannedSeries"),
                "matchedTmdbSeries": summary.get("matchedTmdbSeries"),
                "missingSeries": summary.get("missingSeries"),
                "missingEpisodeCount": summary.get("missingEpisodeCount"),
                "unknownMatchCount": summary.get("unknownMatchCount"),
            },
        )
        self._send_json(200, output)

    def _handle_missing_list_query(self, raw_query: str) -> None:
        params = urllib.parse.parse_qs(raw_query or "")
        keyword = str((params.get("q") or [""])[0]).strip().lower()
        series_filter = str((params.get("series") or [""])[0]).strip().lower()
        status_filter = str((params.get("status") or [""])[0]).strip().lower()
        season_filter_raw = str((params.get("season") or [""])[0]).strip()
        try:
            limit = int(str((params.get("limit") or ["500"])[0]).strip() or "500")
        except Exception:
            limit = 500
        limit = max(1, min(5000, limit))

        payload = self._read_missing_scan_cache()
        rows = payload.get("rows") if isinstance(payload, dict) else []
        if not isinstance(rows, list):
            rows = []
        filtered: list[dict[str, Any]] = []
        season_filter = 0
        if season_filter_raw:
            try:
                season_filter = int(season_filter_raw)
            except Exception:
                season_filter = 0

        for row in rows:
            if not isinstance(row, dict):
                continue
            status = str(row.get("status") or "").strip().lower()
            series_name = str(row.get("seriesName") or "").strip()
            season_no = int(row.get("seasonNo") or 0)
            missing_episodes = row.get("missingEpisodes")
            missing_text = ",".join(str(item) for item in missing_episodes) if isinstance(missing_episodes, list) else ""
            reason_text = str(row.get("reason") or "").strip()
            if status_filter and status_filter != "all" and status != status_filter:
                continue
            if series_filter and series_filter not in series_name.lower():
                continue
            if season_filter and season_no != season_filter:
                continue
            if keyword:
                haystack = " ".join(
                    [
                        series_name,
                        str(row.get("tmdbId") or ""),
                        str(row.get("embySeriesId") or ""),
                        str(row.get("completeness") or ""),
                        missing_text,
                        reason_text,
                    ]
                ).lower()
                if keyword not in haystack:
                    continue
            filtered.append(row)

        summary = payload.get("summary") if isinstance(payload, dict) and isinstance(payload.get("summary"), dict) else {}
        warnings = payload.get("warnings") if isinstance(payload, dict) and isinstance(payload.get("warnings"), list) else []
        debug = payload.get("debug") if isinstance(payload, dict) and isinstance(payload.get("debug"), dict) else {}
        self._send_json(
            200,
            {
                "ok": True,
                "summary": summary,
                "rows": filtered[:limit],
                "total": len(filtered),
                "warnings": warnings[:200],
                "debug": debug,
            },
        )

    def _read_missing_scan_cache(self) -> dict[str, Any]:
        if not MISSING_SCAN_CACHE_FILE.exists():
            return {
                "summary": {
                    "scannedSeries": 0,
                    "matchedTmdbSeries": 0,
                    "missingSeries": 0,
                    "missingEpisodeCount": 0,
                    "unknownMatchCount": 0,
                    "scannedAt": "",
                },
                "rows": [],
                "warnings": [],
                "debug": {},
            }
        try:
            payload = json.loads(MISSING_SCAN_CACHE_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {
                "summary": {
                    "scannedSeries": 0,
                    "matchedTmdbSeries": 0,
                    "missingSeries": 0,
                    "missingEpisodeCount": 0,
                    "unknownMatchCount": 0,
                    "scannedAt": "",
                },
                "rows": [],
                "warnings": ["上次缺集巡检缓存损坏，已忽略。"],
                "debug": {},
            }
        if not isinstance(payload, dict):
            return {"summary": {}, "rows": [], "warnings": [], "debug": {}}
        return payload

    def _handle_project_logs_download(self) -> None:
        PROJECT_EVENT_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        if PROJECT_EVENT_LOG_FILE.exists():
            content = PROJECT_EVENT_LOG_FILE.read_bytes()
        else:
            content = b""
        filename = f"project-events-{datetime.now().strftime('%Y%m%d-%H%M%S')}.jsonl"
        self.send_response(200)
        self.send_header("Content-Type", "application/x-ndjson; charset=utf-8")
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        if content:
            self.wfile.write(content)

    def _handle_project_logs_clear(self) -> None:
        clear_project_events(PROJECT_EVENT_LOG_FILE)
        self._log_event(
            level="warning",
            module="system",
            action="logs_cleared",
            message="管理员清空了项目日志。",
            status=200,
        )
        self._send_json(200, {"ok": True, "message": "日志已清空"})

    def _handle_project_logs_client(self) -> None:
        payload = self._read_json_body()
        if payload is None:
            return
        level = str(payload.get("level") or "info")
        module = str(payload.get("module") or "system")
        action = str(payload.get("action") or "")
        message = str(payload.get("message") or "")
        user_id = str(payload.get("user_id") or payload.get("userId") or "")
        status = payload.get("status") if payload.get("status") is not None else ""
        detail = payload.get("detail") if isinstance(payload, dict) else {}
        self._log_event(
            level=level,
            module=module,
            action=action,
            message=message,
            user_id=user_id,
            status=status,
            detail=detail,
        )
        self._send_json(200, {"ok": True})

    def _handle_auth_me(self) -> None:
        service = ADMIN_AUTH_SERVICE
        if service is None or not service.enabled:
            self._send_json(200, {"ok": True, "authEnabled": False, "user": {"name": "admin"}})
            return

        authorized, username = service.auth_me(
            cookie_header=str(self.headers.get("Cookie") or ""),
            client_ip=self._client_ip(),
        )
        if authorized:
            self._send_json(200, {"ok": True, "authEnabled": True, "user": {"name": username}})
            return
        self._send_json(401, {"ok": False, "error": "unauthorized", "authEnabled": True})

    def _handle_auth_login(self) -> None:
        service = ADMIN_AUTH_SERVICE
        if service is None:
            self._send_json(500, {"ok": False, "error": "auth service unavailable"})
            return

        payload = self._read_json_body()
        if payload is None:
            return
        username = str(payload.get("username") or "").strip()
        password = str(payload.get("password") or "")
        remember_raw = payload.get("rememberMe")
        if isinstance(remember_raw, bool):
            remember_me = remember_raw
        else:
            remember_me = str(remember_raw or "").strip().lower() in {"1", "true", "yes", "on"}
        result = service.login(
            username=username,
            password=password,
            client_ip=self._client_ip(),
            user_agent=str(self.headers.get("User-Agent") or ""),
            remember_me=remember_me,
        )

        if result.get("ok"):
            sid = str(result.get("sid") or "").strip()
            if not sid:
                self._send_json(500, {"ok": False, "error": "session create failed"})
                return
            cookie_value = service.build_set_cookie(
                sid=sid,
                secure=self._is_https_request(),
                remember_me=bool(result.get("rememberMe")),
            )
            safe_username = str(result.get("username") or username or "admin")
            self._log_event(
                level="info",
                module="auth",
                action="admin_login_success",
                message="管理员登录成功。",
                user_id=safe_username,
                status=200,
                detail={"rememberMe": bool(result.get("rememberMe"))},
            )
            self._send_json(
                200,
                {
                    "ok": True,
                    "authEnabled": service.enabled,
                    "user": {"name": safe_username},
                    "rememberMe": bool(result.get("rememberMe")),
                },
                extra_headers={"Set-Cookie": cookie_value},
            )
            return

        status = int(result.get("status") or 401)
        locked = bool(result.get("locked"))
        error_text = str(result.get("error") or "账号或密码错误。")
        detail = {
            "username": username,
            "retryAfter": result.get("retryAfter"),
            "remaining": result.get("remaining"),
            "locked": locked,
        }
        self._log_event(
            level="warning",
            module="auth",
            action="admin_login_locked" if locked else "admin_login_failed",
            message=error_text,
            user_id=username,
            status=status,
            detail=detail,
        )
        self._send_json(
            status,
            {
                "ok": False,
                "authEnabled": service.enabled,
                "error": error_text,
                "retryAfter": result.get("retryAfter"),
                "remaining": result.get("remaining"),
                "locked": locked,
            },
        )

    def _handle_auth_logout(self) -> None:
        service = ADMIN_AUTH_SERVICE
        if service is None:
            self._send_json(500, {"ok": False, "error": "auth service unavailable"})
            return
        authorized, username = service.auth_me(
            cookie_header=str(self.headers.get("Cookie") or ""),
            client_ip=self._client_ip(),
        )
        service.logout(cookie_header=str(self.headers.get("Cookie") or ""))
        clear_cookie = service.build_clear_cookie(secure=self._is_https_request())
        self._log_event(
            level="info",
            module="auth",
            action="admin_logout",
            message="管理员已退出登录。",
            user_id=username if authorized else "",
            status=200,
        )
        self._send_json(
            200,
            {"ok": True, "authEnabled": bool(service.enabled)},
            extra_headers={"Set-Cookie": clear_cookie},
        )

    def _handle_auth_admin_credential_meta(self) -> None:
        service = ADMIN_AUTH_SERVICE
        if service is None:
            self._send_json(500, {"ok": False, "error": "auth service unavailable"})
            return
        meta = service.get_admin_credential_meta()
        self._send_json(
            200,
            {
                "ok": True,
                "authEnabled": bool(meta.get("authEnabled")),
                "username": str(meta.get("username") or ""),
                "managedByEnv": bool(meta.get("managedByEnv")),
                "allowUpdate": bool(meta.get("allowUpdate")),
                "source": str(meta.get("source") or "missing"),
                "envControlledFields": _env_controlled_fields_payload(),
            },
        )

    def _handle_auth_admin_credentials_update(self) -> None:
        service = ADMIN_AUTH_SERVICE
        if service is None:
            self._send_json(500, {"ok": False, "error": "auth service unavailable"})
            return
        payload = self._read_json_body()
        if payload is None:
            return
        current_password = str(payload.get("currentPassword") or "")
        next_username = str(payload.get("nextUsername") or "").strip()
        next_password = str(payload.get("nextPassword") or "")
        if not current_password:
            self._send_json(400, {"ok": False, "error": "请输入当前密码。"})
            return
        if not next_username:
            self._send_json(400, {"ok": False, "error": "请输入新用户名。"})
            return
        if not next_password:
            self._send_json(400, {"ok": False, "error": "请输入新密码。"})
            return

        result = service.update_admin_credentials(
            current_password=current_password,
            next_username=next_username,
            next_password=next_password,
        )
        ok = bool(result.get("ok"))
        status = int(result.get("status") or (200 if ok else 400))
        message = str(result.get("error") or ("管理员凭据已更新。" if ok else "管理员凭据更新失败。"))
        managed_by_env = bool(result.get("managedByEnv"))
        if ok:
            self._log_event(
                level="info",
                module="auth",
                action="admin_credentials_update_success",
                message="管理员凭据已更新，已强制所有会话退出。",
                user_id=str(result.get("username") or ""),
                status=200,
                detail={"forcedLogout": bool(result.get("forcedLogout")), "managedByEnv": managed_by_env},
            )
            clear_cookie = service.build_clear_cookie(secure=self._is_https_request())
            self._send_json(
                200,
                {
                    "ok": True,
                    "message": "管理员凭据已更新，请重新登录。",
                    "forcedLogout": bool(result.get("forcedLogout")),
                    "username": str(result.get("username") or ""),
                },
                extra_headers={"Set-Cookie": clear_cookie},
            )
            return

        self._log_event(
            level="warning" if status < 500 else "error",
            module="auth",
            action="admin_credentials_env_locked" if managed_by_env else "admin_credentials_update_failed",
            message=message,
            status=status,
            detail={"managedByEnv": managed_by_env},
        )
        self._send_json(
            status,
            {
                "ok": False,
                "error": message,
                "managedByEnv": managed_by_env,
                "allowUpdate": False if managed_by_env else None,
            },
        )

    def _handle_wecom_verify(self) -> None:
        parsed = urllib.parse.urlsplit(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        msg_signature = str((params.get("msg_signature") or [""])[0]).strip()
        timestamp = str((params.get("timestamp") or [""])[0]).strip()
        nonce = str((params.get("nonce") or [""])[0]).strip()
        echostr = str((params.get("echostr") or [""])[0]).strip()
        required_params = ["msg_signature", "timestamp", "nonce", "echostr"]
        has_verify_params = bool(msg_signature and timestamp and nonce and echostr)

        if not has_verify_params:
            self._send_json(
                200,
                {
                    "ok": True,
                    "message": "企业微信回调接口已启动",
                    "path": "/api/bot/wecom_webhook",
                    "need_params": required_params,
                },
            )
            return

        with STORE_LOCK:
            store = _read_store_unlocked()
            bot_config = _apply_bot_env_overrides(store.get("botConfig"))

        callback_token = str(bot_config.get("wechatCallbackToken") or "").strip()
        callback_aes = str(bot_config.get("wechatCallbackAes") or "").strip()
        corp_id = str(bot_config.get("wechatCorpId") or "").strip()
        if not callback_token or not callback_aes:
            self._log_wecom_failure(
                "verify_config_missing",
                "企业微信回调安全配置缺失",
                config=bot_config,
            )
            self._send_json(
                400,
                {
                    "ok": False,
                    "error": "企业微信回调安全配置不完整，请先填写回调 Token 和 EncodingAESKey",
                },
            )
            return

        if not self._wecom_check_signature(
            signature=msg_signature,
            token=callback_token,
            timestamp=timestamp,
            nonce=nonce,
            encrypted=echostr,
        ):
            self._log_wecom_failure(
                "verify_signature_failed",
                "企业微信 URL 验证签名不通过",
                config=bot_config,
            )
            self._send_json(
                403,
                {
                    "ok": False,
                    "error": "企业微信回调验证失败：msg_signature 校验不通过",
                },
            )
            return

        try:
            plain_echostr, receive_id = self._wecom_decrypt_payload(echostr, callback_aes)
        except Exception as err:
            self._log_wecom_failure(
                "verify_decrypt_failed",
                f"企业微信 echostr 解密失败：{err}",
                config=bot_config,
            )
            self._send_json(
                400,
                {
                    "ok": False,
                    "error": f"企业微信回调验证失败：echostr 解密失败（{err}）",
                },
            )
            return

        if corp_id and receive_id and receive_id != corp_id:
            self._log_wecom_failure(
                "verify_receive_id_mismatch",
                "企业微信 receiveid 与 corpId 不一致",
                config=bot_config,
            )
            self._send_json(
                400,
                {
                    "ok": False,
                    "error": "企业微信回调验证失败：receiveid 与 corpId 不匹配",
                },
            )
            return

        self._send_text(200, plain_echostr)

    def _handle_wecom_event(self) -> None:
        content_length = self.headers.get("Content-Length")
        if content_length:
            try:
                length = int(content_length)
            except ValueError:
                length = 0
            if length > 0:
                _ = self.rfile.read(length)

        # 企业微信回调约定返回 success
        self._send_text(200, "success")

    def _wecom_check_signature(
        self,
        *,
        signature: str,
        token: str,
        timestamp: str,
        nonce: str,
        encrypted: str,
    ) -> bool:
        candidates = [str(token or ""), str(timestamp or ""), str(nonce or ""), str(encrypted or "")]
        digest = hashlib.sha1("".join(sorted(candidates)).encode("utf-8")).hexdigest()
        return digest == str(signature or "").strip()

    def _wecom_decrypt_payload(self, encrypted: str, aes_key: str) -> tuple[str, str]:
        if _AES is None:
            raise RuntimeError("服务端缺少 Crypto 库，请安装 pycryptodome")

        key_text = str(aes_key or "").strip()
        if not key_text:
            raise ValueError("EncodingAESKey 为空")
        try:
            key = base64.b64decode(f"{key_text}=", validate=False)
        except Exception as err:
            raise ValueError(f"EncodingAESKey 非法：{err}") from None
        if len(key) != 32:
            raise ValueError("EncodingAESKey 长度不正确，应为 43 位")

        try:
            encrypted_bytes = base64.b64decode(str(encrypted or ""), validate=False)
        except Exception as err:
            raise ValueError(f"echostr Base64 解析失败：{err}") from None
        if not encrypted_bytes:
            raise ValueError("echostr 为空")

        cipher = _AES.new(key, _AES.MODE_CBC, key[:16])
        plain = cipher.decrypt(encrypted_bytes)
        if not plain:
            raise ValueError("echostr 解密后为空")

        pad = plain[-1]
        if pad < 1 or pad > 32:
            raise ValueError("echostr 填充位无效")
        plain = plain[:-pad]
        if len(plain) < 20:
            raise ValueError("echostr 解密结果长度异常")

        msg_len = struct.unpack(">I", plain[16:20])[0]
        body = plain[20 : 20 + msg_len]
        receive_id_raw = plain[20 + msg_len :]
        if len(body) != msg_len:
            raise ValueError("echostr 消息体长度异常")

        text = body.decode("utf-8", errors="replace")
        receive_id = receive_id_raw.decode("utf-8", errors="replace")
        return text, receive_id

    def _mask_secret(self, value: str, *, left: int = 3, right: int = 2) -> str:
        raw = str(value or "").strip()
        if not raw:
            return ""
        if len(raw) <= left + right:
            return "*" * len(raw)
        return f"{raw[:left]}***{raw[-right:]}"

    def _log_wecom_failure(self, code: str, message: str, *, config: dict[str, Any] | None = None) -> None:
        conf = config if isinstance(config, dict) else {}
        safe = {
            "wechatCorpId": self._mask_secret(str(conf.get("wechatCorpId") or "")),
            "wechatCallbackToken": self._mask_secret(str(conf.get("wechatCallbackToken") or "")),
            "wechatCallbackAes": self._mask_secret(str(conf.get("wechatCallbackAes") or ""), left=4, right=4),
            "wechatAgentId": self._mask_secret(str(conf.get("wechatAgentId") or "")),
        }
        self._log_event(
            level="warning",
            module="webhook",
            action=f"wecom_{code}",
            message=message,
            detail=safe,
        )
        print(f"[wecom_webhook] {code}: {message}; config={safe}")

    def _handle_bot_config_get(self) -> None:
        with STORE_LOCK:
            store = _read_store_unlocked()
            path = _store_path()
            needs_persist = True
            if path.exists():
                try:
                    raw = json.loads(path.read_text(encoding="utf-8"))
                    needs_persist = not isinstance(raw, dict) or not isinstance(raw.get("botConfig"), dict)
                except Exception:
                    needs_persist = True
            if needs_persist:
                _write_store_unlocked(store)

            bot_config = _apply_bot_env_overrides(store.get("botConfig"))

        self._send_json(
            200,
            {
                "ok": True,
                "botConfig": bot_config,
                "envControlledFields": _env_controlled_fields_payload(),
                "managedByEnv": _env_controlled_fields_payload(),
            },
        )

    def _handle_bot_config_save(self) -> None:
        payload = self._read_json_body()
        if payload is None:
            return

        raw_bot_config = payload.get("botConfig")
        if raw_bot_config is None:
            raw_bot_config = payload

        bot_config, error = _validate_bot_config(raw_bot_config)
        if error:
            self._send_json(400, {"error": error})
            return
        if bot_config is None:
            self._send_json(400, {"error": "机器人配置无效"})
            return

        with STORE_LOCK:
            store = _read_store_unlocked()
            current = _normalize_bot_config(store.get("botConfig"))
            locked = _env_managed_bot_fields()
            if locked:
                for field in locked:
                    bot_config[field] = current.get(field)
            store["botConfig"] = _normalize_bot_config(bot_config)
            _write_store_unlocked(store)
            saved_config = _apply_bot_env_overrides(store.get("botConfig"))

        if TELEGRAM_COMMAND_SERVICE is not None:
            TELEGRAM_COMMAND_SERVICE.wakeup()

        self._log_event(
            level="info",
            module="system",
            action="bot_config_saved",
            message="机器人配置已保存。",
            status=200,
            detail={"changedFields": sorted(redact_sensitive(raw_bot_config).keys()) if isinstance(raw_bot_config, dict) else []},
        )

        self._send_json(
            200,
            {
                "ok": True,
                "botConfig": saved_config,
                "envControlledFields": _env_controlled_fields_payload(),
                "managedByEnv": _env_controlled_fields_payload(),
            },
        )

    def _handle_ai_config_get(self) -> None:
        with STORE_LOCK:
            store = _read_store_unlocked()
            path = _store_path()
            needs_persist = True
            if path.exists():
                try:
                    raw = json.loads(path.read_text(encoding="utf-8"))
                    needs_persist = not isinstance(raw, dict) or not isinstance(raw.get("aiConfig"), dict)
                except Exception:
                    needs_persist = True
            if needs_persist:
                _write_store_unlocked(store)
            ai_config = _apply_ai_env_overrides(store.get("aiConfig"))

        self._send_json(
            200,
            {
                "ok": True,
                "aiConfig": ai_config,
                "envControlledFields": _env_controlled_fields_payload(),
                "managedByEnv": _env_controlled_fields_payload(),
            },
        )

    def _handle_ai_config_save(self) -> None:
        payload = self._read_json_body()
        if payload is None:
            return

        raw_ai_config = payload.get("aiConfig")
        if raw_ai_config is None:
            raw_ai_config = payload

        if not isinstance(raw_ai_config, dict):
            self._send_json(400, {"error": "AI 配置必须是对象"})
            return

        with STORE_LOCK:
            current_store = _read_store_unlocked()
            current_config = _apply_ai_env_overrides(current_store.get("aiConfig"))
            locked = _env_managed_ai_fields()
        if locked:
            raw_ai_config = dict(raw_ai_config)
            for field in locked:
                raw_ai_config[field] = current_config.get(field)

        ai_config, error = _validate_ai_config(raw_ai_config)
        if error:
            self._send_json(400, {"error": error})
            return
        if ai_config is None:
            self._send_json(400, {"error": "AI 配置无效"})
            return

        with STORE_LOCK:
            store = _read_store_unlocked()
            current = _normalize_ai_config(store.get("aiConfig"))
            locked = _env_managed_ai_fields()
            if locked:
                for field in locked:
                    ai_config[field] = current.get(field)
            store["aiConfig"] = _normalize_ai_config(ai_config)
            _write_store_unlocked(store)
            saved_config = _apply_ai_env_overrides(store.get("aiConfig"))

        if TELEGRAM_COMMAND_SERVICE is not None:
            TELEGRAM_COMMAND_SERVICE.wakeup()

        self._log_event(
            level="info",
            module="system",
            action="ai_config_saved",
            message="AI 助手配置已保存。",
            status=200,
            detail={"changedFields": sorted(redact_sensitive(raw_ai_config).keys()) if isinstance(raw_ai_config, dict) else []},
        )

        self._send_json(
            200,
            {
                "ok": True,
                "aiConfig": saved_config,
                "envControlledFields": _env_controlled_fields_payload(),
                "managedByEnv": _env_controlled_fields_payload(),
            },
        )

    def _handle_ai_config_test(self) -> None:
        payload = self._read_json_body()
        if payload is None:
            return
        raw_ai_config = payload.get("aiConfig") if isinstance(payload, dict) else payload
        ai_config, error = _validate_ai_config(raw_ai_config)
        if error:
            self._send_json(400, {"ok": False, "error": error})
            return
        if ai_config is None:
            self._send_json(400, {"ok": False, "error": "AI 配置无效"})
            return
        try:
            started = time.time()
            answer = chat_completion(
                config=ai_config,
                messages=[
                    {"role": "system", "content": "你是连接测试助手。只回复“连接成功”。"},
                    {"role": "user", "content": "测试连接"},
                ],
                timeout_seconds=20,
            )
            elapsed_ms = int((time.time() - started) * 1000)
        except Exception as err:
            self._log_event(
                level="warning",
                module="system",
                action="ai_config_test_failed",
                message="AI 助手连接测试失败。",
                detail={"model": str(ai_config.get("model") or ""), "error": str(err)},
            )
            self._send_json(502, {"ok": False, "error": str(err)})
            return

        self._log_event(
            level="info",
            module="system",
            action="ai_config_test_success",
            message="AI 助手连接测试成功。",
            status=200,
            detail={"model": str(ai_config.get("model") or ""), "elapsedMs": elapsed_ms},
        )
        self._send_json(
            200,
            {"ok": True, "message": "AI 连接测试成功", "sample": self._shorten(answer, limit=80), "elapsedMs": elapsed_ms},
        )

    def _handle_tmdb_test(self) -> None:
        payload = self._read_json_body()
        if payload is None:
            return

        with STORE_LOCK:
            store = _read_store_unlocked()
            saved_config = _apply_emby_env_overrides(store.get("embyConfig"))

        submitted = payload.get("tmdbConfig") if isinstance(payload, dict) else {}
        submitted = submitted if isinstance(submitted, dict) else {}
        token = str(submitted.get("tmdbToken") or saved_config.get("tmdbToken") or "").strip()
        language = str(submitted.get("tmdbLanguage") or saved_config.get("tmdbLanguage") or "zh-CN").strip() or "zh-CN"
        if not token:
            self._send_json(400, {"ok": False, "status": "not_configured", "error": "TMDB Token 尚未保存。"})
            return

        request = urllib.request.Request(
            "https://api.themoviedb.org/3/configuration",
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "Accept-Language": language,
                "User-Agent": "Vistamirror/1.0",
            },
        )
        started = time.time()
        try:
            with urllib.request.urlopen(request, timeout=12) as response:
                result = json.loads(response.read().decode("utf-8", errors="replace"))
            if not isinstance(result, dict) or not isinstance(result.get("images"), dict):
                raise RuntimeError("TMDB 返回了无法识别的响应。")
        except urllib.error.HTTPError as err:
            status = "invalid_token" if err.code in {401, 403} else "upstream_error"
            message = "TMDB Token 无效或无权访问。" if status == "invalid_token" else f"TMDB 返回 HTTP {err.code}。"
            self._log_event(
                level="warning",
                module="system",
                action="tmdb_test_failed",
                message="TMDB 连接测试失败。",
                detail={"status": status, "httpStatus": err.code},
            )
            self._send_json(401 if status == "invalid_token" else 502, {"ok": False, "status": status, "error": message})
            return
        except (urllib.error.URLError, TimeoutError, OSError) as err:
            self._log_event(
                level="warning",
                module="system",
                action="tmdb_test_failed",
                message="TMDB 网络连接失败。",
                detail={"status": "network_error", "errorType": type(err).__name__},
            )
            self._send_json(502, {"ok": False, "status": "network_error", "error": "无法连接 TMDB，请检查网络或代理设置。"})
            return
        except Exception as err:
            self._log_event(
                level="warning",
                module="system",
                action="tmdb_test_failed",
                message="TMDB 连接测试失败。",
                detail={"status": "invalid_response", "errorType": type(err).__name__},
            )
            self._send_json(502, {"ok": False, "status": "invalid_response", "error": str(err)})
            return

        elapsed_ms = int((time.time() - started) * 1000)
        self._log_event(
            level="info",
            module="system",
            action="tmdb_test_success",
            message="TMDB 连接测试成功。",
            status=200,
            detail={"elapsedMs": elapsed_ms},
        )
        self._send_json(200, {"ok": True, "status": "connected", "message": "TMDB 连接正常，Token 已生效。", "elapsedMs": elapsed_ms})

    def _handle_drive115_config_get(self) -> None:
        with STORE_LOCK:
            store = _read_store_unlocked()
            path = _store_path()
            needs_persist = True
            if path.exists():
                try:
                    raw = json.loads(path.read_text(encoding="utf-8"))
                    needs_persist = not isinstance(raw, dict) or not isinstance(raw.get("drive115Config"), dict)
                except Exception:
                    needs_persist = True
            if needs_persist:
                _write_store_unlocked(store)
            config = _apply_drive115_env_overrides(store.get("drive115Config"))

        self._send_json(
            200,
            {
                "ok": True,
                "drive115Config": admin_drive115_config(config),
                "envControlledFields": _env_controlled_fields_payload(),
                "managedByEnv": _env_controlled_fields_payload(),
            },
        )

    def _handle_drive115_config_save(self) -> None:
        payload = self._read_json_body()
        if payload is None:
            return
        raw_config = payload.get("drive115Config") if isinstance(payload, dict) else payload
        if not isinstance(raw_config, dict):
            self._send_json(400, {"ok": False, "error": "115 配置必须是对象"})
            return
        with STORE_LOCK:
            store = _read_store_unlocked()
            current = _normalize_drive115_config(store.get("drive115Config"))
            locked = _env_managed_drive115_fields()
            next_config = merge_drive115_config_for_save(current, raw_config)
            for field in locked:
                next_config[field] = current.get(field)
            store["drive115Config"] = _normalize_drive115_config(next_config)
            _write_store_unlocked(store)
            saved = _apply_drive115_env_overrides(store.get("drive115Config"))

        if TELEGRAM_COMMAND_SERVICE is not None:
            TELEGRAM_COMMAND_SERVICE.wakeup()

        self._log_event(
            level="info",
            module="drive115",
            action="drive115_config_saved",
            message="115 网盘配置已保存。",
            status=200,
            detail=redact_drive115_payload({"enabled": raw_config.get("enabled"), "defaultCid": raw_config.get("defaultCid")}),
        )
        self._send_json(
            200,
            {
                "ok": True,
                "drive115Config": admin_drive115_config(saved),
                "envControlledFields": _env_controlled_fields_payload(),
                "managedByEnv": _env_controlled_fields_payload(),
            },
        )

    def _drive115_service_from_store(self, override_config: dict[str, Any] | None = None) -> Drive115Service:
        with STORE_LOCK:
            store = _read_store_unlocked()
            base = _apply_drive115_env_overrides(store.get("drive115Config"))
        if isinstance(override_config, dict):
            merged = dict(base)
            for key, value in override_config.items():
                if key in {"enabled", "cookie", "defaultCid"}:
                    merged[key] = value
            base = _apply_drive115_env_overrides(merged)
        return Drive115Service(base)

    def _handle_drive115_test(self) -> None:
        payload = self._read_json_body()
        if payload is None:
            return
        raw_config = payload.get("drive115Config") if isinstance(payload, dict) else {}
        try:
            result = self._drive115_service_from_store(raw_config if isinstance(raw_config, dict) else {}).test_cookie()
        except Exception as err:
            self._log_event(
                level="warning",
                module="drive115",
                action="drive115_test_failed",
                message="115 Cookie 测试失败。",
                detail={"error": str(err)},
            )
            self._send_json(502, {"ok": False, "error": str(err)})
            return
        self._log_event(
            level="info",
            module="drive115",
            action="drive115_test_success",
            message="115 Cookie 测试成功。",
            status=200,
            detail={"userName": result.get("userName") or ""},
        )
        self._send_json(200, result)

    def _handle_drive115_parse(self) -> None:
        payload = self._read_json_body()
        if payload is None:
            return
        share_url = str(payload.get("shareUrl") or payload.get("url") or "").strip()
        receive_code = str(payload.get("receiveCode") or payload.get("password") or "").strip()
        try:
            result = self._drive115_service_from_store().parse_share(share_url=share_url, receive_code=receive_code)
        except Exception as err:
            self._log_event(
                level="warning",
                module="drive115",
                action="drive115_parse_failed",
                message="115 分享链接解析失败。",
                detail={"error": str(err)},
            )
            self._send_json(502, {"ok": False, "error": str(err)})
            return
        self._log_event(
            level="info",
            module="drive115",
            action="drive115_parse_success",
            message="115 分享链接解析成功。",
            status=200,
            detail={"shareCode": result.get("shareCode"), "fileCount": result.get("fileCount"), "title": result.get("title")},
        )
        self._send_json(200, result)

    def _handle_drive115_transfer(self) -> None:
        payload = self._read_json_body()
        if payload is None:
            return
        share_code = str(payload.get("shareCode") or "").strip()
        if not share_code:
            share_url = str(payload.get("shareUrl") or payload.get("url") or "").strip()
            share_code = extract_115_share(share_url, str(payload.get("receiveCode") or "")).get("shareCode") or ""
        receive_code = str(payload.get("receiveCode") or "").strip()
        target_cid = str(payload.get("targetCid") or payload.get("cid") or "").strip()
        file_ids = payload.get("fileIds") if isinstance(payload.get("fileIds"), list) else []
        source_files = payload.get("sourceFiles") if isinstance(payload.get("sourceFiles"), list) else []
        try:
            result = self._drive115_service_from_store().transfer_share(
                share_code=share_code,
                receive_code=receive_code,
                target_cid=target_cid,
                file_ids=[str(item) for item in file_ids],
                source_files=[row for row in source_files if isinstance(row, dict)],
            )
        except Exception as err:
            self._log_event(
                level="error",
                module="drive115",
                action="drive115_transfer_failed",
                message="115 分享转存失败。",
                detail={"shareCode": share_code, "targetCid": target_cid, "error": str(err)},
            )
            self._send_json(502, {"ok": False, "error": str(err)})
            return
        self._log_event(
            level="info",
            module="drive115",
            action="drive115_transfer_success",
            message="115 分享转存已提交。",
            status=200,
            detail={"shareCode": share_code, "targetCid": result.get("targetCid")},
        )
        self._send_json(200, result)

    def _cleanup_drive115_qrcode_sessions(self) -> None:
        now = time.time()
        with DRIVE115_QRCODE_LOCK:
            expired = [
                key
                for key, value in DRIVE115_QRCODE_SESSIONS.items()
                if now - float(value.get("createdAt") or 0) > 240 or value.get("stopped")
            ]
            for key in expired:
                DRIVE115_QRCODE_SESSIONS.pop(key, None)

    def _handle_drive115_qrcode_start(self) -> None:
        payload = self._read_json_body()
        if payload is None:
            return
        client = str(payload.get("client") or "qandroid").strip() or "qandroid"
        try:
            session = self._drive115_service_from_store().create_qrcode_session(client=client)
        except Exception as err:
            self._log_event(
                level="warning",
                module="drive115",
                action="drive115_qrcode_start_failed",
                message="115 扫码二维码生成失败。",
                detail={"client": client, "error": str(err)},
            )
            self._send_json(502, {"ok": False, "error": str(err)})
            return
        session_id = str(session.get("sessionId") or "").strip()
        with DRIVE115_QRCODE_LOCK:
            DRIVE115_QRCODE_SESSIONS[session_id] = {
                "uid": session.get("uid"),
                "time": session.get("time"),
                "sign": session.get("sign"),
                "client": session.get("client"),
                "clientLabel": session.get("clientLabel"),
                "imageUrl": session.get("imageUrl"),
                "createdAt": time.time(),
                "status": "waiting",
            }
        self._cleanup_drive115_qrcode_sessions()
        self._log_event(
            level="info",
            module="drive115",
            action="drive115_qrcode_started",
            message="115 扫码登录二维码已生成。",
            status=200,
            detail={"client": session.get("client")},
        )
        self._send_json(
            200,
            {
                "ok": True,
                "sessionId": session_id,
                "imageUrl": session.get("imageUrl"),
                "client": session.get("client"),
                "clientLabel": session.get("clientLabel"),
                "expiresIn": session.get("expiresIn") or 180,
                "clients": drive115_qrcode_clients(),
                "message": "二维码已生成，请用 115 App 扫码。",
            },
        )

    def _handle_drive115_qrcode_stop(self) -> None:
        payload = self._read_json_body()
        if payload is None:
            return
        session_id = str(payload.get("sessionId") or "").strip()
        with DRIVE115_QRCODE_LOCK:
            if session_id:
                DRIVE115_QRCODE_SESSIONS.pop(session_id, None)
        self._send_json(200, {"ok": True, "message": "已停止二维码轮询。"})

    def _handle_drive115_qrcode_status(self, query: str) -> None:
        params = urllib.parse.parse_qs(query or "")
        session_id = str((params.get("sessionId") or [""])[0] or "").strip()
        if not session_id:
            self._send_json(400, {"ok": False, "status": "failed", "error": "缺少二维码会话 ID。"})
            return
        with DRIVE115_QRCODE_LOCK:
            session = dict(DRIVE115_QRCODE_SESSIONS.get(session_id) or {})
        if not session:
            self._send_json(404, {"ok": False, "status": "expired", "error": "二维码会话已过期，请重新生成。"})
            return
        if time.time() - float(session.get("createdAt") or 0) > 180:
            with DRIVE115_QRCODE_LOCK:
                DRIVE115_QRCODE_SESSIONS.pop(session_id, None)
            self._send_json(410, {"ok": False, "status": "expired", "error": "二维码已过期，请重新生成。"})
            return
        try:
            service = self._drive115_service_from_store()
            status = service.check_qrcode_status(
                uid=str(session.get("uid") or ""),
                time_value=str(session.get("time") or ""),
                sign=str(session.get("sign") or ""),
            )
            if status.get("status") != "confirmed":
                self._send_json(200, status)
                return
            login_result = service.login_qrcode(
                uid=str(session.get("uid") or ""),
                client=str(session.get("client") or "qandroid"),
            )
            cookie = str(login_result.get("cookie") or "").strip()
            if not cookie:
                raise RuntimeError("115 登录成功但未返回 Cookie。")
            with STORE_LOCK:
                store = _read_store_unlocked()
                current = _normalize_drive115_config(store.get("drive115Config"))
                current["enabled"] = True
                current["cookie"] = cookie
                current["updatedAt"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
                store["drive115Config"] = current
                _write_store_unlocked(store)
                saved = _apply_drive115_env_overrides(store.get("drive115Config"))
            with DRIVE115_QRCODE_LOCK:
                DRIVE115_QRCODE_SESSIONS.pop(session_id, None)
            if TELEGRAM_COMMAND_SERVICE is not None:
                TELEGRAM_COMMAND_SERVICE.wakeup()
            self._log_event(
                level="info",
                module="drive115",
                action="drive115_qrcode_login_success",
                message="115 扫码登录成功，Cookie 已保存。",
                status=200,
                detail={"client": session.get("client")},
            )
            self._send_json(
                200,
                {
                    "ok": True,
                    "status": "success",
                    "message": "扫码登录成功，Cookie 已自动保存。",
                    "drive115Config": admin_drive115_config(saved),
                },
            )
        except Exception as err:
            self._log_event(
                level="warning",
                module="drive115",
                action="drive115_qrcode_status_failed",
                message="115 扫码状态查询失败。",
                detail={"sessionId": session_id, "error": str(err)},
            )
            self._send_json(502, {"ok": False, "status": "failed", "error": str(err)})

    def _save_hdhive_runtime_config(self, config: dict[str, Any]) -> None:
        with STORE_LOCK:
            store = _read_store_unlocked()
            stored = _normalize_hdhive_config(config)
            for field in _env_managed_hdhive_fields():
                stored[field] = _normalize_hdhive_config(store.get("hdhiveConfig")).get(field)
            store["hdhiveConfig"] = stored
            _write_store_unlocked(store)

    def _hdhive_service_from_store(self) -> HDHiveService:
        with STORE_LOCK:
            store = _read_store_unlocked()
            config = _apply_hdhive_env_overrides(store.get("hdhiveConfig"))
        return HDHiveService(config, save_config=self._save_hdhive_runtime_config)

    def _handle_hdhive_config_get(self) -> None:
        with STORE_LOCK:
            store = _read_store_unlocked()
            config = _apply_hdhive_env_overrides(store.get("hdhiveConfig"))
        payload = public_hdhive_config(config)
        payload["callbackUri"] = str(config.get("redirectUri") or "").strip() or f"{self._resolve_public_origin()}/api/hdhive/oauth/callback"
        self._send_json(200, {"ok": True, "hdhiveConfig": payload, "envControlledFields": _env_controlled_fields_payload()})

    def _handle_hdhive_config_save(self) -> None:
        payload = self._read_json_body()
        if payload is None:
            return
        incoming = payload.get("hdhiveConfig") if isinstance(payload, dict) else payload
        if not isinstance(incoming, dict):
            self._send_json(400, {"ok": False, "error": "影巢配置必须是对象"})
            return
        with STORE_LOCK:
            store = _read_store_unlocked()
            current = _normalize_hdhive_config(store.get("hdhiveConfig"))
            saved = merge_hdhive_config_for_save(current, incoming)
            broker_changed = (
                str(saved.get("authMode") or "") != str(current.get("authMode") or "")
                or str(saved.get("brokerUrl") or "") != str(current.get("brokerUrl") or "")
            )
            if broker_changed:
                for field in ("installationId", "installationSecret", "oauthSessionId"):
                    saved[field] = ""
                for field in ("oauthSessionExpiresAt",):
                    saved[field] = 0
                saved["user"] = {}
                saved["lastCheckin"] = {}
            if (
                str(saved.get("clientId") or "") != str(current.get("clientId") or "")
                or str(saved.get("appSecret") or "") != str(current.get("appSecret") or "")
                or str(saved.get("redirectUri") or "") != str(current.get("redirectUri") or "")
            ):
                for field in ("accessToken", "refreshToken", "accessExpiresAt", "refreshExpiresAt", "user"):
                    saved[field] = {} if field == "user" else 0 if field.endswith("At") else ""
            for field in _env_managed_hdhive_fields():
                saved[field] = current.get(field)
            store["hdhiveConfig"] = _normalize_hdhive_config(saved)
            _write_store_unlocked(store)
            effective = _apply_hdhive_env_overrides(store["hdhiveConfig"])
        if TELEGRAM_COMMAND_SERVICE is not None:
            TELEGRAM_COMMAND_SERVICE.wakeup()
        if str(effective.get("authMode") or "") == "broker" and effective.get("user"):
            try:
                HDHiveService(effective, save_config=self._save_hdhive_runtime_config).update_broker_preferences()
            except HDHiveError as err:
                self._log_event(level="warning", module="hdhive", action="hdhive_preferences_sync_failed", message="影巢签到设置同步失败。", detail={"code": err.code, "error": str(err)})
        self._log_event(
            level="info",
            module="hdhive",
            action="hdhive_config_saved",
            message="影巢 OpenAPI 配置已保存。",
            status=200,
            detail={"enabled": bool(effective.get("enabled")), "clientIdConfigured": bool(effective.get("clientId"))},
        )
        result = public_hdhive_config(effective)
        result["callbackUri"] = str(effective.get("redirectUri") or "").strip() or f"{self._resolve_public_origin()}/api/hdhive/oauth/callback"
        self._send_json(200, {"ok": True, "hdhiveConfig": result, "envControlledFields": _env_controlled_fields_payload()})

    def _handle_hdhive_test(self) -> None:
        try:
            service = self._hdhive_service_from_store()
            app = service.ping()
            has_authorization = bool(service.config.get("user")) if service.is_broker else bool(service.config.get("accessToken") or service.config.get("refreshToken"))
            user = service.me() if has_authorization else {}
            self._send_json(200, {"ok": True, "status": "connected", "app": app, "user": user})
        except HDHiveError as err:
            self._log_event(level="warning", module="hdhive", action="hdhive_test_failed", message="影巢连接测试失败。", detail={"code": err.code, "error": str(err)})
            self._send_json(err.status if 400 <= err.status < 500 else 502, {"ok": False, "code": err.code, "error": str(err), "retryAfter": err.retry_after})

    def _handle_hdhive_oauth_start(self) -> None:
        try:
            service = self._hdhive_service_from_store()
            if service.is_broker:
                session = service.create_broker_oauth_session()
                self._send_json(200, {"ok": True, "authorizeUrl": session.get("authorizeUrl"), "sessionId": session.get("sessionId"), "expiresAt": session.get("expiresAt"), "mode": "broker"})
                return
            redirect_uri = str(service.config.get("redirectUri") or "").strip() or f"{self._resolve_public_origin()}/api/hdhive/oauth/callback"
            state = secrets.token_urlsafe(32)
            with HDHIVE_OAUTH_LOCK:
                now = time.time()
                HDHIVE_OAUTH_STATES.clear()
                HDHIVE_OAUTH_STATES[state] = {"createdAt": now, "redirectUri": redirect_uri}
            authorize_url = service.build_authorize_url(state=state, redirect_uri=redirect_uri)
            self._send_json(200, {"ok": True, "authorizeUrl": authorize_url, "redirectUri": redirect_uri})
        except HDHiveError as err:
            self._send_json(400, {"ok": False, "code": err.code, "error": str(err)})

    def _handle_hdhive_oauth_status(self, query: str) -> None:
        params = urllib.parse.parse_qs(query)
        session_id = str((params.get("sessionId") or [""])[0]).strip()
        try:
            service = self._hdhive_service_from_store()
            if not service.is_broker:
                self._send_json(200, {"ok": True, "status": "authorized" if service.config.get("accessToken") else "idle", "mode": "direct"})
                return
            result = service.broker_oauth_status(session_id)
            self._send_json(200, {"ok": True, **result, "mode": "broker"})
        except HDHiveError as err:
            self._send_json(err.status if 400 <= err.status < 500 else 502, {"ok": False, "code": err.code, "error": str(err)})

    def _handle_hdhive_oauth_callback(self, query: str) -> None:
        params = urllib.parse.parse_qs(query)
        state = str((params.get("state") or [""])[0]).strip()
        code = str((params.get("code") or [""])[0]).strip()
        error = str((params.get("error_description") or params.get("error") or [""])[0]).strip()
        with HDHIVE_OAUTH_LOCK:
            session = HDHIVE_OAUTH_STATES.pop(state, None) if state else None
        if not session or time.time() - float(session.get("createdAt") or 0) > 600:
            self._send_json(400, {"ok": False, "error": "影巢授权 state 无效或已过期，请重新授权。"})
            return
        if error or not code:
            self._send_json(400, {"ok": False, "error": error or "影巢未返回授权码。"})
            return
        try:
            service = self._hdhive_service_from_store()
            service.exchange_code(code=code, redirect_uri=str(session.get("redirectUri") or ""))
            user = service.me()
            self._log_event(level="info", module="hdhive", action="hdhive_oauth_success", message="影巢账号授权成功。", status=200, detail={"userId": str(user.get("id") or ""), "level": str(user.get("level") or "")})
        except HDHiveError as err:
            self._log_event(level="error", module="hdhive", action="hdhive_oauth_failed", message="影巢账号授权失败。", detail={"code": err.code, "error": str(err)})
            self._send_json(err.status if 400 <= err.status < 500 else 502, {"ok": False, "code": err.code, "error": str(err)})
            return
        self.send_response(302)
        self.send_header("Location", "/?hdhive=authorized")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _handle_hdhive_disconnect(self) -> None:
        try:
            service = self._hdhive_service_from_store()
            service.disconnect()
            self._log_event(level="info", module="hdhive", action="hdhive_disconnected", message="影巢账号授权已解除。", status=200, detail={"mode": "broker" if service.is_broker else "direct"})
            self._send_json(200, {"ok": True, "message": "影巢账号授权已解除。"})
        except HDHiveError as err:
            self._send_json(err.status if 400 <= err.status < 500 else 502, {"ok": False, "code": err.code, "error": str(err)})

    def _handle_hdhive_checkin(self) -> None:
        try:
            service = self._hdhive_service_from_store()
            result = service.checkin()
            user = service.me()
            self._log_event(level="info", module="hdhive", action="hdhive_checkin", message="影巢普通签到已执行。", status=200, detail={"checkedIn": bool(result.get("checked_in")), "points": result.get("points")})
            self._send_json(200, {"ok": True, "result": result, "user": user})
        except HDHiveError as err:
            self._log_event(level="warning", module="hdhive", action="hdhive_checkin_failed", message="影巢签到失败。", detail={"code": err.code, "error": str(err)})
            self._send_json(err.status if 400 <= err.status < 500 else 502, {"ok": False, "code": err.code, "error": str(err)})

    def _hdhive_tmdb_fetcher(self, path: str) -> dict[str, Any] | list[Any] | None:
        with STORE_LOCK:
            config = _apply_emby_env_overrides(_read_store_unlocked().get("embyConfig"))
        token = str(config.get("tmdbToken") or "").strip()
        if not bool(config.get("tmdbEnabled")) or not token:
            raise RuntimeError("请先在系统设置中启用并保存 TMDB Token。")
        request = urllib.request.Request(
            f"https://api.themoviedb.org/3{path}",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
            method="GET",
        )
        with urllib.request.urlopen(request, timeout=20) as response:
            return json.loads(response.read().decode("utf-8", errors="replace"))

    def _resolve_hdhive_identity(self, query: str, media_type: str = "", tmdb_id: str = "") -> dict[str, Any]:
        if str(tmdb_id or "").strip():
            normalized_type = "series" if str(media_type).lower() in {"tv", "series"} else "movie"
            return {"title": str(query or "").strip(), "type": normalized_type, "tmdbId": str(tmdb_id).strip(), "year": ""}
        with STORE_LOCK:
            config = _apply_emby_env_overrides(_read_store_unlocked().get("embyConfig"))
        service = MediaIdentityService(
            emby_fetcher=lambda _path: None,
            tmdb_fetcher=self._hdhive_tmdb_fetcher,
            language=str(config.get("tmdbLanguage") or "zh-CN"),
            region=str(config.get("tmdbRegion") or "CN"),
        )
        candidates = service.search_media(str(query or "").strip(), media_type=str(media_type or ""))
        if not candidates:
            raise RuntimeError(f"TMDB 没有找到《{str(query or '').strip()}》。")
        return candidates[0]

    @staticmethod
    def _public_hdhive_resource(row: Any) -> dict[str, Any]:
        source = row if isinstance(row, dict) else {}
        pan_type = str(source.get("pan_type") or source.get("website") or "").strip()
        return {
            "slug": str(source.get("slug") or "").strip(),
            "title": str(source.get("title") or "").strip(),
            "panType": pan_type,
            "shareSize": str(source.get("share_size") or "").strip(),
            "resolution": source.get("video_resolution") or [],
            "source": source.get("source") or [],
            "subtitleLanguage": source.get("subtitle_language") or [],
            "subtitleType": source.get("subtitle_type") or [],
            "unlockPoints": int(source.get("unlock_points") or 0),
            "isUnlocked": bool(source.get("is_unlocked")),
            "publisher": str((source.get("user") or {}).get("username") or "") if isinstance(source.get("user"), dict) else "",
            "is115": "115" in pan_type.lower(),
        }

    def _handle_hdhive_search(self) -> None:
        payload = self._read_json_body()
        if payload is None:
            return
        query = str(payload.get("query") or payload.get("keyword") or "").strip()
        try:
            identity = self._resolve_hdhive_identity(query, str(payload.get("mediaType") or ""), str(payload.get("tmdbId") or ""))
            service = self._hdhive_service_from_store()
            if not service.config.get("enabled"):
                raise HDHiveError("影巢搜索尚未启用。")
            result = service.search_resources(media_type=str(identity.get("type") or ""), tmdb_id=str(identity.get("tmdbId") or ""))
            resources = [self._public_hdhive_resource(row) for row in result.get("items") or []]
            resources = [row for row in resources if row.get("slug")]
            user = service.me()
            self._log_event(level="info", module="hdhive", action="hdhive_search_success", message="影巢资源搜索完成。", status=200, detail={"tmdbId": identity.get("tmdbId"), "mediaType": identity.get("type"), "resultCount": len(resources)})
            self._send_json(200, {"ok": True, "identity": identity, "resources": resources, "meta": result.get("meta") or {}, "user": public_hdhive_config({**service.config, "user": user}).get("user")})
        except (HDHiveError, RuntimeError, urllib.error.URLError, urllib.error.HTTPError) as err:
            code = err.code if isinstance(err, HDHiveError) else ""
            status = err.status if isinstance(err, HDHiveError) else 0
            self._log_event(level="warning", module="hdhive", action="hdhive_search_failed", message="影巢资源搜索失败。", detail={"code": code, "error": str(err)})
            self._send_json(status if 400 <= status < 500 else 502, {"ok": False, "code": code, "error": str(err), "retryAfter": err.retry_after if isinstance(err, HDHiveError) else 0})

    def _handle_hdhive_transfer(self) -> None:
        payload = self._read_json_body()
        if payload is None:
            return
        slug = str(payload.get("slug") or "").strip()
        target_cid = str(payload.get("targetCid") or "").strip()
        if not slug:
            self._send_json(400, {"ok": False, "error": "缺少影巢资源 slug。"})
            return
        try:
            service = self._hdhive_service_from_store()
            unlocked = service.unlock(slug)
            full_url = str(unlocked.get("full_url") or unlocked.get("url") or "").strip()
            access_code = str(unlocked.get("access_code") or "").strip()
            share = extract_115_share(full_url, access_code)
            if not share.get("shareCode"):
                raise RuntimeError("该影巢资源不是可识别的 115 分享链接，无法转存。")
            parsed = self._drive115_service_from_store().parse_share(share_url=full_url, receive_code=access_code)
            parsed_files = parsed.get("files") if isinstance(parsed.get("files"), list) else []
            transfer = self._drive115_service_from_store().transfer_share(
                share_code=str(parsed.get("shareCode") or share.get("shareCode") or ""),
                receive_code=str(parsed.get("receiveCode") or access_code),
                target_cid=target_cid,
                file_ids=[str(row.get("id") or "").strip() for row in parsed_files if isinstance(row, dict) and str(row.get("id") or "").strip()],
                source_files=parsed_files,
            )
            self._log_event(level="info", module="hdhive", action="hdhive_transfer_success", message="影巢资源已解锁并提交 115 转存。", status=200, detail={"slug": slug, "targetCid": transfer.get("targetCid"), "alreadyOwned": bool(unlocked.get("already_owned"))})
            transfer_status = str(transfer.get("status") or "submitted")
            message = "目标目录已存在相同资源。" if transfer_status == "exists" else "影巢资源已解锁并提交 115 转存。"
            self._send_json(200, {"ok": True, "message": message, "status": transfer_status, "targetCid": transfer.get("targetCid"), "alreadyOwned": bool(unlocked.get("already_owned"))})
        except (HDHiveError, RuntimeError) as err:
            code = err.code if isinstance(err, HDHiveError) else ""
            status = err.status if isinstance(err, HDHiveError) else 0
            self._log_event(level="error", module="hdhive", action="hdhive_transfer_failed", message="影巢资源解锁或 115 转存失败。", detail={"slug": slug, "code": code, "error": str(err)})
            self._send_json(status if 400 <= status < 500 else 502, {"ok": False, "code": code, "error": str(err), "retryAfter": err.retry_after if isinstance(err, HDHiveError) else 0})

    def _first_forwarded_value(self, header_name: str) -> str:
        raw = str(self.headers.get(header_name) or "").strip()
        if not raw:
            return ""
        return raw.split(",")[0].strip()

    def _resolve_public_origin(self) -> str:
        manual_base = str(os.environ.get("BOT_PUBLIC_BASE_URL") or "").strip()
        manual_origin = _parse_origin_from_url(manual_base)
        if manual_origin:
            return manual_origin

        forwarded_proto = self._first_forwarded_value("X-Forwarded-Proto").lower()
        scheme = forwarded_proto if forwarded_proto in {"http", "https"} else "http"

        candidates: list[str] = []

        forwarded_host = self._first_forwarded_value("X-Forwarded-Host")
        host = forwarded_host or self._first_forwarded_value("Host")
        if host:
            if host.lower().startswith("http://") or host.lower().startswith("https://"):
                origin_from_host = _parse_origin_from_url(host)
                if origin_from_host:
                    candidates.append(origin_from_host)
            else:
                host_netloc = urllib.parse.urlsplit(f"//{host}").netloc if host else ""
                if host_netloc:
                    candidates.append(f"{scheme}://{host_netloc}".rstrip("/"))

        origin_header = _parse_origin_from_url(self._first_forwarded_value("Origin"))
        if origin_header:
            candidates.append(origin_header)

        referer_origin = _parse_origin_from_url(self._first_forwarded_value("Referer"))
        if referer_origin:
            candidates.append(referer_origin)

        server_host = str(getattr(self.server, "server_name", "") or "").strip() or "127.0.0.1"
        server_port = int(getattr(self.server, "server_port", 8080) or 8080)
        if server_host in {"0.0.0.0", "::"}:
            server_host = "127.0.0.1"
        candidates.append(f"{scheme}://{server_host}:{server_port}")

        for candidate in candidates:
            if not _is_local_or_private_host(_extract_host_from_origin(candidate)):
                return candidate.rstrip("/")

        return (candidates[0] if candidates else "http://127.0.0.1:8080").rstrip("/")

    def _handle_bot_webhook_url_get(self) -> None:
        token = str(os.environ.get("BOT_WEBHOOK_TOKEN") or DEFAULT_WEBHOOK_TOKEN).strip() or DEFAULT_WEBHOOK_TOKEN
        base_url = self._resolve_public_origin()
        encoded_token = urllib.parse.quote(token, safe="")
        webhook_url = f"{base_url}/api/v1/webhook?token={encoded_token}"
        self._send_json(200, {"ok": True, "webhookUrl": webhook_url})

    def _handle_bot_webhook_status_get(self) -> None:
        payload = {"ok": True}
        payload.update(_build_webhook_status_payload())
        self._send_json(200, payload)

    def _record_webhook_status(self, *, event_type: str, result: str, detail: str = "") -> None:
        _set_last_webhook_processed(event_type=event_type, result=result, detail=detail)
        level = "error" if "error" in result or result in {"token_invalid", "invalid_payload"} else "info"
        if result.endswith("_disabled") or result.endswith("_skipped") or result in {"telegram_not_configured", "unsupported_event"}:
            level = "warning"
        self._log_event(
            level=level,
            module="webhook",
            action=str(result or "processed"),
            message=f"Webhook {event_type or 'unknown'}：{detail or result}",
            status=200 if level != "error" else "",
            detail={"eventType": event_type, "result": result, "detail": detail},
        )

    def _mark_webhook_received(self) -> None:
        _mark_webhook_received()

    def _build_telegram_opener(self) -> urllib.request.OpenerDirector:
        # deprecated: keep for compatibility
        return urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def _telegram_api_request(self, *, token: str, method: str, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            return TELEGRAM_SENDER.api_request(token=token, method=method, payload=payload)
        except Exception as err:
            raise RuntimeError(str(err)) from None

    def _send_telegram_text(self, *, token: str, chat_id: str, text: str) -> None:
        TELEGRAM_SENDER.send_text(token=token, chat_id=str(chat_id), text=str(text))

    def _send_telegram_photo(
        self,
        *,
        token: str,
        chat_id: str,
        photo_url: str,
        caption: str,
        button_text: str = "",
        button_url: str = "",
    ) -> None:
        TELEGRAM_SENDER.send_photo(
            token=token,
            chat_id=str(chat_id),
            photo_url=str(photo_url).strip(),
            caption=str(caption or ""),
            button_text=str(button_text or ""),
            button_url=str(button_url or ""),
        )

    def _get_payload_str(self, payload: dict[str, Any], *path: str) -> str:
        current: Any = payload
        for key in path:
            if not isinstance(current, dict):
                return ""
            current = current.get(key)
        if current is None:
            return ""
        return str(current).strip()

    def _classify_webhook_type(self, payload: dict[str, Any]) -> tuple[str, str]:
        event_name = ""
        event_sources: list[str] = []
        for keys in (
            ("Event",),
            ("event",),
            ("NotificationType",),
            ("notificationType",),
            ("Type",),
            ("type",),
            ("Name",),
            ("name",),
        ):
            value = self._get_payload_str(payload, *keys)
            if value:
                event_sources.append(value.lower())
                event_name = value
        if not event_sources:
            for keys in (("MessageType",), ("messageType",), ("EventId",), ("eventId",), ("Action",), ("action",)):
                value = self._get_payload_str(payload, *keys)
                if value:
                    event_sources.append(value.lower())

        combined = " ".join(event_sources)
        if any(keyword in combined for keyword in PLAYBACK_EVENT_KEYWORDS):
            return "playback", event_name
        if any(keyword in combined for keyword in LIBRARY_EVENT_KEYWORDS):
            return "library", event_name
        return "other", event_name

    def _to_seconds_from_ticks(self, raw: Any) -> int:
        try:
            ticks = int(raw or 0)
        except (TypeError, ValueError):
            return 0
        if ticks <= 0:
            return 0
        return ticks // 10000000

    def _format_hms(self, total_seconds: int) -> str:
        safe = max(0, int(total_seconds or 0))
        hours = safe // 3600
        minutes = (safe % 3600) // 60
        seconds = safe % 60
        if hours > 0:
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        return f"{minutes:02d}:{seconds:02d}"

    def _format_hms_full(self, total_seconds: int) -> str:
        safe = max(0, int(total_seconds or 0))
        hours = safe // 3600
        minutes = (safe % 3600) // 60
        seconds = safe % 60
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    def _safe_float(self, raw: Any) -> float | None:
        try:
            value = float(raw)
        except (TypeError, ValueError):
            return None
        if value != value:
            return None
        return value

    def _shorten(self, text: str, *, limit: int = 160) -> str:
        clean = str(text or "").strip()
        if not clean:
            return ""
        if len(clean) <= limit:
            return clean
        return clean[: max(8, limit - 1)].rstrip() + "…"

    def _pick_first_value(self, payload: dict[str, Any], paths: list[tuple[str, ...]]) -> str:
        for keys in paths:
            candidate = self._get_payload_str(payload, *keys)
            if candidate:
                return candidate
        return ""

    def _pick_first_int(self, payload: dict[str, Any], paths: list[tuple[str, ...]]) -> int | None:
        for keys in paths:
            candidate = self._get_payload_str(payload, *keys)
            if candidate in (None, ""):
                continue
            try:
                return int(candidate)
            except (TypeError, ValueError):
                continue
        return None

    def _normalize_remote_ip(self, raw: str) -> str:
        value = str(raw or "").strip()
        if not value:
            return ""
        if value.startswith("[") and "]:" in value:
            return value[1 : value.find("]")]
        if value.count(":") == 1 and "." in value:
            host, _, _ = value.partition(":")
            return host.strip()
        return value

    def _infer_geo_suffix(self, payload: dict[str, Any]) -> str:
        geo = self._pick_first_value(
            payload,
            [
                ("Geo",),
                ("geo",),
                ("IpLocation",),
                ("ipLocation",),
                ("Location",),
                ("location",),
                ("Area",),
                ("area",),
                ("Address",),
                ("address",),
            ],
        )
        return re.sub(r"\s+", " ", str(geo or "").strip())

    def _compose_ip_display(self, payload: dict[str, Any]) -> str:
        ip_raw = self._pick_first_value(
            payload,
            [("RemoteEndPoint",), ("remoteEndPoint",), ("IpAddress",), ("ipAddress",), ("Session", "RemoteEndPoint"), ("session", "remoteEndPoint")],
        )
        ip_only = self._normalize_remote_ip(ip_raw)
        if not ip_only:
            return ""
        geo_suffix = self._infer_geo_suffix(payload)
        if geo_suffix and geo_suffix not in ip_only:
            return f"{ip_only} {geo_suffix}"
        return ip_only

    def _format_episode_tag(self, payload: dict[str, Any], item_detail: dict[str, Any]) -> str:
        season = self._pick_first_int(
            payload,
            [
                ("ParentIndexNumber",),
                ("parentIndexNumber",),
                ("NowPlayingItem", "ParentIndexNumber"),
                ("nowPlayingItem", "parentIndexNumber"),
                ("Item", "ParentIndexNumber"),
                ("item", "parentIndexNumber"),
            ],
        )
        if season is None:
            try:
                season = int(item_detail.get("ParentIndexNumber"))
            except (TypeError, ValueError):
                season = None

        episode = self._pick_first_int(
            payload,
            [
                ("IndexNumber",),
                ("indexNumber",),
                ("NowPlayingItem", "IndexNumber"),
                ("nowPlayingItem", "indexNumber"),
                ("Item", "IndexNumber"),
                ("item", "indexNumber"),
            ],
        )
        if episode is None:
            try:
                episode = int(item_detail.get("IndexNumber"))
            except (TypeError, ValueError):
                episode = None

        if season and episode:
            return f"S{season:02d}E{episode:02d}"
        if season:
            return f"S{season:02d}"
        if episode:
            return f"E{episode:02d}"
        return ""

    def _event_display_time(self, payload: dict[str, Any]) -> str:
        raw = self._pick_first_value(payload, [("Date",), ("date",), ("EventTime",), ("eventTime",), ("Timestamp",), ("timestamp",)])
        if raw:
            text = str(raw).strip()
            try:
                parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
                return parsed.astimezone().strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                pass
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _detect_playback_action(self, payload: dict[str, Any], event_name: str) -> str:
        return detect_playback_action(payload, event_name)

    def _extract_item_id(self, payload: dict[str, Any]) -> str:
        return self._pick_first_value(
            payload,
            [
                ("ItemId",),
                ("itemId",),
                ("Item", "Id"),
                ("item", "id"),
                ("NowPlayingItem", "Id"),
                ("nowPlayingItem", "id"),
                ("Session", "NowPlayingItem", "Id"),
                ("session", "nowPlayingItem", "id"),
            ],
        )

    def _extract_session_id(self, payload: dict[str, Any]) -> str:
        return self._pick_first_value(
            payload,
            [
                ("SessionId",),
                ("sessionId",),
                ("Session", "Id"),
                ("session", "id"),
            ],
        )

    def _resolve_emby_api_base(self, emby_config: dict[str, Any]) -> str:
        public_web = str(os.environ.get("EMBY_PUBLIC_WEB_URL") or "").strip().rstrip("/")
        if public_web:
            if public_web.lower().endswith("/emby"):
                return public_web
            return f"{public_web}/emby"

        server_url = str(emby_config.get("serverUrl") or "").strip().rstrip("/")
        if not server_url:
            return ""
        if server_url.lower().endswith("/emby"):
            return server_url
        return f"{server_url}/emby"

    def _resolve_emby_web_base(self, emby_config: dict[str, Any]) -> str:
        public_web = str(os.environ.get("EMBY_PUBLIC_WEB_URL") or "").strip().rstrip("/")
        if public_web:
            return public_web
        server_url = str(emby_config.get("serverUrl") or "").strip().rstrip("/")
        if server_url.lower().endswith("/emby"):
            return server_url[:-5].rstrip("/")
        return server_url

    def _fetch_emby_item_detail(self, *, emby_config: dict[str, Any], item_id: str) -> dict[str, Any]:
        safe_item_id = str(item_id or "").strip()
        if not safe_item_id:
            return {}
        api_base = self._resolve_emby_api_base(emby_config)
        api_key = str(emby_config.get("apiKey") or "").strip()
        if not api_base or not api_key:
            return {}

        fields = ",".join(
            [
                "Overview",
                "CommunityRating",
                "Type",
                "RunTimeTicks",
                "Genres",
                "SeriesName",
                "SeriesId",
                "SeasonId",
                "ParentId",
                "ParentIndexNumber",
                "IndexNumber",
                "ProductionYear",
                "PremiereDate",
                "DateCreated",
                "People",
                "Studios",
                "MediaSources",
                "MediaStreams",
                "ChildCount",
                "RecursiveItemCount",
                "PrimaryImageItemId",
                "ImageTags",
                "Container",
                "Path",
            ]
        )
        encoded_item_id = urllib.parse.quote(safe_item_id, safe="")
        query = urllib.parse.urlencode({"Fields": fields})
        path = f"/Items/{encoded_item_id}?{query}"
        try:
            detail = self._emby_request(base_url=api_base, api_key=api_key, path=path, method="GET")
        except Exception:
            return {}
        return detail if isinstance(detail, dict) else {}

    def _fetch_emby_session_detail(self, *, emby_config: dict[str, Any], session_id: str) -> dict[str, Any]:
        safe_session_id = str(session_id or "").strip()
        if not safe_session_id:
            return {}
        api_base = self._resolve_emby_api_base(emby_config)
        api_key = str(emby_config.get("apiKey") or "").strip()
        if not api_base or not api_key:
            return {}
        try:
            result = self._emby_request(base_url=api_base, api_key=api_key, path="/Sessions", method="GET")
        except Exception:
            return {}
        rows = result if isinstance(result, list) else []
        for row in rows:
            if not isinstance(row, dict):
                continue
            if str(row.get("Id") or "").strip() == safe_session_id:
                return row
        return {}

    def _build_emby_item_urls(self, *, emby_config: dict[str, Any], item_id: str) -> tuple[str, str]:
        safe_item_id = str(item_id or "").strip()
        if not safe_item_id:
            return "", ""
        api_base = self._resolve_emby_api_base(emby_config)
        web_base = self._resolve_emby_web_base(emby_config)
        api_key = str(emby_config.get("apiKey") or "").strip()

        poster_url = ""
        if api_base:
            encoded_item_id = urllib.parse.quote(safe_item_id, safe="")
            params = {"maxWidth": "1100", "quality": "90"}
            if api_key:
                params["api_key"] = api_key
            poster_url = f"{api_base}/Items/{encoded_item_id}/Images/Primary?{urllib.parse.urlencode(params)}"

        detail_url = ""
        if web_base:
            encoded_item_id = urllib.parse.quote(safe_item_id, safe="")
            detail_url = f"{web_base}/web/index.html#!/item?id={encoded_item_id}"

        return poster_url, detail_url

    def _handle_annual_ranking(self, query: str) -> None:
        with STORE_LOCK:
            store = _read_store_unlocked()
            emby_config = _apply_emby_env_overrides(store.get("embyConfig"))

        api_base = self._resolve_emby_api_base(emby_config)
        api_key = str(emby_config.get("apiKey") or "").strip()
        if not api_base or not api_key:
            self._send_json(400, {"ok": False, "error": "未配置 Emby 连接", "code": "emby_config_missing"})
            return

        options = self._parse_annual_ranking_query(query)
        cache_key = self._build_annual_ranking_cache_key(emby_config=emby_config, options=options)
        if not options["forceRefresh"]:
            cached = _annual_cache_get(cache_key)
            if cached:
                self._send_json(200, cached)
                return

        try:
            payload = self._build_annual_ranking_payload(
                api_base=api_base,
                api_key=api_key,
                options=options,
            )
        except urllib.error.HTTPError as err:
            detail = err.read().decode("utf-8", errors="replace")[:300]
            self._send_json(
                502,
                {
                    "ok": False,
                    "error": "读取 Emby 排行榜数据失败",
                    "code": "emby_ranking_failed",
                    "status": err.code,
                    "detail": detail,
                },
            )
            return
        except Exception as err:
            self._send_json(
                502,
                {
                    "ok": False,
                    "error": "生成排行榜失败",
                    "code": "ranking_build_failed",
                    "detail": str(err)[:300],
                },
            )
            return

        _annual_cache_set(cache_key, payload, int(options["ttlSeconds"]))
        self._send_json(200, payload)

    def _parse_annual_ranking_query(self, query: str) -> dict[str, Any]:
        params = urllib.parse.parse_qs(query or "", keep_blank_values=True)

        def first(name: str, default: str = "") -> str:
            rows = params.get(name)
            return str(rows[0] if rows else default).strip()

        category = first("category", "global")
        if category not in {"global", "movie", "series"}:
            category = "global"
        sort_by = first("sortBy", "playCount")
        if sort_by not in {"playCount", "duration"}:
            sort_by = "playCount"
        scope = first("scope", "all") or "all"
        try:
            limit = int(first("limit", "50") or "50")
        except Exception:
            limit = 50
        limit = max(1, min(80, limit))
        try:
            ttl_seconds = int(first("ttlSeconds", str(ANNUAL_RANKING_CACHE_TTL_SECONDS)) or ANNUAL_RANKING_CACHE_TTL_SECONDS)
        except Exception:
            ttl_seconds = ANNUAL_RANKING_CACHE_TTL_SECONDS
        ttl_seconds = max(60, min(300, ttl_seconds))
        force_refresh = first("refresh") in {"1", "true", "yes"} or first("force") in {"1", "true", "yes"}
        return {
            "category": category,
            "sortBy": sort_by,
            "scope": scope,
            "limit": limit,
            "ttlSeconds": ttl_seconds,
            "forceRefresh": force_refresh,
        }

    def _build_annual_ranking_cache_key(self, *, emby_config: dict[str, Any], options: dict[str, Any]) -> str:
        server_url = str(emby_config.get("serverUrl") or "").strip().rstrip("/").lower()
        raw = json.dumps(
            {
                "version": 4,
                "server": server_url,
                "category": options.get("category"),
                "sortBy": options.get("sortBy"),
                "scope": options.get("scope"),
                "limit": options.get("limit"),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _build_annual_ranking_payload(self, *, api_base: str, api_key: str, options: dict[str, Any]) -> dict[str, Any]:
        logs = self._fetch_annual_activity_logs(api_base=api_base, api_key=api_key, max_count=2000, chunk=200)
        debug = self._annual_init_debug(logs)
        events = self._normalize_annual_playback_events(logs, debug=debug)
        self._annual_resolve_event_item_ids_by_title(api_base=api_base, api_key=api_key, events=events, debug=debug)
        scope_options = self._build_annual_scope_options(events)
        requested_scope = str(options.get("scope") or "all")
        if not any(option.get("value") == requested_scope for option in scope_options):
            requested_scope = "all"
        scoped_events = self._filter_annual_events_by_scope(events, requested_scope)
        item_details = self._fetch_annual_item_details_for_events(api_base=api_base, api_key=api_key, events=scoped_events)
        item_detail_failed = 0
        for event in scoped_events:
            item_id = str(event.get("itemId") or "").strip()
            if item_id and not item_details.get(item_id):
                item_detail_failed += 1
        debug["skipReasons"]["item_detail_failed"] = int(debug["skipReasons"].get("item_detail_failed") or 0) + item_detail_failed
        items = self._aggregate_annual_items(
            api_base=api_base,
            api_key=api_key,
            events=scoped_events,
            item_details=item_details,
        )
        category = str(options.get("category") or "global")
        if category == "movie":
            items = [item for item in items if item.get("type") == "movie"]
        elif category == "series":
            items = [item for item in items if item.get("type") == "series"]

        if options.get("sortBy") == "duration":
            items.sort(key=lambda item: (float(item.get("duration") or 0), int(item.get("playCount") or 0)), reverse=True)
        else:
            items.sort(key=lambda item: (int(item.get("playCount") or 0), float(item.get("duration") or 0)), reverse=True)

        limit = int(options.get("limit") or 50)
        generated_at = _now_iso()
        ttl_seconds = int(options.get("ttlSeconds") or ANNUAL_RANKING_CACHE_TTL_SECONDS)
        matched_count = len(events)
        raw_count = len(logs)
        warning = ""
        if raw_count > 0 and matched_count == 0:
            warning = "已获取到 Emby 活动日志，但没有识别到播放事件，请检查日志语言、关键词规则、时间范围或 ItemId 字段。"
        debug["rawLogCount"] = raw_count
        debug["matchedEventCount"] = matched_count
        return {
            "ok": True,
            "cached": False,
            "cacheStore": "fresh",
            "ttlSeconds": ttl_seconds,
            "generatedAt": generated_at,
            "expiresAt": datetime.fromtimestamp(time.time() + ttl_seconds).isoformat(timespec="seconds"),
            "source": "backend",
            "category": category,
            "sortBy": str(options.get("sortBy") or "playCount"),
            "scope": requested_scope,
            "rawLogCount": raw_count,
            "matchedEventCount": matched_count,
            "viewCount": max(0, len(scope_options) - 1),
            "scopeOptions": scope_options,
            "items": items[:limit],
            "warning": warning,
            "debug": debug,
        }

    def _fetch_annual_activity_logs(
        self,
        *,
        api_base: str,
        api_key: str,
        max_count: int = 2000,
        chunk: int = 200,
    ) -> list[dict[str, Any]]:
        def normalize_page(result: Any) -> list[dict[str, Any]]:
            rows = result.get("Items") if isinstance(result, dict) else result
            return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []

        first_result = self._emby_request(
            base_url=api_base,
            api_key=api_key,
            path=f"/System/ActivityLog/Entries?Limit={chunk}&StartIndex=0",
            method="GET",
        )
        first_items = normalize_page(first_result)
        total = len(first_items)
        if isinstance(first_result, dict):
            total = int(first_result.get("TotalRecordCount") or total or 0)
        total = min(max(0, total), max_count)
        if not first_items or len(first_items) >= total:
            return first_items[:max_count]

        starts = list(range(len(first_items), total, chunk))
        pages: list[list[dict[str, Any]]] = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
            future_map = {
                executor.submit(
                    self._emby_request,
                    base_url=api_base,
                    api_key=api_key,
                    path=f"/System/ActivityLog/Entries?Limit={chunk}&StartIndex={start}",
                    method="GET",
                ): start
                for start in starts
            }
            for future in concurrent.futures.as_completed(future_map):
                try:
                    pages.append(normalize_page(future.result()))
                except Exception:
                    pages.append([])
        return [*first_items, *[item for page in pages for item in page]][:max_count]

    def _annual_init_debug(self, logs: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            "rawLogCount": len(logs),
            "matchedEventCount": 0,
            "skippedCount": 0,
            "skipReasons": {
                "no_play_keyword": 0,
                "date_parse_failed": 0,
                "out_of_range": 0,
                "no_item_id": 0,
                "title_extract_failed": 0,
                "item_detail_failed": 0,
            },
            "sampleRawLogs": [],
            "sampleMatchedEvents": [],
            "sampleSkippedLogs": [],
        }

    def _annual_debug_push(self, rows: list[dict[str, Any]], row: dict[str, Any], limit: int = 10) -> None:
        if len(rows) < limit:
            rows.append(row)

    def _annual_debug_log_core(self, log: dict[str, Any]) -> dict[str, Any]:
        item = log.get("Item") if isinstance(log.get("Item"), dict) else {}
        user = log.get("User") if isinstance(log.get("User"), dict) else {}
        return {
            "Name": str(log.get("Name") or ""),
            "Overview": str(log.get("Overview") or ""),
            "ShortOverview": str(log.get("ShortOverview") or ""),
            "Type": str(log.get("Type") or ""),
            "Event": str(log.get("Event") or log.get("EventName") or ""),
            "ItemId": str(log.get("ItemId") or item.get("Id") or ""),
            "UserId": str(log.get("UserId") or user.get("Id") or ""),
            "UserName": str(log.get("UserName") or user.get("Name") or ""),
            "Date": str(log.get("Date") or log.get("StartDate") or log.get("DateCreated") or ""),
        }

    def _annual_note_skip(self, debug: dict[str, Any], log: dict[str, Any], reason: str, note: str = "") -> None:
        skip_reasons = debug.get("skipReasons", {})
        skip_reasons[reason] = int(skip_reasons.get(reason) or 0) + 1
        debug["skipReasons"] = skip_reasons
        debug["skippedCount"] = int(debug.get("skippedCount") or 0) + 1
        self._annual_debug_push(
            debug["sampleSkippedLogs"],
            {
                "reason": reason,
                "note": note,
                "log": self._annual_debug_log_core(log),
            },
        )

    def _annual_extract_item_id(self, log: dict[str, Any]) -> str:
        item = log.get("Item") if isinstance(log.get("Item"), dict) else {}
        for key in ("ItemId", "MediaSourceId", "Id"):
            value = str(log.get(key) or "").strip()
            if value and key != "Id":
                return value
        for key in ("Id", "ItemId", "SeriesId", "ParentId"):
            value = str(item.get(key) or "").strip()
            if value:
                return value
        text = self._annual_log_text(log)
        match = re.search(r"(?:itemid|item id|mediaid|media id)\s*[:=]\s*([a-zA-Z0-9_-]{8,})", text, flags=re.IGNORECASE)
        return str(match.group(1) if match else "").strip()

    def _annual_is_probable_playback_log(self, log: dict[str, Any], text: str, action: str) -> bool:
        event_text = " ".join(
            [
                str(log.get("Event") or ""),
                str(log.get("EventName") or ""),
                str(log.get("Type") or ""),
            ]
        ).lower()
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
        if action in {"start", "stop", "progress"}:
            return True
        lower = str(text or "").lower()
        keywords = (
            "播放",
            "开始播放",
            "正在播放",
            "已播放",
            "观看",
            "播放了",
            "play",
            "played",
            "playing",
            "playback",
            "stream",
            "watched",
            "userstartedplaying",
            "userstoppedplaying",
        )
        return any(keyword in lower for keyword in keywords)

    def _normalize_annual_playback_events(self, logs: list[dict[str, Any]], debug: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        debug_payload = debug if isinstance(debug, dict) else self._annual_init_debug(logs)
        events: list[dict[str, Any]] = []
        dedupe: set[str] = set()
        now_ts = time.time()
        min_valid_ts = datetime(2000, 1, 1).timestamp()
        for log in logs:
            self._annual_debug_push(debug_payload["sampleRawLogs"], self._annual_debug_log_core(log))
            text = self._annual_log_text(log)
            action = self._annual_infer_playback_action(
                text,
                event_text=" ".join((str(log.get("Event") or ""), str(log.get("EventName") or ""), str(log.get("Type") or ""))),
            )
            if action == "pause":
                self._annual_note_skip(debug_payload, log, "no_play_keyword", "pause event skipped")
                continue
            if not self._annual_is_probable_playback_log(log, text, action):
                self._annual_note_skip(debug_payload, log, "no_play_keyword", "no playback keyword/action")
                continue

            title = self._annual_extract_media_title(log)
            item_id = self._annual_extract_item_id(log)
            if not title and not item_id:
                self._annual_note_skip(debug_payload, log, "title_extract_failed", "missing title and itemId")
                continue
            if not title:
                title = "未命名内容"
            played_at = str(log.get("Date") or log.get("StartDate") or log.get("DateCreated") or "").strip()
            played_ts = self._annual_datetime_ts(played_at)
            unknown_time = False
            if played_ts <= 0:
                unknown_time = True
                debug_payload["skipReasons"]["date_parse_failed"] = int(debug_payload["skipReasons"].get("date_parse_failed") or 0) + 1
            elif played_ts < min_valid_ts or played_ts > now_ts + 3 * 24 * 60 * 60:
                self._annual_note_skip(debug_payload, log, "out_of_range", f"playedTs={played_ts}")
                continue
            user = log.get("User") if isinstance(log.get("User"), dict) else {}
            user_name = str(
                log.get("UserName")
                or log.get("ByUserName")
                or log.get("Client")
                or log.get("DeviceName")
                or user.get("Name")
                or self._annual_parse_user_from_text(text)
                or "未知用户"
            ).strip()
            dedupe_bucket = int(played_ts // 60) if played_ts > 0 else (played_at[:16] or "unknown_time")
            dedupe_key = f"{item_id or self._annual_lookup_key(title)}|{user_name.lower()}|{dedupe_bucket}"
            if dedupe_key in dedupe:
                continue
            dedupe.add(dedupe_key)
            event = {
                "title": self._annual_normalize_media_title(title),
                "userName": user_name,
                "itemId": item_id,
                "playedAt": played_at,
                "playedTs": played_ts,
                "unknownTime": unknown_time,
                "durationMin": self._annual_parse_duration_minutes(log, text),
                "type": self._annual_type_from_log(log, title),
            }
            if not item_id:
                debug_payload["skipReasons"]["no_item_id"] = int(debug_payload["skipReasons"].get("no_item_id") or 0) + 1
            self._annual_debug_push(
                debug_payload["sampleMatchedEvents"],
                {
                    "title": event["title"],
                    "userName": event["userName"],
                    "itemId": event["itemId"],
                    "playedAt": event["playedAt"],
                    "unknownTime": event["unknownTime"],
                    "type": event["type"],
                },
            )
            events.append(event)
        debug_payload["matchedEventCount"] = len(events)
        return events

    def _annual_log_text(self, log: dict[str, Any]) -> str:
        return " ".join(
            str(log.get(key) or "")
            for key in (
                "Name",
                "ShortOverview",
                "Overview",
                "Message",
                "Description",
                "Type",
                "Event",
                "EventName",
            )
        ).strip()

    def _annual_extract_media_title(self, log: dict[str, Any]) -> str:
        item = log.get("Item") if isinstance(log.get("Item"), dict) else {}
        direct = (
            log.get("ItemName")
            or log.get("MediaName")
            or log.get("MediaTitle")
            or item.get("Name")
            or item.get("SeriesName")
            or ""
        )
        if str(direct or "").strip():
            return str(direct).strip()

        text = self._annual_log_text(log).replace("：", ":")
        for pattern in (
            r"《([^》]+)》",
            r'"([^"]+)"',
            r"(?:用户|user)\s*[\w\u4e00-\u9fa5.-]+\s*(?:开始播放|正在播放|已播放|播放了|观看)\s+(.+)$",
            r"[\w\u4e00-\u9fa5.-]+\s*(?:开始播放|正在播放|已播放|播放了|观看)\s+(.+)$",
            r"(?:开始播放|已开始播放|正在播放|已停止播放|停止播放|播放|观看)\s+(.+)$",
            r"(?:user\s+[\w.-]+\s+is\s+playing)\s+(.+)$",
            r"(?:user\s+[\w.-]+\s+started\s+playing)\s+(.+)$",
            r"[\w.-]+\s+played\s+(.+)$",
            r"(?:started playing|is playing|playing|played|watched)\s+(.+)$",
        ):
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match and match.group(1):
                return self._annual_cleanup_title(match.group(1))
        name = str(log.get("Name") or "").strip()
        return self._annual_cleanup_title(name)

    def _annual_cleanup_title(self, title: str) -> str:
        text = str(title or "").strip().strip("《》“”\"'")
        text = re.sub(r"\s+(?:on|via|from)\s+.+$", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s+在\s+.+?上.*$", "", text)
        return text.strip()

    def _annual_normalize_media_title(self, title: str) -> str:
        text = self._annual_cleanup_title(title)
        for pattern in (
            r"\s*[-–—]\s*S\d+\s*,?\s*E(?:P)?\d+\b.*$",
            r"\s*[-–—]\s*S\d+\s*E\d+\b.*$",
            r"\s*[-–—]\s*第\s*\d+\s*季\s*第\s*\d+\s*集.*$",
            r"\s*[-–—]\s*第\s*\d+\s*集.*$",
        ):
            text = re.sub(pattern, "", text, flags=re.IGNORECASE).strip()
        return text or "未命名内容"

    def _annual_lookup_key(self, title: str) -> str:
        return re.sub(r"\s+", " ", str(title or "").lower().replace("《", "").replace("》", "").strip())

    def _annual_infer_playback_action(self, text: str, event_text: str = "") -> str:
        content = f"{str(text or '').lower()} {str(event_text or '').lower()}".strip()
        if re.search(r"(暂停播放|播放暂停|paused)", content, flags=re.IGNORECASE):
            return "pause"
        if re.search(
            r"(停止播放|已停止播放|结束播放|播放结束|stopped|ended|finished|session ended|playbackstopped|userstoppedplaying)",
            content,
            flags=re.IGNORECASE,
        ):
            return "stop"
        if re.search(r"(playbackprogress|progress|播放进度|播放中|播放了)", content, flags=re.IGNORECASE):
            return "progress"
        if re.search(
            r"(开始播放|已开始播放|继续播放|恢复播放|start(?:ed)?|resum(?:e|ed)|playing|playbackstart|userstartedplaying)",
            content,
            flags=re.IGNORECASE,
        ):
            return "start"
        return "other"

    def _annual_parse_user_from_text(self, text: str) -> str:
        for pattern in (
            r"(?P<user>[\w\u4e00-\u9fa5.-]+)\s+在\s+[^，。,.\s]+\s+上(?:开始|停止|继续)?播放",
            r"用户\s*(?P<user>[\w\u4e00-\u9fa5.-]+).{0,12}(?:设备|客户端)",
        ):
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                return str(match.group("user") or "").strip()
        return ""

    def _annual_datetime_ts(self, value: str) -> float:
        raw = str(value or "").strip()
        if not raw:
            return 0.0
        if raw.isdigit():
            try:
                value_int = int(raw)
                if value_int > 10_000_000_000:
                    return float(value_int / 1000.0)
                return float(value_int)
            except Exception:
                return 0.0
        try:
            normalized = raw.replace("Z", "+00:00")
            normalized = re.sub(r"\.(\d{6})\d+(?=(?:[+-]\d{2}:\d{2})$)", r".\1", normalized)
            normalized = re.sub(r"\.(\d{6})\d+$", r".\1", normalized)
            return datetime.fromisoformat(normalized).timestamp()
        except Exception:
            return 0.0

    def _annual_parse_duration_minutes(self, log: dict[str, Any], text: str) -> int:
        for key in ("PlaybackPositionTicks", "PositionTicks", "StopPositionTicks", "LastPositionTicks", "RunTimeTicks"):
            minutes = self._annual_runtime_minutes(log.get(key))
            if minutes:
                return minutes
        lower = str(text or "").lower().replace("：", ":")
        hms_match = re.search(r"\b(\d{1,2}):(\d{2}):(\d{2})\b", lower)
        if hms_match:
            seconds = int(hms_match.group(1)) * 3600 + int(hms_match.group(2)) * 60 + int(hms_match.group(3))
            return max(1, round(seconds / 60))
        mmss_match = re.search(r"(?:时长|持续|耗时|duration|elapsed|played|watch(?:ed|ing)?|播放|观看)[^0-9]{0,12}(\d{1,2}):(\d{2})", lower)
        if mmss_match:
            seconds = int(mmss_match.group(1)) * 60 + int(mmss_match.group(2))
            return max(1, round(seconds / 60))
        hour_match = re.search(r"(\d+)\s*(?:小时|hour|hours|hr|hrs)", lower)
        minute_match = re.search(r"(\d+)\s*(?:分钟|分|min(?:ute)?s?)", lower)
        second_match = re.search(r"(\d+)\s*(?:秒|sec(?:ond)?s?)", lower)
        seconds = int(hour_match.group(1)) * 3600 if hour_match else 0
        seconds += int(minute_match.group(1)) * 60 if minute_match else 0
        seconds += int(second_match.group(1)) if second_match else 0
        return max(1, round(seconds / 60)) if seconds > 0 else 0

    def _annual_runtime_minutes(self, ticks: Any) -> int:
        try:
            value = float(ticks or 0)
        except Exception:
            return 0
        if value <= 0:
            return 0
        return max(1, round(value / 10000000 / 60))

    def _annual_type_from_log(self, log: dict[str, Any], title: str) -> str:
        raw = str(log.get("ItemType") or (log.get("Item") or {}).get("Type") or log.get("Type") or "").lower()
        return self._annual_normalize_type(raw, title)

    def _annual_normalize_type(self, value: str, title: str = "") -> str:
        raw = str(value or "").lower()
        if "movie" in raw:
            return "movie"
        if "series" in raw or "episode" in raw or "season" in raw:
            return "series"
        hint = str(title or "").lower()
        if re.search(r"s\d+\s*[,.-]?\s*e?p?\d+|第\s*\d+\s*集|season|第\s*\d+\s*季", hint):
            return "series"
        return "other"

    def _build_annual_scope_options(self, events: list[dict[str, Any]]) -> list[dict[str, str]]:
        counts: dict[str, int] = {}
        for event in events:
            name = str(event.get("userName") or "").strip()
            if name:
                counts[name] = counts.get(name, 0) + 1
        users = sorted(counts, key=lambda name: (-counts[name], name.lower()))
        return [{"value": "all", "label": "全服"}, *[{"value": f"user:{urllib.parse.quote(name)}", "label": name} for name in users]]

    def _filter_annual_events_by_scope(self, events: list[dict[str, Any]], scope: str) -> list[dict[str, Any]]:
        value = str(scope or "")
        if not value.startswith("user:"):
            return events
        try:
            user_name = urllib.parse.unquote(value[5:]).strip().lower()
        except Exception:
            user_name = ""
        if not user_name:
            return events
        return [event for event in events if str(event.get("userName") or "").strip().lower() == user_name]

    def _annual_pick_best_search_item(self, search_title: str, rows: list[dict[str, Any]]) -> dict[str, Any] | None:
        if not rows:
            return None
        key = self._annual_lookup_key(search_title)
        normalized = [row for row in rows if isinstance(row, dict)]
        exact = [row for row in normalized if self._annual_lookup_key(str(row.get("Name") or "")) == key]
        if exact:
            normalized = exact
        preferred: list[dict[str, Any]] = []
        for row in normalized:
            row_type = str(row.get("Type") or "").lower()
            score = 0
            if row_type == "series":
                score += 4
            elif row_type == "movie":
                score += 3
            elif row_type == "season":
                score += 2
            elif row_type == "episode":
                score += 1
            image_tags = row.get("ImageTags") if isinstance(row.get("ImageTags"), dict) else {}
            if str(image_tags.get("Primary") or "").strip():
                score += 2
            if str(row.get("PrimaryImageItemId") or "").strip():
                score += 1
            row["_searchScore"] = score
            preferred.append(row)
        preferred.sort(key=lambda row: (int(row.get("_searchScore") or 0), len(str(row.get("Name") or ""))), reverse=True)
        return preferred[0] if preferred else None

    def _annual_search_item_by_title(self, *, api_base: str, api_key: str, title: str) -> dict[str, Any] | None:
        safe_title = str(title or "").strip()
        if not safe_title:
            return None
        query = urllib.parse.urlencode(
            {
                "Recursive": "true",
                "SearchTerm": safe_title,
                "IncludeItemTypes": "Series,Movie,Season,Episode",
                "Fields": "Name,SeriesName,SeriesId,SeasonId,ParentId,Type,ImageTags,PrimaryImageItemId,ProductionYear,PremiereDate",
                "Limit": "12",
            }
        )
        try:
            result = self._emby_request(base_url=api_base, api_key=api_key, path=f"/Items?{query}", method="GET")
        except Exception:
            return None
        rows = result.get("Items") if isinstance(result, dict) else result
        if not isinstance(rows, list):
            return None
        return self._annual_pick_best_search_item(safe_title, [row for row in rows if isinstance(row, dict)])

    def _annual_resolve_event_item_ids_by_title(
        self,
        *,
        api_base: str,
        api_key: str,
        events: list[dict[str, Any]],
        debug: dict[str, Any],
    ) -> None:
        unresolved: dict[str, str] = {}
        for event in events:
            if str(event.get("itemId") or "").strip():
                continue
            title = str(event.get("title") or "").strip()
            if not title or title == "未命名内容":
                continue
            key = self._annual_lookup_key(title)
            if key and key not in unresolved:
                unresolved[key] = title
        if not unresolved:
            return

        resolved_map: dict[str, dict[str, Any] | None] = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
            future_map = {
                executor.submit(self._annual_search_item_by_title, api_base=api_base, api_key=api_key, title=title): key
                for key, title in unresolved.items()
            }
            for future in concurrent.futures.as_completed(future_map):
                key = future_map[future]
                try:
                    resolved_map[key] = future.result()
                except Exception:
                    resolved_map[key] = None

        for event in events:
            if str(event.get("itemId") or "").strip():
                continue
            key = self._annual_lookup_key(str(event.get("title") or ""))
            detail = resolved_map.get(key) if key else None
            if not isinstance(detail, dict):
                continue
            resolved_id = str(detail.get("Id") or detail.get("SeriesId") or detail.get("ParentId") or "").strip()
            if not resolved_id:
                continue
            event["itemId"] = resolved_id
            detail_type = str(detail.get("Type") or "").strip().lower()
            if detail_type:
                event["type"] = self._annual_normalize_type(detail_type, str(event.get("title") or ""))
            sample = {
                "title": str(event.get("title") or ""),
                "userName": str(event.get("userName") or ""),
                "itemId": resolved_id,
                "playedAt": str(event.get("playedAt") or ""),
                "unknownTime": bool(event.get("unknownTime")),
                "type": str(event.get("type") or "other"),
                "resolvedBy": "title_search",
            }
            self._annual_debug_push(debug["sampleMatchedEvents"], sample)

    def _fetch_annual_item_details_for_events(
        self,
        *,
        api_base: str,
        api_key: str,
        events: list[dict[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        ids = sorted({str(event.get("itemId") or "").strip() for event in events if str(event.get("itemId") or "").strip()})
        details = self._fetch_annual_items_by_ids(api_base=api_base, api_key=api_key, item_ids=ids)
        for _ in range(2):
            related_ids: set[str] = set()
            for detail in list(details.values()):
                related_ids.update(self._annual_related_item_ids(detail))
            missing = sorted(item_id for item_id in related_ids if item_id and item_id not in details)
            if not missing:
                break
            details.update(self._fetch_annual_items_by_ids(api_base=api_base, api_key=api_key, item_ids=missing))
        return details

    def _fetch_annual_items_by_ids(self, *, api_base: str, api_key: str, item_ids: list[str]) -> dict[str, dict[str, Any]]:
        ids = [str(item_id or "").strip() for item_id in item_ids if str(item_id or "").strip()]
        if not ids:
            return {}
        fields = ",".join(
            [
                "Name",
                "SortName",
                "Overview",
                "ProductionYear",
                "PremiereDate",
                "ImageTags",
                "PrimaryImageItemId",
                "Type",
                "RunTimeTicks",
                "SeriesName",
                "SeriesId",
                "SeasonId",
                "ParentId",
                "IndexNumber",
                "ParentIndexNumber",
                "UserData",
            ]
        )

        def fetch_chunk(chunk_ids: list[str]) -> list[dict[str, Any]]:
            query = urllib.parse.urlencode(
                {
                    "Ids": ",".join(chunk_ids),
                    "Fields": fields,
                    "Limit": str(len(chunk_ids)),
                    "Recursive": "true",
                }
            )
            try:
                result = self._emby_request(base_url=api_base, api_key=api_key, path=f"/Items?{query}", method="GET")
            except Exception:
                rows: list[dict[str, Any]] = []
                for item_id in chunk_ids:
                    try:
                        detail = self._emby_request(
                            base_url=api_base,
                            api_key=api_key,
                            path=f"/Items/{urllib.parse.quote(item_id, safe='')}?{urllib.parse.urlencode({'Fields': fields})}",
                            method="GET",
                        )
                    except Exception:
                        detail = None
                    if isinstance(detail, dict):
                        rows.append(detail)
                return rows
            items = result.get("Items") if isinstance(result, dict) else result
            return [item for item in items if isinstance(item, dict)] if isinstance(items, list) else []

        chunks = [ids[index : index + 100] for index in range(0, len(ids), 100)]
        details: dict[str, dict[str, Any]] = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
            for rows in executor.map(fetch_chunk, chunks):
                for item in rows:
                    item_id = str(item.get("Id") or "").strip()
                    if item_id:
                        details[item_id] = item
        return details

    def _annual_related_item_ids(self, detail: dict[str, Any]) -> set[str]:
        ids: set[str] = set()
        for key in ("SeriesId", "SeasonId", "ParentId", "PrimaryImageItemId"):
            value = str(detail.get(key) or "").strip()
            if value:
                ids.add(value)
        return ids

    def _aggregate_annual_items(
        self,
        *,
        api_base: str,
        api_key: str,
        events: list[dict[str, Any]],
        item_details: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        rows: dict[str, dict[str, Any]] = {}
        for event in events:
            detail = item_details.get(str(event.get("itemId") or "").strip(), {})
            aggregate = self._annual_resolve_aggregate(
                event,
                detail,
                item_details,
                api_base=api_base,
                api_key=api_key,
            )
            key = str(aggregate.get("key") or "").strip()
            if not key:
                continue
            current = rows.get(key)
            if current is None:
                current = {
                    "id": str(aggregate.get("id") or key),
                    "itemId": str(aggregate.get("id") or ""),
                    "name": str(aggregate.get("name") or "未命名内容"),
                    "title": str(aggregate.get("name") or "未命名内容"),
                    "type": str(aggregate.get("type") or "other"),
                    "year": aggregate.get("year") or None,
                    "overview": str(aggregate.get("overview") or ""),
                    "imageTag": str(aggregate.get("imageTag") or ""),
                    "imageUrl": str(aggregate.get("imageUrl") or ""),
                    "playCount": 0,
                    "duration": 0,
                    "totalDurationMinutes": 0,
                    "lastPlayed": "",
                }
            current["playCount"] = int(current.get("playCount") or 0) + 1
            fallback_minutes = self._annual_runtime_minutes(detail.get("RunTimeTicks")) if detail else 0
            minutes = int(event.get("durationMin") or 0) or fallback_minutes
            current["duration"] = int(current.get("duration") or 0) + max(0, minutes)
            current["totalDurationMinutes"] = current["duration"]
            if self._annual_datetime_ts(str(event.get("playedAt") or "")) >= self._annual_datetime_ts(str(current.get("lastPlayed") or "")):
                current["lastPlayed"] = str(event.get("playedAt") or "")
            rows[key] = current
        return list(rows.values())

    def _annual_resolve_aggregate(
        self,
        event: dict[str, Any],
        detail: dict[str, Any],
        item_details: dict[str, dict[str, Any]],
        api_base: str,
        api_key: str,
    ) -> dict[str, Any]:
        event_title = self._annual_normalize_media_title(str(event.get("title") or ""))
        raw_type = str(detail.get("Type") or "").lower()
        event_type = str(event.get("type") or "other")
        aggregate_type = self._annual_normalize_type(raw_type or event_type, event_title)
        aggregate_id = str(detail.get("Id") or event.get("itemId") or "").strip()
        aggregate_detail = detail

        if raw_type == "episode":
            aggregate_id = str(detail.get("SeriesId") or detail.get("PrimaryImageItemId") or detail.get("ParentId") or aggregate_id).strip()
            aggregate_type = "series"
            aggregate_detail = item_details.get(aggregate_id, detail)
        elif raw_type == "season":
            aggregate_id = str(detail.get("SeriesId") or detail.get("ParentId") or aggregate_id).strip()
            aggregate_type = "series"
            aggregate_detail = item_details.get(aggregate_id, detail)
        elif raw_type == "series":
            aggregate_type = "series"
        elif raw_type == "movie":
            aggregate_type = "movie"

        name = str(
            aggregate_detail.get("Name")
            or detail.get("SeriesName")
            or detail.get("Name")
            or event_title
            or "未命名内容"
        ).strip()
        year = self._annual_item_year(aggregate_detail) or self._annual_item_year(detail)
        overview = str(aggregate_detail.get("Overview") or detail.get("Overview") or "").strip()
        image_item_id, image_tag = self._annual_primary_image_source(aggregate_detail, detail, aggregate_id)
        image_url = self._annual_primary_image_url(
            api_base=api_base,
            api_key=api_key,
            item_id=image_item_id,
            image_tag=image_tag,
        )
        key = f"{aggregate_type}:{aggregate_id}" if aggregate_id else f"title:{aggregate_type}:{self._annual_lookup_key(name)}"
        return {
            "key": key,
            "id": aggregate_id or key,
            "name": self._annual_normalize_media_title(name),
            "type": aggregate_type,
            "year": year,
            "overview": overview,
            "imageTag": image_tag,
            "imageUrl": image_url,
        }

    def _annual_primary_image_source(
        self,
        aggregate_detail: dict[str, Any],
        fallback_detail: dict[str, Any],
        aggregate_id: str,
    ) -> tuple[str, str]:
        for detail in (aggregate_detail, fallback_detail):
            item_id = str(detail.get("Id") or "").strip()
            image_tags = detail.get("ImageTags") if isinstance(detail.get("ImageTags"), dict) else {}
            primary_tag = str(image_tags.get("Primary") or "").strip()
            if item_id and primary_tag:
                return item_id, primary_tag
            primary_image_item_id = str(detail.get("PrimaryImageItemId") or "").strip()
            if primary_image_item_id:
                return primary_image_item_id, primary_tag
            if item_id:
                return item_id, primary_tag
        return str(aggregate_id or "").strip(), ""

    def _annual_primary_image_url(self, *, api_base: str, api_key: str, item_id: str, image_tag: str = "", max_width: int = 420) -> str:
        safe_item_id = str(item_id or "").strip()
        if not api_base or not safe_item_id:
            return ""
        params = {"maxWidth": str(max(120, int(max_width or 420))), "quality": "88"}
        if api_key:
            params["api_key"] = api_key
        if image_tag:
            params["tag"] = image_tag
        return f"{api_base.rstrip('/')}/Items/{urllib.parse.quote(safe_item_id, safe='')}/Images/Primary?{urllib.parse.urlencode(params)}"

    def _annual_item_year(self, detail: dict[str, Any]) -> int | None:
        try:
            production_year = int(detail.get("ProductionYear") or 0)
            if production_year > 0:
                return production_year
        except Exception:
            pass
        premiere = str(detail.get("PremiereDate") or "")
        match = re.match(r"^(\d{4})", premiere)
        if match:
            return int(match.group(1))
        return None

    def _should_dedupe_webhook(self, key: str, *, window_seconds: int = 15) -> bool:
        safe_key = str(key or "").strip()
        if not safe_key:
            return False
        now = time.time()
        with LAST_WEBHOOK_LOCK:
            expired = [k for k, ts in RECENT_WEBHOOK_EVENTS.items() if now - float(ts) > max(window_seconds * 4, 45)]
            for k in expired:
                RECENT_WEBHOOK_EVENTS.pop(k, None)
            previous = RECENT_WEBHOOK_EVENTS.get(safe_key)
            if previous is not None and now - float(previous) < window_seconds:
                return True
            RECENT_WEBHOOK_EVENTS[safe_key] = now
        return False

    def _build_playback_payload(
        self,
        payload: dict[str, Any],
        *,
        action: str,
        event_name: str,
        emby_config: dict[str, Any],
        bot_config: dict[str, Any],
    ) -> dict[str, str]:
        session_id = self._extract_session_id(payload)
        session_detail = self._fetch_emby_session_detail(emby_config=emby_config, session_id=session_id)
        if session_detail:
            merged_payload = dict(payload)
            for key in ("UserName", "DeviceName", "Client", "RemoteEndPoint"):
                if not str(merged_payload.get(key) or "").strip() and str(session_detail.get(key) or "").strip():
                    merged_payload[key] = session_detail.get(key)
            if not isinstance(merged_payload.get("Session"), dict):
                merged_payload["Session"] = {}
            if isinstance(merged_payload.get("Session"), dict):
                session_map = dict(merged_payload["Session"])
                for key in ("UserName", "DeviceName", "Client", "RemoteEndPoint"):
                    if not str(session_map.get(key) or "").strip() and str(session_detail.get(key) or "").strip():
                        session_map[key] = session_detail.get(key)
                merged_payload["Session"] = session_map
            payload = merged_payload

        item_id = self._extract_item_id(payload)
        item_detail = self._fetch_emby_item_detail(emby_config=emby_config, item_id=item_id)

        user_name = self._pick_first_value(
            payload,
            [("UserName",), ("userName",), ("User", "Name"), ("user", "name"), ("Session", "UserName"), ("session", "userName")],
        ) or "未知用户"
        item_name = self._pick_first_value(
            payload,
            [
                ("ItemName",),
                ("itemName",),
                ("NowPlayingItem", "Name"),
                ("nowPlayingItem", "name"),
                ("Item", "Name"),
                ("item", "name"),
                ("Name",),
            ],
        )
        series_name = self._pick_first_value(
            payload,
            [("SeriesName",), ("seriesName",), ("NowPlayingItem", "SeriesName"), ("nowPlayingItem", "seriesName"), ("Item", "SeriesName")],
        ) or str(item_detail.get("SeriesName") or "").strip()
        if not item_name:
            item_name = str(item_detail.get("Name") or "").strip() or "未知内容"

        content_type = self._pick_first_value(payload, [("ItemType",), ("itemType",), ("Type",), ("type",)])
        if not content_type:
            content_type = str(item_detail.get("Type") or "").strip()
        content_type = normalize_content_type(content_type)

        rating = self._safe_float(
            self._pick_first_value(payload, [("CommunityRating",), ("communityRating",), ("Item", "CommunityRating"), ("item", "communityRating")])
            or item_detail.get("CommunityRating")
        )
        rating_text = f"{rating:.1f}/10" if rating is not None and rating > 0 else ""

        position_ticks = self._pick_first_value(
            payload,
            [("PositionTicks",), ("positionTicks",), ("PlaybackPositionTicks",), ("Session", "PlayState", "PositionTicks"), ("session", "playState", "positionTicks")],
        )
        runtime_ticks = self._pick_first_value(
            payload,
            [("RunTimeTicks",), ("runTimeTicks",), ("Item", "RunTimeTicks"), ("item", "runTimeTicks"), ("Session", "NowPlayingItem", "RunTimeTicks")],
        )
        position_sec = ticks_to_seconds(position_ticks)
        runtime_sec = ticks_to_seconds(runtime_ticks or item_detail.get("RunTimeTicks"))
        percent = ""
        if runtime_sec > 0:
            ratio = max(0.0, min(1.0, float(position_sec) / float(runtime_sec)))
            percent = f"{int(round(ratio * 100))}%"

        device_name = self._pick_first_value(
            payload,
            [("DeviceName",), ("deviceName",), ("Session", "DeviceName"), ("session", "deviceName"), ("Client",), ("client")],
        )
        overview = self._pick_first_value(payload, [("Overview",), ("overview",), ("Item", "Overview"), ("item", "overview")]) or str(item_detail.get("Overview") or "").strip()

        poster_url, detail_url = self._build_emby_item_urls(emby_config=emby_config, item_id=item_id)

        caption = compose_playback_message(
            payload=payload,
            item_detail=item_detail,
            action=action,
            username=user_name,
            series_name=series_name,
            item_name=item_name,
            content_type=content_type,
            rating_text=rating_text,
            position_sec=position_sec,
            runtime_sec=runtime_sec,
            percent_text=percent,
            device_name=device_name,
            overview=overview,
            show_ip=bool(bot_config.get("showIp", True)),
            show_ip_geo=bool(bot_config.get("showIpGeo", True)),
            show_overview=bool(bot_config.get("showOverview", True)),
        )
        if len(caption) > 1000:
            caption = self._shorten(caption, limit=1000)
        return {
            "caption": caption,
            "posterUrl": poster_url,
            "detailUrl": detail_url,
            "itemId": item_id,
            "eventName": str(event_name or "").strip(),
        }

    def _library_event_source_text(self, payload: dict[str, Any], event_name: str) -> str:
        values = [str(event_name or "").strip()]
        for keys in (
            ("Event",),
            ("event",),
            ("NotificationType",),
            ("notificationType",),
            ("Type",),
            ("type",),
            ("Name",),
            ("name",),
            ("MessageType",),
            ("messageType",),
            ("EventId",),
            ("eventId",),
            ("Action",),
            ("action",),
        ):
            value = self._get_payload_str(payload, *keys)
            if value:
                values.append(value)
        return " ".join(values).lower()

    def _is_library_item_added_event(self, payload: dict[str, Any], event_name: str) -> bool:
        source = self._library_event_source_text(payload, event_name)
        if not source:
            return False
        if any(token in source for token in ("scanfinished", "scancompleted", "scan finished", "scan completed", "扫描完成")):
            return False
        return any(token in source for token in ("itemadded", "newitem", "newitems", "library.new", "新增", "入库"))

    def _build_library_payload(
        self,
        payload: dict[str, Any],
        *,
        event_name: str,
        emby_config: dict[str, Any],
    ) -> dict[str, str]:
        item_id = self._extract_item_id(payload)
        item_detail = self._fetch_emby_item_detail(emby_config=emby_config, item_id=item_id)
        joined = dict(payload)
        if item_detail:
            joined.update(item_detail)

        title = self._library_title(joined)
        year = self._library_year(joined)
        item_type = str(joined.get("Type") or "").strip().lower()
        genres = self._library_genres(joined)
        category = genres or self._library_type_label(item_type)
        content_type = self._library_content_type_text(item_type, genres)
        rating = self._library_rating(joined)
        created_at = self._library_datetime(joined.get("DateCreated") or joined.get("PremiereDate"))
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        episode_info = self._library_episode_info(joined)
        people = self._library_people(joined)
        studios = self._library_studios(joined)
        media_summary = self._library_media_summary(joined)
        size_text = self._library_file_size(joined)
        overview = self._shorten(str(joined.get("Overview") or "暂无简介").replace("\n", " "), limit=360)

        lines = [
            f"🎬 {title}（{year}）" if year else f"🎬 {title}",
            "=== 基本信息 ===",
            f"🕒 整理时间 | {now}",
            "📺 内容状态 | 整理完成",
        ]
        if episode_info:
            lines.append(f"🎞 剧集信息 | {episode_info}")
        lines.extend(
            [
                f"🎭 内容分类 | {category or '未分类'}",
                "=== 媒体信息 ===",
                f"🏷 内容类型 | {content_type}",
            ]
        )
        if rating:
            lines.append(f"⭐ 用户评分 | {rating}")
        if created_at:
            lines.append(f"📅 数据入库 | {created_at}")
        if people:
            lines.extend(["=== 创作信息 ===", f"👥 主演阵容 | {people}"])
        lines.append("=== 资源详情 ===")
        if studios:
            lines.append(f"👥 发布小组 | {studios}")
        if media_summary:
            lines.append(f"🧾 资源规格 | {media_summary}")
        if size_text:
            lines.append(f"📦 文件大小 | {size_text}")
        lines.extend(["=== 内容简介 ===", f"📜 {overview}"])

        caption = "\n".join(lines)
        if len(caption) > 1000:
            caption = self._shorten(caption, limit=1000)
        poster_url, detail_url = self._build_emby_item_urls(emby_config=emby_config, item_id=item_id)
        return {
            "caption": caption,
            "posterUrl": poster_url,
            "detailUrl": detail_url,
            "itemId": item_id,
            "eventName": str(event_name or "").strip(),
        }

    def _library_title(self, detail: dict[str, Any]) -> str:
        item_type = str(detail.get("Type") or "").strip().lower()
        if item_type == "episode":
            return str(detail.get("SeriesName") or detail.get("Name") or "未命名内容").strip()
        return str(detail.get("Name") or detail.get("ItemName") or detail.get("SeriesName") or "未命名内容").strip()

    @staticmethod
    def _library_year(detail: dict[str, Any]) -> str:
        year = detail.get("ProductionYear")
        if isinstance(year, int) and year > 0:
            return str(year)
        premiere = str(detail.get("PremiereDate") or "").strip()
        if re.match(r"^\d{4}", premiere):
            return premiere[:4]
        return ""

    @staticmethod
    def _library_datetime(raw: Any) -> str:
        value = str(raw or "").strip()
        if not value:
            return ""
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone().strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return value[:19].replace("T", " ")

    @staticmethod
    def _library_type_label(item_type: str) -> str:
        mapping = {
            "movie": "电影",
            "series": "电视剧",
            "season": "季",
            "episode": "剧集",
            "audio": "音频",
        }
        return mapping.get(str(item_type or "").lower(), "媒体")

    def _library_content_type_text(self, item_type: str, genres: str) -> str:
        base = self._library_type_label(item_type)
        if genres and genres != base:
            return f"{base} · {genres}"
        return base

    @staticmethod
    def _library_genres(detail: dict[str, Any]) -> str:
        genres = detail.get("Genres") if isinstance(detail.get("Genres"), list) else []
        rows = [str(row).strip() for row in genres if str(row).strip()]
        return " / ".join(rows[:3])

    @staticmethod
    def _library_rating(detail: dict[str, Any]) -> str:
        try:
            rating = float(detail.get("CommunityRating") or 0)
        except (TypeError, ValueError):
            return ""
        if rating <= 0:
            return ""
        return f"{rating:.1f}".rstrip("0").rstrip(".")

    @staticmethod
    def _library_episode_info(detail: dict[str, Any]) -> str:
        item_type = str(detail.get("Type") or "").strip().lower()
        season = AppHandler._library_int(detail.get("ParentIndexNumber"))
        episode = AppHandler._library_int(detail.get("IndexNumber"))
        if item_type == "episode":
            season_text = f"S{season:02d}" if season is not None else "SXX"
            episode_text = f"E{episode:02d}" if episode is not None else "EXX"
            return f"{season_text} {episode_text}"
        if item_type in {"season", "series"}:
            count = AppHandler._library_int(detail.get("ChildCount")) or AppHandler._library_int(detail.get("RecursiveItemCount"))
            if count and count > 0:
                season_text = f"S{season:02d}" if season is not None else "SXX"
                return f"{season_text} E01-E{count:02d}"
        return ""

    @staticmethod
    def _library_int(raw: Any) -> int | None:
        try:
            value = int(float(raw))
        except (TypeError, ValueError):
            return None
        return value if value >= 0 else None

    @staticmethod
    def _library_people(detail: dict[str, Any]) -> str:
        people = detail.get("People") if isinstance(detail.get("People"), list) else []
        rows: list[str] = []
        for person in people:
            if not isinstance(person, dict):
                continue
            name = str(person.get("Name") or "").strip()
            if name:
                rows.append(name)
        return "、".join(rows[:5])

    @staticmethod
    def _library_studios(detail: dict[str, Any]) -> str:
        studios = detail.get("Studios") if isinstance(detail.get("Studios"), list) else []
        rows: list[str] = []
        for studio in studios:
            if isinstance(studio, dict):
                name = str(studio.get("Name") or "").strip()
            else:
                name = str(studio or "").strip()
            if name:
                rows.append(name)
        return "、".join(rows[:3])

    @staticmethod
    def _library_media_summary(detail: dict[str, Any]) -> str:
        sources = detail.get("MediaSources") if isinstance(detail.get("MediaSources"), list) else []
        source = sources[0] if sources and isinstance(sources[0], dict) else {}
        streams = source.get("MediaStreams") if isinstance(source.get("MediaStreams"), list) else detail.get("MediaStreams")
        streams = streams if isinstance(streams, list) else []
        container = str(source.get("Container") or detail.get("Container") or "").strip().upper()
        video = next((row for row in streams if isinstance(row, dict) and str(row.get("Type") or "").lower() == "video"), {})
        audio = next((row for row in streams if isinstance(row, dict) and str(row.get("Type") or "").lower() == "audio"), {})
        width = video.get("Width") if isinstance(video, dict) else None
        height = video.get("Height") if isinstance(video, dict) else None
        resolution = ""
        if isinstance(height, int) and height > 0:
            resolution = f"{height}p"
        elif isinstance(width, int) and width >= 3000:
            resolution = "4K"
        video_codec = str(video.get("Codec") or "").strip().upper() if isinstance(video, dict) else ""
        audio_codec = str(audio.get("Codec") or "").strip().upper() if isinstance(audio, dict) else ""
        rows = [value for value in (container, resolution, video_codec, audio_codec) if value]
        return " · ".join(rows[:4])

    @staticmethod
    def _library_file_size(detail: dict[str, Any]) -> str:
        sources = detail.get("MediaSources") if isinstance(detail.get("MediaSources"), list) else []
        source = sources[0] if sources and isinstance(sources[0], dict) else {}
        raw_size = source.get("Size") or detail.get("Size")
        try:
            size = int(raw_size or 0)
        except (TypeError, ValueError):
            return ""
        if size <= 0:
            return ""
        units = ["B", "KB", "MB", "GB", "TB"]
        value = float(size)
        unit = units[0]
        for unit in units:
            if value < 1024 or unit == units[-1]:
                break
            value /= 1024
        return f"{value:.2f}{unit}".rstrip("0").rstrip(".") if unit == "B" else f"{value:.2f}{unit}"

    def _build_webhook_message(self, payload: dict[str, Any], *, event_type: str, event_name: str) -> str:
        if event_type == "playback":
            title = "播放状态通知"
        elif event_type == "library":
            title = "新资源入库通知"
        else:
            title = "系统事件通知"

        item_name = ""
        for keys in (
            ("ItemName",),
            ("Name",),
            ("itemName",),
            ("Item", "Name"),
            ("item", "name"),
            ("SeriesName",),
            ("Item", "SeriesName"),
        ):
            candidate = self._get_payload_str(payload, *keys)
            if candidate:
                item_name = candidate
                break

        user_name = ""
        for keys in (
            ("UserName",),
            ("userName",),
            ("User", "Name"),
            ("user", "name"),
            ("Session", "UserName"),
            ("session", "userName"),
        ):
            candidate = self._get_payload_str(payload, *keys)
            if candidate:
                user_name = candidate
                break

        server_name = ""
        for keys in (("ServerName",), ("serverName",), ("Server", "Name"), ("server", "name")):
            candidate = self._get_payload_str(payload, *keys)
            if candidate:
                server_name = candidate
                break

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        lines = ["镜界Vistamirror 通知", f"类型：{title}"]
        if event_name:
            lines.append(f"事件：{event_name}")
        if item_name:
            lines.append(f"内容：{item_name}")
        if user_name:
            lines.append(f"用户：{user_name}")
        if server_name:
            lines.append(f"服务器：{server_name}")
        lines.append(f"时间：{now}")
        return "\n".join(lines)

    def _handle_bot_test(self) -> None:
        payload = self._read_json_body()
        if payload is None:
            return

        channel = str(payload.get("channel") or "").strip().lower() or "telegram"
        if channel != "telegram":
            self._send_json(400, {"error": "目前仅支持 Telegram 测试"})
            return

        with STORE_LOCK:
            store = _read_store_unlocked()
            bot_config = _apply_bot_env_overrides(store.get("botConfig"))

        token = str(bot_config.get("telegramToken") or "").strip()
        chat_id = str(bot_config.get("telegramChatId") or "").strip()
        if not token or not chat_id:
            self._send_json(400, {"error": "请先保存 Telegram Token 和 Chat ID"})
            return

        message = "\n".join(
            [
                "【镜界 VistaMirror】测试通知",
                "",
                "▸ 状态  ✅ 通道可用",
                f"▸ 时间  🕐 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            ]
        )

        try:
            self._send_telegram_text(token=token, chat_id=chat_id, text=message)
        except ValueError as err:
            self._send_json(400, {"error": str(err)})
            return
        except RuntimeError as err:
            self._send_json(502, {"error": str(err)})
            return
        except Exception as err:
            self._send_json(502, {"error": f"Telegram 发送失败：{err}"})
            return

        self._send_json(200, {"ok": True, "detail": "Telegram 测试消息发送成功"})

    def _handle_emby_webhook(self, raw_query: str) -> None:
        self._mark_webhook_received()
        params = urllib.parse.parse_qs(raw_query or "")
        token = str((params.get("token") or [""])[0]).strip()
        expected = str(os.environ.get("BOT_WEBHOOK_TOKEN") or DEFAULT_WEBHOOK_TOKEN).strip() or DEFAULT_WEBHOOK_TOKEN
        if token != expected:
            self._record_webhook_status(event_type="unknown", result="token_invalid", detail="Webhook token 无效")
            self._send_json(403, {"error": "Webhook token 无效"})
            return

        payload = self._read_json_body()
        if payload is None:
            self._record_webhook_status(event_type="unknown", result="invalid_payload", detail="Webhook 请求体无效")
            return

        with STORE_LOCK:
            store = _read_store_unlocked()
            bot_config = _apply_bot_env_overrides(store.get("botConfig"))
            emby_config = _apply_emby_env_overrides(store.get("embyConfig"))

        if not bot_config.get("enableCore", True):
            self._record_webhook_status(event_type="unknown", result="core_disabled", detail="总开关已关闭")
            self._send_json(200, {"ok": True, "skipped": "core_disabled"})
            return

        event_type, event_name = self._classify_webhook_type(payload)
        if event_type == "playback" and not bot_config.get("enablePlayback", True):
            self._record_webhook_status(event_type=event_type, result="playback_disabled", detail="播放通知开关已关闭")
            self._send_json(200, {"ok": True, "skipped": "playback_disabled"})
            return
        if event_type == "library" and not bot_config.get("enableLibrary", True):
            self._record_webhook_status(event_type=event_type, result="library_disabled", detail="入库通知开关已关闭")
            self._send_json(200, {"ok": True, "skipped": "library_disabled"})
            return
        if event_type == "other":
            self._record_webhook_status(event_type=event_type, result="unsupported_event", detail="未识别事件类型")
            self._send_json(200, {"ok": True, "skipped": "unsupported_event"})
            return

        token_value = str(bot_config.get("telegramToken") or "").strip()
        chat_id = str(bot_config.get("telegramChatId") or "").strip()
        if not token_value or not chat_id:
            self._record_webhook_status(event_type=event_type, result="telegram_not_configured", detail="Telegram 未配置完整")
            self._send_json(200, {"ok": True, "skipped": "telegram_not_configured"})
            return

        if event_type == "playback":
            action = self._detect_playback_action(payload, event_name)
            if action not in {"start", "pause", "resume", "stop"}:
                self._record_webhook_status(event_type=event_type, result="playback_event_filtered", detail="仅开始/暂停/恢复/停止播放会推送")
                self._send_json(200, {"ok": True, "skipped": "playback_event_filtered"})
                return

            if not event_enabled(bot_config, action):
                self._record_webhook_status(event_type=event_type, result="playback_event_disabled", detail=f"{action} 事件已关闭")
                self._send_json(200, {"ok": True, "skipped": "playback_event_disabled"})
                return

            session_id = self._extract_session_id(payload)
            item_id = self._extract_item_id(payload)
            user_name = self._pick_first_value(payload, [("UserName",), ("userName",), ("Session", "UserName"), ("session", "userName")])
            media_name = maybe_extract_media_name(payload)
            dedupe_key = build_dedupe_key(username=user_name, item_id=item_id, media_name=media_name, action=action)
            dedupe_window = int(bot_config.get("eventDedupSeconds") or 10)
            if self._should_dedupe_webhook(dedupe_key, window_seconds=max(1, dedupe_window)):
                self._record_webhook_status(event_type=event_type, result="duplicate_skipped", detail="重复事件已去重")
                self._send_json(200, {"ok": True, "skipped": "duplicate_skipped"})
                return

            ip_for_log = build_ip_display(
                payload,
                show_ip=bool(bot_config.get("showIp", True)),
                show_geo=bool(bot_config.get("showIpGeo", True)),
            )
            device_name = self._pick_first_value(
                payload,
                [("DeviceName",), ("deviceName",), ("Session", "DeviceName"), ("session", "deviceName"), ("Client",), ("client")],
            )
            append_playback_event(
                PLAYBACK_EVENT_LOG_FILE,
                username=user_name,
                media_name=media_name,
                event_type=action,
                device=device_name,
                ip=ip_for_log,
                raw_payload=payload if isinstance(payload, dict) else {},
            )

            card = self._build_playback_payload(
                payload,
                action=action,
                event_name=event_name,
                emby_config=emby_config,
                bot_config=bot_config,
            )
            caption = card.get("caption") or "播放状态通知"
            poster_url = str(card.get("posterUrl") or "").strip()
            detail_url = str(card.get("detailUrl") or "").strip()
            try:
                if poster_url:
                    try:
                        self._send_telegram_photo(
                            token=token_value,
                            chat_id=chat_id,
                            photo_url=poster_url,
                            caption=caption,
                            button_text="🔗 跳转详情",
                            button_url=detail_url,
                        )
                    except RuntimeError as err:
                        lowered = str(err).lower()
                        should_fallback = any(
                            key in lowered
                            for key in ("wrong type", "failed to get http url", "wrong file identifier", "http url", "bad request")
                        )
                        if not should_fallback:
                            raise
                        self._send_telegram_text(token=token_value, chat_id=chat_id, text=caption)
                else:
                    self._send_telegram_text(token=token_value, chat_id=chat_id, text=caption)
            except ValueError as err:
                self._record_webhook_status(event_type=event_type, result="telegram_error", detail=str(err))
                self._send_json(400, {"error": str(err)})
                return
            except RuntimeError as err:
                self._record_webhook_status(event_type=event_type, result="telegram_error", detail=str(err))
                self._send_json(502, {"error": str(err)})
                return
            except Exception as err:
                self._record_webhook_status(event_type=event_type, result="telegram_error", detail=str(err))
                self._send_json(502, {"error": f"Telegram 发送失败：{err}"})
                return

            action_text = action
            self._record_webhook_status(event_type=event_type, result="sent", detail=f"Telegram 富媒体推送成功（{action_text}）")
            self._send_json(200, {"ok": True, "sent": True, "eventType": event_type, "action": action})
            return

        if event_type == "library":
            item_id = self._extract_item_id(payload)
            if not item_id:
                self._record_webhook_status(event_type=event_type, result="library_item_missing", detail="入库事件未包含具体资源 ID，已跳过")
                self._send_json(200, {"ok": True, "skipped": "library_item_missing"})
                return
            if not self._is_library_item_added_event(payload, event_name):
                self._record_webhook_status(event_type=event_type, result="library_event_filtered", detail="非新增入库资源事件，已跳过")
                self._send_json(200, {"ok": True, "skipped": "library_event_filtered"})
                return

            dedupe_window = int(bot_config.get("eventDedupSeconds") or 10)
            dedupe_key = f"library|{item_id}|{event_name or 'item_added'}"
            if self._should_dedupe_webhook(dedupe_key, window_seconds=max(1, dedupe_window)):
                self._record_webhook_status(event_type=event_type, result="duplicate_skipped", detail="重复入库事件已去重")
                self._send_json(200, {"ok": True, "skipped": "duplicate_skipped"})
                return

            try:
                if TELEGRAM_COMMAND_SERVICE is None:
                    raise RuntimeError("Telegram 入库通知服务尚未启动")
                notify_result = TELEGRAM_COMMAND_SERVICE.notify_library_item(
                    item_id=item_id,
                    payload=payload,
                    source="webhook",
                )
                result_status = str(notify_result.get("status") or "unknown")
                if not bool(notify_result.get("ok")):
                    raise RuntimeError(f"入库通知处理失败：{result_status}")
            except ValueError as err:
                self._record_webhook_status(event_type=event_type, result="telegram_error", detail=str(err))
                self._send_json(400, {"error": str(err)})
                return
            except RuntimeError as err:
                self._record_webhook_status(event_type=event_type, result="telegram_error", detail=str(err))
                self._send_json(502, {"error": str(err)})
                return
            except Exception as err:
                self._record_webhook_status(event_type=event_type, result="telegram_error", detail=str(err))
                self._send_json(502, {"error": f"Telegram 发送失败：{err}"})
                return

            if result_status == "sent":
                self._record_webhook_status(event_type=event_type, result="sent", detail="Telegram 新入库海报通知已发送")
            elif result_status == "duplicate":
                self._record_webhook_status(event_type=event_type, result="duplicate_skipped", detail="该入库资源已经通知，已跳过")
            elif result_status == "filtered":
                self._record_webhook_status(event_type=event_type, result="library_event_filtered", detail="仅通知电影和单集资源")
            else:
                self._record_webhook_status(event_type=event_type, result=result_status, detail="入库事件已处理")
            self._send_json(
                200,
                {
                    "ok": True,
                    "sent": result_status == "sent",
                    "eventType": event_type,
                    "itemId": item_id,
                    "result": result_status,
                },
            )
            return

        message = self._build_webhook_message(payload, event_type=event_type, event_name=event_name)
        try:
            self._send_telegram_text(token=token_value, chat_id=chat_id, text=message)
        except ValueError as err:
            self._record_webhook_status(event_type=event_type, result="telegram_error", detail=str(err))
            self._send_json(400, {"error": str(err)})
            return
        except RuntimeError as err:
            self._record_webhook_status(event_type=event_type, result="telegram_error", detail=str(err))
            self._send_json(502, {"error": str(err)})
            return
        except Exception as err:
            self._record_webhook_status(event_type=event_type, result="telegram_error", detail=str(err))
            self._send_json(502, {"error": f"Telegram 发送失败：{err}"})
            return

        self._record_webhook_status(event_type=event_type, result="sent", detail="Telegram 推送成功")
        self._send_json(200, {"ok": True, "sent": True, "eventType": event_type})

    def _serve_invite_page(self) -> None:
        self._serve_static_file("register.html", "text/html; charset=utf-8")

    def _handle_invite_register_page(self, path: str, raw_query: str) -> None:
        invite_code = ""
        path_match = INVITE_CODE_PAGE_PATTERN.match(path)
        if path_match:
            invite_code = urllib.parse.unquote(path_match.group(1)).strip()
        elif path in {"/invite", "/invite/"}:
            query_params = urllib.parse.parse_qs(raw_query or "")
            invite_code = str((query_params.get("code") or [""])[0]).strip()
        else:
            self.send_error(404, "File not found")
            return

        if not invite_code:
            self._send_invite_page_error(
                400,
                "邀请链接无效",
                "链接中缺少邀请码，请检查链接是否完整。",
            )
            return

        with STORE_LOCK:
            store = _read_store_unlocked()
            _, invite = self._find_invite_by_code(store.get("invites", []), invite_code)

        if not invite:
            self._send_invite_page_error(
                404,
                "邀请码不存在",
                f"未找到邀请码：{invite_code}",
            )
            return

        status = _effective_invite_status(invite)
        if status == "used":
            used_username = str(invite.get("usedUsername") or "").strip()
            detail = "该邀请码已被使用。"
            if used_username:
                detail = f"该邀请码已被用户 {used_username} 使用。"
            self._send_invite_page_error(400, "邀请码已使用", detail)
            return
        if status == "expired":
            self._send_invite_page_error(
                400,
                "邀请码已过期",
                "该邀请码已过期，请联系管理员获取新的邀请码。",
            )
            return
        if status != "active":
            self._send_invite_page_error(
                400,
                "邀请码不可用",
                "该邀请码当前不可用，请联系管理员。",
            )
            return

        self._serve_invite_page()

    def _send_invite_page_error(self, status: int, title: str, detail: str) -> None:
        title_text = html.escape(title)
        detail_text = html.escape(detail)
        page = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{title_text}</title>
  <style>
    :root {{
      --bg: #f4f1fb;
      --card: #ffffff;
      --line: #e7ddfa;
      --text: #1f1740;
      --muted: #6f668e;
      --primary: #722ed1;
      --shadow: 0 24px 52px rgba(68, 24, 122, 0.14);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      min-height: 100vh;
      display: grid;
      place-items: center;
      font-family: "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif;
      background:
        radial-gradient(circle at 12% 8%, rgba(114, 46, 209, 0.16) 0, transparent 42%),
        radial-gradient(circle at 90% 92%, rgba(156, 112, 228, 0.14) 0, transparent 44%),
        linear-gradient(180deg, #f8f5ff 0%, var(--bg) 100%);
      color: var(--text);
      padding: 16px;
    }}
    .card {{
      width: min(620px, 100%);
      border-radius: 28px;
      border: 1px solid var(--line);
      background: var(--card);
      padding: 26px 24px;
      box-shadow: var(--shadow);
    }}
    .brand {{
      margin: 0 0 10px;
      color: var(--primary);
      font-size: 13px;
      font-weight: 700;
    }}
    h1 {{
      margin: 0 0 12px;
      font-size: 34px;
      line-height: 1.15;
      letter-spacing: 0.01em;
    }}
    p {{
      margin: 0;
      color: var(--muted);
      line-height: 1.7;
      font-size: 16px;
    }}
  </style>
</head>
<body>
  <main class="card">
    <p class="brand">镜界Vistamirror</p>
    <h1>{title_text}</h1>
    <p>{detail_text}</p>
  </main>
</body>
</html>
"""
        payload = page.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _serve_static_file(self, filename: str, content_type: str) -> None:
        base_dir = pathlib.Path(self.directory or ".")
        file_path = (base_dir / filename).resolve()
        if not file_path.exists() or not file_path.is_file():
            self.send_error(404, "Not Found")
            return

        payload = file_path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _read_json_body(self) -> dict[str, Any] | None:
        content_length = self.headers.get("Content-Length")
        try:
            length = int(content_length or "0")
        except ValueError:
            self._send_json(400, {"error": "Invalid Content-Length"})
            return None

        if length <= 0:
            self._send_json(400, {"error": "Request body is required"})
            return None

        raw_body = self.rfile.read(length)
        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except Exception:
            self._send_json(400, {"error": "Invalid JSON body"})
            return None

        if not isinstance(payload, dict):
            self._send_json(400, {"error": "JSON body must be an object"})
            return None
        return payload

    def _find_invite_by_code(self, invites: list[dict[str, Any]], code: str) -> tuple[int, dict[str, Any]] | tuple[None, None]:
        normalized = _normalize_invite_code(code)
        for index, invite in enumerate(invites):
            if _normalize_invite_code(str(invite.get("code") or "")) == normalized:
                return index, invite
        return None, None

    def _invite_duration(self, invite: dict[str, Any]) -> int | None:
        initial_days = _parse_positive_int(invite.get("initialDays"))
        if initial_days is not None:
            return initial_days
        return _parse_positive_int(invite.get("duration"))

    def _invite_to_public(self, invite: dict[str, Any], *, with_status_text: bool = False) -> dict[str, Any]:
        status = _effective_invite_status(invite)
        duration = self._invite_duration(invite)
        status_text = "已用" if status == "used" else "空闲"
        row = {
            "id": invite.get("id") or "",
            "code": invite.get("code") or "",
            "label": invite.get("label") or "",
            "username": invite.get("username") or "",
            "plan": invite.get("plan") or "",
            "initialDays": duration,
            "duration": duration,
            "expiresAt": invite.get("expiresAt") or "",
            "status": status_text if with_status_text else status,
            "statusCode": status,
            "createdAt": invite.get("createdAt") or "",
            "usedAt": invite.get("usedAt") or "",
            "createdUserId": invite.get("createdUserId") or "",
            "usedUsername": invite.get("usedUsername") or "",
        }
        return row

    def _handle_invite_generate(self) -> None:
        payload = self._read_json_body()
        if payload is None:
            return

        quantity = _parse_positive_int(payload.get("quantity")) or 1
        quantity = max(1, min(quantity, 50))
        label = str(payload.get("label") or "").strip()
        username = str(payload.get("username") or "").strip()
        plan = str(payload.get("plan") or "").strip()
        days = _resolve_invite_days(payload)
        expires_at_raw = str(payload.get("expiresAt") or "").strip()
        expires_at = expires_at_raw if _parse_expiry(expires_at_raw) else ""

        generated: list[dict[str, Any]] = []
        with STORE_LOCK:
            store = _read_store_unlocked()
            invites = store.get("invites", [])
            if not isinstance(invites, list):
                invites = []

            existing_codes = {
                _normalize_invite_code(str(item.get("code") or ""))
                for item in invites
                if isinstance(item, dict) and str(item.get("code") or "").strip()
            }

            for _ in range(quantity):
                code = _generate_invite_code(existing_codes)
                existing_codes.add(code)
                created_at = _now_iso()
                resolved_expiry = expires_at
                if not resolved_expiry and days is not None:
                    resolved_expiry = (date.today() + timedelta(days=days)).isoformat()

                invite = {
                    "id": secrets.token_hex(8),
                    "code": code,
                    "label": label,
                    "username": username,
                    "plan": plan,
                    "initialDays": days,
                    "duration": days,
                    "expiresAt": resolved_expiry,
                    "status": "空闲",
                    "createdAt": created_at,
                    "usedAt": "",
                    "createdUserId": "",
                    "usedUsername": "",
                }
                invites.append(invite)
                generated.append(self._invite_to_public(invite))

            store["invites"] = invites
            _write_store_unlocked(store)
            rows = [self._invite_to_public(item) for item in invites]

        self._log_event(
            level="info",
            module="invite",
            action="invite_created",
            message=f"已创建 {len(generated)} 个邀请码。",
            status=200,
            detail={
                "quantity": len(generated),
                "codes": [item.get("code") for item in generated],
                "duration": days,
                "expiresAt": expires_at,
            },
        )
        rows.sort(key=lambda item: str(item.get("createdAt") or ""), reverse=True)
        self._send_json(
            200,
            {
                "ok": True,
                "synced": True,
                "generatedCount": len(generated),
                "generated": generated,
                "storedInviteCount": len(rows),
                "invites": rows,
            },
        )

    def _handle_invite_sync_status(self) -> None:
        with STORE_LOCK:
            store = _read_store_unlocked()
            invites = store.get("invites", [])
            if not isinstance(invites, list):
                invites = []
            emby_config = _apply_emby_env_overrides(store.get("embyConfig"))

            rows = [self._invite_to_public(item) for item in invites if isinstance(item, dict)]

        rows.sort(key=lambda item: str(item.get("createdAt") or ""), reverse=True)
        active_count = sum(1 for item in rows if item.get("statusCode") == "active")
        used_count = sum(1 for item in rows if item.get("statusCode") == "used")
        self._send_json(
            200,
            {
                "ok": True,
                "synced": True,
                "inviteCount": len(rows),
                "activeCount": active_count,
                "usedCount": used_count,
                "invites": rows,
                "embyConfig": emby_config,
                "envControlledFields": _env_controlled_fields_payload(),
                "managedByEnv": _env_controlled_fields_payload(),
                "updatedAt": _now_iso(),
            },
        )

    def _handle_invite_sync(self) -> None:
        payload = self._read_json_body()
        if payload is None:
            return

        emby_config_raw = payload.get("embyConfig")
        invites_raw = payload.get("invites")

        if not isinstance(emby_config_raw, dict):
            self._send_json(400, {"error": "embyConfig must be an object"})
            return
        if not isinstance(invites_raw, list):
            self._send_json(400, {"error": "invites must be an array"})
            return

        sanitized_invites: list[dict[str, Any]] = []
        for invite in invites_raw:
            sanitized = _sanitize_invite_record(invite)
            if sanitized:
                sanitized_invites.append(sanitized)

        with STORE_LOCK:
            stored = _read_store_unlocked()
            current_emby_config = stored.get("embyConfig") if isinstance(stored.get("embyConfig"), dict) else {}

        emby_config = _merge_emby_config_for_save(current_emby_config, emby_config_raw)

        with STORE_LOCK:
            store = _read_store_unlocked()
            current_emby_config = store.get("embyConfig") if isinstance(store.get("embyConfig"), dict) else {}
            locked = _env_managed_emby_fields()
            if locked:
                for field in locked:
                    emby_config[field] = str(current_emby_config.get(field) or "").strip()
            merged_invites = _merge_invites(store.get("invites", []), sanitized_invites)
            store["embyConfig"] = emby_config
            store["invites"] = merged_invites
            _write_store_unlocked(store)
            effective_emby_config = _apply_emby_env_overrides(store.get("embyConfig"))

        self._log_event(
            level="info",
            module="system",
            action="server_config_synced",
            message="媒体服务器配置与邀请码已同步到后端。",
            status=200,
            detail={
                "inviteCount": len(sanitized_invites),
                "storedInviteCount": len(merged_invites),
                "changedFields": ["serverUrl", "apiKey", "clientName", "tmdbEnabled", "tmdbToken", "tmdbLanguage", "tmdbRegion"],
                "envLockedFields": sorted(_env_managed_emby_fields()),
                "tmdbConfigured": bool(effective_emby_config.get("tmdbToken")),
            },
        )
        self._send_json(
            200,
            {
                "ok": True,
                "message": "Invite store synced",
                "inviteCount": len(sanitized_invites),
                "storedInviteCount": len(merged_invites),
                "embyConfig": effective_emby_config,
                "envControlledFields": _env_controlled_fields_payload(),
                "managedByEnv": _env_controlled_fields_payload(),
            },
        )

    def _handle_invite_query(self, path: str) -> None:
        match = INVITE_API_GET_PATTERN.match(path)
        if not match:
            self._send_json(404, {"error": "Invite endpoint not found"})
            return

        code = urllib.parse.unquote(match.group(1))
        with STORE_LOCK:
            store = _read_store_unlocked()
            emby_config = _apply_emby_env_overrides(store.get("embyConfig"))
            _, invite = self._find_invite_by_code(store.get("invites", []), code)
        server_hint = _public_emby_server_hint(emby_config)

        if not invite:
            self._send_json(
                200,
                {
                    "ok": True,
                    "code": code,
                    "status": "invalid",
                    "valid": False,
                    "invite": None,
                    "serverHint": server_hint,
                },
            )
            return

        status = _effective_invite_status(invite)
        invite_row = self._invite_to_public(invite)
        self._send_json(
            200,
            {
                "ok": True,
                "code": invite.get("code") or code,
                "status": status,
                "valid": status == "active",
                "invite": invite_row,
                "serverHint": server_hint,
            },
        )

    def _handle_invite_list(self) -> None:
        with STORE_LOCK:
            store = _read_store_unlocked()
            invites = store.get("invites", [])

        rows: list[dict[str, Any]] = []
        for invite in invites:
            if not isinstance(invite, dict):
                continue
            rows.append(self._invite_to_public(invite))

        rows.sort(key=lambda item: str(item.get("createdAt") or ""), reverse=True)
        self._send_json(200, {"ok": True, "invites": rows})

    def _emby_request(
        self,
        *,
        base_url: str,
        api_key: str,
        path: str,
        method: str = "GET",
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any] | str | None:
        body: bytes | None = None
        headers: dict[str, str] = {"X-Emby-Token": api_key}
        if payload is not None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json; charset=utf-8"

        request = urllib.request.Request(
            f"{base_url.rstrip('/')}{path}",
            data=body,
            method=method,
            headers=headers,
        )
        ssl_ctx = ssl._create_unverified_context()
        try:
            with urllib.request.urlopen(request, context=ssl_ctx, timeout=30) as response:
                content = response.read()
                if not content:
                    return None
                content_type = response.headers.get("Content-Type", "")
                if "application/json" in content_type:
                    return json.loads(content.decode("utf-8"))
                return content.decode("utf-8", errors="replace")
        except urllib.error.HTTPError:
            raise
        except Exception as err:
            if self._should_retry_emby_with_curl(base_url=base_url, err=err):
                return self._emby_request_via_curl(
                    base_url=base_url,
                    api_key=api_key,
                    path=path,
                    method=method,
                    payload=payload,
                )
            raise

    def _should_retry_emby_with_curl(self, *, base_url: str, err: Exception) -> bool:
        if not str(base_url or "").lower().startswith("https://"):
            return False
        text = f"{type(err).__name__}: {err}".lower()
        markers = (
            "eof occurred in violation of protocol",
            "unexpected eof while reading",
            "ssleoferror",
            "tlsv1 alert",
            "ssl:",
            "handshake",
            "wrong version number",
        )
        return any(marker in text for marker in markers)

    def _emby_request_via_curl(
        self,
        *,
        base_url: str,
        api_key: str,
        path: str,
        method: str = "GET",
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any] | str | None:
        target_url = f"{base_url.rstrip('/')}{path}"
        cmd: list[str] = [
            "curl",
            "-k",
            "-sS",
            "--max-time",
            "30",
            "-X",
            str(method or "GET").upper(),
            target_url,
            "-H",
            f"X-Emby-Token: {api_key}",
            "-H",
            "Accept: application/json",
            "-w",
            "\n__HTTP_STATUS__:%{http_code}",
        ]
        if payload is not None:
            cmd.extend(
                [
                    "-H",
                    "Content-Type: application/json; charset=utf-8",
                    "--data",
                    json.dumps(payload, ensure_ascii=False),
                ]
            )

        result = subprocess.run(cmd, capture_output=True, text=False)
        stderr_text = (result.stderr or b"").decode("utf-8", errors="replace").strip()
        if result.returncode != 0:
            raise RuntimeError(f"curl emby request failed (exit {result.returncode}): {stderr_text[:300]}")

        raw_output = result.stdout or b""
        marker = b"\n__HTTP_STATUS__:"
        marker_index = raw_output.rfind(marker)
        if marker_index < 0:
            raise RuntimeError("curl emby request failed: missing HTTP status marker")
        body = raw_output[:marker_index]
        status_raw = raw_output[marker_index + len(marker) :].strip().splitlines()
        status_text = status_raw[0].decode("utf-8", errors="replace").strip() if status_raw else "0"
        try:
            status_code = int(status_text)
        except Exception:
            status_code = 0

        if status_code >= 400:
            raise urllib.error.HTTPError(
                target_url,
                status_code,
                f"HTTP {status_code}",
                hdrs=None,
                fp=io.BytesIO(body),
            )

        if not body:
            return None
        body_text = body.decode("utf-8", errors="replace")
        stripped = body_text.lstrip()
        if stripped.startswith("{") or stripped.startswith("["):
            try:
                return json.loads(body_text)
            except Exception:
                pass
        return body_text

    def _proxy_emby_via_curl(
        self,
        *,
        target: str,
        method: str,
        headers: dict[str, str],
        body: bytes | None,
    ) -> tuple[int, str, bytes]:
        cmd: list[str] = [
            "curl",
            "-k",
            "-sS",
            "--max-time",
            "30",
            "-X",
            str(method or "GET").upper(),
            target,
            "-w",
            "\n__HTTP_STATUS__:%{http_code}\n__CONTENT_TYPE__:%{content_type}",
        ]
        for key, value in headers.items():
            if not key or value is None:
                continue
            cmd.extend(["-H", f"{key}: {value}"])
        if body is not None:
            cmd.extend(["--data-binary", "@-"])

        result = subprocess.run(cmd, input=body, capture_output=True, text=False)
        stderr_text = (result.stderr or b"").decode("utf-8", errors="replace").strip()
        if result.returncode != 0:
            raise RuntimeError(f"curl emby proxy failed (exit {result.returncode}): {stderr_text[:300]}")

        raw_output = result.stdout or b""
        status_marker = b"\n__HTTP_STATUS__:"
        content_type_marker = b"\n__CONTENT_TYPE__:"
        status_index = raw_output.rfind(status_marker)
        if status_index < 0:
            raise RuntimeError("curl emby proxy failed: missing HTTP status marker")
        content_type_index = raw_output.rfind(content_type_marker)
        if content_type_index < status_index:
            content_type_index = len(raw_output)

        body_bytes = raw_output[:status_index]
        status_raw = raw_output[status_index + len(status_marker) : content_type_index].strip()
        content_type_raw = b""
        if content_type_index < len(raw_output):
            content_type_raw = raw_output[content_type_index + len(content_type_marker) :].strip()
        status_text = status_raw.decode("utf-8", errors="replace").strip() or "0"
        content_type = content_type_raw.decode("utf-8", errors="replace").strip() or "application/json; charset=utf-8"
        try:
            status_code = int(status_text)
        except Exception:
            status_code = 0
        return status_code, content_type, body_bytes

    def _handle_invite_register(self, code: str) -> None:
        payload = self._read_json_body()
        if payload is None:
            return

        username = str(payload.get("username") or "").strip()
        password = str(payload.get("password") or "")
        self._register_with_invite(code=code, username=username, password=password)

    def _handle_register_api(self) -> None:
        payload = self._read_json_body()
        if payload is None:
            return

        code = str(payload.get("inviteCode") or payload.get("code") or "").strip()
        username = str(payload.get("username") or "").strip()
        password = str(payload.get("password") or "")
        self._register_with_invite(code=code, username=username, password=password)

    def _register_with_invite(self, *, code: str, username: str, password: str) -> None:
        invite_code = str(code or "").strip()
        def fail(status_code: int, payload: dict[str, object], *, action: str, message: str) -> None:
            self._log_event(
                level="warning" if status_code < 500 else "error",
                module="auth",
                action=action,
                message=message,
                user_id=username,
                status=status_code,
                detail={"inviteCode": invite_code, "code": payload.get("code"), "error": payload.get("error")},
            )
            self._send_json(status_code, payload)

        if not invite_code:
            fail(400, {"error": "缺少邀请码", "code": "invite_missing"}, action="register_failed", message="注册失败：缺少邀请码。")
            return

        if len(username) < 2:
            fail(400, {"error": "用户名至少 2 位", "code": "username_invalid"}, action="register_failed", message="注册失败：用户名不合法。")
            return
        if len(password) < 6:
            fail(400, {"error": "密码至少 6 位", "code": "password_invalid"}, action="register_failed", message="注册失败：密码不合法。")
            return

        with STORE_LOCK:
            store = _read_store_unlocked()
            invites = store.get("invites", [])
            index, invite = self._find_invite_by_code(invites, invite_code)

            if invite is None or index is None:
                fail(400, {"error": "邀请码无效", "code": "invite_invalid"}, action="register_failed", message="注册失败：邀请码无效。")
                return

            status = _effective_invite_status(invite)
            if status == "used":
                fail(400, {"error": "邀请码已使用", "code": "invite_used"}, action="register_failed", message="注册失败：邀请码已使用。")
                return
            if status == "expired":
                fail(400, {"error": "邀请码已过期", "code": "invite_expired"}, action="register_failed", message="注册失败：邀请码已过期。")
                return
            if status != "active":
                fail(400, {"error": "邀请码不可用", "code": "invite_invalid"}, action="register_failed", message="注册失败：邀请码不可用。")
                return

            emby_config = _apply_emby_env_overrides(store.get("embyConfig"))
            server_url = str(emby_config.get("serverUrl") or "").strip()
            api_key = str(emby_config.get("apiKey") or "").strip()
            client_name = str(emby_config.get("clientName") or "").strip()
            if not server_url or not api_key:
                fail(
                    400,
                    {"error": "服务端缺少 Emby 配置，请先在管理台保存并同步", "code": "emby_config_missing"},
                    action="register_failed",
                    message="注册失败：后端缺少媒体服务器配置。",
                )
                return

            if not server_url.lower().endswith("/emby"):
                server_url = f"{server_url.rstrip('/')}/emby"

            try:
                created = self._emby_request(
                    base_url=server_url,
                    api_key=api_key,
                    path="/Users/New",
                    method="POST",
                    payload={"Name": username},
                )
                if not isinstance(created, dict) or not created.get("Id"):
                    fail(502, {"error": "Emby 未返回有效用户 ID", "code": "emby_create_failed"}, action="register_failed", message="注册失败：媒体服务器未返回用户 ID。")
                    return

                user_id = str(created.get("Id"))
                self._emby_request(
                    base_url=server_url,
                    api_key=api_key,
                    path=f"/Users/{user_id}/Password",
                    method="POST",
                    payload={
                        "Id": user_id,
                        "CurrentPw": "",
                        "NewPw": password,
                        "ResetPassword": False,
                    },
                )
            except urllib.error.HTTPError as err:
                raw_detail = err.read().decode("utf-8", errors="replace")
                if err.code in (400, 409):
                    fail(
                        409,
                        {
                            "error": "创建用户失败，用户名可能已存在",
                            "code": "user_create_conflict",
                            "detail": raw_detail[:300],
                        },
                        action="register_failed",
                        message="注册失败：用户名可能已存在。",
                    )
                    return
                fail(
                    502,
                    {
                        "error": "Emby 请求失败",
                        "code": "emby_request_failed",
                        "status": err.code,
                        "detail": raw_detail[:300],
                    },
                    action="register_failed",
                    message="注册失败：媒体服务器请求失败。",
                )
                return
            except Exception as err:  # pragma: no cover
                fail(502, {"error": "注册失败", "code": "register_failed", "detail": str(err)}, action="register_failed", message="注册失败：后端异常。")
                return

            invite["status"] = "已用"
            invite["usedAt"] = _now_iso()
            invite["createdUserId"] = user_id
            invite["usedUsername"] = username
            invites[index] = invite
            store["invites"] = invites
            _write_store_unlocked(store)

        server_hint = _public_emby_server_hint(emby_config)
        self._log_event(
            level="info",
            module="auth",
            action="register_success",
            message=f"邀请注册成功：{username}",
            user_id=username,
            status=200,
            detail={"inviteCode": invite_code, "createdUserId": user_id},
        )
        self._log_event(
            level="info",
            module="invite",
            action="invite_used",
            message=f"邀请码 {invite_code} 已被使用。",
            user_id=username,
            status=200,
            detail={"inviteCode": invite_code, "createdUserId": user_id},
        )
        self._send_json(
            200,
            {
                "ok": True,
                "message": "注册成功，可前往 Emby 登录",
                "user": {"id": user_id, "name": username},
                "invite": {
                    "code": invite.get("code") or invite_code,
                    "status": "used",
                    "usedAt": invite.get("usedAt") or "",
                    "usedUsername": invite.get("usedUsername") or "",
                },
                "clientName": client_name,
                "serverHint": server_hint,
            },
        )
    def _proxy_emby(self) -> None:
        base_url = (self.headers.get("X-Emby-Base-Url") or "").strip().rstrip("/")
        api_key = (self.headers.get("X-Emby-Api-Key") or "").strip()
        if not base_url:
            self._send_json(400, {"error": "Missing X-Emby-Base-Url header"})
            return
        if not api_key:
            self._send_json(400, {"error": "Missing X-Emby-Api-Key header"})
            return

        parsed = urllib.parse.urlsplit(self.path)
        upstream_path = parsed.path[len("/api/emby") :]
        if not upstream_path:
            upstream_path = "/"
        if parsed.query:
            upstream_path = f"{upstream_path}?{parsed.query}"
        target = f"{base_url}{upstream_path}"

        body = None
        content_length = self.headers.get("Content-Length")
        if content_length:
            try:
                length = int(content_length)
            except ValueError:
                length = 0
            if length > 0:
                body = self.rfile.read(length)

        request_headers: dict[str, str] = {"X-Emby-Token": api_key}
        for header in PASS_HEADERS:
            value = self.headers.get(header)
            if value:
                request_headers[header] = value

        request = urllib.request.Request(
            target,
            data=body,
            method=self.command,
            headers=request_headers,
        )
        ssl_ctx = ssl._create_unverified_context()

        try:
            with urllib.request.urlopen(request, context=ssl_ctx, timeout=30) as response:
                payload = response.read()
                self.send_response(response.status)
                content_type = response.headers.get("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                if payload:
                    self.wfile.write(payload)
        except urllib.error.HTTPError as err:
            payload = err.read()
            self._log_event(
                level="warning" if err.code < 500 else "error",
                module="system",
                action="emby_proxy_error",
                message=f"Emby 代理请求失败：HTTP {err.code}",
                status=err.code,
                detail={"targetPath": upstream_path, "method": self.command},
            )
            self.send_response(err.code)
            content_type = err.headers.get("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            if payload:
                self.wfile.write(payload)
        except Exception as err:  # pragma: no cover
            if self._should_retry_emby_with_curl(base_url=base_url, err=err):
                try:
                    status_code, content_type, payload = self._proxy_emby_via_curl(
                        target=target,
                        method=self.command,
                        headers=request_headers,
                        body=body,
                    )
                    self.send_response(status_code if status_code > 0 else 502)
                    self.send_header("Content-Type", content_type)
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    if payload:
                        self.wfile.write(payload)
                    return
                except Exception as fallback_err:
                    self._send_json(
                        502,
                        {
                            "error": "Proxy request failed",
                            "detail": str(fallback_err),
                            "target": target,
                        },
                    )
                    return
            self._send_json(
                502,
                {
                    "error": "Proxy request failed",
                    "detail": str(err),
                    "target": target,
                },
            )

    def _send_json(
        self,
        status: int,
        payload: dict[str, object],
        *,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        request_path = urllib.parse.urlsplit(self.path).path
        if status >= 400 and request_path.startswith("/api/") and not request_path.startswith("/api/logs"):
            self._log_event(
                level="error" if status >= 500 else "warning",
                module="system",
                action="api_error",
                message=str(payload.get("error") or "接口请求失败"),
                status=status,
                detail=redact_sensitive(payload),
            )
        content = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        if extra_headers:
            for key, value in extra_headers.items():
                if key and value is not None:
                    self.send_header(str(key), str(value))
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def _send_text(self, status: int, payload: str) -> None:
        content = payload.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)


def main() -> None:
    global TELEGRAM_COMMAND_SERVICE, ADMIN_AUTH_SERVICE
    parser = argparse.ArgumentParser(description="Run Emby Pulse local proxy server")
    env_host = str(os.environ.get("APP_HOST") or "").strip() or "127.0.0.1"
    env_port_raw = str(os.environ.get("APP_PORT") or "").strip()
    try:
        env_port = int(env_port_raw) if env_port_raw else 8080
    except ValueError:
        env_port = 8080
    parser.add_argument("--host", default=env_host, help=f"Bind host (default: {env_host})")
    parser.add_argument("--port", default=env_port, type=int, help=f"Bind port (default: {env_port})")
    args = parser.parse_args()

    web_root = RUNTIME_DIR if RUNTIME_DIR.exists() else BASE_DIR
    auth_config = AuthConfig(
        enabled=_env_bool("APP_ADMIN_AUTH_ENABLED", False),
        username=str(os.environ.get("APP_ADMIN_USERNAME") or "").strip(),
        plain_password=str(os.environ.get("APP_ADMIN_PASSWORD") or ""),
        password_hash=str(os.environ.get("APP_ADMIN_PASSWORD_HASH") or "").strip(),
        session_ttl_seconds=max(600, _env_int("APP_ADMIN_SESSION_TTL_SECONDS", 86400)),
        login_max_fails=max(3, _env_int("APP_ADMIN_LOGIN_MAX_FAILS", 5)),
        login_lock_seconds=max(60, _env_int("APP_ADMIN_LOGIN_LOCK_SECONDS", 900)),
    )
    ADMIN_AUTH_SERVICE = AdminAuthService(
        session_file=DATA_DIR / "admin_sessions.json",
        credential_file=DATA_DIR / "admin_auth.json",
        config=auth_config,
    )
    ADMIN_AUTH_SERVICE.validate_startup()

    TELEGRAM_COMMAND_SERVICE = TelegramCommandService(
        store_path=_store_path(),
        event_log_path=PLAYBACK_EVENT_LOG_FILE,
        event_logger=_write_project_event,
    )
    TELEGRAM_COMMAND_SERVICE.start()
    HDHIVE_CHECKIN_STOP.clear()
    hdhive_checkin_thread = threading.Thread(target=_hdhive_direct_checkin_loop, name="hdhive-checkin", daemon=True)
    hdhive_checkin_thread.start()
    handler_cls = AppHandler
    server = ThreadingHTTPServer((args.host, args.port), handler_cls)
    _record_service_start(str(args.host), int(args.port))
    atexit.register(_record_service_stop, "atexit")
    print(f"Emby Pulse local server running at http://{args.host}:{args.port}")
    print("Press Ctrl+C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        HDHIVE_CHECKIN_STOP.set()
        server.server_close()
        if TELEGRAM_COMMAND_SERVICE is not None:
            TELEGRAM_COMMAND_SERVICE.stop()
        _record_service_stop("shutdown")


if __name__ == "__main__":
    main()
