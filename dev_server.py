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
from functools import partial
import hashlib
import html
import ipaddress
import json
import os
import pathlib
import re
import secrets
import socket
import ssl
import struct
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from backend_modules.ip_locator import build_ip_display
from backend_modules.message_formatter import compose_playback_message, normalize_content_type, ticks_to_seconds
from backend_modules.notification_config import (
    default_bot_config as module_default_bot_config,
    normalize_bot_config as module_normalize_bot_config,
    validate_bot_config as module_validate_bot_config,
)
from backend_modules.playback_event_logger import append_playback_event
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

EMBY_ENV_FIELD_MAP: dict[str, str] = {
    "serverUrl": "APP_EMBY_SERVER_URL",
    "apiKey": "APP_EMBY_API_KEY",
    "clientName": "APP_EMBY_CLIENT_NAME",
}
BOT_ENV_FIELD_MAP: dict[str, str] = {
    "telegramToken": "APP_BOT_TELEGRAM_TOKEN",
    "telegramChatId": "APP_BOT_TELEGRAM_CHAT_ID",
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


def _normalize_bot_config(raw: Any) -> dict[str, Any]:
    return module_normalize_bot_config(raw)


def _validate_bot_config(raw: Any) -> tuple[dict[str, Any] | None, str | None]:
    return module_validate_bot_config(raw)


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


def _apply_emby_env_overrides(raw: Any) -> dict[str, Any]:
    source = raw if isinstance(raw, dict) else {}
    merged = {
        "serverUrl": str(source.get("serverUrl") or "").strip(),
        "apiKey": str(source.get("apiKey") or "").strip(),
        "clientName": str(source.get("clientName") or "").strip() or DEFAULT_EMBY_CLIENT_NAME,
        "updatedAt": str(source.get("updatedAt") or "").strip(),
    }
    for field, env_name in EMBY_ENV_FIELD_MAP.items():
        env_value = _env_override_value(env_name)
        if env_value:
            merged[field] = env_value
    return merged


def _apply_bot_env_overrides(raw: Any) -> dict[str, Any]:
    source = _normalize_bot_config(raw)
    merged = dict(source)
    for field, env_name in BOT_ENV_FIELD_MAP.items():
        env_value = _env_override_value(env_name)
        if env_value:
            merged[field] = env_value
    return _normalize_bot_config(merged)


def _env_controlled_fields_payload() -> dict[str, list[str]]:
    return {
        "embyConfig": _env_managed_emby_fields(),
        "botConfig": _env_managed_bot_fields(),
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
        return {"embyConfig": {}, "invites": [], "botConfig": _default_bot_config()}

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"embyConfig": {}, "invites": [], "botConfig": _default_bot_config()}

    emby_config = data.get("embyConfig") if isinstance(data, dict) else {}
    invites = data.get("invites") if isinstance(data, dict) else []
    bot_config = data.get("botConfig") if isinstance(data, dict) else {}
    if not isinstance(emby_config, dict):
        emby_config = {}
    if not isinstance(invites, list):
        invites = []
    return {
        "embyConfig": emby_config,
        "invites": invites,
        "botConfig": _normalize_bot_config(bot_config),
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


class AppHandler(SimpleHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def __init__(self, *args, **kwargs):
        runtime_dir = RUNTIME_DIR.resolve()
        runtime_dir.mkdir(parents=True, exist_ok=True)
        super().__init__(*args, directory=str(runtime_dir), **kwargs)

    def do_GET(self) -> None:
        parsed = urllib.parse.urlsplit(self.path)
        path = parsed.path

        if path == "/api/bot/config":
            self._handle_bot_config_get()
            return
        if path == "/api/bot/webhook-url":
            self._handle_bot_webhook_url_get()
            return
        if path == "/api/bot/webhook-status":
            self._handle_bot_webhook_status_get()
            return
        if path.startswith("/api/bot/wecom_webhook"):
            self._handle_wecom_verify()
            return
        if path == "/api/ranking/annual":
            self._handle_annual_ranking(parsed.query)
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

        if path == "/api/bot/config":
            self._handle_bot_config_save()
            return
        if path == "/api/bot/test":
            self._handle_bot_test()
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
        if self.path.startswith("/api/emby"):
            self._proxy_emby()
            return
        self.send_error(405, "Method Not Allowed")

    def do_DELETE(self) -> None:
        if self.path.startswith("/api/emby"):
            self._proxy_emby()
            return
        self.send_error(405, "Method Not Allowed")

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

        self._send_json(
            200,
            {
                "ok": True,
                "botConfig": saved_config,
                "envControlledFields": _env_controlled_fields_payload(),
                "managedByEnv": _env_controlled_fields_payload(),
            },
        )

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
                "ParentIndexNumber",
                "IndexNumber",
                "ProductionYear",
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

        emby_config = {
            "serverUrl": str(emby_config_raw.get("serverUrl") or "").strip(),
            "apiKey": str(emby_config_raw.get("apiKey") or "").strip(),
            "clientName": str(emby_config_raw.get("clientName") or "").strip(),
            "updatedAt": _now_iso(),
        }

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
            _, invite = self._find_invite_by_code(store.get("invites", []), code)

        if not invite:
            self._send_json(
                200,
                {
                    "ok": True,
                    "code": code,
                    "status": "invalid",
                    "valid": False,
                    "invite": None,
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

        with urllib.request.urlopen(request, context=ssl_ctx, timeout=30) as response:
            content = response.read()
            if not content:
                return None
            content_type = response.headers.get("Content-Type", "")
            if "application/json" in content_type:
                return json.loads(content.decode("utf-8"))
            return content.decode("utf-8", errors="replace")

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
        if not invite_code:
            self._send_json(400, {"error": "缺少邀请码", "code": "invite_missing"})
            return

        if len(username) < 2:
            self._send_json(400, {"error": "用户名至少 2 位", "code": "username_invalid"})
            return
        if len(password) < 6:
            self._send_json(400, {"error": "密码至少 6 位", "code": "password_invalid"})
            return

        with STORE_LOCK:
            store = _read_store_unlocked()
            invites = store.get("invites", [])
            index, invite = self._find_invite_by_code(invites, invite_code)

            if invite is None or index is None:
                self._send_json(400, {"error": "邀请码无效", "code": "invite_invalid"})
                return

            status = _effective_invite_status(invite)
            if status == "used":
                self._send_json(400, {"error": "邀请码已使用", "code": "invite_used"})
                return
            if status == "expired":
                self._send_json(400, {"error": "邀请码已过期", "code": "invite_expired"})
                return
            if status != "active":
                self._send_json(400, {"error": "邀请码不可用", "code": "invite_invalid"})
                return

            emby_config = _apply_emby_env_overrides(store.get("embyConfig"))
            server_url = str(emby_config.get("serverUrl") or "").strip()
            api_key = str(emby_config.get("apiKey") or "").strip()
            client_name = str(emby_config.get("clientName") or "").strip()
            if not server_url or not api_key:
                self._send_json(
                    400,
                    {"error": "服务端缺少 Emby 配置，请先在管理台保存并同步", "code": "emby_config_missing"},
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
                    self._send_json(502, {"error": "Emby 未返回有效用户 ID", "code": "emby_create_failed"})
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
                    self._send_json(
                        409,
                        {
                            "error": "创建用户失败，用户名可能已存在",
                            "code": "user_create_conflict",
                            "detail": raw_detail[:300],
                        },
                    )
                    return
                self._send_json(
                    502,
                    {
                        "error": "Emby 请求失败",
                        "code": "emby_request_failed",
                        "status": err.code,
                        "detail": raw_detail[:300],
                    },
                )
                return
            except Exception as err:  # pragma: no cover
                self._send_json(502, {"error": "注册失败", "code": "register_failed", "detail": str(err)})
                return

            invite["status"] = "已用"
            invite["usedAt"] = _now_iso()
            invite["createdUserId"] = user_id
            invite["usedUsername"] = username
            invites[index] = invite
            store["invites"] = invites
            _write_store_unlocked(store)

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
            self.send_response(err.code)
            content_type = err.headers.get("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            if payload:
                self.wfile.write(payload)
        except Exception as err:  # pragma: no cover
            self._send_json(
                502,
                {
                    "error": "Proxy request failed",
                    "detail": str(err),
                    "target": target,
                },
            )

    def _send_json(self, status: int, payload: dict[str, object]) -> None:
        content = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
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
    global TELEGRAM_COMMAND_SERVICE
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
    TELEGRAM_COMMAND_SERVICE = TelegramCommandService(store_path=_store_path(), event_log_path=PLAYBACK_EVENT_LOG_FILE)
    TELEGRAM_COMMAND_SERVICE.start()
    handler_cls = partial(AppHandler, directory=str(web_root))
    server = ThreadingHTTPServer((args.host, args.port), handler_cls)
    print(f"Emby Pulse local server running at http://{args.host}:{args.port}")
    print("Press Ctrl+C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        if TELEGRAM_COMMAND_SERVICE is not None:
            TELEGRAM_COMMAND_SERVICE.stop()


if __name__ == "__main__":
    main()


