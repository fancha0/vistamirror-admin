from __future__ import annotations

import json
import re
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


_LOCK = threading.Lock()
_SENSITIVE_KEY_RE = re.compile(
    r"(api[_-]?key|token|secret|password|passwd|pwd|authorization|encodingaeskey|aes)",
    re.IGNORECASE,
)
_LEVELS = {"info", "warning", "error"}
_MODULES = {"auth", "invite", "webhook", "playback", "system", "docker"}


def redact_sensitive(value: Any) -> Any:
    if isinstance(value, dict):
        redacted: dict[str, Any] = {}
        for key, item in value.items():
            key_text = str(key)
            if _SENSITIVE_KEY_RE.search(key_text):
                redacted[key_text] = "[REDACTED]"
            else:
                redacted[key_text] = redact_sensitive(item)
        return redacted
    if isinstance(value, list):
        return [redact_sensitive(item) for item in value]
    if isinstance(value, tuple):
        return [redact_sensitive(item) for item in value]
    return value


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def _normalize_level(level: str) -> str:
    safe = str(level or "info").strip().lower()
    return safe if safe in _LEVELS else "info"


def _normalize_module(module: str) -> str:
    safe = str(module or "system").strip().lower()
    return safe if safe in _MODULES else "system"


def append_project_event(
    path: Path,
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
) -> dict[str, Any]:
    event = {
        "id": uuid.uuid4().hex,
        "time": _now_iso(),
        "level": _normalize_level(level),
        "module": _normalize_module(module),
        "action": str(action or "").strip()[:80],
        "message": str(message or "").strip()[:500],
        "user_id": str(user_id or "").strip()[:120],
        "ip": str(ip or "").strip()[:120],
        "path": str(request_path or "").strip()[:300],
        "status": status,
        "detail": redact_sensitive(detail if detail is not None else {}),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(event, ensure_ascii=False, separators=(",", ":"))
    with _LOCK:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(f"{line}\n")
    return event


def read_project_events(
    path: Path,
    *,
    level: str = "",
    module: str = "",
    keyword: str = "",
    limit: int = 200,
) -> tuple[list[dict[str, Any]], int]:
    safe_level = str(level or "").strip().lower()
    safe_module = str(module or "").strip().lower()
    needle = str(keyword or "").strip().lower()
    safe_limit = max(1, min(int(limit or 200), 1000))

    if not path.exists():
        return [], 0

    events: list[dict[str, Any]] = []
    total = 0
    with _LOCK:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()

    for line in reversed(lines):
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(event, dict):
            continue
        total += 1
        if safe_level and str(event.get("level") or "").lower() != safe_level:
            continue
        if safe_module and str(event.get("module") or "").lower() != safe_module:
            continue
        if needle:
            haystack = json.dumps(event, ensure_ascii=False).lower()
            if needle not in haystack:
                continue
        events.append(event)
        if len(events) >= safe_limit:
            break
    return events, total


def clear_project_events(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with _LOCK:
        path.write_text("", encoding="utf-8")
