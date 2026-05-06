from __future__ import annotations

import json
import pathlib
from datetime import datetime
from typing import Any


def append_playback_event(
    log_file: pathlib.Path,
    *,
    username: str,
    media_name: str,
    event_type: str,
    device: str,
    ip: str,
    raw_payload: dict[str, Any],
) -> None:
    record = {
        "username": str(username or "").strip() or "未知用户",
        "mediaName": str(media_name or "").strip() or "未知内容",
        "eventType": str(event_type or "").strip() or "unknown",
        "device": str(device or "").strip(),
        "ip": str(ip or "").strip(),
        "at": datetime.now().isoformat(timespec="seconds"),
        "raw": raw_payload if isinstance(raw_payload, dict) else {},
    }
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with log_file.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + "\n")


def read_recent_playback_events(log_file: pathlib.Path, *, limit: int = 10) -> list[dict[str, Any]]:
    if not log_file.exists():
        return []
    rows: list[dict[str, Any]] = []
    try:
        lines = log_file.read_text(encoding="utf-8").splitlines()
    except Exception:
        return []
    for line in reversed(lines):
        text = str(line or "").strip()
        if not text:
            continue
        try:
            payload = json.loads(text)
        except Exception:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
        if len(rows) >= max(1, int(limit or 10)):
            break
    return rows


def summarize_playback_events(
    events: list[dict[str, Any]],
    *,
    start_at: datetime,
    end_at: datetime,
) -> dict[str, Any]:
    total = 0
    user_set: set[str] = set()
    media_set: set[str] = set()
    for row in events:
        at_text = str(row.get("at") or "").strip()
        if not at_text:
            continue
        try:
            at = datetime.fromisoformat(at_text)
        except Exception:
            continue
        if at < start_at or at > end_at:
            continue
        total += 1
        user_name = str(row.get("username") or "").strip()
        media_name = str(row.get("mediaName") or "").strip()
        if user_name:
            user_set.add(user_name)
        if media_name:
            media_set.add(media_name)
    return {
        "totalEvents": total,
        "totalUsers": len(user_set),
        "totalMedia": len(media_set),
    }
