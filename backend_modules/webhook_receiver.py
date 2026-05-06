from __future__ import annotations

import re
from typing import Any


def detect_playback_action(payload: dict[str, Any], event_name: str) -> str:
    event = str(event_name or "").strip().lower()
    if not event:
        for key in ("Event", "event", "NotificationType", "notificationType", "Action", "action", "PlaybackState", "playbackState", "Message", "message"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                event = value.strip().lower()
                break
    if any(key in event for key in ("resume", "resumed", "恢复")):
        return "resume"
    if any(key in event for key in ("pause", "paused", "playbackpause", "暂停")):
        return "pause"
    if any(key in event for key in ("stop", "sessionend", "playbackend", "结束", "停止")):
        return "stop"
    if any(key in event for key in ("start", "sessionstart", "playbackstart", "开始")):
        return "start"
    return "other"


def event_enabled(bot_config: dict[str, Any], action: str) -> bool:
    notify_events = bot_config.get("notifyEvents")
    if not isinstance(notify_events, dict):
        return True
    return bool(notify_events.get(action, True))


def build_dedupe_key(*, username: str, item_id: str, media_name: str, action: str) -> str:
    user = str(username or "").strip().lower() or "-"
    item = str(item_id or "").strip().lower() or str(media_name or "").strip().lower() or "-"
    evt = str(action or "").strip().lower() or "-"
    return f"{user}|{item}|{evt}"


def maybe_extract_media_name(payload: dict[str, Any], fallback: str = "") -> str:
    for key in ("ItemName", "itemName", "Name", "name", "MediaName", "mediaName"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    now_item = payload.get("NowPlayingItem")
    if isinstance(now_item, dict):
        for key in ("Name", "name"):
            value = now_item.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    item = payload.get("Item")
    if isinstance(item, dict):
        for key in ("Name", "name"):
            value = item.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    if fallback:
        return fallback
    return "未知内容"
