from __future__ import annotations

from datetime import datetime
import re
from typing import Any

from .ip_locator import build_ip_display


def short_text(text: str, *, limit: int = 220) -> str:
    clean = str(text or "").strip()
    if not clean:
        return ""
    if len(clean) <= limit:
        return clean
    return clean[: max(8, limit - 1)].rstrip() + "…"


def hms_full(total_seconds: int) -> str:
    safe = max(0, int(total_seconds or 0))
    hours = safe // 3600
    minutes = (safe % 3600) // 60
    seconds = safe % 60
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def ticks_to_seconds(raw: Any) -> int:
    try:
        ticks = int(raw or 0)
    except (TypeError, ValueError):
        return 0
    if ticks <= 0:
        return 0
    return ticks // 10000000


def format_episode_tag(payload: dict[str, Any], item_detail: dict[str, Any]) -> str:
    def pick_int(*keys: str) -> int | None:
        for key in keys:
            parts = key.split(".")
            node: Any = payload
            ok = True
            for part in parts:
                if not isinstance(node, dict):
                    ok = False
                    break
                node = node.get(part)
            if ok and node not in (None, ""):
                try:
                    return int(node)
                except (TypeError, ValueError):
                    pass
        return None

    season = pick_int("ParentIndexNumber", "parentIndexNumber", "NowPlayingItem.ParentIndexNumber", "Item.ParentIndexNumber")
    episode = pick_int("IndexNumber", "indexNumber", "NowPlayingItem.IndexNumber", "Item.IndexNumber")
    if season is None:
        try:
            season = int(item_detail.get("ParentIndexNumber"))
        except (TypeError, ValueError):
            season = None
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


def detect_event_time(payload: dict[str, Any]) -> str:
    for key in ("Date", "date", "EventTime", "eventTime", "Timestamp", "timestamp"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            try:
                parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
                return parsed.astimezone().strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                pass
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def normalize_content_type(raw: str) -> str:
    text = str(raw or "").strip().lower()
    mapping = {
        "episode": "剧集",
        "movie": "电影",
        "audio": "音频",
        "series": "剧集",
    }
    return mapping.get(text, str(raw or "").strip())


def compose_playback_message(
    *,
    payload: dict[str, Any],
    item_detail: dict[str, Any],
    action: str,
    username: str,
    series_name: str,
    item_name: str,
    content_type: str,
    rating_text: str,
    position_sec: int,
    runtime_sec: int,
    percent_text: str,
    device_name: str,
    overview: str,
    show_ip: bool,
    show_ip_geo: bool,
    show_overview: bool,
) -> str:
    action_map = {
        "start": ("▶️", "开始播放"),
        "pause": ("⏸️", "暂停播放"),
        "resume": ("⏯️", "恢复播放"),
        "stop": ("⏹️", "停止播放"),
    }
    icon, action_text = action_map.get(action, ("▶️", "播放状态"))
    episode_tag = format_episode_tag(payload, item_detail)
    parts = [f"{icon} 【{username}】{action_text}"]
    if content_type:
        parts.append(content_type)
    if series_name:
        parts.append(series_name)
    if episode_tag:
        parts.append(episode_tag)
    if item_name:
        parts.append(item_name)

    lines: list[str] = [" ".join(parts).strip(), ""]
    meta: list[str] = []
    if rating_text:
        meta.append(f"⭐ 评分：{rating_text}")
    if content_type:
        meta.append(f"📚 类型：{content_type}")
    if meta:
        lines.append(" ｜ ".join(meta))

    if runtime_sec > 0:
        progress = f"🔄 进度：{hms_full(position_sec)} / {hms_full(runtime_sec)}"
        if percent_text:
            progress += f" ({percent_text})"
        lines.append(progress)

    ip_display = build_ip_display(payload, show_ip=show_ip, show_geo=show_ip_geo)
    if ip_display:
        lines.append(f"🌐 IP地址：{ip_display}")
    if device_name:
        lines.append(f"📱 设备：{device_name}")
    lines.append(f"🕒 时间：{detect_event_time(payload)}")
    if show_overview and overview:
        lines.append("")
        lines.append(f"📝 剧情：{short_text(overview)}")
    return "\n".join(lines)
