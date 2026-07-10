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


def format_episode_display_tag(payload: dict[str, Any], item_detail: dict[str, Any]) -> str:
    episode_tag = format_episode_tag(payload, item_detail)
    match = re.fullmatch(r"S(\d{2})E(\d{2,})", episode_tag)
    if match:
        season = int(match.group(1))
        episode = int(match.group(2))
        return f"S{season}, Ep{episode}"
    match = re.fullmatch(r"S(\d{2})", episode_tag)
    if match:
        season = int(match.group(1))
        return f"S{season}"
    match = re.fullmatch(r"E(\d{2,})", episode_tag)
    if match:
        episode = int(match.group(1))
        return f"Ep{episode}"
    return episode_tag


def playback_state_meta(action: str) -> dict[str, str]:
    return {
        "start": {
            "title_icon": "🟢",
            "title_text": "正在播放",
            "action_text": "开始播放",
            "progress_icon": "🟢",
        },
        "pause": {
            "title_icon": "🟡",
            "title_text": "暂停播放",
            "action_text": "暂停播放",
            "progress_icon": "🟡",
        },
        "resume": {
            "title_icon": "🟢",
            "title_text": "恢复播放",
            "action_text": "恢复播放",
            "progress_icon": "🟢",
        },
        "stop": {
            "title_icon": "🔴",
            "title_text": "播放停止",
            "action_text": "停止播放",
            "progress_icon": "🔴",
        },
    }.get(
        str(action or "").strip().lower(),
        {
            "title_icon": "🟢",
            "title_text": "播放状态",
            "action_text": "播放状态",
            "progress_icon": "🟢",
        },
    )


def extract_media_year(payload: dict[str, Any], item_detail: dict[str, Any]) -> str:
    for source in (
        item_detail,
        payload,
        payload.get("Item") if isinstance(payload.get("Item"), dict) else {},
        payload.get("NowPlayingItem") if isinstance(payload.get("NowPlayingItem"), dict) else {},
    ):
        if not isinstance(source, dict):
            continue
        year = str(source.get("ProductionYear") or "").strip()
        if year:
            return year
        premiere = str(source.get("PremiereDate") or "").strip()
        if len(premiere) >= 4 and premiere[:4].isdigit():
            return premiere[:4]
    return ""


def format_playback_title_line(
    *,
    action: str,
    payload: dict[str, Any],
    item_detail: dict[str, Any],
    content_type: str,
    series_name: str,
    item_name: str,
) -> str:
    state = playback_state_meta(action)
    if content_type == "剧集":
        episode_display_tag = format_episode_display_tag(payload, item_detail)
        title_body = " - ".join(
            part for part in (series_name, episode_display_tag, item_name) if str(part or "").strip()
        ).strip()
    else:
        title_body = str(item_name or series_name or "").strip()
        year = extract_media_year(payload, item_detail)
        if title_body and year:
            title_body = f"{title_body} ({year})"
    if not title_body:
        return f"{state['title_icon']} 【{state['title_text']}】"
    return f"{state['title_icon']} 【{state['title_text']}】{title_body}"


def _pick_streams(payload: dict[str, Any], item_detail: dict[str, Any]) -> list[dict[str, Any]]:
    playback_info = payload.get("PlaybackInfo") if isinstance(payload.get("PlaybackInfo"), dict) else {}
    media_source = playback_info.get("MediaSource") if isinstance(playback_info.get("MediaSource"), dict) else {}
    for source in (media_source, item_detail):
        streams = source.get("MediaStreams") if isinstance(source, dict) else None
        if isinstance(streams, list):
            return [row for row in streams if isinstance(row, dict)]
    return []


def format_playback_strategy(payload: dict[str, Any], item_detail: dict[str, Any]) -> str:
    playback_info = payload.get("PlaybackInfo") if isinstance(payload.get("PlaybackInfo"), dict) else {}
    media_source = playback_info.get("MediaSource") if isinstance(playback_info.get("MediaSource"), dict) else {}
    raw_method = ""
    for source in (payload, playback_info, media_source, item_detail):
        if not isinstance(source, dict):
            continue
        for key in ("PlayMethod", "playMethod", "Method", "method"):
            value = str(source.get(key) or "").strip()
            if value:
                raw_method = value
                break
        if raw_method:
            break
    normalized = raw_method.replace("-", "").replace("_", "").replace(" ", "").casefold()
    if normalized in {"directplay"}:
        return "直接播放 (Direct Play)"
    if normalized in {"directstream"}:
        return "直接串流 (Direct Stream)"
    if normalized in {"transcode", "transcoding"}:
        return "转码播放 (Transcode)"
    if any(str(media_source.get(key) or "").strip() for key in ("TranscodingUrl", "TranscodingSubProtocol", "TranscodingContainer")):
        return "转码播放 (Transcode)"
    if bool(media_source.get("SupportsDirectPlay")):
        return "直接播放 (Direct Play)"
    if bool(media_source.get("SupportsDirectStream")):
        return "直接串流 (Direct Stream)"
    if bool(media_source.get("SupportsTranscoding")):
        return "转码播放 (Transcode)"
    return ""


def _format_resolution_label(width: Any, height: Any) -> str:
    try:
        safe_height = int(height or 0)
    except (TypeError, ValueError):
        safe_height = 0
    try:
        safe_width = int(width or 0)
    except (TypeError, ValueError):
        safe_width = 0
    if safe_height >= 2160 or safe_width >= 3800:
        return "2160p"
    if safe_height >= 1440:
        return "1440p"
    if safe_height >= 1080:
        return "1080p"
    if safe_height >= 720:
        return "720p"
    if safe_height > 0:
        return f"{safe_height}p"
    return ""


def _format_video_codec(codec: Any) -> str:
    text = str(codec or "").strip().lower()
    mapping = {
        "h264": "H.264",
        "avc": "H.264",
        "hevc": "H.265",
        "h265": "H.265",
        "x265": "H.265",
        "av1": "AV1",
        "vp9": "VP9",
    }
    if not text:
        return ""
    return mapping.get(text, str(codec or "").strip().upper())


def _format_audio_codec(codec: Any) -> str:
    text = str(codec or "").strip().lower()
    mapping = {
        "aac": "AAC",
        "ac3": "AC-3",
        "eac3": "E-AC-3",
        "dca": "DTS",
        "dts": "DTS",
        "truehd": "TrueHD",
        "flac": "FLAC",
        "mp3": "MP3",
    }
    if not text:
        return ""
    return mapping.get(text, str(codec or "").strip().upper())


def format_playback_media_spec(payload: dict[str, Any], item_detail: dict[str, Any]) -> str:
    streams = _pick_streams(payload, item_detail)
    video = next((row for row in streams if str(row.get("Type") or "").strip().lower() == "video"), {})
    audio = next((row for row in streams if str(row.get("Type") or "").strip().lower() == "audio"), {})
    resolution = _format_resolution_label(
        video.get("Width") if isinstance(video, dict) else item_detail.get("Width"),
        video.get("Height") if isinstance(video, dict) else item_detail.get("Height"),
    )
    video_codec = _format_video_codec(video.get("Codec") if isinstance(video, dict) else "")
    audio_codec = _format_audio_codec(audio.get("Codec") if isinstance(audio, dict) else "")
    rows = [value for value in (resolution, video_codec, audio_codec) if value]
    return " | ".join(rows)


def format_playback_device_name(client_name: str, device_name: str) -> str:
    client = str(client_name or "").strip()
    device = str(device_name or "").strip()
    if client and device:
        client_norm = re.sub(r"[\s_\-]+", "", client).casefold()
        device_norm = re.sub(r"[\s_\-]+", "", device).casefold()
        if client_norm == device_norm:
            return client
        if client_norm and client_norm in device_norm:
            return device
        if device_norm and device_norm in client_norm:
            return client
        return f"{client} {device}"
    return client or device


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
    state = playback_state_meta(action)
    lines: list[str] = [
        format_playback_title_line(
            action=action,
            payload=payload,
            item_detail=item_detail,
            content_type=content_type,
            series_name=series_name,
            item_name=item_name,
        ),
        "",
        f"🍿 播放用户：{username}",
    ]
    strategy_text = format_playback_strategy(payload, item_detail)
    if strategy_text:
        lines.append(f"📽️ 播放策略：{strategy_text}")
    media_spec = format_playback_media_spec(payload, item_detail)
    if media_spec:
        lines.append(f"🎟️ 媒体规格：{media_spec}")
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    lines.append("📋 播放数据")
    if rating_text:
        lines.append(f"▸ 评分：🌟 {rating_text.replace('/10', ' / 10')}")
    else:
        lines.append("▸ 评分：🌟 暂无")
    if runtime_sec > 0:
        progress = f"▸ 进度：{state['progress_icon']} {hms_full(position_sec)} / {hms_full(runtime_sec)}"
        if percent_text:
            progress += f" ({percent_text})"
        lines.append(progress)
    lines.append("")
    lines.append("🛋️ 终端状态")
    ip_display = build_ip_display(payload, show_ip=show_ip, show_geo=show_ip_geo)
    if device_name:
        lines.append(f"▸ 设备：📺 {device_name}")
    if ip_display:
        lines.append(f"▸ 网络：📍 {ip_display}")
    lines.append(f"▸ 时间：⏰ {detect_event_time(payload)}")
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    if show_overview and overview:
        lines.append(f"📖 剧情简介：{short_text(overview)}")
    return "\n".join(lines)
