from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any


class AIMediaFormatter:
    def format_recent_playback_row(self, row: dict[str, Any]) -> str:
        start_value = str(row.get("startTime") or "").strip()
        point_value = str(row.get("at") or row.get("time") or "").strip()
        primary_time_value = start_value or point_value
        date_text = self._iso_to_mmdd(primary_time_value) or "--/--"
        time_text = self._iso_to_hhmm(primary_time_value) or "--:--"
        username = str(row.get("username") or row.get("user") or "").strip() or "未知用户"
        filename, _parsed_episode, _fallback_title = self.format_recent_playback_filename_with_status(row)
        return f"🔹 {date_text} {time_text} | 👤 {username} | 📺「{filename}」"

    @classmethod
    def format_recent_playback_filename_with_status(cls, row: dict[str, Any]) -> tuple[str, bool, bool]:
        raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
        if raw:
            episode_filename = cls._build_recent_playback_episode_filename(raw)
            if episode_filename:
                return episode_filename, True, False
            for key in ("ItemName", "Name", "FileName", "Filename"):
                value = str(raw.get(key) or "").strip()
                if value:
                    parsed = cls._parse_episode_from_text(value)
                    if parsed:
                        return cls._build_recent_playback_episode_filename(parsed), True, False
                    return cls._clean_recent_playback_filename(value), False, False

        unified_title = str(row.get("title") or "").strip()
        if unified_title:
            parsed = cls._parse_episode_from_text(unified_title)
            if parsed:
                return cls._build_recent_playback_episode_filename(parsed), True, False
            return cls._clean_recent_playback_filename(unified_title), False, True

        fallback = str(row.get("mediaName") or "").strip()
        if fallback:
            parsed = cls._parse_episode_from_text(fallback)
            if parsed:
                return cls._build_recent_playback_episode_filename(parsed), True, False
            return cls._clean_recent_playback_filename(fallback), False, True

        return "未知内容", False, True

    @staticmethod
    def format_now_playing_title(item: dict[str, Any]) -> str:
        item_name = str(item.get("Name") or "").strip() or "未知内容"
        item_type = str(item.get("Type") or "").strip().lower()
        if item_type == "episode":
            series_name = str(item.get("SeriesName") or "").strip() or "未知剧名"
            season_number = item.get("ParentIndexNumber")
            episode_number = item.get("IndexNumber")
            season_text = str(season_number) if isinstance(season_number, int) else "X"
            episode_text = str(episode_number) if isinstance(episode_number, int) else "X"
            return f"《{series_name}》第{season_text}季 第{episode_text}集「{item_name}」"
        return f"《{item_name}》"

    @classmethod
    def format_media_quality(cls, item: dict[str, Any]) -> str:
        media = cls._best_media_source(item)
        width, height = cls._media_dimensions(item, media)
        resolution = cls._format_resolution(width=width, height=height)
        hdr = cls._format_hdr(media)
        bitrate = cls._format_bitrate(cls._media_bitrate(media))
        video_label = " ".join([part for part in (resolution, hdr) if part])
        parts = [part for part in (video_label, bitrate) if part]
        return " | ".join(parts) if parts else "质量未知"

    @staticmethod
    def format_ai_latest_episode_label(latest_text: str) -> str:
        text = str(latest_text or "").strip()
        match = re.search(r"S(?P<season>\d{1,2})E(?P<episode>\d{1,4})", text, flags=re.IGNORECASE)
        if not match:
            return text
        season = int(match.group("season") or 0)
        episode = int(match.group("episode") or 0)
        return f"S{season:02d}E{episode:02d}（第{episode}集）"

    @staticmethod
    def _best_media_source(item: dict[str, Any]) -> dict[str, Any]:
        sources = item.get("MediaSources") if isinstance(item.get("MediaSources"), list) else []
        best: dict[str, Any] = {}
        best_bitrate = -1
        for source in sources:
            if not isinstance(source, dict):
                continue
            bitrate = AIMediaFormatter._media_bitrate(source)
            if bitrate > best_bitrate:
                best = source
                best_bitrate = bitrate
        return best if best else item

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

    @staticmethod
    def _coerce_index_number(value: Any) -> int | None:
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value if value > 0 else None
        text = str(value or "").strip()
        if not text or not re.fullmatch(r"\d+", text):
            return None
        number = int(text)
        return number if number > 0 else None

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
        clean_title = cls._clean_recent_playback_episode_title(
            item_name,
            series_name=clean_series,
            season_number=season_number,
            episode_number=episode_number,
        )
        episode_code = f"S{season_number:02d}E{episode_number:02d}"
        return f"{clean_series} - {episode_code} - {clean_title}"

    @classmethod
    def _clean_recent_playback_episode_title(cls, value: str, *, series_name: str, season_number: int, episode_number: int) -> str:
        clean = cls._clean_recent_playback_filename(value)
        if not clean or clean == "未知内容":
            return f"第 {episode_number} 集"
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
            tail_match = re.match(
                r"S(?P<season>\d{1,2})\s*[,，]?\s*Ep?(?P<episode>\d{1,3})(?:\s*[-—–]?\s*(?P<title>.+))?$",
                tail,
                flags=re.IGNORECASE,
            )
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
