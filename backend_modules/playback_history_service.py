from __future__ import annotations

from datetime import datetime
import re
from typing import Any, Callable, Optional, Union

PlaybackFetcher = Callable[[str], Union[dict[str, Any], list[Any], None]]
PlaybackLogger = Callable[..., None]


class PlaybackHistoryService:
    def __init__(
        self,
        *,
        fetcher: PlaybackFetcher,
        event_logger: Optional[PlaybackLogger] = None,
    ) -> None:
        self.fetcher = fetcher
        self.event_logger = event_logger

    def collect(
        self,
        *,
        limit: int = 300,
        scan_limit: int = 2000,
        keyword: str = "",
        username: str = "",
    ) -> dict[str, Any]:
        safe_limit = max(1, min(1000, int(limit or 300)))
        safe_scan_limit = max(100, min(5000, int(scan_limit or 2000)))
        target_hits = max(safe_limit * 3, 120)

        scanned = 0
        skipped = 0
        start_index = 0
        chunk = 200
        strict_rows: list[dict[str, Any]] = []
        relaxed_rows: list[dict[str, Any]] = []
        device_client_map = self._build_device_client_map()
        user_id_cache: dict[str, str] = {}
        strict_seen: set[str] = set()
        relaxed_seen: set[str] = set()

        while scanned < safe_scan_limit and len(strict_rows) < target_hits and len(relaxed_rows) < target_hits:
            page_size = min(chunk, safe_scan_limit - scanned)
            if page_size <= 0:
                break
            payload = self.fetcher(f"/System/ActivityLog/Entries?Limit={page_size}&StartIndex={start_index}")
            items = payload.get("Items") if isinstance(payload, dict) else payload
            if not isinstance(items, list):
                break
            if not items:
                break

            for log in items:
                if scanned >= safe_scan_limit:
                    break
                scanned += 1
                if not isinstance(log, dict):
                    skipped += 1
                    continue
                strict_row = self._normalize_activity_log(
                    log,
                    mode="strict",
                    device_client_map=device_client_map,
                    user_id_cache=user_id_cache,
                )
                if strict_row:
                    key = self._dedupe_key(strict_row)
                    if key not in strict_seen:
                        strict_seen.add(key)
                        strict_rows.append(strict_row)
                    continue

                relaxed_row = self._normalize_activity_log(
                    log,
                    mode="relaxed",
                    device_client_map=device_client_map,
                    user_id_cache=user_id_cache,
                )
                if relaxed_row:
                    key = self._dedupe_key(relaxed_row)
                    if key not in relaxed_seen:
                        relaxed_seen.add(key)
                        relaxed_rows.append(relaxed_row)
                    continue

                skipped += 1

            start_index += len(items)
            if len(items) < page_size:
                break

        mode = "strict"
        fallback_source = "none"
        selected_rows = strict_rows
        if len(strict_rows) == 0 and len(relaxed_rows) > 0:
            mode = "relaxed"
            fallback_source = "relaxed"
            selected_rows = relaxed_rows

        selected_rows = self._attach_playback_ranges(selected_rows)
        filtered = self._filter_rows(selected_rows, keyword=keyword, username=username)
        filtered.sort(key=lambda row: str(row.get("time") or ""), reverse=True)
        result = filtered[:safe_limit]

        debug = {
            "scanned": scanned,
            "matched": len(selected_rows),
            "strictMatched": len(strict_rows),
            "relaxedMatched": len(relaxed_rows),
            "skipped": skipped,
            "filtered": len(filtered),
            "returned": len(result),
            "limit": safe_limit,
            "scanLimit": safe_scan_limit,
            "source": "emby_activity_log",
            "mode": mode,
            "fallbackSource": fallback_source,
        }
        warning = ""
        if scanned > 0 and len(strict_rows) == 0 and len(relaxed_rows) > 0:
            warning = "已切换宽松识别模式返回播放历史。"
        elif scanned > 0 and len(strict_rows) == 0 and len(relaxed_rows) == 0:
            warning = "已扫描 Emby 活动日志，但未识别到播放事件。"
        elif scanned == 0:
            warning = "未扫描到 Emby 活动日志。"
        return {"rows": result, "debug": debug, "warning": warning}

    def _filter_rows(self, rows: list[dict[str, Any]], *, keyword: str = "", username: str = "") -> list[dict[str, Any]]:
        safe_keyword = str(keyword or "").strip().lower()
        safe_user = str(username or "").strip().lower()
        if not safe_keyword and not safe_user:
            return rows

        result: list[dict[str, Any]] = []
        for row in rows:
            user = str(row.get("user") or "").strip()
            title = str(row.get("title") or "").strip()
            client = str(row.get("client") or "").strip()
            device = str(row.get("device") or "").strip()
            if safe_user and user.lower() != safe_user:
                continue
            if safe_keyword:
                haystack = f"{user} {title} {client} {device}".lower()
                if safe_keyword not in haystack:
                    continue
            result.append(row)
        return result

    def _normalize_activity_log(
        self,
        log: dict[str, Any],
        *,
        mode: str = "strict",
        device_client_map: dict[str, str] | None = None,
        user_id_cache: dict[str, str] | None = None,
    ) -> dict[str, Any] | None:
        text = self._activity_log_text(log)
        if mode == "strict":
            if not self._is_strict_playback_activity_log(log, text):
                return None
        elif mode == "relaxed":
            if not self._is_relaxed_playback_activity_log(log, text):
                return None
        else:
            return None

        played_at = self._parse_activity_log_datetime(log)
        if not played_at:
            return None
        duration_seconds = self._extract_activity_log_duration_seconds(log, text)
        duration_minutes = int(round(duration_seconds / 60)) if duration_seconds > 0 else 0
        parsed_user, parsed_device = self._extract_user_and_device_from_text(text)
        user = self._extract_activity_log_username(log, text)
        if parsed_user:
            user = parsed_user
        user_id = str(log.get("UserId") or "").strip()
        if user in {"", "未知用户"} and user_id:
            resolved = self._resolve_user_name_by_id(user_id, user_id_cache)
            if resolved:
                user = resolved
        title = self._extract_activity_log_media_name(log, text)
        title = self._sanitize_media_title(title)
        action = self._infer_action(log, text)
        item_id = self._extract_activity_log_item_id(log)
        raw_item = self._extract_activity_log_item_payload(log)
        device = str(log.get("DeviceName") or log.get("DeviceId") or "").strip()
        if not device and parsed_device:
            device = parsed_device
        client = str(log.get("Client") or log.get("AppName") or "").strip()
        if not client:
            client = self._guess_client_from_device(device, device_client_map)
        time_text = played_at.isoformat(timespec="seconds")
        duration_text = f"{duration_minutes} 分钟" if duration_minutes > 0 else "-"
        return {
            "time": time_text,
            "user": user,
            "title": title,
            "duration": duration_minutes,
            "durationText": duration_text,
            "client": client or "未知客户端",
            "device": device or "未知设备",
            "itemId": item_id,
            "action": action,
            "userId": user_id,
            "source": "emby_activity_log_relaxed" if mode == "relaxed" else "emby_activity_log",
            "raw": raw_item,
            # 兼容旧渲染字段
            "date": time_text,
            "userName": user,
            "durationMin": duration_minutes,
            "player": {
                "software": client or "未知客户端",
                "device": device or "未知设备",
            },
        }

    def _dedupe_key(self, row: dict[str, Any]) -> str:
        time_text = str(row.get("time") or "").strip()
        try:
            dt = datetime.fromisoformat(time_text)
            bucket = dt.strftime("%Y-%m-%d %H:%M")
        except Exception:
            bucket = time_text[:16]
        user = str(row.get("user") or "").strip().lower()
        title = str(row.get("title") or "").strip().lower()
        action = str(row.get("action") or "").strip().lower()
        item_id = str(row.get("itemId") or "").strip().lower()
        device = str(row.get("device") or "").strip().lower()
        return f"{bucket}|{user}|{title}|{action}|{item_id}|{device}"

    def _build_device_client_map(self) -> dict[str, str]:
        try:
            payload = self.fetcher("/Devices?Limit=500")
        except Exception:
            return {}
        items = payload.get("Items") if isinstance(payload, dict) else payload
        if not isinstance(items, list):
            return {}
        mapping: dict[str, str] = {}
        for item in items:
            if not isinstance(item, dict):
                continue
            device_name = str(item.get("Name") or "").strip()
            app_name = str(item.get("AppName") or item.get("Client") or "").strip()
            if not device_name or not app_name:
                continue
            mapping[device_name.lower()] = app_name
        return mapping

    def _resolve_user_name_by_id(self, user_id: str, cache: dict[str, str] | None = None) -> str:
        safe_user_id = str(user_id or "").strip()
        if not safe_user_id:
            return ""
        if cache is not None and safe_user_id in cache:
            return cache[safe_user_id]
        resolved = ""
        try:
            payload = self.fetcher(f"/Users/{safe_user_id}")
        except Exception:
            payload = None
        if isinstance(payload, dict):
            resolved = str(payload.get("Name") or payload.get("UserName") or payload.get("Username") or "").strip()
        if cache is not None:
            cache[safe_user_id] = resolved
        return resolved

    @staticmethod
    def _guess_client_from_device(device_name: str, device_client_map: dict[str, str] | None = None) -> str:
        safe_device = str(device_name or "").strip()
        if safe_device and isinstance(device_client_map, dict):
            mapped = str(device_client_map.get(safe_device.lower()) or "").strip()
            if mapped:
                return mapped
        return ""

    @staticmethod
    def _extract_user_and_device_from_text(text: str) -> tuple[str, str]:
        safe = str(text or "").strip()
        if not safe:
            return "", ""
        patterns = (
            r"^(?P<user>[^\s]{1,64})\s+在\s+(?P<device>[^，。,；;]{1,120})\s+上(?:开始|停止|继续|暂停)?播放",
            r"^(?P<device>[^，。,；;]{1,120})\s+上\s+(?P<user>[^\s]{1,64})\s+已(?:开始|停止|继续|暂停)?播放",
            r"^(?P<user>[^\s]{1,64})\s+(?:started|stopped|is)\s+playing\s+(?P<device>[^，。,；;]{1,120})",
        )
        for pattern in patterns:
            match = re.search(pattern, safe, flags=re.IGNORECASE)
            if not match:
                continue
            user = str(match.groupdict().get("user") or "").strip()
            device = str(match.groupdict().get("device") or "").strip()
            if user or device:
                return user, device
        return "", ""

    def _attach_playback_ranges(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not rows:
            return rows
        chronological: list[dict[str, Any]] = sorted(rows, key=lambda row: str(row.get("time") or ""))
        starts: dict[str, list[tuple[int, datetime]]] = {}
        paired_start_indexes: set[int] = set()

        for idx, row in enumerate(chronological):
            action = str(row.get("action") or "").strip().lower()
            played_at = self._safe_parse_iso_datetime(str(row.get("time") or ""))
            if played_at is None:
                continue
            session_key = self._session_identity_key(row)
            if action == "start":
                starts.setdefault(session_key, []).append((idx, played_at))
                continue
            if action not in {"stop", "pause", "resume"}:
                continue
            stack = starts.get(session_key) or []
            start_candidate: tuple[int, datetime] | None = None
            while stack:
                candidate = stack.pop()
                if candidate[1] <= played_at:
                    start_candidate = candidate
                    break
            if start_candidate is None:
                continue
            start_idx, start_time = start_candidate
            paired_start_indexes.add(start_idx)
            duration_seconds = int((played_at - start_time).total_seconds())
            if duration_seconds <= 0:
                continue
            duration_minutes = max(1, int(round(duration_seconds / 60)))
            if int(row.get("duration") or 0) <= 0:
                row["duration"] = duration_minutes
                row["durationMin"] = duration_minutes
            row["durationSec"] = duration_seconds
            row["startTime"] = start_time.isoformat(timespec="seconds")
            row["endTime"] = played_at.isoformat(timespec="seconds")
            row["durationText"] = f"{duration_minutes} 分钟"
            row["durationWindow"] = self._format_duration_window(start_time, played_at, duration_seconds)

        result: list[dict[str, Any]] = []
        for idx, row in enumerate(chronological):
            action = str(row.get("action") or "").strip().lower()
            if action == "start" and idx in paired_start_indexes:
                continue
            if "durationWindow" not in row and "startTime" in row and "endTime" in row:
                start_dt = self._safe_parse_iso_datetime(str(row.get("startTime") or ""))
                end_dt = self._safe_parse_iso_datetime(str(row.get("endTime") or ""))
                if start_dt and end_dt and end_dt >= start_dt:
                    seconds = int((end_dt - start_dt).total_seconds())
                    row["durationWindow"] = self._format_duration_window(start_dt, end_dt, seconds)
            result.append(row)
        return result

    @staticmethod
    def _session_identity_key(row: dict[str, Any]) -> str:
        user = str(row.get("user") or "").strip().lower()
        item_id = str(row.get("itemId") or "").strip().lower()
        title = str(row.get("title") or "").strip().lower()
        device = str(row.get("device") or "").strip().lower()
        if item_id:
            return f"{user}|{item_id}|{device}"
        return f"{user}|{title}|{device}"

    @staticmethod
    def _safe_parse_iso_datetime(value: str) -> datetime | None:
        safe = str(value or "").strip()
        if not safe:
            return None
        try:
            return datetime.fromisoformat(safe)
        except Exception:
            return None

    @staticmethod
    def _format_duration_window(start: datetime, end: datetime, seconds: int) -> str:
        safe_seconds = max(0, int(seconds or 0))
        mm = safe_seconds // 60
        ss = safe_seconds % 60
        return f"{start.strftime('%H:%M')} → {end.strftime('%H:%M')}（{mm}分{ss:02d}秒）"

    @staticmethod
    def _sanitize_media_title(raw: str) -> str:
        text = str(raw or "").strip()
        if not text:
            return "未知内容"
        text = re.sub(r"\s+playback\.(?:start|stop|progress|pause|resume)\s*$", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s+(?:sessionstart|sessionend)\s*$", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s{2,}", " ", text).strip(" -—·")
        return text or "未知内容"

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
        normalized = value.strip().replace(" ", "T")
        # Python 3.9 对 fromisoformat 的容错较弱，尤其是 7+ 位小数秒。
        # Emby 常见时间类似：2026-05-21T12:33:56.1234567Z
        normalized = normalized.replace("Z", "+00:00")
        frac_match = re.search(r"\.(\d+)(?=(?:\+|-)\d{2}:\d{2}$)", normalized)
        if frac_match:
            frac = frac_match.group(1)
            if len(frac) > 6:
                normalized = normalized.replace(f".{frac}", f".{frac[:6]}", 1)
        try:
            dt = datetime.fromisoformat(normalized)
        except Exception:
            # 兜底尝试：去掉小数秒后再解析一次
            normalized_no_frac = re.sub(r"\.\d+(?=(?:\+|-)\d{2}:\d{2}$)", "", normalized)
            try:
                dt = datetime.fromisoformat(normalized_no_frac)
            except Exception:
                return None
        if dt.tzinfo is not None:
            dt = dt.astimezone().replace(tzinfo=None)
        return dt

    @staticmethod
    def _is_strict_playback_activity_log(log: dict[str, Any], text: str) -> bool:
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
            if "is online from" in lower and "playing" not in lower:
                return False
            return True
        return False

    @staticmethod
    def _is_relaxed_playback_activity_log(log: dict[str, Any], text: str) -> bool:
        lower = str(text or "").lower()
        # 明确排除明显非播放行为，避免把登录/系统事件误判为播放。
        negative_tokens = (
            "is online from",
            "authenticated",
            "login",
            "logout",
            "webhook",
            "bot command",
            "token",
            "密码",
            "登录",
            "退出",
            "入库",
            "library.new",
            "scheduledtasks",
        )
        if any(token in lower for token in negative_tokens):
            return False

        relaxed_tokens = (
            "播放",
            "观看",
            "观影",
            "started playing",
            "stopped playing",
            "playback",
            "watched",
            "is playing",
            "now playing",
            "继续播放",
            "暂停播放",
        )
        if any(token in lower for token in relaxed_tokens):
            return True

        # 常见剧集标题形态：沧元图 – S1, Ep65 – 元初山番外篇：陆
        if re.search(r"\bS\d{1,2}\b\s*[,，-]?\s*\bEp?\d{1,3}\b", str(text or ""), flags=re.IGNORECASE):
            return True
        if re.search(r"第\s*\d+\s*季|第\s*\d+\s*集", str(text or "")):
            return True
        if re.search(r"《[^》]{1,120}》", str(text or "")):
            return True

        item = log.get("Item") if isinstance(log.get("Item"), dict) else {}
        item_name = str(item.get("Name") or item.get("ItemName") or log.get("ItemName") or "").strip()
        user_name = str(log.get("UserName") or log.get("ByUserName") or "").strip()
        if item_name and user_name:
            return True
        return False

    @staticmethod
    def _extract_activity_log_username(log: dict[str, Any], text: str) -> str:
        nested_user = log.get("User") if isinstance(log.get("User"), dict) else {}
        nested_user_name = str(
            nested_user.get("Name")
            or nested_user.get("UserName")
            or nested_user.get("Username")
            or ""
        ).strip()
        if nested_user_name:
            return nested_user_name
        for key in ("UserName", "ByUserName", "Client", "DeviceName"):
            value = str(log.get(key) or "").strip()
            if value:
                return value
        patterns = [
            r"^([^\s:：]+)\s+(?:started playing|stopped playing|is playing|has finished playing|played|finished playing|正在播放|开始播放|停止播放|播放了|观看了)",
            r"^([^\s:：]+)\s*[：:]\s*",
        ]
        for pattern in patterns:
            match = re.match(pattern, str(text or ""), flags=re.IGNORECASE)
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
        patterns = [
            r"(?:started playing|stopped playing|playing|has finished playing|played|finished playing)\s+(.+?)(?:\s+on\s+|\s+using\s+|$)",
            r"(?:开始播放|停止播放|正在播放|播放了|观看了|播放)\s*[：: ]?\s*(.+)$",
        ]
        for pattern in patterns:
            match = re.search(pattern, str(text or ""), flags=re.IGNORECASE)
            if match:
                value = str(match.group(1) or "").strip(" 。.")
                if value:
                    return value
        fallback = str(log.get("Name") or log.get("ShortOverview") or log.get("Overview") or "").strip()
        # 去掉日志尾部动作标签，避免 UI 显示 "... playback.start/stop"
        fallback = re.sub(r"\s+playback\.(?:start|stop|progress|pause|resume)\s*$", "", fallback, flags=re.IGNORECASE)
        fallback = re.sub(r"\s{2,}", " ", fallback).strip(" -—·")
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

    @staticmethod
    def _extract_activity_log_item_id(log: dict[str, Any]) -> str:
        item = log.get("Item") if isinstance(log.get("Item"), dict) else {}
        value = str(log.get("ItemId") or item.get("Id") or item.get("ItemId") or "").strip()
        return value

    @staticmethod
    def _extract_activity_log_item_payload(log: dict[str, Any]) -> dict[str, Any]:
        item = log.get("Item") if isinstance(log.get("Item"), dict) else {}
        keys = (
            "Id",
            "ItemId",
            "Name",
            "ItemName",
            "FileName",
            "Filename",
            "Type",
            "SeriesName",
            "ParentIndexNumber",
            "IndexNumber",
        )
        payload: dict[str, Any] = {}
        for key in keys:
            if item:
                value = item.get(key)
            elif key not in {"Id", "Name", "ItemName", "FileName", "Filename", "Type"}:
                value = log.get(key)
            else:
                value = None
            if value not in (None, ""):
                payload[key] = value
        return payload

    @staticmethod
    def _infer_action(log: dict[str, Any], text: str) -> str:
        event_text = " ".join(
            [
                str(log.get("Event") or ""),
                str(log.get("EventName") or ""),
                str(log.get("Type") or ""),
                str(text or ""),
            ]
        ).lower()
        if any(token in event_text for token in ("stop", "stopped", "停止播放", "结束播放", "playbackstopped", "userstoppedplaying")):
            return "stop"
        if any(token in event_text for token in ("pause", "paused", "暂停播放", "playbackprogress")):
            return "pause"
        if any(token in event_text for token in ("resume", "resumed", "恢复播放")):
            return "resume"
        if any(token in event_text for token in ("start", "started", "开始播放", "playbackstart", "userstartedplaying", "playing")):
            return "start"
        return "unknown"
