from __future__ import annotations

from datetime import datetime, timedelta
import json
import pathlib
import re
import ssl
import threading
import time
import urllib.parse
import urllib.request
from typing import Any

from .notification_config import normalize_bot_config
from .playback_event_logger import read_recent_playback_events
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
    {"command": "help", "description": "🤖 帮助菜单"},
    {"command": "start", "description": "🚀 启动机器人"},
    {"command": "check", "description": "📡 系统探针"},
]


def _read_store(store_path: pathlib.Path) -> dict[str, Any]:
    if not store_path.exists():
        return {"embyConfig": {}, "invites": [], "botConfig": normalize_bot_config({})}
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
    }


def _help_text() -> str:
    lines = ["🤖 镜界 Vistamirror Bot 帮助菜单", ""]
    lines.extend([f"/{row['command']} - {row['description']}" for row in COMMAND_MENU])
    return "\n".join(lines)


class TelegramCommandService:
    def __init__(self, *, store_path: pathlib.Path, event_log_path: pathlib.Path) -> None:
        self.store_path = store_path
        self.event_log_path = event_log_path
        self.sender = TelegramSender()
        self._stop_event = threading.Event()
        self._wakeup_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._offset = 0
        self._last_token = ""
        self._commands_registered_token = ""

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
        message = update.get("message")
        if not isinstance(message, dict):
            return
        chat = message.get("chat") if isinstance(message.get("chat"), dict) else {}
        chat_id = chat.get("id")
        if chat_id in (None, ""):
            return
        chat_type = str(chat.get("type") or "").strip().lower()
        if chat_type and chat_type != "private":
            return
        text = str(message.get("text") or "").strip()
        if not text.startswith("/"):
            return
        cmd_text, _, args = text.partition(" ")
        cmd_name = cmd_text.split("@", 1)[0].lower().strip("/")
        reply = self._dispatch_command(cmd_name, args.strip())
        if not reply:
            return
        try:
            self.sender.send_text(token=token, chat_id=str(chat_id), text=reply)
        except Exception:
            return

    def _dispatch_command(self, cmd: str, args: str) -> str:
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
        emby = store["embyConfig"]
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

    def _cmd_recent_playback(self) -> str:
        rows = read_recent_playback_events(self.event_log_path, limit=10)
        if rows:
            lines = ["📜 最近播放记录", ""]
            for row in rows:
                lines.append(self._format_recent_playback_row(row))
            return "\n".join(lines)
        try:
            logs = self._emby_get("/System/ActivityLog/Entries?Limit=10&StartIndex=0")
        except Exception as err:
            return f"📜 最近播放记录\n获取失败：{err}"
        items = logs.get("Items") if isinstance(logs, dict) else []
        if not isinstance(items, list) or not items:
            return "📜 最近播放记录\n暂无记录。"
        lines = ["📜 最近播放记录", ""]
        for row in items:
            if not isinstance(row, dict):
                continue
            text = str(row.get("Name") or row.get("ShortOverview") or row.get("Overview") or "").strip()
            if not text:
                continue
            lines.append(f"🕰 --:-- » 👤 未知用户 » 📺 《{text[:48]}》")
        return "\n".join(lines[:11]) if len(lines) > 1 else "📜 最近播放记录\n暂无记录。"

    def _cmd_recent_library(self) -> str:
        try:
            latest = self._emby_get("/Items/Latest?Limit=10")
        except Exception as err:
            return f"🆕 最近入库\n获取失败：{err}"
        rows = latest if isinstance(latest, list) else []
        if not rows:
            return "🆕 最近入库\n暂无入库数据。"
        lines = ["🆕 最近入库"]
        for row in rows[:10]:
            if not isinstance(row, dict):
                continue
            title = str(row.get("Name") or "未命名内容")
            item_type = str(row.get("Type") or "")
            lines.append(f"• {title} ({item_type or 'Unknown'})")
        return "\n".join(lines)

    def _cmd_search(self, args: str) -> str:
        keyword = str(args or "").strip()
        if not keyword:
            return "🔍 搜索资源\n用法：/sousuo 关键词"
        query = urllib.parse.urlencode(
            {
                "Recursive": "true",
                "SearchTerm": keyword,
                "IncludeItemTypes": "Series,Movie,Episode",
                "Fields": "Name,Type,ProductionYear,Overview,Genres,People,PremiereDate,ChildCount,ParentIndexNumber,IndexNumber,SeriesName",
                "Limit": "8",
            }
        )
        try:
            result = self._emby_get(f"/Items?{query}")
        except Exception as err:
            return f"🔍 搜索资源\n搜索失败：{err}"
        items = result.get("Items") if isinstance(result, dict) else []
        if not isinstance(items, list) or not items:
            return f"🔍 搜索资源\n未找到“{keyword}”"
        first = items[0] if isinstance(items[0], dict) else {}
        return self._format_search_result(first)

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

    def _format_recent_playback_row(self, row: dict[str, Any]) -> str:
        at_text = str(row.get("at") or "").strip()
        try:
            at = datetime.fromisoformat(at_text)
            hhmm = at.strftime("%H:%M")
        except Exception:
            hhmm = "--:--"
        username = str(row.get("username") or "").strip() or "未知用户"
        media = self._format_episode_title_from_row(row)
        return f"🕰 {hhmm} » 👤 {username} » 📺 {media}"

    def _format_episode_title_from_row(self, row: dict[str, Any]) -> str:
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
                return f"《{series_name}》第{season_text}季 第{episode_text}集「{title}」"
            if item_name:
                return f"《{item_name}》"
        fallback = str(row.get("mediaName") or "").strip()
        parsed = self._parse_episode_from_text(fallback)
        if parsed:
            return parsed
        return f"《{fallback or '未知内容'}》"

    @staticmethod
    def _parse_episode_from_text(text: str) -> str:
        value = str(text or "").strip()
        if not value:
            return ""
        patterns = [
            r"^(?P<series>.+?)\s+S(?P<season>\d{1,2})E(?P<episode>\d{1,3})\s+(?P<title>.+)$",
            r"^(?P<series>.+?)\s*-\s*S(?P<season>\d{1,2}),\s*Ep(?P<episode>\d{1,3})\s*-\s*(?P<title>.+)$",
        ]
        for pattern in patterns:
            match = re.match(pattern, value, flags=re.IGNORECASE)
            if not match:
                continue
            series = str(match.group("series") or "").strip()
            season = str(match.group("season") or "").strip()
            episode = str(match.group("episode") or "").strip()
            title = str(match.group("title") or "").strip()
            if series and title:
                return f"《{series}》第{int(season)}季 第{int(episode)}集「{title}」"
        return f"《{value}》"

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

    def _format_search_result(self, item: dict[str, Any]) -> str:
        item_id = str(item.get("Id") or "").strip()
        detail = self._emby_get(
            f"/Items/{urllib.parse.quote(item_id, safe='')}?Fields=Overview,Genres,People,PremiereDate,ProductionYear,ChildCount,RecursiveItemCount,SeriesName,ParentIndexNumber,IndexNumber"
        )
        detail = detail if isinstance(detail, dict) else {}
        joined = dict(item)
        joined.update(detail)

        item_type = str(joined.get("Type") or "").strip().lower()
        title = str(joined.get("Name") or "未知内容").strip()
        genres = joined.get("Genres") if isinstance(joined.get("Genres"), list) else []
        genre_text = str(genres[0]).strip() if genres else "未分类"
        type_map = {"series": "剧集", "movie": "电影", "episode": "剧集"}
        type_text = type_map.get(item_type, "内容")
        season_count, episode_count = self._resolve_pack_count(item_type=item_type, item_id=item_id, detail=joined)
        people = joined.get("People") if isinstance(joined.get("People"), list) else []
        directors = [str(p.get("Name") or "").strip() for p in people if isinstance(p, dict) and str(p.get("Type") or "").lower() == "director"]
        writers = [str(p.get("Name") or "").strip() for p in people if isinstance(p, dict) and str(p.get("Type") or "").lower() == "writer"]
        actors = [str(p.get("Name") or "").strip() for p in people if isinstance(p, dict) and str(p.get("Type") or "").lower() == "actor"]
        year = self._resolve_year(joined)
        overview = self._truncate_text(str(joined.get("Overview") or "暂无简介").strip().replace("\n", " "), 100)
        library_line = self._format_library_line(item_type=item_type, season_count=season_count, episode_count=episode_count, joined=joined)

        return "\n".join(
            [
                "🔍 搜索结果",
                "",
                f"📺 {title}",
                f"🗂 {type_text}·{genre_text}",
                f"📦 {self._format_pack_text(item_type=item_type, season_count=season_count, episode_count=episode_count)}",
                "",
                "━━━━━━━━━━━━━━━",
                "📡 媒体信息",
                "━━━━━━━━━━━━━━━",
                f"🎬 导演：{self._join_names(directors)}",
                f"✍️ 编剧：{self._join_names(writers)}",
                f"⭐ 主演：{self._join_names(actors[:3])}",
                f"🏷 类型：{self._join_names(genres)}",
                f"📅 上映：{year}",
                "",
                "━━━━━━━━━━━━━━━",
                "📖 内容简介",
                "━━━━━━━━━━━━━━━",
                overview,
                "",
                "━━━━━━━━━━━━━━━",
                "📂 媒体库收录",
                "━━━━━━━━━━━━━━━",
                library_line,
            ]
        )

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
            return f"{year}年"
        premiere = str(detail.get("PremiereDate") or "").strip()
        if re.match(r"^\d{4}", premiere):
            return f"{premiere[:4]}年"
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
