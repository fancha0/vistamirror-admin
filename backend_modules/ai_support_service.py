from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from .ai_host_adapter import AIHostAdapter
from .notification_config import normalize_bot_config

if TYPE_CHECKING:
    from .telegram_commands import TelegramCommandService


class AISupportService:
    def __init__(self, service: "AIHostAdapter | TelegramCommandService") -> None:
        self.host = AIHostAdapter.coerce(service)

    def build_safety_reply(self, question: str) -> str:
        text = str(question or "").strip()
        if re.search(
            r"删除|清空|重置|改.*(token|api key|apikey|密钥|密码)|修改.*(token|api key|apikey|密钥|密码)|显示.*(token|api key|apikey|密钥|密码)|告诉我.*(token|api key|apikey|密钥|密码)",
            text,
            flags=re.IGNORECASE,
        ):
            return "这类删除、清空、重置、修改或查看密钥/密码的操作不能由 AI 执行。\n请到后台页面手动操作；我只能告诉你这些敏感项是否已配置。"
        return ""

    def build_bot_status_context(self) -> str:
        store = self.host.platform.read_store()
        bot = normalize_bot_config(store.get("botConfig"))
        return "\n".join(
            [
                "机器人状态：",
                f"- 指令轮询：{'开启' if bot.get('enableCommands', True) else '关闭'}",
                f"- 播放通知：{'开启' if bot.get('enablePlayback', True) else '关闭'}",
                f"- 入库通知：{'开启' if bot.get('enableLibrary', True) else '关闭'}",
                f"- Telegram Token：{'已配置' if str(bot.get('telegramToken') or '').strip() else '未配置'}",
                f"- Chat ID：{'已配置' if str(bot.get('telegramChatId') or '').strip() else '未配置'}",
            ]
        )

    def build_library_stats_context(self) -> str:
        try:
            counts = self.host.media_service.emby_get("/Items/Counts")
        except Exception as err:
            return f"当前媒体库统计：读取失败（{self.host.media_service.format_emby_error(err)}）。"
        if not isinstance(counts, dict):
            return "当前媒体库统计：暂无可用数据。"

        def _count(*keys: str) -> int:
            for key in keys:
                value = counts.get(key)
                if isinstance(value, (int, float)) and int(value) >= 0:
                    return int(value)
            return 0

        movie_count = _count("MovieCount", "movies", "Movies")
        series_count = _count("SeriesCount", "series", "Series")
        episode_count = _count("EpisodeCount", "episodes", "Episodes")
        music_video_count = _count("MusicVideoCount", "musicVideos", "MusicVideos")
        trailer_count = _count("TrailerCount", "trailers", "Trailers")
        total = movie_count + series_count + episode_count + music_video_count + trailer_count
        if total <= 0:
            total = _count("ItemCount", "TotalItemCount", "total", "Total")

        rows = [
            "当前媒体库统计：",
            f"- 电影：{movie_count} 部",
            f"- 剧集：{series_count} 部",
            f"- 单集：{episode_count} 集",
        ]
        if music_video_count:
            rows.append(f"- 音乐视频：{music_video_count} 个")
        if trailer_count:
            rows.append(f"- 预告片：{trailer_count} 个")
        if total:
            rows.append(f"- 可统计资源合计：{total} 个")
        return "\n".join(rows)
