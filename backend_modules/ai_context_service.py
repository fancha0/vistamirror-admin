from __future__ import annotations

import os
import re
import urllib.parse
from datetime import datetime
from typing import TYPE_CHECKING, Any

from .ai_host_adapter import AIHostAdapter
from .playback_history_service import PlaybackHistoryService
from .project_event_logger import read_project_events
from .notification_config import normalize_bot_config
from .ai_query_service import AIQueryService

if TYPE_CHECKING:
    from .telegram_commands import TelegramCommandService


class AIContextService:
    def __init__(
        self,
        host: "AIHostAdapter | TelegramCommandService",
        *,
        chat_id: str = "",
        conversation_key: str = "",
    ) -> None:
        self.host = AIHostAdapter.coerce(host)
        self.chat_id = str(chat_id or "").strip()
        self.conversation_key = str(conversation_key or "").strip()

    def build_messages(self, question: str, *, ai_config: dict[str, Any] | None = None) -> list[dict[str, str]]:
        context_parts = [
            self.build_media_context(question=question),
            self.build_recent_operations_context(),
        ]
        conversation_context = self.build_conversation_context()
        active_context = self.build_active_media_context()
        if conversation_context:
            context_parts.append(conversation_context)
        if active_context:
            context_parts.append(active_context)
        context = self.host.platform.limit_ai_context_text(
            "\n\n".join(part for part in context_parts if part),
            ai_config=ai_config,
        )
        subagent = self.host.registry.pick_subagent(
            question,
            conversation_key=self.conversation_key,
            chat_id=self.chat_id,
        )
        subagent_instruction = str(getattr(subagent, "instruction", "") or "").strip()
        system_lines = [
            "你是 VistaMirror 的媒体库 AI 助手。",
            "请用简洁中文回答，优先依据提供的媒体库上下文。",
            "如果上下文里有“当前媒体库统计”或“命中资源详情”，必须直接使用这些准确数字回答；",
            "如果上下文里有“最终判断”，必须按最终判断回答，并可简短说明冲突来源；",
            "如果上下文里包含任务、日志、缺集或播放历史摘要，也要优先引用这些项目资料。",
            "如果用户问“刚才”“执行了吗”“扫描了吗”“上一个操作怎么样”，必须优先查看最近项目操作和会话历史。",
            "如果最近操作显示已提交扫描或任务，直接说明目标、时间和结果。",
            "如果用户用“它/这个/那个”追问，要结合最近会话历史判断指代对象。",
            "如果上下文不足，要明确说明不确定，不要编造不存在的片名或数据。",
            "不要输出任何 Token、API Key、密码或密钥。",
            "如果用户请求删除、清空、改密钥或改密码，应拒绝并建议到后台手动操作。",
            "普通回答直接像正常机器人聊天一样输出，不要自称“AI 媒体问答”。",
            "不要输出“Text”、type、label、title 这类结构字段。",
            "普通说明不要包成代码块；只有命令、日志、脚本片段才适合代码样式。",
            "查询结果优先使用简洁列表和短段落。",
        ]
        if subagent and subagent_instruction:
            system_lines.append(f"当前子代理：{subagent.name}。{subagent_instruction}")
        return [
            {"role": "system", "content": "".join(system_lines)},
            {"role": "user", "content": f"媒体库上下文：\n{context}\n\n用户问题：{question}"},
        ]

    def build_conversation_context(self) -> str:
        persistent = self.host.conversations.get(self.conversation_key)
        persistent_rows = persistent.get("recent") if isinstance(persistent.get("recent"), list) else []
        if persistent_rows:
            lines = ["最近 Telegram 对话上下文："]
            summary = str(persistent.get("summary") or "").strip()
            if summary:
                lines.append("长期摘要：")
                lines.append(summary)
            for row in persistent_rows:
                if not isinstance(row, dict):
                    continue
                lines.append(f"- {row.get('time', '')} 用户：{row.get('user', '')}")
                lines.append(f"  AI：{row.get('assistant', '')}")
            return "\n".join(lines)
        rows = self.host.conversations.get_chat_history(self.chat_id) if self.chat_id else []
        if not rows:
            return ""
        lines = ["最近 Telegram 对话上下文："]
        for row in rows[-6:]:
            if not isinstance(row, dict):
                continue
            lines.append(f"- {row.get('time', '')} 用户：{row.get('user', '')}")
            lines.append(f"  AI：{row.get('assistant', '')}")
        return "\n".join(lines)

    def build_active_media_context(self) -> str:
        session = self.host.conversations.get(self.conversation_key)
        media = session.get("activeMedia") if isinstance(session.get("activeMedia"), dict) else {}
        if not media:
            return ""
        return "\n".join(
            [
                "当前对话媒体：",
                f"- 标题：{media.get('title') or '未知'}",
                f"- Emby Series ID：{media.get('embySeriesId') or '未知'}",
                f"- TMDB ID：{media.get('tmdbId') or '未知'}",
                f"- 最新单集：{media.get('latestEpisode') or '未知'}",
                f"- 实际可读取：{media.get('actualEpisodeCount') or 0} 集",
                f"- TMDB 应有：{media.get('expectedEpisodeCount') or 0} 集",
                f"- 当前缺失：{media.get('missingEpisodeCount') or 0} 集",
                "- 规则：用户说“它/这部/这个/缺失的集”时指向该媒体。",
            ]
        )

    def build_recent_operations_context(self) -> str:
        try:
            events, _total = read_project_events(self.host.event_log_path, limit=60)
        except Exception as err:
            return f"最近项目操作：读取失败（{err}）。"
        important_actions = {
            "telegram_library_scan_submitted",
            "telegram_library_scan_failed",
            "telegram_library_scan_reply_sent",
            "telegram_library_scan_list_ready",
            "telegram_command_reply_failed",
            "telegram_ai_success",
            "telegram_ai_failed",
            "telegram_callback_failed",
            "client_sync_event",
        }
        rows = [event for event in events if isinstance(event, dict) and str(event.get("action") or "") in important_actions]
        if not rows:
            return "最近项目操作：暂无可用记录。"
        lines = ["最近项目操作："]
        for event in rows[:12]:
            action = str(event.get("action") or "").strip()
            message = str(event.get("message") or "").strip()
            detail = event.get("detail") if isinstance(event.get("detail"), dict) else {}
            summary = self.host.platform.format_ai_event_detail(action=action, detail=detail)
            suffix = f"；{summary}" if summary else ""
            lines.append(f"- {event.get('time', '')} {message or action}{suffix}")
        return "\n".join(lines)

    def build_media_context(self, *, question: str = "") -> str:
        parts: list[str] = []
        base_url, api_key = self.host.media_service.emby_context()
        parts.append("Emby 连接状态：已配置。" if base_url and api_key else "Emby 连接状态：未配置或不可用。")
        parts.append(self.build_tool_registry_context())

        stats_context = self.host.platform.build_library_stats_context()
        if stats_context:
            parts.append(stats_context)

        focus_context = self._query_service().build_focus_media_context(question)
        if focus_context:
            parts.append(focus_context)

        listing_context = self._query_service().build_category_listing_context(question)
        if listing_context:
            parts.append(listing_context)

        parts.extend(self.build_project_tool_contexts(question))

        try:
            latest, _tried, _err = self.host.media_service.fetch_latest_items_with_fallback(limit=8)
            rows = latest if isinstance(latest, list) else []
            if rows:
                latest_lines = [self.host.media_service.format_recent_library_row(row) for row in rows[:8] if isinstance(row, dict)]
                parts.append("最近入库：\n" + "\n".join(latest_lines))
            else:
                parts.append("最近入库：暂无可用数据。")
        except Exception as err:
            parts.append(f"最近入库：读取失败（{err}）。")

        try:
            service = PlaybackHistoryService(fetcher=self.host.media_service.emby_get, event_logger=None)
            result = service.collect(limit=8, scan_limit=600)
            rows = result.get("rows") if isinstance(result, dict) else []
            if rows:
                playback_lines = [self.host.media_formatter.format_recent_playback_row(row) for row in rows[:8] if isinstance(row, dict)]
                parts.append("最近播放：\n" + "\n".join(playback_lines))
            else:
                parts.append("最近播放：暂无可用数据。")
        except Exception as err:
            parts.append(f"最近播放：读取失败（{err}）。")
        return "\n\n".join(parts)

    def build_project_tool_contexts(self, question: str) -> list[str]:
        text = str(question or "").lower()
        contexts: list[str] = []
        if re.search(r"正在播放|当前播放|谁在看|在线|播放中", text):
            contexts.append(self.build_now_playing_context())
        if re.search(r"播放历史|最近.*看|看了什么|播放记录|播放最多|观看历史", text):
            contexts.append(self.build_playback_history_context())
        if re.search(r"任务|扫描|计划任务|后台任务|task|scheduled", text):
            contexts.append(self.build_tasks_context())
        if re.search(r"缺集|缺少|漏集|巡检|missing", text):
            contexts.append(self.build_missing_context())
        if re.search(r"日志|报错|错误|失败|异常|log|error", text):
            contexts.append(self.build_logs_context())
        if re.search(r"邀请|邀请码|注册|invite", text):
            contexts.append(self.build_invites_context())
        if re.search(r"用户|账号|会员|user", text):
            contexts.append(self.build_users_context())
        if re.search(r"排行|排名|年度|最多|榜|ranking", text):
            contexts.append(self.build_ranking_context())
        if re.search(r"质量|分辨率|码率|编码|4k|1080|720|hdr|quality", text):
            contexts.append(self.build_quality_context())
        if re.search(r"风险|异常|失败|告警|问题|健康|risk", text):
            contexts.append(self.build_risk_context())
        if re.search(r"客户端|设备|终端|在线|client|device|session", text):
            contexts.append(self.build_clients_context())
        if re.search(r"配置|设置|token|api key|apikey|密码|密钥|secret|config", text):
            contexts.append(self.build_settings_context())
        return [item for item in contexts if item]

    def build_tool_registry_context(self) -> str:
        tool_context = self.host.registry.tool_registry(
            conversation_key=self.conversation_key,
            chat_id=self.chat_id,
        ).format_context()
        subagent_rows = self.host.registry.subagent_registry(
            conversation_key=self.conversation_key,
            chat_id=self.chat_id,
        ).definitions()
        if not subagent_rows:
            return tool_context
        subagent_context = "\n".join(
            [
                "AI 子代理：",
                *[
                    f"- {row.name}：{row.description}（工具：{'、'.join(row.tool_names) or '暂无'}）"
                    + (f"；工作方式：{row.instruction}" if str(getattr(row, 'instruction', '') or '').strip() else "")
                    for row in subagent_rows
                ],
            ]
        )
        return f"{tool_context}\n{subagent_context}"

    def build_playback_history_context(self) -> str:
        return self._query_service().build_playback_history_context()

    def build_recent_library_summary_reply(self) -> str:
        return self._query_service().build_recent_library_summary_reply()

    def _query_service(self) -> AIQueryService:
        return AIQueryService(
            self.host,
            conversation_key=self.conversation_key,
            chat_id=self.chat_id,
        )

    def build_now_playing_context(self) -> str:
        try:
            sessions = self.host.media_service.emby_get("/Sessions")
        except Exception as err:
            return f"当前播放：读取失败（{self.host.media_service.format_emby_error(err)}）。"
        rows = sessions if isinstance(sessions, list) else []
        active: list[str] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            item = row.get("NowPlayingItem") if isinstance(row.get("NowPlayingItem"), dict) else {}
            name = str(item.get("Name") or "").strip()
            if not name:
                continue
            user_name = str(row.get("UserName") or row.get("UserId") or "未知用户").strip()
            client = str(row.get("Client") or row.get("DeviceName") or "未知客户端").strip()
            active.append(f"- {user_name} 正在看 {self.host.media_formatter.format_now_playing_title(item)}（{client}）")
        return "当前播放：\n" + ("\n".join(active[:8]) if active else "暂无活跃播放。")

    def build_tasks_context(self) -> str:
        try:
            tasks = self.host.media_service.fetch_scheduled_tasks()
        except Exception as err:
            return f"任务中心：读取失败（{self.host.media_service.format_emby_error(err)}）。"
        if not tasks:
            return "任务中心：没有读取到计划任务。"
        running = [task for task in tasks if str(task.get("State") or "").lower() == "running"]
        lines = ["任务中心：", f"- 任务总数：{len(tasks)}", f"- 正在运行：{len(running)} 个"]
        for task in tasks[:12]:
            if not isinstance(task, dict):
                continue
            name = str(task.get("Name") or task.get("Key") or task.get("Id") or "未命名任务").strip()
            state = str(task.get("State") or "Idle").strip()
            last = task.get("LastExecutionResult") if isinstance(task.get("LastExecutionResult"), dict) else {}
            last_status = str(last.get("Status") or "").strip() or "无记录"
            last_end = str(last.get("EndTimeUtc") or last.get("EndTime") or "").strip()
            lines.append(f"- {name}：{state}，上次结果 {last_status}{f'，结束 {last_end}' if last_end else ''}")
        return "\n".join(lines)

    def build_missing_context(self) -> str:
        cache = self.host.media_service.read_missing_scan_cache()
        if not cache:
            return "缺集巡检：暂无缓存，请先在后台或通过确认执行触发缺集巡检。"
        summary = cache.get("summary") if isinstance(cache.get("summary"), dict) else {}
        rows = cache.get("rows") if isinstance(cache.get("rows"), list) else []
        lines = [
            "缺集巡检摘要：",
            f"- 扫描剧集：{summary.get('scannedSeries', 0)}",
            f"- 缺集剧集：{summary.get('missingSeries', 0)}",
            f"- 缺失集数：{summary.get('missingEpisodeCount', 0)}",
            f"- 未匹配：{summary.get('unknownMatchCount', 0)}",
        ]
        for row in rows[:8]:
            if not isinstance(row, dict):
                continue
            name = str(row.get("seriesName") or "未知剧集").strip()
            season = row.get("seasonNo")
            missing = row.get("missingEpisodes") if isinstance(row.get("missingEpisodes"), list) else []
            status = str(row.get("status") or "").strip()
            lines.append(f"- {name} S{season}：{status}，缺 {missing[:12]}")
        return "\n".join(lines)

    def build_logs_context(self) -> str:
        try:
            events, total = read_project_events(self.host.event_log_path, limit=30)
        except Exception as err:
            return f"系统日志摘要：读取失败（{err}）。"
        if not events:
            return "系统日志摘要：暂无日志。"
        levels: dict[str, int] = {}
        modules: dict[str, int] = {}
        lines = ["系统日志摘要：", f"- 日志总数：{total}"]
        for event in events:
            level = str(event.get("level") or "info")
            module = str(event.get("module") or "system")
            levels[level] = int(levels.get(level) or 0) + 1
            modules[module] = int(modules.get(module) or 0) + 1
        lines.append("- 级别分布：" + "、".join(f"{key} {value}" for key, value in sorted(levels.items())))
        lines.append("- 模块分布：" + "、".join(f"{key} {value}" for key, value in sorted(modules.items())))
        lines.append("- 最近日志：")
        for event in events[:8]:
            message = str(event.get("message") or event.get("action") or "").strip()
            lines.append(f"  {event.get('time', '')} [{event.get('level', '')}/{event.get('module', '')}] {message[:120]}")
        return "\n".join(lines)

    def build_invites_context(self) -> str:
        store = self.host.platform.read_store()
        invites = store.get("invites") if isinstance(store.get("invites"), list) else []
        active = used = expired = 0
        rows = []
        now = datetime.now()
        for invite in invites:
            if not isinstance(invite, dict):
                continue
            status = str(invite.get("status") or "").strip()
            expires_at = str(invite.get("expiresAt") or "").strip()
            is_used = bool(str(invite.get("usedAt") or invite.get("createdUserId") or invite.get("usedUsername") or "").strip()) or status in {"已用", "used"}
            is_expired = False
            if expires_at:
                try:
                    is_expired = datetime.fromisoformat(expires_at).date() < now.date()
                except Exception:
                    is_expired = False
            if is_used:
                used += 1
                status_text = "已用"
            elif is_expired:
                expired += 1
                status_text = "过期"
            else:
                active += 1
                status_text = "可用"
            rows.append(f"- {invite.get('code', '')}：{status_text}，标签 {invite.get('label') or '-'}，到期 {expires_at or '未设置'}")
        return "\n".join(["邀请码摘要：", f"- 总数：{len(invites)}", f"- 可用：{active}", f"- 已用：{used}", f"- 过期：{expired}", *rows[:10]])

    def build_users_context(self) -> str:
        try:
            payload = self.host.media_service.emby_get("/Users")
        except Exception as err:
            return f"用户管理摘要：读取失败（{self.host.media_service.format_emby_error(err)}）。"
        rows = payload.get("Items") if isinstance(payload, dict) else payload
        users = [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []
        lines = ["用户管理摘要：", f"- 用户总数：{len(users)}"]
        for user in users[:15]:
            name = str(user.get("Name") or user.get("Username") or user.get("Id") or "未知用户").strip()
            policy = user.get("Policy") if isinstance(user.get("Policy"), dict) else {}
            disabled = bool(policy.get("IsDisabled"))
            admin = bool(policy.get("IsAdministrator"))
            lines.append(f"- {name}：{'管理员' if admin else '普通用户'}，{'禁用' if disabled else '启用'}")
        return "\n".join(lines)

    def build_ranking_context(self) -> str:
        try:
            service = PlaybackHistoryService(fetcher=self.host.media_service.emby_get, event_logger=None)
            result = service.collect(limit=80, scan_limit=2000)
            rows = result.get("rows") if isinstance(result, dict) else []
        except Exception as err:
            return f"播放排行摘要：读取失败（{err}）。"
        media_counts: dict[str, int] = {}
        user_counts: dict[str, int] = {}
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict):
                continue
            media = self.host.media_formatter.format_recent_playback_filename_with_status(row)[0]
            user = str(row.get("username") or row.get("user") or "未知用户").strip()
            media_counts[media] = int(media_counts.get(media) or 0) + 1
            user_counts[user] = int(user_counts.get(user) or 0) + 1
        media_top = sorted(media_counts.items(), key=lambda item: item[1], reverse=True)[:10]
        user_top = sorted(user_counts.items(), key=lambda item: item[1], reverse=True)[:10]
        lines = ["播放排行摘要：", f"- 样本记录：{len(rows) if isinstance(rows, list) else 0}"]
        lines.append("- 影片排行：" + ("、".join(f"{name} {count}次" for name, count in media_top) if media_top else "暂无"))
        lines.append("- 用户排行：" + ("、".join(f"{name} {count}次" for name, count in user_top) if user_top else "暂无"))
        return "\n".join(lines)

    def build_quality_context(self) -> str:
        try:
            query = urllib.parse.urlencode(
                {
                    "Recursive": "true",
                    "IncludeItemTypes": "Movie,Episode",
                    "Fields": "Name,Type,SeriesName,MediaSources,MediaStreams,Width,Height",
                    "Limit": "800",
                }
            )
            payload = self.host.media_service.emby_get(f"/Items?{query}")
        except Exception as err:
            return f"质量盘点摘要：读取失败（{self.host.media_service.format_emby_error(err)}）。"
        rows = payload.get("Items") if isinstance(payload, dict) else payload
        items = [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []
        buckets = {"4K": 0, "1080p": 0, "720p": 0, "其他": 0}
        hdr_count = 0
        for item in items:
            quality = self.host.media_formatter.format_media_quality(item)
            low = quality.lower()
            if "4k" in low:
                buckets["4K"] += 1
            elif "1080" in low:
                buckets["1080p"] += 1
            elif "720" in low:
                buckets["720p"] += 1
            else:
                buckets["其他"] += 1
            if "hdr" in low or "dovi" in low:
                hdr_count += 1
        return "\n".join(
            [
                "质量盘点摘要：",
                f"- 样本资源：{len(items)}",
                f"- 4K：{buckets['4K']}",
                f"- 1080p：{buckets['1080p']}",
                f"- 720p：{buckets['720p']}",
                f"- 其他/未知：{buckets['其他']}",
                f"- HDR/DoVi：{hdr_count}",
            ]
        )

    def build_risk_context(self) -> str:
        contexts = [self.build_logs_context(), self.build_tasks_context()]
        try:
            events, _total = read_project_events(self.host.event_log_path, limit=100)
            error_count = sum(1 for event in events if str(event.get("level") or "") == "error")
            warning_count = sum(1 for event in events if str(event.get("level") or "") == "warning")
            contexts.insert(0, f"风险概览：最近日志中 error {error_count} 条，warning {warning_count} 条。")
        except Exception:
            pass
        return "\n\n".join(contexts)

    def build_clients_context(self) -> str:
        try:
            sessions = self.host.media_service.emby_get("/Sessions")
        except Exception as err:
            return f"客户端摘要：读取失败（{self.host.media_service.format_emby_error(err)}）。"
        rows = sessions if isinstance(sessions, list) else []
        lines = ["客户端摘要：", f"- 在线会话：{len(rows)}"]
        for row in rows[:12]:
            if not isinstance(row, dict):
                continue
            user = str(row.get("UserName") or row.get("UserId") or "未知用户").strip()
            client = str(row.get("Client") or "未知客户端").strip()
            device = str(row.get("DeviceName") or row.get("DeviceId") or "未知设备").strip()
            item = row.get("NowPlayingItem") if isinstance(row.get("NowPlayingItem"), dict) else {}
            now = self.host.media_formatter.format_now_playing_title(item) if item else "未播放"
            lines.append(f"- {user}：{client} / {device} / {now}")
        return "\n".join(lines)

    def build_settings_context(self) -> str:
        store = self.host.platform.read_store()
        emby = self.host.platform.apply_emby_env_overrides(store.get("embyConfig"))
        bot = normalize_bot_config(store.get("botConfig"))
        ai = self.host.platform.load_ai_config(chat_id=self.chat_id)
        return "\n".join(
            [
                "系统设置摘要（已脱敏）：",
                f"- Emby 地址：{'已配置' if str(emby.get('serverUrl') or '').strip() else '未配置'}",
                f"- Emby API Key：{'已配置' if str(emby.get('apiKey') or '').strip() else '未配置'}",
                f"- TMDB Token：{'已配置' if str(emby.get('tmdbToken') or os.environ.get('APP_TMDB_TOKEN') or os.environ.get('TMDB_TOKEN') or '').strip() else '未配置'}",
                f"- Telegram Token：{'已配置' if str(bot.get('telegramToken') or '').strip() else '未配置'}",
                f"- Telegram Chat ID：{'已配置' if str(bot.get('telegramChatId') or '').strip() else '未配置'}",
                f"- AI 助手：{'启用' if ai.get('enabled') else '关闭'}",
                f"- AI Base URL：{'已配置' if str(ai.get('baseUrl') or '').strip() else '未配置'}",
                f"- AI API Key：{'已配置' if str(ai.get('apiKey') or '').strip() else '未配置'}",
                f"- AI 模型：{str(ai.get('model') or '未配置')}",
            ]
        )
