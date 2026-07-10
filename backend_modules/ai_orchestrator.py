from __future__ import annotations

import re
import time
import uuid
from typing import TYPE_CHECKING, Any

from .ai_host_adapter import AIHostAdapter
from .ai_intent_router import AiIntentRouter
from .ai_missing_episode_support import is_missing_episode_meta_question
from .ai_tool_handlers import is_library_directory_question, is_library_exists_question
from .ai_assistant import chat_completion

if TYPE_CHECKING:
    from .telegram_commands import TelegramCommandService
    from .ai_agent_service import CommandReply


class AIOrchestrator:
    def __init__(self, host: "AIHostAdapter | TelegramCommandService", *, conversation_key: str = "") -> None:
        self.host = AIHostAdapter.coerce(host)
        self.conversation_key = str(conversation_key or "").strip()

    def prepare_routed_question(
        self,
        question: str,
        *,
        ai_config: dict[str, Any],
    ) -> tuple[str, str]:
        original = str(question or "").strip()
        if is_missing_episode_meta_question(original):
            return original, ""
        if is_library_exists_question(original) or is_library_directory_question(original):
            return original, ""
        active = self.host.conversations.get_active_media(self.conversation_key)
        router = AiIntentRouter(chat_completion)
        route = router.route(original, config=ai_config, active_media=active)
        intent = str(route.get("intent") or "general_chat")
        media_intents = {
            "media_missing_episodes",
            "media_episode_progress",
            "media_detail",
            "media_search",
            "media_correction",
        }
        immediate = ""
        if intent in media_intents:
            route, immediate = self.validate_media_route(original, route=route)
        self.host.platform.log_project_event(
            level="info",
            module="webhook",
            action="telegram_ai_intent_routed",
            message="Telegram AI 意图识别完成。",
            detail={
                "intent": intent,
                "mediaTitle": str(route.get("mediaTitle") or ""),
                "useActiveMedia": bool(route.get("useActiveMedia")),
                "isCorrection": bool(route.get("isCorrection")),
                "confidence": route.get("confidence"),
                "source": str(route.get("source") or "fallback"),
                "validated": bool(route.get("validated")),
                "routerError": str(route.get("routerError") or ""),
            },
        )
        if immediate:
            return original, immediate

        title = str(route.get("mediaTitle") or "").strip()
        if bool(route.get("useActiveMedia")) and not title:
            title = str(active.get("title") or "").strip()
        if intent == "media_missing_episodes":
            if bool(route.get("useActiveMedia")):
                return "查看一下缺失的集", ""
            return (f"查看一下{title}的缺失集" if title else "查看一下缺失的集"), ""
        if intent == "media_episode_progress":
            if bool(route.get("useActiveMedia")):
                return "它现在最新多少集", ""
            return (f"{title}现在最新多少集" if title else "它现在最新多少集"), ""
        if intent == "media_detail":
            if bool(route.get("useActiveMedia")):
                return "它的简介详情", ""
            return (f"{title}的简介详情" if title else "它的简介详情"), ""
        if intent == "media_search" and title:
            return f"媒体库里有没有{title}", ""
        if intent == "media_correction" and title:
            return original, f"已确认，你要查询的是《{title}》。"
        return original, ""

    def validate_media_route(
        self,
        question: str,
        *,
        route: dict[str, Any],
    ) -> tuple[dict[str, Any], str]:
        output = dict(route)
        active = self.host.conversations.get_active_media(self.conversation_key)
        active_title = str(active.get("title") or "").strip()
        if bool(output.get("useActiveMedia")):
            if not active_title:
                return output, "当前没有记住正在讨论的作品，请带上片名再查询。"
            output["mediaTitle"] = active_title
            output["validated"] = True
            return output, ""

        llm_title = str(output.get("mediaTitle") or "").strip()
        rule_title = self.host.media.extract_ai_media_keyword(question)
        candidates = AiIntentRouter.title_candidates(
            question=question,
            llm_title=llm_title,
            rule_title=rule_title,
        )
        if (
            not candidates
            and active_title
            and str(output.get("intent") or "") in {"media_missing_episodes", "media_episode_progress", "media_detail"}
            and self.host.media.is_ai_reference_question(question)
        ):
            output["mediaTitle"] = active_title
            output["useActiveMedia"] = True
            output["validated"] = True
            output["resolvedIdentity"] = {
                "title": active_title,
                "year": active.get("year"),
                "type": active.get("type"),
                "tmdbId": str(active.get("tmdbId") or ""),
                "embySeriesId": str(active.get("embySeriesId") or ""),
            }
            return output, ""
        if not candidates:
            if bool(output.get("isCorrection")):
                return output, "我知道刚才理解错了，但还没识别出你要查询的片名。请直接发送作品名称。"
            return output, "请带上要查询的影视名称。"

        service = self.host.media_service.media_identity_service()
        preferred_type = "series" if str(output.get("mediaType") or "") == "tv" else str(output.get("mediaType") or "")
        ambiguous_candidates: list[dict[str, Any]] = []
        for candidate in candidates:
            try:
                resolution = service.resolve(candidate, preferred_type=preferred_type)
            except Exception:
                continue
            rows = resolution.get("candidates") if isinstance(resolution.get("candidates"), list) else []
            if resolution.get("ambiguous") and rows:
                ambiguous_candidates = rows
                continue
            identity = resolution.get("identity") if isinstance(resolution.get("identity"), dict) else {}
            emby_item = resolution.get("embyItem") if isinstance(resolution.get("embyItem"), dict) else {}
            resolved_title = str(identity.get("title") or emby_item.get("Name") or "").strip()
            if not resolved_title:
                continue
            output["mediaTitle"] = resolved_title
            output["mediaType"] = "tv" if str(identity.get("type") or emby_item.get("Type") or "").lower() in {"series", "tv"} else "movie"
            output["validated"] = True
            output["resolvedIdentity"] = {
                "title": resolved_title,
                "year": identity.get("year") or emby_item.get("ProductionYear"),
                "type": str(identity.get("type") or "series"),
                "tmdbId": str(identity.get("tmdbId") or ""),
                "embySeriesId": str(emby_item.get("Id") or identity.get("embyId") or ""),
            }
            if bool(output.get("isCorrection")) or str(output.get("intent")) == "media_correction":
                self.host.conversations.set_active_media(self.conversation_key, output["resolvedIdentity"])
            return output, ""

        if ambiguous_candidates:
            return output, self.host.media.format_ai_identity_candidates(candidates[0], ambiguous_candidates)
        shown = candidates[-1] if len(candidates) > 1 else candidates[0]
        return output, f"没有在 Emby 或 TMDB 中确认《{shown}》。请检查片名，或用书名号明确输入，例如《仙逆》。"

    def build_execution_proposal(self, question: str) -> "CommandReply | None":
        text = str(question or "").strip()
        renderer = self.host.platform.telegram_renderer()
        query_intent = bool(re.search(r"列出|列出来|全部列|查询|查找|看看|看一下|有哪些|哪些|显示|统计|清单|列表|片单|多少|什么|是否|有没有", text, flags=re.IGNORECASE))
        if query_intent:
            return None
        if self.host.media_service.is_library_scan_request(text):
            keyword = self.host.media_service.extract_library_scan_keyword(text)
            return self.host.media_service.cmd_scan_library(keyword)
        has_execute_intent = bool(re.search(r"执行|触发|启动|运行|生成|创建|新增|同步|开始.*任务|运行.*任务|执行.*任务|触发.*任务|run|start|generate|create|sync", text, flags=re.IGNORECASE))
        scan_execute_intent = bool(re.search(r"(开始|执行|运行|触发).{0,12}(媒体库)?扫描(任务)?", text, flags=re.IGNORECASE))
        refresh_execute_intent = bool(re.search(r"(刷新|执行|运行|触发).{0,12}(缺集|巡检|webhook|机器人.*状态)", text, flags=re.IGNORECASE))
        if not (has_execute_intent or scan_execute_intent or refresh_execute_intent):
            return None

        action: dict[str, Any] | None = None
        if re.search(r"邀请码|邀请|invite", text, flags=re.IGNORECASE) and re.search(r"生成|创建|新增|发一个|来一个|generate|create", text, flags=re.IGNORECASE):
            quantity_match = re.search(r"(\d{1,2})\s*(个|条|枚)?", text)
            quantity = int(quantity_match.group(1)) if quantity_match else 1
            quantity = max(1, min(quantity, 10))
            action = {
                "type": "invite_generate",
                "label": f"生成 {quantity} 个邀请码",
                "summary": "将生成新的可用邀请码并保存到后端。",
                "quantity": quantity,
            }
        elif re.search(r"邀请码|邀请|invite", text, flags=re.IGNORECASE) and re.search(r"同步|刷新|sync", text, flags=re.IGNORECASE):
            action = {
                "type": "invite_sync",
                "label": "同步邀请码状态摘要",
                "summary": "将重新读取邀请码存储并返回可用/已用/过期统计，不修改媒体服务器配置。",
            }
        elif re.search(r"缺集|漏集|巡检|missing", text, flags=re.IGNORECASE):
            action = {
                "type": "missing_scan",
                "label": "触发缺集巡检",
                "summary": "将扫描 Emby 剧集并结合 TMDB 判断缺失集数。耗时可能较长。",
            }
        elif re.search(r"webhook|机器人.*状态|状态刷新", text, flags=re.IGNORECASE):
            action = {
                "type": "webhook_status",
                "label": "刷新机器人 Webhook 状态",
                "summary": "将读取当前 Bot Webhook/轮询状态并返回摘要，不修改配置。",
            }
        elif re.search(r"任务|扫描|计划任务|媒体库|scheduled|task", text, flags=re.IGNORECASE):
            task = self.host.media_service.match_scheduled_task_for_question(text)
            if task:
                task_id = str(task.get("Id") or task.get("Key") or "").strip()
                task_name = str(task.get("Name") or task.get("Key") or task_id).strip()
                action = {
                    "type": "scheduled_task",
                    "label": f"运行计划任务：{task_name}",
                    "summary": f"将触发 Emby 计划任务“{task_name}”。",
                    "taskId": task_id,
                    "taskName": task_name,
                }
            else:
                return renderer.ai_markdown_reply(
                    "AI 执行建议",
                    "没有匹配到可执行的 Emby 计划任务。\n你可以说：帮我运行媒体库扫描任务。",
                )
        if not action:
            return None

        action_id = uuid.uuid4().hex[:12]
        action["createdAt"] = time.time()
        self.host.actions.register_pending_ai_action(action_id, action)
        body_lines = [
            f"操作：{action['label']}",
            f"说明：{action['summary']}",
            "",
            "点击“确认执行”才会真正运行；取消则不做任何改变。",
        ]
        return renderer.ai_markdown_reply(
            "AI 需要你确认后再执行",
            "\n".join(body_lines),
            reply_markup={
                "inline_keyboard": [
                    [
                        {"text": "确认执行", "callback_data": f"ai_exec:ok:{action_id}"},
                        {"text": "取消", "callback_data": f"ai_exec:cancel:{action_id}"},
                    ]
                ]
            },
        )
