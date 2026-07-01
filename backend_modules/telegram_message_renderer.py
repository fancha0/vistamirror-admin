from __future__ import annotations

import re
import time
from typing import TYPE_CHECKING, Any

from .ai_assistant import stream_chat_completion
from .ai_host_adapter import AIHostAdapter

if TYPE_CHECKING:
    from .telegram_commands import TelegramCommandService


class TelegramMessageRenderer:
    def __init__(self, service: "AIHostAdapter | TelegramCommandService", *, chat_id: str = "") -> None:
        self.host = AIHostAdapter.coerce(service)
        self.sender = self.host.platform.sender
        self.chat_id = str(chat_id or "").strip()

    @staticmethod
    def normalize_ai_reply_text(body: Any, *, title: str = "") -> str:
        text = str(body or "").replace("\r\n", "\n").strip()
        normalized_title = str(title or "").strip()
        if normalized_title and text.startswith(normalized_title):
            text = text[len(normalized_title):].lstrip()
        if text.startswith("```") and text.endswith("```"):
            lines = text.splitlines()
            if len(lines) >= 3:
                text = "\n".join(lines[1:-1]).strip()
        lines = text.splitlines()
        if lines and lines[0].strip().lower() == "text":
            text = "\n".join(lines[1:]).strip()
        text = re.sub(r"^\s*🧠\s*AI\s*媒体问答\s*\n*", "", text, flags=re.IGNORECASE).strip()
        text = re.sub(r"^\s*AI\s*媒体问答\s*\n*", "", text, flags=re.IGNORECASE).strip()
        return text or "暂时没有可返回的内容。"

    def ai_markdown_reply(self, title: str, body: Any, *, reply_markup: dict[str, Any] | None = None) -> dict[str, Any]:
        plain = self.normalize_ai_reply_text(body, title=title)
        return {
            "text": plain,
            "parse_mode": "",
            "fallback_text": plain,
            "reply_markup": reply_markup if isinstance(reply_markup, dict) else None,
        }

    @staticmethod
    def _render_missing_episode_table(rows: list[dict[str, str]]) -> list[str]:
        headers = ["季", "已有集数", "总集数", "缺失集", "状态"]
        normalized_rows = [
            [
                str(row.get("seasonLabel") or "-"),
                str(row.get("existingText") or "-"),
                str(row.get("totalText") or "-"),
                str(row.get("missingText") or "-"),
                str(row.get("statusText") or "-"),
            ]
            for row in rows
            if isinstance(row, dict)
        ]
        widths = [len(header) for header in headers]
        for row in normalized_rows:
            for index, value in enumerate(row):
                widths[index] = max(widths[index], len(value))

        def _line(values: list[str]) -> str:
            return " | ".join(value.ljust(widths[index]) for index, value in enumerate(values))

        separator = "-+-".join("-" * width for width in widths)
        lines = [_line(headers), separator]
        lines.extend(_line(row) for row in normalized_rows)
        return lines

    @staticmethod
    def _fallback_missing_summary(report: dict[str, Any], *, reliable: bool) -> str:
        summary_text = str(report.get("summaryText") or "").strip()
        if summary_text:
            return summary_text
        if not reliable:
            return "当前编号映射异常，以下缺失仅供参考，不建议直接搜索。"
        labels = [str(value or "").strip() for value in report.get("missingLabels", []) if str(value or "").strip()]
        if not labels:
            total_count = int(report.get("airedCount") or report.get("registeredCount") or report.get("localCount") or 0)
            return f"全集 {total_count} 集均已入库，无缺失。"
        season_labels = [str(row.get("seasonLabel") or "").strip() for row in report.get("seasonRows", []) if isinstance(row, dict) and str(row.get("missingText") or "").strip() not in {"", "无"}]
        target = season_labels[0] if len(season_labels) == 1 else "相关季"
        return f"共缺失 {len(labels)} 集（{str(report.get('missingText') or '').strip() or '待确认'}），{target} 尚不完整。"

    def missing_episode_report_reply(self, report: dict[str, Any]) -> dict[str, Any]:
        title = str(report.get("title") or "未知作品").strip()
        year = str(report.get("year") or "年份未知").strip()
        mapping_confidence = str(report.get("mappingConfidence") or "").strip() or "high"
        mapping_warning = str(report.get("mappingWarning") or "").strip()
        reliable = bool(report.get("isReliable", mapping_confidence.lower() != "low" and not mapping_warning))
        search_count = max(0, int(report.get("searchCount") or 0))
        data_query_count = max(0, int(report.get("dataQueryCount") or 0))
        future_text = str(report.get("futureText") or "").strip()
        unknown_text = str(report.get("unknownText") or "").strip()
        detail_lines = [str(value or "").strip() for value in report.get("detailLines", []) if str(value or "").strip()]
        season_rows = [row for row in report.get("seasonRows", []) if isinstance(row, dict)]
        if not season_rows:
            season_rows = [
                {
                    "seasonLabel": "Season 1",
                    "existingText": str(report.get("localCount") or "0"),
                    "totalText": str(report.get("airedCount") or report.get("registeredCount") or "0"),
                    "missingText": str(report.get("missingText") or "无"),
                    "statusText": "⚠️ 待确认" if not reliable else ("⚠️ 部分缺失" if report.get("missingLabels") else "✅ 完整"),
                }
            ]
        summary_text = self._fallback_missing_summary({**report, "seasonRows": season_rows}, reliable=reliable)
        lines = [
            f"（执行 {search_count} 次搜索，查询 {data_query_count} 次数据）",
            "",
            f"📝 《{title}》（{year}）— 缺失集",
            "",
            "Emby 媒体库状态：",
            "",
        ]
        lines.extend(self._render_missing_episode_table(season_rows))
        if summary_text:
            lines.extend(["", summary_text])
        if future_text:
            lines.append(f"未来未播：{future_text}（不计入缺失）")
        if unknown_text:
            lines.append(f"播出日期未知：{unknown_text}（暂不计入缺失）")
        if detail_lines:
            lines.extend(["", "说明：", *[f"- {value}" for value in detail_lines]])
        unmapped_samples = [row for row in report.get("unmappedSamples", []) if isinstance(row, dict)]
        if unmapped_samples and not reliable:
            lines.append("")
            lines.append("未映射样例：")
            for row in unmapped_samples[:5]:
                name = str(row.get("name") or "").strip()
                path = str(row.get("path") or "").strip()
                raw_episode = str(row.get("rawEpisode") or row.get("episode") or "").strip() or "-"
                sample = f"- E{raw_episode}"
                if name:
                    sample += f" | {name}"
                if path:
                    sample += f" | {path}"
                lines.append(sample)
        if reliable and report.get("missingLabels"):
            lines.extend(["", "需要生成这些缺失集的搜索清单吗？"])
        fallback = "\n".join(lines).strip()
        reply_markup = None
        labels = [str(value or "").strip() for value in report.get("missingLabels", []) if str(value or "").strip()]
        if reliable and labels:
            action_id = self.host.actions.register_pending_missing_search(
                title=title,
                labels=labels,
                chat_id=self.chat_id,
            )
            reply_markup = {"inline_keyboard": [[{"text": "生成搜索清单", "callback_data": f"missing_search:{action_id}"}]]}
        return {
            "text": fallback,
            "parse_mode": "",
            "fallback_text": fallback,
            "reply_markup": reply_markup,
            "memory_text": str(report.get("memoryText") or fallback),
        }

    def missing_episode_identity_mismatch_reply(self, report: dict[str, Any]) -> dict[str, Any]:
        title = str(report.get("title") or report.get("targetTitle") or "未知作品").strip()
        target_title = str(report.get("targetTitle") or title).strip()
        target_year = str(report.get("targetYear") or "年份未知").strip()
        target_tmdb = str(report.get("targetTmdbId") or "-").strip() or "-"
        confidence_reason = str(report.get("confidenceReason") or "需要先从 Emby 本地候选里确认具体条目。").strip()
        action_id = str(report.get("actionId") or "").strip()
        candidates = [row for row in report.get("candidates", []) if isinstance(row, dict)]
        lines = [
            f"《{title}》缺集查询需要先确认作品身份：",
            f"- 目标作品：{target_title}（{target_year}）",
            f"- 目标 TMDB：{target_tmdb}",
            "",
            confidence_reason,
        ]
        if candidates:
            lines.extend(["", "Emby 本地候选："])
            for index, row in enumerate(candidates[:5], start=1):
                candidate_title = str(row.get("title") or "未知作品").strip()
                candidate_year = str(row.get("year") or "年份未知").strip()
                candidate_tmdb = str(row.get("tmdbId") or "-").strip() or "-"
                candidate_count = int(row.get("episodeCount") or 0)
                candidate_reason = str(row.get("scoreReason") or "").strip()
                lines.append(
                    f"{index}. {candidate_title}（{candidate_year}） / TMDB {candidate_tmdb} / {candidate_count} 集"
                    + (f" / {candidate_reason}" if candidate_reason else "")
                )
        reply_markup = None
        if action_id:
            rows: list[list[dict[str, str]]] = []
            for index, row in enumerate(candidates[:5]):
                candidate_title = str(row.get("title") or "未知作品").strip()
                candidate_year = str(row.get("year") or "年份未知").strip()
                rows.append([{"text": f"{index + 1}. {candidate_title}（{candidate_year}）", "callback_data": f"missing_identity:pick:{action_id}:{index}"}])
            rows.append([{"text": "查看候选详情", "callback_data": f"missing_identity:candidates:{action_id}"}])
            rows.append([{"text": "重新选择", "callback_data": f"missing_identity:reselect:{action_id}"}])
            reply_markup = {"inline_keyboard": rows}
        fallback = "\n".join(lines).strip()
        return {
            "text": fallback,
            "parse_mode": "",
            "fallback_text": fallback,
            "reply_markup": reply_markup,
            "memory_text": fallback,
        }

    def missing_episode_identity_candidates_reply(self, report: dict[str, Any]) -> dict[str, Any]:
        target_title = str(report.get("targetTitle") or report.get("title") or "未知作品").strip()
        candidates = [row for row in report.get("candidates", []) if isinstance(row, dict)]
        action_id = str(report.get("actionId") or "").strip()
        lines = [f"《{target_title}》请选择正确作品："]
        rows: list[list[dict[str, str]]] = []
        for index, row in enumerate(candidates[:5]):
            candidate_title = str(row.get("title") or target_title).strip()
            candidate_year = str(row.get("year") or "年份未知").strip()
            candidate_tmdb = str(row.get("tmdbId") or "-").strip() or "-"
            candidate_count = int(row.get("episodeCount") or 0)
            reason = str(row.get("scoreReason") or "").strip()
            lines.append(
                f"{index + 1}. {candidate_title}（{candidate_year}）/ TMDB {candidate_tmdb} / {candidate_count} 集"
                + (f" / {reason}" if reason else "")
            )
            if action_id:
                rows.append([{"text": f"{index + 1}. {candidate_title}（{candidate_year}）", "callback_data": f"missing_identity:pick:{action_id}:{index}"}])
        fallback = "\n".join(lines).strip()
        return {
            "text": fallback,
            "parse_mode": "",
            "fallback_text": fallback,
            "reply_markup": {"inline_keyboard": rows} if rows else None,
            "memory_text": fallback,
        }

    def send_ai_message(self, *, token: str, chat_id: str, title: str, body: Any) -> int:
        reply = self.ai_markdown_reply(title, body)
        text = str(reply.get("text") or "")
        fallback_text = str(reply.get("fallback_text") or text)
        try:
            result = self.sender.send_text(token=token, chat_id=chat_id, text=text, parse_mode="MarkdownV2")
        except Exception:
            result = self.sender.send_text(token=token, chat_id=chat_id, text=fallback_text)
        return self.host.platform.extract_telegram_message_id(result)

    def edit_ai_message(
        self,
        *,
        token: str,
        chat_id: str,
        message_id: int,
        title: str,
        body: Any,
        reply_markup: dict[str, Any] | None = None,
    ) -> None:
        reply = self.ai_markdown_reply(title, body, reply_markup=reply_markup)
        text = str(reply.get("text") or "")
        fallback = str(reply.get("fallback_text") or text)
        markup = reply.get("reply_markup") if isinstance(reply.get("reply_markup"), dict) else reply_markup
        try:
            self.sender.edit_message_text(
                token=token,
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                reply_markup=markup,
                parse_mode="MarkdownV2",
            )
        except Exception:
            self.sender.edit_message_text(
                token=token,
                chat_id=chat_id,
                message_id=message_id,
                text=fallback,
                reply_markup=markup,
                parse_mode="",
            )

    def stream_ai_answer_to_telegram(
        self,
        *,
        token: str,
        chat_id: str,
        message_id: int,
        ai_config: dict[str, Any],
        messages: list[dict[str, str]],
    ) -> str:
        if not message_id:
            raise RuntimeError("Telegram 占位消息发送失败，无法流式编辑")
        chunks: list[str] = []
        last_edit_at = 0.0
        min_interval = 1.8
        for chunk in stream_chat_completion(config=ai_config, messages=messages, timeout_seconds=60):
            chunks.append(chunk)
            answer = self.host.platform.truncate_text("".join(chunks), 3400)
            now = time.time()
            if now - last_edit_at < min_interval:
                continue
            self.edit_ai_message(
                token=token,
                chat_id=chat_id,
                message_id=message_id,
                title="",
                body=answer or "正在生成...",
            )
            last_edit_at = now
        final_answer = self.host.platform.truncate_text("".join(chunks).strip(), 3400)
        if not final_answer:
            raise RuntimeError("AI 流式返回内容为空")
        self.edit_ai_message(
            token=token,
            chat_id=chat_id,
            message_id=message_id,
            title="",
            body=final_answer,
        )
        return final_answer
